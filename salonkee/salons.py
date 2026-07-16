import hashlib
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery

from shared.bq import get_bq_client, load_dataframe_in_chunks
from shared.mail import send_email
from shared.metadata import log_pipeline_run
from shared.utils import (
    build_incremental_params,
    generate_record_hash_from_values,
    normalize_nullable_string,
    validate_common_config,
)


logger = logging.getLogger(__name__)


# =================================
# Config
# =================================

PROJECT_ID = os.environ.get("PROJECT_ID", "cpb-data-platform-prod")
DATASET_RAW = os.environ.get("DATASET_RAW", "cpb_raw")
DATASET_META = os.environ.get("DATASET_META", "cpb_meta")

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_salons")
TABLE_NAME = os.environ.get("TABLE_NAME", "salons")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
INCREMENTAL_FIELD = os.environ.get("INCREMENTAL_FIELD", "updatedSince")
INCREMENTAL_LOOKBACK_DAYS = int(
    os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2)
)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
RATE_LIMIT_SLEEP_SECONDS = int(
    os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90)
)
RETRY_SLEEP_SECONDS = int(os.environ.get("RETRY_SLEEP_SECONDS", 5))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

RAW_TABLE = (
    f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
)
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
# Raw = keep source names as-is
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("id", "INT64"),
    bigquery.SchemaField("displayName", "STRING"),
    bigquery.SchemaField("link", "STRING"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = [
    "id",
    "displayName",
    "link",
]


# =================================
# Helpers
# =================================

def validate_config() -> None:
    validate_common_config({
        "PROJECT_ID": PROJECT_ID,
        "DATASET_RAW": DATASET_RAW,
        "DATASET_META": DATASET_META,
        "PIPELINE_NAME": PIPELINE_NAME,
        "TABLE_NAME": TABLE_NAME,
        "API_URL": API_URL,
        "API_TOKEN": API_TOKEN,
        "SOURCE_SYSTEM": SOURCE_SYSTEM,
    })

    if LOAD_MODE not in ["full", "incremental"]:
        raise ValueError(
            "LOAD_MODE must be either 'full' or 'incremental'"
        )

    if MAX_RETRIES < 1:
        raise ValueError("MAX_RETRIES must be at least 1")

    if REQUEST_TIMEOUT < 1:
        raise ValueError("REQUEST_TIMEOUT must be at least 1 second")


def normalize_api_token(token: str) -> str:
    """
    Ensures the token does not become:
    Authorization: Bearer Bearer <token>
    """
    normalized_token = token.strip()

    if normalized_token.lower().startswith("bearer "):
        normalized_token = normalized_token[7:].strip()

    if not normalized_token:
        raise ValueError("API_TOKEN is empty after normalization")

    return normalized_token


def extract_page_records(payload):
    if not payload:
        return []

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]

        return [payload]

    raise ValueError(
        f"Unsupported API response format: {type(payload).__name__}"
    )


def get_response_body_preview(
    response: requests.Response,
    max_length: int = 2000,
) -> str:
    """
    Returns a safe, truncated response-body preview for logging.
    """
    try:
        body = response.text
    except Exception:
        return "<response body could not be read>"

    if not body:
        return "<empty response body>"

    body = body.replace("\n", " ").replace("\r", " ")
    return body[:max_length]


def raise_for_api_status(response: requests.Response) -> None:
    """
    Logs detailed response information before raising an HTTP error.
    """
    if response.ok:
        return

    body_preview = get_response_body_preview(response)

    redirect_history = [
        {
            "status_code": redirect.status_code,
            "url": redirect.url,
            "location": redirect.headers.get("Location"),
        }
        for redirect in response.history
    ]

    logger.error(
        "Salonkee API request failed | "
        "method=%s | "
        "requested_url=%s | "
        "final_url=%s | "
        "status_code=%s | "
        "reason=%s | "
        "allow=%s | "
        "content_type=%s | "
        "server=%s | "
        "redirect_history=%s | "
        "response_body=%s",
        response.request.method,
        response.request.url,
        response.url,
        response.status_code,
        response.reason,
        response.headers.get("Allow"),
        response.headers.get("Content-Type"),
        response.headers.get("Server"),
        redirect_history,
        body_preview,
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise requests.exceptions.HTTPError(
            (
                f"{response.status_code} {response.reason} "
                f"for {response.request.method} {response.url}. "
                f"Allow={response.headers.get('Allow')}. "
                f"Response body={body_preview}"
            ),
            response=response,
            request=response.request,
        ) from exc


def should_retry_status(status_code: int) -> bool:
    """
    Retry only errors that can reasonably be temporary.

    405 is not retried because the same request method will continue
    returning the same response.
    """
    return status_code in {
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    }


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    token = normalize_api_token(API_TOKEN)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    params = build_incremental_params(
        load_mode=LOAD_MODE,
        incremental_field=INCREMENTAL_FIELD,
        incremental_lookback_days=INCREMENTAL_LOOKBACK_DAYS,
    )

    token_fingerprint = hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()[:12]

    logger.info(
        "Fetching data from API | "
        "method=GET | "
        "url=%s | "
        "params=%s | "
        "token_length=%s | "
        "token_fingerprint=%s",
        API_URL,
        params,
        len(token),
        token_fingerprint,
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Sending API request | attempt=%s/%s",
                attempt,
                MAX_RETRIES,
            )

            response = requests.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            logger.info(
                "API response received | "
                "attempt=%s/%s | "
                "method=%s | "
                "requested_url=%s | "
                "final_url=%s | "
                "status_code=%s | "
                "redirect_count=%s",
                attempt,
                MAX_RETRIES,
                response.request.method,
                response.request.url,
                response.url,
                response.status_code,
                len(response.history),
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")

                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else RATE_LIMIT_SLEEP_SECONDS
                )

                logger.warning(
                    "Rate limit reached | "
                    "attempt=%s/%s | "
                    "wait_seconds=%s | "
                    "headers=%s | "
                    "response_body=%s",
                    attempt,
                    MAX_RETRIES,
                    wait_seconds,
                    dict(response.headers),
                    get_response_body_preview(response),
                )

                if attempt == MAX_RETRIES:
                    raise_for_api_status(response)

                time.sleep(wait_seconds)
                continue

            if not response.ok:
                status_code = response.status_code

                try:
                    raise_for_api_status(response)
                except requests.exceptions.HTTPError:
                    if (
                        not should_retry_status(status_code)
                        or attempt == MAX_RETRIES
                    ):
                        raise

                    logger.warning(
                        "Retryable HTTP error | "
                        "status_code=%s | "
                        "attempt=%s/%s | "
                        "retry_in_seconds=%s",
                        status_code,
                        attempt,
                        MAX_RETRIES,
                        RETRY_SLEEP_SECONDS,
                    )

                    time.sleep(RETRY_SLEEP_SECONDS)
                    continue

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError as exc:
                body_preview = get_response_body_preview(response)

                raise ValueError(
                    "Salonkee returned a non-JSON response | "
                    f"status_code={response.status_code} | "
                    f"content_type="
                    f"{response.headers.get('Content-Type')} | "
                    f"body={body_preview}"
                ) from exc

            records = extract_page_records(data)
            df = pd.json_normalize(records)

            logger.info(
                "Fetched data successfully | rows=%s | columns=%s",
                len(df),
                list(df.columns),
            )

            return df

        except requests.exceptions.HTTPError as exc:
            status_code = (
                exc.response.status_code
                if exc.response is not None
                else None
            )

            logger.warning(
                "HTTP error | "
                "attempt=%s/%s | "
                "status_code=%s | "
                "error=%s",
                attempt,
                MAX_RETRIES,
                status_code,
                exc,
            )

            if (
                attempt == MAX_RETRIES
                or status_code is None
                or not should_retry_status(status_code)
            ):
                raise

            time.sleep(RETRY_SLEEP_SECONDS)

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            logger.warning(
                "Temporary request error | "
                "attempt=%s/%s | "
                "error_type=%s | "
                "error=%s",
                attempt,
                MAX_RETRIES,
                type(exc).__name__,
                exc,
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(RETRY_SLEEP_SECONDS)

        except requests.exceptions.RequestException as exc:
            logger.error(
                "Non-retryable request error | "
                "attempt=%s/%s | "
                "error_type=%s | "
                "error=%s",
                attempt,
                MAX_RETRIES,
                type(exc).__name__,
                exc,
            )
            raise

        except Exception as exc:
            logger.warning(
                "Attempt failed | "
                "attempt=%s/%s | "
                "error_type=%s | "
                "error=%s",
                attempt,
                MAX_RETRIES,
                type(exc).__name__,
                exc,
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(RETRY_SLEEP_SECONDS)

    raise RuntimeError("Failed to fetch data from API")


# =================================
# Transform
# =================================

def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    logger.info("Transforming dataframe for raw layer")

    missing_cols = [
        col
        for col in SELECTED_COLUMNS
        if col not in df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Missing expected raw columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[SELECTED_COLUMNS].copy()

    df["id"] = pd.to_numeric(
        df["id"],
        errors="coerce",
    ).astype("Int64")

    df["displayName"] = normalize_nullable_string(
        df["displayName"]
    )

    df["link"] = normalize_nullable_string(
        df["link"]
    )

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date

    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("id"),
            row.get("displayName"),
            row.get("link"),
        ),
        axis=1,
    )

    logger.info(
        "Transformation complete | rows=%s",
        len(df),
    )

    return df


# =================================
# Main ETL
# =================================

def run_etl():
    client = get_bq_client()

    run_id = datetime.utcnow().strftime(
        "%Y%m%d_%H%M%S"
    )

    started_at = datetime.utcnow()

    logger.info(
        "Pipeline started | pipeline=%s | run_id=%s",
        PIPELINE_NAME,
        run_id,
    )

    logger.info(
        "Target raw table: %s",
        RAW_TABLE,
    )

    try:
        validate_config()

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=TABLE_SCHEMA,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = datetime.utcnow()

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
            message="Pipeline succeeded",
        )

        logger.info(
            "Pipeline finished successfully | "
            "rows_loaded=%s | "
            "run_id=%s",
            len(df),
            run_id,
        )

        return (
            f"{len(df)} rows loaded into {RAW_TABLE}",
            200,
        )

    except Exception as exc:
        finished_at = datetime.utcnow()

        try:
            log_pipeline_run(
                client=client,
                meta_table=META_TABLE,
                pipeline_name=PIPELINE_NAME,
                run_id=run_id,
                status="FAILED",
                rows_loaded=0,
                started_at=started_at,
                finished_at=finished_at,
                message=str(exc),
            )

        except Exception as log_error:
            logger.error(
                "Could not log failed pipeline run: %s",
                log_error,
            )

        try:
            send_email(
                subject=f"❌ {PIPELINE_NAME} pipeline failed",
                body=(
                    f"Pipeline: {PIPELINE_NAME}\n"
                    f"Run ID: {run_id}\n"
                    f"Time: {finished_at}\n"
                    f"Error type: {type(exc).__name__}\n"
                    f"Error: {str(exc)}"
                ),
            )

        except Exception as email_error:
            logger.error(
                "Could not send failure email: %s",
                email_error,
            )

        logger.exception(
            "Pipeline failed | run_id=%s",
            run_id,
        )

        return (
            f"Pipeline failed: {str(exc)}",
            500,
        )