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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_customers")
TABLE_NAME = os.environ.get("TABLE_NAME", "customers")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
INCREMENTAL_FIELD = os.environ.get("INCREMENTAL_FIELD", "updatedSince")
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
# Raw = keep source names as-is
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("online_user_id", "STRING"),
    bigquery.SchemaField("salon_user_id", "STRING"),
    bigquery.SchemaField("prename", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("mobile", "STRING"),
    bigquery.SchemaField("birthday", "DATE"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("salon_id", "INT64"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = [
    "online_user_id",
    "salon_user_id",
    "prename",
    "email",
    "mobile",
    "birthday",
    "name",
    "language",
    "gender",
    "created",
    "salon_id",
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
        raise ValueError("LOAD_MODE must be either 'full' or 'incremental'")


def extract_page_records(payload):
    if not payload:
        return []

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        return [payload]

    raise ValueError("Unsupported API response format")


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    params = build_incremental_params(
        load_mode=LOAD_MODE,
        incremental_field=INCREMENTAL_FIELD,
        incremental_lookback_days=INCREMENTAL_LOOKBACK_DAYS,
    )

    logger.info(f"Fetching data from API | url={API_URL} | params={params}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Sending request attempt {attempt}/{MAX_RETRIES}")

            response = requests.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else RATE_LIMIT_SLEEP_SECONDS
                )

                logger.warning(
                    f"Rate limit hit on attempt {attempt}/{MAX_RETRIES}. "
                    f"Waiting {wait_seconds} seconds before retry."
                )
                logger.warning(f"429 response headers: {dict(response.headers)}")
                logger.warning(f"429 response body: {response.text}")

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            data = response.json()
            records = extract_page_records(data)
            df = pd.json_normalize(records)

            logger.info(f"Fetched {len(df)} rows from API")
            logger.info(f"Columns received: {list(df.columns)}")

            return df

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on attempt {attempt}/{MAX_RETRIES}: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

    raise RuntimeError("Failed to fetch data from API")


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming dataframe for raw layer")

    missing_cols = [col for col in SELECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing expected raw columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[SELECTED_COLUMNS].copy()

    df["online_user_id"] = normalize_nullable_string(df["online_user_id"])
    df["salon_user_id"] = normalize_nullable_string(df["salon_user_id"])
    df["prename"] = normalize_nullable_string(df["prename"])
    df["email"] = normalize_nullable_string(df["email"])
    df["mobile"] = normalize_nullable_string(df["mobile"])
    df["name"] = normalize_nullable_string(df["name"])
    df["language"] = normalize_nullable_string(df["language"])
    df["gender"] = normalize_nullable_string(df["gender"])

    birthday_raw = normalize_nullable_string(df["birthday"])
    df["birthday"] = pd.to_datetime(birthday_raw, errors="coerce").dt.date

    df["created"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
    df["salon_id"] = pd.to_numeric(df["salon_id"], errors="coerce").astype("Int64")

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date
    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("online_user_id"),
            row.get("salon_user_id"),
            row.get("prename"),
            row.get("email"),
            row.get("mobile"),
            row.get("birthday"),
            row.get("name"),
            row.get("language"),
            row.get("gender"),
            row.get("created"),
            row.get("salon_id"),
        ),
        axis=1,
    )

    logger.info(f"Transformation complete | rows={len(df)}")
    return df


# =================================
# Main ETL
# =================================

def run_etl():
    client = get_bq_client()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.utcnow()

    logger.info(f"Pipeline started | pipeline={PIPELINE_NAME} | run_id={run_id}")
    logger.info(f"Target raw table: {RAW_TABLE}")

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
            f"Pipeline finished successfully | rows_loaded={len(df)} | run_id={run_id}"
        )
        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as e:
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
                message=str(e),
            )
        except Exception as log_error:
            logger.error(f"Could not log failed pipeline run: {log_error}")

        send_email(
            subject=f"❌ {PIPELINE_NAME} pipeline failed",
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")
        return f"Pipeline failed: {str(e)}", 500