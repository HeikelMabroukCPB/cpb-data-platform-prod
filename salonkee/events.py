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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_events")
TABLE_NAME = os.environ.get("TABLE_NAME", "events")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
INCREMENTAL_FIELD = os.environ.get("INCREMENTAL_FIELD", "updatedSince")
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")  # YYYY-MM-DD
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")      # YYYY-MM-DD
WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()  # append | replace_window

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
    bigquery.SchemaField("employee_id", "INT64"),
    bigquery.SchemaField("booking_id", "INT64"),
    bigquery.SchemaField("start", "TIMESTAMP"),
    bigquery.SchemaField("end", "TIMESTAMP"),
    bigquery.SchemaField("duration", "INT64"),
    bigquery.SchemaField("service_name", "STRING"),
    bigquery.SchemaField("service_group_id", "INT64"),
    bigquery.SchemaField("service_group_name", "STRING"),
    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("service_id", "INT64"),
    bigquery.SchemaField("booking_not_attended", "INT64"),
    bigquery.SchemaField("is_online_booking", "INT64"),
    bigquery.SchemaField("service_item_ids", "STRING"),
    bigquery.SchemaField("salon_id", "INT64"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("payment_status", "STRING"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = [
    "employee_id",
    "booking_id",
    "start",
    "end",
    "duration",
    "service_name",
    "service_group_id",
    "service_group_name",
    "created",
    "service_id",
    "booking_not_attended",
    "is_online_booking",
    "service_item_ids",
    "salon_id",
    "customer_id",
    "payment_status",
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

    if LOAD_MODE not in ["full", "incremental", "backfill"]:
        raise ValueError("LOAD_MODE must be either 'full', 'incremental', or 'backfill'")

    if WRITE_MODE not in ["append", "replace_window"]:
        raise ValueError("WRITE_MODE must be either 'append' or 'replace_window'")

    if LOAD_MODE == "backfill":
        if not BACKFILL_START_DATE or not BACKFILL_END_DATE:
            raise ValueError(
                "BACKFILL_START_DATE and BACKFILL_END_DATE are required when LOAD_MODE='backfill'"
            )

        try:
            start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
            end_date = datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("BACKFILL_START_DATE and BACKFILL_END_DATE must be in YYYY-MM-DD format")

        if start_date > end_date:
            raise ValueError("BACKFILL_START_DATE cannot be later than BACKFILL_END_DATE")


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


def build_api_params():
    if LOAD_MODE == "full":
        return {}

    if LOAD_MODE == "incremental":
        return build_incremental_params(
            load_mode=LOAD_MODE,
            incremental_field=INCREMENTAL_FIELD,
            incremental_lookback_days=INCREMENTAL_LOOKBACK_DAYS,
        )

    if LOAD_MODE == "backfill":
        return {
            "startTime": BACKFILL_START_DATE,
            "endTime": BACKFILL_END_DATE,
        }

    raise ValueError(f"Unsupported LOAD_MODE: {LOAD_MODE}")


def delete_backfill_window(
    client: bigquery.Client,
    table_id: str,
    start_date: str,
    end_date: str,
) -> None:
    logger.info(
        f"Deleting existing rows from backfill window based on DATE(start) | "
        f"table={table_id} | start_date={start_date} | end_date={end_date}"
    )

    query = f"""
    DELETE FROM `{table_id}`
    WHERE DATE(start) BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    client.query(query, job_config=job_config).result()
    logger.info("Backfill window delete completed")


def apply_backfill_window_filter(df: pd.DataFrame) -> pd.DataFrame:
    if LOAD_MODE != "backfill":
        return df

    logger.info(
        f"Applying local backfill filter on start date | "
        f"start_date={BACKFILL_START_DATE} | end_date={BACKFILL_END_DATE}"
    )

    start_date = pd.to_datetime(BACKFILL_START_DATE).date()
    end_date = pd.to_datetime(BACKFILL_END_DATE).date()

    filtered_df = df[
        df["start"].notna() &
        (df["start"].dt.date >= start_date) &
        (df["start"].dt.date <= end_date)
    ].copy()

    logger.info(
        f"Backfill window filter complete | rows_before={len(df)} | rows_after={len(filtered_df)}"
    )
    return filtered_df


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    params = build_api_params()

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

    df["employee_id"] = pd.to_numeric(df["employee_id"], errors="coerce").astype("Int64")
    df["booking_id"] = pd.to_numeric(df["booking_id"], errors="coerce").astype("Int64")
    df["start"] = pd.to_datetime(df["start"], errors="coerce", utc=True)
    df["end"] = pd.to_datetime(df["end"], errors="coerce", utc=True)
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce").astype("Int64")
    df["service_name"] = normalize_nullable_string(df["service_name"])
    df["service_group_id"] = pd.to_numeric(df["service_group_id"], errors="coerce").astype("Int64")
    df["service_group_name"] = normalize_nullable_string(df["service_group_name"])
    df["created"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
    df["service_id"] = pd.to_numeric(df["service_id"], errors="coerce").astype("Int64")
    df["booking_not_attended"] = pd.to_numeric(df["booking_not_attended"], errors="coerce").astype("Int64")
    df["is_online_booking"] = pd.to_numeric(df["is_online_booking"], errors="coerce").astype("Int64")
    df["service_item_ids"] = normalize_nullable_string(df["service_item_ids"])
    df["salon_id"] = pd.to_numeric(df["salon_id"], errors="coerce").astype("Int64")
    df["customer_id"] = normalize_nullable_string(df["customer_id"])
    df["payment_status"] = normalize_nullable_string(df["payment_status"])

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date
    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("employee_id"),
            row.get("booking_id"),
            row.get("start"),
            row.get("end"),
            row.get("duration"),
            row.get("service_name"),
            row.get("service_group_id"),
            row.get("service_group_name"),
            row.get("created"),
            row.get("service_id"),
            row.get("booking_not_attended"),
            row.get("is_online_booking"),
            row.get("service_item_ids"),
            row.get("salon_id"),
            row.get("customer_id"),
            row.get("payment_status"),
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
    logger.info(
        f"Execution context | load_mode={LOAD_MODE} | write_mode={WRITE_MODE} | "
        f"backfill_start_date={BACKFILL_START_DATE} | backfill_end_date={BACKFILL_END_DATE}"
    )

    try:
        validate_config()

        if LOAD_MODE == "backfill" and WRITE_MODE == "replace_window":
            delete_backfill_window(
                client=client,
                table_id=RAW_TABLE,
                start_date=BACKFILL_START_DATE,
                end_date=BACKFILL_END_DATE,
            )

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        if LOAD_MODE == "backfill":
            df = apply_backfill_window_filter(df)

        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=TABLE_SCHEMA,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = datetime.utcnow()

        success_message = (
            f"Pipeline succeeded | load_mode={LOAD_MODE} | write_mode={WRITE_MODE}"
        )
        if LOAD_MODE == "backfill":
            success_message += f" | window_on_start={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
            message=success_message,
        )

        logger.info(
            f"Pipeline finished successfully | rows_loaded={len(df)} | run_id={run_id}"
        )
        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as e:
        finished_at = datetime.utcnow()

        error_message = str(e)
        if LOAD_MODE == "backfill":
            error_message = (
                f"{error_message} | load_mode={LOAD_MODE} | "
                f"window_on_start={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"
            )

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
                message=error_message,
            )
        except Exception as log_error:
            logger.error(f"Could not log failed pipeline run: {log_error}")

        send_email(
            subject=f"❌ {PIPELINE_NAME} pipeline failed",
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
                f"Load mode: {LOAD_MODE}\n"
                f"Write mode: {WRITE_MODE}\n"
                f"Backfill start date: {BACKFILL_START_DATE}\n"
                f"Backfill end date: {BACKFILL_END_DATE}\n"
                f"Backfill basis: start\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")
        return f"Pipeline failed: {str(e)}", 500