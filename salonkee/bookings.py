import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from google.cloud import bigquery

from shared.bq import get_bq_client, load_dataframe_in_chunks
from shared.mail import send_email
from shared.metadata import log_pipeline_run
from shared.utils import (
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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_bookings")
TABLE_NAME = os.environ.get("TABLE_NAME", "bookings")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()  # append | replace_window

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")  # YYYY-MM-DD
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")      # YYYY-MM-DD

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

# API limitation:
# The API only allows filtering on booking startTime and max 100-day ranges.
# For incremental loads, we scan 1 year into the future starting from yesterday.
CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS = int(
    os.environ.get("CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS", 365)
)

API_MAX_RANGE_DAYS = int(os.environ.get("API_MAX_RANGE_DAYS", 100))

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
# Raw = keep source names as-is
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("booking_id", "INT64"),
    bigquery.SchemaField("date", "TIMESTAMP"),
    bigquery.SchemaField("notAttended", "INT64"),
    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("updated", "TIMESTAMP"),
    bigquery.SchemaField("isOnlineBooking", "INT64"),
    bigquery.SchemaField("salon_id", "INT64"),
    bigquery.SchemaField("customer_id", "STRING"),

    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = [
    "booking_id",
    "date",
    "notAttended",
    "created",
    "updated",
    "isOnlineBooking",
    "salon_id",
    "customer_id",
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

    if API_MAX_RANGE_DAYS <= 0:
        raise ValueError("API_MAX_RANGE_DAYS must be greater than 0")

    if API_MAX_RANGE_DAYS > 100:
        raise ValueError("API_MAX_RANGE_DAYS cannot be greater than 100 because of the API limit")

    if CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS <= 0:
        raise ValueError("CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS must be greater than 0")

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


def normalize_json_field(value):
    if pd.isna(value) or value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    return str(value)


def resolve_window_field() -> str:
    """
    For this bookings pipeline, the incremental business logic is:
    ingest records created yesterday.

    The API cannot filter on created.
    The API only filters on booking startTime.
    Therefore:
    - API window field = startTime
    - BigQuery replace/delete window field = created
    """
    return "created"


def get_today_utc_date():
    return datetime.now(timezone.utc).date()


def get_yesterday_utc_date():
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def get_incremental_window_dates():
    """
    BigQuery replacement window for incremental loads.

    Since the incremental logic loads records created yesterday,
    the delete window should only delete yesterday's created records.
    """
    yesterday = get_yesterday_utc_date()
    return yesterday, yesterday


def build_starttime_windows(start_date, end_date, max_range_days=100):
    """
    Builds API windows because the API only allows a max range of 100 days
    on startTime / endTime.
    """
    windows = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=max_range_days - 1),
            end_date
        )

        windows.append({
            "startTime": current_start.isoformat(),
            "endTime": current_end.isoformat(),
        })

        current_start = current_end + timedelta(days=1)

    return windows


def delete_window(
    client: bigquery.Client,
    table_id: str,
    start_date: str,
    end_date: str,
    window_field: str,
) -> None:
    logger.info(
        f"Deleting existing rows from window | table={table_id} | "
        f"window_field={window_field} | start_date={start_date} | end_date={end_date}"
    )

    query = f"""
    DELETE FROM `{table_id}`
    WHERE DATE({window_field}) BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    client.query(query, job_config=job_config).result()
    logger.info("Window delete completed")


def apply_window_filter(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    window_field: str,
) -> pd.DataFrame:
    logger.info(
        f"Applying local window filter | "
        f"window_field={window_field} | start_date={start_date} | end_date={end_date}"
    )

    if df.empty:
        logger.info("Dataframe is empty. Skipping local window filter.")
        return df

    if window_field not in df.columns:
        raise ValueError(
            f"Window field '{window_field}' not found in dataframe. "
            f"Available columns: {list(df.columns)}"
        )

    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    filtered_df = df[
        df[window_field].notna()
        & (df[window_field].dt.date >= start_date)
        & (df[window_field].dt.date <= end_date)
    ].copy()

    logger.info(
        f"Window filter complete | rows_before={len(df)} | rows_after={len(filtered_df)}"
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

    all_records = []

    if LOAD_MODE == "incremental":
        yesterday = get_yesterday_utc_date()

        start_date = yesterday
        end_date = yesterday + timedelta(days=CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS)

        api_windows = build_starttime_windows(
            start_date=start_date,
            end_date=end_date,
            max_range_days=API_MAX_RANGE_DAYS,
        )

        logger.info(
            f"Incremental created-yesterday load | "
            f"scanning booking startTime from {start_date} to {end_date} | "
            f"lookahead_days={CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS} | "
            f"windows={len(api_windows)}"
        )

    elif LOAD_MODE == "backfill":
        start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
        end_date = datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").date()

        api_windows = build_starttime_windows(
            start_date=start_date,
            end_date=end_date,
            max_range_days=API_MAX_RANGE_DAYS,
        )

        logger.info(
            f"Backfill load | scanning booking startTime from {start_date} to {end_date} | "
            f"windows={len(api_windows)}"
        )

    else:
        api_windows = [{}]
        logger.info("Full load | fetching without startTime/endTime params")

    for params in api_windows:
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

                logger.info(
                    f"Fetched {len(records)} rows from API window | params={params}"
                )

                all_records.extend(records)
                break

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

    df = pd.json_normalize(all_records)

    logger.info(f"Fetched total rows before local filtering: {len(df)}")

    if not df.empty:
        logger.info(f"Columns received: {list(df.columns)}")
    else:
        logger.info("No rows received from API")

    return df


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming dataframe for raw layer")

    if df.empty:
        logger.info("Input dataframe is empty. Returning empty dataframe with target columns.")

        empty_df = pd.DataFrame(columns=[
            *SELECTED_COLUMNS,
            "source_system",
            "run_id",
            "load_timestamp",
            "load_date",
            "record_hash",
        ])

        return empty_df

    missing_cols = [col for col in SELECTED_COLUMNS if col not in df.columns]

    if missing_cols:
        raise ValueError(
            f"Missing expected raw columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[SELECTED_COLUMNS].copy()

    df["booking_id"] = pd.to_numeric(df["booking_id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    df["notAttended"] = pd.to_numeric(df["notAttended"], errors="coerce").astype("Int64")

    df["created"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
    df["updated"] = pd.to_datetime(df["updated"], errors="coerce", utc=True)

    df["isOnlineBooking"] = pd.to_numeric(df["isOnlineBooking"], errors="coerce").astype("Int64")

    df["salon_id"] = pd.to_numeric(df["salon_id"], errors="coerce").astype("Int64")
    df["customer_id"] = normalize_nullable_string(df["customer_id"])

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date

    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("booking_id"),
            row.get("date"),
            row.get("notAttended"),
            row.get("created"),
            row.get("updated"),
            row.get("isOnlineBooking"),
            row.get("salon_id"),
            row.get("customer_id"),
        ),
        axis=1,
    )

    logger.info(f"Transformation complete | rows={len(df)}")

    return df


def filter_incremental_created_yesterday(df: pd.DataFrame) -> pd.DataFrame:
    """
    The API only allows filtering on booking startTime.
    So in incremental mode we fetch all bookings with startTime between:
    yesterday and yesterday + 365 days.

    Then we keep only the records created yesterday.
    """
    if df.empty:
        logger.info("Dataframe is empty. Skipping created-yesterday filter.")
        return df

    yesterday = get_yesterday_utc_date()
    rows_before = len(df)

    df = df[
        df["created"].notna()
        & (df["created"].dt.date == yesterday)
    ].copy()

    logger.info(
        f"Filtered to bookings created yesterday | "
        f"created_date={yesterday} | rows_before={rows_before} | rows_after={len(df)}"
    )

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
        f"backfill_start_date={BACKFILL_START_DATE} | backfill_end_date={BACKFILL_END_DATE} | "
        f"created_records_future_lookahead_days={CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS} | "
        f"api_max_range_days={API_MAX_RANGE_DAYS}"
    )

    try:
        validate_config()

        window_field = resolve_window_field()
        window_start = None
        window_end = None

        if LOAD_MODE == "backfill" and WRITE_MODE == "replace_window":
            window_start = BACKFILL_START_DATE
            window_end = BACKFILL_END_DATE

            delete_window(
                client=client,
                table_id=RAW_TABLE,
                start_date=window_start,
                end_date=window_end,
                window_field=window_field,
            )

        if LOAD_MODE == "incremental" and WRITE_MODE == "replace_window":
            incremental_start, incremental_end = get_incremental_window_dates()

            window_start = incremental_start.isoformat()
            window_end = incremental_end.isoformat()

            delete_window(
                client=client,
                table_id=RAW_TABLE,
                start_date=window_start,
                end_date=window_end,
                window_field=window_field,
            )

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        if LOAD_MODE == "incremental":
            df = filter_incremental_created_yesterday(df)

        if LOAD_MODE == "backfill" and WRITE_MODE == "replace_window":
            df = apply_window_filter(
                df=df,
                start_date=window_start,
                end_date=window_end,
                window_field=window_field,
            )

        if not df.empty:
            load_dataframe_in_chunks(
                client=client,
                df=df,
                table_id=RAW_TABLE,
                schema=TABLE_SCHEMA,
                chunk_size=CHUNK_SIZE,
            )
        else:
            logger.info("No rows to load into BigQuery.")

        finished_at = datetime.utcnow()

        success_message = (
            f"Pipeline succeeded | load_mode={LOAD_MODE} | write_mode={WRITE_MODE}"
        )

        if LOAD_MODE == "backfill":
            success_message += (
                f" | window={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"
            )

        if LOAD_MODE == "incremental":
            success_message += (
                f" | created_window={window_start} to {window_end}"
                f" | scanned_startTime_days={CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS}"
            )

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
                f"window={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"
            )

        if LOAD_MODE == "incremental" and WRITE_MODE == "replace_window":
            incremental_start, incremental_end = get_incremental_window_dates()

            error_message = (
                f"{error_message} | load_mode={LOAD_MODE} | write_mode={WRITE_MODE} | "
                f"created_window={incremental_start.isoformat()} to {incremental_end.isoformat()} | "
                f"scanned_startTime_days={CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS}"
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
                f"Created records future lookahead days: {CREATED_RECORDS_FUTURE_LOOKAHEAD_DAYS}\n"
                f"API max range days: {API_MAX_RANGE_DAYS}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")

        return f"Pipeline failed: {str(e)}", 500