import logging
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
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

PIPELINE_NAME = os.environ.get(
    "PIPELINE_NAME",
    "salonkee_events_future",
)
TABLE_NAME = os.environ.get(
    "TABLE_NAME",
    "events_future",
)

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

# Dynamic future range:
#   default = tomorrow through the following 365 calendar days.
FUTURE_START_OFFSET_DAYS = int(
    os.environ.get("FUTURE_START_OFFSET_DAYS", 1)
)
FUTURE_LOOKAHEAD_DAYS = int(
    os.environ.get("FUTURE_LOOKAHEAD_DAYS", 365)
)

# Optional fixed range for testing or one-off reloads.
# Supply both values or neither.
FUTURE_START_DATE = os.environ.get("FUTURE_START_DATE")  # YYYY-MM-DD
FUTURE_END_DATE = os.environ.get("FUTURE_END_DATE")      # YYYY-MM-DD

# Salonkee accepts at most 90 calendar days per request.
MAX_API_WINDOW_DAYS = int(
    os.environ.get("MAX_API_WINDOW_DAYS", 90)
)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
RATE_LIMIT_SLEEP_SECONDS = int(
    os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90)
)

BUSINESS_TIMEZONE = os.environ.get(
    "BUSINESS_TIMEZONE",
    "Europe/Brussels",
)

ALLOW_EMPTY_REPLACE = (
    os.environ.get("ALLOW_EMPTY_REPLACE", "false")
    .strip()
    .lower()
    in {"1", "true", "yes", "y"}
)

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

RAW_TABLE = (
    f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
)
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
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
# Validation and date windows
# =================================

def parse_iso_date(value: str, variable_name: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"{variable_name} must use YYYY-MM-DD format"
        ) from exc


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

    try:
        ZoneInfo(BUSINESS_TIMEZONE)
    except Exception as exc:
        raise ValueError(
            f"Invalid BUSINESS_TIMEZONE: {BUSINESS_TIMEZONE}"
        ) from exc

    if FUTURE_START_OFFSET_DAYS < 0:
        raise ValueError(
            "FUTURE_START_OFFSET_DAYS cannot be negative"
        )

    if FUTURE_LOOKAHEAD_DAYS < 1:
        raise ValueError(
            "FUTURE_LOOKAHEAD_DAYS must be at least 1"
        )

    if not 1 <= MAX_API_WINDOW_DAYS <= 90:
        raise ValueError(
            "MAX_API_WINDOW_DAYS must be between 1 and 90"
        )

    fixed_start_supplied = bool(FUTURE_START_DATE)
    fixed_end_supplied = bool(FUTURE_END_DATE)

    if fixed_start_supplied != fixed_end_supplied:
        raise ValueError(
            "FUTURE_START_DATE and FUTURE_END_DATE must either "
            "both be supplied or both be empty"
        )

    if fixed_start_supplied:
        start_date = parse_iso_date(
            FUTURE_START_DATE,
            "FUTURE_START_DATE",
        )
        end_date = parse_iso_date(
            FUTURE_END_DATE,
            "FUTURE_END_DATE",
        )

        if start_date > end_date:
            raise ValueError(
                "FUTURE_START_DATE cannot be later than "
                "FUTURE_END_DATE"
            )


def get_future_range():
    if FUTURE_START_DATE and FUTURE_END_DATE:
        return (
            parse_iso_date(
                FUTURE_START_DATE,
                "FUTURE_START_DATE",
            ),
            parse_iso_date(
                FUTURE_END_DATE,
                "FUTURE_END_DATE",
            ),
        )

    local_today = datetime.now(
        ZoneInfo(BUSINESS_TIMEZONE)
    ).date()

    start_date = local_today + timedelta(
        days=FUTURE_START_OFFSET_DAYS
    )
    end_date = start_date + timedelta(
        days=FUTURE_LOOKAHEAD_DAYS - 1
    )

    return start_date, end_date


def split_date_windows(
    start_date,
    end_date,
    max_window_days: int,
):
    windows = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(
                days=max_window_days - 1
            ),
            end_date,
        )
        windows.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return windows


# =================================
# BigQuery table handling
# =================================

def ensure_target_table(
    client: bigquery.Client,
    table_id: str,
) -> None:
    try:
        client.get_table(table_id)
        logger.info(f"Target table exists | table={table_id}")
        return
    except NotFound:
        logger.info(
            f"Target table does not exist; creating it | "
            f"table={table_id}"
        )

    table = bigquery.Table(
        table_id,
        schema=TABLE_SCHEMA,
    )

    # The table is current-state future data, so partitioning on the
    # appointment start timestamp supports future-date filtering and
    # efficient refresh validation.
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="start",
    )
    table.clustering_fields = [
        "salon_id",
        "booking_id",
        "customer_id",
    ]

    client.create_table(table)
    logger.info(f"Target table created | table={table_id}")


def create_staging_table(
    client: bigquery.Client,
    staging_table_id: str,
) -> None:
    table = bigquery.Table(
        staging_table_id,
        schema=TABLE_SCHEMA,
    )
    table.expires = (
        datetime.now(timezone.utc) + timedelta(days=1)
    )

    client.create_table(table)
    logger.info(
        f"Staging table created | table={staging_table_id}"
    )


def replace_target_from_staging(
    client: bigquery.Client,
    target_table_id: str,
    staging_table_id: str,
) -> None:
    column_list = ",\n        ".join(
        f"`{field.name}`"
        for field in TABLE_SCHEMA
    )

    query = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{target_table_id}`
    WHERE TRUE;

    INSERT INTO `{target_table_id}` (
        {column_list}
    )
    SELECT
        {column_list}
    FROM `{staging_table_id}`;

    COMMIT TRANSACTION;
    """

    logger.info(
        f"Replacing future table from staging | "
        f"target={target_table_id} | "
        f"staging={staging_table_id}"
    )

    client.query(query).result()

    logger.info(
        "Target replacement completed successfully"
    )


# =================================
# API response handling
# =================================

def extract_page_records(payload):
    if not payload:
        return []

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if (
            "data" in payload
            and isinstance(payload["data"], list)
        ):
            return payload["data"]
        return [payload]

    raise ValueError(
        "Unsupported API response format"
    )


def build_api_params(
    start_date,
    end_date,
):
    return {
        "startTime": start_date.isoformat(),
        "endTime": end_date.isoformat(),
    }


def fetch_data(
    start_date,
    end_date,
) -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    params = build_api_params(
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(
        f"Fetching future events | "
        f"url={API_URL} | params={params}"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"Sending request attempt "
                f"{attempt}/{MAX_RETRIES}"
            )

            response = requests.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After"
                )
                wait_seconds = (
                    int(retry_after)
                    if (
                        retry_after
                        and retry_after.isdigit()
                    )
                    else RATE_LIMIT_SLEEP_SECONDS
                )

                logger.warning(
                    f"Rate limit hit on attempt "
                    f"{attempt}/{MAX_RETRIES}. "
                    f"Waiting {wait_seconds} seconds."
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            try:
                payload = response.json()
            except ValueError as exc:
                body_preview = response.text[:1000]
                raise ValueError(
                    "API returned a non-JSON response. "
                    f"Body preview: {body_preview}"
                ) from exc

            records = extract_page_records(payload)
            df = pd.json_normalize(records)

            logger.info(
                f"Fetched future events | "
                f"rows={len(df)} | "
                f"window={start_date} to {end_date}"
            )

            return df

        except requests.exceptions.HTTPError as exc:
            logger.warning(
                f"HTTP error on attempt "
                f"{attempt}/{MAX_RETRIES}: {exc}"
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

        except Exception as exc:
            logger.warning(
                f"Attempt {attempt}/{MAX_RETRIES} failed: "
                f"{exc}"
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

    raise RuntimeError(
        "Failed to fetch future events"
    )


# =================================
# Transform
# =================================

def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    logger.info(
        "Transforming future events for raw layer"
    )

    missing_cols = [
        column
        for column in SELECTED_COLUMNS
        if column not in df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Missing expected raw columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[SELECTED_COLUMNS].copy()

    df["employee_id"] = pd.to_numeric(
        df["employee_id"],
        errors="coerce",
    ).astype("Int64")

    df["booking_id"] = pd.to_numeric(
        df["booking_id"],
        errors="coerce",
    ).astype("Int64")

    df["start"] = pd.to_datetime(
        df["start"],
        errors="coerce",
        utc=True,
    )

    df["end"] = pd.to_datetime(
        df["end"],
        errors="coerce",
        utc=True,
    )

    df["duration"] = pd.to_numeric(
        df["duration"],
        errors="coerce",
    ).astype("Int64")

    df["service_name"] = normalize_nullable_string(
        df["service_name"]
    )

    df["service_group_id"] = pd.to_numeric(
        df["service_group_id"],
        errors="coerce",
    ).astype("Int64")

    df["service_group_name"] = (
        normalize_nullable_string(
            df["service_group_name"]
        )
    )

    df["created"] = pd.to_datetime(
        df["created"],
        errors="coerce",
        utc=True,
    )

    df["service_id"] = pd.to_numeric(
        df["service_id"],
        errors="coerce",
    ).astype("Int64")

    df["booking_not_attended"] = pd.to_numeric(
        df["booking_not_attended"],
        errors="coerce",
    ).astype("Int64")

    df["is_online_booking"] = pd.to_numeric(
        df["is_online_booking"],
        errors="coerce",
    ).astype("Int64")

    df["service_item_ids"] = (
        normalize_nullable_string(
            df["service_item_ids"]
        )
    )

    df["salon_id"] = pd.to_numeric(
        df["salon_id"],
        errors="coerce",
    ).astype("Int64")

    df["customer_id"] = normalize_nullable_string(
        df["customer_id"]
    )

    df["payment_status"] = normalize_nullable_string(
        df["payment_status"]
    )

    load_timestamp = datetime.now(timezone.utc)
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

    logger.info(
        f"Transformation complete | rows={len(df)}"
    )
    return df


def apply_window_filter(
    df: pd.DataFrame,
    start_date,
    end_date,
) -> pd.DataFrame:
    if "start" not in df.columns:
        raise ValueError(
            "Window field 'start' is missing"
        )

    local_start_dates = (
        df["start"]
        .dt.tz_convert(BUSINESS_TIMEZONE)
        .dt.date
    )

    filtered_df = df[
        df["start"].notna()
        & (local_start_dates >= start_date)
        & (local_start_dates <= end_date)
    ].copy()

    # Remove only exact duplicate source records. A booking may contain
    # multiple service rows, so booking_id alone is not a safe key.
    filtered_df = filtered_df.drop_duplicates(
        subset=["record_hash"],
        keep="last",
    )

    logger.info(
        f"Local future-window filter complete | "
        f"rows_before={len(df)} | "
        f"rows_after={len(filtered_df)} | "
        f"window={start_date} to {end_date}"
    )

    return filtered_df


# =================================
# Main ETL
# =================================

def run_etl():
    client = get_bq_client()
    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )
    started_at = datetime.now(timezone.utc)

    staging_table_id = (
        f"{RAW_TABLE}_staging_{run_id}"
    )
    staging_created = False
    current_window = None
    future_start = None
    future_end = None
    total_rows_loaded = 0

    logger.info(
        f"Future-events pipeline started | "
        f"pipeline={PIPELINE_NAME} | run_id={run_id}"
    )
    logger.info(
        f"Target raw table: {RAW_TABLE}"
    )

    try:
        validate_config()

        future_start, future_end = get_future_range()
        windows = split_date_windows(
            start_date=future_start,
            end_date=future_end,
            max_window_days=MAX_API_WINDOW_DAYS,
        )

        logger.info(
            f"Future refresh range | "
            f"start={future_start} | "
            f"end={future_end} | "
            f"window_count={len(windows)} | "
            f"max_api_window_days="
            f"{MAX_API_WINDOW_DAYS}"
        )

        ensure_target_table(
            client=client,
            table_id=RAW_TABLE,
        )

        create_staging_table(
            client=client,
            staging_table_id=staging_table_id,
        )
        staging_created = True

        for window_number, (
            window_start,
            window_end,
        ) in enumerate(windows, start=1):
            current_window = (
                window_start,
                window_end,
            )

            logger.info(
                f"Processing future window "
                f"{window_number}/{len(windows)} | "
                f"{window_start} to {window_end}"
            )

            raw_df = fetch_data(
                start_date=window_start,
                end_date=window_end,
            )

            if raw_df.empty:
                logger.warning(
                    f"API returned zero rows for window | "
                    f"{window_start} to {window_end}"
                )
                continue

            df = transform_dataframe(
                df=raw_df,
                run_id=run_id,
            )

            df = apply_window_filter(
                df=df,
                start_date=window_start,
                end_date=window_end,
            )

            if df.empty:
                logger.warning(
                    f"Zero valid rows remained after filtering | "
                    f"{window_start} to {window_end}"
                )
                continue

            load_dataframe_in_chunks(
                client=client,
                df=df,
                table_id=staging_table_id,
                schema=TABLE_SCHEMA,
                chunk_size=CHUNK_SIZE,
            )

            total_rows_loaded += len(df)

            logger.info(
                f"Future window staged successfully | "
                f"rows={len(df)} | "
                f"total_staged={total_rows_loaded}"
            )

        if (
            total_rows_loaded == 0
            and not ALLOW_EMPTY_REPLACE
        ):
            raise RuntimeError(
                "The complete future refresh returned zero "
                "valid rows. The existing future table was "
                "not replaced. Set ALLOW_EMPTY_REPLACE=true "
                "only when an empty future table is expected."
            )

        replace_target_from_staging(
            client=client,
            target_table_id=RAW_TABLE,
            staging_table_id=staging_table_id,
        )

        finished_at = datetime.now(timezone.utc)

        success_message = (
            f"Future pipeline succeeded | "
            f"range={future_start} to {future_end} | "
            f"api_windows={len(windows)} | "
            f"rows_loaded={total_rows_loaded}"
        )

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=total_rows_loaded,
            started_at=started_at,
            finished_at=finished_at,
            message=success_message,
        )

        logger.info(
            f"Future-events pipeline finished successfully | "
            f"rows_loaded={total_rows_loaded} | "
            f"run_id={run_id}"
        )

        return (
            f"{total_rows_loaded} future event rows loaded "
            f"into {RAW_TABLE}",
            200,
        )

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)

        current_window_text = (
            f"{current_window[0]} to {current_window[1]}"
            if current_window
            else "not started"
        )

        error_message = (
            f"{exc} | "
            f"future_range={future_start} to {future_end} | "
            f"current_window={current_window_text} | "
            f"rows_staged={total_rows_loaded}"
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
            logger.error(
                f"Could not log failed pipeline run: "
                f"{log_error}"
            )

        try:
            send_email(
                subject=(
                    f"❌ {PIPELINE_NAME} pipeline failed"
                ),
                body=(
                    f"Pipeline: {PIPELINE_NAME}\n"
                    f"Run ID: {run_id}\n"
                    f"Time: {finished_at}\n"
                    f"Target table: {RAW_TABLE}\n"
                    f"Future start: {future_start}\n"
                    f"Future end: {future_end}\n"
                    f"Current API window: "
                    f"{current_window_text}\n"
                    f"Rows staged: "
                    f"{total_rows_loaded}\n"
                    f"Error: {exc}"
                ),
            )
        except Exception as email_error:
            logger.error(
                f"Could not send failure email: "
                f"{email_error}"
            )

        logger.exception(
            f"Future-events pipeline failed | "
            f"run_id={run_id}"
        )

        return (
            f"Future-events pipeline failed: {exc}",
            500,
        )

    finally:
        if staging_created:
            try:
                client.delete_table(
                    staging_table_id,
                    not_found_ok=True,
                )
                logger.info(
                    f"Staging table deleted | "
                    f"table={staging_table_id}"
                )
            except Exception as cleanup_error:
                logger.error(
                    f"Could not delete staging table "
                    f"{staging_table_id}: "
                    f"{cleanup_error}"
                )