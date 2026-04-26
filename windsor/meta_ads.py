import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "windsor_meta_ads")
TABLE_NAME = os.environ.get("TABLE_NAME", "meta_ads")

API_URL = os.environ.get("API_URL", "https://connectors.windsor.ai/all")
API_TOKEN = os.environ.get("API_TOKEN")  # Windsor API key

LOAD_MODE = os.environ.get("LOAD_MODE", "incremental").lower()  # full | incremental | backfill
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 7))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")  # YYYY-MM-DD
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")      # YYYY-MM-DD
WRITE_MODE = os.environ.get("WRITE_MODE", "replace_window").lower()  # append | replace_window

DATE_PRESET = os.environ.get("DATE_PRESET", "last_7d")

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 120))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "windsor")

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Windsor fields - main Meta performance + leads/engagement
# =================================

WINDSOR_FIELDS = [
    "account_name",
    "ad_id",
    "ad_name",
    "ad_object_type",
    "adcontent",
    "adset_id",
    "adset_name",
    "adset_status",
    "campaign",
    "campaign_id",
    "campaign_status",
    "clicks",
    "cpc",
    "cpm",
    "ctr",
    "datasource",
    "date",
    "device_platform",
    "frequency",
    "impressions",
    "link_clicks",
    "outbound_clicks_outbound_click",
    "publisher_platform",
    "reach",
    "spend",
    "thumbnail_url",
    "unique_clicks",
    "unique_ctr",
    "post_engagement",
    "leads",
]


# =================================
# Table schema
# Raw = keep Windsor source names as-is
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("account_name", "STRING"),

    bigquery.SchemaField("ad_id", "STRING"),
    bigquery.SchemaField("ad_name", "STRING"),
    bigquery.SchemaField("ad_object_type", "STRING"),
    bigquery.SchemaField("adcontent", "STRING"),

    bigquery.SchemaField("adset_id", "STRING"),
    bigquery.SchemaField("adset_name", "STRING"),
    bigquery.SchemaField("adset_status", "STRING"),

    bigquery.SchemaField("campaign", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_status", "STRING"),

    bigquery.SchemaField("clicks", "INT64"),
    bigquery.SchemaField("cpc", "FLOAT64"),
    bigquery.SchemaField("cpm", "FLOAT64"),
    bigquery.SchemaField("ctr", "FLOAT64"),

    bigquery.SchemaField("datasource", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("device_platform", "STRING"),
    bigquery.SchemaField("frequency", "FLOAT64"),

    bigquery.SchemaField("impressions", "INT64"),
    bigquery.SchemaField("link_clicks", "INT64"),
    bigquery.SchemaField("outbound_clicks_outbound_click", "INT64"),
    bigquery.SchemaField("publisher_platform", "STRING"),
    bigquery.SchemaField("reach", "INT64"),
    bigquery.SchemaField("spend", "FLOAT64"),
    bigquery.SchemaField("thumbnail_url", "STRING"),

    bigquery.SchemaField("unique_clicks", "INT64"),
    bigquery.SchemaField("unique_ctr", "FLOAT64"),

    bigquery.SchemaField("post_engagement", "INT64"),
    bigquery.SchemaField("leads", "INT64"),

    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = WINDSOR_FIELDS.copy()


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

        if "rows" in payload and isinstance(payload["rows"], list):
            return payload["rows"]

        return [payload]

    raise ValueError("Unsupported API response format")


def normalize_json_field(value):
    if pd.isna(value) or value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    return str(value)


def normalize_int_column(df: pd.DataFrame, column: str) -> None:
    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")


def normalize_float_column(df: pd.DataFrame, column: str) -> None:
    df[column] = pd.to_numeric(df[column], errors="coerce")


def normalize_string_column(df: pd.DataFrame, column: str) -> None:
    df[column] = normalize_nullable_string(df[column])


def resolve_window_field() -> str:
    return "date"


def get_incremental_window_dates():
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    return start_date, end_date


def build_api_params():
    params = {
        "api_key": API_TOKEN,
        "fields": ",".join(WINDSOR_FIELDS),
    }

    if LOAD_MODE == "full":
        if DATE_PRESET:
            params["date_preset"] = DATE_PRESET
        return params

    if LOAD_MODE == "incremental":
        start_date, end_date = get_incremental_window_dates()
        params["date_from"] = start_date.isoformat()
        params["date_to"] = end_date.isoformat()
        return params

    if LOAD_MODE == "backfill":
        params["date_from"] = BACKFILL_START_DATE
        params["date_to"] = BACKFILL_END_DATE
        return params

    raise ValueError(f"Unsupported LOAD_MODE: {LOAD_MODE}")


def table_exists(client: bigquery.Client, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def delete_window(
    client: bigquery.Client,
    table_id: str,
    start_date: str,
    end_date: str,
    window_field: str,
) -> None:
    if not table_exists(client, table_id):
        logger.warning(f"Table {table_id} does not exist yet. Skipping window delete.")
        return

    logger.info(
        f"Deleting existing rows from window | table={table_id} | "
        f"window_field={window_field} | start_date={start_date} | end_date={end_date}"
    )

    query = f"""
    DELETE FROM `{table_id}`
    WHERE {window_field} BETWEEN @start_date AND @end_date
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

    if window_field not in df.columns:
        raise ValueError(
            f"Window field '{window_field}' not found in dataframe. "
            f"Available columns: {list(df.columns)}"
        )

    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    field_as_date = pd.to_datetime(df[window_field], errors="coerce").dt.date

    filtered_df = df[
        field_as_date.notna()
        & (field_as_date >= start_date)
        & (field_as_date <= end_date)
    ].copy()

    logger.info(f"Window filter complete | rows_before={len(df)} | rows_after={len(filtered_df)}")
    return filtered_df


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    headers = {
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    params = build_api_params()

    safe_params = params.copy()
    safe_params["api_key"] = "***"

    logger.info(f"Fetching data from Windsor API | url={API_URL} | params={safe_params}")

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

            if not df.empty and "date" in df.columns:
                date_series = pd.to_datetime(df["date"], errors="coerce")

                logger.info(
                    f"Windsor response date range | "
                    f"min_date={date_series.min()} | "
                    f"max_date={date_series.max()} | "
                    f"distinct_dates={date_series.dt.date.nunique()}"
                )

            logger.info(f"Fetched {len(df)} rows from Windsor API")
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

    raise RuntimeError("Failed to fetch data from Windsor API")


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming dataframe for raw layer")

    missing_cols = [col for col in SELECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing expected Windsor columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[SELECTED_COLUMNS].copy()

    string_columns = [
        "account_name",
        "ad_id",
        "ad_name",
        "ad_object_type",
        "adcontent",
        "adset_id",
        "adset_name",
        "adset_status",
        "campaign",
        "campaign_id",
        "campaign_status",
        "datasource",
        "device_platform",
        "publisher_platform",
        "thumbnail_url",
    ]

    int_columns = [
        "clicks",
        "impressions",
        "link_clicks",
        "outbound_clicks_outbound_click",
        "reach",
        "unique_clicks",
        "post_engagement",
        "leads",
    ]

    float_columns = [
        "cpc",
        "cpm",
        "ctr",
        "frequency",
        "spend",
        "unique_ctr",
    ]

    for col in string_columns:
        normalize_string_column(df, col)

    for col in int_columns:
        normalize_int_column(df, col)

    for col in float_columns:
        normalize_float_column(df, col)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date

    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("account_name"),
            row.get("ad_id"),
            row.get("ad_name"),
            row.get("ad_object_type"),
            row.get("adcontent"),
            row.get("adset_id"),
            row.get("adset_name"),
            row.get("adset_status"),
            row.get("campaign"),
            row.get("campaign_id"),
            row.get("campaign_status"),
            row.get("clicks"),
            row.get("cpc"),
            row.get("cpm"),
            row.get("ctr"),
            row.get("datasource"),
            row.get("date"),
            row.get("device_platform"),
            row.get("frequency"),
            row.get("impressions"),
            row.get("link_clicks"),
            row.get("outbound_clicks_outbound_click"),
            row.get("publisher_platform"),
            row.get("reach"),
            row.get("spend"),
            row.get("thumbnail_url"),
            row.get("unique_clicks"),
            row.get("unique_ctr"),
            row.get("post_engagement"),
            row.get("leads"),
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
        f"date_preset={DATE_PRESET} | "
        f"backfill_start_date={BACKFILL_START_DATE} | backfill_end_date={BACKFILL_END_DATE}"
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

        if LOAD_MODE in ["backfill", "incremental"] and WRITE_MODE == "replace_window":
            df = apply_window_filter(
                df=df,
                start_date=window_start,
                end_date=window_end,
                window_field=window_field,
            )

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

        if LOAD_MODE == "full":
            success_message += f" | date_preset={DATE_PRESET}"

        if LOAD_MODE == "backfill":
            success_message += f" | window={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"

        if LOAD_MODE == "incremental" and WRITE_MODE == "replace_window":
            success_message += f" | window={window_start} to {window_end}"

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

        if LOAD_MODE == "full":
            error_message = (
                f"{error_message} | load_mode={LOAD_MODE} | date_preset={DATE_PRESET}"
            )

        if LOAD_MODE == "backfill":
            error_message = (
                f"{error_message} | load_mode={LOAD_MODE} | "
                f"window={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"
            )

        if LOAD_MODE == "incremental" and WRITE_MODE == "replace_window":
            incremental_start, incremental_end = get_incremental_window_dates()
            error_message = (
                f"{error_message} | load_mode={LOAD_MODE} | write_mode={WRITE_MODE} | "
                f"window={incremental_start.isoformat()} to {incremental_end.isoformat()}"
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
                f"Date preset: {DATE_PRESET}\n"
                f"Backfill start date: {BACKFILL_START_DATE}\n"
                f"Backfill end date: {BACKFILL_END_DATE}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")
        return f"Pipeline failed: {str(e)}", 500