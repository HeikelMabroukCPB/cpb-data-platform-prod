import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from google.cloud import bigquery

from shared.bq import get_bq_client, load_dataframe_in_chunks
from shared.mail import send_email
from shared.metadata import log_pipeline_run
from shared.utils import (
    generate_record_hash_from_values,
    validate_common_config,
)

logger = logging.getLogger(__name__)


# =================================
# Config
# =================================

PROJECT_ID = os.environ.get("PROJECT_ID", "cpb-data-platform-prod")
DATASET_RAW = os.environ.get("DATASET_RAW", "cpb_raw")
DATASET_META = os.environ.get("DATASET_META", "cpb_meta")

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "ninox_generic")
TABLE_NAME = os.environ.get("TABLE_NAME", "generic")

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "ninox")

NINOX_API_TOKEN = os.environ.get("NINOX_API_TOKEN")
NINOX_API_BASE_URL = os.environ.get("NINOX_API_BASE_URL", "https://api.ninox.com/v1")

NINOX_TEAM_ID = os.environ.get("NINOX_TEAM_ID")
NINOX_DATABASE_ID = os.environ.get("NINOX_DATABASE_ID")
NINOX_TABLE_ID = os.environ.get("NINOX_TABLE_ID")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()  # full | incremental | backfill
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")  # YYYY-MM-DD
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")      # YYYY-MM-DD
WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()  # append | replace_window

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", 250))
RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 30))

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"

SCHEMA_URL = (
    f"{NINOX_API_BASE_URL}/teams/{NINOX_TEAM_ID}/databases/{NINOX_DATABASE_ID}/tables/{NINOX_TABLE_ID}"
)
RECORDS_URL = (
    f"{NINOX_API_BASE_URL}/teams/{NINOX_TEAM_ID}/databases/{NINOX_DATABASE_ID}/tables/{NINOX_TABLE_ID}/records"
)

TECHNICAL_COLUMNS = [
    "ninox_record_id",
    "ninox_sequence",
    "ninox_created_at",
    "ninox_modified_at",
    "source_system",
    "run_id",
    "load_timestamp",
    "load_date",
    "record_hash",
]


# =================================
# Validation
# =================================

def validate_config() -> None:
    validate_common_config({
        "PROJECT_ID": PROJECT_ID,
        "DATASET_RAW": DATASET_RAW,
        "DATASET_META": DATASET_META,
        "PIPELINE_NAME": PIPELINE_NAME,
        "TABLE_NAME": TABLE_NAME,
        "SOURCE_SYSTEM": SOURCE_SYSTEM,
        "NINOX_API_TOKEN": NINOX_API_TOKEN,
        "NINOX_API_BASE_URL": NINOX_API_BASE_URL,
        "NINOX_TEAM_ID": NINOX_TEAM_ID,
        "NINOX_DATABASE_ID": NINOX_DATABASE_ID,
        "NINOX_TABLE_ID": NINOX_TABLE_ID,
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


# =================================
# Helpers
# =================================

def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {NINOX_API_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }


def sanitize_column_name(name: str) -> str:
    if not name:
        return "unnamed_field"

    value = str(name).strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")

    if not value:
        value = "unnamed_field"

    if re.match(r"^\d", value):
        value = f"field_{value}"

    return value


def make_unique_column_names(field_names: list[str]) -> dict:
    seen = {}
    mapping = {}

    for field_name in field_names:
        base_name = sanitize_column_name(field_name)

        if base_name not in seen:
            seen[base_name] = 1
            mapping[field_name] = base_name
        else:
            seen[base_name] += 1
            mapping[field_name] = f"{base_name}_{seen[base_name]}"

    return mapping


def normalize_json_field(value):
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    return value


def safe_get_field(fields_obj: dict, field_name: str):
    if not isinstance(fields_obj, dict):
        return None
    return fields_obj.get(field_name)


def parse_timestamp(value):
    if value is None or value == "":
        return pd.NaT
    return pd.to_datetime(value, errors="coerce", utc=True)


def map_ninox_type_to_bq(field_meta: dict) -> str:
    ninox_type = str(field_meta.get("type", "")).strip().lower()

    if ninox_type in ["text", "string", "email", "url", "phone", "tel", "choice", "select"]:
        return "STRING"

    if ninox_type in ["integer", "int"]:
        return "INT64"

    if ninox_type in ["number", "decimal", "numeric", "float", "currency"]:
        return "FLOAT64"

    if ninox_type in ["bool", "boolean", "yesno"]:
        return "BOOL"

    if ninox_type in ["date"]:
        return "DATE"

    if ninox_type in ["datetime", "timestamp", "time"]:
        return "TIMESTAMP"

    # safest fallback for references, arrays, files, formulas, unknown structures
    return "STRING"


def cast_series_by_bq_type(series: pd.Series, bq_type: str) -> pd.Series:
    if bq_type == "STRING":
        return series.apply(
            lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else normalize_json_field(x)
        )

    if bq_type == "INT64":
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    if bq_type == "FLOAT64":
        return pd.to_numeric(series, errors="coerce")

    if bq_type == "BOOL":
        def to_bool(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return pd.NA
            if isinstance(v, bool):
                return v
            if isinstance(v, str):
                value = v.strip().lower()
                if value in ["true", "1", "yes", "y"]:
                    return True
                if value in ["false", "0", "no", "n"]:
                    return False
            if isinstance(v, (int, float)):
                return bool(v)
            return pd.NA

        return series.apply(to_bool).astype("boolean")

    if bq_type == "DATE":
        return pd.to_datetime(series, errors="coerce").dt.date

    if bq_type == "TIMESTAMP":
        return pd.to_datetime(series, errors="coerce", utc=True)

    return series.apply(normalize_json_field)


def build_incremental_cutoff() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)


# =================================
# Schema fetch
# =================================

def fetch_ninox_table_schema() -> tuple[list[dict], dict[str, str], dict[str, str]]:
    logger.info(f"Fetching Ninox table schema | url={SCHEMA_URL}")

    headers = get_headers()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                SCHEMA_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                logger.warning(
                    f"Rate limit hit on schema fetch attempt {attempt}/{MAX_RETRIES}. "
                    f"Waiting {RATE_LIMIT_SLEEP_SECONDS} seconds before retry."
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError(f"Unexpected Ninox schema response type: {type(payload)}")

            fields = payload.get("fields", [])
            if not isinstance(fields, list):
                raise ValueError("Ninox schema response missing valid 'fields' list")

            field_names = [field.get("name") for field in fields if field.get("name")]
            column_name_map = make_unique_column_names(field_names)

            field_type_map = {}
            for field in fields:
                field_name = field.get("name")
                if not field_name:
                    continue
                safe_name = column_name_map[field_name]
                field_type_map[safe_name] = map_ninox_type_to_bq(field)

            logger.info(f"Fetched Ninox schema successfully | fields={len(field_names)}")
            logger.info(f"Column mapping: {column_name_map}")

            return fields, column_name_map, field_type_map

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on schema fetch attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

        except Exception as e:
            logger.warning(f"Schema fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    raise RuntimeError("Failed to fetch Ninox schema")


def build_table_schema(field_type_map: dict[str, str]) -> list[bigquery.SchemaField]:
    schema = [
        bigquery.SchemaField("ninox_record_id", "INT64"),
        bigquery.SchemaField("ninox_sequence", "INT64"),
        bigquery.SchemaField("ninox_created_at", "TIMESTAMP"),
        bigquery.SchemaField("ninox_modified_at", "TIMESTAMP"),
    ]

    for column_name, bq_type in field_type_map.items():
        schema.append(bigquery.SchemaField(column_name, bq_type))

    schema.extend([
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("load_date", "DATE"),
        bigquery.SchemaField("record_hash", "STRING"),
    ])

    return schema


# =================================
# API fetch
# =================================

def fetch_page(page: int) -> list:
    headers = get_headers()
    params = {
        "page": page,
        "perPage": PAGE_SIZE,
    }

    logger.info(f"Fetching Ninox page | page={page} | params={params}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                RECORDS_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                logger.warning(
                    f"Rate limit hit on page fetch attempt {attempt}/{MAX_RETRIES}. "
                    f"Waiting {RATE_LIMIT_SLEEP_SECONDS} seconds before retry."
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            response.raise_for_status()
            payload = response.json()

            if not payload:
                return []

            if isinstance(payload, list):
                return payload

            raise ValueError(f"Unexpected Ninox records response type: {type(payload)}")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on page fetch attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

        except Exception as e:
            logger.warning(f"Page fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    raise RuntimeError("Failed to fetch Ninox page")


def fetch_data() -> pd.DataFrame:
    all_records = []
    page = 0

    while True:
        page_records = fetch_page(page)

        if not page_records:
            break

        all_records.extend(page_records)

        logger.info(
            f"Fetched page {page} successfully | rows_in_page={len(page_records)} | total_rows={len(all_records)}"
        )

        if len(page_records) < PAGE_SIZE:
            break

        page += 1

    df = pd.DataFrame(all_records)

    logger.info(f"Fetched {len(df)} total rows from Ninox")
    logger.info(f"Top-level columns received: {list(df.columns)}")

    return df


# =================================
# Backfill / incremental helpers
# =================================

def delete_backfill_window(client: bigquery.Client, table_id: str, start_date: str, end_date: str) -> None:
    logger.info(
        f"Deleting existing rows from backfill window | table={table_id} | "
        f"start_date={start_date} | end_date={end_date}"
    )

    query = f"""
    DELETE FROM `{table_id}`
    WHERE DATE(ninox_modified_at) BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    client.query(query, job_config=job_config).result()
    logger.info("Backfill window delete completed")


def apply_time_window_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if LOAD_MODE == "incremental":
        cutoff = build_incremental_cutoff()

        filtered_df = df[
            df["ninox_modified_at"].notna() &
            (df["ninox_modified_at"] >= cutoff)
        ].copy()

        logger.info(
            f"Incremental filter complete | cutoff={cutoff.isoformat()} | "
            f"rows_before={len(df)} | rows_after={len(filtered_df)}"
        )
        return filtered_df

    if LOAD_MODE == "backfill":
        start_date = pd.to_datetime(BACKFILL_START_DATE).date()
        end_date = pd.to_datetime(BACKFILL_END_DATE).date()

        filtered_df = df[
            df["ninox_modified_at"].notna() &
            (df["ninox_modified_at"].dt.date >= start_date) &
            (df["ninox_modified_at"].dt.date <= end_date)
        ].copy()

        logger.info(
            f"Backfill filter complete | start_date={start_date} | end_date={end_date} | "
            f"rows_before={len(df)} | rows_after={len(filtered_df)}"
        )
        return filtered_df

    return df


# =================================
# Transform
# =================================

def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
    column_name_map: dict[str, str],
    field_type_map: dict[str, str],
) -> pd.DataFrame:
    logger.info("Transforming Ninox dataframe for raw layer")

    if df.empty:
        base_df = pd.DataFrame(columns=TECHNICAL_COLUMNS + list(field_type_map.keys()))
        logger.info("No Ninox rows returned, returning empty dataframe")
        return base_df

    required_top_level_cols = ["id", "sequence", "createdAt", "modifiedAt", "fields"]
    missing_cols = [col for col in required_top_level_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing expected Ninox columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    transformed = pd.DataFrame()

    transformed["ninox_record_id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    transformed["ninox_sequence"] = pd.to_numeric(df["sequence"], errors="coerce").astype("Int64")
    transformed["ninox_created_at"] = df["createdAt"].apply(parse_timestamp)
    transformed["ninox_modified_at"] = df["modifiedAt"].apply(parse_timestamp)

    for original_field_name, safe_column_name in column_name_map.items():
        transformed[safe_column_name] = df["fields"].apply(
            lambda fields: safe_get_field(fields, original_field_name)
        )

    for column_name, bq_type in field_type_map.items():
        transformed[column_name] = cast_series_by_bq_type(transformed[column_name], bq_type)

    transformed = apply_time_window_filter(transformed)

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    transformed["source_system"] = SOURCE_SYSTEM
    transformed["run_id"] = run_id
    transformed["load_timestamp"] = load_timestamp
    transformed["load_date"] = load_date

    hash_columns = [
        "ninox_record_id",
        "ninox_sequence",
        "ninox_created_at",
        "ninox_modified_at",
    ] + list(field_type_map.keys())

    transformed["record_hash"] = transformed.apply(
        lambda row: generate_record_hash_from_values(
            *[row.get(col) for col in hash_columns]
        ),
        axis=1,
    )

    final_columns = [
        "ninox_record_id",
        "ninox_sequence",
        "ninox_created_at",
        "ninox_modified_at",
    ] + list(field_type_map.keys()) + [
        "source_system",
        "run_id",
        "load_timestamp",
        "load_date",
        "record_hash",
    ]

    transformed = transformed[final_columns].copy()

    logger.info(f"Transformation complete | rows={len(transformed)} | columns={len(transformed.columns)}")
    return transformed


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

        _, column_name_map, field_type_map = fetch_ninox_table_schema()
        table_schema = build_table_schema(field_type_map)

        if LOAD_MODE == "backfill" and WRITE_MODE == "replace_window":
            delete_backfill_window(
                client=client,
                table_id=RAW_TABLE,
                start_date=BACKFILL_START_DATE,
                end_date=BACKFILL_END_DATE,
            )

        raw_df = fetch_data()
        df = transform_dataframe(
            df=raw_df,
            run_id=run_id,
            column_name_map=column_name_map,
            field_type_map=field_type_map,
        )

        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=table_schema,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = datetime.utcnow()

        success_message = (
            f"Pipeline succeeded | load_mode={LOAD_MODE} | write_mode={WRITE_MODE}"
        )
        if LOAD_MODE == "backfill":
            success_message += f" | window={BACKFILL_START_DATE} to {BACKFILL_END_DATE}"

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
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")
        return f"Pipeline failed: {str(e)}", 500