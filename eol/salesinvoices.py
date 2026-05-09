import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine, text

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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "sqlserver_generic")
TABLE_NAME = os.environ.get("TABLE_NAME", "generic")
SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "sqlserver")

SQLSERVER_HOST = os.environ.get("SQLSERVER_HOST")
SQLSERVER_PORT = os.environ.get("SQLSERVER_PORT", "1433")
SQLSERVER_DATABASE = os.environ.get("SQLSERVER_DATABASE")
SQLSERVER_USERNAME = os.environ.get("SQLSERVER_USERNAME")
SQLSERVER_PASSWORD = os.environ.get("SQLSERVER_PASSWORD")
SQLSERVER_SCHEMA = os.environ.get("SQLSERVER_SCHEMA", "dbo")
SQLSERVER_TABLE = os.environ.get("SQLSERVER_TABLE")

# Optional custom query.
# If provided, this query is used instead of SELECT * FROM schema.table.
# Example:
# SELECT id, name, modified_at FROM dbo.Customers
SQL_QUERY = os.environ.get("SQL_QUERY")

# Date/datetime column used for incremental and backfill filtering.
# Example: modified_at, updated_at, invoice_date, created_at
WINDOW_FIELD = os.environ.get("WINDOW_FIELD")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()  # full | incremental | backfill
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")

WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()  # append | replace_window

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
SQL_CHUNK_SIZE = int(os.environ.get("SQL_CHUNK_SIZE", 50000))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"

TECHNICAL_COLUMNS = [
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
        "SQLSERVER_HOST": SQLSERVER_HOST,
        "SQLSERVER_DATABASE": SQLSERVER_DATABASE,
        "SQLSERVER_USERNAME": SQLSERVER_USERNAME,
        "SQLSERVER_PASSWORD": SQLSERVER_PASSWORD,
    })

    if not SQL_QUERY and not SQLSERVER_TABLE:
        raise ValueError("Either SQL_QUERY or SQLSERVER_TABLE must be provided")

    if LOAD_MODE not in ["full", "incremental", "backfill"]:
        raise ValueError("LOAD_MODE must be either 'full', 'incremental', or 'backfill'")

    if WRITE_MODE not in ["append", "replace_window"]:
        raise ValueError("WRITE_MODE must be either 'append' or 'replace_window'")

    if LOAD_MODE in ["incremental", "backfill"] and not WINDOW_FIELD:
        raise ValueError("WINDOW_FIELD is required for incremental or backfill loads")

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

def sanitize_column_name(name: str) -> str:
    if not name:
        return "unnamed_field"

    value = str(name).strip().lower()
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")

    if not value:
        value = "unnamed_field"

    if re.match(r"^\d", value):
        value = f"field_{value}"

    return value


def make_unique_columns(columns: list[str]) -> list[str]:
    seen = {}
    result = []

    for col in columns:
        base = sanitize_column_name(col)

        if base not in seen:
            seen[base] = 1
            result.append(base)
        else:
            seen[base] += 1
            result.append(f"{base}_{seen[base]}")

    return result


def get_sqlserver_engine():
    """
    Uses ODBC Driver 18 for SQL Server.
    Your Docker image must install msodbcsql18 and pyodbc.
    """

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQLSERVER_HOST},{SQLSERVER_PORT};"
        f"DATABASE={SQLSERVER_DATABASE};"
        f"UID={SQLSERVER_USERNAME};"
        f"PWD={SQLSERVER_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

    encoded = quote_plus(connection_string)

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={encoded}",
        fast_executemany=True,
        pool_pre_ping=True,
    )

    return engine


def build_incremental_dates() -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

    return start_date.isoformat(), end_date.isoformat()


def build_source_query() -> tuple[str, dict]:
    """
    Builds SQL query for full, incremental, or backfill mode.
    """

    params = {}

    if SQL_QUERY:
        base_query = SQL_QUERY.strip().rstrip(";")
    else:
        base_query = f"SELECT * FROM [{SQLSERVER_SCHEMA}].[{SQLSERVER_TABLE}]"

    if LOAD_MODE == "full":
        return base_query, params

    if LOAD_MODE == "incremental":
        start_date, end_date = build_incremental_dates()
        params["start_date"] = start_date
        params["end_date"] = end_date

    elif LOAD_MODE == "backfill":
        params["start_date"] = BACKFILL_START_DATE
        params["end_date"] = BACKFILL_END_DATE

    query = f"""
    SELECT *
    FROM (
        {base_query}
    ) src
    WHERE CAST(src.[{WINDOW_FIELD}] AS date) BETWEEN :start_date AND :end_date
    """

    return query, params


def normalize_value(value):
    if value is None:
        return None

    if pd.api.types.is_scalar(value) and pd.isna(value):
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    return value


def infer_bq_type(series: pd.Series) -> str:
    dtype = series.dtype

    if pd.api.types.is_bool_dtype(dtype):
        return "BOOL"

    if pd.api.types.is_integer_dtype(dtype):
        return "INT64"

    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT64"

    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"

    return "STRING"


def build_table_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    schema = []

    for col in df.columns:
        if col in TECHNICAL_COLUMNS:
            continue

        bq_type = infer_bq_type(df[col])
        schema.append(bigquery.SchemaField(col, bq_type))

    schema.extend([
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("load_date", "DATE"),
        bigquery.SchemaField("record_hash", "STRING"),
    ])

    return schema


def delete_bq_window(client: bigquery.Client, table_id: str, start_date: str, end_date: str) -> None:
    logger.info(
        f"Deleting existing rows from BigQuery window | table={table_id} | "
        f"window_field={WINDOW_FIELD} | start_date={start_date} | end_date={end_date}"
    )

    query = f"""
    DELETE FROM `{table_id}`
    WHERE DATE({sanitize_column_name(WINDOW_FIELD)}) BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    client.query(query, job_config=job_config).result()
    logger.info("BigQuery window delete completed")


# =================================
# Fetch
# =================================

def fetch_data() -> pd.DataFrame:
    query, params = build_source_query()

    logger.info(f"Fetching data from SQL Server | load_mode={LOAD_MODE}")
    logger.info(f"SQL params: {params}")

    engine = get_sqlserver_engine()

    all_chunks = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with engine.connect() as connection:
                chunks = pd.read_sql(
                    sql=text(query),
                    con=connection,
                    params=params,
                    chunksize=SQL_CHUNK_SIZE,
                )

                for chunk in chunks:
                    logger.info(f"Fetched SQL chunk | rows={len(chunk)}")
                    all_chunks.append(chunk)

            break

        except Exception as e:
            logger.warning(f"SQL Server fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

    if not all_chunks:
        logger.info("No rows returned from SQL Server")
        return pd.DataFrame()

    df = pd.concat(all_chunks, ignore_index=True)

    logger.info(f"Fetched total rows from SQL Server | rows={len(df)}")
    logger.info(f"Columns received: {list(df.columns)}")

    return df


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming SQL Server dataframe for raw layer")

    if df.empty:
        logger.info("No SQL Server rows returned")
        return df

    original_columns = list(df.columns)
    safe_columns = make_unique_columns(original_columns)

    rename_map = dict(zip(original_columns, safe_columns))
    df = df.rename(columns=rename_map)

    logger.info(f"Column mapping: {rename_map}")

    # Normalize object columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(normalize_value)

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date

    hash_columns = [col for col in df.columns if col not in TECHNICAL_COLUMNS]

    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            *[row.get(col) for col in hash_columns]
        ),
        axis=1,
    )

    final_columns = hash_columns + TECHNICAL_COLUMNS
    df = df[final_columns].copy()

    # Final STRING cleanup
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda v: None if v is None or (pd.api.types.is_scalar(v) and pd.isna(v)) else str(v)
            ).astype("string")

    logger.info(f"Transformation complete | rows={len(df)} | columns={len(df.columns)}")
    logger.info(f"Dataframe dtypes before load: {df.dtypes.to_dict()}")

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
        f"window_field={WINDOW_FIELD} | backfill_start_date={BACKFILL_START_DATE} | "
        f"backfill_end_date={BACKFILL_END_DATE}"
    )

    try:
        validate_config()

        if LOAD_MODE == "incremental":
            start_date, end_date = build_incremental_dates()
        elif LOAD_MODE == "backfill":
            start_date, end_date = BACKFILL_START_DATE, BACKFILL_END_DATE
        else:
            start_date, end_date = None, None

        if LOAD_MODE in ["incremental", "backfill"] and WRITE_MODE == "replace_window":
            delete_bq_window(
                client=client,
                table_id=RAW_TABLE,
                start_date=start_date,
                end_date=end_date,
            )

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id=run_id)

        if df.empty:
            table_schema = [
                bigquery.SchemaField("source_system", "STRING"),
                bigquery.SchemaField("run_id", "STRING"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("load_date", "DATE"),
                bigquery.SchemaField("record_hash", "STRING"),
            ]
        else:
            table_schema = build_table_schema(df)

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

        if LOAD_MODE in ["incremental", "backfill"]:
            success_message += f" | window_field={WINDOW_FIELD} | window={start_date} to {end_date}"

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

        logger.info(f"Pipeline finished successfully | rows_loaded={len(df)} | run_id={run_id}")

        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as e:
        finished_at = datetime.utcnow()

        error_message = str(e)

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
                f"Window field: {WINDOW_FIELD}\n"
                f"Backfill start date: {BACKFILL_START_DATE}\n"
                f"Backfill end date: {BACKFILL_END_DATE}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")

        return f"Pipeline failed: {str(e)}", 500