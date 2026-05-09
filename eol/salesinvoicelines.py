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

SQL_QUERY = os.environ.get("SQL_QUERY")
WINDOW_FIELD = os.environ.get("WINDOW_FIELD")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")

WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
SQL_CHUNK_SIZE = int(os.environ.get("SQL_CHUNK_SIZE", 10000))
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

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={encoded}",
        fast_executemany=True,
        pool_pre_ping=True,
    )


def build_incremental_dates() -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

    return start_date.isoformat(), end_date.isoformat()


def build_source_query() -> tuple[str, dict]:
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


def to_raw_string(value):
    """
    Converts any SQL Server value to a safe raw-layer string.
    Keeps nulls as null.
    """
    value = normalize_value(value)

    if value is None:
        return None

    if pd.api.types.is_scalar(value) and pd.isna(value):
        return None

    return str(value)


def infer_bq_type(series: pd.Series) -> str:
    """
    Raw-layer strategy:
    all SQL Server source columns are stored as STRING.
    Type casting happens later in cpb_prep.
    """
    return "STRING"


def build_table_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    schema = []

    for col in df.columns:
        if col in TECHNICAL_COLUMNS:
            continue

        schema.append(bigquery.SchemaField(col, "STRING"))

    schema.extend([
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("load_date", "DATE"),
        bigquery.SchemaField("record_hash", "STRING"),
    ])

    return schema


def align_dataframe_to_schema(
    df: pd.DataFrame,
    schema: list[bigquery.SchemaField],
) -> pd.DataFrame:
    df = df.copy()

    for field in schema:
        col = field.name

        if col not in df.columns:
            df[col] = None

        if field.field_type == "STRING":
            df[col] = df[col].apply(to_raw_string).astype("string")

        elif field.field_type == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        elif field.field_type == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        elif field.field_type == "INT64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif field.field_type == "FLOAT64":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif field.field_type == "BOOL":
            df[col] = df[col].astype("boolean")

    schema_columns = [field.name for field in schema]
    df = df[schema_columns].copy()

    return df


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

def fetch_sql_chunks():
    query, params = build_source_query()

    logger.info(f"Fetching data from SQL Server in chunks | load_mode={LOAD_MODE}")
    logger.info(f"SQL params: {params}")
    logger.info(f"SQL_CHUNK_SIZE={SQL_CHUNK_SIZE}")

    engine = get_sqlserver_engine()

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
                    yield chunk

            break

        except Exception as e:
            logger.warning(f"SQL Server fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming SQL Server dataframe chunk for raw layer")

    if df.empty:
        logger.info("Empty SQL Server chunk")
        return df

    original_columns = list(df.columns)
    safe_columns = make_unique_columns(original_columns)

    rename_map = dict(zip(original_columns, safe_columns))
    df = df.rename(columns=rename_map)

    logger.info(f"Column mapping: {rename_map}")

    # Convert all SQL Server source columns to STRING for raw layer
    source_columns = list(df.columns)

    for col in source_columns:
        df[col] = df[col].apply(to_raw_string).astype("string")

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date

    hash_columns = source_columns

    df["record_hash"] = df.apply(
        lambda row: generate_record_hash_from_values(
            *[row.get(col) for col in hash_columns]
        ),
        axis=1,
    ).astype("string")

    final_columns = hash_columns + TECHNICAL_COLUMNS
    df = df[final_columns].copy()

    logger.info(f"Transformation complete | rows={len(df)} | columns={len(df.columns)}")
    logger.info(f"Dataframe dtypes before schema alignment: {df.dtypes.to_dict()}")

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

        total_rows_loaded = 0
        table_schema = None
        chunk_number = 0

        for raw_chunk in fetch_sql_chunks():
            chunk_number += 1

            df_chunk = transform_dataframe(raw_chunk, run_id=run_id)

            if df_chunk.empty:
                logger.info(f"Skipping empty chunk | chunk_number={chunk_number}")
                continue

            if table_schema is None:
                table_schema = build_table_schema(df_chunk)
                logger.info(
                    f"Schema created from first chunk: "
                    f"{[(field.name, field.field_type) for field in table_schema]}"
                )

            df_chunk = align_dataframe_to_schema(df_chunk, table_schema)

            logger.info(f"Dataframe dtypes before load: {df_chunk.dtypes.to_dict()}")

            load_dataframe_in_chunks(
                client=client,
                df=df_chunk,
                table_id=RAW_TABLE,
                schema=table_schema,
                chunk_size=CHUNK_SIZE,
            )

            total_rows_loaded += len(df_chunk)

            logger.info(
                f"Chunk loaded successfully | chunk_number={chunk_number} | "
                f"chunk_rows={len(df_chunk)} | total_rows_loaded={total_rows_loaded}"
            )

        if table_schema is None:
            logger.info("No rows returned from SQL Server. No data loaded.")

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
            rows_loaded=total_rows_loaded,
            started_at=started_at,
            finished_at=finished_at,
            message=success_message,
        )

        logger.info(
            f"Pipeline finished successfully | rows_loaded={total_rows_loaded} | run_id={run_id}"
        )

        return f"{total_rows_loaded} rows loaded into {RAW_TABLE}", 200

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