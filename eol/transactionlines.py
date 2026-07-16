import logging
import os
import re
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import quote_plus

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine, text

from shared.bq import get_bq_client
from shared.mail import send_email
from shared.metadata import log_pipeline_run
from shared.utils import generate_record_hash_from_values, validate_common_config

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
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", "2"))

BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE")
BACKFILL_END_DATE = os.environ.get("BACKFILL_END_DATE")

WRITE_MODE = os.environ.get("WRITE_MODE", "append").lower()

# Larger extraction chunks reduce SQL Server round trips.
SQL_CHUNK_SIZE = int(os.environ.get("SQL_CHUNK_SIZE", "100000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

# Temporary Parquet staging in Cloud Storage.
GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "sqlserver-staging").strip("/")
PARQUET_COMPRESSION = os.environ.get("PARQUET_COMPRESSION", "snappy")
CLEANUP_GCS_FILES = os.environ.get("CLEANUP_GCS_FILES", "true").lower() == "true"

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"

# The existing hash helper is preserved so historical record_hash values
# remain comparable. Hashing is performed with itertuples instead of
# DataFrame.apply(axis=1), which reduces Python overhead.
STAGING_TECHNICAL_COLUMNS = [
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
        "GCS_BUCKET": GCS_BUCKET,
    })

    if not SQL_QUERY and not SQLSERVER_TABLE:
        raise ValueError("Either SQL_QUERY or SQLSERVER_TABLE must be provided")

    if LOAD_MODE not in {"full", "incremental", "backfill"}:
        raise ValueError("LOAD_MODE must be 'full', 'incremental', or 'backfill'")

    if WRITE_MODE not in {"append", "replace_window"}:
        raise ValueError("WRITE_MODE must be 'append' or 'replace_window'")

    if LOAD_MODE in {"incremental", "backfill"} and not WINDOW_FIELD:
        raise ValueError("WINDOW_FIELD is required for incremental or backfill loads")

    if LOAD_MODE == "backfill":
        if not BACKFILL_START_DATE or not BACKFILL_END_DATE:
            raise ValueError(
                "BACKFILL_START_DATE and BACKFILL_END_DATE are required "
                "when LOAD_MODE='backfill'"
            )

        try:
            start_date = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
            end_date = datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                "BACKFILL_START_DATE and BACKFILL_END_DATE must use YYYY-MM-DD"
            ) from exc

        if start_date > end_date:
            raise ValueError(
                "BACKFILL_START_DATE cannot be later than BACKFILL_END_DATE"
            )

    if SQL_CHUNK_SIZE <= 0:
        raise ValueError("SQL_CHUNK_SIZE must be greater than zero")


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


def sanitize_table_component(value: str) -> str:
    return sanitize_column_name(value).replace("-", "_")


def make_unique_columns(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []

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
        pool_pre_ping=True,
    )


def build_incremental_dates() -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    return start_date.isoformat(), end_date.isoformat()


def build_source_query() -> tuple[str, dict]:
    params: dict[str, str] = {}

    if SQL_QUERY:
        base_query = SQL_QUERY.strip().rstrip(";")
    else:
        base_query = f"SELECT * FROM [{SQLSERVER_SCHEMA}].[{SQLSERVER_TABLE}]"

    if LOAD_MODE == "full":
        return base_query, params

    if LOAD_MODE == "incremental":
        start_date, end_date = build_incremental_dates()
    else:
        start_date, end_date = BACKFILL_START_DATE, BACKFILL_END_DATE

    params["start_date"] = start_date
    params["end_date"] = end_date

    # Important performance improvement:
    # do not CAST the SQL Server column in the WHERE clause. Keeping the
    # source column untouched makes the predicate eligible for index seeks.
    query = f"""
    SELECT *
    FROM (
        {base_query}
    ) AS src
    WHERE src.[{WINDOW_FIELD}] >= CAST(:start_date AS date)
      AND src.[{WINDOW_FIELD}] < DATEADD(day, 1, CAST(:end_date AS date))
    """

    return query, params


def build_staging_schema(
    source_columns: list[str],
) -> list[bigquery.SchemaField]:
    schema = [
        bigquery.SchemaField(column, "STRING")
        for column in source_columns
    ]

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
    result = df.copy()

    for field in schema:
        column = field.name

        if column not in result.columns:
            result[column] = None

        if field.field_type == "STRING":
            result[column] = result[column].astype("string")
        elif field.field_type == "TIMESTAMP":
            result[column] = pd.to_datetime(
                result[column],
                errors="coerce",
                utc=True,
            )
        elif field.field_type == "DATE":
            result[column] = pd.to_datetime(
                result[column],
                errors="coerce",
            ).dt.date

    return result[[field.name for field in schema]].copy()


def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
    expected_source_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    if df.empty:
        return df, expected_source_columns or []

    safe_columns = make_unique_columns(list(df.columns))

    if expected_source_columns is not None and safe_columns != expected_source_columns:
        raise ValueError(
            "SQL Server columns changed between chunks. "
            f"Expected={expected_source_columns}, received={safe_columns}"
        )

    df = df.copy()
    df.columns = safe_columns
    source_columns = safe_columns

    # Fast, column-based conversion. This replaces the previous Python
    # row-by-row and cell-by-cell conversion/hash work.
    for column in source_columns:
        df[column] = df[column].astype("string")

    load_timestamp = datetime.now(timezone.utc)
    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_timestamp.date()

    # Preserve the current hash implementation, but avoid the slower
    # DataFrame.apply(axis=1) path.
    record_hashes = [
        generate_record_hash_from_values(*values)
        for values in df[source_columns].itertuples(
            index=False,
            name=None,
        )
    ]
    df["record_hash"] = pd.Series(
        record_hashes,
        index=df.index,
        dtype="string",
    )

    return df, source_columns


def staging_table_id(run_id: str) -> str:
    safe_source = sanitize_table_component(SOURCE_SYSTEM)
    safe_table = sanitize_table_component(TABLE_NAME)
    return (
        f"{PROJECT_ID}.{DATASET_RAW}."
        f"_stg_{safe_source}_{safe_table}_{run_id}"
    )


def gcs_run_prefix(run_id: str, attempt: int) -> str:
    safe_source = sanitize_table_component(SOURCE_SYSTEM)
    safe_table = sanitize_table_component(TABLE_NAME)
    return (
        f"{GCS_PREFIX}/{safe_source}/{safe_table}/"
        f"{run_id}/attempt-{attempt}"
    )


def delete_gcs_prefix(
    storage_client: storage.Client,
    prefix: str,
) -> None:
    if not GCS_BUCKET or not prefix:
        return

    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = list(storage_client.list_blobs(GCS_BUCKET, prefix=prefix))

    if not blobs:
        return

    logger.info(
        "Deleting temporary GCS files | bucket=%s | prefix=%s | files=%s",
        GCS_BUCKET,
        prefix,
        len(blobs),
    )

    for blob in blobs:
        bucket.blob(blob.name).delete()


# =================================
# Extract + Parquet staging
# =================================

def extract_to_gcs_parquet(
    run_id: str,
) -> tuple[
    list[str],
    int,
    list[str],
    list[bigquery.SchemaField],
    str,
]:
    query, params = build_source_query()
    storage_client = storage.Client(project=PROJECT_ID)

    logger.info(
        "Starting SQL Server extraction | load_mode=%s | sql_chunk_size=%s | params=%s",
        LOAD_MODE,
        SQL_CHUNK_SIZE,
        params,
    )

    for attempt in range(1, MAX_RETRIES + 1):
        prefix = gcs_run_prefix(run_id, attempt)
        gcs_uris: list[str] = []
        total_rows = 0
        source_columns: list[str] | None = None
        staging_schema: list[bigquery.SchemaField] | None = None
        engine = get_sqlserver_engine()
        attempt_started = perf_counter()

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                with engine.connect().execution_options(
                    stream_results=True
                ) as connection:
                    chunks = pd.read_sql(
                        sql=text(query),
                        con=connection,
                        params=params,
                        chunksize=SQL_CHUNK_SIZE,
                    )

                    for chunk_number, raw_chunk in enumerate(chunks, start=1):
                        chunk_started = perf_counter()

                        transformed, current_columns = transform_dataframe(
                            raw_chunk,
                            run_id=run_id,
                            expected_source_columns=source_columns,
                        )

                        if transformed.empty:
                            continue

                        if source_columns is None:
                            source_columns = current_columns
                            staging_schema = build_staging_schema(source_columns)

                        transformed = align_dataframe_to_schema(
                            transformed,
                            staging_schema,
                        )

                        local_file = (
                            Path(temp_dir)
                            / f"part-{chunk_number:05d}.parquet"
                        )

                        parquet_started = perf_counter()
                        transformed.to_parquet(
                            local_file,
                            engine="pyarrow",
                            compression=PARQUET_COMPRESSION,
                            index=False,
                        )
                        parquet_seconds = perf_counter() - parquet_started

                        blob_name = (
                            f"{prefix}/part-{chunk_number:05d}.parquet"
                        )
                        upload_started = perf_counter()
                        blob = storage_client.bucket(
                            GCS_BUCKET
                        ).blob(blob_name)
                        blob.upload_from_filename(str(local_file))
                        upload_seconds = perf_counter() - upload_started

                        gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
                        gcs_uris.append(gcs_uri)
                        total_rows += len(transformed)

                        logger.info(
                            "Chunk staged | chunk=%s | rows=%s | "
                            "parquet_seconds=%.2f | upload_seconds=%.2f | "
                            "chunk_seconds=%.2f | total_rows=%s",
                            chunk_number,
                            len(transformed),
                            parquet_seconds,
                            upload_seconds,
                            perf_counter() - chunk_started,
                            total_rows,
                        )

            engine.dispose()

            if source_columns is None or staging_schema is None:
                return [], 0, [], [], prefix

            logger.info(
                "SQL extraction and GCS staging completed | files=%s | "
                "rows=%s | seconds=%.2f",
                len(gcs_uris),
                total_rows,
                perf_counter() - attempt_started,
            )

            return (
                gcs_uris,
                total_rows,
                source_columns,
                staging_schema,
                prefix,
            )

        except Exception as exc:
            engine.dispose()

            logger.warning(
                "Extraction attempt %s/%s failed after %.2f seconds: %s",
                attempt,
                MAX_RETRIES,
                perf_counter() - attempt_started,
                exc,
            )

            try:
                delete_gcs_prefix(storage_client, prefix)
            except Exception as cleanup_error:
                logger.warning(
                    "Could not clean failed GCS attempt: %s",
                    cleanup_error,
                )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

    raise RuntimeError("SQL Server extraction failed unexpectedly")


# =================================
# BigQuery staging + atomic publish
# =================================

def load_parquet_to_staging(
    client: bigquery.Client,
    gcs_uris: list[str],
    schema: list[bigquery.SchemaField],
    table_id: str,
) -> None:
    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(
        "Starting one BigQuery load job | files=%s | staging_table=%s",
        len(gcs_uris),
        table_id,
    )


    load_started = perf_counter()

    job = client.load_table_from_uri(
        gcs_uris,
        table_id,
        job_config=load_config,
    )
    job.result()

    table = client.get_table(table_id)
    table.expires = datetime.now(timezone.utc) + timedelta(days=1)
    client.update_table(table, ["expires"])

    logger.info(
        "BigQuery staging load completed | rows=%s | seconds=%.2f",
        table.num_rows,
        perf_counter() - load_started,
    )


def quoted_columns(columns: list[str], alias: str | None = None) -> str:
    if alias:
        return ",\n      ".join(
            f"{alias}.`{column}`"
            for column in columns
        )

    return ",\n      ".join(f"`{column}`" for column in columns)


def ensure_raw_table_exists(
    client: bigquery.Client,
    staging_table: str,
    source_columns: list[str],
) -> None:
    all_staging_columns = source_columns + STAGING_TECHNICAL_COLUMNS
    select_columns = quoted_columns(all_staging_columns, alias="s")

    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{RAW_TABLE}` AS
    SELECT
      {select_columns}
    FROM `{staging_table}` AS s
    WHERE FALSE
    """

    client.query(create_query).result()

    # Preserve the generic-loader behaviour when a new SQL Server column
    # appears later: add missing nullable columns to the existing raw table.
    expected_fields = {
        **{column: "STRING" for column in source_columns},
        "source_system": "STRING",
        "run_id": "STRING",
        "load_timestamp": "TIMESTAMP",
        "load_date": "DATE",
        "record_hash": "STRING",
    }

    existing_table = client.get_table(RAW_TABLE)
    existing_columns = {
        field.name
        for field in existing_table.schema
    }

    alter_statements = [
        (
            f"ALTER TABLE `{RAW_TABLE}` "
            f"ADD COLUMN IF NOT EXISTS `{column}` {field_type}"
        )
        for column, field_type in expected_fields.items()
        if column not in existing_columns
    ]

    if alter_statements:
        client.query(";\n".join(alter_statements) + ";").result()
        logger.info(
            "Added missing raw-table columns: %s",
            [
                column
                for column in expected_fields
                if column not in existing_columns
            ],
        )


def publish_staging_to_raw(
    client: bigquery.Client,
    staging_table: str,
    source_columns: list[str],
    start_date: str | None,
    end_date: str | None,
) -> None:
    ensure_raw_table_exists(
        client=client,
        staging_table=staging_table,
        source_columns=source_columns,
    )

    raw_columns = source_columns + STAGING_TECHNICAL_COLUMNS
    insert_columns = quoted_columns(raw_columns)
    staging_select_columns = quoted_columns(
        raw_columns,
        alias="s",
    )

    insert_statement = f"""
    INSERT INTO `{RAW_TABLE}` (
      {insert_columns}
    )
    SELECT
      {staging_select_columns}
    FROM `{staging_table}` AS s
    """

    query_parameters: list[bigquery.ScalarQueryParameter] = []

    if (
        LOAD_MODE in {"incremental", "backfill"}
        and WRITE_MODE == "replace_window"
    ):
        safe_window_field = sanitize_column_name(WINDOW_FIELD)

        publish_query = f"""
        BEGIN TRANSACTION;

        DELETE FROM `{RAW_TABLE}`
        WHERE SAFE_CAST(`{safe_window_field}` AS DATE)
              BETWEEN @start_date AND @end_date;

        {insert_statement};

        COMMIT TRANSACTION;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "start_date",
                "DATE",
                start_date,
            ),
            bigquery.ScalarQueryParameter(
                "end_date",
                "DATE",
                end_date,
            ),
        ]
    else:
        publish_query = insert_statement

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    publish_started = perf_counter()
    client.query(
        publish_query,
        job_config=job_config,
    ).result()

    logger.info(
        "Atomic publish to raw table completed | table=%s | seconds=%.2f",
        RAW_TABLE,
        perf_counter() - publish_started,
    )


# =================================
# Main ETL
# =================================

def run_etl():
    client = get_bq_client()
    storage_client = storage.Client(project=PROJECT_ID)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc)
    pipeline_started = perf_counter()

    staging_table: str | None = None
    staged_gcs_prefix: str | None = None
    total_rows_loaded = 0

    logger.info(
        "Pipeline started | pipeline=%s | run_id=%s | raw_table=%s",
        PIPELINE_NAME,
        run_id,
        RAW_TABLE,
    )
    logger.info(
        "Execution context | load_mode=%s | write_mode=%s | "
        "window_field=%s | backfill_start_date=%s | "
        "backfill_end_date=%s | sql_chunk_size=%s",
        LOAD_MODE,
        WRITE_MODE,
        WINDOW_FIELD,
        BACKFILL_START_DATE,
        BACKFILL_END_DATE,
        SQL_CHUNK_SIZE,
    )

    try:
        validate_config()

        if LOAD_MODE == "incremental":
            start_date, end_date = build_incremental_dates()
        elif LOAD_MODE == "backfill":
            start_date = BACKFILL_START_DATE
            end_date = BACKFILL_END_DATE
        else:
            start_date = None
            end_date = None

        (
            gcs_uris,
            total_rows_loaded,
            source_columns,
            staging_schema,
            staged_gcs_prefix,
        ) = extract_to_gcs_parquet(run_id)

        if total_rows_loaded == 0:
            logger.info("No rows returned from SQL Server. No data loaded.")
        else:
            staging_table = staging_table_id(run_id)

            load_parquet_to_staging(
                client=client,
                gcs_uris=gcs_uris,
                schema=staging_schema,
                table_id=staging_table,
            )

            publish_staging_to_raw(
                client=client,
                staging_table=staging_table,
                source_columns=source_columns,
                start_date=start_date,
                end_date=end_date,
            )

        finished_at = datetime.now(timezone.utc)

        success_message = (
            f"Pipeline succeeded | load_mode={LOAD_MODE} | "
            f"write_mode={WRITE_MODE} | "
            f"gcs_files={len(gcs_uris)} | "
            f"duration_seconds={perf_counter() - pipeline_started:.2f}"
        )

        if LOAD_MODE in {"incremental", "backfill"}:
            success_message += (
                f" | window_field={WINDOW_FIELD} | "
                f"window={start_date} to {end_date}"
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
            "Pipeline finished successfully | rows_loaded=%s | "
            "run_id=%s | total_seconds=%.2f",
            total_rows_loaded,
            run_id,
            perf_counter() - pipeline_started,
        )

        return f"{total_rows_loaded} rows loaded into {RAW_TABLE}", 200

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        error_message = str(exc)

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
                "Could not log failed pipeline run: %s",
                log_error,
            )

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
                f"Rows staged before failure: {total_rows_loaded}\n"
                f"Duration seconds: {perf_counter() - pipeline_started:.2f}\n"
                f"Error: {error_message}"
            ),
        )

        logger.exception(
            "Pipeline failed | run_id=%s",
            run_id,
        )

        return f"Pipeline failed: {error_message}", 500

    finally:
        if staging_table:
            try:
                client.delete_table(
                    staging_table,
                    not_found_ok=True,
                )
                logger.info(
                    "Deleted temporary BigQuery staging table: %s",
                    staging_table,
                )
            except Exception as staging_cleanup_error:
                logger.warning(
                    "Could not delete staging table: %s",
                    staging_cleanup_error,
                )

        if CLEANUP_GCS_FILES and staged_gcs_prefix:
            try:
                delete_gcs_prefix(
                    storage_client,
                    staged_gcs_prefix,
                )
            except Exception as gcs_cleanup_error:
                logger.warning(
                    "Could not delete temporary GCS files: %s",
                    gcs_cleanup_error,
                )