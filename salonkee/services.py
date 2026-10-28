import json
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

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

PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_services")
TABLE_NAME = os.environ.get("TABLE_NAME", "services")

API_URL = os.environ.get(
    "API_URL",
    (
        "https://salonkee.be/api/web-services/"
        "v3/partners/salons/services?salonGroupId=157"
    ),
)
API_TOKEN = os.environ.get("API_TOKEN")
API_LANGUAGE = os.environ.get("API_LANGUAGE", "nl")

# This endpoint does not document an incremental filter.
LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", 10000))

RATE_LIMIT_SLEEP_SECONDS = int(
    os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90)
)

INCLUDE_DELETED = os.environ.get(
    "INCLUDE_DELETED", "false"
).strip().lower()

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
# =================================
# Keep the existing destination columns.
# Nested fields are stored as JSON-formatted STRINGs.
# raw_payload preserves the complete source record.

TABLE_SCHEMA = [
    bigquery.SchemaField("service_id", "INT64"),
    bigquery.SchemaField("service_name", "STRING"),
    bigquery.SchemaField("service_group_id", "INT64"),
    bigquery.SchemaField("service_group_name", "STRING"),
    bigquery.SchemaField("salon_id", "INT64"),

    # Additional V3 fields
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("displayGroupSortIndex", "INT64"),
    bigquery.SchemaField("updated", "TIMESTAMP"),
    bigquery.SchemaField("items", "STRING"),

    # Preserve translations and complete source data
    bigquery.SchemaField("name_json", "STRING"),
    bigquery.SchemaField("displayGroupName_json", "STRING"),
    bigquery.SchemaField("raw_payload", "STRING"),

    # Pipeline metadata
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]


# =================================
# Helpers
# =================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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

    if LOAD_MODE != "full":
        raise ValueError(
            "This V3 endpoint does not document an incremental filter. "
            "Set LOAD_MODE=full."
        )

    if MAX_RETRIES < 1:
        raise ValueError("MAX_RETRIES must be at least 1")

    if REQUEST_TIMEOUT <= 0:
        raise ValueError("REQUEST_TIMEOUT must be greater than 0")

    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0")

    if not 1 <= PAGE_SIZE <= 100000:
        raise ValueError("PAGE_SIZE must be between 1 and 100000")

    if RATE_LIMIT_SLEEP_SECONDS < 0:
        raise ValueError("RATE_LIMIT_SLEEP_SECONDS cannot be negative")

    if INCLUDE_DELETED not in {"true", "false"}:
        raise ValueError("INCLUDE_DELETED must be 'true' or 'false'")


def extract_page_records(payload) -> list:
    if isinstance(payload, list):
        records = payload
    elif (
        isinstance(payload, dict)
        and isinstance(payload.get("data"), list)
    ):
        records = payload["data"]
    else:
        raise ValueError(
            "Unexpected API response: expected a list or "
            "an object containing a 'data' list"
        )

    if any(not isinstance(record, dict) for record in records):
        raise ValueError("Every API record must be a JSON object")

    return records


def to_json(value):
    if value is None:
        return None

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def localized_name(value):
    if value is None:
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        languages = list(dict.fromkeys([
            API_LANGUAGE,
            "nl",
            "en",
            "fr",
            "de",
            "it",
        ]))

        for language in languages:
            text = value.get(language)

            if isinstance(text, str) and text:
                return text

        return next(
            (
                text
                for text in value.values()
                if isinstance(text, str) and text
            ),
            None,
        )

    raise ValueError(
        f"Unexpected localized name type: {type(value).__name__}"
    )


def retry_after_seconds(response) -> float:
    value = response.headers.get("Retry-After")

    if value:
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)

                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)

                return max(
                    0.0,
                    (retry_at - utc_now()).total_seconds(),
                )
            except (TypeError, ValueError, OverflowError):
                pass

    return float(RATE_LIMIT_SLEEP_SECONDS)


def ensure_raw_table_schema(client) -> None:
    """
    Create the table if needed, or add missing nullable columns.
    Existing column types must match the configured schema.
    """
    desired_table = bigquery.Table(RAW_TABLE, schema=TABLE_SCHEMA)
    table = client.create_table(desired_table, exists_ok=True)

    existing_fields = {
        field.name.lower(): field
        for field in table.schema
    }

    missing_fields = []

    for field in TABLE_SCHEMA:
        existing = existing_fields.get(field.name.lower())

        if existing is None:
            missing_fields.append(field)
            continue

        if (
            existing.field_type != field.field_type
            or existing.mode != field.mode
        ):
            raise ValueError(
                f"Incompatible BigQuery column '{field.name}': "
                f"existing={existing.field_type}/{existing.mode}, "
                f"expected={field.field_type}/{field.mode}"
            )

    if missing_fields:
        table.schema = list(table.schema) + missing_fields
        client.update_table(table, ["schema"])

        logger.info(
            "Added BigQuery columns: %s",
            [field.name for field in missing_fields],
        )


# =================================
# API fetch
# =================================

def fetch_page(session, headers, params) -> list:
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(
            "Fetching API page | offset=%s | limit=%s | attempt=%s/%s",
            params["offset"],
            params["limit"],
            attempt,
            MAX_RETRIES,
        )

        try:
            response = session.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as error:
            if attempt == MAX_RETRIES:
                raise

            wait_seconds = min(5 * (2 ** (attempt - 1)), 60)

            logger.warning(
                "API connection failed: %s | retrying in %s seconds",
                error,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        retryable = (
            response.status_code == 429
            or 500 <= response.status_code < 600
        )

        if retryable:
            if attempt == MAX_RETRIES:
                response.raise_for_status()

            wait_seconds = (
                retry_after_seconds(response)
                if response.status_code == 429
                else min(5 * (2 ** (attempt - 1)), 60)
            )

            logger.warning(
                "API returned HTTP %s | retrying in %s seconds",
                response.status_code,
                wait_seconds,
            )

            response.close()
            time.sleep(wait_seconds)
            continue

        # Other HTTP errors, including authentication failures,
        # fail immediately.
        response.raise_for_status()

        return extract_page_records(response.json())

    raise RuntimeError("Failed to fetch API page")


def fetch_data() -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    if API_LANGUAGE:
        headers["Accept-Language"] = API_LANGUAGE

    all_records = []
    offset = 0

    with requests.Session() as session:
        while True:
            params = {
                "offset": offset,
                "limit": PAGE_SIZE,
                "includeDeleted": INCLUDE_DELETED,
            }

            records = fetch_page(
                session=session,
                headers=headers,
                params=params,
            )

            all_records.extend(records)

            logger.info(
                "Fetched API page | offset=%s | page_rows=%s | total_rows=%s",
                offset,
                len(records),
                len(all_records),
            )

            if len(records) < PAGE_SIZE:
                break

            offset += len(records)

    logger.info("API fetch complete | rows=%s", len(all_records))

    # Preserve nested objects without flattening them.
    return pd.DataFrame({"raw_payload": all_records})


# =================================
# Transform
# =================================

def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    logger.info("Transforming V3 services and bundles for raw layer")

    rows = []

    for record in df["raw_payload"]:
        if record.get("id") is None:
            raise ValueError("API record is missing required field 'id'")

        if record.get("salonId") is None:
            raise ValueError(
                "API record is missing required field 'salonId'"
            )

        payload_json = to_json(record)

        rows.append({
            # Existing destination columns
            "service_id": record.get("id"),
            "service_name": localized_name(record.get("name")),
            "service_group_id": record.get("displayGroupId"),
            "service_group_name": localized_name(
                record.get("displayGroupName")
            ),
            "salon_id": record.get("salonId"),

            # Keep service, bundle, and bundle_group records
            "type": record.get("type"),
            "displayGroupSortIndex": record.get(
                "displayGroupSortIndex"
            ),
            "updated": record.get("updated"),

            # Includes every nested item field
            "items": to_json(record.get("items")),

            # Preserve returned translation objects or strings
            "name_json": to_json(record.get("name")),
            "displayGroupName_json": to_json(
                record.get("displayGroupName")
            ),

            # Preserve all returned fields
            "raw_payload": payload_json,

            # Detect changes to any source field
            "record_hash": generate_record_hash_from_values(
                payload_json
            ),
        })

    metadata_columns = {
        "source_system",
        "run_id",
        "load_timestamp",
        "load_date",
    }

    source_columns = [
        field.name
        for field in TABLE_SCHEMA
        if field.name not in metadata_columns
    ]

    result = pd.DataFrame(rows, columns=source_columns)

    integer_columns = [
        "service_id",
        "service_group_id",
        "salon_id",
        "displayGroupSortIndex",
    ]

    for column in integer_columns:
        result[column] = pd.to_numeric(
            result[column],
            errors="raise",
        ).astype("Int64")

    string_columns = [
        "service_name",
        "service_group_name",
        "type",
        "items",
        "name_json",
        "displayGroupName_json",
        "raw_payload",
        "record_hash",
    ]

    for column in string_columns:
        result[column] = normalize_nullable_string(
            result[column]
        )

    result["updated"] = pd.to_datetime(
        result["updated"],
        utc=True,
        errors="raise",
    )

    load_timestamp = utc_now()

    result["source_system"] = SOURCE_SYSTEM
    result["run_id"] = run_id
    result["load_timestamp"] = load_timestamp
    result["load_date"] = load_timestamp.date()

    result = result[
        [field.name for field in TABLE_SCHEMA]
    ]

    logger.info(
        "Transformation complete | rows=%s",
        len(result),
    )

    return result


# =================================
# Main ETL
# =================================

def run_etl():
    client = None
    run_id = utc_now().strftime("%Y%m%d_%H%M%S_%f")
    started_at = utc_now()

    logger.info(
        "Pipeline started | pipeline=%s | run_id=%s",
        PIPELINE_NAME,
        run_id,
    )
    logger.info("Target raw table: %s", RAW_TABLE)

    try:
        validate_config()
        client = get_bq_client()

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        ensure_raw_table_schema(client)

        # Keep the existing shared loader's write behavior.
        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=TABLE_SCHEMA,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = utc_now()

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
            "Pipeline finished successfully | rows_loaded=%s | run_id=%s",
            len(df),
            run_id,
        )

        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as error:
        finished_at = utc_now()

        logger.exception(
            "Pipeline failed | run_id=%s",
            run_id,
        )

        if client is not None:
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
                    message=str(error),
                )
            except Exception:
                logger.exception(
                    "Could not log failed pipeline run"
                )

        try:
            send_email(
                subject=f"❌ {PIPELINE_NAME} pipeline failed",
                body=(
                    f"Pipeline: {PIPELINE_NAME}\n"
                    f"Run ID: {run_id}\n"
                    f"Time: {finished_at.isoformat()}\n"
                    f"Error: {error}"
                ),
            )
        except Exception:
            logger.exception(
                "Could not send pipeline failure email"
            )

        return f"Pipeline failed: {error}", 500


# =================================
# Cloud Run job entry point
# =================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    message, status_code = run_etl()
    logger.info("%s", message)

    # Cloud Run jobs use the process exit code to determine success.
    raise SystemExit(0 if status_code == 200 else 1)