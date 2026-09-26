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
    validate_common_config,
)


logger = logging.getLogger(__name__)


# =================================
# Config
# =================================

PROJECT_ID = os.environ.get(
    "PROJECT_ID",
    "cpb-data-platform-prod",
)

DATASET_RAW = os.environ.get(
    "DATASET_RAW",
    "cpb_raw",
)

DATASET_META = os.environ.get(
    "DATASET_META",
    "cpb_meta",
)

PIPELINE_NAME = os.environ.get(
    "PIPELINE_NAME",
    "pipedrive_organizations",
)

SOURCE_SYSTEM = "pipedrive"
TABLE_NAME = "organizations"

PIPEDRIVE_API_TOKEN = os.environ.get(
    "PIPEDRIVE_API_TOKEN"
)

PIPEDRIVE_COMPANY_DOMAIN = os.environ.get(
    "PIPEDRIVE_COMPANY_DOMAIN"
)

LOAD_MODE = os.environ.get(
    "LOAD_MODE",
    "full",
).lower()  # full | incremental

INCREMENTAL_LOOKBACK_DAYS = int(
    os.environ.get(
        "INCREMENTAL_LOOKBACK_DAYS",
        2,
    )
)

MAX_RETRIES = int(
    os.environ.get(
        "MAX_RETRIES",
        3,
    )
)

REQUEST_TIMEOUT = int(
    os.environ.get(
        "REQUEST_TIMEOUT",
        60,
    )
)

PAGE_SIZE = int(
    os.environ.get(
        "PAGE_SIZE",
        500,
    )
)

CHUNK_SIZE = int(
    os.environ.get(
        "CHUNK_SIZE",
        5000,
    )
)

RAW_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_RAW}."
    f"{SOURCE_SYSTEM}_{TABLE_NAME}"
)

META_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_META}."
    f"pipeline_runs"
)

ORGANIZATIONS_URL = (
    f"https://{PIPEDRIVE_COMPANY_DOMAIN}.pipedrive.com"
    f"/api/v2/organizations"
)


# =================================
# Schema
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField(
        "organization_id",
        "INT64",
    ),
    bigquery.SchemaField(
        "name",
        "STRING",
    ),
    bigquery.SchemaField(
        "owner_id",
        "INT64",
    ),

    bigquery.SchemaField(
        "add_time",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "update_time",
        "TIMESTAMP",
    ),

    bigquery.SchemaField(
        "visible_to",
        "STRING",
    ),

    # Labels
    bigquery.SchemaField(
        "label_ids",
        "STRING",
    ),
    bigquery.SchemaField(
        "labels",
        "STRING",
    ),

    # Address
    bigquery.SchemaField(
        "address",
        "STRING",
    ),

    # Full API response record
    bigquery.SchemaField(
        "raw_payload",
        "STRING",
    ),

    # Technical columns
    bigquery.SchemaField(
        "source_system",
        "STRING",
    ),
    bigquery.SchemaField(
        "run_id",
        "STRING",
    ),
    bigquery.SchemaField(
        "load_timestamp",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "load_date",
        "DATE",
    ),
    bigquery.SchemaField(
        "record_hash",
        "STRING",
    ),
]


# =================================
# Validation
# =================================

def validate_config():

    validate_common_config({
        "PROJECT_ID": PROJECT_ID,
        "DATASET_RAW": DATASET_RAW,
        "DATASET_META": DATASET_META,
        "PIPELINE_NAME": PIPELINE_NAME,
        "PIPEDRIVE_API_TOKEN": PIPEDRIVE_API_TOKEN,
        "PIPEDRIVE_COMPANY_DOMAIN": PIPEDRIVE_COMPANY_DOMAIN,
    })

    if LOAD_MODE not in [
        "full",
        "incremental",
    ]:
        raise ValueError(
            "LOAD_MODE must be "
            "'full' or 'incremental'"
        )

    if PAGE_SIZE > 500:
        raise ValueError(
            "PAGE_SIZE cannot be greater than 500"
        )


# =================================
# Helpers
# =================================

def parse_timestamp(value):

    if value is None or value == "":
        return pd.NaT

    return pd.to_datetime(
        value,
        errors="coerce",
        utc=True,
    )


def normalize_json(value):

    if value is None:
        return None

    if (
        isinstance(value, float)
        and pd.isna(value)
    ):
        return None

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )


# =================================
# API
# =================================

def fetch_page(cursor=None):

    params = {
        "api_token": PIPEDRIVE_API_TOKEN,
        "limit": PAGE_SIZE,
        "sort_by": "update_time",
        "sort_direction": "asc",
        "include_labels": "true",
    }

    if cursor:
        params["cursor"] = cursor

    # ---------------------------------
    # Incremental loading
    # ---------------------------------

    if LOAD_MODE == "incremental":

        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(
                days=INCREMENTAL_LOOKBACK_DAYS
            )
        ).replace(
            microsecond=0
        )

        params["updated_since"] = (
            cutoff
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

    # ---------------------------------
    # Request
    # ---------------------------------

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            logger.info(
                "Fetching Pipedrive organizations "
                f"| cursor={cursor}"
            )

            response = requests.get(
                ORGANIZATIONS_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            # ---------------------------------
            # Rate limit
            # ---------------------------------

            if response.status_code == 429:

                logger.warning(
                    "Pipedrive rate limit reached "
                    f"| attempt={attempt}"
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(30)
                continue

            # ---------------------------------
            # Log API error body
            # ---------------------------------

            if not response.ok:

                logger.error(
                    "Pipedrive API error "
                    f"| status={response.status_code} "
                    f"| response={response.text}"
                )

            response.raise_for_status()

            # ---------------------------------
            # Parse response
            # ---------------------------------

            payload = response.json()

            if not payload.get(
                "success",
                False,
            ):
                raise ValueError(
                    "Pipedrive returned "
                    "unsuccessful response "
                    f"| response={payload}"
                )

            data = (
                payload.get("data")
                or []
            )

            additional_data = (
                payload.get(
                    "additional_data"
                )
                or {}
            )

            next_cursor = (
                additional_data.get(
                    "next_cursor"
                )
            )

            # Some responses may nest
            # pagination information
            if not next_cursor:

                pagination = (
                    additional_data.get(
                        "pagination"
                    )
                    or {}
                )

                next_cursor = (
                    pagination.get(
                        "next_cursor"
                    )
                )

            return (
                data,
                next_cursor,
            )

        except requests.RequestException as e:

            logger.warning(
                "Pipedrive request failed "
                f"| attempt={attempt} "
                f"| error={e}"
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(5)

    raise RuntimeError(
        "Failed to fetch "
        "Pipedrive organizations"
    )


def fetch_data():

    records = []
    cursor = None
    page = 1

    while True:

        page_records, next_cursor = (
            fetch_page(
                cursor=cursor
            )
        )

        if not page_records:
            break

        records.extend(
            page_records
        )

        logger.info(
            "Pipedrive page fetched "
            f"| page={page} "
            f"| rows={len(page_records)} "
            f"| total={len(records)}"
        )

        if not next_cursor:
            break

        cursor = next_cursor
        page += 1

    logger.info(
        "Finished fetching "
        "Pipedrive organizations "
        f"| total={len(records)}"
    )

    return pd.DataFrame(
        records
    )


# =================================
# Transform
# =================================

def transform_dataframe(
    df: pd.DataFrame,
    run_id: str,
):

    if df.empty:

        logger.info(
            "No Pipedrive organizations returned"
        )

        return pd.DataFrame(
            columns=[
                field.name
                for field in TABLE_SCHEMA
            ]
        )

    transformed = pd.DataFrame()

    # ---------------------------------
    # Organization
    # ---------------------------------

    transformed["organization_id"] = (
        pd.to_numeric(
            df["id"],
            errors="coerce",
        ).astype(
            "Int64"
        )
    )

    transformed["name"] = (
        df["name"]
        .astype("string")
    )

    # ---------------------------------
    # Owner
    # ---------------------------------

    transformed["owner_id"] = (
        pd.to_numeric(
            df.get("owner_id"),
            errors="coerce",
        ).astype(
            "Int64"
        )
    )

    # ---------------------------------
    # Dates
    # ---------------------------------

    transformed["add_time"] = (
        df["add_time"]
        .apply(
            parse_timestamp
        )
    )

    transformed["update_time"] = (
        df["update_time"]
        .apply(
            parse_timestamp
        )
    )

    # ---------------------------------
    # Visibility
    # ---------------------------------

    transformed["visible_to"] = (
        df["visible_to"]
        .astype("string")
        if "visible_to" in df.columns
        else None
    )

    # =================================
    # Labels
    # =================================

    transformed["label_ids"] = (
        df["label_ids"]
        .apply(
            normalize_json
        )
        if "label_ids" in df.columns
        else None
    )

    transformed["labels"] = (
        df["labels"]
        .apply(
            normalize_json
        )
        if "labels" in df.columns
        else None
    )

    # =================================
    # Address
    # =================================

    transformed["address"] = (
        df["address"]
        .apply(
            lambda value:
                value.get("value")
                if isinstance(
                    value,
                    dict,
                )
                else value
        )
        if "address" in df.columns
        else None
    )

    # =================================
    # Raw payload
    # =================================

    transformed["raw_payload"] = (
        df.apply(
            lambda row:
                json.dumps(
                    row.to_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            axis=1,
        )
    )

    # =================================
    # Technical metadata
    # =================================

    load_timestamp = (
        datetime.now(
            timezone.utc
        )
    )

    transformed[
        "source_system"
    ] = SOURCE_SYSTEM

    transformed[
        "run_id"
    ] = run_id

    transformed[
        "load_timestamp"
    ] = load_timestamp

    transformed[
        "load_date"
    ] = load_timestamp.date()

    # =================================
    # Record hash
    # =================================

    transformed[
        "record_hash"
    ] = transformed.apply(
        lambda row:
            generate_record_hash_from_values(
                row["organization_id"],
                row["name"],
                row["owner_id"],
                row["update_time"],
                row["label_ids"],
                row["labels"],
                row["raw_payload"],
            ),
        axis=1,
    )

    # ---------------------------------
    # Final string normalization
    # ---------------------------------

    string_columns = [
        "name",
        "visible_to",
        "label_ids",
        "labels",
        "address",
        "raw_payload",
        "source_system",
        "run_id",
        "record_hash",
    ]

    for col in string_columns:

        if col in transformed.columns:

            transformed[col] = (
                transformed[col]
                .apply(
                    lambda value:
                        None
                        if value is None
                        or (
                            pd.api.types.is_scalar(value)
                            and pd.isna(value)
                        )
                        else str(value)
                )
                .astype("string")
            )

    logger.info(
        "Transformation complete "
        f"| rows={len(transformed)} "
        f"| columns={len(transformed.columns)}"
    )

    return transformed


# =================================
# Main ETL
# =================================

def run_etl():

    client = get_bq_client()

    run_id = (
        datetime.now(
            timezone.utc
        )
        .strftime(
            "%Y%m%d_%H%M%S"
        )
    )

    started_at = (
        datetime.now(
            timezone.utc
        )
    )

    try:

        # ---------------------------------
        # Validate
        # ---------------------------------

        validate_config()

        logger.info(
            "Pipeline started "
            f"| pipeline={PIPELINE_NAME} "
            f"| run_id={run_id} "
            f"| load_mode={LOAD_MODE}"
        )

        logger.info(
            f"Target raw table: {RAW_TABLE}"
        )

        # ---------------------------------
        # Fetch
        # ---------------------------------

        raw_df = fetch_data()

        # ---------------------------------
        # Transform
        # ---------------------------------

        df = transform_dataframe(
            df=raw_df,
            run_id=run_id,
        )

        # ---------------------------------
        # BigQuery load
        # ---------------------------------

        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=TABLE_SCHEMA,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = (
            datetime.now(
                timezone.utc
            )
        )

        # ---------------------------------
        # Pipeline metadata
        # ---------------------------------

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
            message=(
                "Pipeline succeeded "
                f"| load_mode={LOAD_MODE}"
            ),
        )

        logger.info(
            "Pipeline finished successfully "
            f"| rows_loaded={len(df)} "
            f"| run_id={run_id}"
        )

        return (
            f"{len(df)} rows loaded "
            f"into {RAW_TABLE}",
            200,
        )

    except Exception as e:

        finished_at = (
            datetime.now(
                timezone.utc
            )
        )

        # ---------------------------------
        # Failed pipeline metadata
        # ---------------------------------

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

            logger.error(
                "Could not log "
                "failed pipeline run "
                f"| error={log_error}"
            )

        # ---------------------------------
        # Failure email
        # ---------------------------------

        send_email(
            subject=(
                f"❌ {PIPELINE_NAME} "
                "pipeline failed"
            ),
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
                f"Load mode: {LOAD_MODE}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(
            "Pipeline failed "
            f"| run_id={run_id}"
        )

        return (
            f"Pipeline failed: {str(e)}",
            500,
        )