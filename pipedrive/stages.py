import json
import logging
import os
import time
from datetime import datetime, timezone

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
    "pipedrive_stages",
)

SOURCE_SYSTEM = "pipedrive"
TABLE_NAME = "stages"

PIPEDRIVE_API_TOKEN = os.environ.get(
    "PIPEDRIVE_API_TOKEN"
)

PIPEDRIVE_COMPANY_DOMAIN = os.environ.get(
    "PIPEDRIVE_COMPANY_DOMAIN"
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

STAGES_URL = (
    f"https://{PIPEDRIVE_COMPANY_DOMAIN}.pipedrive.com"
    f"/api/v2/stages"
)


# =================================
# Schema
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField(
        "stage_id",
        "INT64",
    ),
    bigquery.SchemaField(
        "name",
        "STRING",
    ),
    bigquery.SchemaField(
        "pipeline_id",
        "INT64",
    ),
    bigquery.SchemaField(
        "order_nr",
        "INT64",
    ),

    bigquery.SchemaField(
        "deal_probability",
        "INT64",
    ),

    bigquery.SchemaField(
        "is_deal_rot_enabled",
        "BOOL",
    ),
    bigquery.SchemaField(
        "days_to_rotten",
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
        "raw_payload",
        "STRING",
    ),

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


def clean_json_value(value):

    if value is None:
        return None

    if (
        isinstance(value, float)
        and pd.isna(value)
    ):
        return None

    if isinstance(value, dict):
        return {
            key: clean_json_value(val)
            for key, val in value.items()
        }

    if isinstance(value, list):
        return [
            clean_json_value(val)
            for val in value
        ]

    return value


# =================================
# API
# =================================

def fetch_page(cursor=None):

    params = {
        "api_token": PIPEDRIVE_API_TOKEN,
        "limit": PAGE_SIZE,
        "sort_by": "order_nr",
        "sort_direction": "asc",
    }

    if cursor:
        params["cursor"] = cursor

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            logger.info(
                "Fetching Pipedrive stages "
                f"| cursor={cursor}"
            )

            response = requests.get(
                STAGES_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:

                logger.warning(
                    "Pipedrive rate limit reached "
                    f"| attempt={attempt}"
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(30)
                continue

            if not response.ok:

                logger.error(
                    "Pipedrive API error "
                    f"| status={response.status_code} "
                    f"| response={response.text}"
                )

            response.raise_for_status()

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
        "Failed to fetch Pipedrive stages"
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
            "Pipedrive stages page fetched "
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
        "Pipedrive stages "
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
            "No Pipedrive stages returned"
        )

        return pd.DataFrame(
            columns=[
                field.name
                for field in TABLE_SCHEMA
            ]
        )

    transformed = pd.DataFrame()

    # =================================
    # Stage
    # =================================

    transformed["stage_id"] = (
        pd.to_numeric(
            df["id"],
            errors="coerce",
        ).astype("Int64")
    )

    transformed["name"] = (
        df["name"].astype("string")
        if "name" in df.columns
        else None
    )

    transformed["pipeline_id"] = (
        pd.to_numeric(
            df.get("pipeline_id"),
            errors="coerce",
        ).astype("Int64")
    )

    transformed["order_nr"] = (
        pd.to_numeric(
            df.get("order_nr"),
            errors="coerce",
        ).astype("Int64")
    )

    # =================================
    # Probability
    # =================================

    transformed["deal_probability"] = (
        pd.to_numeric(
            df.get("deal_probability"),
            errors="coerce",
        ).astype("Int64")
    )

    # =================================
    # Rotten deal settings
    # =================================

    transformed["is_deal_rot_enabled"] = (
        df["is_deal_rot_enabled"]
        .astype("boolean")
        if "is_deal_rot_enabled" in df.columns
        else None
    )

    transformed["days_to_rotten"] = (
        pd.to_numeric(
            df.get("days_to_rotten"),
            errors="coerce",
        ).astype("Int64")
    )

    # =================================
    # Dates
    # =================================

    transformed["add_time"] = (
        df["add_time"].apply(
            parse_timestamp
        )
        if "add_time" in df.columns
        else None
    )

    transformed["update_time"] = (
        df["update_time"].apply(
            parse_timestamp
        )
        if "update_time" in df.columns
        else None
    )

    # =================================
    # Raw payload
    # =================================

    transformed["raw_payload"] = (
        df.apply(
            lambda row:
                json.dumps(
                    clean_json_value(
                        row.to_dict()
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                    allow_nan=False,
                ),
            axis=1,
        )
    )

    # =================================
    # Technical
    # =================================

    load_timestamp = (
        datetime.now(
            timezone.utc
        )
    )

    transformed["source_system"] = (
        SOURCE_SYSTEM
    )

    transformed["run_id"] = (
        run_id
    )

    transformed["load_timestamp"] = (
        load_timestamp
    )

    transformed["load_date"] = (
        load_timestamp.date()
    )

    # =================================
    # Record hash
    # =================================

    transformed["record_hash"] = (
        transformed.apply(
            lambda row:
                generate_record_hash_from_values(
                    row["stage_id"],
                    row["name"],
                    row["pipeline_id"],
                    row["order_nr"],
                    row["deal_probability"],
                    row["is_deal_rot_enabled"],
                    row["days_to_rotten"],
                    row["update_time"],
                    row["raw_payload"],
                ),
            axis=1,
        )
    )

    # =================================
    # String normalization
    # =================================

    string_columns = [
        "name",
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

        validate_config()

        logger.info(
            "Pipeline started "
            f"| pipeline={PIPELINE_NAME} "
            f"| run_id={run_id}"
        )

        logger.info(
            f"Target raw table: {RAW_TABLE}"
        )

        raw_df = fetch_data()

        df = transform_dataframe(
            df=raw_df,
            run_id=run_id,
        )

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

        send_email(
            subject=(
                f"❌ {PIPELINE_NAME} "
                "pipeline failed"
            ),
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
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