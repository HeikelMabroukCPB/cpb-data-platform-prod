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
    "pipedrive_users",
)

SOURCE_SYSTEM = "pipedrive"
TABLE_NAME = "users"

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

USERS_URL = (
    f"https://{PIPEDRIVE_COMPANY_DOMAIN}.pipedrive.com"
    f"/api/v1/users"
)


# =================================
# Schema
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("user_id", "INT64"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("email", "STRING"),

    bigquery.SchemaField("active_flag", "BOOL"),
    bigquery.SchemaField("is_admin", "BOOL"),

    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("modified", "TIMESTAMP"),

    bigquery.SchemaField("timezone_name", "STRING"),
    bigquery.SchemaField("locale", "STRING"),
    bigquery.SchemaField("language", "STRING"),

    bigquery.SchemaField("icon_url", "STRING"),

    bigquery.SchemaField("raw_payload", "STRING"),

    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
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


# =================================
# API
# =================================

def fetch_data():

    params = {
        "api_token": PIPEDRIVE_API_TOKEN,
    }

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            logger.info(
                "Fetching Pipedrive users"
            )

            response = requests.get(
                USERS_URL,
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

            logger.info(
                "Finished fetching "
                "Pipedrive users "
                f"| total={len(data)}"
            )

            return pd.DataFrame(
                data
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
        "Failed to fetch Pipedrive users"
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
            "No Pipedrive users returned"
        )

        return pd.DataFrame(
            columns=[
                field.name
                for field in TABLE_SCHEMA
            ]
        )

    transformed = pd.DataFrame()

    # =================================
    # Core user fields
    # =================================

    transformed["user_id"] = (
        pd.to_numeric(
            df["id"],
            errors="coerce",
        ).astype("Int64")
    )

    transformed["name"] = (
        df["name"]
        .astype("string")
    )

    transformed["email"] = (
        df["email"]
        .astype("string")
        if "email" in df.columns
        else None
    )

    transformed["active_flag"] = (
        df["active_flag"]
        .astype("boolean")
        if "active_flag" in df.columns
        else None
    )

    transformed["is_admin"] = (
        df["is_admin"]
        .astype("boolean")
        if "is_admin" in df.columns
        else None
    )

    # =================================
    # Dates
    # =================================

    transformed["created"] = (
        df["created"]
        .apply(parse_timestamp)
        if "created" in df.columns
        else None
    )

    transformed["modified"] = (
        df["modified"]
        .apply(parse_timestamp)
        if "modified" in df.columns
        else None
    )

    # =================================
    # User settings
    # =================================

    transformed["timezone_name"] = (
        df["timezone_name"]
        .astype("string")
        if "timezone_name" in df.columns
        else None
    )

    transformed["locale"] = (
        df["locale"]
        .astype("string")
        if "locale" in df.columns
        else None
    )

    transformed["language"] = (
        df["lang"]
        .astype("string")
        if "lang" in df.columns
        else None
    )

    # =================================
    # Icon
    # =================================

    transformed["icon_url"] = (
        df["icon_url"]
        .astype("string")
        if "icon_url" in df.columns
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
    # Technical
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
                row["user_id"],
                row["name"],
                row["email"],
                row["active_flag"],
                row["is_admin"],
                row["modified"],
                row["raw_payload"],
            ),
        axis=1,
    )

    # =================================
    # String normalization
    # =================================

    string_columns = [
        "name",
        "email",
        "timezone_name",
        "locale",
        "language",
        "icon_url",
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