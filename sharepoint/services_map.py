import json
import logging
import os
import time
from datetime import datetime

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

MODULE_NAME = os.environ.get("MODULE_NAME", "sharepoint_list.services_map")
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "sharepoint_services_map")
TABLE_NAME = os.environ.get("TABLE_NAME", "services_map")

TENANT_ID = os.environ.get("TENANT_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

SHAREPOINT_SITE_ID = os.environ.get("SHAREPOINT_SITE_ID")
SHAREPOINT_LIST_ID = os.environ.get("SHAREPOINT_LIST_ID")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 10000))
RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 90))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", 500))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "sharepoint")

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{SOURCE_SYSTEM}_{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Table schema
# Raw = keep generic SharePoint raw structure
# =================================

TABLE_SCHEMA = [
    bigquery.SchemaField("item_id", "STRING"),
    bigquery.SchemaField("etag", "STRING"),
    bigquery.SchemaField("created_datetime", "TIMESTAMP"),
    bigquery.SchemaField("last_modified_datetime", "TIMESTAMP"),
    bigquery.SchemaField("web_url", "STRING"),
    bigquery.SchemaField("content_type", "STRING"),
    bigquery.SchemaField("fields_json", "STRING"),
    bigquery.SchemaField("raw_item_json", "STRING"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("record_hash", "STRING"),
]

SELECTED_COLUMNS = [
    "item_id",
    "etag",
    "created_datetime",
    "last_modified_datetime",
    "web_url",
    "content_type",
    "fields_json",
    "raw_item_json",
]


# =================================
# Helpers
# =================================

def validate_config() -> None:
    validate_common_config({
        "PROJECT_ID": PROJECT_ID,
        "DATASET_RAW": DATASET_RAW,
        "DATASET_META": DATASET_META,
        "MODULE_NAME": MODULE_NAME,
        "PIPELINE_NAME": PIPELINE_NAME,
        "TABLE_NAME": TABLE_NAME,
        "SOURCE_SYSTEM": SOURCE_SYSTEM,
        "TENANT_ID": TENANT_ID,
        "CLIENT_ID": CLIENT_ID,
        "CLIENT_SECRET": CLIENT_SECRET,
        "SHAREPOINT_SITE_ID": SHAREPOINT_SITE_ID,
        "SHAREPOINT_LIST_ID": SHAREPOINT_LIST_ID,
    })

    if LOAD_MODE not in ["full"]:
        raise ValueError("For this SharePoint pipeline, LOAD_MODE must be 'full'")


def normalize_json_field(value):
    if pd.isna(value) or value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    return str(value)


def get_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    logger.info("Requesting Microsoft Graph access token")

    response = requests.post(
        token_url,
        data=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    access_token = response.json().get("access_token")
    if not access_token:
        raise ValueError("No access_token returned from Microsoft Graph auth")

    return access_token


def build_api_url() -> str:
    return (
        f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}"
        f"/lists/{SHAREPOINT_LIST_ID}/items"
        f"?expand=fields&$top={PAGE_SIZE}"
    )


def extract_page_records(payload):
    if not payload:
        return []

    if isinstance(payload, dict) and "value" in payload and isinstance(payload["value"], list):
        return payload["value"]

    raise ValueError("Unsupported SharePoint API response format")


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    access_token = get_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "cpb-data-platform/1.0",
    }

    url = build_api_url()
    all_records = []

    logger.info(f"Fetching SharePoint list data | initial_url={url}")

    page_number = 1

    while url:
        logger.info(f"Fetching page {page_number}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    headers=headers,
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
                        f"Rate limit hit on page {page_number}, attempt {attempt}/{MAX_RETRIES}. "
                        f"Waiting {wait_seconds} seconds before retry."
                    )

                    if attempt == MAX_RETRIES:
                        response.raise_for_status()

                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()

                data = response.json()
                page_records = extract_page_records(data)
                all_records.extend(page_records)

                logger.info(
                    f"Fetched {len(page_records)} rows on page {page_number} | total_rows={len(all_records)}"
                )

                url = data.get("@odata.nextLink")
                page_number += 1
                break

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

    df = pd.json_normalize(all_records)
    logger.info(f"Fetched total {len(df)} rows from SharePoint")
    logger.info(f"Columns received: {list(df.columns)}")

    return df


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    logger.info("Transforming dataframe for raw layer")

    expected_cols = [
        "id",
        "eTag",
        "createdDateTime",
        "lastModifiedDateTime",
        "webUrl",
    ]

    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing expected raw columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )

    transformed_df = pd.DataFrame()

    transformed_df["item_id"] = normalize_nullable_string(df["id"])
    transformed_df["etag"] = normalize_nullable_string(df["eTag"])
    transformed_df["created_datetime"] = pd.to_datetime(df["createdDateTime"], errors="coerce", utc=True)
    transformed_df["last_modified_datetime"] = pd.to_datetime(df["lastModifiedDateTime"], errors="coerce", utc=True)
    transformed_df["web_url"] = normalize_nullable_string(df["webUrl"])

    if "contentType.name" in df.columns:
        transformed_df["content_type"] = normalize_nullable_string(df["contentType.name"])
    else:
        transformed_df["content_type"] = None

    field_columns = [col for col in df.columns if col.startswith("fields.")]
    if field_columns:
        transformed_df["fields_json"] = df[field_columns].apply(
            lambda row: json.dumps(
                {
                    col.replace("fields.", "", 1): (
                        None if pd.isna(row[col]) else row[col]
                    )
                    for col in field_columns
                },
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ),
            axis=1,
        )
    else:
        transformed_df["fields_json"] = None

    transformed_df["raw_item_json"] = df.apply(
        lambda row: json.dumps(row.dropna().to_dict(), ensure_ascii=False, sort_keys=True, default=str),
        axis=1,
    )

    transformed_df = transformed_df[SELECTED_COLUMNS].copy()

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    transformed_df["source_system"] = SOURCE_SYSTEM
    transformed_df["run_id"] = run_id
    transformed_df["load_timestamp"] = load_timestamp
    transformed_df["load_date"] = load_date
    transformed_df["record_hash"] = transformed_df.apply(
        lambda row: generate_record_hash_from_values(
            row.get("item_id"),
            row.get("etag"),
            row.get("created_datetime"),
            row.get("last_modified_datetime"),
            row.get("web_url"),
            row.get("content_type"),
            row.get("fields_json"),
            row.get("raw_item_json"),
        ),
        axis=1,
    )

    logger.info(f"Transformation complete | rows={len(transformed_df)}")
    return transformed_df


# =================================
# Main ETL
# =================================

def run_etl():
    client = get_bq_client()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.utcnow()

    logger.info(
        f"Pipeline started | module_name={MODULE_NAME} | pipeline={PIPELINE_NAME} | run_id={run_id}"
    )
    logger.info(f"Target raw table: {RAW_TABLE}")
    logger.info(f"Execution context | load_mode={LOAD_MODE}")

    try:
        validate_config()

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        load_dataframe_in_chunks(
            client=client,
            df=df,
            table_id=RAW_TABLE,
            schema=TABLE_SCHEMA,
            chunk_size=CHUNK_SIZE,
        )

        finished_at = datetime.utcnow()

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
            message=f"Pipeline succeeded | module_name={MODULE_NAME} | load_mode=full",
        )

        logger.info(
            f"Pipeline finished successfully | rows_loaded={len(df)} | run_id={run_id}"
        )
        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as e:
        finished_at = datetime.utcnow()

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
                message=f"module_name={MODULE_NAME} | {str(e)}",
            )
        except Exception as log_error:
            logger.error(f"Could not log failed pipeline run: {log_error}")

        send_email(
            subject=f"❌ {PIPELINE_NAME} pipeline failed",
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Module name: {MODULE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
                f"Load mode: {LOAD_MODE}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.exception(f"Pipeline failed | run_id={run_id}")
        return f"Pipeline failed: {str(e)}", 500