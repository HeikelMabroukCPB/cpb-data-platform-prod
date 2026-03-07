import os
import time
import smtplib
import hashlib
import logging
from datetime import datetime, timedelta
from email.message import EmailMessage

import pandas as pd
import requests
from flask import Flask
from google.cloud import bigquery


# =================================
# App
# =================================

app = Flask(__name__)


# =================================
# Configuration
# =================================

PROJECT_ID = os.environ.get("PROJECT_ID", "cpb-data-platform-prod")
DATASET_RAW = os.environ.get("DATASET_RAW", "cpb_raw")
DATASET_META = os.environ.get("DATASET_META", "cpb_meta")

TABLE_NAME = os.environ.get("TABLE_NAME", "salonkee_salons")
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "salonkee_salons")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

LOAD_MODE = os.environ.get("LOAD_MODE", "full").lower()
INCREMENTAL_FIELD = os.environ.get("INCREMENTAL_FIELD", "updatedSince")
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", 2))

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 5000))

SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "salonkee")

RATE_LIMIT_SLEEP_SECONDS = int(os.environ.get("RATE_LIMIT_SLEEP_SECONDS", 60))

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{TABLE_NAME}"
META_TABLE = f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"


# =================================
# Logging
# =================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =================================
# Schema / table config
# =================================

TABLE_CONFIG = {
    "salonkee_salons": {
        "rename_map": {
            "id": "salon_id",
            "displayName": "display_name",
            "link": "salon_link",
        },
        "selected_columns": [
            "salon_id",
            "display_name",
            "salon_link",
        ],
        "schema": [
            bigquery.SchemaField("salon_id", "INT64"),
            bigquery.SchemaField("display_name", "STRING"),
            bigquery.SchemaField("salon_link", "STRING"),
            bigquery.SchemaField("source_system", "STRING"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("load_date", "DATE"),
            bigquery.SchemaField("record_hash", "STRING"),
        ],
    }
}


# =================================
# Helpers
# =================================

def send_email(subject: str, body: str) -> None:
    sender = os.environ.get("EMAIL_SENDER")
    receiver = os.environ.get("EMAIL_RECEIVER")
    password = os.environ.get("EMAIL_APP_PASSWORD")

    if not sender or not receiver or not password:
        logger.warning("Email not configured")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("Failure email sent")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def validate_config() -> None:
    required = {
        "API_URL": API_URL,
        "API_TOKEN": API_TOKEN,
        "TABLE_NAME": TABLE_NAME,
        "PIPELINE_NAME": PIPELINE_NAME,
    }

    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    if TABLE_NAME not in TABLE_CONFIG:
        raise ValueError(f"No table config found for TABLE_NAME='{TABLE_NAME}'")

    if LOAD_MODE not in ["full", "incremental"]:
        raise ValueError("LOAD_MODE must be 'full' or 'incremental'")


def build_incremental_params() -> dict:
    if LOAD_MODE != "incremental":
        return {}

    since_date = datetime.utcnow() - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    since_value = since_date.strftime("%Y-%m-%d")

    logger.info(f"Incremental load enabled | {INCREMENTAL_FIELD}={since_value}")
    return {INCREMENTAL_FIELD: since_value}


def generate_record_hash(row: pd.Series) -> str:
    value = f"{row['salon_id']}_{row['display_name']}_{row['salon_link']}"
    return hashlib.sha256(value.encode()).hexdigest()


def extract_page_records(page_data):
    if not page_data:
        return []

    if isinstance(page_data, list):
        return page_data

    if isinstance(page_data, dict):
        if "data" in page_data and isinstance(page_data["data"], list):
            return page_data["data"]
        return [page_data]

    raise ValueError("Unsupported API response format")


# =================================
# API fetch
# =================================

def fetch_data() -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
    }

    params = build_incremental_params()

    logger.info("Fetching salons data")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                logger.warning(
                    f"Rate limit hit (attempt {attempt}/{MAX_RETRIES}). "
                    f"Sleeping {RATE_LIMIT_SLEEP_SECONDS} seconds."
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            response.raise_for_status()

            data = response.json()
            records = extract_page_records(data)

            df = pd.json_normalize(records)

            logger.info(f"Fetched {len(df)} rows from API")

            return df

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error attempt {attempt}/{MAX_RETRIES}: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(2)

        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(2)

    raise RuntimeError("Failed to fetch data from API")


# =================================
# Transform
# =================================

def transform_dataframe(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    config = TABLE_CONFIG[TABLE_NAME]
    rename_map = config["rename_map"]
    selected_columns = config["selected_columns"]

    logger.info("Transforming dataframe")

    df = df.rename(columns=rename_map)

    missing_cols = [col for col in selected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns after rename: {missing_cols}")

    df = df[selected_columns].copy()

    df["salon_id"] = pd.to_numeric(df["salon_id"], errors="coerce").astype("Int64")
    df["display_name"] = df["display_name"].astype("string")
    df["salon_link"] = df["salon_link"].astype("string")

    load_timestamp = datetime.utcnow()
    load_date = load_timestamp.date()

    df["source_system"] = SOURCE_SYSTEM
    df["run_id"] = run_id
    df["load_timestamp"] = load_timestamp
    df["load_date"] = load_date
    df["record_hash"] = df.apply(generate_record_hash, axis=1)

    logger.info(f"Transformation complete | rows={len(df)}")
    return df


# =================================
# BigQuery load
# =================================

def load_dataframe_in_chunks(client: bigquery.Client, df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No rows to load")
        return

    schema = TABLE_CONFIG[TABLE_NAME]["schema"]
    total_rows = len(df)

    logger.info(f"Loading {total_rows} rows into {RAW_TABLE} in chunks of {CHUNK_SIZE}")

    for start in range(0, total_rows, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, total_rows)
        chunk = df.iloc[start:end]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )

        job = client.load_table_from_dataframe(
            chunk,
            RAW_TABLE,
            job_config=job_config,
        )
        job.result()

        logger.info(f"Loaded chunk rows {start} to {end}")


def log_pipeline_run(
    client: bigquery.Client,
    run_id: str,
    status: str,
    rows_loaded: int,
    started_at: datetime,
    finished_at: datetime,
    message: str,
) -> None:
    duration = (finished_at - started_at).total_seconds()

    rows = [{
        "pipeline_name": PIPELINE_NAME,
        "run_id": run_id,
        "status": status,
        "rows_loaded": rows_loaded,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "message": message,
    }]

    errors = client.insert_rows_json(META_TABLE, rows)

    if errors:
        logger.error(f"Failed to log pipeline metadata: {errors}")


# =================================
# Main ETL
# =================================

def run_etl():
    client = bigquery.Client()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.utcnow()

    logger.info(f"Pipeline started | pipeline={PIPELINE_NAME} | run_id={run_id}")

    try:
        validate_config()

        raw_df = fetch_data()
        df = transform_dataframe(raw_df, run_id)

        load_dataframe_in_chunks(client, df)

        finished_at = datetime.utcnow()

        log_pipeline_run(
            client=client,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
            message="Pipeline succeeded",
        )

        logger.info(f"Pipeline finished successfully | rows_loaded={len(df)} | run_id={run_id}")
        return f"{len(df)} rows loaded into {RAW_TABLE}", 200

    except Exception as e:
        finished_at = datetime.utcnow()

        try:
            log_pipeline_run(
                client=client,
                run_id=run_id,
                status="FAILED",
                rows_loaded=0,
                started_at=started_at,
                finished_at=finished_at,
                message=str(e),
            )
        except Exception as log_error:
            logger.error(f"Could not log failed pipeline run: {log_error}")

        send_email(
            subject=f"❌ {PIPELINE_NAME} pipeline failed",
            body=(
                f"Pipeline: {PIPELINE_NAME}\n"
                f"Run ID: {run_id}\n"
                f"Time: {finished_at}\n"
                f"Error: {str(e)}"
            ),
        )

        logger.error(f"Pipeline failed | run_id={run_id} | error={e}")
        return f"Pipeline failed: {e}", 500


# =================================
# Flask routes for Cloud Run
# =================================

@app.route("/", methods=["GET", "POST"])
def trigger():
    message, status_code = run_etl()
    return message, status_code


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


# =================================
# Local run
# =================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)