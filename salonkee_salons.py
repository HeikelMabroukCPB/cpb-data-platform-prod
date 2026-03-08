import os
from datetime import datetime

import pandas as pd
import requests
from flask import Flask
from google.cloud import bigquery


app = Flask(__name__)

# ==============================
# Config
# ==============================
PROJECT_ID = os.environ.get("PROJECT_ID", "cpb-data-platform-prod")
DATASET_RAW = os.environ.get("DATASET_RAW", "cpb_raw")
TABLE_NAME = os.environ.get("TABLE_NAME", "salonkee_salons")

API_URL = os.environ.get("API_URL")
API_TOKEN = os.environ.get("API_TOKEN")

RAW_TABLE = f"{PROJECT_ID}.{DATASET_RAW}.{TABLE_NAME}"


# ==============================
# Helpers
# ==============================
def fetch_data() -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
    }

    response = requests.get(API_URL, headers=headers, timeout=60)
    response.raise_for_status()

    data = response.json()

    if isinstance(data, list):
        records = data
    elif isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        records = data["data"]
    elif isinstance(data, dict):
        records = [data]
    else:
        raise ValueError("Unsupported API response format")

    return pd.json_normalize(records)


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "id": "salon_id",
        "displayName": "display_name",
        "link": "salon_link",
    })

    df = df[["salon_id", "display_name", "salon_link"]].copy()

    df["load_timestamp"] = datetime.utcnow()

    return df


def load_to_bigquery(df: pd.DataFrame) -> None:
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, RAW_TABLE, job_config=job_config)
    job.result()


def run_etl():
    if not API_URL or not API_TOKEN:
        return "Missing API_URL or API_TOKEN", 500

    df = fetch_data()
    df = transform_dataframe(df)
    load_to_bigquery(df)

    return f"Loaded {len(df)} rows into {RAW_TABLE}", 200


# ==============================
# Flask route
# ==============================
@app.route("/", methods=["GET", "POST"])
def trigger():
    return run_etl()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)