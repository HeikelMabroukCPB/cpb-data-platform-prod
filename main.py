import os
import requests
import pandas as pd
from google.cloud import bigquery

API_URL = os.environ["API_URL"]
API_TOKEN = os.environ["API_TOKEN"]
TABLE_ID = os.environ["TABLE_ID"]  # project.dataset.table

response = requests.get(
    API_URL,
    headers={"Authorization": f"Bearer {API_TOKEN}"},
    timeout=60,
)
response.raise_for_status()

data = response.json()

if isinstance(data, dict) and "data" in data:
    data = data["data"]
elif isinstance(data, dict):
    data = [data]

df = pd.json_normalize(data)

client = bigquery.Client()
job = client.load_table_from_dataframe(
    df,
    TABLE_ID,
    job_config=bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    ),
)
job.result()

print(f"{len(df)} rows loaded into {TABLE_ID}")