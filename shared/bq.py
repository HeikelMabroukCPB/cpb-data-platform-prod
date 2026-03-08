import logging

from google.cloud import bigquery


logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client()


def load_dataframe_in_chunks(
    client: bigquery.Client,
    df,
    table_id: str,
    schema: list,
    chunk_size: int,
) -> None:
    if df.empty:
        logger.warning(f"No rows to load into {table_id}")
        return

    total_rows = len(df)
    logger.info(f"Loading {total_rows} rows into {table_id} in chunks of {chunk_size}")

    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )

        job = client.load_table_from_dataframe(
            chunk,
            table_id,
            job_config=job_config,
        )
        job.result()

        logger.info(f"Loaded chunk rows {start} to {end}")