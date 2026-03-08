import logging
from datetime import datetime

from google.cloud import bigquery


logger = logging.getLogger(__name__)


def log_pipeline_run(
    client: bigquery.Client,
    meta_table: str,
    pipeline_name: str,
    run_id: str,
    status: str,
    rows_loaded: int,
    started_at: datetime,
    finished_at: datetime,
    message: str,
) -> None:
    duration = (finished_at - started_at).total_seconds()

    rows = [{
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "status": status,
        "rows_loaded": rows_loaded,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "message": message,
    }]

    errors = client.insert_rows_json(meta_table, rows)

    if errors:
        logger.error(f"Failed to log pipeline metadata: {errors}")