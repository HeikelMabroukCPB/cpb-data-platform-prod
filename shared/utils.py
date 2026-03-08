import hashlib
import logging
import time
from datetime import datetime, timedelta

import pandas as pd


logger = logging.getLogger(__name__)


def validate_common_config(required: dict) -> None:
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def build_incremental_params(
    load_mode: str,
    incremental_field: str,
    incremental_lookback_days: int,
) -> dict:
    if load_mode != "incremental":
        return {}

    since_date = datetime.utcnow() - timedelta(days=incremental_lookback_days)
    since_value = since_date.strftime("%Y-%m-%d")

    logger.info(f"Incremental load enabled | {incremental_field}={since_value}")
    return {incremental_field: since_value}


def generate_record_hash_from_values(*values) -> str:
    joined = "||".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def sleep_with_log(seconds: int, reason: str) -> None:
    logger.warning(f"{reason}. Sleeping {seconds} seconds.")
    time.sleep(seconds)


def normalize_nullable_string(series: pd.Series) -> pd.Series:
    return series.astype("string")