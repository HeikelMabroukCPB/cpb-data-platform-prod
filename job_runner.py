import importlib
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    source_system = os.environ.get("SOURCE_SYSTEM")
    table_name = os.environ.get("TABLE_NAME")

    if not source_system or not table_name:
        raise ValueError("SOURCE_SYSTEM and TABLE_NAME environment variables are required")

    module_name = f"{source_system}.{table_name}"
    logger.info(f"Running pipeline module: {module_name}")

    module = importlib.import_module(module_name)

    if not hasattr(module, "run_etl"):
        raise ValueError(f"Module '{module_name}' does not expose run_etl()")

    message, status_code = module.run_etl()

    logger.info(f"Pipeline finished with status_code={status_code} | message={message}")

    if status_code >= 400:
        raise RuntimeError(message)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Job failed")
        sys.exit(1)