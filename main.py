import importlib
import logging
import os

from flask import Flask


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def run_pipeline():
    source_system = os.environ.get("SOURCE_SYSTEM")
    table_name = os.environ.get("TABLE_NAME")

    if not source_system or not table_name:
        raise ValueError("SOURCE_SYSTEM and TABLE_NAME environment variables are required")

    module_name = f"{source_system}.{table_name}"
    logger.info(f"Trying to run pipeline module: {module_name}")

    module = importlib.import_module(module_name)

    if not hasattr(module, "run_etl"):
        raise ValueError(f"Module '{module_name}' does not expose a run_etl() function")

    return module.run_etl()


@app.route("/", methods=["GET", "POST"])
def trigger():
    try:
        message, status_code = run_pipeline()
        return message, status_code
    except Exception as e:
        logger.exception("Pipeline execution failed")
        return f"Pipeline execution failed: {str(e)}", 500


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)