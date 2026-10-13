
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parent

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# Use Python from the active virtual environment.
PYTHON = sys.executable


# ============================================================
# 2. PIPELINE STEPS
# ============================================================

# Each step runs in dependency order.
# Quality gates must pass before downstream processing continues.

PIPELINE_STEPS = [
    # BRONZE
    ("Bronze ingestion", "src/ingestion/ingest_bronze.py"),

    # SILVER AND QUARANTINE
    ("Order quarantine", "src/quality/build_quarantine.py"),
    ("Customer and product Silver", "src/quality/build_silver_dimensions.py"),
    ("Silver orders", "src/quality/build_silver_orders.py"),
    ("Silver order items", "src/quality/build_silver_order_items.py"),
    ("Order item quarantine report", "src/quality/quarantine_order_items.py"),
    ("Silver deliveries", "src/quality/build_silver_deliveries.py"),
    ("Silver returns", "src/quality/build_silver_returns.py"),

    # QUALITY GATE 1
    ("Cross-table reconciliation", "src/quality/cross_table_reconciliation.py"),

    # REPORTING
    ("Data quality report", "src/quality/quality_report.py"),

    # GOLD
    ("Gold daily sales", "src/quality/build_gold_daily_sales.py"),
    ("Gold product performance", "src/quality/build_gold_product_performance.py"),
    ("Gold delivery performance", "src/quality/build_gold_delivery_performance.py"),
    ("Gold customer analytics", "src/quality/build_gold_customer_analytics.py"),
    ("Gold returns analytics", "src/quality/build_gold_returns_analytics.py"),

    # QUALITY GATE 2
    ("Gold validation", "src/quality/validate_gold_layer.py"),

    # FINAL LOAD — only after all preceding steps pass
    ("Load Gold into PostgreSQL", "src/quality/load_gold_to_postgres.py"),
]


# ============================================================
# 3. PIPELINE EXECUTION
# ============================================================

def run_step(index, total_steps, step_name, relative_script):
    """Execute one pipeline step and return its exit code."""

    script_path = ROOT / relative_script

    logging.info("-" * 65)
    logging.info("STEP %s/%s: %s", index, total_steps, step_name)
    logging.info("Script: %s", relative_script)

    # Check that the script exists before execution.
    if not script_path.is_file():
        logging.error("SCRIPT NOT FOUND: %s", script_path)
        return 1

    try:
        result = subprocess.run(
            [PYTHON, "-u", str(script_path)],
            cwd=ROOT,
            check=False,
        )

        if result.returncode != 0:
            logging.error(
                "FAILED: %s | Exit code: %s",
                step_name,
                result.returncode,
            )

            logging.error(
                "QUALITY GATE FAILED OR SCRIPT ERROR. "
                "Pipeline execution stopped."
            )

            return result.returncode or 1

        logging.info("PASSED: %s", step_name)
        return 0

    except KeyboardInterrupt:
        logging.warning("Pipeline interrupted by user.")
        return 130

    except Exception:
        logging.exception("Unexpected error in step: %s", step_name)
        return 1


# ============================================================
# 4. MAIN PIPELINE
# ============================================================

def main():
    start_time = datetime.now()

    logging.info("=" * 65)
    logging.info("TEC SYS DATA ENGINEERING PIPELINE STARTED")
    logging.info("Project root: %s", ROOT)
    logging.info("Python interpreter: %s", PYTHON)
    logging.info("Log file: %s", LOG_FILE)
    logging.info("Total steps: %s", len(PIPELINE_STEPS))
    logging.info("=" * 65)

    for index, (step_name, relative_script) in enumerate(
        PIPELINE_STEPS,
        start=1,
    ):
        exit_code = run_step(
            index,
            len(PIPELINE_STEPS),
            step_name,
            relative_script,
        )

        if exit_code != 0:
            elapsed = datetime.now() - start_time

            logging.error("=" * 65)
            logging.error("PIPELINE FAILED")
            logging.error("Failed step: %s", step_name)
            logging.error("Exit code: %s", exit_code)
            logging.error("Duration before failure: %s", elapsed)
            logging.error(
                "PostgreSQL loading and subsequent steps were skipped "
                "if they had not already run."
            )
            logging.error("Review the log file: %s", LOG_FILE)
            logging.error("=" * 65)

            return exit_code

    elapsed = datetime.now() - start_time

    logging.info("=" * 65)
    logging.info("PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("All %s steps passed.", len(PIPELINE_STEPS))
    logging.info("Total duration: %s", elapsed)
    logging.info("Log file: %s", LOG_FILE)
    logging.info("=" * 65)

    return 0


# ============================================================
# 5. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
