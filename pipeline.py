# pipeline.py
#
# End-to-end orchestration for the Medical Telegram Warehouse project.
# Works on Windows + PowerShell + venv, and avoids "python -m dbt" issues by
# calling the dbt executable from .venv\Scripts\dbt.exe.

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from dagster import op, job, get_dagster_logger


# ---------- paths / helpers ----------

REPO_ROOT = Path(__file__).resolve().parent
DBT_DIR = REPO_ROOT / "medical_warehouse"
ENV_PATH = REPO_ROOT / ".env"

# Use the dbt executable in the repo venv
DBT_EXE = REPO_ROOT / ".venv" / "Scripts" / "dbt.exe"


def _load_env() -> None:
    # Always load from repo-root .env
    load_dotenv(dotenv_path=ENV_PATH, override=True)


def run_cmd(cmd: str) -> None:
    """
    Run a shell command from repo root.
    Raises RuntimeError if it fails.
    """
    log = get_dagster_logger()
    log.info(f"Running: {cmd}")

    result = subprocess.run(cmd, shell=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {cmd}")


def today_partition() -> str:
    # "YYYY-MM-DD"
    return datetime.now().strftime("%Y-%m-%d")


# ---------- Dagster ops ----------

@op
def scrape_telegram_data():
    """
    Scrape raw Telegram messages + images into the data lake.
    Skip if today's partition already exists.
    """
    _load_env()
    log = get_dagster_logger()

    day = today_partition()
    partition_dir = REPO_ROOT / "data" / "raw" / "telegram_messages" / day

    if partition_dir.exists() and any(partition_dir.glob("*.json")):
        log.info(f"Raw data already exists for {day}. Skipping scrape step.")
        return {"partition": day, "skipped": True}

    # NOTE: keep the same channels/limit you used before
    cmd = (
        r'python src\scraper.py --path data '
        r'--channels https://t.me/lobelia4cosmetics https://t.me/tikvahpharma '
        r'--limit 300 --message-delay 0.7'
    )
    run_cmd(cmd)
    return {"partition": day, "skipped": False}


@op
def load_raw_to_postgres(_scrape_result):
    """
    Load raw telegram JSON into Postgres (raw.telegram_messages).
    """
    _load_env()
    run_cmd(r"python src\load_raw_to_postgres.py")
    return {"loaded": True}


@op
def run_yolo_enrichment(_raw_load_result):
    """
    Run YOLO detection over images and create/update data\yolo_detections.csv
    """
    _load_env()
    run_cmd(r"python src\yolo_detect.py")
    return {"yolo_done": True}


@op
def load_yolo_to_postgres(_yolo_result):
    """
    Create YOLO raw tables (if needed) and load detections into raw.yolo_detections
    """
    _load_env()

    # Create table(s) via psql inside container using PowerShell piping
    run_cmd(
        r'powershell -Command "Get-Content scripts\create_yolo_tables.sql | '
        r'docker exec -i med_postgres psql -U med_user -d med_warehouse"'
    )

    run_cmd(r"python src\load_yolo_to_postgres.py")
    return {"yolo_loaded": True}


@op
def run_all_dbt_models(_yolo_loaded_result):
    """
    Run all dbt models (staging + marts).
    Uses dbt executable from the repo venv (fixes 'python -m dbt' issue).
    """
    _load_env()

    if not DBT_EXE.exists():
        raise RuntimeError(
            f"dbt executable not found at: {DBT_EXE}\n"
            "Make sure you created/installed dependencies in the repo venv (.venv)."
        )

    # dbt deps is safe to run; if already installed it’s quick
    run_cmd(rf'cd medical_warehouse && "{DBT_EXE}" deps --profiles-dir .')

    # Run everything
    run_cmd(rf'cd medical_warehouse && "{DBT_EXE}" run --profiles-dir .')

    # Optional but recommended: run tests too
    run_cmd(rf'cd medical_warehouse && "{DBT_EXE}" test --profiles-dir .')

    return {"dbt_ok": True}


# ---------- job ----------

@job
def medical_telegram_job():
    scrape = scrape_telegram_data()
    raw_loaded = load_raw_to_postgres(scrape)
    yolo_done = run_yolo_enrichment(raw_loaded)
    yolo_loaded = load_yolo_to_postgres(yolo_done)
    run_all_dbt_models(yolo_loaded)
