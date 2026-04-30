"""
paysim_batch_ingestion.py
--------------------------
Orchestrates the full PaySim ingestion pipeline:

  [detect_next_batch] → [upload_to_gcs] → [raw_to_snowflake] → [dbt_staging] → [dbt_trusted]

Trigger options:
  - Manual with config:  {"batch_filename": "batch_0005_179016rows.csv"}
  - Manual without config: auto-detects the next unprocessed batch
  - Scheduled: runs on schedule and auto-detects
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# ─── constants ────────────────────────────────────────────────────────────────
BATCHES_DIR = os.path.join(PROJECT_ROOT, "dataset", "batches_output")
TRANSFORM_DIR = os.path.join(PROJECT_ROOT, "transform")

# ─── helpers ──────────────────────────────────────────────────────────────────

def _detect_next_batch(**context) -> str:
    """
    Returns the batch filename to process.
    Priority:
      1. dag_run.conf['batch_filename']  (manual trigger with explicit file)
      2. Airflow Variable 'paysim_last_batch' + 1
      3. batch_0001 as fallback
    Pushes the result to XCom for downstream tasks.
    """
    conf = context["dag_run"].conf or {}
    batch_filename = conf.get("batch_filename")

    if not batch_filename:
        last = Variable.get("paysim_last_batch", default_var=None)
        if last:
            last_num = int(last.split("_")[1])
            next_num = last_num + 1
        else:
            next_num = 1

        # find the file matching the next batch number
        all_batches = sorted(
            f for f in os.listdir(BATCHES_DIR) if f.endswith(".csv")
        )
        matching = [f for f in all_batches if f.startswith(f"batch_{next_num:04d}_")]

        if not matching:
            raise AirflowSkipException(
                f"No batch found for number {next_num:04d} — all batches processed."
            )
        batch_filename = matching[0]

    full_path = os.path.join(BATCHES_DIR, batch_filename)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Batch file not found: {full_path}")

    context["ti"].xcom_push(key="batch_filename", value=batch_filename)
    return batch_filename


def _upload_batch(**context) -> None:
    from ingestion.jobs.landing.upload_batch_to_gcs import run_upload_batch

    batch_filename = context["ti"].xcom_pull(
        task_ids="detect_next_batch", key="batch_filename"
    )
    run_upload_batch(batch_filename=batch_filename, local_batches_dir=BATCHES_DIR)


def _raw_to_snowflake(**context) -> None:
    from ingestion.jobs.raw.raw_transactions import run_raw_transactions
    run_raw_transactions()


def _mark_batch_done(**context) -> None:
    """Saves the processed batch number to Airflow Variable for next run."""
    batch_filename = context["ti"].xcom_pull(
        task_ids="detect_next_batch", key="batch_filename"
    )
    batch_num = batch_filename.split("_")[1]   # e.g. '0003'
    Variable.set("paysim_last_batch", f"batch_{batch_num}")


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="paysim_batch_ingestion",
    description="PaySim batch → GCS landing → Snowflake RAW → dbt staging + trusted",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,          # manual / change to "@daily" for scheduled
    catchup=False,
    tags=["paysim", "ingestion", "dbt"],
    doc_md=__doc__,
) as dag:

    detect_next_batch = PythonOperator(
        task_id="detect_next_batch",
        python_callable=_detect_next_batch,
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_batch,
    )

    raw_to_snowflake = PythonOperator(
        task_id="raw_to_snowflake",
        python_callable=_raw_to_snowflake,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"source {PROJECT_ROOT}/.env && "
            f"cd {TRANSFORM_DIR} && "
            "dbt run --select staging --profiles-dir . --no-version-check"
        ),
    )

    dbt_trusted = BashOperator(
        task_id="dbt_trusted",
        bash_command=(
            f"source {PROJECT_ROOT}/.env && "
            f"cd {TRANSFORM_DIR} && "
            "dbt run --select dimensions facts --profiles-dir . --no-version-check"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"source {PROJECT_ROOT}/.env && "
            f"cd {TRANSFORM_DIR} && "
            "dbt snapshot --profiles-dir . --no-version-check"
        ),
    )

    mark_done = PythonOperator(
        task_id="mark_batch_done",
        python_callable=_mark_batch_done,
    )

    detect_next_batch >> upload_to_gcs >> raw_to_snowflake >> dbt_staging >> dbt_trusted >>  dbt_snapshot >> mark_done
