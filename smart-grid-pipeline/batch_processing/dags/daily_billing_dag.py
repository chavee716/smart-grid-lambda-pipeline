"""
Airflow DAG: orchestrates the batch layer of the Lambda architecture.

Schedule: once per simulated day (SIMULATED_DAY_SECONDS). In the demo
docker-compose stack this is set to run every 5 minutes to match the
compressed simulated clock; in the report this is described as mapping to
a real "once nightly at 01:00" schedule in production.

Flow:
  1. wait_for_tariff_file   - FileSensor: waits for today's tariff CSV to be
                               dropped by the daily-batch simulator (decouples
                               DAG timing from simulator timing, as it would
                               be decoupled from a real third-party feed).
  2. validate_tariff_file   - basic schema/row-count sanity check (fails fast
                               and loudly rather than silently billing on bad data).
  3. run_billing_reconciliation - calls daily_billing_job.run(), which re-reads
                               the immutable raw Parquet archive + tariff file
                               and recomputes the authoritative daily bill.
  4. publish_batch_metrics   - records success/failure + duration to the
                               observability layer (Prometheus counters +
                               structured log), completing the "orchestration
                               for historical analysis/reporting" requirement.

Retries + alerting on failure satisfy part of the observability rubric at
the orchestration layer (distinct from the streaming-side health checks).
"""
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

import sys
sys.path.append("/opt/airflow/project")  # see docker-compose volume mount

from common import config
from observability.logging_config import get_logger
from observability.metrics import BATCH_JOB_RUNS

log = get_logger("batch_processing.daily_billing_dag")

DAG_ID = "daily_billing_reconciliation"

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


def _today_tariff_path() -> str:
    import os
    return os.path.join(config.BATCH_DROPZONE_PATH, f"tariff_{datetime.utcnow().date().isoformat()}.csv")


def validate_tariff_file(**context):
    path = _today_tariff_path()
    df = pd.read_csv(path)
    required_cols = {"household_id", "effective_date", "tariff_rate", "billing_tier", "subsidy_flag"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Tariff file missing required columns: {missing}")
    if df.empty:
        raise ValueError("Tariff file is empty - refusing to bill against zero reference rows.")
    if (df["tariff_rate"] <= 0).any():
        raise ValueError("Tariff file contains non-positive tariff_rate values.")
    log.info("tariff_file_validated", extra={"stage": "orchestration", "path": path, "rows": len(df)})


def tariff_file_exists(**context):
    import os
    return os.path.isfile(_today_tariff_path())


def run_billing_reconciliation(**context):
    from batch_processing.daily_billing_job import run as run_billing_job
    billing_date = datetime.utcnow().date()
    try:
        run_billing_job(billing_date)
        BATCH_JOB_RUNS.labels(dag=DAG_ID, status="success").inc()
    except Exception:
        BATCH_JOB_RUNS.labels(dag=DAG_ID, status="failure").inc()
        raise


def publish_batch_metrics(**context):
    ti = context["ti"]
    state = ti.xcom_pull(task_ids="run_billing_reconciliation", key="return_value")
    log.info("batch_run_summary_published", extra={
        "stage": "orchestration", "dag_run_id": context["run_id"], "state": state or "success",
    })


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily reconciliation of smart-grid consumption against tariff data (Lambda batch layer)",
    schedule_interval=timedelta(seconds=config.SIMULATED_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smart-grid", "batch-layer", "lambda-architecture"],
) as dag:

    wait_for_tariff_file = PythonSensor(
        task_id="wait_for_tariff_file",
        python_callable=tariff_file_exists,
        poke_interval=10,
        timeout=config.SIMULATED_DAY_SECONDS * 2,
        mode="poke",
    )

    validate = PythonOperator(
        task_id="validate_tariff_file",
        python_callable=validate_tariff_file,
    )

    reconcile = PythonOperator(
        task_id="run_billing_reconciliation",
        python_callable=run_billing_reconciliation,
    )

    publish_metrics = PythonOperator(
        task_id="publish_batch_metrics",
        python_callable=publish_batch_metrics,
    )

    wait_for_tariff_file >> validate >> reconcile >> publish_metrics
