"""
orchestration/dags/logiflow_pipeline.py

Daily pipeline: generate -> ETL -> quality check -> retrain.

Every task is a thin wrapper around an already-tested pipeline module
-- no business logic lives in this file. Imports are deferred inside
each task function (not at module level) so Airflow's frequent DAG-file
re-parsing stays fast and doesn't need xgboost/sklearn just to render
the DAG graph.

Depends on: pipelines/generate_shipments.py (4), pipelines/etl.py (5),
pipelines/quality_checks.py (6), ml/train.py (7). Requires the whole
repo root mounted into the container and on sys.path (see Module 14).
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT = "/opt/airflow/project"
sys.path.insert(0, PROJECT)

default_args = {
    "owner": "logiflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def task_generate(**context):
    from pipelines.generate_shipments import run
    object_key = run(n=100, days_back=1)
    context["ti"].xcom_push(key="object_key", value=object_key)
    return object_key


def task_etl(**context):
    from pipelines.etl import run
    run()
    return "ETL done"


def task_quality_check(**context):
    from pipelines.quality_checks import CRITICAL_CHECKS, run_all_checks
    results = run_all_checks()
    failed = [k for k, v in results.items() if not v]
    critical_failures = [f for f in failed if f in CRITICAL_CHECKS]
    if critical_failures:
        raise ValueError(f"Critical checks failed: {critical_failures}")
    if failed:
        print(f"Non-critical checks failed: {failed} -- continuing")
    return f"{len(results) - len(failed)}/{len(results)} checks passed"


def task_retrain(**context):
    from ml.train import main
    main()
    return "Model retrained"


with DAG(
    dag_id="logiflow_daily_pipeline",
    default_args=default_args,
    description="LogiFlow -- daily: generate -> ETL -> quality check -> retrain",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["logiflow"],
) as dag:

    generate = PythonOperator(task_id="generate_shipments", python_callable=task_generate)
    etl      = PythonOperator(task_id="run_etl", python_callable=task_etl)
    quality  = PythonOperator(task_id="quality_check", python_callable=task_quality_check)
    retrain  = PythonOperator(task_id="retrain_model", python_callable=task_retrain)

    generate >> etl >> quality >> retrain