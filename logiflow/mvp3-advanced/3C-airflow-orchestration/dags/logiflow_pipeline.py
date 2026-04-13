from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# ─── Project path inside container ────────────────────────────
PROJECT = "/opt/airflow/project"
sys.path.insert(0, PROJECT)

log = logging.getLogger(__name__)

default_args = {
    "owner":          "logiflow",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}


# ══════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════

def task_generate_data(**context):
    """Generate new shipments with real/simulated weather"""
    sys.path.insert(0, f"{PROJECT}/mvp3-advanced/3B-real-data")
    from real_data_fetcher import generate_batch, save_batch

    df   = generate_batch(n=100, days_back=1)
    path = save_batch(df, output_dir=f"{PROJECT}/mvp1-data-pipeline/data-generation/data")

    log.info(f"✅ Generated {len(df)} shipments → {path}")
    log.info(f"   Delayed: {df['is_delayed'].sum()} ({df['is_delayed'].mean()*100:.1f}%)")

    # Push file path to XCom so next task can use it
    context["ti"].xcom_push(key="data_path", value=path)
    return path


def task_upload_to_minio(**context):
    """Upload generated CSV files to MinIO data lake"""
    from minio import Minio
    import glob

    client = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

    bucket = os.getenv("BUCKET_NAME", "logiflow-raw")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Created bucket: {bucket}")

    # Upload all CSVs from data-generation folder
    data_dir = f"{PROJECT}/mvp1-data-pipeline/data-generation/data"
    files    = glob.glob(f"{data_dir}/*.csv")

    for filepath in files:
        filename   = os.path.basename(filepath)
        minio_path = f"raw/{filename}"
        client.fput_object(bucket, minio_path, filepath)
        log.info(f"✅ Uploaded {filename} → {bucket}/{minio_path}")

    return f"Uploaded {len(files)} files"


def task_run_etl(**context):
    """Run ETL: MinIO → Transform → PostgreSQL"""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT}/mvp1-data-pipeline/ingestion/etl_pipeline.py"],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
        }
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"ETL failed: {result.stderr}")
    log.info("✅ ETL pipeline completed")
    return "ETL done"


def task_quality_check(**context):
    sys.path.insert(0, f"{PROJECT}/mvp2-analytics-layer/2C-data-quality")
    from quality_checks import run_all_checks

    results  = run_all_checks()
    failed   = [k for k, v in results.items() if not v]
    passed   = len(results) - len(failed)

    log.info(f"Quality checks: {passed}/{len(results)} passed")

    # Critical checks that STOP the pipeline
    critical = ["null_foreign_keys", "row_counts", "orphan_records"]
    critical_failures = [f for f in failed if f in critical]

    if critical_failures:
        raise ValueError(f"❌ Critical checks failed: {critical_failures}")

    # Non-critical: warn but continue
    if failed:
        log.warning(f"⚠️ Non-critical checks failed: {failed} — pipeline continues")

    log.info("✅ Quality check task completed")
    return f"{passed}/{len(results)} checks passed"


def task_retrain_model(**context):
    """Retrain ML model on latest warehouse data"""
    import subprocess
    result = subprocess.run(
        ["python", f"{PROJECT}/mvp3-advanced/3A-ml-prediction/train.py"],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
        }
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"Training failed: {result.stderr}")
    log.info("✅ Model retrained successfully")
    return "Model retrained"


def task_pipeline_summary(**context):
    """Log final pipeline summary"""
    ti = context["ti"]

    log.info("=" * 55)
    log.info("🚚 LogiFlow Pipeline Summary")
    log.info(f"   Run date : {context['ds']}")
    log.info(f"   Data path: {ti.xcom_pull(task_ids='generate_data', key='data_path')}")
    log.info("   Status   : ✅ All tasks completed successfully")
    log.info("=" * 55)


# ══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id="logiflow_daily_pipeline",
    default_args=default_args,
    description="LogiFlow — Daily data pipeline: generate → upload → ETL → quality → retrain",
    schedule_interval="0 2 * * *",   # Every day at 2AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["logiflow", "etl", "ml", "production"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        provide_context=True,
        doc_md="Generates 100 new shipments with real/simulated weather"
    )

    upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=task_upload_to_minio,
        provide_context=True,
        doc_md="Uploads generated CSVs to MinIO data lake"
    )

    etl = PythonOperator(
        task_id="run_etl",
        python_callable=task_run_etl,
        provide_context=True,
        doc_md="Runs ETL pipeline: MinIO → Transform → PostgreSQL"
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
        provide_context=True,
        doc_md="Runs 8 data quality checks on the warehouse"
    )

    retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=task_retrain_model,
        provide_context=True,
        doc_md="Retrains ML delay prediction model on latest data"
    )

    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        provide_context=True,
        doc_md="Logs final pipeline summary"
    )

    end = EmptyOperator(task_id="end")

    # ── Pipeline flow ─────────────────────────────────────────
    start >> generate >> upload >> etl >> quality >> retrain >> summary >> end