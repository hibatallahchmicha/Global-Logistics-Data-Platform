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
    """Generate new shipments with real/simulated weather.
    Deletes all previous real_shipments_*.csv files before saving the new batch.
    """
    import glob
    sys.path.insert(0, f"{PROJECT}/mvp3-advanced/3B-real-data")
    from real_data_fetcher import generate_batch, save_batch

    data_dir = f"{PROJECT}/mvp1-data-pipeline/data-generation/data"

    # Delete old generated CSVs so only the latest batch is present
    old_files = glob.glob(f"{data_dir}/real_shipments_*.csv")
    for old_f in old_files:
        os.remove(old_f)
        log.info(f"🗑️  Deleted old CSV: {old_f}")
    log.info(f"Removed {len(old_files)} old file(s)")

    df   = generate_batch(n=100, days_back=1)
    path = save_batch(df, output_dir=data_dir)

    log.info(f"✅ Generated {len(df)} shipments → {path}")
    log.info(f"   Delayed: {df['is_delayed'].sum()} ({df['is_delayed'].mean()*100:.1f}%)")

    # Push file path to XCom so next task can use it
    context["ti"].xcom_push(key="data_path", value=path)
    return path


def task_upload_to_minio(**context):
    """Split the flat real_shipments CSV into the 4 dimension-structured files
    (customers, drivers, vehicles, shipments) expected by etl_pipeline.py,
    then upload them to MinIO under raw/.
    """
    import glob
    import io as _io
    import pandas as pd
    from minio import Minio

    # ── MinIO client ──────────────────────────────────────────────────────────
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    bucket = os.getenv("BUCKET_NAME", "logiflow-raw")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Created bucket: {bucket}")

    # ── Load all generated CSVs ───────────────────────────────────────────────
    data_dir = f"{PROJECT}/mvp1-data-pipeline/data-generation/data"
    files    = glob.glob(f"{data_dir}/real_shipments_*.csv")
    if not files:
        raise FileNotFoundError("No real_shipments_*.csv found in data-generation/data")

    flat = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    log.info(f"Loaded {len(flat)} rows from {len(files)} file(s)")

    # City → country lookup (matches cities in real_data_fetcher.py)
    CITY_COUNTRY = {
        "Paris": "France", "Berlin": "Germany", "Madrid": "Spain",
        "Rome": "Italy", "Amsterdam": "Netherlands", "Casablanca": "Morocco",
        "Tunis": "Tunisia", "Cairo": "Egypt", "Dubai": "UAE",
        "Istanbul": "Turkey", "London": "UK", "Brussels": "Belgium",
        "Vienna": "Austria", "Warsaw": "Poland", "Lisbon": "Portugal",
    }

    # ── 1. dim_customers ─────────────────────────────────────────────────────
    custs = (
        flat[["company_name", "industry", "customer_country", "customer_city", "segment", "contract_type"]]
        .drop_duplicates(subset=["company_name"])
        .reset_index(drop=True)
    )
    custs.insert(0, "customer_id", custs.index + 1)
    custs = custs.rename(columns={"customer_country": "country", "customer_city": "city"})
    cust_map = dict(zip(custs["company_name"], custs["customer_id"]))

    # ── 2. dim_drivers ───────────────────────────────────────────────────────
    drvs = (
        flat[["driver_name", "license_type", "experience_years", "driver_rating", "driver_country"]]
        .drop_duplicates(subset=["driver_name"])
        .reset_index(drop=True)
    )
    drvs.insert(0, "driver_id", drvs.index + 1)
    drvs = drvs.rename(columns={"driver_name": "full_name", "driver_rating": "rating", "driver_country": "country"})
    drvs["is_active"] = True
    drv_map = dict(zip(drvs["full_name"], drvs["driver_id"]))

    # ── 3. dim_vehicles ──────────────────────────────────────────────────────
    vehs = (
        flat[["plate_number", "vehicle_type", "capacity_kg", "manufacture_year", "mileage_km"]]
        .drop_duplicates(subset=["plate_number"])
        .reset_index(drop=True)
    )
    vehs.insert(0, "vehicle_id", vehs.index + 1)
    vehs["last_service_date"] = None
    vehs["is_active"] = True
    veh_map = dict(zip(vehs["plate_number"], vehs["vehicle_id"]))

    # ── 4. fact_shipments ────────────────────────────────────────────────────
    ship = flat.copy()
    ship["shipment_id"]           = range(1, len(ship) + 1)
    ship["customer_id"]           = ship["company_name"].map(cust_map)
    ship["driver_id"]             = ship["driver_name"].map(drv_map)
    ship["vehicle_id"]            = ship["plate_number"].map(veh_map)
    ship["route_origin_city"]     = ship["origin_city"]
    ship["route_origin_country"]  = ship["origin_city"].map(CITY_COUNTRY).fillna("Unknown")
    ship["route_dest_city"]       = ship["destination_city"]
    ship["route_dest_country"]    = ship["destination_city"].map(CITY_COUNTRY).fillna("Unknown")

    ship_cols = [
        "shipment_id", "customer_id", "driver_id", "vehicle_id",
        "route_origin_city", "route_origin_country", "route_dest_city", "route_dest_country",
        "distance_km", "region", "route_type",
        "scheduled_pickup", "actual_pickup", "scheduled_delivery", "actual_delivery",
        "planned_duration_hrs", "actual_duration_hrs", "delay_minutes", "is_delayed", "status",
        "weight_kg", "cost_usd", "fuel_consumed_liters",
        "weather_condition", "temperature_celsius", "wind_speed_kmh",
    ]
    ship = ship[ship_cols]

    # ── Upload helper ─────────────────────────────────────────────────────────
    def upload_df(df: pd.DataFrame, minio_path: str) -> None:
        buf  = _io.BytesIO()
        df.to_csv(buf, index=False)
        data = buf.getvalue()
        client.put_object(bucket, minio_path, _io.BytesIO(data), len(data), content_type="text/csv")
        log.info(f"✅ Uploaded {len(df)} rows → {bucket}/{minio_path}")

    upload_df(custs, "raw/customers.csv")
    upload_df(drvs,  "raw/drivers.csv")
    upload_df(vehs,  "raw/vehicles.csv")
    upload_df(ship,  "raw/shipments.csv")

    return f"Uploaded 4 files ({len(flat)} shipments total)"


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