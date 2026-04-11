import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# ─── Add project root to path so we can import ETL ────────────
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../mvp1-data-pipeline/ingestion")
))

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

# ─── Logging setup ────────────────────────────────────────────
# Logs go to both terminal AND a file so you can review history
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/scheduler_{datetime.now().strftime('%Y%m')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# JOB DEFINITIONS
# Each function = one scheduled job

def job_run_etl():
    """
    Main ETL job — runs the full pipeline:
    MinIO (CSV files) → Transform → PostgreSQL (star schema)
    """
    log.info("=" * 50)
    log.info("JOB STARTED: ETL Pipeline")
    start = datetime.now()

    try:
        # Import and run your existing ETL
        import etl_pipeline
        etl_pipeline.run()   # make sure your ETL has a run() function
        duration = (datetime.now() - start).seconds
        log.info(f"JOB DONE: ETL completed in {duration}s")

    except Exception as e:
        log.error(f"JOB FAILED: ETL error - {e}")
        raise


def job_data_quality_check():
    """
    Quick sanity check — queries PostgreSQL to verify
    the last ETL loaded data correctly.
    Logs a warning if something looks wrong.
    """
    log.info("JOB STARTED: Data Quality Check")

    try:
        from sqlalchemy import create_engine, text

        user     = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host     = os.getenv("POSTGRES_HOST", "localhost")
        port     = os.getenv("POSTGRES_PORT", "5432")
        db       = os.getenv("POSTGRES_DB")

        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

        with engine.connect() as conn:
            # Check 1: total row count
            result = conn.execute(text("SELECT COUNT(*) FROM fact_shipments"))
            total = result.scalar()
            log.info(f"Total shipments in warehouse: {total:,}")

            # Check 2: rows loaded today
            result = conn.execute(text("""
                SELECT COUNT(*) FROM fact_shipments f
                JOIN dim_date d ON f.date_id = d.date_id
                WHERE d.full_date = CURRENT_DATE
            """))
            today = result.scalar()
            log.info(f"Shipments with today's date: {today}")

            # Check 3: null critical fields
            result = conn.execute(text("""
                SELECT COUNT(*) FROM fact_shipments
                WHERE customer_id IS NULL
                   OR driver_id IS NULL
                   OR route_id IS NULL
            """))
            nulls = result.scalar()
            if nulls > 0:
                log.warning(f"Found {nulls} rows with NULL foreign keys")
            else:
                log.info(f"No NULL foreign keys found")

            # Check 4: negative costs
            result = conn.execute(text("""
                SELECT COUNT(*) FROM fact_shipments
                WHERE cost_usd < 0
            """))
            neg = result.scalar()
            if neg > 0:
                log.warning(f"Found {neg} rows with negative cost")
            else:
                log.info(f"No negative costs found")

        log.info("JOB DONE: Data quality check passed")

    except Exception as e:
        log.error(f"JOB FAILED: Quality check error - {e}")


def job_generate_new_data():
    """
    Simulates fresh daily data arriving —
    generates a small batch of new shipment records
    and uploads them to MinIO, ready for the ETL to process.
    """
    log.info("JOB STARTED: Generate new daily data")

    try:
        sys.path.append(os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../mvp1-data-pipeline/data-generation")
        ))
        import generate_data
        import upload_to_minio

        # Generate 100 new records for today
        filepath = generate_data.generate(
            n_records=100,
            filename=f"shipments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        upload_to_minio.upload(filepath)
        log.info(f"JOB DONE: New data generated and uploaded - {filepath}")

    except Exception as e:
        log.error(f"JOB FAILED: Data generation error - {e}")


def job_heartbeat():
    """
    Lightweight job that just logs 'I am alive'
    every 10 minutes so you can confirm the scheduler
    is running without checking manually.
    """
    log.info(f"HEARTBEAT - Scheduler is alive | "
             f"Uptime check: {datetime.now().strftime('%H:%M:%S')}")


# ══════════════════════════════════════════════════════════════
# SCHEDULE CONFIGURATION
# ══════════════════════════════════════════════════════════════

def main():
    scheduler = BlockingScheduler(timezone="UTC")

    # ── Job 1: ETL runs every day at 02:00 AM UTC
    # CronTrigger = runs at specific time like a cron job
    scheduler.add_job(
        job_run_etl,
        CronTrigger(hour=2, minute=0),
        id="etl_daily",
        name="Daily ETL Pipeline",
        misfire_grace_time=3600   # if missed, still run within 1 hour
    )

    # ── Job 2: Data quality check after ETL (02:30 AM)
    scheduler.add_job(
        job_data_quality_check,
        CronTrigger(hour=2, minute=30),
        id="quality_check",
        name="Data Quality Check"
    )

    # ── Job 3: Generate new data every 6 hours (simulates live feed)
    scheduler.add_job(
        job_generate_new_data,
        IntervalTrigger(hours=6),
        id="generate_data",
        name="Generate New Shipment Data"
    )

    # ── Job 4: Heartbeat every 10 minutes (proof of life)
    scheduler.add_job(
        job_heartbeat,
        IntervalTrigger(minutes=10),
        id="heartbeat",
        name="Scheduler Heartbeat"
    )

    # Print schedule summary on startup
    log.info("=" * 60)
    log.info("LogiFlow Scheduler Started")
    log.info("=" * 60)
    for job in scheduler.get_jobs():
        next_run = getattr(job, 'next_run_time', None) or "Not scheduled yet"
        log.info(f"{job.name} - next run: {next_run}")
    log.info("=" * 60)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()