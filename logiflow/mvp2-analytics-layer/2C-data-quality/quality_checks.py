import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Works both locally and inside Airflow container
load_dotenv()  # reads from environment variables already set by Docker

os.makedirs("/opt/airflow/logs/quality", exist_ok=True) if os.path.exists("/opt/airflow") else os.makedirs("logs", exist_ok=True)

LOG_DIR = "/opt/airflow/logs/quality" if os.path.exists("/opt/airflow") else "logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/quality_{datetime.now().strftime('%Y%m')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )


# ══════════════════════════════════════════════════════════════
# CHECKS
# ══════════════════════════════════════════════════════════════

def check_row_counts(conn):
    """Every table should have data"""
    log.info("── Check 1: Row Counts ──────────────────────")
    tables = ["fact_shipments","dim_customer","dim_driver","dim_vehicle","dim_route","dim_date"]
    passed = True
    for t in tables:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
        status = "PASS" if n > 0 else "FAIL"
        log.info(f"   {status} {t}: {n:,} rows")
        if n == 0:
            passed = False
    return passed


def check_null_foreign_keys(conn):
    """Fact table should have no NULL foreign keys"""
    log.info("── Check 2: NULL Foreign Keys ───────────────")
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_shipments
        WHERE customer_id IS NULL
           OR driver_id   IS NULL
           OR vehicle_id  IS NULL
           OR route_id    IS NULL
           OR date_id     IS NULL
    """)).scalar()
    if result == 0:
        log.info("   PASS No NULL foreign keys")
        return True
    log.warning(f"   FAIL {result} rows with NULL foreign keys")
    return False


def check_invalid_status(conn):
    """Status must be one of the 3 valid values"""
    log.info("── Check 3: Invalid Status Values ───────────")
    result = conn.execute(text("""
        SELECT status, COUNT(*) as cnt
        FROM fact_shipments
        WHERE status NOT IN ('on_time','delayed','failed')
        GROUP BY status
    """)).fetchall()
    if not result:
        log.info("   PASS All status values are valid")
        return True
    for row in result:
        log.warning(f"   FAIL Invalid status '{row[0]}': {row[1]} rows")
    return False


def check_negative_values(conn):
    """Costs, weights and distances must be positive"""
    log.info("── Check 4: Negative Numeric Values ─────────")
    checks = {
        "cost_usd":               "cost_usd < 0",
        "weight_kg":              "weight_kg < 0",
        "distance_km":            "distance_km < 0",
        "fuel_consumed_liters":   "fuel_consumed_liters < 0",
    }
    passed = True
    for label, condition in checks.items():
        n = conn.execute(text(
            f"SELECT COUNT(*) FROM fact_shipments WHERE {condition}"
        )).scalar()
        if n == 0:
            log.info(f"   PASS {label}: no negatives")
        else:
            log.warning(f"   FAIL {label}: {n} negative values")
            passed = False
    return passed


def check_delay_consistency(conn):
    """is_delayed flag must match delay_minutes > 30"""
    log.info("── Check 5: Delay Flag Consistency ──────────")
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_shipments
        WHERE (is_delayed = TRUE  AND delay_minutes <= 30)
           OR (is_delayed = FALSE AND delay_minutes >  30)
    """)).scalar()
    if result == 0:
        log.info("   PASS is_delayed flag is consistent")
        return True
    log.warning(f"   FAIL {result} rows where flag doesn't match delay_minutes")
    return False


def check_driver_ratings(conn):
    """Driver ratings must be between 1.0 and 5.0"""
    log.info("── Check 6: Driver Ratings Range ────────────")
    result = conn.execute(text("""
        SELECT COUNT(*) FROM dim_driver
        WHERE rating < 1.0 OR rating > 5.0
    """)).scalar()
    if result == 0:
        log.info("   PASS All driver ratings are in range [1.0 – 5.0]")
        return True
    log.warning(f"   FAIL {result} drivers with out-of-range rating")
    return False


def check_orphan_records(conn):
    """Every FK in fact table must exist in its dimension"""
    log.info("── Check 7: Orphan Records ───────────────────")
    orphan_queries = {
        "customer_id": "SELECT COUNT(*) FROM fact_shipments f LEFT JOIN dim_customer c ON f.customer_id=c.customer_id WHERE c.customer_id IS NULL",
        "driver_id":   "SELECT COUNT(*) FROM fact_shipments f LEFT JOIN dim_driver   d ON f.driver_id=d.driver_id     WHERE d.driver_id IS NULL",
        "vehicle_id":  "SELECT COUNT(*) FROM fact_shipments f LEFT JOIN dim_vehicle  v ON f.vehicle_id=v.vehicle_id   WHERE v.vehicle_id IS NULL",
        "route_id":    "SELECT COUNT(*) FROM fact_shipments f LEFT JOIN dim_route    r ON f.route_id=r.route_id       WHERE r.route_id IS NULL",
    }
    passed = True
    for fk, q in orphan_queries.items():
        n = conn.execute(text(q)).scalar()
        if n == 0:
            log.info(f"   PASS {fk}: no orphans")
        else:
            log.warning(f"   FAIL {fk}: {n} orphan records")
            passed = False
    return passed


def check_duplicate_shipments(conn):
    """No two shipments should share the same scheduled_pickup + customer"""
    log.info("── Check 8: Duplicate Shipments ─────────────")
    result = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT customer_id, scheduled_pickup, COUNT(*) as cnt
            FROM fact_shipments
            GROUP BY customer_id, scheduled_pickup
            HAVING COUNT(*) > 1
        ) dupes
    """)).scalar()
    if result == 0:
        log.info("   PASS No duplicate shipments found")
        return True
    log.warning(f"   FAIL {result} duplicate (customer, pickup_time) combinations")
    return False


# ══════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════

def run_all_checks():
    log.info("=" * 55)
    log.info("LogiFlow - Data Quality Report")
    log.info(f"   Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    engine = get_engine()
    results = {}

    with engine.connect() as conn:
        results["row_counts"]          = check_row_counts(conn)
        results["null_foreign_keys"]   = check_null_foreign_keys(conn)
        results["invalid_status"]      = check_invalid_status(conn)
        results["negative_values"]     = check_negative_values(conn)
        results["delay_consistency"]   = check_delay_consistency(conn)
        results["driver_ratings"]      = check_driver_ratings(conn)
        results["orphan_records"]      = check_orphan_records(conn)
        results["duplicates"]          = check_duplicate_shipments(conn)

    # ── Summary ───────────────────────────────────────────────
    passed = sum(results.values())
    total  = len(results)
    log.info("=" * 55)
    log.info(f"RESULT: {passed}/{total} checks passed")
    for name, ok in results.items():
        icon = "PASS" if ok else "FAIL"
        log.info(f"   {icon} {name}")
    log.info("=" * 55)

    return results


if __name__ == "__main__":
    run_all_checks()