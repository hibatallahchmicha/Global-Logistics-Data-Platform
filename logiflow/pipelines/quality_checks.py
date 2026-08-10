"""
pipelines/quality_checks.py

Post-load validation of the warehouse. 8 checks, split into critical
(pipeline should stop) and non-critical (log and continue) -- the
CRITICAL_CHECKS set gets used by Module 11's DAG.

Depends on: common.config (1). Run after pipelines/etl.py (5).
"""

import logging

from sqlalchemy import create_engine, text

from common.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CRITICAL_CHECKS = {"row_counts", "null_foreign_keys", "orphan_records"}


def check_row_counts(conn) -> bool:
    tables = ["fact_shipments", "dim_customer", "dim_driver", "dim_vehicle", "dim_route", "dim_date"]
    ok = True
    for t in tables:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
        log.info("%s %s: %d rows", "PASS" if n > 0 else "FAIL", t, n)
        ok &= n > 0
    return ok


def check_null_foreign_keys(conn) -> bool:
    n = conn.execute(text("""
        SELECT COUNT(*) FROM fact_shipments
        WHERE customer_id IS NULL OR driver_id IS NULL OR vehicle_id IS NULL
           OR route_id IS NULL OR date_id IS NULL
    """)).scalar()
    log.info("%s null_foreign_keys: %d rows", "PASS" if n == 0 else "FAIL", n)
    return n == 0


def check_invalid_status(conn) -> bool:
    rows = conn.execute(text("""
        SELECT status, COUNT(*) FROM fact_shipments
        WHERE status NOT IN ('on_time','delayed','failed') GROUP BY status
    """)).fetchall()
    log.info("%s invalid_status: %d bad value(s)", "PASS" if not rows else "FAIL", len(rows))
    return not rows


def check_negative_values(conn) -> bool:
    ok = True
    for col in ["cost_usd", "weight_kg", "distance_km", "fuel_consumed_liters"]:
        n = conn.execute(text(f"SELECT COUNT(*) FROM fact_shipments WHERE {col} < 0")).scalar()
        log.info("%s negative_values(%s): %d rows", "PASS" if n == 0 else "FAIL", col, n)
        ok &= n == 0
    return ok


def check_delay_consistency(conn) -> bool:
    n = conn.execute(text("""
        SELECT COUNT(*) FROM fact_shipments
        WHERE (is_delayed = TRUE AND delay_minutes <= 30)
           OR (is_delayed = FALSE AND delay_minutes > 30)
    """)).scalar()
    log.info("%s delay_consistency: %d mismatched rows", "PASS" if n == 0 else "FAIL", n)
    return n == 0


def check_driver_ratings(conn) -> bool:
    n = conn.execute(text("SELECT COUNT(*) FROM dim_driver WHERE rating < 1.0 OR rating > 5.0")).scalar()
    log.info("%s driver_ratings: %d out-of-range", "PASS" if n == 0 else "FAIL", n)
    return n == 0


def check_orphan_records(conn) -> bool:
    checks = {
        "customer_id": "dim_customer c ON f.customer_id = c.customer_id WHERE c.customer_id IS NULL",
        "driver_id":   "dim_driver d ON f.driver_id = d.driver_id WHERE d.driver_id IS NULL",
        "vehicle_id":  "dim_vehicle v ON f.vehicle_id = v.vehicle_id WHERE v.vehicle_id IS NULL",
        "route_id":    "dim_route r ON f.route_id = r.route_id WHERE r.route_id IS NULL",
    }
    ok = True
    for fk, join in checks.items():
        n = conn.execute(text(f"SELECT COUNT(*) FROM fact_shipments f LEFT JOIN {join}")).scalar()
        log.info("%s orphan_records(%s): %d rows", "PASS" if n == 0 else "FAIL", fk, n)
        ok &= n == 0
    return ok


def check_duplicate_shipments(conn) -> bool:
    """Business-logic duplicate check, distinct from the DB-level UNIQUE
    constraint on source_shipment_id (technical dupes are impossible by
    construction). This catches two different shipments that shouldn't
    both exist: same customer, same exact pickup timestamp."""
    n = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT customer_id, scheduled_pickup, COUNT(*) c
            FROM fact_shipments GROUP BY customer_id, scheduled_pickup HAVING COUNT(*) > 1
        ) dupes
    """)).scalar()
    log.info("%s duplicate_shipments: %d dupe combo(s)", "PASS" if n == 0 else "FAIL", n)
    return n == 0


def run_all_checks() -> dict:
    engine = create_engine(settings.database_url)
    checks = {
        "row_counts": check_row_counts,
        "null_foreign_keys": check_null_foreign_keys,
        "invalid_status": check_invalid_status,
        "negative_values": check_negative_values,
        "delay_consistency": check_delay_consistency,
        "driver_ratings": check_driver_ratings,
        "orphan_records": check_orphan_records,
        "duplicate_shipments": check_duplicate_shipments,
    }
    results = {}
    with engine.connect() as conn:
        for name, fn in checks.items():
            results[name] = fn(conn)

    passed = sum(results.values())
    log.info("RESULT: %d/%d checks passed", passed, len(results))
    failed_critical = [k for k, v in results.items() if not v and k in CRITICAL_CHECKS]
    if failed_critical:
        log.error("CRITICAL checks failed: %s", failed_critical)
    return results


if __name__ == "__main__":
    run_all_checks()