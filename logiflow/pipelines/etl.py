"""
pipelines/etl.py

Extract -> Transform -> Load: reads every generator-produced shipment
CSV from MinIO, splits it into the star schema, and loads it into
Postgres using ON CONFLICT upserts. Safe to re-run any number of times
on any number of files -- nothing ever gets duplicated or wiped.

Depends on: common.config (1), common.storage (2), infra/schema.sql (3),
pipelines/generate_shipments.py (4) for input.
"""

import io
import logging

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from common.config import settings
from common.storage import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def extract() -> pd.DataFrame:
    keys = [k for k in storage.list_objects("raw/shipments_") if k.endswith(".csv")]
    if not keys:
        raise FileNotFoundError("No raw/shipments_*.csv in MinIO -- run the generator first")
    log.info("Found %d shipment file(s)", len(keys))
    frames = []
    for k in keys:
        f = pd.read_csv(io.BytesIO(storage.download_bytes(k)))
        f["source_file"] = k
        frames.append(f)
    df = pd.concat(frames, ignore_index=True)
    log.info("Extracted %d shipment rows total", len(df))
    return df


def _upsert_dimension(cur, table, natural_col, insert_cols, rows) -> dict:
    if not rows:
        return {}
    id_col = f"{table.replace('dim_', '')}_id"
    cols_sql = ", ".join(insert_cols)
    execute_values(
        cur,
        f"INSERT INTO {table} ({cols_sql}) VALUES %s ON CONFLICT ({natural_col}) DO NOTHING",
        rows,
    )
    natural_values = [r[insert_cols.index(natural_col)] for r in rows]
    cur.execute(
        f"SELECT {natural_col}, {id_col} FROM {table} WHERE {natural_col} = ANY(%s)",
        (natural_values,),
    )
    return dict(cur.fetchall())


def load(df: pd.DataFrame) -> None:
    conn = psycopg2.connect(settings.database_url)
    cur = conn.cursor()
    try:
        customers = df[["company_name", "industry", "customer_country", "customer_city",
                         "segment", "contract_type"]].drop_duplicates(subset=["company_name"])
        customer_map = _upsert_dimension(
            cur, "dim_customer", "company_name",
            ["company_name", "industry", "country", "city", "segment", "contract_type"],
            list(customers.itertuples(index=False, name=None)),
        )

        drivers = df[["driver_name", "license_type", "experience_years", "driver_rating",
                       "driver_country"]].drop_duplicates(subset=["driver_name"])
        driver_map = _upsert_dimension(
            cur, "dim_driver", "full_name",
            ["full_name", "license_type", "experience_years", "rating", "country"],
            list(drivers.itertuples(index=False, name=None)),
        )

        vehicles = df[["plate_number", "vehicle_type", "capacity_kg", "manufacture_year",
                        "mileage_km"]].drop_duplicates(subset=["plate_number"])
        vehicle_map = _upsert_dimension(
            cur, "dim_vehicle", "plate_number",
            ["plate_number", "vehicle_type", "capacity_kg", "manufacture_year", "mileage_km"],
            list(vehicles.itertuples(index=False, name=None)),
        )

        routes = df[["origin_city", "origin_country", "destination_city", "destination_country",
                      "distance_km", "region", "route_type"]].drop_duplicates(
            subset=["origin_city", "origin_country", "destination_city", "destination_country"])
        execute_values(
            cur,
            """INSERT INTO dim_route (origin_city, origin_country, destination_city,
                   destination_country, distance_km, region, route_type)
               VALUES %s
               ON CONFLICT (origin_city, origin_country, destination_city, destination_country)
               DO NOTHING""",
            list(routes.itertuples(index=False, name=None)),
        )
        cur.execute("SELECT origin_city, origin_country, destination_city, destination_country, route_id FROM dim_route")
        route_map = {(r[0], r[1], r[2], r[3]): r[4] for r in cur.fetchall()}

        dates = pd.to_datetime(df["scheduled_pickup"]).dt.date.unique()
        date_rows = []
        for d in dates:
            ts = pd.Timestamp(d)
            date_rows.append((d, ts.day, ts.month, ts.strftime("%B"), ts.quarter,
                               ts.year, ts.strftime("%A"), ts.dayofweek >= 5))
        execute_values(
            cur,
            """INSERT INTO dim_date (full_date, day, month, month_name, quarter, year, weekday, is_weekend)
               VALUES %s ON CONFLICT (full_date) DO NOTHING""",
            date_rows,
        )
        cur.execute("SELECT full_date, date_id FROM dim_date")
        date_map = {r[0]: r[1] for r in cur.fetchall()}

        fact_rows = []
        for row in df.itertuples(index=False):
            pickup_date = pd.Timestamp(row.scheduled_pickup).date()
            fact_rows.append((
                row.source_shipment_id,
                customer_map.get(row.company_name),
                driver_map.get(row.driver_name),
                vehicle_map.get(row.plate_number),
                route_map.get((row.origin_city, row.origin_country, row.destination_city, row.destination_country)),
                date_map.get(pickup_date),
                row.source_file,
                row.planned_duration_hrs, row.actual_duration_hrs, row.delay_minutes,
                row.distance_km, row.weight_kg, row.cost_usd, row.fuel_consumed_liters,
                row.weather_condition, row.temperature_celsius, row.wind_speed_kmh,
                row.traffic_congestion_ratio, row.traffic_condition,
                row.status, row.is_delayed,
                row.scheduled_pickup, row.actual_pickup, row.scheduled_delivery, row.actual_delivery,
            ))

        execute_values(
            cur,
            """INSERT INTO fact_shipments (
                   source_shipment_id, customer_id, driver_id, vehicle_id, route_id, date_id, raw_file,
                   planned_duration_hrs, actual_duration_hrs, delay_minutes, distance_km, weight_kg,
                   cost_usd, fuel_consumed_liters, weather_condition, temperature_celsius, wind_speed_kmh,
                   traffic_congestion_ratio, traffic_condition, status, is_delayed,
                   scheduled_pickup, actual_pickup, scheduled_delivery, actual_delivery
               ) VALUES %s
               ON CONFLICT (source_shipment_id) DO NOTHING""",
            fact_rows,
            page_size=1000,
        )
        conn.commit()
        log.info("Processed %d shipment rows (duplicates auto-skipped)", len(fact_rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def run() -> None:
    load(extract())


if __name__ == "__main__":
    run()