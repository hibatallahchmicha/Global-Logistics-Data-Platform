from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import sys
import os

# Works both locally and inside Docker (reads env already injected by compose)
load_dotenv()

# Add ML prediction module to path — works from any working directory
ML_DIR = Path(__file__).resolve().parents[2] / "mvp3-advanced" / "3A-ml-prediction"
sys.path.insert(0, str(ML_DIR))

app = FastAPI(
    title="LogiFlow API",
    description="REST API for the LogiFlow logistics data warehouse",
    version="1.0.0"
)


def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )


def query_to_list(sql: str, params: dict = {}):
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


# ══════════════════════════════════════════════════════════════
# ROOT
# ══════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {
        "project": "LogiFlow",
        "version": "1.0.0",
        "status":  "running",
        "docs":    "/docs"
    }


# ══════════════════════════════════════════════════════════════
# SHIPMENTS
# ══════════════════════════════════════════════════════════════

@app.get("/shipments")
def get_shipments(
    limit:  int = Query(50,  ge=1, le=500),
    offset: int = Query(0,   ge=0),
    status: str = Query(None, description="on_time | delayed | failed")
):
    where = "WHERE f.status = :status" if status else ""
    sql = f"""
        SELECT
            f.shipment_id, f.status, f.is_delayed, f.delay_minutes,
            f.cost_usd, f.weight_kg, f.distance_km,
            d.full_date, d.month_name, d.year,
            c.company_name, c.segment,
            dr.full_name AS driver_name, dr.rating AS driver_rating,
            v.vehicle_type,
            r.origin_city, r.destination_city, r.region, r.route_type
        FROM fact_shipments f
        JOIN dim_date     d  ON f.date_id     = d.date_id
        JOIN dim_customer c  ON f.customer_id = c.customer_id
        JOIN dim_driver   dr ON f.driver_id   = dr.driver_id
        JOIN dim_vehicle  v  ON f.vehicle_id  = v.vehicle_id
        JOIN dim_route    r  ON f.route_id    = r.route_id
        {where}
        ORDER BY f.shipment_id
        LIMIT :limit OFFSET :offset
    """
    params = {"limit": limit, "offset": offset}
    if status:
        params["status"] = status
    return query_to_list(sql, params)


@app.get("/shipments/{shipment_id}")
def get_shipment(shipment_id: int):
    sql = """
        SELECT f.*, d.full_date, c.company_name, c.segment,
               dr.full_name, v.vehicle_type,
               r.origin_city, r.destination_city, r.region
        FROM fact_shipments f
        JOIN dim_date     d  ON f.date_id     = d.date_id
        JOIN dim_customer c  ON f.customer_id = c.customer_id
        JOIN dim_driver   dr ON f.driver_id   = dr.driver_id
        JOIN dim_vehicle  v  ON f.vehicle_id  = v.vehicle_id
        JOIN dim_route    r  ON f.route_id    = r.route_id
        WHERE f.shipment_id = :id
    """
    rows = query_to_list(sql, {"id": shipment_id})
    if not rows:
        raise HTTPException(status_code=404, detail="Shipment not found")
    return rows[0]


# ══════════════════════════════════════════════════════════════
# KPIs
# ══════════════════════════════════════════════════════════════

@app.get("/kpis/summary")
def get_kpi_summary():
    sql = """
        SELECT
            COUNT(*)                                        AS total_shipments,
            ROUND(AVG(CASE WHEN status='on_time' THEN 1.0 ELSE 0.0 END)*100, 2)
                                                            AS on_time_rate_pct,
            ROUND(AVG(CASE WHEN is_delayed THEN delay_minutes END), 1)
                                                            AS avg_delay_minutes,
            ROUND(SUM(cost_usd)::numeric, 2)               AS total_revenue_usd,
            ROUND(AVG(cost_usd)::numeric, 2)               AS avg_cost_usd,
            ROUND(AVG(CASE WHEN status='failed' THEN 1.0 ELSE 0.0 END)*100, 2)
                                                            AS failure_rate_pct,
            ROUND(AVG(fuel_consumed_liters)::numeric, 2)   AS avg_fuel_liters
        FROM fact_shipments
    """
    return query_to_list(sql)[0]


@app.get("/kpis/by-month")
def get_kpis_by_month():
    sql = """
        SELECT
            d.year, d.month, d.month_name,
            COUNT(*)                                                      AS shipments,
            ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2)
                                                                          AS on_time_pct,
            ROUND(SUM(f.cost_usd)::numeric, 2)                           AS revenue_usd,
            ROUND(AVG(f.delay_minutes)::numeric, 1)                      AS avg_delay_min
        FROM fact_shipments f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """
    return query_to_list(sql)


@app.get("/kpis/by-region")
def get_kpis_by_region():
    sql = """
        SELECT
            r.region,
            COUNT(*)                                                      AS shipments,
            ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2)
                                                                          AS on_time_pct,
            ROUND(AVG(f.cost_usd)::numeric, 2)                           AS avg_cost_usd,
            ROUND(AVG(f.delay_minutes)::numeric, 1)                      AS avg_delay_min
        FROM fact_shipments f
        JOIN dim_route r ON f.route_id = r.route_id
        GROUP BY r.region
        ORDER BY shipments DESC
    """
    return query_to_list(sql)


# ══════════════════════════════════════════════════════════════
# DRIVERS
# ══════════════════════════════════════════════════════════════

@app.get("/drivers")
def get_drivers():
    sql = """
        SELECT
            dr.driver_id, dr.full_name, dr.license_type,
            dr.experience_years, dr.rating, dr.country,
            COUNT(f.shipment_id)                                          AS total_shipments,
            ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2)
                                                                          AS on_time_pct,
            ROUND(AVG(f.delay_minutes)::numeric, 1)                      AS avg_delay_min
        FROM dim_driver dr
        LEFT JOIN fact_shipments f ON dr.driver_id = f.driver_id
        GROUP BY dr.driver_id, dr.full_name, dr.license_type,
                 dr.experience_years, dr.rating, dr.country
        ORDER BY on_time_pct DESC NULLS LAST
    """
    return query_to_list(sql)


@app.get("/drivers/{driver_id}")
def get_driver(driver_id: int):
    sql = """
        SELECT dr.*, COUNT(f.shipment_id) AS total_shipments,
               ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100,2) AS on_time_pct
        FROM dim_driver dr
        LEFT JOIN fact_shipments f ON dr.driver_id = f.driver_id
        WHERE dr.driver_id = :id
        GROUP BY dr.driver_id
    """
    rows = query_to_list(sql, {"id": driver_id})
    if not rows:
        raise HTTPException(status_code=404, detail="Driver not found")
    return rows[0]


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@app.get("/routes")
def get_routes():
    sql = """
        SELECT
            r.route_id, r.origin_city, r.origin_country,
            r.destination_city, r.destination_country,
            r.distance_km, r.region, r.route_type,
            COUNT(f.shipment_id)                                          AS total_shipments,
            ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2)
                                                                          AS on_time_pct,
            ROUND(AVG(f.cost_usd)::numeric, 2)                           AS avg_cost_usd
        FROM dim_route r
        LEFT JOIN fact_shipments f ON r.route_id = f.route_id
        GROUP BY r.route_id, r.origin_city, r.origin_country,
                 r.destination_city, r.destination_country,
                 r.distance_km, r.region, r.route_type
        ORDER BY total_shipments DESC NULLS LAST
    """
    return query_to_list(sql)


# ══════════════════════════════════════════════════════════════
# WEATHER
# ══════════════════════════════════════════════════════════════

@app.get("/weather/impact")
def get_weather_impact():
    sql = """
        SELECT
            weather_condition,
            COUNT(*)                                                      AS shipments,
            ROUND(AVG(CASE WHEN is_delayed THEN 1.0 ELSE 0 END)*100, 2)  AS delay_rate_pct,
            ROUND(AVG(delay_minutes)::numeric, 1)                        AS avg_delay_min,
            ROUND(AVG(temperature_celsius)::numeric, 1)                  AS avg_temp_c,
            ROUND(AVG(wind_speed_kmh)::numeric, 1)                       AS avg_wind_kmh
        FROM fact_shipments
        GROUP BY weather_condition
        ORDER BY delay_rate_pct DESC
    """
    return query_to_list(sql)


# ══════════════════════════════════════════════════════════════
# ML PREDICTION
# ══════════════════════════════════════════════════════════════

class ShipmentInput(BaseModel):
    weight_kg:            float
    distance_km:          int
    planned_duration_hrs: float
    cost_usd:             float
    weather_condition:    str
    temperature_celsius:  float
    wind_speed_kmh:       float
    experience_years:     int
    driver_rating:        float
    license_type:         str
    vehicle_type:         str
    vehicle_age_years:    int
    mileage_km:           int
    region:               str
    route_type:           str
    route_distance:       int
    month:                int
    quarter:              int
    is_weekend:           int
    weekday:              str
    segment:              str
    industry:             str


@app.post("/predict/delay")
def predict_delay(shipment: ShipmentInput):
    """Predict if a shipment will be delayed. Returns probability and risk level."""
    try:
        from predict import predict_single
        result = predict_single(shipment.dict())
        return result
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="Model not trained yet. Run train.py first."
        )