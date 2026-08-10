"""
services/api/main.py

REST API over the warehouse + ML prediction endpoint.

Depends on: common.config (1), ml.predict (8). Assumes the repo root
is on sys.path -- true locally via `uvicorn services.api.main:app`
from the repo root, and true in Docker because the Dockerfile copies
common/, ml/, and services/api/ as siblings under /app, mirroring the
local layout exactly (see Module 14 for the docker-compose build
context change this requires).
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from common.config import settings
from ml.predict import predict_single

app = FastAPI(title="LogiFlow API", version="2.0.0")
engine = create_engine(settings.database_url)


def query_to_list(sql: str, params: dict | None = None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


@app.get("/")
def root():
    return {"project": "LogiFlow", "version": "2.0.0", "status": "running", "docs": "/docs"}


@app.get("/shipments")
def get_shipments(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    status: str = Query(None, description="on_time | delayed | failed"),
):
    where = "WHERE f.status = :status" if status else ""
    sql = f"""
        SELECT f.shipment_id, f.status, f.is_delayed, f.delay_minutes, f.cost_usd,
               f.weight_kg, f.distance_km, d.full_date, d.month_name, d.year,
               c.company_name, c.segment, dr.full_name AS driver_name, dr.rating AS driver_rating,
               v.vehicle_type, r.origin_city, r.destination_city, r.region, r.route_type
        FROM fact_shipments f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_customer c ON f.customer_id = c.customer_id
        JOIN dim_driver dr ON f.driver_id = dr.driver_id
        JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
        JOIN dim_route r ON f.route_id = r.route_id
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
        SELECT f.*, d.full_date, c.company_name, c.segment, dr.full_name, v.vehicle_type,
               r.origin_city, r.destination_city, r.region
        FROM fact_shipments f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_customer c ON f.customer_id = c.customer_id
        JOIN dim_driver dr ON f.driver_id = dr.driver_id
        JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
        JOIN dim_route r ON f.route_id = r.route_id
        WHERE f.shipment_id = :id
    """
    rows = query_to_list(sql, {"id": shipment_id})
    if not rows:
        raise HTTPException(status_code=404, detail="Shipment not found")
    return rows[0]


@app.get("/kpis/summary")
def get_kpi_summary():
    sql = """
        SELECT COUNT(*) AS total_shipments,
               ROUND(AVG(CASE WHEN status='on_time' THEN 1.0 ELSE 0 END)*100, 2) AS on_time_rate_pct,
               ROUND(AVG(CASE WHEN is_delayed THEN delay_minutes END), 1) AS avg_delay_minutes,
               ROUND(SUM(cost_usd)::numeric, 2) AS total_revenue_usd,
               ROUND(AVG(cost_usd)::numeric, 2) AS avg_cost_usd,
               ROUND(AVG(CASE WHEN status='failed' THEN 1.0 ELSE 0 END)*100, 2) AS failure_rate_pct
        FROM fact_shipments
    """
    return query_to_list(sql)[0]


@app.get("/kpis/by-month")
def get_kpis_by_month():
    sql = """
        SELECT d.year, d.month, d.month_name, COUNT(*) AS shipments,
               ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2) AS on_time_pct,
               ROUND(SUM(f.cost_usd)::numeric, 2) AS revenue_usd
        FROM fact_shipments f JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name ORDER BY d.year, d.month
    """
    return query_to_list(sql)


@app.get("/kpis/by-region")
def get_kpis_by_region():
    sql = """
        SELECT r.region, COUNT(*) AS shipments,
               ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2) AS on_time_pct,
               ROUND(AVG(f.cost_usd)::numeric, 2) AS avg_cost_usd
        FROM fact_shipments f JOIN dim_route r ON f.route_id = r.route_id
        GROUP BY r.region ORDER BY shipments DESC
    """
    return query_to_list(sql)


@app.get("/drivers")
def get_drivers():
    sql = """
        SELECT dr.driver_id, dr.full_name, dr.license_type, dr.experience_years, dr.rating,
               COUNT(f.shipment_id) AS total_shipments,
               ROUND(AVG(CASE WHEN f.status='on_time' THEN 1.0 ELSE 0 END)*100, 2) AS on_time_pct
        FROM dim_driver dr LEFT JOIN fact_shipments f ON dr.driver_id = f.driver_id
        GROUP BY dr.driver_id ORDER BY on_time_pct DESC NULLS LAST
    """
    return query_to_list(sql)


@app.get("/routes")
def get_routes():
    sql = """
        SELECT r.route_id, r.origin_city, r.destination_city, r.distance_km, r.region,
               COUNT(f.shipment_id) AS total_shipments,
               ROUND(AVG(f.cost_usd)::numeric, 2) AS avg_cost_usd
        FROM dim_route r LEFT JOIN fact_shipments f ON r.route_id = f.route_id
        GROUP BY r.route_id ORDER BY total_shipments DESC NULLS LAST
    """
    return query_to_list(sql)


@app.get("/weather/impact")
def get_weather_impact():
    sql = """
        SELECT weather_condition, COUNT(*) AS shipments,
               ROUND(AVG(CASE WHEN is_delayed THEN 1.0 ELSE 0 END)*100, 2) AS delay_rate_pct,
               ROUND(AVG(delay_minutes)::numeric, 1) AS avg_delay_min
        FROM fact_shipments GROUP BY weather_condition ORDER BY delay_rate_pct DESC
    """
    return query_to_list(sql)


class ShipmentInput(BaseModel):
    weight_kg: float
    distance_km: int
    planned_duration_hrs: float
    cost_usd: float
    weather_condition: str
    temperature_celsius: float
    wind_speed_kmh: float
    traffic_congestion_ratio: float
    traffic_condition: str
    experience_years: int
    driver_rating: float
    license_type: str
    vehicle_type: str
    vehicle_capacity: int
    manufacture_year: int
    mileage_km: int
    region: str
    route_type: str
    month: int
    quarter: int
    is_weekend: int
    weekday: str
    segment: str
    industry: str


@app.post("/predict/delay")
def predict_delay(shipment: ShipmentInput):
    try:
        return predict_single(shipment.model_dump())
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Model not trained yet. Run ml/train.py first.")