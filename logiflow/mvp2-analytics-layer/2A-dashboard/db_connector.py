import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Connection ───────────────────────────────────────────────
def get_engine():
    user     = os.getenv("POSTGRES_USER", "logiflow")
    password = os.getenv("POSTGRES_PASSWORD", "Hiba2005")
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB", "logiflow_dw")
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


# ─── Main Query: Fact + All Dimensions joined ─────────────────
def load_shipments() -> pd.DataFrame:
    query = """
        SELECT
            f.shipment_id,
            f.status,
            f.is_delayed,
            f.delay_minutes,
            f.cost_usd,
            f.weight_kg,
            f.distance_km,
            f.fuel_consumed_liters,
            f.planned_duration_hrs,
            f.actual_duration_hrs,
            f.weather_condition,
            f.temperature_celsius,
            f.wind_speed_kmh,
            f.scheduled_pickup,
            f.actual_delivery,

            -- Date dimension
            d.full_date,
            d.month,
            d.month_name,
            d.quarter,
            d.year,
            d.weekday,
            d.is_weekend,

            -- Customer dimension
            c.company_name,
            c.industry,
            c.country      AS customer_country,
            c.city         AS customer_city,
            c.segment,
            c.contract_type,

            -- Driver dimension
            dr.full_name   AS driver_name,
            dr.license_type,
            dr.experience_years,
            dr.rating      AS driver_rating,

            -- Vehicle dimension
            v.vehicle_type,
            v.capacity_kg,
            v.manufacture_year,
            v.mileage_km,

            -- Route dimension
            r.origin_city,
            r.origin_country,
            r.destination_city,
            r.destination_country,
            r.region,
            r.route_type

        FROM fact_shipments f
        JOIN dim_date     d  ON f.date_id     = d.date_id
        JOIN dim_customer c  ON f.customer_id = c.customer_id
        JOIN dim_driver   dr ON f.driver_id   = dr.driver_id
        JOIN dim_vehicle  v  ON f.vehicle_id  = v.vehicle_id
        JOIN dim_route    r  ON f.route_id    = r.route_id
    """
    engine = get_engine()
    return pd.read_sql(query, engine)


# ─── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    df = load_shipments()
    print(f"✅ Loaded {len(df)} rows, {df.shape[1]} columns")
    print(df.head(3))