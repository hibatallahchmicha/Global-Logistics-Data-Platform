-- ============================================================
-- LOGIFLOW DATA WAREHOUSE -- STAR SCHEMA
-- Single source of truth for the warehouse structure.
-- Applied automatically by Postgres on first container start
-- (see docker-compose.yml's docker-entrypoint-initdb.d mount,
-- wired up properly in Module 14).
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    day             INT,
    month           INT,
    month_name      VARCHAR(15),
    quarter         INT,
    year            INT,
    weekday         VARCHAR(10),
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     SERIAL PRIMARY KEY,
    company_name    VARCHAR(150) UNIQUE NOT NULL,
    industry        VARCHAR(80),
    country         VARCHAR(60),
    city            VARCHAR(80),
    segment         VARCHAR(20),
    contract_type   VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id           SERIAL PRIMARY KEY,
    full_name           VARCHAR(100) UNIQUE NOT NULL,
    license_type        VARCHAR(10),
    experience_years    INT,
    rating               NUMERIC(3,2),
    country              VARCHAR(60),
    is_active             BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id          SERIAL PRIMARY KEY,
    plate_number        VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type        VARCHAR(30),
    capacity_kg          INT,
    manufacture_year     INT,
    mileage_km            INT,
    last_service_date     DATE,
    is_active              BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_route (
    route_id             SERIAL PRIMARY KEY,
    origin_city          VARCHAR(80)  NOT NULL,
    origin_country       VARCHAR(60)  NOT NULL,
    destination_city     VARCHAR(80)  NOT NULL,
    destination_country  VARCHAR(60)  NOT NULL,
    distance_km           INT,
    region                 VARCHAR(40),
    route_type              VARCHAR(20),
    UNIQUE (origin_city, origin_country, destination_city, destination_country)
);

CREATE TABLE IF NOT EXISTS fact_shipments (
    shipment_id             SERIAL PRIMARY KEY,
    source_shipment_id      VARCHAR(64) UNIQUE NOT NULL,
    customer_id              INT REFERENCES dim_customer(customer_id),
    driver_id                 INT REFERENCES dim_driver(driver_id),
    vehicle_id                 INT REFERENCES dim_vehicle(vehicle_id),
    route_id                     INT REFERENCES dim_route(route_id),
    date_id                       INT REFERENCES dim_date(date_id),
    raw_file                       VARCHAR(200),
    planned_duration_hrs            NUMERIC(6,2),
    actual_duration_hrs              NUMERIC(6,2),
    delay_minutes                     INT,
    distance_km                        INT,
    weight_kg                           NUMERIC(8,2),
    cost_usd                             NUMERIC(10,2),
    fuel_consumed_liters                  NUMERIC(8,2),
    weather_condition                      VARCHAR(30),
    temperature_celsius                     NUMERIC(5,2),
    wind_speed_kmh                           NUMERIC(6,2),
    traffic_congestion_ratio                 NUMERIC(4,2),  -- current_speed / free_flow_speed, ~0.2-1.0
    traffic_condition                         VARCHAR(10),   -- LOW / MEDIUM / HIGH
    status                                    VARCHAR(20),
    is_delayed                                 BOOLEAN,
    scheduled_pickup                            TIMESTAMP,
    actual_pickup                                TIMESTAMP,
    scheduled_delivery                            TIMESTAMP,
    actual_delivery                                TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shipments_customer   ON fact_shipments(customer_id);
CREATE INDEX IF NOT EXISTS idx_shipments_driver     ON fact_shipments(driver_id);
CREATE INDEX IF NOT EXISTS idx_shipments_date       ON fact_shipments(date_id);
CREATE INDEX IF NOT EXISTS idx_shipments_status     ON fact_shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_is_delayed ON fact_shipments(is_delayed);

CREATE TABLE IF NOT EXISTS realtime_shipments (
    id                  SERIAL PRIMARY KEY,
    event_id            VARCHAR(36) UNIQUE NOT NULL,
    event_type          VARCHAR(50),
    event_timestamp     TIMESTAMP,
    shipment_id         VARCHAR(50),
    status               VARCHAR(20),
    origin_city           VARCHAR(100),
    destination_city       VARCHAR(100),
    vehicle_type             VARCHAR(20),
    weight_kg                 NUMERIC(10,2),
    distance_km                 NUMERIC(10,2),
    revenue                       NUMERIC(10,2),
    is_delayed                     BOOLEAN,
    delay_hours                     NUMERIC(5,1),
    driver_rating                     NUMERIC(3,1),
    temperature_c                       NUMERIC(5,1),
    humidity_pct                          NUMERIC(5,1),
    cost_per_km                            NUMERIC(10,4),
    weather_risk                             VARCHAR(10),
    ingested_at                                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rt_shipments_timestamp   ON realtime_shipments(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_rt_shipments_status      ON realtime_shipments(status);
CREATE INDEX IF NOT EXISTS idx_rt_shipments_shipment_id ON realtime_shipments(shipment_id);