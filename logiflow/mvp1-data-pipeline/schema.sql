
-- LOGIFLOW DATA WAREHOUSE — STAR SCHEMA
-- Pattern: one fact table + dimension tables
-- fact_shipments = what happened (numbers, events)
-- dim_* = who, what, where, when (context)


--  DIMENSION: Date 
-- Pre-computed date attributes so analysts never
-- need to calculate day/month/quarter in SQL
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

-- DIMENSION: Customer 
-- Who is shipping? B2B customers (companies)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     SERIAL PRIMARY KEY,
    company_name    VARCHAR(150),
    industry        VARCHAR(80),
    country         VARCHAR(60),
    city            VARCHAR(80),
    segment         VARCHAR(20),   -- 'Enterprise', 'SME', 'Startup'
    contract_type   VARCHAR(20)    -- 'Annual', 'Monthly', 'Spot'
);

-- DIMENSION: Driver 
-- Who delivered the shipment?
-- Experience and rating are key ML features for delay prediction
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id           SERIAL PRIMARY KEY,
    full_name           VARCHAR(100),
    license_type        VARCHAR(10),   -- 'B', 'C', 'CE'
    experience_years    INT,
    rating              NUMERIC(3,2),  -- 1.0 to 5.0
    country             VARCHAR(60),
    is_active           BOOLEAN DEFAULT TRUE
);

-- DIMENSION: Vehicle 
-- What vehicle was used?
-- Age and mileage are key features for breakdown risk
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id          SERIAL PRIMARY KEY,
    plate_number        VARCHAR(20) UNIQUE,
    vehicle_type        VARCHAR(30),   -- 'Truck', 'Van', 'Motorcycle'
    capacity_kg         INT,
    manufacture_year    INT,
    mileage_km          INT,
    last_service_date   DATE,
    is_active           BOOLEAN DEFAULT TRUE
);

-- DIMENSION: Route 
-- What path did the shipment take?
-- Distance and region drive cost and delay risk
CREATE TABLE IF NOT EXISTS dim_route (
    route_id            SERIAL PRIMARY KEY,
    origin_city         VARCHAR(80),
    origin_country      VARCHAR(60),
    destination_city    VARCHAR(80),
    destination_country VARCHAR(60),
    distance_km         INT,
    region              VARCHAR(40),   -- 'Europe', 'Africa', 'Middle East'
    route_type          VARCHAR(20)    -- 'Road', 'Air', 'Sea'
);

--  FACT TABLE: Shipments
-- One row per shipment
-- Contains measurable facts + foreign keys to dimensions
-- This is the center of the star
CREATE TABLE IF NOT EXISTS fact_shipments (
    shipment_id         SERIAL PRIMARY KEY,
    -- Foreign keys to dimensions
    customer_id         INT REFERENCES dim_customer(customer_id),
    driver_id           INT REFERENCES dim_driver(driver_id),
    vehicle_id          INT REFERENCES dim_vehicle(vehicle_id),
    route_id            INT REFERENCES dim_route(route_id),
    date_id             INT REFERENCES dim_date(date_id),
    -- Raw source reference
    raw_file            VARCHAR(200),  -- which MinIO file this came from
    -- Measurable facts (the numbers analysts query)
    planned_duration_hrs    NUMERIC(6,2),
    actual_duration_hrs     NUMERIC(6,2),
    delay_minutes           INT,           -- actual - planned (in minutes)
    distance_km             INT,
    weight_kg               NUMERIC(8,2),
    cost_usd                NUMERIC(10,2),
    fuel_consumed_liters    NUMERIC(8,2),
    -- Weather at time of delivery (from Open-Meteo API)
    weather_condition       VARCHAR(30),   -- 'Clear', 'Rain', 'Snow', 'Fog'
    temperature_celsius     NUMERIC(5,2),
    wind_speed_kmh          NUMERIC(6,2),
    -- Outcome
    status                  VARCHAR(20),   -- 'on_time', 'delayed', 'failed'
    is_delayed              BOOLEAN,       -- TRUE if delay_minutes > 30
    -- Timestamps
    scheduled_pickup        TIMESTAMP,
    actual_pickup           TIMESTAMP,
    scheduled_delivery      TIMESTAMP,
    actual_delivery         TIMESTAMP
);

-- INDEXES for query performance 
-- Without indexes, every query scans the entire table
-- With indexes, lookups on these columns are instant
CREATE INDEX IF NOT EXISTS idx_shipments_customer
    ON fact_shipments(customer_id);
CREATE INDEX IF NOT EXISTS idx_shipments_driver
    ON fact_shipments(driver_id);
CREATE INDEX IF NOT EXISTS idx_shipments_date
    ON fact_shipments(date_id);
CREATE INDEX IF NOT EXISTS idx_shipments_status
    ON fact_shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_is_delayed
    ON fact_shipments(is_delayed);

-- ─────────────────────────────────────────────────
-- MVP 4 — REAL-TIME STREAMING TABLE
-- One row per Kafka event consumed by Spark Streaming
-- Enriched with computed fields (cost_per_km, weather_risk)
-- ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS realtime_shipments (
    id                  SERIAL PRIMARY KEY,
    event_id            VARCHAR(36) UNIQUE NOT NULL,
    event_type          VARCHAR(50),          -- NEW_SHIPMENT, STATUS_UPDATE, LOCATION_UPDATE, DELIVERY_COMPLETE
    event_timestamp     TIMESTAMP,
    shipment_id         VARCHAR(50),
    status              VARCHAR(20),
    origin_city         VARCHAR(100),
    destination_city    VARCHAR(100),
    vehicle_type        VARCHAR(20),
    weight_kg           NUMERIC(10,2),
    distance_km         NUMERIC(10,2),
    revenue             NUMERIC(10,2),
    is_delayed          BOOLEAN,
    delay_hours         NUMERIC(5,1),
    driver_rating       NUMERIC(3,1),
    temperature_c       NUMERIC(5,1),
    humidity_pct        NUMERIC(5,1),
    -- Derived by Spark Streaming
    cost_per_km         NUMERIC(10,4),        -- revenue / distance_km
    weather_risk        VARCHAR(10),          -- LOW / MEDIUM / HIGH
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rt_shipments_timestamp   ON realtime_shipments(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_rt_shipments_status      ON realtime_shipments(status);
CREATE INDEX IF NOT EXISTS idx_rt_shipments_shipment_id ON realtime_shipments(shipment_id);