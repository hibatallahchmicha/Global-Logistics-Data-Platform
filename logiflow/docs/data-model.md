# LogiFlow — Data Model Reference

> This document describes the complete database schema used by the LogiFlow platform,
> including the star schema warehouse (MVPs 1–3) and the real-time streaming table (MVP 4).

---

## Table of Contents

1. [Schema Design Philosophy](#1-schema-design-philosophy)
2. [Star Schema Overview](#2-star-schema-overview)
3. [Dimension Tables](#3-dimension-tables)
4. [Fact Table](#4-fact-table)
5. [Real-Time Streaming Table (MVP 4)](#5-real-time-streaming-table-mvp-4)
6. [Indexes](#6-indexes)
7. [Entity Relationship Diagram](#7-entity-relationship-diagram)

---

## 1. Schema Design Philosophy

LogiFlow uses a **star schema** for its analytical warehouse. This pattern was chosen over
third-normal-form (3NF) for these reasons:

- **Query performance:** Analytical queries involve grouping and aggregation across multiple
  dimensions. Star schema allows direct dimension-to-fact joins without chained lookups.
- **Readability:** Business users and BI tools can understand a star schema intuitively.
- **Extensibility:** New dimension attributes (e.g., adding a `customer.tier` column)
  don't require restructuring the fact table.

The schema lives in `mvp1-data-pipeline/schema.sql` and is applied once at infrastructure setup.

---

## 2. Star Schema Overview

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼───────────┐    ┌──────────────┐
│ dim_customer ├────►                  ◄────┤  dim_driver  │
└──────────────┘    │  fact_shipments  │    └──────────────┘
                    │                  │
┌──────────────┐    │  (center of star)│    ┌──────────────┐
│  dim_route   ├────►                  ◄────┤  dim_vehicle │
└──────────────┘    └──────────────────┘    └──────────────┘
```

**5 tables:** 1 fact + 4 dimensions + 1 date dimension = 6 total warehouse tables
**Plus:** 1 real-time streaming table added in MVP 4

---

## 3. Dimension Tables

### `dim_date`

Pre-computed calendar attributes to avoid repeated date calculations in queries.

| Column | Type | Description |
|--------|------|-------------|
| `date_id` | SERIAL PK | Surrogate key |
| `full_date` | DATE UNIQUE | Calendar date |
| `day` | INT | Day of month (1–31) |
| `month` | INT | Month number (1–12) |
| `month_name` | VARCHAR(15) | e.g., `January` |
| `quarter` | INT | Quarter (1–4) |
| `year` | INT | Calendar year |
| `weekday` | VARCHAR(10) | e.g., `Monday` |
| `is_weekend` | BOOLEAN | TRUE for Saturday/Sunday |

**Usage:** Group by month, quarter, or weekday without string parsing.

---

### `dim_customer`

B2B customers who place shipment orders.

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | SERIAL PK | Surrogate key |
| `company_name` | VARCHAR(150) | Company name |
| `industry` | VARCHAR(80) | e.g., `Retail`, `Pharma`, `Automotive` |
| `country` | VARCHAR(60) | Country of customer |
| `city` | VARCHAR(80) | City of customer |
| `segment` | VARCHAR(20) | `Enterprise`, `SME`, or `Startup` |
| `contract_type` | VARCHAR(20) | `Annual`, `Monthly`, or `Spot` |

**Key analytics dimension:** Revenue by segment, delay rates by industry.

---

### `dim_driver`

Drivers who execute deliveries. Rating and experience are key ML features.

| Column | Type | Description |
|--------|------|-------------|
| `driver_id` | SERIAL PK | Surrogate key |
| `full_name` | VARCHAR(100) | Driver full name |
| `license_type` | VARCHAR(10) | `B`, `C`, or `CE` (truck license classes) |
| `experience_years` | INT | Years of professional driving experience |
| `rating` | NUMERIC(3,2) | Average customer rating (1.0 – 5.0) |
| `country` | VARCHAR(60) | Country of operation |
| `is_active` | BOOLEAN | Whether driver is currently active |

**ML feature:** `experience_years` and `rating` are strong predictors of on-time delivery.

---

### `dim_vehicle`

Fleet vehicles used for deliveries. Age and mileage correlate with breakdown risk.

| Column | Type | Description |
|--------|------|-------------|
| `vehicle_id` | SERIAL PK | Surrogate key |
| `plate_number` | VARCHAR(20) UNIQUE | License plate identifier |
| `vehicle_type` | VARCHAR(30) | `Truck`, `Van`, or `Motorcycle` |
| `capacity_kg` | INT | Maximum cargo capacity in kilograms |
| `manufacture_year` | INT | Year the vehicle was manufactured |
| `mileage_km` | INT | Total kilometers driven |
| `last_service_date` | DATE | Date of last maintenance service |
| `is_active` | BOOLEAN | Whether vehicle is in service |

**ML features:** `vehicle_age = current_year - manufacture_year`, `mileage_per_year`.

---

### `dim_route`

Origin-to-destination route definitions.

| Column | Type | Description |
|--------|------|-------------|
| `route_id` | SERIAL PK | Surrogate key |
| `origin_city` | VARCHAR(80) | Departure city |
| `origin_country` | VARCHAR(60) | Departure country |
| `destination_city` | VARCHAR(80) | Arrival city |
| `destination_country` | VARCHAR(60) | Arrival country |
| `distance_km` | INT | Route distance in kilometers |
| `region` | VARCHAR(40) | Geographic region: `Europe`, `Africa`, `Middle East` |
| `route_type` | VARCHAR(20) | Transport mode: `Road`, `Air`, or `Sea` |

**Key analytics dimension:** Cost per km by region, delay rates by route type.

---

## 4. Fact Table

### `fact_shipments`

The center of the star. Each row represents one completed shipment.

| Column | Type | Description |
|--------|------|-------------|
| `shipment_id` | SERIAL PK | Surrogate key |
| `customer_id` | INT FK | → `dim_customer` |
| `driver_id` | INT FK | → `dim_driver` |
| `vehicle_id` | INT FK | → `dim_vehicle` |
| `route_id` | INT FK | → `dim_route` |
| `date_id` | INT FK | → `dim_date` |
| `raw_file` | VARCHAR(200) | Source MinIO file path |
| `planned_duration_hrs` | NUMERIC(6,2) | Expected delivery time in hours |
| `actual_duration_hrs` | NUMERIC(6,2) | Actual delivery time in hours |
| `delay_minutes` | INT | `actual - planned` in minutes (negative = early) |
| `distance_km` | INT | Shipment distance |
| `weight_kg` | NUMERIC(8,2) | Cargo weight in kilograms |
| `cost_usd` | NUMERIC(10,2) | Total shipment cost in USD |
| `fuel_consumed_liters` | NUMERIC(8,2) | Fuel usage |
| `weather_condition` | VARCHAR(30) | `Clear`, `Rain`, `Snow`, `Fog` at time of delivery |
| `temperature_celsius` | NUMERIC(5,2) | Temperature at delivery location |
| `wind_speed_kmh` | NUMERIC(6,2) | Wind speed at delivery |
| `status` | VARCHAR(20) | `on_time`, `delayed`, or `failed` |
| `is_delayed` | BOOLEAN | TRUE if `delay_minutes > 30` |
| `scheduled_pickup` | TIMESTAMP | Planned pickup time |
| `actual_pickup` | TIMESTAMP | Actual pickup time |
| `scheduled_delivery` | TIMESTAMP | Planned delivery time |
| `actual_delivery` | TIMESTAMP | Actual delivery time |

**Volume:** 10,000+ rows of synthetic + real-weather-enriched data.

---

## 5. Real-Time Streaming Table (MVP 4)

### `realtime_shipments`

Appended to by Spark Structured Streaming. One row per Kafka event consumed.
Events are enriched with derived fields computed in the Spark job.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment surrogate key |
| `event_id` | VARCHAR(36) UNIQUE | UUID from Kafka message — prevents duplicates |
| `event_type` | VARCHAR(50) | `NEW_SHIPMENT`, `STATUS_UPDATE`, `LOCATION_UPDATE`, `DELIVERY_COMPLETE` |
| `event_timestamp` | TIMESTAMP | Time the event occurred |
| `shipment_id` | VARCHAR(50) | Business shipment identifier |
| `status` | VARCHAR(20) | `in_transit`, `delivered`, `delayed`, `failed` |
| `origin_city` | VARCHAR(100) | Departure city |
| `destination_city` | VARCHAR(100) | Arrival city |
| `vehicle_type` | VARCHAR(20) | `truck`, `van`, `motorcycle` |
| `weight_kg` | NUMERIC(10,2) | Cargo weight |
| `distance_km` | NUMERIC(10,2) | Route distance |
| `revenue` | NUMERIC(10,2) | Shipment revenue in USD |
| `is_delayed` | BOOLEAN | Delay flag from event payload |
| `delay_hours` | NUMERIC(5,1) | Hours of delay |
| `driver_rating` | NUMERIC(3,1) | Driver rating from event payload |
| `temperature_c` | NUMERIC(5,1) | Temperature at time of event |
| `humidity_pct` | NUMERIC(5,1) | Humidity percentage |
| `cost_per_km` | NUMERIC(10,4) | **Derived by Spark:** `revenue / distance_km` |
| `weather_risk` | VARCHAR(10) | **Derived by Spark:** `LOW` / `MEDIUM` / `HIGH` |
| `ingested_at` | TIMESTAMP | Timestamp when inserted by Spark (default NOW()) |

**Derivation logic in Spark:**
```python
# cost_per_km
df = df.withColumn("cost_per_km", col("revenue") / col("distance_km"))

# weather_risk based on temperature
weather_risk = when(col("temperature_c") < 0, "HIGH") \
               .when(col("temperature_c") < 10, "MEDIUM") \
               .otherwise("LOW")
```

**Volume:** 6,000+ rows after initial pipeline run (1 event/second continuous ingestion).

---

## 6. Indexes

Indexes are created on high-frequency query columns to ensure sub-millisecond lookup performance.

### `fact_shipments` Indexes

```sql
CREATE INDEX idx_shipments_customer   ON fact_shipments(customer_id);
CREATE INDEX idx_shipments_driver     ON fact_shipments(driver_id);
CREATE INDEX idx_shipments_date       ON fact_shipments(date_id);
CREATE INDEX idx_shipments_status     ON fact_shipments(status);
CREATE INDEX idx_shipments_is_delayed ON fact_shipments(is_delayed);
```

### `realtime_shipments` Indexes

```sql
CREATE INDEX idx_rt_shipments_timestamp  ON realtime_shipments(event_timestamp);
CREATE INDEX idx_rt_shipments_status     ON realtime_shipments(status);
CREATE INDEX idx_rt_shipments_shipment_id ON realtime_shipments(shipment_id);
```

---

## 7. Entity Relationship Diagram

```
dim_date
  ├── date_id (PK)
  └── full_date, day, month, quarter, year, weekday, is_weekend
          │
          │ (FK: date_id)
          ▼
dim_customer ──(FK: customer_id)──►
dim_driver   ──(FK: driver_id)────►  fact_shipments
dim_vehicle  ──(FK: vehicle_id)───►      ├── shipment_id (PK)
dim_route    ──(FK: route_id)─────►      ├── planned/actual duration, delay
                                          ├── cost, weight, distance, fuel
                                          ├── weather condition + temperature
                                          ├── status, is_delayed
                                          └── scheduled/actual timestamps


realtime_shipments (independent streaming table — MVP 4)
  ├── id (PK), event_id (UNIQUE)
  ├── event_type, event_timestamp, shipment_id
  ├── status, origin/destination, vehicle_type
  ├── weight_kg, distance_km, revenue
  ├── is_delayed, delay_hours, driver_rating
  ├── temperature_c, humidity_pct
  ├── cost_per_km (derived), weather_risk (derived)
  └── ingested_at
```
