# LogiFlow — Data Model Reference

> The complete database schema. Single source of truth: `infra/schema.sql`. This
> document explains the *why* behind it; if the two ever disagree, the SQL file is right.

---

## 1. Schema Design Philosophy

Star schema, chosen over 3NF, for the same reasons as before: analytical queries here are
aggregation-heavy (the API and dashboard group by month, region, weather, driver — direct
dimension-to-fact joins beat chained lookups), and it's readable without a diagram.

**What changed in this rebuild, and why:** every dimension table now has a `UNIQUE`
constraint on its real-world identity (`company_name`, driver `full_name`,
`plate_number`, the origin+destination pair), and `fact_shipments` has a
`source_shipment_id` — a UUID assigned by the generator, not the database. This is not
cosmetic. The original schema had no way to recognize "I've already loaded this exact
record," so its ETL truncated and reloaded the *entire* warehouse on every run rather
than risk duplicates — only the most recent run's data ever survived a day. With these
constraints, `pipelines/etl.py` can safely upsert (`ON CONFLICT ... DO NOTHING`) instead
of truncating, and re-running the loader on the same input is a verified no-op.

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
                    │  (center of star)│
┌──────────────┐    │                  │    ┌──────────────┐
│  dim_route   ├────►                  ◄────┤  dim_vehicle │
└──────────────┘    └──────────────────┘    └──────────────┘
```

5 dimensions + 1 fact table = the warehouse. Plus `realtime_shipments`, populated
independently by the streaming layer (Module 13), not by the batch ETL.

---

## 3. Dimension Tables

### `dim_date`
| Column | Type | Notes |
|---|---|---|
| `date_id` | SERIAL PK | Surrogate key |
| `full_date` | DATE, **UNIQUE NOT NULL** | The natural key |
| `day`, `month`, `month_name`, `quarter`, `year`, `weekday` | — | Pre-computed to avoid date math in every query |
| `is_weekend` | BOOLEAN | — |

### `dim_customer`
| Column | Type | Notes |
|---|---|---|
| `customer_id` | SERIAL PK | Surrogate — assigned by Postgres, never supplied by the loader |
| `company_name` | VARCHAR(150), **UNIQUE NOT NULL** | Natural key ETL upserts on |
| `industry`, `country`, `city`, `segment`, `contract_type` | — | — |

### `dim_driver`
| Column | Type | Notes |
|---|---|---|
| `driver_id` | SERIAL PK | Surrogate |
| `full_name` | VARCHAR(100), **UNIQUE NOT NULL** | Natural key |
| `license_type`, `experience_years`, `rating`, `country` | — | `rating` checked in range [1.0, 5.0] by `quality_checks.py` |
| `is_active` | BOOLEAN DEFAULT TRUE | — |

### `dim_vehicle`
| Column | Type | Notes |
|---|---|---|
| `vehicle_id` | SERIAL PK | Surrogate |
| `plate_number` | VARCHAR(20), **UNIQUE NOT NULL** | Natural key |
| `vehicle_type`, `capacity_kg`, `manufacture_year`, `mileage_km` | — | — |
| `last_service_date`, `is_active` | DATE, BOOLEAN | **Known gap:** declared here but never populated by `pipelines/etl.py`'s insert — always NULL/default. The generator doesn't produce these fields yet. |

### `dim_route`
| Column | Type | Notes |
|---|---|---|
| `route_id` | SERIAL PK | Surrogate |
| `origin_city`, `origin_country`, `destination_city`, `destination_country` | VARCHAR, NOT NULL | Composite natural key |
| `distance_km`, `region`, `route_type` | — | — |
| — | `UNIQUE (origin_city, origin_country, destination_city, destination_country)` | The actual upsert target |

---

## 4. Fact Table

### `fact_shipments`
| Column | Type | Notes |
|---|---|---|
| `shipment_id` | SERIAL PK | Internal surrogate, meaningless outside the DB |
| `source_shipment_id` | VARCHAR(64), **UNIQUE NOT NULL** | UUID assigned at generation time — the real identity, what ETL upserts on |
| `customer_id`, `driver_id`, `vehicle_id`, `route_id`, `date_id` | INT, FK | Resolved by the ETL after upserting each dimension |
| `raw_file` | VARCHAR(200) | Which MinIO object this row came from |
| `planned_duration_hrs`, `actual_duration_hrs`, `delay_minutes` | NUMERIC/INT | `actual_duration_hrs` is **not** used as an ML feature — it directly encodes the delay outcome and would leak the label |
| `distance_km`, `weight_kg`, `cost_usd`, `fuel_consumed_liters` | — | — |
| `weather_condition`, `temperature_celsius`, `wind_speed_kmh` | — | From OpenWeatherMap (real, near-term) or simulated (historical) |
| `traffic_congestion_ratio`, `traffic_condition` | NUMERIC(4,2), VARCHAR(10) | From TomTom Traffic (real, near-term) or simulated (historical); ratio = current speed / free-flow speed |
| `status`, `is_delayed` | VARCHAR, BOOLEAN | `is_delayed = delay_minutes > 30`, enforced by both the generator and `quality_checks.py`'s `check_delay_consistency` |
| `scheduled_pickup`, `actual_pickup`, `scheduled_delivery`, `actual_delivery` | TIMESTAMP | — |

**Indexes:** `customer_id`, `driver_id`, `date_id`, `status`, `is_delayed` — the columns
actually filtered/grouped on by the API and dashboard.

---

## 5. Real-Time Streaming Table

### `realtime_shipments`
Populated independently by `streaming/spark_streaming.py` (Module 13), not by the batch
pipeline. `event_id UNIQUE NOT NULL` is what makes the Spark sink's
`ON CONFLICT (event_id) DO NOTHING` upsert meaningful — without it, a duplicate event
(expected, given the producer's at-least-once delivery config) would either silently
duplicate or crash a naive append-only sink.

Not currently joined against or read by the batch warehouse, the API, or the dashboard —
a deliberately separate, independently-verified path, not yet integrated into the
analytics layer.

---

## 6. What Validates This Schema

`pipelines/quality_checks.py` runs 8 checks post-load: row counts, null foreign keys,
invalid status values, negative numerics, delay/flag consistency, driver rating range,
orphan foreign keys, and a business-logic duplicate check (distinct from the
`source_shipment_id` uniqueness, which is enforced at the database level and can't
technically fail — this one catches two *different* shipments sharing a customer and
exact pickup timestamp, which is a coincidence at small data volumes and worth a second
look, not necessarily a bug, at large ones).
