# LogiFlow — System Architecture

> This document describes the full technical architecture of the LogiFlow platform,
> covering all four MVPs and the data journey from ingestion to real-time streaming.

---

## Table of Contents

1. [High-Level Overview](#1-high-level-overview)
2. [Component Inventory](#2-component-inventory)
3. [Data Flow Diagrams](#3-data-flow-diagrams)
4. [MVP Architecture Layers](#4-mvp-architecture-layers)
5. [Infrastructure & Networking](#5-infrastructure--networking)
6. [Security Considerations](#6-security-considerations)

---

## 1. High-Level Overview

LogiFlow is a multi-layer logistics data platform composed of four progressive MVPs:

| Layer | MVP | Purpose |
|-------|-----|---------|
| Foundation | MVP 1 | Batch data ingestion, ETL, star schema warehouse |
| Analytics | MVP 2 | Dashboard, REST API, scheduling, data quality |
| Intelligence | MVP 3 | ML delay prediction, live weather data, Airflow orchestration |
| Streaming | MVP 4 | Real-time Kafka events, Spark Structured Streaming |

**End-to-end data journey:**

```
 Raw CSV / Live API / Kafka Events
         │
         ▼
 ┌───────────────┐
 │  MinIO (S3)   │  ← Staging layer — raw files before transformation
 └───────┬───────┘
         │ ETL
         ▼
 ┌────────────────────┐
 │   PostgreSQL       │  ← Analytical core — star schema + realtime tables
 │   (Warehouse)      │
 └────────┬───────────┘
          │
    ┌─────┴──────────────────────┐
    │                            │
    ▼                            ▼
 ┌──────────┐            ┌─────────────┐
 │Streamlit │            │  FastAPI    │
 │Dashboard │            │  REST API   │
 └──────────┘            └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │ XGBoost ML  │
                         │  Predictor  │
                         └─────────────┘
```

---

## 2. Component Inventory

### Core Services (docker-compose.yml)

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `logiflow_postgres` | `postgres:15` | `5432` | Primary data warehouse + Airflow metadata DB |
| `logiflow_minio` | `minio/minio:latest` | `9000` (API), `9001` (Console) | S3-compatible object store for raw CSV staging |
| `logiflow_airflow` | `apache/airflow:2.8.0-python3.11` | `8080` | DAG orchestration and pipeline scheduling |
| `logiflow_api` | Custom build | `8000` | FastAPI REST API with ML prediction endpoint |

### Streaming Services (docker-compose.streaming.yml)

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `logiflow_kafka` | `apache/kafka:3.7.0` | `9092` (internal), `9094` (external) | Message broker — KRaft mode (no Zookeeper) |
| `logiflow_spark_master` | `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu` | `7077` (cluster), `8082` (UI) | Spark cluster master node |
| `logiflow_spark_worker` | same | `8083` (UI) | Spark worker node |
| `logiflow_shipment_producer` | Custom build | — | Kafka producer: 1 event/second |
| `logiflow_spark_streaming` | Custom build | — | Spark Structured Streaming job |
| `logiflow_kafka_ui` | `provectuslabs/kafka-ui` | `8090` | Optional Kafka topic browser (profile: ui) |

---

## 3. Data Flow Diagrams

### 3A — Batch Pipeline (MVPs 1–3)

```
┌────────────────────────────────────────────────────────────────────┐
│                       BATCH PIPELINE                               │
│                                                                    │
│  1. generate_data.py                                               │
│     └─ Generates synthetic shipments (10,000+ rows)               │
│        with real weather enrichment (OpenWeatherMap API)           │
│                 │                                                  │
│                 ▼                                                  │
│  2. upload_to_minio.py                                             │
│     └─ Uploads CSV files to MinIO bucket: logiflow-raw            │
│                 │                                                  │
│                 ▼                                                  │
│  3. etl_pipeline.py                                                │
│     ├─ Extract: reads CSV from MinIO (boto3)                       │
│     ├─ Transform: clean, type-cast, compute derived fields         │
│     │   (delay_minutes, is_delayed, fuel efficiency, etc.)         │
│     └─ Load: INSERT into star schema via psycopg2                  │
│              (dim_date, dim_customer, dim_driver,                  │
│               dim_vehicle, dim_route → fact_shipments)             │
│                 │                                                  │
│                 ▼                                                  │
│  4. quality_checks.py                                              │
│     └─ 8 validation checks:                                        │
│        row_counts, null_fk, invalid_status, negative_values,       │
│        delay_consistency, rating_range, orphan_records,            │
│        duplicate_shipments                                         │
│                 │                                                  │
│                 ▼                                                  │
│  5. train.py                                                       │
│     └─ Retrains 4 classifiers on latest warehouse data             │
│        Best model saved as delay_predictor.pkl                     │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 3B — Real-Time Streaming Pipeline (MVP 4)

```
┌────────────────────────────────────────────────────────────────────┐
│                    STREAMING PIPELINE                              │
│                                                                    │
│  kafka_producer.py                                                 │
│  └─ Generates synthetic shipment events (JSON)                     │
│     Publishes 1 event/second → Kafka topic: shipment_events        │
│                 │                                                  │
│                 ▼                                                  │
│  Apache Kafka 3.7.0 (KRaft mode)                                   │
│  └─ Brokers messages with guaranteed delivery                      │
│     Internal: kafka:9092 | External: localhost:9094                │
│                 │                                                  │
│                 ▼                                                  │
│  spark_streaming.py (Spark Structured Streaming)                   │
│  ├─ Reads from Kafka using readStream                              │
│  ├─ Parses JSON payload                                            │
│  ├─ Computes derived fields:                                       │
│  │   cost_per_km = revenue / distance_km                           │
│  │   weather_risk = LOW / MEDIUM / HIGH (based on temperature)     │
│  └─ Writes to PostgreSQL: realtime_shipments                       │
│     Trigger: 10-second micro-batches                               │
│                 │                                                  │
│                 ▼                                                  │
│  PostgreSQL: realtime_shipments table                              │
│  └─ Stores enriched real-time events for analytics                 │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 3C — Airflow Orchestration (MVP 3)

```
  logiflow_daily_pipeline DAG (schedule: daily @ 02:00 UTC)

  generate_data ──► upload_to_minio ──► run_etl ──► quality_check ──► retrain_model ──► pipeline_summary
       │                  │               │              │                  │                  │
  Synthetic data     MinIO staging     Star schema   8 checks          XGBoost           Slack/log
  + weather API      logiflow-raw      insert        (pass/fail)       retrain           summary
```

---

## 4. MVP Architecture Layers

### MVP 1 — Data Foundation

**Architecture pattern:** Lambda batch ingestion

- Data is generated with domain-realistic logic (shipment weights, routes, costs, weather)
- Uploaded as CSV files to MinIO (simulating a data lake landing zone)
- ETL pipeline extracts from MinIO, transforms with pandas, loads into PostgreSQL
- Star schema design: `fact_shipments` connected to 5 dimension tables

**Key design decisions:**
- Star schema over 3NF for analytics query performance
- MinIO as intermediate staging layer (not direct DB insert) for replayability
- Foreign key constraints enforced at DB level for data integrity

---

### MVP 2 — Analytics Layer

**Architecture pattern:** Layered analytics on top of warehouse

- **Dashboard (Streamlit):** connects directly to PostgreSQL via `db_connector.py`
- **API (FastAPI):** exposes warehouse data through RESTful endpoints, Swagger UI at `/docs`
- **Scheduler (APScheduler):** runs ETL + quality checks on cron schedule
- **Quality checks:** run post-ETL to validate referential integrity and business rules

---

### MVP 3 — Intelligence Layer

**Architecture pattern:** ML training loop integrated into pipeline

- Warehouse is used as the ML training set (10,000+ labeled shipments)
- Feature engineering: vehicle_age, load_ratio, cost_per_km, mileage_per_year
- 4 models compared; best saved via joblib; loaded by FastAPI for live inference
- Airflow DAG replaces APScheduler: all 6 tasks orchestrated as a DAG with dependencies

---

### MVP 4 — Streaming Layer

**Architecture pattern:** Kappa architecture — streaming-first with PostgreSQL sink

- Kafka acts as the event bus (decoupled producer/consumer)
- Spark Structured Streaming provides exactly-once processing guarantees
- Events enriched with derived metrics before writing to PostgreSQL
- Streaming table (`realtime_shipments`) can be queried alongside batch data for unified analytics

---

## 5. Infrastructure & Networking

### Docker Network

All services communicate on a shared bridge network: `logiflow_net`

```
logiflow_net (bridge)
├── logiflow_postgres      :5432
├── logiflow_minio         :9000/:9001
├── logiflow_airflow       :8080
├── logiflow_api           :8000
├── logiflow_kafka         :9092 (internal) / :9094 (external)
├── logiflow_spark_master  :7077/:8082
├── logiflow_spark_worker  :8083
├── logiflow_shipment_producer
└── logiflow_spark_streaming
```

### Volume Mounts

| Volume | Service | Purpose |
|--------|---------|---------|
| `postgres_data` | PostgreSQL | Persistent warehouse storage |
| `minio_data` | MinIO | Persistent object store data |
| `./logs/airflow` | Airflow | DAG and task execution logs |
| `./3C-airflow-orchestration/dags` | Airflow | DAG definition files |
| `./mvp4-streaming` | Spark/Producer | Source code hot-reload |

### Environment Configuration

All sensitive configuration is stored in `.env` (see `.env.example`):

```
POSTGRES_USER=logiflow_user
POSTGRES_PASSWORD=<your-password>
POSTGRES_DB=logiflow
MINIO_ROOT_USER=<your-user>
MINIO_ROOT_PASSWORD=<your-password>
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=<your-password>
OPENWEATHER_API_KEY=<optional>
```

---

## 6. Security Considerations

- All credentials are externalized to `.env` — never committed to version control
- PostgreSQL listens only within the Docker bridge network (not exposed on host by default)
- MinIO access keys are rotated via the MinIO Console
- Airflow webserver authentication is enabled (username/password)
- Kafka external listener (`9094`) is intended for local development only — should be restricted or removed in production
- FastAPI does not implement authentication in the current version — suitable for internal/dev use; add OAuth2 before production exposure
