# LogiFlow — End-to-End Logistics Data Platform

> A production-grade data engineering, machine learning, and real-time streaming platform
> for logistics analytics, built progressively using a modern data stack.

**Author:** Hibatallah Chmicha · Data Science Student @ INSEA  
**Stack:** Python · PostgreSQL · MinIO · Apache Airflow · Apache Kafka · Apache Spark · Streamlit · FastAPI · Scikit-learn · XGBoost · Docker

---

## Project Overview

LogiFlow is a full-stack data platform that simulates a production logistics intelligence system.
It covers the complete data lifecycle — from batch ingestion and warehousing, through analytics and
machine learning, to real-time streaming — organized across four progressive MVPs.

Each MVP reflects how production data systems are actually built: one layer at a time, with each
layer providing a foundation for the next.

**What this project demonstrates:**
- Data engineering: ETL pipelines, star schema warehousing, data lake staging
- Analytics engineering: dashboards, REST APIs, automated quality checks
- Machine learning: classification models, feature engineering, ML in production
- Streaming: Kafka event ingestion, Spark Structured Streaming, real-time enrichment
- Platform engineering: Docker orchestration, Airflow DAGs, modular architecture

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          LOGIFLOW PLATFORM                               │
│                                                                          │
│  Data Sources               Staging              Warehouse               │
│  ┌─────────────┐          ┌─────────┐          ┌────────────────────┐   │
│  │ Synthetic   │          │         │   ETL     │    PostgreSQL      │   │
│  │ Data Gen    │─────────►│  MinIO  │──────────►│   (Star Schema)   │   │
│  │             │          │  (S3)   │           │                    │   │
│  │ Live Weather│          └─────────┘           └────────┬───────────┘   │
│  │ (OWM API)   │                                         │               │
│  └─────────────┘                                         │               │
│                                                          │               │
│  Streaming Sources                                       │               │
│  ┌─────────────┐  events   ┌─────────┐  Spark   ┌───────▼──────────┐   │
│  │ Kafka       │──────────►│  Kafka  │─────────►│  realtime_       │   │
│  │ Producer    │           │ (KRaft) │ Streaming │  shipments       │   │
│  └─────────────┘           └─────────┘           └──────────────────┘   │
│                                                                          │
│  Orchestration                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Apache Airflow 2.8   logiflow_daily_pipeline DAG (daily 02:00)  │   │
│  │  generate_data → upload_minio → run_etl → quality → retrain → summary │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  Consumption Layer                                                       │
│  ┌────────────┐    ┌──────────────┐    ┌─────────────────────────┐     │
│  │ Streamlit  │    │   FastAPI    │    │   XGBoost ML Predictor  │     │
│  │ Dashboard  │    │   REST API   │    │   /predict/delay         │     │
│  └────────────┘    └──────────────┘    └─────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Category | Technology | Version | Purpose |
|----------|-----------|---------|---------|
| Infrastructure | Docker, Docker Compose | 24.x / 2.x | Container orchestration |
| Data Lake | MinIO | latest | S3-compatible object store for raw CSV staging |
| Data Warehouse | PostgreSQL | 15 | Star schema analytical warehouse |
| Data Modeling | Star Schema (5 tables) | — | Optimized analytical query design |
| ETL | Python, pandas, boto3 | 3.11 | Extract → Transform → Load pipeline |
| Orchestration | Apache Airflow | 2.8.0 | DAG scheduling and task monitoring |
| Event Streaming | Apache Kafka | 3.7.0 | Message broker in KRaft mode (no Zookeeper) |
| Stream Processing | Apache Spark | 3.5.1 | Structured Streaming with micro-batching |
| Kafka Producer | kafka-python | 2.x | Python event generator (1 event/second) |
| Dashboard | Streamlit, Plotly | — | Interactive analytics dashboard |
| REST API | FastAPI, Pydantic | — | 10 endpoints with Swagger UI |
| Machine Learning | Scikit-learn, XGBoost | — | Binary delay prediction classifier |
| Scheduling (v1) | APScheduler | — | Pre-Airflow automated pipeline (MVP 2) |
| Data Quality | Custom checks (8) | — | Post-ETL validation framework |
| Weather Data | OpenWeatherMap API | — | Real weather enrichment (15 cities) |
| ORM / DB Driver | SQLAlchemy, psycopg2 | — | PostgreSQL connectivity |
| Configuration | python-dotenv | — | Environment-based secrets management |

---

## MVP Roadmap

### MVP 1 — Data Pipeline Foundation

> *"Build the warehouse before filling it."*

Designed and implemented a complete batch data pipeline from raw files to a structured warehouse.

**What was built:**
- Docker Compose infrastructure: PostgreSQL 15 + MinIO (S3-compatible)
- Star schema data warehouse: `fact_shipments` + `dim_date`, `dim_customer`, `dim_driver`, `dim_vehicle`, `dim_route`
- Python ETL pipeline: Extract (MinIO via boto3) → Transform (pandas) → Load (PostgreSQL via psycopg2)
- Synthetic data generator producing 10,000+ realistic shipment records with domain logic
- Data lake staging in MinIO before insertion (enabling pipeline replayability)

**Key design decisions:**
- Star schema over 3NF for analytical query performance
- Surrogate keys on all dimension tables
- Performance indexes on `fact_shipments` for status, date, and delay filtering

**Skills demonstrated:**
`Data Modeling` · `ETL Development` · `Docker` · `PostgreSQL` · `MinIO / S3` · `pandas` · `Star Schema`

---

### MVP 2 — Analytics Layer

> *"Raw warehouse data has no value until it is exposed."*

Built four independent analytics modules on top of the warehouse.

#### 2A — Interactive Dashboard (Streamlit)
- 7-section dashboard with 18+ Plotly charts
- Dynamic sidebar filters: year, status, region, customer segment
- KPI cards: on-time rate, total revenue, delay analysis, driver ratings
- Sections: delivery performance, cost analysis, route geography, weather impact
- CSV export for filtered datasets

#### 2B — Automated Scheduler (APScheduler)
- 4 scheduled pipeline jobs with cron-style timing
- Daily ETL at 02:00 UTC, quality checks at 02:30 UTC
- Incremental data generation every 6 hours
- Heartbeat monitoring every 10 minutes with rotating log files

#### 2C — Data Quality Framework
- 8 automated post-ETL validation checks:
  `row_counts` · `null_foreign_keys` · `invalid_status_values` · `negative_numerics`
  `delay_flag_consistency` · `rating_range_check` · `orphan_records` · `duplicate_shipments`
- Pass/fail report generated per run with detailed logging

#### 2D — REST API (FastAPI)
- 10 RESTful endpoints with auto-generated Swagger UI at `/docs`
- Paginated shipment queries with status filtering
- KPI endpoints: summary, by-month, by-region
- Driver profiles, route performance, weather impact analysis
- `/predict/delay` endpoint exposing the ML model

**Skills demonstrated:**
`Streamlit` · `Plotly` · `FastAPI` · `Pydantic` · `SQLAlchemy` · `APScheduler` · `Data Quality`

---

### MVP 3 — Intelligence & Orchestration

> *"Make the platform smart and production-ready."*

#### 3A — Machine Learning: Delay Prediction
- Binary classification: predicts whether a shipment will be delayed
- 4 models compared: Logistic Regression, Random Forest, Gradient Boosting, XGBoost
- Feature engineering: `vehicle_age`, `load_ratio`, `cost_per_km`, `mileage_per_year`
- 5-fold cross-validation with ROC-AUC scoring and confusion matrix analysis
- Best model serialized with full preprocessing pipeline (encoders + scaler) via `joblib`
- Risk levels returned by API: LOW / MEDIUM / HIGH with probability score

#### 3B — Real Weather Data Integration
- Live weather from OpenWeatherMap API for 15 global logistics cities
- Delay probability dynamically adjusted based on temperature, wind, and precipitation
- Graceful fallback to simulation when API key is not configured
- Seamlessly integrated into the data generation pipeline

#### 3C — Airflow DAG Orchestration
- Replaced APScheduler with Apache Airflow for full task dependency management
- DAG: `logiflow_daily_pipeline` — 6 tasks, scheduled daily at 02:00 UTC
- Task chain: `generate_data → upload_to_minio → run_etl → quality_check → retrain_model → pipeline_summary`
- All tasks verified green; quality check reports 8/8 checks passing

**Skills demonstrated:**
`Apache Airflow` · `DAG Design` · `Scikit-learn` · `XGBoost` · `Feature Engineering`
`Cross-Validation` · `joblib` · `ML in Production` · `REST API Consumption`

---

### MVP 4 — Real-Time Streaming

> *"Transform the platform from batch to real-time."*

Added a full streaming layer using Apache Kafka and Spark Structured Streaming,
delivering continuous event ingestion and enrichment into PostgreSQL.

#### Architecture: Kappa Pattern

```
kafka_producer.py
  └─ 1 JSON event/second → Kafka topic: shipment_events
       │
       ▼
Apache Kafka 3.7.0 (KRaft mode — no Zookeeper)
  └─ Internal: kafka:9092 | External: localhost:9094
       │
       ▼
Spark Structured Streaming (10-second micro-batches)
  ├─ Parse JSON payload
  ├─ Compute cost_per_km = revenue / distance_km
  ├─ Compute weather_risk (LOW / MEDIUM / HIGH)
  └─ Write to PostgreSQL: realtime_shipments
```

#### Key Components

| Component | Image | Description |
|-----------|-------|-------------|
| Kafka Broker | `apache/kafka:3.7.0` | KRaft mode — no Zookeeper dependency |
| Spark Master | `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu` | Cluster manager |
| Spark Worker | same | Executes streaming tasks |
| Producer | Custom (`python:3.11-slim`) | Publishes 1 event/second |
| Streaming Job | Custom (Spark image) | Reads Kafka, enriches, writes PostgreSQL |
| Kafka UI | `provectuslabs/kafka-ui` | Optional topic browser (profile: ui) |

#### Results
- 6,000+ rows confirmed in `realtime_shipments` after initial run
- Continuous ingestion at ~360 events/hour
- Enriched fields computed in-stream: `cost_per_km`, `weather_risk`
- Streaming table queryable alongside batch data for unified analytics

**Skills demonstrated:**
`Apache Kafka` · `KRaft` · `Apache Spark` · `PySpark` · `Structured Streaming`
`Real-Time Pipelines` · `Event-Driven Architecture` · `Docker Multi-Compose`

---

## Repository Structure

```
Global-Logistics-Data-Platform/
├── README.md
└── logiflow/
    ├── docker-compose.yml               # Core services (postgres, minio, airflow, api)
    ├── docker-compose.streaming.yml     # MVP4 streaming (kafka, spark, producer)
    ├── requirements.txt
    ├── .env.example
    │
    ├── docs/
    │   ├── architecture.md              # Full system architecture & data flows
    │   ├── data-model.md                # Schema reference for all tables
    │   └── setup-guide.md              # Step-by-step deployment guide
    │
    ├── mvp1-data-pipeline/
    │   ├── schema.sql                   # Star schema DDL + realtime_shipments
    │   ├── data-generation/
    │   │   └── generate_data.py         # Synthetic data generator
    │   └── ingestion/
    │       ├── upload_to_minio.py       # MinIO staging upload
    │       └── etl_pipeline.py          # Extract → Transform → Load
    │
    ├── mvp2-analytics-layer/
    │   ├── 2A-dashboard/
    │   │   ├── app.py                   # Streamlit dashboard (7 sections, 18+ charts)
    │   │   ├── db_connector.py          # PostgreSQL → pandas
    │   │   └── utils.py
    │   ├── 2B-scheduler/
    │   │   └── scheduler.py             # APScheduler (4 jobs)
    │   ├── 2C-data-quality/
    │   │   └── quality_checks.py        # 8 validation checks
    │   └── 2D-api/
    │       └── main.py                  # FastAPI (10 endpoints + ML)
    │
    ├── mvp3-advanced/
    │   ├── 3A-ml-prediction/
    │   │   ├── train.py                 # Model training (4 classifiers)
    │   │   ├── predict.py               # Inference module
    │   │   ├── models/delay_predictor.pkl
    │   │   └── reports/                 # ROC, confusion matrix, feature importance plots
    │   ├── 3B-real-data/
    │   │   └── real_data_fetcher.py     # OpenWeatherMap API integration
    │   └── 3C-airflow-orchestration/
    │       └── dags/
    │           └── logiflow_pipeline.py # Airflow DAG (6 tasks)
    │
    └── mvp4-streaming/
        ├── kafka_producer.py            # Kafka event publisher (1 event/second)
        ├── spark_streaming.py           # Spark Structured Streaming job
        ├── Dockerfile.producer          # Producer container
        ├── Dockerfile.spark_job         # Spark job container
        ├── requirements.producer.txt
        └── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop (running) with Docker Compose v2
- Python 3.10+ (for local script execution)
- WSL2 recommended on Windows

### 1. Configure environment

```bash
cd logiflow
cp .env.example .env
# Edit .env with your PostgreSQL, MinIO, and Airflow credentials
```

### 2. Start the core stack (MVPs 1–3)

```bash
docker compose up -d
```

Services started: PostgreSQL · MinIO · Airflow · FastAPI

Wait ~60 seconds, then access Airflow at [http://localhost:8080](http://localhost:8080).

### 3. Apply the schema and run the pipeline

The Airflow DAG `logiflow_daily_pipeline` orchestrates everything. Trigger it manually:

1. Open [http://localhost:8080](http://localhost:8080)
2. Toggle `logiflow_daily_pipeline` ON
3. Click the **Trigger DAG** button (▶)

All 6 tasks should turn green within 3–5 minutes.

### 4. Start the streaming layer (MVP 4)

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
```

Services added: Kafka · Spark Master · Spark Worker · Producer · Spark Streaming Job

### 5. Verify end-to-end

```bash
# Batch data
docker exec logiflow_postgres psql -U logiflow_user -d logiflow \
  -c "SELECT COUNT(*) FROM fact_shipments;"

# Real-time data
docker exec logiflow_postgres psql -U logiflow_user -d logiflow \
  -c "SELECT COUNT(*) FROM realtime_shipments;"
```

---

## Service Access

| Service | URL | Notes |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | DAG monitoring |
| MinIO Console | http://localhost:9001 | Object store browser |
| FastAPI Swagger | http://localhost:8000/docs | REST API + ML prediction |
| Streamlit Dashboard | http://localhost:8501 | Run locally (see docs) |
| Spark Master UI | http://localhost:8082 | Cluster status |
| Spark Worker UI | http://localhost:8083 | Worker metrics |
| Kafka UI | http://localhost:8090 | Optional (`--profile ui`) |

---

## Key Results

| Metric | Value |
|--------|-------|
| Batch shipments in warehouse | 10,000+ |
| Real-time events ingested | 6,000+ (continuous) |
| Dashboard sections | 7 |
| Charts & visualizations | 18+ |
| API endpoints | 10 |
| Data quality checks | 8 |
| Airflow DAG tasks | 6 (all green) |
| ML classifiers compared | 4 |
| Features engineered | 25+ |
| Cities with real weather | 15 |
| Kafka throughput | ~360 events/hour |
| Spark micro-batch interval | 10 seconds |

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture.md](logiflow/docs/architecture.md) | System architecture, component inventory, data flow diagrams |
| [docs/data-model.md](logiflow/docs/data-model.md) | Full schema reference: all tables, columns, types, indexes |
| [docs/setup-guide.md](logiflow/docs/setup-guide.md) | Step-by-step deployment guide for all MVPs |
| [logiflow/AIRFLOW_SETUP.md](logiflow/AIRFLOW_SETUP.md) | Airflow-specific configuration notes |

---

## What I Learned

This project was built progressively, each MVP exposing a new engineering challenge:

**MVP 1** — Data modeling decisions made early have massive downstream impact.
A star schema designed at the start still serves analytical queries, ML training, and streaming
sinks three MVPs later without restructuring.

**MVP 2** — Raw data in a warehouse has no value until exposed.
Building dashboards, APIs, and quality checks on the same schema taught me how different
consumers have completely different requirements from the same data.

**MVP 3** — Orchestration is not optional at scale.
Replacing APScheduler with Airflow revealed the difference between *automation* and *orchestration*:
dependency management, retries, observability, and alerting are not afterthoughts.

**MVP 4** — Streaming requires a different mental model.
Batch pipelines ask "what happened?". Streaming asks "what is happening right now?"
Kafka's decoupling and Spark's micro-batching make this tractable — but the operational
complexity (KRaft, healthchecks, Spark packages, exactly-once semantics) is real.

**The biggest lesson:** A data project is never just about the data.
It is about the pipeline that moves it, the warehouse that structures it,
the quality checks that validate it, the models that learn from it,
and the streams that deliver it — all running reliably, together, in production.

---

## Contact

**Hibatallah Chmicha**  
Data Science Student @ INSEA  
[GitHub](https://github.com/hibatallahchmicha) · [LinkedIn](https://linkedin.com/in/hibatallahchmicha)