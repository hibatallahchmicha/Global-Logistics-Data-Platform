# LogiFlow — Setup Guide

> Step-by-step instructions to deploy the full LogiFlow platform locally,
> covering the core stack (MVPs 1–3) and the real-time streaming layer (MVP 4).

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Structure](#2-repository-structure)
3. [Environment Configuration](#3-environment-configuration)
4. [Core Stack Setup (MVPs 1–3)](#4-core-stack-setup-mvps-13)
5. [Verify the Pipeline](#5-verify-the-pipeline)
6. [Streaming Stack Setup (MVP 4)](#6-streaming-stack-setup-mvp-4)
7. [Access All Services](#7-access-all-services)
8. [Running the Analytics Layer](#8-running-the-analytics-layer)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

Ensure the following are installed and running before you begin:

| Requirement | Minimum Version | Notes |
|-------------|----------------|-------|
| Docker Desktop | 24.x | Must be running |
| Docker Compose | 2.x (plugin) | Use `docker compose` not `docker-compose` |
| Python | 3.10+ | For running local scripts |
| Git | Any | To clone the repository |

> **Windows users:** All commands assume WSL2 (Ubuntu). Run them in a WSL terminal,
> not PowerShell or CMD. The project path in WSL will be under `/mnt/c/Users/...`.

---

## 2. Repository Structure

```
Global-Logistics-Data-Platform/
├── README.md                        ← Project overview
└── logiflow/
    ├── docker-compose.yml           ← Core services (postgres, minio, airflow, api)
    ├── docker-compose.streaming.yml ← MVP4 streaming services (kafka, spark, producer)
    ├── .env                         ← Your local secrets (not committed)
    ├── .env.example                 ← Template for .env
    ├── requirements.txt             ← Python dependencies
    ├── docs/                        ← This documentation
    ├── mvp1-data-pipeline/          ← Schema, ETL, data generation
    ├── mvp2-analytics-layer/        ← Dashboard, API, scheduler, quality checks
    ├── mvp3-advanced/               ← ML model, real data, Airflow orchestration
    └── mvp4-streaming/              ← Kafka producer, Spark streaming job
```

---

## 3. Environment Configuration

### 3A. Copy the example file

```bash
cd logiflow
cp .env.example .env
```

### 3B. Edit `.env` with your values

```env
# PostgreSQL
POSTGRES_USER=logiflow_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=logiflow
POSTGRES_HOST=logiflow_postgres
POSTGRES_PORT=5432

# MinIO (S3-compatible object store)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_minio_password
MINIO_ENDPOINT=http://logiflow_minio:9000
MINIO_BUCKET=logiflow-raw

# Airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=your_airflow_password
AIRFLOW__CORE__FERNET_KEY=your_fernet_key   # generate via: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Kafka (MVP 4)
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=shipment_events

# Optional: OpenWeatherMap for real weather enrichment
OPENWEATHER_API_KEY=your_api_key_here
```

---

## 4. Core Stack Setup (MVPs 1–3)

### Step 1: Navigate to the logiflow directory

```bash
cd "/path/to/Global-Logistics-Data-Platform/logiflow"
```

### Step 2: Start the core services

```bash
docker compose up -d
```

This starts:
- `logiflow_postgres` — PostgreSQL 15 on port 5432
- `logiflow_minio` — MinIO on ports 9000/9001
- `logiflow_airflow` — Airflow 2.8.0 on port 8080
- `logiflow_api` — FastAPI on port 8000

Wait ~60 seconds for all services to initialize.

### Step 3: Apply the database schema

The schema is applied automatically on first startup via Airflow's init container.
To apply it manually if needed:

```bash
docker exec -i logiflow_postgres psql -U logiflow_user -d logiflow \
  < mvp1-data-pipeline/schema.sql
```

### Step 4: Create the MinIO bucket

Open the MinIO Console at [http://localhost:9001](http://localhost:9001),
log in with your `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, and create a bucket named `logiflow-raw`.

Or create it programmatically:
```bash
docker exec logiflow_minio mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
docker exec logiflow_minio mc mb local/logiflow-raw
```

### Step 5: Trigger the Airflow DAG

1. Open [http://localhost:8080](http://localhost:8080)
2. Log in with your Airflow credentials
3. Find the DAG: `logiflow_daily_pipeline`
4. Toggle it ON (the blue switch)
5. Click the ▶ (Trigger DAG) button

The DAG runs 6 tasks in sequence:
```
generate_data → upload_to_minio → run_etl → quality_check → retrain_model → pipeline_summary
```

All tasks should turn green within 3–5 minutes.

---

## 5. Verify the Pipeline

### Check task status

In the Airflow UI, all 6 task circles should be **dark green** (success).

### Verify data was loaded

```bash
docker exec logiflow_postgres psql -U logiflow_user -d logiflow \
  -c "SELECT COUNT(*) FROM fact_shipments;"
```

Expected output: `10000+` rows.

### Check data quality results

The Airflow logs for `quality_check` task show all 8 checks. You can also run:

```bash
docker exec logiflow_postgres psql -U logiflow_user -d logiflow \
  -c "SELECT status, COUNT(*) FROM fact_shipments GROUP BY status;"
```

### Verify ML model was trained

```bash
ls mvp3-advanced/3A-ml-prediction/models/
# Should contain: delay_predictor.pkl
```

---

## 6. Streaming Stack Setup (MVP 4)

### Step 1: Start all services (core + streaming)

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
```

This adds 5 services on top of the core stack:
- `logiflow_kafka` — Apache Kafka 3.7.0 in KRaft mode
- `logiflow_spark_master` — Spark cluster master (UI: port 8082)
- `logiflow_spark_worker` — Spark worker node (UI: port 8083)
- `logiflow_shipment_producer` — Kafka producer (1 event/second)
- `logiflow_spark_streaming` — Spark Structured Streaming job

### Step 2: Wait for Kafka to become healthy

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep logiflow
```

The `logiflow_kafka` container should show `(healthy)` status.
This takes approximately 30 seconds.

### Step 3: Verify events are flowing

Check producer logs:
```bash
docker logs logiflow_shipment_producer --tail 20
```
You should see JSON events being published every second.

Check streaming job logs:
```bash
docker logs logiflow_spark_streaming --tail 30
```
You should see batch processing output every 10 seconds.

### Step 4: Confirm data in PostgreSQL

```bash
docker exec logiflow_postgres psql -U logiflow_user -d logiflow \
  -c "SELECT COUNT(*) FROM realtime_shipments;"
```

The count increases by ~6 every 10 seconds (micro-batch interval).

### Step 5 (Optional): Launch Kafka UI

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml \
  --profile ui up -d kafka-ui
```

Access the Kafka UI at [http://localhost:8090](http://localhost:8090)
to browse the `shipment_events` topic and monitor message throughput.

---

## 7. Access All Services

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| Airflow UI | http://localhost:8080 | From `.env`: AIRFLOW_ADMIN_USERNAME / PASSWORD |
| MinIO Console | http://localhost:9001 | From `.env`: MINIO_ROOT_USER / PASSWORD |
| FastAPI Swagger | http://localhost:8000/docs | No auth required |
| Spark Master UI | http://localhost:8082 | No auth required |
| Spark Worker UI | http://localhost:8083 | No auth required |
| Kafka UI | http://localhost:8090 | No auth required (optional profile) |
| PostgreSQL | localhost:5432 | From `.env`: POSTGRES_USER / PASSWORD |

---

## 8. Running the Analytics Layer

### Streamlit Dashboard (local)

```bash
cd mvp2-analytics-layer/2A-dashboard
pip install -r requirements.txt
streamlit run app.py
# → http://localhost:8501
```

### FastAPI (already containerized)

The API is available at [http://localhost:8000/docs](http://localhost:8000/docs).

Key endpoints:
- `GET /shipments` — paginated shipment list (filter by status)
- `GET /kpi/summary` — overall KPI metrics
- `GET /kpi/by-month` — revenue and delay trends by month
- `POST /predict/delay` — ML delay prediction (JSON payload with shipment features)

### Predicting a Delay

```bash
curl -X POST http://localhost:8000/predict/delay \
  -H "Content-Type: application/json" \
  -d '{
    "distance_km": 850,
    "weight_kg": 2500,
    "vehicle_age": 5,
    "driver_experience": 3,
    "driver_rating": 3.8,
    "weather_condition": "Rain",
    "route_type": "Road"
  }'
```

Response:
```json
{
  "prediction": "delayed",
  "probability": 0.73,
  "risk_level": "HIGH"
}
```

---

## 9. Troubleshooting

### Kafka container shows unhealthy

The healthcheck runs `/opt/kafka/bin/kafka-topics.sh`. Ensure the container is using
`apache/kafka:3.7.0` (not a Bitnami image). If still failing, wait 60 seconds and re-check.

### Spark job exits immediately

Check for missing Python packages:
```bash
docker logs logiflow_spark_streaming 2>&1 | grep -i error
```

Ensure `spark_streaming.py` uses `/opt/spark/bin/spark-submit` in the Dockerfile CMD.

### realtime_shipments table not found

The table must be created before Spark tries to write. Apply the schema manually:
```bash
docker exec -i logiflow_postgres psql -U logiflow_user -d logiflow \
  < mvp1-data-pipeline/schema.sql
```

### Airflow tasks failing with "exit code 1"

Check the task log in the Airflow UI. Common causes:
- `.env` not mounted or missing variables → verify docker-compose volume mounts
- MinIO bucket doesn't exist → create `logiflow-raw` bucket manually
- PostgreSQL not yet ready → wait and re-trigger the DAG

### ETL loads 0 rows (quality check fails)

Ensure MinIO contains CSV files. The `generate_data` task should upload them.
Check MinIO at http://localhost:9001 → bucket `logiflow-raw`.

### Stopping all services

```bash
# Stop core stack only
docker compose down

# Stop everything including streaming
docker compose -f docker-compose.yml -f docker-compose.streaming.yml down

# Stop and remove all volumes (WARNING: destroys all data)
docker compose -f docker-compose.yml -f docker-compose.streaming.yml down -v
```
