# LogiFlow — System Architecture

> Technical architecture as it actually exists after the structural rebuild — organized
> by role (shared library / pipeline / service / orchestration / streaming), not by the
> order things were originally built in.

---

## 1. Design Principle: One Source of Truth Per Concern

The original version of this project had configuration duplicated across ~6 files, three
independent MinIO client constructions, and two incompatible synthetic data generators.
The rebuild's organizing rule: every concern lives in exactly one place, and everything
else imports it.

| Concern | Single source of truth |
|---|---|
| Environment/config | `common/config.py` — fails fast if a required var is missing |
| Object storage access | `common/storage.py` — the only file that touches the MinIO SDK directly |
| Warehouse structure | `infra/schema.sql` — applied once via `docker-entrypoint-initdb.d` |
| Feature engineering | `ml/train.py`'s logic, mirrored exactly in `ml/predict.py` |
| Business logic | `pipelines/` and `ml/` modules — the Airflow DAG only calls them, never reimplements them |

---

## 2. Component Inventory

### Core Services (`docker-compose.yml`)

| Service | Image / Build | Port(s) | Role |
|---|---|---|---|
| `logiflow_postgres` | `postgres:15` | 5432 | Warehouse + Airflow metadata DB |
| `logiflow_minio` | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | 9000/9001 | Object storage staging |
| `logiflow_airflow` | `apache/airflow:2.8.0-python3.11` | 8080 | Daily pipeline orchestration |
| `logiflow_api` | build: repo root, `services/api/Dockerfile` | 8000 | FastAPI REST API + ML serving |
| `logiflow_dashboard` | build: repo root, `services/dashboard/Dockerfile` | 8501 | Streamlit analytics dashboard |

The API and dashboard Dockerfiles build from the **repo root**, not their own
subdirectory — this is deliberate. The original version built the API from just its own
folder, which meant the code that imported `ml.predict` had no way to correctly locate it
inside the container and crashed on startup. Building from the root lets both Dockerfiles
copy `common/` (and `ml/`, for the API) in as real siblings, mirroring the local layout
exactly — no path math required.

### Streaming Services (`docker-compose.streaming.yml`)

| Service | Image / Build | Role |
|---|---|---|
| `logiflow_kafka` | `apache/kafka:3.7.0` | KRaft mode, no Zookeeper |
| `logiflow_spark_master` / `logiflow_spark_worker` | `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu` | Spark cluster |
| `logiflow_shipment_producer` | build: repo root, `streaming/Dockerfile.producer` | Publishes synthetic events |
| `logiflow_spark_streaming` | build: `streaming/`, `Dockerfile.spark_job` | Consumes, enriches, upserts to Postgres |
| `logiflow_kafka_ui` (optional, `profile: ui`) | `provectuslabs/kafka-ui:v0.7.2` | Topic browser |

---

## 3. Data Flow

### 3A — Batch Pipeline

```
pipelines/generate_shipments.py
  ├─ fixed roster of 8 customers / 8 drivers / 8 vehicles / 10 routes
  │  (reused every run, not regenerated -- dimension tables need stable identity)
  ├─ real weather (OpenWeatherMap) + real traffic (TomTom), cached per city,
  │  only for days_back <= 7 -- a live snapshot API cannot answer for the past,
  │  so historical seeding always uses simulated (but realistically distributed) data
  ├─ tags every shipment with source_shipment_id (UUID, assigned at creation)
  └─ uploads one flat CSV to MinIO: raw/shipments_<timestamp>.csv

pipelines/etl.py
  ├─ reads every raw/shipments_*.csv currently in MinIO (accumulates, no truncation)
  ├─ upserts each dimension on its natural key (company_name, full_name, plate_number,
  │  origin+destination pair) -- ON CONFLICT DO NOTHING, then looks up the surrogate ID
  └─ upserts fact_shipments ON CONFLICT (source_shipment_id) DO NOTHING
     -- re-running this on the same files is a no-op, verified by running it twice
        back-to-back and confirming the row count didn't change

pipelines/quality_checks.py
  └─ 8 checks, split into CRITICAL_CHECKS (row_counts, null_foreign_keys,
     orphan_records -- stop the pipeline) and non-critical (log and continue)

ml/train.py
  └─ loads the warehouse via SQLAlchemy `conn.execute(text(...))` (not pd.read_sql --
     see the note below), trains 4 models, saves the best by ROC-AUC
```

**Why `ml/train.py` doesn't use `pd.read_sql(query, engine)`:** inside the Airflow
container, `pandas` and the SQLAlchemy version Airflow itself depends on internally don't
agree on how `pd.read_sql` should detect a valid connection — it raised
`AttributeError: 'Connection' object has no attribute 'cursor'` in practice. The fix was
to stop relying on pandas' internal SQLAlchemy-connectable detection entirely and build
the DataFrame directly from `conn.execute(text(query)).fetchall()` — the same pattern
`pipelines/quality_checks.py` already used successfully in the same environment.

### 3B — Streaming Pipeline (independent path)

```
streaming/kafka_producer.py
  └─ synthetic shipment events, acks="all" + retries=3 (at-least-once delivery --
     duplicates are an expected condition, not a hypothetical one)
       │
       ▼
Kafka (shipment_events topic)
       │
       ▼
streaming/spark_streaming.py
  ├─ parses JSON, computes cost_per_km and weather_risk
  └─ foreachPartition upsert: INSERT ... ON CONFLICT (event_id) DO NOTHING
     -- not a plain JDBC append. A duplicate event_id from producer retries would
        violate realtime_shipments' UNIQUE constraint and crash a plain-append sink;
        this doesn't, because it's a real upsert, not a claim of one.
```

Uses plain `os.getenv()`, not `common.config` — the one deliberate exception. Spark
distributes this code across driver and executor processes; `common.config`'s
assumptions (repo root on `sys.path`, `.env` at a fixed relative path) don't reliably
hold across that boundary. Config is passed in as explicit environment variables by
`spark-submit`/Docker instead.

**Honest status:** built and individually correct, but not yet verified end-to-end
against the current `docker-compose.streaming.yml`, and nothing downstream (API,
dashboard) currently reads `realtime_shipments`.

### 3C — Orchestration

```
orchestration/dags/logiflow_pipeline.py
  generate_shipments >> run_etl >> quality_check >> retrain_model
```

Every task function is a thin wrapper: `from pipelines.etl import run; run()`. No
business logic lives in the DAG file — the original version had ~100 lines of CSV-
splitting logic embedded in one Airflow operator, untestable outside Airflow. If a
module works when run standalone (`python -m pipelines.etl`), the DAG task does exactly
that, because it *is* that call.

`retries: 1`, `retry_delay: 5 minutes`, `email_on_failure: False` — retry logic is real
and was observed firing correctly during development; failure alerting is not
implemented, a known gap.

---

## 4. Infrastructure Notes

### Dependency isolation inside the Airflow container

Installing pipeline dependencies (`pandas`, `scikit-learn`, `xgboost`, etc.) directly
into the Airflow image is riskier than it looks: Airflow pins its own internal
dependencies strictly (notably `SQLAlchemy<2.0`), and a naive `pip install` upgrading
those breaks Airflow's own database engine setup. `orchestration/entrypoint.sh` installs
via `pip install --no-deps`, with every package pinned explicitly in
`orchestration/requirements.txt` (including transitive dependencies that `--no-deps`
otherwise skips, like `argon2-cffi-bindings` for the `minio` package's crypto module) —
this avoids fighting Airflow's own dependency resolution entirely, at the cost of having
to enumerate transitive dependencies by hand.

### Image pinning

The official `minio/minio` Docker Hub repository was archived in April 2026 — MinIO
stopped publishing pre-compiled images in October 2025. `docker-compose.yml` pins to the
last real release, `RELEASE.2025-09-07T16-13-09Z`, rather than `:latest`. This is also a
concrete argument for the `common/storage.py` abstraction: when this image eventually
needs replacing, exactly one file changes.

### What's not yet done

- No drift detection, no retrain-quality gate (a worse model silently replaces a better
  one), no alerting on pipeline failure.
- Model artifacts aren't versioned — each retrain overwrites the only saved `.pkl`.
- Streaming stack not yet verified end-to-end against this structure.
- No CI/CD yet (planned as the next piece of work, after the structure stabilizes).
