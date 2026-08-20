# LogiFlow — End-to-End Logistics Data Platform

> A data engineering, machine learning, and real-time streaming platform for logistics
> analytics — synthetic data generation, a star-schema warehouse, an ML delay predictor,
> a REST API, a dashboard, daily orchestration, and an independent Kafka/Spark streaming
> layer, all containerized.

**Author:** Hibatallah Chmicha · Data Science Student @ INSEA
**Stack:** Python · PostgreSQL · MinIO · Apache Airflow · Apache Kafka · Apache Spark · Streamlit · FastAPI · Scikit-learn · XGBoost · Docker

---

## Project Overview

LogiFlow simulates a production logistics intelligence system end to end: synthetic
shipment generation (enriched with two live external APIs), a star-schema warehouse
loaded idempotently, an ML delay classifier, a REST API and dashboard on top of it, a
daily Airflow pipeline tying it together, and an independent real-time streaming path.

The repository was rebuilt from an earlier chronological (`mvp1`–`mvp4`) layout into one
organized by actual architectural role: a shared library, pipeline scripts, the ML
package, deployable services, orchestration, and the streaming layer. Every module was
verified individually before being wired together — the sections below reflect what was
actually run and observed, not aspirational claims.

**What this project demonstrates:**
- Data engineering: idempotent ELT, star-schema warehousing, object-storage staging with a swappable storage abstraction
- Analytics engineering: a REST API, a dashboard, and an 8-check automated data quality suite
- Machine learning: a real train/serve feature contract (no silent fallbacks), model comparison, cross-validated evaluation
- Orchestration: an Airflow DAG that calls independently-tested modules, not logic embedded in operators
- Streaming: Kafka event ingestion, Spark Structured Streaming with an idempotent upsert sink
- Platform engineering: multi-service Docker Compose, pinned dependencies, environment separation

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          LOGIFLOW PLATFORM                               │
│                                                                          │
│  pipelines/generate_shipments.py                                        │
│  ├─ fixed customer/driver/vehicle/route roster (stable identity)        │
│  ├─ real weather (OpenWeatherMap) + real traffic (TomTom), cached/city  │
│  │   for near-term batches; simulated for historical seeding            │
│  └─ uploads to MinIO: raw/shipments_<timestamp>.csv                     │
│                 │                                                        │
│                 ▼                                                        │
│  pipelines/etl.py                                                       │
│  ├─ reads every raw/shipments_*.csv file (accumulates, never wipes)     │
│  ├─ upserts dimensions on natural keys (company/driver/plate/route)     │
│  └─ upserts fact_shipments on source_shipment_id -- reruns are safe      │
│                 │                                                        │
│                 ▼                                                        │
│  PostgreSQL (star schema) ──┬──► pipelines/quality_checks.py (8 checks) │
│                              │                                           │
│                              ├──► ml/train.py → ml/predict.py            │
│                              │      (enforced train/serve contract)      │
│                              │                                           │
│                              ├──► services/api  (FastAPI, 10 endpoints)  │
│                              │                                           │
│                              └──► services/dashboard (Streamlit)         │
│                                                                          │
│  orchestration/dags/logiflow_pipeline.py (Airflow, daily 02:00 UTC)     │
│  generate_shipments >> run_etl >> quality_check >> retrain_model         │
│  -- every task imports and calls an independently-tested module          │
│                                                                          │
│  streaming/ (independent path, not read by the API or dashboard yet)    │
│  kafka_producer.py → Kafka → spark_streaming.py → realtime_shipments    │
│  -- idempotent upsert sink (ON CONFLICT on event_id), not plain append   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Category | Technology | Purpose |
|----------|-----------|---------|
| Infrastructure | Docker, Docker Compose | Multi-service container orchestration |
| Object Storage | MinIO | S3-compatible staging layer, swappable via `common/storage.py` |
| Data Warehouse | PostgreSQL 15 | Star schema, natural-key uniqueness for idempotent loads |
| Orchestration | Apache Airflow 2.8.0 | Daily DAG calling independently-tested modules |
| Event Streaming | Apache Kafka 3.7.0 | KRaft mode, no Zookeeper |
| Stream Processing | Apache Spark 3.5.1 | Structured Streaming, idempotent upsert sink |
| Dashboard | Streamlit, Plotly | Containerized (was manual-only before this rebuild) |
| REST API | FastAPI, Pydantic | 10 endpoints, enforced request schema |
| Machine Learning | Scikit-learn, XGBoost | 4-model comparison, cross-validated |
| Data Quality | Custom, 8 checks | Critical vs. non-critical, feeds the DAG's failure logic |
| Live Enrichment | OpenWeatherMap + TomTom Traffic | Real APIs, cached per city, simulated for historical backfill |
| Config | `common/config.py` | Single source of truth, fail-fast on missing required vars |

---

## Repository Structure

```
Global-Logistics-Data-Platform/
├── README.md
└── logiflow/
    ├── docker-compose.yml                # postgres, minio, airflow, api, dashboard
    ├── docker-compose.streaming.yml      # kafka, spark, producer, spark-streaming
    ├── .env.example
    │
    ├── common/                # shared library -- everything below imports from here
    │   ├── config.py          # single source of truth for env vars, fail-fast
    │   └── storage.py         # single MinIO/S3 client (the seam for a future S3 migration)
    │
    ├── infra/
    │   └── schema.sql         # star schema DDL, single source of truth for table structure
    │
    ├── pipelines/              # scripts that run, do a job, and exit
    │   ├── generate_shipments.py
    │   ├── etl.py
    │   └── quality_checks.py
    │
    ├── ml/
    │   ├── train.py            # trains + saves the delay classifier
    │   ├── predict.py          # enforced train/serve feature contract
    │   └── models/              # delay_predictor.pkl (gitignored, regenerated by train.py)
    │
    ├── services/               # long-running, containerized, has a port
    │   ├── api/                # FastAPI, Dockerfile builds from repo root
    │   └── dashboard/          # Streamlit, Dockerfile builds from repo root
    │
    ├── orchestration/
    │   ├── dags/logiflow_pipeline.py
    │   ├── entrypoint.sh
    │   └── requirements.txt
    │
    ├── streaming/
    │   ├── kafka_producer.py
    │   ├── spark_streaming.py
    │   ├── Dockerfile.producer
    │   └── Dockerfile.spark_job
    │
    └── docs/
        ├── architecture.md
        ├── data-model.md
        ├── setup-guide.md
        └── decisions.md
```

The legacy `mvp1-4` chronological structure has been fully removed — the tree above is
the entire project, one structure, no leftovers.

## Documentation

| Document | What it covers |
|---|---|
| [docs/architecture.md](logiflow/docs/architecture.md) | Component inventory, data flow, infrastructure notes |
| [docs/data-model.md](logiflow/docs/data-model.md) | Full schema reference, table by table |
| [docs/setup-guide.md](logiflow/docs/setup-guide.md) | Step-by-step local setup and troubleshooting |
| [docs/decisions.md](logiflow/docs/decisions.md) | **Why** each major decision was made — the part a jury actually asks about |

---

## Quick Start

```bash
cd logiflow
cp .env.example .env
# fill in POSTGRES_*, MINIO_ROOT_*, AIRFLOW_*; OPENWEATHER_API_KEY and TOMTOM_API_KEY are optional (falls back to simulated data)

docker compose up -d --build
```

This starts Postgres, MinIO, Airflow, the API, and the dashboard. Postgres applies
`infra/schema.sql` automatically on first init.

**Bootstrap data** (nothing does this automatically on first run):
```bash
python -m pipelines.generate_shipments --n 1000000 --days-back 730   # or a smaller --n for a quick test
python -m pipelines.etl
python -m pipelines.quality_checks
python -m ml.train
```

**Or let Airflow do it**: open `http://localhost:8080`, trigger `logiflow_daily_pipeline`
manually. It runs `generate_shipments → run_etl → quality_check → retrain_model` in
order — each task is a direct call into the modules above, not separate logic.

**Add the streaming layer:**
```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
```

---

## Service Access

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| FastAPI Swagger | http://localhost:8000/docs |
| Streamlit Dashboard | http://localhost:8501 |
| Spark Master UI | http://localhost:8082 |
| Kafka UI (optional, `--profile ui`) | http://localhost:8090 |

---

## Key Results (verified, not estimated)

| Metric | Value | How verified |
|--------|-------|---------------|
| Shipments in warehouse | 1,001,000 | `SELECT COUNT(*) FROM fact_shipments` after a real ETL run |
| Best model ROC-AUC | 0.645 (Gradient Boosting) | Full training run inside Airflow, CV score (0.646) matches test score — not a lucky split |
| Models compared | 4 (Logistic Regression, Random Forest, Gradient Boosting, XGBoost) | `ml/train.py` |
| ETL idempotency | Confirmed | Ran `pipelines/etl.py` twice on identical input — row count unchanged both times |
| Data quality checks | 8, split critical/non-critical | `pipelines/quality_checks.py`, wired into the DAG's failure logic |
| Live external integrations | 2 (OpenWeatherMap, TomTom Traffic) | Cached per city, real API calls confirmed in logs |
| Airflow DAG | 4 tasks, all verified green in a real run | `generate_shipments → run_etl → quality_check → retrain_model` |
| API endpoints | 10 | `services/api/main.py` |

| Streaming pipeline | 2,185 rows in `realtime_shipments`, verified live | Kafka producer -> Kafka -> Spark Structured Streaming -> idempotent Postgres upsert, confirmed end-to-end |

**Known, honest gaps** — not yet done, not hidden: no drift detection (a worse retrain
overwrites a better model with no check, though `ml/train.py` now at least warns loudly
when the new ROC-AUC is worse than the previous one), no alerting on pipeline failure
(`email_on_failure: False`), model artifacts are not versioned beyond one rollback copy,
nothing downstream (API, dashboard) reads `realtime_shipments` yet.

---

## Why the ROC-AUC is 0.645, not higher

Worth stating plainly rather than letting the number speak for itself: all four models
converge to roughly the same ceiling (0.62–0.65) regardless of algorithm complexity. That
convergence is the actual finding — the limiting factor isn't model choice, it's that the
synthetic label itself is generated stochastically (`pipelines/generate_shipments.py`
computes a delay *probability* from weather/traffic/driver/vehicle risk factors, then
draws a random outcome from it). Even a theoretically perfect classifier recovering the
true risk exactly would be capped well below 1.0 ROC-AUC on this data, by construction.

---

## What I'd Defend Under Questioning

- **Why a star schema, not 3NF?** Direct dimension-to-fact joins for the aggregation-heavy
  queries the API and dashboard actually run; extensible without restructuring the fact table.
- **Why does the ETL upsert instead of truncate-and-reload?** The original version of this
  project truncated the whole warehouse before every load — only the latest ~100 rows ever
  survived a day. `source_shipment_id` (a UUID assigned at generation time) plus `UNIQUE`
  constraints on every dimension's natural key make every load idempotent — verified by
  running the loader twice on the same input and confirming the row count didn't change.
- **Why is `common/storage.py` a separate file?** It's the only file that changes when this
  migrates from MinIO to real S3 — every pipeline calls `storage.upload_bytes()` /
  `storage.download_bytes()`, none of them touch the MinIO SDK directly.
- **What happens if Kafka is down mid-stream, or an event gets delivered twice?** The
  producer uses `acks="all"` (at-least-once delivery, duplicates are expected, not
  hypothetical). The Spark sink does a real `INSERT ... ON CONFLICT (event_id) DO NOTHING`
  per micro-batch partition — not a plain JDBC append, which would crash on the first
  duplicate.
- **What's decorative vs. load-bearing?** Being direct about it: nothing downstream (API,
  dashboard) currently reads `realtime_shipments` — the streaming layer is a real,
  independently-working skill demonstration, not yet integrated into the analytics layer.

---

## Contact

**Hibatallah Chmicha**
Data Science Student @ INSEA
[GitHub](https://github.com/hibatallahchmicha) · [LinkedIn](https://linkedin.com/in/hibatallahchmicha)
