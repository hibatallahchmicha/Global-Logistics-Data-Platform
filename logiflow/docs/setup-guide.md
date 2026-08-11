# LogiFlow — Setup Guide

> Step-by-step instructions to run the platform locally, current structure.

---

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop | Running, with Docker Compose v2 (`docker compose`, not `docker-compose`) |
| Python 3.11+ | For running pipeline scripts directly on the host |
| PowerShell or a POSIX shell | Commands below are shown for PowerShell; adjust quoting for bash |

No WSL2 requirement — this rebuild runs directly on Windows/PowerShell.

---

## 2. Environment Configuration

```powershell
cd logiflow
Copy-Item .env.example .env
```

Fill in `.env`:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` — required.
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` — required; also used as the S3-style access/secret key.
- `AIRFLOW_USER`, `AIRFLOW_PASSWORD`, `AIRFLOW_FIRSTNAME`, `AIRFLOW_LASTNAME`, `AIRFLOW_EMAIL`, `AIRFLOW_SECRET_KEY` — required.
- `OPENWEATHER_API_KEY`, `TOMTOM_API_KEY` — optional. Without them, the generator falls back to simulated weather/traffic automatically, no error.

**One thing that trips people up:** `MINIO_ENDPOINT` needs different values depending on
where a script runs. Inside Docker (Airflow, the API), it must be `minio:9000` (Docker's
internal service DNS) — the `environment:` blocks in `docker-compose.yml` already set
this correctly per-service. If you're running a pipeline script directly on your host
(not in a container), your `.env` needs `MINIO_ENDPOINT=localhost:9000` instead, since
`minio` as a hostname doesn't resolve outside the Docker network.

---

## 3. Start the Core Stack

```powershell
docker compose up -d --build
```

Starts Postgres, MinIO, Airflow, the API, and the dashboard. Postgres applies
`infra/schema.sql` automatically on first container init (via
`docker-entrypoint-initdb.d` — this only runs once, on a genuinely fresh volume; it will
not retroactively apply schema changes to an existing `postgres_data` volume).

Airflow takes a couple of minutes to come up — it installs pipeline dependencies on
every container start (`orchestration/entrypoint.sh`). Check readiness with:
```powershell
docker compose logs airflow -f
```
Watch for `Starting Airflow...`, or check `http://localhost:8080/health` directly —
`HTTP 200` means the webserver is actually up, which is more reliable than trusting the
log stream (output buffering can make the log look stalled when the container is fine).

---

## 4. Bootstrap Data

Nothing in `docker-compose.yml` generates data automatically on first run. Two ways to do it:

**Manually, from the host** (good for a quick correctness check first):
```powershell
python -m pipelines.generate_shipments --n 50 --days-back 1
python -m pipelines.etl
python -m pipelines.quality_checks
python -m ml.train
```

**At real scale**, once the small run confirms everything works:
```powershell
python -m pipelines.generate_shipments --n 1000000 --days-back 730
python -m pipelines.etl
python -m ml.train
```
This is a genuinely heavy run — generation takes a few minutes, and training (especially
`GradientBoostingClassifier`, which doesn't scale well) can take 40+ minutes on a
million-row dataset. That's expected, not a hang — check `docker stats` if unsure
whether a container is actually working or stuck.

**Or trigger the Airflow DAG** instead of running scripts manually: open
`http://localhost:8080`, find `logiflow_daily_pipeline`, click the trigger (▶) button.
It runs the same four steps in order — `generate_shipments → run_etl → quality_check →
retrain_model` — via `orchestration/dags/logiflow_pipeline.py`. Airflow's Grid view can
mix historical runs with new ones if you're reusing an old Postgres volume; check the
actual `run_id` timestamp, not just square color, before trusting what you're looking at.

---

## 5. Verify

```powershell
docker exec -i logiflow_postgres psql -U logiflow_user -d logiflow -c "SELECT COUNT(*) FROM fact_shipments;"
```

Check the API and dashboard:
- `http://localhost:8000/docs` — try `/kpis/summary` and `/predict/delay`
- `http://localhost:8501` — dashboard should show real data, including the weather/traffic panels

---

## 6. Streaming Layer (Optional, Separate Path)

```powershell
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
```

Adds Kafka, Spark master/worker, the event producer, and the Spark Structured Streaming
job. This layer is independent of the batch pipeline — verify it separately:

```powershell
docker compose logs shipment-producer -f
docker compose logs spark-streaming -f
docker exec -i logiflow_postgres psql -U logiflow_user -d logiflow -c "SELECT COUNT(*) FROM realtime_shipments;"
```

Note: nothing in the API or dashboard currently reads `realtime_shipments` — this is
intentional scope, not a bug. It's a standalone demonstration of a streaming ingestion
path, not yet wired into the analytics layer.

---

## 7. Troubleshooting Notes Worth Knowing

- **`docker compose restart` vs. recreate:** `restart` reuses the same container,
  including whatever got installed into it by a previous (possibly broken) run. If
  you've changed `orchestration/requirements.txt` or `entrypoint.sh` and Airflow is still
  showing the old failure, use `docker compose up -d --force-recreate airflow` instead —
  `restart` alone won't discard a contaminated container filesystem.
- **Airflow + your own Python packages:** installing packages like `pandas`/`scikit-learn`
  directly into the Airflow image can conflict with Airflow's own pinned dependencies
  (notably `SQLAlchemy`). `orchestration/entrypoint.sh` uses `pip install --no-deps` with
  every dependency (including transitive ones) pinned explicitly in
  `orchestration/requirements.txt`, specifically to avoid this.
- **Checking if a slow container is actually working or stuck:** `docker stats
  <container>` — high CPU/memory means it's genuinely computing; near-zero usage for
  several minutes on a task that should be active means something's actually hung.

---

## 8. Service Access Reference

| Service | URL | Auth |
|---|---|---|
| Airflow UI | http://localhost:8080 | From `.env`: `AIRFLOW_USER` / `AIRFLOW_PASSWORD` |
| MinIO Console | http://localhost:9001 | From `.env`: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| FastAPI Swagger | http://localhost:8000/docs | None |
| Streamlit Dashboard | http://localhost:8501 | None |
| PostgreSQL | localhost:5432 | From `.env`: `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| Spark Master UI | http://localhost:8082 | None |
| Kafka UI (optional) | http://localhost:8090 | None — start with `--profile ui` |
