# LogiFlow — Design Decisions

> Why this project is built the way it is. Every entry is a decision that had a real
> alternative, with the reasoning for the path taken and — where relevant — the honest
> cost of that choice.
>
> Read `architecture.md` for *what* the system is. This document is *why*.

---

## 1. Problem Framing

**Target variable:** `is_delayed` — a binary flag, true when a shipment's `delay_minutes`
exceeds 30. Chosen over predicting `delay_minutes` as a continuous value because the
business question a logistics operator actually asks is "which shipments do I need to
intervene on today," which is a ranking/triage problem, not an estimation one.

**Success metric: ROC-AUC, not accuracy.** The class balance sits around 20–30% delayed,
which means a model that predicts "on time" for everything scores ~70–80% accuracy while
being completely useless. ROC-AUC measures whether the model ranks a genuinely delayed
shipment above a genuinely on-time one, which is what a triage use case needs. Accuracy is
reported alongside it for context, never as the headline.

**Explicitly out of scope, and why:**
- *Predicting delay duration* — the intervention decision is binary; duration adds
  modeling complexity without changing what an operator does.
- *Route optimization / ETA prediction* — a different problem class (optimization, not
  classification) that would double the project's surface area.
- *Real customer data* — no logistics company provides this. Synthetic generation with
  deliberately correlated risk factors was chosen instead, which has the side benefit of
  making the ground-truth relationships knowable (see §6).

---

## 2. Repository Structure: Role, Not Chronology

The project was originally organized as `mvp1-data-pipeline/`, `mvp2-analytics-layer/`,
`mvp3-advanced/`, `mvp4-streaming/` — the order things were built in.

That layout answered a question nobody asks. It was replaced with organization by
architectural role:

| Directory | What it is |
|---|---|
| `common/` | Shared library. Everything imports from here; it imports from nothing. |
| `infra/` | Warehouse DDL — the single source of truth for table structure. |
| `pipelines/` | Scripts that run, do a job, and exit. |
| `ml/` | Training and inference. |
| `services/` | Long-running, containerized, has a port. |
| `orchestration/` | Schedules the scripts. Contains no business logic. |
| `streaming/` | The independent real-time path. |

**Cost of the change:** every Dockerfile build context, compose mount path, and import
statement had to move with it. Worth it — the old layout made it impossible to answer
"what's a deployable service versus a script" without reading every file.

---

## 3. One Source of Truth Per Concern

The original codebase rebuilt the Postgres connection string from `os.getenv` in five
separate files, constructed the MinIO client three different ways, and defined the schema
in two places (one of which was incomplete and silently unreachable).

**`common/config.py`** is now the only module that reads environment variables. It does two
things the scattered version didn't:
- **Fails fast.** `_require()` raises immediately with the missing variable's name. The old
  pattern let `os.getenv` return `None`, which then travelled three layers deep before
  producing an unrelated-looking error.
- **Centralizes drift.** Two of the five old copies had different default values for the
  same setting.

**`common/storage.py`** is the only module that touches the object-storage SDK. This is the
single most load-bearing abstraction in the project: **it is the only file that changes
when MinIO is swapped for real S3.** Every caller uses `storage.upload_bytes()` /
`storage.download_bytes()` and never learns what's underneath.

**Accepted trade-off:** `common/storage.py` connects at import time, which means CI cannot
import it without live infrastructure. The CI workflow works around this by syntax-checking
that module via `ast.parse()` instead of importing it. A lazier connection would avoid this,
at the cost of failing later and less obviously in normal use — the current behaviour is
preferred.

---

## 4. Star Schema, and Natural Keys for Idempotency

**Star schema over 3NF** — the query pattern is aggregation-heavy (group by month, region,
weather, driver), which favours direct dimension-to-fact joins over chained lookups. It is
also readable without a diagram, which matters for a project meant to be explained.

**The more consequential decision: every dimension has a `UNIQUE` constraint on its
real-world identity** (`company_name`, driver `full_name`, `plate_number`, the
origin+destination pair), and `fact_shipments` carries a `source_shipment_id` UUID assigned
by the generator at creation time.

This exists to fix a specific, severe bug in the original pipeline. That version had no way
to recognize a record it had already loaded, so its ETL **truncated the entire warehouse
before every run**. The consequence: the daily Airflow job destroyed all history and left
only that run's ~100 rows. A warehouse that resets itself daily is not a warehouse.

With natural keys plus `ON CONFLICT ... DO NOTHING`, the loader appends safely and
re-running it is a no-op. **Verified, not assumed:** the ETL was run twice back-to-back on
identical input and the row count was unchanged (200 in, 200 out — not 400).

---

## 5. Live APIs for Recent Data, Simulated for History

Weather (OpenWeatherMap) and traffic (TomTom) are fetched live — but **only for batches
spanning the last 7 days**. Anything older is simulated.

The obvious assumption is that this is about rate limits. It isn't; caching already solves
that (one call per city per run, ~10 cities regardless of batch size). **It is about
correctness.** Both APIs answer "what is happening right now." Asked about a shipment dated
18 months ago, they would return *today's* conditions, stamped onto a historical record —
factually wrong data wearing the costume of real data. Openly synthetic values are the more
honest choice for backfill, and no amount of caching changes that.

**Related:** the customer/driver/vehicle/route roster is a fixed list, reused every run
rather than randomly regenerated. Dimension tables need stable identity for the natural
keys in §4 to mean anything — a new random driver name every run would defeat the purpose.

---

## 6. Model Choices, and Why ROC-AUC Is 0.645

Four classifiers are compared (Logistic Regression, Random Forest, Gradient Boosting,
XGBoost) and the best by ROC-AUC is saved. On the full 1,001,000-row dataset:

| Model | Accuracy | ROC-AUC | CV Score |
|---|---|---|---|
| Logistic Regression | 0.696 | 0.631 | 0.632 |
| Random Forest | 0.692 | 0.620 | 0.620 |
| **Gradient Boosting (saved)** | **0.700** | **0.645** | **0.646** |
| XGBoost | 0.698 | 0.641 | 0.640 |

**The result is trustworthy.** Cross-validation scores match test scores almost exactly for
every model (0.645 vs 0.646, 0.641 vs 0.640). That agreement is the signal that the number
is stable rather than an artifact of a lucky split — on an earlier 200-row test run, CV sat
far *below* test score, which is the classic small-sample warning sign.

**0.645 is real signal, and the ceiling is explainable.** All four models converge to
roughly the same score regardless of complexity. That convergence is itself the finding: the
limiting factor is not model choice, it is the label. `generate_shipments.py` computes a
delay *probability* from weather/traffic/driver/vehicle risk factors, then draws a random
outcome from it. Even a perfect classifier that recovered the true underlying probability
exactly would be capped well below 1.0, because the outcome is genuinely stochastic **by
construction**. This is a property of the data design, not a modeling failure.

**Leakage was checked, not assumed.** `delay_minutes`, `status`, and `actual_duration_hrs`
all encode the outcome directly and are excluded from the feature set.

**Known weakness:** the HIGH/MEDIUM/LOW risk thresholds in `ml/predict.py` (0.7 / 0.4) are
hardcoded, not tuned against a precision/recall or cost-based criterion. Named here rather
than hidden.

**Efficiency finding worth acting on:** Gradient Boosting won by 0.004 ROC-AUC but consumed
~37 of the 51-minute training run — sklearn's implementation is not histogram-based and does
not scale. For a daily retrain, `HistGradientBoostingClassifier` is the correct swap.

---

## 7. Train/Serve Contract, Enforced

`ml/predict.py` declares `REQUIRED_FIELDS` and raises if any are missing. No defaults, no
silent substitution.

This replaces a genuine bug: the original inference code substituted `weight_kg` for
`vehicle_capacity` whenever a caller omitted it. Since the API's request schema never asked
for `vehicle_capacity`, that fallback fired on **every single live prediction**, silently
producing a garbage `load_ratio` — a feature the model was actually trained on. It never
errored. It just returned quietly wrong probabilities.

The contract is now enforced at three layers: training defines it, `predict.py` validates
it, and the FastAPI request schema requires it (rejecting incomplete requests with a 422
before they ever reach the model).

---

## 8. Orchestration Contains No Business Logic

Every task in `orchestration/dags/logiflow_pipeline.py` is a thin wrapper:
`from pipelines.etl import run; run()`.

The original DAG had ~100 lines of CSV-splitting and schema-remapping logic embedded
directly in an Airflow operator — untestable outside Airflow, and duplicating logic that
belonged in a module. The rule now: **if it works standalone (`python -m pipelines.etl`),
the DAG task does exactly that, because it is that call.**

Imports are deferred inside each task function rather than at module level, so Airflow's
frequent DAG-file reparsing doesn't need to load xgboost and sklearn just to render the
graph.

---

## 9. Deliberate Exceptions

Three places knowingly break a project-wide rule. Each is a decision, not an oversight.

**`streaming/spark_streaming.py` uses `os.getenv`, not `common.config`.** Spark distributes
execution across driver and executor processes; `common.config`'s assumptions (repo root on
`sys.path`, `.env` at a fixed relative path) do not hold reliably across that boundary.
Configuration is passed explicitly as environment variables by `spark-submit`/Docker instead.

**`orchestration/entrypoint.sh` installs with `pip install --no-deps`.** Airflow pins its own
dependencies strictly (notably `SQLAlchemy<2.0`), and a normal install upgrading those breaks
Airflow's own database engine at startup. `--no-deps` sidesteps the resolver entirely, at the
real cost of having to enumerate transitive dependencies by hand in
`orchestration/requirements.txt` — `argon2-cffi-bindings` and `pycryptodome` are listed there
solely because the `minio` package needs them and `--no-deps` won't fetch them.

**Ruff's `DTZ` rules are disabled** (see `ruff.toml`). Timezone-aware datetimes are correct
in general, but this system runs in a single timezone and never compares timestamps across
zones. Documented as a deliberate simplicity choice with a note to revisit if that changes.
Similarly, several `except Exception` blocks carry `# noqa: BLE001` — they are intentional
graceful-degradation paths (external API failures fall back to simulated data; model-backup
failures must never block a save), and narrowing them would defeat the design.

---

## 10. Streaming: Idempotent, and Honestly Scoped

The Spark sink performs a real `INSERT ... ON CONFLICT (event_id) DO NOTHING` upsert per
micro-batch partition. The original used a plain JDBC `.mode("append")` while *claiming* in a
code comment that conflicts were handled at the database level — they weren't. Since the
producer runs `acks="all"` with retries (at-least-once delivery), duplicate events are an
expected condition, not a hypothetical; a plain append would have crashed the streaming query
on the first one.

**A note on what this layer is and isn't:** nothing downstream — not the API, not the
dashboard — currently reads `realtime_shipments`. The streaming path is verified working end
to end (2,185 rows ingested in a live run) but it is a self-contained demonstration, not an
integrated part of the analytics layer. Stating this plainly is preferable to letting it
imply more than it does.

---

## 11. Cloud Scope: What Moves and What Doesn't

The migration target is **S3 + Glue + Athena, provisioned with Terraform**. Orchestration
(Airflow) and streaming (Kafka, Spark) stay containerized locally.

This is a cost decision made deliberately. Managed Airflow (MWAA) runs roughly $350/month and
managed Kafka (MSK) roughly $150/month, neither with a free tier — for a single-user portfolio
project, that is real money for no additional learning. The services being moved are all
serverless, so nothing bills by the hour and there is nothing to forget to shut down.

Terraform earns its place here specifically because of the demo-then-destroy workflow:
`terraform destroy` guarantees no orphaned resources, which manual console teardown does not.

---

## 12. Known Gaps

Stated plainly rather than discovered by a reviewer:

- **No drift detection.** `ml/train.py` compares the new model's ROC-AUC against the previous
  one and warns loudly when it's worse (keeping one rollback copy), but nothing blocks the
  swap and nothing monitors feature distributions over time.
- **No failure alerting.** `email_on_failure: False` — a failed DAG run is visible only to
  someone who opens the Airflow UI.
- **Model artifacts are barely versioned.** One current model, one previous. No registry, no
  lineage back to the training data snapshot.
- **`dim_vehicle.last_service_date` and `is_active`** are declared in the schema but never
  populated by the ETL — the generator does not produce them.
- **Validation runs after load, not before.** `quality_checks.py` catches bad data once it is
  already in the warehouse. A pre-load contract check would catch it earlier.
- **Test coverage is minimal** — two unit tests covering `common/config.py`'s fail-fast
  behaviour. CI additionally lints, validates both compose files, and syntax-checks every
  module, but there is no meaningful test coverage of pipeline logic.
