# 📊 MVP 2 — Analytics Layer

Complete analytics layer built on top of the MVP 1 data warehouse.
Four independent modules that together form a production-ready data platform.

---

## 🗂️ Structure
mvp2-analytics-layer/
├── 2A-dashboard/      # Streamlit interactive dashboard
├── 2B-scheduler/      # Automated ETL scheduling
├── 2C-data-quality/   # Data validation & monitoring
└── 2D-api/            # FastAPI REST endpoints

---

## 2A — Dashboard
**What it does:** Interactive analytics dashboard with 7 sections and 18+ charts.

**Run:**
```bash
cd 2A-dashboard && streamlit run app.py
```
**URL:** http://localhost:8501

**Sections:**
- 6 KPI cards (shipments, on-time rate, delay, revenue, failures, driver rating)
- Delivery performance (status breakdown, monthly trend, weekday analysis)
- Cost & revenue (by segment, cost vs distance, monthly revenue)
- Route & geography (top routes, delay by region, route type)
- Driver & vehicle performance (rankings, rating vs delay, vehicle comparison)
- Weather impact (delay by condition, temperature, wind speed)
- Raw data explorer (filterable table + CSV download)

---

## 2B — Scheduler
**What it does:** Runs ETL jobs automatically on a defined schedule using APScheduler.

**Run:**
```bash
cd 2B-scheduler && python scheduler.py
```

**Schedule:**
| Job | Trigger |
|-----|---------|
| ETL Pipeline | Daily @ 02:00 UTC |
| Data Quality Check | Daily @ 02:30 UTC |
| Generate New Data | Every 6 hours |
| Heartbeat | Every 10 minutes |

> **Note:** Currently runs on simulated data from MVP 1.
> MVP 3 will connect this to live data sources.

---

## 2C — Data Quality
**What it does:** Runs 8 automated checks to validate warehouse integrity.

**Run:**
```bash
cd 2C-data-quality && python quality_checks.py
```

**Checks:**
| # | Check | Validates |
|---|-------|-----------|
| 1 | Row counts | All tables have data |
| 2 | NULL foreign keys | Fact table fully linked |
| 3 | Invalid status | Only on_time / delayed / failed |
| 4 | Negative values | Cost, weight, distance > 0 |
| 5 | Delay consistency | is_delayed flag matches delay_minutes |
| 6 | Driver ratings | Ratings between 1.0 and 5.0 |
| 7 | Orphan records | All FKs exist in dimensions |
| 8 | Duplicates | No duplicate shipments |

---

## 2D — REST API
**What it does:** Exposes warehouse data via REST endpoints with auto-generated Swagger docs.

**Run:**
```bash
cd 2D-api && uvicorn main:app --reload --port 8000
```
**URL:** http://localhost:8000/docs

**Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API status |
| GET | `/shipments` | Paginated shipments (filter by status) |
| GET | `/shipments/{id}` | Single shipment detail |
| GET | `/kpis/summary` | Top-level KPIs |
| GET | `/kpis/by-month` | Monthly KPI breakdown |
| GET | `/kpis/by-region` | Regional KPI breakdown |
| GET | `/drivers` | All drivers + performance stats |
| GET | `/drivers/{id}` | Single driver profile |
| GET | `/routes` | All routes + performance stats |
| GET | `/weather/impact` | Delay rate by weather condition |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Streamlit | Dashboard framework |
| Plotly | Interactive charts |
| FastAPI | REST API framework |
| APScheduler | Job scheduling |
| SQLAlchemy | Database ORM |
| psycopg2 | PostgreSQL driver |
| pandas | Data manipulation |

---

## ✅ Status

| Module | Status |
|--------|--------|
| 2A Dashboard | ✅ Complete |
| 2B Scheduler | ✅ Complete |
| 2C Data Quality | ✅ Complete |
| 2D API | ✅ Complete |

---

## 👉 Next: MVP 3 — Real Data & Advanced Features

- Connect scheduler to live APIs (weather, ERP, TMS)
- Incremental data loading
- Machine learning — delay prediction model
- Real-time streaming pipeline
- Advanced visualizations (maps, forecasting)