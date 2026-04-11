# 🚚 LogiFlow — End-to-End Logistics Data Platform

> A production-grade data engineering and machine learning platform for logistics analytics,
> built from scratch using modern data stack technologies.

**Author:** Hibatallah Chmicha · Data Science Student @ INSEA  
**Stack:** Python · PostgreSQL · MinIO · Streamlit · FastAPI · Scikit-learn · XGBoost · Docker  

---

## 📌 Project Overview

LogiFlow is a full-stack data platform that simulates a real-world logistics intelligence system.
It covers the complete data lifecycle — from raw ingestion to machine learning predictions —
organized across three progressive MVPs that reflect how production data systems are actually built.

The project was designed to demonstrate practical expertise in:
- Data engineering (pipelines, warehousing, ETL)
- Analytics (dashboards, KPIs, data quality)
- Software engineering (REST APIs, scheduling, modular architecture)
- Machine learning (classification, feature engineering, model evaluation)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LOGIFLOW PLATFORM                            │
│                                                                     │
│  Data Sources          Storage           Warehouse                  │
│  ┌──────────┐         ┌───────┐         ┌────────────────────┐     │
│  │ CSV Files│────────►│ MinIO │────────►│    PostgreSQL       │     │
│  │ Live APIs│         │ (S3)  │   ETL   │   (Star Schema)    │     │
│  │ Weather  │         └───────┘         └────────┬───────────┘     │
│  └──────────┘                                    │                  │
│                                                  │                  │
│              ┌───────────────────────────────────┤                  │
│              │               │                   │                  │
│         ┌────▼────┐    ┌─────▼────┐    ┌────────▼──────┐          │
│         │Dashboard│    │ REST API │    │  ML Predictor │          │
│         │Streamlit│    │ FastAPI  │    │  XGBoost      │          │
│         └─────────┘    └──────────┘    └───────────────┘          │
│                                                                     │
│         ┌─────────────────────┐  ┌──────────────────────┐         │
│         │   APScheduler       │  │   Data Quality       │         │
│         │ (Automated ETL)     │  │   (8 Checks)         │         │
│         └─────────────────────┘  └──────────────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 MVP Roadmap

### ✅ MVP 1 — Data Pipeline Foundation
> *"Build the warehouse before filling it"*

Designed and implemented a complete data pipeline from raw files to a structured warehouse.

**What was built:**
- Docker-based infrastructure (PostgreSQL + MinIO) via `docker-compose`
- Star schema data warehouse with 1 fact table + 4 dimension tables
- Python ETL pipeline: Extract from CSVs → Transform → Load into PostgreSQL
- MinIO as S3-compatible data lake for raw file storage
- 10,000+ realistic shipment records generated with domain logic

**Skills demonstrated:**
`Data Modeling` · `ETL Development` · `Docker` · `PostgreSQL` · `MinIO / S3` · `pandas`

---

### ✅ MVP 2 — Analytics Layer
> *"Make the data useful for everyone"*

Built four independent analytics modules on top of the warehouse.

#### 2A — Interactive Dashboard
- 7-section Streamlit dashboard with 18+ Plotly charts
- Sidebar filters (year, status, region, segment)
- KPI cards: on-time rate, revenue, delay analysis, driver ratings
- Sections: delivery performance, cost analysis, route geography, weather impact
- CSV export for filtered data

**Skills:** `Streamlit` · `Plotly` · `Data Visualization` · `UX Design`

---

#### 2B — Automated Scheduler
- APScheduler-based job runner with 4 scheduled jobs
- Daily ETL at 02:00 UTC, quality check at 02:30 UTC
- Incremental data generation every 6 hours
- Heartbeat monitoring every 10 minutes
- Rotating log files per month

**Skills:** `APScheduler` · `Pipeline Automation` · `Logging` · `Production Patterns`

---

#### 2C — Data Quality Framework
- 8 automated validation checks on every ETL run
- Checks: row counts, NULL foreign keys, invalid status values,
  negative numerics, delay flag consistency, rating ranges,
  orphan records, duplicate shipments
- Pass/fail report with detailed logging

**Skills:** `Data Quality` · `Validation` · `Testing` · `Data Reliability`

---

#### 2D — REST API
- FastAPI application exposing warehouse data via 10 endpoints
- Auto-generated Swagger UI at `/docs`
- Paginated shipment queries with status filtering
- KPI endpoints: summary, by-month, by-region
- Driver profiles, route performance, weather impact analysis

**Skills:** `FastAPI` · `REST API Design` · `Pydantic` · `SQLAlchemy` · `API Documentation`

---

### ✅ MVP 3 — Advanced Features
> *"Make the platform smart and production-ready"*

#### 3A — Machine Learning: Delay Prediction
- Binary classification model predicting shipment delays
- 4 models trained and compared: Logistic Regression, Random Forest,
  Gradient Boosting, XGBoost
- Feature engineering: vehicle age, load ratio, cost per km, mileage per year
- 5-fold cross-validation, ROC-AUC scoring, confusion matrix analysis
- Best model serialized with full preprocessing pipeline (encoders + scaler)
- `/predict/delay` API endpoint integrated into FastAPI
- Risk levels: 🟢 LOW / 🟡 MEDIUM / 🔴 HIGH with probability score

**Skills:** `Scikit-learn` · `XGBoost` · `Feature Engineering` · `Model Evaluation`  
`Cross-Validation` · `joblib` · `ML in Production`

---

#### 3B — Real Data Sources
- Live weather data via OpenWeatherMap API (15 cities, real coordinates)
- Delay probability dynamically adjusted based on actual weather conditions
- Graceful fallback to simulation when API key unavailable
- Batch generation with configurable size and date range
- Seamless integration with existing ETL pipeline

**Skills:** `REST API Consumption` · `requests` · `Real-time Data` · `Data Simulation`

---

## 🛠️ Tech Stack

| Category | Technology | Purpose |
|----------|-----------|---------|
| Infrastructure | Docker, Docker Compose | Container orchestration |
| Data Lake | MinIO (S3-compatible) | Raw file storage |
| Data Warehouse | PostgreSQL | Structured analytics storage |
| Data Modeling | Star Schema | Optimized query design |
| ETL | Python, pandas | Data transformation |
| Dashboard | Streamlit, Plotly | Interactive visualization |
| API | FastAPI, Pydantic | REST endpoints |
| Scheduling | APScheduler | Automated pipeline execution |
| ML | Scikit-learn, XGBoost | Delay prediction |
| Environment | python-dotenv | Configuration management |
| ORM | SQLAlchemy, psycopg2 | Database connectivity |

---

## 📁 Repository Structure

```
logiflow/
│
├── mvp1-data-pipeline/
│   ├── database/
│   │   └── create_schema.sql          # Star schema DDL
│   ├── data-generation/
│   │   └── generate_data.py           # Synthetic data generator
│   └── ingestion/
│       ├── upload_to_minio.py         # Data lake upload
│       └── etl_pipeline.py            # ETL orchestration
│
├── mvp2-analytics-layer/
│   ├── 2A-dashboard/
│   │   ├── app.py                     # Streamlit dashboard
│   │   ├── db_connector.py            # PostgreSQL → pandas
│   │   └── assets/screenshots/        # Dashboard previews
│   ├── 2B-scheduler/
│   │   └── scheduler.py               # APScheduler jobs
│   ├── 2C-data-quality/
│   │   └── quality_checks.py          # 8 validation checks
│   └── 2D-api/
│       └── main.py                    # FastAPI application
│
├── mvp3-advanced/
│   ├── 3A-ml-prediction/
│   │   ├── train.py                   # Model training pipeline
│   │   ├── predict.py                 # Inference module
│   │   ├── models/
│   │   │   └── delay_predictor.pkl    # Serialized best model
│   │   └── reports/
│   │       └── evaluation_*.png       # Training evaluation plots
│   └── 3B-real-data/
│       └── real_data_fetcher.py       # Live weather API integration
│
├── docs/
│   ├── architecture.md
│   ├── data-model.md
│   └── setup-guide.md
│
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## 🚦 Quick Start

### Prerequisites
- Docker Desktop running
- Python 3.10+
- `.env` file configured (see `.env.example`)

### 1. Start infrastructure
```bash
docker-compose up -d
```

### 2. Run ETL pipeline
```bash
cd mvp1-data-pipeline/ingestion
python etl_pipeline.py
```

### 3. Launch dashboard
```bash
cd mvp2-analytics-layer/2A-dashboard
streamlit run app.py
# → http://localhost:8501
```

### 4. Start REST API
```bash
cd mvp2-analytics-layer/2D-api
uvicorn main:app --reload --port 8000
# → http://localhost:8000/docs
```

### 5. Train ML model
```bash
cd mvp3-advanced/3A-ml-prediction
python train.py
python predict.py
```

---

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Shipments in warehouse | 10,000+ |
| Dashboard sections | 7 |
| Charts & visualizations | 18+ |
| API endpoints | 10 |
| Data quality checks | 8 |
| ML models compared | 4 |
| Features engineered | 25+ |
| Cities with real weather | 15 |

---

## 🧠 What I Learned

This project was built progressively, each MVP adding a new layer of complexity:

**MVP 1** taught me that data modeling decisions made early (star schema vs flat tables)
have massive downstream impact on query performance and analytics flexibility.

**MVP 2** showed me that raw data in a warehouse has no value until it's exposed —
through dashboards for business users, APIs for developers, and quality checks for trust.

**MVP 3** connected everything: the warehouse became training data,
real weather made predictions meaningful, and the ML model became a live API endpoint.

The biggest lesson: **a data project is never just about the data.**
It's about the pipeline that moves it, the warehouse that structures it,
the tools that expose it, and the models that learn from it.

---

## 📬 Contact

**Hibatallah Chmicha**  
Data Science Student @ INSEA  
[GitHub](https://github.com/hibatallahchmicha) · [LinkedIn](https://linkedin.com/in/hibatallahchmicha)