# 🚚 LogiFlow — Global Logistics Data Platform

An end-to-end data platform for a fictional logistics company,
built to showcase both Data Engineering and Data Science skills
in a single production-grade project.

## 🏗️ Architecture
Raw Data → MinIO (Data Lake) → ETL → PostgreSQL (Warehouse)
→ dbt (Transforms) → Airflow (Orchestration)
→ Great Expectations (Quality) → ML Model → Streamlit Dashboard

## 🛠️ Tech Stack
- MinIO — S3-compatible data lake (Docker)
- PostgreSQL — Star schema warehouse (Docker)
- dbt — SQL transformations & testing
- Apache Airflow — Pipeline orchestration
- Great Expectations — Data quality validation
- XGBoost — Delay prediction model
- FastAPI — Model serving
- Streamlit — Analytics dashboard
- Docker Compose — One-command stack
- GitHub Actions — CI/CD

## 💼 Two Resume Stories
- **DE:** Built the platform that makes reliable data possible
- **DS:** Built the intelligence layer on top of a production platform

## 📅 Build Plan
- Week 1 — Foundation: data generation, MinIO, PostgreSQL
- Week 2 — Pipelines: dbt, Airflow orchestration
- Week 3 — Quality & Infrastructure: Great Expectations, Docker, CI/CD
- Week 4 — ML & Serving: XGBoost model, FastAPI, Streamlit dashboard