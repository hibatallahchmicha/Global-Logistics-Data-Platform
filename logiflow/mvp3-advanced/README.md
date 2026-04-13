# MVP 3 - Advanced Intelligence and Orchestration

MVP 3 upgrades LogiFlow from a batch analytics platform into an intelligent, production-style system by combining:

- ML delay prediction
- Real weather-enriched shipment generation
- Airflow orchestration of end-to-end pipeline tasks

## Modules

### 3A - ML Prediction
Path: `3A-ml-prediction/`

What is included:
- Model training pipeline (`train.py`)
- Inference script (`predict.py`)
- Model artifact output (`models/delay_predictor.pkl`)
- Evaluation reports and plots (`reports/`)

Current evidence (generated in your workspace):

![ML Evaluation](3A-ml-prediction/reports/evaluation_20260413_161458.png)

### 3B - Real Data Integration
Path: `3B-real-data/`

What is included:
- Real weather integration via OpenWeather API
- Realistic shipment generation with weather effects
- CSV output consumed by ETL/Airflow

### 3C - Airflow Orchestration
Path: `3C-airflow-orchestration/`

What is included:
- DAG orchestration for full daily pipeline
- Task sequence:
  1. `generate_data`
  2. `upload_to_minio`
  3. `run_etl`
  4. `quality_check`
  5. `retrain_model`
  6. `pipeline_summary`
- Successful run state confirmed in Airflow UI (all tasks green)

## Airflow Success Screenshot

Save your Airflow success image in this folder:
- `assets/screenshots/airflow_pipeline_success.png`

Then this README will automatically display it:

![Airflow Pipeline Success](assets/screenshots/airflow_pipeline_success.png)


## How to Run MVP 3

From `logiflow/`:

```bash
docker compose up -d
```

Trigger DAG manually:

```bash
docker exec logiflow_airflow airflow dags trigger logiflow_daily_pipeline
```

Check task states in Airflow UI:
- URL: `http://localhost:8080`
- DAG: `logiflow_daily_pipeline`
