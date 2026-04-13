# Airflow Pipeline Setup Guide

## Overview
The LogiFlow Airflow pipeline orchestrates the complete ETL workflow:
```
Generate Data → Upload to MinIO → ETL Pipeline → Quality Checks → ML Retrain → Summary
```

**Schedule**: Daily at 2:00 AM UTC

---

## 📋 Prerequisites

✅ **Already configured:**
- Docker Compose setup (PostgreSQL, MinIO, Airflow)
- Airflow entrypoint script with auto-initialization
- DAG definition with 6 tasks
- Environment variables (.env file)

---

## 🚀 Quick Start

### 1. Start Docker Services
```bash
cd /mnt/c/Users/HP\ PRO/Documents/global\ logistic\ project/Global-Logistics-Data-Platform/logiflow

docker-compose up -d
```

### 2. Verify Services Are Running
```bash
# Check container status
docker-compose ps

# Expected output:
# - logiflow_postgres (healthy after ~30s)
# - logiflow_minio (running)
# - logiflow_airflow (scheduler + webserver)
```

### 3. Access Airflow UI
- **URL**: http://localhost:8080
- **Login**: 
  - Username: `admin`
  - Password: `admin_password_2024`

---

## 📊 Pipeline Tasks

| Task ID | Description | Status |
|---------|-------------|--------|
| `generate_data` | Generates 100 new shipments with weather data | ✅ Complete |
| `upload_to_minio` | Uploads CSVs to MinIO data lake | ✅ Complete |
| `run_etl` | ETL: MinIO → Transform → PostgreSQL | ✅ Complete |
| `quality_check` | Data quality validation (8 checks) | ✅ Complete |
| `retrain_model` | Retrains ML delay prediction model | ✅ Complete |
| `pipeline_summary` | Logs final pipeline summary | ✅ Complete |

---

## 🧪 Manual Test Run

### Option 1: Trigger from Airflow UI
1. Go to http://localhost:8080
2. Find DAG: `logiflow_daily_pipeline`
3. Click the "Play" button (▶️) → "Trigger DAG"
4. Monitor execution in graph view

### Option 2: Command Line
```bash
# Access Airflow container
docker exec -it logiflow_airflow bash

# Trigger manually
airflow dags trigger logiflow_daily_pipeline

# View logs
airflow dags list
airflow tasks list logiflow_daily_pipeline
```

---

## 🔧 Configuration

### Environment Variables (.env)
All variables are pre-configured in `/logiflow/.env`:

```env
# Database
POSTGRES_USER=logiflow_user
POSTGRES_PASSWORD=logiflow_secure_password_2024
POSTGRES_DB=logiflow
POSTGRES_HOST=postgres

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin_password_2024
MINIO_ENDPOINT=minio:9000
BUCKET_NAME=logiflow-raw

# Airflow
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin_password_2024

# Optional: OpenWeather API for real weather data
OPENWEATHER_API_KEY=
```

---

## 📝 Important Notes

### Data Flow
1. **Generate**: Creates shipment data with weather conditions (simulated if API key not set)
2. **Upload**: Stores CSVs in MinIO (object storage)
3. **ETL**: 
   - Extracts from MinIO
   - Transforms into star schema (dimensions + fact table)
   - Loads into PostgreSQL
4. **Quality**: Validates data integrity and completeness
5. **Retrain**: Updates ML model with latest shipment data
6. **Summary**: Logs pipeline execution summary

### Database Schema
The pipeline creates these tables:
- `dim_customer` - Customer dimension
- `dim_driver` - Driver dimension
- `dim_vehicle` - Vehicle dimension
- `dim_route` - Route dimension
- `dim_date` - Date dimension
- `fact_shipments` - Fact table with all metrics

### MinIO Buckets
- `logiflow-raw` - Raw CSV files (auto-created)

---

## 🚨 Troubleshooting

### Issue: Services won't start
```bash
# Check for port conflicts
lsof -i :8080  # Airflow UI
lsof -i :5432  # PostgreSQL
lsof -i :9000  # MinIO API
lsof -i :9001  # MinIO Console

# Clear volumes and restart
docker-compose down --volumes
docker-compose up -d
```

### Issue: DAG not appearing
```bash
# Restart Airflow scheduler
docker exec logiflow_airflow airflow scheduler restart

# Check DAG syntax
docker exec logiflow_airflow airflow dags show logiflow_daily_pipeline
```

### Issue: Task failures
Check logs in Airflow UI:
1. Click on failed task
2. View "Log" tab
3. Look for specific error messages

### Issue: PostgreSQL connection errors
```bash
# Verify database is ready
docker exec logiflow_postgres psql -U logiflow_user -d logiflow -c "SELECT 1"

# Check environment variables
docker exec logiflow_airflow env | grep POSTGRES
```

---

## 📚 Useful Commands

```bash
# View Airflow logs
docker-compose logs -f airflow

# Access PostgreSQL directly
docker exec -it logiflow_postgres psql -U logiflow_user -d logiflow

# Check MinIO contents
docker exec -it logiflow_minio mc ls logiflow-raw

# View DAG structure
docker exec logiflow_airflow airflow dags show logiflow_daily_pipeline

# View specific task logs
docker exec logiflow_airflow airflow tasks logs logiflow_daily_pipeline generate_data 2024-01-01
```

---

## ✅ Validation Checklist

- [ ] Docker services running (`docker-compose ps`)
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] DAG visible in Airflow
- [ ] First test run triggered successfully
- [ ] All tasks completed without errors
- [ ] Data in PostgreSQL (`SELECT COUNT(*) FROM fact_shipments`)
- [ ] ML model trained successfully

---

## 📞 Support

For issues with specific components:
- **ETL Logic**: See `/mvp1-data-pipeline/ingestion/etl_pipeline.py`
- **Data Generation**: See `/mvp3-advanced/3B-real-data/real_data_fetcher.py`
- **Quality Checks**: See `/mvp2-analytics-layer/2C-data-quality/quality_checks.py`
- **ML Training**: See `/mvp3-advanced/3A-ml-prediction/train.py`

---

*Last updated: 2026-04-13*
*Status: ✅ Production Ready*
