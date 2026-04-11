# MVP 1: Data Pipeline Foundation

## 🎯 Goal
Build a containerized ETL pipeline that extracts logistics data from MinIO 
and loads it into a PostgreSQL star schema.

## 📋 Features
- Generates realistic logistics data (10,000+ shipments)
- Uploads CSV files to MinIO data lake
- ETL pipeline with error handling
- Star schema with proper indexing

## 🏃 How to Run
1. Start Docker containers: `docker compose up -d`
2. Create schema: `docker exec -i logiflow_postgres psql ...`
3. Generate data: `python data-generation/generate_data.py`
4. Upload to MinIO: `python ingestion/upload_to_minio.py`
5. Run ETL: `python ingestion/etl_pipeline.py`

## 📊 Results
[Row counts, sample queries, verification steps]