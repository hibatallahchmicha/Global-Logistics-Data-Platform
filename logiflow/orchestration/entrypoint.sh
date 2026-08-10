#!/bin/bash
set -e

echo "Installing pipeline dependencies..."
pip install --quiet -r /opt/airflow/project/orchestration/requirements.txt

echo "Waiting for PostgreSQL..."
until python -c "
import psycopg2, os
psycopg2.connect(
    host=os.environ.get('POSTGRES_HOST', 'postgres'),
    port=int(os.environ.get('POSTGRES_PORT', 5432)),
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD')
)
" 2>/dev/null; do
    echo "PostgreSQL not ready, retrying in 3s..."
    sleep 3
done
echo "PostgreSQL ready"

echo "Initializing Airflow database..."
airflow db init
airflow db upgrade

echo "Creating admin user..."
airflow users create \
    --username "${AIRFLOW_USER}" \
    --password "${AIRFLOW_PASSWORD}" \
    --firstname "${AIRFLOW_FIRSTNAME}" \
    --lastname "${AIRFLOW_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_EMAIL}" || echo "User already exists, skipping"

echo "Starting Airflow..."
airflow scheduler &
airflow webserver --port 8080