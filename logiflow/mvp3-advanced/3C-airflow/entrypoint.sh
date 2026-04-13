#!/bin/bash
set -e

echo "================================================"
echo "🚀 LogiFlow Airflow Startup"
echo "================================================"

# Step 1 — Install dependencies
echo "📦 Installing Python packages..."
pip install --quiet \
    pandas numpy faker minio \
    scikit-learn xgboost joblib \
    python-dotenv sqlalchemy \
    psycopg2-binary requests

echo "✅ Packages installed"

# Step 2 — Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL..."
until python -c "
import psycopg2
psycopg2.connect(
    host='postgres',
    port=5432,
    dbname='logiflow_dw',
    user='logiflow',
    password='Hiba2005'
)
print('PostgreSQL ready')
" 2>/dev/null; do
    echo "   PostgreSQL not ready yet, retrying in 3s..."
    sleep 3
done
echo "✅ PostgreSQL is ready"

# Step 3 — Initialize Airflow database
echo "🗄️  Initializing Airflow database..."
airflow db init
airflow db upgrade
echo "✅ Database initialized"

# Step 4 — Create admin user
echo "👤 Creating admin user..."
airflow users create \
    --username admin \
    --password Hiba2005 \
    --firstname Hiba \
    --lastname Chmicha \
    --role Admin \
    --email hiba@logiflow.com || echo "User already exists"
echo "✅ Admin user ready"

# Step 5 — Start Airflow
echo "================================================"
echo "🌐 Starting Airflow — http://localhost:8080"
echo "   Login: admin / Hiba2005"
echo "================================================"

airflow scheduler &
airflow webserver --port 8080
