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

# Step 2 — Wait for PostgreSQL
echo "⏳ Waiting for PostgreSQL..."
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
    echo "   PostgreSQL not ready yet, retrying in 3s..."
    sleep 3
done
echo "✅ PostgreSQL is ready"

# Step 3 — Initialize Airflow database
echo "🗄️  Initializing Airflow database..."
airflow db init
airflow db upgrade
echo "✅ Database initialized"

# Step 4 — Create admin user from environment variables
echo "👤 Creating admin user..."
airflow users create \
    --username "${AIRFLOW_USER}" \
    --password "${AIRFLOW_PASSWORD}" \
    --firstname "${AIRFLOW_FIRSTNAME}" \
    --lastname "${AIRFLOW_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_EMAIL}" || echo "ℹ️  User already exists, skipping"
echo "✅ Admin user ready"

# Step 5 — Start Airflow services
echo "================================================"
echo "🌐 Airflow UI → http://localhost:8080"
echo "================================================"
airflow scheduler &
airflow webserver --port 8080