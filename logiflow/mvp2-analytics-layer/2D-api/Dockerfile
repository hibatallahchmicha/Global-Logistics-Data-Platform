FROM python:3.11-slim

WORKDIR /app

# Install API deps + ML inference deps (CPU-only, no CUDA)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        "scikit-learn==1.4.2" \
        "xgboost==2.0.3" \
        "joblib>=1.3" \
        "numpy>=1.26" \
        "pandas>=2.2"

# Copy API source
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
