FROM python:3.11-slim

WORKDIR /app

# Build context is the repo root (Module 14) so common/ and ml/ can be
# copied in as real siblings -- this is what fixes the original path
# bug. No relative traversal needed; the container mirrors the local layout.
COPY services/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        "scikit-learn==1.4.2" "xgboost==2.0.3" "joblib>=1.3" "numpy>=1.26" "pandas>=2.2"

COPY common/ common/
COPY ml/ ml/
COPY services/api/ services/api/

EXPOSE 8000
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]