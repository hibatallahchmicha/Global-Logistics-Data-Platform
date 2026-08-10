FROM python:3.11-slim

WORKDIR /app

COPY services/dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY services/dashboard/ services/dashboard/

EXPOSE 8501
CMD ["streamlit", "run", "services/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]