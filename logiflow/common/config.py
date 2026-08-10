"""
common/config.py

Single source of truth for every environment-driven setting in LogiFlow.
Every other module imports `settings` from here instead of calling
os.getenv() itself.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT_DIR / ".env")


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    # --- PostgreSQL ---
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str
    postgres_db: str

    # --- MinIO / S3 ---
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bucket_name: str

    # --- Kafka (not used until the streaming modules, defined here anyway) ---
    kafka_bootstrap_servers: str
    kafka_topic: str

    # --- Optional: live weather enrichment ---
    openweather_api_key: str | None

    # --- Optional: live traffic enrichment ---
    tomtom_api_key: str | None

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def _load_settings() -> Settings:
    return Settings(
        postgres_user=_require("POSTGRES_USER"),
        postgres_password=_require("POSTGRES_PASSWORD"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=os.getenv("POSTGRES_PORT", "5432"),
        postgres_db=_require("POSTGRES_DB"),

        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=_require("MINIO_ROOT_USER"),
        minio_secret_key=_require("MINIO_ROOT_PASSWORD"),
        bucket_name=os.getenv("BUCKET_NAME", "logiflow-raw"),

        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "shipment_events"),

        openweather_api_key=os.getenv("OPENWEATHER_API_KEY") or None,
        tomtom_api_key=os.getenv("TOMTOM_API_KEY") or None,
    )


settings = _load_settings()


if __name__ == "__main__":
    masked = settings.database_url.replace(settings.postgres_password, "****")
    print("Postgres  ->", masked)
    print("MinIO     ->", settings.minio_endpoint, "| bucket:", settings.bucket_name)
    print("Kafka     ->", settings.kafka_bootstrap_servers, "| topic:", settings.kafka_topic)
    print("Weather   ->", "configured" if settings.openweather_api_key else "not set (will simulate)")
    print("Traffic   ->", "configured" if settings.tomtom_api_key else "not set (will simulate)")