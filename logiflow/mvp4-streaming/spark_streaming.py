"""
MVP 4 — Spark Structured Streaming Job
Reads shipment events from Kafka → parses JSON → enriches → writes to PostgreSQL.

Pipeline:
  Kafka (shipment_events topic)
    → parse JSON with defined schema
    → compute cost_per_km  (revenue / distance_km)
    → classify weather_risk (LOW / MEDIUM / HIGH)
    → micro-batch sink → PostgreSQL realtime_shipments table

Trigger: every 10 seconds (configurable via SPARK_TRIGGER_INTERVAL env var).
Checkpoint stored at /tmp/spark-checkpoint/shipments (inside the container).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "shipment_events")
POSTGRES_HOST           = os.getenv("POSTGRES_HOST",           "localhost")
POSTGRES_PORT           = os.getenv("POSTGRES_PORT",           "5432")
POSTGRES_DB             = os.getenv("POSTGRES_DB",             "logiflow")
POSTGRES_USER           = os.getenv("POSTGRES_USER",           "logiflow")
POSTGRES_PASSWORD       = os.getenv("POSTGRES_PASSWORD",       "logiflow")
TRIGGER_INTERVAL        = os.getenv("SPARK_TRIGGER_INTERVAL",  "10 seconds")
CHECKPOINT_DIR          = "/tmp/spark-checkpoint/shipments"

POSTGRES_URL   = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_TABLE = "realtime_shipments"

# ── Kafka message schema ─────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("event_type",       StringType(),  True),
    StructField("event_timestamp",  StringType(),  True),
    StructField("shipment_id",      StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("origin_city",      StringType(),  True),
    StructField("destination_city", StringType(),  True),
    StructField("vehicle_type",     StringType(),  True),
    StructField("weight_kg",        DoubleType(),  True),
    StructField("distance_km",      DoubleType(),  True),
    StructField("revenue",          DoubleType(),  True),
    StructField("is_delayed",       BooleanType(), True),
    StructField("delay_hours",      DoubleType(),  True),
    StructField("driver_rating",    DoubleType(),  True),
    StructField("temperature_c",    DoubleType(),  True),
    StructField("humidity_pct",     DoubleType(),  True),
])


# ── Batch writer (foreachBatch) ──────────────────────────────────────────────

def write_batch_to_postgres(batch_df, batch_id: int) -> None:
    """Write a micro-batch DataFrame to PostgreSQL via JDBC."""
    row_count = batch_df.count()
    if row_count == 0:
        return

    (
        batch_df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  POSTGRES_TABLE)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        # INSERT … ON CONFLICT DO NOTHING handled at DB level via UNIQUE(event_id)
        .mode("append")
        .save()
    )
    print(f"[Spark] Batch {batch_id}: wrote {row_count} rows to {POSTGRES_TABLE}")


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LogiFlow-Streaming")
        .config(
            "spark.jars.packages",
            # Kafka connector + PostgreSQL JDBC (must match Scala 2.12 / Spark 3.5)
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Reduce shuffle partitions for single-worker dev setup
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] Reading from Kafka topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS}")

    # 1. Read raw bytes from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # 2. Parse JSON value
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
    )

    # 3. Enrich: cast timestamp + derived columns
    enriched = (
        parsed
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        # cost per km — guard against zero distance
        .withColumn(
            "cost_per_km",
            when(col("distance_km") > 0, col("revenue") / col("distance_km"))
            .otherwise(lit(0.0)),
        )
        # weather risk classification based on temperature and humidity
        .withColumn(
            "weather_risk",
            when((col("temperature_c") > 40) | (col("temperature_c") < 0), "HIGH")
            .when(col("humidity_pct") > 90, "MEDIUM")
            .otherwise("LOW"),
        )
    )

    # 4. Write micro-batches to PostgreSQL
    query = (
        enriched.writeStream
        .foreachBatch(write_batch_to_postgres)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print(f"[Spark] Streaming query started — trigger interval: {TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
