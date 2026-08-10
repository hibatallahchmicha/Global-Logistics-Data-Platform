"""
streaming/spark_streaming.py

Spark Structured Streaming: Kafka -> parse -> enrich -> upsert into
Postgres. Fixes the original's non-idempotent sink -- a real
ON CONFLICT DO NOTHING upsert per partition, not a plain JDBC append.

Depends on: infra/schema.sql (3) -- realtime_shipments.event_id UNIQUE.
Consumes: streaming/kafka_producer.py (12).

Uses plain os.getenv, not common.config -- see module notes on why.
"""

import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "shipment_events")
POSTGRES_HOST           = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT           = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB             = os.getenv("POSTGRES_DB", "logiflow")
POSTGRES_USER           = os.getenv("POSTGRES_USER", "logiflow")
POSTGRES_PASSWORD       = os.getenv("POSTGRES_PASSWORD", "logiflow")
TRIGGER_INTERVAL        = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")
CHECKPOINT_DIR          = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint/shipments")

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

UPSERT_SQL = """
    INSERT INTO realtime_shipments (
        event_id, event_type, event_timestamp, shipment_id, status,
        origin_city, destination_city, vehicle_type, weight_kg, distance_km,
        revenue, is_delayed, delay_hours, driver_rating, temperature_c,
        humidity_pct, cost_per_km, weather_risk
    ) VALUES %s
    ON CONFLICT (event_id) DO NOTHING
"""


def _upsert_partition(rows) -> None:
    """Runs once per Spark partition, on the executor. Real upsert --
    duplicate event_id (retries, replays) get skipped at the DB level
    instead of crashing the whole streaming query."""
    rows = list(rows)
    if not rows:
        return
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        cur = conn.cursor()
        values = [
            (r.event_id, r.event_type, r.event_timestamp, r.shipment_id, r.status,
             r.origin_city, r.destination_city, r.vehicle_type, r.weight_kg, r.distance_km,
             r.revenue, r.is_delayed, r.delay_hours, r.driver_rating, r.temperature_c,
             r.humidity_pct, r.cost_per_km, r.weather_risk)
            for r in rows
        ]
        execute_values(cur, UPSERT_SQL, values)
        conn.commit()
    finally:
        conn.close()


def write_batch_to_postgres(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    batch_df.foreachPartition(_upsert_partition)
    print(f"[Spark] Batch {batch_id}: upserted (duplicates skipped automatically)")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LogiFlow-Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
    )

    enriched = (
        parsed
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn(
            "cost_per_km",
            when(col("distance_km") > 0, col("revenue") / col("distance_km")).otherwise(lit(0.0)),
        )
        .withColumn(
            "weather_risk",
            when((col("temperature_c") > 40) | (col("temperature_c") < 0), "HIGH")
            .when(col("humidity_pct") > 90, "MEDIUM")
            .otherwise("LOW"),
        )
    )

    query = (
        enriched.writeStream
        .foreachBatch(write_batch_to_postgres)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print(f"[Spark] Streaming started -- trigger interval: {TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()