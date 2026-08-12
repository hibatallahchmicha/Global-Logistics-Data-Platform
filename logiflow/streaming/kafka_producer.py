"""
streaming/kafka_producer.py

Publishes synthetic shipment events to Kafka at a configurable rate.
Independent of the batch pipeline -- demonstrates a streaming-shaped
data source, consumed by streaming/spark_streaming.py (Module 13).

Depends on: common.config (1) for Kafka connection settings.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime

from common.config import settings
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "1"))

STATUSES = ["PENDING", "IN_TRANSIT", "DELIVERED", "DELAYED", "CANCELLED"]
EVENT_TYPES = ["NEW_SHIPMENT", "STATUS_UPDATE", "LOCATION_UPDATE", "DELIVERY_COMPLETE"]
VEHICLE_TYPES = ["TRUCK", "VAN", "MOTORCYCLE", "SHIP", "PLANE"]
CITIES = [
    "Casablanca", "Rabat", "Marrakech", "Fes", "Tangier",
    "Paris", "Lyon", "Marseille", "London", "Madrid",
    "Barcelona", "Rome", "Berlin", "Amsterdam", "Brussels",
    "Dubai", "Istanbul", "Cairo", "Lagos", "Nairobi",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def generate_shipment_event() -> dict:
    is_delayed = random.random() < 0.20
    return {
        "event_id":          str(uuid.uuid4()),
        "event_type":        random.choice(EVENT_TYPES),
        "event_timestamp":   datetime.utcnow().isoformat(),
        "shipment_id":       f"RT-{random.randint(10000, 99999)}",
        "status":            random.choice(STATUSES),
        "origin_city":       random.choice(CITIES),
        "destination_city":  random.choice(CITIES),
        "vehicle_type":      random.choice(VEHICLE_TYPES),
        "weight_kg":         round(random.uniform(1, 5000), 2),
        "distance_km":       round(random.uniform(10, 5000), 2),
        "revenue":           round(random.uniform(50, 10000), 2),
        "is_delayed":        is_delayed,
        "delay_hours":       round(random.uniform(1, 72), 1) if is_delayed else 0.0,
        "driver_rating":     round(random.uniform(1, 5), 1),
        "temperature_c":     round(random.uniform(-5, 45), 1),
        "humidity_pct":      round(random.uniform(10, 100), 1),
    }


def connect_producer(max_attempts: int = 15) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
                compression_type="gzip",
            )
            log.info("Connected to Kafka at %s", settings.kafka_bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            log.warning("Attempt %d/%d -- Kafka not ready, retrying in 5s...", attempt, max_attempts)
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka after {max_attempts} attempts")


def main():
    log.info("Starting LogiFlow shipment event producer")
    log.info("Topic: %s | Rate: %.1f event/s", settings.kafka_topic, EVENTS_PER_SECOND)

    producer = connect_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    count = 0

    try:
        while True:
            event = generate_shipment_event()
            producer.send(settings.kafka_topic, key=event["shipment_id"], value=event)
            count += 1
            if count % 100 == 0:
                log.info("Published %d events total", count)
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("Shutting down -- flushing remaining messages...")
        producer.flush()
        producer.close()
        log.info("Producer stopped after %d events", count)


if __name__ == "__main__":
    main()