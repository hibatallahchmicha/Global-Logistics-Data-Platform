"""
MVP 4 — Kafka Shipment Event Producer
Generates realistic shipment events and publishes them to a Kafka topic
at a configurable rate (default: 1 event/second).

Event types:
  - NEW_SHIPMENT       : a brand-new shipment just entered the system
  - STATUS_UPDATE      : an existing shipment changed status
  - LOCATION_UPDATE    : GPS ping with current location metadata
  - DELIVERY_COMPLETE  : shipment was delivered

Each event is a JSON object consumed downstream by Spark Streaming.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config from environment ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipment_events")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "1"))

# ── Domain constants ─────────────────────────────────────────────────────────
STATUSES = ["PENDING", "IN_TRANSIT", "DELIVERED", "DELAYED", "CANCELLED"]
EVENT_TYPES = ["NEW_SHIPMENT", "STATUS_UPDATE", "LOCATION_UPDATE", "DELIVERY_COMPLETE"]
VEHICLE_TYPES = ["TRUCK", "VAN", "MOTORCYCLE", "SHIP", "PLANE"]
CITIES = [
    "Casablanca", "Rabat", "Marrakech", "Fes", "Tangier",
    "Paris", "Lyon", "Marseille", "London", "Madrid",
    "Barcelona", "Rome", "Berlin", "Amsterdam", "Brussels",
    "Dubai", "Istanbul", "Cairo", "Lagos", "Nairobi",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Event generation ─────────────────────────────────────────────────────────

def generate_shipment_event() -> dict:
    is_delayed = random.random() < 0.20          # 20 % of events are delayed
    temp = round(random.uniform(-5, 45), 1)
    humidity = round(random.uniform(10, 100), 1)

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
        "temperature_c":     temp,
        "humidity_pct":      humidity,
    }


# ── Kafka connection (with retry) ────────────────────────────────────────────

def connect_producer(max_attempts: int = 15) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,               # small batching buffer
                compression_type="gzip",
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("Attempt %d/%d — Kafka not ready, retrying in 5 s …",
                           attempt, max_attempts)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % max_attempts)


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    logger.info("Starting LogiFlow shipment event producer")
    logger.info("Topic: %s  |  Rate: %.1f event/s", KAFKA_TOPIC, EVENTS_PER_SECOND)

    producer = connect_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    count = 0

    try:
        while True:
            event = generate_shipment_event()
            producer.send(KAFKA_TOPIC, key=event["shipment_id"], value=event)
            count += 1
            if count % 100 == 0:
                logger.info("Published %d events total", count)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Shutting down — flushing remaining messages …")
        producer.flush()
        producer.close()
        logger.info("Producer stopped after %d events", count)


if __name__ == "__main__":
    main()
