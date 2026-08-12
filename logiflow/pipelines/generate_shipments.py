"""
pipelines/generate_shipments.py

Generates synthetic (but realistically correlated) shipment records and
uploads them straight to object storage as one CSV per run.

Two live data sources, same rule for both: real + cached-per-city for
near-term batches (days_back <= 7), simulated for historical seeding --
a live snapshot API can't tell you what the weather or traffic was a
year ago, so it's not even logically valid to call it for backdated data.

Depends on:
  - common.config.settings   (Module 1) -- weather + traffic API keys
  - common.storage.storage   (Module 2) -- upload

Feeds into:
  - pipelines/etl.py (Module 5), which reads whatever this uploads
    under raw/ and loads it into the warehouse.
"""

import argparse
import logging
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from common.config import settings
from common.storage import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# FIXED ROSTER -- reused every run, on purpose (see Module 4 notes)
# ══════════════════════════════════════════════════════════════

CUSTOMERS = [
    ("Carrefour SA",   "Retail",    "France",  "Paris",      "Enterprise", "Annual"),
    ("Siemens AG",     "Tech",      "Germany", "Berlin",     "Enterprise", "Annual"),
    ("Zara Group",     "Fashion",   "Spain",   "Madrid",     "Enterprise", "Monthly"),
    ("NovaTech",       "Tech",      "Morocco", "Casablanca", "SME",        "Monthly"),
    ("AlphaLogistics", "Logistics", "Egypt",   "Cairo",      "SME",        "Spot"),
    ("BlueStar Ltd",   "Retail",    "UAE",     "Dubai",      "Startup",    "Spot"),
    ("GreenMove GmbH", "Energy",    "Germany", "Berlin",     "SME",        "Annual"),
    ("MedEx Corp",     "Medical",   "France",  "Paris",      "Enterprise", "Annual"),
]

DRIVERS = [
    ("Ahmed Hassan",  "CE", 8,  4.7, "Morocco"),
    ("Marie Dupont",  "C",  5,  4.2, "France"),
    ("Klaus Weber",   "CE", 12, 4.9, "Germany"),
    ("Sara Ali",      "B",  3,  3.8, "Egypt"),
    ("Juan Garcia",   "CE", 7,  4.5, "Spain"),
    ("Amina Benali",  "C",  2,  3.5, "Morocco"),
    ("Pierre Martin", "CE", 15, 4.8, "France"),
    ("Yusuf Al-Amin", "C",  4,  4.0, "UAE"),
]

VEHICLES = [
    ("TRK-001", "Truck", 20000, 2018, 150000),
    ("VAN-002", "Van",    3500, 2021,  45000),
    ("TRK-003", "Truck", 25000, 2015, 280000),
    ("VAN-004", "Van",    3000, 2022,  20000),
    ("TRK-005", "Truck", 18000, 2019, 120000),
    ("MCY-006", "Motorcycle", 500, 2023, 8000),
    ("TRK-007", "Truck", 22000, 2016, 210000),
    ("VAN-008", "Van",    4000, 2020,  80000),
]

ROUTES = [
    ("Casablanca", "Morocco", "Paris",    "France",   2400, "Europe-Africa", "Road"),
    ("London",     "UK",      "Berlin",   "Germany",   930, "Europe",        "Road"),
    ("Dubai",      "UAE",     "Cairo",    "Egypt",    2400, "Middle East",   "Air"),
    ("Paris",      "France",  "Madrid",   "Spain",    1270, "Europe",        "Road"),
    ("Berlin",     "Germany", "Warsaw",   "Poland",    575, "Europe",        "Road"),
    ("Madrid",     "Spain",   "Lisbon",   "Portugal",  630, "Europe",        "Road"),
    ("Cairo",      "Egypt",   "Dubai",    "UAE",      2400, "Middle East",   "Road"),
    ("Casablanca", "Morocco", "Tunis",    "Tunisia",  1780, "Africa",        "Road"),
    ("Amsterdam",  "NL",      "Brussels", "Belgium",   210, "Europe",        "Road"),
    ("Berlin",     "Germany", "Vienna",   "Austria",   680, "Europe",        "Road"),
]

CITY_COORDS = {
    "Casablanca": (33.5731, -7.5898),  "Paris":    (48.8566,  2.3522),
    "London":     (51.5074, -0.1278),  "Berlin":   (52.5200, 13.4050),
    "Dubai":      (25.2048, 55.2708),  "Cairo":    (30.0444, 31.2357),
    "Madrid":     (40.4168, -3.7038),  "Warsaw":   (52.2297, 21.0122),
    "Lisbon":     (38.7169, -9.1395),  "Tunis":    (36.8189, 10.1658),
    "Amsterdam":  (52.3676,  4.9041),  "Brussels": (50.8503,  4.3517),
    "Vienna":     (48.2082, 16.3738),
}

WEATHER_CONDITIONS = ["Clear", "Cloudy", "Rain", "Heavy Rain", "Fog", "Snow"]
WEATHER_WEIGHTS    = [0.40,    0.25,     0.18,   0.07,         0.06,  0.04]

_weather_cache: dict[str, dict] = {}
_traffic_cache: dict[str, dict] = {}


# ══════════════════════════════════════════════════════════════
# WEATHER
# ══════════════════════════════════════════════════════════════

def fetch_weather(city: str, use_real: bool) -> dict:
    if not use_real or not settings.openweather_api_key:
        return _simulate_weather()
    if city in _weather_cache:
        return _weather_cache[city]

    lat, lon = CITY_COORDS.get(city, (48.8566, 2.3522))
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={settings.openweather_api_key}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        condition_map = {
            "Clear": "Clear", "Clouds": "Cloudy", "Rain": "Rain", "Snow": "Snow",
            "Thunderstorm": "Storm", "Fog": "Fog", "Mist": "Fog", "Drizzle": "Rain", "Haze": "Fog",
        }
        result = {
            "weather_condition":   condition_map.get(data["weather"][0]["main"], "Clear"),
            "temperature_celsius": round(data["main"]["temp"], 1),
            "wind_speed_kmh":      round(data["wind"]["speed"] * 3.6, 1),
        }
    except Exception as e:  # noqa: BLE001 -- external API, any failure should fall back to simulated data
        status = getattr(getattr(e, "response", None), "status_code", "unknown")
        log.warning("Weather API error for %s: HTTP %s -- using simulated", city, status)
        result = _simulate_weather()

    _weather_cache[city] = result
    return result


def _simulate_weather() -> dict:
    condition = np.random.choice(WEATHER_CONDITIONS, p=WEATHER_WEIGHTS)
    return {
        "weather_condition":   condition,
        "temperature_celsius": round(np.random.uniform(-10, 40), 1),
        "wind_speed_kmh":      round(np.random.uniform(0, 100), 1),
    }


# ══════════════════════════════════════════════════════════════
# TRAFFIC
# ══════════════════════════════════════════════════════════════

def fetch_traffic(city: str, use_real: bool) -> dict:
    """Same rule as weather: real + cached-per-city near-term, simulated for history."""
    if not use_real or not settings.tomtom_api_key:
        return _simulate_traffic()
    if city in _traffic_cache:
        return _traffic_cache[city]

    lat, lon = CITY_COORDS.get(city, (48.8566, 2.3522))
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={lat},{lon}&key={settings.tomtom_api_key}"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        flow = resp.json()["flowSegmentData"]
        current, free_flow = flow["currentSpeed"], flow["freeFlowSpeed"]
        ratio = round(current / free_flow, 2) if free_flow else 1.0
        result = {
            "traffic_congestion_ratio": ratio,
            "traffic_condition": _classify_congestion(ratio),
        }
    except Exception as e:  # noqa: BLE001 -- external API, any failure should fall back to simulated data
        status = getattr(getattr(e, "response", None), "status_code", "unknown")
        log.warning("Traffic API error for %s: HTTP %s -- using simulated", city, status)
        result = _simulate_traffic()

    _traffic_cache[city] = result
    return result


def _simulate_traffic() -> dict:
    ratio = round(float(np.clip(np.random.normal(0.75, 0.15), 0.2, 1.0)), 2)
    return {
        "traffic_congestion_ratio": ratio,
        "traffic_condition": _classify_congestion(ratio),
    }


def _classify_congestion(ratio: float) -> str:
    if ratio >= 0.8:
        return "LOW"
    if ratio >= 0.5:
        return "MEDIUM"
    return "HIGH"


# ══════════════════════════════════════════════════════════════
# ONE SHIPMENT
# ══════════════════════════════════════════════════════════════

def _generate_one_shipment(pickup_date: datetime, use_real: bool) -> dict:
    origin_city, origin_country, dest_city, dest_country, distance_km, region, route_type = \
        ROUTES[np.random.randint(len(ROUTES))]
    weather  = fetch_weather(origin_city, use_real)
    traffic  = fetch_traffic(origin_city, use_real)
    customer = CUSTOMERS[np.random.randint(len(CUSTOMERS))]
    driver   = DRIVERS[np.random.randint(len(DRIVERS))]
    vehicle  = VEHICLES[np.random.randint(len(VEHICLES))]

    risk = 0.10
    risk += {"Clear": 0, "Cloudy": 0.02, "Rain": 0.10, "Heavy Rain": 0.20,
             "Fog": 0.15, "Snow": 0.25}.get(weather["weather_condition"], 0.05)
    risk += {"LOW": 0, "MEDIUM": 0.08, "HIGH": 0.18}.get(traffic["traffic_condition"], 0.05)
    if driver[3] < 3.0:   risk += 0.15
    elif driver[3] < 4.0: risk += 0.05
    vehicle_age = datetime.now().year - vehicle[3]
    if vehicle_age > 10 and vehicle[4] > 200000: risk += 0.15
    elif vehicle_age > 5:                        risk += 0.05
    if distance_km > 2000:   risk += 0.10
    elif distance_km > 1000: risk += 0.05
    risk = min(risk, 0.95)

    is_delayed = np.random.random() < risk

    if is_delayed:
        delay_minutes = int(np.random.exponential(90)) + 31
    else:
        delay_minutes = int(np.random.randint(0, 31))

    planned_hrs = round(distance_km / 75, 2)
    actual_hrs  = round(planned_hrs + delay_minutes / 60, 2)

    scheduled_pickup   = pickup_date.replace(hour=int(np.random.randint(6, 18)), minute=int(np.random.randint(0, 60)), second=0, microsecond=0)
    actual_pickup      = scheduled_pickup + timedelta(minutes=int(np.random.randint(0, 20)))
    scheduled_delivery = scheduled_pickup + timedelta(hours=planned_hrs)
    actual_delivery    = actual_pickup + timedelta(hours=actual_hrs)

    status = "failed" if delay_minutes > 240 else "delayed" if is_delayed else "on_time"

    return {
        "source_shipment_id": str(uuid.uuid4()),

        "company_name": customer[0], "industry": customer[1],
        "customer_country": customer[2], "customer_city": customer[3],
        "segment": customer[4], "contract_type": customer[5],

        "driver_name": driver[0], "license_type": driver[1],
        "experience_years": driver[2], "driver_rating": driver[3],
        "driver_country": driver[4],

        "plate_number": vehicle[0], "vehicle_type": vehicle[1],
        "capacity_kg": vehicle[2], "manufacture_year": vehicle[3],
        "mileage_km": vehicle[4],

        "origin_city": origin_city, "origin_country": origin_country,
        "destination_city": dest_city, "destination_country": dest_country,
        "distance_km": distance_km, "region": region, "route_type": route_type,

        "scheduled_pickup": scheduled_pickup.isoformat(),
        "actual_pickup": actual_pickup.isoformat(),
        "scheduled_delivery": scheduled_delivery.isoformat(),
        "actual_delivery": actual_delivery.isoformat(),

        "planned_duration_hrs": planned_hrs, "actual_duration_hrs": actual_hrs,
        "delay_minutes": delay_minutes, "is_delayed": is_delayed, "status": status,

        "weight_kg": round(np.random.uniform(100, vehicle[2] * 0.85), 2),
        "cost_usd": round(distance_km * np.random.uniform(1.0, 1.8), 2),
        "fuel_consumed_liters": round(distance_km * np.random.uniform(0.25, 0.40), 2),

        **weather,
        **traffic,
    }


# ══════════════════════════════════════════════════════════════
# BATCH GENERATION + UPLOAD
# ══════════════════════════════════════════════════════════════

def generate_batch(n: int, days_back: int) -> pd.DataFrame:
    use_real = days_back <= 7
    log.info("Generating %d shipments (last %d day(s), live data=%s)...",
              n, days_back, use_real)

    log_every = max(1, n // 20)
    records = []
    for i in range(n):
        days_ago = int(np.random.randint(0, max(days_back, 1)))
        pickup_date = datetime.now() - timedelta(days=days_ago)
        records.append(_generate_one_shipment(pickup_date, use_real))
        if (i + 1) % log_every == 0:
            log.info("  ...%d/%d", i + 1, n)

    df = pd.DataFrame(records)
    log.info("Done. Delay rate: %.1f%%", df["is_delayed"].mean() * 100)
    return df


def run(n: int = 50, days_back: int = 1) -> str:
    df = generate_batch(n, days_back)

    object_key = f"raw/shipments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    storage.upload_bytes(object_key, csv_bytes)

    log.info("Uploaded %d shipments (%.1f MB) -> %s/%s",
              len(df), len(csv_bytes) / 1_048_576, settings.bucket_name, object_key)
    return object_key


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic LogiFlow shipments")
    parser.add_argument("--n", type=int, default=50, help="number of shipments to generate")
    parser.add_argument("--days-back", type=int, default=1, help="spread shipments over this many past days")
    args = parser.parse_args()

    run(n=args.n, days_back=args.days_back)