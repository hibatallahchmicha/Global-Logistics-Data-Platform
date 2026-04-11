import os
import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OPENWEATHER_KEY = os.getenv("OPENWEATHER_API_KEY")

# Real city coordinates for routes
CITY_COORDS = {
    "Paris":       (48.8566,  2.3522),
    "Berlin":      (52.5200, 13.4050),
    "Madrid":      (40.4168, -3.7038),
    "Rome":        (41.9028, 12.4964),
    "Amsterdam":   (52.3676,  4.9041),
    "Casablanca":  (33.5731, -7.5898),
    "Tunis":       (36.8189,  10.1658),
    "Cairo":       (30.0444, 31.2357),
    "Dubai":       (25.2048, 55.2708),
    "Istanbul":    (41.0082, 28.9784),
    "London":      (51.5074, -0.1278),
    "Brussels":    (50.8503,  4.3517),
    "Vienna":      (48.2082, 16.3738),
    "Warsaw":      (52.2297, 21.0122),
    "Lisbon":      (38.7169, -9.1395),
}

ROUTE_PAIRS = [
    ("Paris",      "Berlin"),
    ("Madrid",     "Lisbon"),
    ("Casablanca", "Tunis"),
    ("Cairo",      "Dubai"),
    ("Istanbul",   "Vienna"),
    ("Amsterdam",  "Brussels"),
    ("London",     "Paris"),
    ("Berlin",     "Warsaw"),
    ("Rome",       "Vienna"),
    ("Dubai",      "Cairo"),
]


# ══════════════════════════════════════════════════════════════
# WEATHER FETCHER
# ══════════════════════════════════════════════════════════════

def fetch_real_weather(city: str) -> dict:
    """Fetch current weather for a city from OpenWeatherMap"""
    if not OPENWEATHER_KEY:
        log.warning("No OPENWEATHER_API_KEY — using simulated weather")
        return simulate_weather()

    lat, lon = CITY_COORDS.get(city, (48.8566, 2.3522))
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_KEY}&units=metric"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        condition_map = {
            "Clear":       "Clear",
            "Clouds":      "Cloudy",
            "Rain":        "Rain",
            "Snow":        "Snow",
            "Thunderstorm":"Storm",
            "Fog":         "Fog",
            "Mist":        "Fog",
            "Drizzle":     "Rain",
        }
        raw_condition = data["weather"][0]["main"]
        condition     = condition_map.get(raw_condition, "Clear")

        return {
            "weather_condition":  condition,
            "temperature_celsius": round(data["main"]["temp"], 1),
            "wind_speed_kmh":     round(data["wind"]["speed"] * 3.6, 1),
        }

    except Exception as e:
        log.warning(f"Weather API error for {city}: {e} — using simulated")
        return simulate_weather()


def simulate_weather() -> dict:
    """Fallback when API key not available"""
    conditions = ["Clear", "Rain", "Cloudy", "Snow", "Fog", "Storm"]
    weights    = [0.40,    0.25,   0.20,    0.05,  0.06,  0.04]
    condition  = np.random.choice(conditions, p=weights)
    return {
        "weather_condition":   condition,
        "temperature_celsius": round(np.random.uniform(-10, 40), 1),
        "wind_speed_kmh":      round(np.random.uniform(0, 100), 1),
    }


# ══════════════════════════════════════════════════════════════
# SHIPMENT GENERATOR WITH REAL WEATHER
# ══════════════════════════════════════════════════════════════

CUSTOMERS = [
    ("Carrefour SA",     "Retail",    "France",  "Paris",     "Enterprise", "Annual"),
    ("Siemens AG",       "Tech",      "Germany", "Berlin",    "Enterprise", "Annual"),
    ("Zara Group",       "Fashion",   "Spain",   "Madrid",    "Enterprise", "Monthly"),
    ("NovaTech",         "Tech",      "Morocco", "Casablanca","SME",        "Monthly"),
    ("AlphaLogistics",   "Logistics", "Egypt",   "Cairo",     "SME",        "Spot"),
    ("BlueStar Ltd",     "Retail",    "UAE",     "Dubai",     "Startup",    "Spot"),
    ("GreenMove GmbH",   "Energy",    "Germany", "Berlin",    "SME",        "Annual"),
    ("MedEx Corp",       "Medical",   "France",  "Paris",     "Enterprise", "Annual"),
]

DRIVERS = [
    ("Ahmed Hassan",   "CE", 8,  4.7, "Morocco"),
    ("Marie Dupont",   "C",  5,  4.2, "France"),
    ("Klaus Weber",    "CE", 12, 4.9, "Germany"),
    ("Sara Ali",       "B",  3,  3.8, "Egypt"),
    ("Juan Garcia",    "CE", 7,  4.5, "Spain"),
    ("Amina Benali",   "C",  2,  3.5, "Morocco"),
    ("Pierre Martin",  "CE", 15, 4.8, "France"),
    ("Yusuf Al-Amin",  "C",  4,  4.0, "UAE"),
]

VEHICLES = [
    ("TRK-001", "Truck",      20000, 2018, 150000),
    ("VAN-002", "Van",         3500, 2021,  45000),
    ("TRK-003", "Truck",      25000, 2015, 280000),
    ("VAN-004", "Van",         3000, 2022,  20000),
    ("TRK-005", "Truck",      18000, 2019, 120000),
    ("MCY-006", "Motorcycle",   500, 2023,   8000),
    ("TRK-007", "Truck",      22000, 2016, 210000),
    ("VAN-008", "Van",         4000, 2020,  80000),
]


def generate_real_shipment(date: datetime) -> dict:
    """Generate one shipment with real weather data"""

    # Pick random route
    origin_city, dest_city = ROUTE_PAIRS[np.random.randint(len(ROUTE_PAIRS))]

    # Fetch REAL weather for origin city
    weather = fetch_real_weather(origin_city)

    # Pick random customer, driver, vehicle
    customer = CUSTOMERS[np.random.randint(len(CUSTOMERS))]
    driver   = DRIVERS[np.random.randint(len(DRIVERS))]
    vehicle  = VEHICLES[np.random.randint(len(VEHICLES))]

    # Calculate distance (simplified)
    lat1, lon1 = CITY_COORDS[origin_city]
    lat2, lon2 = CITY_COORDS[dest_city]
    distance   = int(((lat2-lat1)**2 + (lon2-lon1)**2)**0.5 * 111)

    # Base delay probability influenced by REAL weather
    base_delay_prob = 0.25
    if weather["weather_condition"] == "Snow":   base_delay_prob += 0.35
    if weather["weather_condition"] == "Storm":  base_delay_prob += 0.30
    if weather["weather_condition"] == "Rain":   base_delay_prob += 0.15
    if weather["weather_condition"] == "Fog":    base_delay_prob += 0.10
    if weather["wind_speed_kmh"] > 70:           base_delay_prob += 0.15
    if driver[2] < 3:                            base_delay_prob += 0.10  # low experience
    if driver[3] < 3.5:                          base_delay_prob += 0.10  # low rating

    is_delayed     = np.random.random() < base_delay_prob
    delay_minutes  = int(np.random.exponential(60)) if is_delayed else np.random.randint(-10, 30)
    delay_minutes  = max(delay_minutes, -10)

    planned_hrs    = round(distance / 80, 2)
    actual_hrs     = round(planned_hrs + delay_minutes / 60, 2)

    weight_kg      = round(np.random.uniform(100, vehicle[2] * 0.9), 2)
    cost_usd       = round(distance * np.random.uniform(1.2, 2.5), 2)
    fuel_liters    = round(distance * np.random.uniform(0.25, 0.45), 2)

    scheduled_pickup   = date.replace(hour=np.random.randint(6, 14), minute=0, second=0)
    actual_pickup      = scheduled_pickup + timedelta(minutes=np.random.randint(-5, 20))
    scheduled_delivery = scheduled_pickup + timedelta(hours=planned_hrs)
    actual_delivery    = scheduled_delivery + timedelta(minutes=delay_minutes)

    region_map = {
        ("Paris","Berlin"):    "Europe",
        ("Madrid","Lisbon"):   "Europe",
        ("Casablanca","Tunis"):"Africa",
        ("Cairo","Dubai"):     "Middle East",
        ("Istanbul","Vienna"): "Europe",
        ("Amsterdam","Brussels"):"Europe",
        ("London","Paris"):    "Europe",
        ("Berlin","Warsaw"):   "Europe",
        ("Rome","Vienna"):     "Europe",
        ("Dubai","Cairo"):     "Middle East",
    }

    return {
        # Customer
        "company_name":    customer[0],
        "industry":        customer[1],
        "customer_country":customer[2],
        "customer_city":   customer[3],
        "segment":         customer[4],
        "contract_type":   customer[5],
        # Driver
        "driver_name":     driver[0],
        "license_type":    driver[1],
        "experience_years":driver[2],
        "driver_rating":   driver[3],
        "driver_country":  driver[4],
        # Vehicle
        "plate_number":    vehicle[0],
        "vehicle_type":    vehicle[1],
        "capacity_kg":     vehicle[2],
        "manufacture_year":vehicle[3],
        "mileage_km":      vehicle[4],
        # Route
        "origin_city":     origin_city,
        "destination_city":dest_city,
        "distance_km":     distance,
        "region":          region_map.get((origin_city, dest_city), "Europe"),
        "route_type":      "Road",
        # Shipment facts
        "weight_kg":             weight_kg,
        "cost_usd":              cost_usd,
        "fuel_consumed_liters":  fuel_liters,
        "planned_duration_hrs":  planned_hrs,
        "actual_duration_hrs":   actual_hrs,
        "delay_minutes":         delay_minutes,
        "is_delayed":            is_delayed,
        "status": (
            "failed"   if delay_minutes > 240 else
            "delayed"  if is_delayed else
            "on_time"
        ),
        # Real weather
        **weather,
        # Timestamps
        "scheduled_pickup":   scheduled_pickup.isoformat(),
        "actual_pickup":      actual_pickup.isoformat(),
        "scheduled_delivery": scheduled_delivery.isoformat(),
        "actual_delivery":    actual_delivery.isoformat(),
        # Date fields
        "date": date.date().isoformat(),
    }


def generate_batch(n: int = 50, days_back: int = 1) -> pd.DataFrame:
    """Generate n shipments for the last N days"""
    log.info(f"🏭 Generating {n} real shipments (last {days_back} days)...")
    records = []

    for i in range(n):
        days_ago = np.random.randint(0, days_back)
        date     = datetime.now() - timedelta(days=days_ago)
        record   = generate_real_shipment(date)
        records.append(record)

        if (i+1) % 10 == 0:
            log.info(f"   Generated {i+1}/{n}...")

    df = pd.DataFrame(records)
    log.info(f"✅ Done — {len(df)} shipments with real weather data")
    return df


def save_batch(df: pd.DataFrame, output_dir: str = "data"):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"real_shipments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path     = os.path.join(output_dir, filename)
    df.to_csv(path, index=False)
    log.info(f"💾 Saved → {path}")
    return path


if __name__ == "__main__":
    df   = generate_batch(n=100, days_back=7)
    path = save_batch(df)
    print(f"\n✅ Generated {len(df)} shipments with real weather")
    print(f"   Delayed: {df['is_delayed'].sum()} ({df['is_delayed'].mean()*100:.1f}%)")
    print(f"   Saved  : {path}")
    print(df[["origin_city","destination_city","weather_condition",
              "temperature_celsius","status"]].head(10))