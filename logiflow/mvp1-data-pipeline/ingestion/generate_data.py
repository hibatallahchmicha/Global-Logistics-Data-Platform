"""
Generates realistic fake logistics data for LogiFlow (mvp 1 )

Why fake data?
- No real company gives you their shipment data
- Faker lets us control distributions (delay rates, routes, etc.)
- We can generate exactly the patterns our ML model needs to learn

Output files (saved to ingestion/data/):
- customers.csv   → 100 B2B companies
- drivers.csv     → 50 drivers with ratings and experience
- vehicles.csv    → 30 trucks/vans with mileage and age
- shipments.csv   → 10,000 shipment records linking everything
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os 

# SETUP

fake    = Faker()
np.random.seed(42)   # reproducible results
random.seed(42)

# Output folder
OUTPUT_DIR = "ingestion/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# MASTER DATA — Routes, Weather, etc.

# Real logistics routes between major cities
ROUTES = [
    ("Casablanca", "Morocco",  "Paris",     "France",  2400, "Europe-Africa", "Road"),
    ("London",     "UK",       "Berlin",    "Germany",  930, "Europe",        "Road"),
    ("Dubai",      "UAE",      "Mumbai",    "India",   1930, "Middle East",   "Air"),
    ("Paris",      "France",   "Madrid",    "Spain",   1270, "Europe",        "Road"),
    ("Lagos",      "Nigeria",  "Accra",     "Ghana",    530, "Africa",        "Road"),
    ("Cairo",      "Egypt",    "Riyadh",    "KSA",     1490, "Middle East",   "Road"),
    ("Amsterdam",  "NL",       "Warsaw",    "Poland",  1150, "Europe",        "Road"),
    ("Istanbul",   "Turkey",   "Athens",    "Greece",   680, "Europe",        "Road"),
    ("Nairobi",    "Kenya",    "Dar Salaam","Tanzania",  530, "Africa",        "Road"),
    ("Barcelona",  "Spain",    "Lyon",      "France",   640, "Europe",        "Road"),
]

WEATHER_CONDITIONS = ["Clear", "Cloudy", "Rain", "Heavy Rain", "Fog", "Snow"]
WEATHER_WEIGHTS    = [0.40,    0.25,     0.18,   0.07,         0.06,  0.04]

INDUSTRIES      = ["Retail", "Manufacturing", "Pharma",
                   "Electronics", "Food & Beverage", "Automotive"]
SEGMENTS        = ["Enterprise", "SME", "Startup"]
CONTRACT_TYPES  = ["Annual", "Monthly", "Spot"]
LICENSE_TYPES   = ["B", "C", "CE"]
VEHICLE_TYPES   = ["Van", "Truck", "Heavy Truck"]

# GENERATOR 1 — Customers

def generate_customers(n=100):
    """
    100 B2B companies that use LogiFlow for shipping.
    Segment affects how many shipments they generate.
    """
    print(f"  Generating {n} customers...")
    customers = []

    for i in range(1, n + 1):
        customers.append({
            "customer_id":   i,
            "company_name":  fake.company(),
            "industry":      random.choice(INDUSTRIES),
            "country":       random.choice([r[1] for r in ROUTES]),
            "city":          random.choice([r[0] for r in ROUTES]),
            "segment":       random.choice(SEGMENTS),
            "contract_type": random.choice(CONTRACT_TYPES),
        })

    df = pd.DataFrame(customers)
    df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"  customers.csv saved ({len(df)} rows)")
    return df



# GENERATOR 2 — Drivers

def generate_drivers(n=50):
    """
    50 drivers. Experience and rating are key ML features.

    Design decision:
    - Senior drivers (5+ years) get higher base ratings
    - This creates a realistic correlation between
      experience and on-time performance
    """
    print(f"  Generating {n} drivers...")
    drivers = []

    for i in range(1, n + 1):
        experience = random.randint(1, 20)

        # More experience = slightly better rating
        # but with noise — not perfect correlation
        base_rating = 3.0 + (experience / 20) * 1.5
        rating      = round(min(5.0, max(1.0,
                        base_rating + np.random.normal(0, 0.3))), 2)

        drivers.append({
            "driver_id":        i,
            "full_name":        fake.name(),
            "license_type":     random.choice(LICENSE_TYPES),
            "experience_years": experience,
            "rating":           rating,
            "country":          random.choice([r[1] for r in ROUTES]),
            "is_active":        random.random() > 0.05,  # 95% active
        })

    df = pd.DataFrame(drivers)
    df.to_csv(f"{OUTPUT_DIR}/drivers.csv", index=False)
    print(f" drivers.csv saved ({len(df)} rows)")
    return df

# GENERATOR 3 — Vehicles

def generate_vehicles(n=30):
    """
    30 vehicles. Age and mileage affect breakdown risk.

    Design decision:
    - Older vehicles with high mileage have higher delay rates
    - Last service date affects reliability
    - These become important ML features
    """
    print(f"  Generating {n} vehicles...")
    vehicles = []

    for i in range(1, n + 1):
        manufacture_year = random.randint(2010, 2023)
        age_years        = 2024 - manufacture_year

        # Older vehicles have more mileage
        base_mileage = age_years * random.randint(15000, 35000)
        mileage      = int(base_mileage * random.uniform(0.8, 1.2))

        # Last service: more recent for newer vehicles
        days_since_service = random.randint(10, 365)
        last_service = datetime.now() - timedelta(days=days_since_service)

        vehicle_type = random.choice(VEHICLE_TYPES)
        capacity     = {"Van": 1000, "Truck": 5000,
                        "Heavy Truck": 20000}[vehicle_type]

        vehicles.append({
            "vehicle_id":       i,
            "plate_number":     fake.license_plate(),
            "vehicle_type":     vehicle_type,
            "capacity_kg":      capacity + random.randint(-200, 200),
            "manufacture_year": manufacture_year,
            "mileage_km":       mileage,
            "last_service_date":last_service.strftime("%Y-%m-%d"),
            "is_active":        random.random() > 0.05,
        })

    df = pd.DataFrame(vehicles)
    df.to_csv(f"{OUTPUT_DIR}/vehicles.csv", index=False)
    print(f"  vehicles.csv saved ({len(df)} rows)")
    return df


# GENERATOR 4 — Shipments

def generate_shipments(customers, drivers, vehicles, n=10000):
    """
    10,000 shipments — the core of the dataset.

    Delay logic (realistic, not random):
    A shipment is more likely to be delayed if:
    - Bad weather (Rain, Snow, Fog)
    - Long distance route
    - Inexperienced or low-rated driver
    - Old vehicle with high mileage
    - Heavy weight shipment

    This realistic correlation is what makes the
    ML model actually work — it can learn these patterns.
    """
    print(f"  Generating {n} shipments...")
    shipments  = []
    start_date = datetime(2023, 1, 1)
    end_date   = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days

    active_drivers  = drivers[drivers["is_active"] == True]
    active_vehicles = vehicles[vehicles["is_active"] == True]

    for i in range(1, n + 1):
        # Pick random entities
        customer = customers.sample(1).iloc[0]
        driver   = active_drivers.sample(1).iloc[0]
        vehicle  = active_vehicles.sample(1).iloc[0]
        route    = random.choice(ROUTES)
        weather  = random.choices(WEATHER_CONDITIONS,
                                  weights=WEATHER_WEIGHTS)[0]

        # Scheduled pickup
        scheduled_pickup = start_date + timedelta(
            days=random.randint(0, date_range),
            hours=random.randint(6, 18),
            minutes=random.choice([0, 15, 30, 45])
        )

        # Planned duration based on distance
        distance_km      = route[4]
        planned_duration = round(distance_km / random.uniform(60, 90), 2)

        # Delay calculation
        # Each risk factor adds delay probability
        delay_risk = 0.10  # base 10% chance of delay

        # Weather risk
        weather_risk = {"Clear": 0, "Cloudy": 0.02, "Rain": 0.10,
                        "Heavy Rain": 0.20, "Fog": 0.15, "Snow": 0.25}
        delay_risk += weather_risk[weather]

        # Driver risk (low rating = higher delay risk)
        if driver["rating"] < 3.0:
            delay_risk += 0.15
        elif driver["rating"] < 4.0:
            delay_risk += 0.05

        # Vehicle risk (old + high mileage)
        vehicle_age = 2024 - vehicle["manufacture_year"]
        if vehicle_age > 10 and vehicle["mileage_km"] > 200000:
            delay_risk += 0.15
        elif vehicle_age > 5:
            delay_risk += 0.05

        # Distance risk (longer = more things can go wrong)
        if distance_km > 2000:
            delay_risk += 0.10
        elif distance_km > 1000:
            delay_risk += 0.05

        # Is this shipment delayed?
        is_delayed    = random.random() < delay_risk
        delay_minutes = 0

        if is_delayed:
            # Delay severity depends on weather
            if weather in ["Heavy Rain", "Snow"]:
                delay_minutes = random.randint(60, 480)
            elif weather in ["Rain", "Fog"]:
                delay_minutes = random.randint(30, 180)
            else:
                delay_minutes = random.randint(30, 120)

        # Calculate actual duration
        actual_duration = round(
            planned_duration + (delay_minutes / 60), 2)

        # Timestamps
        actual_pickup     = scheduled_pickup + timedelta(
                            minutes=random.randint(0, 20))
        scheduled_delivery = actual_pickup + timedelta(
                            hours=planned_duration)
        actual_delivery   = actual_pickup + timedelta(
                            hours=actual_duration)

        # Weight and cost
        weight_kg    = round(random.uniform(50, vehicle["capacity_kg"] * 0.8), 2)
        cost_per_km  = {"Van": 0.8, "Truck": 1.2,
                        "Heavy Truck": 1.8}[vehicle["vehicle_type"]]
        cost_usd     = round(distance_km * cost_per_km *
                       random.uniform(0.9, 1.1), 2)
        fuel_consumed = round(distance_km *
                        random.uniform(0.25, 0.40), 2)

        # Status
        if not is_delayed:
            status = "on_time"
        elif delay_minutes > 240:
            status = "failed"
        else:
            status = "delayed"

        # Temperature based on weather
        temp_ranges = {"Clear": (15, 35),  "Cloudy": (10, 25),
                       "Rain":  (8, 20),   "Heavy Rain": (5, 15),
                       "Fog":   (2, 12),   "Snow": (-10, 2)}
        temp_range   = temp_ranges[weather]
        temperature  = round(random.uniform(*temp_range), 1)
        wind_speed   = round(random.uniform(5, 80), 1)

        shipments.append({
            "shipment_id":          i,
            "customer_id":          int(customer["customer_id"]),
            "driver_id":            int(driver["driver_id"]),
            "vehicle_id":           int(vehicle["vehicle_id"]),
            "route_origin_city":    route[0],
            "route_origin_country": route[1],
            "route_dest_city":      route[2],
            "route_dest_country":   route[3],
            "distance_km":          distance_km,
            "region":               route[5],
            "route_type":           route[6],
            "scheduled_pickup":     scheduled_pickup.isoformat(),
            "actual_pickup":        actual_pickup.isoformat(),
            "scheduled_delivery":   scheduled_delivery.isoformat(),
            "actual_delivery":      actual_delivery.isoformat(),
            "planned_duration_hrs": planned_duration,
            "actual_duration_hrs":  actual_duration,
            "delay_minutes":        delay_minutes,
            "is_delayed":           is_delayed,
            "status":               status,
            "weight_kg":            weight_kg,
            "cost_usd":             cost_usd,
            "fuel_consumed_liters": fuel_consumed,
            "weather_condition":    weather,
            "temperature_celsius":  temperature,
            "wind_speed_kmh":       wind_speed,
        })

        # Progress indicator
        if i % 2000 == 0:
            print(f"    Generated {i}/{n} shipments...")

    df = pd.DataFrame(shipments)
    df.to_csv(f"{OUTPUT_DIR}/shipments.csv", index=False)

    # Print summary stats
    delay_rate = df["is_delayed"].mean() * 100
    print(f" shipments.csv saved ({len(df)} rows)")
    print(f"     Delay rate    : {delay_rate:.1f}%")
    print(f"     Avg delay     : {df[df['delay_minutes']>0]['delay_minutes'].mean():.0f} min")
    print(f"     Date range    : {df['scheduled_pickup'].min()[:10]}"
          f" → {df['scheduled_pickup'].max()[:10]}")
    return df

# MAIN

if __name__ == "__main__":

    print("   LOGIFLOW DATA GENERATOR")

    print("\n Generating master data...")
    customers = generate_customers(100)
    drivers   = generate_drivers(50)
    vehicles  = generate_vehicles(30)

    print("\n Generating shipments...")
    shipments = generate_shipments(customers, drivers, vehicles, 10000)

    
    print(" All files saved to ingestion/data/")
    print(f"   customers.csv : {len(customers)} rows")
    print(f"   drivers.csv   : {len(drivers)} rows")
    print(f"   vehicles.csv  : {len(vehicles)} rows")
    print(f"   shipments.csv : {len(shipments)} rows")
    