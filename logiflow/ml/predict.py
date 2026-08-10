"""
ml/predict.py

Loads the trained model bundle and predicts delay probability for one
shipment. Enforces the exact feature contract the model was trained on
-- no silent fallbacks for missing fields.

Depends on: ml/train.py (7) -- the feature engineering below must
mirror train.py's exactly, and REQUIRED_FIELDS must match its query.
Feeds into: services/api/main.py (9), which must require every one of
these fields from callers.
"""

import os
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd

_default = Path(__file__).resolve().parent / "models" / "delay_predictor.pkl"
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(_default)))

REQUIRED_FIELDS = [
    "weight_kg", "distance_km", "planned_duration_hrs", "cost_usd",
    "weather_condition", "temperature_celsius", "wind_speed_kmh",
    "traffic_congestion_ratio", "traffic_condition",
    "experience_years", "driver_rating", "license_type",
    "vehicle_type", "vehicle_capacity", "manufacture_year", "mileage_km",
    "region", "route_type",
    "month", "quarter", "is_weekend", "weekday",
    "segment", "industry",
]

CATEGORICAL_COLS = ["weather_condition", "traffic_condition", "license_type",
                     "vehicle_type", "region", "route_type", "weekday", "segment", "industry"]


def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Model not found at {MODEL_PATH}. Run ml/train.py first.")
    return joblib.load(MODEL_PATH)


def predict_single(shipment: dict) -> dict:
    missing = [f for f in REQUIRED_FIELDS if f not in shipment]
    if missing:
        raise ValueError(f"Missing required fields for prediction: {missing}")

    bundle   = load_model()
    model    = bundle["model"]
    encoders = bundle["encoders"]
    scaler   = bundle["scaler"]
    features = bundle["feature_names"]

    df = pd.DataFrame([shipment])

    # Same feature engineering as train.py -- must mirror it exactly
    current_year = datetime.now().year
    df["vehicle_age_years"] = current_year - df["manufacture_year"]
    df["mileage_per_year"]  = df["mileage_km"] / (df["vehicle_age_years"] + 1)
    df["load_ratio"]        = df["weight_kg"] / (df["vehicle_capacity"] + 1)
    df["cost_per_km"]       = df["cost_usd"] / (df["distance_km"] + 1)
    df["is_weekend"]        = df["is_weekend"].astype(int)
    df = df.drop(columns=["manufacture_year", "vehicle_capacity"])

    for col in CATEGORICAL_COLS:
        le = encoders[col]
        val = str(df[col].iloc[0])
        df[col] = [le.transform([val])[0]] if val in le.classes_ else [0]

    df = df[features]  # exact column order the scaler/model expect

    X = scaler.transform(df)
    prediction  = model.predict(X)[0]
    probability = model.predict_proba(X)[0][1]

    return {
        "will_be_delayed": bool(prediction),
        "delay_probability": round(float(probability) * 100, 1),
        "risk_level": "HIGH" if probability >= 0.7 else "MEDIUM" if probability >= 0.4 else "LOW",
        "model_used": bundle["model_name"],
        "predicted_at": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    test_shipment = {
        "weight_kg": 2000, "distance_km": 1200, "planned_duration_hrs": 14, "cost_usd": 2500,
        "weather_condition": "Snow", "temperature_celsius": -5.0, "wind_speed_kmh": 75.0,
        "traffic_congestion_ratio": 0.35, "traffic_condition": "HIGH",
        "experience_years": 1, "driver_rating": 2.5, "license_type": "C",
        "vehicle_type": "Truck", "vehicle_capacity": 20000, "manufacture_year": 2012, "mileage_km": 350000,
        "region": "Europe", "route_type": "Road",
        "month": 1, "quarter": 1, "is_weekend": 1, "weekday": "Saturday",
        "segment": "Startup", "industry": "Retail",
    }
    print(predict_single(test_shipment))