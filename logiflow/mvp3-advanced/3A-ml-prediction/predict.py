import os
import joblib
import numpy as np
import pandas as pd
from datetime import datetime

from pathlib import Path

MODEL_PATH = Path("/opt/airflow/project/mvp3-advanced/3A-ml-prediction/models/delay_predictor.pkl")


def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError("Model not found. Run train.py first.")
    return joblib.load(MODEL_PATH.as_posix())


def predict_single(shipment: dict) -> dict:
    """
    Predict delay for one shipment.

    Example input:
    {
        "weight_kg": 1200,
        "distance_km": 850,
        "planned_duration_hrs": 10,
        "cost_usd": 1400,
        "weather_condition": "Rain",
        "temperature_celsius": 8.5,
        "wind_speed_kmh": 45.0,
        "experience_years": 3,
        "driver_rating": 3.8,
        "license_type": "CE",
        "vehicle_type": "Truck",
        "vehicle_age_years": 7,
        "mileage_km": 180000,
        "region": "Europe",
        "route_type": "Road",
        "route_distance": 850,
        "month": 12,
        "quarter": 4,
        "is_weekend": 0,
        "weekday": "Monday",
        "segment": "SME",
        "industry": "Retail"
    }
    """
    bundle   = load_model()
    model    = bundle["model"]
    encoders = bundle["encoders"]
    scaler   = bundle["scaler"]
    features = bundle["feature_names"]

    df = pd.DataFrame([shipment])

    # Engineer same features as training
    df["vehicle_age_years"] = df.get("vehicle_age_years", 5)
    df["mileage_per_year"]  = df["mileage_km"] / (df["vehicle_age_years"] + 1)
    df["load_ratio"]        = df["weight_kg"] / (df.get("vehicle_capacity", df["weight_kg"]) + 1)
    df["cost_per_km"]       = df["cost_usd"] / (df["distance_km"] + 1)
    df["is_weekend"]        = df["is_weekend"].astype(int)

    # Encode categoricals
    cat_cols = ["weather_condition","license_type","vehicle_type",
                "region","route_type","weekday","segment","industry"]
    for col in cat_cols:
        if col in encoders and col in df.columns:
            le = encoders[col]
            val = df[col].astype(str).iloc[0]
            if val in le.classes_:
                df[col] = le.transform([val])
            else:
                df[col] = 0   # unknown category

    # Align to training features
    for f in features:
        if f not in df.columns:
            df[f] = 0
    df = df[features]

    # Scale
    X = scaler.transform(df)

    # Predict
    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0][1]

    return {
        "will_be_delayed":  bool(prediction),
        "delay_probability": round(float(probability) * 100, 1),
        "risk_level": (
            "🔴 HIGH"   if probability >= 0.7 else
            "🟡 MEDIUM" if probability >= 0.4 else
            "🟢 LOW"
        ),
        "model_used": bundle["model_name"],
        "predicted_at": datetime.now().isoformat()
    }


if __name__ == "__main__":
    # Test with a high-risk shipment
    test_shipment = {
        "weight_kg":            2000,
        "distance_km":          1200,
        "planned_duration_hrs": 14,
        "cost_usd":             2500,
        "weather_condition":    "Snow",
        "temperature_celsius":  -5.0,
        "wind_speed_kmh":       75.0,
        "experience_years":     1,
        "driver_rating":        2.5,
        "license_type":         "C",
        "vehicle_type":         "Truck",
        "vehicle_age_years":    12,
        "mileage_km":           350000,
        "region":               "Europe",
        "route_type":           "Road",
        "route_distance":       1200,
        "month":                1,
        "quarter":              1,
        "is_weekend":           1,
        "weekday":              "Saturday",
        "segment":              "Startup",
        "industry":             "Retail"
    }

    result = predict_single(test_shipment)
    print("\n🚚 Delay Prediction Result:")
    print(f"   Will be delayed : {result['will_be_delayed']}")
    print(f"   Probability     : {result['delay_probability']}%")
    print(f"   Risk level      : {result['risk_level']}")
    print(f"   Model used      : {result['model_used']}")