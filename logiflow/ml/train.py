"""
ml/train.py

Trains a shipment delay classifier on the warehouse, compares 4
candidate models on ROC-AUC, saves the best.

Depends on: common.config (1). Trains on data from pipelines/etl.py (5).
Feeds into: ml/predict.py (8), services/api/main.py (9).
"""

import logging
from datetime import datetime
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score, roc_curve
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sqlalchemy import create_engine
from xgboost import XGBClassifier

from common.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
MODELS_DIR = BASE_DIR / "models"
REPORTS_DIR = BASE_DIR / "reports"
MODELS_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

CATEGORICAL_COLS = ["weather_condition", "traffic_condition", "license_type",
                     "vehicle_type", "region", "route_type", "weekday", "segment", "industry"]


def load_data() -> pd.DataFrame:
    engine = create_engine(settings.database_url)
    query = """
        SELECT
            f.is_delayed,
            f.weight_kg, f.distance_km, f.planned_duration_hrs, f.cost_usd,
            f.weather_condition, f.temperature_celsius, f.wind_speed_kmh,
            f.traffic_congestion_ratio, f.traffic_condition,
            dr.experience_years, dr.rating AS driver_rating, dr.license_type,
            v.vehicle_type, v.capacity_kg AS vehicle_capacity, v.manufacture_year, v.mileage_km,
            r.region, r.route_type,
            d.month, d.quarter, d.is_weekend, d.weekday,
            c.segment, c.industry
        FROM fact_shipments f
        JOIN dim_driver   dr ON f.driver_id   = dr.driver_id
        JOIN dim_vehicle  v  ON f.vehicle_id  = v.vehicle_id
        JOIN dim_route    r  ON f.route_id    = r.route_id
        JOIN dim_date     d  ON f.date_id     = d.date_id
        JOIN dim_customer c  ON f.customer_id = c.customer_id
    """
    df = pd.read_sql(query, engine)
    log.info("Loaded %d rows for training", len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    current_year = datetime.now().year
    df["vehicle_age_years"] = current_year - df["manufacture_year"]
    df["mileage_per_year"]  = df["mileage_km"] / (df["vehicle_age_years"] + 1)
    df["load_ratio"]        = df["weight_kg"] / (df["vehicle_capacity"] + 1)
    df["cost_per_km"]       = df["cost_usd"] / (df["distance_km"] + 1)
    df["is_weekend"]        = df["is_weekend"].astype(int)
    return df.drop(columns=["manufacture_year", "vehicle_capacity"])


def encode_features(df: pd.DataFrame):
    encoders = {}
    for col in CATEGORICAL_COLS:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le
    return df, encoders


def train_models(X_train, X_test, y_train, y_test) -> dict:
    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
        "Random Forest":       RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
        "Gradient Boosting":   GradientBoostingClassifier(n_estimators=100, random_state=42),
        "XGBoost":             XGBClassifier(n_estimators=100, random_state=42, eval_metric="logloss", verbosity=0),
    }
    results = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred  = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]
        acc     = accuracy_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_proba)
        cv      = cross_val_score(model, X_train, y_train, cv=5, scoring="roc_auc").mean()
        results[name] = {"model": model, "accuracy": acc, "roc_auc": roc_auc, "cv_score": cv,
                          "y_pred": y_pred, "y_proba": y_proba}
        log.info("%s: Accuracy=%.3f | ROC-AUC=%.3f | CV=%.3f", name, acc, roc_auc, cv)
    return results


def plot_results(results: dict, feature_names: list, y_test) -> str:
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("LogiFlow -- Delay Prediction Model Evaluation", fontsize=14, fontweight="bold")

    names = list(results.keys())
    x = np.arange(len(names))
    ax = axes[0, 0]
    ax.bar(x - 0.2, [r["roc_auc"] for r in results.values()], 0.4, label="ROC-AUC", color="#7c3aed")
    ax.bar(x + 0.2, [r["accuracy"] for r in results.values()], 0.4, label="Accuracy", color="#3b82f6")
    ax.set_xticks(x); ax.set_xticklabels(names, rotation=15, ha="right", fontsize=8)
    ax.set_title("Model Comparison"); ax.legend()

    ax = axes[0, 1]
    for (name, r), color in zip(results.items(), ["#7c3aed", "#10b981", "#f59e0b", "#ef4444"]):
        fpr, tpr, _ = roc_curve(y_test, r["y_proba"])
        ax.plot(fpr, tpr, label=f"{name} (AUC={r['roc_auc']:.3f})", color=color)
    ax.plot([0, 1], [0, 1], "k--", alpha=0.4)
    ax.set_title("ROC Curves"); ax.legend(fontsize=7)

    best_name = max(results, key=lambda k: results[k]["roc_auc"])
    best = results[best_name]
    ax = axes[1, 0]
    cm = confusion_matrix(y_test, best["y_pred"])
    sns.heatmap(cm, annot=True, fmt="d", cmap="Purples",
                xticklabels=["On Time", "Delayed"], yticklabels=["On Time", "Delayed"], ax=ax)
    ax.set_title(f"Confusion Matrix -- {best_name}")

    ax = axes[1, 1]
    if hasattr(best["model"], "feature_importances_"):
        importances = best["model"].feature_importances_
        idx = np.argsort(importances)[-12:]
        ax.barh(range(len(idx)), importances[idx], color="#10b981")
        ax.set_yticks(range(len(idx)))
        ax.set_yticklabels([feature_names[i] for i in idx], fontsize=8)
        ax.set_title(f"Top Features -- {best_name}")

    plt.tight_layout()
    path = REPORTS_DIR / f"evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved evaluation plot -> %s", path)
    return best_name


def save_best_model(results, best_name, encoders, scaler, feature_names) -> Path:
    bundle = {
        "model": results[best_name]["model"],
        "encoders": encoders,
        "scaler": scaler,
        "feature_names": feature_names,
        "model_name": best_name,
        "trained_at": datetime.now().isoformat(),
        "metrics": {k: results[best_name][k] for k in ("accuracy", "roc_auc", "cv_score")},
    }
    path = MODELS_DIR / "delay_predictor.pkl"
    joblib.dump(bundle, path)
    log.info("Saved best model (%s) -> %s | ROC-AUC=%.3f", best_name, path, results[best_name]["roc_auc"])
    return path


def main():
    df = load_data()
    df = engineer_features(df)
    df, encoders = encode_features(df)

    X = df.drop(columns=["is_delayed"])
    y = df["is_delayed"].astype(int)
    feature_names = list(X.columns)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )
    log.info("Train: %d | Test: %d | Features: %d", len(X_train), len(X_test), len(feature_names))

    results = train_models(X_train, X_test, y_train, y_test)
    best_name = plot_results(results, feature_names, y_test)
    save_best_model(results, best_name, encoders, scaler, feature_names)
    log.info("Best model: %s", best_name)


if __name__ == "__main__":
    main()