import os
import sys
import joblib
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, confusion_matrix,
    roc_auc_score, roc_curve, accuracy_score
)
from xgboost import XGBClassifier

load_dotenv("/mnt/c/Users/HP PRO/Documents/global logistic project/logiflow/mvp3-advanced/3A-ml-prediction/.env")
os.makedirs("logiflow/mvp3-advanced/3A-ml-prediction/models",  exist_ok=True)
os.makedirs("reports", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# 1. LOAD DATA
# ══════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    query = """
        SELECT
            -- TARGET
            f.is_delayed,

            -- SHIPMENT FEATURES
            f.weight_kg,
            f.distance_km,
            f.planned_duration_hrs,
            f.cost_usd,

            -- WEATHER FEATURES (most impactful)
            f.weather_condition,
            f.temperature_celsius,
            f.wind_speed_kmh,

            -- DRIVER FEATURES
            dr.experience_years,
            dr.rating          AS driver_rating,
            dr.license_type,

            -- VEHICLE FEATURES
            v.vehicle_type,
            v.capacity_kg      AS vehicle_capacity,
            v.manufacture_year,
            v.mileage_km,

            -- ROUTE FEATURES
            r.region,
            r.route_type,
            r.distance_km      AS route_distance,

            -- TIME FEATURES
            d.month,
            d.quarter,
            d.is_weekend,
            d.weekday,

            -- CUSTOMER FEATURES
            c.segment,
            c.industry

        FROM fact_shipments f
        JOIN dim_driver   dr ON f.driver_id   = dr.driver_id
        JOIN dim_vehicle  v  ON f.vehicle_id  = v.vehicle_id
        JOIN dim_route    r  ON f.route_id    = r.route_id
        JOIN dim_date     d  ON f.date_id     = d.date_id
        JOIN dim_customer c  ON f.customer_id = c.customer_id
    """
    df = pd.read_sql(query, engine)
    log.info(f" Loaded {len(df):,} rows for training")
    return df


# ══════════════════════════════════════════════════════════════
# 2. FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("🔧 Engineering features...")

    # Vehicle age (more meaningful than manufacture year)
    current_year = datetime.now().year
    df["vehicle_age_years"] = current_year - df["manufacture_year"]

    # Mileage per year (wear indicator)
    df["mileage_per_year"] = df["mileage_km"] / (df["vehicle_age_years"] + 1)

    # Weight load ratio (how full is the truck?)
    df["load_ratio"] = df["weight_kg"] / (df["vehicle_capacity"] + 1)

    # Cost per km (efficiency indicator)
    df["cost_per_km"] = df["cost_usd"] / (df["distance_km"] + 1)

    # Weekend flag as int
    df["is_weekend"] = df["is_weekend"].astype(int)

    # Drop raw columns replaced by engineered ones
    df.drop(columns=["manufacture_year", "vehicle_capacity"], inplace=True)

    return df


def encode_features(df: pd.DataFrame):
    log.info(" Encoding categorical features...")

    categorical_cols = [
        "weather_condition", "license_type", "vehicle_type",
        "region", "route_type", "weekday", "segment", "industry"
    ]

    encoders = {}
    for col in categorical_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le

    return df, encoders


# ══════════════════════════════════════════════════════════════
# 3. TRAIN MODELS
# ══════════════════════════════════════════════════════════════

def train_models(X_train, X_test, y_train, y_test):
    log.info("🤖 Training models...")

    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
        "Random Forest":       RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
        "Gradient Boosting":   GradientBoostingClassifier(n_estimators=100, random_state=42),
        "XGBoost":             XGBClassifier(n_estimators=100, random_state=42,
                                             eval_metric="logloss", verbosity=0),
    }

    results = {}

    for name, model in models.items():
        log.info(f"   Training {name}...")
        model.fit(X_train, y_train)
        y_pred  = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        acc     = accuracy_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_proba)
        cv      = cross_val_score(model, X_train, y_train, cv=5, scoring="roc_auc").mean()

        results[name] = {
            "model":   model,
            "accuracy": acc,
            "roc_auc":  roc_auc,
            "cv_score": cv,
            "y_pred":   y_pred,
            "y_proba":  y_proba,
        }
        log.info(f"    {name}: Accuracy={acc:.3f} | ROC-AUC={roc_auc:.3f} | CV={cv:.3f}")

    return results


# ══════════════════════════════════════════════════════════════
# 4. EVALUATE & VISUALIZE
# ══════════════════════════════════════════════════════════════

def plot_results(results, X_train, feature_names, y_test):
    log.info(" Generating evaluation plots...")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("LogiFlow — Delay Prediction Model Evaluation", fontsize=16, fontweight="bold")

    # ── Plot 1: Model comparison bar chart
    ax = axes[0, 0]
    names    = list(results.keys())
    aucs     = [r["roc_auc"] for r in results.values()]
    accs     = [r["accuracy"] for r in results.values()]
    x        = np.arange(len(names))
    bars1    = ax.bar(x - 0.2, aucs, 0.4, label="ROC-AUC", color="#7c3aed", alpha=0.85)
    bars2    = ax.bar(x + 0.2, accs, 0.4, label="Accuracy", color="#3b82f6", alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=15, ha="right", fontsize=9)
    ax.set_ylim(0, 1.1)
    ax.set_title("Model Comparison")
    ax.legend()
    ax.set_ylabel("Score")
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                f"{bar.get_height():.3f}", ha="center", fontsize=8)

    # ── Plot 2: ROC curves
    ax = axes[0, 1]
    colors = ["#7c3aed", "#10b981", "#f59e0b", "#ef4444"]
    for (name, r), color in zip(results.items(), colors):
        fpr, tpr, _ = roc_curve(y_test, r["y_proba"])
        ax.plot(fpr, tpr, label=f"{name} (AUC={r['roc_auc']:.3f})", color=color, lw=2)
    ax.plot([0,1],[0,1], "k--", alpha=0.4)
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title("ROC Curves")
    ax.legend(fontsize=8)

    # ── Plot 3: Confusion matrix for best model
    best_name = max(results, key=lambda k: results[k]["roc_auc"])
    best      = results[best_name]
    ax        = axes[1, 0]
    cm        = confusion_matrix(y_test, best["y_pred"])
    sns.heatmap(cm, annot=True, fmt="d", cmap="Purples",
                xticklabels=["On Time","Delayed"],
                yticklabels=["On Time","Delayed"], ax=ax)
    ax.set_title(f"Confusion Matrix — {best_name}")
    ax.set_ylabel("Actual")
    ax.set_xlabel("Predicted")

    # ── Plot 4: Feature importance for best model
    ax = axes[1, 1]
    if hasattr(best["model"], "feature_importances_"):
        importances = best["model"].feature_importances_
        indices     = np.argsort(importances)[-15:]   # top 15
        ax.barh(range(len(indices)),
                importances[indices], color="#10b981", alpha=0.85)
        ax.set_yticks(range(len(indices)))
        ax.set_yticklabels([feature_names[i] for i in indices], fontsize=8)
        ax.set_title(f"Top 15 Features — {best_name}")
        ax.set_xlabel("Importance")

    plt.tight_layout()
    path = f"reports/evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    log.info(f"📊 Saved evaluation plot → {path}")
    plt.close()

    return best_name


# ══════════════════════════════════════════════════════════════
# 5. SAVE BEST MODEL
# ══════════════════════════════════════════════════════════════

def save_best_model(results, best_name, encoders, scaler, feature_names):
    best_model = results[best_name]["model"]

    bundle = {
        "model":         best_model,
        "encoders":      encoders,
        "scaler":        scaler,
        "feature_names": feature_names,
        "model_name":    best_name,
        "trained_at":    datetime.now().isoformat(),
        "metrics": {
            "accuracy": results[best_name]["accuracy"],
            "roc_auc":  results[best_name]["roc_auc"],
            "cv_score": results[best_name]["cv_score"],
        }
    }

    path = "models/delay_predictor.pkl"
    joblib.dump(bundle, path)
    log.info(f"💾 Saved best model ({best_name}) → {path}")
    log.info(f"   Accuracy : {results[best_name]['accuracy']:.3f}")
    log.info(f"   ROC-AUC  : {results[best_name]['roc_auc']:.3f}")
    log.info(f"   CV Score : {results[best_name]['cv_score']:.3f}")

    return path


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("🚚 LogiFlow — Delay Prediction Model Training")
    log.info("=" * 60)

    # 1. Load
    df = load_data()

    # 2. Engineer
    df = engineer_features(df)

    # 3. Encode
    df, encoders = encode_features(df)

    # 4. Split X / y
    X = df.drop(columns=["is_delayed"])
    y = df["is_delayed"].astype(int)
    feature_names = list(X.columns)

    log.info(f"   Features : {len(feature_names)}")
    log.info(f"   Target   : {y.value_counts().to_dict()}")

    # 5. Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 6. Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=y
    )
    log.info(f"   Train: {len(X_train):,} | Test: {len(X_test):,}")

    # 7. Train
    results = train_models(X_train, X_test, y_train, y_test)

    # 8. Evaluate
    best_name = plot_results(results, X_train, feature_names, y_test)
    log.info(f"🏆 Best model: {best_name}")

    # 9. Save
    save_best_model(results, best_name, encoders, scaler, feature_names)

    log.info("=" * 60)
    log.info("✅ Training complete — run predict.py to test predictions")
    log.info("=" * 60)


if __name__ == "__main__":
    main()