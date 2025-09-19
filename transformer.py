import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from pathlib import Path
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from loader import save_processed   # <-- moved reporting/saving into loader.py

# -------------------------------
# Logging
# -------------------------------
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(PROCESSED_DIR / "pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------------------
# Load latest staged file
# -------------------------------
STAGING_DIR = Path("data/staging")
def load_staged_file(source: str) -> pd.DataFrame:
    files = sorted(STAGING_DIR.glob(f"{source}__*.parquet"))
    if not files:
        raise FileNotFoundError(f"No staged files for source: {source}")
    latest = files[-1]
    logger.info(f"Loading staged file: {latest}")
    return pd.read_parquet(latest)

# -------------------------------
# Data Cleaning
# -------------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning data...")
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].astype(str)
    df = df.drop_duplicates()
    for col in df.columns:
        if df[col].dtype in [np.float64, np.int64]:
            df[col] = df[col].fillna(df[col].mean())
        else:
            df[col] = df[col].fillna("missing")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5*IQR, Q3 + 1.5*IQR
        df[col] = np.where((df[col] < lower) | (df[col] > upper), df[col].median(), df[col])
    return df

# -------------------------------
# Feature Engineering
# -------------------------------
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Feature engineering...")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if df["timestamp"].notna().any():
            df["year"] = df["timestamp"].dt.year
            df["month"] = df["timestamp"].dt.month
            df["day"] = df["timestamp"].dt.day
            df["hour"] = df["timestamp"].dt.hour
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        scaler = StandardScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    return df

# -------------------------------
# Data Quality Report
# -------------------------------
def data_quality_report(df: pd.DataFrame):
    logger.info("Generating data quality report...")
    report = {
        "rows": len(df),
        "columns": len(df.columns),
        "missing_values": df.isna().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "stats": df.describe(include="all").to_dict()
    }
    return report

# -------------------------------
# ML Analysis
# -------------------------------
def ml_analysis(df: pd.DataFrame):
    results = {}
    numeric_df = df.select_dtypes(include=[np.number])

    if not numeric_df.empty:
        # Anomaly Detection
        iso = IsolationForest(contamination=0.05, random_state=42)
        df["anomaly"] = iso.fit_predict(numeric_df)
        results["anomaly_counts"] = df["anomaly"].value_counts().to_dict()

        # Clustering
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df["cluster"] = kmeans.fit_predict(numeric_df)
        results["cluster_counts"] = df["cluster"].value_counts().to_dict()
        results["cluster_centers"] = kmeans.cluster_centers_.tolist()

        # Key Statistical Metrics
        results["summary_statistics"] = numeric_df.agg(["mean", "std", "min", "max"]).to_dict()
        results["correlations"] = numeric_df.corr().to_dict()

        # Time Series Trends
        if "timestamp" in df.columns and df["timestamp"].notna().any():
            ts_summary = df.set_index("timestamp").resample("D").mean(numeric_only=True)
            if not ts_summary.empty:
                results["time_series_trend"] = ts_summary.tail(10).to_dict()
                results["time_series_plots"] = []  # filled in loader when saving plots

    return df, results

# -------------------------------
# Main
# -------------------------------
def main(source: str):
    df = load_staged_file(source)
    df = clean_data(df)
    df = feature_engineering(df)
    report = data_quality_report(df)
    df, ml_results = ml_analysis(df)
    final_results = {"data_quality": report, "ml_analysis": ml_results}
    save_processed(df, source, final_results)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Name of source to transform")
    args = parser.parse_args()
    main(args.source)
