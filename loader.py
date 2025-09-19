import logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Paths & Logging

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)


# Reporting (Dashboard Style)

def generate_html_report(df: pd.DataFrame, results: dict, source: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_file = PROCESSED_DIR / f"{source}__report__{timestamp}.html"
    logger.info(f"Generating HTML dashboard report: {report_file}")
    
    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #2c3e50; }
            h2 { color: #34495e; margin-top: 30px; }
            table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f4f4f4; }
            .grid { display: flex; flex-wrap: wrap; gap: 20px; }
            .grid img { border: 1px solid #ccc; padding: 5px; background: #fafafa; }
        </style>
    </head>
    <body>
    """
    html += f"<h1>Pipeline Dashboard: {source}</h1>"

    # Key Insights
    html += "<h2>Summary Insights</h2>"
    html += f"<p><b>Total Rows:</b> {results['data_quality']['rows']}<br>"
    html += f"<b>Total Columns:</b> {results['data_quality']['columns']}<br></p>"

    # Missing values table
    html += "<h2>Missing Values</h2><table><tr><th>Column</th><th>Missing</th></tr>"
    for col, miss in results["data_quality"]["missing_values"].items():
        html += f"<tr><td>{col}</td><td>{miss}</td></tr>"
    html += "</table>"

    # ML Analysis summary
    html += "<h2>ML Analysis</h2>"
    if "anomaly_counts" in results["ml_analysis"]:
        html += "<h3>Anomaly Detection</h3><table><tr><th>Type</th><th>Count</th></tr>"
        for k, v in results["ml_analysis"]["anomaly_counts"].items():
            html += f"<tr><td>{k}</td><td>{v}</td></tr>"
        html += "</table>"

    if "cluster_counts" in results["ml_analysis"]:
        html += "<h3>Cluster Distribution</h3><table><tr><th>Cluster</th><th>Count</th></tr>"
        for k, v in results["ml_analysis"]["cluster_counts"].items():
            html += f"<tr><td>{k}</td><td>{v}</td></tr>"
        html += "</table>"

        # Pie chart for clusters
        plt.figure(figsize=(5,5))
        plt.pie(results["ml_analysis"]["cluster_counts"].values(), 
                labels=results["ml_analysis"]["cluster_counts"].keys(), autopct='%1.1f%%')
        plt.title("Cluster Distribution")
        cluster_pie_file = PROCESSED_DIR / f"{source}__cluster_pie_{timestamp}.png"
        plt.savefig(cluster_pie_file)
        plt.close()
        html += f"<img src='{cluster_pie_file.name}' width='400'>"

    
    # Key Statistical Metrics
    
    if "summary_statistics" in results["ml_analysis"]:
        html += "<h3>Summary Statistics</h3><table><tr><th>Column</th><th>Mean</th><th>Std</th><th>Min</th><th>Max</th></tr>"
        for col, metrics in results["ml_analysis"]["summary_statistics"].items():
            mean = metrics.get("mean", "NA")
            std = metrics.get("std", "NA")
            min_v = metrics.get("min", "NA")
            max_v = metrics.get("max", "NA")
            html += f"<tr><td>{col}</td><td>{mean}</td><td>{std}</td><td>{min_v}</td><td>{max_v}</td></tr>"
        html += "</table>"

    # Distribution plots grid
    html += "<h2>Data Distributions</h2><div class='grid'>"
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        # Histogram
        plt.figure(figsize=(6,4))
        sns.histplot(df[col], kde=True)
        plt.title(f"{col} Distribution")
        plot_file = PROCESSED_DIR / f"{source}__{col}_dist_{timestamp}.png"
        plt.savefig(plot_file)
        plt.close()
        html += f"<div><h4>{col} Distribution</h4><img src='{plot_file.name}' width='350'></div>"

        # Boxplot
        plt.figure(figsize=(6,4))
        sns.boxplot(x=df[col])
        plt.title(f"{col} Boxplot")
        box_file = PROCESSED_DIR / f"{source}__{col}_box_{timestamp}.png"
        plt.savefig(box_file)
        plt.close()
        html += f"<div><h4>{col} Boxplot</h4><img src='{box_file.name}' width='350'></div>"
    html += "</div>"

    # Correlation heatmap
    if len(numeric_cols) > 1:
        plt.figure(figsize=(8,6))
        sns.heatmap(df[numeric_cols].corr(), annot=True, cmap="coolwarm")
        corr_file = PROCESSED_DIR / f"{source}__correlation_{timestamp}.png"
        plt.savefig(corr_file)
        plt.close()
        html += f"<h2>Correlation Heatmap</h2><img src='{corr_file.name}' width='700'>"

    
    # Time Series Trends
    
    if "time_series_trend" in results["ml_analysis"]:
        html += "<h2>Time Series Trends (last 10 days)</h2>"
        ts = results["ml_analysis"]["time_series_trend"]
        df_trend = pd.DataFrame(ts)
        html += df_trend.to_html(border=1, classes="dataframe", justify="left")

    html += "</body></html>"

    with open(report_file, "w", encoding="utf-8") as f:
        f.write(html)


# Save Processed Data

def save_processed(df: pd.DataFrame, source: str, results: dict):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df.to_parquet(PROCESSED_DIR / f"{source}__processed__{timestamp}.parquet", index=False)
    df.to_csv(PROCESSED_DIR / f"{source}__processed__{timestamp}.csv", index=False)
    pd.Series(results).to_json(PROCESSED_DIR / f"{source}__report__{timestamp}.json")
    generate_html_report(df, results, source)
