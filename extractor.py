import argparse
import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_DIR = "data/staging"
os.makedirs(STAGING_DIR, exist_ok=True)


def load_csv(cfg):
    path = cfg["path"]
    logger.info(f"Loading CSV: {path}")
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")
        df = pd.read_csv(path, on_bad_lines="skip")  # skip corrupted lines
        if df.empty:
            raise ValueError(f"CSV file is empty: {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV {path}: {e}")
        raise


def load_excel(cfg):
    path = cfg["path"]
    sheet = cfg.get("sheet_name", 0)
    logger.info(f"Loading Excel: {path} [sheet={sheet}]")
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Excel file not found: {path}")
        df = pd.read_excel(path, sheet_name=sheet)
        if df.empty:
            raise ValueError(f"Excel file is empty: {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load Excel {path}: {e}")
        raise


def load_sqlite(cfg):
    path = cfg["path"]
    query = cfg["query"]
    logger.info(f"Loading SQLite DB: {path}, query: {query}")
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQLite DB not found: {path}")
        conn = sqlite3.connect(path)
        try:
            df = pd.read_sql_query(query, conn)
            if df.empty:
                raise ValueError(f"No data returned from SQLite query: {query}")
            return df
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to load SQLite {path}: {e}")
        raise


def load_api(cfg):
    url = cfg["url"]
    logger.info(f"Loading API: {url}")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise ValueError("API returned empty response")

        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            raise ValueError("Unsupported API response format")

        if df.empty:
            raise ValueError("API returned no usable data")

        return df

    except requests.exceptions.Timeout:
        logger.error("API request timed out")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to process API response: {e}")
        raise


def load_scraper(cfg):
    """
    Scrape structured list data from a webpage.
    Produces columns: rank, title, year, rating, link
    """
    url = cfg.get("url")
    limit = int(cfg.get("limit", 0))  # 0 means no limit
    logger.info(f"Scraping URL: {url} (limit={limit})")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/116.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for list-style items
        items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
        if not items:
            raise ValueError("Unexpected page structure: no list items found")

        records = []
        for i, item in enumerate(items, start=1):
            rank = i

            a = item.find("a", class_="ipc-title-link-wrapper")
            title = a.text.strip() if a else None
            link = None
            if a and a.get("href"):
                link = "https://www.imdb.com" + a["href"].split("?")[0]

            year_span = item.find("span", class_="cli-title-metadata-item")
            year = None
            if year_span:
                try:
                    year = int(year_span.text.strip())
                except Exception:
                    year = None

            rating_span = item.find("span", class_="ipc-rating-star")
            rating = None
            if rating_span:
                try:
                    rating = float(rating_span.text.strip().split()[0])
                except Exception:
                    rating = None

            records.append({
                "rank": rank,
                "title": title,
                "year": year,
                "rating": rating,
                "link": link
            })

            if limit and len(records) >= limit:
                break

        if not records:
            raise ValueError("No records scraped from page")

        df = pd.DataFrame(records)
        return df

    except requests.exceptions.Timeout:
        logger.error("Scrape request timed out")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Scrape request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        raise


def validate_dataframe(df: pd.DataFrame, source_name: str):
    if df is None or df.empty:
        raise ValueError(f"No data extracted from {source_name}")

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Extracted object from {source_name} is not a DataFrame")

    df = df.dropna(axis=1, how="all")

    if df.empty:
        raise ValueError(f"All columns are empty after cleaning in {source_name}")

    logger.info(f"Validation passed for {source_name} with shape {df.shape}")
    return df


def save_to_staging(df: pd.DataFrame, source_name: str):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{source_name}__{timestamp}.parquet"
    path = os.path.join(STAGING_DIR, filename)
    df.to_parquet(path, index=False)
    logger.info(f"Saved to staging: {path}")
    return path


def extract_source(source_name, source_cfg):
    source_type = source_cfg["type"]

    if source_type == "csv":
        df = load_csv(source_cfg)
    elif source_type == "excel":
        df = load_excel(source_cfg)
    elif source_type == "sqlite":
        df = load_sqlite(source_cfg)
    elif source_type == "api":
        df = load_api(source_cfg)
    elif source_type == "scraper":
        df = load_scraper(source_cfg)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    df = validate_dataframe(df, source_name)
    return save_to_staging(df, source_name)


def main(config_path, source):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    sources = config.get("sources", {})
    if source not in sources:
        raise ValueError(f"Source {source} not found in config")

    source_cfg = sources[source]

    try:
        extract_source(source, source_cfg)
    except Exception as e:
        logger.error(f"Failed to extract {source}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--source", required=True, help="Source name defined in config.yaml")
    args = parser.parse_args()

    main(args.config, args.source)
