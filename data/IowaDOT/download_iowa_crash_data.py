#!/usr/bin/env python3
"""
Iowa Crash Data — ArcGIS Feature Server Downloader

Downloads crash data from Iowa Department of Transportation via ArcGIS GeoServices REST API.
Supports jurisdiction filtering, date range queries, gzip compression for R2 storage.

Data Source: https://data.iowadot.gov
Feature Server: https://gis.iowadot.gov/agshost/rest/services/Traffic_Safety/Crash_Data/FeatureServer/0

Usage:
    python download_iowa_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_iowa_crash_data.py --gzip
    python download_iowa_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# =============================================================================
# Constants
# =============================================================================

FEATURE_SERVER_URL = "https://gis.iowadot.gov/agshost/rest/services/Traffic_Safety/Crash_Data/FeatureServer/0"
QUERY_URL = FEATURE_SERVER_URL.rstrip("/") + "/query"

PAGE_SIZE = 2000
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("iowa_downloader")


# =============================================================================
# Helper Functions
# =============================================================================

def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP GET with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(wait)
            else:
                raise


def health_check():
    """Test Feature Server connectivity."""
    log.info("=" * 60)
    log.info("Iowa ArcGIS Feature Server — Health Check")
    log.info("=" * 60)
    try:
        resp = retry_request(FEATURE_SERVER_URL, params={"f": "json"})
        meta = resp.json()
        log.info(f"  Name: {meta.get('name', 'N/A')}")
        log.info(f"  Type: {meta.get('type', 'N/A')}")
        fields = meta.get("fields", [])
        log.info(f"  Fields: {len(fields)}")
        if fields:
            log.info(f"  Sample fields: {[f['name'] for f in fields[:10]]}")
        log.info("  ✓ Feature Server is healthy")
        return True
    except Exception as e:
        log.error(f"  FAILED: {e}")
        return False


def download_data(jurisdiction, years):
    """Download crash data with offset pagination."""
    where_parts = []
    if jurisdiction:
        where_parts.append(f"COUNTY='{jurisdiction}'")
    where = " AND ".join(where_parts) if where_parts else "1=1"

    all_features = []
    offset = 0
    page = 1
    while True:
        params = {
            "where": where,
            "outFields": "*",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "json",
        }
        log.info(f"  Page {page}: offset={offset}")
        resp = retry_request(QUERY_URL, params=params)
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        # Flatten attributes
        records = [f.get("attributes", {}) for f in features]
        # Add geometry if present
        for i, f in enumerate(features):
            geom = f.get("geometry", {})
            if geom:
                records[i]["_longitude"] = geom.get("x")
                records[i]["_latitude"] = geom.get("y")
        all_features.extend(records)
        log.info(f"  Got {len(features)} (total: {len(all_features)})")
        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        page += 1
        time.sleep(0.5)
    return all_features


def save_csv(records, output_path, gzip_output=False):
    """Save records as CSV."""
    if not records:
        return None
    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {gz_path} ({os.path.getsize(gz_path):,} bytes)")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {output_path} ({os.path.getsize(output_path):,} bytes)")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download Iowa crash data from ArcGIS Feature Server")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/IowaDOT")
    parser.add_argument("--gzip", "-g", action="store_true")
    parser.add_argument("--health-check", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.health_check:
        sys.exit(0 if health_check() else 1)

    jurisdiction = args.jurisdiction or "statewide"
    output_path = Path(args.data_dir) / f"{jurisdiction}_crashes.csv"
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        log.info("Output exists. Use --force to re-download.")
        sys.exit(0)

    log.info("=" * 60)
    log.info(f"Iowa Crash Data Downloader")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {len(records):,} records in {time.time()-start:.1f}s -> {saved}")


if __name__ == "__main__":
    main()
