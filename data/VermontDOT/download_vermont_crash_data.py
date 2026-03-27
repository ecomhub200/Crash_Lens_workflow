#!/usr/bin/env python3
"""
Vermont Crash Data — Custom REST API Downloader

Downloads crash data from Vermont Agency of Transportation via custom REST JSON API.

Data Source: https://apps.vtrans.vermont.gov/crashdata
API: https://apps.vtrans.vermont.gov/crashdata/api/Accident

Usage:
    python download_vermont_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_vermont_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

API_BASE_URL = "https://apps.vtrans.vermont.gov/crashdata/api/Accident"
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("vermont_downloader")


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise


def health_check():
    log.info("=" * 60)
    log.info("Vermont REST API — Health Check")
    log.info("=" * 60)
    try:
        resp = retry_request(API_BASE_URL, params={"$top": 1})
        data = resp.json()
        log.info(f"  Response type: {type(data).__name__}")
        if isinstance(data, list) and data:
            log.info(f"  Sample keys: {list(data[0].keys())[:10]}")
        log.info("  ✓ API is healthy")
        return True
    except Exception as e:
        log.error(f"  FAILED: {e}")
        return False


def download_data(jurisdiction, years):
    params = {}
    if jurisdiction:
        params["county"] = jurisdiction
    if years:
        params["year"] = ",".join(str(y) for y in years)
    log.info(f"  Downloading with params: {params}")
    resp = retry_request(API_BASE_URL, params=params)
    data = resp.json()
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and "value" in data:
        return data["value"]
    return []


def save_csv(records, output_path, gzip_output=False):
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
        log.info(f"  Saved: {gz_path}")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {output_path}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download Vermont crash data from REST API")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/VermontDOT")
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
    log.info(f"Vermont Crash Data Downloader")
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
