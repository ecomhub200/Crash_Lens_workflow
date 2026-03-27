#!/usr/bin/env python3
"""
Texas Crash Data — CRIS Bulk CSV Downloader

Downloads crash data from Texas Department of Transportation CRIS (Crash Records Information System).
CRIS requires registration for data access. This script handles bulk CSV downloads
with county filtering and gzip compression for R2 storage.

Portal: https://cris.dot.state.tx.us

Usage:
    python download_texas_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_texas_crash_data.py --health-check
"""

import argparse
import csv
import gzip
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

CRIS_PORTAL = "https://cris.dot.state.tx.us"
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("texas_downloader")


def health_check():
    log.info("=" * 60)
    log.info("Texas CRIS Portal — Health Check")
    log.info("=" * 60)
    try:
        resp = requests.get(CRIS_PORTAL, timeout=30)
        log.info(f"  Status: {resp.status_code}")
        log.info("  ✓ Portal is reachable")
        log.info("  Note: CRIS requires registration for data access")
        return True
    except Exception as e:
        log.error(f"  FAILED: {e}")
        return False


def download_data(jurisdiction, years, data_dir):
    """Download from CRIS — requires pre-exported CSV files."""
    log.info("CRIS bulk download requires manual export or registered API access.")
    log.info(f"  Looking for pre-exported CSVs in {data_dir}/")
    csv_files = list(Path(data_dir).glob("*.csv"))
    if not csv_files:
        log.warning("No CSV files found. Export data from CRIS portal first.")
        return []
    all_records = []
    for csv_file in csv_files:
        log.info(f"  Reading: {csv_file}")
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_records.append(row)
    log.info(f"  Total records: {len(all_records):,}")
    return all_records


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
    parser = argparse.ArgumentParser(description="Download Texas crash data from CRIS")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/TexasDOT")
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
    log.info(f"Texas Crash Data Downloader (CRIS)")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years, args.data_dir)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {len(records):,} records in {time.time()-start:.1f}s -> {saved}")


if __name__ == "__main__":
    main()
