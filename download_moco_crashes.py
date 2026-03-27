#!/usr/bin/env python3
"""
Montgomery County, Maryland Crash Data — Socrata SODA API Downloader

Downloads crash, driver, and non-motorist data from Montgomery County's Open Data
Portal via Socrata SODA API.  Supports year filtering, per-dataset selection, gzip
compression for R2 storage, and health check mode for API connectivity testing.

Data Sources (data.montgomerycountymd.gov):
  Crash Incidents  : https://data.montgomerycountymd.gov/d/bhju-22kf
  Drivers          : https://data.montgomerycountymd.gov/d/mmzv-x632
  Non-Motorists    : https://data.montgomerycountymd.gov/d/n7fk-dce5

Usage:
    python download_moco_crashes.py
    python download_moco_crashes.py --year 2023
    python download_moco_crashes.py --dataset crashes --year 2024 --gzip
    python download_moco_crashes.py --dataset drivers --year 2022 --force
    python download_moco_crashes.py --health-check
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

DATASETS = {
    "crashes": {
        "id": "bhju-22kf",
        "name": "Crash Incidents",
        "date_field": "crash_date_time",
        "file_prefix": "moco_crashes",
    },
    "drivers": {
        "id": "mmzv-x632",
        "name": "Drivers",
        "date_field": "crash_date_time",
        "file_prefix": "moco_drivers",
    },
    "nonmotorists": {
        "id": "n7fk-dce5",
        "name": "Non-Motorists",
        "date_field": "crash_date_time",
        "file_prefix": "moco_nonmotorists",
    },
}

BASE_URL = "https://data.montgomerycountymd.gov/resource"
PAGE_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 4, 8]
DEFAULT_YEARS = list(range(2015, 2025))
DEFAULT_DATA_DIR = "data/MarylandDOT/montgomery"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("moco_downloader")


# =============================================================================
# Helper Functions
# =============================================================================

def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP GET request with exponential backoff retry logic."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                log.info(f"Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"Request failed after {max_retries} attempts: {e}")
                raise


def health_check():
    """Test API connectivity for all three dataset endpoints and report status."""
    log.info("=" * 60)
    log.info("Montgomery County Socrata API — Health Check")
    log.info("=" * 60)

    all_ok = True

    for key, ds in DATASETS.items():
        endpoint = f"{BASE_URL}/{ds['id']}.json"
        log.info(f"  Testing '{ds['name']}' ({key}) — {endpoint}")

        try:
            resp = retry_request(endpoint, params={"$limit": 1})
            data = resp.json()
            if data and len(data) > 0:
                sample = data[0]
                sample_keys = list(sample.keys())[:10]
                log.info(f"    Fields (sample): {sample_keys}...")
                date_val = sample.get(ds["date_field"], "N/A")
                log.info(f"    Sample {ds['date_field']}: {date_val}")
                log.info(f"    OK — endpoint returned data")
            else:
                log.warning(f"    WARN — endpoint returned empty response")
        except Exception as e:
            log.error(f"    FAILED — {e}")
            all_ok = False

    log.info("-" * 60)
    if all_ok:
        log.info("  All endpoints are healthy and responding")
    else:
        log.error("  One or more endpoints failed — check logs above")
    log.info("=" * 60)
    return all_ok


def build_where_clause(date_field, year):
    """Build SoQL $where clause for year filtering.

    Returns a clause like:
        crash_date_time >= '2023-01-01T00:00:00.000' AND crash_date_time < '2024-01-01T00:00:00.000'
    """
    start = f"{year}-01-01T00:00:00.000"
    end = f"{year + 1}-01-01T00:00:00.000"
    return f"{date_field} >= '{start}' AND {date_field} < '{end}'"


def download_data(dataset_key, year):
    """Download one dataset for one year from Socrata API with pagination.

    Returns a list of dict records (JSON rows).
    """
    ds = DATASETS[dataset_key]
    endpoint = f"{BASE_URL}/{ds['id']}.json"
    date_field = ds["date_field"]
    where_clause = build_where_clause(date_field, year)

    log.info(f"Downloading {ds['name']} for {year}...")
    log.info(f"  Endpoint: {endpoint}")
    log.info(f"  SoQL filter: $where={where_clause}")

    all_records = []
    offset = 0
    page = 1

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": f"{date_field} ASC",
            "$where": where_clause,
        }

        log.info(f"  Page {page}: offset={offset}, limit={PAGE_SIZE}")
        resp = retry_request(endpoint, params=params)
        records = resp.json()

        if not records:
            log.info(f"  No more records at offset {offset}")
            break

        all_records.extend(records)
        log.info(f"  Received {len(records)} records (total: {len(all_records)})")

        if len(records) < PAGE_SIZE:
            break  # Last page

        offset += PAGE_SIZE
        page += 1
        time.sleep(0.5)  # Be respectful to the API

    log.info(f"  Total records downloaded: {len(all_records)}")
    return all_records


def save_csv(records, output_path, gzip_output=False):
    """Save records as CSV (optionally gzip-compressed).

    Field names are auto-detected from the union of all record keys,
    preserving the order in which they first appear.
    """
    if not records:
        log.warning("No records to save")
        return None

    # Get all unique field names across all records, preserving order
    fieldnames = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if gzip_output:
        gz_path = output_path.with_suffix(output_path.suffix + ".gz")
        log.info(f"  Saving gzip CSV: {gz_path}")
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        file_size = gz_path.stat().st_size
        log.info(f"  Saved: {gz_path} ({file_size:,} bytes)")
        return str(gz_path)
    else:
        log.info(f"  Saving CSV: {output_path}")
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        file_size = output_path.stat().st_size
        log.info(f"  Saved: {output_path} ({file_size:,} bytes)")
        return str(output_path)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download Montgomery County, MD crash data from Socrata SODA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # All datasets, 2015-2024
  %(prog)s --year 2023                        # All datasets, 2023 only
  %(prog)s --dataset crashes --year 2024      # Crashes only, 2024
  %(prog)s --dataset drivers --gzip           # Drivers, all years, gzipped
  %(prog)s --health-check                     # Test API connectivity

Datasets:
  crashes       Crash Incidents (bhju-22kf)
  drivers       Drivers (mmzv-x632)
  nonmotorists  Non-Motorists (n7fk-dce5)

Default year range: 2015-2024
Output directory: data/MarylandDOT/montgomery/
""",
    )

    parser.add_argument(
        "--year", "-y",
        type=int,
        default=None,
        help="Single year to download (default: all years 2015-2024)",
    )
    parser.add_argument(
        "--dataset", "-s",
        type=str,
        default=None,
        choices=list(DATASETS.keys()),
        help="Download only this dataset (default: all three)",
    )
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        default=DEFAULT_DATA_DIR,
        help=f"Output directory for downloaded data (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--gzip", "-g",
        action="store_true",
        help="Output gzip-compressed CSV (.csv.gz) for R2 storage",
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Test API connectivity for all endpoints and exit",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if output file already exists",
    )

    args = parser.parse_args()

    # Health check mode
    if args.health_check:
        success = health_check()
        sys.exit(0 if success else 1)

    # Determine which datasets to process
    if args.dataset:
        dataset_keys = [args.dataset]
    else:
        dataset_keys = list(DATASETS.keys())

    # Determine which years to process
    if args.year:
        years = [args.year]
    else:
        years = DEFAULT_YEARS

    log.info("=" * 60)
    log.info("Montgomery County Crash Data Downloader")
    log.info(f"Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("=" * 60)
    log.info(f"  Datasets: {', '.join(dataset_keys)}")
    log.info(f"  Years: {', '.join(str(y) for y in years)}")
    log.info(f"  Output dir: {args.data_dir}")
    log.info(f"  Gzip: {args.gzip}")
    log.info(f"  Force: {args.force}")
    log.info("=" * 60)

    overall_start = time.time()
    summary = []  # list of (dataset, year, record_count, path)

    for dataset_key in dataset_keys:
        ds = DATASETS[dataset_key]
        for year in years:
            log.info("-" * 60)

            # Build output path
            filename = f"{ds['file_prefix']}_{year}.csv"
            output_path = Path(args.data_dir) / filename

            # Check if output already exists
            gz_path = Path(str(output_path) + ".gz")
            if not args.force and (output_path.exists() or gz_path.exists()):
                existing = gz_path if gz_path.exists() else output_path
                log.info(f"Skipping {ds['name']} {year} — already exists: {existing}")
                log.info("  Use --force to re-download")
                summary.append((dataset_key, year, -1, str(existing)))
                continue

            # Download
            try:
                records = download_data(dataset_key, year)
            except Exception as e:
                log.error(f"Failed to download {ds['name']} {year}: {e}")
                summary.append((dataset_key, year, 0, "FAILED"))
                continue

            if not records:
                log.warning(f"No records for {ds['name']} {year}")
                summary.append((dataset_key, year, 0, "NO DATA"))
                continue

            # Save
            saved_path = save_csv(records, output_path, gzip_output=args.gzip)
            summary.append((dataset_key, year, len(records), saved_path))

    # Print summary
    elapsed = time.time() - overall_start
    log.info("=" * 60)
    log.info("Download Summary")
    log.info("=" * 60)
    for dataset_key, year, count, path in summary:
        ds_name = DATASETS[dataset_key]["name"]
        if count == -1:
            status = "SKIPPED (exists)"
        elif count == 0:
            status = path  # "FAILED" or "NO DATA"
        else:
            status = f"{count:,} records -> {path}"
        log.info(f"  {ds_name} {year}: {status}")
    log.info("-" * 60)
    log.info(f"  Total elapsed: {elapsed:.1f}s")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
