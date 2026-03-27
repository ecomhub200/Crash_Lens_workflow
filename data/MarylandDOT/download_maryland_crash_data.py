#!/usr/bin/env python3
"""
Maryland Statewide Vehicle Crashes — Socrata SODA API Downloader

Downloads crash data from Maryland Open Data Portal (ACRS system) via Socrata SODA API.
Supports jurisdiction filtering, date range queries, gzip compression for R2 storage,
and health check mode for API connectivity testing.

Data Source: https://opendata.maryland.gov/Public-Safety/Maryland-Statewide-Vehicle-Crashes/65du-s3qu
API Docs: https://dev.socrata.com/foundry/opendata.maryland.gov/65du-s3qu

Usage:
    python download_maryland_crash_data.py --jurisdiction montgomery --years 2023 2024
    python download_maryland_crash_data.py --jurisdiction baltimore_city --gzip
    python download_maryland_crash_data.py --health-check
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

SOCRATA_BASE_URL = "https://opendata.maryland.gov/resource/65du-s3qu.json"
SOCRATA_CSV_URL = "https://opendata.maryland.gov/api/views/65du-s3qu/rows.csv?accessType=DOWNLOAD"
SOCRATA_METADATA_URL = "https://opendata.maryland.gov/api/views/65du-s3qu.json"

PAGE_SIZE = 50000  # Socrata max per request
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]  # seconds

# Maryland jurisdictions: county_desc values → FIPS
JURISDICTIONS = {
    "allegany":         {"county_desc": "Allegany",         "fips": "24001"},
    "anne_arundel":     {"county_desc": "Anne Arundel",     "fips": "24003"},
    "baltimore_county": {"county_desc": "Baltimore County", "fips": "24005"},
    "calvert":          {"county_desc": "Calvert",          "fips": "24009"},
    "caroline":         {"county_desc": "Caroline",         "fips": "24011"},
    "carroll":          {"county_desc": "Carroll",          "fips": "24013"},
    "cecil":            {"county_desc": "Cecil",            "fips": "24015"},
    "charles":          {"county_desc": "Charles",          "fips": "24017"},
    "dorchester":       {"county_desc": "Dorchester",       "fips": "24019"},
    "frederick":        {"county_desc": "Frederick",        "fips": "24021"},
    "garrett":          {"county_desc": "Garrett",          "fips": "24023"},
    "harford":          {"county_desc": "Harford",          "fips": "24025"},
    "howard":           {"county_desc": "Howard",           "fips": "24027"},
    "kent":             {"county_desc": "Kent",             "fips": "24029"},
    "montgomery":       {"county_desc": "Montgomery",       "fips": "24031"},
    "prince_georges":   {"county_desc": "Prince George's",  "fips": "24033"},
    "queen_annes":      {"county_desc": "Queen Anne's",     "fips": "24035"},
    "st_marys":         {"county_desc": "St. Mary's",       "fips": "24037"},
    "somerset":         {"county_desc": "Somerset",         "fips": "24039"},
    "talbot":           {"county_desc": "Talbot",           "fips": "24041"},
    "washington":       {"county_desc": "Washington",       "fips": "24043"},
    "wicomico":         {"county_desc": "Wicomico",         "fips": "24045"},
    "worcester":        {"county_desc": "Worcester",        "fips": "24047"},
    "baltimore_city":   {"county_desc": "Baltimore City",   "fips": "24510"},
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("maryland_downloader")


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
    """Test API connectivity and report dataset status."""
    log.info("=" * 60)
    log.info("Maryland ACRS Socrata API — Health Check")
    log.info("=" * 60)

    # Test metadata endpoint
    try:
        resp = retry_request(SOCRATA_METADATA_URL)
        meta = resp.json()
        log.info(f"  Dataset Name: {meta.get('name', 'N/A')}")
        log.info(f"  Description: {meta.get('description', 'N/A')[:100]}...")
        log.info(f"  Last Updated: {meta.get('rowsUpdatedAt', 'N/A')}")
        log.info(f"  Row Count: {meta.get('cachedContents', {}).get('non_null', 'N/A')}")
    except Exception as e:
        log.error(f"  Metadata endpoint FAILED: {e}")
        return False

    # Test data endpoint with 1-row query
    try:
        resp = retry_request(SOCRATA_BASE_URL, params={"$limit": 1})
        data = resp.json()
        if data and len(data) > 0:
            sample = data[0]
            log.info(f"  Sample record keys: {list(sample.keys())[:10]}...")
            log.info(f"  Sample report_no: {sample.get('report_no', 'N/A')}")
            log.info(f"  Sample acc_date: {sample.get('acc_date', 'N/A')}")
            log.info(f"  Sample county_desc: {sample.get('county_desc', 'N/A')}")
        else:
            log.warning("  Data endpoint returned empty response")
    except Exception as e:
        log.error(f"  Data endpoint FAILED: {e}")
        return False

    log.info("  ✓ API is healthy and responding")
    log.info("=" * 60)
    return True


def build_where_clause(jurisdiction_key, years):
    """Build SoQL $where clause for filtering."""
    clauses = []

    # Jurisdiction filter
    if jurisdiction_key and jurisdiction_key in JURISDICTIONS:
        county_desc = JURISDICTIONS[jurisdiction_key]["county_desc"]
        clauses.append(f"county_desc='{county_desc}'")

    # Year filter
    if years:
        year_clauses = []
        for year in years:
            start = f"{year}-01-01T00:00:00.000"
            end = f"{year}-12-31T23:59:59.999"
            year_clauses.append(f"(acc_date>='{start}' AND acc_date<='{end}')")
        if len(year_clauses) == 1:
            clauses.append(year_clauses[0])
        else:
            clauses.append(f"({' OR '.join(year_clauses)})")

    return " AND ".join(clauses) if clauses else None


def download_data(jurisdiction_key, years, data_dir):
    """Download crash data from Socrata API with pagination."""
    where_clause = build_where_clause(jurisdiction_key, years)

    log.info(f"Downloading Maryland crash data...")
    if jurisdiction_key:
        county_info = JURISDICTIONS.get(jurisdiction_key, {})
        log.info(f"  Jurisdiction: {county_info.get('county_desc', jurisdiction_key)} (FIPS {county_info.get('fips', 'N/A')})")
    if years:
        log.info(f"  Years: {', '.join(str(y) for y in years)}")
    if where_clause:
        log.info(f"  SoQL filter: $where={where_clause}")

    all_records = []
    offset = 0
    page = 1

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "acc_date ASC",
        }
        if where_clause:
            params["$where"] = where_clause

        log.info(f"  Page {page}: offset={offset}, limit={PAGE_SIZE}")
        resp = retry_request(SOCRATA_BASE_URL, params=params)
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
    """Save records as CSV (optionally gzip-compressed)."""
    if not records:
        log.warning("No records to save")
        return None

    # Get all unique field names across all records
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
        description="Download Maryland crash data from Socrata SODA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --jurisdiction montgomery --years 2023 2024
  %(prog)s --jurisdiction baltimore_city --gzip
  %(prog)s --health-check
  %(prog)s --gzip  # Download all statewide data, gzip compressed

Available jurisdictions:
  """ + ", ".join(sorted(JURISDICTIONS.keys())),
    )

    parser.add_argument(
        "--jurisdiction", "-j",
        type=str,
        default=None,
        choices=list(JURISDICTIONS.keys()),
        help="County/city to filter to (default: all statewide)",
    )
    parser.add_argument(
        "--years", "-y",
        type=int,
        nargs="+",
        default=None,
        help="Years to download (e.g., --years 2023 2024)",
    )
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        default="data/MarylandDOT",
        help="Output directory for downloaded data (default: data/MarylandDOT)",
    )
    parser.add_argument(
        "--gzip", "-g",
        action="store_true",
        help="Output gzip-compressed CSV (.csv.gz) for R2 storage",
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Test API connectivity and exit",
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

    # Build output filename
    jurisdiction = args.jurisdiction or "statewide"
    year_suffix = ""
    if args.years:
        if len(args.years) == 1:
            year_suffix = f"_{args.years[0]}"
        else:
            year_suffix = f"_{min(args.years)}-{max(args.years)}"

    output_filename = f"{jurisdiction}{year_suffix}_crashes.csv"
    output_path = Path(args.data_dir) / output_filename

    # Check if output already exists
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        existing = gz_path if gz_path.exists() else output_path
        log.info(f"Output already exists: {existing}")
        log.info("Use --force to re-download")
        sys.exit(0)

    # Download
    log.info("=" * 60)
    log.info(f"Maryland Crash Data Downloader")
    log.info(f"Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("=" * 60)

    start_time = time.time()
    records = download_data(args.jurisdiction, args.years, args.data_dir)

    if not records:
        log.warning("No records downloaded. Exiting.")
        sys.exit(1)

    # Save
    saved_path = save_csv(records, output_path, gzip_output=args.gzip)

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info(f"Download complete!")
    log.info(f"  Records: {len(records):,}")
    log.info(f"  Output: {saved_path}")
    log.info(f"  Elapsed: {elapsed:.1f}s")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
