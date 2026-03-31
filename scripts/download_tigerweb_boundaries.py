#!/usr/bin/env python3
"""
Download TIGERweb county polygon boundaries (GeoJSON) for all US states.

Pre-caches the county boundary GeoJSON files that tigerweb_pip.py's
TIGERwebValidator uses for Tier 1 (local shapely point-in-polygon).
Without these cached files, each state must download boundaries on first
use (Tier 2 API), which requires internet and is slower.

Output directory: cache/
  Files: {state_abbrev}_county_boundaries.geojson  (e.g. va_county_boundaries.geojson)

These are the same files that TIGERwebValidator._download_boundaries() creates
on-demand, but this script downloads them all upfront.

Usage:
    python scripts/download_tigerweb_boundaries.py                # All 50 states + DC
    python scripts/download_tigerweb_boundaries.py --state 51     # Virginia only
    python scripts/download_tigerweb_boundaries.py --state 08,51  # Colorado + Virginia
    python scripts/download_tigerweb_boundaries.py --output-dir shared/boundaries

Requires: requests
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# TIGERweb county layer (same as tigerweb_pip.py)
TIGERWEB_SERVICES = [
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/82/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2025/MapServer/82/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2024/MapServer/82/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/82/query",
]

# State outline layer (layer 80)
TIGERWEB_STATE_SERVICES = [
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/80/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2025/MapServer/80/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2024/MapServer/80/query",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/80/query",
]

MAX_RETRIES = 4
RATE_DELAY = 1.0  # seconds between state downloads to avoid rate limiting

# 50 states + DC: FIPS → (abbreviation, name)
STATE_FIPS = {
    "01": ("al", "Alabama"),       "02": ("ak", "Alaska"),
    "04": ("az", "Arizona"),       "05": ("ar", "Arkansas"),
    "06": ("ca", "California"),    "08": ("co", "Colorado"),
    "09": ("ct", "Connecticut"),   "10": ("de", "Delaware"),
    "11": ("dc", "District of Columbia"),
    "12": ("fl", "Florida"),       "13": ("ga", "Georgia"),
    "15": ("hi", "Hawaii"),        "16": ("id", "Idaho"),
    "17": ("il", "Illinois"),      "18": ("in", "Indiana"),
    "19": ("ia", "Iowa"),          "20": ("ks", "Kansas"),
    "21": ("ky", "Kentucky"),      "22": ("la", "Louisiana"),
    "23": ("me", "Maine"),         "24": ("md", "Maryland"),
    "25": ("ma", "Massachusetts"), "26": ("mi", "Michigan"),
    "27": ("mn", "Minnesota"),     "28": ("ms", "Mississippi"),
    "29": ("mo", "Missouri"),      "30": ("mt", "Montana"),
    "31": ("ne", "Nebraska"),      "32": ("nv", "Nevada"),
    "33": ("nh", "New Hampshire"), "34": ("nj", "New Jersey"),
    "35": ("nm", "New Mexico"),    "36": ("ny", "New York"),
    "37": ("nc", "North Carolina"),"38": ("nd", "North Dakota"),
    "39": ("oh", "Ohio"),          "40": ("ok", "Oklahoma"),
    "41": ("or", "Oregon"),        "42": ("pa", "Pennsylvania"),
    "44": ("ri", "Rhode Island"),  "45": ("sc", "South Carolina"),
    "46": ("sd", "South Dakota"),  "47": ("tn", "Tennessee"),
    "48": ("tx", "Texas"),         "49": ("ut", "Utah"),
    "50": ("vt", "Vermont"),       "51": ("va", "Virginia"),
    "53": ("wa", "Washington"),    "54": ("wv", "West Virginia"),
    "55": ("wi", "Wisconsin"),     "56": ("wy", "Wyoming"),
}


def _find_working_service(service_urls, test_fips="51"):
    """Try each TIGERweb service URL until one returns data."""
    for url in service_urls:
        try:
            resp = requests.get(url, params={
                "where": f"STATE='{test_fips}'",
                "outFields": "GEOID",
                "returnGeometry": "false",
                "resultRecordCount": "1",
                "f": "json",
            }, timeout=15)
            data = resp.json()
            if data.get("features"):
                service_name = url.split("/services/")[1].split("/MapServer")[0]
                print(f"  Using service: {service_name}")
                return url
        except Exception:
            continue
    return service_urls[0]  # fallback to first


def download_county_boundaries(state_fips, service_url):
    """
    Download county polygon boundaries for a single state as GeoJSON.
    Returns GeoJSON dict or None on failure.
    """
    params = {
        "where": f"STATE='{state_fips}'",
        "outFields": "GEOID,STATE,COUNTY,NAME,BASENAME,NAMELSAD",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "geojson",
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(service_url, params=params, timeout=120)
            resp.raise_for_status()
            geojson = resp.json()

            if "features" in geojson and len(geojson["features"]) > 0:
                return geojson

            # Try alternate field names
            if attempt == 0:
                params["where"] = f"STATEFP='{state_fips}'"
                continue
            elif attempt == 1:
                params["where"] = f"STATEFP20='{state_fips}'"
                continue

            return None

        except Exception as e:
            wait = 2 ** (attempt + 1)
            if attempt < MAX_RETRIES - 1:
                print(f"    Retry {attempt+1}/{MAX_RETRIES} in {wait}s: {e}")
                time.sleep(wait)
            else:
                print(f"    Failed after {MAX_RETRIES} attempts: {e}")
                return None

    return None


def download_state_outline(state_fips, service_url):
    """
    Download state outline polygon as GeoJSON.
    Returns GeoJSON dict or None on failure.
    """
    for field in ["STATE", "STATEFP", "STATEFP20"]:
        try:
            resp = requests.get(service_url, params={
                "where": f"{field}='{state_fips}'",
                "outFields": "GEOID,STATE,NAME,BASENAME",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
            }, timeout=120)
            resp.raise_for_status()
            geojson = resp.json()
            if geojson.get("features"):
                return geojson
        except Exception:
            continue
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Download TIGERweb county polygon boundaries for all US states"
    )
    parser.add_argument(
        "--state", type=str, default=None,
        help="Comma-separated 2-digit FIPS codes (e.g. '51' or '08,51'). Default: all states."
    )
    parser.add_argument(
        "--output-dir", type=str, default="cache",
        help="Output directory for GeoJSON files (default: cache/)"
    )
    parser.add_argument(
        "--include-state-outlines", action="store_true",
        help="Also download state outline polygons (layer 80)"
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip states that already have cached boundary files"
    )
    args = parser.parse_args()

    # Determine which states to download
    if args.state:
        fips_list = [f.strip().zfill(2) for f in args.state.split(",")]
        invalid = [f for f in fips_list if f not in STATE_FIPS]
        if invalid:
            print(f"ERROR: Invalid FIPS code(s): {', '.join(invalid)}")
            sys.exit(1)
    else:
        fips_list = sorted(STATE_FIPS.keys())

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  CrashLens — TIGERweb County Boundary Downloader")
    print(f"  Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  States: {len(fips_list)} ({', '.join(fips_list[:10])}{'...' if len(fips_list) > 10 else ''})")
    print(f"  Output: {output_dir}/")
    print("=" * 60)

    # Find a working TIGERweb service
    print("\nDiscovering TIGERweb service...")
    county_url = _find_working_service(TIGERWEB_SERVICES)
    state_url = None
    if args.include_state_outlines:
        state_url = _find_working_service(TIGERWEB_STATE_SERVICES)

    # Download each state
    success = []
    failed = []
    skipped = []

    for i, fips in enumerate(fips_list):
        abbrev, name = STATE_FIPS[fips]
        county_file = output_dir / f"{abbrev}_county_boundaries.geojson"

        # Skip existing?
        if args.skip_existing and county_file.exists():
            print(f"  [{i+1}/{len(fips_list)}] {name} ({abbrev.upper()}) — skipped (exists)")
            skipped.append(fips)
            continue

        print(f"  [{i+1}/{len(fips_list)}] {name} (FIPS {fips})...", end=" ", flush=True)

        geojson = download_county_boundaries(fips, county_url)
        if geojson and geojson.get("features"):
            n_counties = len(geojson["features"])
            with open(county_file, "w", encoding="utf-8") as f:
                json.dump(geojson, f)
            size_kb = os.path.getsize(county_file) / 1024
            print(f"{n_counties} counties ({size_kb:.0f} KB)")
            success.append((fips, name, n_counties))
        else:
            print("FAILED — no features returned")
            failed.append((fips, name))

        # Download state outline if requested
        if args.include_state_outlines and state_url:
            outline_file = output_dir / f"{abbrev}_state_outline.geojson"
            outline = download_state_outline(fips, state_url)
            if outline and outline.get("features"):
                with open(outline_file, "w", encoding="utf-8") as f:
                    json.dump(outline, f)

        # Rate limit between requests
        if i < len(fips_list) - 1:
            time.sleep(RATE_DELAY)

    # Summary
    print("\n" + "=" * 60)
    print(f"  Downloaded: {len(success)} states")
    if skipped:
        print(f"  Skipped:    {len(skipped)} states (already cached)")
    if failed:
        print(f"  Failed:     {len(failed)} states:")
        for fips, name in failed:
            print(f"    - {name} (FIPS {fips})")
    print(f"  Output dir: {output_dir}/")

    total_counties = sum(n for _, _, n in success)
    total_size_mb = sum(
        os.path.getsize(output_dir / f"{STATE_FIPS[fips][0]}_county_boundaries.geojson")
        for fips, _, _ in success
    ) / (1024 * 1024)
    print(f"  Total:      {total_counties} county polygons, {total_size_mb:.1f} MB")
    print("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
