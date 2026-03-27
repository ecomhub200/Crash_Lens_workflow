#!/usr/bin/env python3
"""
Fetch bounding boxes for all Virginia jurisdictions from Census TIGER data.

This script:
1. Reads jurisdictions from config.json
2. Queries Census TIGER Web API for each jurisdiction's boundary
3. Extracts bounding box (envelope) from geometry
4. Updates config.json with bbox data

Usage:
    python scripts/fetch_jurisdiction_bboxes.py

    # Preview without saving:
    python scripts/fetch_jurisdiction_bboxes.py --dry-run

    # Save to separate file:
    python scripts/fetch_jurisdiction_bboxes.py --output config_with_bbox.json
"""

import json
import requests
import time
import argparse
from pathlib import Path

# Census TIGER Web API endpoint for counties (includes VA independent cities)
# Layer 82 = Counties in Census 2020
TIGER_API_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/82/query"

# Virginia state FIPS code
VA_STATE_FIPS = "51"

# Rate limiting - be nice to the Census API
REQUEST_DELAY = 0.3  # seconds between requests


def fetch_bbox_for_fips(fips_code: str) -> dict | None:
    """
    Fetch bounding box for a Virginia jurisdiction from Census TIGER API.

    Args:
        fips_code: 3-digit county/city FIPS code (e.g., "087" for Henrico)

    Returns:
        Dict with bbox [west, south, east, north] or None if failed
    """
    full_fips = f"{VA_STATE_FIPS}{fips_code.zfill(3)}"

    params = {
        "where": f"GEOID='{full_fips}'",
        "outFields": "GEOID,NAME",
        "returnGeometry": "true",
        "returnExtentOnly": "false",
        "f": "json",
        "outSR": "4326"  # WGS84 lat/lng
    }

    try:
        response = requests.get(TIGER_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "features" not in data or len(data["features"]) == 0:
            print(f"  ⚠ No features found for FIPS {full_fips}")
            return None

        feature = data["features"][0]

        # Extract geometry and calculate bounding box
        geometry = feature.get("geometry", {})
        rings = geometry.get("rings", [])

        if not rings:
            print(f"  ⚠ No geometry rings for FIPS {full_fips}")
            return None

        # Flatten all coordinates from all rings
        all_coords = []
        for ring in rings:
            all_coords.extend(ring)

        if not all_coords:
            return None

        # Calculate envelope (bounding box)
        lngs = [coord[0] for coord in all_coords]
        lats = [coord[1] for coord in all_coords]

        bbox = [
            round(min(lngs), 4),  # west
            round(min(lats), 4),  # south
            round(max(lngs), 4),  # east
            round(max(lats), 4)   # north
        ]

        name = feature.get("attributes", {}).get("NAME", "Unknown")
        return {"bbox": bbox, "name": name}

    except requests.exceptions.RequestException as e:
        print(f"  ✗ Request failed for FIPS {full_fips}: {e}")
        return None
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        print(f"  ✗ Parse error for FIPS {full_fips}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Fetch bounding boxes for Virginia jurisdictions")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    parser.add_argument("--output", type=str, help="Output file (default: update config.json in place)")
    parser.add_argument("--config", type=str, default="config.json", help="Input config file")
    args = parser.parse_args()

    # Find config.json
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    config_path = project_root / args.config

    if not config_path.exists():
        print(f"✗ Config file not found: {config_path}")
        return 1

    # Load config
    print(f"Loading config from: {config_path}")
    with open(config_path, "r") as f:
        config = json.load(f)

    jurisdictions = config.get("jurisdictions", {})
    if not jurisdictions:
        print("✗ No jurisdictions found in config")
        return 1

    print(f"Found {len(jurisdictions)} jurisdictions")
    print("-" * 50)

    # Track statistics
    success_count = 0
    skip_count = 0
    fail_count = 0

    # Fetch bbox for each jurisdiction
    for key, jur_data in jurisdictions.items():
        fips = jur_data.get("fips", "")
        name = jur_data.get("name", key)

        # Skip if already has bbox
        if "bbox" in jur_data:
            print(f"⏭ {name}: Already has bbox, skipping")
            skip_count += 1
            continue

        if not fips:
            print(f"⚠ {name}: No FIPS code, skipping")
            fail_count += 1
            continue

        print(f"Fetching: {name} (FIPS: 51{fips.zfill(3)})...", end=" ")

        result = fetch_bbox_for_fips(fips)

        if result:
            jur_data["bbox"] = result["bbox"]
            print(f"✓ bbox: {result['bbox']}")
            success_count += 1
        else:
            print(f"✗ Failed")
            fail_count += 1

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    print("-" * 50)
    print(f"Results: {success_count} success, {skip_count} skipped, {fail_count} failed")

    # Save updated config
    if args.dry_run:
        print("\n[DRY RUN] Would save to config.json")
        # Print sample of updated data
        sample_key = list(jurisdictions.keys())[0]
        print(f"\nSample entry ({sample_key}):")
        print(json.dumps(jurisdictions[sample_key], indent=2))
    else:
        output_path = Path(args.output) if args.output else config_path
        print(f"\nSaving to: {output_path}")

        with open(output_path, "w") as f:
            json.dump(config, f, indent=2)

        print(f"✓ Config updated with {success_count} new bounding boxes")

    return 0


if __name__ == "__main__":
    exit(main())
