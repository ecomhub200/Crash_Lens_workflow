#!/usr/bin/env python3
"""
Split crash data into road-type filter files (multi-state aware).

Works with BOTH raw CDOT data AND standardized (post-conversion) data.
Auto-detects the format and uses the appropriate column names.

Reads a source CSV and produces:
  - {jurisdiction}_county_roads.csv  : County/city-maintained roads only
  - {jurisdiction}_no_interstate.csv : All roads EXCEPT interstate
  - {jurisdiction}_all_roads.csv     : Complete dataset (copy of source)

Filtering logic per state:
  Colorado (raw CDOT):
    county_roads   -> Agency Id = "DSO" (or jurisdiction-specific agency)
    no_interstate  -> System Code != "Interstate Highway"

  Colorado (standardized):
    county_roads   -> _co_agency_id = "DSO"
    no_interstate  -> _co_system_code != "Interstate Highway" OR SYSTEM != "Interstate"

  Virginia (standardized):
    county_roads   -> SYSTEM in {Non-DOT secondary, Non-DOT}
    no_interstate  -> SYSTEM != "Interstate"

Usage:
    python split_cdot_data.py [--jurisdiction douglas] [--data-dir ../data/CDOT]
    python split_cdot_data.py -j douglas --state colorado
    python split_cdot_data.py -j henrico --state virginia --data-dir ../data
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Agency IDs for county-maintained roads by jurisdiction (Colorado)
COUNTY_AGENCY_MAP = {
    'douglas': 'DSO',     # Douglas County Sheriff's Office
    'arapahoe': 'ASO',
    'jefferson': 'JSO',
    'elpaso': 'EPSO',
    'denver': 'DPD',
    'adams': 'ACSO',
}

# Virginia county road system values
VIRGINIA_COUNTY_SYSTEMS = {'Non-DOT secondary', 'Non-DOT'}


def detect_format(headers):
    """Detect whether data is raw CDOT, standardized Colorado, or standardized Virginia."""
    h = set(headers)
    if 'CUID' in h and 'Injury 00' in h:
        return 'raw_cdot'
    if '_co_agency_id' in h or '_co_system_code' in h:
        return 'standardized_colorado'
    if 'SYSTEM' in h and 'Document Nbr' in h:
        return 'standardized_virginia'
    if 'Agency Id' in h and 'System Code' in h:
        return 'raw_cdot'
    return 'unknown'


def split_data(source_path, output_dir, jurisdiction, state=None):
    """Read the source CSV and write filtered versions."""

    if not os.path.exists(source_path):
        print(f"ERROR: Source file not found: {source_path}")
        sys.exit(1)

    with open(source_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        all_rows = list(reader)

    fmt = detect_format(headers)
    print(f"Source: {source_path}")
    print(f"Detected format: {fmt}")
    print(f"Total rows: {len(all_rows):,}")
    print(f"Output directory: {output_dir}")
    print()

    # Determine filter columns based on format
    if fmt == 'raw_cdot':
        agency_col = 'Agency Id'
        system_col = 'System Code'
        county_agency = COUNTY_AGENCY_MAP.get(jurisdiction, 'DSO')
        interstate_value = 'Interstate Highway'

        _show_distribution(all_rows, agency_col, f"Agency Id (county = {county_agency})", county_agency)
        _show_distribution(all_rows, system_col, "System Code")

        county_rows = [r for r in all_rows if r.get(agency_col, '').strip() == county_agency]
        no_interstate = [r for r in all_rows if r.get(system_col, '').strip() != interstate_value]

    elif fmt == 'standardized_colorado':
        # Use preserved original Colorado columns
        agency_col = '_co_agency_id'
        system_col = '_co_system_code'
        county_agency = COUNTY_AGENCY_MAP.get(jurisdiction, 'DSO')

        _show_distribution(all_rows, agency_col, f"Agency Id (county = {county_agency})", county_agency)
        _show_distribution(all_rows, 'SYSTEM', "SYSTEM (standardized)")

        county_rows = [r for r in all_rows if r.get(agency_col, '').strip() == county_agency]
        no_interstate = [r for r in all_rows if r.get(system_col, '').strip() != 'Interstate Highway']

    elif fmt == 'standardized_virginia':
        system_col = 'SYSTEM'
        _show_distribution(all_rows, system_col, "SYSTEM")

        county_rows = [r for r in all_rows if r.get(system_col, '').strip() in VIRGINIA_COUNTY_SYSTEMS]
        no_interstate = [r for r in all_rows if r.get(system_col, '').strip() != 'Interstate']

    else:
        print(f"WARNING: Unknown format. Attempting SYSTEM-based filtering.")
        if 'SYSTEM' in headers:
            system_col = 'SYSTEM'
            county_rows = [r for r in all_rows if r.get(system_col, '').strip() in VIRGINIA_COUNTY_SYSTEMS]
            no_interstate = [r for r in all_rows if r.get(system_col, '').strip() != 'Interstate']
        else:
            print("ERROR: Cannot determine filter columns. Aborting.")
            sys.exit(1)

    # === Write all_roads (copy of source if not already named correctly) ===
    all_roads_path = os.path.join(output_dir, f"{jurisdiction}_all_roads.csv")
    if os.path.abspath(source_path) != os.path.abspath(all_roads_path):
        _write_csv(all_roads_path, headers, all_rows, "All Roads (complete)", len(all_rows))
    else:
        print(f"All roads file already exists: {all_roads_path}")
        print()

    # === Write county_roads ===
    _write_csv(
        os.path.join(output_dir, f"{jurisdiction}_county_roads.csv"),
        headers, county_rows,
        "County Roads Only",
        len(all_rows)
    )

    # === Write no_interstate ===
    _write_csv(
        os.path.join(output_dir, f"{jurisdiction}_no_interstate.csv"),
        headers, no_interstate,
        "All Roads (No Interstate)",
        len(all_rows)
    )

    print("Done! All filter files created.")


def _show_distribution(rows, column, label, highlight=None):
    """Show value distribution for a column."""
    counts = {}
    for row in rows:
        val = row.get(column, '').strip()
        counts[val] = counts.get(val, 0) + 1

    print(f"{label} distribution:")
    for val, count in sorted(counts.items(), key=lambda x: -x[1]):
        marker = " <-- county-maintained" if highlight and val == highlight else ""
        print(f"  {val or '(empty)'}: {count:,}{marker}")
    print()


def _write_csv(output_path, headers, rows, description, total):
    """Write a filtered CSV and print summary."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Created: {output_path}")
    print(f"  Filter: {description}")
    print(f"  Rows: {len(rows):,} / {total:,}")
    print(f"  Size: {size_mb:.1f} MB")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Split crash data into road-type filter files (multi-state aware)."
    )
    parser.add_argument(
        "--jurisdiction", "-j",
        default="douglas",
        help="Jurisdiction name (default: douglas)",
    )
    parser.add_argument(
        "--state", "-s",
        default=None,
        help="State key: colorado, virginia (auto-detected from data)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory containing the source CSV (default: auto-detect)",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Explicit source file path (overrides auto-detection)",
    )
    args = parser.parse_args()

    # Determine source file
    if args.source:
        source_file = args.source
        output_dir = args.data_dir or os.path.dirname(source_file)
    else:
        if args.data_dir:
            data_dir = args.data_dir
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            repo_root = os.path.dirname(script_dir)
            data_dir = os.path.join(repo_root, "data", "CDOT")

        # Try standardized file first, then raw all_roads
        standardized = os.path.join(data_dir, f"{args.jurisdiction}_standardized.csv")
        all_roads = os.path.join(data_dir, f"{args.jurisdiction}_all_roads.csv")

        if os.path.exists(standardized):
            source_file = standardized
            print(f"Using standardized file: {standardized}")
        elif os.path.exists(all_roads):
            source_file = all_roads
        else:
            print(f"ERROR: No source file found. Tried:")
            print(f"  {standardized}")
            print(f"  {all_roads}")
            sys.exit(1)

        output_dir = data_dir

    split_data(source_file, output_dir, args.jurisdiction, state=args.state)


if __name__ == "__main__":
    main()
