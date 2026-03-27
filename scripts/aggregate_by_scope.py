#!/usr/bin/env python3
"""
Aggregate crash data CSVs by scope (region, MPO, statewide).

Produces region and MPO aggregate CSVs by concatenating rows from member
county CSVs. Output has the exact same Virginia-standard columns — just
containing rows from multiple counties.

A region/MPO CSV is NOT a summary — it's a concatenation of member county rows.
The frontend reads it exactly like a county CSV.

Usage:
    # Statewide: generate ALL region + ALL MPO aggregate CSVs
    python scripts/aggregate_by_scope.py --state virginia --scope statewide --data-dir data --output-dir data

    # Single region
    python scripts/aggregate_by_scope.py --state virginia --scope region --selection richmond --data-dir data --output-dir data

    # Single MPO
    python scripts/aggregate_by_scope.py --state colorado --scope mpo --selection drcog --data-dir data/CDOT --output-dir data/CDOT

    # Federal cross-state
    python scripts/aggregate_by_scope.py --federal --output-format csv --output-dir data
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

logger = logging.getLogger('aggregate_by_scope')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent

ROAD_TYPES = ['all_roads', 'no_interstate', 'county_roads', 'city_roads']


def load_hierarchy(state):
    path = PROJECT_ROOT / 'states' / state / 'hierarchy.json'
    if not path.exists():
        logger.error(f"No hierarchy.json for '{state}' at {path}")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def county_name_to_key(name):
    """Convert display name to snake_case key."""
    key = name.lower().strip()
    key = key.replace("'", "").replace(".", "")
    key = re.sub(r'[^a-z0-9]+', '_', key)
    return key.strip('_')


def load_config_fips_map(state_abbrev=None):
    """Build FIPS → config.json jurisdiction key mapping.

    config.json jurisdiction keys are the source of truth for filenames
    (e.g., "alexandria" not "alexandria_city"), so we prefer these over
    hierarchy-derived keys when available.

    Args:
        state_abbrev: State abbreviation (e.g., 'VA', 'CO') to filter
            jurisdictions. Required to avoid FIPS collisions across states
            (e.g., FIPS 027 = Buchanan in VA, Custer in CO).
    """
    config_path = PROJECT_ROOT / 'config.json'
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        config = json.load(f)
    fips_to_config_key = {}
    for jid, jinfo in config.get('jurisdictions', {}).items():
        if isinstance(jinfo, dict) and 'fips' in jinfo:
            if state_abbrev and jinfo.get('state', '').upper() != state_abbrev.upper():
                continue
            fips_to_config_key[jinfo['fips']] = jid
    return fips_to_config_key


def build_fips_to_key_map(hierarchy):
    """Build FIPS → jurisdiction key mapping.

    Prefers config.json keys (which match actual output filenames) over
    hierarchy-derived keys. Falls back to hierarchy names for FIPS codes
    not in config.json.
    """
    # Extract state abbreviation from hierarchy to filter config.json entries
    state_abbrev = hierarchy.get('state', {}).get('abbreviation')

    # Prefer config.json keys — they match Stage 1 output filenames
    config_fips_map = load_config_fips_map(state_abbrev)

    all_counties = hierarchy.get('allCounties', {})
    fips_to_key = {}
    for fips, name in all_counties.items():
        if fips in config_fips_map:
            fips_to_key[fips] = config_fips_map[fips]
        elif isinstance(name, str):
            fips_to_key[fips] = county_name_to_key(name)
        elif isinstance(name, dict):
            display = name.get('name', name.get('displayName', fips))
            fips_to_key[fips] = county_name_to_key(display)
    return fips_to_key


def fips_list_to_keys(hierarchy, fips_codes):
    """Convert FIPS codes list to jurisdiction keys."""
    fips_to_key = build_fips_to_key_map(hierarchy)
    return [fips_to_key[code] for code in fips_codes if code in fips_to_key]


def get_regions(hierarchy):
    """Get all regions with their member county keys."""
    fips_to_key = build_fips_to_key_map(hierarchy)
    regions = hierarchy.get('regions', {})
    result = {}
    for rid, rdata in regions.items():
        fips_codes = rdata.get('counties', [])
        member_keys = [fips_to_key[code] for code in fips_codes if code in fips_to_key]
        result[rid] = member_keys
    return result


def get_mpos(hierarchy):
    """Get all MPOs (from tprs with type=mpo) with their member county keys."""
    fips_to_key = build_fips_to_key_map(hierarchy)
    tprs = hierarchy.get('tprs', {})
    mpos = hierarchy.get('mpos', {})

    result = {}
    for key, val in tprs.items():
        if val.get('type') == 'mpo':
            fips_codes = val.get('counties', [])
            member_keys = [fips_to_key[code] for code in fips_codes if code in fips_to_key]
            result[key] = member_keys
    for key, val in mpos.items():
        fips_codes = val.get('counties', [])
        member_keys = [fips_to_key[code] for code in fips_codes if code in fips_to_key]
        result[key] = member_keys
    return result


def concat_county_csvs(member_counties, road_type, data_dir, output_path):
    """Concatenate member county CSVs into one aggregate CSV.

    Same columns, just more rows. Header from first county file.
    """
    data_dir = Path(data_dir)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    header = None
    all_rows = []
    found = 0
    missing = []

    for county in member_counties:
        county_csv = data_dir / f"{county}_{road_type}.csv"
        if not county_csv.exists():
            missing.append(county)
            continue

        with open(county_csv, 'r', newline='', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            try:
                file_header = next(reader)
            except StopIteration:
                continue

            if header is None:
                header = file_header
            # Skip header for subsequent files (they should match)

            for row in reader:
                all_rows.append(row)
            found += 1

    if header is None:
        logger.warning(f"  No county CSVs found for {road_type}")
        return 0

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(all_rows)

    if missing:
        logger.warning(f"  Missing county CSVs: {', '.join(missing)}")

    return len(all_rows)


def generate_group_csvs(group_type, group_id, member_counties, data_dir, output_dir):
    """Generate road-type CSVs for a region or MPO.

    Output filenames use just {road_type}.csv (no group_id prefix) to match
    the R2 key convention expected by the frontend:
        _region/{id}/{road_type}.csv
        _mpo/{id}/{road_type}.csv
    """
    logger.info(f"  Aggregating {group_type}/{group_id} ({len(member_counties)} counties)")

    for road_type in ROAD_TYPES:
        output_path = Path(output_dir) / f"_{group_type}" / group_id / f"{road_type}.csv"
        row_count = concat_county_csvs(member_counties, road_type, data_dir, output_path)
        if row_count > 0:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"    {road_type}.csv: {row_count:,} rows ({size_mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(description='Aggregate crash data CSVs by scope')
    parser.add_argument('--state', help='State key (e.g., virginia, colorado)')
    parser.add_argument('--scope', choices=['jurisdiction', 'region', 'mpo', 'statewide'],
                        help='Aggregation scope')
    parser.add_argument('--selection', default='', help='Region/MPO name (for scope=region/mpo)')
    parser.add_argument('--data-dir', required=True, help='Directory containing county CSVs')
    parser.add_argument('--output-format', default='csv', choices=['csv'],
                        help='Output format (always csv)')
    parser.add_argument('--output-dir', required=True, help='Output directory for aggregate CSVs')
    parser.add_argument('--federal', action='store_true', help='Generate federal cross-state aggregates')
    args = parser.parse_args()

    if args.federal:
        logger.info("Federal cross-state aggregation")
        # Federal aggregates: concatenate statewide CSVs from all active states
        federal_dir = Path(args.output_dir) / '_federal'
        federal_dir.mkdir(parents=True, exist_ok=True)

        # Find all state data directories
        config_path = PROJECT_ROOT / 'config.json'
        with open(config_path) as f:
            config = json.load(f)

        all_rows = []
        header = None
        states_found = 0

        for state_key, state_info in config.get('states', {}).items():
            # Skip non-dict entries (e.g., "_comment" keys that are strings)
            if not isinstance(state_info, dict):
                continue
            data_dir = state_info.get('dataDir')
            if not data_dir:
                continue
            statewide_csv = PROJECT_ROOT / 'data' / data_dir / f"{state_key}_statewide_validated_geocoded.csv"
            if not statewide_csv.exists():
                continue

            logger.info(f"  Including {state_key}: {statewide_csv}")
            with open(statewide_csv, 'r', newline='', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f)
                file_header = next(reader)
                if header is None:
                    header = file_header
                for row in reader:
                    all_rows.append(row)
                states_found += 1

        if header and all_rows:
            output_path = federal_dir / 'all_states_all_roads.csv'
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(all_rows)
            logger.info(f"  Federal aggregate: {len(all_rows):,} rows from {states_found} states")
        else:
            logger.warning("  No statewide CSVs found for federal aggregate")
        return 0

    # State-specific aggregation
    if not args.state:
        logger.error("--state required (unless --federal)")
        return 1
    if not args.scope:
        logger.error("--scope required")
        return 1

    state = args.state
    hierarchy = load_hierarchy(state)

    logger.info(f"[{state}] Aggregating scope={args.scope}, selection={args.selection or 'all'}")

    if args.scope == 'jurisdiction':
        logger.info("  Scope=jurisdiction — no aggregation needed (county CSVs are final)")
        return 0

    elif args.scope == 'region':
        if not args.selection:
            logger.error("--selection required for scope=region")
            return 1
        regions = get_regions(hierarchy)
        selection = args.selection
        if selection not in regions:
            # Try fuzzy match
            for key in regions:
                if key.lower().replace('_', '') == selection.lower().replace('_', ''):
                    selection = key
                    break
            else:
                logger.error(f"Region '{selection}' not found. Available: {', '.join(regions.keys())}")
                return 1
        generate_group_csvs('region', selection, regions[selection], args.data_dir, args.output_dir)

    elif args.scope == 'mpo':
        if not args.selection:
            logger.error("--selection required for scope=mpo")
            return 1
        mpos = get_mpos(hierarchy)
        selection = args.selection
        if selection not in mpos:
            for key in mpos:
                if key.lower().replace('_', '') == selection.lower().replace('_', ''):
                    selection = key
                    break
            else:
                logger.error(f"MPO '{selection}' not found. Available: {', '.join(mpos.keys())}")
                return 1
        generate_group_csvs('mpo', selection, mpos[selection], args.data_dir, args.output_dir)

    elif args.scope == 'statewide':
        # Generate ALL region CSVs + ALL MPO CSVs
        regions = get_regions(hierarchy)
        mpos = get_mpos(hierarchy)

        logger.info(f"  Generating {len(regions)} region aggregates + {len(mpos)} MPO aggregates")

        for rid, members in regions.items():
            generate_group_csvs('region', rid, members, args.data_dir, args.output_dir)

        for mid, members in mpos.items():
            generate_group_csvs('mpo', mid, members, args.data_dir, args.output_dir)

        logger.info(f"  Statewide aggregation complete")

    logger.info("=" * 60)
    logger.info(f"[{state}] AGGREGATION COMPLETE")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
