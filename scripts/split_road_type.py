#!/usr/bin/env python3
"""
Split jurisdiction CSVs into road-type variants.

For each jurisdiction's all_roads.csv, produces:
  - {jurisdiction}_county_roads.csv  (county-maintained roads only)
  - {jurisdiction}_city_roads.csv    (city/town-maintained roads only — if cityRoads config exists)
  - {jurisdiction}_no_interstate.csv (everything except interstates)
  - {jurisdiction}_all_roads.csv     (already exists, copied/kept as-is)

Road system filtering is config-driven from states/{state}/config.json → roadSystems.splitConfig.

Usage:
    # Split a single jurisdiction
    python scripts/split_road_type.py --state virginia --jurisdiction henrico --data-dir data

    # Split multiple jurisdictions
    python scripts/split_road_type.py --state colorado --jurisdictions adams arapahoe douglas --data-dir data/CDOT

    # Split all jurisdictions in a directory (auto-detect from *_all_roads.csv files)
    python scripts/split_road_type.py --state virginia --data-dir data --auto
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

logger = logging.getLogger('split_road_type')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent

# ── ArcGIS FeatureServer raw field name → standard name mapping ──
# The Playwright-based downloader writes raw API field names (e.g., OWNERSHIP)
# while our splitConfig expects human-readable names (e.g., Ownership).
HEADER_ALIASES = {
    'OWNERSHIP': 'Ownership',
    'FUN': 'Functional Class',
    'FAC': 'Facility Type',
    'SYSTEM': 'SYSTEM',  # already matches
    'DOCUMENT_NBR': 'Document Nbr',
    'CRASH_YEAR': 'Crash Year',
    'CRASH_DT': 'Crash Date',
    'CRASH_SEVERITY': 'Crash Severity',
    'CRASH_MILITARY_TM': 'Crash Military Time',
    'PHYSICAL_JURIS': 'Physical Juris Name',
    'RTE_NM': 'RTE Name',
    'COLLISION_TYPE': 'Collision Type',
    'WEATHER_CONDITION': 'Weather Condition',
    'LIGHT_CONDITION': 'Light Condition',
    'AREA_TYPE': 'Area Type',
}

# ── ArcGIS coded domain values → human-readable text ──
# Maps numeric/abbreviated codes to the text labels our pipeline expects.
VALUE_DECODE_MAP = {
    'Ownership': {
        '1': '1. State Hwy Agency',
        '2': '2. County Hwy Agency',
        '3': '3. City or Town Hwy Agency',
        '4': '4. Federal Roads',
        '5': '5. Toll Roads Maintained by Others',
        '6': '6. Private/Unknown Roads',
    },
    'SYSTEM': {
        '1': 'DOT Interstate',
        '2': 'DOT Primary',
        '3': 'DOT Secondary',
        '4': 'Non-DOT primary',
        '5': 'Non-DOT secondary',
    },
    'Functional Class': {
        'INT': '1-Interstate (A,1)',
        'OFE': '2-Principal Arterial - Other Freeways and Expressways (B)',
        'OPA': '3-Principal Arterial - Other (E,2)',
        'MIA': '4-Minor Arterial (H,3)',
        'MAC': '5-Major Collector (I,4)',
        'MIC': '6-Minor Collector (5)',
        'LOC': '7-Local (J,6)',
    },
}


def normalize_headers_and_values(headers, rows):
    """Normalize raw ArcGIS field names and decode coded domain values.

    Handles CSVs from both the old download_crash_data.py (already standardized)
    and the new download_virginia_crash_data.py (raw API field names + codes).
    """
    # Step 1: Rename headers
    new_headers = [HEADER_ALIASES.get(h, h) for h in headers]

    # Check if decoding is needed (are values already decoded text?)
    needs_decoding = False
    col_idx = {h: i for i, h in enumerate(new_headers)}
    if 'Ownership' in col_idx and len(rows) > 0:
        sample = rows[0][col_idx['Ownership']].strip()
        # If the value is a short numeric code, decoding is needed
        if sample.isdigit() and len(sample) <= 2:
            needs_decoding = True

    if not needs_decoding:
        return new_headers, rows

    # Step 2: Decode coded values in-place
    logger.info("  Decoding raw ArcGIS coded values → text labels")
    decode_columns = {}
    for col_name, value_map in VALUE_DECODE_MAP.items():
        if col_name in col_idx:
            decode_columns[col_idx[col_name]] = value_map

    if decode_columns:
        new_rows = []
        for row in rows:
            new_row = list(row)
            for idx, value_map in decode_columns.items():
                val = new_row[idx].strip()
                if val in value_map:
                    new_row[idx] = value_map[val]
            new_rows.append(new_row)
        return new_headers, new_rows

    return new_headers, rows


def load_state_config(state):
    """Load state-specific config for road system split rules."""
    config_path = PROJECT_ROOT / 'states' / state / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def get_split_config(state_config):
    """Get road split configuration."""
    return state_config.get('roadSystems', {}).get('splitConfig', {})


def get_system_column(headers, split_config):
    """Determine which column to use for road system filtering."""
    # Check splitConfig for column hints
    county_config = split_config.get('countyRoads', {})
    interstate_config = split_config.get('interstateExclusion', {})

    # Try configured columns first
    for config in [county_config, interstate_config]:
        col = config.get('column', '')
        if col and col in headers:
            return col

    # Fall back to standard SYSTEM column
    for candidate in ['SYSTEM', 'System', 'system']:
        if candidate in headers:
            return candidate

    return None


def _resolve_column(col_idx, config_section, default_col):
    """Find the actual column name, checking primary name then aliases."""
    col = config_section.get('column', default_col)
    if col in col_idx:
        return col
    # Try columnAliases from config
    for alias in config_section.get('columnAliases', []):
        if alias in col_idx:
            return alias
    return None


def _filter_by_config(rows, headers, config_section, label):
    """Generic ownership/system/agency filter used by both county and city roads.

    Supports the same 3 methods as the original filter_county_roads:
      - system_column: match values in a SYSTEM-style column
      - agency_id: match agency IDs in a custom column (with jurisdiction mapping)
      - ownership: match values in an Ownership-style column

    Returns filtered rows, or all rows if column not found.
    """
    method = config_section.get('method', 'system_column')
    col_idx = {h: i for i, h in enumerate(headers)}

    if method == 'system_column':
        col = _resolve_column(col_idx, config_section, 'SYSTEM')
        include_values = config_section.get('includeValues', [])
        if not include_values:
            include_values = ['Non-DOT secondary', 'Non-DOT']

        if col is None:
            logger.warning(f"  Column not found for {label} filter — returning empty")
            return []

        idx = col_idx[col]
        include_upper = {v.upper() for v in include_values}
        return [r for r in rows if r[idx].strip().upper() in include_upper]

    elif method == 'agency_id':
        col = _resolve_column(col_idx, config_section, '_co_agency_id')
        if col is None:
            logger.warning(f"  Column not found for agency-based {label} filter — returning empty")
            return []

        idx = col_idx[col]
        agency_map = config_section.get('agencyMap', {})
        all_agency_ids = set()
        for ids in agency_map.values():
            all_agency_ids.update(ids)

        if all_agency_ids:
            return [r for r in rows if r[idx].strip() in all_agency_ids]
        else:
            return rows

    elif method == 'ownership':
        col = _resolve_column(col_idx, config_section, 'Ownership')
        include_values = config_section.get('includeValues', [])
        if col is None:
            logger.warning(f"  Column not found for ownership-based {label} filter — returning empty")
            return []
        idx = col_idx[col]
        include_upper = {v.upper() for v in include_values}
        return [r for r in rows if r[idx].strip().upper() in include_upper]

    return rows


def filter_county_roads(rows, headers, split_config):
    """Filter to county-maintained roads only."""
    county_config = split_config.get('countyRoads', {})
    if not county_config:
        return rows
    return _filter_by_config(rows, headers, county_config, 'county roads')


def filter_city_roads(rows, headers, split_config):
    """Filter to city/town-maintained roads only.

    Uses splitConfig.cityRoads — same method options as countyRoads
    (ownership, system_column, agency_id).

    Returns None if no cityRoads config exists (signals: skip city_roads output).
    """
    city_config = split_config.get('cityRoads', {})
    if not city_config:
        return None  # No config → skip city_roads CSV
    return _filter_by_config(rows, headers, city_config, 'city roads')


def filter_no_interstate(rows, headers, split_config):
    """Filter out interstate roads."""
    interstate_config = split_config.get('interstateExclusion', {})
    method = interstate_config.get('method', 'system_column')

    col_idx = {h: i for i, h in enumerate(headers)}

    if method == 'column_value':
        col = _resolve_column(col_idx, interstate_config, '_co_system_code')
        exclude_values = interstate_config.get('excludeValues', [])
        if col is None:
            logger.warning(f"  Column not found for interstate exclusion")
            return rows
        idx = col_idx[col]
        exclude_upper = {v.upper() for v in exclude_values}
        return [r for r in rows if r[idx].strip().upper() not in exclude_upper]

    elif method == 'system_column':
        col = _resolve_column(col_idx, interstate_config, 'SYSTEM')
        exclude_values = interstate_config.get('excludeValues', ['Interstate', 'DOT Interstate'])
        if col is None:
            logger.warning(f"  Column not found for interstate exclusion")
            return rows
        idx = col_idx[col]
        exclude_upper = {v.upper() for v in exclude_values}
        return [r for r in rows if r[idx].strip().upper() not in exclude_upper]

    elif method == 'functional_class':
        col = _resolve_column(col_idx, interstate_config, 'Functional Class')
        exclude_values = interstate_config.get('excludeValues', ['1-Interstate (A,1)'])
        if col is None:
            logger.warning(f"  Column not found for functional class interstate exclusion")
            return rows
        idx = col_idx[col]
        exclude_upper = {v.upper() for v in exclude_values}
        # Include rows where value is NOT in exclude list (including blank/empty values)
        return [r for r in rows if r[idx].strip().upper() not in exclude_upper]

    return rows


def split_jurisdiction(jurisdiction, data_dir, split_config):
    """Split a single jurisdiction's all_roads.csv into road-type CSVs."""
    data_dir = Path(data_dir)
    input_path = data_dir / f"{jurisdiction}_all_roads.csv"

    if not input_path.exists():
        logger.warning(f"  {jurisdiction}: all_roads.csv not found at {input_path}")
        return False

    # Read all rows
    with open(input_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        headers = next(reader)
        all_rows = list(reader)

    # Normalize raw ArcGIS field names and decode coded values if needed
    headers, all_rows = normalize_headers_and_values(headers, all_rows)

    total = len(all_rows)

    # county_roads
    county_rows = filter_county_roads(all_rows, headers, split_config)
    county_path = data_dir / f"{jurisdiction}_county_roads.csv"
    with open(county_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(county_rows)

    # city_roads (only if cityRoads config exists)
    city_rows = filter_city_roads(all_rows, headers, split_config)
    city_count_str = ''
    if city_rows is not None:
        city_path = data_dir / f"{jurisdiction}_city_roads.csv"
        with open(city_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(city_rows)
        city_count_str = f', city_roads={len(city_rows):,}'

    # no_interstate
    no_interstate_rows = filter_no_interstate(all_rows, headers, split_config)
    no_interstate_path = data_dir / f"{jurisdiction}_no_interstate.csv"
    with open(no_interstate_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(no_interstate_rows)

    logger.info(f"  {jurisdiction}: all={total:,}, county_roads={len(county_rows):,}{city_count_str}, no_interstate={len(no_interstate_rows):,}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Split jurisdiction CSVs into road-type variants')
    parser.add_argument('--state', required=True, help='State key (e.g., virginia, colorado)')
    parser.add_argument('--jurisdiction', help='Single jurisdiction to split')
    parser.add_argument('--jurisdictions', nargs='+', help='Multiple jurisdictions to split')
    parser.add_argument('--data-dir', required=True, help='Directory containing jurisdiction CSVs')
    parser.add_argument('--auto', action='store_true',
                        help='Auto-detect jurisdictions from *_all_roads.csv files')
    args = parser.parse_args()

    state_config = load_state_config(args.state)
    split_config = get_split_config(state_config)

    if not split_config:
        logger.warning(f"No splitConfig found in states/{args.state}/config.json — using defaults")
        split_config = {
            'countyRoads': {
                'method': 'system_column',
                'column': 'SYSTEM',
                'includeValues': ['Non-DOT secondary', 'Non-DOT']
            },
            'interstateExclusion': {
                'method': 'system_column',
                'column': 'SYSTEM',
                'excludeValues': ['Interstate', 'DOT Interstate']
            }
        }

    logger.info(f"[{args.state}] Road-type split")
    logger.info(f"[{args.state}] Data dir: {args.data_dir}")
    if 'cityRoads' in split_config:
        logger.info(f"[{args.state}] City roads filter: enabled")

    # Determine jurisdictions to process
    jurisdictions = []
    if args.jurisdiction:
        jurisdictions = [args.jurisdiction]
    elif args.jurisdictions:
        jurisdictions = args.jurisdictions
    elif args.auto:
        data_dir = Path(args.data_dir)
        for f in sorted(data_dir.glob('*_all_roads.csv')):
            j = f.stem.replace('_all_roads', '')
            jurisdictions.append(j)
        logger.info(f"  Auto-detected {len(jurisdictions)} jurisdictions")
    else:
        logger.error("Specify --jurisdiction, --jurisdictions, or --auto")
        return 1

    success = 0
    failed = 0
    for j in jurisdictions:
        if split_jurisdiction(j, args.data_dir, split_config):
            success += 1
        else:
            failed += 1

    logger.info("=" * 60)
    logger.info(f"[{args.state}] ROAD-TYPE SPLIT COMPLETE")
    logger.info(f"[{args.state}]   Success: {success}/{len(jurisdictions)}")
    if failed:
        logger.info(f"[{args.state}]   Failed:  {failed}")
    logger.info("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
