#!/usr/bin/env python3
"""
CRASH LENS - Master Data Processing Pipeline

Orchestrates the complete pipeline for converting raw crash data from any
supported US state into validated, standardized, tool-ready CSV files.

Pipeline stages:
  1. CONVERT  - Detect state format, normalize columns to Virginia-compatible standard
  2. VALIDATE - Run data quality checks with state-specific coordinate bounds
  3. GEOCODE  - Fill in missing GPS coordinates using free geocoding services
  4. SPLIT    - Split into road-type filter files (all roads, county, no-interstate)

The pipeline is designed to be:
  - State-agnostic: Auto-detects Colorado CDOT, Virginia TREDS, etc.
  - User-selectable: Accepts --state and --jurisdiction flags
  - Automatic: Runs all stages by default, or individual stages via flags
  - Dynamic: Reads state configs from states/{state}/config.json

Usage:
    # Full pipeline (auto-detect state from data):
    python process_crash_data.py --input data/CDOT/Douglas_County.csv --jurisdiction douglas

    # Full pipeline (explicit state):
    python process_crash_data.py --input data/CDOT/Douglas_County.csv --state colorado --jurisdiction douglas

    # Specific stages:
    python process_crash_data.py --input data/CDOT/Douglas_County.csv -j douglas --convert-only
    python process_crash_data.py --input data/CDOT/Douglas_County.csv -j douglas --skip-geocode
    python process_crash_data.py --input data/CDOT/Douglas_County.csv -j douglas --skip-split

    # Merge multiple yearly files then process:
    python process_crash_data.py --input data/CDOT/2021*.csv data/CDOT/2022*.csv -j douglas --merge

    # Dry run (preview what would happen):
    python process_crash_data.py --input data/CDOT/Douglas_County.csv -j douglas --dry-run

Author: CRASH LENS Team
Version: 1.0.0
"""

import argparse
import csv
import glob
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent and scripts dirs to path for imports
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from state_adapter import StateDetector, get_normalizer, convert_file, get_supported_states

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('pipeline')

# --- Constants ---
DATA_DIR = PROJECT_ROOT / 'data'
STATES_DIR = PROJECT_ROOT / 'states'


def get_state_dot_name(state_key: str) -> str:
    """Get the DOT folder name for a state from states/{state}/config.json."""
    config_path = STATES_DIR / state_key / 'config.json'
    if config_path.exists():
        with open(config_path, 'r') as f:
            cfg = json.load(f)
        return cfg.get('state', {}).get('dotName', state_key.upper())
    fallback = {
        'colorado': 'CDOT', 'virginia': 'VDOT', 'texas': 'TxDOT',
        'maryland': 'MDOT', 'northcarolina': 'NCDOT', 'pennsylvania': 'PennDOT',
    }
    return fallback.get(state_key, state_key.upper())


class PipelineConfig:
    """Configuration for the processing pipeline."""

    def __init__(self, args):
        self.input_files = args.input
        self.state = args.state
        self.jurisdiction = args.jurisdiction
        self.output_dir = Path(args.output_dir) if args.output_dir else None
        self.dry_run = args.dry_run
        self.verbose = args.verbose
        self.merge = args.merge
        self.merge_existing = args.merge_existing
        self.convert_only = args.convert_only
        self.skip_validation = args.skip_validation
        self.skip_geocode = args.skip_geocode
        self.skip_split = args.skip_split
        self.geocode = not args.skip_geocode
        self.full_validation = args.full_validation
        self.force = args.force

        # Resolve output directory from state DOT name
        if not self.output_dir:
            if self.state:
                # Use state DOT folder: data/{dotName}/
                dot_name = get_state_dot_name(self.state)
                self.output_dir = DATA_DIR / dot_name
            elif self.input_files:
                self.output_dir = Path(self.input_files[0]).parent
            else:
                self.output_dir = DATA_DIR


class PipelineStats:
    """Track pipeline statistics."""

    def __init__(self):
        self.start_time = time.time()
        self.stages_completed = []
        self.total_input_rows = 0
        self.total_output_rows = 0
        self.rows_with_gps = 0
        self.rows_without_gps = 0
        self.rows_geocoded = 0
        self.rows_corrected = 0
        self.rows_flagged = 0
        self.duplicates_removed = 0
        self.detected_state = None
        self.files_created = []
        self.warnings = []

    def elapsed(self) -> str:
        return f"{time.time() - self.start_time:.1f}s"

    def summary(self) -> str:
        lines = [
            "",
            "=" * 60,
            "  PIPELINE SUMMARY",
            "=" * 60,
            f"  State detected:    {self.detected_state}",
            f"  Input rows:        {self.total_input_rows:,}",
            f"  Output rows:       {self.total_output_rows:,}",
            f"  GPS coverage:      {self.rows_with_gps:,} / {self.total_output_rows:,} "
            f"({self.rows_with_gps / max(self.total_output_rows, 1) * 100:.1f}%)",
            f"  Rows geocoded:     {self.rows_geocoded:,}",
            f"  Rows corrected:    {self.rows_corrected:,}",
            f"  Rows flagged:      {self.rows_flagged:,}",
            f"  Duplicates removed:{self.duplicates_removed:,}",
            f"  Stages completed:  {', '.join(self.stages_completed)}",
            f"  Time elapsed:      {self.elapsed()}",
            f"  Files created:",
        ]
        for f in self.files_created:
            size = os.path.getsize(f) / (1024 * 1024) if os.path.exists(f) else 0
            lines.append(f"    - {f} ({size:.1f} MB)")
        if self.warnings:
            lines.append(f"  Warnings:")
            for w in self.warnings[:10]:
                lines.append(f"    ! {w}")
        lines.append("=" * 60)
        return '\n'.join(lines)


# ============================================================
# STAGE 1: MERGE (optional)
# ============================================================

def stage_merge(config: PipelineConfig, stats: PipelineStats) -> str:
    """
    Merge multiple input CSV files into one.

    Returns path to merged file.
    """
    input_files = []
    for pattern in config.input_files:
        expanded = glob.glob(pattern)
        input_files.extend(expanded)

    if not input_files:
        logger.error("No input files found matching patterns: %s", config.input_files)
        sys.exit(1)

    if len(input_files) == 1 and not config.merge:
        logger.info("Single input file: %s", input_files[0])
        return input_files[0]

    logger.info("=" * 50)
    logger.info("STAGE 0: MERGE (%d files)", len(input_files))
    logger.info("=" * 50)

    # Sort for consistent ordering
    input_files.sort()

    merged_path = str(config.output_dir / f"{config.jurisdiction}_merged_raw.csv")
    total_rows = 0
    headers = None

    if config.dry_run:
        logger.info("[DRY RUN] Would merge %d files into %s", len(input_files), merged_path)
        for f in input_files:
            logger.info("  - %s", f)
        return input_files[0]

    with open(merged_path, 'w', newline='', encoding='utf-8') as fout:
        writer = None

        for filepath in input_files:
            logger.info("  Reading: %s", filepath)
            with open(filepath, 'r', encoding='utf-8-sig') as fin:
                reader = csv.DictReader(fin)

                if headers is None:
                    headers = reader.fieldnames
                    writer = csv.DictWriter(fout, fieldnames=headers, extrasaction='ignore')
                    writer.writeheader()

                for row in reader:
                    # Add source file tracking
                    writer.writerow(row)
                    total_rows += 1

    logger.info("  Merged %d rows from %d files -> %s", total_rows, len(input_files), merged_path)
    stats.total_input_rows = total_rows
    stats.files_created.append(merged_path)
    stats.stages_completed.append('merge')
    return merged_path


# ============================================================
# STAGE 2: CONVERT
# ============================================================

def stage_convert(input_path: str, config: PipelineConfig, stats: PipelineStats) -> str:
    """
    Convert raw state-specific CSV to standardized Virginia-compatible format.

    Returns path to converted file.
    """
    logger.info("=" * 50)
    logger.info("STAGE 1: CONVERT (state format -> standardized)")
    logger.info("=" * 50)

    output_path = str(config.output_dir / f"{config.jurisdiction}_standardized.csv")

    if config.dry_run:
        detector = StateDetector()
        detected = detector.detect_from_file(input_path)
        logger.info("[DRY RUN] Detected state: %s", detected)
        logger.info("[DRY RUN] Would convert %s -> %s", input_path, output_path)
        stats.detected_state = detected
        stats.stages_completed.append('convert (dry-run)')
        return input_path

    src_name = os.path.basename(input_path)
    detected_state, total_rows, with_gps = convert_file(
        input_path, output_path,
        state=config.state,
        source_filename=src_name
    )

    stats.detected_state = detected_state
    stats.total_input_rows = total_rows
    stats.total_output_rows = total_rows
    stats.rows_with_gps = with_gps
    stats.rows_without_gps = total_rows - with_gps
    stats.files_created.append(output_path)
    stats.stages_completed.append('convert')

    # Auto-resolve output directory from detected state DOT name if not explicitly set
    if not config.state and detected_state and detected_state != 'unknown':
        config.state = detected_state
        dot_name = get_state_dot_name(detected_state)
        new_output_dir = DATA_DIR / dot_name
        if new_output_dir != config.output_dir:
            new_output_dir.mkdir(parents=True, exist_ok=True)
            new_output_path = str(new_output_dir / f"{config.jurisdiction}_standardized.csv")
            shutil.move(output_path, new_output_path)
            config.output_dir = new_output_dir
            output_path = new_output_path
            # Update tracked files
            stats.files_created[-1] = output_path
            logger.info("  Auto-resolved output dir: %s (from %s DOT)", new_output_dir, dot_name)

    logger.info("  Detected state: %s", detected_state)
    logger.info("  Total rows: %d", total_rows)
    logger.info("  GPS coverage: %d / %d (%.1f%%)",
                with_gps, total_rows, with_gps / max(total_rows, 1) * 100)
    logger.info("  Output: %s", output_path)

    return output_path


# ============================================================
# STAGE 3: VALIDATE
# ============================================================

def stage_validate(standardized_path: str, config: PipelineConfig, stats: PipelineStats) -> str:
    """
    Run validation and auto-correction on standardized data.

    Uses state-specific bounds from the state config.
    Returns path to validated file.
    """
    logger.info("=" * 50)
    logger.info("STAGE 2: VALIDATE (quality checks + auto-correct)")
    logger.info("=" * 50)

    if config.dry_run:
        logger.info("[DRY RUN] Would validate %s", standardized_path)
        stats.stages_completed.append('validate (dry-run)')
        return standardized_path

    # Load state config for bounds
    state_key = stats.detected_state or config.state or 'virginia'
    state_config_path = STATES_DIR / STATE_SIGNATURES_DIRS.get(state_key, state_key) / 'config.json'
    state_bounds = None
    if state_config_path.exists():
        with open(state_config_path, 'r') as f:
            state_cfg = json.load(f)
        bounds = state_cfg.get('state', {}).get('coordinateBounds', {})
        if bounds:
            state_bounds = {
                'minLat': bounds.get('latMin'),
                'maxLat': bounds.get('latMax'),
                'minLon': bounds.get('lonMin'),
                'maxLon': bounds.get('lonMax'),
            }

    # Read standardized data
    import pandas as pd
    df = pd.read_csv(standardized_path, dtype=str)
    original_count = len(df)
    logger.info("  Loaded %d records", original_count)

    corrections_made = 0
    issues_found = 0
    duplicates = 0

    # --- 1. Format normalization ---
    for col in df.select_dtypes(include=['object', 'str']).columns:
        df[col] = df[col].fillna('').astype(str).str.strip()

    # --- 2. Severity validation ---
    valid_severities = {'K', 'A', 'B', 'C', 'O'}
    bad_severity = ~df['Crash Severity'].isin(valid_severities)
    if bad_severity.any():
        # Try common corrections
        sev_map = {'F': 'K', 'FATAL': 'K', 'P': 'O', 'PDO': 'O', '': 'O'}
        for idx in df[bad_severity].index:
            val = df.at[idx, 'Crash Severity'].upper()
            if val in sev_map:
                df.at[idx, 'Crash Severity'] = sev_map[val]
                corrections_made += 1
            else:
                issues_found += 1

    # --- 3. Date validation ---
    current_year = datetime.now().year
    for idx, row in df.iterrows():
        year_str = str(row.get('Crash Year', '')).strip()
        if year_str:
            try:
                year = int(year_str)
                if year < 2015 or year > current_year:
                    issues_found += 1
            except ValueError:
                issues_found += 1

    # --- 4. Coordinate bounds validation (state-specific) ---
    if state_bounds:
        for idx, row in df.iterrows():
            x_str = str(row.get('x', '')).strip()
            y_str = str(row.get('y', '')).strip()
            if x_str and y_str:
                try:
                    lon = float(x_str)
                    lat = float(y_str)

                    # Check for transposed coordinates
                    if (state_bounds['minLat'] <= lon <= state_bounds['maxLat'] and
                            state_bounds['minLon'] <= lat <= state_bounds['maxLon']):
                        # Likely transposed - swap them
                        df.at[idx, 'x'] = y_str
                        df.at[idx, 'y'] = x_str
                        corrections_made += 1
                        logger.debug("  Swapped transposed coords for row %d", idx)
                    elif not (state_bounds['minLon'] <= lon <= state_bounds['maxLon']):
                        issues_found += 1
                    elif not (state_bounds['minLat'] <= lat <= state_bounds['maxLat']):
                        issues_found += 1
                except ValueError:
                    issues_found += 1

    # --- 5. Boolean field normalization ---
    bool_fields = [
        'Pedestrian?', 'Bike?', 'Alcohol?', 'Speed?', 'Hitrun?',
        'Motorcycle?', 'Night?', 'Distracted?', 'Drowsy?', 'Drug Related?',
        'Young?', 'Senior?', 'Unrestrained?', 'School Zone', 'Work Zone Related'
    ]
    bool_map = {
        'Y': 'Yes', 'N': 'No', '1': 'Yes', '0': 'No',
        'TRUE': 'Yes', 'FALSE': 'No', 'true': 'Yes', 'false': 'No',
        'yes': 'Yes', 'no': 'No'
    }
    for field in bool_fields:
        if field in df.columns:
            for old, new in bool_map.items():
                mask = df[field] == old
                if mask.any():
                    df.loc[mask, field] = new
                    corrections_made += mask.sum()

    # --- 6. Cross-field consistency ---
    # Fatal crash should have K_People > 0
    for idx, row in df.iterrows():
        sev = str(row.get('Crash Severity', '')).strip()
        if sev == 'K':
            try:
                k = int(row.get('K_People', '0') or '0')
                if k == 0:
                    issues_found += 1
            except ValueError:
                pass

    # Pedestrian collision should have Pedestrian? = Yes
    ped_mask = df['Collision Type'].str.contains('Pedestrian|Ped', case=False, na=False)
    ped_no = ped_mask & (df.get('Pedestrian?', pd.Series(['No'] * len(df))) != 'Yes')
    if ped_no.any():
        df.loc[ped_no, 'Pedestrian?'] = 'Yes'
        corrections_made += ped_no.sum()

    # Bicycle collision should have Bike? = Yes
    bike_mask = df['Collision Type'].str.contains('Bicyclist|Bicycle', case=False, na=False)
    bike_no = bike_mask & (df.get('Bike?', pd.Series(['No'] * len(df))) != 'Yes')
    if bike_no.any():
        df.loc[bike_no, 'Bike?'] = 'Yes'
        corrections_made += bike_no.sum()

    # --- 7. Duplicate detection ---
    dup_cols = ['Crash Date', 'x', 'y', 'Collision Type']
    existing = [c for c in dup_cols if c in df.columns]
    if len(existing) == 4:
        # Only check rows with valid coordinates
        has_coords = (df['x'] != '') & (df['y'] != '') & (df['x'] != '0') & (df['y'] != '0')
        df_with_coords = df[has_coords]
        dup_mask = df_with_coords.duplicated(subset=existing, keep='first')
        if dup_mask.any():
            duplicates = dup_mask.sum()
            df = df.drop(df_with_coords[dup_mask].index)
            logger.info("  Removed %d duplicates", duplicates)

    # --- 8. Injury count normalization ---
    injury_cols = ['K_People', 'A_People', 'B_People', 'C_People', 'Persons Injured']
    for col in injury_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(str)

    # Save validated data
    validated_path = standardized_path  # Overwrite standardized with validated
    df.to_csv(validated_path, index=False)

    stats.total_output_rows = len(df)
    stats.rows_corrected = corrections_made
    stats.rows_flagged = issues_found
    stats.duplicates_removed = duplicates
    stats.stages_completed.append('validate')

    # Recount GPS coverage after validation
    gps_count = 0
    for _, row in df.iterrows():
        x = str(row.get('x', '')).strip()
        y = str(row.get('y', '')).strip()
        if x and y and x != '0' and y != '0':
            try:
                float(x)
                float(y)
                gps_count += 1
            except ValueError:
                pass
    stats.rows_with_gps = gps_count
    stats.rows_without_gps = len(df) - gps_count

    logger.info("  Corrections applied: %d", corrections_made)
    logger.info("  Issues flagged: %d", issues_found)
    logger.info("  Duplicates removed: %d", duplicates)
    logger.info("  Output rows: %d", len(df))

    # Save validation report
    report = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'state': state_key,
        'jurisdiction': config.jurisdiction,
        'input_rows': int(original_count),
        'output_rows': int(len(df)),
        'corrections': int(corrections_made),
        'issues': int(issues_found),
        'duplicates_removed': int(duplicates),
        'gps_coverage': f"{gps_count / max(len(df), 1) * 100:.1f}%"
    }
    report_path = config.output_dir / '.validation'
    report_path.mkdir(parents=True, exist_ok=True)
    with open(report_path / 'pipeline_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    return validated_path


# State config directory mapping - auto-discovered from states/ directory
# Any subdirectory of states/ with a config.json is a valid state
def _discover_state_dirs():
    """Auto-discover state config directories from the states/ folder."""
    mapping = {}
    if STATES_DIR.exists():
        for child in STATES_DIR.iterdir():
            if child.is_dir() and (child / 'config.json').exists():
                mapping[child.name] = child.name
    # Fallback to known states if directory scanning fails
    if not mapping:
        mapping = {'colorado': 'colorado', 'virginia': 'virginia'}
    return mapping

STATE_SIGNATURES_DIRS = _discover_state_dirs()


# ============================================================
# STAGE 4: GEOCODE
# ============================================================

def _load_geocode_cache(cache_path: str) -> dict:
    """Load persistent geocode cache from disk."""
    try:
        with open(cache_path, 'r') as f:
            data = json.load(f)
        logger.info("  Geocode cache loaded: %d entries from %s",
                     len(data.get('nominatim', {})) + len(data.get('nodes', {})),
                     cache_path)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        logger.info("  No existing geocode cache found. Starting fresh.")
        return {'nominatim': {}, 'nodes': {}}


def _save_geocode_cache(cache_path: str, cache_data: dict) -> None:
    """Save persistent geocode cache to disk."""
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'w') as f:
        json.dump(cache_data, f, indent=1)
    total = len(cache_data.get('nominatim', {})) + len(cache_data.get('nodes', {}))
    logger.info("  Geocode cache saved: %d entries to %s", total, cache_path)


def stage_geocode(validated_path: str, config: PipelineConfig, stats: PipelineStats) -> str:
    """
    Geocode records with missing GPS coordinates.

    Uses a persistent cache to avoid redundant API calls across runs.
    Cache file: {output_dir}/.geocode_cache.json

    Strategies (in order):
      1. Persistent cache - instant lookup from previous runs
      2. Node lookup - find coords from other crashes at the same intersection
      3. Nominatim/OSM - free geocoding from street names (rate-limited)

    Returns path to geocoded file.
    """
    logger.info("=" * 50)
    logger.info("STAGE 3: GEOCODE (fill missing GPS coordinates)")
    logger.info("=" * 50)

    import pandas as pd
    df = pd.read_csv(validated_path, dtype=str)

    # Load persistent cache
    cache_path = str(config.output_dir / '.geocode_cache.json')
    cache_data = _load_geocode_cache(cache_path)
    node_cache = cache_data.get('nodes', {})
    nominatim_cache = cache_data.get('nominatim', {})

    # Find rows without GPS
    missing_mask = (
        (df['x'].fillna('') == '') | (df['y'].fillna('') == '') |
        (df['x'] == '0') | (df['y'] == '0')
    )
    missing_count = missing_mask.sum()

    if missing_count == 0:
        logger.info("  All records have GPS coordinates. Skipping geocode.")
        stats.stages_completed.append('geocode (skipped - 100% coverage)')
        return validated_path

    logger.info("  Records missing GPS: %d / %d (%.1f%%)",
                missing_count, len(df), missing_count / len(df) * 100)

    if config.dry_run:
        logger.info("[DRY RUN] Would attempt to geocode %d records", missing_count)
        stats.stages_completed.append('geocode (dry-run)')
        return validated_path

    geocoded_count = 0
    cache_hits = 0

    # --- Strategy 1: Persistent Node Cache ---
    # Apply cached node coordinates before building the live lookup table
    if node_cache:
        node_cache_hits = 0
        for idx in df[missing_mask].index:
            node = str(df.at[idx, 'Node']).strip()
            if node and node in node_cache:
                coords = node_cache[node]
                df.at[idx, 'x'] = str(coords[0])
                df.at[idx, 'y'] = str(coords[1])
                node_cache_hits += 1
                geocoded_count += 1
                cache_hits += 1
        if node_cache_hits:
            logger.info("  Node cache: %d records resolved from cache", node_cache_hits)

        # Refresh missing mask after cache hits
        missing_mask = (
            (df['x'].fillna('') == '') | (df['y'].fillna('') == '') |
            (df['x'] == '0') | (df['y'] == '0')
        )

    # --- Strategy 2: Live Node Lookup ---
    # Build lookup table from rows that HAVE coordinates at known intersections
    node_coords = {}
    has_coords = ~missing_mask
    for idx, row in df[has_coords].iterrows():
        node = str(row.get('Node', '')).strip()
        if node:
            x = str(row.get('x', '')).strip()
            y = str(row.get('y', '')).strip()
            if x and y:
                try:
                    coords = (float(x), float(y))
                    node_coords[node] = coords
                    # Add to persistent cache
                    node_cache[node] = list(coords)
                except ValueError:
                    pass

    logger.info("  Node lookup table: %d known intersections", len(node_coords))

    node_hits = 0
    for idx in df[missing_mask].index:
        node = str(df.at[idx, 'Node']).strip()
        if node and node in node_coords:
            lon, lat = node_coords[node]
            df.at[idx, 'x'] = str(lon)
            df.at[idx, 'y'] = str(lat)
            node_hits += 1
            geocoded_count += 1

    if node_hits:
        logger.info("  Node lookup: geocoded %d records", node_hits)

    # --- Strategy 3: Nominatim with persistent cache ---
    # Re-check missing after node lookup
    missing_mask = (
        (df['x'].fillna('') == '') | (df['y'].fillna('') == '') |
        (df['x'] == '0') | (df['y'] == '0')
    )
    still_missing = missing_mask.sum()

    if still_missing > 0:
        # Load state config for jurisdiction name
        jurisdiction_name = config.jurisdiction.replace('_', ' ').title()
        state_name = stats.detected_state or config.state or ''

        # Map state key to state name for geocoding
        state_names = {'colorado': 'Colorado', 'virginia': 'Virginia'}
        state_full = state_names.get(state_name, state_name.title())

        # Check how many need API calls vs cache hits
        api_needed = 0
        for idx in df[missing_mask].index:
            loc1 = str(df.at[idx, '_co_location1'] if '_co_location1' in df.columns
                       else df.at[idx, 'RTE Name']).strip()
            loc2 = str(df.at[idx, '_co_location2'] if '_co_location2' in df.columns
                       else '').strip()
            if not loc1:
                continue
            if loc2 and loc2 not in ('', 'UNKNOWN LOC', 'Unknown'):
                query = f"{loc1} and {loc2}, {jurisdiction_name}, {state_full}"
            else:
                query = f"{loc1}, {jurisdiction_name}, {state_full}"
            if query not in nominatim_cache:
                api_needed += 1

        logger.info("  Nominatim: %d records to resolve (%d cached, %d need API)",
                     still_missing, still_missing - api_needed, api_needed)

        nominatim_hits = 0
        api_calls = 0

        try:
            import urllib.request
            import urllib.parse

            for idx in df[missing_mask].index:
                loc1 = str(df.at[idx, '_co_location1'] if '_co_location1' in df.columns
                           else df.at[idx, 'RTE Name']).strip()
                loc2 = str(df.at[idx, '_co_location2'] if '_co_location2' in df.columns
                           else '').strip()

                if not loc1:
                    continue

                # Build search query
                if loc2 and loc2 not in ('', 'UNKNOWN LOC', 'Unknown'):
                    query = f"{loc1} and {loc2}, {jurisdiction_name}, {state_full}"
                else:
                    query = f"{loc1}, {jurisdiction_name}, {state_full}"

                # Check persistent cache first
                if query in nominatim_cache:
                    result = nominatim_cache[query]
                    if result:
                        cache_hits += 1
                else:
                    # Rate limit: 1 request per second (Nominatim policy)
                    time.sleep(1.0)
                    api_calls += 1
                    try:
                        encoded = urllib.parse.urlencode({
                            'q': query,
                            'format': 'json',
                            'limit': 1,
                            'countrycodes': 'us'
                        })
                        url = f"https://nominatim.openstreetmap.org/search?{encoded}"
                        req = urllib.request.Request(url, headers={
                            'User-Agent': 'CrashLens/1.0 (crash-analysis-tool)'
                        })
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            data = json.loads(resp.read().decode())
                            if data and len(data) > 0:
                                result = [float(data[0]['lon']), float(data[0]['lat'])]
                            else:
                                result = None
                    except Exception as e:
                        logger.debug("  Nominatim error for '%s': %s", query, e)
                        result = None

                    # Save to persistent cache (including None for failed lookups)
                    nominatim_cache[query] = result

                if result:
                    lon, lat = result
                    df.at[idx, 'x'] = str(lon)
                    df.at[idx, 'y'] = str(lat)
                    nominatim_hits += 1
                    geocoded_count += 1

                # Progress logging every 25 API calls
                if api_calls > 0 and api_calls % 25 == 0:
                    logger.info("  Nominatim progress: %d API calls, %d geocoded so far...",
                                api_calls, nominatim_hits)

            if nominatim_hits:
                logger.info("  Nominatim: geocoded %d records (%d API calls, %d from cache)",
                            nominatim_hits, api_calls, cache_hits)

        except ImportError:
            logger.warning("  urllib not available for Nominatim geocoding")

    # Save persistent cache
    cache_data['nodes'] = node_cache
    cache_data['nominatim'] = nominatim_cache
    _save_geocode_cache(cache_path, cache_data)

    # Save geocoded data
    df.to_csv(validated_path, index=False)

    # Update stats
    final_missing = (
        (df['x'].fillna('') == '') | (df['y'].fillna('') == '') |
        (df['x'] == '0') | (df['y'] == '0')
    ).sum()

    stats.rows_geocoded = geocoded_count
    stats.rows_with_gps = len(df) - final_missing
    stats.rows_without_gps = final_missing
    stats.stages_completed.append('geocode')

    logger.info("  Total geocoded: %d / %d missing (cache hits: %d)",
                geocoded_count, missing_count, cache_hits)
    logger.info("  Remaining without GPS: %d (%.1f%%)",
                final_missing, final_missing / max(len(df), 1) * 100)

    return validated_path


# ============================================================
# STAGE 5: SPLIT
# ============================================================

def stage_split(validated_path: str, config: PipelineConfig, stats: PipelineStats) -> List[str]:
    """
    Split validated data into road-type filter files.

    Creates three files:
      - {jurisdiction}_all_roads.csv      : Complete dataset
      - {jurisdiction}_county_roads.csv   : County/city-maintained roads only
      - {jurisdiction}_no_interstate.csv  : Everything except interstates

    The split logic is state-aware:
      - Colorado: Agency Id = "DSO" for county roads, System Code for interstate
      - Virginia: SYSTEM column for road classification

    Returns list of created file paths.
    """
    logger.info("=" * 50)
    logger.info("STAGE 4: SPLIT (road-type filter files)")
    logger.info("=" * 50)

    state_key = stats.detected_state or config.state or 'virginia'
    jurisdiction = config.jurisdiction

    # Load state config for split logic
    state_config_path = STATES_DIR / STATE_SIGNATURES_DIRS.get(state_key, state_key) / 'config.json'
    road_systems = {}
    if state_config_path.exists():
        with open(state_config_path, 'r') as f:
            state_cfg = json.load(f)
        road_systems = state_cfg.get('roadSystems', {}).get('values', {})

    if config.dry_run:
        logger.info("[DRY RUN] Would split %s into 3 filter files", validated_path)
        stats.stages_completed.append('split (dry-run)')
        return []

    # Read validated data
    with open(validated_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        all_rows = list(reader)

    logger.info("  Source: %s (%d rows)", validated_path, len(all_rows))

    # --- Merge with existing output files if --merge-existing ---
    if config.merge_existing:
        all_roads_path = str(config.output_dir / f"{jurisdiction}_all_roads.csv")
        if os.path.exists(all_roads_path):
            logger.info("  MERGE MODE: Merging with existing %s", all_roads_path)
            with open(all_roads_path, 'r', encoding='utf-8') as f:
                existing_reader = csv.DictReader(f)
                existing_rows = list(existing_reader)
                # Use existing headers if new headers are a subset
                if existing_reader.fieldnames:
                    # Merge headers: keep all unique columns
                    existing_headers = existing_reader.fieldnames
                    merged_headers = list(existing_headers)
                    for h in headers:
                        if h not in merged_headers:
                            merged_headers.append(h)
                    headers = merged_headers

            existing_count = len(existing_rows)
            new_count = len(all_rows)
            logger.info("    Existing: %d rows, New: %d rows", existing_count, new_count)

            # Combine: existing first (they take priority in dedup)
            combined_rows = existing_rows + all_rows

            # Deduplicate by Document Nbr (primary key)
            doc_col = None
            for col in ['Document Nbr', 'Document Number', 'CrashID', 'CRASH_ID']:
                if col in headers:
                    doc_col = col
                    break

            if doc_col:
                seen_docs = set()
                deduped_rows = []
                for row in combined_rows:
                    doc_val = (row.get(doc_col, '') or '').strip()
                    if doc_val and doc_val != 'nan':
                        if doc_val in seen_docs:
                            continue
                        seen_docs.add(doc_val)
                    deduped_rows.append(row)
                combined_rows = deduped_rows

            # Secondary dedup: Crash Date + coords + Collision Type
            dedup_fields = ['Crash Date', 'x', 'y', 'Collision Type']
            if all(f in headers for f in dedup_fields):
                seen_geo = set()
                deduped_rows = []
                for row in combined_rows:
                    x_val = (row.get('x', '') or '').strip()
                    y_val = (row.get('y', '') or '').strip()
                    date_val = (row.get('Crash Date', '') or '').strip()
                    coll_val = (row.get('Collision Type', '') or '').strip()
                    if x_val and y_val and x_val != '0' and y_val != '0' and date_val and coll_val:
                        try:
                            geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                        except (ValueError, TypeError):
                            geo_key = None
                        if geo_key:
                            if geo_key in seen_geo:
                                continue
                            seen_geo.add(geo_key)
                    deduped_rows.append(row)
                combined_rows = deduped_rows

            duplicates_removed = (existing_count + new_count) - len(combined_rows)
            net_new = len(combined_rows) - existing_count
            logger.info("    Merged: %d total (%d net new, %d duplicates removed)",
                        len(combined_rows), net_new, duplicates_removed)
            all_rows = combined_rows
        else:
            logger.info("  MERGE MODE: No existing file found at %s, creating new", all_roads_path)

    created_files = []

    # --- File 1: All Roads ---
    all_roads_path = str(config.output_dir / f"{jurisdiction}_all_roads.csv")
    _write_split_csv(all_roads_path, headers, all_rows, "All Roads (complete dataset)")
    created_files.append(all_roads_path)
    stats.files_created.append(all_roads_path)

    # --- File 2: County Roads Only ---
    # Config-driven split logic: reads splitConfig from state config
    split_config = state_cfg.get('roadSystems', {}).get('splitConfig', {}) if state_config_path.exists() else {}
    county_config = split_config.get('countyRoads', {})

    if county_config.get('method') == 'agency_id':
        # Agency-based filtering (e.g., Colorado): match agency column against jurisdiction-specific IDs
        agency_col = county_config.get('column', '_co_agency_id')
        agency_map = county_config.get('agencyMap', {})
        allowed_agencies = set(agency_map.get(jurisdiction, []))
        if not allowed_agencies:
            logger.warning("  No agency IDs configured for jurisdiction '%s' in splitConfig.countyRoads.agencyMap", jurisdiction)
        county_rows = [r for r in all_rows
                       if r.get(agency_col, '').strip() in allowed_agencies]
    elif county_config.get('method') == 'system_column':
        # System column filtering (e.g., Virginia): match SYSTEM column against include values
        sys_col = county_config.get('column', 'SYSTEM')
        include_values = set(county_config.get('includeValues', []))
        county_rows = [r for r in all_rows if r.get(sys_col, '').strip() in include_values]
    else:
        # Fallback: legacy hardcoded logic for backward compatibility
        if state_key == 'colorado':
            county_rows = [r for r in all_rows
                           if r.get('_co_agency_id', '').strip() in _get_county_agencies(config, state_key)]
        else:
            county_systems = {'Non-DOT secondary', 'Non-DOT'}
            county_rows = [r for r in all_rows if r.get('SYSTEM', '').strip() in county_systems]

    county_path = str(config.output_dir / f"{jurisdiction}_county_roads.csv")
    _write_split_csv(county_path, headers, county_rows,
                     f"County Roads Only ({len(county_rows)} of {len(all_rows)})")
    created_files.append(county_path)
    stats.files_created.append(county_path)

    # --- File 3: No Interstate ---
    interstate_config = split_config.get('interstateExclusion', {})

    if interstate_config.get('method') in ('column_value', 'system_column'):
        # Config-driven interstate exclusion
        int_col = interstate_config.get('column', 'SYSTEM')
        exclude_values = set(interstate_config.get('excludeValues', []))
        no_interstate = [r for r in all_rows if r.get(int_col, '').strip() not in exclude_values]
    else:
        # Fallback: legacy hardcoded logic for backward compatibility
        if state_key == 'colorado':
            no_interstate = [r for r in all_rows
                             if r.get('_co_system_code', '').strip() != 'Interstate Highway']
        else:
            no_interstate = [r for r in all_rows if r.get('SYSTEM', '').strip() not in ('Interstate', 'DOT Interstate')]

    no_int_path = str(config.output_dir / f"{jurisdiction}_no_interstate.csv")
    _write_split_csv(no_int_path, headers, no_interstate,
                     f"All Roads No Interstate ({len(no_interstate)} of {len(all_rows)})")
    created_files.append(no_int_path)
    stats.files_created.append(no_int_path)

    stats.stages_completed.append('split')
    return created_files


def _get_county_agencies(config: PipelineConfig, state_key: str) -> set:
    """Get the agency IDs that represent county-maintained roads for a state."""
    if state_key == 'colorado':
        # DSO = Douglas County Sheriff's Office
        # Could be extended per-jurisdiction via config
        agency_map = {
            'douglas': {'DSO'},
            'arapahoe': {'ASO'},
            'jefferson': {'JSO'},
            'elpaso': {'EPSO'},
            'denver': {'DPD'},
            'adams': {'ACSO'},
        }
        return agency_map.get(config.jurisdiction, {'DSO'})
    return set()


def _write_split_csv(path: str, headers: list, rows: list, description: str):
    """Write a filtered CSV and log summary."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    size_mb = os.path.getsize(path) / (1024 * 1024)
    logger.info("  Created: %s", path)
    logger.info("    Filter: %s", description)
    logger.info("    Rows: %d | Size: %.1f MB", len(rows), size_mb)


# ============================================================
# MAIN
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='CRASH LENS - Master Data Processing Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline (auto-detect state):
  python process_crash_data.py -i data/CDOT/Douglas_County.csv -j douglas

  # Explicit state:
  python process_crash_data.py -i data/CDOT/Douglas_County.csv -s colorado -j douglas

  # Merge multiple year files:
  python process_crash_data.py -i "data/CDOT/202*.csv" -j douglas --merge

  # Merge new data with existing validated output (append, deduplicate):
  python process_crash_data.py -i data/CDOT/new_crashes.csv -j douglas --merge-existing

  # Convert only (no validation/geocode/split):
  python process_crash_data.py -i data/CDOT/Douglas_County.csv -j douglas --convert-only

  # Skip geocoding (fast):
  python process_crash_data.py -i data/CDOT/Douglas_County.csv -j douglas --skip-geocode

  # Dry run (preview):
  python process_crash_data.py -i data/CDOT/Douglas_County.csv -j douglas --dry-run

Supported states: auto-discovered from states/ directory (currently colorado, virginia)
        """
    )

    parser.add_argument('-i', '--input', nargs='+', required=True,
                        help='Input CSV file(s) or glob pattern(s)')
    parser.add_argument('-s', '--state', type=str, default=None,
                        help='State key (auto-detected if omitted). Auto-discovered from states/ directory.')
    parser.add_argument('-j', '--jurisdiction', type=str, required=True,
                        help='Jurisdiction name (e.g., douglas, henrico)')
    parser.add_argument('-o', '--output-dir', type=str, default=None,
                        help='Output directory (default: same as input)')
    parser.add_argument('--merge', action='store_true',
                        help='Merge multiple input files before processing')
    parser.add_argument('--merge-existing', action='store_true',
                        help='Merge new data with existing validated output files instead of replacing them')
    parser.add_argument('--convert-only', action='store_true',
                        help='Only run conversion stage (no validation/geocode/split)')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation stage')
    parser.add_argument('--skip-geocode', action='store_true',
                        help='Skip geocoding stage')
    parser.add_argument('--skip-split', action='store_true',
                        help='Skip split stage')
    parser.add_argument('--full-validation', action='store_true',
                        help='Force full validation (ignore incremental state)')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Preview pipeline without making changes')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose/debug logging')

    return parser.parse_args()


def main():
    args = parse_args()
    config = PipelineConfig(args)
    stats = PipelineStats()

    if config.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("  CRASH LENS - Data Processing Pipeline v1.0")
    logger.info("=" * 60)
    logger.info("  Input: %s", config.input_files)
    logger.info("  State: %s", config.state or '(auto-detect)')
    logger.info("  Jurisdiction: %s", config.jurisdiction)
    logger.info("  Output dir: %s", config.output_dir)
    if config.dry_run:
        logger.info("  *** DRY RUN MODE ***")
    if config.merge_existing:
        logger.info("  *** MERGE-EXISTING MODE: Will merge with existing validated output ***")
    logger.info("")

    # Ensure output directory exists
    config.output_dir.mkdir(parents=True, exist_ok=True)

    # --- Stage 0: Merge (if multiple files) ---
    if len(config.input_files) > 1 or config.merge:
        working_file = stage_merge(config, stats)
    else:
        working_file = config.input_files[0]
        if not os.path.exists(working_file):
            logger.error("Input file not found: %s", working_file)
            sys.exit(1)
        # Count input rows
        with open(working_file, 'r', encoding='utf-8-sig') as f:
            stats.total_input_rows = sum(1 for _ in f) - 1  # minus header

    # --- Stage 1: Convert ---
    working_file = stage_convert(working_file, config, stats)

    if config.convert_only:
        logger.info(stats.summary())
        return 0

    # --- Stage 2: Validate ---
    if not config.skip_validation:
        working_file = stage_validate(working_file, config, stats)
    else:
        logger.info("STAGE 2: VALIDATE (skipped)")
        stats.stages_completed.append('validate (skipped)')

    # --- Stage 3: Geocode ---
    if not config.skip_geocode:
        working_file = stage_geocode(working_file, config, stats)
    else:
        logger.info("STAGE 3: GEOCODE (skipped)")
        stats.stages_completed.append('geocode (skipped)')

    # --- Stage 4: Split ---
    if not config.skip_split:
        stage_split(working_file, config, stats)
    else:
        logger.info("STAGE 4: SPLIT (skipped)")
        stats.stages_completed.append('split (skipped)')

    # --- Summary ---
    logger.info(stats.summary())

    return 0


if __name__ == '__main__':
    sys.exit(main())
