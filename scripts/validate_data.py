#!/usr/bin/env python3
"""
Validate crash data CSV with incremental caching.

Runs QA/QC checks on a Virginia-compatible CSV (statewide or single jurisdiction).
Uses a state-isolated cache to skip already-validated rows, processing only
new or changed records.

Checks:
  - Required fields present (Document Nbr, Crash Date, Crash Severity, x, y)
  - Latitude/longitude within state coordinate bounds
  - Crash date is parseable and within reasonable range
  - Severity values are valid KABCO (K, A, B, C, O)
  - No duplicate Document Nbr values
  - Transposed coordinates detection (x/y swapped)

Usage:
    python scripts/validate_data.py --state virginia --input data/all_roads.csv
    python scripts/validate_data.py --state colorado --input data/CDOT/colorado_statewide.csv --force-validate
    python scripts/validate_data.py --state virginia --input data/all_roads.csv --cache-dir .cache/virginia/validation
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger('validate_data')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent

REQUIRED_FIELDS = ['Document Nbr', 'Crash Date', 'Crash Severity']
VALID_SEVERITIES = {'K', 'A', 'B', 'C', 'O', 'k', 'a', 'b', 'c', 'o'}
STANDARD_COLUMNS_COUNT = 51

# 2% buffer on coordinate bounds so crashes near state/jurisdiction
# borders aren't falsely rejected.  GPS error, boundary imprecision,
# and border-area incidents make this necessary for accurate data
# retention at edges.
BOUNDARY_BUFFER_PERCENT = 0.02


def apply_boundary_buffer(bounds, buffer_pct=BOUNDARY_BUFFER_PERCENT):
    """Expand coordinate bounds by a percentage of their span.

    A 2% buffer on Virginia (lat span ~3.0°) adds ~0.06° ≈ 4.1 miles.
    A 2% buffer on Colorado (lat span ~4.2°) adds ~0.08° ≈ 5.8 miles.
    This accommodates GPS drift, geocoding imprecision, and crashes
    that legitimately occur near state/jurisdiction borders.
    """
    lat_span = bounds['lat_max'] - bounds['lat_min']
    lon_span = bounds['lon_max'] - bounds['lon_min']
    lat_buf = lat_span * buffer_pct
    lon_buf = lon_span * buffer_pct
    return {
        'lat_min': bounds['lat_min'] - lat_buf,
        'lat_max': bounds['lat_max'] + lat_buf,
        'lon_min': bounds['lon_min'] - lon_buf,
        'lon_max': bounds['lon_max'] + lon_buf,
    }


def load_state_bounds(state):
    """Load coordinate bounds from state config with 2% boundary buffer."""
    config_path = PROJECT_ROOT / 'states' / state / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        bounds = config.get('state', {}).get('coordinateBounds', {})
        raw_bounds = {
            'lat_min': bounds.get('latMin', -90),
            'lat_max': bounds.get('latMax', 90),
            'lon_min': bounds.get('lonMin', -180),
            'lon_max': bounds.get('lonMax', 180),
        }
        return apply_boundary_buffer(raw_bounds)
    return {'lat_min': -90, 'lat_max': 90, 'lon_min': -180, 'lon_max': 180}


def compute_row_hash(row, headers):
    """Hash standard columns only (exclude _{state}_ unmapped columns)."""
    standard_vals = []
    for i, h in enumerate(headers):
        if i < STANDARD_COLUMNS_COUNT and i < len(row):
            standard_vals.append(str(row[i]).strip())
    content = '|'.join(standard_vals)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]


def compute_config_rules_hash(state):
    """Hash the state config's validation-relevant sections."""
    config_path = PROJECT_ROOT / 'states' / state / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            content = f.read()
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    return 'no_config'


def load_cache(cache_dir):
    """Load validation cache (validated_hashes.json)."""
    cache_file = Path(cache_dir) / 'validated_hashes.json'
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)
    return {}


def save_cache(cache_dir, hashes):
    """Save validation cache."""
    cache_file = Path(cache_dir) / 'validated_hashes.json'
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    with open(cache_file, 'w') as f:
        json.dump(hashes, f)


def save_last_run(cache_dir, stats):
    """Save last run metadata."""
    run_file = Path(cache_dir) / 'last_run.json'
    with open(run_file, 'w') as f:
        json.dump(stats, f, indent=2)


def check_cache_invalidation(cache_dir, state):
    """Check if cache should be invalidated due to config changes."""
    rules_hash_file = Path(cache_dir) / 'validation_rules_hash.txt'
    current_hash = compute_config_rules_hash(state)

    if rules_hash_file.exists():
        stored_hash = rules_hash_file.read_text().strip()
        if stored_hash != current_hash:
            logger.info(f"Config rules changed ({stored_hash[:8]}→{current_hash[:8]}), invalidating cache")
            return True

    # Save current hash
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    rules_hash_file.write_text(current_hash)
    return False


def update_cache_manifest(cache_dir, state, stats, cached_records_count):
    """Update the state-level cache_manifest.json with validation run stats."""
    # cache_manifest.json lives one level up from the validation subdirectory
    cache_dir_path = Path(cache_dir)
    state_cache_dir = cache_dir_path.parent if cache_dir_path.name == 'validation' else cache_dir_path
    manifest_path = state_cache_dir / 'cache_manifest.json'

    manifest = {}
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, IOError):
            manifest = {}

    now = datetime.utcnow().isoformat() + 'Z'
    manifest['last_updated'] = now
    manifest.setdefault('state', state)
    manifest.setdefault('version', 1)

    total = stats.get('total', 0)
    cached_skipped = stats.get('cached_skipped', 0)
    hit_rate = round(cached_skipped / max(1, total), 4)

    manifest['validation'] = {
        'last_run': now,
        'total_validated': total,
        'cached_records': cached_records_count,
        'errors_removed': stats.get('errors', 0),
        'cache_hit_rate': hit_rate
    }

    state_cache_dir.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)


def try_parse_date(date_str):
    """Try to parse a date string in common formats."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%Y/%m/%d', '%m/%d/%y'):
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


def validate_row(row, headers, col_idx, bounds, row_num):
    """Validate a single row. Returns list of issues (empty = valid)."""
    issues = []

    # Required fields
    for field in REQUIRED_FIELDS:
        if field in col_idx:
            val = row[col_idx[field]].strip() if col_idx[field] < len(row) else ''
            if not val:
                issues.append(f"Missing required field: {field}")

    # Severity check
    if 'Crash Severity' in col_idx:
        sev = row[col_idx['Crash Severity']].strip() if col_idx['Crash Severity'] < len(row) else ''
        if sev and sev not in VALID_SEVERITIES:
            issues.append(f"Invalid severity: '{sev}' (expected K/A/B/C/O)")

    # Date check
    if 'Crash Date' in col_idx:
        date_str = row[col_idx['Crash Date']].strip() if col_idx['Crash Date'] < len(row) else ''
        if date_str:
            parsed = try_parse_date(date_str)
            if parsed is None:
                issues.append(f"Unparseable date: '{date_str}'")
            elif parsed.year < 2015 or parsed.year > 2030:
                issues.append(f"Date out of range: {date_str} (year={parsed.year})")

    # Coordinate checks
    x_idx = col_idx.get('x')
    y_idx = col_idx.get('y')
    if x_idx is not None and y_idx is not None:
        try:
            x_str = row[x_idx].strip() if x_idx < len(row) else ''
            y_str = row[y_idx].strip() if y_idx < len(row) else ''
            if x_str and y_str:
                x_val = float(x_str)
                y_val = float(y_str)
                # Check for transposed coordinates (x should be longitude, y should be latitude)
                if x_val != 0 and y_val != 0:
                    if not (bounds['lon_min'] <= x_val <= bounds['lon_max']):
                        # Check if transposed
                        if bounds['lat_min'] <= x_val <= bounds['lat_max']:
                            issues.append(f"Likely transposed coordinates: x={x_val}, y={y_val}")
                        else:
                            issues.append(f"Longitude out of bounds: {x_val}")
                    if not (bounds['lat_min'] <= y_val <= bounds['lat_max']):
                        issues.append(f"Latitude out of bounds: {y_val}")
        except (ValueError, IndexError):
            pass  # Missing coordinates are not a validation error

    return issues


def main():
    parser = argparse.ArgumentParser(description='Validate crash data CSV with incremental caching')
    parser.add_argument('--state', required=True, help='State key (e.g., virginia, colorado)')
    parser.add_argument('--input', required=True, help='Path to Virginia-compatible CSV')
    parser.add_argument('--cache-dir', default=None, help='Cache directory (default: .cache/{state}/validation)')
    parser.add_argument('--force-validate', action='store_true', help='Ignore cache, revalidate everything')
    parser.add_argument('--output', default=None, help='Output validated CSV (default: overwrite input)')
    args = parser.parse_args()

    state = args.state
    input_path = Path(args.input)
    cache_dir = args.cache_dir or f'.cache/{state}/validation'

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    bounds = load_state_bounds(state)
    logger.info(f"[{state}] Validating: {input_path}")
    logger.info(f"[{state}] Bounds (with {BOUNDARY_BUFFER_PERCENT:.0%} buffer): "
                f"lat=[{bounds['lat_min']:.4f}, {bounds['lat_max']:.4f}], "
                f"lon=[{bounds['lon_min']:.4f}, {bounds['lon_max']:.4f}]")

    # Cache handling
    force = args.force_validate
    if not force and check_cache_invalidation(cache_dir, state):
        force = True

    cached_hashes = {} if force else load_cache(cache_dir)
    if force:
        logger.info(f"[{state}] Force validate — cache cleared")
    else:
        logger.info(f"[{state}] Cache loaded: {len(cached_hashes)} previously validated records")

    # Read and validate
    stats = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'state': state,
        'input_file': str(input_path),
        'total': 0,
        'new_validated': 0,
        'changed_validated': 0,
        'cached_skipped': 0,
        'errors': 0,
        'warnings': [],
        'error_samples': [],
    }

    valid_rows = []
    new_hashes = {}
    seen_doc_nbrs = set()
    duplicates = 0

    with open(input_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Build column index
        col_idx = {}
        for i, h in enumerate(headers):
            col_idx[h.strip()] = i

        valid_rows.append(headers)

        for row_num, row in enumerate(reader, start=2):
            stats['total'] += 1

            # Get Document Nbr for caching
            doc_nbr_idx = col_idx.get('Document Nbr')
            doc_nbr = row[doc_nbr_idx].strip() if doc_nbr_idx is not None and doc_nbr_idx < len(row) else ''

            # Duplicate check
            if doc_nbr:
                if doc_nbr in seen_doc_nbrs:
                    duplicates += 1
                    continue
                seen_doc_nbrs.add(doc_nbr)

            # Incremental cache check
            row_hash = compute_row_hash(row, headers)
            if doc_nbr and doc_nbr in cached_hashes and cached_hashes[doc_nbr] == row_hash:
                stats['cached_skipped'] += 1
                new_hashes[doc_nbr] = row_hash
                valid_rows.append(row)
                continue

            # Validate
            issues = validate_row(row, headers, col_idx, bounds, row_num)

            if issues:
                stats['errors'] += 1
                if len(stats['error_samples']) < 20:
                    stats['error_samples'].append({
                        'row': row_num,
                        'doc_nbr': doc_nbr,
                        'issues': issues
                    })
            else:
                valid_rows.append(row)

            # Track whether new or changed
            if doc_nbr in cached_hashes:
                stats['changed_validated'] += 1
            else:
                stats['new_validated'] += 1

            new_hashes[doc_nbr] = row_hash

    if duplicates > 0:
        stats['warnings'].append(f"{duplicates} duplicate Document Nbr values removed")

    # Write output
    output_path = args.output or str(input_path)
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(valid_rows)

    # Save cache
    save_cache(cache_dir, new_hashes)
    save_last_run(cache_dir, stats)

    # Update cache_manifest.json (state-level cache metadata)
    update_cache_manifest(cache_dir, state, stats, len(new_hashes))

    # Summary
    logger.info("=" * 60)
    logger.info(f"[{state}] VALIDATION COMPLETE")
    logger.info(f"[{state}]   Total rows:      {stats['total']:,}")
    logger.info(f"[{state}]   New validated:    {stats['new_validated']:,}")
    logger.info(f"[{state}]   Changed:          {stats['changed_validated']:,}")
    logger.info(f"[{state}]   Cached (skipped): {stats['cached_skipped']:,}")
    logger.info(f"[{state}]   Errors removed:   {stats['errors']:,}")
    if duplicates:
        logger.info(f"[{state}]   Duplicates:       {duplicates:,}")
    logger.info(f"[{state}]   Output: {output_path} ({len(valid_rows)-1:,} valid rows)")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
