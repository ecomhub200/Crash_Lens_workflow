"""
CRASH LENS — State Normalizer Template (Enhanced v2.0)
=======================================================
Copy this template to: states/{state}/{state_abbr}_normalize.py
Example:               states/delaware/de_normalize.py

This single file handles the FULL pipeline:
  Phase 1: Column mapping (source → 69 golden standard)
  Phase 2: State-specific transforms (datetime, severity, Y/N)
  Phase 3: Composite crash ID generation
  Phase 4: Geography resolution (via geo_resolver.py)
  Phase 5: EPDO scoring
  Phase 6: Validation & auto-correction (ported from crash-data-validator v13)
  Phase 7: Jurisdiction ranking (24 columns)

Only TWO files needed per state:
  - geo_resolver.py          (shared, at repo root)
  - {abbr}_normalize.py      (this template, per state)

Usage:
  python de_normalize.py --csv delaware_crashes.csv --output de_normalized.csv
"""

import csv
import json
import math
import os
import sys
import re
import logging
import argparse
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Optional, Any

# ── Import the shared geography resolver ──
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from geo_resolver import GeoResolver, resolve_geography

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
logger = logging.getLogger('crashlens.normalize')

# ═══════════════════════════════════════════════════════════════
#  STATE CONFIGURATION — EDIT THESE FOR YOUR STATE
# ═══════════════════════════════════════════════════════════════

STATE_FIPS = '10'           # 2-digit state FIPS
STATE_ABBR = 'DE'           # 2-letter abbreviation
STATE_NAME = 'Delaware'
DOT_NAME = 'DelDOT'

# Path configuration (relative to this script's location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.join(SCRIPT_DIR, '..', '..')
GEO_DIR = os.path.join(REPO_ROOT, 'states', 'geography')
HIERARCHY_PATH = os.path.join(SCRIPT_DIR, 'hierarchy.json')

# EPDO weights (use state-specific or FHWA 2025 default)
EPDO_WEIGHTS = {'K': 883, 'A': 94, 'B': 21, 'C': 11, 'O': 1}  # FHWA 2025

# ═══════════════════════════════════════════════════════════════
#  CRASHLENS GOLDEN STANDARD — 69 COLUMNS
# ═══════════════════════════════════════════════════════════════

GOLDEN_COLUMNS = [
    'OBJECTID', 'Document Nbr', 'Crash Year', 'Crash Date', 'Crash Military Time',
    'Crash Severity', 'K_People', 'A_People', 'B_People', 'C_People',
    'Persons Injured', 'Pedestrians Killed', 'Pedestrians Injured', 'Vehicle Count',
    'Collision Type', 'Weather Condition', 'Light Condition', 'Roadway Surface Condition',
    'Relation To Roadway', 'Roadway Alignment', 'Roadway Surface Type', 'Roadway Defect',
    'Roadway Description', 'Intersection Type', 'Traffic Control Type', 'Traffic Control Status',
    'Work Zone Related', 'Work Zone Location', 'Work Zone Type', 'School Zone',
    'First Harmful Event', 'First Harmful Event Loc',
    'Alcohol?', 'Animal Related?', 'Unrestrained?', 'Bike?', 'Distracted?', 'Drowsy?',
    'Drug Related?', 'Guardrail Related?', 'Hitrun?', 'Lgtruck?', 'Motorcycle?', 'Pedestrian?',
    'Speed?', 'Max Speed Diff', 'RoadDeparture Type', 'Intersection Analysis',
    'Senior?', 'Young?', 'Mainline?', 'Night?',
    'DOT District', 'Juris Code', 'Physical Juris Name', 'Functional Class',
    'Facility Type', 'Area Type', 'SYSTEM', 'VSP', 'Ownership',
    'Planning District', 'MPO Name', 'RTE Name', 'RNS MP', 'Node', 'Node Offset (ft)',
    'x', 'y',
]

ENRICHMENT_COLUMNS = ['FIPS', 'Place FIPS', 'EPDO_Score']

RANKING_SCOPES = ['District', 'Juris', 'PlanningDistrict', 'MPO']
RANKING_METRICS = [
    'total_crash', 'total_ped_crash', 'total_bike_crash',
    'total_fatal', 'total_fatal_serious_injury', 'total_epdo',
]

# ═══════════════════════════════════════════════════════════════
#  PHASE 1: COLUMN MAPPING
#  Map source columns → CrashLens standard columns
# ═══════════════════════════════════════════════════════════════

# Direct renames: source_column → target_column
# ── EDIT THIS FOR YOUR STATE ──
COLUMN_MAP = {
    'CRASH DATETIME':                    'Crash Date',      # Post-processed in Phase 2
    'LATITUDE':                          'y',
    'LONGITUDE':                         'x',
    'CRASH CLASSIFICATION DESCRIPTION':  'Crash Severity',  # Post-processed in Phase 2
    'COUNTY NAME':                       'Physical Juris Name',
    'PEDESTRIAN INVOLVED':               'Pedestrian?',
    'BICYCLED INVOLVED':                 'Bike?',
    'ALCOHOL INVOLVED':                  'Alcohol?',
    'DRUG INVOLVED':                     'Drug Related?',
    'SEATBELT USED':                     'Unrestrained?',
    'MOTORCYCLE INVOLVED':               'Motorcycle?',
    'WEATHER 1 DESCRIPTION':             'Weather Condition',
    'LIGHTING CONDITION DESCRIPTION':    'Light Condition',
    'ROAD SURFACE DESCRIPTION':          'Roadway Surface Condition',
    'MANNER OF IMPACT DESCRIPTION':      'Collision Type',
    'SCHOOL BUS INVOLVED DESCRIPTION':   'School Zone',
    'WORK ZONE':                         'Work Zone Related',
    'YEAR':                              'Crash Year',
    'VEHICLE COUNT':                     'Vehicle Count',
    # Add more mappings as needed...
}


# ═══════════════════════════════════════════════════════════════
#  PHASE 2: STATE-SPECIFIC TRANSFORMS
# ═══════════════════════════════════════════════════════════════

def parse_datetime(raw: str) -> Dict[str, str]:
    """
    Parse state-specific datetime format into standard fields.
    ── EDIT THIS FOR YOUR STATE ──
    Delaware format: "2015 Jul 17 03:15:00 PM"
    """
    if not raw or not raw.strip():
        return {'date': '', 'time': '', 'year': ''}
    months = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
    }
    parts = raw.strip().split()
    if len(parts) < 4:
        return {'date': raw, 'time': '', 'year': ''}
    year = parts[0]
    mon = months.get(parts[1], '01')
    day = parts[2]
    time_parts = (parts[3] or '').split(':')
    hour = int(time_parts[0]) if time_parts else 0
    minute = time_parts[1] if len(time_parts) > 1 else '00'
    ampm = (parts[4] if len(parts) > 4 else '').upper()
    if ampm == 'PM' and hour < 12:
        hour += 12
    if ampm == 'AM' and hour == 12:
        hour = 0
    return {
        'date': f'{int(mon)}/{int(day)}/{year}',
        'time': f'{hour:02d}{minute}',
        'year': year,
    }


def map_severity(description: str, code: str = '') -> str:
    """
    Map state severity to KABCO.
    ── EDIT THIS FOR YOUR STATE ──
    """
    d = (description or '').lower().strip()
    c = (code or '').strip()
    if 'fatal' in d or c == '06':
        return 'K'
    if 'personal injury' in d or 'injury crash' in d or c == '03':
        return 'A'
    if 'property damage' in d or c == '02':
        return 'O'
    return 'O'


def normalize_yn(val: str) -> str:
    """Normalize Y/N/Yes/No/TRUE/FALSE to Yes/No."""
    v = (val or '').strip().upper()
    if v in ('Y', 'YES', '1', 'TRUE', 'T'):
        return 'Yes'
    if v in ('N', 'NO', '0', 'FALSE', 'F'):
        return 'No'
    return 'No'


def apply_state_transforms(row: Dict[str, str], source_row: Dict[str, str]) -> Dict[str, str]:
    """
    Apply all state-specific post-normalization transforms.
    ── EDIT THIS FOR YOUR STATE ──
    """
    # 1. Parse datetime
    raw_dt = source_row.get('CRASH DATETIME', '')
    parsed = parse_datetime(raw_dt)
    row['Crash Date'] = parsed['date']
    row['Crash Military Time'] = parsed['time']
    row['Crash Year'] = parsed['year']

    # 2. Severity mapping
    sev_desc = source_row.get('CRASH CLASSIFICATION DESCRIPTION', '')
    sev_code = source_row.get('CRASH CLASSIFICATION CODE', '')
    row['Crash Severity'] = map_severity(sev_desc, sev_code)

    # 3. Normalize Y/N fields
    yn_fields = [
        'Pedestrian?', 'Bike?', 'Alcohol?', 'Drug Related?',
        'Motorcycle?', 'Work Zone Related',
    ]
    for field in yn_fields:
        if row.get(field):
            row[field] = normalize_yn(row[field])

    # 4. Seatbelt → Unrestrained? (inverted)
    seatbelt = (source_row.get('SEATBELT USED', '') or '').strip().upper()
    if seatbelt == 'Y':
        row['Unrestrained?'] = 'No'
    elif seatbelt == 'N':
        row['Unrestrained?'] = 'Yes'

    # 5. Night detection from lighting
    light = (row.get('Light Condition', '') or '').lower()
    row['Night?'] = 'Yes' if any(k in light for k in ['dark', 'dusk', 'dawn']) else 'No'

    # 6. School Zone normalization
    sz = (row.get('School Zone', '') or '').lower()
    row['School Zone'] = 'Yes' if 'yes' in sz else 'No'

    return row


# ═══════════════════════════════════════════════════════════════
#  PHASE 3: COMPOSITE CRASH ID GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_crash_id(row: Dict[str, str], index: int) -> str:
    """Format: {StateAbbr}-{YYYYMMDD}-{HHMM}-{index:07d}"""
    date_val = row.get('Crash Date', '')
    date_parts = date_val.split('/')
    if len(date_parts) == 3:
        date_clean = f"{date_parts[2]}{date_parts[0].zfill(2)}{date_parts[1].zfill(2)}"
    else:
        date_clean = date_val.replace('/', '').replace('-', '')[:8]
    time_val = row.get('Crash Military Time', '0000')
    return f"{STATE_ABBR}-{date_clean}-{time_val}-{index:07d}"


# ═══════════════════════════════════════════════════════════════
#  PHASE 5: EPDO SCORING
# ═══════════════════════════════════════════════════════════════

def compute_epdo(severity: str) -> int:
    return EPDO_WEIGHTS.get(severity, EPDO_WEIGHTS['O'])


# ═══════════════════════════════════════════════════════════════
#  PHASE 6: VALIDATION & AUTO-CORRECTION ENGINE
#  Ported from crash-data-validator-v13 HTML tool
#  Runs entirely in-memory on the normalized rows — no network,
#  no UI, no Nominatim. Pure batch data-quality fixes.
# ═══════════════════════════════════════════════════════════════

class ValidationEngine:
    """
    Batch validation and auto-correction for normalized crash data.

    8 checks ported from the interactive crash-data-validator:
      1. Whitespace trim + text normalization
      2. Duplicate detection
      3. Missing/zero GPS detection
      4. Coordinate bounds checking
      5. KABCO severity cross-validation & auto-fix
      6. Cross-field flag consistency & auto-fix
      7. Date/time validation & auto-fix
      8. Missing Facility Type / Functional Class inference

    Plus 2 spatial auto-corrections:
      9. Route-median GPS inference (for missing coords)
     10. Route-median + nearest-neighbor bounds snap (for OOB coords)
    """

    def __init__(self, rows: List[Dict[str, str]], state_fips: str = '', state_abbr: str = ''):
        self.rows = rows
        self.state_fips = state_fips
        self.state_abbr = state_abbr
        self.issues: List[Dict] = []
        self.corrections: Dict[str, Dict] = {}
        self.stats: Dict[str, int] = {}

        # Route coordinate index (built once, used by GPS inference + bounds snap)
        self.route_medians: Dict[str, Dict] = {}
        # Spatial grid for nearest-neighbor (built once)
        self.spatial_grid: Dict[str, Dict] = {}

    # ─── Helpers ───

    def _safe_float(self, val: Any) -> float:
        try:
            f = float(val)
            return f if math.isfinite(f) else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _add_issue(self, idx: int, issue_type: str, severity: str, field: str,
                   message: str, current_value: str = '', suggested_fix: str = ''):
        self.issues.append({
            'idx': idx,
            'doc_nbr': self.rows[idx].get('Document Nbr', ''),
            'type': issue_type,
            'severity': severity,
            'field': field,
            'message': message,
            'current_value': current_value,
            'suggested_fix': suggested_fix,
            'fixed': False,
        })

    def _fix_issue(self, issue: Dict, new_value: str):
        """Apply a fix to a row and mark the issue as fixed."""
        row = self.rows[issue['idx']]
        row[issue['field']] = new_value
        issue['fixed'] = True
        issue['suggested_fix'] = new_value
        doc = issue['doc_nbr']
        if doc:
            if doc not in self.corrections:
                self.corrections[doc] = {}
            self.corrections[doc][issue['field']] = new_value

    def _get_majority(self, counter: Dict[str, int], min_count: int = 2) -> Optional[str]:
        """Return the most common value if it appears at least min_count times."""
        if not counter:
            return None
        best_val, best_count = max(counter.items(), key=lambda x: x[1])
        return best_val if best_count >= min_count else None

    # ─── Pre-computation: Build route index + spatial grid ───

    def _build_route_index(self, bounds: Optional[Dict] = None):
        """
        Build route → median coordinate lookup from good records.
        This powers both GPS inference (Check 9) and bounds snap (Check 10).
        """
        route_coords: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        for row in self.rows:
            x = self._safe_float(row.get('x'))
            y = self._safe_float(row.get('y'))
            rte = (row.get('RTE Name', '') or '').strip()
            if not rte or x == 0.0 or y == 0.0:
                continue
            # Exclude out-of-bounds records from the index if bounds are known
            if bounds:
                if (y < bounds['minLat'] - 0.05 or y > bounds['maxLat'] + 0.05 or
                    x < bounds['minLon'] - 0.05 or x > bounds['maxLon'] + 0.05):
                    continue
            route_coords[rte].append((x, y))

        for rte, coords in route_coords.items():
            coords.sort(key=lambda c: c[0])
            mid = len(coords) // 2
            self.route_medians[rte] = {
                'x': coords[mid][0],
                'y': coords[mid][1],
                'sample_size': len(coords),
                'confidence': 'high' if len(coords) >= 5 else 'medium' if len(coords) >= 2 else 'low',
            }

        logger.info(f"  Route index: {len(self.route_medians)} routes from "
                     f"{sum(len(v) for v in route_coords.values()):,} good records")

    def _build_spatial_grid(self, bounds: Optional[Dict] = None):
        """Build a simple spatial grid for nearest-neighbor lookups."""
        grid_size = 0.01  # ~1km cells
        for row in self.rows:
            x = self._safe_float(row.get('x'))
            y = self._safe_float(row.get('y'))
            if x == 0.0 or y == 0.0:
                continue
            if bounds:
                if (y < bounds['minLat'] or y > bounds['maxLat'] or
                    x < bounds['minLon'] or x > bounds['maxLon']):
                    continue
            gx = int(x / grid_size)
            gy = int(y / grid_size)
            key = f"{gx},{gy}"
            if key not in self.spatial_grid:
                self.spatial_grid[key] = {'x': 0.0, 'y': 0.0, 'n': 0}
            cell = self.spatial_grid[key]
            cell['x'] += x
            cell['y'] += y
            cell['n'] += 1

        # Average each cell
        for cell in self.spatial_grid.values():
            if cell['n'] > 0:
                cell['x'] /= cell['n']
                cell['y'] /= cell['n']

    # ═══════════════════════════════════════════════════════════
    #  CHECK 1: Whitespace trim + text normalization
    # ═══════════════════════════════════════════════════════════

    def check_whitespace(self):
        """Trim whitespace and collapse multi-spaces in all fields."""
        fixed = 0
        for row in self.rows:
            for key, val in row.items():
                if isinstance(val, str):
                    trimmed = val.strip().replace('  ', ' ')
                    if trimmed != val:
                        row[key] = trimmed
                        fixed += 1
        self.stats['whitespace_fixed'] = fixed
        if fixed > 0:
            logger.info(f"  Check 1 (whitespace): {fixed:,} fields trimmed")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 2: Duplicate detection
    # ═══════════════════════════════════════════════════════════

    def check_duplicates(self):
        """Detect duplicate records by composite key: DocNbr + Date + Time."""
        seen: Dict[str, int] = {}
        dup_count = 0
        for i, row in enumerate(self.rows):
            key = f"{row.get('Document Nbr', '')}|{row.get('Crash Date', '')}|{row.get('Crash Military Time', '')}"
            if key in seen:
                self._add_issue(i, 'duplicate', 'warning', 'Document Nbr',
                                f'Potential duplicate of row {seen[key] + 1}',
                                current_value=key, suggested_fix='Review for removal')
                dup_count += 1
            else:
                seen[key] = i
        self.stats['duplicates_found'] = dup_count
        logger.info(f"  Check 2 (duplicates): {dup_count:,} potential duplicates")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 3: Missing / zero GPS
    # ═══════════════════════════════════════════════════════════

    def check_missing_gps(self):
        """Flag records with missing or zero coordinates."""
        count = 0
        for i, row in enumerate(self.rows):
            x = (row.get('x', '') or '').strip()
            y = (row.get('y', '') or '').strip()
            if not x or not y or x == '0' or y == '0' or \
               self._safe_float(x) == 0.0 or self._safe_float(y) == 0.0:
                self._add_issue(i, 'missing_gps', 'error', 'x,y',
                                'Missing or zero GPS coordinates',
                                current_value=f'lon={x or "empty"}, lat={y or "empty"}')
                count += 1
        self.stats['missing_gps'] = count
        logger.info(f"  Check 3 (missing GPS): {count:,} records")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 4: Coordinate bounds
    # ═══════════════════════════════════════════════════════════

    def check_bounds(self, bounds: Optional[Dict] = None):
        """
        Check coordinates against county/state bounds.
        bounds = {'minLat':..., 'maxLat':..., 'minLon':..., 'maxLon':...}
        If bounds is None, auto-compute from the 5th-95th percentile of data.
        """
        if bounds is None:
            bounds = self._auto_compute_bounds()
        if not bounds:
            logger.info("  Check 4 (bounds): skipped — no bounds available")
            return

        count = 0
        for i, row in enumerate(self.rows):
            x = self._safe_float(row.get('x'))
            y = self._safe_float(row.get('y'))
            if x == 0.0 or y == 0.0:
                continue
            if (y < bounds['minLat'] or y > bounds['maxLat'] or
                x < bounds['minLon'] or x > bounds['maxLon']):
                self._add_issue(i, 'bounds', 'error', 'x,y',
                                'Coordinates outside expected bounds',
                                current_value=f'{y:.4f}, {x:.4f}')
                count += 1

        self.stats['out_of_bounds'] = count
        logger.info(f"  Check 4 (bounds): {count:,} out-of-bounds records")

    def _auto_compute_bounds(self) -> Optional[Dict]:
        """Compute bounds from 5th-95th percentile of valid coordinates."""
        lats, lons = [], []
        for row in self.rows:
            x = self._safe_float(row.get('x'))
            y = self._safe_float(row.get('y'))
            if x != 0.0 and y != 0.0:
                lats.append(y)
                lons.append(x)
        if len(lats) < 100:
            return None
        lats.sort()
        lons.sort()
        p5 = int(len(lats) * 0.02)
        p95 = int(len(lats) * 0.98)
        return {
            'minLat': lats[p5] - 0.05,
            'maxLat': lats[p95] + 0.05,
            'minLon': lons[p5] - 0.05,
            'maxLon': lons[p95] + 0.05,
        }

    # ═══════════════════════════════════════════════════════════
    #  CHECK 5: KABCO severity cross-validation
    # ═══════════════════════════════════════════════════════════

    def check_severity(self):
        """
        Validate KABCO severity against K/A/B/C people counts.
        Auto-fix: if K_People > 0 but severity != K, correct it.
        """
        mismatch = 0
        fixed = 0
        for i, row in enumerate(self.rows):
            sev = (row.get('Crash Severity', '') or '').strip()
            k = int(row.get('K_People', 0) or 0)
            a = int(row.get('A_People', 0) or 0)
            b = int(row.get('B_People', 0) or 0)
            c = int(row.get('C_People', 0) or 0)

            expected = 'O'
            if k > 0:
                expected = 'K'
            elif a > 0:
                expected = 'A'
            elif b > 0:
                expected = 'B'
            elif c > 0:
                expected = 'C'

            if sev and sev != expected and expected != 'O':
                self._add_issue(i, 'severity', 'warning', 'Crash Severity',
                                f'Severity "{sev}" but K={k},A={a},B={b},C={c} → expected "{expected}"',
                                current_value=sev, suggested_fix=expected)
                mismatch += 1

        # Auto-fix all severity mismatches
        sev_issues = [iss for iss in self.issues if iss['type'] == 'severity' and iss['suggested_fix']]
        for issue in sev_issues:
            self._fix_issue(issue, issue['suggested_fix'])
            fixed += 1

        self.stats['severity_mismatches'] = mismatch
        self.stats['severity_fixed'] = fixed
        logger.info(f"  Check 5 (severity): {mismatch:,} mismatches, {fixed:,} auto-fixed")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 6: Cross-field flag consistency
    # ═══════════════════════════════════════════════════════════

    def check_cross_field(self):
        """
        Fix inconsistent boolean flags:
          - Pedestrian? = No but Pedestrians Killed/Injured > 0
          - Bike? = No but Collision Type contains "bike" or "bicycl"
          - Non-standard Yes/No values in flag columns
        """
        fixed = 0
        for i, row in enumerate(self.rows):
            # Pedestrian flag vs counts
            ped_flag = (row.get('Pedestrian?', '') or '').strip()
            ped_k = int(row.get('Pedestrians Killed', 0) or 0)
            ped_i = int(row.get('Pedestrians Injured', 0) or 0)
            if ped_flag == 'No' and (ped_k > 0 or ped_i > 0):
                self._add_issue(i, 'cross_field', 'warning', 'Pedestrian?',
                                f'Flag=No but {ped_k} killed, {ped_i} injured',
                                current_value='No', suggested_fix='Yes')

            # Bike flag vs collision type
            bike_flag = (row.get('Bike?', '') or '').strip()
            coll_type = (row.get('Collision Type', '') or '').lower()
            if bike_flag == 'No' and ('bike' in coll_type or 'bicycl' in coll_type):
                self._add_issue(i, 'cross_field', 'warning', 'Bike?',
                                'Flag=No but collision type mentions bike',
                                current_value='No', suggested_fix='Yes')

            # Motorcycle flag vs collision type
            moto_flag = (row.get('Motorcycle?', '') or '').strip()
            if moto_flag == 'No' and 'motorcycle' in coll_type:
                self._add_issue(i, 'cross_field', 'warning', 'Motorcycle?',
                                'Flag=No but collision type mentions motorcycle',
                                current_value='No', suggested_fix='Yes')

            # Alcohol flag vs Drug Related (if both exist, check consistency with severity)
            # Non-standard flag values
            for flag_col in ['Hitrun?', 'Speed?', 'Distracted?', 'Drowsy?',
                             'Guardrail Related?', 'Animal Related?', 'Lgtruck?']:
                val = (row.get(flag_col, '') or '').strip()
                if val and val not in ('Yes', 'No', ''):
                    self._add_issue(i, 'cross_field', 'info', flag_col,
                                    f'Non-standard flag value: "{val}"',
                                    current_value=val,
                                    suggested_fix='Yes' if val.upper() in ('Y', '1', 'TRUE') else 'No')

        # Auto-fix all cross-field issues that have a suggested fix
        cross_issues = [iss for iss in self.issues
                        if iss['type'] == 'cross_field' and iss['suggested_fix'] and not iss['fixed']]
        for issue in cross_issues:
            self._fix_issue(issue, issue['suggested_fix'])
            fixed += 1

        self.stats['cross_field_fixed'] = fixed
        logger.info(f"  Check 6 (cross-field): {fixed:,} inconsistencies auto-fixed")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 7: Date / time validation
    # ═══════════════════════════════════════════════════════════

    def check_datetime(self):
        """
        Validate date format and military time range.
        Auto-fix: year mismatches between Crash Date and Crash Year.
        """
        count = 0
        for i, row in enumerate(self.rows):
            date_val = (row.get('Crash Date', '') or '').strip()
            time_val = (row.get('Crash Military Time', '') or '').strip()
            year_val = (row.get('Crash Year', '') or '').strip()

            # Validate date is parseable
            if date_val:
                parts = date_val.split('/')
                if len(parts) == 3:
                    try:
                        m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
                        if m < 1 or m > 12 or d < 1 or d > 31 or y < 1990 or y > 2030:
                            self._add_issue(i, 'date_time', 'warning', 'Crash Date',
                                            f'Date out of expected range',
                                            current_value=date_val)
                            count += 1
                        # Check year consistency
                        if year_val and str(y) != year_val:
                            self._add_issue(i, 'date_time', 'warning', 'Crash Year',
                                            f'Year mismatch: date={y}, field={year_val}',
                                            current_value=year_val, suggested_fix=str(y))
                            count += 1
                    except ValueError:
                        self._add_issue(i, 'date_time', 'error', 'Crash Date',
                                        'Unparseable date', current_value=date_val)
                        count += 1

            # Validate military time
            if time_val:
                try:
                    t = int(time_val)
                    if t < 0 or t > 2359 or (t % 100) >= 60:
                        self._add_issue(i, 'date_time', 'warning', 'Crash Military Time',
                                        f'Invalid military time',
                                        current_value=time_val)
                        count += 1
                except ValueError:
                    pass

        # Auto-fix year mismatches
        dt_issues = [iss for iss in self.issues
                     if iss['type'] == 'date_time' and iss['field'] == 'Crash Year' and iss['suggested_fix']]
        fixed = 0
        for issue in dt_issues:
            self._fix_issue(issue, issue['suggested_fix'])
            fixed += 1

        self.stats['datetime_issues'] = count
        self.stats['datetime_fixed'] = fixed
        logger.info(f"  Check 7 (date/time): {count:,} issues, {fixed:,} year mismatches fixed")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 8: Missing Facility Type / Functional Class inference
    #  (4-tier strategy from crash-data-validator v13)
    # ═══════════════════════════════════════════════════════════

    def check_missing_fields(self):
        """
        Detect missing critical fields and infer values using:
          Tier A: Route-majority vote (most accurate)
          Tier B: FC ↔ Facility Type crosswalk from existing data
          Tier C: Roadway Description → Facility Type mapping
          Tier D: Route name prefix patterns
        """
        # Count missing critical fields
        critical_fields = ['Crash Severity', 'Collision Type', 'Weather Condition',
                           'Light Condition', 'Roadway Surface Condition',
                           'Functional Class', 'Facility Type']
        missing_count = 0
        for i, row in enumerate(self.rows):
            for field in critical_fields:
                val = (row.get(field, '') or '').strip()
                if not val or val.lower() in ('null', 'unknown', 'na', 'n/a', 'not applicable'):
                    self._add_issue(i, 'missing_field', 'warning', field,
                                    f'Missing or unknown value for {field}',
                                    current_value=val or '(empty)')
                    missing_count += 1

        # ── Build lookup tables for inference ──
        fc_to_ft: Dict[str, Dict[str, int]] = defaultdict(Counter)
        route_to_fc: Dict[str, Dict[str, int]] = defaultdict(Counter)
        route_to_ft: Dict[str, Dict[str, int]] = defaultdict(Counter)

        for row in self.rows:
            fc = (row.get('Functional Class', '') or '').strip()
            ft = (row.get('Facility Type', '') or '').strip()
            rte = (row.get('RTE Name', '') or '').strip()
            if fc and ft:
                fc_to_ft[fc][ft] += 1
            if rte and fc:
                route_to_fc[rte][fc] += 1
            if rte and ft:
                route_to_ft[rte][ft] += 1

        fc_ft_map = {fc: self._get_majority(dict(ft_counts))
                     for fc, ft_counts in fc_to_ft.items()}
        fc_ft_map = {k: v for k, v in fc_ft_map.items() if v}

        logger.info(f"  Check 8 (missing fields): {missing_count:,} missing values, "
                     f"built {len(fc_ft_map)} FC→FT, {len(route_to_fc)} route→FC lookups")

        # ── Infer missing values ──
        field_fixed = 0
        field_issues = [iss for iss in self.issues if iss['type'] == 'missing_field' and not iss['fixed']]

        for issue in field_issues:
            row = self.rows[issue['idx']]
            rte = (row.get('RTE Name', '') or '').strip()

            # ── Facility Type inference ──
            if issue['field'] == 'Facility Type':
                ft = self._infer_facility_type(row, rte, route_to_ft, fc_ft_map)
                if ft:
                    self._fix_issue(issue, ft)
                    field_fixed += 1

            # ── Functional Class inference ──
            elif issue['field'] == 'Functional Class':
                fc = self._infer_functional_class(row, rte, route_to_fc)
                if fc:
                    self._fix_issue(issue, fc)
                    field_fixed += 1

        self.stats['missing_fields'] = missing_count
        self.stats['fields_inferred'] = field_fixed
        logger.info(f"  Check 8 inference: {field_fixed:,} values inferred from route/crosswalk/prefix")

    def _infer_facility_type(self, row: Dict, rte: str,
                              route_to_ft: Dict, fc_ft_map: Dict) -> str:
        """4-tier Facility Type inference."""
        # Tier A: Same-route majority vote
        if rte and rte in route_to_ft:
            ft = self._get_majority(dict(route_to_ft[rte]))
            if ft:
                return ft

        # Tier B: Functional Class crosswalk
        fc = (row.get('Functional Class', '') or '').strip()
        if fc and fc in fc_ft_map:
            return fc_ft_map[fc]

        # Tier C: Roadway Description mapping
        rd = (row.get('Roadway Description', '') or '').strip().lower()
        if rd:
            if 'not divided' in rd:
                return '3-Two-Way Undivided'
            if 'positive median' in rd or 'barrier' in rd or 'divided' in rd:
                return '4-Two-Way Divided'
            if 'one-way' in rd or 'one way' in rd:
                return '1-One-Way Undivided'

        # Tier D: Route name prefix (least accurate)
        if rte:
            rte_upper = rte.upper()
            if re.match(r'^(I-|IS\s)', rte_upper):
                return '4-Two-Way Divided'
            if re.match(r'^(US\s|US-)', rte_upper):
                return '4-Two-Way Divided'
            if re.match(r'^(SR\s|SR-|SH\s)', rte_upper):
                return '3-Two-Way Undivided'

        return ''

    def _infer_functional_class(self, row: Dict, rte: str,
                                 route_to_fc: Dict) -> str:
        """3-tier Functional Class inference."""
        # Tier A: Same-route majority vote
        if rte and rte in route_to_fc:
            fc = self._get_majority(dict(route_to_fc[rte]))
            if fc:
                return fc
            # Fallback: accept single-match if only one option
            entries = route_to_fc[rte]
            if len(entries) == 1:
                return list(entries.keys())[0]

        # Tier B: SYSTEM + Area Type
        sys_val = (row.get('SYSTEM', '') or '').strip().lower()
        if sys_val:
            if 'interstate' in sys_val:
                return '1-Interstate (A,1)'
            if 'primary' in sys_val:
                return '3-Principal Arterial - Other (E,2)'
            if 'secondary' in sys_val:
                return '5-Major Collector (I,4)'
            if 'local' in sys_val or 'non' in sys_val:
                return '7-Local (J,6)'

        # Tier C: Route name prefix
        if rte:
            rte_upper = rte.upper()
            if re.match(r'^(I-|IS\s)', rte_upper):
                return '1-Interstate (A,1)'
            if re.match(r'^(US\s|US-)', rte_upper):
                return '3-Principal Arterial - Other (E,2)'
            if re.match(r'^(SR\s|SR-|SH\s)', rte_upper):
                return '4-Minor Arterial (H,3)'

        return ''

    # ═══════════════════════════════════════════════════════════
    #  CHECK 9: Route-median GPS inference
    # ═══════════════════════════════════════════════════════════

    def fix_missing_gps(self):
        """
        For records with missing/zero GPS: assign median coordinates
        from other crashes on the same route.
        """
        gps_issues = [iss for iss in self.issues
                      if iss['type'] == 'missing_gps' and not iss['fixed']]
        if not gps_issues:
            return

        route_fixed = 0
        for issue in gps_issues:
            row = self.rows[issue['idx']]
            rte = (row.get('RTE Name', '') or '').strip()
            if rte and rte in self.route_medians:
                mc = self.route_medians[rte]
                row['x'] = f"{mc['x']:.8f}"
                row['y'] = f"{mc['y']:.8f}"
                issue['fixed'] = True
                issue['suggested_fix'] = f"route median (n={mc['sample_size']})"
                route_fixed += 1

        self.stats['gps_route_fixed'] = route_fixed
        remaining = len([iss for iss in gps_issues if not iss['fixed']])
        logger.info(f"  Check 9 (route-median GPS): {route_fixed:,} fixed, {remaining:,} remaining")

    # ═══════════════════════════════════════════════════════════
    #  CHECK 10: Bounds snap (route-median + nearest-neighbor)
    # ═══════════════════════════════════════════════════════════

    def fix_out_of_bounds(self):
        """
        For OOB records: snap to route median first, then nearest in-bounds neighbor.
        """
        bounds_issues = [iss for iss in self.issues
                         if iss['type'] == 'bounds' and not iss['fixed']]
        if not bounds_issues:
            return

        # Pass 1: Route-median snap
        route_snapped = 0
        for issue in bounds_issues:
            row = self.rows[issue['idx']]
            rte = (row.get('RTE Name', '') or '').strip()
            if rte and rte in self.route_medians:
                mc = self.route_medians[rte]
                row['x'] = f"{mc['x']:.8f}"
                row['y'] = f"{mc['y']:.8f}"
                issue['fixed'] = True
                issue['suggested_fix'] = f"route snap (n={mc['sample_size']})"
                route_snapped += 1

        # Pass 2: Nearest-neighbor for remaining
        remaining = [iss for iss in bounds_issues if not iss['fixed']]
        nn_fixed = 0
        grid_size = 0.01

        for issue in remaining:
            row = self.rows[issue['idx']]
            ox = self._safe_float(row.get('x'))
            oy = self._safe_float(row.get('y'))
            if ox == 0.0 or oy == 0.0:
                continue

            gx = int(ox / grid_size)
            gy = int(oy / grid_size)
            best_dist = 0.1  # max 0.1° ≈ 10km
            best_cell = None

            for dx in range(-10, 11):
                for dy in range(-10, 11):
                    cell = self.spatial_grid.get(f"{gx + dx},{gy + dy}")
                    if not cell:
                        continue
                    dist = math.sqrt((cell['x'] - ox) ** 2 + (cell['y'] - oy) ** 2)
                    if dist < best_dist:
                        best_dist = dist
                        best_cell = cell

            if best_cell:
                row['x'] = f"{best_cell['x']:.8f}"
                row['y'] = f"{best_cell['y']:.8f}"
                issue['fixed'] = True
                issue['suggested_fix'] = f"nearest neighbor ({best_dist * 111:.1f}km)"
                nn_fixed += 1

        self.stats['bounds_route_snapped'] = route_snapped
        self.stats['bounds_nn_fixed'] = nn_fixed
        logger.info(f"  Check 10 (bounds snap): {route_snapped:,} route-median, {nn_fixed:,} nearest-neighbor")

    # ═══════════════════════════════════════════════════════════
    #  RUN ALL CHECKS
    # ═══════════════════════════════════════════════════════════

    def run_all(self, bounds: Optional[Dict] = None):
        """Run the full validation + auto-correction pipeline."""
        total = len(self.rows)
        logger.info(f"Phase 6: Validation & auto-correction on {total:,} rows...")

        # Pre-computation
        self._build_route_index(bounds)
        self._build_spatial_grid(bounds)

        # Checks (detection + auto-fix)
        self.check_whitespace()                        # 1
        self.check_duplicates()                        # 2
        self.check_missing_gps()                       # 3
        self.check_bounds(bounds)                      # 4
        self.check_severity()                          # 5
        self.check_cross_field()                       # 6
        self.check_datetime()                          # 7
        self.check_missing_fields()                    # 8

        # Spatial auto-corrections (use pre-built indexes)
        self.fix_missing_gps()                         # 9
        self.fix_out_of_bounds()                       # 10

        # Summary
        all_issues = len(self.issues)
        fixed = sum(1 for iss in self.issues if iss['fixed'])
        errors = sum(1 for iss in self.issues if iss['severity'] == 'error')
        warnings = sum(1 for iss in self.issues if iss['severity'] == 'warning')
        fix_rate = (fixed / all_issues * 100) if all_issues > 0 else 100

        logger.info(f"  Validation complete: {all_issues:,} issues ({errors:,} errors, "
                     f"{warnings:,} warnings), {fixed:,} auto-fixed ({fix_rate:.1f}% fix rate)")

        self.stats['total_issues'] = all_issues
        self.stats['total_fixed'] = fixed
        self.stats['fix_rate_pct'] = round(fix_rate, 1)
        return self.stats

    def get_report(self) -> Dict:
        """Return a JSON-serializable validation report."""
        type_counts = Counter(iss['type'] for iss in self.issues)
        fixed_by_type = Counter(iss['type'] for iss in self.issues if iss['fixed'])
        unfixed = [iss for iss in self.issues if not iss['fixed'] and iss['severity'] != 'info']
        return {
            'total_issues': len(self.issues),
            'issues_by_type': dict(type_counts),
            'fixed_by_type': dict(fixed_by_type),
            'unfixed_actionable': len(unfixed),
            'stats': self.stats,
            'corrections_count': len(self.corrections),
        }


# ═══════════════════════════════════════════════════════════════
#  PHASE 7: JURISDICTION RANKING (24 columns)
# ═══════════════════════════════════════════════════════════════

def compute_rankings(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Compute jurisdiction-level metrics and 24 ranking columns."""
    metrics: Dict[str, Dict] = {}
    for row in rows:
        key = row.get('FIPS', '') or row.get('Physical Juris Name', '')
        if not key:
            continue
        if key not in metrics:
            metrics[key] = {
                'total_crash': 0, 'total_ped_crash': 0, 'total_bike_crash': 0,
                'total_fatal': 0, 'total_fatal_serious_injury': 0, 'total_epdo': 0,
                'district': row.get('DOT District', ''),
                'mpo': row.get('MPO Name', ''),
                'pd': row.get('Planning District', ''),
            }
        m = metrics[key]
        m['total_crash'] += 1
        m['total_epdo'] += int(row.get('EPDO_Score', '1'))
        if row.get('Pedestrian?') == 'Yes':
            m['total_ped_crash'] += 1
        if row.get('Bike?') == 'Yes':
            m['total_bike_crash'] += 1
        sev = row.get('Crash Severity', '')
        if sev == 'K':
            m['total_fatal'] += 1
        if sev in ('K', 'A'):
            m['total_fatal_serious_injury'] += 1

    # Rank within each scope
    rankings: Dict[str, Dict[str, Any]] = {}
    for metric in RANKING_METRICS:
        # Juris scope (statewide)
        sorted_j = sorted(metrics.items(), key=lambda x: x[1][metric], reverse=True)
        rank, prev_val = 0, -1
        for idx, (key, m) in enumerate(sorted_j):
            if m[metric] != prev_val:
                rank = idx + 1
                prev_val = m[metric]
            rankings.setdefault(key, {})[f'Juris_Rank_{metric}'] = rank

        # District scope
        by_group: Dict[str, List] = defaultdict(list)
        for key, m in metrics.items():
            by_group[m['district'] or '_none'].append((key, m))
        for group, entries in by_group.items():
            entries.sort(key=lambda x: x[1][metric], reverse=True)
            rank, prev_val = 0, -1
            for idx, (key, m) in enumerate(entries):
                if m[metric] != prev_val:
                    rank = idx + 1
                    prev_val = m[metric]
                rankings.setdefault(key, {})[f'District_Rank_{metric}'] = rank if group != '_none' else ''

        # PlanningDistrict scope
        by_group = defaultdict(list)
        for key, m in metrics.items():
            by_group[m['pd'] or '_none'].append((key, m))
        for group, entries in by_group.items():
            entries.sort(key=lambda x: x[1][metric], reverse=True)
            rank, prev_val = 0, -1
            for idx, (key, m) in enumerate(entries):
                if m[metric] != prev_val:
                    rank = idx + 1
                    prev_val = m[metric]
                rankings.setdefault(key, {})[f'PlanningDistrict_Rank_{metric}'] = rank if group != '_none' else ''

        # MPO scope
        by_group = defaultdict(list)
        for key, m in metrics.items():
            if m['mpo']:
                by_group[m['mpo']].append((key, m))
            else:
                rankings.setdefault(key, {})[f'MPO_Rank_{metric}'] = ''
        for group, entries in by_group.items():
            entries.sort(key=lambda x: x[1][metric], reverse=True)
            rank, prev_val = 0, -1
            for idx, (key, m) in enumerate(entries):
                if m[metric] != prev_val:
                    rank = idx + 1
                    prev_val = m[metric]
                rankings.setdefault(key, {})[f'MPO_Rank_{metric}'] = rank

    # Apply rankings to rows
    for row in rows:
        key = row.get('FIPS', '') or row.get('Physical Juris Name', '')
        row_ranks = rankings.get(key, {})
        for scope in RANKING_SCOPES:
            for metric in RANKING_METRICS:
                col = f'{scope}_Rank_{metric}'
                row[col] = str(row_ranks.get(col, ''))

    logger.info(f"Phase 7: Rankings computed — {len(metrics)} jurisdictions × "
                f"{len(RANKING_SCOPES)} scopes × {len(RANKING_METRICS)} metrics")
    return rows


# ═══════════════════════════════════════════════════════════════
#  MAIN PIPELINE (all 7 phases)
# ═══════════════════════════════════════════════════════════════

def normalize(input_csv: str, output_csv: str, report_path: str = ''):
    """Run the full normalization + validation + ranking pipeline."""

    # ── Load source data ──
    logger.info(f"Loading: {input_csv}")
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        source_rows = list(reader)
        source_headers = reader.fieldnames or []
    logger.info(f"Loaded {len(source_rows):,} rows, {len(source_headers)} columns")

    # ══════════════════════════════════════════════════════
    #  Phase 1: Column mapping
    # ══════════════════════════════════════════════════════
    logger.info("Phase 1: Column mapping...")
    normalized = []
    for i, src_row in enumerate(source_rows):
        row = {col: '' for col in GOLDEN_COLUMNS}
        # Apply column map
        for src_col, tgt_col in COLUMN_MAP.items():
            if src_col in src_row:
                row[tgt_col] = (src_row[src_col] or '').strip()
        # Copy coordinates
        for coord_field in ['x', 'y', 'LATITUDE', 'LONGITUDE', 'latitude', 'longitude']:
            if coord_field in src_row and src_row[coord_field]:
                if coord_field.lower() in ('latitude', 'lat', 'y'):
                    row['y'] = src_row[coord_field].strip()
                elif coord_field.lower() in ('longitude', 'lon', 'x'):
                    row['x'] = src_row[coord_field].strip()

        # ══════════════════════════════════════════════════
        #  Phase 2: State-specific transforms
        # ══════════════════════════════════════════════════
        row = apply_state_transforms(row, src_row)

        # ══════════════════════════════════════════════════
        #  Phase 3: Composite crash ID
        # ══════════════════════════════════════════════════
        if not row['Document Nbr']:
            row['Document Nbr'] = generate_crash_id(row, i + 1)

        # Initialize enrichment columns
        row['FIPS'] = ''
        row['Place FIPS'] = ''
        row['EPDO_Score'] = ''

        normalized.append(row)

    logger.info(f"Phases 1-3 complete: {len(normalized):,} rows")

    # ══════════════════════════════════════════════════════
    #  Phase 4: Geography resolution (geo_resolver.py)
    # ══════════════════════════════════════════════════════
    logger.info("Phase 4: Geography resolution...")
    hier_path = HIERARCHY_PATH if os.path.exists(HIERARCHY_PATH) else ''
    geo_path = GEO_DIR if os.path.isdir(GEO_DIR) else ''
    if geo_path:
        resolver = GeoResolver(
            state_fips=STATE_FIPS,
            state_abbr=STATE_ABBR,
            geo_dir=geo_path,
            hierarchy_path=hier_path,
        )
        normalized = resolver.resolve_all(normalized)
        geo_report = resolver.get_resolution_report()
        logger.info(f"Geography: {geo_report['resolved']}/{geo_report['total_jurisdictions']} "
                     f"jurisdictions resolved ({geo_report['coverage_pct']}%)")
    else:
        geo_report = {'resolved': 0, 'total_jurisdictions': 0, 'coverage_pct': 0}
        logger.warning("Phase 4: Skipped — geography directory not found")

    # ══════════════════════════════════════════════════════
    #  Phase 5: EPDO scoring
    # ══════════════════════════════════════════════════════
    logger.info("Phase 5: EPDO scoring...")
    for row in normalized:
        row['EPDO_Score'] = str(compute_epdo(row.get('Crash Severity', 'O')))

    # ══════════════════════════════════════════════════════
    #  Phase 6: Validation & auto-correction
    # ══════════════════════════════════════════════════════
    validator = ValidationEngine(normalized, STATE_FIPS, STATE_ABBR)
    validation_stats = validator.run_all()
    validation_report = validator.get_report()

    # ══════════════════════════════════════════════════════
    #  Phase 7: Rankings
    # ══════════════════════════════════════════════════════
    normalized = compute_rankings(normalized)

    # ── Build output column list ──
    output_cols = list(GOLDEN_COLUMNS) + list(ENRICHMENT_COLUMNS)
    for scope in RANKING_SCOPES:
        for metric in RANKING_METRICS:
            output_cols.append(f'{scope}_Rank_{metric}')

    # ── Write output CSV ──
    logger.info(f"Writing: {output_csv} ({len(output_cols)} columns)")
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_cols, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(normalized)

    # ── Severity distribution ──
    sev_dist = Counter(row.get('Crash Severity', '?') for row in normalized)

    # ── Full report ──
    report = {
        'state': STATE_NAME,
        'state_fips': STATE_FIPS,
        'state_abbr': STATE_ABBR,
        'dot': DOT_NAME,
        'total_rows': len(normalized),
        'total_columns': len(output_cols),
        'golden_columns': len(GOLDEN_COLUMNS),
        'enrichment_columns': len(ENRICHMENT_COLUMNS),
        'ranking_columns': len(RANKING_SCOPES) * len(RANKING_METRICS),
        'severity_distribution': dict(sev_dist),
        'epdo_weights': EPDO_WEIGHTS,
        'geography': geo_report,
        'validation': validation_report,
        'quality_score': _compute_quality_score(geo_report, validation_stats, sev_dist, len(normalized)),
    }

    if report_path:
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Report saved: {report_path}")

    logger.info(f"Pipeline complete: {len(normalized):,} rows × {len(output_cols)} columns "
                f"(quality score: {report['quality_score']}%)")
    return report


def _compute_quality_score(geo_report: Dict, val_stats: Dict,
                           sev_dist: Counter, total_rows: int) -> float:
    """Compute an overall quality score (0-100) for the pipeline output."""
    scores = []

    # Geography coverage (0-25)
    geo_pct = geo_report.get('coverage_pct', 0)
    scores.append(min(25, geo_pct * 0.25))

    # Severity validity (0-25): all rows should have K/A/B/C/O
    valid_sev = sum(sev_dist.get(s, 0) for s in ('K', 'A', 'B', 'C', 'O'))
    sev_pct = (valid_sev / total_rows * 100) if total_rows > 0 else 0
    scores.append(min(25, sev_pct * 0.25))

    # Validation fix rate (0-25)
    fix_rate = val_stats.get('fix_rate_pct', 100)
    scores.append(min(25, fix_rate * 0.25))

    # GPS completeness (0-25)
    missing_gps = val_stats.get('missing_gps', 0)
    gps_fixed = val_stats.get('gps_route_fixed', 0)
    gps_remaining = missing_gps - gps_fixed
    gps_pct = ((total_rows - gps_remaining) / total_rows * 100) if total_rows > 0 else 100
    scores.append(min(25, gps_pct * 0.25))

    return round(sum(scores), 1)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=f'CrashLens {STATE_NAME} Normalizer (Enhanced v2.0 — with validation)')
    parser.add_argument('--csv', required=True, help='Input CSV path')
    parser.add_argument('--output', default='', help='Output CSV path')
    parser.add_argument('--report', default='', help='JSON report output path')
    args = parser.parse_args()

    out = args.output or args.csv.replace('.csv', '_normalized.csv')
    rpt = args.report or args.csv.replace('.csv', '_report.json')
    normalize(args.csv, out, rpt)
