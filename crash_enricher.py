#!/usr/bin/env python3
"""
crash_enricher.py — CrashLens Universal Crash Data Enrichment Module
====================================================================
Shared module that enriches ANY normalized crash dataset by deriving
missing columns from GPS coordinates, contributing circumstances,
temporal data, HPMS federal road data, and OpenStreetMap.

DATA AUTHORITY HIERARCHY (v2.6.5):
  Tier A — HPMS OVERWRITE: FC, Ownership, SYSTEM, Facility Type, Surface Type
    → FHWA-validated road inventory always replaces state crash-report values.
  Tier B — STATE AUTHORITATIVE: RTE Name, Node, Node Offset, RNS MP
    → State data preserved. HPMS/OSM only fill empty cells.
  Tier C — FIRST AVAILABLE: Speed Limit, Alignment, AADT, Lanes, etc.
    → HPMS fills first, then state, then OSM. No overwrites.

ENRICHMENT ORDER:
  Tier 1: Self-enrichment    — derive from existing crash fields (zero deps)
  Tier 3: HPMS (PRIMARY)     — GPS → nearest HPMS segment → federal road attributes
  Tier 2: OSM (fills gaps)   — GPS → nearest OSM road → local road attributes
  Tier 2b: POI proximity     — GPS → nearby bars, schools, signals, hospitals
  Tier 2c: Federal safety    — GPS → NBI bridges, FRA rail, Urban schools, NTM transit

Usage:
    from crash_enricher import CrashEnricher
    enricher = CrashEnricher(state_fips="10", state_abbr="DE")
    df = enricher.enrich_all(df)  # enriches in-place, returns df
"""

import gc
import math
import os
import re
import json
import time
import traceback
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# ── Node-based intersection classification tunables ──
# Crashes within INTERSECTION_THRESHOLD_M (meters) of a real intersection
# node (streets_per_node >= 3) are tagged "at intersection". 50 m ≈ 164 ft
# matches FHWA's typical "at-intersection" definition and yields a Delaware
# at-intersection rate in the 55–65% expected band.
INTERSECTION_THRESHOLD_M = 50


# ─────────────────────────────────────────────────────────────────────────────
#  DUCKDB SPATIAL GRID ENGINE (v2.7)
# ─────────────────────────────────────────────────────────────────────────────
#
#  For 1M+ crash datasets, KDTree uses too much RAM on GitHub Actions (7GB).
#  DuckDB reads parquet files with near-zero memory via memory-mapped I/O,
#  then aggregates into a spatial grid dict for O(1) lookups per crash.
#
#  Grid cell: 0.001° ≈ 111m (matches GPS accuracy of crash data)
#  Memory:    ~50MB for 75K HPMS segments vs ~500MB for KDTree+arrays
#  Speed:     O(1) dict lookup vs O(log n) KDTree query
#
#  Falls back to chunked KDTree (numpy+scipy) if DuckDB not installed.
# ─────────────────────────────────────────────────────────────────────────────

_HAS_DUCKDB = False
try:
    import duckdb
    _HAS_DUCKDB = True
except ImportError:
    pass


def _build_spatial_grid(parquet_path, lat_col, lon_col, value_cols, grid_resolution=1000):
    """Build spatial grid lookup from a parquet file using DuckDB.

    Grid resolution 1000 → round(lat*1000) → ~111m cells.
    Returns dict: (grid_lat, grid_lon) → {col: value, ...}
    Returns None if DuckDB unavailable (caller falls back to KDTree).
    """
    if not _HAS_DUCKDB:
        return None

    try:
        con = duckdb.connect()
        # Filter to valid columns that exist in the parquet
        existing = set(con.execute(
            f"SELECT name FROM parquet_schema('{parquet_path}') WHERE name != 'schema'"
        ).fetchdf()["name"])
        use_cols = [c for c in value_cols if c in existing]
        if not use_cols:
            con.close()
            return None

        agg_exprs = ", ".join(
            f'FIRST("{c}" ORDER BY "{c}" IS NOT NULL DESC) AS "{c}"' for c in use_cols
        )
        query = f"""
            SELECT
                CAST(ROUND("{lat_col}" * {grid_resolution}) AS INTEGER) AS grid_lat,
                CAST(ROUND("{lon_col}" * {grid_resolution}) AS INTEGER) AS grid_lon,
                {agg_exprs}
            FROM read_parquet('{parquet_path}')
            WHERE "{lat_col}" IS NOT NULL AND "{lon_col}" IS NOT NULL
            GROUP BY grid_lat, grid_lon
        """
        result = con.execute(query).fetchall()
        con.close()

        grid = {}
        for row in result:
            attrs = {}
            for i, col in enumerate(use_cols):
                val = row[i + 2]
                if val is not None:
                    attrs[col] = val
            if attrs:
                grid[(row[0], row[1])] = attrs

        return grid
    except Exception as e:
        print(f"    DuckDB grid build failed: {e} — falling back to KDTree")
        return None


def _grid_enrich_crashes(df, grid, lat_series, lon_series, valid_mask,
                         column_map, overwrite_cols=None, fill_cols=None,
                         grid_resolution=1000):
    """Enrich crash DataFrame using spatial grid lookups (O(1) per crash).

    column_map: dict mapping grid column names → DataFrame column names
    overwrite_cols: set of DataFrame columns where grid always wins
    fill_cols: set of DataFrame columns where grid only fills empty cells
    Returns (df, filled_counts dict).
    """
    if overwrite_cols is None:
        overwrite_cols = set()
    if fill_cols is None:
        fill_cols = set()

    lats = lat_series[valid_mask]
    lons = lon_series[valid_mask]
    valid_indices = df.index[valid_mask]

    filled = defaultdict(int)
    matched = 0

    for i, (lat, lon) in enumerate(zip(lats, lons)):
        key = (round(float(lat) * grid_resolution), round(float(lon) * grid_resolution))
        attrs = grid.get(key)
        if attrs is None:
            # Try 8 neighboring cells (handles grid boundary crashes)
            for dlat in [-1, 0, 1]:
                for dlon in [-1, 0, 1]:
                    if dlat == 0 and dlon == 0:
                        continue
                    attrs = grid.get((key[0] + dlat, key[1] + dlon))
                    if attrs:
                        break
                if attrs:
                    break

        if attrs is None:
            continue

        matched += 1
        idx = valid_indices[i]
        for grid_col, df_col in column_map.items():
            if grid_col not in attrs:
                continue
            val = str(attrs[grid_col]).strip()
            if not val or val in ("nan", "None", ""):
                continue

            current = str(df.at[idx, df_col]).strip() if df_col in df.columns else ""

            if df_col in overwrite_cols:
                df.at[idx, df_col] = val
                filled[df_col] += 1
            elif df_col in fill_cols or not current:
                if not current:
                    df.at[idx, df_col] = val
                    filled[df_col] += 1

    return df, filled, matched

# ─────────────────────────────────────────────────────────────────────────────
#  CROSSWALK TABLES — OSM Tags → CrashLens Standard Values
# ─────────────────────────────────────────────────────────────────────────────

# OSM highway tag → FHWA Functional Class (CrashLens standard values)
OSM_HIGHWAY_TO_FC = {
    "motorway":        "1-Interstate (A,1)",
    "motorway_link":   "1-Interstate (A,1)",
    "trunk":           "2-Principal Arterial - Other Freeways and Expressways (B)",
    "trunk_link":      "2-Principal Arterial - Other Freeways and Expressways (B)",
    "primary":         "3-Principal Arterial - Other (E,2)",
    "primary_link":    "3-Principal Arterial - Other (E,2)",
    "secondary":       "4-Minor Arterial (H,3)",
    "secondary_link":  "4-Minor Arterial (H,3)",
    "tertiary":        "5-Major Collector (I,4)",
    "tertiary_link":   "5-Major Collector (I,4)",
    "unclassified":    "6-Minor Collector (5)",
    "residential":     "7-Local (J,6)",
    "service":         "7-Local (J,6)",
    "living_street":   "7-Local (J,6)",
    "track":           "7-Local (J,6)",
    "path":            "7-Local (J,6)",
}

# Functional Class → Ownership derivation
FC_TO_OWNERSHIP = {
    "1-Interstate (A,1)":  "1. State Hwy Agency",
    "2-Principal Arterial - Other Freeways and Expressways (B)": "1. State Hwy Agency",
    "3-Principal Arterial - Other (E,2)": "1. State Hwy Agency",
    "4-Minor Arterial (H,3)": "1. State Hwy Agency",
    "5-Major Collector (I,4)": "2. County Hwy Agency",
    "6-Minor Collector (5)":   "2. County Hwy Agency",
    "7-Local (J,6)":          "3. City or Town Hwy Agency",
}

# Functional Class → SYSTEM derivation (generic — states customize the label)
FC_TO_SYSTEM = {
    "1-Interstate (A,1)":  "DOT Interstate",
    "2-Principal Arterial - Other Freeways and Expressways (B)": "DOT Primary",
    "3-Principal Arterial - Other (E,2)": "DOT Primary",
    "4-Minor Arterial (H,3)": "DOT Secondary",
    "5-Major Collector (I,4)": "DOT Secondary",
    "6-Minor Collector (5)":   "Non-DOT primary",
    "7-Local (J,6)":          "Non-DOT secondary",
}

# Functional Class → default Facility Type
FC_TO_FACILITY_TYPE = {
    "1-Interstate (A,1)":  "4-Two-Way Divided",
    "2-Principal Arterial - Other Freeways and Expressways (B)": "4-Two-Way Divided",
    "3-Principal Arterial - Other (E,2)": "3-Two-Way Undivided",
    "4-Minor Arterial (H,3)": "3-Two-Way Undivided",
    "5-Major Collector (I,4)": "3-Two-Way Undivided",
    "6-Minor Collector (5)":   "3-Two-Way Undivided",
    "7-Local (J,6)":          "3-Two-Way Undivided",
}

# FC → Mainline?
FC_TO_MAINLINE = {
    "1-Interstate (A,1)": "Yes",
    "2-Principal Arterial - Other Freeways and Expressways (B)": "Yes",
    "3-Principal Arterial - Other (E,2)": "Yes",
    "4-Minor Arterial (H,3)": "No",
    "5-Major Collector (I,4)": "No",
    "6-Minor Collector (5)": "No",
    "7-Local (J,6)": "No",
}

# OSM oneway tag → Facility Type refinement
OSM_ONEWAY_FACILITY = {
    "yes": "1-One-Way Undivided",
    "-1":  "1-One-Way Undivided",
}

# OSM lanes + divided → Roadway Description
def derive_roadway_description(oneway, lanes, divided):
    """Derive CrashLens Roadway Description from OSM road attributes."""
    if oneway in ("yes", "-1"):
        return "4. One-Way, Not Divided"
    if divided in ("yes", "true", "1"):
        return "2. Two-Way, Divided, Unprotected Median"
    return "1. Two-Way, Not Divided"

# Route name → FC override (catches cases OSM misclassifies)
ROUTE_PREFIX_TO_FC = {
    r"^I[-\s]?\d":   "1-Interstate (A,1)",
    r"^US[-\s]?\d":  "3-Principal Arterial - Other (E,2)",
    r"^SR[-\s]?\d":  "4-Minor Arterial (H,3)",
    r"^DE[-\s]?\d":  "4-Minor Arterial (H,3)",
    r"^CR[-\s]?\d":  "6-Minor Collector (5)",
    r"^CO[-\s]?\d":  "6-Minor Collector (5)",
}

# ─────────────────────────────────────────────────────────────────────────────
#  TIER 2b: POI PROXIMITY THRESHOLDS
#  Distances for crash-to-POI proximity analysis
# ─────────────────────────────────────────────────────────────────────────────

POI_PROXIMITY = {
    # category     threshold_m  column_name             value_if_near
    "bar":         (457,        "Near_Bar_1500ft",      "Yes"),    # 1500 ft / 0.28 mi
    "school":      (305,        "Near_School_1000ft",   "Yes"),    # 1000 ft / 0.19 mi
    "crossing":    (30,         "Near_Crossing_100ft",  "Yes"),    # 100 ft
    "parking":     (46,         "Near_Parking_150ft",   "Yes"),    # 150 ft
    "rail_xing":   (46,         "Near_Rail_Xing_150ft", "Yes"),    # 150 ft
}

# Traffic control: signal within 30m → "3. Traffic Signal", stop sign within 20m → "4. Stop Sign"
POI_TRAFFIC_CONTROL = {
    "signal":    (30,  "3. Traffic Signal"),
    "stop_sign": (20,  "4. Stop Sign"),
}

# Hospital distance column (continuous, in miles)
POI_HOSPITAL_DIST_COL = "Nearest_Hospital_mi"

# ─────────────────────────────────────────────────────────────────────────────
#  TIER 2c: FEDERAL SAFETY DATA
#  NBI bridges, FRA rail crossings, Urban Institute schools, NTM transit stops
#  These UPGRADE existing OSM/POI proximity flags with authoritative detail.
# ─────────────────────────────────────────────────────────────────────────────

# Federal cache file suffixes (looked up as {abbr}_{suffix}.parquet in cache_dir)
FEDERAL_SOURCES = {
    "schools":        "schools",
    "bridges":        "bridges",
    "rail_crossings": "rail_crossings",
    "transit":        "transit",
}

# Proximity thresholds for federal enrichment (in meters)
FEDERAL_SCHOOL_THRESHOLD_M = 305     # 1000 ft (same as POI Near_School_1000ft)
FEDERAL_BRIDGE_THRESHOLD_M = 46      # 150 ft
FEDERAL_RAIL_THRESHOLD_M = 46        # 150 ft (same as POI Near_Rail_Xing_150ft)
FEDERAL_TRANSIT_THRESHOLD_M = 152    # 500 ft

# Output column names
FEDERAL_COL_SCHOOL_ENROLLMENT = "School_Enrollment_Nearest"
FEDERAL_COL_BRIDGE_CONDITION = "Bridge_Condition"
FEDERAL_COL_BRIDGE_YEAR = "Bridge_Year_Built"
FEDERAL_COL_RAIL_WARNING = "Rail_Warning_Device"
FEDERAL_COL_RAIL_TRAINS = "Rail_Trains_Per_Day"
FEDERAL_COL_TRANSIT = "Near_Transit_500ft"

# ─────────────────────────────────────────────────────────────────────────────
#  TIER 3: HPMS — Federal Highway Performance Monitoring System
#  Official FHWA road data: AADT, FC, speed, lanes, surface, ownership
#  HPMS is the PRIMARY source; OSM fills gaps for roads not in HPMS.
# ─────────────────────────────────────────────────────────────────────────────

# HPMS F_System → CrashLens Functional Class
HPMS_FSYSTEM_TO_FC = {
    1: "1-Interstate (A,1)",
    2: "2-Principal Arterial - Other Freeways and Expressways (B)",
    3: "3-Principal Arterial - Other (E,2)",
    4: "4-Minor Arterial (H,3)",
    5: "5-Major Collector (I,4)",
    6: "6-Minor Collector (5)",
    7: "7-Local (J,6)",
}

# HPMS Ownership → CrashLens Ownership (full FHWA 27-code table)
# Source: HPMS Field Manual Item 6 (Dec 2016 + 2022 draft)
# Must match OWNERSHIP_LABELS in road_data_authority.py and
# HPMS_CODE_TO_OWNERSHIP in road_inventory_postprocess.py
HPMS_OWNERSHIP_MAP = {
    # Primary agencies
    1:  "1. State Hwy Agency",
    2:  "2. County Hwy Agency",
    3:  "3. City or Town Hwy Agency",       # Town or Township
    4:  "3. City or Town Hwy Agency",       # City or Municipal (NOT Federal!)
    # State sub-agencies
    11: "1. State Hwy Agency",              # State Park, Forest, Reservation
    12: "3. City or Town Hwy Agency",       # Local Park, Forest, Reservation
    21: "1. State Hwy Agency",              # Other State Agency
    25: "3. City or Town Hwy Agency",       # Other Local Agency
    # Private / Railroad
    26: "6. Private/Unknown Roads",         # Private (other than Railroad)
    27: "6. Private/Unknown Roads",         # Railroad
    # Toll authorities
    31: "1. State Hwy Agency",              # State Toll Road Authority
    32: "2. County Hwy Agency",             # Local Toll Authority
    # Public instrumentalities
    40: "1. State Hwy Agency",              # Other Public Instrumentality
    # Tribal
    50: "4. Federal Roads",                 # Indian Tribe Nation
    # Federal agencies
    60: "4. Federal Roads",                 # Other Federal Agency
    62: "4. Federal Roads",                 # Bureau of Indian Affairs
    63: "4. Federal Roads",                 # Bureau of Fish and Wildlife
    64: "4. Federal Roads",                 # U.S. Forest Service
    66: "4. Federal Roads",                 # National Park Service
    67: "4. Federal Roads",                 # Tennessee Valley Authority
    68: "4. Federal Roads",                 # Bureau of Land Management
    69: "4. Federal Roads",                 # Bureau of Reclamation
    70: "4. Federal Roads",                 # Corps of Engineers (Civil)
    72: "4. Federal Roads",                 # Air Force
    73: "4. Federal Roads",                 # Navy/Marines
    74: "4. Federal Roads",                 # Army
    80: "6. Private/Unknown Roads",         # Other
}

# HPMS Surface_Type → CrashLens Roadway Surface Type
HPMS_SURFACE_MAP = {
    1: "4. Slag, Gravel, Stone",           # Unpaved
    2: "2. Blacktop, Asphalt, Bituminous",  # Asphalt
    3: "2. Blacktop, Asphalt, Bituminous",  # Asphalt over concrete
    4: "2. Blacktop, Asphalt, Bituminous",  # Asphalt on other
    5: "1. Concrete",                       # Concrete (JPCP)
    6: "1. Concrete",                       # Concrete (JRCP)
    7: "1. Concrete",                       # Concrete (CRCP)
    8: "1. Concrete",                       # Concrete over asphalt
    9: "1. Concrete",                       # Concrete on other
    10: "6. Other",                         # Composite
    11: "3. Brick or Block",               # Brick
}

# HPMS Median_Type → CrashLens Roadway Description refinement
HPMS_MEDIAN_TO_DESC = {
    1: "1. Two-Way, Not Divided",           # No median
    2: "2. Two-Way, Divided, Unprotected Median",  # Curbed
    3: "3. Two-Way, Divided, Positive Median Barrier",  # Positive barrier
    4: "2. Two-Way, Divided, Unprotected Median",  # Painted/flush
    5: "2. Two-Way, Divided, Unprotected Median",  # Depressed
    6: "2. Two-Way, Divided, Unprotected Median",  # Raised
}

# HPMS Terrain → CrashLens Roadway Alignment refinement
HPMS_TERRAIN_TO_ALIGNMENT = {
    1: "1. Straight - Level",       # Flat
    2: "3. Grade - Straight",       # Rolling
    3: "4. Grade - Curve",          # Mountainous
}

# ─────────────────────────────────────────────────────────────────────────────
#  DATA AUTHORITY HIERARCHY
#  Determines whether HPMS overwrites state data or only fills gaps.
#
#  TIER A — HPMS-AUTHORITATIVE (OVERWRITE):
#    HPMS always wins when it has a value. Rationale: FHWA-validated road
#    inventory is more accurate than crash-report linked attributes.
#    State data in these columns comes from officer reports or imperfect
#    crash-to-road linking. HPMS data went through federal QA.
#
#  TIER B — STATE-AUTHORITATIVE (FILL only):
#    State data preserved. HPMS/OSM only fills empty cells.
#    Rationale: States know their own route naming, LRS nodes,
#    mileposts, and local designations better than any federal dataset.
#
#  TIER C — FIRST-AVAILABLE (FILL only):
#    HPMS fills first, then state, then OSM. No overwrites.
#    Rationale: Multiple sources are roughly equivalent in quality,
#    or the "best" source depends on context.
# ─────────────────────────────────────────────────────────────────────────────

# Tier A: HPMS overwrites state data (FHWA-validated road inventory)
HPMS_OVERWRITE_COLUMNS = {
    "Functional Class",     # FHWA approves all state FC assignments
    "Ownership",            # FHWA tracks legal road ownership
    "SYSTEM",               # Derived from FC — must be consistent
    "Facility Type",        # One-way/two-way from road inventory, not officer report
    "Roadway Surface Type", # Systematic pavement survey > officer observation
}

# Tier B: State data wins — HPMS/OSM only fill if cell is empty
STATE_AUTHORITATIVE_COLUMNS = {
    "RTE Name",             # State route naming convention (US 29 BUS, SR 234)
    "Node",                 # State LRS node system
    "Node Offset (ft)",     # State LRS reference
    "RNS MP",               # State milepost system
}

# Tier C: First-available wins (HPMS fills → state fills → OSM fills)
# Everything not in Tier A or B defaults to this behavior.
# Includes: Speed Limit, Roadway Alignment, Roadway Description,
#           Traffic Control Type, Intersection Type, Max Speed Diff,
#           all HPMS-only columns (AADT, Through_Lanes, etc.)

# ─────────────────────────────────────────────────────────────────────────────
#  TIER 2c: ROAD ATTRIBUTE ENRICHMENT — Surface, Curvature, Lighting, Sidewalk
# ─────────────────────────────────────────────────────────────────────────────

# OSM surface tag → Roadway Surface Type (golden column)
OSM_SURFACE_MAP = {
    "asphalt":  "2. Blacktop, Asphalt, Bituminous",
    "paved":    "2. Blacktop, Asphalt, Bituminous",
    "concrete": "1. Concrete",
    "gravel":   "4. Slag, Gravel, Stone",
    "dirt":     "5. Dirt",
    "unpaved":  "4. Slag, Gravel, Stone",
    "sand":     "5. Dirt",
    "paving_stones": "3. Brick or Block",
    "cobblestone":   "3. Brick or Block",
}

# Curvature → Roadway Alignment (golden column)
# curvature = road_length / straight_line_distance (1.0 = straight)
# OSM curvature ratio threshold: 1.15 = road is 15% longer than straight-line
# Fallback only — used when HPMS curve_class unavailable (~5% of crashes)
# Calibrated to FHWA benchmark: 25-30% of crashes occur on curves nationwide
def derive_roadway_alignment(curvature):
    """Derive CrashLens Roadway Alignment from OSM curvature ratio.

    FALLBACK ONLY — used when HPMS curve_class is not available.
    curvature = road_length / straight_line_distance (1.0 = perfectly straight).

    Threshold calibrated against HPMS curve_class cross-reference:
    - Ratio <= 1.15: HPMS A/B (tangent/slight) — STRAIGHT
    - Ratio 1.15-1.40: HPMS C (moderate) — CURVE
    - Ratio > 1.40: HPMS D/E (sharp/very sharp) — SHARP CURVE
    """
    # OSM curvature ratio threshold: 1.15 = road 15% longer than straight-line.
    # Fallback only — HPMS curve_class covers ~95% of crashes.
    # Calibrated to FHWA: curves = 5-10% of roads, 25-30% of crashes.
    if curvature <= 1.15:
        return "1. Straight - Level"
    elif curvature <= 1.40:
        return "2. Curve - Level"
    else:
        return "4. Grade - Curve"

def parse_maxspeed_mph(maxspeed_str):
    """Extract numeric speed in mph from OSM maxspeed tag."""
    if not maxspeed_str or maxspeed_str == 'nan':
        return None
    s = str(maxspeed_str).strip().split(';')[0].strip()
    is_kmh = 'km/h' in s or 'kmh' in s
    s = s.replace(' mph', '').replace(' km/h', '').replace(' kmh', '').strip()
    try:
        val = int(float(s))
        if is_kmh or val > 120:
            val = round(val * 0.621371)
        return val
    except (ValueError, TypeError):
        return None

# ─────────────────────────────────────────────────────────────────────────────
#  TIER 1: CONTRIBUTING CIRCUMSTANCE → FLAG DERIVATION
#  These mappings derive boolean flag columns from the state's
#  "Primary Contributing Circumstance" field. Works for any state.
# ─────────────────────────────────────────────────────────────────────────────

# Keywords in contributing circumstance → CrashLens flag columns
CIRCUMSTANCE_TO_FLAGS = {
    "Distracted?": [
        "distract", "inattenti", "cell phone", "texting", "electronic device",
        "passenger distract", "outside distract", "eating", "grooming",
    ],
    "Drowsy?": [
        "drowsy", "asleep", "fell asleep", "fatigued",
        # NOTE: "fatigue" alone is excluded because many states combine
        # "distraction or fatigue" — use only strong fatigue indicators
    ],
    "Speed?": [
        "speed", "exceeding", "too fast", "racing", "aggressive",
    ],
    "Animal Related?": [
        "animal", "deer", "wildlife", "elk", "moose", "horse",
    ],
    "Hitrun?": [
        "hit and run", "hit-and-run", "hitrun", "hit & run", "left scene",
        "fled", "fleeing",
    ],
}

# Combined flag: Distraction + Fatigue often in same field
# (Delaware: "Driver inattention, distraction, or fatigue")
COMBINED_DISTRACTION_FATIGUE_KEYWORDS = [
    "inattention, distraction, or fatigue",
    "inattention/distraction",
]


# ─────────────────────────────────────────────────────────────────────────────
#  TIER 1: INTERSECTION CLUSTERING (GPS-based)
#  Detect intersections by finding GPS coordinate clusters
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_meters(lat1, lon1, lat2, lon2):
    """Haversine distance in meters between two GPS points."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_crash_clusters(lats, lons, radius_m=30.0, min_crashes=3):
    """
    Find GPS crash clusters (potential intersections) using spatial proximity.
    Returns list of (center_lat, center_lon, crash_count, crash_indices).
    """
    n = len(lats)
    if n == 0:
        return []

    # Grid-based pre-filter for O(n) instead of O(n²)
    grid_size = radius_m / 111000  # approx degrees
    grid = defaultdict(list)
    for i in range(n):
        gx = int(lats[i] / grid_size)
        gy = int(lons[i] / grid_size)
        grid[(gx, gy)].append(i)

    clusters = []
    visited = set()

    for i in range(n):
        if i in visited:
            continue
        gx = int(lats[i] / grid_size)
        gy = int(lons[i] / grid_size)

        # Check 3x3 grid neighborhood
        neighbors = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for j in grid.get((gx + dx, gy + dy), []):
                    if j not in visited:
                        dist = _haversine_meters(lats[i], lons[i], lats[j], lons[j])
                        if dist <= radius_m:
                            neighbors.append(j)

        if len(neighbors) >= min_crashes:
            clat = sum(lats[j] for j in neighbors) / len(neighbors)
            clon = sum(lons[j] for j in neighbors) / len(neighbors)
            clusters.append((clat, clon, len(neighbors), neighbors))
            visited.update(neighbors)

    return clusters


# ─────────────────────────────────────────────────────────────────────────────
#  TIER 2: OSM ROAD NETWORK MATCHING
#  Downloads state road network via osmnx, builds KD-tree, matches crashes
# ─────────────────────────────────────────────────────────────────────────────

def _load_or_download_road_network(state_name, state_abbr, cache_dir="cache"):
    """
    Load cached road network or download from OSM using osmnx.
    Returns a GeoDataFrame of road edges with attributes.
    """
    cache_path = Path(cache_dir) / f"{state_abbr.lower()}_roads.parquet"

    if cache_path.exists():
        print(f"    Loading cached road network: {cache_path}")
        return pd.read_parquet(cache_path)

    # ── Import osmnx separately so we don't mask internal ImportErrors ──
    try:
        import osmnx as ox
    except ImportError:
        print("    osmnx not installed — Tier 2 OSM enrichment skipped")
        print("    Install: pip install osmnx")
        return None

    try:
        print(f"    Downloading {state_name} road network from OSM (this takes 2-10 min)...")

        # Download drivable road network for the state
        G = ox.graph_from_place(
            f"{state_name}, United States",
            network_type="drive",
            simplify=True,
        )
        # Convert to GeoDataFrame of edges
        edges = ox.graph_to_gdfs(G, nodes=True, edges=True)
        nodes_gdf, edges_gdf = edges

        # Extract key attributes
        road_data = []
        for idx, row in edges_gdf.iterrows():
            u, v, key = idx
            u_node = nodes_gdf.loc[u]
            v_node = nodes_gdf.loc[v]

            # Midpoint of edge
            mid_lat = (u_node.y + v_node.y) / 2
            mid_lon = (u_node.x + v_node.x) / 2

            highway = row.get("highway", "")
            if isinstance(highway, list):
                highway = highway[0]

            name = row.get("name", "")
            if isinstance(name, list):
                name = name[0] if name else ""

            ref = row.get("ref", "")
            if isinstance(ref, list):
                ref = ref[0] if ref else ""

            road_data.append({
                "u_node": u,
                "v_node": v,
                "u_lat": u_node.y,
                "u_lon": u_node.x,
                "v_lat": v_node.y,
                "v_lon": v_node.x,
                "mid_lat": mid_lat,
                "mid_lon": mid_lon,
                "highway": highway or "",
                "name": name or "",
                "ref": ref or "",
                "oneway": str(row.get("oneway", "")),
                "lanes": str(row.get("lanes", "")),
                "maxspeed": str(row.get("maxspeed", "")),
                "length_m": float(row.get("length", 0)),
                "bridge": str(row.get("bridge", "")),
                "tunnel": str(row.get("tunnel", "")),
            })

        road_df = pd.DataFrame(road_data)

        # Also extract intersection nodes. Keep both directed degree (legacy)
        # and undirected streets_per_node (MIRE-correct physical approach
        # count). Filter is permissive (deg>=3 or spn>=3) for backward compat.
        node_degrees = dict(G.degree())
        try:
            spn_dict = ox.stats.streets_per_node(G)
        except Exception:
            spn_dict = {}
            for _n_id in G.nodes():
                nbrs = set()
                for _u, _v, _k in G.out_edges(_n_id, keys=True):
                    if _v != _n_id:
                        nbrs.add(_v)
                for _u, _v, _k in G.in_edges(_n_id, keys=True):
                    if _u != _n_id:
                        nbrs.add(_u)
                spn_dict[_n_id] = len(nbrs)

        intersections = []
        for node_id, degree in node_degrees.items():
            spn = int(spn_dict.get(node_id, 0) or 0)
            if degree >= 3 or spn >= 3:
                n = nodes_gdf.loc[node_id]
                intersections.append({
                    "node_id": node_id,
                    "lat": n.y,
                    "lon": n.x,
                    "degree": degree,
                    "streets_per_node": spn,
                })
        intersection_df = pd.DataFrame(intersections) if intersections else pd.DataFrame()

        # Cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        road_df.to_parquet(cache_path, index=False)
        if len(intersection_df) > 0:
            int_path = Path(cache_dir) / f"{state_abbr.lower()}_intersections.parquet"
            intersection_df.to_parquet(int_path, index=False)

        print(f"    Cached {len(road_df):,} road segments, {len(intersection_df):,} intersections")
        return road_df

    except ImportError as e:
        print(f"    OSM download failed — missing dependency: {e}")
        return None
    except Exception as e:
        print(f"    OSM download error: {e}")
        import traceback
        traceback.print_exc()
        return None


def _build_kdtree(lats, lons):
    """Build a KD-tree from lat/lon arrays for fast nearest-neighbor lookup."""
    import numpy as np
    from scipy.spatial import KDTree
    mid_lat = sum(lats) / len(lats)
    lon_scale = math.cos(math.radians(mid_lat))
    points = np.column_stack([
        np.array(lats, dtype=np.float64) * 111000,
        np.array(lons, dtype=np.float64) * 111000 * lon_scale,
    ])
    return KDTree(points), lon_scale


# Chunk size for KDTree queries — keeps peak memory under ~500MB per query.
# 566K rows at once = ~2GB peak. 100K chunks = ~350MB peak.
KDTREE_CHUNK_SIZE = 100_000


def _chunked_kdtree_query(tree, points, k=1):
    """Query KDTree in chunks to prevent OOM on large datasets (566K+ rows).
    
    GitHub Actions runners have 7GB RAM. Processing 566K crash points × 
    75K+ reference points at once can spike to ~4GB. Chunking keeps peak
    memory manageable.
    """
    import numpy as np
    n = len(points)
    if n <= KDTREE_CHUNK_SIZE:
        return tree.query(points, k=k)

    all_dists = np.empty(n, dtype=np.float64)
    all_idxs = np.empty(n, dtype=np.intp)

    for start in range(0, n, KDTREE_CHUNK_SIZE):
        end = min(start + KDTREE_CHUNK_SIZE, n)
        chunk = points[start:end]
        d, i = tree.query(chunk, k=k)
        all_dists[start:end] = d
        all_idxs[start:end] = i

    return all_dists, all_idxs


def _make_crash_points(lats, lons, lon_scale):
    """Convert lat/lon lists to numpy array for KDTree (memory-efficient)."""
    import numpy as np
    return np.column_stack([
        np.array(lats, dtype=np.float64) * 111000,
        np.array(lons, dtype=np.float64) * 111000 * lon_scale,
    ])


def _match_crashes_to_roads(crash_lats, crash_lons, road_df, max_dist_m=100):
    """
    Match each crash GPS point to the nearest road segment using KD-tree.
    Returns (distances, indices, valid_count) — caller reads road_df.iloc[idx] directly.
    Avoids building 300K+ intermediate Python dicts (~300MB saved).
    """
    if road_df is None or len(road_df) == 0:
        return np.array([]), np.array([]), 0

    try:
        from scipy.spatial import KDTree
    except ImportError:
        print("    scipy not installed — KD-tree matching unavailable")
        return np.array([]), np.array([]), 0

    road_lats = road_df["mid_lat"].values.tolist()
    road_lons = road_df["mid_lon"].values.tolist()

    mid_lat = sum(road_lats) / max(len(road_lats), 1)
    lon_scale = math.cos(math.radians(mid_lat))

    road_points = np.column_stack([
        np.array(road_lats, dtype=np.float64) * 111000,
        np.array(road_lons, dtype=np.float64) * 111000 * lon_scale,
    ])
    tree = KDTree(road_points)

    crash_points = _make_crash_points(crash_lats, crash_lons, lon_scale)
    distances, indices = _chunked_kdtree_query(tree, crash_points, k=1)
    del road_points, crash_points
    matched = int((distances <= max_dist_m).sum())

    return distances, indices, matched


def _match_crashes_to_intersections(crash_lats, crash_lons, state_abbr, cache_dir="cache"):
    """Match each crash GPS to its nearest real intersection node (STRtree).

    Replaces the old segment-inherited classification (which tagged every
    crash on a segment that touched an intersection — over-counting Delaware
    at-intersection by ~30 percentage points). This function:

      1. Loads `cache/{abbr}_intersections.parquet` (fall back to .parquet.gz).
      2. Filters to REAL intersection nodes (streets_per_node >= 3 if present,
         else directed degree >= 6 — degree >= 6 corresponds to a 3-road
         T-intersection in osmnx's MultiDiGraph).
      3. Projects nodes + crashes into a flat-earth metric plane (meters)
         using the same lat/lon scaling used elsewhere in this module so
         distances come out directly in meters.
      4. Builds a `shapely.strtree.STRtree` of the projected node Points and
         queries the nearest node per crash via `query_nearest(..., return_
         distance=True)` (shapely >= 2.0).
      5. Tags any crash within INTERSECTION_THRESHOLD_M of a real node as
         "at intersection" with the matched node's streets_per_node, and
         everything else as "1. Not at Intersection" (streets_per_node = 0).

    Returns: dict {crash_index → {node_id, distance_m, distance_ft,
                                  intersection_type, streets_per_node}}.
    Returns {} on any failure (missing cache, empty cache, exception) so an
    intersection-cache problem degrades to "no fill" rather than killing
    enrichment.
    """
    int_path = Path(cache_dir) / f"{state_abbr.lower()}_intersections.parquet"
    if not int_path.exists():
        int_path = Path(cache_dir) / f"{state_abbr.lower()}_intersections.parquet.gz"
    if not int_path.exists():
        print(f"    [Node match] Intersection cache not found: "
              f"{state_abbr.lower()}_intersections.parquet[.gz]")
        return {}

    try:
        from shapely.geometry import Point
        from shapely.strtree import STRtree

        # --- Load intersection cache ---
        if str(int_path).endswith(".gz"):
            import gzip
            import io
            with gzip.open(int_path, "rb") as f:
                int_df = pd.read_parquet(io.BytesIO(f.read()))
        else:
            int_df = pd.read_parquet(int_path)

        if len(int_df) == 0:
            print(f"    [Node match] Intersection cache empty: {int_path.name}")
            return {}

        # --- Filter to REAL intersection nodes ---
        # Prefer undirected streets_per_node (MIRE-correct physical street
        # count); fall back to directed degree (osmnx MultiDiGraph each
        # two-way road contributes 2 edges, so a 3-road T-intersection = 6).
        if "streets_per_node" in int_df.columns:
            int_df = int_df[
                pd.to_numeric(int_df["streets_per_node"], errors="coerce")
                .fillna(0).astype(int) >= 3
            ].reset_index(drop=True)
        elif "degree" in int_df.columns:
            int_df = int_df[
                pd.to_numeric(int_df["degree"], errors="coerce")
                .fillna(0).astype(int) >= 6
            ].reset_index(drop=True)
        else:
            print(f"    [Node match] Cache missing both streets_per_node and "
                  f"degree columns: {int_path.name}")
            return {}

        if len(int_df) == 0:
            print(f"    [Node match] No real intersection nodes after filter")
            return {}

        # Pre-extract node arrays in index order for STRtree result mapping
        node_lats = int_df["lat"].values.astype(np.float64)
        node_lons = int_df["lon"].values.astype(np.float64)
        node_ids = int_df["node_id"].values

        has_spn = "streets_per_node" in int_df.columns
        if has_spn:
            node_spn = pd.to_numeric(
                int_df["streets_per_node"], errors="coerce"
            ).fillna(0).astype(int).values
        else:
            # Derive a plausible spn from directed degree: deg <= 7 → 3,
            # deg <= 9 → 4, else 5. Matches the previous fallback mapping.
            deg = pd.to_numeric(
                int_df["degree"], errors="coerce"
            ).fillna(0).astype(int).values
            node_spn = np.where(deg <= 7, 3, np.where(deg <= 9, 4, 5))

        # --- Flat-earth projection (meters) ---
        # Use the mean latitude of the node set so the longitudinal scale is
        # stable across the whole state. This is the same projection used
        # elsewhere in this module (see _build_kdtree, _make_crash_points)
        # and is accurate to well under 50 m anywhere in the contiguous US.
        mid_lat = float(np.nanmean(node_lats))
        lon_scale = math.cos(math.radians(mid_lat))
        M = 111000.0  # ~meters per degree of latitude

        node_x = node_lats * M
        node_y = node_lons * M * lon_scale

        # --- Build STRtree once per call ---
        node_points = [Point(node_x[i], node_y[i]) for i in range(len(int_df))]
        tree = STRtree(node_points)

        # --- Project crashes into the same plane ---
        crash_lats_arr = np.asarray(crash_lats, dtype=np.float64)
        crash_lons_arr = np.asarray(crash_lons, dtype=np.float64)
        crash_x = crash_lats_arr * M
        crash_y = crash_lons_arr * M * lon_scale

        # Skip crashes with missing/zero GPS — they get no node match
        valid_mask = (
            np.isfinite(crash_x)
            & np.isfinite(crash_y)
            & (crash_lats_arr != 0)
            & (crash_lons_arr != 0)
        )
        valid_idx = np.where(valid_mask)[0]
        if len(valid_idx) == 0:
            print(f"    [Node match] No valid crash GPS points")
            return {}

        # --- Vectorised nearest-node query (shapely >= 2.0) ---
        # `all_matches=False` is critical here: with the default True the
        # tree returns one row per geometric tie, so a crash equidistant
        # from two nodes yields two pair entries — which would corrupt
        # our 1:1 valid_idx → tree_idx mapping. all_matches=False
        # guarantees exactly one (input_idx, tree_idx) pair per input
        # geometry, returned in input order, so pair_arr[1] is directly
        # the per-crash tree index.
        crash_points = np.array(
            [Point(crash_x[i], crash_y[i]) for i in valid_idx],
            dtype=object,
        )
        try:
            pair_arr, dists = tree.query_nearest(
                crash_points, return_distance=True, all_matches=False
            )
            tree_idxs = pair_arr[1]
        except TypeError:
            # Older shapely without the all_matches kwarg / vectorised
            # query_nearest. Fall back to a per-point loop. requirements.txt
            # pins shapely>=2.0 so this branch is defensive only.
            tree_idxs = np.empty(len(crash_points), dtype=np.int64)
            dists = np.empty(len(crash_points), dtype=np.float64)
            for i in range(len(crash_points)):
                idx = tree.nearest(crash_points[i])
                tree_idxs[i] = int(idx)
                dists[i] = crash_points[i].distance(node_points[idx])

        # --- Build the result dict ---
        matches = {}
        within = 0
        beyond = 0
        for k, ci in enumerate(valid_idx):
            d_m = float(dists[k])
            tree_i = int(tree_idxs[k])
            if d_m <= INTERSECTION_THRESHOLD_M:
                spn = int(node_spn[tree_i])
                if spn >= 5:
                    int_type = "5. Five-Point, or More"
                elif spn == 4:
                    int_type = "4. Four Approaches"
                elif spn == 3:
                    int_type = "3. Three Approaches"
                else:
                    # Shouldn't happen — filter above requires spn>=3 — but
                    # be defensive in case the fallback degree mapping
                    # produced something odd.
                    int_type = "1. Not at Intersection"
                    spn = 0
                within += 1
            else:
                int_type = "1. Not at Intersection"
                spn = 0
                beyond += 1

            matches[int(ci)] = {
                "node_id":          int(node_ids[tree_i]),
                "distance_m":       round(d_m, 1),
                "distance_ft":      round(d_m * 3.28084),
                "intersection_type": int_type,
                "streets_per_node": spn,
            }

        total = within + beyond
        if total > 0:
            print(f"    [Node match] {within:,}/{total:,} "
                  f"({within/total*100:.1f}%) within "
                  f"{INTERSECTION_THRESHOLD_M}m of a real intersection node "
                  f"(beyond: {beyond:,}); cache: {int_path.name}, "
                  f"real nodes: {len(int_df):,}")

        return matches

    except Exception as e:
        print(f"    [Node match] Intersection matching error: {e}")
        traceback.print_exc()
        return {}


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN ENRICHER CLASS
# ─────────────────────────────────────────────────────────────────────────────

class CrashEnricher:
    """
    Universal crash data enricher. Works for any state.

    Usage:
        enricher = CrashEnricher(state_fips="10", state_abbr="DE", state_name="Delaware")
        df = enricher.enrich_all(df)

    Enrichment tiers:
        Tier 1: Self-enrichment (always runs, zero dependencies)
        Tier 2: OSM road matching (needs osmnx + scipy, cached after first run)
    """

    def __init__(self, state_fips, state_abbr, state_name=None, cache_dir="cache",
                 circumstance_col="PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION",
                 private_property_col="COLLISION ON PRIVATE PROPERTY"):
        self.state_fips = state_fips
        self.state_abbr = state_abbr
        self.state_name = state_name or state_abbr
        self.cache_dir = cache_dir
        self.circumstance_col = circumstance_col
        self.private_property_col = private_property_col
        self.stats = {}
        self.not_tracked_flags = self._load_not_tracked_flags()

    def _load_not_tracked_flags(self):
        """Load the {ABBR}_NOT_TRACKED_FLAGS set from the state normalize module.

        Flags listed there are ones the source state does NOT track. They must
        stay NULL (empty string) in output rather than being force-defaulted to
        "No" — a "No" for an untracked field is a lie.
        """
        abbr = (self.state_abbr or "").lower()
        if not abbr:
            return set()

        repo_root = Path(__file__).resolve().parent
        state_dir_name = (self.state_name or "").lower().replace(" ", "_")
        candidates = [
            repo_root / "states" / state_dir_name / f"{abbr}_normalize.py",
            repo_root / f"{abbr}_normalize.py",
        ]

        for mod_file in candidates:
            if not mod_file.exists():
                continue
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location(f"{abbr}_normalize", mod_file)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                attr = f"{abbr.upper()}_NOT_TRACKED_FLAGS"
                if hasattr(mod, attr):
                    flags = set(getattr(mod, attr))
                    if flags:
                        print(f"    Not-tracked flags for {abbr}: {sorted(flags)}")
                    return flags
            except Exception as e:
                print(f"    ⚠️  Could not load not-tracked flags from {mod_file.name}: {e}")
            break
        return set()

    def enrich_all(self, df, skip_tier2=False):
        """Run enrichment: Tier1 (self) → Road Inventory (single spatial join).

        The road inventory replaces the old 4-tier system (HPMS/OSM/POI/Federal)
        with one pre-built database containing all 300+ columns per road segment.
        """
        t0 = time.time()
        print(f"\n  {'='*55}")
        print(f"  CrashLens Universal Enricher | {self.state_name} ({self.state_abbr})")
        print(f"  {'='*55}")

        df = self.enrich_tier1(df)

        if not skip_tier2:
            ri_path = Path(self.cache_dir) / f"{self.state_abbr.lower()}_road_inventory.parquet.gz"
            if ri_path.exists():
                try:
                    from road_inventory_enricher import enrich_from_road_inventory
                    df = enrich_from_road_inventory(
                        df, self.state_abbr, self.cache_dir)
                    gc.collect()
                except ImportError:
                    print(f"\n  ⚠️  road_inventory_enricher.py not found — Tier 1 only")
                except Exception as e:
                    print(f"\n  ⚠️  Road inventory enrichment failed: {e}")
                    print(f"    Continuing with Tier 1 only")
            else:
                print(f"\n  ⚠️  Road inventory not found: {ri_path}")
                print(f"    Build with: python build_road_inventory.py --state {self.state_abbr.lower()}")

        # ── Node-based intersection classification (source of truth) ──
        # Measures crash GPS → nearest real intersection node distance via
        # shapely STRtree and tags as "at intersection" only within
        # INTERSECTION_THRESHOLD_M (50 m). Overwrites any prior Intersection
        # Type value (segment-inherited values were over-counting at-int by
        # ~30 percentage points). Fills `node_intersection_type`,
        # `node_streets_per_node`, `nearest_node_id`, `node_distance_m`,
        # `node_distance_ft` on every crash with a usable GPS fix.
        print(f"\n  [Node match] Classifying intersections via "
              f"node distance (≤{INTERSECTION_THRESHOLD_M}m, STRtree)...")
        crash_lats_arr = pd.to_numeric(
            df.get("y", pd.Series(dtype=float)), errors="coerce"
        ).fillna(0).values
        crash_lons_arr = pd.to_numeric(
            df.get("x", pd.Series(dtype=float)), errors="coerce"
        ).fillna(0).values
        node_matches = _match_crashes_to_intersections(
            crash_lats_arr, crash_lons_arr, self.state_abbr, self.cache_dir
        )
        if node_matches:
            # Ensure all target columns exist with object dtype so the
            # bulk assigns below stay string/object-typed (matches the
            # rest of the pipeline's pyarrow-safe convention).
            for col in (
                "Intersection Type",
                "node_intersection_type",
                "nearest_node_id",
                "node_distance_m",
                "node_distance_ft",
                "node_streets_per_node",
            ):
                if col not in df.columns:
                    df[col] = pd.Series("", index=df.index, dtype=object)

            # Build positional arrays once, then bulk-assign
            match_idx = np.fromiter(node_matches.keys(), dtype=np.int64)
            int_types = np.array(
                [node_matches[i]["intersection_type"] for i in match_idx],
                dtype=object,
            )
            node_ids = np.array(
                [str(node_matches[i]["node_id"]) for i in match_idx],
                dtype=object,
            )
            d_m = np.array(
                [str(node_matches[i]["distance_m"]) for i in match_idx],
                dtype=object,
            )
            d_ft = np.array(
                [str(node_matches[i]["distance_ft"]) for i in match_idx],
                dtype=object,
            )
            spn_arr = np.array(
                [str(node_matches[i]["streets_per_node"]) for i in match_idx],
                dtype=object,
            )

            # Map positional crash indices → df labels (df is currently
            # using its original index here, since road_inventory_enricher
            # restores it before returning).
            label_idx = df.index.to_numpy()[match_idx]

            # Overwrite Intersection Type for ALL matched crashes — node
            # result is the source of truth, segment-inherited tags are
            # discarded.
            df.loc[label_idx, "Intersection Type"] = int_types
            df.loc[label_idx, "node_intersection_type"] = int_types
            df.loc[label_idx, "nearest_node_id"] = node_ids
            df.loc[label_idx, "node_distance_m"] = d_m
            df.loc[label_idx, "node_distance_ft"] = d_ft
            df.loc[label_idx, "node_streets_per_node"] = spn_arr
            gc.collect()
        else:
            print(f"    [Node match] No matches returned — falling back to "
                  f"segment/cluster Intersection Type (cache missing or empty)")

        # ── Derive Node from OSM intersection node IDs ──
        if "osm_u_node" in df.columns and "Node" in df.columns:
            # Only fill Node for crashes at intersections with empty Node
            mask = (
                (df["Node"].fillna("").astype(str).str.strip() == "") &
                (df.get("is_intersection", pd.Series(dtype=str)).fillna("") == "Yes") &
                (df["osm_u_node"].notna())
            )
            df.loc[mask, "Node"] = df.loc[mask, "osm_u_node"].astype(str)
            node_filled = mask.sum()
            print(f"    Node derived from OSM: {node_filled:,} crashes")

        # Also fill Node for non-intersection crashes using road segment node
        if "osm_u_node" in df.columns and "Node" in df.columns:
            mask2 = (
                (df["Node"].fillna("").astype(str).str.strip() == "") &
                (df["osm_u_node"].notna())
            )
            df.loc[mask2, "Node"] = df.loc[mask2, "osm_u_node"].astype(str)
            node_filled2 = mask2.sum()
            if node_filled2 > 0:
                print(f"    Node derived from OSM (segments): {node_filled2:,} crashes")

        # ── Default remaining empty Intersection Type ──
        # After node-based classification, the only empty cells should be
        # crashes with no usable GPS (rare). Default them to "Not at
        # Intersection" rather than leaving the column blank.
        if "Intersection Type" in df.columns:
            empty_int = df["Intersection Type"].fillna("").astype(str).str.strip().isin(
                ["", "Not Applicable", "nan", "None"])
            if empty_int.any():
                df.loc[empty_int, "Intersection Type"] = "1. Not at Intersection"
                print(f"    Intersection Type: {empty_int.sum():,} defaulted to Not at Intersection")

        # ── Intersection Type distribution (diagnostic + assertion) ──
        # With node-based classification (50 m STRtree), Delaware should
        # land in the 55–65% at-intersection band per FHWA expectations.
        # The 40–80% assertion below catches inverted-comparison bugs and
        # cache-misses that would silently ship bad numbers.
        AT_INT_LABELS = (
            "3. Three Approaches",
            "4. Four Approaches",
            "5. Five-Point, or More",
        )
        if "Intersection Type" in df.columns:
            total = len(df)
            int_dist = df["Intersection Type"].value_counts(dropna=False)
            at_int = int(sum(int_dist.get(lbl, 0) for lbl in AT_INT_LABELS))
            not_int = int(int_dist.get("1. Not at Intersection", 0))
            unfilled = total - at_int - not_int
            if total > 0:
                print(f"\n    Intersection Type ({total:,} crashes):")
                print(f"      At intersection:     {at_int:>7,} "
                      f"({at_int/total*100:.1f}%)")
                print(f"      Not at intersection: {not_int:>7,} "
                      f"({not_int/total*100:.1f}%)")
                if unfilled:
                    print(f"      Unfilled / other:    {unfilled:>7,} "
                          f"({unfilled/total*100:.1f}%)")
                for val, cnt in sorted(int_dist.items(), key=lambda kv: str(kv[0])):
                    print(f"        {cnt:>7,} ({cnt/total*100:5.1f}%): {val}")
                # Detect whether the node-based matcher actually populated
                # node_distance_m for at least one crash. The two assertions
                # below only run when node-based classification produced
                # results — if the intersection cache was missing the
                # function already printed a loud warning and we'd rather
                # let the enricher complete (degraded) than fail the whole
                # batch. Cache-misses are caught by the missing-cache log,
                # not the assertion.
                node_match_ran = False
                if "node_distance_m" in df.columns:
                    nd_check = pd.to_numeric(
                        df["node_distance_m"], errors="coerce")
                    node_match_ran = bool(nd_check.notna().any())

                if node_match_ran:
                    print(f"      Method: node-based "
                          f"({INTERSECTION_THRESHOLD_M} m threshold, "
                          f"streets_per_node, STRtree)")
                else:
                    print(f"      Method: fallback (intersection cache "
                          f"missing — node-based matcher did not run; "
                          f"see [Node match] warning above)")

                # ── Sanity assertions (only when node-based ran) ──
                if node_match_ran:
                    # 1. At-intersection rate within FHWA-plausible band.
                    at_pct = at_int / total * 100.0
                    if not (40.0 <= at_pct <= 80.0):
                        raise AssertionError(
                            f"Intersection Type at-intersection rate "
                            f"{at_pct:.1f}% is outside the 40–80% sanity "
                            f"window (expected ~55–65% for Delaware). "
                            f"Likely causes: inverted distance comparison, "
                            f"wrong streets_per_node filter, or stale "
                            f"intersection cache. Method: node-based "
                            f"({INTERSECTION_THRESHOLD_M} m threshold)."
                        )
                    # 2. Mean distance check: at-intersection rows should
                    #    have mean node distance below the threshold and
                    #    not-at-intersection rows should have mean distance
                    #    above it. Catches an inverted comparison silently.
                    nd = pd.to_numeric(
                        df["node_distance_m"], errors="coerce")
                    it = df["Intersection Type"].fillna("").astype(str)
                    at_mask = it.isin(AT_INT_LABELS) & nd.notna()
                    not_mask = (it == "1. Not at Intersection") & nd.notna()
                    if at_mask.any() and not_mask.any():
                        at_mean = float(nd[at_mask].mean())
                        not_mean = float(nd[not_mask].mean())
                        print(f"      Mean node_distance_m — "
                              f"at-int: {at_mean:.1f}, "
                              f"not-int: {not_mean:.1f}")
                        if at_mean > INTERSECTION_THRESHOLD_M:
                            raise AssertionError(
                                f"At-intersection mean node_distance_m "
                                f"({at_mean:.1f}) exceeds threshold "
                                f"({INTERSECTION_THRESHOLD_M}) — likely "
                                f"inverted distance comparison."
                            )
                        if not_mean < INTERSECTION_THRESHOLD_M:
                            raise AssertionError(
                                f"Not-at-intersection mean node_distance_m "
                                f"({not_mean:.1f}) is below threshold "
                                f"({INTERSECTION_THRESHOLD_M}) — likely "
                                f"inverted distance comparison."
                            )

        # ── Intersection Analysis (derived AFTER enrichment) ──
        df = self._derive_intersection_analysis(df)

        # ── Post-enrichment canonicalization & honest derivations ──
        df = self._canonicalize_post_enrichment(df)

        # ── Curvature diagnostic — Roadway Alignment distribution ──
        if "Roadway Alignment" in df.columns:
            ra_dist = df["Roadway Alignment"].fillna("").value_counts()
            total = len(df)
            print(f"\n    Roadway Alignment distribution ({total:,} total):")
            for val, cnt in ra_dist.items():
                if val:
                    print(f"      {cnt:>7,} ({cnt/total*100:5.1f}%): {val}")

        elapsed = time.time() - t0
        print(f"\n  Enrichment complete in {elapsed:.1f}s")
        self._print_fill_report(df)
        return df

    # ─── TIER 1: Self-Enrichment ─────────────────────────────────────────

    def enrich_tier1(self, df):
        """
        Tier 1: Derive missing columns from existing crash data fields.
        No external data needed. Works for any state.
        """
        print("\n  [Tier 1] Self-enrichment from existing fields...")

        # Snapshot not-tracked flags as a sorted list so the
        # post-enrichment canonicalizer can null them out after every
        # downstream step, no matter which one re-defaulted a "No".
        self._not_tracked_flags = sorted(self.not_tracked_flags)

        # 1. Contributing Circumstance → Flag columns
        df = self._derive_flags_from_circumstance(df)

        # 2. Private Property → Mainline?
        df = self._derive_mainline_from_private_property(df)

        # 3. Collision Type + Pedestrian/Bike → cross-validate flags
        df = self._cross_validate_flags(df)

        # 4. GPS clustering → intersection proximity detection
        df = self._detect_intersections_from_clusters(df)

        # 5. Severity → K/A/B/C People count estimation
        df = self._estimate_kabco_people(df)

        # 6. Route name pattern → Functional Class (if RTE Name exists)
        df = self._derive_fc_from_route_name(df)

        return df

    def _derive_flags_from_circumstance(self, df):
        """Derive Distracted?, Drowsy?, Speed?, Animal Related?, Hitrun? from contributing circumstance."""
        circ_col = self.circumstance_col

        # Find the circumstance column (case-insensitive search)
        actual_col = None
        for c in df.columns:
            if c.upper().strip() == circ_col.upper().strip():
                actual_col = c
                break
        if actual_col is None:
            # Try finding it among extra columns
            for c in df.columns:
                cl = c.upper().strip()
                if "CONTRIBUTING" in cl and "CIRCUMSTANCE" in cl:
                    actual_col = c
                    break
        if actual_col is None:
            print("    No contributing circumstance column found — skipping flag derivation")
            return df

        circ_lower = df[actual_col].fillna("").str.strip().str.lower()
        derived_count = 0

        for flag, keywords in CIRCUMSTANCE_TO_FLAGS.items():
            # Honor state-specific NULL policy: leave untracked flags empty,
            # never lie with a default "No".
            if flag in self.not_tracked_flags:
                if flag in df.columns:
                    df[flag] = ""
                continue

            if flag in df.columns and df[flag].fillna("").str.strip().ne("").any():
                existing_yes = (df[flag] == "Yes").sum()
                if existing_yes > 0:
                    continue  # Don't overwrite existing data

            mask = pd.Series(False, index=df.index)
            for kw in keywords:
                mask |= circ_lower.str.contains(kw, na=False)

            df[flag] = mask.map({True: "Yes", False: "No"})
            yes_count = mask.sum()
            if yes_count > 0:
                derived_count += yes_count
                print(f"    {flag}: {yes_count} 'Yes' derived from contributing circumstance")

        # ── Speed investigation logging (diagnostic) ──
        if "Speed?" in df.columns and self.state_abbr.lower() == "de":
            speed_yes = (df["Speed?"] == "Yes").sum()
            speed_total = len(df)
            print(f"    Speed? diagnostic: {speed_yes:,}/{speed_total:,} "
                  f"({speed_yes/speed_total*100:.1f}%)")
            if actual_col:
                speed_kw = "speed|fast|aggressive|exceeding|racing"
                speed_mask = circ_lower.str.contains(speed_kw, na=False)
                if speed_mask.any():
                    print(f"    Speed-matching circumstances ({speed_mask.sum():,} rows):")
                    speed_vals = df.loc[speed_mask, actual_col].value_counts().head(10)
                    for val, cnt in speed_vals.items():
                        print(f"      {cnt:>6,}: {val}")

                all_circs = df[actual_col].value_counts().head(20)
                print(f"    Top 20 contributing circumstances:")
                for val, cnt in all_circs.items():
                    print(f"      {cnt:>6,}: {val}")

        # Handle combined Distraction+Fatigue fields (e.g., Delaware)
        # Conservative: combined field → Distracted?=Yes only
        # Drowsy? only from strong standalone fatigue indicators
        for combo_kw in COMBINED_DISTRACTION_FATIGUE_KEYWORDS:
            combo_mask = circ_lower.str.contains(combo_kw, na=False)
            if combo_mask.any():
                df.loc[combo_mask, "Distracted?"] = "Yes"
                # DON'T flag Drowsy? from combined field — too imprecise
                print(f"    Note: {combo_mask.sum()} rows have combined distraction/fatigue coding — "
                      f"only Distracted? flagged (Drowsy? requires standalone fatigue indicator)")

        self.stats["tier1_flags_derived"] = derived_count
        return df

    def _derive_mainline_from_private_property(self, df):
        """Derive Mainline? from Collision on Private Property: N (public road) = potential mainline."""
        pp_col = self.private_property_col
        actual_col = None
        for c in df.columns:
            if c.upper().strip() == pp_col.upper().strip():
                actual_col = c
                break

        if actual_col is None:
            return df

        existing = df.get("Mainline?", pd.Series("", index=df.index))
        needs_fill = existing.fillna("").str.strip() == ""

        if needs_fill.any():
            pp_val = df[actual_col].fillna("").str.strip().str.upper()
            # Private property = NOT mainline; public road = potentially mainline
            # (will be refined by Tier 2 FC-based derivation)
            df.loc[needs_fill, "Mainline?"] = pp_val.map(
                {"N": "Yes", "Y": "No"}
            ).fillna("No")
            print(f"    Mainline?: derived from Private Property ({(pp_val == 'N').sum()} public road crashes)")

        return df

    def _cross_validate_flags(self, df):
        """Cross-validate flag columns against collision type and each other."""
        fixed = 0

        # If Pedestrian?=No but Collision Type contains pedestrian → fix
        if "Pedestrian?" in df.columns and "Collision Type" in df.columns:
            ped_collision = df["Collision Type"].fillna("").str.contains("12\\. Ped|ped", case=False, na=False)
            ped_no = df["Pedestrian?"].fillna("") == "No"
            fix_mask = ped_collision & ped_no
            if fix_mask.any():
                df.loc[fix_mask, "Pedestrian?"] = "Yes"
                fixed += fix_mask.sum()

        # If Bike?=No but Collision Type contains bicyclist → fix
        if "Bike?" in df.columns and "Collision Type" in df.columns:
            bike_collision = df["Collision Type"].fillna("").str.contains("13\\. Bicycl|bicycl", case=False, na=False)
            bike_no = df["Bike?"].fillna("") == "No"
            fix_mask = bike_collision & bike_no
            if fix_mask.any():
                df.loc[fix_mask, "Bike?"] = "Yes"
                fixed += fix_mask.sum()

        if fixed:
            print(f"    Cross-validation: {fixed} flag corrections (Ped/Bike vs Collision Type)")
        self.stats["tier1_cross_validated"] = fixed
        return df

    def _detect_intersections_from_clusters(self, df):
        """DEPRECATED: GPS clustering removed — replaced by road inventory
        intersection node proximity in road_inventory_enricher.py.

        Old approach tagged 85.7%+ of crashes as intersection (wrong).
        New approach uses actual OSM intersection nodes (streets_per_node >= 3
        when available, else intersection_degree >= 6; within 30m of crash)
        from the road inventory.
        """
        return df

    def _estimate_kabco_people(self, df):
        """Estimate K/A/B/C people counts from severity (1 per crash as minimum)."""
        if "Crash Severity" not in df.columns:
            return df

        for sev, col in [("K", "K_People"), ("A", "A_People"), ("B", "B_People"), ("C", "C_People")]:
            if col in df.columns:
                needs_fill = df[col].fillna("").str.strip().isin(["", "0"])
                sev_match = df["Crash Severity"] == sev
                fill_mask = needs_fill & sev_match
                if fill_mask.any():
                    df.loc[fill_mask, col] = "1"  # Minimum 1 person

        return df

    def _derive_fc_from_route_name(self, df):
        """If RTE Name is populated, derive Functional Class from route name patterns."""
        if "RTE Name" not in df.columns or "Functional Class" not in df.columns:
            return df

        needs_fc = df["Functional Class"].fillna("").str.strip() == ""
        has_rte = df["RTE Name"].fillna("").str.strip() != ""
        fill_mask = needs_fc & has_rte

        if not fill_mask.any():
            return df

        filled = 0
        for pattern, fc in ROUTE_PREFIX_TO_FC.items():
            match_mask = fill_mask & df["RTE Name"].str.upper().str.match(pattern, na=False)
            if match_mask.any():
                df.loc[match_mask, "Functional Class"] = fc
                filled += match_mask.sum()
                fill_mask &= ~match_mask  # Don't double-fill

        if filled:
            print(f"    Route name → FC: {filled} rows derived from route name patterns")

        return df


    def _derive_intersection_analysis(self, df):
        """
        Derive Intersection Analysis from Intersection Type + Ownership.
        Frontend expects: 'Not Intersection', 'Urban Intersection', 'DOT Intersection'.

        Logic:
          - Intersection Type = '1. Not at Intersection' → 'Not Intersection'
          - Ownership = '1. State Hwy Agency' (DOT road) → 'DOT Intersection'
          - Everything else at intersection → 'Urban Intersection'

        Always re-derives from the CURRENT Intersection Type — the node
        matcher overwrites Intersection Type after road_inventory_enricher
        populates Intersection Analysis, so any pre-existing value would
        be stale. Vectorized over the full frame.
        """
        if "Intersection Type" not in df.columns:
            return df
        if "Intersection Analysis" not in df.columns:
            df["Intersection Analysis"] = ""
        if "Ownership" not in df.columns:
            df["Ownership"] = ""

        it = df["Intersection Type"].fillna("").astype(str).str.strip()
        ow = df["Ownership"].fillna("").astype(str).str.strip()

        not_int = it.isin(["1. Not at Intersection", "", "nan", "None"])
        state_hw = ow == "1. State Hwy Agency"

        df.loc[not_int, "Intersection Analysis"] = "Not Intersection"
        df.loc[~not_int & state_hw, "Intersection Analysis"] = "DOT Intersection"
        df.loc[~not_int & ~state_hw, "Intersection Analysis"] = "Urban Intersection"

        dot_ct = (df["Intersection Analysis"] == "DOT Intersection").sum()
        urb_ct = (df["Intersection Analysis"] == "Urban Intersection").sum()
        not_ct = (df["Intersection Analysis"] == "Not Intersection").sum()
        print(f"\n  [Intersection Analysis] {len(df):,} rows derived:")
        print(f"    DOT Intersection: {dot_ct:,}, Urban Intersection: {urb_ct:,}, Not Intersection: {not_ct:,}")

        return df

    def _canonicalize_post_enrichment(self, df):
        """State-agnostic post-enrichment canonicalization & honest derivations.

        Runs after road-inventory enrichment and intersection analysis. Only
        fills from definitive, already-computed signals. Where truth isn't
        knowable, leaves NULL rather than guessing.

        Covers:
            Fix 2  — RTE Name fallback from road-inventory route columns
            Fix 4  — School Zone from resolved_school_zone / Near_School_1500ft
            Fix 7  — RoadDeparture Type from definitive indicators only
            Fix 8  — Persons Injured = A_People + B_People + C_People (exact)
            Fix 9  — Pedestrians Killed/Injured from ped flag + severity (min estimate)
            Fix 10 — Relation To Roadway from is_ramp only (no defaulting)
        """
        n = len(df)
        if n == 0:
            return df

        print(f"\n  [Post-Enrichment Canonicalization]")

        # ── Roadway Alignment from curve_class (FHWA authority — OVERWRITE) ──
        if "curve_class" in df.columns:
            cc = pd.to_numeric(df["curve_class"], errors="coerce").fillna(0).astype(int)
            has_cc = cc > 0
            if has_cc.any():
                if "Roadway Alignment" not in df.columns:
                    df["Roadway Alignment"] = ""
                # Preserve ramp designations
                is_ramp = df["Roadway Alignment"].fillna("").astype(str).str.contains(
                    "Ramp", na=False)
                overwrite = has_cc & ~is_ramp

                # FHWA: 1-2 = Straight/Slight (base condition), 3 = Moderate, 4-5 = Sharp+
                cc_straight = overwrite & (cc <= 2)
                cc_curve = overwrite & (cc == 3)
                cc_sharp = overwrite & (cc >= 4)

                before_curve = df["Roadway Alignment"].fillna("").str.contains(
                    "Curve", case=False, na=False).sum()

                df.loc[cc_straight, "Roadway Alignment"] = "1. Straight - Level"
                df.loc[cc_curve, "Roadway Alignment"] = "2. Curve - Level"
                df.loc[cc_sharp, "Roadway Alignment"] = "4. Grade - Curve"

                after_curve = df["Roadway Alignment"].fillna("").str.contains(
                    "Curve", case=False, na=False).sum()
                print(f"    Roadway Alignment (curve_class): {overwrite.sum():,} set "
                      f"(curve: {before_curve:,} -> {after_curve:,})")

            # Terrain fallback where curve_class unavailable
            if "hpms_terrain_type" in df.columns:
                tt = pd.to_numeric(df["hpms_terrain_type"], errors="coerce").fillna(0).astype(int)
                ra_empty = df["Roadway Alignment"].fillna("").astype(str).str.strip().isin(
                    ["", "nan", "None"])
                terrain_fill = ra_empty & (tt > 0)
                if terrain_fill.any():
                    for t_val, align_val in HPMS_TERRAIN_TO_ALIGNMENT.items():
                        df.loc[terrain_fill & (tt == t_val), "Roadway Alignment"] = align_val
                    print(f"    Roadway Alignment (terrain fallback): {terrain_fill.sum():,}")

        # ── Fix 2: RTE Name fallback from road-inventory route columns ──
        if "RTE Name" in df.columns:
            rte = df["RTE Name"].fillna("").astype(str).str.strip()
            rte_before = (rte != "").sum()
            empty = rte == ""
            fallback_cols = [
                "hpms_route_name", "ri_route_name", "route_name",
                "map_road_name", "osm_name", "road_name", "dot_road_name",
            ]
            for col in fallback_cols:
                if col in df.columns and empty.any():
                    fb = df[col].fillna("").astype(str).str.strip()
                    m = empty & (fb != "")
                    filled = m.sum()
                    if filled > 0:
                        df.loc[m, "RTE Name"] = fb[m]
                        print(f"    RTE Name: +{filled:,} from {col}")
                        empty = df["RTE Name"].fillna("").astype(str).str.strip() == ""
            rte_after = (df["RTE Name"].fillna("").astype(str).str.strip() != "").sum()
            print(f"    RTE Name: {rte_before:,} → {rte_after:,} ({rte_after/n*100:.0f}%)")

        # ── Fix 4: School Zone from federal proximity only ──
        # Mapillary `map_school_zone` tags everything near a school-zone
        # sign (inflates to ~13.7%); `resolved_school_zone` is a derived
        # blend of the same noisy signal. Federal `Near_School_1500ft`
        # is the accurate ground-truth proximity flag.
        if "School Zone" in df.columns and "Near_School_1500ft" in df.columns:
            df["School Zone"] = "No"
            near_school = df["Near_School_1500ft"].fillna("").astype(str).str.strip() == "Yes"
            df.loc[near_school, "School Zone"] = "Yes"
            sz_yes = near_school.sum()
            print(f"    School Zone: 0 → {sz_yes:,} (federal proximity only)")

        # ── Fix 7: RoadDeparture Type from definitive indicators only ──
        if "RoadDeparture Type" in df.columns:
            rd = df["RoadDeparture Type"].fillna("").astype(str).str.strip()
            empty = rd == ""
            if empty.any():
                ct = df.get("Collision Type", pd.Series("", index=df.index)) \
                    .fillna("").astype(str).str.lower()
                rel = df.get("Relation To Roadway", pd.Series("", index=df.index)) \
                    .fillna("").astype(str).str.lower()
                definitive = empty & (
                    ct.str.contains("ran off", na=False)
                    | ct.str.contains("overturn", na=False)
                    | rel.str.contains("off roadway", na=False)
                )
                align = df.get("Roadway Alignment", pd.Series("", index=df.index)) \
                    .fillna("").astype(str).str.lower()
                on_curve = align.str.contains("curve", na=False)

                curve_dep = definitive & on_curve
                straight_dep = definitive & ~on_curve
                df.loc[curve_dep, "RoadDeparture Type"] = "Curve Departure"
                df.loc[straight_dep, "RoadDeparture Type"] = "Straight Departure"
                dep_count = definitive.sum()
                if dep_count > 0:
                    print(f"    RoadDeparture Type: {dep_count:,} definitive departures "
                          f"(curve={curve_dep.sum():,}, straight={straight_dep.sum():,})")

        # ── Fix 8: Persons Injured = A + B + C (exact math) ──
        # Only runs if Persons Injured is wholly empty AND at least one of
        # A/B/C_People exists. Using pd.Series(0, index=df.index) as default
        # keeps arithmetic vectorized even when some KABCO columns are missing.
        if "Persons Injured" in df.columns:
            pi = pd.to_numeric(df["Persons Injured"], errors="coerce").fillna(0)
            kabco_cols_present = [c for c in ("A_People", "B_People", "C_People") if c in df.columns]
            if pi.sum() == 0 and kabco_cols_present:
                zero = pd.Series(0, index=df.index)
                a = pd.to_numeric(df["A_People"], errors="coerce").fillna(0) if "A_People" in df.columns else zero
                b = pd.to_numeric(df["B_People"], errors="coerce").fillna(0) if "B_People" in df.columns else zero
                c = pd.to_numeric(df["C_People"], errors="coerce").fillna(0) if "C_People" in df.columns else zero
                total = (a + b + c).astype(int)
                has_injury = total > 0
                if has_injury.any():
                    df.loc[has_injury, "Persons Injured"] = total[has_injury].astype(str)
                    print(f"    Persons Injured: {has_injury.sum():,} from "
                          f"{'+'.join(kabco_cols_present)} (exact)")

        # ── Fix 9: Pedestrians Killed/Injured (documented minimum estimates) ──
        if "Pedestrian?" in df.columns and "Crash Severity" in df.columns:
            ped = df["Pedestrian?"].fillna("").astype(str).str.strip() == "Yes"

            if "Pedestrians Killed" in df.columns and ped.any():
                pk = pd.to_numeric(df["Pedestrians Killed"], errors="coerce").fillna(0)
                if pk.sum() == 0:
                    fatal_ped = ped & (df["Crash Severity"] == "K")
                    if fatal_ped.any():
                        df.loc[fatal_ped, "Pedestrians Killed"] = "1"
                        print(f"    Pedestrians Killed: {fatal_ped.sum():,} derived (ped+K, minimum estimate)")

            if "Pedestrians Injured" in df.columns and ped.any():
                pi_ped = pd.to_numeric(df["Pedestrians Injured"], errors="coerce").fillna(0)
                if pi_ped.sum() == 0:
                    inj_ped = ped & df["Crash Severity"].isin(["A", "B", "C"])
                    if inj_ped.any():
                        df.loc[inj_ped, "Pedestrians Injured"] = "1"
                        print(f"    Pedestrians Injured: {inj_ped.sum():,} derived (ped+A/B/C, minimum estimate)")

        # ── Fix 10: Relation To Roadway from road inventory ramps only ──
        if "Relation To Roadway" in df.columns:
            rel = df["Relation To Roadway"].fillna("").astype(str).str.strip()
            empty = rel == ""
            if empty.any() and "is_ramp" in df.columns:
                is_ramp = df["is_ramp"].fillna("").astype(str).str.strip()
                ramp_mask = empty & (is_ramp == "Yes")
                if ramp_mask.any():
                    df.loc[ramp_mask, "Relation To Roadway"] = "10. On/Off Ramp"
                    still_empty = (df["Relation To Roadway"].fillna("").astype(str).str.strip() == "").sum()
                    print(f"    Relation To Roadway: +{ramp_mask.sum():,} ramps, {still_empty:,} left unknown")

        # ── Null out not-tracked flags (accuracy rule: NULL is honest) ──
        not_tracked = getattr(self, '_not_tracked_flags', [])
        for flag in not_tracked:
            if flag in df.columns:
                df[flag] = ""
        if not_tracked:
            print(f"    Not-tracked flags nulled: {', '.join(not_tracked)}")

        return df

    # ─── REPORTING ───────────────────────────────────────────────────────

    def _print_fill_report(self, df):
        """Print before/after column fill rates."""
        key_columns = [
            # Golden columns (Tier 2 road matching)
            "RTE Name", "Functional Class", "Facility Type", "Ownership",
            "SYSTEM", "Mainline?", "Roadway Description", "Intersection Type",
            "Intersection Name", "Node", "Node Offset (ft)",
            # Golden columns (new fills from road attributes)
            "Roadway Surface Type", "Roadway Alignment", "Max Speed Diff",
            "Traffic Control Type", "Intersection Analysis",
            # HPMS-only columns
            "AADT", "Through_Lanes", "Access_Control",
            "Lane_Width_ft", "Median_Width_ft", "Shoulder_Width_ft",
            "AADT_Trucks", "Design_Speed_mph",
            # Tier 1 flags
            "Distracted?", "Drowsy?", "Speed?", "Animal Related?", "Hitrun?",
            "K_People", "A_People", "Area Type",
            # New: road infrastructure columns
            "Has_Street_Lighting", "Has_Sidewalk", "Has_Bike_Lane", "On_Bridge",
            # New: POI proximity columns (ft-based)
            "Near_Bar_1500ft", "Near_School_1000ft", "Near_Crossing_100ft",
            "Near_Parking_150ft", "Near_Rail_Xing_150ft", "Nearest_Hospital_mi",
            # New: Federal safety data (Tier 2c)
            "School_Enrollment_Nearest", "Bridge_Condition", "Bridge_Year_Built",
            "Rail_Warning_Device", "Rail_Trains_Per_Day", "Near_Transit_500ft",
        ]
        total = len(df)
        print(f"\n  {'─'*55}")
        print(f"  Column Fill Report ({total:,} rows)")
        print(f"  {'─'*55}")
        print(f"  {'Column':<28} {'Filled':>8} {'%':>8}")
        print(f"  {'─'*44}")

        for col in key_columns:
            if col in df.columns:
                filled = (df[col].fillna("").str.strip() != "").sum()
                pct = filled / max(total, 1) * 100
                marker = "***" if pct > 0 and pct < 100 else ("   " if pct == 100 else "---")
                print(f"  {col:<28} {filled:>8,} {pct:>7.1f}% {marker}")

        print(f"  {'─'*55}")


# ─────────────────────────────────────────────────────────────────────────────
#  CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CrashLens Universal Crash Data Enricher")
    parser.add_argument("--input", "-i", required=True, help="Normalized CSV path")
    parser.add_argument("--output", "-o", default=None, help="Output CSV path")
    parser.add_argument("--state-fips", required=True, help="State FIPS code")
    parser.add_argument("--state-abbr", required=True, help="State abbreviation")
    parser.add_argument("--state-name", default=None, help="State full name")
    parser.add_argument("--cache-dir", default="cache", help="Cache directory for OSM data")
    parser.add_argument("--skip-osm", action="store_true", help="Skip Tier 2 OSM enrichment")
    parser.add_argument("--circumstance-col", default="PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION",
                        help="Contributing circumstance column name")
    args = parser.parse_args()

    # Auto-detect input format (parquet.gz or CSV)
    inp = args.input
    if inp.endswith(('.parquet.gz', '.parquet')):
        df = pd.read_parquet(inp).astype(str).replace({"nan": "", "None": "", "<NA>": ""})
    else:
        df = pd.read_csv(inp, dtype=str, low_memory=False)
    enricher = CrashEnricher(
        state_fips=args.state_fips,
        state_abbr=args.state_abbr,
        state_name=args.state_name,
        cache_dir=args.cache_dir,
        circumstance_col=args.circumstance_col,
    )
    df = enricher.enrich_all(df, skip_tier2=args.skip_osm)

    # Output as parquet.gz (default) or CSV
    out = args.output or inp.replace(".csv", "_enriched.parquet.gz").replace(".parquet.gz", "_enriched.parquet.gz")
    if out.endswith(('.parquet.gz', '.parquet')):
        df.to_parquet(out, engine='pyarrow', compression='snappy', index=False)
    else:
        df.to_csv(out, index=False)
    print(f"\n  Output: {out}")
