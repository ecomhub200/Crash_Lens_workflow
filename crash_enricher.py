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
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


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
def derive_roadway_alignment(curvature):
    """Derive CrashLens Roadway Alignment from computed curvature ratio.
    Note: curvature only captures horizontal curves, not vertical grade.
    Grade info comes from HPMS terrain_type (Tier 3, runs first).
    """
    if curvature <= 1.05:
        return "1. Straight - Level"
    elif curvature <= 1.15:
        return "2. Curve - Level"       # slight curve, no grade info
    elif curvature <= 1.40:
        return "2. Curve - Level"       # moderate curve
    else:
        return "2. Curve - Level"       # sharp curve (no grade from OSM)

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

        # Also extract intersection nodes (degree ≥ 3)
        node_degrees = dict(G.degree())
        intersections = []
        for node_id, degree in node_degrees.items():
            if degree >= 3:
                n = nodes_gdf.loc[node_id]
                intersections.append({
                    "node_id": node_id,
                    "lat": n.y,
                    "lon": n.x,
                    "degree": degree,
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
    """Match crashes to nearest intersection node. Returns dict of crash_index → node info."""
    int_path = Path(cache_dir) / f"{state_abbr.lower()}_intersections.parquet"
    if not int_path.exists():
        return {}

    try:
        from scipy.spatial import KDTree
        int_df = pd.read_parquet(int_path)
        if len(int_df) == 0:
            return {}

        int_lats = int_df["lat"].values.tolist()
        int_lons = int_df["lon"].values.tolist()

        mid_lat = sum(int_lats) / len(int_lats)
        lon_scale = math.cos(math.radians(mid_lat))

        int_points = _make_crash_points(int_lats, int_lons, lon_scale)
        tree = KDTree(int_points)

        crash_points = _make_crash_points(crash_lats, crash_lons, lon_scale)
        distances, indices = _chunked_kdtree_query(tree, crash_points, k=1)
        del int_points, crash_points

        matches = {}
        for i, (dist, idx) in enumerate(zip(distances, indices)):
            node = int_df.iloc[idx]
            degree = int(node["degree"])

            # Derive Intersection Type from node degree
            if dist > 50:  # More than 50m from intersection
                int_type = "1. Not at Intersection"
            elif degree == 3:
                int_type = "3. Three Approaches"
            elif degree == 4:
                int_type = "4. Four Approaches"
            elif degree >= 5:
                int_type = "5. Five-Point, or More"
            else:
                int_type = "2. Two Approaches"

            matches[i] = {
                "node_id":          int(node["node_id"]),
                "distance_ft":      round(dist * 3.28084),  # meters → feet
                "intersection_type": int_type,
                "degree":           degree,
            }

        return matches

    except Exception as e:
        print(f"    Intersection matching error: {e}")
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

        # ── Intersection Analysis (derived AFTER enrichment) ──
        df = self._derive_intersection_analysis(df)

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
        """Use GPS clustering to detect intersection-proximity crashes."""
        if "x" not in df.columns or "y" not in df.columns:
            return df

        try:
            lons = pd.to_numeric(df["x"], errors="coerce")
            lats = pd.to_numeric(df["y"], errors="coerce")
            valid = lats.notna() & lons.notna() & (lats != 0) & (lons != 0)

            if valid.sum() < 10:
                return df

            valid_lats = lats[valid].tolist()
            valid_lons = lons[valid].tolist()
            valid_indices = df.index[valid].tolist()

            clusters = detect_crash_clusters(valid_lats, valid_lons, radius_m=30, min_crashes=3)

            # Mark crashes near cluster centers as "at intersection"
            cluster_count = 0
            for clat, clon, count, member_indices in clusters:
                for mi in member_indices:
                    actual_idx = valid_indices[mi]
                    # Only fill if Intersection Type is currently blank
                    if df.at[actual_idx, "Intersection Type"] in ("", "Not Applicable", None):
                        df.at[actual_idx, "Intersection Type"] = "4. Four Approaches"  # conservative default
                        cluster_count += 1

            if cluster_count:
                print(f"    GPS clustering: {len(clusters)} potential intersections detected, "
                      f"{cluster_count} crashes tagged")
            self.stats["tier1_intersection_clusters"] = len(clusters)

        except Exception as e:
            print(f"    GPS clustering error: {e}")

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
        """
        if "Intersection Analysis" not in df.columns:
            df["Intersection Analysis"] = ""

        ia_filled = 0
        for idx in df.index:
            if df.at[idx, "Intersection Analysis"]:
                continue  # already set by state data

            int_type = str(df.at[idx, "Intersection Type"]).strip() if "Intersection Type" in df.columns else ""
            ownership = str(df.at[idx, "Ownership"]).strip() if "Ownership" in df.columns else ""

            if int_type == "1. Not at Intersection" or not int_type or int_type in ("nan", "None"):
                df.at[idx, "Intersection Analysis"] = "Not Intersection"
            elif ownership == "1. State Hwy Agency":
                df.at[idx, "Intersection Analysis"] = "DOT Intersection"
            else:
                df.at[idx, "Intersection Analysis"] = "Urban Intersection"
            ia_filled += 1

        if ia_filled > 0:
            dot_ct = (df["Intersection Analysis"] == "DOT Intersection").sum()
            urb_ct = (df["Intersection Analysis"] == "Urban Intersection").sum()
            not_ct = (df["Intersection Analysis"] == "Not Intersection").sum()
            print(f"\n  [Intersection Analysis] {ia_filled:,} rows derived:")
            print(f"    DOT Intersection: {dot_ct:,}, Urban Intersection: {urb_ct:,}, Not Intersection: {not_ct:,}")

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
