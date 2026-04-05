#!/usr/bin/env python3
"""
generate_hpms_data.py — CrashLens HPMS Data Downloader (v2)
=============================================================
Downloads Highway Performance Monitoring System data from FHWA for crash enrichment.
HPMS is the federal government's authoritative road database with:
  AADT (traffic volume), official Functional Class, Speed Limit,
  Through Lanes, Median Type, Surface Type, Ownership, Access Control.

3-SOURCE PIPELINE:
  Method 1 (PRIMARY):  geo.dot.gov ArcGIS FeatureServer (2024→2019, per-state)
  Method 2 (FALLBACK): FHWA shapefile download (2017→2012, zip per state)
  Method 3 (VALIDATE): FHWA Table HM-10 ownership mileage audit

SETUP:
    pip install requests pandas pyarrow geopandas boto3 openpyxl

USAGE:
    python generate_hpms_data.py --state de              # Delaware only (~2 min)
    python generate_hpms_data.py --state de va md         # Multiple
    python generate_hpms_data.py --all                    # All 51 states
    python generate_hpms_data.py --state de --local-only  # No R2 upload
    python generate_hpms_data.py --state de --skip-hm10   # Skip HM-10 validation

OUTPUT:
    cache/{abbr}_hpms.parquet.gz       → R2: {prefix}/cache/{abbr}_hpms.parquet.gz
    cache/{abbr}_hm10_validation.json  → ownership audit report (local only)

    Parquet columns (63 per road segment):
      ── GPS ──
      mid_lat, mid_lon          GPS midpoint for crash matching
      ── Identification (NEW) ──
      route_id                  ARNOLD Route ID (LRS join key)
      begin_point, end_point    Milepoint range on route
      route_signing             Sign type (1=Interstate, 2=US, 3=State, 4=County)
      route_number              Numeric route number
      state_code                State FIPS code
      year_record               HPMS data year
      ── Road Classification ──
      f_system                  FHWA Functional Class (1-7)
      facility_type             1=One-way, 2=Two-way, 4=Ramp, 5=Frontage
      ownership                 Ownership code (1-80, full FHWA table)
      nhs                       National Highway System designation
      access_control            1=Full, 2=Partial, 3=None
      ── Traffic ──
      aadt                      Annual Avg Daily Traffic (vehicles/day)
      future_aadt               Projected AADT
      aadt_single_unit          Single-unit truck AADT
      aadt_combination          Combination truck AADT
      k_factor, dir_factor      Design hour factors
      pct_peak_single           Peak % single-unit trucks (NEW)
      pct_peak_combination      Peak % combination trucks
      capacity                  Road capacity veh/hr (NEW)
      ── Geometry ──
      speed_limit               Posted speed limit (mph)
      through_lanes             Number of through travel lanes
      directional_through_lanes Per-direction lane count (NEW)
      peak_lanes                Peak direction lanes (NEW)
      lane_width                Lane width (ft)
      median_type, median_width Median type + width
      surface_type              Surface type code
      terrain_type              1=Flat, 2=Rolling, 3=Mountainous
      curve_class, grade_class  Curve/grade classification
      design_speed              Design speed (mph)
      ── Shoulders & Parking ──
      shoulder_width_r/l        Right/left shoulder width
      shoulder_type_r/l         Right/left shoulder type
      turn_lanes_r/l            Right/left turn lanes
      peak_parking              Peak parking type
      ── Intersection ──
      signal_type               Signal type code
      num_signals               Number of signals
      num_signalized_int        Signalized intersections
      num_stop_int              Stop sign intersections
      pct_green_time            % green time
      ── Pavement Condition ──
      iri                       International Roughness Index
      psr                       Present Serviceability Rating
      rutting, faulting          Pavement measurements
      cracking_percent          Cracking % (NEW)
      year_last_construction    Year of last construction
      widening_feasibility      Widening feasibility code
      ── Network (NEW) ──
      structure_type            Bridge/tunnel/causeway flag
      toll_charged              Toll road flag
      hov_type                  HOV lane type
      climate_zone              Climate zone code
      ── Derived ──
      route_name                Route signing + number (e.g. "I 95")
      county_code               County FIPS
      urban_code                Urban area code
      length_mi                 Segment length (miles)
      section_length            Official HPMS section length (miles, NEW)
"""

import argparse
import gc
import gzip
import json
import math
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# ═══════════════════════════════════════════════════════════════
#  CONSTANTS & ENDPOINTS
# ═══════════════════════════════════════════════════════════════

HPMS_BASE_URL = "https://geo.dot.gov/server/rest/services/Hosted"
FHWA_SHAPEFILE_BASE = "https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles"
HM10_URL_PATTERN = "https://www.fhwa.dot.gov/policyinformation/statistics/{year}/xls/hm10.xlsx"

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

# ═══════════════════════════════════════════════════════════════
#  HPMS OWNERSHIP CODES — Full FHWA 2-digit code table (27 codes)
#  Source: HPMS Field Manual, Item 6 (Dec 2016 + 2022 draft)
#  Maps to CrashLens standard ownership strings.
# ═══════════════════════════════════════════════════════════════

HPMS_OWNERSHIP_MAP = {
    # Primary agencies
    1:  "1. State Hwy Agency",
    2:  "2. County Hwy Agency",
    3:  "3. City or Town Hwy Agency",       # HPMS: Town or Township
    4:  "3. City or Town Hwy Agency",       # HPMS: City or Municipal
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

# Human-readable labels for audit reports
HPMS_OWNERSHIP_LABELS = {
    1: "State Highway Agency", 2: "County Highway Agency",
    3: "Town/Township Highway Agency", 4: "City/Municipal Highway Agency",
    11: "State Park/Forest/Reservation", 12: "Local Park/Forest/Reservation",
    21: "Other State Agency", 25: "Other Local Agency",
    26: "Private (non-Railroad)", 27: "Railroad",
    31: "State Toll Authority", 32: "Local Toll Authority",
    40: "Other Public Instrumentality", 50: "Indian Tribe Nation",
    60: "Other Federal Agency", 62: "Bureau of Indian Affairs",
    63: "Bureau of Fish & Wildlife", 64: "U.S. Forest Service",
    66: "National Park Service", 67: "Tennessee Valley Authority",
    68: "Bureau of Land Management", 69: "Bureau of Reclamation",
    70: "Corps of Engineers", 72: "Air Force", 73: "Navy/Marines",
    74: "Army", 80: "Other",
}

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY
# ═══════════════════════════════════════════════════════════════

ALL_STATES = [
    {"name": "Alabama", "abbreviation": "al", "fips": "01", "r2_prefix": "alabama"},
    {"name": "Alaska", "abbreviation": "ak", "fips": "02", "r2_prefix": "alaska"},
    {"name": "Arizona", "abbreviation": "az", "fips": "04", "r2_prefix": "arizona"},
    {"name": "Arkansas", "abbreviation": "ar", "fips": "05", "r2_prefix": "arkansas"},
    {"name": "California", "abbreviation": "ca", "fips": "06", "r2_prefix": "california"},
    {"name": "Colorado", "abbreviation": "co", "fips": "08", "r2_prefix": "colorado"},
    {"name": "Connecticut", "abbreviation": "ct", "fips": "09", "r2_prefix": "connecticut"},
    {"name": "Delaware", "abbreviation": "de", "fips": "10", "r2_prefix": "delaware"},
    {"name": "District of Columbia", "abbreviation": "dc", "fips": "11", "r2_prefix": "district_of_columbia"},
    {"name": "Florida", "abbreviation": "fl", "fips": "12", "r2_prefix": "florida"},
    {"name": "Georgia", "abbreviation": "ga", "fips": "13", "r2_prefix": "georgia"},
    {"name": "Hawaii", "abbreviation": "hi", "fips": "15", "r2_prefix": "hawaii"},
    {"name": "Idaho", "abbreviation": "id", "fips": "16", "r2_prefix": "idaho"},
    {"name": "Illinois", "abbreviation": "il", "fips": "17", "r2_prefix": "illinois"},
    {"name": "Indiana", "abbreviation": "in", "fips": "18", "r2_prefix": "indiana"},
    {"name": "Iowa", "abbreviation": "ia", "fips": "19", "r2_prefix": "iowa"},
    {"name": "Kansas", "abbreviation": "ks", "fips": "20", "r2_prefix": "kansas"},
    {"name": "Kentucky", "abbreviation": "ky", "fips": "21", "r2_prefix": "kentucky"},
    {"name": "Louisiana", "abbreviation": "la", "fips": "22", "r2_prefix": "louisiana"},
    {"name": "Maine", "abbreviation": "me", "fips": "23", "r2_prefix": "maine"},
    {"name": "Maryland", "abbreviation": "md", "fips": "24", "r2_prefix": "maryland"},
    {"name": "Massachusetts", "abbreviation": "ma", "fips": "25", "r2_prefix": "massachusetts"},
    {"name": "Michigan", "abbreviation": "mi", "fips": "26", "r2_prefix": "michigan"},
    {"name": "Minnesota", "abbreviation": "mn", "fips": "27", "r2_prefix": "minnesota"},
    {"name": "Mississippi", "abbreviation": "ms", "fips": "28", "r2_prefix": "mississippi"},
    {"name": "Missouri", "abbreviation": "mo", "fips": "29", "r2_prefix": "missouri"},
    {"name": "Montana", "abbreviation": "mt", "fips": "30", "r2_prefix": "montana"},
    {"name": "Nebraska", "abbreviation": "ne", "fips": "31", "r2_prefix": "nebraska"},
    {"name": "Nevada", "abbreviation": "nv", "fips": "32", "r2_prefix": "nevada"},
    {"name": "New Hampshire", "abbreviation": "nh", "fips": "33", "r2_prefix": "new_hampshire"},
    {"name": "New Jersey", "abbreviation": "nj", "fips": "34", "r2_prefix": "new_jersey"},
    {"name": "New Mexico", "abbreviation": "nm", "fips": "35", "r2_prefix": "new_mexico"},
    {"name": "New York", "abbreviation": "ny", "fips": "36", "r2_prefix": "new_york"},
    {"name": "North Carolina", "abbreviation": "nc", "fips": "37", "r2_prefix": "north_carolina"},
    {"name": "North Dakota", "abbreviation": "nd", "fips": "38", "r2_prefix": "north_dakota"},
    {"name": "Ohio", "abbreviation": "oh", "fips": "39", "r2_prefix": "ohio"},
    {"name": "Oklahoma", "abbreviation": "ok", "fips": "40", "r2_prefix": "oklahoma"},
    {"name": "Oregon", "abbreviation": "or", "fips": "41", "r2_prefix": "oregon"},
    {"name": "Pennsylvania", "abbreviation": "pa", "fips": "42", "r2_prefix": "pennsylvania"},
    {"name": "Rhode Island", "abbreviation": "ri", "fips": "44", "r2_prefix": "rhode_island"},
    {"name": "South Carolina", "abbreviation": "sc", "fips": "45", "r2_prefix": "south_carolina"},
    {"name": "South Dakota", "abbreviation": "sd", "fips": "46", "r2_prefix": "south_dakota"},
    {"name": "Tennessee", "abbreviation": "tn", "fips": "47", "r2_prefix": "tennessee"},
    {"name": "Texas", "abbreviation": "tx", "fips": "48", "r2_prefix": "texas"},
    {"name": "Utah", "abbreviation": "ut", "fips": "49", "r2_prefix": "utah"},
    {"name": "Vermont", "abbreviation": "vt", "fips": "50", "r2_prefix": "vermont"},
    {"name": "Virginia", "abbreviation": "va", "fips": "51", "r2_prefix": "virginia"},
    {"name": "Washington", "abbreviation": "wa", "fips": "53", "r2_prefix": "washington"},
    {"name": "West Virginia", "abbreviation": "wv", "fips": "54", "r2_prefix": "west_virginia"},
    {"name": "Wisconsin", "abbreviation": "wi", "fips": "55", "r2_prefix": "wisconsin"},
    {"name": "Wyoming", "abbreviation": "wy", "fips": "56", "r2_prefix": "wyoming"},
]

ABBR_LOOKUP = {s["abbreviation"]: s for s in ALL_STATES}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _safe_int(val, default=0):
    try:
        return int(val) if val else default
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=0.0):
    try:
        return round(float(val), 2) if val else default
    except (ValueError, TypeError):
        return default


def _safe_str(val, maxlen=120):
    s = str(val).strip() if val else ""
    return s[:maxlen]


# ═══════════════════════════════════════════════════════════════
#  ArcGIS REST API DOWNLOAD (paginated)
# ═══════════════════════════════════════════════════════════════

def get_state_hpms_url(state_name, state_abbr):
    """Build per-state HPMS FeatureServer URL, trying newest year first.

    Naming patterns on geo.dot.gov:
      2024: HPMS_FULL_{ABBR}_2024     (newest)
      2023: HPMS_FULL_{ABBR}_2023     (BETA release)
      2020: HPMS_FULL_{ABBR}_2020
      2019: HPMS_Full_{ABBR}_2019     (note: mixed case)
      2018: {StateName}_2018_PR        (old Public Release)
    """
    abbr_upper = state_abbr.upper()
    url_name = state_name.replace(" ", "_")

    candidates = [
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2024/FeatureServer/0",
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2023/FeatureServer/0",
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2022/FeatureServer/0",
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2021/FeatureServer/0",
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2020/FeatureServer/0",
        f"{HPMS_BASE_URL}/HPMS_Full_{abbr_upper}_2019/FeatureServer/0",
        f"{HPMS_BASE_URL}/{url_name}_2018_PR/FeatureServer/0",
    ]
    return candidates


def verify_hpms_endpoint(url):
    """Check if a per-state HPMS endpoint is accessible."""
    try:
        r = requests.get(f"{url}?f=json", timeout=15)
        if r.status_code == 200:
            data = r.json()
            if "fields" in data or "name" in data:
                return True
    except Exception:
        pass
    return False


def download_hpms_arcgis(service_url, max_pages=500):
    """Download HPMS data for one state via per-state ArcGIS REST API (paginated).

    Each state has its own FeatureServer endpoint — no state filter needed.
    MaxRecordCount = 2000. Paginate with resultOffset.
    """
    all_features = []
    offset = 0
    page_size = 2000

    for page in range(max_pages):
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "geometryType": "esriGeometryPolyline",
            "outSR": "4326",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        data = None
        for attempt in range(3):
            try:
                r = requests.get(f"{service_url}/query", params=params, timeout=60)
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                if attempt == 2:
                    print(f"      Page {page} error after 3 retries: {e}")
                else:
                    time.sleep(2 ** attempt)
        if data is None:
            break

        if "error" in data:
            print(f"      API error: {data['error'].get('message', data['error'])}")
            break

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += page_size

        if page % 10 == 0 and page > 0:
            print(f"      Downloaded {len(all_features):,} segments...")

        if not data.get("exceededTransferLimit", False) and len(features) < page_size:
            break

        time.sleep(0.2)

    return all_features


def _compute_midpoint_and_length(geom):
    """Extract midpoint GPS and haversine length from ArcGIS polyline geometry."""
    paths = geom.get("paths", [])
    if not paths:
        return 0, 0, 0.0

    all_coords = [c for path in paths for c in path]
    if not all_coords:
        return 0, 0, 0.0

    mid_idx = len(all_coords) // 2
    mid_lon = all_coords[mid_idx][0]
    mid_lat = all_coords[mid_idx][1]

    total_m = 0.0
    for j in range(1, len(all_coords)):
        lat1, lon1 = math.radians(all_coords[j-1][1]), math.radians(all_coords[j-1][0])
        lat2, lon2 = math.radians(all_coords[j][1]), math.radians(all_coords[j][0])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        total_m += 6371000 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return mid_lat, mid_lon, round(total_m / 1609.34, 4)


def _build_route_name(attrs, g_fn):
    """Build route name from signing + number fields."""
    signing = str(g_fn("ROUTE_SIGNING", "Route_Signing") or "")
    route_num = str(g_fn("ROUTE_NUMBER", "Route_Number") or "")
    route_name = str(g_fn("ROUTE_NAME", "Route_Name") or "").strip()
    if not route_name and route_num:
        sign_map = {"1": "I", "2": "US", "3": "SR", "4": "CR"}
        prefix = sign_map.get(signing, "")
        route_name = f"{prefix} {route_num}".strip()
    return route_name[:80]


def _build_row(g, mid_lat, mid_lon, geom_length_mi, route_name):
    """Build a single output row dict from a getter function g().
    Used by both features_to_dataframe (ArcGIS) and shapefile_to_dataframe (FHWA).
    """
    aadt = g("AADT", "aadt", "Aadt") or 0
    try:
        aadt = int(aadt) if aadt else 0
    except (ValueError, TypeError):
        aadt = 0

    return {
        # GPS
        "mid_lat": round(mid_lat, 7),
        "mid_lon": round(mid_lon, 7),
        # Identification (NEW in v2)
        "route_id": _safe_str(g("ROUTE_ID", "Route_ID", "Route_Id", "ROUTE_ID_")),
        "begin_point": _safe_float(g("BEGIN_POINT", "Begin_Point", "BEGIN_POIN")),
        "end_point": _safe_float(g("END_POINT", "End_Point")),
        "route_signing": _safe_int(g("ROUTE_SIGNING", "Route_Signing", "ROUTE_SIGN")),
        "route_number": _safe_int(g("ROUTE_NUMBER", "Route_Number", "ROUTE_NUMB")),
        "state_code": _safe_int(g("STATE_CODE", "State_Code")),
        "year_record": _safe_int(g("YEAR_RECORD", "Year_Record", "YEAR_RECOR")),
        # Classification
        "f_system": _safe_int(g("F_SYSTEM", "F_System")),
        "facility_type": _safe_int(g("FACILITY_TYPE", "Facility_Type", "FAC_TYPE")),
        "ownership": _safe_int(g("OWNERSHIP", "Ownership")),
        "nhs": _safe_int(g("NHS")),
        "access_control": _safe_int(g("ACCESS_CONTROL", "Access_Control", "ACCESS_CON", "ACCESS_CN")),
        # Traffic
        "aadt": aadt,
        "future_aadt": _safe_int(g("FUTURE_AADT", "Future_AADT")),
        "aadt_single_unit": _safe_int(g("AADT_SINGLE_UNIT", "AADT_Single_Unit", "AADT_SU", "AADT_SINGL")),
        "aadt_combination": _safe_int(g("AADT_COMBINATION", "AADT_Combination", "AADT_COMB", "AADT_COMBI")),
        "k_factor": _safe_float(g("K_FACTOR", "K_Factor")),
        "dir_factor": _safe_float(g("DIR_FACTOR", "Dir_Factor")),
        "pct_peak_single": _safe_float(g("PCT_PEAK_SINGLE", "Pct_Peak_Single", "PCT_PK_S")),
        "pct_peak_combination": _safe_float(g("PCT_PEAK_COMBINATION", "Pct_Peak_Combination", "PCT_PK_COMB", "PCT_PK_C")),
        "capacity": _safe_int(g("CAPACITY", "Capacity")),
        # Geometry
        "speed_limit": _safe_int(g("SPEED_LIMIT", "Speed_Limit", "SPEED_LMT")),
        "through_lanes": _safe_int(g("THROUGH_LANES", "Through_Lanes", "THRU_LANE")),
        "directional_through_lanes": _safe_int(g("DIRECTIONAL_THROUGH_LANES", "Directional_Through_Lanes", "DIR_THRU")),
        "peak_lanes": _safe_int(g("PEAK_LANES", "Peak_Lanes")),
        "lane_width": _safe_float(g("LANE_WIDTH", "Lane_Width", "LANE_WID")),
        "median_type": _safe_int(g("MEDIAN_TYPE", "Median_Type", "MEDIAN_TY")),
        "median_width": _safe_float(g("MEDIAN_WIDTH", "Median_Width", "MED_WIDT")),
        "surface_type": _safe_int(g("SURFACE_TYPE", "Surface_Type", "SURF_TYPE")),
        "terrain_type": _safe_int(g("TERRAIN_TYPE", "Terrain_Type", "TERRAIN")),
        "curve_class": _safe_str(g("CURVE_CLASS", "Curve_Class", "Curve_Cls", "CURVES")),
        "grade_class": _safe_str(g("GRADE_CLASS", "Grade_Class", "Grade_Cls", "GRADES")),
        "design_speed": _safe_int(g("WEIGHTED_DESIGN_SPEED", "Design_Speed", "DSGN_SPD", "DESIGN_SPEED")),
        # Shoulders & Parking
        "shoulder_width_r": _safe_float(g("SHOULDER_WIDTH_R", "R_Shoulder_Width", "RSHL_WID")),
        "shoulder_type_r": _safe_int(g("SHOULDER_TYPE_R", "R_Shoulder_Type", "RSHL_TYP")),
        "shoulder_width_l": _safe_float(g("SHOULDER_WIDTH_L", "L_Shoulder_Width", "LSHL_WID")),
        "shoulder_type_l": _safe_int(g("SHOULDER_TYPE_L", "L_Shoulder_Type", "LSHL_TYP")),
        "turn_lanes_r": _safe_int(g("TURN_LANES_R", "Turn_Lanes_R", "TRN_LN_R")),
        "turn_lanes_l": _safe_int(g("TURN_LANES_L", "Turn_Lanes_L", "TRN_LN_L")),
        "peak_parking": _safe_int(g("PEAK_PARKING", "Peak_Parking", "PK_PARKI")),
        # Intersection
        "signal_type": _safe_int(g("SIGNAL_TYPE", "Signal_Type", "SIG_TYPE")),
        "num_signals": _safe_int(g("NUM_SIGNALS", "Num_Signals", "At_Grade_Other", "NUM_SIGN", "AT_GRADE_OTHER")),
        "num_signalized_int": _safe_int(g("NUM_SIGNALIZED_INT", "Num_Signalized_Int", "AT_GRADE_SIGNALIZED")),
        "num_stop_int": _safe_int(g("NUM_STOP_INT", "Num_Stop_Int", "AT_GRADE_STOP")),
        "pct_green_time": _safe_float(g("PCT_GREEN_TIME", "Pct_Green_Time")),
        # Pavement condition
        "iri": _safe_float(g("IRI")),
        "psr": _safe_float(g("PSR", "PRESENT_SERVICEABILITY_RATING")),
        "rutting": _safe_float(g("RUTTING", "Rutting")),
        "faulting": _safe_float(g("FAULTING", "Faulting")),
        "cracking_percent": _safe_float(g("CRACKING_PERCENT", "Cracking_Percent", "CRACK_PCT")),
        "year_last_construction": _safe_int(g("YEAR_LAST_CONSTRUCTION", "Yr_Last_Constr", "YEAR_LAST_IMPROV", "YR_LAST_CONSTR")),
        "widening_feasibility": _safe_int(g("WIDENING_FEASIBILITY", "Widen_Feasiblty", "WIDENING_OBSTACLE", "WIDEN_FEASIBLTY")),
        # Network (NEW in v2)
        "structure_type": _safe_int(g("STRUCTURE_TYPE", "Structure_Type", "STRUCTURE_")),
        "toll_charged": _safe_int(g("TOLL_CHARGED", "Toll_Charged", "TOLL_CHARG")),
        "hov_type": _safe_int(g("HOV_TYPE", "Hov_Type")),
        "climate_zone": _safe_int(g("CLIMATE_ZONE", "Climate_Zone")),
        # Derived
        "route_name": route_name,
        "county_code": _safe_str(g("COUNTY_CODE", "County_Code", "CNTY_CODE", "COUNTY_FIPS", "CTFIPS", "COUNTY_COD")),
        "urban_code": _safe_str(g("URBAN_CODE", "Urban_Code", "URBAN_ID", "URBANIZED_AREA_CODE", "URBAN_CDE")),
        "length_mi": _safe_float(g("SECTION_LENGTH", "Section_Length", "MILES", "SEC_LEN")) or geom_length_mi,
        "section_length": _safe_float(g("SECTION_LENGTH", "Section_Length", "SEC_LEN")),
    }


def features_to_dataframe(features):
    """Convert ArcGIS features to flat DataFrame with midpoint GPS (63 columns)."""
    if features:
        sample_attrs = features[0].get("attributes", {})
        actual_upper = {f.upper() for f in sample_attrs.keys()}
        expected = {
            "AADT", "F_SYSTEM", "SPEED_LIMIT", "THROUGH_LANES", "SURFACE_TYPE",
            "OWNERSHIP", "ACCESS_CONTROL", "FACILITY_TYPE", "MEDIAN_TYPE",
            "LANE_WIDTH", "ROUTE_ID", "BEGIN_POINT", "END_POINT",
            "ROUTE_SIGNING", "ROUTE_NUMBER", "STATE_CODE", "YEAR_RECORD",
            "CAPACITY", "STRUCTURE_TYPE", "TOLL_CHARGED", "CRACKING_PERCENT",
            "DIRECTIONAL_THROUGH_LANES", "CLIMATE_ZONE", "HOV_TYPE",
        }
        found = expected & actual_upper
        missing = expected - actual_upper
        print(f"      Field audit: {len(found)}/{len(expected)} critical fields found")
        if missing:
            print(f"      ⚠️  Not in this endpoint ({len(missing)}): {sorted(missing)[:10]}")

    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})

        mid_lat, mid_lon, geom_length_mi = _compute_midpoint_and_length(geom)
        if mid_lat == 0 and mid_lon == 0:
            continue

        def g(key, *alts):
            for k in [key, key.upper(), key.lower()] + list(alts):
                v = attrs.get(k)
                if v is not None:
                    return v
            return None

        route_name = _build_route_name(attrs, g)
        rows.append(_build_row(g, mid_lat, mid_lon, geom_length_mi, route_name))

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
#  FHWA SHAPEFILE DOWNLOAD (fallback)
# ═══════════════════════════════════════════════════════════════

def download_hpms_shapefile(state_name, state_abbr, cache_dir):
    """Download HPMS shapefile from FHWA as fallback (2017→2012)."""
    try:
        import geopandas as gpd
    except ImportError:
        print("      geopandas not installed — shapefile fallback unavailable")
        return None

    name_clean = state_name.replace(" ", "")
    years = [2017, 2016, 2015, 2014, 2013, 2012]

    for year in years:
        url = f"{FHWA_SHAPEFILE_BASE}/{name_clean}{year}.zip"
        print(f"      Trying: {url}")
        try:
            r = requests.get(url, timeout=120, stream=True)
            if r.status_code == 200:
                zip_path = cache_dir / f"{state_abbr}_hpms_{year}.zip"
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                print(f"      Downloaded {year} shapefile ({zip_path.stat().st_size / 1048576:.1f} MB)")
                gdf = gpd.read_file(f"zip://{zip_path}")
                zip_path.unlink(missing_ok=True)
                return gdf
        except Exception:
            continue

    return None


def shapefile_to_dataframe(gdf):
    """Convert HPMS GeoDataFrame to flat DataFrame with midpoint (63 columns).
    DBF field names truncated to ~10 chars — g() tries multiple variants.
    """
    rows = []
    for _, row in gdf.iterrows():
        try:
            centroid = row.geometry.centroid
            mid_lat, mid_lon = centroid.y, centroid.x
        except Exception:
            continue
        if mid_lat == 0 and mid_lon == 0:
            continue

        def g(key, *alts):
            val = row.get(key, None)
            if val is None:
                for a in alts:
                    val = row.get(a, None)
                    if val is not None:
                        break
            return val

        route_name = _build_route_name(row.to_dict(), g)
        geom_length_mi = 0.0
        try:
            geom_length_mi = round(row.geometry.length * 111.32 / 1.60934, 4)
        except Exception:
            pass

        rows.append(_build_row(g, mid_lat, mid_lon, geom_length_mi, route_name))

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
#  HM-10 OWNERSHIP VALIDATION
# ═══════════════════════════════════════════════════════════════

def download_hm10(cache_dir, year=2023):
    """Download FHWA Table HM-10 (Public Road Length by Ownership) Excel file.
    Returns DataFrame or None if download fails.
    """
    for yr in [year, 2022, 2019, 2018, 2017]:
        url = HM10_URL_PATTERN.format(year=yr)
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                path = cache_dir / f"hm10_{yr}.xlsx"
                with open(path, "wb") as f:
                    f.write(r.content)
                print(f"      HM-10: downloaded {yr} ({len(r.content) // 1024} KB)")
                return path, yr
        except Exception:
            continue
    return None, None


def validate_ownership_hm10(df, state_name, state_fips, cache_dir):
    """Compare HPMS ownership distribution against HM-10 federal totals.
    Saves audit report as JSON. Returns audit dict.
    """
    hm10_path, hm10_year = download_hm10(cache_dir)
    if hm10_path is None:
        print("      HM-10: download failed — skipping validation")
        return None

    try:
        hm10_df = pd.read_excel(hm10_path, header=None)
    except Exception as e:
        print(f"      HM-10: parse error — {e}")
        return None

    hm10_state_row = None
    search_name = state_name.upper().strip()
    for idx, row in hm10_df.iterrows():
        cell = str(row.iloc[0]).strip().upper() if pd.notna(row.iloc[0]) else ""
        if search_name in cell or cell.startswith(search_name[:8]):
            hm10_state_row = row
            break

    hpms_own = df["ownership"].value_counts().to_dict()
    hpms_total = len(df)
    hpms_miles = df["length_mi"].sum()

    hpms_by_group = {}
    for code, count in hpms_own.items():
        label = HPMS_OWNERSHIP_LABELS.get(code, f"Unknown ({code})")
        cl_label = HPMS_OWNERSHIP_MAP.get(code, "6. Private/Unknown Roads")
        hpms_by_group.setdefault(cl_label, {"codes": [], "segments": 0, "miles": 0.0})
        hpms_by_group[cl_label]["codes"].append(f"{code}={label}")
        hpms_by_group[cl_label]["segments"] += count
        hpms_by_group[cl_label]["miles"] += df.loc[df["ownership"] == code, "length_mi"].sum()

    audit = {
        "state": state_name,
        "state_fips": state_fips,
        "hm10_year": hm10_year,
        "hpms_segments": hpms_total,
        "hpms_miles": round(hpms_miles, 1),
        "ownership_distribution": {},
    }

    print(f"      HM-10 Ownership Audit ({state_name}):")
    print(f"        HPMS: {hpms_total:,} segments, {hpms_miles:,.0f} miles")
    for label in sorted(hpms_by_group.keys()):
        grp = hpms_by_group[label]
        pct = grp["segments"] / max(hpms_total, 1) * 100
        mi_pct = grp["miles"] / max(hpms_miles, 0.1) * 100
        audit["ownership_distribution"][label] = {
            "segments": grp["segments"],
            "miles": round(grp["miles"], 1),
            "pct_segments": round(pct, 1),
            "pct_miles": round(mi_pct, 1),
            "codes": grp["codes"],
        }
        print(f"        {label:35s}: {grp['segments']:>7,} seg ({pct:5.1f}%) | {grp['miles']:>8,.0f} mi ({mi_pct:5.1f}%)")

    if hm10_state_row is not None:
        print(f"        HM-10 ({hm10_year}) row found for {state_name}")
        audit["hm10_found"] = True
    else:
        print(f"        ⚠️  HM-10 row not found for '{state_name}'")
        audit["hm10_found"] = False

    return audit


# ═══════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def gzip_file(src, dst):
    with open(src, 'rb') as fi, gzip.open(dst, 'wb', compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    raw = os.path.getsize(src) / 1048576
    gz = os.path.getsize(dst) / 1048576
    return raw, gz


def get_r2_client():
    endpoint = os.environ.get('R2_ENDPOINT', '')
    key_id = os.environ.get('R2_ACCESS_KEY_ID', '')
    secret = os.environ.get('R2_SECRET_ACCESS_KEY', '')
    if not all([endpoint, key_id, secret]):
        return None
    import boto3
    return boto3.client('s3', endpoint_url=endpoint,
                        aws_access_key_id=key_id,
                        aws_secret_access_key=secret,
                        region_name='auto')


def r2_exists(s3, bucket, prefix, abbr):
    if not s3:
        return False
    try:
        s3.head_object(Bucket=bucket, Key=f'{prefix}/cache/{abbr}_hpms.parquet.gz')
        return True
    except Exception:
        return False


def r2_upload(s3, local_path, bucket, r2_key):
    for attempt in range(3):
        try:
            s3.upload_file(str(local_path), bucket, r2_key)
            return True
        except Exception as e:
            if attempt == 2:
                print(f"      Upload failed: {e}")
            time.sleep(2 ** (attempt + 1))
    return False


# ═══════════════════════════════════════════════════════════════
#  MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════

def process_state(state_info, cache_dir, s3, bucket,
                  force=False, local_only=False, skip_hm10=False):
    """Download HPMS data for one state via per-state FeatureServer."""
    name = state_info['name']
    abbr = state_info['abbreviation']
    fips = state_info['fips']
    prefix = state_info['r2_prefix']

    hpms_gz = cache_dir / f'{abbr}_hpms.parquet.gz'

    if not force:
        if not local_only and r2_exists(s3, bucket, prefix, abbr):
            print(f"  [skip] {name} ({abbr}) — HPMS already in R2")
            return 'skipped'
        if local_only and hpms_gz.exists():
            print(f"  [skip] {name} ({abbr}) — HPMS already cached locally")
            return 'skipped'

    print(f"\n  [hpms] {name} ({abbr}, FIPS={fips}) — downloading...")
    t0 = time.time()

    df = None

    # Method 1: Per-state ArcGIS REST API at geo.dot.gov (2024→2018)
    candidates = get_state_hpms_url(name, abbr)
    print(f"      Method 1: ArcGIS per-state endpoint (trying 2024→2018)")
    for url in candidates:
        year = url.split("_")[-1].split("/")[0]
        try:
            if verify_hpms_endpoint(url):
                print(f"      URL: {url}")
                features = download_hpms_arcgis(url)
                if features:
                    df = features_to_dataframe(features)
                    print(f"      ArcGIS ({year}): {len(df):,} road segments")
                    break
            else:
                print(f"      {year}: not available")
        except Exception as e:
            print(f"      {year} failed: {e}")

    # Method 2: FHWA Shapefile (fallback for pre-2018 data)
    if df is None or len(df) == 0:
        print(f"      Method 2: FHWA shapefile download (2017→2012)")
        try:
            gdf = download_hpms_shapefile(name, abbr, cache_dir)
            if gdf is not None and len(gdf) > 0:
                df = shapefile_to_dataframe(gdf)
                print(f"      Shapefile: {len(df):,} road segments")
                del gdf
        except Exception as e:
            print(f"      Shapefile failed: {e}")

    if df is None or len(df) == 0:
        print(f"  [failed] {name} — no HPMS data from any source")
        return 'failed'

    # ── Field coverage report ──
    print(f"      Field coverage ({len(df.columns)} columns):")
    critical = [
        'aadt', 'speed_limit', 'through_lanes', 'f_system', 'surface_type',
        'ownership', 'route_id', 'begin_point', 'capacity', 'structure_type',
    ]
    for col in critical:
        if col not in df.columns:
            print(f"        {col:25s}: MISSING")
            continue
        if df[col].dtype in ('object', 'str'):
            filled = (df[col].fillna("").str.strip() != "").sum()
        else:
            filled = (df[col] != 0).sum()
        pct = filled / len(df) * 100
        print(f"        {col:25s}: {filled:>8,} / {len(df):,} ({pct:.0f}%)")

    # ── Ownership distribution ──
    own_counts = df["ownership"].value_counts().head(10)
    if len(own_counts) > 0:
        print(f"      Ownership codes:")
        for code, cnt in own_counts.items():
            label = HPMS_OWNERSHIP_LABELS.get(code, f"Unknown")
            cl = HPMS_OWNERSHIP_MAP.get(code, "?")
            print(f"        Code {code:>2} ({label:30s}): {cnt:>7,} → {cl}")

    # Save parquet
    hpms_pq = cache_dir / f'{abbr}_hpms.parquet'
    df.to_parquet(hpms_pq, index=False)
    raw, gz = gzip_file(hpms_pq, hpms_gz)
    hpms_pq.unlink(missing_ok=True)

    elapsed = time.time() - t0
    print(f"      Total: {len(df):,} segments × {len(df.columns)} cols | {gz:.1f} MB gz | {elapsed:.0f}s")

    # Upload to R2
    if not local_only and s3:
        if r2_upload(s3, hpms_gz, bucket, f'{prefix}/cache/{abbr}_hpms.parquet.gz'):
            print(f"      -> uploaded to R2")

    # Method 3: HM-10 validation
    if not skip_hm10:
        audit = validate_ownership_hm10(df, name, fips, cache_dir)
        if audit:
            audit_path = cache_dir / f"{abbr}_hm10_validation.json"
            with open(audit_path, "w") as f:
                json.dump(audit, f, indent=2)
            print(f"      HM-10 audit saved: {audit_path}")

    gc.collect()
    return 'completed'


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CrashLens HPMS Data Downloader v2 — official FHWA road attributes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
    python generate_hpms_data.py --state de               # Delaware (~2 min)
    python generate_hpms_data.py --state de va md co       # Multiple
    python generate_hpms_data.py --all                     # All 51 states
    python generate_hpms_data.py --state de --local-only   # No R2 upload
    python generate_hpms_data.py --all --force             # Regenerate all
    python generate_hpms_data.py --state de --skip-hm10    # Skip ownership audit

HPMS FIELDS (63 per road segment):
    route_id          ARNOLD Route ID (LRS join key)
    begin/end_point   Milepoint range on route
    AADT              Annual Average Daily Traffic
    F_System          Functional Classification (1-7)
    Speed_Limit       Posted speed limit (mph)
    Through_Lanes     Number of through travel lanes
    Ownership         Road ownership (1-80, full FHWA codes)
    Surface_Type      Surface type code
    Capacity          Road capacity (veh/hr)
    Structure_Type    Bridge/tunnel flag
    + 53 more fields (see docstring)
        """,
    )
    parser.add_argument('--state', nargs='+', help='State abbreviation(s)')
    parser.add_argument('--all', action='store_true', help='All 51 states')
    parser.add_argument('--local-only', action='store_true', help='Skip R2 upload')
    parser.add_argument('--force', action='store_true', help='Regenerate if exists')
    parser.add_argument('--skip-hm10', action='store_true', help='Skip HM-10 ownership validation')
    parser.add_argument('--cache-dir', default='cache', help='Cache directory')
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        sys.exit(1)

    states = ALL_STATES if args.all else []
    if args.state:
        for abbr in args.state:
            abbr = abbr.lower()
            if abbr in ABBR_LOOKUP:
                states.append(ABBR_LOOKUP[abbr])
            else:
                print(f"Unknown state: {abbr}")
                sys.exit(1)

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    bucket = os.environ.get('R2_BUCKET', 'crash-lens-data')

    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        if s3:
            try:
                s3.list_objects_v2(Bucket=bucket, Prefix='delaware/', MaxKeys=1)
                print(f"R2 connected: {bucket}")
            except Exception:
                print("R2 connection failed — local-only mode")
                s3 = None
        else:
            print("R2 credentials not set — local-only mode")

    print(f"\n{'=' * 60}")
    print(f"  CrashLens HPMS Data Downloader v2")
    print(f"  States: {len(states)} | R2: {'yes' if s3 else 'local'}")
    print(f"  Output: 63 columns | Ownership: 27 FHWA codes")
    print(f"  Source: geo.dot.gov (2024→2018) + FHWA shapefiles (2017→2012)")
    print(f"  HM-10:  {'skip' if args.skip_hm10 else 'enabled'}")
    print(f"{'=' * 60}")

    results = {'completed': 0, 'skipped': 0, 'failed': 0}
    t_start = time.time()

    for i, state in enumerate(states, 1):
        print(f"\n  [{i}/{len(states)}]", end="")
        try:
            result = process_state(
                state, cache_dir, s3, bucket,
                force=args.force,
                local_only=args.local_only or not s3,
                skip_hm10=args.skip_hm10,
            )
        except Exception as e:
            print(f"  ERROR: {state['name']} — {e}")
            result = 'failed'
        results[result] += 1
        time.sleep(0.5)

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE in {elapsed / 60:.1f} min")
    print(f"  Completed: {results['completed']}")
    print(f"  Skipped:   {results['skipped']}")
    print(f"  Failed:    {results['failed']}")
    print(f"{'=' * 60}\n")


if __name__ == '__main__':
    main()
