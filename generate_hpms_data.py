#!/usr/bin/env python3
"""
generate_hpms_data.py — CrashLens HPMS Data Downloader
=======================================================
Downloads Highway Performance Monitoring System data from FHWA for crash enrichment.
HPMS is the federal government's authoritative road database with:
  AADT (traffic volume), official Functional Class, Speed Limit,
  Through Lanes, Median Type, Surface Type, Ownership, Access Control.

PRIMARY:  ArcGIS FeatureServer at geodata.bts.gov (paginated, per-state)
FALLBACK: FHWA shapefile download (zip per state)

SETUP:
    pip install requests pandas pyarrow geopandas boto3

USAGE:
    python generate_hpms_data.py --state de              # Delaware only (~2 min)
    python generate_hpms_data.py --state de va md         # Multiple
    python generate_hpms_data.py --all                    # All 51 states
    python generate_hpms_data.py --state de --local-only  # No R2 upload

OUTPUT:
    cache/{abbr}_hpms.parquet.gz → R2: {prefix}/cache/{abbr}_hpms.parquet.gz

    Parquet columns:
      mid_lat, mid_lon        GPS midpoint for crash matching
      f_system                Official FHWA Functional Classification (1-7)
      aadt                    Annual Average Daily Traffic (vehicles/day)
      speed_limit             Posted speed limit (mph)
      through_lanes           Number of through travel lanes
      median_type             Median type code (1=None, 2=Curbed, 3=Positive barrier, etc)
      surface_type            Surface type code (1=unpaved, 2=paved, etc)
      ownership               Ownership code (1=State, 2=County, 3=City, 4=Federal, etc)
      access_control          Access control (1=Full, 2=Partial, 3=None)
      urban_code              Urban area code (99999=Rural, 99998=Small urban)
      facility_type           Facility type (1=One-way, 2=Two-way)
      route_name              Route signing + route number
      terrain_type            Terrain (1=Flat, 2=Rolling, 3=Mountainous)
      curve_class             Curve classification (A-E)
      length_mi               Segment length in miles
"""

import argparse
import gc
import gzip
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# ═══════════════════════════════════════════════════════════════
#  HPMS ArcGIS REST API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

# BTS GeoData Hub — HPMS 2018 Public Release (per-state FeatureServers)
# URL pattern: https://geo.dot.gov/server/rest/services/Hosted/{StateName}_2018_PR/FeatureServer/0
# Each state has its own endpoint — no state filter needed in query.
# Field names are UPPERCASE: AADT, F_SYSTEM, SPEED_LIMIT, THROUGH_LANES, etc.
HPMS_BASE_URL = "https://geo.dot.gov/server/rest/services/Hosted"

# FHWA shapefile download (fallback)
FHWA_SHAPEFILE_BASE = "https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles"

# Fields we want from HPMS (expanded — 40+ attributes)
HPMS_FIELDS = [
    # Core identification
    "State_Code", "County_Code", "F_System", "NHS",
    "Route_Signing", "Route_Number", "Route_Name",
    "Section_Length", "Facility_Type", "Ownership",
    # Traffic volume (CRITICAL — enables crash rate)
    "AADT", "Future_AADT",
    "AADT_Single_Unit", "AADT_Combination",
    "K_Factor", "Dir_Factor",
    "Pct_Peak_Single", "Pct_Peak_Combination",
    # Road geometry
    "Speed_Limit", "Through_Lanes", "Access_Control",
    "Median_Type", "Median_Width",
    "Lane_Width", "Surface_Type",
    "Terrain_Type", "Curve_Class", "Grade_Class",
    "Urban_Code",
    # Intersection inventory
    "Signal_Type", "Pct_Green_Time",
    "Num_Signalized_Int", "Num_Stop_Int", "At_Grade_Other",
    # Shoulders and turn lanes
    "Shoulder_Type_R", "Shoulder_Type_L",
    "Shoulder_Width_R", "Shoulder_Width_L",
    "Turning_Lanes_R", "Turning_Lanes_L",
    "Peak_Parking",
    # Pavement condition
    "IRI", "PSR", "Rutting", "Faulting",
    "Year_Last_Construction",
    # Widening feasibility
    "Widening_Feasibility",
]

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
#  ArcGIS REST API DOWNLOAD (paginated)
# ═══════════════════════════════════════════════════════════════

def get_state_hpms_url(state_name, state_abbr):
    """Build per-state HPMS FeatureServer URL, trying newest year first.
    
    Naming patterns on geo.dot.gov:
      2023: HPMS_FULL_{ABBR}_2023     (newest, BETA release)
      2020: HPMS_FULL_{ABBR}_2020
      2019: HPMS_Full_{ABBR}_2019     (note: mixed case)
      2018: {StateName}_2018_PR        (old Public Release)
    """
    abbr_upper = state_abbr.upper()
    url_name = state_name.replace(" ", "_")
    
    candidates = [
        f"{HPMS_BASE_URL}/HPMS_FULL_{abbr_upper}_2023/FeatureServer/0",
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
    Field names are UPPERCASE in the 2018 Public Release schema.
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

        try:
            r = requests.get(f"{service_url}/query", params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"      Page {page} error: {e}")
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

        # Check if there are more
        if not data.get("exceededTransferLimit", False) and len(features) < page_size:
            break

        time.sleep(0.2)  # polite rate limit

    return all_features


def features_to_dataframe(features):
    """Convert ArcGIS features to flat DataFrame with midpoint GPS.
    
    FHWA 2018 Public Release uses UPPERCASE field names:
    AADT, F_SYSTEM, SPEED_LIMIT, THROUGH_LANES, etc.
    We try UPPERCASE first, then Title_Case as fallback.
    """
    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})

        # Compute midpoint from polyline geometry
        paths = geom.get("paths", [])
        mid_lat, mid_lon = 0, 0
        if paths:
            all_coords = [c for path in paths for c in path]
            if all_coords:
                mid_idx = len(all_coords) // 2
                mid_lon = all_coords[mid_idx][0]
                mid_lat = all_coords[mid_idx][1]

        if mid_lat == 0 and mid_lon == 0:
            continue

        # Helper: try UPPERCASE first, then Title_Case, then lowercase
        def g(key, *alts):
            for k in [key, key.upper(), key.lower()] + list(alts):
                v = attrs.get(k)
                if v is not None:
                    return v
            return None

        # Build route name from signing + number
        signing = str(g("ROUTE_SIGNING", "Route_Signing") or "")
        route_num = str(g("ROUTE_NUMBER", "Route_Number") or "")
        route_name = str(g("ROUTE_NAME", "Route_Name") or "").strip()
        if not route_name and route_num:
            sign_map = {"1": "I", "2": "US", "3": "SR", "4": "CR"}
            prefix = sign_map.get(signing, "")
            route_name = f"{prefix} {route_num}".strip()

        # Extract AADT
        aadt = g("AADT", "aadt", "Aadt") or 0
        try:
            aadt = int(aadt) if aadt else 0
        except (ValueError, TypeError):
            aadt = 0

        def safe_int(val, default=0):
            try:
                return int(val) if val else default
            except (ValueError, TypeError):
                return default

        def safe_float(val, default=0.0):
            try:
                return round(float(val), 1) if val else default
            except (ValueError, TypeError):
                return default

        rows.append({
            "mid_lat": round(mid_lat, 7),
            "mid_lon": round(mid_lon, 7),
            "f_system": safe_int(g("F_SYSTEM", "F_System")),
            "aadt": aadt,
            "speed_limit": safe_int(g("SPEED_LIMIT", "Speed_Limit")),
            "through_lanes": safe_int(g("THROUGH_LANES", "Through_Lanes")),
            "median_type": safe_int(g("MEDIAN_TYPE", "Median_Type")),
            "median_width": safe_float(g("MEDIAN_WIDTH", "Median_Width")),
            "surface_type": safe_int(g("SURFACE_TYPE", "Surface_Type")),
            "ownership": safe_int(g("OWNERSHIP", "Ownership")),
            "access_control": safe_int(g("ACCESS_CONTROL", "Access_Control")),
            "urban_code": str(g("URBAN_CODE", "Urban_Code") or ""),
            "facility_type": safe_int(g("FACILITY_TYPE", "Facility_Type")),
            "route_name": route_name[:80],
            "terrain_type": safe_int(g("TERRAIN_TYPE", "Terrain_Type")),
            "curve_class": str(g("CURVE_CLASS", "Curve_Class", "Curve_Cls") or ""),
            "grade_class": str(g("GRADE_CLASS", "Grade_Class", "Grade_Cls") or ""),
            "nhs": safe_int(g("NHS")),
            "county_code": str(g("COUNTY_CODE", "County_Code") or ""),
            "length_mi": safe_float(g("SECTION_LENGTH", "Section_Length", "MILES")),
            "lane_width": safe_float(g("LANE_WIDTH", "Lane_Width")),
            "shoulder_width_r": safe_float(g("SHOULDER_WIDTH_R", "R_Shoulder_Width")),
            "shoulder_type_r": safe_int(g("SHOULDER_TYPE_R", "R_Shoulder_Type")),
            "aadt_combination": safe_int(g("AADT_COMBINATION", "AADT_Combination")),
            "aadt_single_unit": safe_int(g("AADT_SINGLE_UNIT", "AADT_Single_Unit")),
            "pct_peak_combination": safe_float(g("PCT_PEAK_COMBINATION", "Pct_Peak_Combination")),
            "signal_type": safe_int(g("SIGNAL_TYPE", "Signal_Type")),
            "num_signals": safe_int(g("NUM_SIGNALS", "Num_Signals", "At_Grade_Other")),
            "turn_lanes_r": safe_int(g("TURN_LANES_R", "Turn_Lanes_R")),
            "turn_lanes_l": safe_int(g("TURN_LANES_L", "Turn_Lanes_L")),
            "design_speed": safe_int(g("WEIGHTED_DESIGN_SPEED", "Design_Speed")),
            "peak_parking": safe_int(g("PEAK_PARKING", "Peak_Parking")),
            "iri": safe_float(g("IRI")),
            "k_factor": safe_float(g("K_FACTOR", "K_Factor")),
            "dir_factor": safe_float(g("DIR_FACTOR", "Dir_Factor")),
            "future_aadt": safe_int(g("FUTURE_AADT", "Future_AADT")),
            "num_signalized_int": safe_int(g("NUM_SIGNALIZED_INT", "Num_Signalized_Int")),
            "num_stop_int": safe_int(g("NUM_STOP_INT", "Num_Stop_Int")),
            "psr": safe_float(g("PSR")),
            "rutting": safe_float(g("RUTTING", "Rutting")),
            "faulting": safe_float(g("FAULTING", "Faulting")),
            "year_last_construction": safe_int(g("YEAR_LAST_CONSTRUCTION", "Yr_Last_Constr")),
            "widening_feasibility": safe_int(g("WIDENING_FEASIBILITY", "Widen_Feasiblty")),
            "shoulder_width_l": safe_float(g("SHOULDER_WIDTH_L", "L_Shoulder_Width")),
            "shoulder_type_l": safe_int(g("SHOULDER_TYPE_L", "L_Shoulder_Type")),
            "pct_green_time": safe_float(g("PCT_GREEN_TIME", "Pct_Green_Time")),
        })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
#  FHWA SHAPEFILE DOWNLOAD (fallback)
# ═══════════════════════════════════════════════════════════════

def download_hpms_shapefile(state_name, state_abbr, cache_dir):
    """Download HPMS shapefile from FHWA as fallback."""
    try:
        import geopandas as gpd
    except ImportError:
        print("      geopandas not installed — shapefile fallback unavailable")
        return None

    # FHWA uses state names in URLs (e.g., Delaware2018.zip)
    name_clean = state_name.replace(" ", "")
    years = [2023, 2022, 2021, 2020, 2019, 2018]

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
    """Convert HPMS GeoDataFrame to flat DataFrame with midpoint."""
    rows = []
    for _, row in gdf.iterrows():
        try:
            centroid = row.geometry.centroid
            mid_lat, mid_lon = centroid.y, centroid.x
        except Exception:
            continue

        if mid_lat == 0 and mid_lon == 0:
            continue

        # Field names in shapefiles are often truncated to 8 chars
        def g(key, *alts):
            val = row.get(key, None)
            if val is None:
                for a in alts:
                    val = row.get(a, None)
                    if val is not None:
                        break
            return val

        aadt = g("AADT", "AADT_VN", "aadt") or 0
        try:
            aadt = int(aadt)
        except (ValueError, TypeError):
            aadt = 0

        rows.append({
            "mid_lat": round(mid_lat, 7),
            "mid_lon": round(mid_lon, 7),
            "f_system": int(g("F_SYSTEM", "F_System", "FUNC_CLS") or 0),
            "aadt": aadt,
            "speed_limit": int(g("SPEED_LMT", "Speed_Limit", "SPD_LIMIT") or 0),
            "through_lanes": int(g("THRU_LANE", "Through_Lanes", "THRULANES") or 0),
            "median_type": int(g("MEDIAN_TY", "Median_Type", "MEDIAN") or 0),
            "surface_type": int(g("SURF_TYPE", "Surface_Type") or 0),
            "ownership": int(g("OWNERSHIP", "Ownership") or 0),
            "access_control": int(g("ACCESS_CN", "Access_Control", "ACCESS") or 0),
            "urban_code": str(g("URBAN_CDE", "Urban_Code", "URBAN_CODE") or ""),
            "facility_type": int(g("FAC_TYPE", "Facility_Type") or 0),
            "route_name": str(g("ROUTE_NAM", "Route_Name", "LNAME") or "")[:80],
            "terrain_type": int(g("TERRAIN", "Terrain_Type") or 0),
            "curve_class": str(g("CURVES", "Curve_Class", "Curve_Cls") or ""),
            "grade_class": str(g("GRADES", "Grade_Class", "Grade_Cls") or ""),
            "nhs": int(g("NHS") or 0),
            "county_code": str(g("CNTY_CODE", "County_Code", "CTFIPS") or ""),
            "length_mi": round(float(g("SEC_LEN", "Section_Length", "MILES") or 0), 3),
            # NEW high-value fields
            "lane_width": round(float(g("LANE_WID", "Lane_Width") or 0), 1),
            "median_width": round(float(g("MED_WIDT", "Median_Width") or 0), 1),
            "shoulder_width_r": round(float(g("RSHL_WID", "Shoulder_Width_R", "R_Shoulder_Width") or 0), 1),
            "shoulder_type_r": int(g("RSHL_TYP", "Shoulder_Type_R", "R_Shoulder_Type") or 0),
            "aadt_combination": int(g("AADT_COM", "AADT_Combination", "AADT_Comb") or 0),
            "aadt_single_unit": int(g("AADT_SU", "AADT_Single_Unit") or 0),
            "pct_peak_combination": round(float(g("PCT_PK_C", "Pct_Peak_Combination") or 0), 1),
            "signal_type": int(g("SIG_TYPE", "Signal_Type") or 0),
            "num_signals": int(g("NUM_SIGN", "Num_Signals") or 0),
            "turn_lanes_r": int(g("TRN_LN_R", "Turn_Lanes_R") or 0),
            "turn_lanes_l": int(g("TRN_LN_L", "Turn_Lanes_L") or 0),
            "design_speed": int(g("DSGN_SPD", "Weighted_Design_Speed", "Design_Speed") or 0),
            "peak_parking": int(g("PK_PARKI", "Peak_Parking") or 0),
            "iri": round(float(g("IRI") or 0), 1),
            "k_factor": round(float(g("K_FACTOR", "K_Factor") or 0), 1),
        })

    return pd.DataFrame(rows)


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
                  force=False, local_only=False):
    """Download HPMS data for one state via per-state FeatureServer."""
    name = state_info['name']
    abbr = state_info['abbreviation']
    fips = state_info['fips']
    prefix = state_info['r2_prefix']

    hpms_gz = cache_dir / f'{abbr}_hpms.parquet.gz'

    # Skip if already cached
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

    # Method 1: Per-state ArcGIS REST API at geo.dot.gov (try 2023→2020→2019→2018)
    candidates = get_state_hpms_url(name, abbr)
    print(f"      Method: ArcGIS per-state endpoint (trying 2023→2020→2019→2018)")
    for url in candidates:
        year = url.split("_")[-1].split("/")[0]  # extract year from URL
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

    # Method 2: FHWA Shapefile (fallback)
    if df is None or len(df) == 0:
        print(f"      Fallback: FHWA shapefile download")
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

    # Report key field coverage
    print(f"      Field coverage:")
    for col in ['aadt', 'speed_limit', 'through_lanes', 'f_system', 'surface_type']:
        filled = (df[col] != 0).sum() if col in df.columns else 0
        pct = filled / len(df) * 100
        print(f"        {col:15s}: {filled:>8,} / {len(df):,} ({pct:.0f}%)")

    # Save parquet
    hpms_pq = cache_dir / f'{abbr}_hpms.parquet'
    df.to_parquet(hpms_pq, index=False)
    raw, gz = gzip_file(hpms_pq, hpms_gz)
    hpms_pq.unlink(missing_ok=True)

    elapsed = time.time() - t0
    print(f"      Total: {len(df):,} segments | {gz:.1f} MB gz | {elapsed:.0f}s")

    # Upload to R2
    if not local_only and s3:
        if r2_upload(s3, hpms_gz, bucket, f'{prefix}/cache/{abbr}_hpms.parquet.gz'):
            print(f"      -> uploaded to R2")

    gc.collect()
    return 'completed'


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CrashLens HPMS Data Downloader — official FHWA road attributes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
    python generate_hpms_data.py --state de               # Delaware (~2 min)
    python generate_hpms_data.py --state de va md co       # Multiple
    python generate_hpms_data.py --all                     # All 51 states
    python generate_hpms_data.py --state de --local-only   # No R2 upload
    python generate_hpms_data.py --all --force             # Regenerate all

HPMS FIELDS DOWNLOADED (63 per road segment):
    AADT             Annual Average Daily Traffic (vehicles/day)
    F_System         Official Functional Classification (1-7)
    Speed_Limit      Posted speed limit (mph)
    Through_Lanes    Number of through travel lanes
    Median_Type      Median type (1=None, 2=Curbed, 3=Barrier...)
    Surface_Type     Surface type (1=unpaved, 2=paved...)
    Ownership        Road ownership (1=State, 2=County, 3=City...)
    Access_Control   Access control (1=Full, 2=Partial, 3=None)
    Terrain_Type     Terrain (1=Flat, 2=Rolling, 3=Mountainous)
    Curve_Class      Curve classification
    Grade_Class      Grade classification
        """,
    )
    parser.add_argument('--state', nargs='+', help='State abbreviation(s)')
    parser.add_argument('--all', action='store_true', help='All 51 states')
    parser.add_argument('--local-only', action='store_true', help='Skip R2 upload')
    parser.add_argument('--force', action='store_true', help='Regenerate if exists')
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
    print(f"  CrashLens HPMS Data Downloader")
    print(f"  States: {len(states)} | R2: {'yes' if s3 else 'local'}")
    print(f"  Source: geo.dot.gov per-state FeatureServer + shapefile fallback")
    print(f"{'=' * 60}")

    results = {'completed': 0, 'skipped': 0, 'failed': 0}
    t_start = time.time()

    for i, state in enumerate(states, 1):
        print(f"\n  [{i}/{len(states)}]", end="")
        try:
            result = process_state(
                state, cache_dir, s3, bucket,
                force=args.force, local_only=args.local_only or not s3,
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
