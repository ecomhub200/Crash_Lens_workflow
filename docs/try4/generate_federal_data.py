#!/usr/bin/env python3
"""
generate_federal_data.py — CrashLens Federal Safety Data Downloader
====================================================================
Downloads authoritative federal infrastructure data for crash enrichment.
Four sources, each producing a per-state parquet cache in R2:

  1. SCHOOLS      Urban Institute Education API (enrollment, grade level, NCES ID)
  2. BRIDGES      NBI via BTS ArcGIS (condition rating, year built, ADT)
  3. RAIL XINGS   FRA via BTS ArcGIS (warning devices, crash history, trains/day)
  4. TRANSIT      NTM via BTS ArcGIS (agency, route type, stop locations)

These are FEDERAL AUTHORITATIVE sources that upgrade OSM POI data:
  OSM Near_School_1000ft=Yes  →  + School_Enrollment_Nearest=1847
  OSM On_Bridge=Yes           →  + Bridge_Condition=Poor (NBI deck+super+sub)
  OSM Near_Rail_Xing_150ft    →  + Rail_Warning_Device=Gates (FRA WDCODE)
  (no OSM equivalent)         →  + Near_Transit_500ft=Yes

ENRICHMENT PRIORITY (v2.6.5): HPMS → Federal → OSM
  Federal data fills Tier 2c, between HPMS (Tier 3) and OSM (Tier 2).

SETUP:
    pip install requests pandas pyarrow boto3

USAGE:
    python generate_federal_data.py --state de                    # Delaware, all sources
    python generate_federal_data.py --state de --source schools   # Schools only
    python generate_federal_data.py --state de va md              # Multiple states
    python generate_federal_data.py --all                         # All 51 states
    python generate_federal_data.py --state de --local-only       # No R2 upload

OUTPUT (per state):
    cache/{abbr}_schools.parquet.gz        → R2: {prefix}/cache/{abbr}_schools.parquet.gz
    cache/{abbr}_bridges.parquet.gz        → R2: {prefix}/cache/{abbr}_bridges.parquet.gz
    cache/{abbr}_rail_crossings.parquet.gz → R2: {prefix}/cache/{abbr}_rail_crossings.parquet.gz
    cache/{abbr}_transit.parquet.gz        → R2: {prefix}/cache/{abbr}_transit.parquet.gz
"""

import argparse
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
#  FEDERAL DATA SOURCE ENDPOINTS
# ═══════════════════════════════════════════════════════════════

# 1. Schools — Urban Institute Education Data API
SCHOOLS_API = "https://educationdata.urban.org/api/v1/schools/ccd"

# 2. Bridges — National Bridge Inventory via BTS/NTAD ArcGIS
BRIDGES_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services"
    "/NTAD_National_Bridge_Inventory/FeatureServer/0"
)

# 3. Railroad Grade Crossings — FRA via BTS/NTAD ArcGIS
RAIL_XING_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services"
    "/NTAD_Railroad_Grade_Crossings/FeatureServer/0"
)

# 4. Transit Stops — National Transit Map via BTS/NTAD ArcGIS
TRANSIT_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services"
    "/NTAD_National_Transit_Map_Stops/FeatureServer/0"
)

# Fields to download from each ArcGIS source
BRIDGE_FIELDS = [
    "LATITUDE", "LONGITUDE", "STATE_CODE_001", "COUNTY_CODE_003",
    "FEATURES_DESC_006A", "FACILITY_CARRIED_007", "STRUCTURE_NUMBER_008",
    "YEAR_BUILT_027", "ADT_029", "YEAR_ADT_030",
    "DECK_COND_058", "SUPERSTRUCTURE_COND_059", "SUBSTRUCTURE_COND_060",
    "STRUCTURAL_EVAL_067", "DECK_GEOMETRY_EVAL_068",
    "OPERATING_RATING_064", "INVENTORY_RATING_066",
    "APPR_WIDTH_MT_032", "ROADWAY_WIDTH_MT_051",
    "DETOUR_KILOS_019", "TRAFFIC_LANES_ON_028A",
    "OPEN_CLOSED_POSTED_041",
]

RAIL_XING_FIELDS = [
    "LATITUDE", "LONGITUDE", "STREET", "HIGHWAY", "RAILROAD",
    "CROSSING", "CITY", "STATE", "COUNTY",
    "TYPEXING",   # Crossing type (1=public, 2=private)
    "WDCODE",     # Warning device (1=None, 2=Signs, 3=FlashLights, 4=Gates)
    "TTSTN",      # Total trains per day
    "APTS",       # Auto-involved crashes in past 5 years
    "UPDTL",      # Last update
    "POSXING",    # Position (1=at grade, 2=under, 3=over)
    "ILLUMINA",   # Illumination (1=yes, 0=no)
    "XSURFACE",   # Surface type
]

TRANSIT_FIELDS = [
    "stop_lat", "stop_lon", "stop_name", "stop_id",
    "agency_name", "route_type_text", "stop_desc",
    "wheelchair_boarding",
]

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY (same as generate_hpms_data.py)
# ═══════════════════════════════════════════════════════════════

ALL_STATES = [
    {"name": "Alabama", "abbreviation": "al", "fips": "01", "fips_num": 1, "r2_prefix": "alabama"},
    {"name": "Alaska", "abbreviation": "ak", "fips": "02", "fips_num": 2, "r2_prefix": "alaska"},
    {"name": "Arizona", "abbreviation": "az", "fips": "04", "fips_num": 4, "r2_prefix": "arizona"},
    {"name": "Arkansas", "abbreviation": "ar", "fips": "05", "fips_num": 5, "r2_prefix": "arkansas"},
    {"name": "California", "abbreviation": "ca", "fips": "06", "fips_num": 6, "r2_prefix": "california"},
    {"name": "Colorado", "abbreviation": "co", "fips": "08", "fips_num": 8, "r2_prefix": "colorado"},
    {"name": "Connecticut", "abbreviation": "ct", "fips": "09", "fips_num": 9, "r2_prefix": "connecticut"},
    {"name": "Delaware", "abbreviation": "de", "fips": "10", "fips_num": 10, "r2_prefix": "delaware"},
    {"name": "District of Columbia", "abbreviation": "dc", "fips": "11", "fips_num": 11, "r2_prefix": "district_of_columbia"},
    {"name": "Florida", "abbreviation": "fl", "fips": "12", "fips_num": 12, "r2_prefix": "florida"},
    {"name": "Georgia", "abbreviation": "ga", "fips": "13", "fips_num": 13, "r2_prefix": "georgia"},
    {"name": "Hawaii", "abbreviation": "hi", "fips": "15", "fips_num": 15, "r2_prefix": "hawaii"},
    {"name": "Idaho", "abbreviation": "id", "fips": "16", "fips_num": 16, "r2_prefix": "idaho"},
    {"name": "Illinois", "abbreviation": "il", "fips": "17", "fips_num": 17, "r2_prefix": "illinois"},
    {"name": "Indiana", "abbreviation": "in", "fips": "18", "fips_num": 18, "r2_prefix": "indiana"},
    {"name": "Iowa", "abbreviation": "ia", "fips": "19", "fips_num": 19, "r2_prefix": "iowa"},
    {"name": "Kansas", "abbreviation": "ks", "fips": "20", "fips_num": 20, "r2_prefix": "kansas"},
    {"name": "Kentucky", "abbreviation": "ky", "fips": "21", "fips_num": 21, "r2_prefix": "kentucky"},
    {"name": "Louisiana", "abbreviation": "la", "fips": "22", "fips_num": 22, "r2_prefix": "louisiana"},
    {"name": "Maine", "abbreviation": "me", "fips": "23", "fips_num": 23, "r2_prefix": "maine"},
    {"name": "Maryland", "abbreviation": "md", "fips": "24", "fips_num": 24, "r2_prefix": "maryland"},
    {"name": "Massachusetts", "abbreviation": "ma", "fips": "25", "fips_num": 25, "r2_prefix": "massachusetts"},
    {"name": "Michigan", "abbreviation": "mi", "fips": "26", "fips_num": 26, "r2_prefix": "michigan"},
    {"name": "Minnesota", "abbreviation": "mn", "fips": "27", "fips_num": 27, "r2_prefix": "minnesota"},
    {"name": "Mississippi", "abbreviation": "ms", "fips": "28", "fips_num": 28, "r2_prefix": "mississippi"},
    {"name": "Missouri", "abbreviation": "mo", "fips": "29", "fips_num": 29, "r2_prefix": "missouri"},
    {"name": "Montana", "abbreviation": "mt", "fips": "30", "fips_num": 30, "r2_prefix": "montana"},
    {"name": "Nebraska", "abbreviation": "ne", "fips": "31", "fips_num": 31, "r2_prefix": "nebraska"},
    {"name": "Nevada", "abbreviation": "nv", "fips": "32", "fips_num": 32, "r2_prefix": "nevada"},
    {"name": "New Hampshire", "abbreviation": "nh", "fips": "33", "fips_num": 33, "r2_prefix": "new_hampshire"},
    {"name": "New Jersey", "abbreviation": "nj", "fips": "34", "fips_num": 34, "r2_prefix": "new_jersey"},
    {"name": "New Mexico", "abbreviation": "nm", "fips": "35", "fips_num": 35, "r2_prefix": "new_mexico"},
    {"name": "New York", "abbreviation": "ny", "fips": "36", "fips_num": 36, "r2_prefix": "new_york"},
    {"name": "North Carolina", "abbreviation": "nc", "fips": "37", "fips_num": 37, "r2_prefix": "north_carolina"},
    {"name": "North Dakota", "abbreviation": "nd", "fips": "38", "fips_num": 38, "r2_prefix": "north_dakota"},
    {"name": "Ohio", "abbreviation": "oh", "fips": "39", "fips_num": 39, "r2_prefix": "ohio"},
    {"name": "Oklahoma", "abbreviation": "ok", "fips": "40", "fips_num": 40, "r2_prefix": "oklahoma"},
    {"name": "Oregon", "abbreviation": "or", "fips": "41", "fips_num": 41, "r2_prefix": "oregon"},
    {"name": "Pennsylvania", "abbreviation": "pa", "fips": "42", "fips_num": 42, "r2_prefix": "pennsylvania"},
    {"name": "Rhode Island", "abbreviation": "ri", "fips": "44", "fips_num": 44, "r2_prefix": "rhode_island"},
    {"name": "South Carolina", "abbreviation": "sc", "fips": "45", "fips_num": 45, "r2_prefix": "south_carolina"},
    {"name": "South Dakota", "abbreviation": "sd", "fips": "46", "fips_num": 46, "r2_prefix": "south_dakota"},
    {"name": "Tennessee", "abbreviation": "tn", "fips": "47", "fips_num": 47, "r2_prefix": "tennessee"},
    {"name": "Texas", "abbreviation": "tx", "fips": "48", "fips_num": 48, "r2_prefix": "texas"},
    {"name": "Utah", "abbreviation": "ut", "fips": "49", "fips_num": 49, "r2_prefix": "utah"},
    {"name": "Vermont", "abbreviation": "vt", "fips": "50", "fips_num": 50, "r2_prefix": "vermont"},
    {"name": "Virginia", "abbreviation": "va", "fips": "51", "fips_num": 51, "r2_prefix": "virginia"},
    {"name": "Washington", "abbreviation": "wa", "fips": "53", "fips_num": 53, "r2_prefix": "washington"},
    {"name": "West Virginia", "abbreviation": "wv", "fips": "54", "fips_num": 54, "r2_prefix": "west_virginia"},
    {"name": "Wisconsin", "abbreviation": "wi", "fips": "55", "fips_num": 55, "r2_prefix": "wisconsin"},
    {"name": "Wyoming", "abbreviation": "wy", "fips": "56", "fips_num": 56, "r2_prefix": "wyoming"},
]

ABBR_LOOKUP = {s["abbreviation"]: s for s in ALL_STATES}

# State bounding boxes (west, south, east, north) for ArcGIS spatial queries
# Source: Census TIGER/Line. Includes ~0.1° buffer.
STATE_BBOX = {
    "al": (-88.6, 30.1, -84.8, 35.1), "ak": (-180.0, 51.1, -129.9, 71.5),
    "az": (-114.9, 31.3, -109.0, 37.1), "ar": (-94.7, 33.0, -89.6, 36.6),
    "ca": (-124.5, 32.5, -114.1, 42.1), "co": (-109.1, 36.9, -102.0, 41.1),
    "ct": (-73.8, 40.9, -71.7, 42.1), "de": (-75.9, 38.4, -75.0, 39.9),
    "dc": (-77.2, 38.8, -76.9, 39.0), "fl": (-87.7, 24.4, -79.9, 31.1),
    "ga": (-85.7, 30.3, -80.7, 35.1), "hi": (-160.3, 18.9, -154.7, 22.3),
    "id": (-117.3, 41.9, -111.0, 49.1), "il": (-91.6, 36.9, -87.4, 42.6),
    "in": (-88.1, 37.7, -84.7, 41.8), "ia": (-96.7, 40.3, -90.1, 43.6),
    "ks": (-102.1, 36.9, -94.6, 40.1), "ky": (-89.6, 36.4, -81.9, 39.2),
    "la": (-94.1, 28.9, -88.7, 33.1), "me": (-71.1, 43.0, -66.9, 47.5),
    "md": (-79.6, 37.9, -75.0, 39.8), "ma": (-73.6, 41.2, -69.9, 42.9),
    "mi": (-90.5, 41.6, -82.1, 48.3), "mn": (-97.3, 43.4, -89.4, 49.4),
    "ms": (-91.7, 30.1, -88.0, 35.0), "mo": (-95.8, 35.9, -89.0, 40.7),
    "mt": (-116.1, 44.3, -104.0, 49.1), "ne": (-104.1, 39.9, -95.3, 43.1),
    "nv": (-120.1, 35.0, -114.0, 42.1), "nh": (-72.6, 42.6, -71.0, 45.4),
    "nj": (-75.6, 38.9, -73.9, 41.4), "nm": (-109.1, 31.3, -103.0, 37.1),
    "ny": (-79.8, 40.4, -71.8, 45.1), "nc": (-84.4, 33.8, -75.4, 36.6),
    "nd": (-104.1, 45.9, -96.5, 49.1), "oh": (-84.9, 38.3, -80.5, 42.0),
    "ok": (-103.1, 33.6, -94.4, 37.1), "or": (-124.7, 41.9, -116.4, 46.3),
    "pa": (-80.6, 39.7, -74.7, 42.3), "ri": (-71.9, 41.1, -71.1, 42.1),
    "sc": (-83.4, 32.0, -78.5, 35.3), "sd": (-104.1, 42.4, -96.4, 46.0),
    "tn": (-90.4, 34.9, -81.6, 36.7), "tx": (-106.7, 25.8, -93.5, 36.6),
    "ut": (-114.1, 36.9, -109.0, 42.1), "vt": (-73.5, 42.7, -71.5, 45.1),
    "va": (-83.7, 36.5, -75.2, 39.5), "wa": (-124.8, 45.5, -116.9, 49.1),
    "wv": (-82.7, 37.1, -77.7, 40.7), "wi": (-93.0, 42.4, -86.7, 47.3),
    "wy": (-111.1, 40.9, -104.0, 45.1),
}

# ═══════════════════════════════════════════════════════════════
#  ArcGIS PAGINATED DOWNLOAD (shared by bridges, rail, transit)
# ═══════════════════════════════════════════════════════════════

# BTS ArcGIS servers aggressively reset keep-alive connections.
# Use Connection: close to force fresh TCP per request, and create
# a new Session on connection reset to clear poisoned socket pool.
_HEADERS = {
    "User-Agent": "CrashLens-FederalData/1.0",
    "Connection": "close",          # ← Prevent keep-alive reuse
}
SESSION = requests.Session()
SESSION.headers.update(_HEADERS)


def _fresh_session():
    """Create a fresh requests Session (clears poisoned connection pool)."""
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _resilient_get(url, params, max_attempts=5):
    """
    GET with connection-reset resilience.

    BTS servers drop TCP connections unpredictably. On ConnectionResetError,
    we create a fresh Session (new socket pool) and retry with increasing
    backoff. 5 attempts with 3-10s delays covers transient BTS failures.
    """
    global SESSION
    last_err = None

    for attempt in range(max_attempts):
        try:
            resp = SESSION.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
            last_err = e
            # Connection poisoned — create fresh session with new socket pool
            SESSION = _fresh_session()
            delay = 3 + attempt * 2  # 3, 5, 7, 9, 11 seconds
            print(f"        Connection reset (attempt {attempt + 1}/{max_attempts}), "
                  f"fresh session, retry in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            last_err = e
            delay = 2 ** (attempt + 1)
            if attempt == max_attempts - 1:
                break
            time.sleep(delay)

    print(f"      Request failed after {max_attempts} attempts: {last_err}")
    return None


def download_arcgis_features(url, bbox, fields, state_fips=None,
                              state_filter_field=None, page_size=2000,
                              max_pages=50):
    """
    Download features from an ArcGIS FeatureServer using spatial envelope.

    Resilience features for BTS servers:
      - outFields=* to avoid field-name mismatch (column filtering after download)
      - Connection: close header to prevent keep-alive reuse
      - Fresh Session on ConnectionResetError (clears poisoned socket pool)
      - 5 retries per request with 3-11s backoff
      - ArcGIS JSON error detection (API errors come with HTTP 200)
      - Auto-fallback from state filter to bbox-only on empty first page

    Args:
        url: FeatureServer endpoint URL
        bbox: (west, south, east, north) bounding box
        fields: List of field names (for documentation; outFields=* used for download)
        state_fips: Optional state FIPS for attribute-based filter
        state_filter_field: Field name for state FIPS filter (e.g., 'STATE_CODE_001')
        page_size: Records per request
        max_pages: Safety limit on pagination

    Returns:
        List of dicts (feature attributes with geometry coordinates)
    """
    west, south, east, north = bbox
    geometry = json.dumps({
        "xmin": west, "ymin": south, "xmax": east, "ymax": north,
        "spatialReference": {"wkid": 4326}
    })

    # Build where clause
    where = "1=1"
    if state_fips and state_filter_field:
        where = f"{state_filter_field}='{state_fips}'"

    all_rows = []
    offset = 0

    for page in range(max_pages):
        params = {
            "where": where,
            "geometry": geometry,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "outSR": "4326",
            "f": "json",
            "resultRecordCount": page_size,
            "returnGeometry": True,
        }

        # Only add resultOffset on page 2+ (some servers choke on offset=0)
        if offset > 0:
            params["resultOffset"] = offset

        data = _resilient_get(f"{url}/query", params)
        if data is None:
            return all_rows  # All retries exhausted

        # Check for ArcGIS error in JSON body (returned with HTTP 200)
        if "error" in data:
            err = data["error"]
            msg = err.get("message", str(err))
            print(f"      ArcGIS error: {msg}")
            # If state filter caused the error, retry bbox-only
            if page == 0 and where != "1=1":
                print(f"      Retrying with where=1=1 (bbox-only)...")
                where = "1=1"
                params["where"] = where
                time.sleep(2)
                data = _resilient_get(f"{url}/query", params)
                if data is None or "error" in data:
                    return all_rows
            else:
                return all_rows

        features = data.get("features", [])

        # If first page with state filter returned empty, try bbox-only
        if not features and page == 0 and where != "1=1":
            print(f"      0 features with state filter — retrying bbox-only...")
            where = "1=1"
            params["where"] = where
            time.sleep(2)
            data = _resilient_get(f"{url}/query", params)
            if data is None or "error" in data:
                return all_rows
            features = data.get("features", [])

        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})

            # Extract lat/lon from point geometry
            if "y" in geom and "x" in geom:
                attrs["lat"] = round(geom["y"], 6)
                attrs["lon"] = round(geom["x"], 6)
            elif "latitude" in {k.lower() for k in attrs}:
                # Some layers have lat/lon in attributes
                for k in attrs:
                    if k.lower() == "latitude":
                        attrs["lat"] = attrs[k]
                    if k.lower() == "longitude":
                        attrs["lon"] = attrs[k]

            all_rows.append(attrs)

        if len(features) < page_size:
            break  # Last page
        offset += page_size
        time.sleep(1.0)  # BTS needs breathing room between pages

    return all_rows


# ═══════════════════════════════════════════════════════════════
#  SOURCE 1: SCHOOLS (Urban Institute Education Data API)
# ═══════════════════════════════════════════════════════════════

def download_schools(state_info):
    """
    Download school locations + enrollment from Urban Institute API.

    Tries year = current-2 through current-6 (data lags ~2 years).
    Returns DataFrame with: lat, lon, school_name, ncessch, enrollment,
    school_level, county_fips, leaid.
    """
    fips_num = state_info["fips_num"]
    name = state_info["name"]
    import datetime
    current_year = datetime.datetime.now().year

    # Try recent years (data lags ~2 years)
    directory_df = None
    enrollment_df = None
    data_year = None

    for year in range(current_year - 2, current_year - 7, -1):
        try:
            # Directory (locations)
            dir_url = f"{SCHOOLS_API}/directory/{year}/?fips={fips_num}"
            dir_rows = _paginate_urban_api(dir_url)
            if not dir_rows:
                continue

            # Enrollment (student counts)
            enr_url = f"{SCHOOLS_API}/enrollment/{year}/grade-99/?fips={fips_num}"
            enr_rows = _paginate_urban_api(enr_url)

            directory_df = pd.DataFrame(dir_rows)
            enrollment_df = pd.DataFrame(enr_rows) if enr_rows else None
            data_year = year
            print(f"      Schools: {len(directory_df):,} from {year}")
            break
        except Exception as e:
            continue

    if directory_df is None or directory_df.empty:
        print(f"      Schools: no data found")
        return pd.DataFrame()

    # Merge enrollment into directory
    if enrollment_df is not None and not enrollment_df.empty:
        if "ncessch" in enrollment_df.columns and "enrollment" in enrollment_df.columns:
            enr_agg = enrollment_df.groupby("ncessch")["enrollment"].sum().reset_index()
            directory_df = directory_df.merge(enr_agg, on="ncessch", how="left",
                                              suffixes=("", "_enr"))

    # Standardize columns
    col_map = {
        "latitude": "lat", "longitude": "lon",
        "school_name": "school_name", "ncessch": "ncessch",
        "enrollment": "enrollment", "school_level": "school_level",
        "county_fips": "county_fips", "leaid": "leaid",
        "school_type": "school_type",
    }

    keep = [c for c in col_map if c in directory_df.columns]
    df = directory_df[keep].rename(columns=col_map)

    # Filter valid coordinates
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df[df["lat"].notna() & df["lon"].notna() & (df["lat"] != 0)].copy()

    # Clean enrollment
    if "enrollment" in df.columns:
        df["enrollment"] = pd.to_numeric(df["enrollment"], errors="coerce").fillna(0).astype(int)

    df["data_year"] = data_year
    return df.reset_index(drop=True)


def _paginate_urban_api(url, max_pages=10):
    """Paginate Urban Institute API (uses 'next' URL)."""
    all_rows = []
    current_url = url

    for page in range(max_pages):
        for attempt in range(3):
            try:
                resp = SESSION.get(current_url, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    return all_rows
                time.sleep(2 ** (attempt + 1))

        results = data.get("results", [])
        all_rows.extend(results)

        next_url = data.get("next")
        if not next_url:
            break
        current_url = next_url
        time.sleep(0.3)

    return all_rows


# ═══════════════════════════════════════════════════════════════
#  SOURCE 2: BRIDGES (National Bridge Inventory)
# ═══════════════════════════════════════════════════════════════

def download_bridges(state_info, bbox):
    """
    Download bridge inventory from NBI via BTS ArcGIS.
    State filter: STATE_CODE_001 = state FIPS.
    """
    fips = state_info["fips"]
    rows = download_arcgis_features(
        BRIDGES_URL, bbox, BRIDGE_FIELDS,
        state_fips=fips, state_filter_field="STATE_CODE_001",
        page_size=2000, max_pages=100,
    )

    if not rows:
        print(f"      Bridges: no data")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Standardize lat/lon — geometry-extracted "lat"/"lon" already present
    # from download_arcgis_features; fall back to attribute columns if not
    if "lat" not in df.columns:
        for col in df.columns:
            if col.upper() == "LATITUDE":
                df["lat"] = df[col]
            elif col.upper() == "LONGITUDE":
                df["lon"] = df[col]

    # Derive condition rating (Good/Fair/Poor)
    # NBI: deck=058, super=059, sub=060. Rating 0-9, N=not applicable.
    # Good: all ≥ 7. Fair: min 5-6. Poor: any ≤ 4.
    def _nbi_condition(row):
        ratings = []
        for col in ["DECK_COND_058", "SUPERSTRUCTURE_COND_059", "SUBSTRUCTURE_COND_060"]:
            val = str(row.get(col, "")).strip()
            if val.isdigit():
                ratings.append(int(val))
        if not ratings:
            return "Unknown"
        min_r = min(ratings)
        if min_r <= 4:
            return "Poor"
        elif min_r <= 6:
            return "Fair"
        else:
            return "Good"

    df["condition"] = df.apply(_nbi_condition, axis=1)

    # Clean up
    df["lat"] = pd.to_numeric(df.get("lat", pd.Series()), errors="coerce")
    df["lon"] = pd.to_numeric(df.get("lon", pd.Series()), errors="coerce")
    df = df[df["lat"].notna() & df["lon"].notna()].copy()

    # Rename to standard lowercase
    rename = {
        "FEATURES_DESC_006A": "feature_desc",
        "FACILITY_CARRIED_007": "facility_carried",
        "STRUCTURE_NUMBER_008": "structure_number",
        "YEAR_BUILT_027": "year_built",
        "ADT_029": "adt",
        "TRAFFIC_LANES_ON_028A": "lanes",
        "OPEN_CLOSED_POSTED_041": "status",
        "ROADWAY_WIDTH_MT_051": "width_m",
    }
    for old, new in rename.items():
        if old in df.columns:
            df[new] = df[old]

    keep = ["lat", "lon", "condition", "feature_desc", "facility_carried",
            "structure_number", "year_built", "adt", "lanes", "status", "width_m"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].reset_index(drop=True)

    print(f"      Bridges: {len(df):,} (Good={sum(df['condition']=='Good'):,}, "
          f"Fair={sum(df['condition']=='Fair'):,}, Poor={sum(df['condition']=='Poor'):,})")
    return df


# ═══════════════════════════════════════════════════════════════
#  SOURCE 3: RAILROAD GRADE CROSSINGS (FRA)
# ═══════════════════════════════════════════════════════════════

# FRA Warning Device codes
WDCODE_MAP = {
    1: "None", 2: "Other Signs/Signals", 3: "Crossbucks",
    4: "Stop Signs", 5: "Special Warning", 6: "Flashing Lights",
    7: "Flashing Lights + Gates", 8: "Wigwags", 9: "Bells",
    10: "Special", 11: "Highway Traffic Signals", 12: "Other",
}


def download_rail_crossings(state_info, bbox):
    """
    Download railroad grade crossings from FRA via BTS ArcGIS.
    Uses spatial filter (no state FIPS field available on all records).
    """
    rows = download_arcgis_features(
        RAIL_XING_URL, bbox, RAIL_XING_FIELDS,
        page_size=2000, max_pages=50,
    )

    if not rows:
        print(f"      Rail crossings: no data")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Standardize lat/lon — geometry-extracted "lat"/"lon" already present
    # from download_arcgis_features; fall back to attribute columns if not
    if "lat" not in df.columns:
        for col in df.columns:
            if col.upper() == "LATITUDE":
                df["lat"] = df[col]
            elif col.upper() == "LONGITUDE":
                df["lon"] = df[col]

    if "lat" not in df.columns:
        print(f"      Rail crossings: no lat/lon fields found")
        return pd.DataFrame()

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df[df["lat"].notna() & df["lon"].notna()].copy()

    # Map warning device code
    if "WDCODE" in df.columns:
        df["warning_device"] = df["WDCODE"].apply(
            lambda x: WDCODE_MAP.get(int(x), "Unknown") if pd.notna(x) and str(x).strip().isdigit() else "Unknown"
        )

    # Derive simplified warning level for enricher
    def _warning_level(wd):
        if wd in ("Flashing Lights + Gates",):
            return "Gates"
        elif wd in ("Flashing Lights", "Highway Traffic Signals"):
            return "Signals"
        elif wd in ("Crossbucks", "Stop Signs", "Other Signs/Signals"):
            return "Signs"
        elif wd == "None":
            return "None"
        return "Other"

    if "warning_device" in df.columns:
        df["warning_level"] = df["warning_device"].apply(_warning_level)

    # Rename
    rename = {
        "STREET": "street", "HIGHWAY": "highway", "RAILROAD": "railroad",
        "CROSSING": "crossing_id", "CITY": "city",
        "TTSTN": "trains_per_day", "APTS": "auto_crashes_5yr",
        "TYPEXING": "crossing_type", "POSXING": "position",
        "ILLUMINA": "illuminated",
    }
    for old, new in rename.items():
        if old in df.columns:
            df[new] = df[old]

    keep = ["lat", "lon", "crossing_id", "street", "highway", "railroad",
            "city", "warning_device", "warning_level",
            "trains_per_day", "auto_crashes_5yr", "illuminated"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].reset_index(drop=True)

    if "warning_level" in df.columns:
        wl = df["warning_level"].value_counts()
        print(f"      Rail crossings: {len(df):,} "
              f"(Gates={wl.get('Gates',0):,}, Signals={wl.get('Signals',0):,}, "
              f"Signs={wl.get('Signs',0):,}, None={wl.get('None',0):,})")
    else:
        print(f"      Rail crossings: {len(df):,}")
    return df


# ═══════════════════════════════════════════════════════════════
#  SOURCE 4: TRANSIT STOPS (National Transit Map)
# ═══════════════════════════════════════════════════════════════

def download_transit(state_info, bbox):
    """
    Download transit stops from NTM via BTS ArcGIS.
    Uses spatial filter (bounding box).
    """
    rows = download_arcgis_features(
        TRANSIT_URL, bbox, TRANSIT_FIELDS,
        page_size=5000, max_pages=50,
    )

    if not rows:
        print(f"      Transit stops: no data")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Standardize lat/lon (field names vary)
    lat_field = None
    lon_field = None
    for c in df.columns:
        cl = c.lower()
        if cl in ("stop_lat", "lat", "latitude", "y"):
            lat_field = c
        if cl in ("stop_lon", "lon", "longitude", "lng", "x"):
            lon_field = c

    if lat_field and lon_field:
        df["lat"] = pd.to_numeric(df[lat_field], errors="coerce")
        df["lon"] = pd.to_numeric(df[lon_field], errors="coerce")
    elif "lat" not in df.columns:
        print(f"      Transit stops: no lat/lon fields found")
        return pd.DataFrame()

    df = df[df["lat"].notna() & df["lon"].notna()].copy()

    # Standardize column names
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "stop_name":
            rename[c] = "stop_name"
        elif cl == "stop_id":
            rename[c] = "stop_id"
        elif cl == "agency_name":
            rename[c] = "agency"
        elif cl == "route_type_text":
            rename[c] = "route_type"
        elif cl == "wheelchair_boarding":
            rename[c] = "wheelchair"
    df = df.rename(columns=rename)

    keep = ["lat", "lon", "stop_name", "stop_id", "agency", "route_type", "wheelchair"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].reset_index(drop=True)

    if "route_type" in df.columns:
        rt = df["route_type"].value_counts().head(5)
        print(f"      Transit stops: {len(df):,} (top: {dict(rt)})")
    else:
        print(f"      Transit stops: {len(df):,}")
    return df


# ═══════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS (same pattern as generate_hpms_data.py)
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


def r2_exists(s3, bucket, prefix, abbr, source_type):
    if not s3:
        return False
    try:
        s3.head_object(Bucket=bucket, Key=f'{prefix}/cache/{abbr}_{source_type}.parquet.gz')
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


def save_and_upload(df, cache_dir, abbr, prefix, source_type, s3, bucket, local_only):
    """Save DataFrame as gzipped parquet and optionally upload to R2."""
    if df is None or df.empty:
        return False

    parquet_path = cache_dir / f"{abbr}_{source_type}.parquet"
    gz_path = cache_dir / f"{abbr}_{source_type}.parquet.gz"

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    raw_mb, gz_mb = gzip_file(parquet_path, gz_path)
    parquet_path.unlink(missing_ok=True)  # Remove uncompressed

    print(f"      Saved: {gz_path.name} ({raw_mb:.1f}MB → {gz_mb:.1f}MB)")

    if not local_only and s3:
        r2_key = f"{prefix}/cache/{abbr}_{source_type}.parquet.gz"
        if r2_upload(s3, gz_path, bucket, r2_key):
            print(f"      Uploaded: {r2_key}")
            return True

    return True


# ═══════════════════════════════════════════════════════════════
#  MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════

VALID_SOURCES = {"schools", "bridges", "rail_crossings", "transit", "all"}


def process_state(state_info, cache_dir, s3, bucket,
                  force=False, local_only=False, sources=None):
    """Download federal data for one state."""
    name = state_info["name"]
    abbr = state_info["abbreviation"]
    fips = state_info["fips"]
    prefix = state_info["r2_prefix"]
    bbox = STATE_BBOX.get(abbr)

    if sources is None:
        sources = {"schools", "bridges", "rail_crossings", "transit"}

    print(f"\n    {name} ({abbr.upper()}, FIPS={fips})")
    results = {}

    # ── Schools ──
    if "schools" in sources:
        file_type = "schools"
        if not force and not local_only and r2_exists(s3, bucket, prefix, abbr, file_type):
            print(f"      [skip] schools already in R2")
            results[file_type] = "skipped"
        elif not force and local_only and (cache_dir / f"{abbr}_{file_type}.parquet.gz").exists():
            print(f"      [skip] schools already cached locally")
            results[file_type] = "skipped"
        else:
            try:
                df = download_schools(state_info)
                if save_and_upload(df, cache_dir, abbr, prefix, file_type, s3, bucket, local_only):
                    results[file_type] = "completed"
                else:
                    results[file_type] = "empty"
            except Exception as e:
                print(f"      Schools ERROR: {e}")
                results[file_type] = "failed"

    # ── Bridges ──
    if "bridges" in sources:
        file_type = "bridges"
        if not force and not local_only and r2_exists(s3, bucket, prefix, abbr, file_type):
            print(f"      [skip] bridges already in R2")
            results[file_type] = "skipped"
        elif not force and local_only and (cache_dir / f"{abbr}_{file_type}.parquet.gz").exists():
            print(f"      [skip] bridges already cached locally")
            results[file_type] = "skipped"
        else:
            try:
                df = download_bridges(state_info, bbox)
                if save_and_upload(df, cache_dir, abbr, prefix, file_type, s3, bucket, local_only):
                    results[file_type] = "completed"
                else:
                    results[file_type] = "empty"
            except Exception as e:
                print(f"      Bridges ERROR: {e}")
                results[file_type] = "failed"
        time.sleep(5)  # BTS needs breathing room between sources

    # ── Railroad Crossings ──
    if "rail_crossings" in sources:
        file_type = "rail_crossings"
        if not force and not local_only and r2_exists(s3, bucket, prefix, abbr, file_type):
            print(f"      [skip] rail_crossings already in R2")
            results[file_type] = "skipped"
        elif not force and local_only and (cache_dir / f"{abbr}_{file_type}.parquet.gz").exists():
            print(f"      [skip] rail_crossings already cached locally")
            results[file_type] = "skipped"
        else:
            try:
                df = download_rail_crossings(state_info, bbox)
                if save_and_upload(df, cache_dir, abbr, prefix, file_type, s3, bucket, local_only):
                    results[file_type] = "completed"
                else:
                    results[file_type] = "empty"
            except Exception as e:
                print(f"      Rail crossings ERROR: {e}")
                results[file_type] = "failed"
        time.sleep(5)  # BTS needs breathing room between sources

    # ── Transit Stops ──
    if "transit" in sources:
        file_type = "transit"
        if not force and not local_only and r2_exists(s3, bucket, prefix, abbr, file_type):
            print(f"      [skip] transit already in R2")
            results[file_type] = "skipped"
        elif not force and local_only and (cache_dir / f"{abbr}_{file_type}.parquet.gz").exists():
            print(f"      [skip] transit already cached locally")
            results[file_type] = "skipped"
        else:
            try:
                df = download_transit(state_info, bbox)
                if save_and_upload(df, cache_dir, abbr, prefix, file_type, s3, bucket, local_only):
                    results[file_type] = "completed"
                else:
                    results[file_type] = "empty"
            except Exception as e:
                print(f"      Transit ERROR: {e}")
                results[file_type] = "failed"

    return results


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CrashLens Federal Safety Data Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
    python generate_federal_data.py --state de                     # All 4 sources
    python generate_federal_data.py --state de --source schools    # Schools only
    python generate_federal_data.py --state de --source bridges rail_crossings
    python generate_federal_data.py --state de va md               # Multiple states
    python generate_federal_data.py --all                          # All 51 states
    python generate_federal_data.py --state de --local-only        # No R2 upload
    python generate_federal_data.py --all --force                  # Regenerate all

SOURCES:
    schools         School locations + enrollment (Urban Institute API)
    bridges         National Bridge Inventory (BTS ArcGIS)
    rail_crossings  Railroad grade crossings (FRA via BTS)
    transit         Transit stops (NTM via BTS)
    all             All 4 sources (default)
        """,
    )
    parser.add_argument('--state', nargs='+', help='State abbreviation(s)')
    parser.add_argument('--all', action='store_true', help='All 51 states')
    parser.add_argument('--source', nargs='+', default=['all'],
                        choices=['schools', 'bridges', 'rail_crossings', 'transit', 'all'],
                        help='Which source(s) to download')
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

    sources = set(args.source)
    if "all" in sources:
        sources = {"schools", "bridges", "rail_crossings", "transit"}

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

    print(f"\n{'=' * 65}")
    print(f"  CrashLens Federal Safety Data Downloader")
    print(f"  States: {len(states)} | Sources: {sorted(sources)}")
    print(f"  R2: {'yes' if s3 else 'local'} | Force: {args.force}")
    print(f"{'=' * 65}")

    totals = {"completed": 0, "skipped": 0, "failed": 0, "empty": 0}
    t_start = time.time()

    for i, state in enumerate(states, 1):
        print(f"\n  [{i}/{len(states)}]", end="")
        try:
            results = process_state(
                state, cache_dir, s3, bucket,
                force=args.force, local_only=args.local_only or not s3,
                sources=sources,
            )
            for src, status in results.items():
                totals[status] = totals.get(status, 0) + 1
        except Exception as e:
            print(f"  ERROR: {state['name']} — {e}")
            totals["failed"] += 1
        time.sleep(0.5)

    elapsed = time.time() - t_start
    print(f"\n{'=' * 65}")
    print(f"  COMPLETE in {elapsed / 60:.1f} min")
    print(f"  Completed: {totals['completed']}")
    print(f"  Skipped:   {totals['skipped']}")
    print(f"  Empty:     {totals['empty']}")
    print(f"  Failed:    {totals['failed']}")
    print(f"{'=' * 65}\n")


if __name__ == '__main__':
    main()
