#!/usr/bin/env python3
"""
Download crash data from Virginia Roads ArcGIS API.
Filters for a specified Virginia jurisdiction and applies road type filters.

Usage:
    python download_crash_data.py                      # Uses default jurisdiction from config
    python download_crash_data.py --jurisdiction henrico
    python download_crash_data.py --jurisdiction chesterfield --filter countyPlusVDOT
    python download_crash_data.py --list               # List available jurisdictions
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Config file path
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")

# Output configuration
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")

# Pagination settings
RECORDS_PER_REQUEST = 2000

# Retry settings
MAX_RETRIES = 4
RETRY_BACKOFF_FACTOR = 2  # 2s, 4s, 8s, 16s


def create_session_with_retries():
    """Create a requests session with retry logic and browser-like headers."""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # ArcGIS Hub blocks requests with default Python User-Agent (returns 403).
    # Use a browser-like User-Agent to avoid this.
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/csv,application/json,text/html,*/*',
    })
    return session


def make_request_with_retry(url, params=None, timeout=60, max_manual_retries=3):
    """
    Make HTTP request with manual retry logic for network errors.
    Uses exponential backoff: 2s, 4s, 8s, 16s
    """
    session = create_session_with_retries()
    last_exception = None

    for attempt in range(max_manual_retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout as e:
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"Request timeout (attempt {attempt + 1}/{max_manual_retries}). Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"Connection error (attempt {attempt + 1}/{max_manual_retries}). Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            # Don't retry on 4xx errors (except 429 which is handled by Retry strategy)
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                raise
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"HTTP error (attempt {attempt + 1}/{max_manual_retries}). Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise last_exception or Exception("Request failed after all retries")


def health_check_api(api_url):
    """
    Perform a health check on the API endpoint.
    Returns True if API is accessible, False otherwise.
    """
    try:
        params = {'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}
        response = make_request_with_retry(api_url, params=params, timeout=30, max_manual_retries=2)
        data = response.json()

        if 'error' in data:
            logger.error(f"API health check failed: {data['error']}")
            return False

        count = data.get('count', 0)
        logger.info(f"API health check passed. Total records available: {count:,}")
        return True
    except Exception as e:
        logger.error(f"API health check failed: {e}")
        return False


def load_config():
    """Load configuration from config.json."""
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file not found: {CONFIG_FILE}")
        logger.info("Please ensure config.json exists in the same directory as this script.")
        sys.exit(1)

    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)

    logger.info(f"Loaded config with {len(config.get('jurisdictions', {}))} jurisdictions")
    return config


def get_jurisdiction_config(config, jurisdiction_id):
    """Get configuration for a specific jurisdiction."""
    jurisdictions = config.get('jurisdictions', {})

    if jurisdiction_id not in jurisdictions:
        logger.error(f"Unknown jurisdiction: {jurisdiction_id}")
        logger.info(f"Available jurisdictions: {', '.join(sorted(jurisdictions.keys()))}")
        sys.exit(1)

    return jurisdictions[jurisdiction_id]


def list_jurisdictions(config):
    """List all available jurisdictions."""
    jurisdictions = config.get('jurisdictions', {})

    counties = []
    cities = []

    for jid, jdata in jurisdictions.items():
        item = (jid, jdata.get('name', jid), jdata.get('jurisCode', '?'))
        if jdata.get('type') == 'city':
            cities.append(item)
        else:
            counties.append(item)

    counties.sort(key=lambda x: x[1])
    cities.sort(key=lambda x: x[1])

    print("\n" + "=" * 60)
    print("AVAILABLE VIRGINIA JURISDICTIONS")
    print("=" * 60)

    print(f"\nCOUNTIES ({len(counties)}):")
    print("-" * 40)
    for jid, name, code in counties:
        print(f"  {jid:<25} {name:<30} (Code: {code})")

    print(f"\nINDEPENDENT CITIES ({len(cities)}):")
    print("-" * 40)
    for jid, name, code in cities:
        print(f"  {jid:<25} {name:<30} (Code: {code})")

    print(f"\nTotal: {len(jurisdictions)} jurisdictions")
    print("=" * 60)


def get_arcgis_record_count(api_url, where_clause):
    """Get total record count from ArcGIS API with retry logic."""
    params = {
        'where': where_clause,
        'returnCountOnly': 'true',
        'f': 'json'
    }

    response = make_request_with_retry(api_url, params=params, timeout=60)
    data = response.json()

    if 'error' in data:
        raise Exception(f"ArcGIS API error: {data['error']}")

    return data.get('count', 0)


def download_arcgis_page(api_url, where_clause, offset):
    """Download a page of records from ArcGIS API with retry logic."""
    params = {
        'where': where_clause,
        'outFields': '*',
        'returnGeometry': 'true',
        'outSR': '4326',
        'resultOffset': offset,
        'resultRecordCount': RECORDS_PER_REQUEST,
        'f': 'json'
    }

    response = make_request_with_retry(api_url, params=params, timeout=120)
    data = response.json()

    if 'error' in data:
        raise Exception(f"ArcGIS API error: {data['error']}")

    features = data.get('features', [])
    records = []
    for feature in features:
        record = feature.get('attributes', {})
        # Extract geometry coordinates
        if 'geometry' in feature and feature['geometry']:
            record['x'] = feature['geometry'].get('x')
            record['y'] = feature['geometry'].get('y')
        records.append(record)
    return records


def get_data_source_config(config, state='virginia'):
    """
    Get the data source configuration for a state.
    Handles both old format (config['dataSource']) and new format (config['dataSources'][state]).
    """
    # Try new format first: dataSources.<state>
    data_sources = config.get('dataSources', {})
    if state in data_sources:
        return data_sources[state]

    # Fall back to old format: dataSource (singular)
    return config.get('dataSource', {})


def find_working_api_url(config, state='virginia'):
    """
    Try to find a working API URL from configured options.
    Returns the first URL that passes health check.
    """
    data_source = get_data_source_config(config, state)
    primary_url = data_source.get('apiUrl')
    alternative_urls = data_source.get('apiUrlAlternatives', [])

    # Build list of URLs to try
    urls_to_try = []
    if primary_url:
        urls_to_try.append(primary_url)
    urls_to_try.extend(alternative_urls)

    # Default fallback
    if not urls_to_try:
        urls_to_try.append("https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/CrashData_Basic_Updated/FeatureServer/0/query")

    for url in urls_to_try:
        logger.info(f"Testing API endpoint: {url}")
        if health_check_api(url):
            logger.info(f"Using API endpoint: {url}")
            return url
        else:
            logger.warning(f"API endpoint not available: {url}")

    logger.error("No working API endpoint found!")
    return None


def download_from_arcgis(config, jurisdiction_config, state='virginia'):
    """
    Download crash data from ArcGIS REST API with pagination.
    Tries multiple API endpoints and filters for the specified jurisdiction.
    """
    api_url = find_working_api_url(config, state)

    if not api_url:
        raise Exception("No working ArcGIS API endpoint available")

    juris_code = jurisdiction_config.get('jurisCode', '')
    name_patterns = jurisdiction_config.get('namePatterns', [])
    fips = jurisdiction_config.get('fips', '')
    name = jurisdiction_config.get('name', 'Unknown')

    logger.info(f"Attempting download from ArcGIS REST API for {name}...")

    # Build WHERE clauses for this jurisdiction
    # Order matters: Physical_Juris_Name is tried first because it captures ALL
    # roads within a jurisdiction's boundaries (including Interstate, State Hwy).
    # Juris_Code may only match locally-maintained roads, giving partial results.
    where_clauses = []

    if name_patterns:
        for pattern in name_patterns:
            where_clauses.append(f"Physical_Juris_Name LIKE '%{pattern}%'")

    if juris_code:
        where_clauses.append(f"Juris_Code = '{juris_code}' OR Juris_Code = {juris_code}")

    if fips:
        where_clauses.append(f"COUNTYFP = '{fips}' OR FIPS = '{fips}'")

    if not where_clauses:
        logger.error("No filter criteria available for this jurisdiction")
        sys.exit(1)

    # Try each where clause until one works
    all_records = []
    successful_clause = None
    count = 0

    for where_clause in where_clauses:
        try:
            count = get_arcgis_record_count(api_url, where_clause)
            if count > 0:
                logger.info(f"Found {count} records with filter: {where_clause}")
                successful_clause = where_clause
                break
        except Exception as e:
            logger.debug(f"Filter '{where_clause}' failed: {e}")
            continue

    if not successful_clause:
        # Fallback: get all records and filter locally
        logger.info("Specific filters failed, downloading all records for local filtering...")
        successful_clause = "1=1"
        count = get_arcgis_record_count(api_url, successful_clause)
        logger.info(f"Total records in dataset: {count}")

        # Warn if dataset is suspiciously small (likely district-specific, not statewide)
        if count < 50000:
            logger.warning(f"API has only {count:,} records — likely a district-specific dataset, not statewide")
            logger.warning("This endpoint may not contain data for the requested jurisdiction")

    # Download with pagination
    offset = 0
    while offset < count:
        logger.info(f"Downloading records {offset} to {min(offset + RECORDS_PER_REQUEST, count)}...")
        records = download_arcgis_page(api_url, successful_clause, offset)

        if not records:
            break

        all_records.extend(records)
        offset += RECORDS_PER_REQUEST

    logger.info(f"Downloaded {len(all_records)} total records from ArcGIS API")

    if not all_records:
        raise Exception("No records returned from ArcGIS API")

    return pd.DataFrame(all_records)


def download_csv_from_url(url, max_poll_attempts=6, poll_interval_seconds=30):
    """Download crash data CSV from a single URL with polling for async generation.

    ArcGIS Hub may return a JSON "Pending" status for large datasets that are
    generated asynchronously. This function polls until the CSV is ready.
    """
    import io

    for attempt in range(max_poll_attempts):
        response = make_request_with_retry(url, timeout=300, max_manual_retries=4)
        text = response.text.strip()

        # Check for async generation response (ArcGIS Hub returns JSON when generating)
        if text.startswith('{'):
            try:
                status = json.loads(text)
                if status.get('status') == 'Pending' or 'being generated' in status.get('message', ''):
                    if attempt < max_poll_attempts - 1:
                        logger.info(f"CSV generation pending, polling in {poll_interval_seconds}s "
                                    f"(attempt {attempt + 1}/{max_poll_attempts})...")
                        time.sleep(poll_interval_seconds)
                        continue
                    else:
                        raise Exception(
                            f"CSV still pending after {max_poll_attempts * poll_interval_seconds}s. "
                            f"The ArcGIS Hub may be slow — try again later.")
            except json.JSONDecodeError:
                pass  # Not valid JSON, try parsing as CSV below

        # Parse as CSV
        df = pd.read_csv(io.StringIO(text))

        # Validate it looks like real crash data
        if len(df) < 10 or len(df.columns) < 5:
            raise Exception(f"CSV appears empty or malformed ({len(df)} rows, {len(df.columns)} columns)")

        logger.info(f"Downloaded {len(df)} records from CSV endpoint")
        return df

    raise Exception("CSV download timed out waiting for file generation")


def download_from_fallback(config, state='virginia'):
    """Download crash data from CSV URLs with retry logic.

    Tries multiple CSV download URLs in order. ArcGIS Hub sometimes blocks
    specific item URLs or returns 403, so having alternatives is important.
    """
    data_source = get_data_source_config(config, state)

    # Build list of CSV URLs to try (primary + alternatives)
    csv_urls = []

    primary_url = data_source.get('fallbackUrl')
    if primary_url:
        csv_urls.append(primary_url)

    csv_urls.extend(data_source.get('fallbackUrlAlternatives', []))

    # Hardcoded defaults if nothing configured
    if not csv_urls:
        csv_urls = [
            # CrashData Basic (the original default)
            "https://www.virginiaroads.org/api/download/v1/items/1a96a2f31b4f4d77991471b6cabb38ba/csv?layers=0",
            # Full Crash dataset (statewide with all fields)
            "https://www.virginiaroads.org/api/download/v1/items/3bd854bff90d49eaa85bdc68acf952e0/csv?layers=0",
            # CrashData Details (layer 1)
            "https://www.virginiaroads.org/api/download/v1/items/101101cecac34f28b38c0846e847bd0b/csv?layers=1",
        ]

    last_error = None
    for url in csv_urls:
        try:
            logger.info(f"Trying CSV download: {url}")
            df = download_csv_from_url(url)
            return df
        except Exception as e:
            last_error = e
            logger.warning(f"CSV download failed for {url}: {e}")
            continue

    raise last_error or Exception("All CSV download URLs failed")


def filter_jurisdiction(df, jurisdiction_config):
    """Filter dataframe to only include records for the specified jurisdiction."""
    original_count = len(df)

    juris_code = jurisdiction_config.get('jurisCode', '')
    name_patterns = jurisdiction_config.get('namePatterns', [])
    fips = jurisdiction_config.get('fips', '')
    name = jurisdiction_config.get('name', 'Unknown')

    # Log available columns for debugging
    logger.info(f"Available columns in data: {list(df.columns)[:20]}...")  # First 20

    # Try multiple filter approaches
    mask = pd.Series([False] * len(df))
    found_columns = []

    # Check various possible column names for jurisdiction code
    if juris_code:
        juris_columns = [
            'Juris_Code', 'JURIS_CODE', 'juris_code', 'Juris Code',
            'JURISCODE', 'JurisCode', 'Jurisdiction_Code', 'JURISDICTION_CODE'
        ]
        for col in juris_columns:
            if col in df.columns:
                found_columns.append(f"juris_code:{col}")
                # Try both numeric and string comparison
                mask |= df[col].astype(str).str.strip() == str(juris_code)
                mask |= df[col].astype(str).str.strip() == str(int(juris_code)) if juris_code.isdigit() else False
                break
        # Also try case-insensitive column search
        if not any('juris_code' in c for c in found_columns):
            for col in df.columns:
                if 'juris' in col.lower() and 'code' in col.lower():
                    found_columns.append(f"juris_code:{col}")
                    mask |= df[col].astype(str).str.strip() == str(juris_code)
                    break

    # Check Physical Juris Name
    if name_patterns:
        name_columns = [
            'Physical_Juris_Name', 'PHYSICAL_JURIS_NAME', 'Physical Juris Name',
            'PHYSICAL_JURIS', 'PhysicalJurisName', 'PHYSICALJURISNAME',
            'Physical_Jurisdiction', 'PHYSICAL_JURISDICTION', 'Jurisdiction_Name'
        ]
        for col in name_columns:
            if col in df.columns:
                found_columns.append(f"name:{col}")
                for pattern in name_patterns:
                    mask |= df[col].astype(str).str.upper().str.contains(pattern.upper(), na=False)
                break
        # Also try case-insensitive column search
        if not any('name:' in c for c in found_columns):
            for col in df.columns:
                if 'juris' in col.lower() and 'name' in col.lower():
                    found_columns.append(f"name:{col}")
                    for pattern in name_patterns:
                        mask |= df[col].astype(str).str.upper().str.contains(pattern.upper(), na=False)
                    break

    # Check FIPS code
    if fips:
        fips_columns = [
            'COUNTYFP', 'FIPS', 'County_FIPS', 'countyfp',
            'COUNTY_FIPS', 'CountyFIPS', 'FIPS_Code', 'FIPSCode'
        ]
        for col in fips_columns:
            if col in df.columns:
                found_columns.append(f"fips:{col}")
                mask |= df[col].astype(str).str.strip() == fips
                mask |= df[col].astype(str).str.strip().str.zfill(3) == fips.zfill(3)
                break

    logger.info(f"Filter columns found: {found_columns}")
    logger.info(f"Filter criteria - juris_code: {juris_code}, fips: {fips}, patterns: {name_patterns}")

    df_filtered = df[mask].copy().reset_index(drop=True)

    # If no records found, log sample values from relevant columns
    if len(df_filtered) == 0 and len(df) > 0:
        logger.warning("No records matched! Sample values from potential jurisdiction columns:")
        for col in df.columns:
            col_lower = col.lower()
            if any(kw in col_lower for kw in ['juris', 'county', 'fips', 'physical']):
                sample_vals = df[col].dropna().astype(str).unique()[:5]
                logger.warning(f"  {col}: {list(sample_vals)}")

    logger.info(f"Filtered from {original_count} to {len(df_filtered)} {name} records")

    return df_filtered


def filter_by_road_system(df, filter_profile):
    """
    Filter dataframe based on road system type or ownership values.
    """
    original_count = len(df)

    system_values = filter_profile.get('systemValues', [])
    ownership_values = filter_profile.get('ownershipValues', [])
    exclude_patterns = filter_profile.get('excludeRoutePatterns', [])
    filter_name = filter_profile.get('name', 'Unknown Filter')

    if ownership_values:
        # Filter by ownership column
        ownership_columns = ['Ownership', 'OWNERSHIP', 'ownership']
        ownership_col = None
        for col in ownership_columns:
            if col in df.columns:
                ownership_col = col
                break

        if ownership_col is None:
            logger.warning("Could not find Ownership column, skipping ownership filter")
            return df

        mask = df[ownership_col].astype(str).apply(
            lambda x: any(val in x for val in ownership_values)
        )
        df_filtered = df[mask].copy().reset_index(drop=True)

    elif system_values:
        # Filter by system values
        system_columns = ['SYSTEM', 'System', 'system']
        system_col = None
        for col in system_columns:
            if col in df.columns:
                system_col = col
                break

        if system_col is None:
            logger.warning("Could not find SYSTEM column, skipping road system filter")
            return df

        mask = df[system_col].astype(str).str.upper().apply(
            lambda x: any(sys_val.upper() in x for sys_val in system_values)
        )
        df_filtered = df[mask].copy().reset_index(drop=True)

    else:
        logger.warning(f"Filter profile '{filter_name}' has no systemValues or ownershipValues, returning all data")
        return df

    # Apply route exclusion patterns if specified
    if exclude_patterns and len(df_filtered) > 0:
        route_columns = ['RTE_NM', 'RTE_NAME', 'RTE NAME', 'Rte_Name', 'Route_Name', 'ROUTE_NAME', 'RTE_Name', 'RTE Name']
        route_col = None
        for col in route_columns:
            if col in df_filtered.columns:
                route_col = col
                break

        if route_col:
            route_values = df_filtered[route_col].astype(str)
            exclude_mask = route_values.apply(
                lambda x: any(pd.Series([x]).str.contains(pattern, case=False, na=False, regex=True).iloc[0]
                             for pattern in exclude_patterns)
            )

            df_filtered = df_filtered[~exclude_mask].copy().reset_index(drop=True)

    logger.info(f"Filtered from {original_count} to {len(df_filtered)} records using '{filter_name}'")

    return df_filtered


def standardize_columns(df):
    """Standardize column names to match expected format for index.html."""
    column_mapping = {
        # Core identifiers
        'OBJECTID': 'OBJECTID',
        'DOCUMENT_NBR': 'Document Nbr',
        'Document_Nbr': 'Document Nbr',

        # Crash timing
        'CRASH_YEAR': 'Crash Year',
        'Crash_Year': 'Crash Year',
        'CRASH_DT': 'Crash Date',
        'Crash_Date': 'Crash Date',
        'CRASH_MILITARY_TM': 'Crash Military Time',
        'Crash_Military_Time': 'Crash Military Time',

        # Severity
        'CRASH_SEVERITY': 'Crash Severity',
        'Crash_Severity': 'Crash Severity',
        'K_PEOPLE': 'K_People',
        'A_PEOPLE': 'A_People',
        'B_PEOPLE': 'B_People',
        'C_PEOPLE': 'C_People',

        # Injury counts
        'PERSONS_INJURED': 'Persons Injured',
        'Persons_Injured': 'Persons Injured',
        'PEDESTRIANS_KILLED': 'Pedestrians Killed',
        'Pedestrians_Killed': 'Pedestrians Killed',
        'PEDESTRIANS_INJURED': 'Pedestrians Injured',
        'Pedestrians_Injured': 'Pedestrians Injured',
        'VEH_COUNT': 'Vehicle Count',
        'Vehicle_Count': 'Vehicle Count',

        # Crash characteristics
        'COLLISION_TYPE': 'Collision Type',
        'Collision_Type': 'Collision Type',
        'WEATHER_CONDITION': 'Weather Condition',
        'Weather_Condition': 'Weather Condition',
        'LIGHT_CONDITION': 'Light Condition',
        'Light_Condition': 'Light Condition',
        'ROADWAY_SURFACE_COND': 'Roadway Surface Condition',
        'Roadway_Surface_Condition': 'Roadway Surface Condition',
        'RELATION_TO_ROADWAY': 'Relation To Roadway',
        'Relation_To_Roadway': 'Relation To Roadway',
        'ROADWAY_ALIGNMENT': 'Roadway Alignment',
        'Roadway_Alignment': 'Roadway Alignment',
        'ROADWAY_SURFACE_TYPE': 'Roadway Surface Type',
        'Roadway_Surface_Type': 'Roadway Surface Type',
        'ROADWAY_DEFECT': 'Roadway Defect',
        'Roadway_Defect': 'Roadway Defect',
        'ROADWAY_DESCRIPTION': 'Roadway Description',
        'Roadway_Description': 'Roadway Description',

        # Intersection/control
        'INTERSECTION_TYPE': 'Intersection Type',
        'Intersection_Type': 'Intersection Type',
        'TRAFFIC_CONTROL_TYPE': 'Traffic Control Type',
        'Traffic_Control_Type': 'Traffic Control Type',
        'TRFC_CTRL_STATUS_TYPE': 'Traffic Control Status',
        'Traffic_Control_Status': 'Traffic Control Status',

        # Work zone / school
        'WORK_ZONE_RELATED': 'Work Zone Related',
        'Work_Zone_Related': 'Work Zone Related',
        'WORK_ZONE_LOCATION': 'Work Zone Location',
        'Work_Zone_Location': 'Work Zone Location',
        'WORK_ZONE_TYPE': 'Work Zone Type',
        'Work_Zone_Type': 'Work Zone Type',
        'SCHOOL_ZONE': 'School Zone',
        'School_Zone': 'School Zone',

        # First harmful event
        'FIRST_HARMFUL_EVENT': 'First Harmful Event',
        'First_Harmful_Event': 'First Harmful Event',
        'FIRST_HARMFUL_EVENT_LOC': 'First Harmful Event Loc',
        'First_Harmful_Event_Loc': 'First Harmful Event Loc',

        # Jurisdiction
        'JURIS_CODE': 'Juris Code',
        'Juris_Code': 'Juris Code',
        'PHYSICAL_JURIS': 'Physical Juris Name',
        'Physical_Juris_Name': 'Physical Juris Name',

        # Road classification
        'FUN': 'Functional Class',
        'Functional_Class': 'Functional Class',
        'FAC': 'Facility Type',
        'Facility_Type': 'Facility Type',
        'AREA_TYPE': 'Area Type',
        'Area_Type': 'Area Type',
        'SYSTEM': 'SYSTEM',
        'VSP': 'VSP',
        'OWNERSHIP': 'Ownership',

        # Planning/admin
        'PLAN_DISTRICT': 'Planning District',
        'Planning_District': 'Planning District',
        'MPO_NAME': 'MPO Name',
        'MPO_Name': 'MPO Name',
        'VDOT_DISTRICT': 'DOT District',
        'VDOT_District': 'DOT District',

        # Route/location
        'RTE_NM': 'RTE Name',
        'RTE_NAME': 'RTE Name',
        'RTE_Name': 'RTE Name',
        'RNS_MP': 'RNS MP',
        'NODE': 'Node',
        'OFFSET': 'Node Offset (ft)',
        'Node_Offset': 'Node Offset (ft)',

        # Coordinates (keep lowercase)
        'x': 'x',
        'y': 'y',

        # Boolean flags
        'ALCOHOL_NOTALCOHOL': 'Alcohol?',
        'BIKE_NONBIKE': 'Bike?',
        'PED_NONPED': 'Pedestrian?',
        'SPEED_NOTSPEED': 'Speed?',
        'DISTRACTED_NOTDISTRACTED': 'Distracted?',
        'DROWSY_NOTDROWSY': 'Drowsy?',
        'HITRUN_NOT_HITRUN': 'Hitrun?',
        'SENIOR_NOTSENIOR': 'Senior?',
        'YOUNG_NOTYOUNG': 'Young?',
        'NIGHT': 'Night?',
        'BELTED_UNBELTED': 'Unrestrained?',
        'MOTOR_NONMOTOR': 'Motorcycle?',

        # Additional boolean flags
        'DRUG_NODRUG': 'Drug Related?',
        'GR_NOGR': 'Guardrail Related?',
        'LGTRUCK_NONLGTRUCK': 'Lgtruck?',
        'MAINLINE_YN': 'Mainline?',

        # Speed and road attributes
        'SPEED_DIFF_MAX': 'Max Speed Diff',
        'RD_TYPE': 'RoadDeparture Type',
    }

    # Additional column names from newer ArcGIS endpoints
    column_mapping.update({
        'LOCAL_CASE_CD': 'Local Case CD',
        'ROUTE_OR_STREET_NM': 'Route or Street Name',
        'INTERSECTION_ANALYSIS': 'Intersection Analysis',
        'ANIMAL': 'Animal Related?',
    })

    # Column aliases for VDOT website download format (renamed columns)
    column_mapping.update({
        'Hit & Run?': 'Hitrun?',
        'Large Vehicle?': 'Lgtruck?',
        'UnBelted?': 'Unrestrained?',
        'Senior Driver?': 'Senior?',
        'Young Driver?': 'Young?',
    })

    # Rename columns that exist
    rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=rename_dict)

    # ── Decode ArcGIS FeatureServer coded domain values ──
    # The FeatureServer API returns raw numeric/abbreviated codes instead of
    # human-readable text. These mappings match the previous VDOT data format
    # that the frontend was built on.

    def _decode_column(col_name, code_map, skip_if_contains=None):
        """Decode a column's coded values to text labels (only if values are coded)."""
        if col_name not in df.columns:
            return
        raw = df[col_name].astype(str).str.strip()
        if not raw.isin(code_map.keys()).any():
            return
        if skip_if_contains and raw.str.contains(skip_if_contains, na=False).any():
            return
        df[col_name] = raw.map(code_map).fillna(df[col_name])

    # ── Date conversion: epoch milliseconds → formatted date ──
    # ArcGIS FeatureServer stores dates as epoch ms in UTC.
    # The previous VDOT dataset used M/D/YYYY H:MM in UTC (no offset).
    if 'Crash Date' in df.columns:
        import pandas as pd
        from datetime import timezone
        raw = df['Crash Date'].astype(str).str.strip()
        # Check if values look like epoch milliseconds (13+ digit numbers)
        sample = raw.dropna().iloc[:5] if len(raw) > 0 else pd.Series()
        if len(sample) > 0 and sample.str.match(r'^\d{10,}$').any():
            def _epoch_to_date(val):
                try:
                    from datetime import datetime as _dt
                    ts = int(float(val)) / 1000  # ms → seconds
                    dt = _dt.fromtimestamp(ts, tz=timezone.utc)
                    return dt.strftime('%-m/%-d/%Y %-I:%M:%S %p')
                except (ValueError, TypeError, OSError):
                    return val
            df['Crash Date'] = raw.apply(_epoch_to_date)

    # ── Ownership ──
    _decode_column('Ownership', {
        '1': '1. State Hwy Agency',
        '2': '2. County Hwy Agency',
        '3': '3. City or Town Hwy Agency',
        '4': '4. Federal Roads',
        '5': '5. Toll Roads Maintained by Others',
        '6': '6. Private/Unknown Roads',
    }, skip_if_contains='Hwy Agency')

    # ── SYSTEM ──
    _decode_column('SYSTEM', {
        '1': 'DOT Interstate',
        '2': 'DOT Primary',
        '3': 'DOT Secondary',
        '4': 'Non-DOT primary',
        '5': 'Non-DOT secondary',
    }, skip_if_contains='DOT')

    # ── Functional Class ──
    _decode_column('Functional Class', {
        'INT': '1-Interstate (A,1)',
        'OFE': '2-Principal Arterial - Other Freeways and Expressways (B)',
        'OPA': '3-Principal Arterial - Other (E,2)',
        'MIA': '4-Minor Arterial (H,3)',
        'MAC': '5-Major Collector (I,4)',
        'MIC': '6-Minor Collector (5)',
        'LOC': '7-Local (J,6)',
    }, skip_if_contains='Interstate')

    # ── Facility Type ──
    _decode_column('Facility Type', {
        'OUD': '1-One-Way Undivided',
        'OWD': '2-One-Way Divided',
        'TUD': '3-Two-Way Undivided',
        'TWD': '4-Two-Way Divided',
        'REX': '5-Reversible Exclusively (e.g. 395R)',
    }, skip_if_contains='Way')

    # ── Collision Type ──
    _decode_column('Collision Type', {
        '0': 'Not Applicable',
        '1': '1. Rear End',
        '2': '2. Angle',
        '3': '3. Head On',
        '4': '4. Sideswipe - Same Direction',
        '5': '5. Sideswipe - Opposite Direction',
        '6': '6. Fixed Object in Road',
        '7': '7. Train',
        '8': '8. Non-Collision',
        '9': '9. Fixed Object - Off Road',
        '10': '10. Deer',
        '11': '11. Other Animal',
        '12': '12. Ped',
        '13': '13. Bicyclist',
        '14': '14. Motorcyclist',
        '15': '15. Backed Into',
        '16': '16. Other',
        '99': 'Not Provided',
    }, skip_if_contains='Rear End')

    # ── Weather Condition ──
    _decode_column('Weather Condition', {
        '1': '1. No Adverse Condition (Clear/Cloudy)',
        '3': '3. Fog',
        '4': '4. Mist',
        '5': '5. Rain',
        '6': '6. Snow',
        '7': '7. Sleet/Hail',
        '8': '8. Smoke/Dust',
        '9': '9. Other',
        '10': '10. Blowing Sand, Soil, Dirt, or Snow',
        '11': '11. Severe Crosswinds',
        '99': 'Not Applicable',
    }, skip_if_contains='Adverse')

    # ── Light Condition ──
    _decode_column('Light Condition', {
        '1': '1. Dawn',
        '2': '2. Daylight',
        '3': '3. Dusk',
        '4': '4. Darkness - Road Lighted',
        '5': '5. Darkness - Road Not Lighted',
        '6': '6. Darkness - Unknown Road Lighting',
        '7': '7. Unknown',
        '99': 'Not Applicable',
    }, skip_if_contains='Dawn')

    # ── Roadway Surface Condition ──
    _decode_column('Roadway Surface Condition', {
        '1': '1. Dry',
        '2': '2. Wet',
        '3': '3. Snowy',
        '4': '4. Icy',
        '5': '5. Muddy',
        '6': '6. Oil/Other Fluids',
        '7': '7. Other',
        '8': '8. Natural Debris',
        '9': '9. Water (Standing, Moving)',
        '10': '10. Slush',
        '11': '11. Sand, Dirt, Gravel',
        '99': 'Not Applicable',
    }, skip_if_contains='Dry')

    # ── Relation To Roadway ──
    _decode_column('Relation To Roadway', {
        '0': 'Not Applicable',
        '1': '1. Main-Line Roadway',
        '2': '2. Acceleration/Deceleration Lanes',
        '3': '3. Gore Area (b/w Ramp and Highway Edgelines)',
        '4': '4. Collector/Distributor Road',
        '5': '5. On Entrance/Exit Ramp',
        '6': '6. Intersection at end of Ramp',
        '7': '7. Other location not listed above within an interchange area (median, shoulder , roadside)',
        '8': '8. Non-Intersection',
        '9': '9. Within Intersection',
        '10': '10. Intersection Related - Within 150 Feet',
        '11': '11. Intersection Related - Outside 150 Feet',
        '12': '12. Crossover Related',
        '13': '13. Driveway, Alley-Access - Related',
        '14': '14. Railway Grade Crossing',
        '15': '15. Other Crossing (Crossing for Bikes, School, etc.)',
        '99': 'Not Provided',
    }, skip_if_contains='Main-Line')

    # ── Roadway Alignment ──
    _decode_column('Roadway Alignment', {
        '1': '1. Straight - Level',
        '2': '2. Curve - Level',
        '3': '3. Grade - Straight',
        '4': '4. Grade - Curve',
        '5': '5. Hillcrest - Straight',
        '6': '6. Hillcrest - Curve',
        '7': '7. Dip - Straight',
        '8': '8. Dip - Curve',
        '9': '9. Other',
        '10': '10. On/Off Ramp',
        '99': 'Not Applicable',
    }, skip_if_contains='Straight')

    # ── Roadway Surface Type ──
    _decode_column('Roadway Surface Type', {
        '1': '1. Concrete',
        '2': '2. Blacktop, Asphalt, Bituminous',
        '3': '3. Brick or Block',
        '4': '4. Slag, Gravel, Stone',
        '5': '5. Dirt',
        '6': '6. Other',
        '99': 'Not Applicable',
    }, skip_if_contains='Concrete')

    # ── Roadway Defect ──
    _decode_column('Roadway Defect', {
        '0': 'Not Applicable',
        '1': '1. No Defects',
        '2': '2. Holes, Ruts, Bumps',
        '3': '3. Soft or Low Shoulder',
        '4': '4. Under Repair',
        '5': '5. Loose Material',
        '6': '6. Restricted Width',
        '7': '7. Slick Pavement',
        '8': '8. Roadway Obstructed',
        '9': '9. Other',
        '10': '10. Edge Pavement Drop Off',
        '99': 'Not Provided',
    }, skip_if_contains='No Defects')

    # ── Roadway Description ──
    _decode_column('Roadway Description', {
        '0': 'Not Applicable',
        '1': '1. Two-Way, Not Divided',
        '2': '2. Two-Way, Divided, Unprotected Median',
        '3': '3. Two-Way, Divided, Positive Median Barrier',
        '4': '4. One-Way, Not Divided',
        '5': '5. Unknown',
        '99': 'Not Provided',
    }, skip_if_contains='Two-Way')

    # ── Intersection Type ──
    _decode_column('Intersection Type', {
        '1': '1. Not at Intersection',
        '2': '2. Two Approaches',
        '3': '3. Three Approaches',
        '4': '4. Four Approaches',
        '5': '5. Five-Point, or More',
        '6': '6. Roundabout',
        '99': 'Not Applicable',
    }, skip_if_contains='Not at Intersection')

    # ── Traffic Control Type ──
    _decode_column('Traffic Control Type', {
        '1': '1. No Traffic Control',
        '2': '2. Officer or Flagger',
        '3': '3. Traffic Signal',
        '4': '4. Stop Sign',
        '5': '5. Slow or Warning Sign',
        '6': '6. Traffic Lanes Marked',
        '7': '7. No Passing Lines',
        '8': '8. Yield Sign',
        '9': '9. One Way Road or Street',
        '10': '10. Railroad Crossing With Markings and Signs',
        '11': '11. Railroad Crossing With Signals',
        '12': '12. Railroad Crossing With Gate and Signals',
        '13': '13. Other',
        '14': '14. Ped Crosswalk',
        '15': '15. Reduced Speed - School Zone',
        '16': '16. Reduced Speed - Work Zone',
        '17': '17. Highway Safety Corridor',
        '99': 'Not Applicable',
    }, skip_if_contains='No Traffic Control')

    # ── Traffic Control Status ──
    _decode_column('Traffic Control Status', {
        '1': '1. Yes - Working',
        '2': '2. Yes - Working and Obscured',
        '3': '3. Yes - Not Working',
        '4': '4. Yes - Not Working and Obscured',
        '5': '5. Yes - Missing',
        '6': '6. No Traffic Control Device Present',
        '99': 'Not Applicable',
    }, skip_if_contains='Working')

    # ── Work Zone Related ──
    _decode_column('Work Zone Related', {
        '0': 'Not Applicable',
        '1': '1. Yes',
        '2': '2. No',
        '99': 'Not Provided',
    }, skip_if_contains='Yes')

    # ── School Zone ──
    _decode_column('School Zone', {
        '0': 'Not Applicable',
        '1': '1. Yes',
        '2': '2. Yes - With School Activity',
        '3': '3. No',
        '99': 'Not Provided',
    }, skip_if_contains='Yes')

    # ── First Harmful Event ──
    _decode_column('First Harmful Event', {
        '1': '1. Bank Or Ledge',
        '2': '2. Trees',
        '3': '3. Utility Pole',
        '4': '4. Fence Or Post',
        '5': '5. Guard Rail',
        '6': '6. Parked Vehicle',
        '7': '7. Tunnel, Bridge, Underpass, Culvert, etc.',
        '8': '8. Sign, Traffic Signal',
        '9': '9. Impact Cushioning Device',
        '10': '10. Other',
        '11': '11. Jersey Wall',
        '12': '12. Building/Structure',
        '13': '13. Curb',
        '14': '14. Ditch',
        '15': '15. Other Fixed Object',
        '16': '16. Other Traffic Barrier',
        '17': '17. Traffic Sign Support',
        '18': '18. Mailbox',
        '19': '19. Ped',
        '20': '20. Motor Vehicle In Transport',
        '21': '21. Train',
        '22': '22. Bicycle',
        '23': '23. Animal',
        '24': '24. Work Zone Maintenance Equipment',
        '25': '25. Other Movable Object',
        '26': '26. Unknown Movable Object',
        '27': '27. Other',
        '28': '28. Ran Off Road',
        '29': '29. Jack Knife',
        '30': '30. Overturn (Rollover)',
        '31': '31. Downhill Runaway',
        '32': '32. Cargo Loss or Shift',
        '33': '33. Explosion or Fire',
        '34': '34. Separation of Units',
        '35': '35. Cross Median',
        '36': '36. Cross Centerline',
        '37': '37. Equipment Failure (Tire, etc)',
        '38': '38. Immersion',
        '39': '39. Fell/Jumped From Vehicle',
        '40': '40. Thrown or Falling Object',
        '41': '41. Non-Collision Unknown',
        '42': '42. Other Non-Collision',
    }, skip_if_contains='Bank Or Ledge')

    # ── First Harmful Event Location ──
    _decode_column('First Harmful Event Loc', {
        '1': '1. On Roadway',
        '2': '2. Shoulder',
        '3': '3. Median',
        '4': '4. Roadside',
        '5': '5. Gore',
        '6': '6. Separator',
        '7': '7. In Parking Lane or Zone',
        '8': '8. Off Roadway, Location Unknown',
        '9': '9. Outside Right-of-Way',
        '99': 'Not Applicable',
    }, skip_if_contains='On Roadway')

    # ── DOT District ──
    _decode_column('DOT District', {
        '1': '1. Bristol',
        '2': '2. Salem',
        '3': '3. Lynchburg',
        '4': '4. Richmond',
        '5': '5. Hampton Roads',
        '6': '6. Fredericksburg',
        '7': '7. Culpeper',
        '8': '8. Staunton',
        '9': '9. Northern Virginia',
    }, skip_if_contains='Bristol')

    # ── Area Type ──
    _decode_column('Area Type', {
        '0': 'Rural',
        '1': 'Urban',
    }, skip_if_contains='Rural')

    # ── Physical Juris Name (jurisdiction code → name) ──
    # Complete VDOT jurisdiction table (324 entries) from authoritative dataset
    if 'Physical Juris Name' in df.columns:
        juris_map = {
            '0': '000. Arlington County', '1': '001. Accomack County',
            '2': '002. Albemarle County', '3': '003. Alleghany County',
            '4': '004. Amelia County', '5': '005. Amherst County',
            '6': '006. Appomattox County', '7': '007. Augusta County',
            '8': '008. Bath County', '9': '009. Bedford County',
            '10': '010. Bland County', '11': '011. Botetourt County',
            '12': '012. Brunswick County', '13': '013. Buchanan County',
            '14': '014. Buckingham County', '15': '015. Campbell County',
            '16': '016. Caroline County', '17': '017. Carroll County',
            '18': '018. Charles City County', '19': '019. Charlotte County',
            '20': '020. Chesterfield County', '21': '021. Clarke County',
            '22': '022. Craig County', '23': '023. Culpeper County',
            '24': '024. Cumberland County', '25': '025. Dickenson County',
            '26': '026. Dinwiddie County',
            '28': '028. Essex County', '29': '029. Fairfax County',
            '30': '030. Fauquier County', '31': '031. Floyd County',
            '32': '032. Fluvanna County', '33': '033. Franklin County',
            '34': '034. Frederick County', '35': '035. Giles County',
            '36': '036. Gloucester County', '37': '037. Goochland County',
            '38': '038. Grayson County', '39': '039. Greene County',
            '40': '040. Greensville County', '41': '041. Halifax County',
            '42': '042. Hanover County', '43': '043. Henrico County',
            '44': '044. Henry County', '45': '045. Highland County',
            '46': '046. Isle of Wight County', '47': '047. James City County',
            '48': '048. King George County', '49': '049. King & Queen County',
            '50': '050. King William County', '51': '051. Lancaster County',
            '52': '052. Lee County', '53': '053. Loudoun County',
            '54': '054. Louisa County', '55': '055. Lunenburg County',
            '56': '056. Madison County', '57': '057. Mathews County',
            '58': '058. Mecklenburg County', '59': '059. Middlesex County',
            '60': '060. Montgomery County',
            '62': '062. Nelson County', '63': '063. New Kent County',
            '65': '065. Northampton County',
            '66': '066. Northumberland County', '67': '067. Nottoway County',
            '68': '068. Orange County', '69': '069. Page County',
            '70': '070. Patrick County', '71': '071. Pittsylvania County',
            '72': '072. Powhatan County', '73': '073. Prince Edward County',
            '74': '074. Prince George County',
            '76': '076. Prince William County', '77': '077. Pulaski County',
            '78': '078. Rappahannock County', '79': '079. Richmond County',
            '80': '080. Roanoke County', '81': '081. Rockbridge County',
            '82': '082. Rockingham County', '83': '083. Russell County',
            '84': '084. Scott County', '85': '085. Shenandoah County',
            '86': '086. Smyth County', '87': '087. Southampton County',
            '88': '088. Spotsylvania County', '89': '089. Stafford County',
            '90': '090. Surry County', '91': '091. Sussex County',
            '92': '092. Tazewell County', '93': '093. Warren County',
            '95': '095. Washington County',
            '96': '096. Westmoreland County', '97': '097. Wise County',
            '98': '098. Wythe County', '99': '099. York County',
            # Independent cities
            '100': '100. City of Alexandria', '101': '101. Town of Big Stone Gap',
            '102': '102. City of Bristol', '103': '103. City of Buena Vista',
            '104': '104. City of Charlottesville', '105': '105. Town of Clifton Forge',
            '106': '106. City of Colonial Heights', '107': '107. City of Covington',
            '108': '108. City of Danville', '109': '109. City of Emporia',
            '110': '110. City of Falls Church', '111': '111. City of Fredericksburg',
            '112': '112. Town of Front Royal', '113': '113. City of Galax',
            '114': '114. City of Hampton', '115': '115. City of Harrisonburg',
            '116': '116. City of Hopewell', '117': '117. City of Lexington',
            '118': '118. City of Lynchburg', '119': '119. Town of Marion',
            '120': '120. City of Martinsville', '121': '121. City of Newport News',
            '122': '122. City of Norfolk', '123': '123. City of Petersburg',
            '124': '124. City of Portsmouth', '125': '125. Town of Pulaski',
            '126': '126. City of Radford', '127': '127. City of Richmond',
            '128': '128. City of Roanoke', '129': '129. City of Salem',
            '130': '130. Town of South Boston', '131': '131. City of Chesapeake',
            '132': '132. City of Staunton', '133': '133. City of Suffolk',
            '134': '134. City of Virginia Beach',
            '136': '136. City of Waynesboro', '137': '137. City of Williamsburg',
            '138': '138. City of Winchester', '139': '139. Town of Wytheville',
            # Towns
            '140': '140. Town of Abingdon', '141': '141. Town of Bedford',
            '142': '142. Town of Blackstone', '143': '143. Town of Bluefield',
            '144': '144. Town of Farmville', '145': '145. City of Franklin',
            '146': '146. City of Norton', '147': '147. City of Poquoson',
            '148': '148. Town of Richlands', '149': '149. Town of Vinton',
            '150': '150. Town of Blacksburg', '151': '151. City of Fairfax',
            '152': '152. City of Manassas Park', '153': '153. Town of Vienna',
            '154': '154. Town of Christiansburg', '155': '155. City of Manassas',
            '156': '156. Town of Warrenton', '157': '157. Town of Rocky Mount',
            '158': '158. Town of Tazewell', '159': '159. Town of Luray',
            '160': '160. Town of Accomac', '161': '161. Town of Alberta',
            '162': '162. Town of Altavista', '163': '163. Town of Amherst',
            '164': '164. Town of Appalachia', '165': '165. Town of Appomattox',
            '166': '166. Town of Ashland', '167': '167. Town of Belle Haven',
            '168': '168. Town of Berryville', '169': '169. Town of Bloxom',
            '170': '170. Town of Boones Mill', '171': '171. Town of Bowling Green',
            '172': '172. Town of Boyce', '173': '173. Town of Boydton',
            '174': '174. Town of Boykins', '175': '175. Town of Branchville',
            '176': '176. Town of Bridgewater', '177': '177. Town of Broadway',
            '178': '178. Town of Brodnax', '179': '179. Town of Brookneal',
            '180': '180. Town of Buchanan', '181': '181. Town of Burkeville',
            '182': '182. Town of Cape Charles', '183': '183. Town of Capron',
            '184': '184. Town of Cedar Bluff', '185': '185. Town of Charlotte C.H.',
            '186': '186. Town of Chase City', '187': '187. Town of Chatham',
            '188': '188. Town of Cheriton', '189': '189. Town of Chilhowie',
            '190': '190. Town of Chincoteague', '191': '191. Town of Claremont',
            '192': '192. Town of Clarksville', '193': '193. Town of Cleveland',
            '194': '194. Town of Clifton', '195': '195. Town of Clinchport',
            '196': '196. Town of Clintwood',
            '198': '198. Town of Coeburn', '199': '199. Town of Colonial Beach',
            '200': '200. Town of Columbia', '201': '201. Town of Courtland',
            '202': '202. Town of Craigsville', '203': '203. Town of Crewe',
            '204': '204. Town of Culpeper', '205': '205. Town of Damascus',
            '206': '206. Town of Dayton', '207': '207. Town of Dendron',
            '208': '208. Town of Dillwyn', '209': '209. Town of Drakes Branch',
            '210': '210. Town of Dublin', '211': '211. Town of Duffield',
            '212': '212. Town of Dumfries', '213': '213. Town of Dungannon',
            '214': '214. Town of Eastville', '215': '215. Town of Edinburg',
            '216': '216. Town of Elkton', '217': '217. Town of Exmore',
            '218': '218. Town of Fincastle', '219': '219. Town of Floyd',
            '220': '220. Town of Fries', '221': '221. Town of Gate City',
            '222': '222. Town of Glade Spring', '223': '223. Town of Glasgow',
            '224': '224. Town of Glen Lyn', '225': '225. Town of Gordonsville',
            '226': '226. Town of Goshen', '227': '227. Town of Gretna',
            '228': '228. Town of Grottoes', '229': '229. Town of Grundy',
            '230': '230. Town of Halifax', '231': '231. Town of Hallwood',
            '232': '232. Town of Hamilton', '233': '233. Town of Haymarket',
            '234': '234. Town of Haysi', '235': '235. Town of Herndon',
            '236': '236. Town of Hillsboro', '237': '237. Town of Hillsville',
            '239': '239. Town of Honaker', '240': '240. Town of Independence',
            '241': '241. Town of Iron Gate', '242': '242. Town of Irvington',
            '243': '243. Town of Ivor', '244': '244. Town of Jarratt',
            '245': '245. Town of Jonesville', '246': '246. Town of Keller',
            '247': '247. Town of Kenbridge', '248': '248. Town of Keysville',
            '249': '249. Town of Kilmarnock', '250': '250. Town of LaCrosse',
            '251': '251. Town of Lawrenceville', '252': '252. Town of Lebanon',
            '253': '253. Town of Leesburg', '254': '254. Town of Louisa',
            '255': '255. Town of Lovettsville', '256': '256. Town of Madison',
            '257': '257. Town of McKenney', '258': '258. Town of Melfa',
            '259': '259. Town of Middleburg', '260': '260. Town of Middletown',
            '261': '261. Town of Mineral', '262': '262. Town of Monterey',
            '263': '263. Town of Montross', '264': '264. Town of Mount Crawford',
            '265': '265. Town of Mount Jackson', '266': '266. Town of Narrows',
            '267': '267. Town of Nassawadox', '268': '268. Town of New Castle',
            '269': '269. Town of New Market', '270': '270. Town of Newsoms',
            '271': '271. Town of Nickelsville', '272': '272. Town of Occoquan',
            '273': '273. Town of Onancock', '274': '274. Town of Onley',
            '275': '275. Town of Orange', '276': '276. Town of Painter',
            '277': '277. Town of Pamplin City', '278': '278. Town of Parksley',
            '279': '279. Town of Pearisburg', '280': '280. Town of Pembroke',
            '281': '281. Town of Pennington Gap', '282': '282. Town of Phenix',
            '283': '283. Town of Pocahontas', '284': '284. Town of Port Royal',
            '285': '285. Town of Pound', '286': '286. Town of Purcellville',
            '287': '287. Town of Quantico', '288': '288. Town of Remington',
            '289': '289. Town of Rich Creek', '290': '290. Town of Ridgeway',
            '291': '291. Town of Round Hill', '292': '292. Town of Rural Retreat',
            '293': '293. Town of St. Charles', '294': '294. Town of Saint Paul',
            '295': '295. Town of Saltville', '296': '296. Town of Saxis',
            '297': '297. Town of Scottsburg', '298': '298. Town of Scottsville',
            '299': '299. Town of Shenandoah', '300': '300. Town of Smithfield',
            '301': '301. Town of South Hill', '302': '302. Town of Stanardsville',
            '303': '303. Town of Stanley', '304': '304. Town of Stephens City',
            '305': '305. Town of Stony Creek', '306': '306. Town of Strasburg',
            '307': '307. Town of Stuart', '308': '308. Town of Surry',
            '309': '309. Town of Tangier', '310': '310. Town of Tappahannock',
            '311': '311. Town of The Plains', '312': '312. Town of Timberville',
            '313': '313. Town of Toms Brook', '314': '314. Town of Troutdale',
            '315': '315. Town of Troutville', '316': '316. Town of Urbanna',
            '317': '317. Town of Victoria', '318': '318. Town of Virgilina',
            '319': '319. Town of Wachapreague', '320': '320. Town of Wakefield',
            '321': '321. Town of Warsaw', '322': '322. Town of Washington',
            '323': '323. Town of Waverly', '324': '324. Town of Weber City',
            '325': '325. Town of West Point', '327': '327. Town of White Stone',
            '328': '328. Town of Windsor', '329': '329. Town of Wise',
            '330': '330. Town of Woodstock', '331': '331. Town of Hurt',
            '339': '339. Town of Clinchco',
        }
        raw = df['Physical Juris Name'].astype(str).str.strip()
        if raw.isin(juris_map.keys()).any() and not raw.str.contains('County|City|Town', na=False, regex=True).any():
            df['Physical Juris Name'] = raw.map(juris_map).fillna(df['Physical Juris Name'])

    # ── Planning District ──
    if 'Planning District' in df.columns:
        plan_map = {
            '1': 'Lenowisco', '2': 'Cumberland Plateau',
            '3': 'Mount Rogers', '4': 'New River Valley',
            '5': 'Roanoke Valley-Alleghany', '6': 'Central Shenandoah',
            '7': 'Northern Shenandoah Valley', '8': 'Northern Virginia',
            '9': 'Rappahannock - Rapidan', '10': 'Thomas Jefferson',
            '11': 'Region 2000', '12': 'West Piedmont',
            '13': 'Southside', '14': 'Commonwealth Regional',
            '15': 'Richmond Regional', '16': 'George Washington Regional',
            '17': 'Northern Neck', '18': 'Middle Peninsula',
            '19': 'Crater', '20': 'Piedmont',
            '21': 'Rappahannock Area', '22': 'Accomack-Northampton',
            '23': 'Hampton Roads',
            # Combo districts
            '5,12': 'Roanoke Valley-Alleghany, West Piedmont',
            '15,19': 'Richmond Regional, Crater',
            '18,23': 'Middle Peninsula, Hampton Roads',
            '19,23': 'Crater, Hampton Roads',
        }
        raw = df['Planning District'].astype(str).str.strip()
        if raw.isin(plan_map.keys()).any() and not raw.str.contains('[a-zA-Z]', na=False, regex=True).any():
            df['Planning District'] = raw.map(plan_map).fillna(df['Planning District'])

    # ── Boolean fields: 0/1 → Yes/No ──
    bool_yes_no_fields = [
        'Alcohol?', 'Bike?', 'Pedestrian?', 'Speed?', 'Distracted?',
        'Drowsy?', 'Drug Related?', 'Guardrail Related?', 'Hitrun?',
        'Lgtruck?', 'Motorcycle?', 'Animal Related?', 'Senior?',
        'Young?', 'Mainline?', 'Night?',
    ]
    for col in bool_yes_no_fields:
        if col in df.columns:
            raw = df[col].astype(str).str.strip()
            if raw.isin({'0', '1'}).any() and not raw.str.contains('Yes|No', na=False, regex=True).any():
                df[col] = raw.map({'0': 'No', '1': 'Yes'}).fillna(df[col])

    # ── Unrestrained? (special: 0→Belted, 1→Unbelted) ──
    if 'Unrestrained?' in df.columns:
        raw = df['Unrestrained?'].astype(str).str.strip()
        if raw.isin({'0', '1'}).any() and not raw.str.contains('Belted|Unbelted', na=False, regex=True).any():
            df['Unrestrained?'] = raw.map({'0': 'Belted', '1': 'Unbelted'}).fillna(df['Unrestrained?'])

    # ── RoadDeparture Type ──
    _decode_column('RoadDeparture Type', {
        '0': 'NOT_RD',
        '1': 'RD_LEFT',
        '2': 'RD_RIGHT',
        '3': 'RD_UNKNOWN',
    }, skip_if_contains='NOT_RD')

    # ── Intersection Analysis ──
    _decode_column('Intersection Analysis', {
        '0': 'Not Intersection',
        '1': 'Urban Intersection',
        '2': 'DOT Intersection',
    }, skip_if_contains='Intersection')

    # ── Work Zone Location ──
    _decode_column('Work Zone Location', {
        '0': '',
        '1': '1. Advance Warning Area',
        '2': '2. Transition Area',
        '3': '3. Activity Area',
        '4': '4. Termination Area',
        '99': '',
    }, skip_if_contains='Warning')

    # ── Work Zone Type ──
    _decode_column('Work Zone Type', {
        '0': '',
        '1': '1. Lane Closure',
        '2': '2. Lane Shift/Crossover',
        '3': '3. Work on Shoulder or Median',
        '4': '4. Intermittent or Moving Work',
        '5': '5. Other',
        '99': '',
    }, skip_if_contains='Lane Closure')

    return df


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Download crash data from Virginia Roads for a specific jurisdiction.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_crash_data.py                          # Use default jurisdiction (henrico)
  python download_crash_data.py --jurisdiction chesterfield
  python download_crash_data.py --jurisdiction fairfax_county --filter allRoads
  python download_crash_data.py --list                   # Show all available jurisdictions
        """
    )

    parser.add_argument(
        '--jurisdiction', '-j',
        type=str,
        help='Jurisdiction ID (e.g., henrico, chesterfield, alexandria)'
    )

    parser.add_argument(
        '--filter', '-f',
        type=str,
        choices=['countyOnly', 'cityOnly', 'countyPlusVDOT', 'allRoads'],
        help='Road type filter profile'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output file path (default: data/<jurisdiction>_all_roads.csv)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory (auto-generates filename from jurisdiction). Ignored if --output is set.'
    )

    parser.add_argument(
        '--force-download',
        action='store_true',
        help='Force re-download even if cached data exists'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available jurisdictions and exit'
    )

    parser.add_argument(
        '--health-check',
        action='store_true',
        help='Run API health check and exit'
    )

    parser.add_argument(
        '--merge',
        action='store_true',
        help='Merge new data with existing validated dataset instead of replacing it'
    )

    parser.add_argument(
        '--save-statewide',
        action='store_true',
        help='Save a gzipped copy of the full statewide dataset before jurisdiction filtering'
    )

    return parser.parse_args()


def main():
    """Main function to download and process crash data."""
    args = parse_args()

    # Load configuration
    config = load_config()

    # Handle --list option
    if args.list:
        list_jurisdictions(config)
        return 0

    # Handle --health-check option
    if args.health_check:
        logger.info("Running API health check...")
        working_url = find_working_api_url(config)
        if working_url:
            logger.info(f"Health check PASSED. Working endpoint: {working_url}")
            return 0
        else:
            logger.error("Health check FAILED. No working API endpoint found.")
            return 1

    # Get jurisdiction
    jurisdiction_id = args.jurisdiction or config.get('defaults', {}).get('jurisdiction', 'henrico')
    jurisdiction_config = get_jurisdiction_config(config, jurisdiction_id)

    # Get filter profile
    filter_id = args.filter or config.get('defaults', {}).get('filterProfile', 'allRoads')
    filter_profiles = config.get('filterProfiles', {})
    filter_profile = filter_profiles.get(filter_id, filter_profiles.get('countyOnly', {}))

    # Get output file
    if args.output:
        output_file = args.output
    elif args.output_dir:
        output_file = os.path.join(args.output_dir, f"{jurisdiction_id}_all_roads.csv")
    else:
        output_file = os.path.join(OUTPUT_DIR, f"{jurisdiction_id}_all_roads.csv")

    logger.info("=" * 60)
    logger.info(f"Starting crash data download at {datetime.now()}")
    logger.info(f"Jurisdiction: {jurisdiction_config.get('name', jurisdiction_id)}")
    logger.info(f"Filter: {filter_profile.get('name', filter_id)}")
    logger.info(f"Output: {output_file}")
    logger.info("=" * 60)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

    df = None
    df_filtered = None
    used_api_fallback = False
    statewide_saved = False

    # Try CSV download first (primary source - has complete statewide data)
    try:
        logger.info("Downloading from Virginia Roads CSV (primary source)...")
        df = download_from_fallback(config, state='virginia')

        # Stage 1.5: Save statewide copy as gzip before jurisdiction filtering
        if args.save_statewide and df is not None and not df.empty:
            try:
                statewide_dir = args.output_dir or OUTPUT_DIR
                statewide_path = os.path.join(statewide_dir, "virginia_statewide_all_roads.csv")
                logger.info(f"Saving statewide dataset ({len(df):,} records) before filtering...")
                df_statewide = standardize_columns(df.copy())

                # Validate that this is crash-level data (not driver-level)
                required_cols = {'Crash_Severity', 'Crash Severity', 'crash_seve', 'Crash_Year', 'Crash Year', 'crash_year'}
                has_crash_cols = bool(required_cols & set(df_statewide.columns))
                if not has_crash_cols:
                    logger.warning(f"Downloaded data appears to be driver-level (columns: {list(df_statewide.columns)[:10]}...)")
                    logger.warning("Skipping statewide save — data lacks crash-level columns needed for splitting.")
                else:
                    # Validate that jurisdiction columns exist (needed for split_jurisdictions.py)
                    jurisdiction_cols = {
                        'Juris_Code', 'Juris Code', 'JURIS_CODE',
                        'Physical_Juris_Name', 'Physical Juris Name', 'PHYSICAL_JURIS_NAME',
                        'FIPS', 'County_FIPS', 'COUNTY_FIPS', 'COUNTYFP',
                        'Jurisdiction', 'JURISDICTION', 'County_City', 'COUNTY_CITY'
                    }
                    has_jurisdiction_cols = bool(jurisdiction_cols & set(df_statewide.columns))
                    if not has_jurisdiction_cols:
                        logger.warning(f"Downloaded data has NO jurisdiction columns (columns: {list(df_statewide.columns)[:15]}...)")
                        logger.warning("Skipping statewide save — data cannot be split by jurisdiction without Juris_Code/FIPS/Physical_Juris_Name.")
                    else:
                        df_statewide.to_csv(statewide_path, index=False)
                        statewide_saved = True
                        logger.info(f"Statewide CSV saved: {statewide_path} ({os.path.getsize(statewide_path):,} bytes)")

                        # Gzip the statewide CSV
                        import gzip
                        import shutil
                        gz_path = f"{statewide_path}.gz"
                        with open(statewide_path, 'rb') as f_in:
                            with gzip.open(gz_path, 'wb', compresslevel=6) as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        uncompressed_size = os.path.getsize(statewide_path)
                        compressed_size = os.path.getsize(gz_path)
                        # Keep uncompressed CSV alongside gzip for batch splitting workflows
                        # (split_jurisdictions.py needs the uncompressed CSV to split into
                        # per-jurisdiction files). The batch workflow cleans up after splitting.
                        ratio = uncompressed_size / compressed_size if compressed_size > 0 else 0
                        logger.info(f"Statewide gzip saved: {gz_path} ({compressed_size:,} bytes, {ratio:.1f}x compression)")
            except Exception as e:
                logger.warning(f"Failed to save statewide copy (non-fatal): {e}")

        # Filter by jurisdiction
        logger.info("Filtering by jurisdiction...")
        df_filtered = filter_jurisdiction(df, jurisdiction_config)

        if df_filtered.empty:
            logger.warning(f"CSV data has no records for {jurisdiction_config.get('name', jurisdiction_id)}")
            df = None  # Reset to trigger API fallback

    except Exception as e:
        logger.error(f"CSV download failed: {e}")
        logger.info("Falling back to ArcGIS API...")

    # Try ArcGIS API as fallback if CSV failed or returned no records
    if df is None or (df_filtered is not None and df_filtered.empty):
        try:
            logger.info("=" * 40)
            logger.info("Attempting ArcGIS API fallback...")
            df = download_from_arcgis(config, jurisdiction_config, state='virginia')
            used_api_fallback = True

            # Filter by jurisdiction
            logger.info("Filtering API data by jurisdiction...")
            df_filtered = filter_jurisdiction(df, jurisdiction_config)

        except Exception as e:
            logger.error(f"ArcGIS API fallback also failed: {e}")
            sys.exit(1)

    # Use the filtered dataframe
    df = df_filtered

    if df is None or df.empty:
        logger.error(f"No {jurisdiction_config.get('name', jurisdiction_id)} records found after filtering!")
        logger.error("Neither CSV nor API contained data for this jurisdiction.")
        sys.exit(1)

    if used_api_fallback:
        logger.info(f"Successfully retrieved {len(df)} records using ArcGIS API fallback")
        # CRITICAL: If --save-statewide was requested but CSV download failed,
        # the ArcGIS API only returns jurisdiction-specific data (not statewide).
        # The statewide CSV was NOT saved. Warn loudly so the batch workflow knows.
        if args.save_statewide and not statewide_saved:
            logger.error("=" * 60)
            logger.error("WARNING: --save-statewide was requested but CSV download failed!")
            logger.error("ArcGIS API fallback only has jurisdiction-specific data.")
            logger.error("NO statewide CSV was saved. Batch pipeline will NOT work.")
            logger.error("Fix: Ensure Virginia Roads CSV URLs are accessible.")
            logger.error("=" * 60)
    else:
        logger.info(f"Successfully retrieved {len(df)} records from CSV source")

    # Filter by road system
    logger.info("Applying road type filter...")
    df = filter_by_road_system(df, filter_profile)

    if df.empty:
        logger.error("No records remaining after road type filter!")
        sys.exit(1)

    # Standardize column names
    df = standardize_columns(df)

    # Merge with existing validated dataset if --merge flag is set
    if args.merge and os.path.exists(output_file):
        logger.info("=" * 40)
        logger.info("MERGE MODE: Merging with existing dataset")
        logger.info("=" * 40)

        try:
            existing_df = pd.read_csv(output_file, dtype=str)
            existing_count = len(existing_df)
            logger.info(f"Existing dataset: {existing_count} records")
            logger.info(f"New download: {len(df)} records")

            # Concatenate existing + new
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_count = len(combined_df)

            # Deduplicate: prefer existing records (keep='first')
            # Primary key: Document Nbr (if available)
            doc_col = None
            for col in ['Document Nbr', 'Document Number', 'CrashID', 'CRASH_ID']:
                if col in combined_df.columns:
                    doc_col = col
                    break

            dedup_count_before = len(combined_df)

            if doc_col:
                # Remove rows with duplicate Document Numbers (keep existing = first)
                mask = combined_df[doc_col].notna() & (combined_df[doc_col] != '') & (combined_df[doc_col] != 'nan')
                has_doc = combined_df[mask]
                no_doc = combined_df[~mask]
                has_doc_deduped = has_doc.drop_duplicates(subset=[doc_col], keep='first')
                combined_df = pd.concat([has_doc_deduped, no_doc], ignore_index=True)
                logger.info(f"  Doc# dedup: {dedup_count_before} -> {len(combined_df)} records")

            # Secondary dedup: Crash Date + coordinates + Collision Type
            dedup_cols = ['Crash Date', 'x', 'y', 'Collision Type']
            existing_dedup_cols = [c for c in dedup_cols if c in combined_df.columns]
            if len(existing_dedup_cols) == 4:
                has_coords = (
                    combined_df['x'].notna() & (combined_df['x'] != '') & (combined_df['x'] != '0') &
                    combined_df['y'].notna() & (combined_df['y'] != '') & (combined_df['y'] != '0')
                )
                df_with_coords = combined_df[has_coords]
                df_without_coords = combined_df[~has_coords]
                dedup_before = len(df_with_coords)
                df_with_coords_deduped = df_with_coords.drop_duplicates(
                    subset=existing_dedup_cols, keep='first'
                )
                combined_df = pd.concat([df_with_coords_deduped, df_without_coords], ignore_index=True)
                logger.info(f"  Geo dedup: {dedup_before + len(df_without_coords)} -> {len(combined_df)} records")

            duplicates_removed = dedup_count_before - len(combined_df)
            new_records = len(combined_df) - existing_count
            logger.info(f"  Merge result: {existing_count} existing + {new_records} new ({duplicates_removed} duplicates removed)")
            df = combined_df

        except Exception as e:
            logger.error(f"Merge failed: {e}")
            logger.info("Falling back to full replacement")

    # Save to CSV
    logger.info(f"Saving {len(df)} records to {output_file}")
    df.to_csv(output_file, index=False)

    logger.info("=" * 60)
    logger.info(f"Successfully downloaded {len(df)} crash records")
    logger.info(f"Output saved to: {output_file}")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
