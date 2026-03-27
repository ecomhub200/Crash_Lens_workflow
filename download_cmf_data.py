#!/usr/bin/env python3
"""
Download and transform CMF (Crash Modification Factor) data from FHWA Clearinghouse.
Normalizes crash types, adds Virginia relevance scoring, and outputs optimized JSON for web.

Usage:
    python download_cmf_data.py                    # Full download and transform
    python download_cmf_data.py --transform-only   # Transform existing raw CSV
    python download_cmf_data.py --stats            # Show statistics only
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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

# Output configuration
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")
RAW_CSV_FILE = os.path.join(OUTPUT_DIR, "cmfclearinghouse_raw.csv")
PROCESSED_JSON_FILE = os.path.join(OUTPUT_DIR, "cmf_processed.json")
METADATA_FILE = os.path.join(OUTPUT_DIR, "cmf_metadata.json")

# FHWA CMF Clearinghouse URLs
CMF_DOWNLOAD_URLS = [
    "https://cmfclearinghouse.fhwa.dot.gov/ExcelDownload.aspx?format=csv",
    "https://cmfclearinghouse.fhwa.dot.gov/data/CMFClearinghouseData.csv",
]

# Retry settings
MAX_RETRIES = 4
RETRY_BACKOFF_FACTOR = 2  # 2s, 4s, 8s, 16s

# Quality threshold (1-5 stars)
MIN_QUALITY_RATING = 2  # Lowered from 3 to include more countermeasures

# Valid CMF range (expanded to include more records)
MIN_CMF = 0.001  # Lowered from 0.01
MAX_CMF = 5.0    # Raised from 3.0 to capture high-increase CMFs

# ============================================================
# CRASH TYPE NORMALIZATION MAPPING
# Maps 209 unique Clearinghouse crashType values to ~15 normalized tags
# ============================================================

CRASH_TYPE_MAPPING = {
    # Single type mappings
    'all': ['all'],
    'angle': ['angle'],
    'rear end': ['rear_end'],
    'rear-end': ['rear_end'],
    'head on': ['head_on'],
    'head-on': ['head_on'],
    'sideswipe': ['sideswipe'],
    'run off road': ['run_off_road'],
    'fixed object': ['run_off_road'],
    'left turn': ['left_turn'],
    'right turn': ['right_turn'],
    'vehicle/pedestrian': ['pedestrian'],
    'vehicle/bicycle': ['bicycle'],
    'nighttime': ['nighttime'],
    'wet road': ['wet_road'],
    'speed related': ['speed'],
    'single vehicle': ['single_vehicle'],
    'multiple vehicle': ['multiple_vehicle'],
    'cross median': ['head_on', 'median'],
    'truck related': ['truck'],
    'non-intersection': ['segment'],
    'parking related': ['parking'],
    'day time': ['daytime'],
    'dry weather': ['dry'],
    'rear to rear': ['rear_end'],
    'backed into': ['rear_end'],
    'frontal and opposing direction sideswipe': ['head_on', 'sideswipe'],
    'not specified': ['all'],
    'other': ['all'],
    '\\n': ['all'],
    '': ['all'],
}

# Keywords for parsing compound crash types
CRASH_TYPE_KEYWORDS = {
    'angle': 'angle',
    'rear end': 'rear_end',
    'rear-end': 'rear_end',
    'head on': 'head_on',
    'head-on': 'head_on',
    'sideswipe': 'sideswipe',
    'run off road': 'run_off_road',
    'fixed object': 'run_off_road',
    'left turn': 'left_turn',
    'right turn': 'right_turn',
    'pedestrian': 'pedestrian',
    'bicycle': 'bicycle',
    'pedalcyclist': 'bicycle',
    'nighttime': 'nighttime',
    'night': 'nighttime',
    'wet road': 'wet_road',
    'wet': 'wet_road',
    'speed': 'speed',
    'single vehicle': 'single_vehicle',
    'multiple vehicle': 'multiple_vehicle',
    'cross median': 'median',
    'median': 'median',
    'truck': 'truck',
    'non-intersection': 'segment',
}

# Virginia and regional states for relevance scoring
VIRGINIA_STATES = ['va', 'virginia']
REGIONAL_STATES = ['md', 'maryland', 'nc', 'north carolina', 'dc',
                   'district of columbia', 'wv', 'west virginia',
                   'de', 'delaware', 'pa', 'pennsylvania']

# Cost tier keywords
COST_TIER_KEYWORDS = {
    1: ['sign', 'marking', 'stripe', 'edge line', 'delineator', 'warning',
        'chevron', 'pavement marking', 'rumble strip'],
    2: ['timing', 'phasing', 'rrfb', 'phb', 'beacon', 'flashing',
        'signal timing', 'lpi', 'leading pedestrian'],
    3: ['signal', 'median', 'turn lane', 'turning lane', 'shoulder',
        'lighting', 'illumination', 'crosswalk', 'refuge'],
    4: ['roundabout', 'interchange', 'overpass', 'underpass', 'bridge',
        'grade separation', 'flyover'],
}

# MySQL-style null markers
NULL_MARKERS = ['\\N', '\\n', 'NULL', 'null', 'None', 'none', 'nan', 'NaN', 'NA', 'N/A', '']


def safe_float(value, default=None):
    """Safely convert value to float, handling MySQL-style nulls."""
    if pd.isna(value):
        return default
    val_str = str(value).strip()
    if val_str in NULL_MARKERS:
        return default
    try:
        return float(val_str)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=None):
    """Safely convert value to int, handling MySQL-style nulls."""
    if pd.isna(value):
        return default
    val_str = str(value).strip()
    if val_str in NULL_MARKERS:
        return default
    # Handle 'All' and other non-numeric strings
    if not val_str.replace('-', '').replace('.', '').isdigit():
        return default
    try:
        return int(float(val_str))
    except (ValueError, TypeError):
        return default


def safe_str(value, default=''):
    """Safely convert value to string, handling MySQL-style nulls."""
    if pd.isna(value):
        return default
    val_str = str(value).strip()
    if val_str in NULL_MARKERS or val_str.lower() == 'nan':
        return default
    return val_str


def create_session_with_retries():
    """Create a requests session with retry logic."""
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
    return session


def make_request_with_retry(url: str, timeout: int = 300, max_manual_retries: int = 3):
    """Make HTTP request with manual retry logic for network errors."""
    session = create_session_with_retries()
    last_exception = None

    for attempt in range(max_manual_retries):
        try:
            response = session.get(url, timeout=timeout)
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

    raise last_exception or Exception("Request failed after all retries")


def download_cmf_csv() -> Optional[pd.DataFrame]:
    """Download CMF data from FHWA Clearinghouse."""

    for url in CMF_DOWNLOAD_URLS:
        logger.info(f"Attempting download from: {url}")

        try:
            response = make_request_with_retry(url, timeout=300)

            if response.status_code == 200:
                # Try to parse as CSV
                import io

                # Handle potential encoding issues
                try:
                    df = pd.read_csv(io.StringIO(response.text), low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(response.content), encoding='latin-1', low_memory=False)

                logger.info(f"Successfully downloaded {len(df)} records from FHWA")
                return df
            else:
                logger.warning(f"HTTP {response.status_code} from {url}")

        except Exception as e:
            logger.warning(f"Failed to download from {url}: {e}")
            continue

    logger.error("Could not download CMF data from any source")
    return None


def load_existing_csv() -> Optional[pd.DataFrame]:
    """Load existing raw CSV if available."""
    # Check for uploaded file first
    uploaded_file = os.path.join(OUTPUT_DIR, "cmfclearinghouse.csv")

    if os.path.exists(uploaded_file):
        logger.info(f"Loading existing uploaded CSV: {uploaded_file}")
        df = pd.read_csv(uploaded_file, low_memory=False)
        logger.info(f"Loaded {len(df)} records from uploaded file")
        return df

    if os.path.exists(RAW_CSV_FILE):
        logger.info(f"Loading existing raw CSV: {RAW_CSV_FILE}")
        df = pd.read_csv(RAW_CSV_FILE, low_memory=False)
        logger.info(f"Loaded {len(df)} records from raw backup")
        return df

    return None


def normalize_crash_type(crash_type_str: str) -> List[str]:
    """
    Normalize a crashType string to a list of standard tags.
    Handles both single values and comma-separated compound values.
    """
    if pd.isna(crash_type_str) or not crash_type_str:
        return ['all']

    crash_type_str = str(crash_type_str).strip().lower()

    # Handle null markers
    if crash_type_str in ['\\n', 'nan', 'none', 'null', '']:
        return ['all']

    # Check for exact match first
    if crash_type_str in CRASH_TYPE_MAPPING:
        return CRASH_TYPE_MAPPING[crash_type_str]

    # Parse compound types (comma-separated)
    normalized_tags = set()

    # Split by comma and process each part
    parts = [p.strip().lower() for p in crash_type_str.split(',')]

    for part in parts:
        # Check exact match
        if part in CRASH_TYPE_MAPPING:
            normalized_tags.update(CRASH_TYPE_MAPPING[part])
        else:
            # Check keyword matches
            matched = False
            for keyword, tag in CRASH_TYPE_KEYWORDS.items():
                if keyword in part:
                    normalized_tags.add(tag)
                    matched = True

            # If no match found, skip (don't add 'all' for each unmatched part)
            if not matched and len(parts) == 1:
                normalized_tags.add('all')

    # If nothing matched, default to 'all'
    if not normalized_tags:
        return ['all']

    # Remove 'all' if other specific types are present
    if len(normalized_tags) > 1 and 'all' in normalized_tags:
        normalized_tags.discard('all')

    return sorted(list(normalized_tags))


def calculate_virginia_relevance(state_str: str) -> int:
    """
    Calculate Virginia relevance score based on study state(s).
    Returns: 100 (VA), 50 (regional), 25 (national/unknown), 10 (other)
    """
    if pd.isna(state_str) or not state_str:
        return 25  # National/unknown - moderately relevant

    state_str = str(state_str).lower()

    # Check for Virginia
    for va in VIRGINIA_STATES:
        if va in state_str:
            return 100

    # Check for regional states
    for regional in REGIONAL_STATES:
        if regional in state_str:
            return 50

    # Check if it's a multi-state or national study
    if ',' in state_str or 'national' in state_str or 'multiple' in state_str:
        return 25

    # Other single state
    return 10


def estimate_cost_tier(cmf_name: str, category: str) -> int:
    """
    Estimate cost tier (1-4) based on countermeasure name and category.
    1 = Low (<$10K), 2 = Medium ($10K-$100K), 3 = High ($100K-$500K), 4 = Very High (>$500K)
    """
    if pd.isna(cmf_name):
        return 2  # Default to medium

    name_lower = str(cmf_name).lower()
    category_lower = str(category).lower() if category else ''

    # Check keywords in order of cost (highest first)
    for tier in [4, 3, 2, 1]:
        for keyword in COST_TIER_KEYWORDS.get(tier, []):
            if keyword in name_lower:
                return tier

    # Category-based defaults
    if 'intersection' in category_lower:
        return 3
    elif 'roadway' in category_lower:
        return 2
    elif 'pedestrian' in category_lower or 'bicycle' in category_lower:
        return 2

    return 2  # Default to medium


def parse_severity(severity_str: str) -> List[str]:
    """Parse crashSeverityKABCO field to list of severity codes."""
    if pd.isna(severity_str) or not severity_str:
        return ['K', 'A', 'B', 'C', 'O']  # Default: applies to all

    severity_str = str(severity_str).upper()

    severities = []
    for code in ['K', 'A', 'B', 'C', 'O']:
        if code in severity_str:
            severities.append(code)

    return severities if severities else ['K', 'A', 'B', 'C', 'O']


def determine_location_type(row: pd.Series) -> str:
    """Determine if CMF applies to intersection, segment, or both."""
    intersection_related = str(row.get('intersectionRelated', '')).lower()
    intersec_type = str(row.get('intersecType', '')).lower()
    category = str(row.get('catname', '')).lower()

    if intersection_related == 'yes' or 'intersection' in category:
        return 'intersection'
    elif intersection_related == 'no' or 'segment' in intersec_type:
        return 'segment'
    elif 'roadway' in category and 'intersection' not in intersec_type:
        return 'segment'
    else:
        return 'both'


def transform_cmf_data(df: pd.DataFrame) -> Tuple[List[Dict], Dict]:
    """
    Transform raw CMF data to optimized format for web.
    Returns tuple of (records list, statistics dict).
    """
    logger.info("Starting data transformation...")

    # Track statistics
    stats = {
        'totalRaw': len(df),
        'filtered': 0,
        'qualityFiltered': 0,
        'invalidCMF': 0,
        'duplicatesRemoved': 0,
        'virginiaRelevant': 0,
        'provenSafetyCountermeasures': 0,
        'hsmIncluded': 0,
    }

    # Identify columns (handle different naming conventions)
    col_mapping = {
        'id': ['crfid', 'cmfid', 'id', 'CRFID'],
        'cmf_id': ['cmid', 'cmfid', 'CMID'],
        'name': ['cmName', 'cmname', 'countermeasure', 'name', 'CMName'],
        'desc': ['cmDesc', 'cmdesc', 'description', 'CMDesc'],
        'category': ['catname', 'category', 'CatName'],
        'subcategory': ['subcatname', 'subcategory', 'SubCatName'],
        'rating': ['qualRating', 'qualrating', 'rating', 'QualRating', 'starRating'],
        'crf': ['crfactor', 'crf', 'crashReductionFactor', 'CRFactor'],
        'cmf': ['accModFactor', 'cmf', 'accmodfactor', 'AccModFactor'],
        'std_error': ['adjStanErrorCrf', 'standardError', 'stdError', 'AdjStanErrorCrf'],
        'hsm': ['inFirstHSM', 'hsm', 'inHSM', 'InFirstHSM'],
        'state': ['state', 'State', 'studyState'],
        'area_type': ['areaType', 'AreaType', 'area_type'],
        'crash_type': ['crashType', 'CrashType', 'crash_type'],
        'severity': ['crashSeverityKABCO', 'severity', 'CrashSeverityKABCO'],
        'intersection_related': ['intersectionRelated', 'IntersectionRelated'],
        'intersec_type': ['intersecType', 'IntersecType'],
        'intersec_geometry': ['intersecGeometry', 'IntersecGeometry'],
        'traffic_control': ['trafficControl', 'TrafficControl'],
        'min_aadt': ['minTrafficVol', 'MinTrafficVol', 'minAADT'],
        'max_aadt': ['maxTrafficVol', 'MaxTrafficVol', 'maxAADT'],
        'road_division': ['roadDivType', 'RoadDivType'],
        'pub_year': ['pubYear', 'PubYear', 'year'],
    }

    def get_col(name: str) -> Optional[str]:
        """Find actual column name from mapping."""
        for col in col_mapping.get(name, []):
            if col in df.columns:
                return col
        return None

    # Process each record
    records = []
    seen_names = set()

    for idx, row in df.iterrows():
        try:
            # Get CMF value
            cmf_col = get_col('cmf')
            cmf_val = safe_float(row[cmf_col]) if cmf_col else None

            # Skip invalid CMF values
            if cmf_val is None or cmf_val < MIN_CMF or cmf_val > MAX_CMF:
                stats['invalidCMF'] += 1
                continue

            # Get quality rating
            rating_col = get_col('rating')
            rating = safe_int(row[rating_col], 0) if rating_col else 0

            # Skip low quality
            if rating < MIN_QUALITY_RATING:
                stats['qualityFiltered'] += 1
                continue

            # Get name and check for duplicates
            name_col = get_col('name')
            name = safe_str(row[name_col]) if name_col else ''

            if not name:
                continue

            # Deduplicate by name (keep first/highest rated)
            name_key = name.lower()[:100]
            if name_key in seen_names:
                stats['duplicatesRemoved'] += 1
                continue
            seen_names.add(name_key)

            # Get other fields
            id_col = get_col('id')
            record_id = safe_str(row[id_col], str(idx)) if id_col else str(idx)

            category_col = get_col('category')
            category = safe_str(row[category_col], 'Other') if category_col else 'Other'

            subcategory_col = get_col('subcategory')
            subcategory = safe_str(row[subcategory_col]) if subcategory_col else ''

            crf_col = get_col('crf')
            crf = safe_float(row[crf_col]) if crf_col else None
            if crf is None:
                crf = round((1 - cmf_val) * 100, 1)

            std_error_col = get_col('std_error')
            std_error = safe_float(row[std_error_col]) if std_error_col else None

            hsm_col = get_col('hsm')
            hsm_val = safe_str(row[hsm_col]).lower() if hsm_col else ''
            in_hsm = hsm_val in ['yes', 'true', '1', 'y']

            state_col = get_col('state')
            state = safe_str(row[state_col]) if state_col else ''

            area_type_col = get_col('area_type')
            area_type = safe_str(row[area_type_col], 'All') if area_type_col else 'All'

            crash_type_col = get_col('crash_type')
            crash_type_raw = safe_str(row[crash_type_col], 'All') if crash_type_col else 'All'

            severity_col = get_col('severity')
            severity_raw = safe_str(row[severity_col]) if severity_col else ''

            intersec_geometry_col = get_col('intersec_geometry')
            intersec_geometry = safe_str(row[intersec_geometry_col]) if intersec_geometry_col else ''

            traffic_control_col = get_col('traffic_control')
            traffic_control = safe_str(row[traffic_control_col]) if traffic_control_col else ''

            min_aadt_col = get_col('min_aadt')
            min_aadt = safe_int(row[min_aadt_col]) if min_aadt_col else None

            max_aadt_col = get_col('max_aadt')
            max_aadt = safe_int(row[max_aadt_col]) if max_aadt_col else None

            pub_year_col = get_col('pub_year')
            pub_year = safe_int(row[pub_year_col]) if pub_year_col else None

            # Normalize crash types
            crash_types = normalize_crash_type(crash_type_raw)

            # Calculate Virginia relevance
            va_relevance = calculate_virginia_relevance(state)

            # Determine location type
            location_type = determine_location_type(row)

            # Parse severity
            severities = parse_severity(severity_raw)

            # Estimate cost tier
            cost_tier = estimate_cost_tier(name, category)

            # Check if FHWA Proven Safety Countermeasure (simplified check)
            is_proven = 'proven' in name.lower() or rating == 5

            # Build record with short keys for size optimization
            record = {
                'id': record_id,
                'n': name,                           # name
                'c': category,                       # category
                'sc': subcategory if subcategory else None,  # subcategory
                'cmf': round(cmf_val, 3),
                'crf': round(crf, 1),                # crash reduction %
                'r': rating,                         # rating
                'ct': crash_types,                   # crashTypes (normalized)
                'sev': severities,                   # severities
                'loc': location_type,                # locationType
                'se': round(std_error, 3) if std_error else None,  # standardError
                'hsm': in_hsm,                       # inHSM
                'psc': is_proven,                    # provenSafetyCountermeasure
                'va': va_relevance,                  # virginiaRelevance (0-100)
                'st': state if state and state.lower() != 'nan' else None,  # states
                'at': area_type if area_type.lower() != 'nan' else 'All',   # areaType
                'ig': intersec_geometry if intersec_geometry and intersec_geometry.lower() != 'nan' else None,  # intersectionGeometry
                'tc': traffic_control if traffic_control and traffic_control.lower() != 'nan' else None,  # trafficControl
                'aadt': [min_aadt, max_aadt] if min_aadt or max_aadt else None,  # AADT range
                'yr': pub_year,                      # pubYear
                'cost': cost_tier,                   # costTier (1-4)
            }

            # Remove None values to save space
            record = {k: v for k, v in record.items() if v is not None}

            records.append(record)

            # Update statistics
            if va_relevance >= 50:
                stats['virginiaRelevant'] += 1
            if is_proven:
                stats['provenSafetyCountermeasures'] += 1
            if in_hsm:
                stats['hsmIncluded'] += 1

        except Exception as e:
            logger.warning(f"Error processing row {idx}: {e}")
            continue

    stats['filtered'] = len(records)

    logger.info(f"Transformation complete: {stats['filtered']} records from {stats['totalRaw']} raw")

    return records, stats


def build_indexes(records: List[Dict]) -> Dict:
    """Build lookup indexes for faster client-side queries."""
    indexes = {
        'byCategory': {},
        'byCrashType': {},
        'byLocationType': {},
        'byVirginia': [],
        'byProven': [],
        'byHSM': [],
        'byRating': {5: [], 4: [], 3: []},
    }

    for i, record in enumerate(records):
        # By category
        cat = record.get('c', 'Other')
        if cat not in indexes['byCategory']:
            indexes['byCategory'][cat] = []
        indexes['byCategory'][cat].append(i)

        # By crash type
        for ct in record.get('ct', ['all']):
            if ct not in indexes['byCrashType']:
                indexes['byCrashType'][ct] = []
            indexes['byCrashType'][ct].append(i)

        # By location type
        loc = record.get('loc', 'both')
        if loc not in indexes['byLocationType']:
            indexes['byLocationType'][loc] = []
        indexes['byLocationType'][loc].append(i)

        # By Virginia relevance (score >= 50)
        if record.get('va', 0) >= 50:
            indexes['byVirginia'].append(i)

        # By proven status
        if record.get('psc', False):
            indexes['byProven'].append(i)

        # By HSM inclusion
        if record.get('hsm', False):
            indexes['byHSM'].append(i)

        # By rating
        rating = record.get('r', 3)
        if rating in indexes['byRating']:
            indexes['byRating'][rating].append(i)

    return indexes


def get_unique_values(records: List[Dict]) -> Dict:
    """Extract unique values for dropdowns/filters."""
    categories = set()
    crash_types = set()

    for record in records:
        categories.add(record.get('c', 'Other'))
        for ct in record.get('ct', []):
            crash_types.add(ct)

    return {
        'categories': sorted(list(categories)),
        'crashTypes': sorted(list(crash_types)),
    }


def save_processed_json(records: List[Dict], stats: Dict, indexes: Dict, unique_values: Dict):
    """Save processed data to optimized JSON file."""

    output = {
        'version': datetime.now().strftime('%Y-Q%q').replace('%q', str((datetime.now().month - 1) // 3 + 1)),
        'updated': datetime.now().strftime('%Y-%m-%d'),
        'source': 'FHWA CMF Clearinghouse',
        'stats': stats,
        'categories': unique_values['categories'],
        'crashTypeVocab': unique_values['crashTypes'],
        'indexes': indexes,
        'records': records,
    }

    # Write JSON with minimal whitespace
    with open(PROCESSED_JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'), ensure_ascii=False)

    file_size = os.path.getsize(PROCESSED_JSON_FILE)
    logger.info(f"Saved processed JSON: {PROCESSED_JSON_FILE} ({file_size / 1024 / 1024:.2f} MB)")

    # Save metadata separately
    metadata = {
        'version': output['version'],
        'updated': output['updated'],
        'source': output['source'],
        'stats': stats,
        'fileSizeBytes': file_size,
    }

    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Saved metadata: {METADATA_FILE}")


def print_statistics(stats: Dict, records: List[Dict]):
    """Print detailed statistics about the processed data."""
    print("\n" + "=" * 60)
    print("CMF DATA PROCESSING STATISTICS")
    print("=" * 60)

    print(f"\nRECORD COUNTS:")
    print(f"  Total raw records:           {stats['totalRaw']:,}")
    print(f"  Invalid CMF values:          {stats['invalidCMF']:,}")
    print(f"  Below quality threshold:     {stats['qualityFiltered']:,}")
    print(f"  Duplicates removed:          {stats['duplicatesRemoved']:,}")
    print(f"  Final processed records:     {stats['filtered']:,}")

    print(f"\nQUALITY INDICATORS:")
    print(f"  Virginia-relevant (score>=50): {stats['virginiaRelevant']:,}")
    print(f"  Proven Safety Countermeasures: {stats['provenSafetyCountermeasures']:,}")
    print(f"  In Highway Safety Manual:      {stats['hsmIncluded']:,}")

    # Category breakdown
    categories = {}
    for r in records:
        cat = r.get('c', 'Other')
        categories[cat] = categories.get(cat, 0) + 1

    print(f"\nBY CATEGORY:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat:<30} {count:,}")

    # Crash type breakdown
    crash_types = {}
    for r in records:
        for ct in r.get('ct', ['all']):
            crash_types[ct] = crash_types.get(ct, 0) + 1

    print(f"\nBY CRASH TYPE:")
    for ct, count in sorted(crash_types.items(), key=lambda x: -x[1])[:15]:
        print(f"  {ct:<20} {count:,}")

    # Rating breakdown
    ratings = {}
    for r in records:
        rating = r.get('r', 0)
        ratings[rating] = ratings.get(rating, 0) + 1

    print(f"\nBY RATING:")
    for rating in sorted(ratings.keys(), reverse=True):
        stars = '*' * rating
        print(f"  {rating} stars ({stars:<5}): {ratings[rating]:,}")

    print("\n" + "=" * 60)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Download and transform CMF data from FHWA Clearinghouse.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_cmf_data.py                    # Full download and transform
  python download_cmf_data.py --transform-only   # Transform existing CSV
  python download_cmf_data.py --stats            # Show statistics only
        """
    )

    parser.add_argument(
        '--transform-only', '-t',
        action='store_true',
        help='Transform existing CSV without downloading'
    )

    parser.add_argument(
        '--stats', '-s',
        action='store_true',
        help='Show statistics for existing processed data'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output JSON file path'
    )

    return parser.parse_args()


def main():
    """Main function to download and process CMF data."""
    args = parse_args()

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Stats only mode
    if args.stats:
        if os.path.exists(PROCESSED_JSON_FILE):
            with open(PROCESSED_JSON_FILE, 'r') as f:
                data = json.load(f)
            print_statistics(data['stats'], data['records'])
            return 0
        else:
            logger.error(f"No processed data found at {PROCESSED_JSON_FILE}")
            return 1

    logger.info("=" * 60)
    logger.info(f"Starting CMF data processing at {datetime.now()}")
    logger.info("=" * 60)

    # Get raw data
    df = None

    if args.transform_only:
        df = load_existing_csv()
        if df is None:
            logger.error("No existing CSV found for transformation")
            return 1
    else:
        # Try to download fresh data
        df = download_cmf_csv()

        if df is None:
            logger.info("Download failed, trying to use existing CSV...")
            df = load_existing_csv()
        else:
            # Save raw backup
            df.to_csv(RAW_CSV_FILE, index=False)
            logger.info(f"Saved raw backup: {RAW_CSV_FILE}")

    if df is None:
        logger.error("No CMF data available to process")
        return 1

    # Transform data
    records, stats = transform_cmf_data(df)

    if not records:
        logger.error("No valid records after transformation")
        return 1

    # Build indexes
    indexes = build_indexes(records)

    # Get unique values
    unique_values = get_unique_values(records)

    # Save processed JSON (use output arg if provided)
    if args.output:
        output_path = args.output
    else:
        output_path = PROCESSED_JSON_FILE

    save_processed_json(records, stats, indexes, unique_values)

    # Print statistics
    print_statistics(stats, records)

    logger.info("=" * 60)
    logger.info("CMF data processing complete!")
    logger.info(f"Output: {PROCESSED_JSON_FILE}")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
