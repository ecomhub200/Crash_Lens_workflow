#!/usr/bin/env python3
"""
Download traffic safety grants from Grants.gov and include state-specific grants.
Supports multiple states (Virginia, Colorado, etc.) with state-specific HSIP/402/405 programs.
Filters for transportation category, safety-related CFDA numbers, and keywords.
"""

import io
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta

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

# Grants.gov extract URL template
GRANTS_URL_TEMPLATE = "https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/GrantsDBExtract{date}v2.zip"

# Traffic Safety CFDA Numbers (most precise filtering)
SAFETY_CFDA_NUMBERS = [
    '20.600',  # State and Community Highway Safety (NHTSA 402)
    '20.601',  # Alcohol Impaired Driving Countermeasures (405d)
    '20.602',  # Occupant Protection (405b)
    '20.610',  # State Traffic Safety Information System (405c)
    '20.205',  # Highway Planning and Construction (includes HSIP)
    '20.614',  # National Priority Safety Programs (405)
    '20.616',  # Safe Streets and Roads for All (SS4A)
    '20.933',  # RAISE Discretionary Grants
    '20.934',  # INFRA Grants
    '20.218',  # Motor Carrier Safety Assistance Program
]

# Traffic safety related keywords (case-insensitive) - secondary filter
SAFETY_KEYWORDS = [
    'traffic safety',
    'highway safety',
    'pedestrian safety',
    'crash reduction',
    'vision zero',
    'safe routes to school',
    'intersection safety',
    'road safety',
    'bicycle safety',
    'roadway safety',
    'safe streets',
]

# Relevant agencies
SAFETY_AGENCIES = ['NHTSA', 'FHWA', 'DOT', 'Department of Transportation', 'Highway Administration']

# Output configuration
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "grants.csv")

# Number of days to look back for extracts if today's isn't available
MAX_LOOKBACK_DAYS = 7

# Retry settings
MAX_RETRIES = 4
RETRY_BACKOFF_FACTOR = 2  # 2s, 4s, 8s, 16s


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


def make_request_with_retry(url, timeout=300, max_manual_retries=3):
    """
    Make HTTP request with manual retry logic for network errors.
    Uses exponential backoff: 2s, 4s, 8s, 16s
    """
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

# State-specific grant configurations
# Each state has HSIP (via state DOT) and 402/405 (via state Highway Safety Office)
STATE_GRANT_CONFIGS = {
    'VA': {
        'name': 'Virginia',
        'dot_name': 'VDOT',
        'dot_full': 'VDOT Traffic Engineering Division',
        'hso_name': 'VAHSO',
        'hso_full': 'Virginia Highway Safety Office',
        'hsip_url': 'https://www.virginiadot.org/business/ted_app_pro.asp',
        'hso_url': 'https://www.dmv.virginia.gov/safety/grants-management',
    },
    'CO': {
        'name': 'Colorado',
        'dot_name': 'CDOT',
        'dot_full': 'CDOT Safety & Traffic Engineering Branch',
        'hso_name': 'CDOT OEHS',
        'hso_full': 'Colorado Office of Transportation Safety',
        'hsip_url': 'https://www.codot.gov/safety/traffic-safety/safety-programs-data/hsip',
        'hso_url': 'https://www.codot.gov/safety',
    },
}

# Federal grants (not state-specific, always included)
FEDERAL_STATIC_GRANTS = [
    {
        'grant_id': 'FED-SS4A-FY2026',
        'title': 'Safe Streets and Roads for All (SS4A) FY2026',
        'agency': 'USDOT',
        'cfda_number': '20.616',
        'program_type': 'SS4A',
        'close_date': '2026-04-15',
        'post_date': '2026-02-01',
        'federal_share_pct': 80,
        'award_ceiling': 50000000,
        'award_floor': 100000,
        'emphasis_areas': 'VRU|Fatal|Serious Injury|Equity|Vision Zero',
        'eligible_activities': 'Construction|Planning',
        'requires_crash_data': 'Y',
        'application_url': 'https://www.transportation.gov/grants/SS4A',
        'contact_info': 'SS4A@dot.gov',
        'status': 'Forecasted',
        'virginia_specific': 'N',
        'state_specific': 'N',
        'state_code': '',
        'description': 'Grants for Action Plans and Implementation projects to prevent roadway deaths and serious injuries. Focus on vulnerable road users.'
    },
    {
        'grant_id': 'FED-BUILD-FY2026',
        'title': 'BUILD Discretionary Grants FY2026',
        'agency': 'USDOT',
        'cfda_number': '20.933',
        'program_type': 'BUILD',
        'close_date': '2026-04-15',
        'post_date': '2026-02-01',
        'federal_share_pct': 80,
        'award_ceiling': 45000000,
        'award_floor': 5000000,
        'emphasis_areas': 'Infrastructure|Safety|Equity|Climate',
        'eligible_activities': 'Construction|Planning',
        'requires_crash_data': 'Y',
        'application_url': 'https://www.transportation.gov/BUILDgrants',
        'contact_info': 'BUILDgrants@dot.gov',
        'status': 'Forecasted',
        'virginia_specific': 'N',
        'state_specific': 'N',
        'state_code': '',
        'description': 'Discretionary grant program for surface transportation infrastructure projects with significant local or regional impact.'
    },
    {
        'grant_id': 'FED-INFRA-FY2026',
        'title': 'INFRA Discretionary Grants FY2026',
        'agency': 'USDOT',
        'cfda_number': '20.934',
        'program_type': 'INFRA',
        'close_date': '2026-05-15',
        'post_date': '2026-02-01',
        'federal_share_pct': 60,
        'award_ceiling': 100000000,
        'award_floor': 5000000,
        'emphasis_areas': 'Infrastructure|Freight|Safety',
        'eligible_activities': 'Construction',
        'requires_crash_data': 'Y',
        'application_url': 'https://www.transportation.gov/grants/infra-grant-program',
        'contact_info': 'MPDGrants@dot.gov',
        'status': 'Forecasted',
        'virginia_specific': 'N',
        'state_specific': 'N',
        'state_code': '',
        'description': 'Infrastructure for Rebuilding America grants for highway and freight projects of national or regional significance.'
    }
]


def get_state_grants(state_abbr, config):
    """Generate state-specific grant entries for HSIP, 402, 405b, 405c, 405d."""
    abbr = state_abbr.upper()
    is_va = abbr == 'VA'
    return [
        {
            'grant_id': f'{abbr}-HSIP-FY2027',
            'title': 'Highway Safety Improvement Program (HSIP) FY2027',
            'agency': config['dot_name'],
            'cfda_number': '20.205',
            'program_type': 'HSIP',
            'close_date': '2026-10-31',
            'post_date': '2026-08-01',
            'federal_share_pct': 90,
            'award_ceiling': None,
            'award_floor': None,
            'emphasis_areas': 'Intersection|Systemic|VRU|CMF-Based',
            'eligible_activities': 'Construction|Planning',
            'requires_crash_data': 'Y',
            'application_url': config['hsip_url'],
            'contact_info': config['dot_full'],
            'status': 'Forecasted',
            'virginia_specific': 'Y' if is_va else 'N',
            'state_specific': 'Y',
            'state_code': abbr,
            'description': f"{config['name']} federally-funded program for data-driven safety improvements on public roads. Requires benefit-cost analysis and CMF documentation."
        },
        {
            'grant_id': f'{abbr}-402-FY2027',
            'title': 'NHTSA Section 402 Highway Safety Grant FY2027',
            'agency': config['hso_name'],
            'cfda_number': '20.600',
            'program_type': '402',
            'close_date': '2026-02-28',
            'post_date': '2025-12-01',
            'federal_share_pct': 100,
            'award_ceiling': None,
            'award_floor': None,
            'emphasis_areas': 'Speed|Distracted|Pedestrian|Impaired|Occupant Protection',
            'eligible_activities': 'Enforcement|Education|Planning',
            'requires_crash_data': 'Y',
            'application_url': config['hso_url'],
            'contact_info': config['hso_full'],
            'status': 'Forecasted',
            'virginia_specific': 'Y' if is_va else 'N',
            'state_specific': 'Y',
            'state_code': abbr,
            'description': f"Formula grants for highway safety programs including enforcement, education, and public awareness campaigns in {config['name']}."
        },
        {
            'grant_id': f'{abbr}-405B-FY2027',
            'title': 'NHTSA Section 405b Occupant Protection FY2027',
            'agency': config['hso_name'],
            'cfda_number': '20.602',
            'program_type': '405b',
            'close_date': '2026-02-28',
            'post_date': '2025-12-01',
            'federal_share_pct': 100,
            'award_ceiling': None,
            'award_floor': None,
            'emphasis_areas': 'Occupant Protection|Seatbelt|Child Restraint',
            'eligible_activities': 'Enforcement|Education',
            'requires_crash_data': 'Y',
            'application_url': config['hso_url'],
            'contact_info': config['hso_full'],
            'status': 'Forecasted',
            'virginia_specific': 'Y' if is_va else 'N',
            'state_specific': 'Y',
            'state_code': abbr,
            'description': f"Incentive grants for occupant protection programs in {config['name']} including Click It or Ticket enforcement and child passenger safety."
        },
        {
            'grant_id': f'{abbr}-405C-FY2027',
            'title': 'NHTSA Section 405c Traffic Records FY2027',
            'agency': config['hso_name'],
            'cfda_number': '20.610',
            'program_type': '405c',
            'close_date': '2026-02-28',
            'post_date': '2025-12-01',
            'federal_share_pct': 100,
            'award_ceiling': None,
            'award_floor': None,
            'emphasis_areas': 'Traffic Records|Data Quality|Crash Data',
            'eligible_activities': 'Planning|Data Systems',
            'requires_crash_data': 'N',
            'application_url': config['hso_url'],
            'contact_info': config['hso_full'],
            'status': 'Forecasted',
            'virginia_specific': 'Y' if is_va else 'N',
            'state_specific': 'Y',
            'state_code': abbr,
            'description': f"Grants to improve traffic records systems in {config['name']} including crash, roadway, citation, and injury surveillance data."
        },
        {
            'grant_id': f'{abbr}-405D-FY2027',
            'title': 'NHTSA Section 405d Impaired Driving FY2027',
            'agency': config['hso_name'],
            'cfda_number': '20.601',
            'program_type': '405d',
            'close_date': '2026-02-28',
            'post_date': '2025-12-01',
            'federal_share_pct': 100,
            'award_ceiling': None,
            'award_floor': None,
            'emphasis_areas': 'Impaired|DUI|Alcohol|Drugs',
            'eligible_activities': 'Enforcement|Education|Courts',
            'requires_crash_data': 'Y',
            'application_url': config['hso_url'],
            'contact_info': config['hso_full'],
            'status': 'Forecasted',
            'virginia_specific': 'Y' if is_va else 'N',
            'state_specific': 'Y',
            'state_code': abbr,
            'description': f"Incentive grants for impaired driving countermeasures in {config['name']} including high-visibility enforcement and DUI courts."
        },
    ]


def get_all_static_grants():
    """Get all static grants: state-specific for each configured state + federal."""
    all_grants = []
    for state_abbr, config in STATE_GRANT_CONFIGS.items():
        all_grants.extend(get_state_grants(state_abbr, config))
    all_grants.extend(FEDERAL_STATIC_GRANTS)
    return all_grants


# Legacy alias for backward compatibility
VIRGINIA_STATIC_GRANTS = get_all_static_grants()


def get_grants_url(target_date: datetime) -> str:
    """Generate the Grants.gov extract URL for a specific date."""
    date_str = target_date.strftime('%Y%m%d')
    return GRANTS_URL_TEMPLATE.format(date=date_str)


def download_grants_extract(max_attempts: int = MAX_LOOKBACK_DAYS) -> pd.DataFrame:
    """
    Download and extract Grants.gov data.
    Tries today's extract first, then looks back up to max_attempts days.
    """
    target_date = datetime.now()

    for attempt in range(max_attempts):
        url = get_grants_url(target_date)
        logger.info(f"Attempting to download: {url}")

        try:
            response = make_request_with_retry(url, timeout=300)

            if response.status_code == 200:
                logger.info(f"Successfully downloaded extract for {target_date.strftime('%Y-%m-%d')}")

                # Extract ZIP file
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    file_list = zf.namelist()
                    logger.info(f"Files in archive: {file_list}")

                    # Look for opportunities data file
                    opportunities_file = None
                    for filename in file_list:
                        if 'opportunit' in filename.lower():
                            opportunities_file = filename
                            break

                    if opportunities_file is None:
                        for filename in file_list:
                            if filename.endswith('.csv') or filename.endswith('.xml'):
                                opportunities_file = filename
                                break

                    if opportunities_file is None and file_list:
                        opportunities_file = file_list[0]

                    if opportunities_file:
                        logger.info(f"Reading file: {opportunities_file}")

                        with zf.open(opportunities_file) as f:
                            content = f.read()

                            # Try to parse as CSV first
                            try:
                                df = pd.read_csv(io.BytesIO(content), low_memory=False)
                                logger.info(f"Parsed {len(df)} records from CSV")
                                return df
                            except Exception:
                                pass

                            # Try to parse as XML
                            try:
                                df = pd.read_xml(io.BytesIO(content))
                                logger.info(f"Parsed {len(df)} records from XML")
                                return df
                            except Exception:
                                pass

                            # Try pipe-delimited
                            try:
                                df = pd.read_csv(io.BytesIO(content), delimiter='|', low_memory=False)
                                logger.info(f"Parsed {len(df)} records from pipe-delimited file")
                                return df
                            except Exception as e:
                                logger.error(f"Failed to parse {opportunities_file}: {e}")

            elif response.status_code == 404:
                logger.info(f"Extract not found for {target_date.strftime('%Y-%m-%d')}, trying previous day...")
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout downloading {url}")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")

        # Try previous day
        target_date -= timedelta(days=1)

    logger.warning(f"Could not download grants extract after {max_attempts} attempts")
    return pd.DataFrame()


def filter_by_cfda(df: pd.DataFrame) -> pd.Series:
    """Filter for traffic safety CFDA numbers."""
    cfda_columns = [
        'CFDANumbers', 'CFDA_Numbers', 'cfda_numbers', 'CfdaNumber',
        'CFDANumber', 'CFDA', 'cfda'
    ]

    cfda_col = None
    for col in cfda_columns:
        if col in df.columns:
            cfda_col = col
            break

    if cfda_col:
        # Create pattern for CFDA matching
        cfda_pattern = '|'.join([re.escape(cfda) for cfda in SAFETY_CFDA_NUMBERS])
        mask = df[cfda_col].astype(str).str.contains(cfda_pattern, na=False, regex=True)
        logger.info(f"Found {mask.sum()} grants matching safety CFDA numbers in {cfda_col}")
        return mask
    else:
        logger.warning("Could not find CFDA column")
        return pd.Series([False] * len(df))


def filter_by_agency(df: pd.DataFrame) -> pd.Series:
    """Filter for relevant safety agencies."""
    agency_columns = [
        'AgencyName', 'Agency', 'agency_name', 'AGENCY_NAME',
        'GrantorContactName', 'FundingAgency', 'AgencyCode'
    ]

    mask = pd.Series([False] * len(df))
    agency_pattern = '|'.join([re.escape(a) for a in SAFETY_AGENCIES])

    for col in agency_columns:
        if col in df.columns:
            col_mask = df[col].astype(str).str.contains(agency_pattern, na=False, case=False, regex=True)
            mask |= col_mask
            logger.info(f"Found {col_mask.sum()} agency matches in {col}")

    return mask


def filter_by_keywords(df: pd.DataFrame) -> pd.Series:
    """Filter for traffic safety related keywords in title and description."""
    keyword_pattern = '|'.join([re.escape(kw) for kw in SAFETY_KEYWORDS])
    mask = pd.Series([False] * len(df))

    # Check title columns
    title_columns = [
        'OpportunityTitle', 'Title', 'opportunity_title',
        'OPPORTUNITY_TITLE', 'Grant_Title', 'ProjectTitle'
    ]

    for col in title_columns:
        if col in df.columns:
            col_mask = df[col].astype(str).str.lower().str.contains(keyword_pattern, na=False, regex=True)
            mask |= col_mask
            logger.info(f"Found {col_mask.sum()} keyword matches in {col}")
            break

    # Check description columns
    desc_columns = [
        'Description', 'OpportunityDescription', 'description',
        'DESCRIPTION', 'Synopsis', 'Abstract', 'Summary'
    ]

    for col in desc_columns:
        if col in df.columns:
            col_mask = df[col].astype(str).str.lower().str.contains(keyword_pattern, na=False, regex=True)
            mask |= col_mask
            logger.info(f"Found {col_mask.sum()} keyword matches in {col}")
            break

    return mask


def filter_grants(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all filters to grants data."""
    original_count = len(df)

    # Priority: CFDA > Agency > Keywords
    cfda_mask = filter_by_cfda(df)
    agency_mask = filter_by_agency(df)
    keyword_mask = filter_by_keywords(df)

    # Combine: CFDA OR (Agency AND Keywords)
    combined_mask = cfda_mask | (agency_mask & keyword_mask)

    df_filtered = df[combined_mask].copy()
    logger.info(f"Filtered from {original_count} to {len(df_filtered)} relevant grants")

    return df_filtered


def filter_active_grants(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to only include grants that are still open or recently closed."""
    date_columns = [
        'CloseDate', 'close_date', 'CLOSE_DATE', 'ApplicationDueDate',
        'DueDate', 'Deadline', 'ResponseDate'
    ]

    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break

    if date_col:
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            cutoff_date = datetime.now() - timedelta(days=30)
            mask = (df[date_col].isna()) | (df[date_col] >= cutoff_date)
            df_filtered = df[mask].copy()
            logger.info(f"Filtered to {len(df_filtered)} active/recent grants")
            return df_filtered
        except Exception as e:
            logger.warning(f"Could not filter by date: {e}")

    return df


def map_to_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map Grants.gov columns to our standard output format."""
    column_mappings = {
        # Source column -> Output column
        'OpportunityID': 'grant_id',
        'OpportunityNumber': 'grant_id',
        'OpportunityTitle': 'title',
        'AgencyName': 'agency',
        'CFDANumbers': 'cfda_number',
        'Description': 'description',
        'Synopsis': 'description',
        'CloseDate': 'close_date',
        'PostDate': 'post_date',
        'AwardCeiling': 'award_ceiling',
        'AwardFloor': 'award_floor',
        'AdditionalInformationURL': 'application_url',
        'GrantorContactEmail': 'contact_info',
    }

    output_data = {}

    for orig_col, new_col in column_mappings.items():
        if orig_col in df.columns and new_col not in output_data:
            output_data[new_col] = df[orig_col]
        else:
            # Try case-insensitive match
            for col in df.columns:
                if col.lower() == orig_col.lower() and new_col not in output_data:
                    output_data[new_col] = df[col]
                    break

    df_output = pd.DataFrame(output_data)

    # Add default values for missing columns
    if 'program_type' not in df_output.columns:
        df_output['program_type'] = 'Federal'
    if 'federal_share_pct' not in df_output.columns:
        df_output['federal_share_pct'] = None
    if 'emphasis_areas' not in df_output.columns:
        df_output['emphasis_areas'] = 'Safety'
    if 'eligible_activities' not in df_output.columns:
        df_output['eligible_activities'] = None
    if 'requires_crash_data' not in df_output.columns:
        df_output['requires_crash_data'] = 'N'
    if 'status' not in df_output.columns:
        df_output['status'] = 'Open'
    if 'virginia_specific' not in df_output.columns:
        df_output['virginia_specific'] = 'N'
    if 'state_specific' not in df_output.columns:
        df_output['state_specific'] = 'N'
    if 'state_code' not in df_output.columns:
        df_output['state_code'] = ''

    return df_output


def get_virginia_static_grants() -> pd.DataFrame:
    """Get all static grants (all states + federal) as a DataFrame."""
    return pd.DataFrame(get_all_static_grants())


def main():
    """Main function to download and process grants data."""
    logger.info("=" * 60)
    logger.info(f"Starting grants data download at {datetime.now()}")
    logger.info("=" * 60)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Start with all static grants (all configured states + federal)
    static_grants = get_virginia_static_grants()
    logger.info(f"Loaded {len(static_grants)} static grants ({len(STATE_GRANT_CONFIGS)} states + federal)")

    # Try to download federal grants from Grants.gov
    federal_grants = pd.DataFrame()

    try:
        df = download_grants_extract()

        if not df.empty:
            logger.info(f"Downloaded {len(df)} total grants from Grants.gov")

            # Filter for traffic safety related grants
            df = filter_grants(df)

            if not df.empty:
                # Filter to active grants
                df = filter_active_grants(df)

                # Map to output columns
                federal_grants = map_to_output_columns(df)
                logger.info(f"Processed {len(federal_grants)} federal grants")

    except Exception as e:
        logger.warning(f"Could not download federal grants: {e}")
        logger.info("Proceeding with static grants only")

    # Combine static and federal grants
    if not federal_grants.empty:
        # Ensure columns match
        all_columns = [
            'grant_id', 'title', 'agency', 'cfda_number', 'program_type',
            'close_date', 'post_date', 'federal_share_pct', 'award_ceiling',
            'award_floor', 'emphasis_areas', 'eligible_activities',
            'requires_crash_data', 'application_url', 'contact_info',
            'status', 'virginia_specific', 'state_specific', 'state_code',
            'description'
        ]

        for col in all_columns:
            if col not in static_grants.columns:
                static_grants[col] = None
            if col not in federal_grants.columns:
                federal_grants[col] = None

        combined_grants = pd.concat([static_grants[all_columns], federal_grants[all_columns]], ignore_index=True)
    else:
        combined_grants = static_grants

    # Remove duplicates based on title similarity
    combined_grants = combined_grants.drop_duplicates(subset=['title'], keep='first')

    # Add last_updated timestamp
    combined_grants['last_updated'] = datetime.now().strftime('%Y-%m-%d')

    # Sort by close_date
    combined_grants['close_date'] = pd.to_datetime(combined_grants['close_date'], errors='coerce')
    combined_grants = combined_grants.sort_values('close_date', ascending=True, na_position='last')
    combined_grants['close_date'] = combined_grants['close_date'].dt.strftime('%Y-%m-%d')

    # Save to CSV
    logger.info(f"Saving {len(combined_grants)} grants to {OUTPUT_FILE}")
    combined_grants.to_csv(OUTPUT_FILE, index=False)

    logger.info("=" * 60)
    logger.info(f"Successfully processed grants data")
    logger.info(f"Output saved to: {OUTPUT_FILE}")
    logger.info(f"Total grants: {len(combined_grants)}")
    state_specific = combined_grants[combined_grants.get('state_specific', combined_grants.get('virginia_specific', '')) == 'Y']
    logger.info(f"  - State-specific: {len(state_specific)}")
    for state_code in combined_grants['state_code'].dropna().unique():
        if state_code:
            count = len(combined_grants[combined_grants['state_code'] == state_code])
            logger.info(f"    - {state_code}: {count}")
    federal_count = len(combined_grants) - len(state_specific)
    logger.info(f"  - Federal: {federal_count}")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
