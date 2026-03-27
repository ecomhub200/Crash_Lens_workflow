#!/usr/bin/env python3
"""
Generate hierarchy.json files for all 50 US states + DC.

Reads:
  - docs/CDOT/us_states_dot_districts_mpos.json  (external DOT district & MPO data)
  - states/us_counties_db.js                       (county name → FIPS lookup)
  - states/fips_database.js                        (state metadata)

Outputs:
  - states/{state_name}/hierarchy.json for each state (skips VA and CO which are hand-curated)

Format matches existing VA/CO hierarchy.json structure for HierarchyRegistry compatibility.
"""

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ─── State FIPS ↔ abbreviation mapping ───
FIPS_TO_ABBR = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY'
}
ABBR_TO_FIPS = {v: k for k, v in FIPS_TO_ABBR.items()}

# ─── DOT full names by abbreviation ───
DOT_FULL_NAMES = {
    'AL': ('ALDOT', 'Alabama Department of Transportation'),
    'AK': ('AKDOT&PF', 'Alaska Department of Transportation and Public Facilities'),
    'AZ': ('ADOT', 'Arizona Department of Transportation'),
    'AR': ('ArDOT', 'Arkansas Department of Transportation'),
    'CA': ('Caltrans', 'California Department of Transportation'),
    'CO': ('CDOT', 'Colorado Department of Transportation'),
    'CT': ('CTDOT', 'Connecticut Department of Transportation'),
    'DE': ('DelDOT', 'Delaware Department of Transportation'),
    'DC': ('DDOT', 'District Department of Transportation'),
    'FL': ('FDOT', 'Florida Department of Transportation'),
    'GA': ('GDOT', 'Georgia Department of Transportation'),
    'HI': ('HDOT', 'Hawaii Department of Transportation'),
    'ID': ('ITD', 'Idaho Transportation Department'),
    'IL': ('IDOT', 'Illinois Department of Transportation'),
    'IN': ('INDOT', 'Indiana Department of Transportation'),
    'IA': ('Iowa DOT', 'Iowa Department of Transportation'),
    'KS': ('KDOT', 'Kansas Department of Transportation'),
    'KY': ('KYTC', 'Kentucky Transportation Cabinet'),
    'LA': ('LADOTD', 'Louisiana Department of Transportation and Development'),
    'ME': ('MaineDOT', 'Maine Department of Transportation'),
    'MD': ('MDOT', 'Maryland Department of Transportation'),
    'MA': ('MassDOT', 'Massachusetts Department of Transportation'),
    'MI': ('MDOT', 'Michigan Department of Transportation'),
    'MN': ('MnDOT', 'Minnesota Department of Transportation'),
    'MS': ('MDOT', 'Mississippi Department of Transportation'),
    'MO': ('MoDOT', 'Missouri Department of Transportation'),
    'MT': ('MDT', 'Montana Department of Transportation'),
    'NE': ('NDOT', 'Nebraska Department of Transportation'),
    'NV': ('NDOT', 'Nevada Department of Transportation'),
    'NH': ('NHDOT', 'New Hampshire Department of Transportation'),
    'NJ': ('NJDOT', 'New Jersey Department of Transportation'),
    'NM': ('NMDOT', 'New Mexico Department of Transportation'),
    'NY': ('NYSDOT', 'New York State Department of Transportation'),
    'NC': ('NCDOT', 'North Carolina Department of Transportation'),
    'ND': ('NDDOT', 'North Dakota Department of Transportation'),
    'OH': ('ODOT', 'Ohio Department of Transportation'),
    'OK': ('ODOT', 'Oklahoma Department of Transportation'),
    'OR': ('ODOT', 'Oregon Department of Transportation'),
    'PA': ('PennDOT', 'Pennsylvania Department of Transportation'),
    'RI': ('RIDOT', 'Rhode Island Department of Transportation'),
    'SC': ('SCDOT', 'South Carolina Department of Transportation'),
    'SD': ('SDDOT', 'South Dakota Department of Transportation'),
    'TN': ('TDOT', 'Tennessee Department of Transportation'),
    'TX': ('TxDOT', 'Texas Department of Transportation'),
    'UT': ('UDOT', 'Utah Department of Transportation'),
    'VT': ('VTrans', 'Vermont Agency of Transportation'),
    'VA': ('VDOT', 'Virginia Department of Transportation'),
    'WA': ('WSDOT', 'Washington State Department of Transportation'),
    'WV': ('WVDOT', 'West Virginia Division of Highways'),
    'WI': ('WisDOT', 'Wisconsin Department of Transportation'),
    'WY': ('WYDOT', 'Wyoming Department of Transportation'),
}


def parse_us_counties_db(js_path):
    """
    Parse us_counties_db.js to build a county name → FIPS lookup per state.
    Returns: { state_fips: { normalized_name: county_fips, ... }, ... }
    """
    with open(js_path, 'r') as f:
        content = f.read()

    lookup = {}  # state_fips -> { name_lower: county_fips }

    # Match state blocks: '01': { ... }
    state_pattern = re.compile(r"'(\d{2})':\s*\{")
    # Match county entries: 'name': {n:"Name County",t:'county',f:'001',...}
    county_pattern = re.compile(r"'([^']+)':\s*\{n:\"([^\"]+)\",t:'[^']*',f:'(\d{3})'")

    current_state = None
    for line in content.split('\n'):
        state_match = state_pattern.search(line)
        if state_match:
            current_state = state_match.group(1)
            if current_state not in lookup:
                lookup[current_state] = {}

        if current_state:
            county_match = county_pattern.search(line)
            if county_match:
                _key = county_match.group(1)
                full_name = county_match.group(2)  # e.g., "Autauga County"
                county_fips = county_match.group(3)

                # Store multiple lookup forms
                name_lower = full_name.lower().replace(' county', '').replace(' parish', '').replace(' borough', '').replace(' census area', '').replace(' municipality', '').strip()
                lookup[current_state][name_lower] = county_fips

                # Also store the full name variant
                lookup[current_state][full_name.lower()] = county_fips

                # Store by key
                lookup[current_state][_key] = county_fips

    return lookup


def resolve_county_fips(county_name, state_fips, county_lookup):
    """Resolve a county name to its FIPS code using the lookup table."""
    if state_fips not in county_lookup:
        return None

    state_counties = county_lookup[state_fips]
    name = county_name.strip()

    # Try exact match
    name_lower = name.lower()
    if name_lower in state_counties:
        return state_counties[name_lower]

    # Try without common suffixes
    for suffix in [' county', ' parish', ' borough', ' census area', ' municipality',
                   ' city', ' city and borough', ' city and county']:
        cleaned = name_lower.replace(suffix, '').strip()
        if cleaned in state_counties:
            return state_counties[cleaned]

    # Try key-style (lowercase, no special chars)
    key_style = re.sub(r'[^a-z0-9]', '', name_lower.replace("'", '').replace('.', '').replace(' ', ''))
    for k, v in state_counties.items():
        k_clean = re.sub(r'[^a-z0-9]', '', k.replace("'", '').replace('.', '').replace(' ', ''))
        if k_clean == key_style:
            return v

    # Try partial match (first word)
    first_word = name_lower.split()[0] if ' ' in name_lower else name_lower
    for k, v in state_counties.items():
        if k.startswith(first_word) and len(first_word) > 3:
            return v

    return None


def make_region_id(state_abbr, district):
    """Create a clean region ID from district data."""
    raw_id = district.get('id', '')
    # Use the existing ID format but normalize
    return raw_id.lower().replace(' ', '_')


def make_mpo_key(mpo):
    """Create a clean MPO key from MPO data."""
    name = mpo.get('name', '')
    # Extract acronym from parenthetical or create from name
    paren_match = re.search(r'\(([A-Z]{2,}[A-Za-z]*)\)', name)
    if paren_match:
        return paren_match.group(1).lower()

    # Try to find acronym-like patterns
    words = re.findall(r'[A-Z][a-z]*', name)
    if len(words) >= 2:
        # Make acronym from capital letters
        acronym = ''.join(w[0] for w in words if w[0].isupper())
        if len(acronym) >= 2:
            return acronym.lower()

    # Fallback: use ID
    return mpo.get('id', 'unknown').lower().replace('-', '_')


def guess_bts_acronym(mpo_name):
    """
    Attempt to extract a BTS-compatible acronym from the MPO name.
    Returns the best guess acronym or None.
    """
    name = mpo_name.strip()

    # Check for explicit acronym in parentheses
    paren_match = re.search(r'\(([A-Z]{2,}[A-Za-z/&]*)\)', name)
    if paren_match:
        return paren_match.group(1).replace('/', '').replace('&', '')

    # Common known MPO acronyms by partial name match
    known = {
        'Birmingham': 'RPCGB',
        'Mobile Area Transportation Study': 'MATS',
        'Huntsville Area': 'HAMPO',
        'Tuscaloosa': 'WACOG',
        'Columbus-Phenix': 'CVRPC',
        'Anchorage': 'AMATS',
        'Fairbanks': 'FMATS',
        'Mat-Su': 'MSBOA',
    }
    for key, acronym in known.items():
        if key in name:
            return acronym

    # Try to create acronym from capitalized words
    # e.g., "Denver Regional Council of Governments" -> "DRCOG"
    words = name.split()
    # Filter out small connecting words
    skip_words = {'of', 'the', 'and', 'for', 'in', 'on', 'at', 'to', 'a', 'an'}
    significant = [w for w in words if w.lower() not in skip_words and not w.startswith('(')]

    if len(significant) >= 2:
        acronym = ''.join(w[0].upper() for w in significant if w[0].isalpha())
        if 2 <= len(acronym) <= 8:
            return acronym

    return None


def build_hierarchy(state_abbr, state_data, county_lookup):
    """Build a hierarchy.json structure from external state data."""
    state_fips = ABBR_TO_FIPS.get(state_abbr)
    if not state_fips:
        print(f"  WARNING: No FIPS code for {state_abbr}, skipping")
        return None

    dot_short, dot_full = DOT_FULL_NAMES.get(state_abbr, (state_data.get('dot_name', ''), ''))

    # State-level info
    map_center = state_data.get('mapCenter', {})
    hierarchy = {
        "state": {
            "fips": state_fips,
            "name": state_data.get('name', ''),
            "abbreviation": state_abbr,
            "dot": dot_short or state_data.get('dot_name', ''),
            "dotFullName": dot_full,
            "center": [map_center.get('lng', 0), map_center.get('lat', 0)],
            "zoom": state_data.get('mapZoom', 7)
        },
        "regionType": {
            "label": "DOT District",
            "labelPlural": "DOT Districts",
            "shortLabel": "District"
        },
        "regions": {},
        "tprType": {
            "label": "MPO",
            "labelPlural": "Metropolitan Planning Organizations"
        },
        "tprs": {},
        "allCounties": {}
    }

    # Track all counties for the allCounties section
    all_counties_map = {}

    # ─── DOT Districts → regions ───
    for district in state_data.get('dot_districts', []):
        dist_id = district.get('id', '').lower().replace('-', '_').replace(' ', '_')
        dist_name = district.get('name', '')
        dist_center = district.get('mapCenter', {})

        county_fips_list = []
        county_names_map = {}

        for county_name in district.get('counties', []):
            fips = resolve_county_fips(county_name, state_fips, county_lookup)
            if fips:
                county_fips_list.append(fips)
                county_names_map[fips] = county_name
                all_counties_map[fips] = county_name
            else:
                # Store name as-is for unresolved counties
                print(f"  [WARN] Could not resolve county '{county_name}' in {state_abbr} (FIPS {state_fips})")

        region_entry = {
            "name": dist_name,
            "shortName": dist_name.split('(')[0].strip() if '(' in dist_name else dist_name,
            "center": [dist_center.get('lng', 0), dist_center.get('lat', 0)],
            "zoom": district.get('mapZoom', 8),
            "counties": county_fips_list,
            "countyNames": county_names_map
        }

        # Add mapBounds if available
        bounds = district.get('mapBounds')
        if bounds:
            region_entry["mapBounds"] = {
                "sw": bounds['sw'],
                "ne": bounds['ne']
            }

        # Add HQ if available
        if district.get('hq'):
            region_entry["hq"] = district['hq']

        hierarchy["regions"][dist_id] = region_entry

    # ─── MPOs → tprs ───
    for mpo in state_data.get('mpos', []):
        mpo_key = make_mpo_key(mpo)
        mpo_name = mpo.get('name', '')
        mpo_center = mpo.get('mapCenter', {})

        county_fips_list = []
        county_names_map = {}

        for county_name in mpo.get('counties', []):
            fips = resolve_county_fips(county_name, state_fips, county_lookup)
            if fips:
                county_fips_list.append(fips)
                county_names_map[fips] = county_name
                all_counties_map[fips] = county_name

        # Extract short name from parenthetical or use full name
        short_match = re.search(r'\(([^)]+)\)', mpo_name)
        short_name = short_match.group(1) if short_match else mpo_name.split(' MPO')[0].split(' Metropolitan')[0].strip()

        bts_acronym = guess_bts_acronym(mpo_name)

        mpo_entry = {
            "name": mpo_name,
            "shortName": short_name,
            "type": "mpo",
            "center": [mpo_center.get('lng', 0), mpo_center.get('lat', 0)],
            "zoom": mpo.get('mapZoom', 10),
            "counties": county_fips_list,
            "countyNames": county_names_map
        }

        if bts_acronym:
            mpo_entry["btsAcronym"] = bts_acronym

        # Add mapBounds if available
        bounds = mpo.get('mapBounds')
        if bounds:
            mpo_entry["mapBounds"] = {
                "sw": bounds['sw'],
                "ne": bounds['ne']
            }

        hierarchy["tprs"][mpo_key] = mpo_entry

    # Build allCounties map (FIPS → name)
    hierarchy["allCounties"] = dict(sorted(all_counties_map.items()))

    return hierarchy


def main():
    # Load external data
    ext_path = os.path.join(ROOT, 'docs', 'CDOT', 'us_states_dot_districts_mpos.json')
    print(f"Loading external data from: {ext_path}")
    with open(ext_path) as f:
        ext_data = json.load(f)

    states_data = ext_data.get('states', {})
    print(f"Found {len(states_data)} states in external data")

    # Parse county lookup from us_counties_db.js
    counties_db_path = os.path.join(ROOT, 'states', 'us_counties_db.js')
    print(f"Parsing county database from: {counties_db_path}")
    county_lookup = parse_us_counties_db(counties_db_path)
    total_counties = sum(len(v) for v in county_lookup.values())
    print(f"Loaded {total_counties} county entries across {len(county_lookup)} states")

    # States to skip (already have hand-curated hierarchy.json)
    SKIP_STATES = {'VA', 'CO'}

    generated = 0
    skipped = 0
    errors = 0

    for state_abbr in sorted(states_data.keys()):
        state_data = states_data[state_abbr]
        state_name = state_data.get('name', state_abbr)

        if state_abbr in SKIP_STATES:
            print(f"\n[SKIP] {state_abbr} ({state_name}) — hand-curated hierarchy exists")
            skipped += 1
            continue

        print(f"\n[GEN] {state_abbr} ({state_name})...")
        print(f"  Districts: {len(state_data.get('dot_districts', []))}, MPOs: {len(state_data.get('mpos', []))}")

        try:
            hierarchy = build_hierarchy(state_abbr, state_data, county_lookup)
            if not hierarchy:
                errors += 1
                continue

            # Create state directory
            dir_name = state_name.lower().replace(' ', '_')
            state_dir = os.path.join(ROOT, 'states', dir_name)
            os.makedirs(state_dir, exist_ok=True)

            # Write hierarchy.json
            out_path = os.path.join(state_dir, 'hierarchy.json')
            with open(out_path, 'w') as f:
                json.dump(hierarchy, f, indent=2)

            regions_count = len(hierarchy.get('regions', {}))
            mpos_count = len(hierarchy.get('tprs', {}))
            counties_count = len(hierarchy.get('allCounties', {}))
            print(f"  ✓ Written: {out_path}")
            print(f"    {regions_count} regions, {mpos_count} MPOs, {counties_count} counties")
            generated += 1

        except Exception as e:
            print(f"  ERROR: {e}")
            errors += 1

    print(f"\n{'='*60}")
    print(f"SUMMARY: {generated} generated, {skipped} skipped, {errors} errors")
    print(f"Total states with hierarchy: {generated + skipped}")


if __name__ == '__main__':
    main()
