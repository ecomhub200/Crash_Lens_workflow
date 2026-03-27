#!/usr/bin/env python3
"""
Resolve pipeline scope to a concrete list of jurisdiction IDs.

Translates user selection (state + scope + selection) into:
  - A list of jurisdiction IDs to process
  - The download mode (statewide vs per-jurisdiction)
  - State config metadata (dot_name, data_dir, r2_prefix)

Usage:
    python scripts/resolve_scope.py --state virginia --scope jurisdiction --selection henrico
    python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads
    python scripts/resolve_scope.py --state colorado --scope mpo --selection drcog
    python scripts/resolve_scope.py --state virginia --scope statewide
    python scripts/resolve_scope.py --state virginia --list
    python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads --json
"""

import argparse
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
STATEWIDE_DOWNLOAD_THRESHOLD = 3


def load_hierarchy(state):
    path = PROJECT_ROOT / 'states' / state / 'hierarchy.json'
    if not path.exists():
        print(f"ERROR: No hierarchy.json for '{state}' at {path}", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def load_state_config(state):
    config_path = PROJECT_ROOT / 'config.json'
    with open(config_path) as f:
        config = json.load(f)
    states = config.get('states', {})
    if state not in states:
        for key, val in states.items():
            if key.lower() == state.lower():
                return val
        print(f"ERROR: State '{state}' not in config.json", file=sys.stderr)
        sys.exit(1)
    return states[state]


def county_name_to_key(name):
    """Convert a county display name to a snake_case jurisdiction key.

    Examples:
        'Henrico' -> 'henrico'
        'El Paso' -> 'el_paso'
        'Prince George\\'s' -> 'prince_georges'
        'St. Mary\\'s' -> 'st_marys'
        'Alexandria City' -> 'alexandria_city'
    """
    key = name.lower().strip()
    key = key.replace("'", "").replace(".", "")
    key = re.sub(r'[^a-z0-9]+', '_', key)
    key = key.strip('_')
    return key


def load_config_fips_map(state_abbr=None):
    """Build FIPS → config.json jurisdiction key mapping.

    config.json jurisdiction keys are the source of truth for filenames
    (e.g., "alexandria" not "alexandria_city"), so we prefer these over
    hierarchy-derived keys when available.

    Args:
        state_abbr: Two-letter state abbreviation (e.g., "VA", "CO") to
            filter jurisdictions. Required to avoid FIPS collisions between
            states (county FIPS codes are only unique within a state).
    """
    config_path = PROJECT_ROOT / 'config.json'
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        config = json.load(f)
    fips_to_config_key = {}
    for jid, jinfo in config.get('jurisdictions', {}).items():
        if isinstance(jinfo, dict) and 'fips' in jinfo:
            if state_abbr and jinfo.get('state') != state_abbr:
                continue
            fips_to_config_key[jinfo['fips']] = jid
    return fips_to_config_key


def build_fips_to_key_map(hierarchy):
    """Build a mapping from FIPS code to jurisdiction key.

    Prefers config.json keys (which match actual output filenames) over
    hierarchy-derived keys. Falls back to hierarchy names for FIPS codes
    not in config.json.
    """
    state_abbr = hierarchy.get('state', {}).get('abbreviation')
    config_fips_map = load_config_fips_map(state_abbr)

    all_counties = hierarchy.get('allCounties', {})
    fips_to_key = {}
    for fips, name in all_counties.items():
        if fips in config_fips_map:
            fips_to_key[fips] = config_fips_map[fips]
        elif isinstance(name, str):
            fips_to_key[fips] = county_name_to_key(name)
        elif isinstance(name, dict):
            display = name.get('name', name.get('displayName', fips))
            fips_to_key[fips] = county_name_to_key(display)
    return fips_to_key


def get_all_counties(hierarchy):
    """Get all county jurisdiction keys, sorted."""
    fips_to_key = build_fips_to_key_map(hierarchy)
    return sorted(fips_to_key.values())


def fips_list_to_keys(hierarchy, fips_codes):
    """Convert a list of FIPS codes to jurisdiction keys."""
    fips_to_key = build_fips_to_key_map(hierarchy)
    keys = []
    for code in fips_codes:
        if code in fips_to_key:
            keys.append(fips_to_key[code])
        else:
            print(f"WARNING: FIPS '{code}' not found in allCounties", file=sys.stderr)
    return sorted(keys)


def resolve_region(hierarchy, region_id):
    regions = hierarchy.get('regions', {})
    if region_id not in regions:
        for key in regions:
            if key.lower().replace('_', '') == region_id.lower().replace('_', ''):
                region_id = key
                break
        else:
            print(f"ERROR: Region '{region_id}' not found. Available: {', '.join(regions.keys())}", file=sys.stderr)
            sys.exit(1)
    fips_codes = regions[region_id].get('counties', [])
    return fips_list_to_keys(hierarchy, fips_codes)


def resolve_mpo(hierarchy, mpo_id):
    """Resolve MPO from tprs section (MPOs have type='mpo')."""
    tprs = hierarchy.get('tprs', {})
    # Also check for a dedicated 'mpos' section
    mpos = hierarchy.get('mpos', {})

    # Merge MPO-type TPRs with explicit mpos
    all_mpos = {}
    for key, val in tprs.items():
        if val.get('type') == 'mpo':
            all_mpos[key] = val
    all_mpos.update(mpos)

    if mpo_id not in all_mpos:
        for key in all_mpos:
            if key.lower().replace('_', '') == mpo_id.lower().replace('_', ''):
                mpo_id = key
                break
        else:
            print(f"ERROR: MPO '{mpo_id}' not found. Available: {', '.join(all_mpos.keys())}", file=sys.stderr)
            sys.exit(1)
    fips_codes = all_mpos[mpo_id].get('counties', [])
    return fips_list_to_keys(hierarchy, fips_codes)


def resolve_scope(state, scope, selection, hierarchy):
    if scope == 'jurisdiction':
        if not selection:
            print("ERROR: --selection required for scope=jurisdiction", file=sys.stderr)
            sys.exit(1)
        return [selection]
    elif scope == 'region':
        if not selection:
            print("ERROR: --selection required for scope=region", file=sys.stderr)
            sys.exit(1)
        return resolve_region(hierarchy, selection)
    elif scope == 'mpo':
        if not selection:
            print("ERROR: --selection required for scope=mpo", file=sys.stderr)
            sys.exit(1)
        return resolve_mpo(hierarchy, selection)
    elif scope == 'statewide':
        return get_all_counties(hierarchy)
    else:
        print(f"ERROR: Unknown scope '{scope}'", file=sys.stderr)
        sys.exit(1)


def list_scopes(state, hierarchy):
    print(f"\n=== Available scopes for {state} ===\n")

    # Regions
    regions = hierarchy.get('regions', {})
    if regions:
        print("Regions:")
        for rid, rdata in sorted(regions.items()):
            name = rdata.get('name', rdata.get('shortName', rid))
            count = len(rdata.get('counties', []))
            print(f"  {rid}: {name} ({count} counties)")

    # MPOs (from tprs with type=mpo, plus any explicit mpos section)
    tprs = hierarchy.get('tprs', {})
    mpos = hierarchy.get('mpos', {})
    all_mpos = {}
    for key, val in tprs.items():
        if val.get('type') == 'mpo':
            all_mpos[key] = val
    all_mpos.update(mpos)

    if all_mpos:
        print("\nMPOs:")
        for mid, mdata in sorted(all_mpos.items()):
            name = mdata.get('name', mdata.get('shortName', mid))
            count = len(mdata.get('counties', []))
            print(f"  {mid}: {name} ({count} counties)")

    all_counties = hierarchy.get('allCounties', {})
    print(f"\nTotal counties: {len(all_counties)}")


def main():
    parser = argparse.ArgumentParser(description='Resolve pipeline scope')
    parser.add_argument('--state', required=True)
    parser.add_argument('--scope', choices=['jurisdiction', 'region', 'mpo', 'statewide'])
    parser.add_argument('--selection', default='')
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()

    hierarchy = load_hierarchy(args.state)
    state_config = load_state_config(args.state)

    if args.list:
        list_scopes(args.state, hierarchy)
        return

    if not args.scope:
        print("ERROR: --scope required (or use --list)", file=sys.stderr)
        sys.exit(1)

    jurisdictions = resolve_scope(args.state, args.scope, args.selection, hierarchy)
    download_mode = 'statewide' if len(jurisdictions) > STATEWIDE_DOWNLOAD_THRESHOLD else 'individual'

    dot_name = state_config.get('dotName', state_config.get('dataDir', args.state.upper()))
    data_dir = f"data/{state_config.get('dataDir', dot_name)}" if state_config.get('dataDir') else "data"
    r2_prefix = state_config.get('r2Prefix', args.state)

    result = {
        'state': args.state,
        'scope': args.scope,
        'selection': args.selection,
        'download_mode': download_mode,
        'jurisdictions': jurisdictions,
        'jurisdiction_count': len(jurisdictions),
        'dot_name': dot_name,
        'data_dir': data_dir,
        'r2_prefix': r2_prefix
    }

    if args.json:
        print(json.dumps(result))
    else:
        print(f"State: {args.state}, Scope: {args.scope} ({args.selection})")
        print(f"Mode: {download_mode}, Jurisdictions ({len(jurisdictions)}): {', '.join(jurisdictions)}")


if __name__ == '__main__':
    main()
