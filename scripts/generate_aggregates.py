#!/usr/bin/env python3
"""
Generate pre-aggregated JSON summaries for State, Region, and MPO tiers.

Reads per-county CSV crash data and produces aggregate JSON files that power
the State/Region/MPO dashboard views without requiring raw CSV loading.

Output structure (designed to match R2 folder hierarchy):
  {state}/_statewide/aggregates.json       — Statewide totals, trends, YoY
  {state}/_statewide/county_summary.json   — Per-county ranking table (by road type)
  {state}/_region/{region}/aggregates.json  — Region totals + member counties
  {state}/_region/{region}/hotspots.json    — Cross-county hotspot ranking
  {state}/_mpo/{mpo}/aggregates.json        — MPO totals + member counties
  {state}/_mpo/{mpo}/hotspots.json          — Cross-county hotspot ranking

Usage:
  # Generate aggregates for Colorado
  python scripts/generate_aggregates.py --state colorado

  # Generate for Virginia
  python scripts/generate_aggregates.py --state virginia

  # Dry run (preview output paths without writing)
  python scripts/generate_aggregates.py --state colorado --dry-run

  # Specify custom data directory
  python scripts/generate_aggregates.py --state colorado --data-dir ./data/CDOT

  # Generate federal-level cross-state aggregates (combines all state aggregates)
  python scripts/generate_aggregates.py --federal

Prerequisites:
  pip install pandas
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent


def load_hierarchy(state_key):
    """Load hierarchy.json for the given state."""
    path = PROJECT_ROOT / 'states' / state_key / 'hierarchy.json'
    if not path.exists():
        print(f"[ERROR] Hierarchy file not found: {path}")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def load_state_config(state_key):
    """Load state config.json for EPDO weights and column mapping."""
    path = PROJECT_ROOT / 'states' / state_key / 'config.json'
    if not path.exists():
        print(f"[WARN] State config not found: {path}, using defaults")
        return {}
    with open(path) as f:
        return json.load(f)


def get_epdo_weights(config):
    """Extract EPDO weights from state config, with FHWA 2025 fallback."""
    defaults = {'K': 883, 'A': 94, 'B': 21, 'C': 11, 'O': 1}
    return config.get('epdoWeights', defaults)


def calc_epdo(severity_counts, weights):
    """Calculate EPDO score from severity counts and weights."""
    return sum(severity_counts.get(sev, 0) * weights.get(sev, 0) for sev in ['K', 'A', 'B', 'C', 'O'])


def find_county_csvs(data_dir, state_key, hierarchy):
    """
    Find all county CSV files in the data directory.

    Searches for files matching patterns like:
      {county_name}_all_roads.csv
      {county_name}_county_roads.csv
      {county_name}_no_interstate.csv
    """
    found = {}
    all_counties = hierarchy.get('allCounties', {})

    # Build name→fips lookup from hierarchy
    name_to_fips = {}
    for fips, name in all_counties.items():
        name_lower = name.lower().replace(' ', '_').replace("'", '')
        name_to_fips[name_lower] = fips

    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"[WARN] Data directory not found: {data_path}")
        return found

    road_types = ['all_roads', 'county_roads', 'city_roads', 'no_interstate']

    for csv_file in data_path.glob('*.csv'):
        fname = csv_file.stem.lower()
        for rt in road_types:
            if fname.endswith(f'_{rt}'):
                county_name = fname[:-len(f'_{rt}')]
                fips = name_to_fips.get(county_name)
                if fips:
                    if fips not in found:
                        found[fips] = {}
                    found[fips][rt] = str(csv_file)
                break

    return found


def parse_csv_crashes(csv_path, col_mapping):
    """Parse a crash CSV and return structured crash records."""
    crashes = []
    severity_map = col_mapping.get('SEVERITY')
    date_col = col_mapping.get('DATE')
    route_col = col_mapping.get('ROUTE')
    node_col = col_mapping.get('NODE', '')
    collision_col = col_mapping.get('COLLISION')
    weather_col = col_mapping.get('WEATHER')
    light_col = col_mapping.get('LIGHT')
    ped_col = col_mapping.get('PED')
    bike_col = col_mapping.get('BIKE')
    speed_col = col_mapping.get('SPEED')
    alcohol_col = col_mapping.get('ALCOHOL')
    x_col = col_mapping.get('X')
    y_col = col_mapping.get('Y')

    # Colorado derives severity from injury columns
    k_col = col_mapping.get('K')
    a_col = col_mapping.get('A')
    b_col = col_mapping.get('B')
    c_col = col_mapping.get('C')
    o_col = col_mapping.get('O')

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Determine severity
                sev = None
                if severity_map and severity_map in row:
                    sev = row[severity_map]
                else:
                    # Derive from injury columns (Colorado pattern)
                    for sev_label, col in [('K', k_col), ('A', a_col), ('B', b_col), ('C', c_col), ('O', o_col)]:
                        if col and col in row:
                            try:
                                if int(float(row[col] or 0)) > 0:
                                    sev = sev_label
                                    break
                            except (ValueError, TypeError):
                                pass
                    if not sev:
                        sev = 'O'

                # Extract year
                year = None
                if date_col and date_col in row and row[date_col]:
                    try:
                        date_str = row[date_col]
                        if '/' in date_str:
                            parts = date_str.split('/')
                            year = int(parts[-1]) if len(parts[-1]) == 4 else int('20' + parts[-1])
                        elif '-' in date_str:
                            year = int(date_str.split('-')[0])
                        else:
                            year = int(date_str[:4])
                    except (ValueError, IndexError):
                        pass

                crash = {
                    'severity': sev,
                    'year': year,
                    'route': row.get(route_col, '') if route_col else '',
                    'node': row.get(node_col, '') if node_col else '',
                    'collision': row.get(collision_col, '') if collision_col else '',
                    'weather': row.get(weather_col, '') if weather_col else '',
                    'light': row.get(light_col, '') if light_col else '',
                    'ped': _is_yes(row.get(ped_col, '')) if ped_col else False,
                    'bike': _is_yes(row.get(bike_col, '')) if bike_col else False,
                    'speed': _is_yes(row.get(speed_col, '')) if speed_col else False,
                    'alcohol': _is_yes(row.get(alcohol_col, '')) if alcohol_col else False,
                }
                crashes.append(crash)
    except Exception as e:
        print(f"[ERROR] Failed to parse {csv_path}: {e}")

    return crashes


def _is_yes(val):
    """Check if a field value indicates 'yes' / true."""
    if not val:
        return False
    v = str(val).strip().upper()
    return v in ('Y', 'YES', '1', 'TRUE', 'T')


def compute_county_stats(crashes, epdo_weights):
    """Compute aggregate statistics for a set of crashes."""
    stats = {
        'total': len(crashes),
        'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0,
        'epdo': 0,
        'pedCrashes': 0,
        'bikeCrashes': 0,
        'speedCrashes': 0,
        'alcoholCrashes': 0,
        'yearlyTrend': defaultdict(lambda: {'total': 0, 'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0}),
        'collisionTypes': defaultdict(int),
        'weatherDist': defaultdict(int),
        'lightDist': defaultdict(int),
        'byRoute': defaultdict(lambda: {'total': 0, 'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0, 'epdo': 0})
    }

    for c in crashes:
        sev = c['severity']
        if sev in ('K', 'A', 'B', 'C', 'O'):
            stats[sev] += 1
        if c.get('ped'):
            stats['pedCrashes'] += 1
        if c.get('bike'):
            stats['bikeCrashes'] += 1
        if c.get('speed'):
            stats['speedCrashes'] += 1
        if c.get('alcohol'):
            stats['alcoholCrashes'] += 1

        if c.get('year'):
            yt = stats['yearlyTrend'][c['year']]
            yt['total'] += 1
            if sev in ('K', 'A', 'B', 'C', 'O'):
                yt[sev] += 1

        if c.get('collision'):
            stats['collisionTypes'][c['collision']] += 1
        if c.get('weather'):
            stats['weatherDist'][c['weather']] += 1
        if c.get('light'):
            stats['lightDist'][c['light']] += 1

        route = c.get('route', '')
        if route:
            br = stats['byRoute'][route]
            br['total'] += 1
            if sev in ('K', 'A', 'B', 'C', 'O'):
                br[sev] += 1

    # Compute EPDO
    sev_counts = {s: stats[s] for s in ['K', 'A', 'B', 'C', 'O']}
    stats['epdo'] = calc_epdo(sev_counts, epdo_weights)

    # Compute per-route EPDO
    for route, rd in stats['byRoute'].items():
        rd['epdo'] = calc_epdo(rd, epdo_weights)

    # Convert yearly trend to sorted list
    years = sorted(stats['yearlyTrend'].keys())
    stats['yearlyTrend'] = [{'year': y, **stats['yearlyTrend'][y]} for y in years]

    # Compute YoY trend (last 2 years)
    if len(stats['yearlyTrend']) >= 2:
        prev = stats['yearlyTrend'][-2]['total']
        curr = stats['yearlyTrend'][-1]['total']
        stats['trend'] = round((curr - prev) / prev, 4) if prev > 0 else 0
    else:
        stats['trend'] = 0

    # Convert collision types to sorted list (top 10)
    ct = sorted(stats['collisionTypes'].items(), key=lambda x: -x[1])[:10]
    total = stats['total'] or 1
    stats['topCollisionTypes'] = [{'type': t, 'count': c, 'pct': round(c / total * 100, 1)} for t, c in ct]

    # Convert weather/light to dicts
    stats['weatherDist'] = dict(stats['weatherDist'])
    stats['lightDist'] = dict(stats['lightDist'])

    # Convert byRoute to sorted list (top 25)
    route_list = sorted(stats['byRoute'].items(), key=lambda x: -x[1]['epdo'])[:25]
    stats['topRoutes'] = [{'route': r, **d} for r, d in route_list]

    # Remove internal defaultdicts before serialization
    del stats['collisionTypes']
    del stats['byRoute']

    return stats


def compute_hotspots(crashes, epdo_weights, top_n=50):
    """Compute top-N hotspot locations ranked by EPDO."""
    by_location = defaultdict(lambda: {'total': 0, 'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0})

    for c in crashes:
        route = c.get('route', '')
        node = c.get('node', '')
        if not route:
            continue
        loc_key = f"{route}|{node}" if node else route
        loc = by_location[loc_key]
        loc['total'] += 1
        sev = c['severity']
        if sev in ('K', 'A', 'B', 'C', 'O'):
            loc[sev] += 1

    # Compute EPDO and sort
    hotspots = []
    for loc_key, stats in by_location.items():
        parts = loc_key.split('|', 1)
        route = parts[0]
        node = parts[1] if len(parts) > 1 else ''
        epdo = calc_epdo(stats, epdo_weights)
        hotspots.append({
            'route': route,
            'node': node,
            'total': stats['total'],
            'K': stats['K'], 'A': stats['A'], 'B': stats['B'],
            'C': stats['C'], 'O': stats['O'],
            'epdo': epdo,
            'ka': stats['K'] + stats['A']
        })

    hotspots.sort(key=lambda x: -x['epdo'])
    return hotspots[:top_n]


def generate_statewide_aggregates(county_data, hierarchy, epdo_weights, road_type='all_roads'):
    """Generate statewide aggregate from all counties."""
    all_crashes = []
    county_stats = {}
    all_counties = hierarchy.get('allCounties', {})

    for fips, road_types in county_data.items():
        csv_path = road_types.get(road_type)
        if not csv_path:
            continue
        col_mapping = _get_col_mapping(hierarchy)
        crashes = parse_csv_crashes(csv_path, col_mapping)
        if crashes:
            stats = compute_county_stats(crashes, epdo_weights)
            county_name = all_counties.get(fips, fips)
            stats['name'] = county_name
            stats['fips'] = fips
            county_stats[fips] = stats
            all_crashes.extend(crashes)
            print(f"  [{fips}] {county_name}: {len(crashes)} crashes, EPDO={stats['epdo']:,}")

    # Statewide totals
    statewide = compute_county_stats(all_crashes, epdo_weights) if all_crashes else {}

    return statewide, county_stats


def generate_group_aggregates(county_data, group_counties, hierarchy, epdo_weights, road_type='all_roads'):
    """Generate aggregate for a group of counties (region or MPO)."""
    group_crashes = []
    member_stats = {}
    all_counties = hierarchy.get('allCounties', {})
    col_mapping = _get_col_mapping(hierarchy)

    for fips in group_counties:
        road_types = county_data.get(fips, {})
        csv_path = road_types.get(road_type)
        if not csv_path:
            continue
        crashes = parse_csv_crashes(csv_path, col_mapping)
        if crashes:
            stats = compute_county_stats(crashes, epdo_weights)
            county_name = all_counties.get(fips, fips)
            stats['name'] = county_name
            stats['fips'] = fips
            member_stats[fips] = stats
            # Tag crashes with county for hotspot attribution
            for c in crashes:
                c['_county_fips'] = fips
                c['_county_name'] = county_name
            group_crashes.extend(crashes)

    group_totals = compute_county_stats(group_crashes, epdo_weights) if group_crashes else {}
    hotspots = compute_hotspots(group_crashes, epdo_weights) if group_crashes else []

    # Add county attribution to hotspots
    crash_by_route_node = defaultdict(lambda: defaultdict(int))
    for c in group_crashes:
        route = c.get('route', '')
        node = c.get('node', '')
        loc_key = f"{route}|{node}" if node else route
        crash_by_route_node[loc_key][c.get('_county_name', 'Unknown')] += 1

    for hs in hotspots:
        loc_key = f"{hs['route']}|{hs['node']}" if hs['node'] else hs['route']
        county_dist = crash_by_route_node.get(loc_key, {})
        if county_dist:
            top_county = max(county_dist.items(), key=lambda x: x[1])
            hs['county'] = top_county[0]
            hs['countyDist'] = dict(county_dist)

    return group_totals, member_stats, hotspots


def _get_col_mapping(hierarchy):
    """Get column mapping for the state.

    Resolves the state directory by trying the abbreviation (e.g. 'co'),
    then scanning states/ for a config.json whose abbreviation matches.
    """
    abbreviation = hierarchy.get('state', {}).get('abbreviation', '').lower()

    # Try abbreviation directly, then scan states/ dirs for a matching abbreviation
    states_dir = PROJECT_ROOT / 'states'
    candidates = [states_dir / abbreviation]
    if states_dir.is_dir():
        for d in sorted(states_dir.iterdir()):
            if d.is_dir() and d.name != abbreviation:
                candidates.append(d)

    for d in candidates:
        p = d / 'config.json'
        if p.exists():
            with open(p) as f:
                config = json.load(f)
            # Verify this config belongs to the right state
            cfg_abbr = config.get('state', {}).get('abbreviation', '').lower()
            if cfg_abbr == abbreviation or d.name == abbreviation:
                return config.get('columnMapping', {})
    return {}


def write_output(output_dir, relative_path, data, dry_run=False):
    """Write JSON output file."""
    out_path = Path(output_dir) / relative_path
    if dry_run:
        size = len(json.dumps(data))
        print(f"  [DRY-RUN] Would write: {out_path} ({size:,} bytes)")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    size = out_path.stat().st_size
    print(f"  [WRITE] {out_path} ({size:,} bytes)")


def generate_federal_aggregates(output_dir, states=None, dry_run=False):
    """
    Generate federal-level cross-state aggregates by combining statewide aggregates.

    Reads existing {state}/_statewide/aggregates.json files for all states
    and produces _federal/aggregates.json + _federal/state_summary.json.
    """
    if states is None:
        states = ['colorado', 'virginia']

    output_base = Path(output_dir)
    state_aggregates = {}

    for state_key in states:
        # Look for statewide aggregates in the output directory structure
        candidates = [
            output_base / state_key / '_statewide' / 'aggregates.json',
            output_base.parent / state_key / '_statewide' / 'aggregates.json',
            PROJECT_ROOT / 'data' / state_key / '_statewide' / 'aggregates.json',
        ]
        for path in candidates:
            if path.exists():
                with open(path) as f:
                    state_aggregates[state_key] = json.load(f)
                print(f"  Loaded statewide aggregates for {state_key}: {path}")
                break
        else:
            print(f"  [WARN] No statewide aggregates found for {state_key}")

    if not state_aggregates:
        print("[WARN] No state aggregates found — skipping federal generation")
        return

    # Combine statewide totals across all states
    fed_totals = {'total': 0, 'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0, 'epdo': 0,
                  'pedCrashes': 0, 'bikeCrashes': 0, 'speedCrashes': 0, 'alcoholCrashes': 0}
    for state_key, agg in state_aggregates.items():
        for field in fed_totals:
            fed_totals[field] += agg.get(field, 0)

    federal_agg = {
        'tier': 'federal',
        'generated': datetime.now(timezone.utc).isoformat(),
        'statesIncluded': list(state_aggregates.keys()),
        **fed_totals
    }
    write_output(str(output_base), '_federal/aggregates.json', federal_agg, dry_run)

    # State summary (ranking table)
    state_summary = {
        'tier': 'federal',
        'generated': datetime.now(timezone.utc).isoformat(),
        'states': {}
    }
    ranked = sorted(state_aggregates.items(), key=lambda x: -x[1].get('epdo', 0))
    for rank, (state_key, agg) in enumerate(ranked, 1):
        state_summary['states'][state_key] = {
            'name': agg.get('stateName', state_key),
            'rank': rank,
            'total': agg.get('total', 0),
            'K': agg.get('K', 0), 'A': agg.get('A', 0),
            'B': agg.get('B', 0), 'C': agg.get('C', 0), 'O': agg.get('O', 0),
            'epdo': agg.get('epdo', 0),
            'pedCrashes': agg.get('pedCrashes', 0),
            'bikeCrashes': agg.get('bikeCrashes', 0),
            'countiesIncluded': agg.get('countiesIncluded', 0)
        }
    write_output(str(output_base), '_federal/state_summary.json', state_summary, dry_run)

    print(f"  Federal aggregates: {len(state_aggregates)} states, {fed_totals['total']:,} total crashes")


def main():
    parser = argparse.ArgumentParser(description='Generate aggregate JSONs for CRASH LENS multi-tier views')
    parser.add_argument('--state', help='State key (colorado, virginia). Required unless --federal is used.')
    parser.add_argument('--federal', action='store_true',
                        help='Generate federal-level cross-state aggregates from existing statewide aggregates')
    parser.add_argument('--data-dir', help='Directory containing county CSVs')
    parser.add_argument('--output-dir', help='Output directory (default: data/{STATE} or data/ for --federal)')
    parser.add_argument('--road-type', default='all_roads', choices=['all_roads', 'county_roads', 'no_interstate'],
                        help='Road type variant to aggregate (default: all_roads)')
    parser.add_argument('--dry-run', action='store_true', help='Preview output paths without writing')
    args = parser.parse_args()

    # Federal mode: generate cross-state aggregates and exit
    if args.federal:
        output_dir = args.output_dir or str(PROJECT_ROOT / 'data')
        print(f"\n{'='*60}")
        print(f"CRASH LENS — Federal Aggregate Generator")
        print(f"{'='*60}\n")
        generate_federal_aggregates(output_dir, dry_run=args.dry_run)
        print(f"\n{'='*60}")
        print(f"Federal aggregate generation {'(DRY RUN) ' if args.dry_run else ''}complete!")
        print(f"{'='*60}\n")
        return

    if not args.state:
        parser.error("--state is required unless --federal is used")

    state_key = args.state.lower()
    print(f"\n{'='*60}")
    print(f"CRASH LENS — Aggregate Generator")
    print(f"State: {state_key}")
    print(f"Road type: {args.road_type}")
    print(f"{'='*60}\n")

    # Load hierarchy
    hierarchy = load_hierarchy(state_key)
    state_info = hierarchy.get('state', {})
    print(f"State: {state_info.get('name', state_key)} ({state_info.get('abbreviation', '?')})")
    print(f"Regions: {len(hierarchy.get('regions', {}))}")
    print(f"TPRs/MPOs: {len(hierarchy.get('tprs', {}))}")
    print(f"Counties: {len(hierarchy.get('allCounties', {}))}")

    # Load state config for EPDO weights
    config = load_state_config(state_key)
    epdo_weights = get_epdo_weights(config)
    print(f"EPDO Weights: K={epdo_weights['K']}, A={epdo_weights['A']}, B={epdo_weights['B']}, C={epdo_weights['C']}, O={epdo_weights['O']}")

    # Determine data directory
    data_dir = args.data_dir
    if not data_dir:
        # Auto-detect
        candidates = [
            PROJECT_ROOT / 'data' / 'CDOT' if state_key == 'colorado' else PROJECT_ROOT / 'data',
            PROJECT_ROOT / 'data',
        ]
        for d in candidates:
            if d.exists():
                data_dir = str(d)
                break
    print(f"Data directory: {data_dir}")

    # Output directory
    output_dir = args.output_dir or str(PROJECT_ROOT / 'data' / state_key)

    # Find county CSVs
    county_data = find_county_csvs(data_dir, state_key, hierarchy)
    print(f"\nFound CSV data for {len(county_data)} counties")

    if not county_data:
        print("[WARN] No county CSVs found. Ensure data directory contains files like 'douglas_all_roads.csv'")
        return

    # ---- Generate Statewide Aggregates ----
    print(f"\n--- Statewide Aggregates ({args.road_type}) ---")
    statewide, county_stats = generate_statewide_aggregates(county_data, hierarchy, epdo_weights, args.road_type)

    if statewide:
        # aggregates.json
        agg = {
            'state': state_key,
            'stateName': state_info.get('name', state_key),
            'period': f"{min(y['year'] for y in statewide.get('yearlyTrend', [{'year': 0}]))}-{max(y['year'] for y in statewide.get('yearlyTrend', [{'year': 0}]))}",
            'generated': datetime.now(timezone.utc).isoformat(),
            'roadType': args.road_type,
            'epdoWeights': epdo_weights,
            'epdoSource': config.get('epdoSource', 'HSM Standard 2010'),
            'countiesIncluded': len(county_stats),
            **statewide
        }
        write_output(output_dir, '_statewide/aggregates.json', agg, args.dry_run)

        # county_summary.json
        summary = {
            'state': state_key,
            'generated': datetime.now(timezone.utc).isoformat(),
            'roadType': args.road_type,
            'epdoWeights': epdo_weights,
            'statewide': {k: statewide[k] for k in ['total', 'K', 'A', 'B', 'C', 'O', 'epdo', 'trend',
                                                      'pedCrashes', 'bikeCrashes'] if k in statewide},
            'counties': {}
        }
        # Rank counties by EPDO
        ranked = sorted(county_stats.items(), key=lambda x: -x[1].get('epdo', 0))
        for rank, (fips, cs) in enumerate(ranked, 1):
            summary['counties'][fips] = {
                'name': cs['name'],
                'fips': fips,
                'rank': rank,
                'total': cs['total'],
                'K': cs['K'], 'A': cs['A'], 'B': cs['B'], 'C': cs['C'], 'O': cs['O'],
                'epdo': cs['epdo'],
                'trend': cs.get('trend', 0),
                'pedCrashes': cs.get('pedCrashes', 0),
                'bikeCrashes': cs.get('bikeCrashes', 0),
                'topCollisionTypes': cs.get('topCollisionTypes', [])[:5]
            }
        write_output(output_dir, '_statewide/county_summary.json', summary, args.dry_run)

        # Monthly snapshot
        snapshot_name = datetime.now().strftime('%Y-%m') + '_aggregates.json'
        write_output(output_dir, f'_statewide/snapshots/{snapshot_name}', agg, args.dry_run)

    # ---- Generate Region Aggregates ----
    regions = hierarchy.get('regions', {})
    if regions:
        print(f"\n--- Region Aggregates ({len(regions)} regions) ---")
        for region_id, region in regions.items():
            print(f"\n  Region: {region.get('shortName', region_id)}")
            counties = region.get('counties', [])
            group_totals, member_stats, hotspots = generate_group_aggregates(
                county_data, counties, hierarchy, epdo_weights, args.road_type)

            if group_totals:
                region_agg = {
                    'regionId': region_id,
                    'regionName': region.get('name', region_id),
                    'state': state_key,
                    'generated': datetime.now(timezone.utc).isoformat(),
                    'roadType': args.road_type,
                    'epdoWeights': epdo_weights,
                    'memberCounties': len(member_stats),
                    **group_totals,
                    'counties': {fips: {
                        'name': ms['name'], 'total': ms['total'],
                        'K': ms['K'], 'A': ms['A'], 'epdo': ms['epdo'], 'trend': ms.get('trend', 0)
                    } for fips, ms in member_stats.items()}
                }
                write_output(output_dir, f'_region/{region_id}/aggregates.json', region_agg, args.dry_run)
                write_output(output_dir, f'_region/{region_id}/hotspots.json',
                             {'regionId': region_id, 'generated': datetime.now(timezone.utc).isoformat(),
                              'hotspots': hotspots}, args.dry_run)

    # ---- Generate MPO/TPR Aggregates ----
    tprs = hierarchy.get('tprs', {})
    mpos = {k: v for k, v in tprs.items() if v.get('type') == 'mpo'}
    if mpos:
        print(f"\n--- MPO Aggregates ({len(mpos)} MPOs) ---")
        for mpo_id, mpo in mpos.items():
            print(f"\n  MPO: {mpo.get('shortName', mpo_id)}")
            counties = mpo.get('counties', [])
            group_totals, member_stats, hotspots = generate_group_aggregates(
                county_data, counties, hierarchy, epdo_weights, args.road_type)

            if group_totals:
                mpo_agg = {
                    'mpoId': mpo_id,
                    'mpoName': mpo.get('name', mpo_id),
                    'btsAcronym': mpo.get('btsAcronym'),
                    'state': state_key,
                    'generated': datetime.now(timezone.utc).isoformat(),
                    'roadType': args.road_type,
                    'epdoWeights': epdo_weights,
                    'memberCounties': len(member_stats),
                    **group_totals,
                    'counties': {fips: {
                        'name': ms['name'], 'total': ms['total'],
                        'K': ms['K'], 'A': ms['A'], 'epdo': ms['epdo'], 'trend': ms.get('trend', 0)
                    } for fips, ms in member_stats.items()}
                }
                write_output(output_dir, f'_mpo/{mpo_id}/aggregates.json', mpo_agg, args.dry_run)
                write_output(output_dir, f'_mpo/{mpo_id}/hotspots.json',
                             {'mpoId': mpo_id, 'generated': datetime.now(timezone.utc).isoformat(),
                              'hotspots': hotspots}, args.dry_run)

    print(f"\n{'='*60}")
    print(f"Aggregate generation {'(DRY RUN) ' if args.dry_run else ''}complete!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
