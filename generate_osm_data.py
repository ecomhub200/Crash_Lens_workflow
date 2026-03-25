#!/usr/bin/env python3
"""
generate_osm_data.py — CrashLens Consolidated OSM Data Generator
================================================================
Downloads ALL OSM data needed for crash enrichment in one run:
  1. Road network → {abbr}_roads.parquet.gz (FC, RTE Name, Ownership, speed, surface, lighting, sidewalk, bike lanes)
  2. Intersections → {abbr}_intersections.parquet.gz (node degree, Intersection Name)
  3. Points of Interest → {abbr}_pois.parquet.gz (bars, schools, signals, hospitals, crosswalks)

SETUP:
    pip install osmnx geopandas pandas pyarrow scipy boto3

USAGE:
    python generate_osm_data.py --state de               # Delaware (~3 min)
    python generate_osm_data.py --state de va md co       # Multiple
    python generate_osm_data.py --all                     # All 51 states
    python generate_osm_data.py --state de --local-only   # No R2 upload
    python generate_osm_data.py --state de --roads-only   # Skip POIs
    python generate_osm_data.py --state de --pois-only    # Skip roads
    python generate_osm_data.py --state de --force        # Regenerate all

OUTPUT PER STATE (3 files):
    cache/{abbr}_roads.parquet.gz          Roads + extra tags (surface, lit, sidewalk, cycleway)
    cache/{abbr}_intersections.parquet.gz  Intersection nodes with degree
    cache/{abbr}_pois.parquet.gz           Bars, schools, signals, hospitals, crosswalks, etc.

ROAD PARQUET COLUMNS (crash_enricher-compatible flat format):
    u_node, v_node, u_lat, u_lon, v_lat, v_lon, mid_lat, mid_lon,
    highway, name, ref, oneway, lanes, maxspeed, length_m,
    bridge, tunnel, surface, lit, sidewalk, cycleway, divider

INTERSECTION PARQUET COLUMNS:
    node_id, lat, lon, degree

POI PARQUET COLUMNS:
    osm_id, lat, lon, category, subcategory, name
"""

import argparse
import gc
import gzip
import os
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY (50 states + DC)
# ═══════════════════════════════════════════════════════════════

ALL_STATES = [
    {"name": "Alabama", "abbreviation": "al", "r2_prefix": "alabama"},
    {"name": "Alaska", "abbreviation": "ak", "r2_prefix": "alaska"},
    {"name": "Arizona", "abbreviation": "az", "r2_prefix": "arizona"},
    {"name": "Arkansas", "abbreviation": "ar", "r2_prefix": "arkansas"},
    {"name": "California", "abbreviation": "ca", "r2_prefix": "california"},
    {"name": "Colorado", "abbreviation": "co", "r2_prefix": "colorado"},
    {"name": "Connecticut", "abbreviation": "ct", "r2_prefix": "connecticut"},
    {"name": "Delaware", "abbreviation": "de", "r2_prefix": "delaware"},
    {"name": "District of Columbia", "abbreviation": "dc", "r2_prefix": "district_of_columbia"},
    {"name": "Florida", "abbreviation": "fl", "r2_prefix": "florida"},
    {"name": "Georgia", "abbreviation": "ga", "r2_prefix": "georgia"},
    {"name": "Hawaii", "abbreviation": "hi", "r2_prefix": "hawaii"},
    {"name": "Idaho", "abbreviation": "id", "r2_prefix": "idaho"},
    {"name": "Illinois", "abbreviation": "il", "r2_prefix": "illinois"},
    {"name": "Indiana", "abbreviation": "in", "r2_prefix": "indiana"},
    {"name": "Iowa", "abbreviation": "ia", "r2_prefix": "iowa"},
    {"name": "Kansas", "abbreviation": "ks", "r2_prefix": "kansas"},
    {"name": "Kentucky", "abbreviation": "ky", "r2_prefix": "kentucky"},
    {"name": "Louisiana", "abbreviation": "la", "r2_prefix": "louisiana"},
    {"name": "Maine", "abbreviation": "me", "r2_prefix": "maine"},
    {"name": "Maryland", "abbreviation": "md", "r2_prefix": "maryland"},
    {"name": "Massachusetts", "abbreviation": "ma", "r2_prefix": "massachusetts"},
    {"name": "Michigan", "abbreviation": "mi", "r2_prefix": "michigan"},
    {"name": "Minnesota", "abbreviation": "mn", "r2_prefix": "minnesota"},
    {"name": "Mississippi", "abbreviation": "ms", "r2_prefix": "mississippi"},
    {"name": "Missouri", "abbreviation": "mo", "r2_prefix": "missouri"},
    {"name": "Montana", "abbreviation": "mt", "r2_prefix": "montana"},
    {"name": "Nebraska", "abbreviation": "ne", "r2_prefix": "nebraska"},
    {"name": "Nevada", "abbreviation": "nv", "r2_prefix": "nevada"},
    {"name": "New Hampshire", "abbreviation": "nh", "r2_prefix": "new_hampshire"},
    {"name": "New Jersey", "abbreviation": "nj", "r2_prefix": "new_jersey"},
    {"name": "New Mexico", "abbreviation": "nm", "r2_prefix": "new_mexico"},
    {"name": "New York", "abbreviation": "ny", "r2_prefix": "new_york"},
    {"name": "North Carolina", "abbreviation": "nc", "r2_prefix": "north_carolina"},
    {"name": "North Dakota", "abbreviation": "nd", "r2_prefix": "north_dakota"},
    {"name": "Ohio", "abbreviation": "oh", "r2_prefix": "ohio"},
    {"name": "Oklahoma", "abbreviation": "ok", "r2_prefix": "oklahoma"},
    {"name": "Oregon", "abbreviation": "or", "r2_prefix": "oregon"},
    {"name": "Pennsylvania", "abbreviation": "pa", "r2_prefix": "pennsylvania"},
    {"name": "Rhode Island", "abbreviation": "ri", "r2_prefix": "rhode_island"},
    {"name": "South Carolina", "abbreviation": "sc", "r2_prefix": "south_carolina"},
    {"name": "South Dakota", "abbreviation": "sd", "r2_prefix": "south_dakota"},
    {"name": "Tennessee", "abbreviation": "tn", "r2_prefix": "tennessee"},
    {"name": "Texas", "abbreviation": "tx", "r2_prefix": "texas"},
    {"name": "Utah", "abbreviation": "ut", "r2_prefix": "utah"},
    {"name": "Vermont", "abbreviation": "vt", "r2_prefix": "vermont"},
    {"name": "Virginia", "abbreviation": "va", "r2_prefix": "virginia"},
    {"name": "Washington", "abbreviation": "wa", "r2_prefix": "washington"},
    {"name": "West Virginia", "abbreviation": "wv", "r2_prefix": "west_virginia"},
    {"name": "Wisconsin", "abbreviation": "wi", "r2_prefix": "wisconsin"},
    {"name": "Wyoming", "abbreviation": "wy", "r2_prefix": "wyoming"},
]

ABBR_LOOKUP = {s["abbreviation"]: s for s in ALL_STATES}

# ═══════════════════════════════════════════════════════════════
#  POI CATEGORIES — OSM tags to download
# ═══════════════════════════════════════════════════════════════

POI_TAGS = [
    # (category,     osm_key,   osm_values)
    ("bar",         "amenity",  ["bar", "pub", "nightclub", "biergarten"]),
    ("school",      "amenity",  ["school", "kindergarten"]),
    ("college",     "amenity",  ["college", "university"]),
    ("hospital",    "amenity",  ["hospital"]),
    ("clinic",      "amenity",  ["clinic"]),
    ("fuel",        "amenity",  ["fuel"]),
    ("parking",     "amenity",  ["parking"]),
    ("restaurant",  "amenity",  ["restaurant", "fast_food"]),
    ("signal",      "highway",  ["traffic_signals"]),
    ("stop_sign",   "highway",  ["stop"]),
    ("crossing",    "highway",  ["crossing"]),
    ("rest_area",   "highway",  ["rest_area", "services"]),
    ("rail_xing",   "railway",  ["level_crossing"]),
]


# ═══════════════════════════════════════════════════════════════
#  ROAD + INTERSECTION GENERATION
# ═══════════════════════════════════════════════════════════════

def convert_to_enricher_format(G):
    """
    Convert osmnx graph to crash_enricher-compatible flat DataFrames.

    Includes NEW tags: surface, lit, sidewalk, cycleway, divider
    These enable filling: Roadway Surface Type, Roadway Alignment,
    Has_Street_Lighting, Has_Sidewalk, Has_Bike_Lane
    """
    import osmnx as ox

    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)

    def _clean(val):
        """Convert OSM list values to semicolon-joined strings."""
        if isinstance(val, (list, tuple)):
            return '; '.join(str(x) for x in val)
        return str(val) if val is not None and str(val) != 'nan' else ''

    # ── Roads (flat format with extra tags) ──
    road_data = []
    for idx, row in edges_gdf.iterrows():
        u, v = idx[0], idx[1]
        coords = list(row.geometry.coords)
        u_lat, u_lon = coords[0][1], coords[0][0]
        v_lat, v_lon = coords[-1][1], coords[-1][0]

        # Compute curvature from FULL geometry (all intermediate coordinates)
        # Method: sum of angular deflections between consecutive segments
        # More accurate than endpoint-only for S-curves and switchbacks
        import math
        road_length = float(row.get('length', 0) or 0)

        if len(coords) >= 3 and road_length > 10:
            # Sum of angular deflections between consecutive segments
            total_deflection = 0
            for j in range(1, len(coords) - 1):
                x0, y0 = coords[j-1]
                x1, y1 = coords[j]
                x2, y2 = coords[j+1]
                a1 = math.atan2(y1 - y0, x1 - x0)
                a2 = math.atan2(y2 - y1, x2 - x1)
                delta = abs(a2 - a1)
                if delta > math.pi:
                    delta = 2 * math.pi - delta
                total_deflection += delta

            # Also compute straight-line ratio as secondary metric
            straight_dist = math.sqrt(
                ((coords[0][1] - coords[-1][1]) * 111000)**2 +
                ((coords[0][0] - coords[-1][0]) * 111000 * math.cos(math.radians(coords[0][1])))**2
            )
            length_ratio = road_length / max(straight_dist, 1) if straight_dist > 5 else 1.0

            # Combined: max of deflection-based and ratio-based
            defl_curvature = 1.0 + total_deflection
            curvature = round(max(length_ratio, defl_curvature), 3)
        else:
            # Short segment or only 2 coords — use simple ratio
            straight_dist = math.sqrt((u_lat - v_lat)**2 + (u_lon - v_lon)**2) * 111000
            curvature = round(road_length / max(straight_dist, 1), 3) if straight_dist > 5 else 1.0

        road_data.append({
            # Core columns (crash_enricher Tier 2)
            'u_node': u, 'v_node': v,
            'u_lat': u_lat, 'u_lon': u_lon,
            'v_lat': v_lat, 'v_lon': v_lon,
            'mid_lat': (u_lat + v_lat) / 2,
            'mid_lon': (u_lon + v_lon) / 2,
            'highway':   _clean(row.get('highway', '')),
            'name':      _clean(row.get('name', '')),
            'ref':       _clean(row.get('ref', '')),
            'oneway':    _clean(row.get('oneway', '')),
            'lanes':     _clean(row.get('lanes', '')),
            'maxspeed':  _clean(row.get('maxspeed', '')),
            'length_m':  road_length,
            'bridge':    _clean(row.get('bridge', '')),
            'tunnel':    _clean(row.get('tunnel', '')),
            # NEW extra tags
            'surface':   _clean(row.get('surface', '')),     # asphalt, concrete, gravel, unpaved
            'lit':       _clean(row.get('lit', '')),          # yes, no
            'sidewalk':  _clean(row.get('sidewalk', '')),     # yes, both, left, right, no
            'cycleway':  _clean(row.get('cycleway', '')),     # lane, track, shared_lane, no
            'divider':   _clean(row.get('divider', '')),      # yes, no (median divider)
            'curvature': curvature,                           # computed: 1.0=straight, >1.15=curve
        })

    road_df = pd.DataFrame(road_data)

    # ── Intersections (flat format) ──
    degrees = dict(G.degree())
    int_data = []
    for node_id, deg in degrees.items():
        if deg >= 3:
            n = nodes_gdf.loc[node_id]
            int_data.append({
                'node_id': node_id,
                'lat': n.geometry.y,
                'lon': n.geometry.x,
                'degree': deg,
            })

    int_df = pd.DataFrame(int_data) if int_data else pd.DataFrame(
        columns=['node_id', 'lat', 'lon', 'degree']
    )

    return road_df, int_df


# ═══════════════════════════════════════════════════════════════
#  POI GENERATION
# ═══════════════════════════════════════════════════════════════

def download_pois(state_name):
    """Download all POI categories for a state from OSM."""
    import osmnx as ox

    # Ensure large-area settings (in case --pois-only skipped road config)
    ox.settings.max_query_area_size = 50_000_000_000
    ox.settings.overpass_rate_limit = False
    ox.settings.timeout = 600

    if state_name == 'District of Columbia':
        place = 'Washington, DC, United States'
    elif state_name == 'Georgia':
        place = 'State of Georgia, United States'
    elif state_name == 'Washington':
        place = 'State of Washington, United States'
    else:
        place = f'{state_name}, United States'

    all_pois = []

    for category, osm_key, osm_values in POI_TAGS:
        try:
            tag_dict = {osm_key: osm_values}
            gdf = ox.features_from_place(place, tags=tag_dict)

            count = 0
            for idx, row in gdf.iterrows():
                try:
                    if row.geometry.geom_type == 'Point':
                        lat, lon = row.geometry.y, row.geometry.x
                    else:
                        centroid = row.geometry.centroid
                        lat, lon = centroid.y, centroid.x
                except Exception:
                    continue

                name = str(row.get("name", "") or "").strip()
                if name == "nan":
                    name = ""

                subcategory = str(row.get(osm_key, "")).strip()
                if subcategory == "nan":
                    subcategory = osm_values[0]

                all_pois.append({
                    "osm_id": idx[1] if isinstance(idx, tuple) else idx,
                    "lat": round(lat, 7),
                    "lon": round(lon, 7),
                    "category": category,
                    "subcategory": subcategory,
                    "name": name[:100],
                })
                count += 1

            print(f"      {category:12s}  {count:>5,} POIs")

        except Exception as e:
            print(f"      {category:12s}  ERROR: {str(e)[:60]}")

    return pd.DataFrame(all_pois)


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


def r2_exists(s3, bucket, prefix, abbr, file_type):
    if not s3:
        return False
    try:
        s3.head_object(Bucket=bucket, Key=f'{prefix}/cache/{abbr}_{file_type}.parquet.gz')
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


def get_place_name(state_name):
    if state_name == 'District of Columbia':
        return 'Washington, DC, United States'
    elif state_name == 'Georgia':
        return 'State of Georgia, United States'
    elif state_name == 'Washington':
        return 'State of Washington, United States'
    return f'{state_name}, United States'


# ═══════════════════════════════════════════════════════════════
#  MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════

def process_state(state_info, cache_dir, s3, bucket, force=False,
                  local_only=False, roads_only=False, pois_only=False):
    """Process one state: download roads + intersections + POIs from OSM."""
    name = state_info['name']
    abbr = state_info['abbreviation']
    prefix = state_info['r2_prefix']

    roads_gz = cache_dir / f'{abbr}_roads.parquet.gz'
    ints_gz = cache_dir / f'{abbr}_intersections.parquet.gz'
    pois_gz = cache_dir / f'{abbr}_pois.parquet.gz'

    result = {'roads': 'skipped', 'pois': 'skipped'}

    # ── ROADS + INTERSECTIONS ──
    if not pois_only:
        need_roads = force or not (
            (not local_only and r2_exists(s3, bucket, prefix, abbr, 'roads'))
            or (local_only and roads_gz.exists())
        )

        if not need_roads:
            print(f"  [skip] {name} ({abbr}) — roads already cached")
        else:
            print(f"\n  [roads] {name} ({abbr}) — downloading from OSM...")
            t0 = time.time()

            try:
                import osmnx as ox
                place = get_place_name(name)

                # ── Configure osmnx for large states ──
                # Default max_query_area_size = 2.5B m² (~50km × 50km).
                # Alabama alone is 60x that = 150B m². Texas is ~280x.
                # Bump to 50B m² so even TX only subdivides into ~15 sub-queries
                # instead of 280, massively reducing Overpass round-trips.
                ox.settings.max_query_area_size = 50_000_000_000  # 50B m²
                ox.settings.overpass_rate_limit = False  # single-runner, no throttle needed
                ox.settings.timeout = 600   # 10 min per Overpass sub-query (default 180)
                ox.settings.overpass_memory = 2_147_483_648  # 2GB Overpass memory allocation

                # Ensure osmnx retains extra road tags we need for enrichment
                extra_tags = ['surface', 'lit', 'sidewalk', 'cycleway', 'divider']
                for tag in extra_tags:
                    if tag not in ox.settings.useful_tags_way:
                        ox.settings.useful_tags_way.append(tag)

                try:
                    G = ox.graph_from_place(place, network_type='drive', simplify=True)
                except Exception:
                    print(f"      Retrying with 'State of {name}'...")
                    G = ox.graph_from_place(f'State of {name}, United States',
                                            network_type='drive', simplify=True)

                dl_sec = time.time() - t0
                print(f"      Downloaded: {G.number_of_nodes():,} nodes, "
                      f"{G.number_of_edges():,} edges ({dl_sec:.0f}s)")

                print(f"      Converting to enricher format (with extra tags)...")
                road_df, int_df = convert_to_enricher_format(G)

                # Report extra tag coverage
                for tag in ['surface', 'lit', 'sidewalk', 'cycleway', 'maxspeed']:
                    filled = (road_df[tag].str.strip() != '').sum() if tag in road_df.columns else 0
                    pct = filled / len(road_df) * 100 if len(road_df) > 0 else 0
                    print(f"      {tag:12s}: {filled:>8,} / {len(road_df):,} ({pct:.0f}%)")

                # Save + gzip roads
                roads_pq = cache_dir / f'{abbr}_roads.parquet'
                road_df.to_parquet(roads_pq, index=False)
                raw_r, gz_r = gzip_file(roads_pq, roads_gz)
                roads_pq.unlink(missing_ok=True)

                # Save + gzip intersections
                ints_pq = cache_dir / f'{abbr}_intersections.parquet'
                int_df.to_parquet(ints_pq, index=False)
                raw_i, gz_i = gzip_file(ints_pq, ints_gz)
                ints_pq.unlink(missing_ok=True)

                elapsed = time.time() - t0
                print(f"      Roads: {len(road_df):,} segments ({gz_r:.1f} MB gz)")
                print(f"      Intersections: {len(int_df):,} nodes ({gz_i:.1f} MB gz)")
                print(f"      Total: {elapsed:.0f}s")

                # Upload to R2
                if not local_only and s3:
                    r2_upload(s3, roads_gz, bucket, f'{prefix}/cache/{abbr}_roads.parquet.gz')
                    r2_upload(s3, ints_gz, bucket, f'{prefix}/cache/{abbr}_intersections.parquet.gz')
                    print(f"      -> uploaded to R2")

                result['roads'] = 'completed'
                del G, road_df, int_df
                gc.collect()

            except Exception as e:
                print(f"  [error] {name} roads failed: {e}")
                result['roads'] = 'failed'

    # ── POIs ──
    if not roads_only:
        need_pois = force or not (
            (not local_only and r2_exists(s3, bucket, prefix, abbr, 'pois'))
            or (local_only and pois_gz.exists())
        )

        if not need_pois:
            print(f"  [skip] {name} ({abbr}) — POIs already cached")
        else:
            print(f"\n  [pois] {name} ({abbr}) — downloading POIs from OSM...")
            t0 = time.time()

            try:
                poi_df = download_pois(name)

                if len(poi_df) > 0:
                    pois_pq = cache_dir / f'{abbr}_pois.parquet'
                    poi_df.to_parquet(pois_pq, index=False)
                    raw_p, gz_p = gzip_file(pois_pq, pois_gz)
                    pois_pq.unlink(missing_ok=True)

                    elapsed = time.time() - t0
                    print(f"      Total: {len(poi_df):,} POIs ({gz_p:.1f} MB gz, {elapsed:.0f}s)")

                    if not local_only and s3:
                        r2_upload(s3, pois_gz, bucket, f'{prefix}/cache/{abbr}_pois.parquet.gz')
                        print(f"      -> uploaded to R2")

                    result['pois'] = 'completed'

                    # Save readable CSV locally
                    poi_df.to_csv(cache_dir / f'{abbr}_pois_readable.csv', index=False)
                    del poi_df
                else:
                    print(f"  [warn] {name} — no POIs found")
                    result['pois'] = 'failed'

            except Exception as e:
                print(f"  [error] {name} POIs failed: {e}")
                result['pois'] = 'failed'

        gc.collect()

    # Summary line
    status = 'completed' if any(v == 'completed' for v in result.values()) else (
        'skipped' if all(v == 'skipped' for v in result.values()) else 'failed'
    )
    print(f"  [{status}] {name} — roads: {result['roads']}, pois: {result['pois']}")
    return status


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CrashLens OSM Data Generator — roads + intersections + POIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
    python generate_osm_data.py --state de               # Delaware (roads+POIs, ~3 min)
    python generate_osm_data.py --state de va md          # Multiple states
    python generate_osm_data.py --all                     # All 51 states
    python generate_osm_data.py --state de --roads-only   # Roads only, skip POIs
    python generate_osm_data.py --state de --pois-only    # POIs only, skip roads
    python generate_osm_data.py --all --local-only        # No R2 upload
    python generate_osm_data.py --state tx --force        # Regenerate even if cached

DATA GENERATED PER STATE:
    {abbr}_roads.parquet.gz          Road segments + speed, surface, lighting, sidewalk, bike
    {abbr}_intersections.parquet.gz  Intersection nodes
    {abbr}_pois.parquet.gz           Bars, schools, signals, hospitals, crosswalks

R2 CREDENTIALS (env vars):
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
        """,
    )
    parser.add_argument('--state', nargs='+', help='State abbreviation(s)')
    parser.add_argument('--all', action='store_true', help='All 51 states')
    parser.add_argument('--local-only', action='store_true', help='Skip R2 upload')
    parser.add_argument('--force', action='store_true', help='Regenerate if exists')
    parser.add_argument('--roads-only', action='store_true', help='Roads + intersections only')
    parser.add_argument('--pois-only', action='store_true', help='POIs only')
    parser.add_argument('--cache-dir', default='cache', help='Cache directory')
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        print("\nSpecify --state or --all")
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

    mode = "roads+pois"
    if args.roads_only: mode = "roads only"
    if args.pois_only: mode = "pois only"

    print(f"\n{'=' * 60}")
    print(f"  CrashLens OSM Data Generator")
    print(f"  States: {len(states)} | Mode: {mode} | R2: {'yes' if s3 else 'local'}")
    print(f"{'=' * 60}")

    results = {'completed': 0, 'skipped': 0, 'failed': 0}
    t_start = time.time()

    for i, state in enumerate(states, 1):
        print(f"\n  [{i}/{len(states)}]", end="")
        try:
            result = process_state(
                state, cache_dir, s3, bucket,
                force=args.force, local_only=args.local_only or not s3,
                roads_only=args.roads_only, pois_only=args.pois_only,
            )
        except Exception as e:
            print(f"  ERROR: {state['name']} — {e}")
            result = 'failed'
        results[result] += 1
        time.sleep(1)

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE in {elapsed / 60:.1f} min")
    print(f"  Completed: {results['completed']}")
    print(f"  Skipped:   {results['skipped']}")
    print(f"  Failed:    {results['failed']}")

    gz_files = sorted(cache_dir.glob('*.parquet.gz'))
    if gz_files:
        total_mb = sum(f.stat().st_size for f in gz_files) / 1048576
        print(f"  Local cache: {len(gz_files)} files, {total_mb:.0f} MB")

    print(f"{'=' * 60}\n")


if __name__ == '__main__':
    main()
