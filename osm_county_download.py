#!/usr/bin/env python3
"""
osm_county_download.py — County-by-county OSM downloader (parallel support)
============================================================================
Same output as generate_osm_data.py but downloads one county at a time
to avoid OOM kills on states like OH, PA, MI, FL, IL, TX, VA.

Produces identical files:
    cache/{abbr}_roads.parquet.gz
    cache/{abbr}_intersections.parquet.gz
    cache/{abbr}_pois.parquet.gz

USAGE:
    python osm_county_download.py --state oh                    # 2 workers default
    python osm_county_download.py --state oh --parallel 3       # 3 workers
    python osm_county_download.py --state oh --roads-only
    python osm_county_download.py --state oh --pois-only
    python osm_county_download.py --state oh --local-only       # skip R2 upload

REQUIRES: us_counties.json in states/geography/ or current directory (auto-downloads from R2)
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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from pathlib import Path

import pandas as pd

COUNTY_TIMEOUT = 6 * 3600  # 6 hours per county max
_print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY
# ═══════════════════════════════════════════════════════════════

STATES = {
    "al": ("Alabama",                "01", "alabama"),
    "ak": ("Alaska",                 "02", "alaska"),
    "az": ("Arizona",                "04", "arizona"),
    "ar": ("Arkansas",               "05", "arkansas"),
    "ca": ("California",             "06", "california"),
    "co": ("Colorado",               "08", "colorado"),
    "ct": ("Connecticut",            "09", "connecticut"),
    "de": ("Delaware",               "10", "delaware"),
    "dc": ("District of Columbia",   "11", "district_of_columbia"),
    "fl": ("Florida",                "12", "florida"),
    "ga": ("Georgia",                "13", "georgia"),
    "hi": ("Hawaii",                 "15", "hawaii"),
    "id": ("Idaho",                  "16", "idaho"),
    "il": ("Illinois",               "17", "illinois"),
    "in": ("Indiana",                "18", "indiana"),
    "ia": ("Iowa",                   "19", "iowa"),
    "ks": ("Kansas",                 "20", "kansas"),
    "ky": ("Kentucky",               "21", "kentucky"),
    "la": ("Louisiana",              "22", "louisiana"),
    "me": ("Maine",                  "23", "maine"),
    "md": ("Maryland",               "24", "maryland"),
    "ma": ("Massachusetts",          "25", "massachusetts"),
    "mi": ("Michigan",               "26", "michigan"),
    "mn": ("Minnesota",              "27", "minnesota"),
    "ms": ("Mississippi",            "28", "mississippi"),
    "mo": ("Missouri",               "29", "missouri"),
    "mt": ("Montana",                "30", "montana"),
    "ne": ("Nebraska",               "31", "nebraska"),
    "nv": ("Nevada",                 "32", "nevada"),
    "nh": ("New Hampshire",          "33", "new_hampshire"),
    "nj": ("New Jersey",             "34", "new_jersey"),
    "nm": ("New Mexico",             "35", "new_mexico"),
    "ny": ("New York",               "36", "new_york"),
    "nc": ("North Carolina",         "37", "north_carolina"),
    "nd": ("North Dakota",           "38", "north_dakota"),
    "oh": ("Ohio",                   "39", "ohio"),
    "ok": ("Oklahoma",               "40", "oklahoma"),
    "or": ("Oregon",                 "41", "oregon"),
    "pa": ("Pennsylvania",           "42", "pennsylvania"),
    "ri": ("Rhode Island",           "44", "rhode_island"),
    "sc": ("South Carolina",         "45", "south_carolina"),
    "sd": ("South Dakota",           "46", "south_dakota"),
    "tn": ("Tennessee",              "47", "tennessee"),
    "tx": ("Texas",                  "48", "texas"),
    "ut": ("Utah",                   "49", "utah"),
    "vt": ("Vermont",                "50", "vermont"),
    "va": ("Virginia",               "51", "virginia"),
    "wa": ("Washington",             "53", "washington"),
    "wv": ("West Virginia",          "54", "west_virginia"),
    "wi": ("Wisconsin",              "55", "wisconsin"),
    "wy": ("Wyoming",                "56", "wyoming"),
}

# ═══════════════════════════════════════════════════════════════
#  COUNTY LOADER
# ═══════════════════════════════════════════════════════════════

def load_counties(state_fips):
    """Load county list from us_counties.json. Auto-downloads from R2 if missing."""
    search_paths = [
        Path("us_counties.json"),
        Path("states/geography/us_counties.json"),
        Path("/opt/crashlens/repo/us_counties.json"),
        Path("/opt/crashlens/repo/states/geography/us_counties.json"),
    ]
    for p in search_paths:
        if p.exists():
            data = json.load(open(p))
            records = data.get("records", data) if isinstance(data, dict) else data
            counties = [r for r in records if r["STATE"] == state_fips]
            print(f"      Loaded counties from: {p}")
            return counties

    # Auto-download from R2
    print("      us_counties.json not found locally — downloading from R2...")
    s3 = get_r2_client()
    if s3:
        bucket = os.environ.get("R2_BUCKET", "crash-lens-data")
        local_path = Path("us_counties.json")
        try:
            s3.download_file(bucket, "_national/us_counties.json", str(local_path))
            print(f"      Downloaded to {local_path}")
            data = json.load(open(local_path))
            records = data.get("records", data) if isinstance(data, dict) else data
            counties = [r for r in records if r["STATE"] == state_fips]
            return counties
        except Exception as e:
            print(f"      R2 download failed: {e}")

    raise FileNotFoundError(
        "us_counties.json not found. Download manually:\n"
        "  aws s3 cp s3://crash-lens-data/_national/us_counties.json us_counties.json "
        '--endpoint-url "$R2_ENDPOINT"'
    )


# ═══════════════════════════════════════════════════════════════
#  ROAD SEGMENT CONVERTER (same as generate_osm_data.py)
# ═══════════════════════════════════════════════════════════════

def _clean(val):
    """Convert OSM list values to semicolon-joined strings."""
    if val is None:
        return ""
    if isinstance(val, list):
        return ";".join(str(v) for v in val)
    return str(val).strip() if str(val) != "nan" else ""


def graph_to_dataframes(G):
    """Convert osmnx graph to road_df + int_df DataFrames."""
    import osmnx as ox

    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)

    road_data = []
    for idx, row in edges_gdf.iterrows():
        u, v, key = idx
        u_lat = nodes_gdf.loc[u].geometry.y
        u_lon = nodes_gdf.loc[u].geometry.x
        v_lat = nodes_gdf.loc[v].geometry.y
        v_lon = nodes_gdf.loc[v].geometry.x

        road_length = float(row.get("length", 0))

        # Curvature from geometry
        geom = row.get("geometry", None)
        if geom is not None and hasattr(geom, "coords") and len(geom.coords) >= 3:
            coords = list(geom.coords)
            total_deflection = 0.0
            for j in range(1, len(coords) - 1):
                x0, y0 = coords[j - 1]
                x1, y1 = coords[j]
                x2, y2 = coords[j + 1]
                h1 = math.atan2(y1 - y0, x1 - x0)
                h2 = math.atan2(y2 - y1, x2 - x1)
                delta = abs(h2 - h1)
                if delta > math.pi:
                    delta = 2 * math.pi - delta
                total_deflection += delta

            straight_dist = math.sqrt(
                ((coords[0][1] - coords[-1][1]) * 111000) ** 2
                + ((coords[0][0] - coords[-1][0]) * 111000
                   * math.cos(math.radians(coords[0][1]))) ** 2
            )
            length_ratio = road_length / max(straight_dist, 1) if straight_dist > 5 else 1.0
            defl_curvature = 1.0 + total_deflection
            curvature = round(max(length_ratio, defl_curvature), 3)
        else:
            straight_dist = math.sqrt((u_lat - v_lat) ** 2 + (u_lon - v_lon) ** 2) * 111000
            curvature = round(road_length / max(straight_dist, 1), 3) if straight_dist > 5 else 1.0

        road_data.append({
            "u_node": u, "v_node": v,
            "u_lat": u_lat, "u_lon": u_lon,
            "v_lat": v_lat, "v_lon": v_lon,
            "mid_lat": (u_lat + v_lat) / 2,
            "mid_lon": (u_lon + v_lon) / 2,
            "highway":  _clean(row.get("highway", "")),
            "name":     _clean(row.get("name", "")),
            "ref":      _clean(row.get("ref", "")),
            "oneway":   _clean(row.get("oneway", "")),
            "lanes":    _clean(row.get("lanes", "")),
            "maxspeed": _clean(row.get("maxspeed", "")),
            "length_m": road_length,
            "bridge":   _clean(row.get("bridge", "")),
            "tunnel":   _clean(row.get("tunnel", "")),
            "surface":  _clean(row.get("surface", "")),
            "lit":      _clean(row.get("lit", "")),
            "sidewalk": _clean(row.get("sidewalk", "")),
            "cycleway": _clean(row.get("cycleway", "")),
            "divider":  _clean(row.get("divider", "")),
            "curvature": curvature,
        })

    road_df = pd.DataFrame(road_data)

    # Intersections (degree >= 3)
    degrees = dict(G.degree())
    int_data = []
    for node_id, deg in degrees.items():
        if deg >= 3:
            n = nodes_gdf.loc[node_id]
            int_data.append({
                "node_id": node_id,
                "lat": n.geometry.y,
                "lon": n.geometry.x,
                "degree": deg,
            })

    int_df = pd.DataFrame(int_data) if int_data else pd.DataFrame(
        columns=["node_id", "lat", "lon", "degree"]
    )

    return road_df, int_df


# ═══════════════════════════════════════════════════════════════
#  POI CATEGORIES (same as generate_osm_data.py)
# ═══════════════════════════════════════════════════════════════

POI_TAGS = [
    ("bar",        "amenity", ["bar", "pub", "nightclub", "biergarten"]),
    ("school",     "amenity", ["school", "kindergarten"]),
    ("college",    "amenity", ["college", "university"]),
    ("hospital",   "amenity", ["hospital"]),
    ("clinic",     "amenity", ["clinic"]),
    ("fuel",       "amenity", ["fuel"]),
    ("parking",    "amenity", ["parking"]),
    ("restaurant", "amenity", ["restaurant", "fast_food"]),
    ("signal",     "highway", ["traffic_signals"]),
    ("stop_sign",  "highway", ["stop"]),
    ("crossing",   "highway", ["crossing"]),
    ("rest_area",  "highway", ["rest_area", "services"]),
    ("rail_xing",  "railway", ["level_crossing"]),
]


# ═══════════════════════════════════════════════════════════════
#  COUNTY-BY-COUNTY ROAD DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_one_road_county(county, state_name, index, total):
    """Download road graph for a single county. Thread-safe. Returns (name, road_df, int_df) or (name, None, None)."""
    import osmnx as ox
    county_name = county["NAMELSAD"]
    place = f"{county_name}, {state_name}, United States"
    t0 = time.time()

    try:
        G = ox.graph_from_place(place, network_type="drive", simplify=True)
        n_nodes = G.number_of_nodes()
        n_edges = G.number_of_edges()

        road_df, int_df = graph_to_dataframes(G)
        del G
        gc.collect()

        elapsed = time.time() - t0
        safe_print(f"      [{index:3d}/{total}] {county_name}... "
                   f"{n_nodes:,} nodes, {n_edges:,} edges ({elapsed:.0f}s)")
        return county_name, road_df, int_df

    except Exception as e:
        elapsed = time.time() - t0
        safe_print(f"      [{index:3d}/{total}] {county_name}... FAILED ({elapsed:.0f}s): {str(e)[:80]}")
        gc.collect()
        return county_name, None, None


def download_roads_by_county(state_name, state_abbr, state_fips, workers=1):
    """Download road network county-by-county, return merged DataFrames."""
    counties = load_counties(state_fips)
    total = len(counties)
    safe_print(f"      {total} counties found for {state_name} (workers: {workers})")

    all_roads = []
    all_ints = []
    failed = []

    if workers <= 1:
        # Sequential (original behavior)
        for i, county in enumerate(counties, 1):
            name, road_df, int_df = _download_one_road_county(county, state_name, i, total)
            if road_df is not None:
                all_roads.append(road_df)
                all_ints.append(int_df)
            else:
                failed.append(name)
    else:
        # Parallel
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_info = {}
            for i, county in enumerate(counties, 1):
                future = executor.submit(_download_one_road_county, county, state_name, i, total)
                future_to_info[future] = county["NAMELSAD"]

            for future in as_completed(future_to_info):
                county_name = future_to_info[future]
                try:
                    name, road_df, int_df = future.result(timeout=COUNTY_TIMEOUT)
                    if road_df is not None:
                        all_roads.append(road_df)
                        all_ints.append(int_df)
                    else:
                        failed.append(name)
                except TimeoutError:
                    safe_print(f"      ⏰ {county_name} — TIMED OUT")
                    failed.append(county_name)
                except Exception as e:
                    safe_print(f"      ❌ {county_name} — ERROR: {str(e)[:80]}")
                    failed.append(county_name)

    if failed:
        safe_print(f"      ⚠️  {len(failed)} counties failed: {', '.join(failed[:5])}"
              + (f"... +{len(failed)-5} more" if len(failed) > 5 else ""))

    if not all_roads:
        return pd.DataFrame(), pd.DataFrame()

    # Merge and deduplicate
    safe_print(f"      Merging {len(all_roads)} county DataFrames...", end=" ", flush=True)
    merged_roads = pd.concat(all_roads, ignore_index=True)
    merged_ints = pd.concat(all_ints, ignore_index=True)

    # Free individual DFs
    del all_roads, all_ints
    gc.collect()

    # Dedup roads: same OSM edge (u_node, v_node) appears in adjacent counties
    before_roads = len(merged_roads)
    merged_roads.drop_duplicates(subset=["u_node", "v_node"], keep="first", inplace=True)
    after_roads = len(merged_roads)

    # Dedup intersections: same OSM node_id in adjacent counties
    before_ints = len(merged_ints)
    if len(merged_ints) > 0:
        # For shared boundary nodes, take the max degree (sees all connected edges)
        merged_ints = (
            merged_ints.groupby("node_id", as_index=False)
            .agg({"lat": "first", "lon": "first", "degree": "max"})
        )
    after_ints = len(merged_ints)

    safe_print(f"roads {before_roads:,}→{after_roads:,}, "
          f"intersections {before_ints:,}→{after_ints:,}")

    return merged_roads, merged_ints


# ═══════════════════════════════════════════════════════════════
#  COUNTY-BY-COUNTY POI DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def _download_one_poi_county(county, state_name, index, total):
    """Download POIs for a single county. Thread-safe. Returns (name, list_of_dicts)."""
    import osmnx as ox
    county_name = county["NAMELSAD"]
    place = f"{county_name}, {state_name}, United States"
    pois = []

    for category, osm_key, osm_values in POI_TAGS:
        try:
            gdf = ox.features_from_place(place, tags={osm_key: osm_values})
            for idx, row in gdf.iterrows():
                try:
                    if row.geometry.geom_type == "Point":
                        lat, lon = row.geometry.y, row.geometry.x
                    else:
                        centroid = row.geometry.centroid
                        lat, lon = centroid.y, centroid.x
                except Exception:
                    continue

                name_val = str(row.get("name", "") or "").strip()
                if name_val == "nan":
                    name_val = ""
                subcategory = str(row.get(osm_key, "")).strip()
                if subcategory == "nan":
                    subcategory = osm_values[0]
                osm_id = idx[1] if isinstance(idx, tuple) else idx
                pois.append({
                    "osm_id": osm_id, "lat": round(lat, 7), "lon": round(lon, 7),
                    "category": category, "subcategory": subcategory, "name": name_val[:100],
                })
        except Exception:
            pass

    if index % 10 == 1 or index == total:
        safe_print(f"      [{index:3d}/{total}] {county_name}... {len(pois)} POIs")

    return county_name, pois


def download_pois_by_county(state_name, state_abbr, state_fips, workers=1):
    """Download POIs county-by-county, return merged DataFrame."""
    counties = load_counties(state_fips)
    total = len(counties)
    safe_print(f"      {total} counties — downloading POIs (workers: {workers})")

    all_pois = []

    if workers <= 1:
        for i, county in enumerate(counties, 1):
            name, pois = _download_one_poi_county(county, state_name, i, total)
            all_pois.extend(pois)
            if i % 20 == 0:
                gc.collect()
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_info = {}
            for i, county in enumerate(counties, 1):
                future = executor.submit(_download_one_poi_county, county, state_name, i, total)
                future_to_info[future] = county["NAMELSAD"]

            for future in as_completed(future_to_info):
                try:
                    name, pois = future.result(timeout=COUNTY_TIMEOUT)
                    all_pois.extend(pois)
                except TimeoutError:
                    safe_print(f"      ⏰ {future_to_info[future]} POIs — TIMED OUT")
                except Exception as e:
                    safe_print(f"      ❌ {future_to_info[future]} POIs — ERROR: {str(e)[:60]}")

    poi_df = pd.DataFrame(all_pois)

    if len(poi_df) > 0:
        before = len(poi_df)
        poi_df.drop_duplicates(subset=["osm_id", "category"], keep="first", inplace=True)
        after = len(poi_df)
        safe_print(f"      POI dedup: {before:,} → {after:,}")

    for cat, _, _ in POI_TAGS:
        count = len(poi_df[poi_df["category"] == cat]) if len(poi_df) > 0 else 0
        safe_print(f"      {cat:12s}  {count:>6,} POIs")

    return poi_df


# ═══════════════════════════════════════════════════════════════
#  R2 UTILITIES (same as generate_osm_data.py)
# ═══════════════════════════════════════════════════════════════

def gzip_file(src, dst):
    with open(src, "rb") as fi, gzip.open(dst, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    raw = os.path.getsize(src) / 1048576
    gz = os.path.getsize(dst) / 1048576
    return raw, gz


def get_r2_client():
    endpoint = os.environ.get("R2_ENDPOINT", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    if not all([endpoint, key_id, secret]):
        return None
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )


def r2_upload(s3, local_path, bucket, key):
    s3.upload_file(str(local_path), bucket, key)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="County-by-county OSM downloader (parallel support)",
    )
    parser.add_argument("--state", required=True, help="State abbreviation (e.g. oh)")
    parser.add_argument("--roads-only", action="store_true", help="Roads + intersections only")
    parser.add_argument("--pois-only", action="store_true", help="POIs only")
    parser.add_argument("--parallel", type=int, default=2,
                        help="Max parallel county downloads (default 2, max 4)")
    parser.add_argument("--local-only", action="store_true", help="Skip R2 upload")
    parser.add_argument("--cache-dir", default="cache", help="Cache directory")
    args = parser.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES:
        print(f"Unknown state: {abbr}")
        sys.exit(1)

    workers = min(args.parallel, 4)  # Hard cap at 4
    state_name, state_fips, r2_prefix = STATES[abbr]
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    do_roads = not args.pois_only
    do_pois = not args.roads_only

    # R2 connection
    bucket = os.environ.get("R2_BUCKET", "crash-lens-data")
    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        if s3:
            print(f"R2 connected: {bucket}")
        else:
            print("R2 credentials not set — local only")

    print(f"\n{'=' * 60}")
    print(f"  County-by-County OSM Download: {state_name} ({abbr})")
    mode = "roads+pois"
    if args.roads_only:
        mode = "roads-only"
    elif args.pois_only:
        mode = "pois-only"
    print(f"  Mode: {mode}")
    print(f"  Workers: {workers}")
    print(f"  Per-county timeout: {COUNTY_TIMEOUT // 3600}h")
    print(f"{'=' * 60}\n")

    # ── ROADS + INTERSECTIONS ──
    if do_roads:
        print(f"  [roads] {state_name} — county-by-county download...")
        t0 = time.time()

        road_df, int_df = download_roads_by_county(state_name, abbr, state_fips, workers)

        if len(road_df) > 0:
            elapsed = time.time() - t0
            print(f"      Total: {len(road_df):,} road segments, "
                  f"{len(int_df):,} intersections ({elapsed:.0f}s)")

            # Tag coverage report
            for tag in ["surface", "lit", "sidewalk", "cycleway", "maxspeed"]:
                if tag in road_df.columns:
                    filled = (road_df[tag].str.strip() != "").sum()
                    pct = filled / len(road_df) * 100
                    print(f"      {tag:12s}  {filled:>7,} ({pct:.1f}%)")

            # Save roads
            roads_pq = cache_dir / f"{abbr}_roads.parquet"
            roads_gz = cache_dir / f"{abbr}_roads.parquet.gz"
            road_df.to_parquet(roads_pq, index=False)
            raw, gz = gzip_file(roads_pq, roads_gz)
            roads_pq.unlink(missing_ok=True)
            print(f"      Roads: {raw:.1f} MB → {gz:.1f} MB gz")

            if s3 and not args.local_only:
                r2_upload(s3, roads_gz, bucket, f"{r2_prefix}/cache/{abbr}_roads.parquet.gz")
                print(f"      → uploaded to R2: {r2_prefix}/cache/{abbr}_roads.parquet.gz")

            # Save intersections
            ints_pq = cache_dir / f"{abbr}_intersections.parquet"
            ints_gz = cache_dir / f"{abbr}_intersections.parquet.gz"
            int_df.to_parquet(ints_pq, index=False)
            raw_i, gz_i = gzip_file(ints_pq, ints_gz)
            ints_pq.unlink(missing_ok=True)
            print(f"      Intersections: {raw_i:.1f} MB → {gz_i:.1f} MB gz")

            if s3 and not args.local_only:
                r2_upload(s3, ints_gz, bucket, f"{r2_prefix}/cache/{abbr}_intersections.parquet.gz")
                print(f"      → uploaded to R2: {r2_prefix}/cache/{abbr}_intersections.parquet.gz")

            del road_df, int_df
            gc.collect()
        else:
            print(f"      ❌ No roads downloaded")

    # ── POIs ──
    if do_pois:
        print(f"\n  [pois] {state_name} — county-by-county download...")
        t0 = time.time()

        poi_df = download_pois_by_county(state_name, abbr, state_fips, workers)

        if len(poi_df) > 0:
            elapsed = time.time() - t0
            pois_pq = cache_dir / f"{abbr}_pois.parquet"
            pois_gz = cache_dir / f"{abbr}_pois.parquet.gz"
            poi_df.to_parquet(pois_pq, index=False)
            raw_p, gz_p = gzip_file(pois_pq, pois_gz)
            pois_pq.unlink(missing_ok=True)
            print(f"      Total: {len(poi_df):,} POIs ({gz_p:.1f} MB gz, {elapsed:.0f}s)")

            if s3 and not args.local_only:
                r2_upload(s3, pois_gz, bucket, f"{r2_prefix}/cache/{abbr}_pois.parquet.gz")
                print(f"      → uploaded to R2: {r2_prefix}/cache/{abbr}_pois.parquet.gz")

            del poi_df
        else:
            print(f"      ❌ No POIs downloaded")

        gc.collect()

    print(f"\n{'=' * 60}")
    print(f"  DONE: {state_name} — {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
