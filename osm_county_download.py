#!/usr/bin/env python3
"""
osm_county_download.py — County-by-county OSM downloader (v2)
=============================================================
Downloads OSM roads, intersections, POIs one county at a time.
Avoids OOM, supports resume, parallel workers, exponential backoff.

STRATEGIES:
  1. Checkpoint saves: Each county saved to R2 temp folder as it completes.
     If job times out, --resume picks up where it left off.
  2. Big-counties-first: Counties sorted by land area descending.
     Large counties run early when there's plenty of time.
  3. Exponential backoff: Overpass errors get 30s→60s→120s retry
     before marking a county as failed.

OUTPUT (statewide — same as generate_osm_data.py):
    {state}/cache/{abbr}_roads.parquet.gz
    {state}/cache/{abbr}_intersections.parquet.gz
    {state}/cache/{abbr}_pois.parquet.gz

CHECKPOINT TEMP (auto-cleaned after consolidation):
    {state}/cache/_county_temp/{slug}_roads.parquet.gz
    {state}/cache/_county_temp/{slug}_ints.parquet.gz
    {state}/cache/_county_temp/{slug}_pois.parquet.gz

USAGE:
    python osm_county_download.py --state oh
    python osm_county_download.py --state oh --resume          # pick up where left off
    python osm_county_download.py --state oh --parallel 3
    python osm_county_download.py --state oh --roads-only
    python osm_county_download.py --state oh --pois-only
    python osm_county_download.py --state oh --local-only
"""

import argparse
import gc
import gzip
import json
import math
import os
import re
import shutil
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from pathlib import Path

import pandas as pd

COUNTY_TIMEOUT = 6 * 3600  # 6 hours (360 min) per county max
MAX_OVERPASS_RETRIES = 3    # Exponential backoff retries per county
_print_lock = threading.Lock()
_s3_lock = threading.Lock()  # boto3 clients are NOT thread-safe


def safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


def _run_with_timeout(fn, args=(), timeout=COUNTY_TIMEOUT):
    """Run a function with a hard timeout."""
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(fn, *args)
        return future.result(timeout=timeout)


# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY
# ═══════════════════════════════════════════════════════════════

STATES = {
    "al": ("Alabama",              "01", "alabama"),
    "ak": ("Alaska",               "02", "alaska"),
    "az": ("Arizona",              "04", "arizona"),
    "ar": ("Arkansas",             "05", "arkansas"),
    "ca": ("California",           "06", "california"),
    "co": ("Colorado",             "08", "colorado"),
    "ct": ("Connecticut",          "09", "connecticut"),
    "de": ("Delaware",             "10", "delaware"),
    "dc": ("District of Columbia", "11", "district_of_columbia"),
    "fl": ("Florida",              "12", "florida"),
    "ga": ("Georgia",              "13", "georgia"),
    "hi": ("Hawaii",               "15", "hawaii"),
    "id": ("Idaho",                "16", "idaho"),
    "il": ("Illinois",             "17", "illinois"),
    "in": ("Indiana",              "18", "indiana"),
    "ia": ("Iowa",                 "19", "iowa"),
    "ks": ("Kansas",               "20", "kansas"),
    "ky": ("Kentucky",             "21", "kentucky"),
    "la": ("Louisiana",            "22", "louisiana"),
    "me": ("Maine",                "23", "maine"),
    "md": ("Maryland",             "24", "maryland"),
    "ma": ("Massachusetts",        "25", "massachusetts"),
    "mi": ("Michigan",             "26", "michigan"),
    "mn": ("Minnesota",            "27", "minnesota"),
    "ms": ("Mississippi",          "28", "mississippi"),
    "mo": ("Missouri",             "29", "missouri"),
    "mt": ("Montana",              "30", "montana"),
    "ne": ("Nebraska",             "31", "nebraska"),
    "nv": ("Nevada",               "32", "nevada"),
    "nh": ("New Hampshire",        "33", "new_hampshire"),
    "nj": ("New Jersey",           "34", "new_jersey"),
    "nm": ("New Mexico",           "35", "new_mexico"),
    "ny": ("New York",             "36", "new_york"),
    "nc": ("North Carolina",       "37", "north_carolina"),
    "nd": ("North Dakota",         "38", "north_dakota"),
    "oh": ("Ohio",                 "39", "ohio"),
    "ok": ("Oklahoma",             "40", "oklahoma"),
    "or": ("Oregon",               "41", "oregon"),
    "pa": ("Pennsylvania",         "42", "pennsylvania"),
    "ri": ("Rhode Island",         "44", "rhode_island"),
    "sc": ("South Carolina",       "45", "south_carolina"),
    "sd": ("South Dakota",         "46", "south_dakota"),
    "tn": ("Tennessee",            "47", "tennessee"),
    "tx": ("Texas",                "48", "texas"),
    "ut": ("Utah",                 "49", "utah"),
    "vt": ("Vermont",              "50", "vermont"),
    "va": ("Virginia",             "51", "virginia"),
    "wa": ("Washington",           "53", "washington"),
    "wv": ("West Virginia",        "54", "west_virginia"),
    "wi": ("Wisconsin",            "55", "wisconsin"),
    "wy": ("Wyoming",              "56", "wyoming"),
}


# ═══════════════════════════════════════════════════════════════
#  COUNTY LOADER + UTILITIES
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
            return [r for r in records if r["STATE"] == state_fips]
        except Exception as e:
            print(f"      R2 download failed: {e}")

    raise FileNotFoundError("us_counties.json not found")


def county_to_slug(county):
    """Convert county name to slug. 'Henrico County' -> 'henrico'."""
    name = county.get("NAMELSAD", county.get("NAME", "unknown"))
    if name.endswith(" County"):
        name = name[:-7]
    if name.endswith(" Municipio"):
        name = name[:-10]
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def sort_counties_big_first(counties):
    """Sort counties by land area descending. Big counties run first."""
    return sorted(counties, key=lambda c: int(c.get("AREALAND", 0)), reverse=True)


# ═══════════════════════════════════════════════════════════════
#  ROAD SEGMENT CONVERTER
# ═══════════════════════════════════════════════════════════════

def _clean(val):
    if val is None:
        return ""
    if isinstance(val, list):
        return ";".join(str(v) for v in val)
    return str(val).strip() if str(val) != "nan" else ""


def graph_to_dataframes(G):
    """Convert osmnx graph to road_df + int_df DataFrames."""
    import osmnx as ox
    ox.settings.useful_tags_way += ['surface', 'lit', 'sidewalk', 'cycleway', 'divider']

    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)

    road_data = []
    for idx, row in edges_gdf.iterrows():
        u, v, key = idx
        u_lat = nodes_gdf.loc[u].geometry.y
        u_lon = nodes_gdf.loc[u].geometry.x
        v_lat = nodes_gdf.loc[v].geometry.y
        v_lon = nodes_gdf.loc[v].geometry.x
        road_length = float(row.get("length", 0))

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
            curvature = round(max(length_ratio, 1.0 + total_deflection), 3)
        else:
            straight_dist = math.sqrt((u_lat - v_lat) ** 2 + (u_lon - v_lon) ** 2) * 111000
            curvature = round(road_length / max(straight_dist, 1), 3) if straight_dist > 5 else 1.0

        road_data.append({
            "u_node": u, "v_node": v,
            "u_lat": u_lat, "u_lon": u_lon, "v_lat": v_lat, "v_lon": v_lon,
            "mid_lat": (u_lat + v_lat) / 2, "mid_lon": (u_lon + v_lon) / 2,
            "highway": _clean(row.get("highway", "")),
            "name": _clean(row.get("name", "")),
            "ref": _clean(row.get("ref", "")),
            "oneway": _clean(row.get("oneway", "")),
            "lanes": _clean(row.get("lanes", "")),
            "maxspeed": _clean(row.get("maxspeed", "")),
            "length_m": road_length,
            "bridge": _clean(row.get("bridge", "")),
            "tunnel": _clean(row.get("tunnel", "")),
            "surface": _clean(row.get("surface", "")),
            "lit": _clean(row.get("lit", "")),
            "sidewalk": _clean(row.get("sidewalk", "")),
            "cycleway": _clean(row.get("cycleway", "")),
            "divider": _clean(row.get("divider", "")),
            "curvature": curvature,
        })

    road_df = pd.DataFrame(road_data)

    # ── Intersections (flat format) ──
    # See generate_osm_data.py for the full explanation. Two metrics kept:
    #   degree: directed MultiDiGraph edges (legacy, ~2 per two-way road)
    #   streets_per_node: undirected physical street count (MIRE-correct)
    # Filter is permissive (deg>=3 OR spn>=3) for backward compatibility
    # during monthly cache regeneration.
    degrees = dict(G.degree())
    try:
        spn_dict = ox.stats.streets_per_node(G)
    except Exception:
        spn_dict = {}
        for _n_id in G.nodes():
            nbrs = set()
            for _u, _v, _k in G.out_edges(_n_id, keys=True):
                if _v != _n_id:
                    nbrs.add(_v)
            for _u, _v, _k in G.in_edges(_n_id, keys=True):
                if _u != _n_id:
                    nbrs.add(_u)
            spn_dict[_n_id] = len(nbrs)

    int_data = [
        {"node_id": nid, "lat": nodes_gdf.loc[nid].geometry.y,
         "lon": nodes_gdf.loc[nid].geometry.x, "degree": deg,
         "streets_per_node": int(spn_dict.get(nid, 0) or 0)}
        for nid, deg in degrees.items()
        if deg >= 3 or int(spn_dict.get(nid, 0) or 0) >= 3
    ]
    int_df = pd.DataFrame(int_data) if int_data else pd.DataFrame(
        columns=["node_id", "lat", "lon", "degree", "streets_per_node"])

    return road_df, int_df


# ═══════════════════════════════════════════════════════════════
#  POI CATEGORIES
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
#  SINGLE COUNTY DOWNLOADERS (with exponential backoff)
# ═══════════════════════════════════════════════════════════════

def _download_one_road_county(county, state_name, index, total):
    """Download road graph for one county with exponential backoff."""
    import osmnx as ox
    ox.settings.useful_tags_way += ['surface', 'lit', 'sidewalk', 'cycleway', 'divider']
    county_name = county["NAMELSAD"]
    place = f"{county_name}, {state_name}, United States"

    for attempt in range(MAX_OVERPASS_RETRIES):
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
            if attempt < MAX_OVERPASS_RETRIES - 1:
                wait = 30 * (2 ** attempt)  # 30s, 60s, 120s
                safe_print(f"      [{index:3d}/{total}] {county_name}... "
                           f"attempt {attempt+1} failed ({elapsed:.0f}s) — retry in {wait}s")
                time.sleep(wait)
                gc.collect()
            else:
                safe_print(f"      [{index:3d}/{total}] {county_name}... "
                           f"FAILED after {MAX_OVERPASS_RETRIES} attempts: {str(e)[:80]}")
                gc.collect()
                return county_name, None, None


def _download_one_poi_county(county, state_name, index, total):
    """Download POIs for one county with exponential backoff per category."""
    import osmnx as ox
    ox.settings.useful_tags_way += ['surface', 'lit', 'sidewalk', 'cycleway', 'divider']
    county_name = county["NAMELSAD"]
    place = f"{county_name}, {state_name}, United States"
    pois = []

    for category, osm_key, osm_values in POI_TAGS:
        for attempt in range(MAX_OVERPASS_RETRIES):
            try:
                gdf = ox.features_from_place(place, tags={osm_key: osm_values})
                for idx, row in gdf.iterrows():
                    try:
                        if row.geometry.geom_type == "Point":
                            lat, lon = row.geometry.y, row.geometry.x
                        else:
                            c = row.geometry.centroid
                            lat, lon = c.y, c.x
                    except Exception:
                        continue
                    name_val = str(row.get("name", "") or "").strip()
                    if name_val == "nan": name_val = ""
                    sub = str(row.get(osm_key, "")).strip()
                    if sub == "nan": sub = osm_values[0]
                    osm_id = idx[1] if isinstance(idx, tuple) else idx
                    pois.append({"osm_id": osm_id, "lat": round(lat, 7), "lon": round(lon, 7),
                                 "category": category, "subcategory": sub, "name": name_val[:100]})
                break
            except Exception:
                if attempt < MAX_OVERPASS_RETRIES - 1:
                    time.sleep(30 * (2 ** attempt))

    if index % 10 == 1 or index == total:
        safe_print(f"      [{index:3d}/{total}] {county_name}... {len(pois)} POIs")
    return county_name, pois


# ═══════════════════════════════════════════════════════════════
#  R2 UTILITIES
# ═══════════════════════════════════════════════════════════════

def gzip_file(src, dst):
    with open(src, "rb") as fi, gzip.open(dst, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    return os.path.getsize(src) / 1048576, os.path.getsize(dst) / 1048576


def get_r2_client():
    endpoint = os.environ.get("R2_ENDPOINT", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    if not all([endpoint, key_id, secret]):
        return None
    import boto3
    return boto3.client("s3", endpoint_url=endpoint,
                        aws_access_key_id=key_id, aws_secret_access_key=secret,
                        region_name="auto")


def r2_upload(s3, local_path, bucket, key):
    with _s3_lock:
        s3.upload_file(str(local_path), bucket, key)


def r2_exists(s3, bucket, key):
    if not s3: return False
    try:
        with _s3_lock:
            s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def r2_delete_prefix(s3, bucket, prefix):
    """Delete all objects under a prefix (temp cleanup)."""
    if not s3: return 0
    deleted = 0
    try:
        with _s3_lock:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = page.get("Contents", [])
                if objects:
                    s3.delete_objects(Bucket=bucket,
                                     Delete={"Objects": [{"Key": o["Key"]} for o in objects]})
                    deleted += len(objects)
    except Exception as e:
        safe_print(f"      Cleanup error: {e}")
    return deleted


def save_checkpoint(df, cache_dir, slug, suffix, s3, bucket, r2_prefix):
    """Save county DataFrame to R2 temp checkpoint."""
    pq = cache_dir / f"{slug}_{suffix}.parquet"
    gz = cache_dir / f"{slug}_{suffix}.parquet.gz"
    df.to_parquet(pq, index=False)
    gzip_file(pq, gz)
    pq.unlink(missing_ok=True)
    if s3:
        r2_upload(s3, gz, bucket, f"{r2_prefix}/cache/_county_temp/{slug}_{suffix}.parquet.gz")
    gz.unlink(missing_ok=True)


def load_checkpoint(slug, suffix, s3, bucket, r2_prefix, cache_dir):
    """Load county checkpoint from R2. Returns DataFrame or None."""
    if not s3: return None
    r2_key = f"{r2_prefix}/cache/_county_temp/{slug}_{suffix}.parquet.gz"
    if not r2_exists(s3, bucket, r2_key): return None
    try:
        local_gz = cache_dir / f"{slug}_{suffix}.parquet.gz"
        local_pq = cache_dir / f"{slug}_{suffix}.parquet"
        with _s3_lock:
            s3.download_file(bucket, r2_key, str(local_gz))
        with gzip.open(local_gz, "rb") as fi, open(local_pq, "wb") as fo:
            shutil.copyfileobj(fi, fo)
        df = pd.read_parquet(local_pq)
        local_gz.unlink(missing_ok=True)
        local_pq.unlink(missing_ok=True)
        return df
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="County-by-county OSM downloader (v2 — checkpoint, backoff, big-first)")
    parser.add_argument("--state", required=True, help="State abbreviation")
    parser.add_argument("--roads-only", action="store_true")
    parser.add_argument("--pois-only", action="store_true")
    parser.add_argument("--parallel", type=int, default=2, help="Workers (default 2, max 4)")
    parser.add_argument("--resume", action="store_true", help="Resume from R2 checkpoints")
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument("--cache-dir", default="cache")
    parser.add_argument("--max-runtime", type=int, default=0,
                        help="Max runtime in minutes (0=unlimited). Exits gracefully, "
                             "preserving per-county checkpoints for --resume on next run.")
    args = parser.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES:
        print(f"Unknown state: {abbr}")
        sys.exit(1)

    workers = min(args.parallel, 4)
    state_name, state_fips, r2_prefix = STATES[abbr]
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    do_roads = not args.pois_only
    do_pois = not args.roads_only

    # Time-budget: graceful exit before CI timeout
    job_start = time.time()
    max_runtime_sec = args.max_runtime * 60 if args.max_runtime > 0 else 0

    def time_expired():
        """Check if we've exceeded --max-runtime budget."""
        if max_runtime_sec <= 0:
            return False
        return (time.time() - job_start) >= max_runtime_sec

    def time_remaining_str():
        if max_runtime_sec <= 0:
            return "unlimited"
        left = max_runtime_sec - (time.time() - job_start)
        return f"{left / 60:.0f}min"

    bucket = os.environ.get("R2_BUCKET", "crash-lens-data")
    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        print(f"R2 connected: {bucket}" if s3 else "R2 not configured — local only")

    counties = sort_counties_big_first(load_counties(state_fips))
    total = len(counties)

    mode = "roads+pois"
    if args.roads_only: mode = "roads-only"
    elif args.pois_only: mode = "pois-only"

    print(f"\n{'=' * 60}")
    print(f"  County-by-County OSM Download: {state_name} ({abbr})")
    print(f"  Mode: {mode} | Workers: {workers} | Counties: {total}")
    print(f"  Resume: {'yes' if args.resume else 'no'} | Big-first: yes")
    print(f"  Per-county timeout: {COUNTY_TIMEOUT // 3600}h | Backoff retries: {MAX_OVERPASS_RETRIES}")
    rt = f"{args.max_runtime}min" if args.max_runtime > 0 else "unlimited"
    print(f"  Max runtime: {rt}")
    print(f"{'=' * 60}\n")

    time_budget_hit = False

    # ══════════════════════════════════════════════════════════
    #  ROADS + INTERSECTIONS
    # ══════════════════════════════════════════════════════════
    if do_roads:
        print(f"  [roads] {state_name} — county-by-county download...")
        t0 = time.time()
        all_roads, all_ints, failed = [], [], []

        def _process_road(county, index):
            slug = county_to_slug(county)
            # Resume check
            if args.resume and s3:
                rd = load_checkpoint(slug, "roads", s3, bucket, r2_prefix, cache_dir)
                if rd is not None:
                    it = load_checkpoint(slug, "ints", s3, bucket, r2_prefix, cache_dir)
                    # Backfill streets_per_node = 0 on pre-2026-04-12 checkpoints
                    # so schemas stay consistent across a mixed resume run.
                    if it is not None and len(it) > 0 and "streets_per_node" not in it.columns:
                        it = it.copy()
                        it["streets_per_node"] = 0
                    safe_print(f"      [{index:3d}/{total}] {county['NAMELSAD']}... "
                               f"resumed ({len(rd):,} roads)")
                    return county["NAMELSAD"], rd, it if it is not None else pd.DataFrame()
            # Download
            name, rd, it = _download_one_road_county(county, state_name, index, total)
            # Save checkpoint
            if rd is not None and s3 and not args.local_only:
                save_checkpoint(rd, cache_dir, slug, "roads", s3, bucket, r2_prefix)
                if it is not None and len(it) > 0:
                    save_checkpoint(it, cache_dir, slug, "ints", s3, bucket, r2_prefix)
            return name, rd, it

        # Download all counties
        time_budget_hit = False
        if workers <= 1:
            for i, county in enumerate(counties, 1):
                if time_expired():
                    safe_print(f"\n      ⏱️  Max runtime ({args.max_runtime}min) reached at county "
                               f"{i}/{total}. Checkpoints saved — re-run with --resume.")
                    time_budget_hit = True
                    break
                try:
                    name, rd, it = _run_with_timeout(_process_road, (county, i))
                    if rd is not None:
                        all_roads.append(rd)
                        if it is not None: all_ints.append(it)
                    else:
                        failed.append(name)
                except TimeoutError:
                    safe_print(f"      [{i:3d}/{total}] {county['NAMELSAD']}... ⏰ TIMED OUT")
                    failed.append(county["NAMELSAD"])
                except Exception as e:
                    safe_print(f"      [{i:3d}/{total}] {county['NAMELSAD']}... ❌ {str(e)[:80]}")
                    failed.append(county["NAMELSAD"])
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                # Submit in batches to allow time checks
                pending = {}
                county_iter = iter(enumerate(counties, 1))
                # Seed initial batch
                for _ in range(workers):
                    try:
                        i, c = next(county_iter)
                        pending[ex.submit(_process_road, c, i)] = c["NAMELSAD"]
                    except StopIteration:
                        break
                while pending:
                    done = set()
                    for fut in as_completed(pending, timeout=COUNTY_TIMEOUT):
                        cn = pending[fut]
                        try:
                            name, rd, it = fut.result(timeout=10)
                            if rd is not None:
                                all_roads.append(rd)
                                if it is not None: all_ints.append(it)
                            else:
                                failed.append(name)
                        except TimeoutError:
                            safe_print(f"      ⏰ {cn} — TIMED OUT")
                            failed.append(cn)
                        except Exception as e:
                            safe_print(f"      ❌ {cn} — {str(e)[:80]}")
                            failed.append(cn)
                        done.add(fut)
                        # Submit next county if time allows
                        if not time_expired():
                            try:
                                i, c = next(county_iter)
                                pending[ex.submit(_process_road, c, i)] = c["NAMELSAD"]
                            except StopIteration:
                                pass
                        break  # Process one at a time from as_completed
                    for f in done:
                        del pending[f]
                    if time_expired() and not any(not f.done() for f in pending):
                        safe_print(f"\n      ⏱️  Max runtime ({args.max_runtime}min) reached. "
                                   f"Checkpoints saved — re-run with --resume.")
                        time_budget_hit = True
                        break

        # Retry failed (1 at a time, 60s cooldown) — skip if time budget hit
        if failed and not time_budget_hit:
            safe_print(f"      ⚠️  {len(failed)} failed: {', '.join(failed[:5])}")
            safe_print(f"      🔄 Retrying 1-at-a-time (60s cooldown)...")
            still_failed = []
            for cn in failed:
                if time_expired():
                    safe_print(f"      ⏱️  Max runtime reached — skipping remaining retries")
                    still_failed.extend(failed[failed.index(cn):])
                    time_budget_hit = True
                    break
                time.sleep(60)
                rec = next((c for c in counties if c["NAMELSAD"] == cn), None)
                if not rec:
                    still_failed.append(cn); continue
                try:
                    name, rd, it = _run_with_timeout(_process_road, (rec, 0))
                    if rd is not None:
                        all_roads.append(rd)
                        if it is not None: all_ints.append(it)
                        safe_print(f"      [retry] ✅ {cn}")
                    else:
                        still_failed.append(cn)
                        safe_print(f"      [retry] ❌ {cn} — skipping")
                except (TimeoutError, Exception) as e:
                    safe_print(f"      [retry] ❌ {cn} — {str(e)[:60]}")
                    still_failed.append(cn)
            if still_failed:
                safe_print(f"      ⚠️  {len(still_failed)} still failed: {', '.join(still_failed[:10])}")

        # Merge + dedup + upload statewide (skip if time-budgeted early exit)
        if time_budget_hit:
            downloaded = len(all_roads)
            safe_print(f"\n      ⏱️  Partial run: {downloaded}/{total} counties downloaded.")
            safe_print(f"      Per-county checkpoints saved to R2. Re-run with --resume to continue.")
            del all_roads, all_ints; gc.collect()
        elif all_roads:
            safe_print(f"      Merging {len(all_roads)} counties...", end=" ", flush=True)
            merged_r = pd.concat(all_roads, ignore_index=True)
            merged_i = pd.concat(all_ints, ignore_index=True) if all_ints else pd.DataFrame()
            del all_roads, all_ints; gc.collect()

            b = len(merged_r)
            merged_r.drop_duplicates(subset=["u_node", "v_node"], keep="first", inplace=True)
            a = len(merged_r)
            bi = len(merged_i)
            if len(merged_i) > 0:
                # Aggregate per unique node_id. For nodes on county boundaries,
                # each county's subgraph sees only part of the true degree —
                # "max" is a best-effort recovery. streets_per_node is included
                # so the MIRE-correct undirected physical approach count
                # survives the county merge (added 2026-04-12).
                int_agg = {"lat": "first", "lon": "first", "degree": "max"}
                if "streets_per_node" in merged_i.columns:
                    int_agg["streets_per_node"] = "max"
                merged_i = merged_i.groupby("node_id", as_index=False).agg(int_agg)
            ai = len(merged_i)
            safe_print(f"roads {b:,}→{a:,}, ints {bi:,}→{ai:,}")

            elapsed = time.time() - t0
            print(f"      Total: {a:,} roads, {ai:,} intersections ({elapsed:.0f}s)")
            for tag in ["surface", "lit", "sidewalk", "cycleway", "maxspeed"]:
                if tag in merged_r.columns:
                    filled = (merged_r[tag].str.strip() != "").sum()
                    print(f"      {tag:12s}  {filled:>7,} ({filled/a*100:.1f}%)")

            # Upload roads
            rpq = cache_dir / f"{abbr}_roads.parquet"
            rgz = cache_dir / f"{abbr}_roads.parquet.gz"
            merged_r.to_parquet(rpq, index=False)
            raw, gz = gzip_file(rpq, rgz); rpq.unlink(missing_ok=True)
            print(f"      Roads: {raw:.1f} → {gz:.1f} MB gz")
            if s3 and not args.local_only:
                r2_upload(s3, rgz, bucket, f"{r2_prefix}/cache/{abbr}_roads.parquet.gz")
                print(f"      → R2: {r2_prefix}/cache/{abbr}_roads.parquet.gz")

            # Upload intersections
            if ai > 0:
                ipq = cache_dir / f"{abbr}_intersections.parquet"
                igz = cache_dir / f"{abbr}_intersections.parquet.gz"
                merged_i.to_parquet(ipq, index=False)
                ri, gi = gzip_file(ipq, igz); ipq.unlink(missing_ok=True)
                print(f"      Ints: {ri:.1f} → {gi:.1f} MB gz")
                if s3 and not args.local_only:
                    r2_upload(s3, igz, bucket, f"{r2_prefix}/cache/{abbr}_intersections.parquet.gz")
                    print(f"      → R2: {r2_prefix}/cache/{abbr}_intersections.parquet.gz")

            del merged_r, merged_i; gc.collect()
        else:
            print(f"      ❌ No roads downloaded")

    # ══════════════════════════════════════════════════════════
    #  POIs
    # ══════════════════════════════════════════════════════════
    if do_pois and not time_budget_hit:
        print(f"\n  [pois] {state_name} — county-by-county download...")
        t0 = time.time()
        all_pois, failed_p = [], []

        def _process_poi(county, index):
            slug = county_to_slug(county)
            if args.resume and s3:
                ex = load_checkpoint(slug, "pois", s3, bucket, r2_prefix, cache_dir)
                if ex is not None:
                    safe_print(f"      [{index:3d}/{total}] {county['NAMELSAD']}... "
                               f"resumed {len(ex):,} POIs")
                    return county["NAMELSAD"], ex.to_dict("records")
            name, pois = _download_one_poi_county(county, state_name, index, total)
            if pois and s3 and not args.local_only:
                save_checkpoint(pd.DataFrame(pois), cache_dir, slug, "pois", s3, bucket, r2_prefix)
            return name, pois

        if workers <= 1:
            for i, county in enumerate(counties, 1):
                if time_expired():
                    safe_print(f"\n      ⏱️  Max runtime ({args.max_runtime}min) reached at POI county "
                               f"{i}/{total}. Checkpoints saved — re-run with --resume.")
                    time_budget_hit = True
                    break
                try:
                    _, pois = _run_with_timeout(_process_poi, (county, i))
                    all_pois.extend(pois)
                except (TimeoutError, Exception) as e:
                    safe_print(f"      ⏰❌ {county['NAMELSAD']} — {str(e)[:60]}")
                    failed_p.append(county)
                if i % 20 == 0: gc.collect()
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(_process_poi, c, i): c for i, c in enumerate(counties, 1)}
                for fut in as_completed(futs):
                    c = futs[fut]
                    try:
                        _, pois = fut.result(timeout=COUNTY_TIMEOUT)
                        all_pois.extend(pois)
                    except (TimeoutError, Exception) as e:
                        safe_print(f"      ⏰❌ {c['NAMELSAD']} — {str(e)[:60]}")
                        failed_p.append(c)

        if failed_p and not time_budget_hit:
            safe_print(f"      🔄 Retrying {len(failed_p)} failed POI counties...")
            for c in failed_p:
                if time_expired():
                    safe_print(f"      ⏱️  Max runtime reached — skipping remaining POI retries")
                    time_budget_hit = True
                    break
                time.sleep(60)
                try:
                    _, pois = _run_with_timeout(_process_poi, (c, 0))
                    all_pois.extend(pois)
                    safe_print(f"      [retry] ✅ {c['NAMELSAD']}")
                except (TimeoutError, Exception):
                    safe_print(f"      [retry] ❌ {c['NAMELSAD']} — skipping")

        if time_budget_hit:
            safe_print(f"\n      ⏱️  Partial POI run. Checkpoints saved — re-run with --resume.")
            gc.collect()
        else:
            pdf = pd.DataFrame(all_pois)
            if len(pdf) > 0:
                b = len(pdf)
                pdf.drop_duplicates(subset=["osm_id", "category"], keep="first", inplace=True)
                safe_print(f"      POI dedup: {b:,} → {len(pdf):,}")
                for cat, _, _ in POI_TAGS:
                    safe_print(f"      {cat:12s}  {len(pdf[pdf['category']==cat]):>6,}")

                ppq = cache_dir / f"{abbr}_pois.parquet"
                pgz = cache_dir / f"{abbr}_pois.parquet.gz"
                pdf.to_parquet(ppq, index=False)
                rp, gp = gzip_file(ppq, pgz); ppq.unlink(missing_ok=True)
                elapsed = time.time() - t0
                print(f"      Total: {len(pdf):,} POIs ({gp:.1f} MB gz, {elapsed:.0f}s)")
                if s3 and not args.local_only:
                    r2_upload(s3, pgz, bucket, f"{r2_prefix}/cache/{abbr}_pois.parquet.gz")
                    print(f"      → R2: {r2_prefix}/cache/{abbr}_pois.parquet.gz")
                del pdf
            else:
                print(f"      ❌ No POIs")
            gc.collect()
    elif do_pois and time_budget_hit:
        safe_print(f"\n  [pois] Skipped — max runtime already exceeded during roads phase.")

    # ══════════════════════════════════════════════════════════
    #  CLEANUP: Delete county temp checkpoints from R2
    #  (skip if time-budgeted — checkpoints needed for resume)
    # ══════════════════════════════════════════════════════════
    if time_budget_hit:
        elapsed_min = (time.time() - job_start) / 60
        print(f"\n  ⏱️  Time-budgeted exit after {elapsed_min:.0f}min.")
        print(f"  Checkpoints preserved in R2 — re-run with --resume to continue.")
    elif s3 and not args.local_only:
        temp_prefix = f"{r2_prefix}/cache/_county_temp/"
        print(f"\n  Cleaning up R2 temp checkpoints: {temp_prefix}")
        deleted = r2_delete_prefix(s3, bucket, temp_prefix)
        print(f"      Deleted {deleted} temp files" if deleted else "      No temp files to clean")

    status = "PARTIAL (time-budgeted)" if time_budget_hit else "DONE"
    print(f"\n{'=' * 60}")
    print(f"  {status}: {state_name} — {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
