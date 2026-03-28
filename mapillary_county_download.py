#!/usr/bin/env python3
"""
mapillary_county_download.py — County-by-county Mapillary Traffic Inventory
============================================================================
Same output as generate_mapillary_data.py but downloads one county at a time:
  1. Computes bounding box per county from us_counties.json centroids + area
  2. Downloads Mapillary features per county (adaptive tiling)
  3. Saves per-county CSV to R2: {state}/{county_slug}/traffic-inventory.csv
  4. Consolidates all counties → statewide: {state}/cache/{abbr}_mapillary.parquet.gz
  5. Skips counties already in R2 (restartable)

USAGE:
    python mapillary_county_download.py --state de
    python mapillary_county_download.py --state va
    python mapillary_county_download.py --state va --local-only
    python mapillary_county_download.py --state va --resume        # skip counties already done
    python mapillary_county_download.py --state va --county "Fairfax County"  # single county

REQUIRES: MAPILLARY_TOKEN env var or --token flag
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
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests

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
#  MAPILLARY OBJECT VALUES + CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

QUERY_VALUES = [
    "regulatory--stop--g1", "regulatory--stop--g2",
    "regulatory--yield--g1", "regulatory--yield--g2",
    "regulatory--no-u-turn--g1",
    "regulatory--no-left-turn--g1", "regulatory--no-right-turn--g1",
    "regulatory--no-parking--g2",
    "regulatory--one-way-left--g1", "regulatory--one-way-right--g1",
    "regulatory--do-not-enter--g1", "regulatory--keep-right--g1",
    *[f"regulatory--maximum-speed-limit-{s}--g{g}" for s in range(5, 80, 5) for g in [1, 2, 3]],
    "warning--stop-ahead--g1", "warning--signal-ahead--g1",
    "warning--curve-left--g1", "warning--curve-right--g1",
    "warning--turn-left--g1", "warning--turn-right--g1",
    "warning--winding-road--g1", "warning--railroad-crossing--g1",
    "warning--pedestrians-crossing--g1",
    "warning--school-zone--g1", "warning--school-zone--g2",
    "warning--children--g1",
    "object--street-light", "object--fire-hydrant",
    "object--traffic-light--general-upright-front",
    "object--traffic-light--general-horizontal-front",
    "object--traffic-light--pedestrians-front",
    "object--guard-rail", "object--bollard", "object--barrier",
    "marking--discrete--crosswalk-zebra", "marking--discrete--crosswalk-plain",
    "marking--discrete--stop-line",
]

MUTCD_PREFIXES = {
    "regulatory--stop": ("R1-1", "STOP"),
    "regulatory--yield": ("R1-2", "YIELD"),
    "regulatory--all-way": ("R1-3P", "ALL WAY"),
    "regulatory--no-u-turn": ("R3-4", "No U-Turn"),
    "regulatory--no-left-turn": ("R3-2", "No Left Turn"),
    "regulatory--no-right-turn": ("R3-1", "No Right Turn"),
    "regulatory--no-parking": ("R7-1", "No Parking"),
    "regulatory--one-way": ("R6-1", "One Way"),
    "regulatory--keep-right": ("R4-7", "Keep Right"),
    "regulatory--do-not-enter": ("R5-1", "Do Not Enter"),
    "warning--stop-ahead": ("W3-1", "Stop Ahead"),
    "warning--signal-ahead": ("W3-3", "Signal Ahead"),
    "warning--curve": ("W1-2", "Curve"),
    "warning--turn": ("W1-1", "Turn"),
    "warning--winding-road": ("W1-5", "Winding Road"),
    "warning--railroad-crossing": ("W10-1", "Railroad Xing"),
    "warning--pedestrians-crossing": ("W11-2", "Ped Crossing"),
    "warning--school-zone": ("S1-1", "School Zone"),
    "warning--children": ("W15-1", "Children"),
    "object--street-light": ("N/A", "Street Light"),
    "object--fire-hydrant": ("N/A", "Fire Hydrant"),
    "object--manhole": ("N/A", "Manhole"),
    "object--traffic-light": ("N/A", "Traffic Signal"),
    "object--guard-rail": ("N/A", "Guard Rail"),
    "object--bollard": ("N/A", "Bollard"),
    "marking--discrete--crosswalk": ("N/A", "Crosswalk"),
    "marking--discrete--stop-line": ("N/A", "Stop Line"),
}


def classify(obj_value):
    """Classify → (mutcd, name, speed, signal_heads)."""
    v = (obj_value or "").lower()
    m = re.search(r"maximum-speed-limit-(\d+)", v)
    if m:
        return "R2-1", f"Speed {m.group(1)}", m.group(1), ""

    sig = ""
    if "traffic-light" in v:
        sig = "3" if "upright" in v else ("5" if "horizontal" in v else "2")

    for prefix, (mutcd, name) in MUTCD_PREFIXES.items():
        if v.startswith(prefix):
            return mutcd, name, "", sig

    part = v.split("--")[1].replace("-", " ") if "--" in v else "Unknown"
    return "N/A", part, "", sig


# ═══════════════════════════════════════════════════════════════
#  COUNTY LOADER + BBOX COMPUTATION
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
            return [r for r in records if r["STATE"] == state_fips]
        except Exception as e:
            print(f"      R2 download failed: {e}")

    raise FileNotFoundError("us_counties.json not found")


def county_to_bbox(county):
    """Compute bounding box from county centroid + area. Returns [west, south, east, north]."""
    lat = float(county["CENTLAT"])
    lon = float(county["CENTLON"])
    area_m2 = float(county.get("AREALAND", 0)) + float(county.get("AREAWATER", 0))

    # Side length in meters, with 15% padding for irregular shapes
    side_m = math.sqrt(area_m2) * 1.15 if area_m2 > 0 else 20000  # 20km fallback

    # Convert to degrees
    lat_offset = (side_m / 2) / 111000
    lon_offset = (side_m / 2) / (111000 * math.cos(math.radians(lat)))

    return [
        round(lon - lon_offset, 6),
        round(lat - lat_offset, 6),
        round(lon + lon_offset, 6),
        round(lat + lat_offset, 6),
    ]


def county_to_slug(county):
    """Convert county name to R2 folder slug matching existing R2 structure.
    
    Examples (matching crash-lens-data R2):
        'Henrico County'     → 'henrico'
        'Carroll County'     → 'carroll'
        'Charles City County'→ 'charles_city'
        'Bristol city'       → 'bristol_city'     (VA independent city)
        'New Castle County'  → 'new_castle'
    """
    name = county.get("NAMELSAD", county.get("NAME", "unknown"))
    # Strip " County" suffix (Census uses capital C for counties)
    if name.endswith(" County"):
        name = name[:-7]
    # Strip " Municipio" (Puerto Rico)
    if name.endswith(" Municipio"):
        name = name[:-10]
    # Lowercase, replace non-alphanumeric with underscore
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug


# ═══════════════════════════════════════════════════════════════
#  TILE GENERATION + API CLIENT (from generate_mapillary_data.py)
# ═══════════════════════════════════════════════════════════════

def generate_tiles(bbox, tile_size=0.02):
    """Generate grid tiles over a bounding box."""
    w, s, e, n = bbox
    tiles = []
    lat = s
    while lat < n:
        lon = w
        while lon < e:
            tiles.append([round(lon, 6), round(lat, 6),
                          round(min(lon + tile_size, e), 6), round(min(lat + tile_size, n), 6)])
            lon += tile_size
        lat += tile_size
    return tiles


def subdivide_tile(tile, sub_size=0.009):
    """Split a tile into sub-tiles (~1km)."""
    w, s, e, n = tile
    subs = []
    lat = s
    while lat < n:
        lon = w
        while lon < e:
            subs.append([round(lon, 6), round(lat, 6),
                         round(min(lon + sub_size, e), 6), round(min(lat + sub_size, n), 6)])
            lon += sub_size
        lat += sub_size
    return subs


def fetch_tile(bbox, values, token, max_retries=5):
    """Fetch Mapillary features from one tile. Returns (features_list, hit_limit)."""
    features = []
    hit_limit = False
    BATCH = 30
    headers = {"Authorization": f"OAuth {token}"}

    for bi in range(0, len(values), BATCH):
        batch = values[bi:bi + BATCH]
        url = (f"https://graph.mapillary.com/map_features"
               f"?fields=id,object_value,geometry,first_seen_at,last_seen_at"
               f"&bbox={','.join(str(b) for b in bbox)}"
               f"&object_values={','.join(batch)}&limit=2000")

        while url:
            for attempt in range(max_retries):
                try:
                    r = requests.get(url, headers=headers, timeout=30)
                    if r.status_code == 429:
                        wait = min(2 ** attempt * 5, 60)
                        print(f"        ⚠️ Rate limit — waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    if r.status_code == 401:
                        print("\n  ❌ Invalid token")
                        sys.exit(1)
                    r.raise_for_status()
                    data = r.json()
                    page_features = data.get("data", [])
                    for f in page_features:
                        coords = f.get("geometry", {}).get("coordinates", [None, None])
                        features.append({
                            "id": f.get("id"),
                            "object_value": f.get("object_value", ""),
                            "lat": coords[1], "lon": coords[0],
                            "first_seen": str(f.get("first_seen_at", ""))[:10],
                        })
                    if len(page_features) >= 2000:
                        hit_limit = True
                    url = data.get("paging", {}).get("next")
                    break
                except (requests.ConnectionError, requests.Timeout):
                    time.sleep(2 ** attempt)
                except Exception:
                    if attempt == max_retries - 1:
                        url = None
                    time.sleep(2 ** attempt)
            else:
                url = None

    return features, hit_limit


# ═══════════════════════════════════════════════════════════════
#  PER-COUNTY DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def download_county(county, state_name, token, tile_size=0.02):
    """Download all Mapillary features for one county. Returns classified DataFrame."""
    county_name = county.get("NAMELSAD", county.get("NAME", "?"))
    bbox = county_to_bbox(county)
    tiles = generate_tiles(bbox, tile_size)

    seen_ids = set()
    all_features = []
    subdivided = 0

    for i, tile in enumerate(tiles):
        features, hit_limit = fetch_tile(tile, QUERY_VALUES, token)

        if hit_limit:
            subdivided += 1
            sub_tiles = subdivide_tile(tile, 0.009)
            for st in sub_tiles:
                sub_feats, _ = fetch_tile(st, QUERY_VALUES, token)
                for f in sub_feats:
                    if f["id"] not in seen_ids:
                        seen_ids.add(f["id"])
                        all_features.append(f)
                time.sleep(0.1)
        else:
            for f in features:
                if f["id"] not in seen_ids:
                    seen_ids.add(f["id"])
                    all_features.append(f)

        time.sleep(0.1)

    if not all_features:
        return pd.DataFrame()

    # Classify
    rows = []
    for f in all_features:
        mutcd, name, speed, sig = classify(f["object_value"])
        rows.append({
            "id": f["id"], "mutcd": mutcd, "name": name,
            "class": f["object_value"], "speed": speed,
            "lat": f["lat"], "lon": f["lon"],
            "first_seen": f["first_seen"], "signal_heads": sig,
        })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
#  R2 UTILITIES
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
        "s3", endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )


def r2_upload(s3, local_path, bucket, key):
    s3.upload_file(str(local_path), bucket, key)


def r2_exists(s3, bucket, key):
    """Check if an object exists in R2."""
    if not s3:
        return False
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="County-by-county Mapillary traffic inventory downloader",
    )
    parser.add_argument("--state", required=True, help="State abbreviation (e.g. va)")
    parser.add_argument("--token", default=os.environ.get("MAPILLARY_TOKEN", ""),
                        help="Mapillary API token (or MAPILLARY_TOKEN env)")
    parser.add_argument("--cache-dir", default="cache", help="Local cache directory")
    parser.add_argument("--tile-size", type=float, default=0.02,
                        help="Tile size in degrees (default 0.02)")
    parser.add_argument("--local-only", action="store_true", help="Skip R2 upload")
    parser.add_argument("--resume", action="store_true",
                        help="Skip counties that already have data in R2")
    parser.add_argument("--county", type=str, default=None,
                        help="Download a single county by name (e.g. 'Fairfax County')")
    args = parser.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES:
        print(f"Unknown state: {abbr}")
        sys.exit(1)

    if not args.token:
        print("❌ Token required. Set MAPILLARY_TOKEN or use --token")
        sys.exit(1)

    state_name, state_fips, r2_prefix = STATES[abbr]
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # R2 connection
    bucket = os.environ.get("R2_BUCKET", "crash-lens-data")
    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        if s3:
            print(f"R2 connected: {bucket}")
        else:
            print("R2 credentials not set — local only")

    # Load counties
    counties = load_counties(state_fips)
    total = len(counties)

    # Filter to single county if requested
    if args.county:
        counties = [c for c in counties if args.county.lower() in c.get("NAMELSAD", "").lower()]
        if not counties:
            print(f"County '{args.county}' not found in {state_name}")
            sys.exit(1)
        total = len(counties)

    print(f"\n{'=' * 65}")
    print(f"  County-by-County Mapillary Download: {state_name} ({abbr})")
    print(f"  Counties: {total}")
    print(f"  Tile size: {args.tile_size}°")
    print(f"  Resume: {'yes' if args.resume else 'no'}")
    print(f"{'=' * 65}\n")

    t0_total = time.time()
    all_county_dfs = []
    completed = 0
    skipped = 0
    failed = []

    for i, county in enumerate(counties, 1):
        county_name = county.get("NAMELSAD", county.get("NAME", "?"))
        county_slug = county_to_slug(county)
        county_r2_key = f"{r2_prefix}/{county_slug}/traffic-inventory.csv"

        # ── Resume check ──
        if args.resume and s3 and r2_exists(s3, bucket, county_r2_key):
            print(f"  [{i:3d}/{total}] {county_name} — already in R2, skipping")
            skipped += 1

            # Still need to download existing data for statewide merge
            try:
                local_csv = cache_dir / f"{abbr}_{county_slug}_mapillary.csv"
                s3.download_file(bucket, county_r2_key, str(local_csv))
                df = pd.read_csv(local_csv)
                all_county_dfs.append(df)
                local_csv.unlink(missing_ok=True)
            except Exception as e:
                print(f"        ⚠️ Could not load existing data: {e}")
            continue

        # ── Download county ──
        bbox = county_to_bbox(county)
        tiles = generate_tiles(bbox, args.tile_size)
        print(f"  [{i:3d}/{total}] {county_name} ({len(tiles)} tiles)...", end=" ", flush=True)
        t0 = time.time()

        try:
            df = download_county(county, state_name, args.token, args.tile_size)
            elapsed = time.time() - t0

            if len(df) == 0:
                print(f"0 features ({elapsed:.0f}s)")
                completed += 1
                continue

            print(f"{len(df):,} features ({elapsed:.0f}s)")
            all_county_dfs.append(df)
            completed += 1

            # ── Save per-county to R2 as traffic-inventory.csv ──
            if s3 and not args.local_only:
                county_csv = cache_dir / f"{abbr}_{county_slug}_mapillary.csv"
                df.to_csv(county_csv, index=False)
                r2_upload(s3, county_csv, bucket, county_r2_key)
                county_csv.unlink(missing_ok=True)

        except Exception as e:
            elapsed = time.time() - t0
            print(f"FAILED ({elapsed:.0f}s): {str(e)[:60]}")
            failed.append(county_name)

        # Progress update every 10 counties
        if i % 10 == 0:
            total_elapsed = time.time() - t0_total
            rate = i / total_elapsed * 60
            eta = (total - i) / max(rate, 0.1)
            total_feats = sum(len(d) for d in all_county_dfs)
            print(f"        Progress: {i}/{total} counties, {total_feats:,} features, "
                  f"ETA {eta:.0f} min")

        gc.collect()

    # ═══════════════════════════════════════════════════════════
    #  CONSOLIDATE STATEWIDE
    # ═══════════════════════════════════════════════════════════

    print(f"\n  Consolidating statewide data...")

    if not all_county_dfs:
        print(f"  ❌ No data collected")
        sys.exit(1)

    statewide = pd.concat(all_county_dfs, ignore_index=True)
    del all_county_dfs
    gc.collect()

    # Dedup by Mapillary feature ID (county boundaries overlap in bbox approximation)
    before = len(statewide)
    statewide.drop_duplicates(subset=["id"], keep="first", inplace=True)
    after = len(statewide)
    print(f"  Dedup: {before:,} → {after:,} features ({before - after:,} boundary dupes removed)")

    # Save statewide
    state_pq = cache_dir / f"{abbr}_mapillary.parquet"
    state_gz = cache_dir / f"{abbr}_mapillary.parquet.gz"
    state_csv = cache_dir / f"{abbr}_mapillary.csv"

    statewide.to_parquet(state_pq, index=False)
    statewide.to_csv(state_csv, index=False)
    raw, gz = gzip_file(state_pq, state_gz)
    state_pq.unlink(missing_ok=True)

    print(f"  Statewide: {len(statewide):,} features ({gz:.1f} MB gz)")

    # Upload statewide to R2 cache/
    if s3 and not args.local_only:
        statewide_r2_key = f"{r2_prefix}/cache/{abbr}_mapillary.parquet.gz"
        r2_upload(s3, state_gz, bucket, statewide_r2_key)
        print(f"  → uploaded to R2: {statewide_r2_key}")

    # ── Summary ──
    total_elapsed = time.time() - t0_total
    print(f"\n{'=' * 65}")
    print(f"  DONE: {state_name}")
    print(f"  Counties: {completed} completed, {skipped} skipped, {len(failed)} failed")
    print(f"  Features: {len(statewide):,}")
    print(f"  Elapsed: {total_elapsed / 60:.1f} min")
    if failed:
        print(f"  Failed: {', '.join(failed[:10])}")
    print(f"\n  Top categories:")
    for code, count in statewide["mutcd"].value_counts().head(10).items():
        print(f"    {code:<8} {count:>8,}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
