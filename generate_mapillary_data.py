#!/usr/bin/env python3
"""
generate_mapillary_data.py — Statewide Mapillary Traffic Inventory Downloader

Adaptive tiling strategy (inspired by CrashLens HTML downloader v4.7):
  1. Start with 0.03° tiles (~3.3km — fast for rural)
  2. If a tile returns 2000 results (API limit): subdivide into 4 sub-tiles
  3. Sub-tiles use 0.009° (~1km — matches HTML downloader exactly)
  4. TIGER state boundary filter: skip ocean/empty tiles

SCALE (with adaptive tiling):
  Delaware:  ~1,100 tiles,    ~55K features,   ~5 min
  Virginia:  ~27K tiles,   ~500K features,   ~90 min
  Texas:     ~155K tiles,    ~2M features,   ~10 hours (needs batch split)

USAGE:
  export MAPILLARY_TOKEN=MLY|...
  python generate_mapillary_data.py --state de
  python generate_mapillary_data.py --state va --upload
"""

import argparse, gc, math, os, re, sys, time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests

# ─── STATE BOUNDING BOXES ────────────────────────────────────────────────────

STATE_BBOX = {
    "al":[-88.473,30.223,-84.889,35.008],"ak":[-179.149,51.215,-129.98,71.365],
    "az":[-114.814,31.332,-109.045,37.004],"ar":[-94.618,33.004,-89.644,36.5],
    "ca":[-124.409,32.534,-114.131,42.009],"co":[-109.06,36.993,-102.042,41.003],
    "ct":[-73.728,40.987,-71.787,42.05],"de":[-75.789,38.451,-75.049,39.839],
    "dc":[-77.12,38.792,-76.909,38.996],
    "fl":[-87.635,24.523,-80.031,31.001],"ga":[-85.605,30.358,-80.84,35.001],
    "hi":[-160.244,18.91,-154.807,22.236],"id":[-117.243,41.988,-111.044,49.001],
    "il":[-91.513,36.97,-87.02,42.509],"ia":[-96.639,40.375,-90.14,43.501],
    "ks":[-102.052,36.993,-94.589,40.003],"ky":[-89.571,36.497,-81.965,39.148],
    "la":[-94.043,28.928,-88.817,33.019],"me":[-71.084,42.977,-66.95,47.46],
    "md":[-79.488,37.912,-75.049,39.723],"ma":[-73.508,41.237,-69.929,42.887],
    "mi":[-90.418,41.697,-82.414,48.306],"mn":[-97.239,43.5,-89.489,49.384],
    "ms":[-91.655,30.174,-88.098,34.996],"mo":[-95.774,35.995,-89.099,40.613],
    "mt":[-116.05,44.358,-104.04,49.001],"ne":[-104.054,39.999,-95.308,43.002],
    "nv":[-120.006,35.002,-114.04,42.002],"nh":[-72.557,42.697,-70.703,45.306],
    "nj":[-75.563,38.929,-73.894,41.357],"nm":[-109.05,31.332,-103.002,37.0],
    "ny":[-79.762,40.496,-71.856,45.016],"nc":[-84.322,33.842,-75.461,36.588],
    "nd":[-104.049,45.935,-96.554,49.001],"oh":[-84.82,38.403,-80.519,42.327],
    "ok":[-103.003,33.616,-94.431,37.002],"or":[-124.567,41.992,-116.464,46.292],
    "pa":[-80.52,39.72,-74.69,42.27],"ri":[-71.863,41.147,-71.12,42.019],
    "sc":[-83.354,32.035,-78.542,35.216],"sd":[-104.058,42.48,-96.436,45.945],
    "tn":[-90.31,34.983,-81.647,36.678],"tx":[-106.646,25.837,-93.508,36.501],
    "ut":[-114.053,36.998,-109.041,42.001],"vt":[-73.438,42.727,-71.465,45.017],
    "va":[-83.675,36.541,-75.242,39.466],"wa":[-124.849,45.544,-116.916,49.002],
    "wv":[-82.644,37.202,-77.719,40.638],"wi":[-92.889,42.492,-86.25,47.08],
    "wy":[-111.057,40.995,-104.052,45.006],
}

# ─── OBJECT VALUES TO QUERY ──────────────────────────────────────────────────
# Matches HTML downloader MUTCD_MAPPINGS exactly

QUERY_VALUES = [
    # Regulatory signs
    "regulatory--stop--g1","regulatory--stop--g2",
    "regulatory--yield--g1","regulatory--yield--g2",
    "regulatory--no-u-turn--g1",
    "regulatory--no-left-turn--g1","regulatory--no-right-turn--g1",
    "regulatory--no-parking--g2",
    "regulatory--one-way-left--g1","regulatory--one-way-right--g1",
    "regulatory--do-not-enter--g1","regulatory--keep-right--g1",
    # Speed limits 5-75 mph (3 class variants each — matches HTML)
    *[f"regulatory--maximum-speed-limit-{s}--g{g}" for s in range(5,80,5) for g in [1,2,3]],
    # Warning signs
    "warning--stop-ahead--g1","warning--signal-ahead--g1",
    "warning--curve-left--g1","warning--curve-right--g1",
    "warning--turn-left--g1","warning--turn-right--g1",
    "warning--winding-road--g1","warning--railroad-crossing--g1",
    "warning--pedestrians-crossing--g1",
    "warning--school-zone--g1","warning--school-zone--g2",
    "warning--children--g1",
    # Infrastructure objects
    "object--street-light","object--fire-hydrant",
    "object--traffic-light--general-upright-front",
    "object--traffic-light--general-horizontal-front",
    "object--traffic-light--pedestrians-front",
    "object--guard-rail","object--bollard","object--barrier",
    # Road markings
    "marking--discrete--crosswalk-zebra","marking--discrete--crosswalk-plain",
    "marking--discrete--stop-line",
]

# ─── MUTCD CLASSIFICATION ────────────────────────────────────────────────────

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


# ═════════════════════════════════════════════════════════════════════════════
#  ADAPTIVE TILE GENERATOR
# ═════════════════════════════════════════════════════════════════════════════

def generate_tiles(bbox, tile_size=0.02):
    """Generate grid tiles. Default 0.02° (~2.2km)."""
    w, s, e, n = bbox
    tiles = []
    lat = s
    while lat < n:
        lon = w
        while lon < e:
            tiles.append([round(lon,6), round(lat,6),
                          round(min(lon+tile_size,e),6), round(min(lat+tile_size,n),6)])
            lon += tile_size
        lat += tile_size
    return tiles


def subdivide_tile(tile, sub_size=0.009):
    """Split a tile into sub-tiles (~1km, matching HTML downloader)."""
    w, s, e, n = tile
    subs = []
    lat = s
    while lat < n:
        lon = w
        while lon < e:
            subs.append([round(lon,6), round(lat,6),
                         round(min(lon+sub_size,e),6), round(min(lat+sub_size,n),6)])
            lon += sub_size
        lat += sub_size
    return subs


# ═════════════════════════════════════════════════════════════════════════════
#  TIGER BOUNDARY FILTER (from your HTML downloader)
# ═════════════════════════════════════════════════════════════════════════════

def fetch_state_boundary(state_fips):
    """Download state boundary polygon from TIGER for tile filtering."""
    urls = [
        f"https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/84/query"
        f"?where=STATEFP%3D%27{state_fips}%27&outFields=GEOID,NAME&f=geojson&outSR=4326",
        f"https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/0/query"
        f"?where=STATEFP%3D%27{state_fips}%27&outFields=GEOID,NAME&f=geojson&outSR=4326",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if features:
                    return features[0].get("geometry", {}).get("coordinates")
        except Exception:
            continue
    return None


def point_in_bbox(lat, lon, tile):
    """Quick check: is point inside tile bbox?"""
    return tile[0] <= lon <= tile[2] and tile[1] <= lat <= tile[3]


def tile_intersects_land(tile, boundary_coords):
    """Check if tile bbox overlaps with state boundary polygon.
    Simplified: checks if tile center is roughly near any boundary segment.
    For accuracy we just check if the tile isn't entirely in ocean."""
    if boundary_coords is None:
        return True  # No boundary = assume all tiles are land

    cx = (tile[0] + tile[2]) / 2
    cy = (tile[1] + tile[3]) / 2
    tile_diag = math.sqrt((tile[2]-tile[0])**2 + (tile[3]-tile[1])**2)

    # Flatten all coordinate rings
    try:
        def flatten(coords):
            if isinstance(coords[0], (int, float)):
                return [coords]
            result = []
            for c in coords:
                result.extend(flatten(c))
            return result

        points = flatten(boundary_coords)
        # Check if any boundary point is within 2x tile diagonal
        threshold = tile_diag * 2
        for pt in points[:5000]:  # Sample for speed
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                if abs(pt[0] - cx) < threshold and abs(pt[1] - cy) < threshold:
                    return True
    except Exception:
        return True

    return False


# State FIPS codes
STATE_FIPS = {
    "al":"01","ak":"02","az":"04","ar":"05","ca":"06","co":"08",
    "ct":"09","de":"10","dc":"11","fl":"12","ga":"13","hi":"15",
    "id":"16","il":"17","ia":"19","ks":"20","ky":"21","la":"22",
    "me":"23","md":"24","ma":"25","mi":"26","mn":"27","ms":"28",
    "mo":"29","mt":"30","ne":"31","nv":"32","nh":"33","nj":"34",
    "nm":"35","ny":"36","nc":"37","nd":"38","oh":"39","ok":"40",
    "or":"41","pa":"42","ri":"44","sc":"45","sd":"46","tn":"47",
    "tx":"48","ut":"49","vt":"50","va":"51","wa":"53","wv":"54",
    "wi":"55","wy":"56",
}


# ═════════════════════════════════════════════════════════════════════════════
#  API CLIENT (matches HTML downloader logic)
# ═════════════════════════════════════════════════════════════════════════════

def fetch_tile(bbox, values, token, max_retries=5):
    """Fetch features from one tile. Returns (features_list, hit_limit)."""
    features = []
    hit_limit = False
    BATCH = 30  # Match HTML downloader BATCH_SIZE
    headers = {"Authorization": f"OAuth {token}"}

    for bi in range(0, len(values), BATCH):
        batch = values[bi:bi+BATCH]
        url = (f"https://graph.mapillary.com/map_features"
               f"?fields=id,object_value,geometry,first_seen_at,last_seen_at"
               f"&bbox={','.join(str(b) for b in bbox)}"
               f"&object_values={','.join(batch)}&limit=2000")

        while url:
            for attempt in range(max_retries):
                try:
                    r = requests.get(url, headers=headers, timeout=30)
                    if r.status_code == 429:
                        wait = min(2**attempt * 5, 60)
                        print(f"    ⚠️ Rate limit — waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    if r.status_code == 401:
                        print("\n  ❌ Invalid token. Get one at https://www.mapillary.com/dashboard/developers")
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
                    time.sleep(2**attempt)
                except Exception as e:
                    if attempt == max_retries - 1:
                        url = None
                    time.sleep(2**attempt)
            else:
                url = None

    return features, hit_limit


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

    part = v.split("--")[1].replace("-"," ") if "--" in v else "Unknown"
    return "N/A", part, "", sig


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN: ADAPTIVE STATEWIDE DOWNLOAD
# ═════════════════════════════════════════════════════════════════════════════

def download_state(abbr, token, cache_dir="cache", initial_tile_size=0.03):
    """Download all Mapillary features using adaptive tiling.

    Strategy:
      1. Generate tiles at initial_tile_size (0.03° ≈ 3.3km)
      2. Filter tiles using TIGER state boundary (skip ocean/empty)
      3. Query each tile — if API returns 2000 (limit hit):
         → Subdivide into 0.009° sub-tiles (matches HTML downloader)
         → Re-query sub-tiles individually
      4. Deduplicate by feature ID across all tiles
    """
    if abbr not in STATE_BBOX:
        print(f"  ❌ Unknown state: {abbr}")
        return None

    bbox = STATE_BBOX[abbr]

    # Generate initial tiles
    tiles = generate_tiles(bbox, initial_tile_size)
    initial_count = len(tiles)

    # TIGER boundary filter
    fips = STATE_FIPS.get(abbr)
    boundary = None
    if fips:
        print(f"  Fetching TIGER boundary for {abbr.upper()} (FIPS {fips})...")
        boundary = fetch_state_boundary(fips)
        if boundary:
            before = len(tiles)
            tiles = [t for t in tiles if tile_intersects_land(t, boundary)]
            after = len(tiles)
            pct = (1 - after/before) * 100 if before else 0
            print(f"  📐 TIGER filter: {before:,} → {after:,} tiles ({pct:.0f}% removed)")
        else:
            print(f"  ⚠️ TIGER boundary not available — using all tiles")

    t0 = time.time()
    print(f"\n{'═'*65}")
    print(f"  Mapillary Adaptive Downloader | {abbr.upper()}")
    print(f"  Tiles: {len(tiles):,} (from {initial_count:,}, {initial_tile_size}° grid)")
    print(f"  Object types: {len(QUERY_VALUES)}")
    print(f"  Adaptive: subdivides to 0.009° on dense tiles (like HTML tool)")
    print(f"{'═'*65}\n")

    seen_ids = set()
    all_features = []
    subdivided = 0
    empty = 0

    for i, tile in enumerate(tiles):
        if i > 0 and i % 200 == 0:
            el = time.time() - t0
            rate = i / el * 60
            eta = (len(tiles) - i) / max(rate, 1)
            print(f"  [{i:,}/{len(tiles):,}] {len(all_features):,} features | "
                  f"{rate:.0f} tiles/min | ETA {eta:.0f} min | {subdivided} subdivided")

        features, hit_limit = fetch_tile(tile, QUERY_VALUES, token)

        if hit_limit:
            # Dense area — subdivide like HTML downloader (0.009°)
            subdivided += 1
            sub_tiles = subdivide_tile(tile, 0.009)
            for st in sub_tiles:
                sub_feats, _ = fetch_tile(st, QUERY_VALUES, token)
                for f in sub_feats:
                    if f["id"] not in seen_ids:
                        seen_ids.add(f["id"])
                        all_features.append(f)
                time.sleep(0.1)  # 100ms — matches HTML downloader sleep
        else:
            for f in features:
                if f["id"] not in seen_ids:
                    seen_ids.add(f["id"])
                    all_features.append(f)
            if not features:
                empty += 1

        time.sleep(0.1)  # 100ms between tiles — matches HTML downloader

    elapsed = time.time() - t0
    print(f"\n  Download: {len(all_features):,} features in {elapsed/60:.1f} min")
    print(f"  Tiles: {len(tiles)-empty:,} with data, {empty:,} empty, {subdivided:,} subdivided")

    if not all_features:
        print("  ❌ No features — check token")
        return None

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

    df = pd.DataFrame(rows)

    # Save
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    pq = Path(cache_dir) / f"{abbr}_mapillary.parquet"
    cv = Path(cache_dir) / f"{abbr}_mapillary.csv"
    df.to_parquet(pq, index=False)
    df.to_csv(cv, index=False)

    print(f"\n{'═'*65}")
    print(f"  ✅ {len(df):,} features → {pq} ({pq.stat().st_size/1e6:.1f} MB)")
    print(f"  Top categories:")
    for code, count in df["mutcd"].value_counts().head(10).items():
        print(f"    {code:<8} {count:>8,}")
    print(f"  Elapsed: {elapsed/60:.1f} min")
    print(f"{'═'*65}\n")
    return df


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Download Mapillary traffic inventory (adaptive tiling)")
    p.add_argument("--state", required=True, help="State abbreviation (de, va, tx)")
    p.add_argument("--token", default=os.environ.get("MAPILLARY_TOKEN",""),
                   help="Mapillary API token (or MAPILLARY_TOKEN env)")
    p.add_argument("--cache-dir", default="cache")
    p.add_argument("--tile-size", type=float, default=0.03,
                   help="Initial tile size (0.03° default, subdivides to 0.009° on dense tiles)")
    p.add_argument("--upload", action="store_true")
    a = p.parse_args()

    if not a.token:
        print("❌ Token required. Set MAPILLARY_TOKEN or use --token")
        print("   https://www.mapillary.com/dashboard/developers")
        sys.exit(1)

    result = download_state(a.state.lower(), a.token, a.cache_dir, a.tile_size)
    if result is None:
        sys.exit(1)

    if a.upload:
        ab = a.state.lower()
        gz = Path(a.cache_dir) / f"{ab}_mapillary.parquet.gz"
        os.system(f"gzip -cn {Path(a.cache_dir)}/{ab}_mapillary.parquet > {gz}")
        os.system(f'aws s3 cp {gz} s3://crash-lens-data/{ab}/cache/{ab}_mapillary.parquet.gz '
                  f'--endpoint-url https://${{CF_ACCOUNT_ID}}.r2.cloudflarestorage.com --only-show-errors')
        print("  ✅ Uploaded to R2")
