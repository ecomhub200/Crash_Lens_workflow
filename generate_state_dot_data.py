#!/usr/bin/env python3
"""
generate_state_dot_data.py — CrashLens State DOT Inventory Downloader
=======================================================================
Generic ArcGIS FeatureServer downloader that works for ANY state.
Each state has a config file at states/{state}/{abbr}_state_dot.py
defining the endpoint URL, field mappings, and value transforms.

This script handles:
  1. ArcGIS FeatureServer paginated download (resultOffset/resultRecordCount)
  2. Geometry extraction (polyline → midpoint + u/v endpoints)
  3. Field renaming via state config FIELD_MAP
  4. State-specific normalization via config normalize() function
  5. Parquet.gz output
  6. R2 upload

Output: {state_prefix}/cache/{abbr}_state_dot.parquet.gz
  Used by build_road_inventory.py as Tier B (State DOT) data source.

Usage:
  python generate_state_dot_data.py --state de
  python generate_state_dot_data.py --state de --upload
  python generate_state_dot_data.py --state de --cache-dir cache

Requires: requests, pandas, pyarrow
"""

import argparse
import gzip
import importlib
import io
import json
import math
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests


# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY: state abbreviation → (name, r2_prefix, fips)
#  Same registry as build_road_inventory.py / generate_hpms_data.py
# ═══════════════════════════════════════════════════════════════

STATES = {
    "al": ("Alabama", "alabama", "01"), "ak": ("Alaska", "alaska", "02"),
    "az": ("Arizona", "arizona", "04"), "ar": ("Arkansas", "arkansas", "05"),
    "ca": ("California", "california", "06"), "co": ("Colorado", "colorado", "08"),
    "ct": ("Connecticut", "connecticut", "09"), "de": ("Delaware", "delaware", "10"),
    "dc": ("District of Columbia", "district_of_columbia", "11"),
    "fl": ("Florida", "florida", "12"), "ga": ("Georgia", "georgia", "13"),
    "hi": ("Hawaii", "hawaii", "15"), "id": ("Idaho", "idaho", "16"),
    "il": ("Illinois", "illinois", "17"), "in": ("Indiana", "indiana", "18"),
    "ia": ("Iowa", "iowa", "19"), "ks": ("Kansas", "kansas", "20"),
    "ky": ("Kentucky", "kentucky", "21"), "la": ("Louisiana", "louisiana", "22"),
    "me": ("Maine", "maine", "23"), "md": ("Maryland", "maryland", "24"),
    "ma": ("Massachusetts", "massachusetts", "25"), "mi": ("Michigan", "michigan", "26"),
    "mn": ("Minnesota", "minnesota", "27"), "ms": ("Mississippi", "mississippi", "28"),
    "mo": ("Missouri", "missouri", "29"), "mt": ("Montana", "montana", "30"),
    "ne": ("Nebraska", "nebraska", "31"), "nv": ("Nevada", "nevada", "32"),
    "nh": ("New Hampshire", "new_hampshire", "33"), "nj": ("New Jersey", "new_jersey", "34"),
    "nm": ("New Mexico", "new_mexico", "35"), "ny": ("New York", "new_york", "36"),
    "nc": ("North Carolina", "north_carolina", "37"), "nd": ("North Dakota", "north_dakota", "38"),
    "oh": ("Ohio", "ohio", "39"), "ok": ("Oklahoma", "oklahoma", "40"),
    "or": ("Oregon", "oregon", "41"), "pa": ("Pennsylvania", "pennsylvania", "42"),
    "ri": ("Rhode Island", "rhode_island", "44"), "sc": ("South Carolina", "south_carolina", "45"),
    "sd": ("South Dakota", "south_dakota", "46"), "tn": ("Tennessee", "tennessee", "47"),
    "tx": ("Texas", "texas", "48"), "ut": ("Utah", "utah", "49"),
    "vt": ("Vermont", "vermont", "50"), "va": ("Virginia", "virginia", "51"),
    "wa": ("Washington", "washington", "53"), "wv": ("West Virginia", "west_virginia", "54"),
    "wi": ("Wisconsin", "wisconsin", "55"), "wy": ("Wyoming", "wyoming", "56"),
}

M_TO_FT = 3.28084


# ═══════════════════════════════════════════════════════════════
#  LOAD STATE CONFIG
# ═══════════════════════════════════════════════════════════════

def load_state_config(abbr):
    """
    Import state-specific config from states/{state}/{abbr}_state_dot.py.

    Expected exports:
      ENDPOINT_URL           str — ArcGIS FeatureServer URL
      MAX_RECORD_COUNT       int — pagination batch size
      OUT_SR                 int — output spatial reference (4326)
      OUT_FIELDS             list or None — fields to request
      FIELD_MAP              dict — {source_field: target_field}
      normalize(df)          function — state-specific value transforms

    Returns module or raises ImportError.
    """
    state_name = STATES[abbr][0].lower().replace(" ", "_")
    module_name = f"states.{state_name}.{abbr}_state_dot"

    # Try import from package path
    try:
        mod = importlib.import_module(module_name)
        print(f"  Loaded config: {module_name}")
        return mod
    except ImportError:
        pass

    # Fallback: try adding states/ to sys.path
    states_dir = Path(__file__).parent / "states" / state_name
    if states_dir.exists():
        sys.path.insert(0, str(states_dir))
        try:
            mod = importlib.import_module(f"{abbr}_state_dot")
            print(f"  Loaded config: {states_dir / f'{abbr}_state_dot.py'}")
            return mod
        except ImportError:
            pass

    raise ImportError(
        f"No state DOT config found for '{abbr}'. "
        f"Expected: states/{state_name}/{abbr}_state_dot.py"
    )


# ═══════════════════════════════════════════════════════════════
#  ARCGIS FEATURESERVER DOWNLOADER
# ═══════════════════════════════════════════════════════════════

def download_features(endpoint_url, max_record_count=1000, out_sr=4326,
                      out_fields=None, timeout=120):
    """
    Download all features from an ArcGIS FeatureServer via pagination.

    Args:
        endpoint_url: FeatureServer layer URL (e.g. .../FeatureServer/0)
        max_record_count: Batch size per request
        out_sr: Output spatial reference (4326 = WGS84)
        out_fields: List of field names to request, or None for all
        timeout: Request timeout in seconds

    Returns:
        (features, fields) — list of feature dicts, list of field metadata dicts
    """
    query_url = f"{endpoint_url}/query"

    # First: get total count
    count_params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    }
    resp = requests.get(query_url, params=count_params, timeout=timeout)
    resp.raise_for_status()
    total = resp.json().get("count", 0)
    print(f"  Total features: {total:,}")

    if total == 0:
        return [], []

    # Get field metadata
    meta_resp = requests.get(f"{endpoint_url}?f=json", timeout=timeout)
    meta_resp.raise_for_status()
    fields = meta_resp.json().get("fields", [])

    # Paginate
    all_features = []
    offset = 0
    batch_num = 0

    while offset < total:
        params = {
            "where": "1=1",
            "outFields": ",".join(out_fields) if out_fields else "*",
            "outSR": out_sr,
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": max_record_count,
            "orderByFields": "OBJECTID ASC",
            "f": "json",
        }

        for attempt in range(3):
            try:
                resp = requests.get(query_url, params=params, timeout=timeout)
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if attempt < 2:
                    wait = (attempt + 1) * 10
                    print(f"    Retry {attempt + 1}/3 in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    raise

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += len(features)
        batch_num += 1

        if batch_num % 10 == 0 or offset >= total:
            print(f"    Downloaded {offset:,}/{total:,} ({offset/total*100:.0f}%)")

        # Respect server rate limits
        time.sleep(0.2)

        # Safety: check for exceededTransferLimit
        if not data.get("exceededTransferLimit", True) and offset < total:
            if len(features) < max_record_count:
                break

    print(f"  Downloaded {len(all_features):,} features in {batch_num} batches")
    return all_features, fields


# ═══════════════════════════════════════════════════════════════
#  GEOMETRY EXTRACTION
# ═══════════════════════════════════════════════════════════════

def extract_geometry(features):
    """
    Extract polyline geometry into structured columns.

    From each polyline, extracts:
      u_lat, u_lon — first coordinate (start point)
      v_lat, v_lon — last coordinate (end point)
      mid_lat, mid_lon — midpoint of the polyline
      length_m — approximate length in meters
      geometry_coords — JSON string of all coordinates

    Returns dict of arrays (one per column).
    """
    n = len(features)
    u_lat = np.zeros(n, dtype=np.float64)
    u_lon = np.zeros(n, dtype=np.float64)
    v_lat = np.zeros(n, dtype=np.float64)
    v_lon = np.zeros(n, dtype=np.float64)
    mid_lat = np.zeros(n, dtype=np.float64)
    mid_lon = np.zeros(n, dtype=np.float64)
    length_m = np.zeros(n, dtype=np.float32)
    geom_strs = []

    for i, feat in enumerate(features):
        geom = feat.get("geometry", {})
        paths = geom.get("paths", [])

        if not paths or not paths[0]:
            geom_strs.append("[]")
            continue

        # Take first path
        coords = paths[0]

        # Start/end points
        u_lon[i], u_lat[i] = coords[0][0], coords[0][1]
        v_lon[i], v_lat[i] = coords[-1][0], coords[-1][1]

        # Midpoint (average of all coordinates)
        lats = [c[1] for c in coords]
        lons = [c[0] for c in coords]
        mid_lat[i] = np.mean(lats)
        mid_lon[i] = np.mean(lons)

        # Approximate length (haversine between consecutive points)
        total_m = 0
        for j in range(1, len(coords)):
            lat1, lon1 = math.radians(coords[j-1][1]), math.radians(coords[j-1][0])
            lat2, lon2 = math.radians(coords[j][1]), math.radians(coords[j][0])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            total_m += 6371000 * 2 * math.asin(math.sqrt(a))
        length_m[i] = total_m

        # Store geometry as JSON string
        geom_strs.append(json.dumps(coords))

    return {
        "u_lat": u_lat, "u_lon": u_lon,
        "v_lat": v_lat, "v_lon": v_lon,
        "mid_lat": mid_lat, "mid_lon": mid_lon,
        "length_m": length_m.astype(np.float32),
        "length_ft": (length_m * M_TO_FT).astype(np.float32),
        "geometry_coords": geom_strs,
    }


# ═══════════════════════════════════════════════════════════════
#  BUILD DATAFRAME
# ═══════════════════════════════════════════════════════════════

def features_to_dataframe(features, field_map):
    """
    Convert ArcGIS features to a pandas DataFrame with mapped column names.

    Args:
        features: List of feature dicts from ArcGIS query
        field_map: Dict mapping source field → target field name

    Returns:
        DataFrame with geometry columns + mapped attribute columns
    """
    # Extract attributes
    records = []
    for feat in features:
        attrs = feat.get("attributes", {})
        records.append(attrs)

    df = pd.DataFrame(records)
    print(f"  Raw attributes: {len(df):,} rows × {len(df.columns)} cols")

    # Extract geometry
    print(f"  Extracting geometry...", end=" ", flush=True)
    geom = extract_geometry(features)
    for col, vals in geom.items():
        df[col] = vals
    print(f"done")

    # Apply field mapping
    rename = {}
    for src, tgt in field_map.items():
        if src in df.columns:
            rename[src] = tgt
    df.rename(columns=rename, inplace=True)

    # Prefix remaining unmapped source columns with "dot_raw_"
    mapped_targets = set(field_map.values())
    geom_cols = set(geom.keys())
    for col in df.columns:
        if col not in mapped_targets and col not in geom_cols and not col.startswith("dot_"):
            df.rename(columns={col: f"dot_raw_{col}"}, inplace=True)

    return df


# ═══════════════════════════════════════════════════════════════
#  R2 UPLOAD
# ═══════════════════════════════════════════════════════════════

def upload_to_r2(local_path, r2_key, bucket="crash-lens-data"):
    """Upload a file to Cloudflare R2."""
    try:
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("R2_ENDPOINT_URL",
                f"https://{os.environ.get('R2_ACCOUNT_ID', '')}.r2.cloudflarestorage.com"),
            aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        )
        s3.upload_file(str(local_path), bucket, r2_key)
        print(f"  Uploaded to R2: {r2_key}")
        return True
    except Exception as e:
        print(f"  R2 upload failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Download and normalize state DOT road inventory"
    )
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation")
    parser.add_argument("--cache-dir", default="cache", help="Local cache directory")
    parser.add_argument("--upload", action="store_true", help="Upload to R2")
    parser.add_argument("--force", action="store_true", help="Re-download even if cache exists")
    parser.add_argument("--timeout", type=int, default=120, help="Request timeout seconds")
    args = parser.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES:
        print(f"Unknown state: {abbr}")
        sys.exit(1)

    state_name, r2_prefix, state_fips = STATES[abbr]
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    out_name = f"{abbr}_state_dot"
    out_pq = cache_dir / f"{out_name}.parquet"
    out_gz = cache_dir / f"{out_name}.parquet.gz"

    print(f"{'='*60}")
    print(f"  State DOT Inventory: {state_name} ({abbr.upper()})")
    print(f"{'='*60}")

    # Check cache
    if out_gz.exists() and not args.force:
        print(f"  Cache exists: {out_gz} ({out_gz.stat().st_size / 1024 / 1024:.1f} MB)")
        print(f"  Use --force to re-download")
        if args.upload:
            r2_key = f"{r2_prefix}/cache/{out_name}.parquet.gz"
            upload_to_r2(out_gz, r2_key)
        return

    # Load state config
    config = load_state_config(abbr)
    endpoint = config.ENDPOINT_URL
    max_rec = getattr(config, "MAX_RECORD_COUNT", 1000)
    out_sr = getattr(config, "OUT_SR", 4326)
    out_fields = getattr(config, "OUT_FIELDS", None)
    field_map = config.FIELD_MAP

    print(f"  Endpoint: {endpoint}")
    print(f"  Batch size: {max_rec}")

    # Download
    t0 = time.time()
    features, fields = download_features(
        endpoint, max_record_count=max_rec, out_sr=out_sr,
        out_fields=out_fields, timeout=args.timeout
    )

    if not features:
        print(f"  No features returned — endpoint may be down")
        sys.exit(1)

    download_time = time.time() - t0
    print(f"  Download: {download_time:.1f}s")

    # Build DataFrame
    df = features_to_dataframe(features, field_map)

    # Normalize (state-specific value transforms)
    print(f"  Normalizing...", end=" ", flush=True)
    if hasattr(config, "normalize"):
        df = config.normalize(df)
    print(f"done → {len(df):,} rows × {len(df.columns)} cols")

    # Add metadata columns
    df["road_source"] = "StateDOT"
    df["dot_state_abbr"] = abbr.upper()
    df["dot_state_fips"] = state_fips

    # Save parquet
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"  Saving {out_pq}...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_pq)

    # Compress
    print(f"  Compressing...", end=" ", flush=True)
    with open(out_pq, "rb") as f_in:
        with gzip.open(out_gz, "wb", compresslevel=6) as f_out:
            f_out.write(f_in.read())
    size_mb = out_gz.stat().st_size / 1024 / 1024
    print(f"{size_mb:.1f} MB")

    # Cleanup uncompressed
    out_pq.unlink(missing_ok=True)

    # Upload to R2
    if args.upload:
        r2_key = f"{r2_prefix}/cache/{out_name}.parquet.gz"
        upload_to_r2(out_gz, r2_key)

    total_time = time.time() - t0
    print(f"\n  {'='*50}")
    print(f"  Complete: {len(df):,} segments in {total_time:.1f}s")
    print(f"  Output: {out_gz}")
    print(f"  {'='*50}")

    # Print coverage summary
    print(f"\n  Coverage Summary:")
    for col in ["Functional Class", "Ownership", "Through_Lanes", "Roadway Surface Type",
                 "RTE Name", "DOT District", "Has_Sidewalk", "Area Type"]:
        if col in df.columns:
            pop = (df[col].fillna("").astype(str).str.strip() != "").sum()
            print(f"    {col:25s}: {pop:>6,}/{len(df):,} ({pop/len(df)*100:5.1f}%)")


if __name__ == "__main__":
    main()
