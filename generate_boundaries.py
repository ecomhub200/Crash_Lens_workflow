#!/usr/bin/env python3
"""
generate_boundaries.py — Download US Boundary Polygons for CrashLens
=====================================================================
Downloads actual polygon geometries (not just centroids) from Census
TIGERweb and BTS/FHWA ArcGIS services. Enables vectorized point-in-polygon
(geopandas.sjoin) instead of row-by-row API calls.

Layers downloaded:
  1. States           — 52 polygons   (TIGERweb Layer 84)
  2. Counties         — 3,222 polygons (TIGERweb Layer 86)
  3. Places (cities)  — ~30,000 polygons (TIGERweb Layer 28)
  4. County Subdiv.   — ~36,000 polygons (TIGERweb Layer 30)
  5. MPOs             — ~400 polygons  (BTS ArcGIS)

Output (R2: _national/boundaries/):
  us_state_boundaries.parquet.gz
  us_county_boundaries.parquet.gz
  us_place_boundaries.parquet.gz
  us_county_subdivision_boundaries.parquet.gz
  us_mpo_boundaries.parquet.gz

Usage:
    python generate_boundaries.py                    # All layers
    python generate_boundaries.py --layer counties   # Single layer
    python generate_boundaries.py --upload            # Upload to R2
    python generate_boundaries.py --force             # Re-download cached

Performance:
    Download: ~5-10 min (paginated ArcGIS queries)
    sjoin 566K crashes × 3,222 counties: ~1-2 seconds
    sjoin 566K crashes × 30K places: ~3-5 seconds
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

# ── ArcGIS REST API endpoints ──

TIGERWEB_BASE = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb"

LAYERS = {
    "states": {
        "url": f"{TIGERWEB_BASE}/tigerWMS_Current/MapServer/84/query",
        "fields": "GEOID,STATE,NAME,BASENAME,NAMELSAD,LSADC,USPS,AREALAND,AREAWATER,CENTLAT,CENTLON",
        "where": "1=1",
        "filename": "us_state_boundaries.parquet.gz",
        "desc": "US State boundaries (52 polygons)",
        "page_size": 100,
    },
    "counties": {
        "url": f"{TIGERWEB_BASE}/tigerWMS_Current/MapServer/86/query",
        "fields": "GEOID,STATE,COUNTY,NAME,BASENAME,NAMELSAD,LSADC,USPS,AREALAND,AREAWATER,CENTLAT,CENTLON",
        "where": "1=1",
        "filename": "us_county_boundaries.parquet.gz",
        "desc": "US County boundaries (3,222 polygons)",
        "page_size": 500,
    },
    "places": {
        "url": f"{TIGERWEB_BASE}/tigerWMS_Current/MapServer/28/query",
        "fields": "GEOID,STATE,PLACE,NAME,BASENAME,NAMELSAD,LSADC,USPS,AREALAND,AREAWATER,CENTLAT,CENTLON,FUNCSTAT",
        "where": "FUNCSTAT='A'",  # Active places only
        "filename": "us_place_boundaries.parquet.gz",
        "desc": "US Place/City boundaries (~30,000 polygons)",
        "page_size": 500,
    },
    "county_subdivisions": {
        "url": f"{TIGERWEB_BASE}/tigerWMS_Current/MapServer/30/query",
        "fields": "GEOID,STATE,COUNTY,COUSUB,NAME,BASENAME,NAMELSAD,LSADC,USPS,AREALAND,CENTLAT,CENTLON,FUNCSTAT",
        "where": "FUNCSTAT='A'",
        "filename": "us_county_subdivision_boundaries.parquet.gz",
        "desc": "US County Subdivision boundaries (~36,000 polygons)",
        "page_size": 500,
    },
    "mpos": {
        "url": "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/Metropolitan_Planning_Organizations_(MPO)_Boundaries/FeatureServer/0/query",
        "fields": "*",
        "where": "1=1",
        "filename": "us_mpo_boundaries.parquet.gz",
        "desc": "US MPO boundaries (~400 polygons)",
        "page_size": 200,
    },
}

# Per-state FIPS for state-by-state download fallback
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}


def _paginated_geojson_download(url, fields, where, page_size=500, max_records=None):
    """Download all records from an ArcGIS REST endpoint with pagination.

    Returns list of GeoJSON features with geometry.
    """
    import requests

    all_features = []
    offset = 0
    total_expected = None

    while True:
        params = {
            "where": where,
            "outFields": fields,
            "returnGeometry": "true",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        for attempt in range(3):
            try:
                resp = requests.get(url, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    print(f"\n    ❌ Failed after 3 attempts at offset {offset}: {e}")
                    return all_features
                time.sleep(5 * (attempt + 1))
                continue

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += len(features)

        # Progress
        if total_expected:
            pct = min(offset / total_expected * 100, 100)
            print(f"\r    Downloaded {offset:,} features ({pct:.0f}%)...", end="", flush=True)
        else:
            print(f"\r    Downloaded {offset:,} features...", end="", flush=True)

        # Check if server indicates more records
        exceeded = data.get("exceededTransferLimit", False)
        if not exceeded and len(features) < page_size:
            break

        if max_records and offset >= max_records:
            break

        time.sleep(0.5)  # Rate limiting

    print(f"\r    Downloaded {len(all_features):,} features total" + " " * 20)
    return all_features


def _per_state_download(url, fields, base_where, page_size=500):
    """Download layer state-by-state (fallback for large layers)."""
    import requests

    all_features = []
    fips_list = sorted(STATE_FIPS.values())

    for i, fips in enumerate(fips_list):
        state_abbr = [k for k, v in STATE_FIPS.items() if v == fips][0]
        where = f"STATE='{fips}'"
        if base_where and base_where != "1=1":
            where = f"{where} AND {base_where}"

        state_features = _paginated_geojson_download(
            url, fields, where, page_size=page_size)

        all_features.extend(state_features)
        print(f"    [{i+1}/{len(fips_list)}] {state_abbr}: {len(state_features):,} features "
              f"(total: {len(all_features):,})")

    return all_features


def _geojson_to_geodataframe(features):
    """Convert GeoJSON features to GeoDataFrame."""
    import geopandas as gpd
    from shapely.geometry import shape

    if not features:
        return gpd.GeoDataFrame()

    rows = []
    geometries = []

    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")

        if geom is None:
            continue

        try:
            shp = shape(geom)
            if shp.is_empty:
                continue
            geometries.append(shp)
            rows.append(props)
        except Exception:
            continue

    if not rows:
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    return gdf


def _save_geoparquet(gdf, output_path):
    """Save GeoDataFrame as gzipped parquet."""
    import geopandas as gpd

    # Convert geometry to WKT for compatibility (geoparquet needs pyarrow)
    df = pd.DataFrame(gdf.drop(columns=["geometry"]))
    df["geometry_wkt"] = gdf.geometry.to_wkt()
    df["centlat"] = gdf.geometry.centroid.y
    df["centlon"] = gdf.geometry.centroid.x
    df["area_sq_m"] = gdf.geometry.to_crs(epsg=3857).area

    # Also save as native geoparquet if pyarrow supports it
    try:
        gdf.to_parquet(output_path, compression="gzip")
        return "geoparquet"
    except Exception:
        # Fallback: save with WKT geometry
        df.to_parquet(output_path, compression="gzip", index=False)
        return "parquet+wkt"


def download_layer(layer_name, cache_dir, force=False):
    """Download a single boundary layer."""
    if layer_name not in LAYERS:
        print(f"Unknown layer: {layer_name}")
        return None

    cfg = LAYERS[layer_name]
    output_path = cache_dir / cfg["filename"]

    if output_path.exists() and not force:
        print(f"  ✅ {layer_name}: cached ({output_path.name})")
        return output_path

    print(f"\n  [{layer_name}] {cfg['desc']}")
    print(f"    Source: {cfg['url'][:80]}...")
    t0 = time.time()

    # Try nationwide download first
    features = _paginated_geojson_download(
        cfg["url"], cfg["fields"], cfg["where"], cfg["page_size"])

    # If we got very few results for a layer that should have many,
    # fall back to per-state download
    if layer_name in ("counties", "places", "county_subdivisions"):
        expected_min = {"counties": 3000, "places": 20000, "county_subdivisions": 30000}
        if len(features) < expected_min.get(layer_name, 0):
            print(f"    ⚠️ Only {len(features):,} features — trying per-state download...")
            features = _per_state_download(
                cfg["url"], cfg["fields"], cfg["where"], cfg["page_size"])

    if not features:
        print(f"    ❌ No features downloaded for {layer_name}")
        return None

    # Convert to GeoDataFrame
    gdf = _geojson_to_geodataframe(features)
    if len(gdf) == 0:
        print(f"    ❌ No valid geometries for {layer_name}")
        return None

    # Validate geometries
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        print(f"    ⚠️ Fixing {invalid.sum():,} invalid geometries...")
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)

    # Save
    fmt = _save_geoparquet(gdf, output_path)
    size_mb = output_path.stat().st_size / 1024 / 1024
    elapsed = time.time() - t0

    print(f"    ✅ {len(gdf):,} polygons → {output_path.name} ({size_mb:.1f} MB, {fmt})")
    print(f"    Elapsed: {elapsed:.0f}s")

    # Summary stats
    if "STATE" in gdf.columns or "USPS" in gdf.columns:
        state_col = "USPS" if "USPS" in gdf.columns else "STATE"
        n_states = gdf[state_col].nunique()
        print(f"    Coverage: {n_states} states")

    return output_path


def upload_to_r2(cache_dir, layers_to_upload=None):
    """Upload boundary files to R2."""
    import boto3

    endpoint = os.environ.get("R2_ENDPOINT")
    key_id = os.environ.get("R2_ACCESS_KEY_ID")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY")
    bucket = os.environ.get("R2_BUCKET", "crash-lens-data")

    if not all([endpoint, key_id, secret]):
        print("\n  ⚠️ R2 credentials not set — skipping upload")
        return

    s3 = boto3.client("s3", endpoint_url=endpoint,
                       aws_access_key_id=key_id,
                       aws_secret_access_key=secret)

    r2_prefix = "_national/boundaries"

    for name, cfg in LAYERS.items():
        if layers_to_upload and name not in layers_to_upload:
            continue

        local = cache_dir / cfg["filename"]
        if not local.exists():
            continue

        r2_key = f"{r2_prefix}/{cfg['filename']}"
        size_mb = local.stat().st_size / 1024 / 1024

        try:
            s3.upload_file(str(local), bucket, r2_key)
            print(f"  ✅ {cfg['filename']} → R2 {r2_key} ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"  ❌ Upload failed: {cfg['filename']}: {e}")


def generate_summary(cache_dir):
    """Print summary of all downloaded boundary files."""
    print(f"\n{'='*65}")
    print(f"  BOUNDARY FILES SUMMARY")
    print(f"{'='*65}")

    total_size = 0
    total_polys = 0

    for name, cfg in LAYERS.items():
        local = cache_dir / cfg["filename"]
        if local.exists():
            size_mb = local.stat().st_size / 1024 / 1024
            total_size += size_mb

            # Read and count
            try:
                import geopandas as gpd
                gdf = gpd.read_parquet(local)
                n = len(gdf)
                total_polys += n
                print(f"  ✅ {name:<25} {n:>8,} polygons  {size_mb:>6.1f} MB")
            except Exception:
                try:
                    df = pd.read_parquet(local)
                    n = len(df)
                    total_polys += n
                    print(f"  ✅ {name:<25} {n:>8,} records   {size_mb:>6.1f} MB")
                except Exception:
                    print(f"  ⚠️ {name:<25} exists but unreadable")
        else:
            print(f"  ❌ {name:<25} not downloaded")

    print(f"  {'─'*55}")
    print(f"     TOTAL: {total_polys:,} polygons, {total_size:.1f} MB")
    print(f"{'='*65}")


def main():
    parser = argparse.ArgumentParser(
        description="Download US boundary polygons for CrashLens")
    parser.add_argument("--layer", "-l", nargs="+",
                        choices=list(LAYERS.keys()) + ["all"],
                        default=["all"],
                        help="Which layers to download (default: all)")
    parser.add_argument("--cache-dir", "-d", default="cache/boundaries",
                        help="Local cache directory")
    parser.add_argument("--upload", action="store_true",
                        help="Upload to R2 after download")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if cached")
    args = parser.parse_args()

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    layers = list(LAYERS.keys()) if "all" in args.layer else args.layer

    print("=" * 65)
    print("  CrashLens Boundary Polygon Generator")
    print(f"  Layers: {', '.join(layers)}")
    print(f"  Cache:  {cache_dir}")
    print("=" * 65)

    t0 = time.time()
    downloaded = []

    for layer in layers:
        result = download_layer(layer, cache_dir, force=args.force)
        if result:
            downloaded.append(layer)

    if args.upload and downloaded:
        print("\n  Uploading to R2...")
        upload_to_r2(cache_dir, downloaded)

    generate_summary(cache_dir)

    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
