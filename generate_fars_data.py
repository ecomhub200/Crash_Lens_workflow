#!/usr/bin/env python3
"""
generate_fars_data.py — CrashLens FARS Data Downloader
=============================================================
Downloads Fatality Analysis Reporting System (FARS) data from NHTSA's public
Crash API. FARS is the federal census of all ~40K fatal motor-vehicle crashes
per year across all 50 states + DC, with 170+ standardized data elements.

API:
    https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData
    Datasets: Accident, Person, Vehicle
    Year range: 2010-2023 (split into 3 requests — API caps ranges at 5 yr)
    Auth: None (public API). No documented rate limits; we sleep 1s between calls.

SETUP:
    pip install requests pandas pyarrow boto3

USAGE:
    python generate_fars_data.py --state de                 # Delaware only
    python generate_fars_data.py --state de va md           # Multiple
    python generate_fars_data.py --all                      # All 51 states + DC
    python generate_fars_data.py --state de --local-only    # No R2 upload
    python generate_fars_data.py --all --force              # Regenerate all

OUTPUT:
    cache/{abbr}_fars.parquet.gz           → R2: {prefix}/cache/{abbr}_fars.parquet.gz
    cache/fars_nationwide.parquet.gz       → R2: _nationwide/fars_nationwide.parquet.gz
                                             (only on --all)

    Per-crash parquet columns (~44):
      Identification: case_id, state_fips, state_name, county_fips, county_name,
                      city_fips, city_name
      When:           crash_year, crash_month, crash_day, crash_hour, crash_minute
      Where:          latitude, longitude, route_name_1, route_name_2
      Road context:   functional_class, road_ownership, route_type, rural_urban,
                      lighting, weather, manner_of_collision, first_harmful_event,
                      relation_to_road, intersection_type
      Severity:       fatalities, drunk_drivers, total_vehicles, total_persons
      Person flags:   any_drunk, any_unrestrained, ped_involved, bike_involved,
                      ped_fatals, bike_fatals, total_fatalities,
                      youngest_driver_age, oldest_driver_age
      Vehicle flags:  any_speeding, any_large_truck, any_motorcycle,
                      any_distracted, hit_and_run
"""

import argparse
import gc
import gzip
import io
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
import requests


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

FARS_BASE = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"
FARS_DATASETS = ("Accident", "Person", "Vehicle")
FARS_MAX_SPAN = 5  # API hard-caps single requests at 5 calendar years

R2_BUCKET = "crash-lens-data"

# Rename FARS columns → CrashLens-friendly names in the final parquet.
# RENAME_MAP is the single source of truth for which FARS API fields survive
# into the output. Everything not in this map is filtered out by build_final_df,
# which locks the final schema at 30 accident cols + 9 person flags + 5 vehicle
# flags = 44 total columns (see EXPECTED_FINAL_COLS in the test suite).
#
# We keep only the *NAME text-label columns (not the numeric code counterparts
# like FUNC_SYS/RD_OWNER/TYP_INT) because the documented CrashLens schema uses
# human-readable labels. If downstream consumers ever need the raw FARS codes
# they should be re-added here AND to EXPECTED_FINAL_COLS in the test.
RENAME_MAP = {
    "ST_CASE": "case_id",
    "STATE": "state_fips",
    "STATENAME": "state_name",
    "COUNTY": "county_fips",
    "COUNTYNAME": "county_name",
    "CITY": "city_fips",
    "CITYNAME": "city_name",
    "YEAR": "crash_year",
    "MONTH": "crash_month",
    "DAY": "crash_day",
    "HOUR": "crash_hour",
    "MINUTE": "crash_minute",
    "LATITUDE": "latitude",
    "LONGITUD": "longitude",
    "FATALS": "fatalities",
    "DRUNK_DR": "drunk_drivers",
    "TOTALVEHICLES": "total_vehicles",
    "PERSONS": "total_persons",
    "FUNC_SYSNAME": "functional_class",
    "RD_OWNERNAME": "road_ownership",
    "ROUTENAME": "route_type",
    "RUR_URBNAME": "rural_urban",
    "LGT_CONDNAME": "lighting",
    "WEATHERNAME": "weather",
    "MAN_COLLNAME": "manner_of_collision",
    "HARM_EVNAME": "first_harmful_event",
    "REL_ROADNAME": "relation_to_road",
    "TYP_INTNAME": "intersection_type",
    "TWAY_ID": "route_name_1",
    "TWAY_ID2": "route_name_2",
}

# Accident-table fields we keep from the FARS API response. Derived from
# RENAME_MAP so there's only one list to maintain. build_final_df() intersects
# this with the actual accident DataFrame's columns before joining — extra
# fields the real API returns (FUNC_SYS code, WRK_ZONE, SCH_BUS, NHS, etc.)
# are dropped here, NOT carried through to the final parquet.
ACCIDENT_COLS = list(RENAME_MAP.keys())


# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY (loaded from states/geography/us_states.json)
# ═══════════════════════════════════════════════════════════════

def load_states():
    """Load 51 states (50 + DC) from the TIGERweb-backfilled gazetteer file.

    Filters out Puerto Rico (GEOID 72) — FARS does not cover it.
    """
    path = Path(__file__).resolve().parent / "states" / "geography" / "us_states.json"
    with open(path) as f:
        payload = json.load(f)
    states = []
    for rec in payload["records"]:
        if rec["GEOID"] == "72":   # Puerto Rico — not in FARS
            continue
        name = rec["NAME"]
        states.append({
            "name": name,
            "abbreviation": rec["USPS"].lower(),
            "fips": rec["GEOID"],                          # e.g. "10"
            "r2_prefix": name.lower().replace(" ", "_"),   # e.g. "district_of_columbia"
        })
    return states


ALL_STATES = load_states()
ABBR_LOOKUP = {s["abbreviation"]: s for s in ALL_STATES}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def gzip_file(src, dst):
    with open(src, 'rb') as fi, gzip.open(dst, 'wb', compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    raw = os.path.getsize(src) / 1048576
    gz = os.path.getsize(dst) / 1048576
    return raw, gz


def read_gz_parquet(path_or_bytes):
    """Read a gzip-wrapped parquet file (our `.parquet.gz` convention).

    Our `gzip_file()` helper wraps a raw parquet file in a gzip container,
    so `pd.read_parquet()` can't read it directly — it would try to parse
    the gzip header as parquet magic bytes and fail. This helper decompresses
    the outer gzip layer first, then hands the inner parquet bytes to pandas.
    """
    if isinstance(path_or_bytes, (bytes, bytearray)):
        src = io.BytesIO(path_or_bytes)
    else:
        src = path_or_bytes  # pathlib.Path or str
    with gzip.open(src, "rb") as f:
        return pd.read_parquet(io.BytesIO(f.read()))


def get_r2_client():
    """Build a boto3 S3 client for Cloudflare R2 using CF_* env vars."""
    acct = os.environ.get("CF_ACCOUNT_ID", "")
    key_id = os.environ.get("CF_R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("CF_R2_SECRET_ACCESS_KEY", "")
    if not all([acct, key_id, secret]):
        return None
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )


def r2_exists(s3, bucket, key):
    if not s3:
        return False
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def r2_upload(s3, local_path, bucket, key):
    for attempt in range(3):
        try:
            s3.upload_file(str(local_path), bucket, key)
            return True
        except Exception as e:
            if attempt == 2:
                print(f"      Upload failed: {e}")
            time.sleep(2 ** (attempt + 1))
    return False


def r2_download_to_df(s3, bucket, key):
    """Download an existing parquet.gz from R2 into a DataFrame (for nationwide rollup)."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return read_gz_parquet(obj["Body"].read())
    except Exception as e:
        print(f"      WARN: could not fetch existing R2 file {key}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  FARS API
# ═══════════════════════════════════════════════════════════════

def fetch_fars_dataset(dataset, fips, from_year, to_year):
    """Fetch one FARS dataset for one state + year range.

    Returns a list of record dicts (possibly empty).
    The API response is shaped like {"Results": [[{...}, {...}, ...]]} — note
    the double nesting. Records are at response["Results"][0].
    """
    params = {
        "dataset": dataset,
        "FromYear": from_year,
        "ToYear": to_year,
        "State": int(fips),
        "format": "json",
    }
    for attempt in range(3):
        try:
            r = requests.get(FARS_BASE, params=params, timeout=120)
            r.raise_for_status()
            payload = r.json()
            results = payload.get("Results") or []
            if results and isinstance(results[0], list):
                return results[0]
            return results  # fall back if shape ever changes
        except (requests.RequestException, ValueError) as e:
            if attempt == 2:
                print(f"    WARNING: {dataset} {from_year}-{to_year} failed: {e}")
                return []
            time.sleep(2 ** attempt)
    return []


def year_chunks(from_year, to_year, max_span=FARS_MAX_SPAN):
    """Split [from_year, to_year] into contiguous chunks of ≤ max_span years.

    The NHTSA CrashAPI caps a single FARS request at 5 calendar years, so the
    pipeline always splits the requested range into 5-year blocks regardless
    of what the user passes. This function is dynamic — it honors whatever
    --from-year / --to-year the user specifies, even if that range extends
    before 2010 or beyond 2023, unlike a hardcoded chunk list.
    """
    chunks = []
    lo = from_year
    while lo <= to_year:
        hi = min(lo + max_span - 1, to_year)
        chunks.append((lo, hi))
        lo = hi + 1
    return chunks


def download_all_datasets(fips, abbr, from_year, to_year):
    """Fetch 3 datasets × N year chunks for one state, concatenated per dataset.

    Year chunks are computed dynamically from the requested range so that
    unusual --from-year / --to-year arguments (e.g. 2008 or 2025) are honored
    instead of silently truncated.
    """
    chunks_to_fetch = year_chunks(from_year, to_year)
    out = {}
    for dataset in FARS_DATASETS:
        chunks = []
        for lo, hi in chunks_to_fetch:
            records = fetch_fars_dataset(dataset, fips, lo, hi)
            print(f"      Downloading {dataset:8s} data ({lo}-{hi})... {len(records):,} records")
            if records:
                chunks.append(pd.DataFrame(records))
            time.sleep(1)
        if chunks:
            out[dataset] = pd.concat(chunks, ignore_index=True)
        else:
            out[dataset] = pd.DataFrame()
    return out


# ═══════════════════════════════════════════════════════════════
#  AGGREGATION (person + vehicle → crash-level flags)
# ═══════════════════════════════════════════════════════════════

def _num(series):
    """Coerce a column to numeric, non-numerics → NaN."""
    return pd.to_numeric(series, errors="coerce")


def aggregate_persons(person_df):
    """Aggregate person records to crash level keyed on (ST_CASE, YEAR)."""
    if person_df.empty:
        return pd.DataFrame(columns=[
            "ST_CASE", "YEAR", "any_drunk", "any_unrestrained",
            "ped_involved", "bike_involved", "ped_fatals", "bike_fatals",
            "total_fatalities", "youngest_driver_age", "oldest_driver_age",
        ])
    df = person_df.copy()
    df["ST_CASE"] = _num(df["ST_CASE"])
    df["YEAR"] = _num(df["YEAR"])
    df["DRINKING"] = _num(df.get("DRINKING"))
    df["ALC_RES"] = _num(df.get("ALC_RES"))
    df["REST_USE"] = _num(df.get("REST_USE"))
    df["PER_TYP"] = _num(df.get("PER_TYP"))
    df["INJ_SEV"] = _num(df.get("INJ_SEV"))
    df["AGE"] = _num(df.get("AGE"))

    # Row-level booleans
    df["_drunk"] = (df["DRINKING"] == 1) | df["ALC_RES"].between(8, 94)
    df["_unrestrained"] = ~df["REST_USE"].isin([3, 7])
    df["_ped"] = df["PER_TYP"] == 5
    df["_bike"] = df["PER_TYP"] == 6
    df["_fatal"] = df["INJ_SEV"] == 4
    df["_ped_fatal"] = df["_ped"] & df["_fatal"]
    df["_bike_fatal"] = df["_bike"] & df["_fatal"]
    # Driver age with non-driver rows masked out
    df["_driver_age"] = df["AGE"].where(df["PER_TYP"] == 1)

    grp = df.groupby(["ST_CASE", "YEAR"], as_index=False, sort=False)
    agg = grp.agg(
        any_drunk=("_drunk", "any"),
        any_unrestrained=("_unrestrained", "any"),
        ped_involved=("_ped", "any"),
        bike_involved=("_bike", "any"),
        ped_fatals=("_ped_fatal", "sum"),
        bike_fatals=("_bike_fatal", "sum"),
        total_fatalities=("_fatal", "sum"),
        youngest_driver_age=("_driver_age", "min"),
        oldest_driver_age=("_driver_age", "max"),
    )
    return agg


def aggregate_vehicles(vehicle_df):
    """Aggregate vehicle records to crash level keyed on (ST_CASE, YEAR)."""
    if vehicle_df.empty:
        return pd.DataFrame(columns=[
            "ST_CASE", "YEAR", "any_speeding", "any_large_truck",
            "any_motorcycle", "any_distracted", "hit_and_run",
        ])
    df = vehicle_df.copy()
    df["ST_CASE"] = _num(df["ST_CASE"])
    df["YEAR"] = _num(df["YEAR"])
    df["SPEEDREL"] = _num(df.get("SPEEDREL"))
    df["BODY_TYP"] = _num(df.get("BODY_TYP"))
    df["MDRDSTRD"] = _num(df.get("MDRDSTRD"))
    df["HIT_RUN"] = _num(df.get("HIT_RUN"))

    df["_speed"] = df["SPEEDREL"].isin([1, 2, 3, 4, 5])
    df["_truck"] = df["BODY_TYP"].between(60, 79)
    df["_moto"] = df["BODY_TYP"].between(80, 89)
    df["_distracted"] = ~df["MDRDSTRD"].isin([0, 96, 99]) & df["MDRDSTRD"].notna()
    df["_hit_run"] = df["HIT_RUN"].isin([1, 2])

    grp = df.groupby(["ST_CASE", "YEAR"], as_index=False, sort=False)
    agg = grp.agg(
        any_speeding=("_speed", "any"),
        any_large_truck=("_truck", "any"),
        any_motorcycle=("_moto", "any"),
        any_distracted=("_distracted", "any"),
        hit_and_run=("_hit_run", "any"),
    )
    return agg


# ═══════════════════════════════════════════════════════════════
#  BUILD FINAL PER-STATE DATAFRAME
# ═══════════════════════════════════════════════════════════════

def _sanitize_gps(df):
    """Mask out FARS sentinel / out-of-range lat/lon values.

    Bounds are the CONUS+AK+HI envelope. Lower latitude is 17.0° to include
    Hawaii (Ka Lae / South Point on the Big Island is at 18.9°N; a tighter
    bound would silently drop every HI fatal crash). Upper latitude is 72.0°
    to include Utqiagvik, AK (71.3°N). Longitude bounds span the Aleutians
    through the East Coast.

    The lat/lon range filter alone catches every documented FARS sentinel
    (77.7777, 88.8888, 777.7777, 888.8888, 99.9999) because all of them lie
    outside the valid envelope.
    """
    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")
    valid = lat.between(17.0, 72.0) & lon.between(-180.0, -65.0)
    df["latitude"] = lat.where(valid)
    df["longitude"] = lon.where(valid)
    return int(valid.sum()), len(df)


def build_final_df(accident_df, person_df, vehicle_df):
    """Join accident + person-agg + vehicle-agg and rename to CrashLens schema."""
    if accident_df.empty:
        return pd.DataFrame()

    # Keep only the columns we want, defensively — some may be missing on
    # older FARS years / small states.
    keep = [c for c in ACCIDENT_COLS if c in accident_df.columns]
    df = accident_df[keep].copy()

    # Coerce keys for merge
    df["ST_CASE"] = _num(df["ST_CASE"])
    df["YEAR"] = _num(df["YEAR"])

    person_agg = aggregate_persons(person_df)
    vehicle_agg = aggregate_vehicles(vehicle_df)

    df = df.merge(person_agg, on=["ST_CASE", "YEAR"], how="left")
    df = df.merge(vehicle_agg, on=["ST_CASE", "YEAR"], how="left")

    # Fill NA on boolean flag columns (crashes with no person/vehicle match → False)
    bool_cols = [
        "any_drunk", "any_unrestrained", "ped_involved", "bike_involved",
        "any_speeding", "any_large_truck", "any_motorcycle",
        "any_distracted", "hit_and_run",
    ]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)

    count_cols = ["ped_fatals", "bike_fatals", "total_fatalities"]
    for c in count_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype("Int64")

    # Rename to final schema
    df = df.rename(columns=RENAME_MAP)

    # Sanitize GPS (FARS sentinels + out-of-range)
    if "latitude" in df.columns and "longitude" in df.columns:
        valid, total = _sanitize_gps(df)
        print(f"      Valid GPS: {valid:,}/{total:,} ({(valid / total * 100 if total else 0):.1f}%)")

    # Deterministic sort
    sort_cols = [c for c in ("crash_year", "crash_month", "crash_day", "crash_hour") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


# ═══════════════════════════════════════════════════════════════
#  PER-STATE PROCESSING
# ═══════════════════════════════════════════════════════════════

def process_state(state_info, cache_dir, s3, bucket, from_year, to_year,
                  force=False, local_only=False):
    """Download + build + upload FARS for one state.

    Returns (result_tag, df_or_none). result_tag ∈ {'completed','skipped','failed'}.
    When skipped during a nationwide run, attempts to pull the existing R2 file
    into memory so the nationwide rollup still has this state's rows.
    """
    name = state_info["name"]
    abbr = state_info["abbreviation"]
    fips = state_info["fips"]
    prefix = state_info["r2_prefix"]

    fars_gz = cache_dir / f"{abbr}_fars.parquet.gz"
    r2_key = f"{prefix}/cache/{abbr}_fars.parquet.gz"

    # ── Skip logic ──
    if not force:
        if not local_only and r2_exists(s3, bucket, r2_key):
            print(f"  [skip] {name} ({abbr}) — FARS already in R2")
            df = r2_download_to_df(s3, bucket, r2_key)
            return ("skipped", df)
        if local_only and fars_gz.exists():
            print(f"  [skip] {name} ({abbr}) — FARS already cached locally")
            try:
                return ("skipped", read_gz_parquet(fars_gz))
            except Exception:
                return ("skipped", None)

    print(f"\n  [fars] {name} ({abbr}, FIPS={fips}) — downloading {from_year}-{to_year}")
    t0 = time.time()

    datasets = download_all_datasets(fips, abbr, from_year, to_year)
    accident_df = datasets.get("Accident", pd.DataFrame())
    person_df = datasets.get("Person", pd.DataFrame())
    vehicle_df = datasets.get("Vehicle", pd.DataFrame())

    if accident_df.empty:
        print(f"  [failed] {name} — no accident data returned from FARS API")
        return ("failed", None)

    print(f"      Processing: {len(accident_df):,} crash records, joining person + vehicle data...")
    df = build_final_df(accident_df, person_df, vehicle_df)

    if df.empty:
        print(f"  [failed] {name} — empty final DataFrame after join")
        return ("failed", None)

    # Aggregate stats summary
    n = len(df)
    if n > 0:
        drunk = int(df["any_drunk"].sum()) if "any_drunk" in df.columns else 0
        speed = int(df["any_speeding"].sum()) if "any_speeding" in df.columns else 0
        ped = int(df["ped_involved"].sum()) if "ped_involved" in df.columns else 0
        print(
            f"      Aggregated: any_drunk={drunk:,} ({drunk / n * 100:.1f}%), "
            f"any_speeding={speed:,} ({speed / n * 100:.1f}%), "
            f"ped_involved={ped:,} ({ped / n * 100:.1f}%)"
        )

    # ── Write parquet ──
    fars_pq = cache_dir / f"{abbr}_fars.parquet"
    df.to_parquet(fars_pq, index=False)
    raw, gz = gzip_file(fars_pq, fars_gz)
    fars_pq.unlink(missing_ok=True)

    elapsed = time.time() - t0
    size_kb = os.path.getsize(fars_gz) / 1024
    print(f"      Output: {abbr}_fars.parquet.gz ({len(df):,} rows × {len(df.columns)} columns, {size_kb:.0f} KB)")
    print(f"      Elapsed: {elapsed:.0f}s")

    # ── Upload ──
    if not local_only and s3:
        if r2_upload(s3, fars_gz, bucket, r2_key):
            print(f"      -> uploaded to R2: {r2_key}")

    gc.collect()
    return ("completed", df)


# ═══════════════════════════════════════════════════════════════
#  NATIONWIDE ROLLUP
# ═══════════════════════════════════════════════════════════════

def build_nationwide(state_dfs, cache_dir, s3, bucket, local_only):
    """Concat all per-state DataFrames → write + upload nationwide parquet."""
    non_empty = [df for df in state_dfs if df is not None and not df.empty]
    if len(non_empty) < 2:
        print("\n  [nationwide] not enough state data to build nationwide rollup")
        return
    print(f"\n  [nationwide] concatenating {len(non_empty)} state DataFrames...")
    nationwide = pd.concat(non_empty, ignore_index=True)
    print(f"  [nationwide] total: {len(nationwide):,} rows × {len(nationwide.columns)} columns")

    nw_pq = cache_dir / "fars_nationwide.parquet"
    nw_gz = cache_dir / "fars_nationwide.parquet.gz"
    nationwide.to_parquet(nw_pq, index=False)
    gzip_file(nw_pq, nw_gz)
    nw_pq.unlink(missing_ok=True)
    size_mb = os.path.getsize(nw_gz) / 1048576
    print(f"  [nationwide] wrote {nw_gz} ({size_mb:.1f} MB gz)")

    if not local_only and s3:
        key = "_nationwide/fars_nationwide.parquet.gz"
        if r2_upload(s3, nw_gz, bucket, key):
            print(f"  [nationwide] -> uploaded to R2: {key}")


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CrashLens FARS Data Downloader — NHTSA fatal crash census",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
    python generate_fars_data.py --state de                # Delaware only
    python generate_fars_data.py --state de va md          # Multiple
    python generate_fars_data.py --all                     # All 51 states + DC
    python generate_fars_data.py --state de --local-only   # No R2 upload
    python generate_fars_data.py --all --force             # Regenerate all
    python generate_fars_data.py --state de --from-year 2018 --to-year 2023

FARS COLUMNS (~44 per crash):
    case_id, state_fips, state_name, county_fips, county_name,
    city_fips, city_name, crash_year, crash_month, crash_day,
    crash_hour, crash_minute, latitude, longitude,
    fatalities, drunk_drivers, total_vehicles, total_persons,
    functional_class, road_ownership, route_type, rural_urban,
    lighting, weather, manner_of_collision, first_harmful_event,
    relation_to_road, intersection_type, route_name_1, route_name_2,
    any_drunk, any_unrestrained, any_speeding,
    ped_involved, bike_involved, ped_fatals, bike_fatals,
    total_fatalities, youngest_driver_age, oldest_driver_age,
    any_large_truck, any_motorcycle, any_distracted, hit_and_run
        """,
    )
    parser.add_argument("--state", nargs="+", help="State abbreviation(s), e.g. de va md")
    parser.add_argument("--all", action="store_true", help="All 51 states + DC")
    parser.add_argument("--local-only", action="store_true", help="Skip R2 upload")
    parser.add_argument("--force", action="store_true", help="Regenerate if already cached")
    parser.add_argument("--cache-dir", default="cache", help="Local cache directory")
    parser.add_argument("--from-year", type=int, default=2010, help="Start year (default: 2010)")
    parser.add_argument("--to-year", type=int, default=2023, help="End year (default: 2023)")
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        sys.exit(1)

    if args.from_year > args.to_year:
        print(f"ERROR: --from-year ({args.from_year}) > --to-year ({args.to_year})")
        sys.exit(1)

    # Build state list
    states = list(ALL_STATES) if args.all else []
    if args.state:
        for abbr in args.state:
            abbr = abbr.lower()
            if abbr in ABBR_LOOKUP:
                if not any(s["abbreviation"] == abbr for s in states):
                    states.append(ABBR_LOOKUP[abbr])
            else:
                print(f"Unknown state: {abbr}")
                sys.exit(1)

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # R2 client setup
    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        if s3:
            try:
                s3.list_objects_v2(Bucket=R2_BUCKET, Prefix="delaware/", MaxKeys=1)
                print(f"R2 connected: {R2_BUCKET}")
            except Exception:
                print("R2 connection failed — local-only mode")
                s3 = None
        else:
            print("R2 credentials not set — local-only mode")

    local_only_effective = args.local_only or not s3

    print(f"\n{'=' * 60}")
    print(f"  CrashLens FARS Data Downloader")
    print(f"  States: {len(states)} | R2: {'yes' if s3 else 'local'}")
    print(f"  Years:  {args.from_year}-{args.to_year}")
    print(f"  Source: NHTSA FARS API (crashviewer.nhtsa.dot.gov)")
    print(f"{'=' * 60}")

    results = {"completed": 0, "skipped": 0, "failed": 0}
    collected_dfs = []
    t_start = time.time()

    for i, state in enumerate(states, 1):
        print(f"\n  [{i}/{len(states)}]", end="")
        try:
            tag, df = process_state(
                state, cache_dir, s3, R2_BUCKET,
                from_year=args.from_year,
                to_year=args.to_year,
                force=args.force,
                local_only=local_only_effective,
            )
        except Exception as e:
            print(f"  ERROR: {state['name']} — {e}")
            tag, df = "failed", None
        results[tag] += 1
        if df is not None:
            collected_dfs.append(df)
        time.sleep(1)

    # Nationwide rollup (only on --all)
    if args.all:
        build_nationwide(collected_dfs, cache_dir, s3, R2_BUCKET, local_only_effective)

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE in {elapsed / 60:.1f} min")
    print(f"  Completed: {results['completed']}")
    print(f"  Skipped:   {results['skipped']}")
    print(f"  Failed:    {results['failed']}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
