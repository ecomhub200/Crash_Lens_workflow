#!/usr/bin/env python3
"""
generate_fars_data.py — CrashLens FARS Data Downloader
=============================================================
Downloads Fatality Analysis Reporting System (FARS) data from NHTSA's public
bulk CSV archive. FARS is the federal census of all ~40K fatal motor-vehicle
crashes per year across all 50 states + DC, with 170+ standardized data
elements.

SOURCE:
    https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip

    One ZIP per calendar year. Each ZIP contains ACCIDENT.CSV / PERSON.CSV /
    VEHICLE.CSV plus auxiliary files. Rows are nationwide — we filter by
    STATE (FIPS) after download. CSV encoding is latin-1.

    We switched from the CrashAPI (crashviewer.nhtsa.dot.gov) to the static
    CDN because NHTSA blocks datacenter IPs (GitHub Actions Azure runners)
    at the network layer with 403 Forbidden regardless of User-Agent.
    static.nhtsa.gov serves files directly from a CDN, so there's no
    anti-abuse filter to trip. See wiki/log.md [2026-04-13] Fix 2 entry.

SETUP:
    pip install requests pandas pyarrow boto3

USAGE:
    python generate_fars_data.py --state de                 # Delaware only
    python generate_fars_data.py --state de va md           # Multiple
    python generate_fars_data.py --all                      # All 51 states + DC
    python generate_fars_data.py --state de --local-only    # No R2 upload
    python generate_fars_data.py --all --force              # Regenerate all

OUTPUT:
    cache/{abbr}_fars.parquet              → R2: {prefix}/cache/{abbr}_fars.parquet
    cache/fars_nationwide.parquet          → R2: _national/fars_nationwide.parquet
                                             (only on --all)

    Parquet files are Snappy-compressed (pyarrow default), **not** gzip-
    wrapped. The frontend hyparquet browser parser cannot decode gzip-
    internal column compression, so we ship plain `.parquet` everywhere.

    Per-crash parquet columns (44):
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
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

FARS_BULK_URL = (
    "https://static.nhtsa.gov/nhtsa/downloads/FARS/"
    "{year}/National/FARS{year}NationalCSV.zip"
)

# Descriptive User-Agent for static.nhtsa.gov (CDN doesn't block datacenter
# IPs, but it's polite to identify ourselves so NHTSA can see in their logs
# that researchers are using the bulk distribution).
BULK_HEADERS = {
    "User-Agent": "CrashLens/1.0 (https://crashlens.com; traffic safety research)",
}

R2_BUCKET = "crash-lens-data"

# Rename FARS columns → CrashLens-friendly names in the final parquet.
# RENAME_MAP is the single source of truth for which FARS fields survive
# into the output. Everything not in this map is filtered out by
# build_final_df, which locks the final schema at 30 accident cols + 9
# person flags + 5 vehicle flags = 44 total columns (see EXPECTED_FINAL_COLS
# in the test suite).
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

# Accident-table fields we keep from the bulk CSVs. Derived from RENAME_MAP
# so there's only one list to maintain. build_final_df() intersects this
# with the actual accident DataFrame's columns before joining — extra fields
# the real bulk CSVs ship (FUNC_SYS code, WRK_ZONE, SCH_BUS, NHS, etc.) are
# dropped here, NOT carried through to the final parquet.
ACCIDENT_COLS = list(RENAME_MAP.keys())

# Columns the final parquet is guaranteed to have. If a year's bulk CSV
# doesn't include a given text-label column (older years sometimes lack
# STATENAME / FUNC_SYSNAME / etc.), build_final_df backfills it as NaN so
# the output always matches the 44-col contract.
_RENAMED_TARGETS = list(RENAME_MAP.values())
_PERSON_AGG_COLS = [
    "any_drunk", "any_unrestrained", "ped_involved", "bike_involved",
    "ped_fatals", "bike_fatals", "total_fatalities",
    "youngest_driver_age", "oldest_driver_age",
]
_VEHICLE_AGG_COLS = [
    "any_speeding", "any_large_truck", "any_motorcycle",
    "any_distracted", "hit_and_run",
]
FINAL_COLUMNS = _RENAMED_TARGETS + _PERSON_AGG_COLS + _VEHICLE_AGG_COLS


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
    """Download an existing plain parquet file from R2 into a DataFrame.

    Used by the nationwide rollup path when a per-state file is skipped
    (already in R2) but we still need its rows to build the nationwide
    parquet. Reads Snappy-compressed parquet directly — no gzip wrapper.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception as e:
        print(f"      WARN: could not fetch existing R2 file {key}: {e}")
        return None


def r2_delete_if_exists(s3, bucket, key):
    """Delete an R2 object if it exists. Used to clean up legacy
    .parquet.gz files after a successful .parquet upload migrated them.
    Silent on failure — cleanup is best-effort.
    """
    if not s3:
        return
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  FARS BULK CSV DOWNLOAD (from static.nhtsa.gov CDN)
# ═══════════════════════════════════════════════════════════════

# Module-level cache: {year: {"Accident": df, "Person": df, "Vehicle": df}}.
# Each year's ZIP is downloaded once and reused across all per-state calls
# in a single run, so `--all` only makes ~14 HTTP requests for 51 states.
# Tests clear this via `clear_bulk_cache()` to avoid cross-test leakage.
_FARS_BULK_CACHE = {}


def clear_bulk_cache():
    """Reset the in-memory bulk-ZIP cache. For tests."""
    _FARS_BULK_CACHE.clear()


def _extract_fars_csv(zf, basenames):
    """Return the DataFrame for the first ZIP entry whose basename
    (path-stripped, upper-cased) matches any filename in `basenames`.

    `basenames` is an iterable of uppercase filenames, e.g.
    ``{"ACCIDENT.CSV"}`` or ``{"VEHICLE.CSV", "VEH.CSV", "VEHICLES.CSV"}``.

    Path stripping via ``os.path.basename`` handles ZIPs that nest their
    files inside a top-level directory (e.g. ``FARS2021NationalCSV/ACCIDENT.CSV``).
    Exact-basename matching (rather than substring) prevents false-positive
    matches on auxiliary files like ``accident_aux.csv``.

    Returns None if no entry matches.
    """
    wanted = {b.upper() for b in basenames}
    for name in zf.namelist():
        base = os.path.basename(name).upper()
        if base in wanted:
            with zf.open(name) as f:
                return pd.read_csv(f, encoding="latin-1", low_memory=False)
    return None


def download_fars_year_bulk(year):
    """Download + extract the national FARS CSV ZIP for a given year.

    Returns a dict with Accident / Person / Vehicle DataFrames (nationwide,
    all 51 states) or None on failure. Results are cached per-year so
    subsequent calls in the same process are free.

    Bulk CSVs come from static.nhtsa.gov (a CDN, not the blocked CrashAPI).
    Encoding is latin-1. Typical ZIP is 30-50MB → ~100-200MB uncompressed.
    """
    if year in _FARS_BULK_CACHE:
        return _FARS_BULK_CACHE[year]

    url = FARS_BULK_URL.format(year=year)
    print(f"      Downloading FARS {year} bulk CSV ZIP from {url}...")

    for attempt in range(3):
        try:
            response = requests.get(url, headers=BULK_HEADERS, timeout=300)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == 2:
                print(f"    WARNING: FARS {year} download failed after 3 attempts: {e}")
                _FARS_BULK_CACHE[year] = None
                return None
            time.sleep(2 ** attempt)

    try:
        zf = zipfile.ZipFile(io.BytesIO(response.content))
    except zipfile.BadZipFile as e:
        print(f"    WARNING: FARS {year} content is not a valid ZIP: {e}")
        _FARS_BULK_CACHE[year] = None
        return None

    # Debug: show the first 20 basenames so filename drift in future years
    # (e.g. VEHICLE.CSV → VEH.CSV, or new subdirectory nesting) is obvious
    # from the log without needing to re-run with extra instrumentation.
    print(
        f"      FARS {year} ZIP contents: "
        f"{[os.path.basename(n) for n in zf.namelist()][:20]}"
    )

    accident_df = _extract_fars_csv(zf, {"ACCIDENT.CSV"})
    person_df = _extract_fars_csv(zf, {"PERSON.CSV", "PER.CSV"})
    vehicle_df = _extract_fars_csv(zf, {"VEHICLE.CSV", "VEH.CSV", "VEHICLES.CSV"})

    # Inject YEAR from the download year into every loaded DF. Some FARS
    # year CSVs omit the YEAR column (or ship it with non-uppercase casing),
    # which breaks the crash-level aggregation and merge downstream. The
    # download year is authoritative for every row in a given national ZIP,
    # so this is a root-cause fix for the KeyError 'YEAR' bug.
    for df in (accident_df, person_df, vehicle_df):
        if df is not None and not df.empty:
            df["YEAR"] = int(year)

    # Vehicle data is optional for crash-level analysis — if the VEHICLE
    # file shrinks unexpectedly (e.g. NHTSA splits it across multiple files
    # for a new year), warn but do not fail. The any_speeding / any_large_truck
    # / etc. flags will just be sparser than usual for that year.
    if vehicle_df is not None and len(vehicle_df) < 1000:
        print(
            f"    WARNING: FARS {year} VEHICLE has only {len(vehicle_df):,} "
            f"rows nationally (expected ~40-50k); vehicle flags may be "
            f"incomplete for this year."
        )

    acc_n = 0 if accident_df is None else len(accident_df)
    per_n = 0 if person_df is None else len(person_df)
    veh_n = 0 if vehicle_df is None else len(vehicle_df)
    print(
        f"      FARS {year}: ACCIDENT={acc_n:,}  PERSON={per_n:,}  VEHICLE={veh_n:,}"
    )

    result = {
        "Accident": accident_df if accident_df is not None else pd.DataFrame(),
        "Person": person_df if person_df is not None else pd.DataFrame(),
        "Vehicle": vehicle_df if vehicle_df is not None else pd.DataFrame(),
    }
    _FARS_BULK_CACHE[year] = result
    return result


def _filter_to_state(df, fips_int):
    """Filter a nationwide FARS DataFrame to rows where STATE == fips_int.

    Coerces STATE to numeric defensively — some older years ship it as str,
    others as int. Explicit ``Int64`` cast after ``to_numeric`` makes the
    comparison predictable even when the column is a mix of types.
    Returns an empty DataFrame if STATE column is missing.
    """
    if df is None or df.empty or "STATE" not in df.columns:
        return pd.DataFrame()
    state = pd.to_numeric(df["STATE"], errors="coerce").astype("Int64")
    return df[state == int(fips_int)].copy()


def download_all_datasets(fips, abbr, from_year, to_year):
    """Assemble per-state Accident/Person/Vehicle DataFrames from bulk ZIPs.

    For each year in [from_year, to_year] we download (or reuse cached) the
    national CSV ZIP, filter each CSV to the requested state FIPS, and concat
    the yearly slices. Return shape mirrors the old API version for drop-in
    compatibility with process_state().
    """
    fips_int = int(fips)
    acc_chunks, per_chunks, veh_chunks = [], [], []

    for year in range(from_year, to_year + 1):
        bulk = download_fars_year_bulk(year)
        if bulk is None:
            continue

        acc = _filter_to_state(bulk["Accident"], fips_int)
        per = _filter_to_state(bulk["Person"], fips_int)
        veh = _filter_to_state(bulk["Vehicle"], fips_int)
        print(
            f"      {abbr.upper()} {year}: "
            f"accident={len(acc):,}  person={len(per):,}  vehicle={len(veh):,}"
        )
        if not acc.empty:
            acc_chunks.append(acc)
        if not per.empty:
            per_chunks.append(per)
        if not veh.empty:
            veh_chunks.append(veh)

    return {
        "Accident": (
            pd.concat(acc_chunks, ignore_index=True)
            if acc_chunks else pd.DataFrame()
        ),
        "Person": (
            pd.concat(per_chunks, ignore_index=True)
            if per_chunks else pd.DataFrame()
        ),
        "Vehicle": (
            pd.concat(veh_chunks, ignore_index=True)
            if veh_chunks else pd.DataFrame()
        ),
    }


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

    # Backfill any missing final columns with NaN, so the output always
    # matches the 44-col contract regardless of which text-label columns
    # the source bulk CSV happened to ship. Older FARS years (pre-2015)
    # sometimes drop FUNC_SYSNAME / STATENAME / etc.
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Sanitize GPS (FARS sentinels + out-of-range)
    if "latitude" in df.columns and "longitude" in df.columns:
        valid, total = _sanitize_gps(df)
        print(f"      Valid GPS: {valid:,}/{total:,} ({(valid / total * 100 if total else 0):.1f}%)")

    # Keep only the 44 contract columns, in a stable order
    df = df[FINAL_COLUMNS]

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

    fars_pq = cache_dir / f"{abbr}_fars.parquet"
    r2_key = f"{prefix}/cache/{abbr}_fars.parquet"
    legacy_r2_gz_key = f"{prefix}/cache/{abbr}_fars.parquet.gz"

    # ── Skip logic ──
    if not force:
        if not local_only and r2_exists(s3, bucket, r2_key):
            print(f"  [skip] {name} ({abbr}) — FARS already in R2")
            df = r2_download_to_df(s3, bucket, r2_key)
            return ("skipped", df)
        if local_only and fars_pq.exists():
            print(f"  [skip] {name} ({abbr}) — FARS already cached locally")
            try:
                return ("skipped", pd.read_parquet(fars_pq))
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
    # Snappy-compressed (pyarrow default), not gzip-wrapped: the frontend
    # hyparquet parser cannot decode gzip-internal column compression.
    df.to_parquet(fars_pq, compression="snappy", index=False)

    elapsed = time.time() - t0
    size_kb = os.path.getsize(fars_pq) / 1024
    print(f"      Output: {abbr}_fars.parquet ({len(df):,} rows × {len(df.columns)} columns, {size_kb:.0f} KB)")
    print(f"      Elapsed: {elapsed:.0f}s")

    # ── Upload ──
    if not local_only and s3:
        if r2_upload(s3, fars_pq, bucket, r2_key):
            print(f"      -> uploaded to R2: {r2_key}")
        # Best-effort cleanup of any legacy .parquet.gz from previous runs
        # so the bucket doesn't accumulate stale dual copies.
        r2_delete_if_exists(s3, bucket, legacy_r2_gz_key)

    gc.collect()
    return ("completed", df)


# ═══════════════════════════════════════════════════════════════
#  NATIONWIDE ROLLUP
# ═══════════════════════════════════════════════════════════════

def build_nationwide(state_dfs, cache_dir, s3, bucket, local_only):
    """Concat all per-state DataFrames → write + upload nationwide parquet.

    Writes Snappy-compressed ``fars_nationwide.parquet`` and uploads it to
    ``_national/fars_nationwide.parquet`` in R2 (the existing CrashLens
    convention for nationwide reference files — see the ``_national/``
    directory in the ``crash-lens-data`` bucket alongside ``us_states.json``,
    ``us_counties.json``, etc.).
    """
    non_empty = [df for df in state_dfs if df is not None and not df.empty]
    if len(non_empty) < 2:
        print("\n  [nationwide] not enough state data to build nationwide rollup")
        return
    print(f"\n  [nationwide] concatenating {len(non_empty)} state DataFrames...")
    nationwide = pd.concat(non_empty, ignore_index=True)
    print(f"  [nationwide] total: {len(nationwide):,} rows × {len(nationwide.columns)} columns")

    nw_pq = cache_dir / "fars_nationwide.parquet"
    nationwide.to_parquet(nw_pq, compression="snappy", index=False)
    size_mb = os.path.getsize(nw_pq) / 1048576
    print(f"  [nationwide] wrote {nw_pq} ({size_mb:.1f} MB)")

    if not local_only and s3:
        key = "_national/fars_nationwide.parquet"
        if r2_upload(s3, nw_pq, bucket, key):
            print(f"  [nationwide] -> uploaded to R2: {key}")
        # Best-effort cleanup of legacy path from before the .parquet.gz migration.
        r2_delete_if_exists(s3, bucket, "_nationwide/fars_nationwide.parquet.gz")


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
    print(f"  Source: NHTSA bulk CSV archive (static.nhtsa.gov)")
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
