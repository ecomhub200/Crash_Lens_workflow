#!/usr/bin/env python3
"""
supabase_sync.py — CrashLens Pipeline Stage 5: Supabase PostgreSQL Sync  v3.0
================================================================================
State-agnostic sync from pipeline output (CSV/parquet.gz) to self-hosted
Supabase PostgreSQL. Handles all 50 states + DC using the same code.

3-TIER COLUMN STRATEGY:
  Tier 1:  111 explicit Postgres columns (queryable, indexable)
  Tier 2:  road_data JSONB — 312+ road inventory columns (hpms_*, map_*, etc.)
  Tier 3:  state_extras JSONB — {abbr}_* columns (varies per state)
  Bonus:   ranking_data JSONB — 76 per-crash ranking columns

STATE-AGNOSTIC:
  - State extras auto-detected by {abbr}_ prefix (de_, va_, co_, etc.)
  - Road data auto-detected by known prefixes (hpms_*, map_*, dot_*, etc.)
  - Unknown columns fall into road_data JSONB (safe catch-all)
  - Same script works for Delaware (517 cols) or any future state

USAGE:
  python supabase_sync.py --state de --input delaware_statewide_all_roads.csv
  python supabase_sync.py --state de --input delaware.parquet.gz
  python supabase_sync.py --state de --from-r2
  python supabase_sync.py --state de --dry-run --input delaware.csv

ENVIRONMENT:
  SUPABASE_DB_HOST        default: localhost (via SSH tunnel)
  SUPABASE_DB_PORT        default: 5432
  SUPABASE_DB_NAME        default: postgres
  SUPABASE_DB_USER        default: postgres
  SUPABASE_DB_PASSWORD    required
  CF_ACCOUNT_ID           for --from-r2
  CF_R2_ACCESS_KEY_ID     for --from-r2
  CF_R2_SECRET_ACCESS_KEY for --from-r2
"""

import argparse
import csv
import json
import os
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY (mirrors build_road_inventory.py)
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

# ═══════════════════════════════════════════════════════════════
#  TIER 1: 111 EXPLICIT COLUMNS
#  CSV header → Postgres column name
# ═══════════════════════════════════════════════════════════════
TIER1_MAP = {
    # ── Golden 69 ─────────────────────────────────────────────
    "OBJECTID":                 "objectid",
    "Document Nbr":             "document_nbr",
    "Crash Year":               "crash_year",
    "Crash Date":               "crash_date",
    "Crash Military Time":      "crash_military_time",
    "Crash Severity":           "crash_severity",
    "K_People":                 "k_people",
    "A_People":                 "a_people",
    "B_People":                 "b_people",
    "C_People":                 "c_people",
    "Persons Injured":          "persons_injured",
    "Pedestrians Killed":       "pedestrians_killed",
    "Pedestrians Injured":      "pedestrians_injured",
    "Vehicle Count":            "vehicle_count",
    "Collision Type":           "collision_type",
    "Weather Condition":        "weather_condition",
    "Light Condition":          "light_condition",
    "Roadway Surface Condition":"roadway_surface_cond",
    "Relation To Roadway":      "relation_to_roadway",
    "Roadway Alignment":        "roadway_alignment",
    "Roadway Surface Type":     "roadway_surface_type",
    "Roadway Defect":           "roadway_defect",
    "Roadway Description":      "roadway_description",
    "Intersection Type":        "intersection_type",
    "Traffic Control Type":     "traffic_control_type",
    "Traffic Control Status":   "traffic_control_status",
    "Work Zone Related":        "work_zone_related",
    "Work Zone Location":       "work_zone_location",
    "Work Zone Type":           "work_zone_type",
    "School Zone":              "school_zone",
    "First Harmful Event":      "first_harmful_event",
    "First Harmful Event Loc":  "first_harmful_event_loc",
    "Alcohol?":                 "alcohol",
    "Animal Related?":          "animal_related",
    "Unrestrained?":            "unrestrained",
    "Bike?":                    "bike",
    "Distracted?":              "distracted",
    "Drowsy?":                  "drowsy",
    "Drug Related?":            "drug_related",
    "Guardrail Related?":       "guardrail_related",
    "Hitrun?":                  "hitrun",
    "Lgtruck?":                 "lgtruck",
    "Motorcycle?":              "motorcycle",
    "Pedestrian?":              "pedestrian",
    "Speed?":                   "speed",
    "Max Speed Diff":           "max_speed_diff",
    "RoadDeparture Type":       "road_departure_type",
    "Intersection Analysis":    "intersection_analysis",
    "Senior?":                  "senior",
    "Young?":                   "young",
    "Mainline?":                "mainline",
    "Night?":                   "night",
    "DOT District":             "dot_district",
    "Juris Code":               "juris_code",
    "Physical Juris Name":      "physical_juris_name",
    "Functional Class":         "functional_class",
    "Facility Type":            "facility_type",
    "Area Type":                "area_type",
    "SYSTEM":                   "system",
    "VSP":                      "vsp",
    "Ownership":                "ownership",
    "Planning District":        "planning_district",
    "MPO Name":                 "mpo_name",
    "RTE Name":                 "rte_name",
    "RNS MP":                   "rns_mp",
    "Node":                     "node",
    "Node Offset (ft)":         "node_offset_ft",
    "x":                        "x",
    "y":                        "y",
    # ── Extra Enrichment (4) ──────────────────────────────────
    "FIPS":                     "fips",
    "Place FIPS":               "place_fips",
    "EPDO_Score":               "epdo_score",
    "Intersection Name":        "intersection_name",
    # ── Key Analysis (10) ─────────────────────────────────────
    "Through_Lanes":            "through_lanes",
    "AADT":                     "aadt",
    "AADT_source":              "aadt_source",
    "Lane_Width_ft":            "lane_width_ft",
    "Median_Width_ft":          "median_width_ft",
    "Shoulder_Width_ft":        "shoulder_width_ft",
    "Has_Sidewalk":             "has_sidewalk",
    "Has_Bike_Lane":            "has_bike_lane",
    "Urban_Area_Name":          "urban_area_name",
    "Urban_Area_GEOID":         "urban_area_geoid",
    # ── POI Proximity Flags (11) ──────────────────────────────
    "Near_PoiBar_1500ft":       "near_poi_bar_1500ft",
    "Near_PoiClinic_1500ft":    "near_poi_clinic_1500ft",
    "Near_PoiCollege_1500ft":   "near_poi_college_1500ft",
    "Near_PoiCrossing_100ft":   "near_poi_crossing_100ft",
    "Near_PoiFuel_500ft":       "near_poi_fuel_500ft",
    "Near_PoiHospital_2000ft":  "near_poi_hospital_2000ft",
    "Near_PoiParking_500ft":    "near_poi_parking_500ft",
    "Near_PoiRestArea_1000ft":  "near_poi_rest_area_1000ft",
    "Near_PoiRestaurant_500ft": "near_poi_restaurant_500ft",
    "Near_PoiSignal_100ft":     "near_poi_signal_100ft",
    "Near_PoiStopSign_100ft":   "near_poi_stop_sign_100ft",
    # ── Federal Asset Proximity (4) ───────────────────────────
    "Near_Bridge_500ft":        "near_bridge_500ft",
    "Near_RailXing_500ft":      "near_rail_xing_500ft",
    "Near_School_1500ft":       "near_school_1500ft",
    "Near_Transit_500ft":       "near_transit_500ft",
    # ── Resolved Values (5) ───────────────────────────────────
    "resolved_speed_limit":     "resolved_speed_limit",
    "resolved_has_lighting":    "resolved_has_lighting",
    "resolved_has_signal":      "resolved_has_signal",
    "resolved_on_bridge":       "resolved_on_bridge",
    "resolved_school_zone":     "resolved_school_zone",
    # ── Intersection & Ramp (4) ───────────────────────────────
    "is_intersection":          "is_intersection",
    "intersection_degree":      "intersection_degree",
    "is_ramp":                  "is_ramp",
    "ramp_type":                "ramp_type",
    # ── Road Geometry (4) ─────────────────────────────────────
    "curvature":                "curvature",
    "length_ft":                "length_ft",
    "road_lon":                 "road_lon",
    "road_lat":                 "road_lat",
}

# Columns auto-generated by Postgres (excluded from COPY)
AUTO_COLUMNS = {"id", "created_at", "updated_at"}

# Tier 2: Road inventory — prefixes that go into road_data JSONB
ROAD_DATA_PREFIXES = (
    "resolved_", "conf_", "xval_", "risk_", "curve_", "te_",
    "hpms_", "nearest_bridge", "bridge_count", "nearest_rail", "rail_xing",
    "nearest_school", "school_count", "nearest_transit", "transit_count",
    "nearest_poi", "poi_", "map_", "osm_", "dot_", "sdot_", "ri_",
    "geometry_coords", "length_m", "divider",
)
ROAD_DATA_EXACT = {"Peak_Lanes", "Structure_Type", "Cracking_Pct",
                    "ARNOLD_Route_ID", "ARNOLD_Begin_MP", "ARNOLD_End_MP"}


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════

def get_db_connection():
    import psycopg2
    pw = os.environ.get("SUPABASE_DB_PASSWORD")
    if not pw:
        print("  ❌ SUPABASE_DB_PASSWORD required"); sys.exit(1)
    conn = psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST", "localhost"),
        port=int(os.environ.get("SUPABASE_DB_PORT", "5432")),
        dbname=os.environ.get("SUPABASE_DB_NAME", "postgres"),
        user=os.environ.get("SUPABASE_DB_USER", "postgres"),
        password=pw,
        connect_timeout=10,
        options='-c search_path=public',
    )
    # Connection test
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user, version();")
        db, user, ver = cur.fetchone()
        print(f"  Connected: {db} as {user}")
        print(f"  PostgreSQL: {ver[:60]}...")
    return conn


def log_run(conn, state, stage, status, rows=None, dur=None, err=None, meta=None):
    cur = conn.cursor()
    cur.execute("""INSERT INTO pipeline_runs (state,stage,status,rows_processed,duration_sec,error_message,metadata_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (state, stage, status, rows, dur, err, json.dumps(meta) if meta else None))
    rid = cur.fetchone()[0]; conn.commit(); return rid


def update_run(conn, rid, status, rows=None, dur=None, err=None):
    cur = conn.cursor()
    cur.execute("UPDATE pipeline_runs SET status=%s,rows_processed=%s,duration_sec=%s,error_message=%s WHERE id=%s",
        (status, rows, dur, err, rid)); conn.commit()


# ═══════════════════════════════════════════════════════════════
#  R2 DOWNLOAD
# ═══════════════════════════════════════════════════════════════

def download_from_r2(state_name, abbr):
    import boto3
    acct = os.environ.get("CF_ACCOUNT_ID")
    akey = os.environ.get("CF_R2_ACCESS_KEY_ID")
    skey = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
    if not all([acct, akey, skey]):
        print("  ❌ R2 creds required: CF_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY"); sys.exit(1)
    s3 = boto3.client("s3", endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
        aws_access_key_id=akey, aws_secret_access_key=skey, region_name="auto")
    for key in [f"{state_name}/_state/all_roads.parquet.gz",
                f"{state_name}/statewide/{state_name}_statewide_all_roads.csv"]:
        try:
            fname = key.split("/")[-1]
            print(f"  Trying R2: {key}")
            s3.download_file("crash-lens-data", key, fname)
            print(f"  ✅ Downloaded: {fname} ({Path(fname).stat().st_size/1048576:.1f} MB)")
            return fname
        except Exception:
            continue
    print(f"  ❌ No statewide file in R2 for {state_name}"); sys.exit(1)


# ═══════════════════════════════════════════════════════════════
#  LOAD & CLASSIFY
# ═══════════════════════════════════════════════════════════════

def load_input(filepath):
    fp = str(filepath)
    # Detect format: try parquet first (handles .parquet.gz, .parquet, and
    # non-standard names like "file_parquet_.gz"), then CSV
    is_parquet = "parquet" in fp.lower() or fp.endswith((".parquet.gz", ".parquet"))
    if is_parquet:
        try:
            df = pd.read_parquet(fp)
        except Exception:
            df = pd.read_csv(fp, compression="gzip" if fp.endswith(".gz") else None,
                             low_memory=False, dtype=str)
    elif fp.endswith(".csv.gz"):
        df = pd.read_csv(fp, compression="gzip", low_memory=False, dtype=str)
    elif fp.endswith(".csv"):
        df = pd.read_csv(fp, low_memory=False, dtype=str)
    else:
        print(f"  ❌ Unsupported format: {fp}")
        print(f"     Expected: .parquet.gz, .parquet, .csv, .csv.gz")
        sys.exit(1)
    # Normalize all to string
    for c in df.columns:
        df[c] = df[c].astype(str).replace({"nan": "", "None": "", "NaT": ""})
    print(f"  Loaded: {len(df):,} rows × {len(df.columns)} cols")
    return df


def classify_columns(df, abbr):
    """Auto-classify every column into tier1 / road_data / state_extras / ranking."""
    tier1_keys = set(TIER1_MAP.keys())
    sp = f"{abbr}_"
    t1, rd, se, rk = [], [], [], []
    for c in df.columns:
        if c in tier1_keys:
            t1.append(c)
        elif "_Rank_" in c:
            rk.append(c)
        elif c.startswith(sp):
            se.append(c)
        elif c in ROAD_DATA_EXACT or any(c.startswith(p) for p in ROAD_DATA_PREFIXES):
            if c not in tier1_keys:
                rd.append(c)
        else:
            rd.append(c)  # Unknown → road_data (safe catch-all)
    return {"tier1": t1, "road_data": rd, "state_extras": se, "ranking": rk}


def _row_to_json(row):
    """Convert a row to compact JSON, dropping empty values."""
    return json.dumps({k: v for k, v in row.items() if v and v != "" and v != "nan"},
                      ensure_ascii=False)


def build_sync_df(df, abbr, state_name):
    """Build final dataframe: Tier 1 columns + 3 JSONB columns."""
    cl = classify_columns(df, abbr)
    print(f"\n  Column classification:")
    print(f"    Tier 1 (explicit):     {len(cl['tier1']):>4d}")
    print(f"    Tier 2 (road_data):    {len(cl['road_data']):>4d}")
    print(f"    Tier 3 (state_extras): {len(cl['state_extras']):>4d}")
    print(f"    Rankings:              {len(cl['ranking']):>4d}")
    print(f"    Total:                 {sum(len(v) for v in cl.values()):>4d} of {len(df.columns)}")

    # ── Tier 1 ────────────────────────────────────────────────
    out = pd.DataFrame(index=df.index)
    out["state"] = state_name
    for csv_col, pg_col in TIER1_MAP.items():
        out[pg_col] = df[csv_col].values if csv_col in df.columns else ""

    # Parse x, y as float
    out["x"] = pd.to_numeric(out["x"], errors="coerce")
    out["y"] = pd.to_numeric(out["y"], errors="coerce")
    # Parse crash_year as int
    out["crash_year"] = pd.to_numeric(out["crash_year"], errors="coerce").astype("Int64")
    # Parse count columns as int
    for c in ["k_people","a_people","b_people","c_people",
              "persons_injured","pedestrians_killed","pedestrians_injured","vehicle_count"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    # ── Parse crash_date to DATE for temporal queries ──
    out["crash_date_parsed"] = pd.to_datetime(
        out["crash_date"], format="mixed", dayfirst=False, errors="coerce"
    ).dt.date
    parsed_count = out["crash_date_parsed"].notna().sum()
    print(f"  crash_date_parsed: {parsed_count:,}/{len(out):,} parsed")

    # ── JSONB columns ─────────────────────────────────────────
    print(f"  Building road_data JSONB ({len(cl['road_data'])} keys)...")
    t_json = time.time()
    out["road_data"] = df[cl["road_data"]].apply(_row_to_json, axis=1) if cl["road_data"] else "{}"

    print(f"  Building state_extras JSONB ({len(cl['state_extras'])} keys)...")
    out["state_extras"] = df[cl["state_extras"]].apply(_row_to_json, axis=1) if cl["state_extras"] else "{}"

    print(f"  Building ranking_data JSONB ({len(cl['ranking'])} keys)...")
    out["ranking_data"] = df[cl["ranking"]].apply(_row_to_json, axis=1) if cl["ranking"] else "{}"
    print(f"  JSONB build: {time.time()-t_json:.1f}s")

    # ── NULL handling ─────────────────────────────────────────
    text_cols = [c for c in out.columns
                 if c not in ("x","y","crash_year","k_people","a_people","b_people","c_people",
                              "persons_injured","pedestrians_killed","pedestrians_injured",
                              "vehicle_count","road_data","state_extras","ranking_data")]
    for c in text_cols:
        out[c] = out[c].replace({"": None, "nan": None, "None": None})

    return out, cl


# ═══════════════════════════════════════════════════════════════
#  BULK INSERT (COPY)
# ═══════════════════════════════════════════════════════════════

def bulk_insert(conn, df, state_name):
    cur = conn.cursor()
    cols = [c for c in df.columns if c not in AUTO_COLUMNS]
    buf = StringIO()
    df[cols].to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N",
                    quoting=csv.QUOTE_NONE, escapechar=None)
    buf.seek(0)
    cur.copy_expert(
        f"COPY crashes_{state_name} ({','.join(cols)}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')",
        buf)
    return len(df)


# ═══════════════════════════════════════════════════════════════
#  SYNC
# ═══════════════════════════════════════════════════════════════

def sync(conn, df, state_name, abbr, fips, display, dry_run=False, resume=False):
    t0 = time.time()
    sync_df, cl = build_sync_df(df, abbr, state_name)
    n = len(sync_df)
    yr_min = int(sync_df["crash_year"].min()) if sync_df["crash_year"].notna().any() else 0
    yr_max = int(sync_df["crash_year"].max()) if sync_df["crash_year"].notna().any() else 0

    if dry_run:
        print(f"\n  {'='*65}")
        print(f"  DRY RUN — {display}")
        print(f"  {'='*65}")
        print(f"  Rows:            {n:,}")
        print(f"  Years:           [{yr_min}, {yr_max}]")
        print(f"  Tier 1:          {len(cl['tier1'])} cols")
        print(f"  road_data:       {len(cl['road_data'])} keys")
        print(f"  state_extras:    {len(cl['state_extras'])} keys")
        print(f"  ranking_data:    {len(cl['ranking'])} keys")
        print(f"  Sync columns:    {len(sync_df.columns)}")
        if "crash_severity" in sync_df.columns:
            print(f"  Severity:        {dict(sync_df['crash_severity'].value_counts().head(6))}")
        sample = json.loads(sync_df["road_data"].iloc[0])
        print(f"  road_data keys:  {len(sample)} (sample: {list(sample.keys())[:8]}...)")
        return

    cur = conn.cursor()
    rid = log_run(conn, state_name, "sync", "running",
                  meta={"rows": n, "years": f"{yr_min}-{yr_max}",
                        "tier1": len(cl["tier1"]), "road_data": len(cl["road_data"]),
                        "state_extras": len(cl["state_extras"]), "ranking": len(cl["ranking"])})
    print(f"  Pipeline run #{rid}")

    try:
        if not resume:
            # Full reload: DROP + CREATE partition
            print(f"  DROP TABLE IF EXISTS crashes_{state_name}")
            cur.execute(f"DROP TABLE IF EXISTS crashes_{state_name}"); conn.commit()
            print(f"  CREATE TABLE crashes_{state_name} PARTITION OF crashes")
            cur.execute(f"CREATE TABLE crashes_{state_name} PARTITION OF crashes FOR VALUES IN ('{state_name}')"); conn.commit()
        else:
            # Resume mode: keep existing data, find what's missing
            cur.execute(f"SELECT COUNT(*) FROM crashes_{state_name}")
            existing = cur.fetchone()[0]
            print(f"  RESUME MODE: {existing:,} rows already in crashes_{state_name}")

            # Get existing objectids to skip
            cur.execute(f"SELECT objectid FROM crashes_{state_name}")
            existing_ids = {r[0] for r in cur.fetchall()}
            print(f"  Found {len(existing_ids):,} existing objectids")

            # Filter sync_df to only new rows
            before = len(sync_df)
            sync_df = sync_df[~sync_df["objectid"].isin(existing_ids)]
            n = len(sync_df)
            print(f"  Filtered: {before:,} total → {n:,} new rows to insert")

            if n == 0:
                print(f"  ✅ All rows already present — nothing to insert")
                dur = round(time.time()-t0, 1)
                update_run(conn, rid, "success", rows=existing, dur=dur)
                return

        # COPY bulk insert
        print(f"  COPY {n:,} rows...")
        ti = time.time()
        inserted = bulk_insert(conn, sync_df, state_name); conn.commit()
        print(f"  ✅ {inserted:,} rows in {time.time()-ti:.1f}s")

        # Populate PostGIS geometry in batches (avoids timeout on large states)
        print(f"  Populating geom column (batched)...")
        ti2 = time.time()
        batch_size = 50000
        total_geom = 0
        while True:
            cur.execute(f"""
                UPDATE crashes_{state_name}
                SET geom = ST_SetSRID(ST_Point(x, y), 4326)
                WHERE x IS NOT NULL AND y IS NOT NULL AND geom IS NULL
                AND id IN (
                    SELECT id FROM crashes_{state_name}
                    WHERE geom IS NULL AND x IS NOT NULL
                    LIMIT {batch_size}
                )
            """)
            batch_count = cur.rowcount
            conn.commit()
            total_geom += batch_count
            if batch_count > 0:
                print(f"    geom batch: +{batch_count:,} ({total_geom:,} total)")
            if batch_count < batch_size:
                break
        print(f"  ✅ geom: {total_geom:,} points in {time.time()-ti2:.1f}s")

        # Update states
        cur.execute("""INSERT INTO states (abbr,name,fips,display_name,pipeline_status,total_crashes,year_range,last_sync_at)
            VALUES (%s,%s,%s,%s,'active',%s,int4range(%s,%s,'[)'),NOW())
            ON CONFLICT (abbr) DO UPDATE SET pipeline_status='active',total_crashes=EXCLUDED.total_crashes,
            year_range=EXCLUDED.year_range,last_sync_at=NOW()""",
            (abbr, state_name, fips, display, n, yr_min, yr_max+1)); conn.commit()

        # Refresh matview
        print(f"  Refreshing federal_summary...")
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY federal_summary"); conn.commit()
        except Exception:
            conn.rollback(); cur = conn.cursor()
            try:
                cur.execute("REFRESH MATERIALIZED VIEW federal_summary"); conn.commit()
            except Exception as e:
                print(f"  ⚠️  {e}"); conn.rollback()

        # Refresh baselines matview
        print(f"  Refreshing jurisdiction_baselines...")
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY jurisdiction_baselines")
            conn.commit()
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            try:
                cur.execute("REFRESH MATERIALIZED VIEW jurisdiction_baselines")
                conn.commit()
            except Exception as e:
                print(f"  ⚠️ baselines: {e}")
                conn.rollback()

        # Verify
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM crashes_{state_name}")
        actual = cur.fetchone()[0]
        dur = round(time.time()-t0, 1)
        update_run(conn, rid, "success", rows=actual, dur=dur)

        print(f"\n  {'='*65}")
        print(f"  SUPABASE SYNC COMPLETE: {display}")
        print(f"  {'='*65}")
        print(f"  Rows:            {actual:,}")
        print(f"  Years:           [{yr_min}, {yr_max}]")
        print(f"  Partition:       crashes_{state_name}")
        print(f"  Tier 1:          {len(cl['tier1'])} explicit columns")
        print(f"  road_data:       {len(cl['road_data'])} JSONB keys")
        print(f"  state_extras:    {len(cl['state_extras'])} JSONB keys ({abbr}_*)")
        print(f"  ranking_data:    {len(cl['ranking'])} JSONB keys")
        print(f"  Duration:        {dur}s")
        print(f"  Pipeline run:    #{rid}")
        print(f"  {'='*65}")
        if actual != n:
            print(f"  ⚠️  Expected {n:,}, got {actual:,}")

    except Exception as e:
        dur = round(time.time()-t0, 1)
        conn.rollback()
        update_run(conn, rid, "failed", dur=dur, err=str(e)[:500])
        print(f"\n  ❌ FAILED: {e}")
        raise


# ═══════════════════════════════════════════════════════════════
#  BATCH SYNC (GitHub Actions matrix — 25K rows per job)
# ═══════════════════════════════════════════════════════════════

def batch_sync(conn, filepath, state_name, abbr, fips, display,
               batch_num, batch_size, total_rows, resume=False):
    """Process a single batch of rows. Memory-safe for GitHub Actions."""
    import gc
    import pyarrow.parquet as pq

    t0 = time.time()
    cur = conn.cursor()

    # Calculate row range
    start_row = (batch_num - 1) * batch_size
    end_row = min(start_row + batch_size, total_rows)
    n_rows = end_row - start_row

    print(f"\n  {'='*65}")
    print(f"  BATCH {batch_num}: rows {start_row:,}-{end_row-1:,} ({n_rows:,} rows)")
    print(f"  State: {display} | Target: crashes_{state_name}")
    print(f"  {'='*65}")

    # Partition created by plan job (before matrix batches start).
    # Verify it exists — fail fast if something went wrong.
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'crashes_{state_name}')")
    if not cur.fetchone()[0]:
        raise RuntimeError(f"crashes_{state_name} partition does not exist. "
                           f"Plan job should have created it. Check plan job logs.")

    # ── Load ONLY this batch's rows using pyarrow slicing ──
    print(f"  Loading rows {start_row:,}-{end_row-1:,} from parquet...")
    pf = pq.ParquetFile(filepath)
    table = pf.read()
    df_batch = table.slice(start_row, n_rows).to_pandas()
    del table
    gc.collect()

    # Convert all to string (matches load_input behavior)
    for c in df_batch.columns:
        df_batch[c] = df_batch[c].astype(str).replace({"nan": "", "None": "", "NaT": ""})

    print(f"  Loaded: {len(df_batch):,} rows x {len(df_batch.columns)} cols")

    # ── Resume: check for existing objectids ──
    if resume:
        if "OBJECTID" in df_batch.columns:
            batch_ids = df_batch["OBJECTID"].tolist()
            placeholders = ",".join(["%s"] * len(batch_ids))
            cur.execute(f"SELECT objectid FROM crashes_{state_name} WHERE objectid IN ({placeholders})", batch_ids)
            existing = {r[0] for r in cur.fetchall()}
            before = len(df_batch)
            df_batch = df_batch[~df_batch["OBJECTID"].isin(existing)]
            print(f"  Resume: {before:,} -> {len(df_batch):,} new rows ({len(existing):,} already exist)")
            if len(df_batch) == 0:
                print(f"  Batch {batch_num} already complete -- skipping")
                return

    # ── Build sync_df for this batch ──
    sync_df, cl = build_sync_df(df_batch, abbr, state_name)
    del df_batch
    gc.collect()

    # ── COPY insert ──
    print(f"  COPY {len(sync_df):,} rows...")
    ti = time.time()
    inserted = bulk_insert(conn, sync_df, state_name)
    conn.commit()
    del sync_df
    gc.collect()

    dur = round(time.time() - t0, 1)
    print(f"  Batch {batch_num}: {inserted:,} rows in {dur}s")

    log_run(conn, state_name, f"batch_{batch_num}", "success",
            rows=inserted, dur=dur,
            meta={"batch": batch_num, "start": start_row, "end": end_row})


def finalize_sync(conn, state_name, abbr, fips, display):
    """Post-batch: geom, crash_date_parsed, matviews, states table."""
    import gc
    t0 = time.time()
    cur = conn.cursor()

    print(f"\n  {'='*65}")
    print(f"  FINALIZE: {display}")
    print(f"  {'='*65}")

    # Count rows
    cur.execute(f"SELECT COUNT(*) FROM crashes_{state_name}")
    total = cur.fetchone()[0]
    print(f"  Total rows: {total:,}")

    # Year range
    cur.execute(f"SELECT MIN(crash_year), MAX(crash_year) FROM crashes_{state_name} WHERE crash_year IS NOT NULL")
    yr_min, yr_max = cur.fetchone()
    yr_min = yr_min or 0
    yr_max = yr_max or 0
    print(f"  Year range: [{yr_min}, {yr_max}]")

    # Populate geom in batches
    print(f"  Populating geom column (batched)...")
    ti = time.time()
    geom_batch = 50000
    total_geom = 0
    while True:
        cur.execute(f"""
            UPDATE crashes_{state_name}
            SET geom = ST_SetSRID(ST_Point(x, y), 4326)
            WHERE x IS NOT NULL AND y IS NOT NULL AND geom IS NULL
            AND id IN (
                SELECT id FROM crashes_{state_name}
                WHERE geom IS NULL AND x IS NOT NULL
                LIMIT {geom_batch}
            )
        """)
        batch_count = cur.rowcount
        conn.commit()
        total_geom += batch_count
        if batch_count > 0:
            print(f"    geom batch: +{batch_count:,} ({total_geom:,} total)")
        if batch_count < geom_batch:
            break
    print(f"  geom: {total_geom:,} points in {time.time()-ti:.1f}s")

    # Update states table
    cur.execute("""INSERT INTO states (abbr,name,fips,display_name,pipeline_status,total_crashes,year_range,last_sync_at)
        VALUES (%s,%s,%s,%s,'active',%s,int4range(%s,%s,'[)'),NOW())
        ON CONFLICT (abbr) DO UPDATE SET pipeline_status='active',total_crashes=EXCLUDED.total_crashes,
        year_range=EXCLUDED.year_range,last_sync_at=NOW()""",
        (abbr, state_name, fips, display, total, yr_min, int(yr_max)+1))
    conn.commit()

    # Refresh matviews
    for mv in ["federal_summary", "jurisdiction_baselines"]:
        print(f"  Refreshing {mv}...")
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            conn.commit()
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
                conn.commit()
            except Exception as e:
                print(f"  WARNING {mv}: {e}")
                conn.rollback()
                cur = conn.cursor()

    # Verify
    cur.execute(f"SELECT COUNT(*), COUNT(geom) FROM crashes_{state_name}")
    total_final, geom_count = cur.fetchone()

    dur = round(time.time() - t0, 1)
    print(f"\n  {'='*65}")
    print(f"  FINALIZE COMPLETE: {display}")
    print(f"  Rows: {total_final:,} | Geom: {geom_count:,} | Duration: {dur}s")
    print(f"  {'='*65}")

    log_run(conn, state_name, "finalize", "success",
            rows=total_final, dur=dur,
            meta={"geom": geom_count, "years": f"{yr_min}-{yr_max}"})


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="CrashLens Supabase Sync v3.0")
    p.add_argument("--state", required=True, help="State abbreviation (de, va, co)")
    p.add_argument("--input", help="CSV or parquet.gz file")
    p.add_argument("--from-r2", action="store_true", help="Download from R2")
    p.add_argument("--dry-run", action="store_true", help="Validate only")
    p.add_argument("--resume", action="store_true",
                   help="Resume: skip DROP, only insert missing rows (by objectid)")
    p.add_argument("--batch", type=int, default=0,
                   help="Batch number (1-indexed). 0=full sync (legacy)")
    p.add_argument("--batch-size", type=int, default=25000,
                   help="Rows per batch (default 25000)")
    p.add_argument("--total-rows", type=int, default=0,
                   help="Total rows (from plan job)")
    p.add_argument("--finalize", action="store_true",
                   help="Run finalize only (geom, matviews, states)")
    args = p.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES:
        print(f"  ❌ Unknown: {abbr}"); sys.exit(1)
    display, state_name, fips = STATES[abbr]

    print(f"\n{'='*65}")
    print(f"  CrashLens Supabase Sync v3.0 — {display}")
    print(f"  {abbr} | crashes_{state_name} | FIPS {fips}")
    print(f"  3-Tier: 111 explicit + road_data JSONB + state_extras JSONB")
    print(f"{'='*65}\n")

    # ── Finalize mode (no input file needed) ──
    if args.finalize:
        conn = get_db_connection()
        print(f"  Connected to Supabase")
        try:
            finalize_sync(conn, state_name, abbr, fips, display)
        finally:
            conn.close()
        return

    # ── Resolve input file ──
    if args.from_r2:
        path = download_from_r2(state_name, abbr)
    elif args.input:
        path = args.input
        if not Path(path).exists():
            print(f"  Not found: {path}"); sys.exit(1)
    else:
        print("  --input or --from-r2 required"); sys.exit(1)

    # ── Batch mode: process one chunk ──
    if args.batch > 0:
        conn = get_db_connection()
        print(f"  Connected to Supabase")
        try:
            batch_sync(conn, path, state_name, abbr, fips, display,
                       batch_num=args.batch, batch_size=args.batch_size,
                       total_rows=args.total_rows, resume=args.resume)
        finally:
            conn.close()
        if args.from_r2 and Path(path).exists():
            Path(path).unlink(missing_ok=True)
        return

    # ── Legacy full sync ──
    df = load_input(path)

    if args.dry_run:
        sync(None, df, state_name, abbr, fips, display, dry_run=True)
    else:
        conn = get_db_connection()
        print(f"  Connected to Supabase")
        try:
            sync(conn, df, state_name, abbr, fips, display, resume=args.resume)
        finally:
            conn.close()

    if args.from_r2 and Path(path).exists():
        Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
