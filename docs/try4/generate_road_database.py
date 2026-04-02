#!/usr/bin/env python3
"""
generate_road_database.py — Unified Road Segment Database Builder (v3.0)

Consolidates 8+ data sources into ONE parquet file per state:
  OSM Roads (base) + HPMS + Intersections + POIs + Federal + Mapillary

All spatial joins use KDTree batch queries — builds in ~30s for Delaware.

USAGE:
  python generate_road_database.py --state de --cache-dir cache
  python generate_road_database.py --state va --cache-dir cache --upload
"""

import argparse, gc, math, os, re, sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.spatial import KDTree

# ─── VALUE MAPPINGS (CrashLens standard) ──────────────────────────────────

HPMS_FSYSTEM_TO_FC = {
    1:"1-Interstate",2:"2-Freeway/Expressway",3:"3-Principal Arterial",
    4:"4-Minor Arterial",5:"5-Major Collector",6:"6-Minor Collector",7:"7-Local",
}
FC_TO_SYSTEM = {
    "1-Interstate":"DOT Interstate","2-Freeway/Expressway":"DOT Primary",
    "3-Principal Arterial":"DOT Primary","4-Minor Arterial":"DOT Secondary",
    "5-Major Collector":"DOT Secondary","6-Minor Collector":"Non-DOT primary",
    "7-Local":"Non-DOT secondary",
}
FC_TO_OWNERSHIP = {
    "1-Interstate":"1. State Hwy Agency","2-Freeway/Expressway":"1. State Hwy Agency",
    "3-Principal Arterial":"1. State Hwy Agency","4-Minor Arterial":"1. State Hwy Agency",
    "5-Major Collector":"2. County Hwy Agency","6-Minor Collector":"2. County Hwy Agency",
    "7-Local":"3. City or Town Hwy Agency",
}
HPMS_OWNERSHIP_MAP = {1:"1. State Hwy Agency",2:"2. County Hwy Agency",
                      3:"3. City or Town Hwy Agency",4:"3. City or Town Hwy Agency"}
HPMS_SURFACE_MAP = {1:"2. Blacktop, Asphalt, Bituminous",2:"1. Concrete",
                    3:"2. Blacktop, Asphalt, Bituminous",4:"2. Blacktop, Asphalt, Bituminous",
                    5:"4. Slag, Gravel, Stone",6:"4. Slag, Gravel, Stone",
                    7:"5. Dirt",8:"3. Brick or Block"}
OSM_HIGHWAY_TO_FC = {
    "motorway":"1-Interstate","motorway_link":"1-Interstate",
    "trunk":"2-Freeway/Expressway","trunk_link":"2-Freeway/Expressway",
    "primary":"3-Principal Arterial","primary_link":"3-Principal Arterial",
    "secondary":"4-Minor Arterial","secondary_link":"4-Minor Arterial",
    "tertiary":"5-Major Collector","tertiary_link":"5-Major Collector",
    "unclassified":"6-Minor Collector",
    "residential":"7-Local","living_street":"7-Local","service":"7-Local",
}


def _build_tree(lats, lons):
    """Build KDTree from lat/lon arrays (meters)."""
    mid = np.nanmean(lats)
    scale = math.cos(math.radians(mid))
    pts = np.column_stack([np.array(lats)*111000, np.array(lons)*111000*scale])
    return KDTree(pts), scale

def _query_tree(tree, lats, lons, scale, max_m=200):
    """Batch query KDTree. Returns (distances, indices)."""
    pts = np.column_stack([np.array(lats)*111000, np.array(lons)*111000*scale])
    return tree.query(pts, k=1)


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 2: HPMS join (KDTree batch)
# ═════════════════════════════════════════════════════════════════════════════

def join_hpms(roads, cache_dir, abbr):
    path = Path(cache_dir) / f"{abbr}_hpms.parquet"
    if not path.exists():
        print("  ⚠️  HPMS not found — skipping")
        return roads

    hpms = pd.read_parquet(path)
    print(f"  HPMS: {len(hpms):,} segments")

    tree, scale = _build_tree(hpms["mid_lat"].values, hpms["mid_lon"].values)
    dists, idxs = _query_tree(tree, roads["mid_lat"].values, roads["mid_lon"].values, scale)

    # Copy HPMS columns where distance < 200m
    mask = dists < 200
    hpms_cols = ["f_system","aadt","speed_limit","through_lanes","ownership",
                 "facility_type","median_type","median_width","access_control",
                 "surface_type","urban_code","nhs","terrain_type","curve_class",
                 "aadt_combination","aadt_single_unit","design_speed","lane_width",
                 "shoulder_width_r","route_name"]

    for col in hpms_cols:
        if col in hpms.columns:
            dst = f"hpms_{col}"
            vals = np.full(len(roads), np.nan, dtype=object)
            vals[mask] = hpms[col].values[idxs[mask]]
            roads[dst] = vals

    matched = mask.sum()
    print(f"  ✅ HPMS: {matched:,}/{len(roads):,} ({matched/len(roads)*100:.1f}%)")

    # Derive standard values
    roads["fc_standard"] = roads["hpms_f_system"].apply(
        lambda x: HPMS_FSYSTEM_TO_FC.get(int(x), "") if pd.notna(x) and str(x).strip() not in ("","nan") else "")
    no_fc = roads["fc_standard"] == ""
    roads.loc[no_fc, "fc_standard"] = roads.loc[no_fc, "highway"].map(OSM_HIGHWAY_TO_FC).fillna("")

    roads["ownership_standard"] = roads.apply(
        lambda r: HPMS_OWNERSHIP_MAP.get(int(r["hpms_ownership"]), FC_TO_OWNERSHIP.get(r["fc_standard"],""))
        if pd.notna(r.get("hpms_ownership")) and str(r.get("hpms_ownership","")).strip() not in ("","nan")
        else FC_TO_OWNERSHIP.get(r["fc_standard"],""), axis=1)
    roads["system_standard"] = roads["fc_standard"].map(FC_TO_SYSTEM).fillna("")
    roads["surface_standard"] = roads["hpms_surface_type"].apply(
        lambda x: HPMS_SURFACE_MAP.get(int(x),"") if pd.notna(x) and str(x).strip() not in ("","nan") else "")

    del hpms, tree; gc.collect()
    return roads


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 3: Intersection join (vectorized)
# ═════════════════════════════════════════════════════════════════════════════

def join_intersections(roads, cache_dir, abbr):
    path = Path(cache_dir) / f"{abbr}_intersections.parquet"
    if not path.exists():
        print("  ⚠️  Intersections not found — skipping")
        return roads

    idf = pd.read_parquet(path)
    print(f"  Intersections: {len(idf):,} nodes")

    # Node degree lookup via endpoint matching
    # For each road's u_lat/u_lon, find nearest intersection
    if "lat" in idf.columns and "degree" in idf.columns:
        tree, scale = _build_tree(idf["lat"].values, idf["lon"].values)

        # u_node degree
        d_u, i_u = _query_tree(tree, roads["u_lat"].values, roads["u_lon"].values, scale)
        roads["u_node_degree"] = np.where(d_u < 30, idf["degree"].values[i_u], 0)

        # v_node degree
        d_v, i_v = _query_tree(tree, roads["v_lat"].values, roads["v_lon"].values, scale)
        roads["v_node_degree"] = np.where(d_v < 30, idf["degree"].values[i_v], 0)

        filled = int((roads["u_node_degree"] > 0).sum())
        print(f"  ✅ Intersections: {filled:,} segments with degree data")
        del tree; gc.collect()
    else:
        roads["u_node_degree"] = 0
        roads["v_node_degree"] = 0

    # Intersection names from road segment endpoints
    node_names = defaultdict(set)
    if "u_node" in roads.columns:
        names = roads["name"].fillna("").values
        refs = roads["ref"].fillna("").values
        u_nodes = roads["u_node"].values
        v_nodes = roads["v_node"].values
        for j in range(len(roads)):
            label = str(names[j]).strip()
            if not label or label == "nan":
                label = str(refs[j]).strip()
            if label and label != "nan":
                node_names[u_nodes[j]].add(label)
                node_names[v_nodes[j]].add(label)

        u_int = []
        v_int = []
        for j in range(len(roads)):
            u_n = sorted(node_names.get(u_nodes[j], set()))
            u_int.append(f"{u_n[0]} & {u_n[1]}" if len(u_n) >= 2 else (u_n[0] if u_n else ""))
            v_n = sorted(node_names.get(v_nodes[j], set()))
            v_int.append(f"{v_n[0]} & {v_n[1]}" if len(v_n) >= 2 else (v_n[0] if v_n else ""))
        roads["u_intersection_name"] = u_int
        roads["v_intersection_name"] = v_int
    else:
        roads["u_intersection_name"] = ""
        roads["v_intersection_name"] = ""

    del idf, node_names; gc.collect()
    return roads


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 4: POI proximity (KDTree per category — batch)
# ═════════════════════════════════════════════════════════════════════════════

POI_THRESHOLDS = {
    "bar": (457, "poi_near_bar_ft"),
    "school": (305, "poi_near_school_ft"),
    "crossing": (30, "poi_near_crossing_ft"),
    "parking": (46, "poi_near_parking_ft"),
    "rail_xing": (46, "poi_near_rail_xing_ft"),
    "signal": (30, "poi_near_signal_ft"),
    "stop_sign": (20, "poi_near_stop_sign_ft"),
    "hospital": (80467, "poi_nearest_hospital_mi"),
}

def join_pois(roads, cache_dir, abbr):
    path = Path(cache_dir) / f"{abbr}_pois.parquet"
    if not path.exists():
        print("  ⚠️  POIs not found — skipping")
        return roads

    poi_df = pd.read_parquet(path)
    print(f"  POIs: {len(poi_df):,} across {poi_df['category'].nunique()} categories")

    r_lats = roads["mid_lat"].values
    r_lons = roads["mid_lon"].values

    for cat, (thresh_m, col) in POI_THRESHOLDS.items():
        cat_pois = poi_df[poi_df["category"] == cat]
        if len(cat_pois) == 0:
            roads[col] = ""
            continue

        tree, scale = _build_tree(cat_pois["lat"].values, cat_pois["lon"].values)
        dists, _ = _query_tree(tree, r_lats, r_lons, scale)

        if cat == "hospital":
            # Continuous distance in miles
            vals = np.where(dists < thresh_m, np.round(dists / 1609.34, 1), np.nan)
            roads[col] = [str(v) if not np.isnan(v) else "" for v in vals]
        else:
            # Distance in feet (within threshold)
            vals = np.where(dists < thresh_m, np.round(dists * 3.28084).astype(int), -1)
            roads[col] = [str(v) if v >= 0 else "" for v in vals]

        filled = int((dists < thresh_m).sum())
        print(f"    {cat}: {filled:,} segments within {thresh_m}m")
        del tree; gc.collect()

    del poi_df; gc.collect()
    print(f"  ✅ POI proximity: {len(POI_THRESHOLDS)} categories")
    return roads


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 5: Federal data (KDTree batch per source)
# ═════════════════════════════════════════════════════════════════════════════

FEDERAL_SOURCES = {
    "schools":        {"thresh": 305,  "cols": {"enrollment": "fed_school_enrollment"}},
    "bridges":        {"thresh": 46,   "cols": {"condition": "fed_bridge_condition", "year_built": "fed_bridge_year"}},
    "rail_crossings": {"thresh": 46,   "cols": {"warning_device": "fed_rail_warning", "trains_per_day": "fed_rail_trains_per_day"}},
    "transit":        {"thresh": 152,  "cols": {"_distance_ft": "fed_near_transit_ft"}},
}

def join_federal(roads, cache_dir, abbr):
    r_lats = roads["mid_lat"].values
    r_lons = roads["mid_lon"].values

    for src_name, cfg in FEDERAL_SOURCES.items():
        path = Path(cache_dir) / f"{abbr}_{src_name}.parquet"
        for dst in cfg["cols"].values():
            roads[dst] = ""

        if not path.exists():
            continue

        sdf = pd.read_parquet(path)
        if len(sdf) == 0:
            continue

        tree, scale = _build_tree(sdf["lat"].values, sdf["lon"].values)
        dists, idxs = _query_tree(tree, r_lats, r_lons, scale)
        mask = dists < cfg["thresh"]

        for src_col, dst_col in cfg["cols"].items():
            if src_col == "_distance_ft":
                roads.loc[mask, dst_col] = (dists[mask] * 3.28084).astype(int).astype(str)
            elif src_col in sdf.columns:
                vals = sdf[src_col].values[idxs]
                for i in np.where(mask)[0]:
                    v = vals[i]
                    if pd.notna(v) and str(v).strip() not in ("", "nan", "0"):
                        roads.iat[i, roads.columns.get_loc(dst_col)] = str(v)

        matched = int(mask.sum())
        print(f"    {src_name}: {matched:,} segments matched")
        del sdf, tree; gc.collect()

    print(f"  ✅ Federal: {len(FEDERAL_SOURCES)} sources")
    return roads


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 6: Mapillary signs (KDTree batch aggregation)
# ═════════════════════════════════════════════════════════════════════════════

SIGN_CATEGORIES = {
    "stop_sign":    ["regulatory--stop"],
    "yield_sign":   ["regulatory--yield"],
    "speed_sign":   ["regulatory--maximum-speed-limit"],
    "signal":       ["object--traffic-light"],
    "street_light": ["object--street-light"],
    "crosswalk":    ["marking--discrete--crosswalk"],
    "fire_hydrant": ["object--fire-hydrant"],
    "warning_sign": ["warning--"],
    "guard_rail":   ["object--guard-rail"],
    "no_parking":   ["regulatory--no-parking"],
}

def join_mapillary(roads, cache_dir, abbr):
    import glob
    pq = Path(cache_dir) / f"{abbr}_mapillary.parquet"
    csvs = glob.glob(str(Path(cache_dir) / f"*mapillary*.csv")) + \
           glob.glob(str(Path(cache_dir) / f"*traffic*inventory*.csv"))

    if pq.exists():
        sdf = pd.read_parquet(pq)
    elif csvs:
        sdf = pd.read_csv(csvs[0], dtype=str)
    else:
        print("  ⚠️  No Mapillary data — skipping")
        for cat in SIGN_CATEGORIES:
            roads[f"map_{cat}_count"] = 0
        roads["map_speed_value"] = ""
        roads["map_signal_heads"] = ""
        return roads

    sdf.columns = sdf.columns.str.lower().str.strip()
    if "latitude" in sdf.columns:
        sdf = sdf.rename(columns={"latitude":"lat","longitude":"lon"})
    print(f"  Mapillary: {len(sdf):,} features")

    s_lats = pd.to_numeric(sdf["lat"], errors="coerce")
    s_lons = pd.to_numeric(sdf["lon"], errors="coerce")
    valid = s_lats.notna() & s_lons.notna()
    sdf = sdf[valid].copy()
    s_lats = s_lats[valid].values
    s_lons = s_lons[valid].values

    # Find nearest road segment for each sign (batch KDTree)
    r_tree, r_scale = _build_tree(roads["mid_lat"].values, roads["mid_lon"].values)
    dists, seg_idxs = _query_tree(r_tree, s_lats, s_lons, r_scale)

    # Filter to signs within 50m of a road
    near_mask = dists < 50
    near_seg = seg_idxs[near_mask]
    near_sdf = sdf[near_mask].copy()
    cls_col = "class" if "class" in near_sdf.columns else "object_value" if "object_value" in near_sdf.columns else None

    # Initialize count columns
    for cat in SIGN_CATEGORIES:
        roads[f"map_{cat}_count"] = 0
    roads["map_speed_value"] = ""
    roads["map_signal_heads"] = ""

    if cls_col is None or len(near_sdf) == 0:
        print("  ⚠️  No classifiable Mapillary features")
        return roads

    classes = near_sdf[cls_col].fillna("").values
    speeds = near_sdf["speed"].values if "speed" in near_sdf.columns else [""] * len(near_sdf)
    sig_heads = near_sdf["signal_heads"].values if "signal_heads" in near_sdf.columns else [""] * len(near_sdf)

    # Aggregate counts per segment
    seg_counts = defaultdict(lambda: defaultdict(int))
    seg_speed = {}
    seg_signal = {}

    for j in range(len(near_sdf)):
        seg = near_seg[j]
        cls = str(classes[j]).lower()

        for cat, prefixes in SIGN_CATEGORIES.items():
            if any(p in cls for p in prefixes):
                seg_counts[seg][cat] += 1
                break

        spd = str(speeds[j]).strip()
        if spd and spd not in ("", "nan"):
            seg_speed[seg] = spd

        sh = str(sig_heads[j]).strip()
        if sh and sh not in ("", "nan"):
            seg_signal[seg] = sh

    # Write to DataFrame
    for seg, counts in seg_counts.items():
        for cat, count in counts.items():
            roads.iat[seg, roads.columns.get_loc(f"map_{cat}_count")] = count

    for seg, spd in seg_speed.items():
        roads.iat[seg, roads.columns.get_loc("map_speed_value")] = spd
    for seg, sh in seg_signal.items():
        roads.iat[seg, roads.columns.get_loc("map_signal_heads")] = sh

    total_signs = sum(sum(c.values()) for c in seg_counts.values())
    segs_with = len(seg_counts)
    print(f"  ✅ Mapillary: {total_signs:,} signs on {segs_with:,} segments")
    del sdf, r_tree; gc.collect()
    return roads


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════

def build(abbr, cache_dir, skip_mapillary=False):
    t0 = time.time()
    print(f"\n{'═'*65}")
    print(f"  CrashLens Road Database Builder v3.0 | {abbr.upper()}")
    print(f"{'═'*65}\n")

    # Step 1: Base OSM roads
    rpath = Path(cache_dir) / f"{abbr}_roads.parquet"
    if not rpath.exists():
        print(f"  ❌ {rpath} not found"); return None
    roads = pd.read_parquet(rpath)
    print(f"  [1/6] OSM roads: {len(roads):,} segments × {len(roads.columns)} cols")

    # Step 2: HPMS
    print("\n  [2/6] HPMS federal data...")
    roads = join_hpms(roads, cache_dir, abbr); gc.collect()

    # Step 3: Intersections
    print("\n  [3/6] Intersections...")
    roads = join_intersections(roads, cache_dir, abbr); gc.collect()

    # Step 4: POI proximity
    print("\n  [4/6] POI proximity...")
    roads = join_pois(roads, cache_dir, abbr); gc.collect()

    # Step 5: Federal
    print("\n  [5/6] Federal safety data...")
    roads = join_federal(roads, cache_dir, abbr); gc.collect()

    # Step 6: Mapillary
    if not skip_mapillary:
        print("\n  [6/6] Mapillary signs...")
        roads = join_mapillary(roads, cache_dir, abbr); gc.collect()
    else:
        print("\n  [6/6] Mapillary — skipped")
        for cat in SIGN_CATEGORIES:
            roads[f"map_{cat}_count"] = 0
        roads["map_speed_value"] = ""
        roads["map_signal_heads"] = ""

    # Save — normalize types for parquet
    out = Path(cache_dir) / f"{abbr}_road_database.parquet"
    numeric = {"mid_lat","mid_lon","u_lat","u_lon","v_lat","v_lon","length_m",
               "curvature","u_node_degree","v_node_degree"}
    numeric |= {c for c in roads.columns if c.startswith("map_") and c.endswith("_count")}
    for col in roads.columns:
        if col in numeric:
            roads[col] = pd.to_numeric(roads[col], errors="coerce").fillna(0)
        elif roads[col].dtype == object:
            roads[col] = roads[col].fillna("").astype(str)

    roads.to_parquet(out, index=False)
    sz = out.stat().st_size / 1e6
    elapsed = time.time() - t0

    print(f"\n{'═'*65}")
    print(f"  ✅ {len(roads):,} segments × {len(roads.columns)} cols → {out} ({sz:.1f} MB)")
    print(f"     Built in {elapsed:.0f}s")
    print(f"{'═'*65}\n")
    return roads


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--state", required=True)
    p.add_argument("--cache-dir", default="cache")
    p.add_argument("--skip-mapillary", action="store_true")
    p.add_argument("--upload", action="store_true")
    a = p.parse_args()
    result = build(a.state.lower(), a.cache_dir, a.skip_mapillary)
    if result is None: sys.exit(1)
    if a.upload:
        ab = a.state.lower()
        out = Path(a.cache_dir) / f"{ab}_road_database.parquet"
        gz = f"{out}.gz"
        os.system(f"gzip -cn {out} > {gz}")
        os.system(f'aws s3 cp {gz} s3://crash-lens-data/{ab}/cache/{ab}_road_database.parquet.gz '
                  f'--endpoint-url https://${{CF_ACCOUNT_ID}}.r2.cloudflarestorage.com --only-show-errors')
        print("  ✅ Uploaded to R2")
