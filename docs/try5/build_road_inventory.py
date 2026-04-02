#!/usr/bin/env python3
"""
build_road_inventory.py — Consolidate all cache files into one road database
===============================================================================
Joins roads + intersections + State DOT + HPMS + POIs + bridges + rail + schools
+ transit + Mapillary into a single statewide parquet file, spatially linked by GPS.

BASE LAYER: OSM road segments (mid_lat, mid_lon)
ENRICHMENT: Each data source joins to nearest road segment via STRtree (linestring)
    with KDTree (midpoint) fallback. STRtree matches to actual road geometry for accuracy.

DATA AUTHORITY HIERARCHY:
    Tier A: State DOT Inventory (OPTIONAL — highest authority when available)
            OVERWRITES all columns. File: {abbr}_state_dot.parquet.gz
            If file missing → silently skipped, Tier B becomes highest.
    Tier B: HPMS Federal Data (highest authority when Tier A unavailable)
            OVERWRITES FC, Ownership, SYSTEM, Facility Type, Surface Type.
    Tier C: OSM Road Network (fills gaps left by A+B)
    Tier D: POI Proximity (always runs last)

OUTPUT: {state}/cache/{abbr}_road_inventory.parquet.gz

INPUT FILES (from {state}/cache/):
    {abbr}_roads.parquet.gz          <- base road network
    {abbr}_intersections.parquet.gz  <- intersection nodes
    {abbr}_state_dot.parquet.gz      <- State DOT inventory (Tier A, OPTIONAL)
    {abbr}_hpms.parquet.gz           <- FHWA road inventory (Tier B, 63 cols v2)
    {abbr}_pois.parquet.gz           <- OSM POIs (bars, schools, signals, etc.)
    {abbr}_bridges.parquet.gz        <- BTS bridges
    {abbr}_rail_crossings.parquet.gz <- BTS rail crossings
    {abbr}_schools.parquet.gz        <- Urban Institute schools
    {abbr}_transit.parquet.gz        <- BTS transit stops
    {abbr}_mapillary.parquet.gz      <- Mapillary traffic inventory
    OR traffic-inventory.parquet.gz  <- Mapillary (alternate name)

PROXIMITY THRESHOLDS (all in feet):
    Schools: 1500ft, Transit: 500ft, Bridges: 500ft, Rail: 500ft
    POI bars: 1500ft, hospitals: 2000ft, parking/fuel: 500ft

MAPILLARY (feet): Speed: 500ft, Signals: 500ft, General: 100ft

USAGE:
    python build_road_inventory.py --state de
    python build_road_inventory.py --state de --upload
    python build_road_inventory.py --state va --cache-dir cache --local-only
"""

import argparse, gc, gzip, json, math, os, shutil, sys, time, warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", message=".*DataFrame is highly fragmented.*")

FT_TO_M = 0.3048
M_TO_FT = 3.28084
EARTH_R = 6371000

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY
# ═══════════════════════════════════════════════════════════════
STATES = {
    "al":("Alabama","alabama","01"),"ak":("Alaska","alaska","02"),
    "az":("Arizona","arizona","04"),"ar":("Arkansas","arkansas","05"),
    "ca":("California","california","06"),"co":("Colorado","colorado","08"),
    "ct":("Connecticut","connecticut","09"),"de":("Delaware","delaware","10"),
    "dc":("District of Columbia","district_of_columbia","11"),
    "fl":("Florida","florida","12"),"ga":("Georgia","georgia","13"),
    "hi":("Hawaii","hawaii","15"),"id":("Idaho","idaho","16"),
    "il":("Illinois","illinois","17"),"in":("Indiana","indiana","18"),
    "ia":("Iowa","iowa","19"),"ks":("Kansas","kansas","20"),
    "ky":("Kentucky","kentucky","21"),"la":("Louisiana","louisiana","22"),
    "me":("Maine","maine","23"),"md":("Maryland","maryland","24"),
    "ma":("Massachusetts","massachusetts","25"),"mi":("Michigan","michigan","26"),
    "mn":("Minnesota","minnesota","27"),"ms":("Mississippi","mississippi","28"),
    "mo":("Missouri","missouri","29"),"mt":("Montana","montana","30"),
    "ne":("Nebraska","nebraska","31"),"nv":("Nevada","nevada","32"),
    "nh":("New Hampshire","new_hampshire","33"),"nj":("New Jersey","new_jersey","34"),
    "nm":("New Mexico","new_mexico","35"),"ny":("New York","new_york","36"),
    "nc":("North Carolina","north_carolina","37"),"nd":("North Dakota","north_dakota","38"),
    "oh":("Ohio","ohio","39"),"ok":("Oklahoma","oklahoma","40"),
    "or":("Oregon","oregon","41"),"pa":("Pennsylvania","pennsylvania","42"),
    "ri":("Rhode Island","rhode_island","44"),"sc":("South Carolina","south_carolina","45"),
    "sd":("South Dakota","south_dakota","46"),"tn":("Tennessee","tennessee","47"),
    "tx":("Texas","texas","48"),"ut":("Utah","utah","49"),
    "vt":("Vermont","vermont","50"),"va":("Virginia","virginia","51"),
    "wa":("Washington","washington","53"),"wv":("West Virginia","west_virginia","54"),
    "wi":("Wisconsin","wisconsin","55"),"wy":("Wyoming","wyoming","56"),
}

# ═══════════════════════════════════════════════════════════════
#  SPATIAL UTILITIES (KDTree + STRtree)
# ═══════════════════════════════════════════════════════════════
def _to_cartesian(lats, lons):
    """Convert lat/lon to 3D cartesian on unit sphere. Gives exact great-circle distances."""
    lat_r = np.deg2rad(lats)
    lon_r = np.deg2rad(lons)
    x = np.cos(lat_r) * np.cos(lon_r)
    y = np.cos(lat_r) * np.sin(lon_r)
    z = np.sin(lat_r)
    return np.column_stack([x, y, z])

def build_kdtree(lats, lons):
    from scipy.spatial import cKDTree
    return cKDTree(_to_cartesian(lats, lons))

def query_nearest(tree, lats, lons):
    points = _to_cartesian(lats, lons)
    chord_dists, indices = tree.query(points, k=1)
    # Convert chord distance to great-circle distance
    dists_m = 2 * EARTH_R * np.arcsin(np.clip(chord_dists / 2, 0, 1))
    return dists_m, indices


# ── STRtree: point-to-linestring (accurate for road matching) ──

_road_strtree = None
_road_linestrings = None

def build_road_strtree(roads):
    """Build STRtree on road LineStrings for accurate point-to-road matching.
    Call once after roads are loaded. All asset enrichment uses this.
    """
    global _road_strtree, _road_linestrings
    try:
        from shapely.geometry import LineString, Point
        from shapely import STRtree
    except ImportError:
        try:
            from shapely.geometry import LineString, Point
            from shapely.strtree import STRtree
        except ImportError:
            print("    ⚠️ Shapely not available — using KDTree fallback")
            return False

    import json as _json
    t0 = time.time()
    lines = []
    gc = roads["geometry_coords"].fillna("").astype(str).values if "geometry_coords" in roads.columns else None

    for i in range(len(roads)):
        parsed = False
        if gc is not None and gc[i].startswith("["):
            try:
                coords = _json.loads(gc[i])
                if len(coords) >= 2:
                    lines.append(LineString(coords))
                    parsed = True
            except Exception:
                pass
        if not parsed:
            # Fallback to u/v or midpoint
            ml, mo = float(roads["mid_lat"].iloc[i]), float(roads["mid_lon"].iloc[i])
            lines.append(Point(mo, ml).buffer(0.00001))

    _road_linestrings = lines
    _road_strtree = STRtree(lines)
    elapsed = time.time() - t0
    n_ls = sum(1 for l in lines if l.geom_type == "LineString")
    print(f"    STRtree built: {n_ls:,} linestrings + {len(lines)-n_ls:,} points ({elapsed:.1f}s)")
    return True


def query_nearest_road_linestring(point_lats, point_lons, threshold_m=None):
    """Find nearest road (linestring) for each point using STRtree.
    Returns (indices, dists_m). Much more accurate than KDTree midpoint.
    """
    global _road_strtree, _road_linestrings
    from shapely.geometry import Point

    n = len(point_lats)
    points = [Point(point_lons[i], point_lats[i]) for i in range(n)]

    # STRtree nearest
    try:
        nearest_idx = _road_strtree.nearest(points)
    except (TypeError, AttributeError):
        nearest_idx = np.array([_road_strtree.nearest(pt) for pt in points], dtype=int)

    nearest_idx = np.asarray(nearest_idx, dtype=int)

    # Compute exact distance in meters
    M = 111320.0
    cos_lat = np.cos(np.radians(np.mean(point_lats)))
    dists_m = np.zeros(n)

    for i in range(n):
        ri = nearest_idx[i]
        geom = _road_linestrings[ri]
        pt = points[i]
        # Shapely distance is in degrees — convert to meters
        deg_dist = pt.distance(geom)
        # Approximate meters (good enough for thresholds)
        dists_m[i] = deg_dist * M * cos_lat

    return nearest_idx, dists_m

def proximity_yesno(road_lats, road_lons, poi_lats, poi_lons, threshold_ft):
    if len(poi_lats) == 0:
        return np.zeros(len(road_lats), dtype=bool)
    tree = build_kdtree(poi_lats, poi_lons)
    dists_m, _ = query_nearest(tree, road_lats, road_lons)
    return dists_m <= (threshold_ft * FT_TO_M)

def count_within_radius(road_lats, road_lons, poi_lats, poi_lons, radius_ft, chunk_size=500_000):
    """Count POIs within radius_ft for each road. Chunked for memory safety on large states."""
    if len(poi_lats) == 0:
        return np.zeros(len(road_lats), dtype=int)
    tree = build_kdtree(poi_lats, poi_lons)
    radius_m = radius_ft * FT_TO_M
    chord_r = 2 * np.sin(radius_m / (2 * EARTH_R))
    
    n = len(road_lats)
    counts = np.zeros(n, dtype=int)
    
    # Process in chunks to limit memory from query_ball_point results
    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        chunk_pts = _to_cartesian(road_lats[start:end], road_lons[start:end])
        results = tree.query_ball_point(chunk_pts, chord_r)
        for i, r in enumerate(results):
            counts[start + i] = len(r)
        del results, chunk_pts
    
    return counts

# ═══════════════════════════════════════════════════════════════
#  FILE LOADING
# ═══════════════════════════════════════════════════════════════
def load_parquet_gz(path):
    if not path.exists(): return None
    try: return pd.read_parquet(path)
    except Exception: pass
    try:
        import io
        with gzip.open(path, "rb") as f:
            return pd.read_parquet(io.BytesIO(f.read()))
    except Exception as e:
        print(f"    Warning: Could not load {path}: {e}")
        return None

# ═══════════════════════════════════════════════════════════════
#  ENRICHMENT FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def enrich_intersections(roads, ints):
    print("    Intersections...", end=" ", flush=True)
    if ints is None or len(ints) == 0:
        roads["is_intersection"] = "No"; roads["intersection_degree"] = 0
        print("skipped"); return

    int_set = set(ints["node_id"].values)
    deg_map = dict(zip(ints["node_id"], ints["degree"]))

    u_in = roads["u_node"].isin(int_set)
    v_in = roads["v_node"].isin(int_set)
    at_int = u_in | v_in
    roads["is_intersection"] = np.where(at_int, "Yes", "No")

    u_deg = roads["u_node"].map(deg_map).fillna(0).astype(int)
    v_deg = roads["v_node"].map(deg_map).fillna(0).astype(int)
    roads["intersection_degree"] = np.maximum(u_deg, v_deg)

    n = at_int.sum()
    print(f"{n:,} segments ({n/len(roads)*100:.1f}%)")


def enrich_ramps(roads):
    """Derive ramp classification from highway tag + node connectivity.

    Adds:
      is_ramp:   "Yes" / "No"
      ramp_type: "Exit" / "Entrance" / "Connector" / "Ramp" / ""
    """
    print("    Ramps...", end=" ", flush=True)
    hw = roads["highway"].fillna("").str.strip()

    # Any _link tag = ramp
    is_ramp = hw.str.contains("_link", na=False)
    roads["is_ramp"] = np.where(is_ramp, "Yes", "No")

    if not is_ramp.any():
        roads["ramp_type"] = ""
        print("0 ramps"); return

    # Build mainline node set (motorway + trunk = limited-access roads)
    mainline_mask = hw.isin(["motorway", "trunk"])
    mainline_nodes = set(roads.loc[mainline_mask, "u_node"].values) | \
                     set(roads.loc[mainline_mask, "v_node"].values)

    # Classify ramps by node connectivity + oneway direction
    # OSM digraph: u→v = travel direction for oneway roads
    # Exit ramp: u_node on mainline (starts on highway, goes to surface)
    # Entrance ramp: v_node on mainline (starts on surface, merges to highway)
    # Connector: both endpoints on mainline (motorway-to-motorway)
    ow = roads["oneway"].fillna("").astype(str).str.strip().str.lower()
    is_oneway = ow.isin(["yes", "true", "1"])

    u_on_main = roads["u_node"].isin(mainline_nodes)
    v_on_main = roads["v_node"].isin(mainline_nodes)

    ramp_type = pd.Series("", index=roads.index)
    ramp_type[is_ramp] = "Ramp"  # default for all ramps

    # Connector: both ends on mainline
    connector = is_ramp & u_on_main & v_on_main
    ramp_type[connector] = "Connector"

    # Exit: starts on mainline, oneway, other end NOT on mainline
    exit_mask = is_ramp & u_on_main & ~v_on_main & is_oneway
    ramp_type[exit_mask] = "Exit"

    # Entrance: ends on mainline, oneway, other end NOT on mainline
    entrance_mask = is_ramp & v_on_main & ~u_on_main & is_oneway
    ramp_type[entrance_mask] = "Entrance"

    roads["ramp_type"] = ramp_type

    n_ramp = is_ramp.sum()
    n_exit = exit_mask.sum()
    n_entrance = entrance_mask.sum()
    n_conn = connector.sum()
    n_other = n_ramp - n_exit - n_entrance - n_conn
    print(f"{n_ramp:,} ramps (exit={n_exit:,}, entrance={n_entrance:,}, "
          f"connector={n_conn:,}, other={n_other:,})")


def enrich_hpms(roads, hpms, threshold_m=100):
    print("    HPMS (all columns)...", end=" ", flush=True)
    if hpms is None or len(hpms) == 0:
        print("skipped"); return

    use_strtree = _road_strtree is not None

    if use_strtree:
        # STRtree: HPMS point → nearest road linestring (accurate)
        indices, dists_m = query_nearest_road_linestring(
            hpms["mid_lat"].values, hpms["mid_lon"].values)

        # Invert: for each ROAD, find closest HPMS that matched to it
        n_roads = len(roads)
        best_hpms_idx = np.full(n_roads, -1, dtype=int)
        best_hpms_dist = np.full(n_roads, np.inf)

        for hpms_i in range(len(hpms)):
            road_i = indices[hpms_i]
            d = dists_m[hpms_i]
            if d < best_hpms_dist[road_i]:
                best_hpms_dist[road_i] = d
                best_hpms_idx[road_i] = hpms_i

        matched = (best_hpms_idx >= 0) & (best_hpms_dist <= threshold_m)
        final_indices = np.clip(best_hpms_idx, 0, len(hpms) - 1)
        final_dists_m = best_hpms_dist
    else:
        # KDTree fallback (midpoint-to-midpoint)
        tree = build_kdtree(hpms["mid_lat"].values, hpms["mid_lon"].values)
        final_dists_m, final_indices = query_nearest(tree, roads["mid_lat"].values, roads["mid_lon"].values)
        matched = final_dists_m <= threshold_m

    skip = {"mid_lat", "mid_lon"}
    for col in hpms.columns:
        if col in skip: continue
        vals = hpms[col].values[final_indices]
        try:
            _ = vals + 0  # numeric check
            is_numeric = True
        except (TypeError, ValueError):
            is_numeric = False
        if is_numeric:
            roads[f"hpms_{col}"] = np.where(matched, vals, 0)
        else:
            roads[f"hpms_{col}"] = np.where(matched, vals, "")

    roads["hpms_match_dist_ft"] = np.round(final_dists_m * M_TO_FT, 1)
    roads["hpms_matched"] = np.where(matched, "Yes", "No")
    n = matched.sum()
    tag = "STRtree" if use_strtree else "KDTree"
    print(f"{n:,}/{len(roads):,} matched ({n/len(roads)*100:.1f}%) [{tag}]")


def enrich_state_dot(roads, sdot, threshold_m=100):
    """
    Tier A: State DOT Road Inventory enrichment (HIGHEST AUTHORITY).

    When a state DOT publishes its road inventory (e.g. DelDOT FeatureServer),
    this data is the ORIGINAL SOURCE — HPMS is just the federal copy.
    State DOT has more columns, more detail, and is more current.

    TIER A BEHAVIOR:
      - OVERWRITES any existing value for all matched columns
      - Only applies where State DOT has a non-empty value
      - Runs BEFORE enrich_hpms() so HPMS only fills what DOT didn't cover

    If {abbr}_state_dot.parquet.gz doesn't exist, this function skips silently
    and Tier B (HPMS) becomes the highest authority automatically.
    """
    print("    State DOT Inventory (Tier A)...", end=" ", flush=True)
    if sdot is None or len(sdot) == 0:
        print("not available — Tier B (HPMS) is highest authority")
        return

    use_strtree = _road_strtree is not None

    if use_strtree and "mid_lat" in sdot.columns and "mid_lon" in sdot.columns:
        indices, dists_m = query_nearest_road_linestring(
            sdot["mid_lat"].values, sdot["mid_lon"].values)

        n_roads = len(roads)
        best_idx = np.full(n_roads, -1, dtype=int)
        best_dist = np.full(n_roads, np.inf)

        for sdot_i in range(len(sdot)):
            road_i = indices[sdot_i]
            d = dists_m[sdot_i]
            if d < best_dist[road_i]:
                best_dist[road_i] = d
                best_idx[road_i] = sdot_i

        matched = (best_idx >= 0) & (best_dist <= threshold_m)
        final_indices = np.clip(best_idx, 0, len(sdot) - 1)
        final_dists_m = best_dist
    elif "mid_lat" in sdot.columns and "mid_lon" in sdot.columns:
        tree = build_kdtree(sdot["mid_lat"].values, sdot["mid_lon"].values)
        final_dists_m, final_indices = query_nearest(tree, roads["mid_lat"].values, roads["mid_lon"].values)
        matched = final_dists_m <= threshold_m
    else:
        print("skipped — no mid_lat/mid_lon in State DOT data")
        return

    n_matched = matched.sum()
    tag = "STRtree" if use_strtree else "KDTree"
    print(f"{n_matched:,}/{len(roads):,} matched ({n_matched/len(roads)*100:.1f}%) [{tag}]")

    if n_matched == 0:
        return

    # ── Store all State DOT columns with sdot_ prefix ──
    skip = {"mid_lat", "mid_lon", "u_lat", "u_lon", "v_lat", "v_lon",
            "length_m", "length_ft", "geometry_coords", "road_source",
            "dot_state_abbr", "dot_state_fips", "dot_source", "dot_source_url"}
    for col in sdot.columns:
        if col in skip:
            continue
        vals = sdot[col].values[final_indices]
        try:
            _ = vals + 0
            is_numeric = True
        except (TypeError, ValueError):
            is_numeric = False

        out_col = col if col.startswith("sdot_") or col.startswith("dot_") else f"sdot_{col}"
        if is_numeric:
            roads[out_col] = np.where(matched, vals, 0)
        else:
            roads[out_col] = np.where(matched, vals, "")

    roads["sdot_match_dist_ft"] = np.round(final_dists_m * M_TO_FT, 1)
    roads["sdot_matched"] = np.where(matched, "Yes", "No")

    # ── Tier A: OVERWRITE frontend standard columns ──
    TIER_A_COLUMNS = [
        "Functional Class", "SYSTEM", "Ownership", "Facility Type",
        "Roadway Surface Type", "Area Type", "Roadway Description",
        "DOT District", "Physical Juris Name",
        "RTE Name", "Through_Lanes",
        "Lane_Width_ft", "Median_Width_ft", "Shoulder_Width_ft",
        "Has_Sidewalk", "Has_Bike_Lane", "Guardrail Related?",
        "RNS MP",
    ]

    filled = {}
    for col in TIER_A_COLUMNS:
        if col not in sdot.columns:
            continue

        sdot_vals = sdot[col].values[final_indices]

        if sdot_vals.dtype.kind in ("i", "f", "u"):
            has_val = matched & (sdot_vals != 0)
        else:
            sdot_str = np.array([str(v).strip() if v is not None else "" for v in sdot_vals])
            has_val = matched & (sdot_str != "") & (sdot_str != "0") & (sdot_str != "nan")
            sdot_vals = sdot_str

        if has_val.any():
            if col not in roads.columns:
                roads[col] = ""
            roads.loc[has_val, col] = sdot_vals[has_val]
            filled[col] = has_val.sum()

    if filled:
        total_fills = sum(filled.values())
        top_cols = sorted(filled.items(), key=lambda x: -x[1])[:8]
        top_str = ", ".join(f"{c}={n:,}" for c, n in top_cols)
        print(f"      Tier A overwrites: {total_fills:,} across {len(filled)} columns")
        print(f"      Top: {top_str}")


def enrich_nearest_asset(roads, df, prefix, threshold_ft, attr_cols, label=""):
    """
    Comprehensive asset enrichment: nearest GPS + attributes + count + Yes/No.
    
    Uses STRtree (point-to-linestring) when available for accurate matching.
    Falls back to KDTree (point-to-midpoint) if STRtree not built.
    
    For each road segment, finds nearest asset and captures:
      nearest_{prefix}_dist_ft    — distance to nearest
      nearest_{prefix}_lat/lon    — GPS of nearest asset
      nearest_{prefix}_{attr}     — each attribute from attr_cols
      {prefix}_count_{threshold}ft — count within threshold
      Near_{Prefix}_{threshold}ft — Yes/No
    """
    desc = label or prefix
    print(f"    {desc} ({threshold_ft}ft)...", end=" ", flush=True)

    # Column names
    yesno_col = f"Near_{prefix.title().replace('_', '')}_{threshold_ft}ft"
    dist_col = f"nearest_{prefix}_dist_ft"
    lat_col = f"nearest_{prefix}_lat"
    lon_col = f"nearest_{prefix}_lon"
    count_col = f"{prefix}_count_{threshold_ft}ft"

    if df is None or len(df) == 0:
        roads[yesno_col] = "No"
        roads[dist_col] = -1.0
        roads[lat_col] = 0.0
        roads[lon_col] = 0.0
        roads[count_col] = 0
        for _, suffix in attr_cols:
            roads[f"nearest_{prefix}_{suffix}"] = ""
        print("skipped (no data)")
        return

    al = df["lat"].values
    ao = df["lon"].values

    # ── Two-pass matching: STRtree finds nearest road per asset, ──
    # ── then we invert to find nearest asset per road              ──
    use_strtree = _road_strtree is not None

    if use_strtree:
        # For each ASSET, find nearest ROAD via STRtree (point-to-linestring)
        asset_road_idx, asset_road_dists_m = query_nearest_road_linestring(al, ao)
        threshold_m = threshold_ft * FT_TO_M

        # Invert: for each ROAD, find closest asset that matched to it
        n_roads = len(roads)
        best_asset_idx = np.full(n_roads, -1, dtype=int)
        best_asset_dist = np.full(n_roads, np.inf)

        for asset_i in range(len(df)):
            road_i = asset_road_idx[asset_i]
            d = asset_road_dists_m[asset_i]
            if d < best_asset_dist[road_i]:
                best_asset_dist[road_i] = d
                best_asset_idx[road_i] = asset_i

        dists_ft = best_asset_dist * M_TO_FT
        indices = np.clip(best_asset_idx, 0, len(df) - 1)  # clip -1 for unmatched
        matched = (best_asset_idx >= 0) & (best_asset_dist <= threshold_m)
    else:
        # KDTree fallback (midpoint-to-midpoint)
        rl = roads["mid_lat"].values
        ro = roads["mid_lon"].values
        tree = build_kdtree(al, ao)
        dists_m, indices = query_nearest(tree, rl, ro)
        dists_ft = dists_m * M_TO_FT
        threshold_m = threshold_ft * FT_TO_M
        matched = dists_m <= threshold_m

    # Yes/No
    roads[yesno_col] = np.where(matched, "Yes", "No")

    # Distance (always set — even beyond threshold)
    roads[dist_col] = np.round(dists_ft, 1)

    # Nearest asset GPS
    roads[lat_col] = np.where(matched, al[indices], 0.0)
    roads[lon_col] = np.where(matched, ao[indices], 0.0)

    # Nearest asset attributes
    for src_col, out_suffix in attr_cols:
        if src_col in df.columns:
            vals = df[src_col].values[indices]
            try:
                is_num = pd.api.types.is_numeric_dtype(vals)
            except Exception:
                is_num = False
            if is_num:
                roads[f"nearest_{prefix}_{out_suffix}"] = np.where(matched, vals, 0)
            else:
                vals_str = pd.array(vals, dtype="string").fillna("")
                roads[f"nearest_{prefix}_{out_suffix}"] = np.where(matched, vals_str, "")
        else:
            roads[f"nearest_{prefix}_{out_suffix}"] = ""

    # Count within radius
    counts = count_within_radius(
        roads["mid_lat"].values, roads["mid_lon"].values, al, ao, threshold_ft)
    roads[count_col] = counts

    n = matched.sum()
    print(f"{n:,} Yes ({n/len(roads)*100:.1f}%), count range 0-{counts.max()}")


def enrich_bridges(roads, df):
    enrich_nearest_asset(roads, df, "bridge", 500,
        attr_cols=[
            ("condition",       "condition"),
            ("year_built",      "year_built"),
            ("adt",             "adt"),
            ("width_m",         "width_m"),
            ("lanes",           "lanes"),
            ("facility_carried","facility"),
            ("feature_desc",    "feature"),
            ("status",          "status"),
            ("structure_number","structure_id"),
        ],
        label="Bridges")


def enrich_rail_crossings(roads, df):
    enrich_nearest_asset(roads, df, "rail_xing", 500,
        attr_cols=[
            ("crossing_id",     "id"),
            ("street",          "street"),
            ("railroad",        "railroad"),
            ("warning_device",  "warning_device"),
            ("warning_level",   "warning_level"),
            ("trains_per_day",  "trains_per_day"),
        ],
        label="Rail crossings")


def enrich_schools(roads, df):
    enrich_nearest_asset(roads, df, "school", 1500,
        attr_cols=[
            ("school_name",   "name"),
            ("school_level",  "level"),
            ("enrollment",    "enrollment"),
            ("school_type",   "type"),
        ],
        label="Schools")


def enrich_transit(roads, df):
    enrich_nearest_asset(roads, df, "transit", 500,
        attr_cols=[
            ("stop_name",   "stop_name"),
            ("stop_id",     "stop_id"),
            ("wheelchair",  "wheelchair"),
        ],
        label="Transit stops")


def enrich_poi_categories(roads, pois):
    if pois is None or len(pois) == 0:
        poi_cats = ["bar","hospital","clinic","parking","fuel","signal",
                    "stop_sign","crossing","college","restaurant","rest_area"]
        for cat in poi_cats:
            for suffix in ["dist_ft","lat","lon","name"]:
                roads[f"nearest_poi_{cat}_{suffix}"] = "" if suffix != "dist_ft" else -1.0
            roads[f"poi_{cat}_count_500ft"] = 0
            roads[f"Near_Poi{cat.title()}_500ft"] = "No"
        print("    POI categories... skipped")
        return

    print("    POI categories...")
    poi_configs = [
        ("bar",        1500),
        ("hospital",   2000),
        ("clinic",     1500),
        ("parking",     500),
        ("fuel",        500),
        ("signal",      100),
        ("stop_sign",   100),
        ("crossing",    100),
        ("college",    1500),
        ("restaurant",  500),
        ("rest_area",  1000),
    ]
    # Report missing categories
    present_cats = set(pois["category"].unique())
    expected_cats = {c for c, _ in poi_configs}
    missing = expected_cats - present_cats
    if missing:
        print(f"      ⚠️ Missing POI categories: {', '.join(sorted(missing))}")

    for cat, radius in poi_configs:
        subset = pois[pois["category"] == cat]
        if len(subset) == 0:
            subset = None
        enrich_nearest_asset(roads, subset, f"poi_{cat}", radius,
            attr_cols=[("name", "name"), ("subcategory", "subcategory")],
            label=f"POI {cat}")


def nearest_value(road_lats, road_lons, poi_lats, poi_lons, poi_values, radius_ft):
    """Find the nearest POI value within radius_ft for each road. Returns (values, dists_ft)."""
    n = len(road_lats)
    result_vals = np.full(n, "", dtype=object)
    result_dists = np.full(n, -1.0)
    if len(poi_lats) == 0:
        return result_vals, result_dists
    tree = build_kdtree(poi_lats, poi_lons)
    dists_m, indices = query_nearest(tree, road_lats, road_lons)
    threshold_m = radius_ft * FT_TO_M
    matched = dists_m <= threshold_m
    result_vals[matched] = np.array(poi_values)[indices[matched]]
    result_dists[matched] = np.round(dists_m[matched] * M_TO_FT, 1)
    return result_vals, result_dists


def enrich_mapillary(roads, mdf):
    """Enrich with ALL Mapillary sign/infrastructure categories — no data loss."""
    print("    Mapillary (comprehensive)...")

    # ── Define ALL columns with defaults ──
    all_mapillary_cols = {
        # Regulatory signs (Yes/No + count)
        "map_stop_sign":           ("R1-1",  100, "yesno"),   # STOP
        "map_yield_sign":          ("R1-2",  100, "yesno"),   # YIELD
        "map_no_right_turn":       ("R3-1",  100, "yesno"),   # No Right Turn
        "map_no_left_turn":        ("R3-2",  100, "yesno"),   # No Left Turn
        "map_no_u_turn":           ("R3-4",  100, "yesno"),   # No U-Turn
        "map_keep_right":          ("R4-7",  100, "yesno"),   # Keep Right
        "map_one_way":             ("R6-1",  100, "yesno"),   # One Way
        "map_no_parking":          ("R7-1",  100, "yesno"),   # No Parking
        "map_do_not_enter":        ("R5-1",  100, "yesno"),   # Do Not Enter

        # Warning signs (Yes/No)
        "map_stop_ahead":          ("W3-1",  500, "yesno"),   # Stop Ahead
        "map_signal_ahead":        ("W3-3",  500, "yesno"),   # Signal Ahead
        "map_turn_warning":        ("W1-1",  500, "yesno"),   # Turn
        "map_curve_warning":       ("W1-2",  500, "yesno"),   # Curve
        "map_winding_road":        ("W1-5",  500, "yesno"),   # Winding Road
        "map_rr_crossing_warning": ("W10-1", 500, "yesno"),   # Railroad Xing warning
        "map_ped_crossing_warning":("W11-2", 500, "yesno"),   # Ped Crossing warning

        # School
        "map_school_zone":         ("S1-1", 1500, "yesno"),   # School Zone sign
    }

    if mdf is None or len(mdf) == 0:
        for col in all_mapillary_cols:
            roads[col] = "No"
            roads[f"{col}_count"] = 0
        roads["map_speed_limit_value"] = ""
        roads["map_speed_limit_dist_ft"] = -1.0
        roads["map_speed_sign_count_500ft"] = 0
        roads["map_signal_present"] = "No"
        roads["map_signal_count_500ft"] = 0
        roads["map_signal_heads"] = ""
        roads["map_street_light_count"] = 0
        roads["map_fire_hydrant_count"] = 0
        roads["map_crosswalk_count"] = 0
        roads["map_stop_line_count"] = 0
        roads["map_guard_rail"] = "No"
        roads["map_bollard"] = "No"
        roads["map_total_features_100ft"] = 0
        print("      skipped (no data)")
        return

    rl = roads["mid_lat"].values
    ro = roads["mid_lon"].values

    # ── 1. Regulatory + Warning signs (MUTCD-coded) ──
    for col, (mutcd, radius, mode) in all_mapillary_cols.items():
        subset = mdf[mdf["mutcd"] == mutcd]
        if len(subset) > 0:
            result = proximity_yesno(rl, ro, subset["lat"].values, subset["lon"].values, radius)
            roads[col] = np.where(result, "Yes", "No")
            counts = count_within_radius(rl, ro, subset["lat"].values, subset["lon"].values, radius)
            roads[f"{col}_count"] = counts
            n = result.sum()
            print(f"      {col:35s} {n:>6,} Yes, {counts.sum():>7,} total ({mutcd})")
        else:
            roads[col] = "No"
            roads[f"{col}_count"] = 0

    # ── 2. Speed limit signs (nearest value within 500ft) ──
    speed_df = mdf[mdf["mutcd"] == "R2-1"].copy()
    if len(speed_df) > 0:
        speed_vals = speed_df["speed"].values
        vals, dists = nearest_value(rl, ro, speed_df["lat"].values, speed_df["lon"].values,
                                    speed_vals, 500)
        roads["map_speed_limit_value"] = vals
        roads["map_speed_limit_dist_ft"] = dists
        counts = count_within_radius(rl, ro, speed_df["lat"].values, speed_df["lon"].values, 500)
        roads["map_speed_sign_count_500ft"] = counts
        matched = (vals != "").sum()
        print(f"      {'map_speed_limit_value':35s} {matched:>6,} matched, "
              f"{counts.sum():>7,} signs within 500ft")

        # Per-speed breakdowns (count of each speed value within 500ft)
        for spd in sorted(speed_df["speed"].unique(), key=lambda x: int(x) if x.isdigit() else 0):
            if not spd or spd == "": continue
            spd_subset = speed_df[speed_df["speed"] == spd]
            col_name = f"map_speed_{spd}_count"
            counts = count_within_radius(rl, ro, spd_subset["lat"].values,
                                         spd_subset["lon"].values, 500)
            roads[col_name] = counts
            n = (counts > 0).sum()
            if n > 0:
                print(f"      {'  speed ' + spd + ' mph':35s} {n:>6,} segs, {counts.sum():>7,} signs")
    else:
        roads["map_speed_limit_value"] = ""
        roads["map_speed_limit_dist_ft"] = -1.0
        roads["map_speed_sign_count_500ft"] = 0

    # ── 3. Traffic signals (within 500ft + head count) ──
    sig_df = mdf[mdf["name"].str.contains("Traffic Signal", case=False, na=False)]
    if len(sig_df) > 0:
        result = proximity_yesno(rl, ro, sig_df["lat"].values, sig_df["lon"].values, 500)
        roads["map_signal_present"] = np.where(result, "Yes", "No")
        counts = count_within_radius(rl, ro, sig_df["lat"].values, sig_df["lon"].values, 500)
        roads["map_signal_count_500ft"] = counts
        # Nearest signal head count
        head_vals = sig_df["signal_heads"].fillna("").values
        vals, _ = nearest_value(rl, ro, sig_df["lat"].values, sig_df["lon"].values, head_vals, 500)
        roads["map_signal_heads"] = vals
        n = result.sum()
        print(f"      {'map_signal_present':35s} {n:>6,} Yes, heads: "
              f"{(vals != '').sum():,} captured")
    else:
        roads["map_signal_present"] = "No"
        roads["map_signal_count_500ft"] = 0
        roads["map_signal_heads"] = ""

    # ── 4. Infrastructure (counts within 100ft) ──
    infra = [
        ("map_street_light_count",   "Street Light",  100),
        ("map_fire_hydrant_count",   "Fire Hydrant",  100),
        ("map_crosswalk_count",      "Crosswalk",     100),
        ("map_stop_line_count",      "Stop Line",     100),
    ]
    for col, name_match, radius in infra:
        subset = mdf[mdf["name"].str.contains(name_match, case=False, na=False)]
        if len(subset) > 0:
            counts = count_within_radius(rl, ro, subset["lat"].values, subset["lon"].values, radius)
            roads[col] = counts
            n = (counts > 0).sum()
            print(f"      {col:35s} {n:>6,} segs, {counts.sum():>7,} features")
        else:
            roads[col] = 0

    # ── 5. Guard rail + Bollard (Yes/No within 100ft) ──
    for col, name_match in [("map_guard_rail", "Guard Rail"), ("map_bollard", "Bollard")]:
        subset = mdf[mdf["name"].str.contains(name_match, case=False, na=False)]
        if len(subset) > 0:
            result = proximity_yesno(rl, ro, subset["lat"].values, subset["lon"].values, 100)
            roads[col] = np.where(result, "Yes", "No")
            print(f"      {col:35s} {result.sum():>6,} Yes")
        else:
            roads[col] = "No"

    # ── 6. Total Mapillary features within 100ft ──
    total = count_within_radius(rl, ro, mdf["lat"].values, mdf["lon"].values, 100)
    roads["map_total_features_100ft"] = total
    print(f"      {'map_total_features_100ft':35s} {(total>0).sum():>6,} segs, "
          f"{total.sum():>7,} total")


# ═══════════════════════════════════════════════════════════════
#  GEOGRAPHY ENRICHMENT
# ═══════════════════════════════════════════════════════════════

def _load_json(path):
    """Load JSON file with records wrapper handling."""
    if not path.exists(): return None
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "records" in data:
        return data["records"]
    return data


def enrich_geography(roads, abbr, state_fips, cache_dir, s3, bucket):
    """
    Add jurisdiction columns to every road segment using _national/ geography files.
    
    Columns added:
        geo_county_fips         3-digit FIPS (e.g. "003")
        geo_county_name         Full name (e.g. "New Castle County")
        geo_county_basename     Short name (e.g. "New Castle")
        geo_juris_code          State+County FIPS (e.g. "10003")
        geo_mpo_name            MPO name or "" if outside MPO
        geo_mpo_id              MPO ID
        geo_dot_region          DOT District name from hierarchy.json
        geo_dot_region_id       DOT District key
        geo_planning_district   Planning District/TPR name from hierarchy.json
        geo_planning_district_id  TPR key
        geo_area_type           Urban/Rural (from HPMS urban_code if available)
    """
    print("    Geography (county/MPO/region)...", end=" ", flush=True)
    
    n = len(roads)
    rl = roads["mid_lat"].values
    ro = roads["mid_lon"].values

    # ── Load geography files (from cache or R2 _national/) ──
    counties_path = cache_dir / "us_counties.json"
    mpos_path = cache_dir / "us_mpos.json"
    hierarchy_path = cache_dir / "hierarchy.json"

    for local, r2_key in [
        (counties_path, "_national/us_counties.json"),
        (mpos_path, "_national/us_mpos.json"),
        (hierarchy_path, f"{STATES[abbr][1]}/_state/hierarchy.json"),
    ]:
        if not local.exists() and s3:
            try:
                s3.download_file(bucket, r2_key, str(local))
            except Exception as e:
                fname = Path(r2_key).name
                print(f"      ⚠️ {fname} not found at {r2_key}: {e}", flush=True)

    # ── County matching via KDTree ──
    counties_data = _load_json(counties_path)
    if counties_data:
        # Filter to this state
        state_counties = [c for c in counties_data if c.get("STATE") == state_fips]
        if not state_counties:
            state_counties = [c for c in counties_data if c.get("USPS", "").lower() == abbr]
        
        if state_counties:
            c_lats = np.array([float(c.get("CENTLAT") or c.get("INTPTLAT", 0)) for c in state_counties])
            c_lons = np.array([float(c.get("CENTLON") or c.get("INTPTLON", 0)) for c in state_counties])
            
            tree = build_kdtree(c_lats, c_lons)
            _, indices = query_nearest(tree, rl, ro)
            
            roads["geo_county_fips"] = [state_counties[i].get("COUNTY", "") for i in indices]
            roads["geo_county_name"] = [state_counties[i].get("NAME", state_counties[i].get("NAMELSAD", "")) for i in indices]
            roads["geo_county_basename"] = [state_counties[i].get("BASENAME", "") for i in indices]
            roads["geo_juris_code"] = [state_fips + state_counties[i].get("COUNTY", "") for i in indices]
            
            print(f"{len(state_counties)} counties matched", end=", ")
        else:
            roads["geo_county_fips"] = ""
            roads["geo_county_name"] = ""
            roads["geo_county_basename"] = ""
            roads["geo_juris_code"] = ""
            print("no counties", end=", ")
    else:
        roads["geo_county_fips"] = ""
        roads["geo_county_name"] = ""
        roads["geo_county_basename"] = ""
        roads["geo_juris_code"] = ""
        print("no county data", end=", ")

    # ── MPO matching (area-based radius) ──
    mpos_data = _load_json(mpos_path)
    if mpos_data:
        state_mpos = [m for m in mpos_data
                      if m.get("STATE", "").lower() == abbr or
                      (m.get("GEOID", "")[:2] == state_fips)]
        
        if state_mpos:
            m_lats = np.array([float(m.get("CENTLAT") or m.get("INTPTLAT", 0)) for m in state_mpos])
            m_lons = np.array([float(m.get("CENTLON") or m.get("INTPTLON", 0)) for m in state_mpos])
            m_areas = np.array([float(m.get("AREA", 0)) for m in state_mpos])
            
            tree = build_kdtree(m_lats, m_lons)
            dists_m, indices = query_nearest(tree, rl, ro)
            
            # Area-based radius: sqrt(area/pi) * 1.5 miles → meters
            mpo_names = []
            mpo_ids = []
            for i in range(n):
                idx = indices[i]
                area = m_areas[idx]
                if area > 0:
                    radius_miles = math.sqrt(area / math.pi) * 1.5
                else:
                    radius_miles = 25.0
                radius_m = radius_miles * 1609.34
                
                if dists_m[i] <= radius_m:
                    mpo_names.append(state_mpos[idx].get("MPO_NAME", state_mpos[idx].get("NAME", "")))
                    mpo_ids.append(str(state_mpos[idx].get("MPO_ID", state_mpos[idx].get("GEOID", ""))))
                else:
                    mpo_names.append("")
                    mpo_ids.append("")
            
            roads["geo_mpo_name"] = mpo_names
            roads["geo_mpo_id"] = mpo_ids
            in_mpo = sum(1 for m in mpo_names if m)
            print(f"{in_mpo:,} in MPO ({in_mpo/n*100:.0f}%)", end=", ")
        else:
            roads["geo_mpo_name"] = ""
            roads["geo_mpo_id"] = ""
            print("no MPOs", end=", ")
    else:
        roads["geo_mpo_name"] = ""
        roads["geo_mpo_id"] = ""
        print("no MPO data", end=", ")

    # ── DOT Region + Planning District (from hierarchy.json → county mapping) ──
    hier = _load_json(hierarchy_path)
    if hier and isinstance(hier, dict):
        regions = hier.get("regions", {})
        tprs = hier.get("tprs", {})
        
        # Build county→region lookup
        county_to_region = {}
        county_to_region_id = {}
        for rid, rdata in regions.items():
            if isinstance(rdata, dict):
                for cfips in rdata.get("counties", []):
                    county_to_region[cfips] = rdata.get("name", rdata.get("shortName", rid))
                    county_to_region_id[cfips] = rid
        
        # Build county→TPR lookup
        county_to_tpr = {}
        county_to_tpr_id = {}
        for tid, tdata in tprs.items():
            if isinstance(tdata, dict):
                for cfips in tdata.get("counties", []):
                    county_to_tpr[cfips] = tdata.get("name", tdata.get("shortName", tid))
                    county_to_tpr_id[cfips] = tid
        
        if "geo_county_fips" in roads.columns:
            roads["geo_dot_region"] = roads["geo_county_fips"].map(county_to_region).fillna("")
            roads["geo_dot_region_id"] = roads["geo_county_fips"].map(county_to_region_id).fillna("")
            roads["geo_planning_district"] = roads["geo_county_fips"].map(county_to_tpr).fillna("")
            roads["geo_planning_district_id"] = roads["geo_county_fips"].map(county_to_tpr_id).fillna("")
            
            has_region = (roads["geo_dot_region"] != "").sum()
            has_tpr = (roads["geo_planning_district"] != "").sum()
            print(f"{has_region:,} in regions, {has_tpr:,} in TPRs")
        else:
            roads["geo_dot_region"] = ""
            roads["geo_dot_region_id"] = ""
            roads["geo_planning_district"] = ""
            roads["geo_planning_district_id"] = ""
            print("no county FIPS for region lookup")
    else:
        roads["geo_dot_region"] = ""
        roads["geo_dot_region_id"] = ""
        roads["geo_planning_district"] = ""
        roads["geo_planning_district_id"] = ""
        print("no hierarchy")

    # geo_area_type derived in main() after HPMS enrichment
    if "geo_area_type" not in roads.columns:
        roads["geo_area_type"] = ""


# ═══════════════════════════════════════════════════════════════
#  R2 UTILITIES
# ═══════════════════════════════════════════════════════════════
def gzip_file(src, dst):
    with open(src, "rb") as fi, gzip.open(dst, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    return os.path.getsize(src)/1048576, os.path.getsize(dst)/1048576

def get_r2_client():
    ep = os.environ.get("R2_ENDPOINT","")
    ki = os.environ.get("R2_ACCESS_KEY_ID","")
    sk = os.environ.get("R2_SECRET_ACCESS_KEY","")
    if not all([ep,ki,sk]): return None
    import boto3
    return boto3.client("s3",endpoint_url=ep,aws_access_key_id=ki,
                        aws_secret_access_key=sk,region_name="auto")

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Build statewide road database")
    parser.add_argument("--state", required=True)
    parser.add_argument("--cache-dir", default="cache")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument("--hpms-threshold", type=int, default=100, help="HPMS match threshold meters")
    parser.add_argument("--county-chunk", action="store_true",
                        help="Process county-by-county for large states (auto-enabled for >2M roads)")
    args = parser.parse_args()

    abbr = args.state.lower()
    if abbr not in STATES: print(f"Unknown: {abbr}"); sys.exit(1)
    state_name, r2_prefix, state_fips = STATES[abbr]
    cache_dir = Path(args.cache_dir); cache_dir.mkdir(parents=True, exist_ok=True)

    bucket = os.environ.get("R2_BUCKET","crash-lens-data")
    s3 = None
    if not args.local_only:
        s3 = get_r2_client()
        if s3: print(f"R2 connected: {bucket}")

    print(f"\n{'='*65}")
    print(f"  Road Database Builder: {state_name} ({abbr})")
    print(f"  HPMS threshold: {args.hpms_threshold}m")
    print(f"{'='*65}\n")

    t0 = time.time()

    # ── Load files (from local or R2) ──
    file_map = {
        "roads":          f"{abbr}_roads.parquet.gz",
        "intersections":  f"{abbr}_intersections.parquet.gz",
        "state_dot":      f"{abbr}_special_data.parquet.gz",
        "hpms":           f"{abbr}_hpms.parquet.gz",
        "pois":           f"{abbr}_pois.parquet.gz",
        "bridges":        f"{abbr}_bridges.parquet.gz",
        "rail_crossings": f"{abbr}_rail_crossings.parquet.gz",
        "schools":        f"{abbr}_schools.parquet.gz",
        "transit":        f"{abbr}_transit.parquet.gz",
        "mapillary":      f"{abbr}_mapillary.parquet.gz",
    }

    print("  Loading cache files...")
    data = {}
    for name, filename in file_map.items():
        local = cache_dir / filename
        if not local.exists() and s3:
            try: s3.download_file(bucket, f"{r2_prefix}/cache/{filename}", str(local))
            except Exception: pass
        # Mapillary alternate name
        if name == "mapillary" and not local.exists():
            alt = cache_dir / "traffic-inventory.parquet.gz"
            if not alt.exists() and s3:
                try: s3.download_file(bucket, f"{r2_prefix}/cache/traffic-inventory.parquet.gz", str(alt))
                except Exception: pass
            if alt.exists(): local = alt

        df = load_parquet_gz(local)
        data[name] = df
        if df is not None:
            print(f"    ✅ {name:18s} {len(df):>8,} rows x {len(df.columns):>2} cols")
        else:
            print(f"    ⬜ {name:18s} not available")

    roads = data["roads"]
    if roads is None or len(roads) == 0:
        print("\n  ❌ Roads file required."); sys.exit(1)

    # ── Fix curvature < 1.0 (impossible — clamp to floor) ──
    if "curvature" in roads.columns:
        below1 = (pd.to_numeric(roads["curvature"], errors="coerce") < 1.0).sum()
        if below1 > 0:
            roads["curvature"] = pd.to_numeric(roads["curvature"], errors="coerce").clip(lower=1.0)
            print(f"  ⚠️  Clamped {below1:,} curvature values < 1.0 to 1.0")

    # ══════════════════════════════════════════════════════════
    #  HYBRID MERGE: Add HPMS orphan segments not in OSM
    # ══════════════════════════════════════════════════════════
    roads["road_source"] = "OSM"
    hpms_df = data["hpms"]

    if hpms_df is not None and len(hpms_df) > 0:
        print(f"\n  Hybrid merge (OSM + HPMS orphans)...")
        print(f"    OSM base: {len(roads):,} segments")

        # Find HPMS segments with no OSM match within 100m
        osm_tree = build_kdtree(roads["mid_lat"].values, roads["mid_lon"].values)
        dists_m, _ = query_nearest(osm_tree, hpms_df["mid_lat"].values, hpms_df["mid_lon"].values)
        orphan_mask = dists_m > 100  # >100m from any OSM road
        orphans = hpms_df[orphan_mask].copy()

        if len(orphans) > 0:
            # Convert HPMS orphans to OSM-compatible format
            FC_TO_HIGHWAY = {
                1: "motorway", 2: "trunk", 3: "primary", 4: "secondary",
                5: "tertiary", 6: "unclassified", 7: "residential",
            }

            # Compute length: prefer length_mi, fallback to 0
            orphan_length_mi = pd.to_numeric(orphans["length_mi"], errors="coerce").fillna(0).values
            orphan_length_m = orphan_length_mi * 1609.34
            zero_len = (orphan_length_m <= 0).sum()
            if zero_len > 0:
                print(f"    ⚠️  {zero_len:,}/{len(orphans):,} HPMS orphans have zero length_mi")

            n_orphans = len(orphans)
            orphan_rows = pd.DataFrame({
                "u_node": range(-1, -n_orphans - 1, -1),
                "v_node": range(-1, -n_orphans - 1, -1),
                "u_lat": orphans["mid_lat"].values,
                "u_lon": orphans["mid_lon"].values,
                "v_lat": orphans["mid_lat"].values,
                "v_lon": orphans["mid_lon"].values,
                "mid_lat": orphans["mid_lat"].values,
                "mid_lon": orphans["mid_lon"].values,
                "highway": [FC_TO_HIGHWAY.get(int(fc), "unclassified")
                            for fc in orphans["f_system"].values],
                "name": [str(r) if r and str(r).strip() else ""
                         for r in orphans["route_name"].values],
                "ref": "",
                "oneway": "",
                "lanes": [str(int(l)) if l and int(l) > 0 else ""
                          for l in orphans["through_lanes"].values],
                "maxspeed": [f"{int(s)} mph" if s and int(s) > 0 else ""
                             for s in orphans["speed_limit"].values],
                "length_m": orphan_length_m,
                "bridge": "", "tunnel": "", "surface": "",
                "lit": "", "sidewalk": "", "cycleway": "",
                "divider": "",
                "curvature": 1.0,
                "road_source": "HPMS",
                # Orphan HPMS = point geometry (no linestring, just 2-point degenerate)
                "geometry_coords": "",
            })

            roads = pd.concat([roads, orphan_rows], ignore_index=True).copy()  # .copy() prevents fragmentation

            print(f"    HPMS orphans added: {len(orphans):,} (not within 100m of any OSM road)")
            print(f"    Hybrid total: {len(roads):,} segments")

            # Breakdown
            by_fc = orphans["f_system"].value_counts().sort_index()
            fc_labels = {1:"Interstate",2:"Fwy/Expwy",3:"Princ Art",
                         4:"Minor Art",5:"Major Coll",6:"Minor Coll",7:"Local"}
            for fc, cnt in by_fc.items():
                print(f"      FC {fc} ({fc_labels.get(fc,'?'):10s}): {cnt:>6,} orphans added")
        else:
            print(f"    No HPMS orphans — all HPMS segments already in OSM")
    else:
        print(f"\n  No HPMS data — OSM-only base ({len(roads):,} segments)")

    print(f"\n  Base: {len(roads):,} road segments ({(roads['road_source']=='OSM').sum():,} OSM + "
          f"{(roads['road_source']=='HPMS').sum():,} HPMS-only)")

    # Auto-enable county chunking for large states
    use_county_chunk = args.county_chunk or len(roads) > 2_000_000
    if use_county_chunk:
        print(f"  ⚡ County-chunk mode: processing in batches to limit memory")
    
    print(f"  Enriching...")

    # ── Enrichment pipeline ──
    enrich_geography(roads, abbr, state_fips, cache_dir, s3, bucket)
    enrich_intersections(roads, data["intersections"])
    enrich_ramps(roads)

    # Build STRtree on road linestrings (once — used by all asset enrichment)
    strtree_ok = build_road_strtree(roads)
    if not strtree_ok:
        print("    Using KDTree midpoint fallback for all enrichment")

    # ── Tier A: State DOT Inventory (highest authority, optional) ──
    # If {abbr}_state_dot.parquet.gz exists → Tier A overwrites all columns.
    # If missing → silently skipped, Tier B (HPMS) becomes highest authority.
    enrich_state_dot(roads, data.get("state_dot"), args.hpms_threshold)

    # ── Tier B: HPMS Federal Data (highest authority when Tier A unavailable) ──
    enrich_hpms(roads, data["hpms"], args.hpms_threshold)

    # Derive area type: Census UA/UC boundaries → HPMS urban_code → AADT fallback
    _area_type_resolved = False

    # Tier 1: Census Urban Area boundaries (most accurate — polygon PIP)
    try:
        from boundary_resolver import BoundaryResolver
        br = BoundaryResolver(cache_dir=str(cache_dir / "boundaries"))
        if br.urban_areas is not None:
            area_type, ua_name, ua_geoid = br.resolve_area_type(
                roads["mid_lat"].astype(float).values,
                roads["mid_lon"].astype(float).values)
            roads["geo_area_type"] = area_type
            roads["Urban_Area_Name"] = ua_name
            roads["Urban_Area_GEOID"] = ua_geoid
            _area_type_resolved = True
        else:
            print("    Census UA boundaries not cached — trying HPMS fallback")
    except ImportError:
        print("    boundary_resolver.py not found — trying HPMS fallback")

    # Tier 2: HPMS urban_code (if Census boundaries unavailable)
    if not _area_type_resolved and "hpms_urban_code" in roads.columns:
        uc = roads["hpms_urban_code"].astype(str).str.strip()
        uc_numeric = pd.to_numeric(uc, errors="coerce").fillna(0)
        has_urban_data = (uc_numeric > 0).sum()

        if has_urban_data > 0:
            roads["geo_area_type"] = np.where(
                uc_numeric == 0, "Rural",
                np.where(uc_numeric >= 50000, "Suburban",
                np.where(uc_numeric > 0, "Urban", "Rural")))
            n_urban = ((uc_numeric > 0) & (uc_numeric < 50000)).sum()
            n_suburban = ((uc_numeric >= 50000) & (uc_numeric < 99999)).sum()
            n_rural = (uc_numeric == 0).sum()
            print(f"    Area type from HPMS urban_code: Urban={n_urban:,}, "
                  f"Suburban={n_suburban:,}, Rural={n_rural:,}")
            _area_type_resolved = True

    # Tier 3: AADT-based heuristic (last resort)
    if not _area_type_resolved:
        aadt = pd.to_numeric(roads.get("hpms_aadt", 0), errors="coerce").fillna(0)
        if "Functional Class" in roads.columns:
            fc = roads["Functional Class"].astype(str)
        elif "resolved_functional_class" in roads.columns:
            fc = roads["resolved_functional_class"].astype(str)
        else:
            fc = pd.Series([""] * len(roads), index=roads.index)

        is_major = fc.str.startswith(("1-", "2-", "3-"))
        # Urban: major road with AADT > 10K, or any road AADT > 5K
        # Suburban: major road AADT 2K-10K, or minor road AADT 1K-5K
        # Rural: everything else
        is_urban = (is_major & (aadt >= 10000)) | (aadt >= 5000)
        is_suburban = ~is_urban & ((is_major & (aadt >= 2000)) | (aadt >= 1000))

        roads["geo_area_type"] = np.where(
            is_urban, "Urban",
            np.where(is_suburban, "Suburban", "Rural"))

        n_u = is_urban.sum()
        n_s = is_suburban.sum()
        n_r = len(roads) - n_u - n_s
        print(f"    ⚠️  Area type from AADT fallback: Urban={n_u:,}, "
              f"Suburban={n_s:,}, Rural={n_r:,}")

    # Ensure Urban_Area_Name and Urban_Area_GEOID columns always exist
    if "Urban_Area_Name" not in roads.columns:
        roads["Urban_Area_Name"] = ""
    if "Urban_Area_GEOID" not in roads.columns:
        roads["Urban_Area_GEOID"] = ""

    enrich_bridges(roads, data["bridges"])
    enrich_rail_crossings(roads, data["rail_crossings"])
    enrich_schools(roads, data["schools"])
    enrich_transit(roads, data["transit"])
    
    # Free source DataFrames to reclaim memory for large states
    for key in ["bridges", "rail_crossings", "schools", "transit"]:
        data[key] = None
    gc.collect()
    
    enrich_poi_categories(roads, data["pois"])
    data["pois"] = None; gc.collect()

    # Score Mapillary freshness before enrichment
    map_data = data["mapillary"]
    if map_data is not None and len(map_data) > 0 and "first_seen" in map_data.columns:
        try:
            import importlib.util
            da_path = Path(__file__).parent / "road_data_authority.py"
            if not da_path.exists(): da_path = Path("road_data_authority.py")
            if da_path.exists():
                spec = importlib.util.spec_from_file_location("da_fresh", da_path)
                da_m = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(da_m)
                map_data = da_m.score_mapillary_freshness(map_data)
                fresh = (map_data["freshness_score"] >= 80).sum()
                print(f"    Mapillary freshness: {fresh:,}/{len(map_data):,} features "
                      f"< 2 years old ({fresh/len(map_data)*100:.0f}%)")
        except Exception:
            pass

    enrich_mapillary(roads, map_data)
    del map_data; data["mapillary"] = None; gc.collect()

    # ── Data authority resolution (resolved columns + sanity checks) ──
    try:
        import importlib.util
        da_path = Path(__file__).parent / "road_data_authority.py"
        if not da_path.exists():
            da_path = Path("road_data_authority.py")
        if da_path.exists():
            spec = importlib.util.spec_from_file_location("road_data_authority", da_path)
            da = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(da)
            da.apply_authority_layer(roads)
            da.merge_frontend_columns(roads)
            da.compute_confidence_scores(roads)
            da.compute_risk_indicators(roads)
            da.compute_curve_analysis(roads)
            # Roadway Alignment depends on curve_class (created above)
            if "curve_class" in roads.columns and "Roadway Alignment" not in roads.columns:
                cc = pd.to_numeric(roads["curve_class"], errors="coerce").fillna(1).astype(int)
                roads["Roadway Alignment"] = np.where(cc <= 1, "1. Straight - Level",
                    np.where(cc <= 3, "2. Curve - Level", "4. Grade - Curve"))
                _ra = (roads["Roadway Alignment"] != "").sum()
                print(f"      Roadway Alignment: {_ra:>7,}/{len(roads):,}")
            if hasattr(da, 'derive_intersection_and_road_names'):
                da.derive_intersection_and_road_names(roads)
            if hasattr(da, 'compute_traffic_engineering_metrics'):
                da.compute_traffic_engineering_metrics(roads)
            checks = da.run_sanity_checks(roads, abbr)
            da.print_sanity_report(checks)
        else:
            print("    Data authority: road_data_authority.py not found — skipping")
    except Exception as e:
        print(f"    Data authority: error — {e}")
        import traceback; traceback.print_exc()

    # ── Census Area Type override ──
    # road_data_authority.merge_frontend_columns() sets Area Type from MPO membership
    # (old county-based logic). Override with Census UA polygon classification which
    # is more accurate. Must run AFTER road_data_authority to win.
    if "geo_area_type" in roads.columns:
        valid_geo = roads["geo_area_type"].fillna("").astype(str).str.strip() != ""
        if valid_geo.any():
            roads.loc[valid_geo, "Area Type"] = roads.loc[valid_geo, "geo_area_type"]
            n_u = (roads["Area Type"] == "Urban").sum()
            n_s = (roads["Area Type"] == "Suburban").sum()
            n_r = (roads["Area Type"] == "Rural").sum()
            print(f"    Census Area Type override: Urban={n_u:,}, Suburban={n_s:,}, Rural={n_r:,}")

    # ── Bug fix: Clean malformed FC values from HPMS ──
    # HPMS f_system sometimes produces "3-Principal Arterial - Other (E,2)" etc.
    if "Functional Class" in roads.columns:
        fc = roads["Functional Class"].fillna("").astype(str)
        canonical_fc = {
            "1-": "1-Interstate", "2-": "2-Freeway/Expressway",
            "3-": "3-Principal Arterial", "4-": "4-Minor Arterial",
            "5-": "5-Major Collector", "6-": "6-Minor Collector", "7-": "7-Local",
        }
        fc_fixed = 0
        for prefix, standard in canonical_fc.items():
            bad = fc.str.startswith(prefix) & (fc != standard)
            if bad.any():
                roads.loc[bad, "Functional Class"] = standard
                fc_fixed += bad.sum()
        if fc_fixed > 0:
            print(f"    FC cleanup: {fc_fixed:,} malformed values standardized")

    # ── Bug fix: Resolve compound OSM highway tags ──
    # Tags like "residential; unclassified" leave FC/Ownership empty.
    # Take the first tag and re-resolve.
    if "highway" in roads.columns and "Functional Class" in roads.columns:
        fc = roads["Functional Class"].fillna("").astype(str)
        hw = roads["highway"].fillna("").astype(str)
        empty_fc = (fc == "") & (hw != "")
        if empty_fc.any():
            # Take first tag from compound values
            first_tag = hw[empty_fc].str.split(r"[;,]").str[0].str.strip()
            # Map to FC
            hw_to_fc = {
                "motorway": "1-Interstate", "motorway_link": "2-Freeway/Expressway",
                "trunk": "2-Freeway/Expressway", "trunk_link": "3-Principal Arterial",
                "primary": "3-Principal Arterial", "primary_link": "3-Principal Arterial",
                "secondary": "4-Minor Arterial", "secondary_link": "4-Minor Arterial",
                "tertiary": "5-Major Collector", "tertiary_link": "5-Major Collector",
                "unclassified": "6-Minor Collector", "residential": "7-Local",
                "residential_link": "7-Local", "living_street": "7-Local",
                "busway": "4-Minor Arterial", "service": "7-Local",
            }
            resolved_fc = first_tag.map(hw_to_fc).fillna("")
            filled = resolved_fc != ""
            if filled.any():
                # Get the actual row indices where FC was resolved
                fill_idx = resolved_fc[filled].index
                roads.loc[fill_idx, "Functional Class"] = resolved_fc[filled].values
                # Also fill Ownership based on FC
                fc_to_own = {
                    "1-Interstate": "1. State Hwy Agency", "2-Freeway/Expressway": "1. State Hwy Agency",
                    "3-Principal Arterial": "1. State Hwy Agency", "4-Minor Arterial": "1. State Hwy Agency",
                    "5-Major Collector": "2. County Hwy Agency", "6-Minor Collector": "2. County Hwy Agency",
                    "7-Local": "3. City or Town Hwy Agency",
                }
                own = roads.loc[fill_idx, "Ownership"].fillna("").astype(str)
                empty_own_mask = own == ""
                if empty_own_mask.any():
                    new_own = resolved_fc[filled].map(fc_to_own).fillna("")
                    own_to_fill = empty_own_mask & (new_own != "")
                    if own_to_fill.any():
                        roads.loc[own_to_fill[own_to_fill].index, "Ownership"] = new_own[own_to_fill].values
                print(f"    Ghost row fix: {filled.sum():,}/{empty_fc.sum():,} compound highway tags resolved")

    gc.collect()

    # ── Proximity threshold cleanup ──
    # Blank out asset data beyond meaningful safety-analysis distance.
    # A bridge 5 miles away is irrelevant to a road segment's safety profile.
    ASSET_THRESHOLDS_FT = {
        "nearest_bridge_":         500,
        "nearest_rail_xing_":      500,
        "nearest_school_":        2000,
        "nearest_transit_":        500,
        "nearest_poi_bar_":       2000,
        "nearest_poi_clinic_":    2000,
        "nearest_poi_college_":   2000,
        "nearest_poi_crossing_":   500,
        "nearest_poi_fuel_":      1000,
        "nearest_poi_hospital_":  2000,
        "nearest_poi_parking_":    500,
        "nearest_poi_rest_area_": 1000,
        "nearest_poi_restaurant_":1000,
        "nearest_poi_signal_":     500,
        "nearest_poi_stop_sign_":  500,
    }

    print(f"\n    Proximity cleanup (dist_ft → 0 beyond threshold, blank detail only):")
    total_blanked = 0
    for prefix, threshold in sorted(ASSET_THRESHOLDS_FT.items()):
        dist_col = f"{prefix}dist_ft"
        if dist_col not in roads.columns:
            continue

        dists = pd.to_numeric(roads[dist_col], errors="coerce").fillna(0)
        beyond = dists > threshold
        n_beyond = beyond.sum()

        if n_beyond == 0:
            continue

        # dist_ft → 0 (keeps column, signals "none within range")
        roads.loc[beyond, dist_col] = 0

        # Blank detail cols only (name, id, lat, lon, etc.)
        detail_cols = [c for c in roads.columns
                       if c.startswith(prefix) and c != dist_col]
        for col in detail_cols:
            dt = roads[col].dtype
            if dt in (np.float32, np.float64, np.int32, np.int64, float, int):
                roads.loc[beyond, col] = 0
            else:
                roads.loc[beyond, col] = None

        total_blanked += n_beyond
        pct = n_beyond / len(roads) * 100
        label = prefix.replace("nearest_", "").replace("poi_", "").rstrip("_")
        print(f"      {label:<20s} >{threshold:>5}ft → {n_beyond:>6,} set to 0 "
              f"({pct:.0f}% of {len(roads):,})")

    if total_blanked > 0:
        print(f"      Total: {total_blanked:,} distance entries → 0")

    # ── Drop duplicate / low-value columns ──
    drop_cols = set()

    # Duplicate geography (geo_ repeats frontend columns)
    drop_cols.update([
        "geo_area_type", "geo_county_basename", "geo_county_fips",
        "geo_county_name", "geo_dot_region", "geo_dot_region_id",
        "geo_juris_code", "geo_mpo_id", "geo_mpo_name",
        "geo_planning_district", "geo_planning_district_id",
    ])

    # HPMS columns: keep ALL hpms_ columns (v2 — 63 fields from FHWA).
    # road_data_authority creates resolved_ columns from hpms_ data, but raw
    # hpms_ columns are needed by: road_inventory_postprocess (ownership, route_name,
    # grade_class, terrain_type), crash_enricher (authority hierarchy), and the
    # 17 new fields (route_id, capacity, structure_type, etc.) have no resolved_
    # equivalent. Dropping hpms_ columns was causing silent data loss.
    # -- Previously dropped: hpms_aadt, hpms_f_system, hpms_facility_type,
    #    hpms_match_dist_ft, hpms_matched, hpms_speed_limit, hpms_surface_type,
    #    hpms_through_lanes. Now all kept.

    # Single-value subcategory columns
    for c in roads.columns:
        if c.endswith("_subcategory") and c.startswith("nearest_"):
            vals = roads[c].dropna().unique()
            non_empty = [v for v in vals if str(v).strip() not in ("", "nan", "None")]
            if len(non_empty) <= 1:
                drop_cols.add(c)

    actual_drop = sorted([c for c in drop_cols if c in roads.columns])
    if actual_drop:
        roads.drop(columns=actual_drop, inplace=True)
        print(f"\n    Dropped {len(actual_drop)} duplicate/low-value columns")
        for c in actual_drop[:5]:
            print(f"      ✂️  {c}")
        if len(actual_drop) > 5:
            print(f"      ... and {len(actual_drop) - 5} more")

    gc.collect()

    # ── Data quality validation + auto-fix ──
    try:
        from road_inventory_validator import validate_and_fix
        print(f"\n    Running data quality validator...")
        report = validate_and_fix(roads, verbose=True, state_abbr=abbr)
    except ImportError:
        print(f"\n    road_inventory_validator.py not found — skipping validation")
    except Exception as e:
        print(f"\n    Validator error: {e} — continuing without validation")

    # ── Post-processor: 13 systemic fixes (state-agnostic) ──
    try:
        from road_inventory_postprocess import postprocess as ri_postprocess
        print(f"\n    Running post-processor...")
        _pp_hier_path = cache_dir / "hierarchy.json"
        _pp_hier = None
        if _pp_hier_path.exists():
            _pp_hier = _load_json(_pp_hier_path)
        roads, pp_report = ri_postprocess(
            roads,
            state_abbr=abbr,
            hierarchy=_pp_hier,
            cache_dir=str(cache_dir / "boundaries"),
            state_fips=state_fips,
            verbose=True,
        )
    except ImportError:
        print(f"\n    road_inventory_postprocess.py not found — skipping")
    except Exception as e:
        print(f"\n    Post-processor error: {e} — continuing")
        import traceback; traceback.print_exc()

    # ── Final FC + SYSTEM cleanup (runs AFTER validator to catch any re-introduced values) ──
    if "Functional Class" in roads.columns:
        fc = roads["Functional Class"].fillna("").astype(str)
        canonical_fc = {
            "1-": "1-Interstate", "2-": "2-Freeway/Expressway",
            "3-": "3-Principal Arterial", "4-": "4-Minor Arterial",
            "5-": "5-Major Collector", "6-": "6-Minor Collector", "7-": "7-Local",
        }
        fc_to_system = {
            "1-Interstate": "DOT Interstate", "2-Freeway/Expressway": "DOT Primary",
            "3-Principal Arterial": "DOT Primary", "4-Minor Arterial": "DOT Secondary",
            "5-Major Collector": "DOT Secondary", "6-Minor Collector": "Non-DOT primary",
            "7-Local": "Non-DOT secondary",
        }
        fc_fixed = 0
        for prefix, standard in canonical_fc.items():
            bad = fc.str.startswith(prefix) & (fc != standard)
            if bad.any():
                roads.loc[bad, "Functional Class"] = standard
                fc_fixed += bad.sum()
        if fc_fixed > 0:
            print(f"    Final FC cleanup: {fc_fixed:,} malformed values standardized")

        # Fill empty SYSTEM from FC
        sys_col = roads["SYSTEM"].fillna("").astype(str) if "SYSTEM" in roads.columns else pd.Series("", index=roads.index)
        empty_sys = sys_col == ""
        if empty_sys.any():
            fc_clean = roads["Functional Class"].fillna("").astype(str)
            new_sys = fc_clean.map(fc_to_system).fillna("")
            fill_mask = empty_sys & (new_sys != "")
            if fill_mask.any():
                roads.loc[fill_mask, "SYSTEM"] = new_sys[fill_mask]
                print(f"    SYSTEM fill: {fill_mask.sum():,} derived from FC")

    gc.collect()

    # ── Column ordering ──
    # Frontend-standard columns go first (for crash_enricher / split.py compatibility)
    frontend = [
        "Functional Class", "SYSTEM", "Ownership", "Facility Type",
        "Roadway Surface Type", "Area Type", "DOT District", "Planning District",
        "MPO Name", "Physical Juris Name", "Juris Code", "RTE Name",
        "Max Speed Diff", "Through_Lanes", "AADT",
        "Traffic Control Type", "Intersection Type", "School Zone",
        "Roadway Condition", "Roadway Alignment", "Roadway Description",
        "VMT_Annual", "AADT_source", "intersection_name",
    ]
    frontend = [c for c in frontend if c in roads.columns]

    base = ["road_source", "u_node","v_node","u_lat","u_lon","v_lat","v_lon","mid_lat","mid_lon",
            "geometry_coords",
            "highway","name","ref","oneway","lanes","maxspeed","length_m",
            "bridge","tunnel","surface","lit","sidewalk","cycleway","divider","curvature"]
    ints_c = ["is_intersection","intersection_degree"]
    ramp_c = ["is_ramp","ramp_type"]
    geo_c = sorted([c for c in roads.columns if c.startswith("geo_")])
    resolved_c = sorted([c for c in roads.columns if c.startswith("resolved_")])
    conf_c = sorted([c for c in roads.columns if c.startswith("conf_") or c.startswith("xval_")])
    risk_c = sorted([c for c in roads.columns if c.startswith("risk_")])
    curve_c = sorted([c for c in roads.columns if c.startswith("curve_")])
    te_c = sorted([c for c in roads.columns if c.startswith("te_")])
    hpms_c = sorted([c for c in roads.columns if c.startswith("hpms_")])

    # Federal assets: group by prefix (nearest_bridge_*, nearest_rail_xing_*, etc.)
    bridge_c = sorted([c for c in roads.columns if "bridge" in c and c not in base and c not in frontend])
    rail_c   = sorted([c for c in roads.columns if "rail_xing" in c])
    school_c = sorted([c for c in roads.columns if "school" in c and c.startswith(("nearest_","Near_","school_")) and c not in frontend])
    transit_c= sorted([c for c in roads.columns if "transit" in c and c.startswith(("nearest_","Near_","transit_"))])

    # POI categories: group by prefix
    poi_c = sorted([c for c in roads.columns if c.startswith(("nearest_poi_","poi_","Near_Poi"))])

    # Mapillary
    map_c = [c for c in roads.columns if c.startswith("map_")]
    map_regulatory = sorted([c for c in map_c if any(c.startswith(p) for p in
                    ["map_stop_sign","map_yield","map_no_","map_keep","map_one_way",
                     "map_do_not","map_no_parking"])])
    map_warning = sorted([c for c in map_c if any(c.startswith(p) for p in
                    ["map_stop_ahead","map_signal_ahead","map_turn","map_curve",
                     "map_winding","map_rr_","map_ped_"])])
    map_school = sorted([c for c in map_c if c.startswith("map_school")])
    map_speed = sorted([c for c in map_c if c.startswith("map_speed")])
    map_signal = sorted([c for c in map_c if c.startswith("map_signal_p") or
                         c.startswith("map_signal_c") or c.startswith("map_signal_h")])
    map_infra = sorted([c for c in map_c if any(c.startswith(p) for p in
                    ["map_street","map_fire","map_crosswalk","map_stop_line",
                     "map_guard","map_bollard","map_total"])])
    map_ordered = map_regulatory + map_warning + map_school + map_speed + map_signal + map_infra
    map_remaining = [c for c in map_c if c not in map_ordered]
    map_ordered.extend(map_remaining)

    # Assemble in order: frontend first, then analysis groups, deduplicating
    all_grouped = (frontend + base + geo_c + resolved_c + conf_c + risk_c + curve_c + te_c + ints_c + ramp_c + hpms_c + bridge_c + rail_c +
                   school_c + transit_c + poi_c + map_ordered)
    ordered = []
    seen = set()
    for c in all_grouped:
        if c in roads.columns and c not in seen:
            ordered.append(c)
            seen.add(c)
    remaining = [c for c in roads.columns if c not in seen]
    ordered.extend(remaining)
    roads = roads[ordered].copy()  # Defragment after all column additions

    # ── Drop always-empty and low-value columns (Tier 4 cleanup) ──
    # PROTECTED prefixes: hpms_, sdot_, resolved_ — these must survive even if
    # empty for a particular state, to keep parquet schema consistent across states.
    PROTECTED_PREFIXES = ("hpms_", "sdot_", "resolved_", "conf_", "risk_")
    empty_cols = []
    for col in roads.columns:
        if any(col.startswith(p) for p in PROTECTED_PREFIXES):
            continue  # Never drop protected columns
        if roads[col].dtype in [np.float32, np.float64, np.int64, np.int32, 
                                 np.int16, np.int8, np.uint8, np.uint16, np.uint32]:
            if (roads[col] != 0).sum() == 0:
                empty_cols.append(col)
        elif roads[col].dtype == object or str(roads[col].dtype) == "string":
            vals = roads[col].astype(str).str.strip()
            if (~vals.isin(["", "nan", "None", "0", "False", "-1.0"])).sum() == 0:
                empty_cols.append(col)

    # Also drop redundant lat/lon/subcategory for very sparse assets (<2%)
    sparse_detail = []
    for col in roads.columns:
        if any(col.endswith(sfx) for sfx in ["_lat","_lon","_subcategory"]):
            dist_col = col.rsplit("_", 1)[0] + "_dist_ft"
            if dist_col not in roads.columns:
                continue
            if roads[col].dtype == object or str(roads[col].dtype) == "string":
                filled = (~roads[col].astype(str).str.strip().isin(["","nan","None","0"])).sum()
            else:
                filled = (roads[col] != 0).sum()
            if filled < len(roads) * 0.02:  # Less than 2% filled
                sparse_detail.append(col)

    all_drop = list(set(empty_cols + sparse_detail))
    if all_drop:
        roads.drop(columns=all_drop, inplace=True)
        print(f"\n  Dropped {len(all_drop)} empty/sparse columns "
              f"({len(empty_cols)} empty + {len(sparse_detail)} sparse detail)")

    # ── Type narrowing for smaller parquet ──
    print(f"\n  Optimizing dtypes...", end=" ", flush=True)
    mem_before = roads.memory_usage(deep=True).sum() / 1048576
    for col in roads.select_dtypes(include=["int64"]).columns:
        cmax, cmin = roads[col].max(), roads[col].min()
        if cmin >= 0 and cmax <= 255: roads[col] = roads[col].astype(np.uint8)
        elif cmin >= -128 and cmax <= 127: roads[col] = roads[col].astype(np.int8)
        elif cmin >= 0 and cmax <= 65535: roads[col] = roads[col].astype(np.uint16)
        elif cmin >= -32768 and cmax <= 32767: roads[col] = roads[col].astype(np.int16)
        elif cmin >= 0 and cmax <= 4294967295: roads[col] = roads[col].astype(np.uint32)
    for col in roads.select_dtypes(include=["float64"]).columns:
        if col not in ("mid_lat","mid_lon","u_lat","u_lon","v_lat","v_lon"):
            roads[col] = roads[col].astype(np.float32)
    mem_after = roads.memory_usage(deep=True).sum() / 1048576
    print(f"{mem_before:.0f} MB → {mem_after:.0f} MB ({(1-mem_after/mem_before)*100:.0f}% reduction)")
    gc.collect()

    # ── Save ──
    out_name = f"{abbr}_road_inventory"
    out_pq = cache_dir / f"{out_name}.parquet"
    out_gz = cache_dir / f"{out_name}.parquet.gz"

    print(f"\n  Saving {out_name}...")
    print(f"    {len(roads):,} rows x {len(roads.columns)} cols")
    roads.to_parquet(out_pq, index=False, use_dictionary=True)
    raw, gz = gzip_file(out_pq, out_gz)
    out_pq.unlink(missing_ok=True)
    print(f"    {raw:.1f} MB -> {gz:.1f} MB gz")

    if s3 and args.upload:
        r2_key = f"{r2_prefix}/cache/{out_name}.parquet.gz"
        s3.upload_file(str(out_gz), bucket, r2_key)
        print(f"    -> R2: {r2_key}")

    # ── Metadata JSON (#10) ──
    elapsed = time.time() - t0
    meta = {
        "state": abbr,
        "state_name": state_name,
        "build_date": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": "v3.1",
        "rows": len(roads),
        "cols": len(roads.columns),
        "size_mb": round(gz, 1),
        "build_time_sec": round(elapsed, 0),
        "sources": {
            "osm_roads": int((roads["road_source"] == "OSM").sum()),
            "hpms_orphans": int((roads["road_source"] == "HPMS").sum()),
            "hpms_matched_pct": round((roads.get("hpms_matched", "") == "Yes").sum() / len(roads) * 100, 1)
                if "hpms_matched" in roads.columns else 0,
            "mapillary_total_features": int(roads.get("map_total_features_100ft", pd.Series([0])).sum()),
        },
        "coverage": {},
        "speed_sources": {},
    }

    # Coverage percentages
    for col, label in [("Functional Class","functional_class"), ("Ownership","ownership"),
                        ("Max Speed Diff","speed_limit"), ("AADT","aadt"),
                        ("Through_Lanes","lanes"), ("Roadway Surface Type","surface_type"),
                        ("Roadway Condition","pavement_condition")]:
        if col in roads.columns:
            if roads[col].dtype in ["int64","float64","int32","float32","uint8","int8","uint16","int16","uint32"]:
                filled = (roads[col] > 0).sum()
            else:
                filled = (roads[col].astype(str).str.strip() != "").sum()
            meta["coverage"][label] = round(filled / len(roads) * 100, 1)

    # Speed source breakdown
    if "resolved_speed_source" in roads.columns:
        for src, cnt in roads["resolved_speed_source"].value_counts().items():
            if src: meta["speed_sources"][src] = int(cnt)

    meta_path = cache_dir / f"{out_name}_meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"    Metadata: {meta_path.name}")

    if s3 and args.upload:
        meta_r2 = f"{r2_prefix}/cache/{out_name}_meta.json"
        s3.upload_file(str(meta_path), bucket, meta_r2)
        print(f"    -> R2: {meta_r2}")

    # ── Split.py compatibility validation (#6) ──
    print(f"\n  Split.py compatibility check:")
    own = roads["Ownership"].fillna("").astype(str) if "Ownership" in roads.columns else pd.Series("", index=roads.index)
    fc_str = roads["Functional Class"].fillna("").astype(str) if "Functional Class" in roads.columns else pd.Series("", index=roads.index)
    split_checks = {
        "dot_roads": (own == "1. State Hwy Agency").sum(),
        "county_roads": (own == "2. County Hwy Agency").sum(),
        "city_roads": (own == "3. City or Town Hwy Agency").sum(),
        "primary_roads": fc_str.str.startswith(("1-", "2-")).sum(),
        "no_interstate": (~fc_str.str.startswith(("1-", "2-"))).sum(),
    }
    all_ok = True
    for name, count in split_checks.items():
        icon = "✅" if count > 0 else "❌"
        if count == 0: all_ok = False
        print(f"    {icon} {name:20s} {count:>7,} rows")
    if all_ok:
        print(f"    ✅ All split.py road type filters will produce data")
    else:
        print(f"    ⚠️  Some road type filters are empty — check Ownership/FC mapping")

    # ── Summary ──
    elapsed = time.time() - t0
    print(f"\n{'='*65}")
    print(f"  DONE: {state_name} Road Database")
    print(f"  Rows: {len(roads):,} | Cols: {len(roads.columns)} | Size: {gz:.1f} MB | Time: {elapsed:.0f}s")
    print(f"\n  Column groups:")
    print(f"    Frontend (VDOT):     {len(frontend)}")
    print(f"    Base (OSM):          {len([c for c in ordered if c in base])}")
    print(f"    Geography:           {len(geo_c)}")
    print(f"    Resolved (authority):{len(resolved_c)}")
    print(f"    Confidence/XVal:     {len(conf_c)}")
    print(f"    Risk indicators:     {len(risk_c)}")
    print(f"    Curve analysis:      {len(curve_c)}")
    print(f"    Traffic engineering: {len(te_c)}")
    print(f"    Intersection:        {len([c for c in ordered if c in ints_c])}")
    print(f"    Ramp:                {len([c for c in ordered if c in ramp_c])}")
    print(f"    HPMS:                {len(hpms_c)}")
    print(f"    Bridges:             {len(bridge_c)}")
    print(f"    Rail crossings:      {len(rail_c)}")
    print(f"    Schools:             {len(school_c)}")
    print(f"    Transit:             {len(transit_c)}")
    print(f"    POI categories:      {len(poi_c)}")
    print(f"    Mapillary:           {len(map_ordered)}")

    # Coverage — Yes/No columns
    print(f"\n  Coverage (Yes/No):")
    yn_cols = [c for c in roads.columns if c.startswith("Near_")]
    for c in sorted(yn_cols):
        n = (roads[c]=="Yes").sum()
        print(f"    {c:40s}  {n:>7,} Yes ({n/len(roads)*100:.1f}%)")
    if "hpms_matched" in roads.columns:
        n = (roads["hpms_matched"]=="Yes").sum()
        print(f"    {'HPMS matched':40s}  {n:>7,} ({n/len(roads)*100:.1f}%)")

    # Coverage — nearest asset columns (show median distance)
    print(f"\n  Nearest asset distances:")
    dist_cols = sorted([c for c in roads.columns if c.startswith("nearest_") and c.endswith("_dist_ft")])
    for c in dist_cols:
        matched = roads[c] > 0
        if matched.sum() > 0:
            med = roads.loc[matched, c].median()
            print(f"    {c:40s}  median {med:>7.0f} ft ({matched.sum():,} matched)")

    # Mapillary summary
    print(f"\n  Mapillary sign/infrastructure counts:")
    map_yn = [c for c in map_ordered if c in roads.columns and roads[c].dtype == object
              and not c.endswith("_count") and not c.endswith("_dist_ft")
              and not c.endswith("_value") and not c.endswith("_heads")]
    for c in map_yn:
        n = (roads[c]=="Yes").sum()
        if n > 0:
            print(f"    {c:40s}  {n:>7,} Yes")

    map_cnt = [c for c in map_ordered if c.endswith("_count") or c.endswith("_100ft")]
    for c in map_cnt:
        if c in roads.columns and pd.api.types.is_numeric_dtype(roads[c]):
            t = int(roads[c].sum())
            if t > 0:
                n = (roads[c]>0).sum()
                print(f"    {c:40s}  {n:>7,} segs, {t:>8,} features")

    # ── Source coverage matrix ──
    print(f"\n  Source contribution matrix:")
    print(f"    {'Attribute':25s} {'HPMS':>8s} {'Mapillary':>10s} {'OSM':>8s} {'Federal':>8s} {'Total':>8s}")
    print(f"    {'-'*25} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")
    matrix = {
        "Speed limit":     ("resolved_speed_source",   ["HPMS","Mapillary","OSM",""]),
        "Lanes":           ("resolved_lanes_source",    ["HPMS","","OSM",""]),
        "Surface":         ("resolved_surface_source",  ["HPMS","","OSM",""]),
        "Signal":          ("resolved_signal_source",   ["HPMS","Mapillary","","POI"]),
        "Lighting":        ("resolved_lighting_source", ["","Mapillary","OSM",""]),
        "On bridge":       ("resolved_bridge_source",   ["","","OSM","Federal"]),
        "School zone":     ("resolved_school_source",   ["","Mapillary","","Federal"]),
        "Func Class":      ("resolved_fc_source",       ["HPMS","","OSM",""]),
        "Ownership":       ("resolved_ownership_source",["HPMS","","OSM",""]),
        "Facility Type":   ("resolved_facility_source", ["HPMS","","OSM",""]),
    }
    for label, (col, tier_vals) in matrix.items():
        if col in roads.columns:
            counts = roads[col].value_counts()
            h = counts.get(tier_vals[0], 0) if tier_vals[0] else 0
            m = counts.get(tier_vals[1], 0) if tier_vals[1] else 0
            o = counts.get(tier_vals[2], 0) if tier_vals[2] else 0
            f = counts.get(tier_vals[3], 0) if tier_vals[3] else 0
            t = h + m + o + f
            print(f"    {label:25s} {h:>8,} {m:>10,} {o:>8,} {f:>8,} {t:>8,}")

    print(f"\n{'='*65}")

if __name__ == "__main__":
    main()
