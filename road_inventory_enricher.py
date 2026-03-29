"""
road_inventory_enricher.py — CrashLens Road Inventory → Crash Spatial Join
===========================================================================
Matches each crash to its nearest road segment and transfers ALL road
inventory columns to the crash record. The enriched output becomes the
single file the frontend reads — every crash row carries its road segment's
full inventory (HPMS, Mapillary, OSM, TE metrics, curve analysis, proximity).

MATCHING: GPS → nearest segment (k=5 perpendicular refinement, 100m default)
TRANSFER: 300+ columns from road inventory → crash record
OUTPUT:   Enriched crash CSV with ~400 columns (crash data + road inventory)

INTEGRATION:
    from road_inventory_enricher import enrich_from_road_inventory

    ri_path = Path(cache_dir) / f"{abbr}_road_inventory.parquet.gz"
    if ri_path.exists():
        df = enrich_from_road_inventory(df, state_abbr, cache_dir)
    else:
        # legacy fallback
"""

import gzip
import io
import math
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  FC NAME CROSSWALK: Road Inventory short → VDOT Frontend long
# ═══════════════════════════════════════════════════════════════

RI_FC_TO_VDOT_FC = {
    "1-Interstate":          "1-Interstate (A,1)",
    "2-Freeway/Expressway":  "2-Principal Arterial - Other Freeways and Expressways (B)",
    "3-Principal Arterial":  "3-Principal Arterial - Other (E,2)",
    "4-Minor Arterial":      "4-Minor Arterial (H,3)",
    "5-Major Collector":     "5-Major Collector (I,4)",
    "6-Minor Collector":     "6-Minor Collector (5)",
    "7-Local":               "7-Local (J,6)",
}

# Columns to EXCLUDE from transfer (internal spatial/matching keys)
EXCLUDE_COLUMNS = {
    # Road geometry endpoints (crash has its own GPS)
    "mid_lat", "mid_lon", "u_lat", "u_lon", "v_lat", "v_lon",
    "u_node", "v_node",
    # Internal build metadata
    "road_source", "hpms_matched", "hpms_match_dist_ft",
    # Raw OSM tags already resolved into frontend columns
    "highway", "oneway", "lanes", "maxspeed", "surface",
    "tunnel", "lit", "sidewalk", "cycleway", "divider",
    "curvature", "ref",
}

# Columns where road inventory OVERWRITES crash data (even if crash has a value)
OVERWRITE_COLUMNS = {
    "Functional Class", "SYSTEM", "Ownership", "Facility Type",
    "Roadway Surface Type", "Area Type",
    "DOT District", "Planning District", "MPO Name",
    "Physical Juris Name", "Juris Code",
}

# Columns where crash data is preserved if already filled (road inventory fills gaps)
FILL_COLUMNS = {
    "Traffic Control Type", "Intersection Type",
    "RTE Name", "Node", "Node Offset (ft)", "RNS MP",
    "Max Speed Diff", "Through_Lanes", "School Zone",
    "Roadway Alignment", "Roadway Description",
}

# Columns that need FC long-name translation
FC_TRANSLATE_COLUMNS = {"Functional Class"}

# Columns that need special type handling
NUMERIC_FRONTEND_COLUMNS = {"Max Speed Diff", "Through_Lanes", "AADT", "VMT_Annual"}


# ═══════════════════════════════════════════════════════════════
#  SPATIAL MATCHING
# ═══════════════════════════════════════════════════════════════

def _point_to_segment_dist_ft(plat, plon, ulat, ulon, vlat, vlon):
    """Perpendicular distance (ft) from point to line segment u→v."""
    cos_lat = math.cos(math.radians(plat))
    ft = 364567.0

    px = (plon - ulon) * ft * cos_lat
    py = (plat - ulat) * ft
    vx = (vlon - ulon) * ft * cos_lat
    vy = (vlat - ulat) * ft

    seg_len_sq = vx * vx + vy * vy
    if seg_len_sq < 1e-10:
        return math.sqrt(px * px + py * py)

    t = max(0.0, min(1.0, (px * vx + py * vy) / seg_len_sq))
    return math.sqrt((px - t * vx) ** 2 + (py - t * vy) ** 2)


def _is_empty(val):
    """Check if a value is empty/null."""
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in ("", "nan", "none", "0", "0.0")


# ═══════════════════════════════════════════════════════════════
#  MAIN ENRICHMENT FUNCTION
# ═══════════════════════════════════════════════════════════════

def enrich_from_road_inventory(df, state_abbr, cache_dir="cache",
                                match_threshold_ft=328,
                                k_candidates=5,
                                x_col="x", y_col="y"):
    """
    Enrich crash records with ALL road inventory columns.

    Each crash gets matched to its nearest road segment via GPS, and the
    full road inventory row transfers to the crash record. The enriched
    output is the single file the frontend reads.

    Args:
        df:                  Crash DataFrame (must have x/y GPS columns)
        state_abbr:          Two-letter state abbreviation
        cache_dir:           Directory containing road inventory
        match_threshold_ft:  Max match distance in feet (default 328 = 100m)
        k_candidates:        KDTree candidates for refinement (default 5)
        x_col, y_col:        GPS column names in crash data

    Returns:
        Enriched DataFrame with crash columns + road inventory columns.
    """
    t0 = time.time()
    abbr = state_abbr.lower()
    cache = Path(cache_dir)

    # ── 1. Load road inventory ──
    ri_path = cache / f"{abbr}_road_inventory.parquet.gz"
    if not ri_path.exists():
        ri_path = cache / f"{abbr}_road_inventory.parquet"
    if not ri_path.exists():
        print(f"\n  [Road Inventory] ⚠️  Not found: {ri_path}")
        print(f"    Build with: python build_road_inventory.py --state {abbr}")
        return df

    try:
        from scipy.spatial import cKDTree
    except ImportError:
        print("\n  [Road Inventory] ⚠️  scipy not installed")
        return df

    print(f"\n  [Road Inventory] Loading {ri_path.name}...")
    if str(ri_path).endswith(".gz"):
        with gzip.open(ri_path, "rb") as f:
            ri = pd.read_parquet(io.BytesIO(f.read()))
    else:
        ri = pd.read_parquet(ri_path)

    print(f"    {len(ri):,} road segments × {len(ri.columns)} cols")

    # Determine which columns to transfer
    transfer_cols = [c for c in ri.columns if c not in EXCLUDE_COLUMNS]
    print(f"    Transferring {len(transfer_cols)} columns to each matched crash")

    # ── 2. Validate crash GPS ──
    try:
        crash_lons = pd.to_numeric(df[x_col], errors="coerce").values
        crash_lats = pd.to_numeric(df[y_col], errors="coerce").values
    except KeyError:
        print(f"    ⚠️  GPS columns '{x_col}'/'{y_col}' not found")
        return df

    valid_gps = (
        np.isfinite(crash_lats) & np.isfinite(crash_lons) &
        (crash_lats != 0) & (crash_lons != 0) &
        (crash_lats > 20) & (crash_lats < 72) &
        (crash_lons < -60) & (crash_lons > -180)
    )
    n_valid = valid_gps.sum()
    if n_valid == 0:
        print("    ⚠️  No valid GPS coordinates")
        return df

    print(f"    {n_valid:,}/{len(df):,} crashes with valid GPS")

    # ── 3. Build spatial index ──
    ri_lats = ri["mid_lat"].values
    ri_lons = ri["mid_lon"].values
    mid_lat_avg = np.mean(crash_lats[valid_gps])
    cos_lat = np.cos(np.radians(mid_lat_avg))
    M = 111320.0

    ri_xy = np.column_stack([ri_lats * M, ri_lons * M * cos_lat])
    tree = cKDTree(ri_xy)

    crash_xy = np.column_stack([crash_lats * M, crash_lons * M * cos_lat])

    # ── 4. Match crashes to nearest segments ──
    print(f"    Matching (k={k_candidates}, perpendicular refinement)...")
    valid_indices = np.where(valid_gps)[0]
    valid_xy = crash_xy[valid_indices]

    _, cand_idxs = tree.query(valid_xy, k=k_candidates)

    # Perpendicular refinement
    u_lats = ri["u_lat"].values
    u_lons = ri["u_lon"].values
    v_lats = ri["v_lat"].values
    v_lons = ri["v_lon"].values

    best_seg = np.zeros(len(valid_indices), dtype=np.int64)
    best_dist = np.full(len(valid_indices), np.inf)

    for ki in range(k_candidates):
        seg_idxs = cand_idxs[:, ki]
        for i in range(len(valid_indices)):
            j = seg_idxs[i]
            ci = valid_indices[i]
            d = _point_to_segment_dist_ft(
                crash_lats[ci], crash_lons[ci],
                u_lats[j], u_lons[j], v_lats[j], v_lons[j])
            if d < best_dist[i]:
                best_dist[i] = d
                best_seg[i] = j

    within = best_dist <= match_threshold_ft
    matched_count = within.sum()
    median_dist = np.median(best_dist[within]) if matched_count > 0 else 0

    print(f"    Matched: {matched_count:,}/{n_valid:,} within {match_threshold_ft}ft "
          f"(median={median_dist:.0f}ft)")

    # ── 5. Intersection preference ──
    if "is_intersection" in ri.columns:
        is_int_ri = ri["is_intersection"].values
        int_col = "Intersection Type" if "Intersection Type" in df.columns else None
        upgrades = 0

        if int_col:
            for ci_idx in range(len(valid_indices)):
                if not within[ci_idx]:
                    continue
                crash_i = valid_indices[ci_idx]
                crash_val = str(df.iat[crash_i, df.columns.get_loc(int_col)]).strip()
                at_int = crash_val and "Not at Intersection" not in crash_val

                if at_int and is_int_ri[best_seg[ci_idx]] != "Yes":
                    for ki in range(k_candidates):
                        alt = cand_idxs[ci_idx, ki]
                        if is_int_ri[alt] == "Yes":
                            alt_d = _point_to_segment_dist_ft(
                                crash_lats[crash_i], crash_lons[crash_i],
                                u_lats[alt], u_lons[alt], v_lats[alt], v_lons[alt])
                            if alt_d <= best_dist[ci_idx] * 2 and alt_d <= match_threshold_ft:
                                best_seg[ci_idx] = alt
                                best_dist[ci_idx] = alt_d
                                upgrades += 1
                                break

            if upgrades:
                print(f"    Intersection preference: {upgrades:,} re-matched")

    # ── 6. Transfer ALL columns ──
    print(f"    Transferring {len(transfer_cols)} road inventory columns...")

    # Build arrays of matched crash indices and their road segment indices
    matched_ci = []  # crash DataFrame integer positions
    matched_ri = []  # road inventory integer positions
    match_dists = []

    for ci_idx in range(len(valid_indices)):
        if within[ci_idx]:
            matched_ci.append(valid_indices[ci_idx])
            matched_ri.append(best_seg[ci_idx])
            match_dists.append(best_dist[ci_idx])

    matched_ci = np.array(matched_ci)
    matched_ri = np.array(matched_ri)
    match_dists = np.array(match_dists)

    if len(matched_ci) == 0:
        print("    No matches — skipping transfer")
        return df

    # Extract matched road inventory rows (one bulk operation)
    ri_matched = ri.iloc[matched_ri].reset_index(drop=True)

    overwrite_n = 0
    fill_n = 0
    new_n = 0

    for col in transfer_cols:
        if col not in ri.columns:
            continue

        # Get road inventory values for matched crashes
        ri_vals = ri_matched[col].values

        # Convert to string for safe assignment into crash DataFrame
        str_vals = np.array([
            str(v) if v is not None and str(v).strip() not in ("nan", "None")
            else ""
            for v in ri_vals
        ])

        # FC long-name translation
        if col in FC_TRANSLATE_COLUMNS:
            str_vals = np.array([RI_FC_TO_VDOT_FC.get(v, v) for v in str_vals])

        # Decide: overwrite, fill, or new
        if col in OVERWRITE_COLUMNS:
            # Always write
            if col not in df.columns:
                df[col] = ""
            df.iloc[matched_ci, df.columns.get_loc(col)] = str_vals
            overwrite_n += 1

        elif col in FILL_COLUMNS:
            # Only write if crash cell is empty
            if col not in df.columns:
                df[col] = ""
            loc = df.columns.get_loc(col)
            for i in range(len(matched_ci)):
                crash_val = df.iat[matched_ci[i], loc]
                if _is_empty(crash_val) and str_vals[i]:
                    df.iat[matched_ci[i], loc] = str_vals[i]
            fill_n += 1

        else:
            # New column — add it
            if col not in df.columns:
                df[col] = ""
            df.iloc[matched_ci, df.columns.get_loc(col)] = str_vals
            new_n += 1

    # ── 7. Derived columns ──

    # Intersection Analysis
    if "Intersection Type" in df.columns and "Ownership" in df.columns:
        if "Intersection Analysis" not in df.columns:
            df["Intersection Analysis"] = ""

        matched_mask = np.zeros(len(df), dtype=bool)
        matched_mask[matched_ci] = True

        int_type = df["Intersection Type"].astype(str)
        ownership = df["Ownership"].astype(str)
        not_int = int_type.str.contains("Not at Intersection", na=False)
        state_own = ownership.str.contains("State", na=False)

        ia = df["Intersection Analysis"].copy()
        ia[matched_mask & not_int] = "Not Intersection"
        ia[matched_mask & ~not_int & state_own] = "DOT Intersection"
        ia[matched_mask & ~not_int & ~state_own] = "Urban Intersection"
        df["Intersection Analysis"] = ia

    # Mainline?
    if "Functional Class" in df.columns:
        fc = df["Functional Class"].astype(str)
        mainline_fcs = {
            "1-Interstate (A,1)",
            "2-Principal Arterial - Other Freeways and Expressways (B)",
            "3-Principal Arterial - Other (E,2)",
        }
        if "Mainline?" not in df.columns:
            df["Mainline?"] = ""
        matched_mask = np.zeros(len(df), dtype=bool)
        matched_mask[matched_ci] = True
        df.loc[matched_mask, "Mainline?"] = np.where(
            fc[matched_mask].isin(mainline_fcs), "Yes", "No")

    # Roadway Description from Facility Type
    if "Facility Type" in df.columns:
        if "Roadway Description" not in df.columns:
            df["Roadway Description"] = ""
        desc_map = {
            "1-One-Way Undivided":  "4. One-Way, Not Divided",
            "2-One-Way Divided":    "4. One-Way, Not Divided",
            "3-Two-Way Undivided":  "1. Two-Way, Not Divided",
            "4-Two-Way Divided":    "2. Two-Way, Divided, Unprotected Median",
        }
        matched_mask = np.zeros(len(df), dtype=bool)
        matched_mask[matched_ci] = True
        ft = df.loc[matched_mask, "Facility Type"].astype(str)
        df.loc[matched_mask, "Roadway Description"] = ft.map(desc_map).fillna("")

    # ── 8. Match metadata ──
    df["ri_matched"] = "No"
    df.iloc[matched_ci, df.columns.get_loc("ri_matched")] = "Yes"

    df["ri_match_dist_ft"] = ""
    dist_strs = match_dists.round(0).astype(int).astype(str)
    for i in range(len(matched_ci)):
        df.iat[matched_ci[i], df.columns.get_loc("ri_match_dist_ft")] = dist_strs[i]

    df["ri_segment_id"] = ""
    seg_strs = matched_ri.astype(str)
    for i in range(len(matched_ci)):
        df.iat[matched_ci[i], df.columns.get_loc("ri_segment_id")] = seg_strs[i]

    # ── 9. Summary ──
    elapsed = time.time() - t0

    print(f"\n    ── Road Inventory Enrichment Complete ──")
    print(f"    Matched:     {matched_count:,}/{len(df):,} crashes ({matched_count/len(df)*100:.0f}%)")
    print(f"    Columns:     {overwrite_n} overwrite + {fill_n} fill + {new_n} new "
          f"= {overwrite_n + fill_n + new_n} transferred")
    print(f"    Output:      {len(df):,} rows × {len(df.columns)} cols")
    print(f"    Time:        {elapsed:.1f}s")

    # Key column fill report
    key_cols = [
        "Functional Class", "Ownership", "SYSTEM", "Facility Type",
        "Roadway Surface Type", "Traffic Control Type", "Intersection Type",
        "Max Speed Diff", "AADT", "RTE Name", "intersection_name",
        "Intersection Analysis", "Mainline?", "Roadway Condition",
        "Area Type", "curve_class", "te_ped_risk_score",
    ]
    print(f"\n    Key columns:")
    for col in key_cols:
        if col in df.columns:
            vals = df[col].astype(str).str.strip()
            filled = (~vals.isin(["", "nan", "None", "0"])).sum()
            pct = filled / len(df) * 100
            print(f"      {col:30s} {filled:>7,}/{len(df):,} ({pct:.0f}%)")

    return df
