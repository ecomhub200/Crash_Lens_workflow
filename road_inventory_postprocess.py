#!/usr/bin/env python3
"""
road_inventory_postprocess.py — CrashLens Road Inventory Post-Processor  v1.0
================================================================================
State-agnostic fixes applied AFTER build_road_inventory.py + road_data_authority
+ road_inventory_validator.py. Fixes 13 systemic issues discovered in audit v12.

Call from build_road_inventory.py AFTER validate_and_fix():
    from road_inventory_postprocess import postprocess
    report = postprocess(roads, state_abbr="de", hierarchy=hierarchy_dict)

Or standalone:
    python road_inventory_postprocess.py --state de --hierarchy hierarchy.json

FIX GROUPS (13):
  FIX_RTE    RTE Name: rank multi-source, purge street names
  FIX_OWN    Ownership: remap "Other State Agency" to VDOT standard
  FIX_INT    Intersection Type: fix degree→approach mapping
  FIX_ALN    Roadway Alignment: grade requires grade data + ramp→On/Off Ramp
  FIX_SRF    Surface Type: no-data rows → default asphalt
  FIX_SCH    School Zone: standardize value format
  FIX_JUR    Physical Juris Name: resolve to city/town via boundary_resolver
  FIX_THL    Through_Lanes: 0-sentinel → empty, FC minimum for Interstate
  FIX_ADT    AADT: propagate from same-road segments, FC average fallback
  FIX_SEN    Sentinels: -1 → NaN for map_ distance columns
  FIX_DUP    Duplicates: dedup same-coord + same-name + same-source
  FIX_GEO    Geography: PD/MPO cross-validation against hierarchy
  FIX_SPD    Max Speed Diff: merge resolved_speed_limit + FC default

Depends on:
  - road_inventory_validator.py   (runs first — we fix what it doesn't)
  - hierarchy.json                (for MPO/PD cross-validation)
  - boundary_resolver.py          (optional — for city/town resolution)
"""

import re
import json
import numpy as np
import pandas as pd
from collections import OrderedDict
from pathlib import Path


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

# --- FIX_RTE: Route pattern for any US state ---
# Matches: I 95, US 13, SR 602, DE 1, CR 36A, MD 54, VA 7, etc.
# state_abbr is inserted at runtime
_ROUTE_PREFIXES_FIXED = [
    r"^I[-\s]?\d",           # Interstate: I-95, I 95
    r"^US[-\s]?\d",          # US route: US 13, US-1
    r"^SR[-\s]?\d",          # State Route
    r"^CR[-\s]?\d",          # County Route
    r"^RD[-\s]?\d",          # Road (some states)
]
# State-specific prefix added dynamically: "^{ABBR}[-\s]?\d"


# --- FIX_OWN: FC-based ownership fallback for "Other State Agency" ---
FC_TO_OWNERSHIP_FALLBACK = {
    "1-": "1. State Hwy Agency",
    "2-": "1. State Hwy Agency",
    "3-": "1. State Hwy Agency",
    "4-": "1. State Hwy Agency",
    "5-": "2. County Hwy Agency",
    "6-": "2. County Hwy Agency",
    "7-": "3. City or Town Hwy Agency",
}

VALID_OWNERSHIP_VALUES = {
    "1. State Hwy Agency",
    "2. County Hwy Agency",
    "3. City or Town Hwy Agency",
    "4. Federal Roads",
    "5. Toll Roads Maintained by Others",
    "6. Private/Unknown Roads",
}


# --- FIX_INT: OSM degree → CrashLens Intersection Type ---
# OSM degree = directed edges. 2 edges per road approach.
# degree 0  = HPMS mid-segment (not at intersection)
# degree 2  = dead end
# degree 3  = one-way road meets two-way (effectively 2 approaches)
# degree 4  = through road (2 approaches) — NOT 4-way unless confirmed
# degree 5  = one-way road at 3-approach node
# degree 6  = 3 approaches (T-intersection)
# degree 7  = 3-4 approaches (mixed one-way)
# degree 8  = 4 approaches (four-way)
# degree 9-10 = 4-5 approaches
# degree 12+ = 5+ approaches
DEGREE_TO_INTERSECTION = {
    0:  "1. Not at Intersection",
    1:  "1. Not at Intersection",
    2:  "1. Not at Intersection",
    3:  "2. Two Approaches",
    4:  "2. Two Approaches",
    5:  "3. T-Intersection",
    6:  "3. T-Intersection",
    7:  "3. T-Intersection",
    8:  "4. Four Approaches",
    9:  "4. Four Approaches",
    10: "4. Four Approaches",
    # 11+ → Five-Point
}
DEGREE_FIVE_PLUS_THRESHOLD = 11


# --- FIX_SCH: School Zone value map ---
SCHOOL_ZONE_REMAP = {
    "no":  "3. No",
    "yes": "1. Yes",
    "0":   "3. No",
    "1":   "1. Yes",
}


# --- FIX_SPD: FC default speed limits (mph) ---
FC_DEFAULT_SPEED = {
    "1-": 65, "2-": 55, "3-": 50, "4-": 45,
    "5-": 50, "6-": 50, "7-": 25,
}


# --- FIX_THL: FC minimum lanes (known minimums) ---
FC_MIN_LANES = {
    "1-": 4,   # Interstate: min 2 lanes each direction = 4 total
    "2-": 2,   # Freeway: min 2
}


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

class PostprocessReport:
    """Collects fix counts for all 13 fix groups."""

    def __init__(self):
        self.fixes = OrderedDict()
        self.total = 0

    def fix(self, group, desc, count):
        if count <= 0:
            return
        key = f"{group}_{len([k for k in self.fixes if k.startswith(group)])}"
        self.fixes[key] = {"group": group, "desc": desc, "count": count}
        self.total += count

    def print_report(self):
        if not self.fixes:
            print("\n    ── Post-Processor: No fixes needed ──")
            return
        print(f"\n    ── Post-Processor Report ({self.total:,} total fixes) ──")
        groups = OrderedDict()
        for k, v in self.fixes.items():
            g = v["group"]
            if g not in groups:
                groups[g] = []
            groups[g].append(v)
        for group, items in groups.items():
            subtotal = sum(i["count"] for i in items)
            print(f"    [{group}] {subtotal:>7,} fixes:")
            for item in items:
                print(f"           {item['desc']:<50s} {item['count']:>7,}")


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _s(ri, col):
    """Safely get string column."""
    if col not in ri.columns:
        return pd.Series("", index=ri.index)
    return ri[col].fillna("").astype(str).str.strip()


def _n(ri, col):
    """Safely get numeric column."""
    if col not in ri.columns:
        return pd.Series(0, index=ri.index)
    return pd.to_numeric(ri[col], errors="coerce").fillna(0)


def _build_route_pattern(state_abbr):
    """Build compiled regex that matches route designations for any US state."""
    patterns = list(_ROUTE_PREFIXES_FIXED)
    if state_abbr:
        patterns.append(rf"^{re.escape(state_abbr.upper())}[-\s]?\d")
    combined = "|".join(f"({p})" for p in patterns)
    return re.compile(combined, re.IGNORECASE)


# ═══════════════════════════════════════════════════════════════
#  FIX FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def fix_rte_name(ri, report, state_abbr=""):
    """
    FIX 1 — RTE Name: Rank multi-source, purge street names.

    Authority hierarchy:
      1. hpms_route_name  (federally validated, always route-format)
      2. OSM ref          (route designations, first ref if compound)
      3. DISCARD          (OSM name = street names, NOT route designations)

    State-agnostic: Works for any state because HPMS and OSM ref always
    contain route designations (I/US/SR/{STATE}/CR patterns).
    """
    has_hpms_rte = "hpms_route_name" in ri.columns
    has_ref = "ref" in ri.columns
    has_name = "name" in ri.columns
    route_pat = _build_route_pattern(state_abbr)

    # Start fresh — build from authoritative sources only
    old_rte = _s(ri, "RTE Name")
    had_old = old_rte != ""
    new_rte = pd.Series("", index=ri.index, dtype=str)

    # Layer 1: HPMS route_name (highest authority)
    if has_hpms_rte:
        hpms_rte = _s(ri, "hpms_route_name")
        has_val = hpms_rte != ""
        new_rte = new_rte.where(~has_val, hpms_rte)
        report.fix("FIX_RTE", "From hpms_route_name", has_val.sum())

    # Layer 2: OSM ref (take first from compound "US 9;DE 404")
    if has_ref:
        ref = _s(ri, "ref").str.split(r"[;,]").str[0].str.strip()
        ref_valid = (ref != "") & (new_rte == "")
        # Only accept ref values that look like route designations
        is_route_ref = ref.apply(lambda x: bool(route_pat.match(x)) if x else False)
        fill_mask = ref_valid & is_route_ref
        new_rte = new_rte.where(~fill_mask, ref[fill_mask])
        report.fix("FIX_RTE", "From OSM ref (route-format)", fill_mask.sum())

        # Non-standard refs that are still clearly numeric routes (e.g., "346")
        numeric_ref = ref_valid & ~is_route_ref & ref.str.match(r"^\d+[A-Za-z]?$", na=False)
        if numeric_ref.any():
            cr_prefixed = "CR " + ref[numeric_ref]
            new_rte = new_rte.where(~numeric_ref, cr_prefixed)
            report.fix("FIX_RTE", "From OSM ref (numeric→CR)", numeric_ref.sum())

    # Purge count: how many old RTE Names were street names (now cleared)
    purged = had_old & (new_rte == "")
    report.fix("FIX_RTE", "Street names purged from RTE Name", purged.sum())

    ri["RTE Name"] = new_rte


def fix_ownership(ri, report):
    """
    FIX 2 — Ownership: Remap non-standard values to VDOT 6-value standard.

    HPMS ownership codes 21/25/40 → "4. Other State Agency" is NOT in standard.
    Remap based on FC:
      FC 1-4 (state system roads)  → "1. State Hwy Agency"
      FC 5-6 (collector roads)     → "2. County Hwy Agency"
      FC 7   (local roads)         → "3. City or Town Hwy Agency"

    State-agnostic: HPMS ownership codes are federal standard, FC-based
    ownership derivation works the same way in every state.
    """
    own = _s(ri, "Ownership")
    fc = _s(ri, "Functional Class")
    invalid = ~own.isin(VALID_OWNERSHIP_VALUES) & (own != "")

    if not invalid.any():
        return

    for prefix, standard_own in FC_TO_OWNERSHIP_FALLBACK.items():
        fix_mask = invalid & fc.str.startswith(prefix)
        if fix_mask.any():
            ri.loc[fix_mask, "Ownership"] = standard_own

    # Anything still invalid after FC fallback → Private/Unknown
    own_after = _s(ri, "Ownership")
    still_bad = ~own_after.isin(VALID_OWNERSHIP_VALUES) & (own_after != "")
    if still_bad.any():
        ri.loc[still_bad, "Ownership"] = "6. Private/Unknown Roads"

    total = invalid.sum()
    report.fix("FIX_OWN", "Non-standard Ownership remapped via FC", total)


def fix_intersection_type(ri, report):
    """
    FIX 4 — Intersection Type: Correct degree→approach mapping.

    OSM intersection_degree counts DIRECTED edges. A 3-road T-intersection
    has degree 6 (each road = 2 directed edges). Current code treated degree
    literally: degree 6 → "Five-Point or More" which is wrong.

    Correct mapping: degree / 2 ≈ number of approaches.

    State-agnostic: OSM node degree semantics are universal.
    """
    if "intersection_degree" not in ri.columns:
        return
    if "is_intersection" not in ri.columns:
        return

    degree = _n(ri, "intersection_degree").astype(int)
    is_int = _s(ri, "is_intersection") == "Yes"

    old_int = _s(ri, "Intersection Type")

    # Build new mapping
    new_int = pd.Series("1. Not at Intersection", index=ri.index, dtype=str)

    for deg_val, int_type in DEGREE_TO_INTERSECTION.items():
        mask = is_int & (degree == deg_val)
        new_int[mask] = int_type

    # Degree >= threshold → Five-Point
    five_plus = is_int & (degree >= DEGREE_FIVE_PLUS_THRESHOLD)
    new_int[five_plus] = "5. Five-Point or More"

    # Not at intersection always wins
    new_int[~is_int] = "1. Not at Intersection"

    changed = (old_int != new_int) & (old_int != "")
    ri["Intersection Type"] = new_int
    report.fix("FIX_INT", "Intersection Type degree→approach corrected", changed.sum())


def fix_roadway_alignment(ri, report):
    """
    FIX 5 — Roadway Alignment: Grade requires grade data, ramps → On/Off Ramp.

    Current bug: curve_class > 3 → "4. Grade - Curve" without ANY grade data.
    Fix: Only assign Grade when hpms_grade_class or hpms_terrain_type confirms it.
    All other curves → "2. Curve - Level".
    All ramps (is_ramp=Yes) → "10. On/Off Ramp".

    State-agnostic: Works for any state. States WITH grade data (mountain states)
    will correctly get Grade values; flat states will only get Straight/Curve/Level.
    """
    if "curve_class" not in ri.columns:
        return

    cc = _n(ri, "curve_class").astype(int)
    is_ramp = _s(ri, "is_ramp") == "Yes"

    # Check for grade data availability
    has_grade_class = (
        "hpms_grade_class" in ri.columns and
        (_s(ri, "hpms_grade_class") != "").any()
    )
    has_terrain = (
        "hpms_terrain_type" in ri.columns and
        (_n(ri, "hpms_terrain_type") > 0).any()
    )

    # Build grade mask: only True where we have positive evidence of grade
    grade_mask = pd.Series(False, index=ri.index)
    if has_grade_class:
        gc = _s(ri, "hpms_grade_class")
        grade_mask |= (gc != "") & (gc != "0")
    if has_terrain:
        # terrain_type: 0=unknown, 1=level, 2=rolling, 3=mountainous
        tt = _n(ri, "hpms_terrain_type").astype(int)
        grade_mask |= tt >= 2  # Rolling or Mountainous = grade present

    # Build alignment
    old_aln = _s(ri, "Roadway Alignment")

    alignment = np.where(
        is_ramp, "10. On/Off Ramp",
        np.where(
            cc <= 1,
            np.where(grade_mask, "3. Grade - Straight", "1. Straight - Level"),
            np.where(grade_mask, "4. Grade - Curve", "2. Curve - Level")
        )
    )
    new_aln = pd.Series(alignment, index=ri.index, dtype=str)

    # Count changes
    ramp_fixed = is_ramp & (old_aln != "10. On/Off Ramp")
    grade_removed = old_aln.str.contains("Grade", na=False) & ~new_aln.str.contains("Grade", na=False)

    ri["Roadway Alignment"] = new_aln
    report.fix("FIX_ALN", "Ramps → 10. On/Off Ramp", ramp_fixed.sum())
    report.fix("FIX_ALN", "False Grade removed (no grade data)", grade_removed.sum())


def fix_surface_type(ri, report):
    """
    FIX 6 — Surface Type: Rows with no data source → default asphalt.

    Rows where resolved_surface_type="" AND OSM surface="" have no backing data.
    Currently assigned Brick/Other by road_data_authority default — incorrect.
    Fix: Default to asphalt for paved roads (FC 1-6), keep unpaved for FC-7 dirt roads.

    State-agnostic: Asphalt is 95%+ of US paved roads regardless of state.
    """
    resolved = _s(ri, "resolved_surface_type")
    osm_surf = _s(ri, "surface")
    current_surf = _s(ri, "Roadway Surface Type")
    fc = _s(ri, "Functional Class")

    # No backing data from any source
    no_data = (resolved == "") & (osm_surf == "")

    # Current value is one of the suspect defaults
    suspect = current_surf.isin([
        "3. Brick or Block", "6. Other", "1. Concrete",
        "Not Applicable", "",
    ])

    fix_mask = no_data & suspect
    if not fix_mask.any():
        return

    # Default to asphalt
    ri.loc[fix_mask, "Roadway Surface Type"] = "2. Blacktop, Asphalt, Bituminous"

    # Exception: if resolved_surface_type = "Unpaved" → keep as-is (already correct)
    # This won't fire here since resolved="" for these rows

    report.fix("FIX_SRF", "No-data surface → Blacktop (default)", fix_mask.sum())


def fix_school_zone(ri, report):
    """
    FIX 7 — School Zone: Standardize value format to VDOT standard.

    Standard: "1. Yes", "2. Yes - With School Activity", "3. No",
              "Not Applicable", "Not Provided"
    Current: mixed "No"/"3. No", "Yes"/"1. Yes"

    State-agnostic: VDOT standard is the CrashLens universal format.
    """
    sz = _s(ri, "School Zone")

    fixed = 0
    for bad_val, good_val in SCHOOL_ZONE_REMAP.items():
        mask = sz.str.lower() == bad_val.lower()
        # Don't remap values that are already in correct format
        already_ok = sz == good_val
        fix_mask = mask & ~already_ok
        if fix_mask.any():
            ri.loc[fix_mask, "School Zone"] = good_val
            fixed += fix_mask.sum()

    report.fix("FIX_SCH", "School Zone values standardized", fixed)


def fix_physical_juris(ri, report, state_fips="", cache_dir=""):
    """
    FIX 8 — Physical Juris Name: Resolve to city/town via Census places.

    Uses boundary_resolver.resolve_places() for point-in-polygon against
    Census place boundaries. Segments inside incorporated cities/towns get
    their name; rural segments keep county name.

    State-agnostic: Census places exist for every state. boundary_resolver
    handles all states with same code path.
    """
    if not cache_dir:
        report.fix("FIX_JUR", "Skipped: no cache_dir for boundaries", 0)
        return

    try:
        from boundary_resolver import BoundaryResolver
    except ImportError:
        report.fix("FIX_JUR", "Skipped: boundary_resolver not available", 0)
        return

    if "mid_lat" not in ri.columns or "mid_lon" not in ri.columns:
        return

    try:
        br = BoundaryResolver(cache_dir=cache_dir)
        if br.places is None:
            report.fix("FIX_JUR", "Skipped: place boundaries not loaded", 0)
            return

        # Build temp DataFrame for resolve_places
        pts = pd.DataFrame({
            "x": ri["mid_lon"].values,
            "y": ri["mid_lat"].values,
        }, index=ri.index)

        pts = br.resolve_places(pts, x_col="x", y_col="y",
                                state_fips=state_fips if state_fips else None)

        place_name = pts.get("resolved_place_name", pd.Series("", index=ri.index))
        place_fips = pts.get("resolved_place_fips", pd.Series("", index=ri.index))

        in_place = place_name.fillna("").str.strip() != ""
        if in_place.any():
            ri.loc[in_place, "Physical Juris Name"] = place_name[in_place].values
            if place_fips is not None:
                ri.loc[in_place, "Juris Code"] = place_fips[in_place].values
            report.fix("FIX_JUR", "Resolved to city/town via Census places", in_place.sum())
        else:
            report.fix("FIX_JUR", "No segments matched Census places", 0)

    except Exception as e:
        report.fix("FIX_JUR", f"Skipped: {str(e)[:60]}", 0)


def fix_through_lanes(ri, report):
    """
    FIX 9 — Through_Lanes: Replace 0-sentinel with empty. Apply FC minimums.

    0 lanes is physically impossible — it means "unknown".
    For Interstate/Freeway, apply minimum lane counts.

    State-agnostic: Lane minimums are federal MUTCD standards.
    """
    tl = _s(ri, "Through_Lanes")
    fc = _s(ri, "Functional Class")

    # Replace "0" with empty (unknown)
    zero_mask = tl == "0"
    if zero_mask.any():
        ri.loc[zero_mask, "Through_Lanes"] = ""
        report.fix("FIX_THL", "Through_Lanes 0→empty (unknown)", zero_mask.sum())

    # Apply FC minimums
    tl_after = _s(ri, "Through_Lanes")
    for prefix, min_lanes in FC_MIN_LANES.items():
        empty_fc = fc.str.startswith(prefix) & (tl_after == "")
        if empty_fc.any():
            ri.loc[empty_fc, "Through_Lanes"] = str(min_lanes)
            report.fix("FIX_THL", f"FC {prefix[0]} empty → min {min_lanes} lanes", empty_fc.sum())


def fix_aadt(ri, report):
    """
    FIX 10 — AADT: Propagate from same-road segments, FC average fallback.

    Layer 1: HPMS direct (already populated)
    Layer 2: Same road name → median AADT of known segments
    Layer 3: FC-based state average (computed from data, not hardcoded)

    State-agnostic: FC averages are computed from THIS state's own data.

    Adds AADT_source column: "HPMS"/"name_propagation"/"ref_propagation"/"fc_average"
    """
    aadt = _n(ri, "AADT").astype(int)

    # Initialize source tracking
    if "AADT_source" not in ri.columns:
        ri["AADT_source"] = ""
    src = _s(ri, "AADT_source")
    # Mark existing non-zero as direct
    direct = (aadt > 0) & (src == "")
    ri.loc[direct, "AADT_source"] = "direct"

    # Layer 2a: Same ref → median AADT
    ref = _s(ri, "ref")
    ref_has_aadt = (aadt > 0) & (ref != "")
    if ref_has_aadt.any():
        ref_medians = (ri.loc[ref_has_aadt]
                       .groupby(ref[ref_has_aadt])["AADT"]
                       .median().round().astype(int))
        needs_fill = (aadt == 0) & (ref != "") & ref.isin(ref_medians.index)
        if needs_fill.any():
            filled_vals = ref[needs_fill].map(ref_medians).fillna(0).astype(int)
            valid = filled_vals > 0
            ri.loc[needs_fill & valid, "AADT"] = filled_vals[valid].values
            ri.loc[needs_fill & valid, "AADT_source"] = "ref_propagation"
            report.fix("FIX_ADT", "AADT from same-ref road segments", (needs_fill & valid).sum())

    # Layer 2b: Same road name → median AADT
    aadt = _n(ri, "AADT").astype(int)
    name = _s(ri, "name")
    name_has_aadt = (aadt > 0) & (name != "")
    if name_has_aadt.any():
        name_medians = (ri.loc[name_has_aadt]
                        .groupby(name[name_has_aadt])["AADT"]
                        .median().round().astype(int))
        needs_fill = (aadt == 0) & (name != "") & name.isin(name_medians.index)
        if needs_fill.any():
            filled_vals = name[needs_fill].map(name_medians).fillna(0).astype(int)
            valid = filled_vals > 0
            ri.loc[needs_fill & valid, "AADT"] = filled_vals[valid].values
            ri.loc[needs_fill & valid, "AADT_source"] = "name_propagation"
            report.fix("FIX_ADT", "AADT from same-name road segments", (needs_fill & valid).sum())

    # Layer 3: FC-based state average (computed from own data, not hardcoded)
    aadt = _n(ri, "AADT").astype(int)
    fc = _s(ri, "Functional Class")
    fc_has_aadt = (aadt > 0) & (fc != "")
    if fc_has_aadt.any():
        fc_avgs = (ri.loc[fc_has_aadt]
                   .groupby(fc[fc_has_aadt])["AADT"]
                   .median().round().astype(int))
        needs_fill = (aadt == 0) & (fc != "") & fc.isin(fc_avgs.index)
        if needs_fill.any():
            filled_vals = fc[needs_fill].map(fc_avgs).fillna(0).astype(int)
            valid = filled_vals > 0
            ri.loc[needs_fill & valid, "AADT"] = filled_vals[valid].values
            ri.loc[needs_fill & valid, "AADT_source"] = "fc_average"
            report.fix("FIX_ADT", "AADT from FC state-average", (needs_fill & valid).sum())


def fix_sentinels(ri, report):
    """
    FIX 11 — Sentinel values: Replace -1 with NaN in distance/node columns.

    -1 is used as "no detection" sentinel in Mapillary and HPMS-miss columns.
    Downstream consumers should see NaN/empty, not a fake negative distance.

    State-agnostic: Sentinel convention is hardcoded in generate_osm_data.py
    and build_road_inventory.py — same for all states.
    """
    sentinel_cols = []
    fixed_total = 0

    for col in ri.columns:
        if ri[col].dtype in ("float32", "float64"):
            neg_count = (ri[col] < 0).sum()
            # Skip longitude columns (legitimately negative in Western Hemisphere)
            if "lon" in col.lower():
                continue
            if neg_count > 0 and neg_count > len(ri) * 0.05:  # >5% sentinel pattern
                ri.loc[ri[col] < 0, col] = np.nan
                sentinel_cols.append(col)
                fixed_total += neg_count

    # Integer node columns: -1 → 0
    for col in ["osm_u_node", "osm_v_node"]:
        if col in ri.columns:
            neg = ri[col] < 0
            if neg.any():
                ri.loc[neg, col] = 0
                fixed_total += neg.sum()
                sentinel_cols.append(col)

    if sentinel_cols:
        report.fix("FIX_SEN", f"Sentinels cleared in {len(sentinel_cols)} columns", fixed_total)


def fix_duplicates(ri, report):
    """
    FIX 12 — Duplicates: Remove rows with identical coordinates + name + source.

    True duplicates = same u/v coords + same road name + same data source.
    Stacked roads (same coords, different names) are legitimate overlaps and kept.

    Returns the deduplicated DataFrame (not in-place — caller must reassign).

    State-agnostic: OSM dedup logic is universal.
    """
    required = ["u_lat", "u_lon", "v_lat", "v_lon", "name", "road_source"]
    if not all(c in ri.columns for c in required):
        return ri

    before = len(ri)
    ri_out = ri.drop_duplicates(subset=required, keep="first")
    removed = before - len(ri_out)

    if removed > 0:
        report.fix("FIX_DUP", "True duplicate segments removed", removed)

    return ri_out


def fix_geography(ri, report, hierarchy=None):
    """
    FIX 13 — Geography: Planning District + MPO cross-validation.

    13a. Planning District: If state has no PDs, use DOT District as proxy.
         Detection: if PD values look like MPO names, they're wrong.
    13b. MPO bleeding: Cross-validate MPO county membership against hierarchy.
    13c. MPO Name consistency: "Dover / Kent County MPO" vs "Dover/Kent County MPO"
         must match hierarchy.json exactly.

    State-agnostic: Reads hierarchy.json for per-state MPO/county structure.
    """
    if hierarchy is None:
        report.fix("FIX_GEO", "Skipped: no hierarchy provided", 0)
        return

    pd_col = _s(ri, "Planning District")
    mpo_col = _s(ri, "MPO Name")
    dot_col = _s(ri, "DOT District")
    juris_code = _s(ri, "Juris Code")

    # --- 13a: Detect if Planning District contains MPO names ---
    # If hierarchy has tprs (Transportation Planning Regions) but no separate PDs,
    # and PD values match TPR names → replace PD with DOT District
    tprs = hierarchy.get("tprs", {})
    tpr_names = {v.get("name", "") for v in tprs.values()}

    if tpr_names:
        pd_is_tpr = pd_col.isin(tpr_names)
        # Check if PD and DOT District are different (i.e., PD wasn't already district)
        pd_not_district = pd_is_tpr & (pd_col != dot_col) & (dot_col != "")
        if pd_not_district.any():
            # Check: does the hierarchy have a separate planning_districts key?
            has_real_pds = "planning_districts" in hierarchy and hierarchy["planning_districts"]
            if not has_real_pds:
                ri.loc[pd_not_district, "Planning District"] = dot_col[pd_not_district].values
                report.fix("FIX_GEO", "Planning District←DOT District (no PDs in state)",
                           pd_not_district.sum())

    # --- 13b: MPO county cross-validation ---
    # Extract county FIPS from Juris Code (last 3 digits for state+county FIPS)
    state_fips = hierarchy.get("state", {}).get("fips", "")
    if state_fips and len(juris_code.iloc[0]) >= 5:
        county_fips = juris_code.str[-3:]  # County portion of 5-digit FIPS
    elif state_fips:
        county_fips = juris_code.str.replace(state_fips, "", regex=False)
    else:
        county_fips = pd.Series("", index=ri.index)

    mpo_fixed = 0
    for tpr_id, tpr_info in tprs.items():
        tpr_name = tpr_info.get("name", "")
        tpr_short = tpr_info.get("shortName", "")
        tpr_counties = set(tpr_info.get("counties", []))

        if not tpr_counties or not tpr_name:
            continue

        # Match MPO Name (handle slight naming variants)
        in_mpo = mpo_col == tpr_name
        # Also try matching shortName variant
        if not in_mpo.any() and tpr_short:
            in_mpo = mpo_col.str.contains(tpr_short, na=False, regex=False)

        if not in_mpo.any():
            continue

        # Find segments assigned to this MPO but in wrong county
        wrong_county = in_mpo & ~county_fips.isin(tpr_counties) & (county_fips != "")
        if wrong_county.any():
            ri.loc[wrong_county, "MPO Name"] = ""
            mpo_fixed += wrong_county.sum()

    if mpo_fixed:
        report.fix("FIX_GEO", "MPO cleared: segment in wrong county", mpo_fixed)


def fix_max_speed_diff(ri, report):
    """
    FIX 14 — Max Speed Diff: Merge resolved_speed_limit with FC default.

    Road inventory stores SPEED LIMIT in this column (not speed differential).
    Use resolved_speed_limit (from HPMS/OSM) when available, otherwise keep
    FC-based default.

    State-agnostic: resolved_speed_limit is populated by build_road_inventory
    universally. FC defaults are federal standards.
    """
    rsl = _n(ri, "resolved_speed_limit").astype(int)
    msd = _n(ri, "Max Speed Diff").astype(int)

    # Where resolved > 0 AND differs from current → use resolved
    better = (rsl > 0) & (rsl != msd)
    if better.any():
        ri.loc[better, "Max Speed Diff"] = rsl[better].astype(str)
        report.fix("FIX_SPD", "Max Speed Diff←resolved_speed_limit", better.sum())

    # Where both are 0 → apply FC default
    fc = _s(ri, "Functional Class")
    both_zero = (rsl == 0) & (msd == 0) & (fc != "")
    if both_zero.any():
        filled = 0
        for prefix, default_spd in FC_DEFAULT_SPEED.items():
            mask = both_zero & fc.str.startswith(prefix)
            if mask.any():
                ri.loc[mask, "Max Speed Diff"] = str(default_spd)
                filled += mask.sum()
        report.fix("FIX_SPD", "Max Speed Diff←FC default (was 0)", filled)


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def postprocess(ri, state_abbr="", hierarchy=None, cache_dir="",
                state_fips="", verbose=True):
    """
    Run all 13 post-processing fixes. Modifies ri in-place (except dedup).

    Args:
        ri:           Road inventory DataFrame
        state_abbr:   Two-letter state abbreviation (e.g. "de")
        hierarchy:    Parsed hierarchy.json dict
        cache_dir:    Path to boundaries cache (for city/town resolution)
        state_fips:   State FIPS code (e.g. "10" for Delaware)
        verbose:      Print progress

    Returns:
        (ri, report): Possibly new DataFrame (dedup may drop rows), report object
    """
    if verbose:
        print(f"\n    ── Road Inventory Post-Processor v1.0 ──")
        print(f"    Input: {len(ri):,} segments × {len(ri.columns)} columns")

    report = PostprocessReport()

    # Infer state_fips from hierarchy if not provided
    if not state_fips and hierarchy:
        state_fips = hierarchy.get("state", {}).get("fips", "")

    # --- Run fixes in dependency order ---

    # Group 1: Value corrections (no dependencies)
    fix_ownership(ri, report)                                  # FIX 2
    fix_intersection_type(ri, report)                          # FIX 4
    fix_roadway_alignment(ri, report)                          # FIX 5
    fix_surface_type(ri, report)                               # FIX 6
    fix_school_zone(ri, report)                                # FIX 7

    # Group 2: Source-ranked columns
    fix_rte_name(ri, report, state_abbr=state_abbr)            # FIX 1
    fix_max_speed_diff(ri, report)                             # FIX 14

    # Group 3: Sentinel cleanup
    fix_sentinels(ri, report)                                  # FIX 11
    fix_through_lanes(ri, report)                              # FIX 9

    # Group 4: Enrichment (depends on clean base data)
    fix_aadt(ri, report)                                       # FIX 10

    # Group 5: Geography (depends on hierarchy)
    fix_geography(ri, report, hierarchy=hierarchy)              # FIX 13

    # Group 6: City/town resolution (optional, slow)
    fix_physical_juris(ri, report, state_fips=state_fips,      # FIX 8
                       cache_dir=cache_dir)

    # Group 7: Dedup (LAST — after all fixes, before output)
    ri = fix_duplicates(ri, report)                            # FIX 12

    if verbose:
        report.print_report()
        print(f"    Output: {len(ri):,} segments")

    return ri, report


# ═══════════════════════════════════════════════════════════════
#  STANDALONE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import gzip
    import io

    parser = argparse.ArgumentParser(description="Post-process road inventory")
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation")
    parser.add_argument("--cache-dir", default="cache", help="Path to cache directory")
    parser.add_argument("--hierarchy", default="", help="Path to hierarchy.json")
    parser.add_argument("--save", action="store_true", help="Save fixed inventory")
    args = parser.parse_args()

    abbr = args.state.lower()
    ri_path = Path(args.cache_dir) / f"{abbr}_road_inventory.parquet.gz"
    if not ri_path.exists():
        print(f"Not found: {ri_path}")
        exit(1)

    # Load hierarchy
    hierarchy = None
    hier_path = Path(args.hierarchy) if args.hierarchy else Path(f"hierarchy.json")
    if hier_path.exists():
        with open(hier_path) as f:
            hierarchy = json.load(f)
        print(f"Loaded hierarchy: {hier_path}")
    else:
        print(f"No hierarchy.json found — geography fixes skipped")

    # Load state FIPS from hierarchy
    state_fips = ""
    if hierarchy:
        state_fips = hierarchy.get("state", {}).get("fips", "")

    # Load inventory
    print(f"Loading {ri_path}...")
    with gzip.open(ri_path, "rb") as f:
        ri = pd.read_parquet(io.BytesIO(f.read()))
    print(f"  {len(ri):,} segments × {len(ri.columns)} columns")

    # Boundaries cache for city/town resolution
    boundaries_dir = str(Path(args.cache_dir) / "boundaries")

    # Run
    ri, report = postprocess(
        ri,
        state_abbr=abbr,
        hierarchy=hierarchy,
        cache_dir=boundaries_dir,
        state_fips=state_fips,
    )

    if args.save and report.total > 0:
        import pyarrow as pa
        import pyarrow.parquet as pq

        print(f"\n  Saving fixed inventory...")
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(ri), buf, compression="gzip")
        with open(ri_path, "wb") as f:
            f.write(buf.getvalue())
        print(f"  Done. {len(ri):,} segments saved.")
    elif report.total > 0:
        print(f"\n  {report.total:,} fixes available. Run with --save to apply.")
    else:
        print(f"\n  No fixes needed.")
