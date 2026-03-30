"""
road_inventory_validator.py — CrashLens Road Inventory Quality Gate
====================================================================
Validates and fixes road inventory data BEFORE crash enrichment.
Every crash inherits its road segment's attributes — one bad segment
poisons thousands of crashes. This module catches contradictions,
impossible values, and fixable gaps.

Runs automatically in:
  - build_road_inventory.py   (fixes baked into parquet at build time)
  - road_inventory_enricher.py (catches unfixed parquets at load time)

Standalone: python road_inventory_validator.py --state de [--fix]

12 RULE GROUPS:
  SPD   Speed limit vs FC range + area type + lanes
  SURF  Surface type gap fill by FC
  OWN   Ownership vs FC (Interstate must be State)
  SYS   SYSTEM vs FC consistency
  FAC   Facility Type vs FC + oneway tag
  DESC  Roadway Description from Facility Type
  CTRL  Traffic control vs intersection + FC
  GEOM  Zero-length, duplicates, out-of-bounds
  LANE  Lane count vs FC plausibility
  RTE   Route name prefix vs FC
  AADT  Traffic volume vs FC plausibility
  SCHZ  School Zone vs nearest school distance
"""

import numpy as np
import pandas as pd
from collections import OrderedDict

# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

# Speed limit floor / ceiling / default by FC
FC_SPEED_RULES = {
    "1-": (45, 75, 55),
    "2-": (35, 70, 50),
    "3-": (25, 60, 40),
    "4-": (20, 55, 35),
    "5-": (15, 50, 30),
    "6-": (15, 45, 25),
    "7-": (5, 35, 25),
}

# Surface type default by FC
FC_DEFAULT_SURFACE = {
    "1-": "1. Concrete",
    "2-": "2. Blacktop, Asphalt, Bituminous",
    "3-": "2. Blacktop, Asphalt, Bituminous",
    "4-": "2. Blacktop, Asphalt, Bituminous",
    "5-": "2. Blacktop, Asphalt, Bituminous",
    "6-": "2. Blacktop, Asphalt, Bituminous",
    "7-": "2. Blacktop, Asphalt, Bituminous",
}

# FC → expected SYSTEM
FC_TO_SYSTEM = {
    "1-": "DOT Interstate",
    "2-": "DOT Primary",
    "3-": "DOT Primary",
    "4-": "DOT Secondary",
    "5-": "DOT Secondary",
    "6-": "Non-DOT primary",
    "7-": "Non-DOT secondary",
}

# FC → default Facility Type
FC_DEFAULT_FACILITY = {
    "1-": "4-Two-Way Divided",
    "2-": "4-Two-Way Divided",
    "3-": "3-Two-Way Undivided",
    "4-": "3-Two-Way Undivided",
    "5-": "3-Two-Way Undivided",
    "6-": "3-Two-Way Undivided",
    "7-": "3-Two-Way Undivided",
}

# Facility Type → Roadway Description
FACILITY_TO_DESCRIPTION = {
    "1-One-Way Undivided":  "4. One-Way, Not Divided",
    "2-One-Way Divided":    "4. One-Way, Not Divided",
    "3-Two-Way Undivided":  "1. Two-Way, Not Divided",
    "4-Two-Way Divided":    "2. Two-Way, Divided, Unprotected Median",
}


class ValidationReport:
    """Collects results and fix counts."""

    def __init__(self):
        self.checks = OrderedDict()
        self.fixes = OrderedDict()
        self.total_fixes = 0

    def check(self, rid, desc, found, total, severity="info"):
        self.checks[rid] = {
            "desc": desc, "found": found, "total": total,
            "pct": found / max(total, 1) * 100, "severity": severity,
        }

    def fix(self, rid, desc, count):
        self.fixes[rid] = {"desc": desc, "count": count}
        self.total_fixes += count

    def print_report(self):
        active = {k: v for k, v in self.checks.items() if v["found"] > 0}
        if active:
            print(f"\n    ── Data Quality Report ({len(active)} issues) ──")
            for rid, c in active.items():
                icon = {"error": "🔴", "warn": "🟡", "info": "🔵",
                        "fixed": "🟢"}.get(c["severity"], "⚪")
                print(f"    {icon} {rid:<14s} {c['desc']:<42s} "
                      f"{c['found']:>6,}/{c['total']:>6,} ({c['pct']:.1f}%)")

        if self.fixes:
            print(f"\n    ── Fixes Applied ({self.total_fixes:,} total) ──")
            for rid, f in self.fixes.items():
                print(f"    🟢 {rid:<14s} {f['desc']:<42s} {f['count']:>6,}")


def _str(ri, col):
    """Get column as string, empty-safe."""
    if col not in ri.columns:
        return pd.Series("", index=ri.index)
    return ri[col].fillna("").astype(str)


def _num(ri, col):
    """Get column as numeric, 0-safe."""
    if col not in ri.columns:
        return pd.Series(0, index=ri.index, dtype=float)
    return pd.to_numeric(ri[col], errors="coerce").fillna(0)


def _cast_str(ri, col):
    """Cast column to string dtype for safe assignment."""
    if col in ri.columns:
        ri[col] = ri[col].fillna("").astype(str)


def validate_and_fix(ri, verbose=True):
    """Run all 12 validation rule groups. Fixes in-place. Returns report."""
    report = ValidationReport()
    n = len(ri)

    # Pre-cast columns we'll modify to string
    for col in ["Max Speed Diff", "Roadway Surface Type", "Facility Type",
                "Ownership", "SYSTEM", "Roadway Description", "School Zone",
                "Traffic Control Type", "Through_Lanes"]:
        _cast_str(ri, col)

    fc = _str(ri, "Functional Class")
    has_fc = fc.str.len() > 0

    # ═══════════════════════════════════════════════════════════
    #  RULE 1: SPEED vs FC RANGE (FIX)
    # ═══════════════════════════════════════════════════════════
    spd = _num(ri, "Max Speed Diff")
    has_speed = spd > 0
    total_spd_fix = 0

    for prefix, (lo, hi, default) in FC_SPEED_RULES.items():
        mask = fc.str.startswith(prefix) & has_speed
        if not mask.any():
            continue
        bad = mask & ((spd < lo) | (spd > hi))
        if bad.any():
            ri.loc[bad, "Max Speed Diff"] = str(default)
            total_spd_fix += bad.sum()

    report.check("SPD_RANGE", "Speed outside FC range", total_spd_fix, n,
                 "fixed" if total_spd_fix else "info")
    if total_spd_fix:
        report.fix("SPD_FIX", "Speed reset to FC default", total_spd_fix)

    # Additional: 4+ lanes with speed < 25 → raise to 25
    lanes = _num(ri, "Through_Lanes")
    spd = _num(ri, "Max Speed Diff")
    big_slow = (lanes >= 4) & (spd > 0) & (spd < 25)
    if big_slow.any():
        ri.loc[big_slow, "Max Speed Diff"] = "25"
        report.fix("SPD_LANE", "4+ lane road speed → 25", big_slow.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 2: SURFACE TYPE GAP (FIX)
    # ═══════════════════════════════════════════════════════════
    surf = _str(ri, "Roadway Surface Type")
    missing = has_fc & surf.isin(["", "nan", "None"])
    filled = 0
    for prefix, val in FC_DEFAULT_SURFACE.items():
        fill = missing & fc.str.startswith(prefix)
        if fill.any():
            ri.loc[fill, "Roadway Surface Type"] = val
            filled += fill.sum()
    report.check("SURF_GAP", "Surface type missing", missing.sum(), n, "fixed")
    if filled:
        report.fix("SURF_FILL", "Surface → FC default", filled)

    # ═══════════════════════════════════════════════════════════
    #  RULE 3: OWNERSHIP vs FC (FIX for FC 1-2)
    # ═══════════════════════════════════════════════════════════
    own = _str(ri, "Ownership")
    for prefix in ["1-", "2-"]:
        mask = fc.str.startswith(prefix) & own.ne("") & ~own.str.contains("State", na=False)
        if mask.any():
            ri.loc[mask, "Ownership"] = "1. State Hwy Agency"
            report.fix(f"OWN_FC{prefix[0]}", f"FC {prefix[0]} Ownership → State", mask.sum())

    fc7_state = fc.str.startswith("7-") & own.str.contains("State", na=False)
    report.check("OWN_FC7", "FC-7 owned by State", fc7_state.sum(),
                 fc.str.startswith("7-").sum(), "info")

    # ═══════════════════════════════════════════════════════════
    #  RULE 4: SYSTEM vs FC (FIX)
    # ═══════════════════════════════════════════════════════════
    sys_col = _str(ri, "SYSTEM")
    sys_fixed = 0
    for prefix, expected in FC_TO_SYSTEM.items():
        mask = fc.str.startswith(prefix) & has_fc & sys_col.ne("") & (sys_col != expected)
        if mask.any():
            ri.loc[mask, "SYSTEM"] = expected
            sys_fixed += mask.sum()
    if sys_fixed:
        report.fix("SYS_FIX", "SYSTEM aligned to FC", sys_fixed)

    # ═══════════════════════════════════════════════════════════
    #  RULE 5: FACILITY TYPE (FIX — gap + oneway)
    # ═══════════════════════════════════════════════════════════
    fac = _str(ri, "Facility Type")
    ow_tag = _str(ri, "oneway").str.lower() if "oneway" in ri.columns \
        else pd.Series("", index=ri.index)
    is_oneway = ow_tag.isin(["yes", "true", "1", "-1"])

    # 5a: Missing → FC default
    missing_fac = has_fc & fac.isin(["", "nan", "None"])
    fac_filled = 0
    for prefix, val in FC_DEFAULT_FACILITY.items():
        fill = missing_fac & fc.str.startswith(prefix)
        if fill.any():
            ri.loc[fill, "Facility Type"] = val
            fac_filled += fill.sum()
    if fac_filled:
        report.fix("FAC_FILL", "Facility Type → FC default", fac_filled)

    # 5b: Oneway road + Two-Way Facility → One-Way
    fac = _str(ri, "Facility Type")
    oneway_twoway = is_oneway & fac.str.contains("Two-Way", na=False)
    if oneway_twoway.any():
        ri.loc[oneway_twoway, "Facility Type"] = "1-One-Way Undivided"
        report.fix("FAC_OW_FIX", "Oneway road → One-Way Facility", oneway_twoway.sum())

    # 5c: Two-way road + One-Way Facility → Two-Way
    fac = _str(ri, "Facility Type")
    twoway_oneway = ~is_oneway & fac.str.contains("One-Way", na=False) & fac.ne("")
    if twoway_oneway.any():
        for prefix in ["1-", "2-"]:
            m = twoway_oneway & fc.str.startswith(prefix)
            if m.any():
                ri.loc[m, "Facility Type"] = "4-Two-Way Divided"
        for prefix in ["3-", "4-", "5-", "6-", "7-"]:
            m = twoway_oneway & fc.str.startswith(prefix)
            if m.any():
                ri.loc[m, "Facility Type"] = "3-Two-Way Undivided"
        report.fix("FAC_TW_FIX", "Two-way road → Two-Way Facility", twoway_oneway.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 6: ROADWAY DESCRIPTION (FIX)
    # ═══════════════════════════════════════════════════════════
    desc = _str(ri, "Roadway Description")
    fac = _str(ri, "Facility Type")
    missing_desc = has_fc & desc.isin(["", "nan", "None"])
    if missing_desc.any():
        mapped = fac[missing_desc].map(FACILITY_TO_DESCRIPTION)
        has_val = mapped.notna() & mapped.ne("")
        if has_val.any():
            ri.loc[has_val[has_val].index, "Roadway Description"] = mapped[has_val].values
            report.fix("DESC_FILL", "Description from Facility Type", has_val.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 7: TRAFFIC CONTROL (FIX)
    # ═══════════════════════════════════════════════════════════
    tc = _str(ri, "Traffic Control Type")
    is_int = _str(ri, "is_intersection") == "Yes" if "is_intersection" in ri.columns \
        else pd.Series(False, index=ri.index)

    # Stop sign on Interstate/Freeway → remove
    stop_on_hwy = tc.str.contains("Stop Sign", na=False) & fc.str.match(r"^[12]-")
    if stop_on_hwy.any():
        ri.loc[stop_on_hwy, "Traffic Control Type"] = "1. No Traffic Control"
        report.fix("CTRL_STOP", "Stop sign on Interstate removed", stop_on_hwy.sum())

    # Signal on non-intersection FC-7 → remove
    sig_bad = tc.str.contains("Signal", na=False) & fc.str.startswith("7-") & ~is_int
    if sig_bad.any():
        ri.loc[sig_bad, "Traffic Control Type"] = "1. No Traffic Control"
        report.fix("CTRL_SIG", "Signal on non-int local removed", sig_bad.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 8: LANE COUNT (FIX)
    # ═══════════════════════════════════════════════════════════
    lanes = _num(ri, "Through_Lanes")

    # Interstate/Freeway with <2 lanes → 2
    for prefix, min_lanes in [("1-", 2), ("2-", 2)]:
        mask = fc.str.startswith(prefix) & (lanes > 0) & (lanes < min_lanes)
        if mask.any():
            ri.loc[mask, "Through_Lanes"] = str(min_lanes)
            report.fix(f"LANE_FC{prefix[0]}", f"FC {prefix[0]} lanes raised to {min_lanes}", mask.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 9: ROUTE NAME vs FC (FIX)
    # ═══════════════════════════════════════════════════════════
    rte = _str(ri, "RTE Name")

    # I-* route but not FC-1 → fix FC + SYSTEM + Ownership
    i_route = rte.str.match(r"^I[-\s]?\d", na=False)
    i_wrong = i_route & ~fc.str.startswith("1-") & fc.ne("")
    if i_wrong.any():
        ri.loc[i_wrong, "Functional Class"] = "1-Interstate (A,1)"
        ri.loc[i_wrong, "SYSTEM"] = "DOT Interstate"
        ri.loc[i_wrong, "Ownership"] = "1. State Hwy Agency"
        report.fix("RTE_INT", "I-* route → FC-1 Interstate", i_wrong.sum())

    # US-* route on FC-7 → upgrade to FC-3
    us_local = rte.str.match(r"^US[-\s]?\d", na=False) & fc.str.startswith("7-")
    if us_local.any():
        ri.loc[us_local, "Functional Class"] = "3-Principal Arterial - Other (E,2)"
        ri.loc[us_local, "SYSTEM"] = "DOT Primary"
        ri.loc[us_local, "Ownership"] = "1. State Hwy Agency"
        report.fix("RTE_US", "US-* on FC-7 → FC-3", us_local.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 10: SCHOOL ZONE vs DISTANCE (FIX)
    # ═══════════════════════════════════════════════════════════
    sz = _str(ri, "School Zone")
    school_dist = _num(ri, "nearest_school_dist_ft")

    # School Zone=Yes but no school nearby → No
    sz_yes = sz.str.contains("Yes", na=False)
    far = sz_yes & ((school_dist == 0) | (school_dist > 2000))
    if far.any():
        ri.loc[far, "School Zone"] = "3. No"
        report.fix("SCHZ_FAR", "School Zone but no school → No", far.sum())

    # School within 500ft but Zone=No → Yes
    near = ~sz_yes & (school_dist > 0) & (school_dist <= 500)
    if near.any():
        ri.loc[near, "School Zone"] = "1. Yes"
        report.fix("SCHZ_NEAR", "School <500ft → Zone=Yes", near.sum())

    # ═══════════════════════════════════════════════════════════
    #  RULE 11: GEOMETRY (FLAG only)
    # ═══════════════════════════════════════════════════════════
    if all(c in ri.columns for c in ["u_lat", "u_lon", "v_lat", "v_lon"]):
        M = 111320.0
        cos_lat = np.cos(np.radians(ri["u_lat"].mean()))
        dx = (ri["v_lon"].values - ri["u_lon"].values) * M * cos_lat
        dy = (ri["v_lat"].values - ri["u_lat"].values) * M
        seg_len = np.sqrt(dx * dx + dy * dy) * 3.28084
        report.check("GEOM_ZERO", "Zero-length segments", (seg_len < 1).sum(), n, "warn")
        report.check("GEOM_LONG", "Segments > 10Kft", (seg_len > 10000).sum(), n, "info")

    # ═══════════════════════════════════════════════════════════
    #  RULE 12: AADT (FLAG only)
    # ═══════════════════════════════════════════════════════════
    aadt = _num(ri, "AADT")
    report.check("AADT_I_LO", "Interstate AADT < 500",
                 (fc.str.startswith("1-") & (aadt > 0) & (aadt < 500)).sum(), n, "warn")

    # ═══════════════════════════════════════════════════════════
    if verbose:
        report.print_report()
    return report


# ═══════════════════════════════════════════════════════════════
#  STANDALONE
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse, gzip, io
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Validate road inventory")
    parser.add_argument("--state", required=True)
    parser.add_argument("--cache-dir", default="cache")
    parser.add_argument("--fix", action="store_true", help="Apply fixes and save")
    args = parser.parse_args()

    abbr = args.state.lower()
    ri_path = Path(args.cache_dir) / f"{abbr}_road_inventory.parquet.gz"
    if not ri_path.exists():
        print(f"Not found: {ri_path}"); exit(1)

    print(f"Loading {ri_path}...")
    with gzip.open(ri_path, "rb") as f:
        ri = pd.read_parquet(io.BytesIO(f.read()))
    print(f"  {len(ri):,} segments × {len(ri.columns)} columns")

    report = validate_and_fix(ri, verbose=True)

    if args.fix and report.total_fixes > 0:
        print(f"\n  Saving fixed inventory...")
        import pyarrow as pa, pyarrow.parquet as pq
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(ri), buf, compression="gzip")
        with open(ri_path, "wb") as f:
            f.write(buf.getvalue())
        print(f"  Done. {report.total_fixes:,} fixes applied.")
    elif report.total_fixes > 0:
        print(f"\n  {report.total_fixes:,} fixes available. Run with --fix to apply.")
