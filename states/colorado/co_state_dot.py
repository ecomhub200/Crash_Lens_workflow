#!/usr/bin/env python3
"""
co_state_dot.py — Colorado DOT (CDOT) Road Inventory Downloader & Normalizer
===============================================================================
Single-file config for Colorado's state DOT road inventory.
Called by generate_state_dot_data.py (root).

Data Source (ONE endpoint — CDOT publishes everything in a single layer):
  CDOT Open Data SDE — Highways (Layer 7):
    https://dtdapps.coloradodot.info/arcgis/rest/services/
      CPLAN/open_data_sde/FeatureServer/7
    ~9,000 segments with FC, speed limit, lanes, surface, AADT,
    county, urban/rural, terrain, region, NHS, median, width

  FALLBACK — CDOT Highways (Layer 15 in same service):
    Same server, potentially different layer index if service changes.
    Also available via Socrata: data.colorado.gov/resource/2h6w-z9ry

ENDPOINT STABILITY:
  CDOT's dtdapps server has been stable since ~2018. The open_data_sde
  FeatureServer provides the same data as the Socrata dataset
  "Highways in Colorado" (2h6w-z9ry) but with ArcGIS pagination.
  MaxRecordCount: 1000. Server version: 10.91.

Colorado is MUCH simpler than Virginia/Delaware — no secondary downloads
needed. All road characteristics are in ONE layer.

VERIFIED FIELD NAMES (from Socrata column list, matching ArcGIS uppercase):
  ROUTE, ROUTESIGN, FUNCCLASS, SPEEDLIM, THRULNQTY, THRULNWD,
  COUNTY, FIPSCOUNTY, REGION, TPRID, COMMDISTID, ACCESS, AADT,
  TERRAIN, POPULATION, PRISURF, SEG_LENGTH, REFPT, ENDREFPT,
  ISDIVIDED, NHSDESIG, ALIAS, CITY, MEDIAN, MEDIANWD

Usage:
  python generate_state_dot_data.py --state co
  python generate_state_dot_data.py --state co --upload
"""

# ═══════════════════════════════════════════════════════════════
#  ENDPOINT CONFIGURATION
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "co"
STATE_NAME = "Colorado"
STATE_FIPS = "08"
STATE_DOT = "CDOT"

# PRIMARY: CDOT Open Data SDE — Highways layer
# This is the richest endpoint — has speed, lanes, surface, AADT, FC, ownership
# ALL in one layer. No secondary downloads needed.
ENDPOINT_URL = (
    "https://dtdapps.coloradodot.info/arcgis/rest/services/"
    "CPLAN/open_data_sde/FeatureServer/7"
)

# ArcGIS pagination
MAX_RECORD_COUNT = 1000
OUT_SR = 4326             # Request WGS84
GEOMETRY_TYPE = "polyline"

# Request all fields — CDOT has ~50 fields, all useful
OUT_FIELDS = None


# ═══════════════════════════════════════════════════════════════
#  FIELD MAPPING: CDOT field → CrashLens column
# ═══════════════════════════════════════════════════════════════
# Field names are UPPERCASE in ArcGIS. If the server returns lowercase
# (some Socrata-backed services do), normalize() handles both.

FIELD_MAP = {
    # ── Route Identification ──
    "ROUTE":              "dot_route",              # Route number ("006", "070")
    "ROUTESIGN":          "dot_route_sign",          # "Interstate", "US Highway", "State Highway"
    "ALIAS":              "dot_alias",               # Route alias/name
    "DESCRIPTION":         "dot_description",         # Segment description

    # ── Classification ──
    "FUNCCLASS":          "dot_func_class",          # FC code (1-7)
    "NHSDESIG":           "dot_nhs",                 # NHS designation
    "ACCESS_":            "dot_access",              # Access control

    # ── Speed & Geometry ──
    "SPEEDLIM":           "dot_speed_limit",         # Posted speed limit (mph)
    "THRULNQTY":          "dot_through_lanes",       # Through lane count
    "THRULNWD":           "dot_lane_width",          # Through lane width (ft)
    "ISDIVIDED":          "dot_is_divided",           # Divided highway flag
    "MEDIAN":             "dot_median_type",          # Median type
    "MEDIANWD":           "dot_median_width",         # Median width (ft)

    # ── Surface ──
    "PRISURF":            "dot_surface",              # Primary surface type

    # ── Traffic ──
    "AADT":               "dot_aadt",                 # Average Annual Daily Traffic
    "AADTYR":             "dot_aadt_year",             # AADT year
    "AADTCOMB":           "dot_aadt_combo",            # AADT combination trucks
    "AADTSINGLE":         "dot_aadt_single",           # AADT single unit trucks
    "VMT":                "dot_vmt",                   # Vehicle miles traveled

    # ── Location ──
    "COUNTY":             "dot_county",               # County name (e.g. "Fremont Co")
    "FIPSCOUNTY":         "dot_fips_county",           # County FIPS code
    "CITY":               "dot_city",                  # City name
    "FIPSCITY":           "dot_fips_city",             # City FIPS code
    "REGION":             "dot_region",                # CDOT region (1-5)
    "TPRID":              "dot_tpr_id",                # Transportation Planning Region
    "COMMDISTID":         "dot_commission_district",   # Commission district
    "MPOID":              "dot_mpo_id",                # MPO identifier

    # ── Milepoints ──
    "REFPT":              "dot_beg_mp",               # Begin reference point
    "ENDREFPT":           "dot_end_mp",               # End reference point
    "SEG_LENGTH":         "dot_seg_length",            # Segment length

    # ── Other ──
    "TERRAIN":            "dot_terrain",               # Terrain type
    "POPULATION":         "dot_population",            # Population category (Urban/Rural)
}


# ═══════════════════════════════════════════════════════════════
#  VALUE TRANSFORMS
# ═══════════════════════════════════════════════════════════════

# FC: CDOT funcclass code → CrashLens standard
# CDOT uses FHWA standard 1-7 codes
FC_CODE_MAP = {
    # Actual CDOT values (digit + double space + description)
    "1  Interstate":                                "1-Interstate",
    "2  Other Freeways and Expressways":            "2-Freeway/Expressway",
    "3  Principal Arterial - Other":                "3-Principal Arterial",
    "4  Minor Arterial":                            "4-Minor Arterial",
    "5  Major Collector":                           "5-Major Collector",
    "6  Minor Collector":                           "6-Minor Collector",
    "7  Local":                                     "7-Local",
    # Single digit fallback
    "1": "1-Interstate",
    "2": "2-Freeway/Expressway",
    "3": "3-Principal Arterial",
    "4": "4-Minor Arterial",
    "5": "5-Major Collector",
    "6": "6-Minor Collector",
    "7": "7-Local",
    # Text-only fallback
    "Interstate":            "1-Interstate",
    "Other Freeways":        "2-Freeway/Expressway",
    "Other Principal":       "3-Principal Arterial",
    "Minor Arterial":        "4-Minor Arterial",
    "Major Collector":       "5-Major Collector",
    "Minor Collector":       "6-Minor Collector",
    "Local":                 "7-Local",
}

FC_TO_SYSTEM = {
    "1-Interstate":          "DOT Interstate",
    "2-Freeway/Expressway":  "DOT Primary",
    "3-Principal Arterial":  "DOT Primary",
    "4-Minor Arterial":      "DOT Secondary",
    "5-Major Collector":     "DOT Secondary",
    "6-Minor Collector":     "Non-DOT primary",
    "7-Local":               "Non-DOT secondary",
}

# Route sign → Ownership
ROUTE_SIGN_OWNERSHIP = {
    "Interstate":      "1. State Hwy Agency",
    "US Highway":      "1. State Hwy Agency",
    "State Highway":   "1. State Hwy Agency",
    "U.S.":            "1. State Hwy Agency",
    "State":           "1. State Hwy Agency",
    "I":               "1. State Hwy Agency",
    "US":              "1. State Hwy Agency",
    "SH":              "1. State Hwy Agency",
}

# Surface type codes (CDOT PRISURF)
SURFACE_MAP = {
    # Actual CDOT values (digit + spaces + text)
    "1    Asphalt":    "2. Blacktop, Asphalt, Bituminous",
    "2    Concrete":   "1. Concrete",
    "3    Composite":  "3. Composite",
    "4    Gravel":     "5. Gravel",
    "5    Dirt":       "6. Dirt",
    # Single digit
    "1":  "2. Blacktop, Asphalt, Bituminous",
    "2":  "1. Concrete",
    "3":  "3. Composite",
    "4":  "4. Brick, Block",
    "5":  "5. Gravel",
    "6":  "6. Dirt",
    "7":  "7. Other",
    # Text values
    "Concrete":        "1. Concrete",
    "Asphalt":         "2. Blacktop, Asphalt, Bituminous",
    "Bituminous":      "2. Blacktop, Asphalt, Bituminous",
    "Composite":       "3. Composite",
    "Gravel":          "5. Gravel",
    "Dirt":            "6. Dirt",
}

# CDOT Regions (5 engineering regions)
REGION_MAP = {
    "1": "Region 1 (Denver Metro)",
    "2": "Region 2 (Southeast)",
    "3": "Region 3 (Grand Junction)",
    "4": "Region 4 (Greeley)",
    "5": "Region 5 (Durango)",
}

# Terrain → Area Type fallback
TERRAIN_AREA = {
    "F": "Rural",        # Flat
    "R": "Rural",        # Rolling
    "M": "Rural",        # Mountainous
    "Flat": "Rural",
    "Rolling": "Suburban",
    "Mountainous": "Rural",
}


# ═══════════════════════════════════════════════════════════════
#  NORMALIZER
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """
    Normalize CDOT highway data → CrashLens columns.
    Colorado is simple — one endpoint has everything.
    No secondary downloads needed.
    """
    import numpy as np
    import pandas as pd

    n = len(df)
    print(f"  Normalizing {n:,} Colorado road segments...")

    # ── Case-insensitive column handling ──
    # CDOT sometimes returns lowercase (Socrata-backed) or UPPERCASE (native ArcGIS)
    col_map = {c.lower(): c for c in df.columns}

    def _get(name):
        """Get column by case-insensitive name."""
        lower = name.lower()
        if lower in col_map:
            return df[col_map[lower]].fillna("").astype(str).str.strip()
        return pd.Series("", index=df.index)

    def _get_numeric(name):
        """Get numeric column."""
        lower = name.lower()
        if lower in col_map:
            return pd.to_numeric(df[col_map[lower]], errors="coerce").fillna(0)
        return pd.Series(0, index=df.index)

    # ════════════════════════════════════════════════════
    #  1. FUNCTIONAL CLASS
    # ════════════════════════════════════════════════════

    fc_raw = _get("dot_func_class")
    df["Functional Class"] = fc_raw.map(FC_CODE_MAP).fillna("")

    # Fallback: extract leading digit
    empty_fc = df["Functional Class"] == ""
    if empty_fc.any():
        leading = fc_raw[empty_fc].str.extract(r'^(\d)', expand=False).fillna("")
        fc_from_digit = leading.map(FC_CODE_MAP).fillna("")
        df.loc[empty_fc & (fc_from_digit != ""), "Functional Class"] = (
            fc_from_digit[empty_fc & (fc_from_digit != "")]
        )
    fc_fill = (df["Functional Class"] != "").sum()
    pct = (fc_fill / n * 100) if n > 0 else 0
    print(f"    Functional Class: {fc_fill:,}/{n:,} ({pct:.1f}%)")

    # ════════════════════════════════════════════════════
    #  2. SYSTEM
    # ════════════════════════════════════════════════════
    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ════════════════════════════════════════════════════
    #  3. OWNERSHIP (from route sign — CDOT highways are all state-maintained)
    # ════════════════════════════════════════════════════

    sign = _get("dot_route_sign")
    df["Ownership"] = sign.map(ROUTE_SIGN_OWNERSHIP).fillna("")

    # Fallback: all CDOT highways are state-maintained
    empty_own = df["Ownership"] == ""
    if empty_own.any():
        df.loc[empty_own, "Ownership"] = "1. State Hwy Agency"
    print(f"    Ownership: {(df['Ownership'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  4. SPEED LIMIT (actual posted speed — not estimate!)
    # ════════════════════════════════════════════════════

    spd = _get_numeric("dot_speed_limit")
    df["Speed_Limit_Est"] = np.where(spd > 0, spd.astype(int).astype(str), "")
    spd_fill = (df["Speed_Limit_Est"] != "").sum()
    pct = (spd_fill / n * 100) if n > 0 else 0
    print(f"    Speed_Limit_Est: {spd_fill:,}/{n:,} ({pct:.1f}%) — ACTUAL posted speeds")

    # ════════════════════════════════════════════════════
    #  5. THROUGH LANES (actual count)
    # ════════════════════════════════════════════════════

    lanes = _get_numeric("dot_through_lanes")
    df["Through_Lanes"] = np.where(lanes > 0, lanes.astype(int).astype(str), "")
    print(f"    Through_Lanes: {(df['Through_Lanes'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  6. SURFACE TYPE
    # ════════════════════════════════════════════════════

    surf = _get("dot_surface")
    df["Roadway Surface Type"] = surf.map(SURFACE_MAP).fillna("")

    # Fallback for unmapped values
    empty_surf = df["Roadway Surface Type"] == ""
    if empty_surf.any():
        # Try partial matching
        for key, val in SURFACE_MAP.items():
            mask = empty_surf & surf.str.contains(key, case=False, na=False)
            df.loc[mask, "Roadway Surface Type"] = val
            empty_surf = df["Roadway Surface Type"] == ""

    # Final fallback: asphalt
    df.loc[df["Roadway Surface Type"] == "", "Roadway Surface Type"] = (
        "2. Blacktop, Asphalt, Bituminous"
    )
    print(f"    Roadway Surface Type: {(df['Roadway Surface Type'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  7. AREA TYPE (from POPULATION field — Urban/Rural designation)
    # ════════════════════════════════════════════════════

    pop = _get("dot_population")
    pop_upper = pop.str.upper()
    # CDOT POPULATION values: "Urban", "Small Urban", "Rural", ""
    df["Area Type"] = np.where(
        pop_upper.str.contains("URBAN", na=False) & ~pop_upper.str.contains("SMALL", na=False), "Urban",
        np.where(pop_upper.str.contains("SMALL URBAN", na=False), "Suburban",
        np.where(pop_upper.str.contains("RURAL", na=False), "Rural", "Rural"))
    )
    print(f"    Area Type: {(df['Area Type'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  8. DOT DISTRICT (from REGION)
    # ════════════════════════════════════════════════════

    region = _get("dot_region")
    df["DOT District"] = region.map(REGION_MAP).fillna("")
    # Fallback: use raw value
    empty_dist = df["DOT District"] == ""
    if empty_dist.any():
        df.loc[empty_dist & (region != ""), "DOT District"] = "Region " + region[empty_dist & (region != "")]
    print(f"    DOT District: {(df['DOT District'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  9. RTE NAME
    # ════════════════════════════════════════════════════

    route_num = _get("dot_route")
    sign = _get("dot_route_sign")
    alias = _get("dot_alias")

    def _build_rte_name(num, sign_val, alias_val):
        # Priority 1: alias if present ("US-50" → "US 50")
        if alias_val:
            import re
            cleaned = re.sub(r'^(I|US|SH|CO)-?\s*(\d+)', r'\1 \2', alias_val)
            return cleaned
        if not num or num == "0":
            return ""
        # Strip trailing letter suffix: "050A" → "050" → "50"
        import re
        num_digits = re.sub(r'[A-Za-z]+$', '', num)
        num_clean = num_digits.lstrip("0") or num_digits
        sign_upper = sign_val.upper().strip() if sign_val else ""
        if "INTERSTATE" in sign_upper:
            return f"I {num_clean}"
        elif "U.S." in sign_upper or "US" in sign_upper:
            return f"US {num_clean}"
        elif "STATE" in sign_upper or "SH" in sign_upper:
            return f"SH {num_clean}"
        else:
            return f"SH {num_clean}"

    df["RTE Name"] = [_build_rte_name(r, s, a) for r, s, a in zip(route_num, sign, alias)]
    print(f"    RTE Name: {(df['RTE Name'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  10. FACILITY TYPE (from ISDIVIDED)
    # ════════════════════════════════════════════════════

    divided = _get("dot_is_divided")
    fc = df["Functional Class"]
    is_major = fc.isin(["1-Interstate", "2-Freeway/Expressway"])
    is_divided = divided.str.upper().isin(["Y", "YES", "TRUE", "1"])

    df["Facility Type"] = "3-Two-Way Undivided"
    df.loc[is_divided & is_major, "Facility Type"] = "4-Two-Way Divided"
    df.loc[is_divided & ~is_major, "Facility Type"] = "4-Two-Way Divided"
    df.loc[~is_divided & is_major, "Facility Type"] = "4-Two-Way Divided"

    # ════════════════════════════════════════════════════
    #  11. COUNTY
    # ════════════════════════════════════════════════════

    df["Physical Juris Name"] = _get("dot_county")

    # ════════════════════════════════════════════════════
    #  12. MILEPOINTS + SEGMENT LENGTH
    # ════════════════════════════════════════════════════

    beg = _get_numeric("dot_beg_mp")
    end = _get_numeric("dot_end_mp")
    df["RNS MP"] = np.where(beg > 0, beg, 0)
    seg = _get_numeric("dot_seg_length")
    df["Segment_Length_mi"] = np.where(seg > 0, np.round(seg, 3), 0)

    # ════════════════════════════════════════════════════
    #  13. AADT
    # ════════════════════════════════════════════════════

    aadt = _get_numeric("dot_aadt")
    df["dot_aadt_value"] = np.where(aadt > 0, aadt, 0)

    # ════════════════════════════════════════════════════
    #  14. ROADWAY DESCRIPTION
    # ════════════════════════════════════════════════════

    desc_map = {
        "3-Two-Way Undivided":  "1. Two-Way, Not Divided",
        "4-Two-Way Divided":    "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = df["Facility Type"].map(desc_map).fillna(
        "1. Two-Way, Not Divided"
    )

    # ════════════════════════════════════════════════════
    #  SOURCE TRACKING
    # ════════════════════════════════════════════════════

    df["dot_source"] = "CDOT Open Data SDE Highways"
    df["dot_source_url"] = ENDPOINT_URL

    # ════════════════════════════════════════════════════
    #  SUMMARY
    # ════════════════════════════════════════════════════

    print(f"\n  Normalization complete — {n:,} segments:")
    for col in ["Functional Class", "Ownership", "Area Type", "DOT District",
                "RTE Name", "Speed_Limit_Est", "Through_Lanes", "Facility Type",
                "Roadway Surface Type", "Physical Juris Name"]:
        if col in df.columns:
            pop = (df[col].fillna("").astype(str).str.strip() != "").sum()
            pct = (pop / n * 100) if n > 0 else 0
            print(f"    {col:25s}: {pop:>6,}/{n:,} ({pct:5.1f}%)")

    return df


# ═══════════════════════════════════════════════════════════════
#  STANDALONE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Colorado DOT Road Inventory Config (CDOT)")
    print(f"  Primary: {ENDPOINT_URL}")
    print(f"  Fields:  {len(FIELD_MAP)} mapped")
    print(f"  Note:    ONE endpoint has everything — speed, lanes, surface, AADT, FC")
    print(f"  Run via: python generate_state_dot_data.py --state co")
