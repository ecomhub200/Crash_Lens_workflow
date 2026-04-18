#!/usr/bin/env python3
"""
co_state_dot.py — Colorado DOT Road Inventory Downloader & Normalizer
=======================================================================
Single-file config for Colorado's state DOT road inventory.
Called by generate_state_dot_data.py (root).

Data Source: CDOT CPLAN open_data_sde FeatureServer — Layer 7 (Highways)
  https://dtdapps.coloradodot.info/arcgis/rest/services/
    CPLAN/open_data_sde/FeatureServer/7

CDOT's "Highways" layer is the CDOT-maintained road inventory. Unlike DelDOT,
it's a single rich layer — posted speed limit, AADT, lane width, surface type,
median, shoulder, functional class, terrain, county, and route are all here,
no multi-layer joins required. Server native SR is 26913 (UTM Zone 13N); we
request outSR=4326 for WGS84. maxRecordCount is 1000.

Scope: CDOT-inventory only (Interstates, US highways, state highways). Does
NOT include county or municipal roads — those live in separate local inventories.

Columns map to CrashLens build_road_inventory.py first-22 architecture:
  Functional Class, SYSTEM, Ownership, Facility Type, Surface Type,
  Area Type, DOT District, RTE Name, Through_Lanes, Speed_Limit_Est, etc.

Usage:
  python generate_state_dot_data.py --state co
"""

# ═══════════════════════════════════════════════════════════════
#  ENDPOINT CONFIGURATION
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "co"
STATE_NAME = "Colorado"
STATE_FIPS = "08"
STATE_DOT  = "CDOT"

# ArcGIS FeatureServer endpoint — Layer 7 "Highways"
ENDPOINT_URL = (
    "https://dtdapps.coloradodot.info/arcgis/rest/services/"
    "CPLAN/open_data_sde/FeatureServer/7"
)

# ArcGIS pagination
MAX_RECORD_COUNT = 1000   # CDOT server's maxRecordCount
OUT_SR = 4326             # Request WGS84 lat/lon (server native is 26913 UTM Z13N)
GEOMETRY_TYPE = "polyline"

# None = request all fields. CDOT Layer 7 has exactly what we need, no bloat.
OUT_FIELDS = None


# ═══════════════════════════════════════════════════════════════
#  FIELD MAPPING: CDOT field → CrashLens column
# ═══════════════════════════════════════════════════════════════
# Keys = CDOT field name, Values = CrashLens target column.
# Fields not listed here are kept with "dot_raw_" prefix by the driver.

FIELD_MAP = {
    # ── Core road classification ──
    "FUNCCLASS":     "dot_fc_text",          # Text FC (e.g. "Rural Minor Arterial")
    "ROUTESIGN":     "dot_route_sign",       # I / US / SH
    "NHSDESIG":      "dot_nhs",              # National Highway System designation
    "ACCESS_":       "dot_access_control",   # Access control category

    # ── Road identification ──
    "ROUTE":         "dot_route_number",     # Route number (e.g. "025")
    "ALIAS":         "dot_road_name",        # Road alias / street name
    "DESCRIPTION":   "dot_description",      # Road description

    # ── Geography ──
    "COUNTY":        "dot_county_name",      # County name (text)
    "FIPSCOUNTY":    "dot_county_fips",      # County FIPS (3 digits)
    "CITY":          "dot_city",             # City name
    "FIPSCITY":      "dot_city_fips",        # City FIPS
    "REGION":        "dot_region_code",      # CDOT engineering region 1-5
    "TPRID":         "dot_tpr_id",           # Transportation Planning Region
    "TERRAIN":       "dot_terrain",          # Flat / Rolling / Mountainous

    # ── Road geometry/design ──
    "THRULNQTY":     "dot_lanes",            # Through lane count
    "THRULNWD":      "dot_lane_width",       # Through lane width (ft)
    "PRISURF":       "dot_surface_type",     # Primary surface type (text)
    "ISDIVIDED":     "dot_is_divided",       # Y/N divided highway flag
    "MEDIAN":        "dot_median_type",      # Median type (text)
    "MEDIANWD":      "dot_median_width",     # Median width
    "PRIOUTSHLD":    "dot_shoulder_type",    # Primary outside shoulder type
    "PRIOUTSHLDWD":  "dot_shoulder_width",   # Outside shoulder width

    # ── Traffic ──
    "AADT":          "dot_aadt",             # Annual average daily traffic
    "AADTSINGLE":    "dot_aadt_single",      # Single-unit truck AADT
    "AADTCOMB":      "dot_aadt_combo",       # Combination truck AADT
    "SPEEDLIM":      "dot_speed_limit",      # Posted speed limit (mph) — ACTUAL!
    "VMT":           "dot_vmt",              # Vehicle miles traveled
    "VCRATIO":       "dot_vc_ratio",         # Volume/capacity ratio

    # ── Milepoints ──
    "REFPT":         "dot_beg_mp",           # Begin milepoint
    "ENDREFPT":      "dot_end_mp",           # End milepoint
    "SEG_LENGTH":    "dot_seg_length",       # Segment length (miles)
}


# ═══════════════════════════════════════════════════════════════
#  VALUE TRANSFORMS: CDOT value → CrashLens standard value
# ═══════════════════════════════════════════════════════════════

# Functional Class: CDOT TEXT → CrashLens standard
# CDOT prefixes with "Rural "/"Urban " — collapse both to the same CrashLens class.
FC_TEXT_MAP = {
    "Rural Interstate":                      "1-Interstate",
    "Urban Interstate":                      "1-Interstate",
    "Rural Freeway and Expressway":          "2-Freeway/Expressway",
    "Urban Freeway and Expressway":          "2-Freeway/Expressway",
    "Rural Principal Arterial - Other":      "3-Principal Arterial",
    "Urban Principal Arterial - Other":      "3-Principal Arterial",
    "Rural Minor Arterial":                  "4-Minor Arterial",
    "Urban Minor Arterial":                  "4-Minor Arterial",
    "Rural Major Collector":                 "5-Major Collector",
    "Urban Major Collector":                 "5-Major Collector",
    "Rural Minor Collector":                 "6-Minor Collector",
    "Urban Minor Collector":                 "6-Minor Collector",
    "Rural Local":                           "7-Local",
    "Urban Local":                           "7-Local",
}

# FC → SYSTEM (CrashLens standard — identical to Delaware)
FC_TO_SYSTEM = {
    "1-Interstate":           "DOT Interstate",
    "2-Freeway/Expressway":   "DOT Primary",
    "3-Principal Arterial":   "DOT Primary",
    "4-Minor Arterial":       "DOT Secondary",
    "5-Major Collector":      "DOT Secondary",
    "6-Minor Collector":      "Non-DOT primary",
    "7-Local":                "Non-DOT secondary",
}

# CDOT engineering regions (1-5)
REGION_MAP = {
    "1": "Region 1 (Denver Metro)",
    "2": "Region 2 (Southeast — Pueblo/Colorado Springs)",
    "3": "Region 3 (Northwest — Grand Junction)",
    "4": "Region 4 (Northeast — Greeley)",
    "5": "Region 5 (Southwest — Durango)",
}

# Surface Type: CDOT TEXT → CrashLens standard.
# IMPORTANT: "Composite" = asphalt overlay, maps to Blacktop.
# Matches SURFACE_LABELS[7] fix in wiki/log.md [2026-04-18] — composite is NOT brick.
SURFACE_TYPE_MAP = {
    "Asphalt":   "2. Blacktop, Asphalt, Bituminous",
    "Bituminous":"2. Blacktop, Asphalt, Bituminous",
    "Concrete":  "1. Concrete",
    "Composite": "2. Blacktop, Asphalt, Bituminous",   # NOT brick — asphalt overlay
    "Gravel":    "4. Slag, Gravel, Stone",
    "Stone":     "4. Slag, Gravel, Stone",
    "Dirt":      "5. Dirt",
    "Unpaved":   "5. Dirt",
    "Other":     "6. Other",
}

# Route sign prefixes (passthrough)
ROUTE_SIGN_MAP = {
    "I":  "I",    # Interstate
    "US": "US",   # US Highway
    "SH": "SH",   # State Highway
}

# Terrain: CDOT text → CrashLens alignment standard
TERRAIN_MAP = {
    "Flat":        "1. Straight - Level",
    "Rolling":     "3. Grade - Straight",
    "Mountainous": "4. Grade - Curve",
}

# Divided flag → Facility Type
DIVIDED_MAP = {
    "Y": "4-Two-Way Divided",
    "N": "3-Two-Way Undivided",
}


# ═══════════════════════════════════════════════════════════════
#  NORMALIZER: Transform raw CDOT data → CrashLens columns
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """
    Normalize raw CDOT Highways inventory data to CrashLens column architecture.

    Called by generate_state_dot_data.py after field mapping.
    Input: DataFrame with dot_ prefixed columns from FIELD_MAP.
    Output: Same DataFrame with CrashLens standard columns added.

    Colorado specifics vs Delaware:
      - FUNCCLASS is text, not numeric — substring-match fallback for rare values
      - PRISURF is text, not code — "Composite" maps to Blacktop (not brick)
      - SPEEDLIM gives actual posted speed, not an FC-table estimate
      - ISDIVIDED is explicit Y/N, no median-code heuristic needed
      - Layer 7 is CDOT-maintained only → Ownership is always State Hwy Agency
    """
    import numpy as np
    import pandas as pd
    import re

    # ── Functional Class (text lookup + substring fallback) ──
    fc_txt = df["dot_fc_text"].fillna("").astype(str).str.strip()
    fc_std = fc_txt.map(FC_TEXT_MAP).fillna("")
    unmapped = fc_std == ""
    if unmapped.any():
        fallbacks = [
            ("Interstate",      "1-Interstate"),
            ("Freeway",         "2-Freeway/Expressway"),
            ("Expressway",      "2-Freeway/Expressway"),
            ("Principal",       "3-Principal Arterial"),
            ("Minor Arterial",  "4-Minor Arterial"),
            ("Major Collector", "5-Major Collector"),
            ("Minor Collector", "6-Minor Collector"),
            ("Collector",       "5-Major Collector"),
            ("Local",           "7-Local"),
        ]
        for keyword, std in fallbacks:
            hit = unmapped & fc_txt.str.contains(keyword, case=False, na=False)
            fc_std.loc[hit] = std
            unmapped = fc_std == ""
    df["Functional Class"] = fc_std

    # ── SYSTEM (derived from FC) ──
    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ── Ownership — Layer 7 is CDOT-inventory only ──
    df["Ownership"] = "1. State Hwy Agency"

    # ── Facility Type (explicit divided flag) ──
    div_raw = df["dot_is_divided"].fillna("").astype(str).str.strip().str.upper()
    df["Facility Type"] = div_raw.map(DIVIDED_MAP).fillna("3-Two-Way Undivided")

    # ── Roadway Surface Type (text lookup + substring fallback + paved override) ──
    surf_raw = df["dot_surface_type"].fillna("").astype(str).str.strip()
    surf_std = surf_raw.map(SURFACE_TYPE_MAP).fillna("")
    unmapped = surf_std == ""
    if unmapped.any():
        surf_low = surf_raw.str.lower()
        surf_std.loc[unmapped & surf_low.str.contains("asphalt|bitum|composite", na=False)] = \
            "2. Blacktop, Asphalt, Bituminous"
        surf_std.loc[(surf_std == "") & surf_low.str.contains("concrete", na=False)] = \
            "1. Concrete"
        surf_std.loc[(surf_std == "") & surf_low.str.contains("gravel|stone", na=False)] = \
            "4. Slag, Gravel, Stone"
        surf_std.loc[(surf_std == "") & surf_low.str.contains("dirt|unpaved", na=False)] = \
            "5. Dirt"
    surf_std = surf_std.replace("", "2. Blacktop, Asphalt, Bituminous")
    # Interstates/Freeways/Arterials are ALWAYS paved
    fc_major = df["Functional Class"].str.match(r"^[123]-", na=False)
    bad_surf = fc_major & surf_std.isin(["5. Dirt", "4. Slag, Gravel, Stone"])
    surf_std.loc[bad_surf] = "2. Blacktop, Asphalt, Bituminous"
    df["Roadway Surface Type"] = surf_std

    # ── Area Type (derive from FC text prefix: Rural/Urban) ──
    area = pd.Series("", index=df.index)
    area.loc[fc_txt.str.contains(r"^Urban\b", case=False, na=False)] = "Urban"
    area.loc[fc_txt.str.contains(r"^Rural\b", case=False, na=False)] = "Rural"
    # Fallback: default to Rural (Colorado is predominantly rural off the Front Range)
    area.loc[area == ""] = "Rural"
    df["Area Type"] = area

    # ── DOT District (CDOT engineering region) ──
    reg_raw = df["dot_region_code"].fillna("").astype(str).str.strip()
    # CDOT region may arrive as "1" or "1.0" from numeric fields — normalize
    reg_raw = reg_raw.str.replace(r"\.0+$", "", regex=True)
    df["DOT District"] = reg_raw.map(REGION_MAP).fillna("")

    # ── Physical Juris Name (county, already text) ──
    df["Physical Juris Name"] = df["dot_county_name"].fillna("").astype(str).str.strip()

    # ── Through_Lanes (cap at 12) ──
    lanes = pd.to_numeric(df["dot_lanes"], errors="coerce").fillna(0).astype(int)
    lanes = lanes.clip(upper=12)
    df["Through_Lanes"] = np.where(lanes > 0, lanes.astype(str), "")

    # ── RTE Name (sign + route number, strip leading zeros: "I" + "025" → "I 25") ──
    sign_raw = df["dot_route_sign"].fillna("").astype(str).str.strip().str.upper()
    num_raw  = df["dot_route_number"].fillna("").astype(str).str.strip()
    sign_std = sign_raw.map(ROUTE_SIGN_MAP).fillna(sign_raw)
    num_clean = num_raw.str.lstrip("0")
    # If stripping left nothing but the original was non-empty (e.g. "000"), keep "0"
    num_clean = np.where((num_clean == "") & (num_raw != ""), "0", num_clean)
    num_clean = pd.Series(num_clean, index=df.index)
    both = (sign_std != "") & (num_clean != "")
    df["RTE Name"] = np.where(both, sign_std + " " + num_clean, "")

    # ── RNS MP ──
    beg_mp = pd.to_numeric(df["dot_beg_mp"], errors="coerce").fillna(0)
    df["RNS MP"] = np.where(beg_mp >= 0, beg_mp, 0)

    # ── Segment_Length_mi (prefer SEG_LENGTH, fallback to end-begin) ──
    seg_len = pd.to_numeric(df["dot_seg_length"], errors="coerce").fillna(0)
    end_mp = pd.to_numeric(df["dot_end_mp"], errors="coerce").fillna(0)
    derived = (end_mp - beg_mp).clip(lower=0)
    seg_final = np.where(seg_len > 0, seg_len, derived)
    df["Segment_Length_mi"] = np.round(seg_final, 3)

    # ── Lane_Width_ft (CDOT reports directly) ──
    lw = pd.to_numeric(df["dot_lane_width"], errors="coerce").fillna(0)
    df["Lane_Width_ft"] = np.where(lw > 0, np.clip(lw, 8, 16), 0)

    # ── Median_Width_ft ──
    df["Median_Width_ft"] = pd.to_numeric(
        df["dot_median_width"], errors="coerce"
    ).fillna(0)

    # ── Shoulder_Width_ft (single value — CDOT reports outside shoulder only) ──
    df["Shoulder_Width_ft"] = pd.to_numeric(
        df["dot_shoulder_width"], errors="coerce"
    ).fillna(0)

    # ── Speed_Limit_Est — USE ACTUAL POSTED SPEED (CDOT's big win) ──
    spd_num = pd.to_numeric(df["dot_speed_limit"], errors="coerce").fillna(0).astype(int)
    df["Speed_Limit_Est"] = np.where(spd_num > 0, spd_num.astype(str), "")

    # ── Roadway Description (from Facility Type, same map as Delaware) ──
    desc_map = {
        "1-One-Way Undivided":  "4. One-Way, Not Divided",
        "2-One-Way Divided":    "4. One-Way, Not Divided",
        "3-Two-Way Undivided":  "1. Two-Way, Not Divided",
        "4-Two-Way Divided":    "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = df["Facility Type"].map(desc_map).fillna(
        "1. Two-Way, Not Divided"
    )

    # ── Is_NHS ──
    nhs_raw = df["dot_nhs"].fillna("").astype(str).str.strip()
    df["Is_NHS"] = np.where(
        (nhs_raw != "") & (nhs_raw.str.upper() != "N") & (nhs_raw != "0"),
        "Yes", "No"
    )

    # CDOT Layer 7 doesn't expose sidewalk / guardrail / bike-lane data.
    # Leave these empty rather than invent values — downstream enrichment
    # pulls them from other sources (HPMS, aerial imagery, etc.).
    df["Has_Sidewalk"] = ""
    df["Guardrail Related?"] = ""
    df["Has_Bike_Lane"] = ""

    # ── Source tracking ──
    df["dot_source"] = "CDOT Highways Inventory"
    df["dot_source_url"] = ENDPOINT_URL

    return df


# ═══════════════════════════════════════════════════════════════
#  STANDALONE (for testing)
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Colorado DOT Road Inventory Config")
    print(f"  Endpoint: {ENDPOINT_URL}")
    print(f"  Fields: {len(FIELD_MAP)} mapped")
    print(f"  Run via: python generate_state_dot_data.py --state co")
