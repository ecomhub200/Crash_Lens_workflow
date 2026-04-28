#!/usr/bin/env python3
"""
va_state_dot.py — Virginia DOT (VDOT) Road Inventory Downloader & Normalizer
===============================================================================
Single-file config for Virginia's state DOT road inventory.
Called by generate_state_dot_data.py (root).

Data Sources (2 VDOT ArcGIS FeatureServer layers from the same service):
  PRIMARY — Functional Classification Master Route (Layer 4):
    https://vdotgisuportal.vdot.virginia.gov/env/rest/services/
      VDOT_Map/Virginia_Tech_LRS_Routes/FeatureServer/4
    ~60,000 segments with functional class, route names, geometry
    Updated NIGHTLY from VDOT LRS

  SECONDARY — Responsibility Master Route (Layer 2):
    Same FeatureServer, Layer 2
    ~125,000 segments with ownership (RIM_MAINT_RESPONSIBILITY_NM)
    Updated WEEKLY

ENDPOINT STABILITY:
  Both endpoints use the same FeatureServer URL since 2020.
  Service Item ID: f59d97ee5be443b5a6548dc2d418f5cc
  VDOT releases quarterly LRS versions (25.1, 24.1) but the URL stays constant.
  The data behind the endpoint refreshes automatically — NO URL changes needed.
  Server version: 10.91, Spatial Reference: 4269 (NAD83)

Speed limits are ESTIMATED from FC + Area Type (Virginia Code §46.2-870).
Actual posted speeds come from HPMS (Tier B) and Mapillary (Tier C) in the pipeline.

FIELD NAMES VERIFIED: 2025-04 from endpoint ?f=json metadata.

Usage:
  python generate_state_dot_data.py --state va
  python generate_state_dot_data.py --state va --upload
  python generate_state_dot_data.py --state va --upload --force
"""

import traceback

# ═══════════════════════════════════════════════════════════════
#  ENDPOINT CONFIGURATION
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "va"
STATE_NAME = "Virginia"
STATE_FIPS = "51"
STATE_DOT = "VDOT"

# PRIMARY: Functional Classification Master Route (Layer 4)
# Verified fields (2025-04):
#   OBJECTID, EVENT_SOURCE_ID, RTE_COMMON_NM, RTE_FROM_MSR, RTE_TO_MSR,
#   CURRENCY_DATE, TMPD_FUNCTIONAL_CLASS_NM, TMPD_FUNCTIONAL_CLASS_CD,
#   TMPD_FUNCTIONAL_CLASS_DSC, EVENT_SOURCE_CD, EVENT_SOURCE_NM,
#   EVENT_LOCATION_ID, EVENT_COMPONENT_ID, RTE_NM, RTE_MEASURE_SYSTEM_CD,
#   LOCATION_VISIBILITY_CD, LOCATION_COMPONENT_STATUS_CD,
#   LOCATION_COMPONENT_STATUS_NAME, RTE_CATEGORY_NM, RTE_TYPE_NM,
#   RTE_RAMP_CD, LOC_COMP_DIRECTIONALITY_NM, LOC_COMP_DIRECTIONALITY_CD,
#   RTE_ID, LRM_CURRENCY_DT, CHANGE_STATUS_CD, CHANGE_STATUS_DT
ENDPOINT_URL = (
    "https://vdotgisuportal.vdot.virginia.gov/env/rest/services/"
    "VDOT_Map/Virginia_Tech_LRS_Routes/FeatureServer/4"
)

# SECONDARY: Responsibility Master Route (Layer 2)
# Has: RIM_MAINT_RESPONSIBILITY_NM, RTE_NM (for joining), updated weekly
RESPONSIBILITY_URL = (
    "https://vdotgisuportal.vdot.virginia.gov/env/rest/services/"
    "VDOT_Map/Virginia_Tech_LRS_Routes/FeatureServer/2"
)

# ArcGIS pagination — VDOT MaxRecordCount = 1000
MAX_RECORD_COUNT = 1000
OUT_SR = 4326             # Request WGS84 (server native is 4269 NAD83)
GEOMETRY_TYPE = "polyline"

# Request all fields — Layer 4 has only ~27 fields, manageable payload
OUT_FIELDS = None


# ═══════════════════════════════════════════════════════════════
#  FIELD MAPPING: VDOT Layer 4 field → CrashLens column
# ═══════════════════════════════════════════════════════════════
# VERIFIED against actual endpoint metadata (2025-04)
# Keys must match EXACTLY what the FeatureServer returns.

FIELD_MAP = {
    # ── Functional Classification ──
    "TMPD_FUNCTIONAL_CLASS_NM":      "dot_fc_name",           # e.g. "1-Interstate (A,1)"
    "TMPD_FUNCTIONAL_CLASS_CD":      "dot_fc_code",           # FC code
    "TMPD_FUNCTIONAL_CLASS_DSC":     "dot_fc_description",    # Full description

    # ── Route Identification ──
    "RTE_NM":                        "dot_route_name_full",   # LRS route ID (e.g. "I0064E000000")
    "RTE_COMMON_NM":                 "dot_common_name",       # Human name (e.g. "I-64")
    "RTE_CATEGORY_NM":               "dot_route_category",    # Interstate/Primary/Secondary/Urban
    "RTE_TYPE_NM":                   "dot_route_type",        # Interstate Route/U.S. Route/State Route
    "RTE_RAMP_CD":                   "dot_ramp_code",         # Ramp indicator
    "RTE_ID":                        "dot_route_id",          # Numeric route ID

    # ── Direction ──
    "LOC_COMP_DIRECTIONALITY_NM":    "dot_direction_name",    # Prime/Non-Prime
    "LOC_COMP_DIRECTIONALITY_CD":    "dot_direction_code",    # P/N

    # ── Location Status ──
    "LOCATION_COMPONENT_STATUS_NAME":"dot_status_name",       # Active/Proposed/Deleted
    "LOCATION_COMPONENT_STATUS_CD":  "dot_status_code",

    # ── Milepoints ──
    "RTE_FROM_MSR":                  "dot_beg_mp",            # Begin milepoint
    "RTE_TO_MSR":                    "dot_end_mp",            # End milepoint

    # ── Metadata ──
    "CURRENCY_DATE":                 "dot_currency_date",
    "LRM_CURRENCY_DT":              "dot_lrm_date",
    "CHANGE_STATUS_CD":              "dot_change_status",
    "CHANGE_STATUS_DT":              "dot_change_status_date",

    # ── Event IDs ──
    "EVENT_SOURCE_ID":               "dot_event_source_id",
    "EVENT_SOURCE_CD":               "dot_event_source_code",
    "EVENT_SOURCE_NM":               "dot_event_source_name",
    "EVENT_LOCATION_ID":             "dot_event_location_id",
    "EVENT_COMPONENT_ID":            "dot_event_component_id",

    # ── Other ──
    "RTE_MEASURE_SYSTEM_CD":         "dot_measure_system",
    "LOCATION_VISIBILITY_CD":        "dot_visibility_code",
}


# ═══════════════════════════════════════════════════════════════
#  VALUE TRANSFORMS
# ═══════════════════════════════════════════════════════════════

# FC: TMPD_FUNCTIONAL_CLASS_NM values → CrashLens standard
# These are the EXACT strings from the FeatureServer (verified 2025-04)
FC_NAME_MAP = {
    "1-Interstate (A,1)":                                        "1-Interstate",
    "2-Principal Arterial - Other Freeways and Expressways (B)": "2-Freeway/Expressway",
    "3-Principal Arterial - Other (E,2)":                        "3-Principal Arterial",
    "4-Minor Arterial (H,3)":                                    "4-Minor Arterial",
    "5-Major Collector (I,4)":                                   "5-Major Collector",
    "6-Minor Collector (5)":                                     "6-Minor Collector",
    "7-Local (J,6)":                                             "7-Local",
}

# Fallback: leading digit → CrashLens FC
FC_DIGIT_MAP = {
    "1": "1-Interstate",
    "2": "2-Freeway/Expressway",
    "3": "3-Principal Arterial",
    "4": "4-Minor Arterial",
    "5": "5-Major Collector",
    "6": "6-Minor Collector",
    "7": "7-Local",
}

# FC → SYSTEM
FC_TO_SYSTEM = {
    "1-Interstate":          "DOT Interstate",
    "2-Freeway/Expressway":  "DOT Primary",
    "3-Principal Arterial":  "DOT Primary",
    "4-Minor Arterial":      "DOT Secondary",
    "5-Major Collector":     "DOT Secondary",
    "6-Minor Collector":     "Non-DOT primary",
    "7-Local":               "Non-DOT secondary",
}

# Route Category (RTE_CATEGORY_NM) → Ownership fallback
# ACTUAL VALUES from endpoint: "Interstate", "Primary", "Secondary", "Urban Streets", "Frontage"
ROUTE_CATEGORY_OWNERSHIP = {
    "Interstate":     "1. State Hwy Agency",
    "Primary":        "1. State Hwy Agency",
    "Secondary":      "2. County Hwy Agency",       # VA secondaries are county-maintained
    "Urban":          "3. City or Town Hwy Agency",  # Partial match fallback
    "Urban Streets":  "3. City or Town Hwy Agency",  # Actual value from VDOT
    "Frontage":       "1. State Hwy Agency",
    "Ramp":           "1. State Hwy Agency",
}

# Responsibility layer: RIM_MAINT_RESPONSIBILITY_NM → CrashLens Ownership
# ACTUAL VALUES from endpoint (2025-04) have coded prefixes:
#   "01-State Hwy Agency (1,2)", "04-Municipal or City Hwy Agency (4* Verify)",
#   "66-National Park Service (D)", "60-Other Federal Agency (A)",
#   "70-Corps of Engineers (F)"
# Strategy: partial match on keywords — check most specific first
MAINT_RESPONSIBILITY_MAP = {
    # Exact coded values from VDOT (most common)
    "01-STATE HWY AGENCY":                  "1. State Hwy Agency",
    "02-COUNTY HWY AGENCY":                 "2. County Hwy Agency",
    "03-TOWN OR TOWNSHIP HWY AGENCY":       "3. City or Town Hwy Agency",
    "04-MUNICIPAL OR CITY HWY AGENCY":      "3. City or Town Hwy Agency",
    "60-OTHER FEDERAL AGENCY":              "4. Federal Roads",
    "66-NATIONAL PARK SERVICE":             "4. Federal Roads",
    "70-CORPS OF ENGINEERS":                "4. Federal Roads",
    "62-MILITARY RESERVATION":              "4. Federal Roads",
    "64-U.S. FOREST SERVICE":               "4. Federal Roads",
    "80-PRIVATE":                           "6. Private/Unknown Roads",
    "99-UNKNOWN":                           "6. Private/Unknown Roads",
    # Keyword fallbacks (for partial matching)
    "STATE":                                "1. State Hwy Agency",
    "VDOT":                                 "1. State Hwy Agency",
    "COUNTY":                               "2. County Hwy Agency",
    "CITY":                                 "3. City or Town Hwy Agency",
    "MUNICIPAL":                            "3. City or Town Hwy Agency",
    "TOWN":                                 "3. City or Town Hwy Agency",
    "FEDERAL":                              "4. Federal Roads",
    "NATIONAL PARK":                        "4. Federal Roads",
    "MILITARY":                             "4. Federal Roads",
    "FOREST SERVICE":                       "4. Federal Roads",
    "CORPS":                                "4. Federal Roads",
    "PRIVATE":                              "6. Private/Unknown Roads",
    "NOT MAINTAINED":                       "6. Private/Unknown Roads",
    "OTHER":                                "6. Private/Unknown Roads",
}

# Route prefix extraction from RTE_NM
# VDOT LRS format: "I0064E000000" → prefix="I", number=64
ROUTE_PREFIX_MAP = {
    "I":   "I",      # Interstate
    "US":  "US",     # U.S. Route
    "VA":  "VA",     # State Route (Virginia)
    "SR":  "SR",     # State Route (alternate)
    "SC":  "SC",     # Secondary
}

# VDOT's 9 construction districts → CrashLens DOT District
# (not available on Layer 4 — derived from Responsibility layer or spatial join)
DISTRICT_MAP = {
    "BRISTOL":           "Bristol District",
    "SALEM":             "Salem District",
    "LYNCHBURG":         "Lynchburg District",
    "RICHMOND":          "Richmond District",
    "HAMPTON ROADS":     "Hampton Roads District",
    "FREDERICKSBURG":    "Fredericksburg District",
    "CULPEPER":          "Culpeper District",
    "STAUNTON":          "Staunton District",
    "NOVA":              "Northern Virginia District",
    "NORTHERN VIRGINIA": "Northern Virginia District",
}

# Districts → default Area Type
DISTRICT_AREA_TYPE = {
    "Northern Virginia District": "Urban",
    "Hampton Roads District":     "Suburban",
    "Richmond District":          "Suburban",
    "Fredericksburg District":    "Suburban",
    "Salem District":             "Rural",
    "Lynchburg District":         "Rural",
    "Staunton District":          "Rural",
    "Bristol District":           "Rural",
    "Culpeper District":          "Rural",
}


# ═══════════════════════════════════════════════════════════════
#  SECONDARY DOWNLOAD HELPER
# ═══════════════════════════════════════════════════════════════

def _download_secondary_attributes(url, label, timeout=120, max_records=1000):
    """
    Download attributes (no geometry) from a secondary VDOT layer.
    Returns list of attribute dicts, or empty list on failure.
    Non-fatal — if it fails, normalize() falls back to estimates.
    """
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CrashLens/1.0",
        "Accept": "application/json",
    })

    query_url = f"{url}/query"

    # Get count
    try:
        resp = session.get(query_url, params={
            "where": "1=1", "returnCountOnly": "true", "f": "json"
        }, timeout=timeout)
        data = resp.json()
        total = data.get("count", 0)
        if total == 0:
            print(f"  [{label}] No features — skipping")
            return []
        print(f"  [{label}] {total:,} features to download")
    except Exception as e:
        print(f"  [{label}] Count failed: {e} — skipping")
        return []

    # Paginated download (attributes only, no geometry)
    all_attrs = []
    offset = 0
    batch_num = 0
    while offset < total:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultOffset": str(offset),
            "resultRecordCount": str(max_records),
            "f": "json",
        }
        try:
            resp = session.get(query_url, params=params, timeout=timeout)
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break
            for f in features:
                all_attrs.append(f.get("attributes", {}))
            offset += len(features)
            batch_num += 1
            if batch_num % 20 == 0:
                print(f"    [{label}] {len(all_attrs):,}/{total:,}...")
        except Exception as e:
            print(f"    [{label}] Batch at offset {offset} failed: {e}")
            break

    print(f"  [{label}] Downloaded {len(all_attrs):,} records")
    return all_attrs


# ═══════════════════════════════════════════════════════════════
#  NORMALIZER
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """
    Normalize VDOT Functional Class Master Route → CrashLens columns.

    Steps:
      1. Filter to Active segments only
      2. Map FC from TMPD_FUNCTIONAL_CLASS_NM (exact string match)
      3. Download Responsibility layer → merge ownership by RTE_NM
      4. Parse route names from RTE_COMMON_NM / RTE_NM
      5. Derive remaining: Facility Type, Area Type, Speed estimate
      6. Fall back to FC-based estimates where secondary data unavailable
    """
    import numpy as np
    import pandas as pd
    import re

    n = len(df)
    print(f"  Normalizing {n:,} Virginia road segments...")

    # ════════════════════════════════════════════════════
    #  FILTER: Keep only Active segments
    # ════════════════════════════════════════════════════

    status = df.get("dot_status_name", pd.Series("", index=df.index))
    status = status.fillna("").astype(str).str.strip().str.upper()
    active_mask = (status == "") | (status.str.contains("ACTIVE", na=False))
    removed = (~active_mask).sum()
    if removed > 0:
        print(f"    Filtered: {removed:,} non-active segments removed")
        df = df[active_mask].reset_index(drop=True)
        n = len(df)
        print(f"    Remaining: {n:,} active segments")

    # Tag ramps (keep them, but mark for downstream)
    ramp = df.get("dot_ramp_code", pd.Series("", index=df.index))
    ramp = ramp.fillna("").astype(str).str.strip()
    df["dot_is_ramp"] = np.where(
        (ramp != "") & (ramp != "0") & (ramp.str.upper() != "N"), "Y", "N"
    )

    # ════════════════════════════════════════════════════
    #  1. FUNCTIONAL CLASS
    # ════════════════════════════════════════════════════

    fc_raw = df.get("dot_fc_name", pd.Series("", index=df.index))
    fc_raw = fc_raw.fillna("").astype(str).str.strip()

    # Exact match on TMPD_FUNCTIONAL_CLASS_NM values (verified strings)
    df["Functional Class"] = fc_raw.map(FC_NAME_MAP).fillna("")

    # Fallback: extract leading digit
    empty_fc = df["Functional Class"] == ""
    if empty_fc.any():
        leading = fc_raw[empty_fc].str.extract(r'^(\d)', expand=False).fillna("")
        fc_from_digit = leading.map(FC_DIGIT_MAP).fillna("")
        df.loc[empty_fc & (fc_from_digit != ""), "Functional Class"] = (
            fc_from_digit[empty_fc & (fc_from_digit != "")]
        )

    # Fallback: try code field
    empty_fc = df["Functional Class"] == ""
    if empty_fc.any():
        fc_code = df.get("dot_fc_code", pd.Series("", index=df.index))
        fc_code = fc_code.fillna("").astype(str).str.strip()
        fc_from_code = fc_code.map(FC_DIGIT_MAP).fillna("")
        df.loc[empty_fc & (fc_from_code != ""), "Functional Class"] = (
            fc_from_code[empty_fc & (fc_from_code != "")]
        )

    fc_fill = (df["Functional Class"] != "").sum()
    print(f"    Functional Class: {fc_fill:,}/{n:,} ({fc_fill/n*100:.1f}%)")

    # ════════════════════════════════════════════════════
    #  2. SYSTEM
    # ════════════════════════════════════════════════════

    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ════════════════════════════════════════════════════
    #  3. OWNERSHIP (from Responsibility layer + fallbacks)
    # ════════════════════════════════════════════════════

    df["Ownership"] = ""

    # Try downloading Responsibility data (non-fatal)
    try:
        resp_attrs = _download_secondary_attributes(
            RESPONSIBILITY_URL, "Responsibility", timeout=120
        )
        if resp_attrs:
            resp_df = pd.DataFrame(resp_attrs)
            cols = list(resp_df.columns)
            print(f"    Responsibility columns: {cols[:8]}...")

            # Find maintenance responsibility column
            maint_col = None
            for c in ["RIM_MAINT_RESPONSIBILITY_NM", "MAINT_RESPONSIBILITY_NM",
                       "MAINTENANCE_JURISDICTION_NM"]:
                if c in cols:
                    maint_col = c
                    break

            if maint_col and "RTE_NM" in cols:
                # Map responsibility values
                # Actual format: "04-Municipal or City Hwy Agency (4* Verify)"
                # Strategy: strip parenthetical, uppercase, try exact then partial match
                resp_df["_own"] = (resp_df[maint_col].fillna("")
                                   .astype(str).str.strip())
                # Strip parenthetical suffix: "(4* Verify)", "(1,2)", "(D)"
                resp_df["_own_clean"] = resp_df["_own"].str.replace(
                    r'\s*\(.*?\)\s*$', '', regex=True
                ).str.strip().str.upper()

                # Try exact match first
                resp_df["_mapped"] = resp_df["_own_clean"].map(MAINT_RESPONSIBILITY_MAP)

                # Partial match fallback for remaining
                unmapped = resp_df["_mapped"].isna()
                if unmapped.any():
                    for key, val in MAINT_RESPONSIBILITY_MAP.items():
                        mask = unmapped & resp_df["_own_clean"].str.contains(key, na=False)
                        resp_df.loc[mask, "_mapped"] = val
                        unmapped = resp_df["_mapped"].isna()

                resp_df["_mapped"] = resp_df["_mapped"].fillna("6. Private/Unknown Roads")

                # Build route → ownership lookup (most common per route)
                lookup = (resp_df.groupby("RTE_NM")["_mapped"]
                          .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else "")
                          .to_dict())

                # Join via dot_route_name_full (= RTE_NM from primary)
                if "dot_route_name_full" in df.columns:
                    df["Ownership"] = df["dot_route_name_full"].map(lookup).fillna("")
                    own_fill = (df["Ownership"] != "").sum()
                    print(f"    Ownership from Responsibility: {own_fill:,}/{n:,} ({own_fill/n*100:.1f}%)")
            else:
                print(f"    Responsibility: columns not matched (maint={maint_col})")

    except Exception as e:
        print(f"    Responsibility download failed (non-fatal): {e}")

    # Fallback: Route Category → Ownership
    empty_own = df["Ownership"] == ""
    if empty_own.any():
        cat = df.get("dot_route_category", pd.Series("", index=df.index))
        cat = cat.fillna("").astype(str).str.strip()
        cat_own = cat.map(ROUTE_CATEGORY_OWNERSHIP).fillna("")
        df.loc[empty_own & (cat_own != ""), "Ownership"] = cat_own[empty_own & (cat_own != "")]

    # Final fallback: FC → Ownership
    empty_own = df["Ownership"] == ""
    if empty_own.any():
        fc_own = {
            "1-Interstate":          "1. State Hwy Agency",
            "2-Freeway/Expressway":  "1. State Hwy Agency",
            "3-Principal Arterial":  "1. State Hwy Agency",
            "4-Minor Arterial":      "1. State Hwy Agency",
            "5-Major Collector":     "2. County Hwy Agency",
            "6-Minor Collector":     "2. County Hwy Agency",
            "7-Local":               "3. City or Town Hwy Agency",
        }
        df.loc[empty_own, "Ownership"] = (
            df.loc[empty_own, "Functional Class"].map(fc_own).fillna("")
        )
    print(f"    Ownership final: {(df['Ownership'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  4. RTE NAME
    # ════════════════════════════════════════════════════

    common = df.get("dot_common_name", pd.Series("", index=df.index))
    common = common.fillna("").astype(str).str.strip()
    rte_full = df.get("dot_route_name_full", pd.Series("", index=df.index))
    rte_full = rte_full.fillna("").astype(str).str.strip()

    def _parse_rte_name(common_nm, full_nm):
        # Priority 1: common name ("A ST (NP - Town of Strasburg)" → "A ST")
        if common_nm:
            # Strip parenthetical city info
            cleaned = re.sub(r'\s*\(.*?\)\s*$', '', common_nm).strip()
            # Normalize route prefixes: "I-64" → "I 64"
            cleaned = re.sub(r'^(I|US|VA|SR|SC)-?\s*(\d+)', r'\1 \2', cleaned)
            if cleaned:
                return cleaned
        # Priority 2: parse from RTE_NM
        # VDOT formats:
        #   "G-VA011US00030PR" → "US 30"
        #   "S-VA306NP A ST"  → "A ST" (street name after direction code)
        #   "R-VA122UR00006NB" → "UR 6"
        if full_nm:
            # Format: {type}-VA{fips}{route_type}{route_num}{dir} {name}
            # Try extracting route type + number
            m = re.match(r'^[A-Z]-VA\d{3}(I|US|VA|SR|SC|UR)(\d{4,5})', full_nm)
            if m:
                rtype = m.group(1)
                rnum = int(m.group(2))
                if rnum > 0:
                    prefix = ROUTE_PREFIX_MAP.get(rtype, rtype)
                    return f"{prefix} {rnum}"
            # Try extracting street name after direction code
            m2 = re.match(r'^[A-Z]-VA\d{3}[A-Z]{2}\s+(.+)$', full_nm)
            if m2:
                street = m2.group(1).strip()
                if street and not re.match(r'^\d+$', street):
                    return street
        return ""

    df["RTE Name"] = [_parse_rte_name(c, f) for c, f in zip(common, rte_full)]
    print(f"    RTE Name: {(df['RTE Name'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  5. FACILITY TYPE
    # ════════════════════════════════════════════════════

    dir_name = df.get("dot_direction_name", pd.Series("", index=df.index))
    dir_name = dir_name.fillna("").astype(str).str.strip().str.upper()
    dir_code = df.get("dot_direction_code", pd.Series("", index=df.index))
    dir_code = dir_code.fillna("").astype(str).str.strip().str.upper()

    is_directional = (dir_name == "NON-PRIME") | (dir_code == "N")
    fc = df["Functional Class"]
    is_major = fc.isin(["1-Interstate", "2-Freeway/Expressway"])

    df["Facility Type"] = "3-Two-Way Undivided"
    df.loc[is_directional & is_major, "Facility Type"] = "2-One-Way Divided"
    df.loc[is_directional & ~is_major, "Facility Type"] = "1-One-Way Undivided"
    df.loc[~is_directional & is_major, "Facility Type"] = "4-Two-Way Divided"

    # ════════════════════════════════════════════════════
    #  6. AREA TYPE
    # ════════════════════════════════════════════════════

    cat = df.get("dot_route_category", pd.Series("", index=df.index))
    cat = cat.fillna("").astype(str).str.strip()
    # "Urban Streets" = city-maintained = urban by definition
    df["Area Type"] = np.where(
        cat.str.contains("Urban", case=False, na=False), "Urban", "Rural"
    )
    # Secondary routes are typically rural in VA
    df.loc[cat == "Secondary", "Area Type"] = "Rural"
    # Primary routes: mix — default suburban
    df.loc[cat == "Primary", "Area Type"] = "Suburban"

    # ════════════════════════════════════════════════════
    #  7. DOT DISTRICT (not in Layer 4)
    # ════════════════════════════════════════════════════

    # District is NOT available on Layer 4. Will be enriched by spatial join
    # in build_road_inventory.py using VDOT Administrative Boundaries.
    df["DOT District"] = ""

    # ════════════════════════════════════════════════════
    #  8. SPEED LIMIT ESTIMATE (Virginia Code §46.2-870)
    # ════════════════════════════════════════════════════

    fc = df["Functional Class"]
    area = df["Area Type"]
    spd = pd.Series("", index=df.index)

    spd = np.where((fc == "1-Interstate") & (area == "Urban"), "65", spd)
    spd = np.where((fc == "1-Interstate") & (area != "Urban"), "70", spd)
    spd = np.where(fc == "2-Freeway/Expressway", "55", spd)
    spd = np.where((fc == "3-Principal Arterial") & (area == "Urban"), "35", spd)
    spd = np.where((fc == "3-Principal Arterial") & (area == "Suburban"), "45", spd)
    spd = np.where((fc == "3-Principal Arterial") & (area == "Rural"), "55", spd)
    spd = np.where((fc == "4-Minor Arterial") & (area == "Urban"), "35", spd)
    spd = np.where((fc == "4-Minor Arterial") & (area == "Suburban"), "45", spd)
    spd = np.where((fc == "4-Minor Arterial") & (area == "Rural"), "55", spd)
    spd = np.where((fc == "5-Major Collector") & (area == "Urban"), "25", spd)
    spd = np.where((fc == "5-Major Collector") & (area != "Urban"), "45", spd)
    spd = np.where((fc == "6-Minor Collector") & (area == "Urban"), "25", spd)
    spd = np.where((fc == "6-Minor Collector") & (area != "Urban"), "35", spd)
    spd = np.where((fc == "7-Local") & (area == "Urban"), "25", spd)
    spd = np.where((fc == "7-Local") & (area != "Urban"), "55", spd)
    df["Speed_Limit_Est"] = spd
    print(f"    Speed_Limit_Est: {(df['Speed_Limit_Est'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  9. SURFACE TYPE (default — actual from HPMS)
    # ════════════════════════════════════════════════════

    df["Roadway Surface Type"] = np.where(
        fc.isin(["1-Interstate", "2-Freeway/Expressway"]),
        "1. Concrete",
        "2. Blacktop, Asphalt, Bituminous"
    )

    # ════════════════════════════════════════════════════
    #  10. THROUGH LANES (estimate — actual from HPMS)
    # ════════════════════════════════════════════════════

    fc_lanes = {
        "1-Interstate": "4", "2-Freeway/Expressway": "4",
        "3-Principal Arterial": "2", "4-Minor Arterial": "2",
        "5-Major Collector": "2", "6-Minor Collector": "2", "7-Local": "2",
    }
    df["Through_Lanes"] = df["Functional Class"].map(fc_lanes).fillna("")

    # ════════════════════════════════════════════════════
    #  11. MILEPOINTS + SEGMENT LENGTH
    # ════════════════════════════════════════════════════

    beg_mp = pd.to_numeric(df.get("dot_beg_mp", 0), errors="coerce").fillna(0)
    end_mp = pd.to_numeric(df.get("dot_end_mp", 0), errors="coerce").fillna(0)
    df["RNS MP"] = np.where(beg_mp > 0, beg_mp, 0)
    seg_len = (end_mp - beg_mp).clip(lower=0)
    df["Segment_Length_mi"] = np.where(seg_len > 0, np.round(seg_len, 3), 0)

    # ════════════════════════════════════════════════════
    #  12. ROADWAY DESCRIPTION
    # ════════════════════════════════════════════════════

    desc_map = {
        "1-One-Way Undivided":  "4. One-Way, Not Divided",
        "2-One-Way Divided":    "4. One-Way, Not Divided",
        "3-Two-Way Undivided":  "1. Two-Way, Not Divided",
        "4-Two-Way Divided":    "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = df["Facility Type"].map(desc_map).fillna(
        "1. Two-Way, Not Divided"
    )

    # ════════════════════════════════════════════════════
    #  13. PHYSICAL JURIS NAME (empty — spatial join later)
    # ════════════════════════════════════════════════════

    df["Physical Juris Name"] = ""

    # ════════════════════════════════════════════════════
    #  SOURCE TRACKING
    # ════════════════════════════════════════════════════

    df["dot_source"] = "VDOT LRS Functional Classification"
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
            print(f"    {col:25s}: {pop:>6,}/{n:,} ({pop/n*100:5.1f}%)")

    return df


# ═══════════════════════════════════════════════════════════════
#  STANDALONE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Virginia DOT Road Inventory Config (VDOT)")
    print(f"  Primary:   {ENDPOINT_URL}")
    print(f"  Secondary: {RESPONSIBILITY_URL}")
    print(f"  Fields:    {len(FIELD_MAP)} mapped (verified 2025-04)")
    print(f"  FC values: {list(FC_NAME_MAP.keys())[:3]}...")
    print(f"  Run via:   python generate_state_dot_data.py --state va")
