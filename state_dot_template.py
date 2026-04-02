#!/usr/bin/env python3
"""
{STATE_NAME} DOT Road Inventory Downloader & Normalizer

TEMPLATE — Copy this file to states/{state_name}/{abbr}_state_dot.py
and fill in the TODO sections for your state.

Called by generate_state_dot_data.py (root).

SETUP STEPS:

1. Copy this file: cp state_dot_template.py states/{state_name}/{abbr}_state_dot.py
1. Create __init__.py: touch states/{state_name}/__init__.py
1. Find your state's ArcGIS road inventory endpoint (see KNOWN_ENDPOINTS below)
1. Query the endpoint to discover field names: curl '{endpoint}?f=json'
1. Map fields to FIELD_MAP (source_field → dot_ target)
1. Map code values (FC, surface, ownership, etc.) to CrashLens standard
1. Test: python generate_state_dot_data.py --state {abbr}

FINDING YOUR STATE'S ENDPOINT:

1. Search: "{state} DOT road inventory ArcGIS FeatureServer"
1. Check: https://enterprise.firstmap.{state}.gov (or similar GIS portal)
1. Check: https://gis.dot.state.{abbr}.us/arcgis/rest/services/
1. Check: ArcGIS Hub pages for the state DOT
1. Test: curl '{endpoint}/query?where=1=1&returnCountOnly=true&f=json'

KNOWN STATE DOT ARCGIS ENDPOINTS (as of 2026):
┌──────────────┬──────────────────────────────────────────────────────────────────────┐
│ State        │ Endpoint URL                                                         │
├──────────────┼──────────────────────────────────────────────────────────────────────┤
│ Delaware     │ enterprise.firstmap.delaware.gov/…/DE_Roadways_Main/FS/2           │
│ Florida      │ gis.fdot.gov/arcgis/rest/services/sso/ssogis_flaris/FS              │
│ Massachusetts│ gis.massdot.state.ma.us/…/Roads/RoadInventoryYearEndFiles/FS/10    │
│ Texas        │ services.arcgis.com/KTcxiTD9dsQw4r7Z/…/TxDOT_Roadways/FS          │
│ Virginia     │ gis.vdot.virginia.gov (needs Playwright — WAF)                       │
│ Washington   │ data.wsdot.wa.gov/arcgis/rest/services/Shared/…                    │
└──────────────┴──────────────────────────────────────────────────────────────────────┘
FS = FeatureServer. Test each with ?f=json to verify it returns JSON.

Usage:
python generate_state_dot_data.py --state {abbr}
"""

# ═══════════════════════════════════════════════════════════════
# ENDPOINT CONFIGURATION — TODO: Fill in for your state
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "xx"               # TODO: Two-letter state abbreviation
STATE_NAME = "Template State"   # TODO: Full state name
STATE_FIPS = "00"               # TODO: Two-digit FIPS code
STATE_DOT = "XXDOT"             # TODO: State DOT abbreviation

# TODO: Set your state's ArcGIS endpoint URL
# Find by searching: "{state} DOT road inventory ArcGIS FeatureServer"
# Test with: curl '{url}/query?where=1=1&returnCountOnly=true&f=json'
ENDPOINT_URL = (
    "https://CHANGE_ME.gov/arcgis/rest/services/"
    "Transportation/CHANGE_ME/FeatureServer/0"
)

# ArcGIS pagination — check MaxRecordCount in the endpoint metadata (?f=json)
MAX_RECORD_COUNT = 2000
OUT_SR = 4326             # Request WGS84 (almost always correct)
GEOMETRY_TYPE = "polyline"

# TODO: Set to None to download ALL fields on first run, then trim to needed ones.
# After first successful download, inspect the columns and list only the ones you need.
OUT_FIELDS = None  # Start with None, then optimize

# ═══════════════════════════════════════════════════════════════
# FIELD MAPPING — TODO: Map your state's field names to CrashLens
# ═══════════════════════════════════════════════════════════════
#
# HOW TO DISCOVER FIELDS:
# curl '{ENDPOINT_URL}?f=json' | python -m json.tool | grep '"name"'
# This shows all field names. Then map each relevant one below.
#
# COMMON PATTERNS (varies by state):
# Lanes:     LANES_QTY, NUM_LANES, THRU_LANES, THROUGH_LANES
# Surface:   SRFC_TYPE_CODE, SURFACE_TYPE, PAVMNT_TYPE
# Width:     RDWAY_WDTH, ROADWAY_WIDTH, RD_WIDTH
# Speed:     SPEED_LIMIT, SPD_LMT, POSTED_SPEED
# FC:        FNCTNL_CLASS, FUNC_CLASS, F_SYSTEM
# Ownership: ROW_AUTHORITY, MAINT_RSP, OWNER_CODE, JURIS
# Route:     ROUTE_NO, RTE_NM, ROUTE_NAME, SIGN_ROUTE
# Shoulder:  LSHLDR_WDTH, RSHLDR_WDTH, SHOULDER_WIDTH
# Median:    MEDIAN_CODE, MEDIAN_TYPE, MEDIAN_WDTH
# District:  DISTRICT_ID, DISTRICT, DOT_DISTRICT
# County:    COUNTY_CODE, COUNTY, CNTY_CODE

FIELD_MAP = {
    # ── Core road classification ──
    # TODO: Find your state's field names for these
    # "YOUR_FC_FIELD":          "dot_fc_code",
    # "YOUR_OWNERSHIP_FIELD":   "dot_ownership_code",
    # "YOUR_MAINT_RSP_FIELD":   "dot_maint_rsp_code",
    # "YOUR_TRAFFIC_DIR_FIELD": "dot_traffic_dir",
    # "YOUR_AREA_TYPE_FIELD":   "dot_area_type_code",

    # ── Road identification ──
    # "YOUR_ROAD_NAME_FIELD":   "dot_road_name",
    # "YOUR_ROAD_NUMBER_FIELD": "dot_road_number",
    # "YOUR_ROUTE_NUMBER_FIELD":"dot_route_number",
    # "YOUR_ROUTE_TYPE_FIELD":  "dot_route_type",

    # ── Geography ──
    # "YOUR_COUNTY_FIELD":      "dot_county_code",
    # "YOUR_DISTRICT_FIELD":    "dot_district_id",
    # "YOUR_MUNICIPALITY_FIELD":"dot_municipality_code",

    # ── Road geometry/design ──
    # "YOUR_LANES_FIELD":       "dot_lanes",
    # "YOUR_SURFACE_FIELD":     "dot_surface_type_code",
    # "YOUR_LANE_WIDTH_FIELD":  "dot_lane_width",
    # "YOUR_ROAD_WIDTH_FIELD":  "dot_roadway_width_ft",
    # "YOUR_SURFACE_WIDTH_FIELD":"dot_surface_width_ft",
    # "YOUR_MEDIAN_CODE_FIELD": "dot_median_code",
    # "YOUR_MEDIAN_WIDTH_FIELD":"dot_median_width_ft",
    # "YOUR_L_SHOULDER_CODE":   "dot_lshldr_code",
    # "YOUR_L_SHOULDER_WIDTH":  "dot_lshldr_width_ft",
    # "YOUR_R_SHOULDER_CODE":   "dot_rshldr_code",
    # "YOUR_R_SHOULDER_WIDTH":  "dot_rshldr_width_ft",

    # ── Safety features ──
    # "YOUR_L_GUARDRAIL_FIELD": "dot_lguardrail",
    # "YOUR_R_GUARDRAIL_FIELD": "dot_rguardrail",
    # "YOUR_L_SIDEWALK_FIELD":  "dot_lsidewalk",
    # "YOUR_R_SIDEWALK_FIELD":  "dot_rsidewalk",

    # ── Milepoints ──
    # "YOUR_BEGIN_MP_FIELD":    "dot_beg_mp",
    # "YOUR_END_MP_FIELD":      "dot_end_mp",

    # ── Speed (if available — not all states have this in road inventory) ──
    # "YOUR_SPEED_LIMIT_FIELD": "dot_speed_limit",
}

# ═══════════════════════════════════════════════════════════════
# VALUE TRANSFORMS — TODO: Map your state's codes to CrashLens
# ═══════════════════════════════════════════════════════════════

# Functional Class — UNIVERSAL (HPMS standard, same for all states)
# Only change if your state uses non-standard codes
FC_CODE_MAP = {
    "1": "1-Interstate",
    "2": "2-Freeway/Expressway",
    "3": "3-Principal Arterial",
    "4": "4-Minor Arterial",
    "5": "5-Major Collector",
    "6": "6-Minor Collector",
    "7": "7-Local",
}

# FC → SYSTEM — UNIVERSAL (CrashLens standard, don't change)
FC_TO_SYSTEM = {
    "1-Interstate": "DOT Interstate",
    "2-Freeway/Expressway": "DOT Primary",
    "3-Principal Arterial": "DOT Primary",
    "4-Minor Arterial": "DOT Secondary",
    "5-Major Collector": "DOT Secondary",
    "6-Minor Collector": "Non-DOT primary",
    "7-Local": "Non-DOT secondary",
}

# TODO: Ownership codes — check your state's field values
# Run first download with OUT_FIELDS=None, then:
# df["YOUR_OWNERSHIP_FIELD"].value_counts()
OWNERSHIP_CODE_MAP = {
    # "S": "1. State Hwy Agency",
    # "C": "2. County Hwy Agency",
    # "M": "3. City or Town Hwy Agency",
}

# TODO: Maintenance Responsibility codes (if available — gives REAL ownership)
MAINT_RSP_MAP = {
    # "D": "1. State Hwy Agency",      # DOT maintains
    # "M": "3. City or Town Hwy Agency", # Municipal
}

# TODO: Surface Type codes — check your state's values
# HPMS standard: 1=Concrete, 2=Asphalt, 3=Composite, 4=Gravel, 5=Dirt
# But some states use different numbering!
SURFACE_TYPE_MAP = {
    "1": "1. Concrete",
    "2": "2. Blacktop, Asphalt, Bituminous",
    "3": "2. Blacktop, Asphalt, Bituminous",  # Composite = asphalt surface
    "4": "4. Slag, Gravel, Stone",
    "5": "5. Dirt",
    "9": "6. Other",
}

# TODO: Area Type codes
AREA_TYPE_MAP = {
    "1": "Urban",
    "2": "Rural",
    "3": "Suburban",
    "U": "Urban",
    "R": "Rural",
    "S": "Suburban",
}

# TODO: Traffic Direction codes → Facility Type
TRAFFIC_DIR_MAP = {
    "1": "3-Two-Way Undivided",
    "2": "1-One-Way Undivided",
    "3": "4-Two-Way Divided",
    "5": "3-Two-Way Undivided",
    "B": "3-Two-Way Undivided",
}

# TODO: County codes → names (state-specific)
COUNTY_MAP = {
    # "001": "County Name",
}

# TODO: DOT District codes → names (state-specific)
DISTRICT_MAP = {
    # "1": "District Name",
}

# ═══════════════════════════════════════════════════════════════
# NORMALIZER — Mostly reusable, tweak per state as needed
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """
    Normalize raw state DOT data to CrashLens column architecture.

    This function is ~90% identical across states. The main differences are:
      - Which dot_ columns exist (depends on FIELD_MAP)
      - Code value mappings (depends on maps above)
      - State-specific quirks (ramp naming, divided hwy conventions, etc.)

    Columns produced (CrashLens standard):
      Functional Class, SYSTEM, Ownership, Facility Type,
      Roadway Surface Type, Area Type, DOT District,
      Physical Juris Name, RTE Name, Through_Lanes,
      Lane_Width_ft, Median_Width_ft, Shoulder_Width_ft,
      Has_Sidewalk, Has_Bike_Lane, Guardrail Related?,
      Roadway Description, RNS MP
    """
    import numpy as np
    import pandas as pd
    import re

    n = len(df)

    def _col(name, default=""):
        """Safely get a column, returning default Series if missing."""
        if name in df.columns:
            return df[name].fillna("").astype(str).str.strip()
        return pd.Series(default, index=df.index)

    def _num(name, default=0):
        """Safely get numeric column."""
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce").fillna(default)
        return pd.Series(default, index=df.index, dtype=float)

    # ══════════════════════════════════════════════════════════
    #  FUNCTIONAL CLASS — universal (HPMS codes 1-7)
    # ══════════════════════════════════════════════════════════
    fc_raw = _col("dot_fc_code")
    df["Functional Class"] = fc_raw.map(FC_CODE_MAP).fillna("")
    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ══════════════════════════════════════════════════════════
    #  OWNERSHIP — 3-tier: MAINT_RSP > ROW_AUTHORITY > FC fallback
    # ══════════════════════════════════════════════════════════
    # Tier 1: Maintenance Responsibility (real ownership)
    maint = _col("dot_maint_rsp_code")
    df["Ownership"] = maint.map(MAINT_RSP_MAP).fillna("")

    # Tier 2: ROW Authority code
    empty = df["Ownership"] == ""
    if empty.any():
        own = _col("dot_ownership_code")
        mapped = own.map(OWNERSHIP_CODE_MAP).fillna("")
        df.loc[empty & (mapped != ""), "Ownership"] = mapped[empty & (mapped != "")]

    # Tier 3: FC fallback
    empty = df["Ownership"] == ""
    if empty.any():
        fc_own = {
            "1-Interstate": "1. State Hwy Agency",
            "2-Freeway/Expressway": "1. State Hwy Agency",
            "3-Principal Arterial": "1. State Hwy Agency",
            "4-Minor Arterial": "1. State Hwy Agency",
            "5-Major Collector": "2. County Hwy Agency",
            "6-Minor Collector": "2. County Hwy Agency",
            "7-Local": "3. City or Town Hwy Agency",
        }
        df.loc[empty, "Ownership"] = df.loc[empty, "Functional Class"].map(fc_own).fillna("")

    # ══════════════════════════════════════════════════════════
    #  FACILITY TYPE — from traffic direction + median upgrade
    # ══════════════════════════════════════════════════════════
    dir_raw = _col("dot_traffic_dir")
    df["Facility Type"] = dir_raw.map(TRAFFIC_DIR_MAP).fillna("3-Two-Way Undivided")

    # Median upgrade (handles divided highways split into one-way segments)
    med_code = _col("dot_median_code")
    has_median = (med_code != "") & (med_code != "0") & (med_code != "N")

    oneway = df["Facility Type"].str.contains("One-Way", na=False)
    df.loc[has_median & oneway, "Facility Type"] = "2-One-Way Divided"

    twoway = df["Facility Type"].str.contains("Two-Way", na=False)
    df.loc[has_median & twoway, "Facility Type"] = "4-Two-Way Divided"

    # ══════════════════════════════════════════════════════════
    #  SURFACE TYPE — with FC sanity override
    # ══════════════════════════════════════════════════════════
    surf_raw = _col("dot_surface_type_code")
    df["Roadway Surface Type"] = surf_raw.map(SURFACE_TYPE_MAP).fillna(
        "2. Blacktop, Asphalt, Bituminous"
    )
    # Override: major roads are always paved
    fc_major = df["Functional Class"].str.match(r"^[123]-", na=False)
    bad_surf = fc_major & df["Roadway Surface Type"].isin(["5. Dirt", "4. Slag, Gravel, Stone"])
    df.loc[bad_surf, "Roadway Surface Type"] = "2. Blacktop, Asphalt, Bituminous"

    # ══════════════════════════════════════════════════════════
    #  AREA TYPE — with county fallback for unmapped codes
    # ══════════════════════════════════════════════════════════
    area_raw = _col("dot_area_type_code")
    df["Area Type"] = area_raw.map(AREA_TYPE_MAP).fillna("")

    empty_area = df["Area Type"] == ""
    if empty_area.any():
        # TODO: Customize county→area type mapping for your state
        df.loc[empty_area, "Area Type"] = "Rural"  # Safe default

    # ══════════════════════════════════════════════════════════
    #  GEOGRAPHY
    # ══════════════════════════════════════════════════════════
    dist_raw = _col("dot_district_id")
    df["DOT District"] = dist_raw.map(DISTRICT_MAP).fillna("")

    county_raw = _col("dot_county_code")
    df["Physical Juris Name"] = county_raw.map(COUNTY_MAP).fillna("")

    # ══════════════════════════════════════════════════════════
    #  THROUGH LANES — capped at 12
    # ══════════════════════════════════════════════════════════
    lanes = _num("dot_lanes").astype(int).clip(upper=12)
    df["Through_Lanes"] = np.where(lanes > 0, lanes.astype(str), "")

    # ══════════════════════════════════════════════════════════
    #  RTE NAME — route_number > route_type+number > road_name extraction
    # ══════════════════════════════════════════════════════════
    rte_raw = _col("dot_route_number")

    # Add space after letter prefix: "US13"→"US 13"
    def _add_space(val):
        if not val:
            return ""
        m = re.match(r'^([A-Za-z]+)(\d+.*)$', val)
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return val

    rte = rte_raw.apply(_add_space)

    # Extract route from ramp names
    road_name = _col("dot_road_name")
    empty_rte = rte == ""
    if empty_rte.any():
        def _extract_route(name):
            m = re.search(r'(?:TO\s+|FROM\s+)((?:I|US|SR|' + STATE_ABBR.upper() + r')\s*-?\s*\d+)',
                          name, re.IGNORECASE)
            if m:
                route = m.group(1).strip()
                route = re.sub(r'([A-Za-z]+)\s*-?\s*(\d+)', r'\1 \2', route)
                return route
            return ""
        rte.loc[empty_rte] = road_name[empty_rte].apply(_extract_route)

    df["RTE Name"] = rte

    # ══════════════════════════════════════════════════════════
    #  MILEPOINTS
    # ══════════════════════════════════════════════════════════
    beg_mp = _num("dot_beg_mp")
    df["RNS MP"] = np.where(beg_mp > 0, beg_mp, 0)

    # ══════════════════════════════════════════════════════════
    #  LANE WIDTH — actual > (surface-shoulders)/lanes > roadway/lanes
    # ══════════════════════════════════════════════════════════
    actual_lw = _num("dot_lane_width")
    surf_w = _num("dot_surface_width_ft")
    lsh = _num("dot_lshldr_width_ft")
    rsh = _num("dot_rshldr_width_ft")
    lanes_num = np.maximum(lanes, 1)

    travel_width = (surf_w - lsh - rsh).clip(lower=0)
    calc_lw = np.where(travel_width > 0, np.round(travel_width / lanes_num, 1), 0)

    rdway_w = _num("dot_roadway_width_ft")
    rough_lw = np.where(rdway_w > 0, np.round(rdway_w / lanes_num, 1), 0)

    df["Lane_Width_ft"] = np.where(actual_lw > 0, actual_lw,
                          np.where(calc_lw > 0, calc_lw, rough_lw))

    # ══════════════════════════════════════════════════════════
    #  MEDIAN, SHOULDER, SIDEWALK, GUARDRAIL, BIKE
    # ══════════════════════════════════════════════════════════
    df["Median_Width_ft"] = _num("dot_median_width_ft")

    df["Shoulder_Width_ft"] = np.where(
        (lsh > 0) | (rsh > 0),
        np.round((lsh + rsh) / np.where((lsh > 0) & (rsh > 0), 2, 1), 1), 0)

    lsw = _col("dot_lsidewalk")
    rsw = _col("dot_rsidewalk")
    has_sw = ((lsw != "") & (lsw != "0") & (lsw != "N")) | \
             ((rsw != "") & (rsw != "0") & (rsw != "N"))
    df["Has_Sidewalk"] = np.where(has_sw, "Yes", "No")

    lgr = _col("dot_lguardrail")
    rgr = _col("dot_rguardrail")
    has_gr = ((lgr != "") & (lgr != "0")) | ((rgr != "") & (rgr != "0"))
    df["Guardrail Related?"] = np.where(has_gr, "Yes", "No")

    bike = _col("dot_bike_path")
    df["Has_Bike_Lane"] = np.where((bike != "") & (bike != "0") & (bike != "N"), "Yes", "No")

    # ══════════════════════════════════════════════════════════
    #  ROADWAY DESCRIPTION (from Facility Type)
    # ══════════════════════════════════════════════════════════
    desc_map = {
        "1-One-Way Undivided": "4. One-Way, Not Divided",
        "2-One-Way Divided": "4. One-Way, Not Divided",
        "3-Two-Way Undivided": "1. Two-Way, Not Divided",
        "4-Two-Way Divided": "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = df["Facility Type"].map(desc_map).fillna("1. Two-Way, Not Divided")

    # ══════════════════════════════════════════════════════════
    #  SPEED LIMIT (if available in this state's data)
    # ══════════════════════════════════════════════════════════
    spd = _num("dot_speed_limit").astype(int)
    if (spd > 0).any():
        df["Max Speed Diff"] = np.where((spd >= 5) & (spd <= 85), spd, 0)

    # ══════════════════════════════════════════════════════════
    #  SOURCE TRACKING
    # ══════════════════════════════════════════════════════════
    df["dot_source"] = f"{STATE_DOT} Road Inventory"
    df["dot_source_url"] = ENDPOINT_URL

    return df


# ═══════════════════════════════════════════════════════════════
# STANDALONE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"{STATE_NAME} DOT Road Inventory Config")
    print(f"  Endpoint: {ENDPOINT_URL}")
    print(f"  Fields: {len(FIELD_MAP)} mapped")
    print(f"  Run via: python generate_state_dot_data.py --state {STATE_ABBR}")
