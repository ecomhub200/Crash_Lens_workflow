#!/usr/bin/env python3
"""
de_state_dot.py — Delaware DOT Road Inventory Downloader & Normalizer
=======================================================================
Single-file config for Delaware's state DOT road inventory.
Called by generate_state_dot_data.py (root).

Data Source: DelDOT Road Inventory via FirstMap Enterprise ArcGIS FeatureServer
  https://enterprise.firstmap.delaware.gov/arcgis/rest/services/
    Transportation/DE_Roadways_Main/FeatureServer/2

Contains ~14,000 segments covering all state-inventoried roads.
NOT every road — primarily state-maintained + major local roads.
Residential/subdivision roads may be missing.

Columns map to CrashLens build_road_inventory.py first-22 architecture:
  Functional Class, SYSTEM, Ownership, Facility Type, Surface Type,
  Area Type, DOT District, RTE Name, Through_Lanes, etc.

Usage:
  python generate_state_dot_data.py --state de
"""

# ═══════════════════════════════════════════════════════════════
#  ENDPOINT CONFIGURATION
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "de"
STATE_NAME = "Delaware"
STATE_FIPS = "10"
STATE_DOT = "DelDOT"

# ArcGIS FeatureServer endpoint
# NOTE: The old server (firstmap.gis.delaware.gov) returns HTML for all API
# requests due to a WAF/reverse proxy. The enterprise server works correctly.
ENDPOINT_URL = (
    "https://enterprise.firstmap.delaware.gov/arcgis/rest/services/"
    "Transportation/DE_Roadways_Main/FeatureServer/2"
)

# ArcGIS pagination
MAX_RECORD_COUNT = 2000   # Server's maxRecordCount
OUT_SR = 4326             # Request WGS84 lat/lon (server native is 102100 Web Mercator)
GEOMETRY_TYPE = "polyline"

# Fields to request from the server (reduces payload size)
# Set to None to request all fields
OUT_FIELDS = [
    "OBJECTID", "RDWAY_NAME", "ROAD_NO", "RD_TYPE", "ROUTE_NO",
    "ROUTE_TYPE_CODE", "COUNTY_CODE", "DISTRICT_ID", "MUNIC_CODE",
    "FNCTNL_CLASS_CODE", "HPMS_FNCTNL_CLASS", "SYSTEM_CLASS_CODE",
    "ROW_AUTHORITY_CODE", "LANES_QTY", "SRFC_TYPE_CODE",
    "SRFC_WDTH_QTY", "RDWAY_WDTH_QTY", "MEDIAN_CODE", "MEDIAN_WDTH_QTY",
    "LSHLDR_CODE", "LSHLDR_WDTH_QTY", "RSHLDR_CODE", "RSHLDR_WDTH_QTY",
    "LGRDRAIL_CODE", "RGRDRAIL_CODE", "LCURB_CODE", "RCURB_CODE",
    "LSDWLK_CODE", "RSDWLK_CODE", "TRAF_DIR_CODE", "RURAL_URBAN_CODE",
    "BEG_MP", "END_MP", "MAINT_AREA_ID", "HUNDREDS_CODE",
    "PRK_CODE", "SNOWPLAN_ROAD_CLASS", "BIKE_PATH_CODE",
    "INVNTRY_DIR_CODE", "ACCEPT_YEAR_CODE", "SURVEY_DATE",
    "ROW_WDTH_QTY",
    # v3 additions — high-value fields missed in v1
    "MAINT_RSP_CODE",     # Maintenance Responsibility → REAL Ownership
    "LANE_WDTH",          # Actual lane width (not calculated)
    "CONTROL_CODE",       # Control type (state/HPMS/traffic break)
    "GUTTER_CODE",        # Gutter presence
    "NHS_CODE",           # National Highway System
    "SRFC_COND_CODE",     # Surface condition
    "CONST_TYPE_CODE",    # Construction type
    "HSIP_CODE",          # Highway Safety Improvement Program
]


# ═══════════════════════════════════════════════════════════════
#  FIELD MAPPING: DelDOT field → CrashLens column
# ═══════════════════════════════════════════════════════════════
# Keys = DelDOT field name, Values = CrashLens target column
# Fields not listed here are kept with "dot_" prefix as extras.

FIELD_MAP = {
    # ── Core road classification ──
    "FNCTNL_CLASS_CODE":    "dot_fc_code",           # Numeric 1-7 → transformed later
    "HPMS_FNCTNL_CLASS":    "dot_hpms_fc",           # HPMS functional class
    "SYSTEM_CLASS_CODE":    "dot_system_class",       # State system classification
    "ROW_AUTHORITY_CODE":   "dot_ownership_code",     # Right-of-way authority → Ownership
    "TRAF_DIR_CODE":        "dot_traffic_dir",        # Traffic direction → Facility Type
    "RURAL_URBAN_CODE":     "dot_area_type_code",     # Rural/Urban code → Area Type

    # ── Road identification ──
    "RDWAY_NAME":           "dot_road_name",          # Street name
    "ROAD_NO":              "dot_road_number",         # Road number
    "RD_TYPE":              "dot_road_type",           # Road type code
    "ROUTE_NO":             "dot_route_number",        # Route number
    "ROUTE_TYPE_CODE":      "dot_route_type",          # I/US/SR

    # ── Geography ──
    "COUNTY_CODE":          "dot_county_code",         # 1=New Castle, 2=Kent, 3=Sussex
    "DISTRICT_ID":          "dot_district_id",         # DelDOT district
    "MUNIC_CODE":           "dot_municipality_code",   # Municipality
    "MAINT_AREA_ID":        "dot_maint_area",          # Maintenance area
    "HUNDREDS_CODE":        "dot_hundreds",            # Delaware hundred

    # ── Road geometry/design ──
    "LANES_QTY":            "dot_lanes",               # Lane count
    "SRFC_TYPE_CODE":       "dot_surface_type_code",   # Surface type code
    "SRFC_WDTH_QTY":        "dot_surface_width_ft",    # Surface width
    "RDWAY_WDTH_QTY":       "dot_roadway_width_ft",    # Roadway width
    "ROW_WDTH_QTY":         "dot_row_width_ft",        # Right-of-way width
    "MEDIAN_CODE":          "dot_median_code",          # Median type
    "MEDIAN_WDTH_QTY":      "dot_median_width_ft",     # Median width
    "LSHLDR_CODE":          "dot_lshldr_code",         # Left shoulder type
    "LSHLDR_WDTH_QTY":      "dot_lshldr_width_ft",    # Left shoulder width
    "RSHLDR_CODE":          "dot_rshldr_code",         # Right shoulder type
    "RSHLDR_WDTH_QTY":      "dot_rshldr_width_ft",    # Right shoulder width

    # ── Safety features ──
    "LGRDRAIL_CODE":        "dot_lguardrail",          # Left guardrail type
    "RGRDRAIL_CODE":        "dot_rguardrail",          # Right guardrail type
    "LCURB_CODE":           "dot_lcurb",               # Left curb
    "RCURB_CODE":           "dot_rcurb",               # Right curb
    "LSDWLK_CODE":          "dot_lsidewalk",           # Left sidewalk
    "RSDWLK_CODE":          "dot_rsidewalk",           # Right sidewalk

    # ── Milepoints ──
    "BEG_MP":               "dot_beg_mp",              # Begin milepoint
    "END_MP":               "dot_end_mp",              # End milepoint

    # ── Misc ──
    "PRK_CODE":             "dot_parking",             # Parking
    "SNOWPLAN_ROAD_CLASS":  "dot_snow_class",          # Snow plow priority
    "BIKE_PATH_CODE":       "dot_bike_path",           # Bike path
    "ACCEPT_YEAR_CODE":     "dot_accept_year",         # Year accepted
    "SURVEY_DATE":          "dot_survey_date",          # Last survey

    # ── v3 additions — high-value fields ──
    "MAINT_RSP_CODE":       "dot_maint_rsp_code",      # Maintenance responsibility → Ownership
    "LANE_WDTH":            "dot_lane_width",           # Actual lane width (feet)
    "CONTROL_CODE":         "dot_control_code",         # Control type
    "GUTTER_CODE":          "dot_gutter_code",          # Gutter presence
    "NHS_CODE":             "dot_nhs_code",             # National Highway System
    "SRFC_COND_CODE":       "dot_surface_condition",    # Surface condition
    "CONST_TYPE_CODE":      "dot_construction_type",    # Construction type
    "HSIP_CODE":            "dot_hsip_code",            # Highway Safety Improvement Program
}


# ═══════════════════════════════════════════════════════════════
#  VALUE TRANSFORMS: Code → CrashLens standard value
# ═══════════════════════════════════════════════════════════════

# Functional Class: DelDOT numeric code → CrashLens standard
FC_CODE_MAP = {
    "1": "1-Interstate",
    "2": "2-Freeway/Expressway",
    "3": "3-Principal Arterial",
    "4": "4-Minor Arterial",
    "5": "5-Major Collector",
    "6": "6-Minor Collector",
    "7": "7-Local",
}

# FC → SYSTEM (standard CrashLens derivation)
FC_TO_SYSTEM = {
    "1-Interstate": "DOT Interstate",
    "2-Freeway/Expressway": "DOT Primary",
    "3-Principal Arterial": "DOT Primary",
    "4-Minor Arterial": "DOT Secondary",
    "5-Major Collector": "DOT Secondary",
    "6-Minor Collector": "Non-DOT primary",
    "7-Local": "Non-DOT secondary",
}

# Ownership: ROW_AUTHORITY_CODE → CrashLens standard
# DelDOT codes from road inventory documentation
OWNERSHIP_CODE_MAP = {
    "1": "1. State Hwy Agency",       # State maintained
    "2": "2. County Hwy Agency",      # County maintained
    "3": "3. City or Town Hwy Agency", # Municipal maintained
    "4": "1. State Hwy Agency",       # Other state agency (parks, universities)
    "5": "6. Private/Unknown Roads",  # Private
    "6": "4. Federal Roads",          # Federal
    "S": "1. State Hwy Agency",       # State (alternate code)
    "C": "2. County Hwy Agency",      # County (alternate code)
    "M": "3. City or Town Hwy Agency", # Municipal (alternate code)
}

# Maintenance Responsibility: MAINT_RSP_CODE → CrashLens standard
# This is the REAL ownership data — who actually maintains the road.
# Values: "D"=DelDOT, "M"=Municipal, "S"=Suburban(DelDOT), "C"=City(DelDOT), "O"=Other
MAINT_RSP_MAP = {
    "D": "1. State Hwy Agency",       # DelDOT maintains
    "S": "1. State Hwy Agency",       # DelDOT in suburban development
    "C": "1. State Hwy Agency",       # DelDOT in city
    "M": "3. City or Town Hwy Agency", # Municipal maintains
    "O": "6. Private/Unknown Roads",  # Other
    "1": "1. State Hwy Agency",       # Numeric codes (if used)
    "2": "3. City or Town Hwy Agency",
    "3": "6. Private/Unknown Roads",
}

# Surface Type: SRFC_TYPE_CODE → CrashLens standard
# DelDOT codes follow HPMS convention:
# 1 = Portland Cement Concrete (Rigid)
# 2 = Bituminous Concrete (Asphalt/Flexible)
# 3 = Composite (Rigid base + Flexible overlay) — NOT Brick
# 4 = Gravel/Stone
# 5 = Dirt
# 9 = Other
SURFACE_TYPE_MAP = {
    "1":    "1. Concrete",
    "2":    "2. Blacktop, Asphalt, Bituminous",
    "3":    "2. Blacktop, Asphalt, Bituminous",  # Composite = asphalt overlay
    "4":    "4. Slag, Gravel, Stone",
    "5":    "5. Dirt",
    "6":    "6. Other",
    "9":    "6. Other",
    "BITM": "2. Blacktop, Asphalt, Bituminous",
    "CONC": "1. Concrete",
    "GRVL": "4. Slag, Gravel, Stone",
    "DIRT": "5. Dirt",
}

# Area Type: RURAL_URBAN_CODE → CrashLens standard
# Codes 1-3 are direct classifications; codes 4+ are subdivision/municipality
# types that need county-based derivation in normalize()
AREA_TYPE_MAP = {
    "1":  "Urban",
    "2":  "Rural",
    "3":  "Suburban",
    "U":  "Urban",
    "R":  "Rural",
    "S":  "Suburban",
}

# Traffic Direction → Facility Type
# DelDOT splits divided highways into two one-way segments (code 2),
# each with median data. Code 5 = undivided two-way (most common).
TRAFFIC_DIR_MAP = {
    "1":  "3-Two-Way Undivided",       # Two-way (rare — mostly local/Interstate)
    "2":  "1-One-Way Undivided",       # One-way (divided hwy = 2 one-way segments)
    "3":  "4-Two-Way Divided",         # Two-way divided (rare in this dataset)
    "5":  "3-Two-Way Undivided",       # Undivided two-way (81K — most roads)
    "N":  "1-One-Way Undivided",       # Northbound (one-way)
    "S":  "1-One-Way Undivided",       # Southbound
    "E":  "1-One-Way Undivided",       # Eastbound
    "W":  "1-One-Way Undivided",       # Westbound
    "B":  "3-Two-Way Undivided",       # Both directions
}

# County code → name
COUNTY_MAP = {
    "1": "New Castle",
    "2": "Kent",
    "3": "Sussex",
}

# District → DOT District name
# Code 4 = New Castle county maintenance areas (9,10,14,11) — maps to North
DISTRICT_MAP = {
    "1": "North District",
    "2": "Central District",
    "3": "South District",
    "4": "North District",      # New Castle county maintenance areas
}

# Route type code → prefix
ROUTE_TYPE_MAP = {
    "I":  "I",
    "U":  "US",
    "S":  "SR",
    "1":  "I",
    "2":  "US",
    "3":  "SR",
}

# Shoulder type codes
SHOULDER_TYPE_MAP = {
    "0": "None",
    "1": "Soil",
    "3": "Surface treated",
    "6": "Asphalt",
    "7": "Concrete",
}

# Guardrail codes
GUARDRAIL_MAP = {
    "1":  "Steel",
    "2":  "Concrete",
    "3":  "Cable",
    "4":  "Fence",
    "5":  "Retaining Wall",
    "6":  "Jersey Barrier",
    "99": "Other",
}


# ═══════════════════════════════════════════════════════════════
#  NORMALIZER: Transform raw DelDOT data → CrashLens columns
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """
    Normalize raw DelDOT road inventory data to CrashLens column architecture.

    Called by generate_state_dot_data.py after field mapping.
    Input: DataFrame with dot_ prefixed columns from FIELD_MAP.
    Output: Same DataFrame with CrashLens standard columns added.

    Fixes applied (v2):
      1. RTE Name: use dot_route_number directly ("US13"→"US 13")
      2. Surface Type: code 3 = Composite (asphalt), not Brick
      3. Facility Type: One-Way + median → "2-One-Way Divided"
      4. Through_Lanes: capped at 12
      5. DOT District: code 4 → North District
      6. Area Type: codes 4+ → county-based fallback
      7. import pandas as pd
    """
    import numpy as np
    import pandas as pd
    import re

    n = len(df)

    # ── Functional Class ──
    fc_raw = df["dot_fc_code"].fillna("").astype(str).str.strip()
    df["Functional Class"] = fc_raw.map(FC_CODE_MAP).fillna("")

    # ── SYSTEM (derived from FC) ──
    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ── Ownership (FC-based — standard for CrashLens split.py) ──
    # MAINT_RSP_CODE tells who MAINTAINS (85% State in DE because DelDOT
    # maintains most roads). But Ownership = who OWNS, which split.py uses
    # for dot_roads/county_roads/city_roads filters.
    # Keep dot_maint_rsp_code as raw column; derive Ownership from FC.
    fc_own = {
        "1-Interstate": "1. State Hwy Agency",
        "2-Freeway/Expressway": "1. State Hwy Agency",
        "3-Principal Arterial": "1. State Hwy Agency",
        "4-Minor Arterial": "1. State Hwy Agency",
        "5-Major Collector": "2. County Hwy Agency",
        "6-Minor Collector": "2. County Hwy Agency",
        "7-Local": "3. City or Town Hwy Agency",
    }
    df["Ownership"] = df["Functional Class"].map(fc_own).fillna("")

    # ── Facility Type (FIX: One-Way + median → "2-One-Way Divided") ──
    dir_raw = df["dot_traffic_dir"].fillna("").astype(str).str.strip()
    df["Facility Type"] = dir_raw.map(TRAFFIC_DIR_MAP).fillna("3-Two-Way Undivided")

    # Median upgrade: DelDOT splits divided highways into two one-way segments,
    # each with median data. One-Way + median = "2-One-Way Divided".
    # FIX: Code "1" = painted median (not a physical barrier) — exclude from upgrade.
    med_code = df["dot_median_code"].fillna("").astype(str).str.strip()
    has_median = (med_code != "") & (med_code != "0") & (med_code != "1") & (med_code != "N")

    oneway = df["Facility Type"].str.contains("One-Way", na=False)
    df.loc[has_median & oneway, "Facility Type"] = "2-One-Way Divided"

    twoway = df["Facility Type"].str.contains("Two-Way", na=False)
    df.loc[has_median & twoway, "Facility Type"] = "4-Two-Way Divided"

    # ── Roadway Surface Type (with FC sanity override) ──
    surf_raw = df["dot_surface_type_code"].fillna("").astype(str).str.strip()
    df["Roadway Surface Type"] = surf_raw.map(SURFACE_TYPE_MAP).fillna(
        "2. Blacktop, Asphalt, Bituminous"
    )
    # Override: Interstates/Freeways/Arterials are ALWAYS paved
    fc_major = df["Functional Class"].str.match(r"^[123]-", na=False)
    bad_surf = fc_major & df["Roadway Surface Type"].isin(["5. Dirt", "4. Slag, Gravel, Stone"])
    df.loc[bad_surf, "Roadway Surface Type"] = "2. Blacktop, Asphalt, Bituminous"

    # ── Area Type (FIX: codes 4+ → county-based fallback) ──
    area_raw = df["dot_area_type_code"].fillna("").astype(str).str.strip()
    df["Area Type"] = area_raw.map(AREA_TYPE_MAP).fillna("")

    # Fallback for unmapped codes: derive from county
    empty_area = df["Area Type"] == ""
    if empty_area.any():
        county = df["dot_county_code"].fillna("").astype(str).str.strip()
        # New Castle (1) = Suburban, Kent (2) = Rural, Sussex (3) = Rural
        county_area = county.map({"1": "Suburban", "2": "Rural", "3": "Rural"}).fillna("Rural")
        df.loc[empty_area, "Area Type"] = county_area[empty_area]

    # ── DOT District ──
    dist_raw = df["dot_district_id"].fillna("").astype(str).str.strip()
    df["DOT District"] = dist_raw.map(DISTRICT_MAP).fillna("")

    # ── Physical Juris Name (county) ──
    county_raw = df["dot_county_code"].fillna("").astype(str).str.strip()
    df["Physical Juris Name"] = county_raw.map(COUNTY_MAP).fillna("")

    # ── Through_Lanes (FIX: cap at 12) ──
    lanes = pd.to_numeric(df["dot_lanes"], errors="coerce").fillna(0).astype(int)
    lanes = lanes.clip(upper=12)  # No Delaware road has >12 lanes
    df["Through_Lanes"] = np.where(lanes > 0, lanes.astype(str), "")

    # ── RTE Name (FIX: use dot_route_number + extract from ramp names) ──
    rte_raw = df["dot_route_number"].fillna("").astype(str).str.strip()

    # Add space after letter prefix: "US13"→"US 13", "SR1"→"SR 1", "I95"→"I 95"
    def _add_space(val):
        if not val:
            return ""
        m = re.match(r'^([A-Za-z]+)(\d+.*)$', val)
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return val

    rte_series = rte_raw.apply(_add_space)

    # Extract route from ramp names: "RAMP TO I 95 N" → "I 95"
    road_name = df["dot_road_name"].fillna("").astype(str).str.strip()
    empty_rte = rte_series == ""
    if empty_rte.any():
        def _extract_route(name):
            m = re.search(r'(?:TO\s+|FROM\s+)((?:I|US|SR|DE)\s*-?\s*\d+)', name, re.IGNORECASE)
            if m:
                route = m.group(1).strip()
                # Normalize: "I-95"→"I 95", "SR1"→"SR 1"
                route = re.sub(r'([A-Za-z]+)\s*-?\s*(\d+)', r'\1 \2', route)
                return route
            return ""
        rte_from_name = road_name[empty_rte].apply(_extract_route)
        rte_series.loc[empty_rte] = rte_from_name

    df["RTE Name"] = rte_series

    # ── RNS MP (milepoint) ──
    beg_mp = pd.to_numeric(df["dot_beg_mp"], errors="coerce").fillna(0)
    df["RNS MP"] = np.where(beg_mp > 0, beg_mp, 0)

    # ── Segment_Length_mi (from milepoints) ──
    end_mp = pd.to_numeric(df["dot_end_mp"], errors="coerce").fillna(0)
    seg_len = (end_mp - beg_mp).clip(lower=0)
    df["Segment_Length_mi"] = np.where(seg_len > 0, np.round(seg_len, 3), 0)

    # ── Lane_Width_ft (LANE_WDTH > corrected calculation > raw calculation) ──
    # Priority 1: actual LANE_WDTH field from DOT (most accurate)
    actual_lw = pd.to_numeric(df.get("dot_lane_width", pd.Series(0, index=df.index)),
                               errors="coerce").fillna(0)

    # Priority 2: (surface_width - shoulders) / lanes (correct formula)
    surf_w = pd.to_numeric(df["dot_surface_width_ft"], errors="coerce").fillna(0)
    lsh = pd.to_numeric(df["dot_lshldr_width_ft"], errors="coerce").fillna(0)
    rsh = pd.to_numeric(df["dot_rshldr_width_ft"], errors="coerce").fillna(0)
    travel_width = (surf_w - lsh - rsh).clip(lower=0)
    lanes_num = np.maximum(lanes, 1)
    calc_lw = np.where(travel_width > 0, np.round(travel_width / lanes_num, 1), 0)

    # Priority 3: roadway_width / lanes (rough fallback)
    rdway_w = pd.to_numeric(df["dot_roadway_width_ft"], errors="coerce").fillna(0)
    rough_lw = np.where(rdway_w > 0, np.round(rdway_w / lanes_num, 1), 0)

    # Merge: actual > calculated > rough, then clip to sane range (8-16 ft)
    raw_lw = np.where(actual_lw > 0, actual_lw,
                      np.where(calc_lw > 0, calc_lw, rough_lw))
    df["Lane_Width_ft"] = np.where(raw_lw > 0, np.clip(raw_lw, 8, 16), 0)

    # ── Median_Width_ft ──
    df["Median_Width_ft"] = pd.to_numeric(
        df["dot_median_width_ft"], errors="coerce"
    ).fillna(0)

    # ── Shoulder_Width_ft (average of left + right) ──
    lsh = pd.to_numeric(df["dot_lshldr_width_ft"], errors="coerce").fillna(0)
    rsh = pd.to_numeric(df["dot_rshldr_width_ft"], errors="coerce").fillna(0)
    df["Shoulder_Width_ft"] = np.where(
        (lsh > 0) | (rsh > 0),
        np.round((lsh + rsh) / np.where((lsh > 0) & (rsh > 0), 2, 1), 1),
        0
    )

    # ── Has_Sidewalk (Yes/No from left + right sidewalk codes) ──
    lsw = df["dot_lsidewalk"].fillna("").astype(str).str.strip()
    rsw = df["dot_rsidewalk"].fillna("").astype(str).str.strip()
    has_sw = ((lsw != "") & (lsw != "0") & (lsw != "N")) | \
             ((rsw != "") & (rsw != "0") & (rsw != "N"))
    df["Has_Sidewalk"] = np.where(has_sw, "Yes", "No")

    # ── Has_Guardrail (Yes/No from left + right guardrail) ──
    lgr = df["dot_lguardrail"].fillna("").astype(str).str.strip()
    rgr = df["dot_rguardrail"].fillna("").astype(str).str.strip()
    has_gr = ((lgr != "") & (lgr != "0")) | ((rgr != "") & (rgr != "0"))
    df["Guardrail Related?"] = np.where(has_gr, "Yes", "No")

    # ── Has_Bike_Lane ──
    bike = df["dot_bike_path"].fillna("").astype(str).str.strip()
    df["Has_Bike_Lane"] = np.where((bike != "") & (bike != "0") & (bike != "N"), "Yes", "No")

    # ── Roadway Description (from updated Facility Type) ──
    fac = df["Facility Type"]
    desc_map = {
        "1-One-Way Undivided": "4. One-Way, Not Divided",
        "2-One-Way Divided": "4. One-Way, Not Divided",
        "3-Two-Way Undivided": "1. Two-Way, Not Divided",
        "4-Two-Way Divided": "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = fac.map(desc_map).fillna("1. Two-Way, Not Divided")

    # ── Is_NHS (National Highway System) ──
    nhs = df.get("dot_nhs_code", pd.Series("", index=df.index))
    nhs = nhs.fillna("").astype(str).str.strip()
    df["Is_NHS"] = np.where((nhs != "") & (nhs != "0") & (nhs != "N"), "Yes", "No")

    # ── Surface_Condition (from SRFC_COND_CODE) ──
    scond = df.get("dot_surface_condition", pd.Series("", index=df.index))
    scond = scond.fillna("").astype(str).str.strip()
    cond_map = {
        "1": "Good", "2": "Good",
        "3": "Fair", "4": "Fair",
        "5": "Poor", "6": "Poor",
        "7": "Very Poor", "8": "Very Poor", "9": "Very Poor",
        "G": "Good", "F": "Fair", "P": "Poor",
    }
    df["Surface_Condition"] = scond.map(cond_map).fillna("")

    # ── Speed_Limit_Est (estimated from FC + Area Type — baseline for HPMS override) ──
    fc = df["Functional Class"]
    area = df["Area Type"]
    spd = pd.Series("", index=df.index)
    # Interstate
    spd = np.where(fc == "1-Interstate", "65", spd)
    # Freeway
    spd = np.where(fc == "2-Freeway/Expressway", "55", spd)
    # Principal Arterial
    spd = np.where((fc == "3-Principal Arterial") & (area == "Urban"), "35", spd)
    spd = np.where((fc == "3-Principal Arterial") & (area == "Suburban"), "45", spd)
    spd = np.where((fc == "3-Principal Arterial") & (area == "Rural"), "55", spd)
    # Minor Arterial
    spd = np.where((fc == "4-Minor Arterial") & (area == "Urban"), "30", spd)
    spd = np.where((fc == "4-Minor Arterial") & (area == "Suburban"), "40", spd)
    spd = np.where((fc == "4-Minor Arterial") & (area == "Rural"), "50", spd)
    # Collector
    spd = np.where((fc == "5-Major Collector") & (area == "Urban"), "25", spd)
    spd = np.where((fc == "5-Major Collector") & (area != "Urban"), "40", spd)
    spd = np.where((fc == "6-Minor Collector") & (area == "Urban"), "25", spd)
    spd = np.where((fc == "6-Minor Collector") & (area != "Urban"), "35", spd)
    # Local
    spd = np.where(fc == "7-Local", "25", spd)
    df["Speed_Limit_Est"] = spd

    # ── Source tracking ──
    df["dot_source"] = "DelDOT Road Inventory"
    df["dot_source_url"] = ENDPOINT_URL

    return df


# ═══════════════════════════════════════════════════════════════
#  STANDALONE (for testing)
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"Delaware DOT Road Inventory Config")
    print(f"  Endpoint: {ENDPOINT_URL}")
    print(f"  Fields: {len(FIELD_MAP)} mapped")
    print(f"  Run via: python generate_state_dot_data.py --state de")
