#!/usr/bin/env python3
"""
de_state_dot.py — Delaware DOT Road Inventory Downloader & Normalizer
=======================================================================
Single-file config for Delaware's state DOT road inventory.
Called by generate_state_dot_data.py (root).

Data Source: DelDOT Road Inventory via FirstMap ArcGIS FeatureServer
  https://firstmap.gis.delaware.gov/arcgis/rest/services/
    Transportation/DE_Road_Inventory/FeatureServer/0

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
ENDPOINT_URL = (
    "https://firstmap.gis.delaware.gov/arcgis/rest/services/"
    "Transportation/DE_Road_Inventory/FeatureServer/0"
)

# ArcGIS pagination
MAX_RECORD_COUNT = 1000   # Server's maxRecordCount
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

# Surface Type: SRFC_TYPE_CODE → CrashLens standard
SURFACE_TYPE_MAP = {
    "1":    "1. Concrete",
    "2":    "2. Blacktop, Asphalt, Bituminous",
    "3":    "3. Brick or Block",
    "4":    "4. Slag, Gravel, Stone",
    "5":    "5. Dirt",
    "6":    "6. Other",
    "BITM": "2. Blacktop, Asphalt, Bituminous",
    "CONC": "1. Concrete",
    "GRVL": "4. Slag, Gravel, Stone",
    "DIRT": "5. Dirt",
}

# Area Type: RURAL_URBAN_CODE → CrashLens standard
AREA_TYPE_MAP = {
    "1":  "Urban",
    "2":  "Rural",
    "3":  "Suburban",
    "U":  "Urban",
    "R":  "Rural",
    "S":  "Suburban",
}

# Traffic Direction → Facility Type
TRAFFIC_DIR_MAP = {
    "1":  "3-Two-Way Undivided",       # Two-way
    "2":  "1-One-Way Undivided",       # One-way
    "3":  "4-Two-Way Divided",         # Two-way divided
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
DISTRICT_MAP = {
    "1": "North District",
    "2": "Central District",
    "3": "South District",
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

    Standard columns added:
      Functional Class, SYSTEM, Ownership, Facility Type,
      Roadway Surface Type, Area Type, DOT District,
      Physical Juris Name, RTE Name, Through_Lanes,
      Median_Width_ft, Shoulder_Width_ft, Has_Sidewalk,
      Has_Guardrail, RNS MP
    """
    import numpy as np

    n = len(df)

    # ── Functional Class ──
    fc_raw = df["dot_fc_code"].fillna("").astype(str).str.strip()
    df["Functional Class"] = fc_raw.map(FC_CODE_MAP).fillna("")

    # ── SYSTEM (derived from FC) ──
    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ── Ownership ──
    own_raw = df["dot_ownership_code"].fillna("").astype(str).str.strip()
    df["Ownership"] = own_raw.map(OWNERSHIP_CODE_MAP).fillna("")
    # Fallback: empty ownership → derive from FC
    empty_own = df["Ownership"] == ""
    if empty_own.any():
        fc_own = {
            "1-Interstate": "1. State Hwy Agency",
            "2-Freeway/Expressway": "1. State Hwy Agency",
            "3-Principal Arterial": "1. State Hwy Agency",
            "4-Minor Arterial": "1. State Hwy Agency",
            "5-Major Collector": "2. County Hwy Agency",
            "6-Minor Collector": "2. County Hwy Agency",
            "7-Local": "3. City or Town Hwy Agency",
        }
        df.loc[empty_own, "Ownership"] = df.loc[empty_own, "Functional Class"].map(fc_own).fillna("")

    # ── Facility Type ──
    dir_raw = df["dot_traffic_dir"].fillna("").astype(str).str.strip()
    df["Facility Type"] = dir_raw.map(TRAFFIC_DIR_MAP).fillna("3-Two-Way Undivided")
    # If median exists → "4-Two-Way Divided"
    med_code = df["dot_median_code"].fillna("").astype(str).str.strip()
    has_median = (med_code != "") & (med_code != "0") & (med_code != "N")
    twoway = df["Facility Type"].str.contains("Two-Way", na=False)
    df.loc[has_median & twoway, "Facility Type"] = "4-Two-Way Divided"

    # ── Roadway Surface Type ──
    surf_raw = df["dot_surface_type_code"].fillna("").astype(str).str.strip()
    df["Roadway Surface Type"] = surf_raw.map(SURFACE_TYPE_MAP).fillna(
        "2. Blacktop, Asphalt, Bituminous"
    )

    # ── Area Type ──
    area_raw = df["dot_area_type_code"].fillna("").astype(str).str.strip()
    df["Area Type"] = area_raw.map(AREA_TYPE_MAP).fillna("")

    # ── DOT District ──
    dist_raw = df["dot_district_id"].fillna("").astype(str).str.strip()
    df["DOT District"] = dist_raw.map(DISTRICT_MAP).fillna("")

    # ── Physical Juris Name (county) ──
    county_raw = df["dot_county_code"].fillna("").astype(str).str.strip()
    df["Physical Juris Name"] = county_raw.map(COUNTY_MAP).fillna("")

    # ── Through_Lanes ──
    lanes = df["dot_lanes"].fillna(0)
    try:
        lanes = lanes.astype(int)
    except (ValueError, TypeError):
        lanes = pd.to_numeric(lanes, errors="coerce").fillna(0).astype(int)
    df["Through_Lanes"] = np.where(lanes > 0, lanes.astype(str), "")

    # ── RTE Name (build from route type + number, fallback to road name) ──
    rt_type = df["dot_route_type"].fillna("").astype(str).str.strip()
    rt_num = df["dot_route_number"].fillna("").astype(str).str.strip()
    rt_prefix = rt_type.map(ROUTE_TYPE_MAP).fillna("")

    # Build route designation: "I 95", "US 13", "SR 9"
    has_route = (rt_prefix != "") & (rt_num != "")
    rte_name = np.where(has_route, rt_prefix + " " + rt_num, "")
    df["RTE Name"] = rte_name

    # ── RNS MP (milepoint) ──
    beg_mp = pd.to_numeric(df["dot_beg_mp"], errors="coerce").fillna(0)
    end_mp = pd.to_numeric(df["dot_end_mp"], errors="coerce").fillna(0)
    df["RNS MP"] = np.where(beg_mp > 0, beg_mp, 0)

    # ── Road width → Lane_Width_ft estimate ──
    rdway_w = pd.to_numeric(df["dot_roadway_width_ft"], errors="coerce").fillna(0)
    lanes_num = np.maximum(lanes, 1)  # avoid division by zero
    df["Lane_Width_ft"] = np.where(rdway_w > 0, np.round(rdway_w / lanes_num, 1), 0)

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

    # ── Roadway Description (from Facility Type) ──
    fac = df["Facility Type"]
    desc_map = {
        "1-One-Way Undivided": "4. One-Way, Not Divided",
        "2-One-Way Divided": "4. One-Way, Not Divided",
        "3-Two-Way Undivided": "1. Two-Way, Not Divided",
        "4-Two-Way Divided": "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = fac.map(desc_map).fillna("1. Two-Way, Not Divided")

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
