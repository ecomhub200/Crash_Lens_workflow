"""
Data Authority Layer for Road Database
=======================================
Adds resolved columns using authority hierarchy (like crash_enricher.py's 4-tier system).

When multiple sources provide the same attribute, the highest-authority source wins.
When the best source is empty, falls through to next tier.

AUTHORITY HIERARCHY:
  Tier 1 — HPMS (Federal, FHWA-validated road inventory)
  Tier 2 — Mapillary (Computer vision, photographed in field)
  Tier 3 — OSM (Community-contributed, variable quality)
  Tier 4 — Federal Point Data (BTS bridges/rail/transit, Urban Institute schools)

RESOLVED COLUMNS:
  resolved_speed_limit        HPMS > Mapillary > OSM
  resolved_speed_source       Which tier provided the value
  resolved_lanes              HPMS > OSM
  resolved_lanes_source       Which tier provided the value
  resolved_surface_type       HPMS > OSM
  resolved_has_signal         Mapillary > POI > HPMS
  resolved_has_lighting       Mapillary(count>0) > OSM(lit=yes)
  resolved_on_bridge          OSM(bridge tag) > Federal(500ft)
  resolved_school_zone        Mapillary(S1-1 sign) > POI(school 1500ft) > Federal(school 1500ft)

SANITY CHECKS:
  Speed: 5-85 mph
  Lanes: 1-12
  AADT: 0-500,000
  Bridge year: 1800-2026
  Enrollment: 1-10,000
  GPS: within continental US bounds
"""

import numpy as np
import pandas as pd


def resolve_speed_limit(df):
    """HPMS > Mapillary > OSM. Returns (value_array, source_array). Vectorized."""
    n = len(df)
    values = np.zeros(n, dtype=int)
    sources = np.full(n, "", dtype=object)

    # Tier 3: OSM (lowest priority)
    if "maxspeed" in df.columns:
        osm_raw = df["maxspeed"].astype(str).str.replace("mph", "", regex=False).str.strip()
        osm_spd = pd.to_numeric(osm_raw, errors="coerce").fillna(0).astype(int).values
        valid = (osm_spd >= 5) & (osm_spd <= 85)
        values[valid] = osm_spd[valid]
        sources[valid] = "OSM"

    # Tier 2: Mapillary (overwrites OSM)
    if "map_speed_limit_value" in df.columns:
        map_spd = pd.to_numeric(df["map_speed_limit_value"], errors="coerce").fillna(0).astype(int).values
        valid = (map_spd >= 5) & (map_spd <= 85)
        values[valid] = map_spd[valid]
        sources[valid] = "Mapillary"

    # Tier 1: HPMS (highest — always wins)
    if "hpms_speed_limit" in df.columns:
        hpms_spd = pd.to_numeric(df["hpms_speed_limit"], errors="coerce").fillna(0).astype(int).values
        valid = (hpms_spd >= 5) & (hpms_spd <= 85)
        values[valid] = hpms_spd[valid]
        sources[valid] = "HPMS"

    return values, sources


def resolve_lanes(df):
    """HPMS > OSM. Vectorized."""
    n = len(df)
    values = np.zeros(n, dtype=int)
    sources = np.full(n, "", dtype=object)

    if "lanes" in df.columns:
        # Take first value if semicolon-separated
        osm_raw = df["lanes"].astype(str).str.split(";").str[0].str.strip()
        osm_ln = pd.to_numeric(osm_raw, errors="coerce").fillna(0).astype(int).values
        valid = (osm_ln >= 1) & (osm_ln <= 12)
        values[valid] = osm_ln[valid]
        sources[valid] = "OSM"

    if "hpms_through_lanes" in df.columns:
        hpms_ln = pd.to_numeric(df["hpms_through_lanes"], errors="coerce").fillna(0).astype(int).values
        valid = (hpms_ln >= 1) & (hpms_ln <= 12)
        values[valid] = hpms_ln[valid]
        sources[valid] = "HPMS"

    return values, sources


def resolve_surface(df):
    """HPMS > OSM. Standardizes to: Paved/Unpaved/Unknown. Vectorized."""
    n = len(df)
    values = np.full(n, "", dtype=object)
    sources = np.full(n, "", dtype=object)

    osm_paved = {"asphalt", "concrete", "paved", "concrete:plates", "concrete:lanes",
                 "paving_stones", "sett", "metal"}
    osm_unpaved = {"unpaved", "gravel", "dirt", "sand", "grass", "ground",
                   "mud", "compacted", "fine_gravel", "earth"}

    if "surface" in df.columns:
        surf = df["surface"].astype(str).str.strip().str.lower().values
        is_paved = np.isin(surf, list(osm_paved))
        is_unpaved = np.isin(surf, list(osm_unpaved))
        values[is_paved] = "Paved"
        sources[is_paved] = "OSM"
        values[is_unpaved] = "Unpaved"
        sources[is_unpaved] = "OSM"

    # HPMS surface_type: 1-5=Paved variants, 7-9=Unpaved
    if "hpms_surface_type" in df.columns:
        st = pd.to_numeric(df["hpms_surface_type"], errors="coerce").fillna(0).astype(int).values
        hpms_paved = np.isin(st, [1, 2, 3, 4, 5, 6])
        hpms_unpaved = np.isin(st, [7, 8, 9, 11])
        values[hpms_paved] = "Paved"
        sources[hpms_paved] = "HPMS"
        values[hpms_unpaved] = "Unpaved"
        sources[hpms_unpaved] = "HPMS"

    return values, sources


def resolve_signals(df):
    """Mapillary > POI > HPMS. Returns Yes/No. Vectorized."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    # Tier 3: HPMS signal_type
    if "hpms_signal_type" in df.columns:
        hpms_sig = pd.to_numeric(df["hpms_signal_type"], errors="coerce").fillna(0).values
        mask = hpms_sig > 0
        values[mask] = "Yes"
        sources[mask] = "HPMS"

    # Tier 2: POI signal
    if "Near_PoiSignal_100ft" in df.columns:
        mask = df["Near_PoiSignal_100ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "POI"

    # Tier 1: Mapillary (most current — photographed)
    if "map_signal_present" in df.columns:
        mask = df["map_signal_present"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


def resolve_lighting(df):
    """Mapillary(count>0) > OSM(lit=yes). Vectorized."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "lit" in df.columns:
        lit = df["lit"].astype(str).str.strip().str.lower().values
        mask = (lit == "yes")
        values[mask] = "Yes"
        sources[mask] = "OSM"

    if "map_street_light_count" in df.columns:
        mask = df["map_street_light_count"].values > 0
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


def resolve_bridge(df):
    """OSM(bridge tag on segment) > Federal(within 500ft). Vectorized."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "Near_Bridge_500ft" in df.columns:
        mask = df["Near_Bridge_500ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Federal"

    # OSM bridge tag is directly ON the segment — higher authority
    if "bridge" in df.columns:
        br = df["bridge"].astype(str).str.strip().str.lower().values
        mask = ~np.isin(br, ["", "no", "nan", "none"])
        values[mask] = "Yes"
        sources[mask] = "OSM"

    return values, sources


def resolve_school_zone(df):
    """Mapillary(S1-1 sign) > POI(school) > Federal(school)."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "Near_School_1500ft" in df.columns:
        mask = df["Near_School_1500ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Federal"

    if "Near_PoiCollege_1500ft" in df.columns:
        # Don't count colleges as school zones
        pass

    if "map_school_zone" in df.columns:
        mask = df["map_school_zone"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


# ═══════════════════════════════════════════════════════════════
#  HPMS → VDOT FRONTEND VALUE MAPPINGS
# ═══════════════════════════════════════════════════════════════

# HPMS f_system → VDOT Functional Class
HPMS_FC_MAP = {
    1: "1-Interstate",
    2: "2-Freeway/Expressway",
    3: "3-Principal Arterial",
    4: "4-Minor Arterial",
    5: "5-Major Collector",
    6: "6-Minor Collector",
    7: "7-Local",
}

# HPMS f_system → VDOT SYSTEM (same as FC_TO_SYSTEM in crash_enricher)
FC_TO_SYSTEM = {
    "1-Interstate": "DOT Interstate",
    "2-Freeway/Expressway": "DOT Primary",
    "3-Principal Arterial": "DOT Primary",
    "4-Minor Arterial": "DOT Secondary",
    "5-Major Collector": "DOT Secondary",
    "6-Minor Collector": "Non-DOT primary",
    "7-Local": "Non-DOT secondary",
}

# OSM highway → VDOT Functional Class
OSM_FC_MAP = {
    "motorway": "1-Interstate",
    "motorway_link": "1-Interstate",
    "trunk": "2-Freeway/Expressway",
    "trunk_link": "2-Freeway/Expressway",
    "primary": "3-Principal Arterial",
    "primary_link": "3-Principal Arterial",
    "secondary": "4-Minor Arterial",
    "secondary_link": "4-Minor Arterial",
    "tertiary": "5-Major Collector",
    "tertiary_link": "5-Major Collector",
    "unclassified": "6-Minor Collector",
    "residential": "7-Local",
    "living_street": "7-Local",
    "service": "7-Local",
}

# HPMS ownership code → VDOT Ownership value
HPMS_OWN_MAP = {
    1:  "1. State Hwy Agency",
    2:  "2. County Hwy Agency",
    3:  "3. City or Town Hwy Agency",
    4:  "3. City or Town Hwy Agency",   # HPMS 4 = City or Municipal Highway Agency
    5:  "1. State Hwy Agency",          # State Park, Forest, or Reservation Agency
    11: "1. State Hwy Agency",          # State Park/Forest Agency
    12: "3. City or Town Hwy Agency",   # Local Park/Forest Agency
    21: "4. Federal Roads",          # Other Federal
    25: "4. Federal Roads",          # Forest Service
    26: "4. Federal Roads",          # National Park Service
    27: "4. Federal Roads",          # Bureau of Indian Affairs
    31: "1. State Hwy Agency",      # State Toll Authority
    32: "5. Toll Roads Maintained by Others",  # Local Toll Authority
    40: "5. Toll Roads Maintained by Others",  # Other Toll
    50: "4. Federal Roads",          # Indian Tribe Nation
    60: "6. Private/Unknown Roads",  # Other Agencies
    62: "4. Federal Roads",          # Bureau of Reclamation
    63: "4. Federal Roads",          # Corps of Engineers
    64: "4. Federal Roads",          # Military
    66: "4. Federal Roads",          # FHWA
    70: "6. Private/Unknown Roads",  # Railroad
    80: "6. Private/Unknown Roads",  # Other
}

# OSM highway → VDOT Ownership (heuristic fallback)
OSM_OWN_MAP = {
    "motorway": "1. State Hwy Agency",
    "motorway_link": "1. State Hwy Agency",
    "trunk": "1. State Hwy Agency",
    "trunk_link": "1. State Hwy Agency",
    "primary": "1. State Hwy Agency",
    "primary_link": "1. State Hwy Agency",
    "secondary": "2. County Hwy Agency",
    "secondary_link": "2. County Hwy Agency",
    "tertiary": "2. County Hwy Agency",
    "tertiary_link": "2. County Hwy Agency",
    "residential": "3. City or Town Hwy Agency",
    "living_street": "3. City or Town Hwy Agency",
    "service": "6. Private/Unknown Roads",
    "unclassified": "3. City or Town Hwy Agency",
}

# HPMS facility_type → VDOT Facility Type
# CRITICAL: HPMS codes are NOT the same numbering as VDOT!
# HPMS: 1=One-Way, 2=Two-Way, 4=Divided, 6=One-Way Couplet
# VDOT: 1=One-Way Undivided, 2=One-Way Divided, 3=Two-Way Undivided, 4=Two-Way Divided
HPMS_FACILITY_MAP = {
    1: "1-One-Way Undivided",       # HPMS 1 = One-Way Roadway
    2: "3-Two-Way Undivided",       # HPMS 2 = Two-Way Roadway (NOT "2-One-Way Divided")
    4: "4-Two-Way Divided",         # HPMS 4 = Divided Highway
    6: "1-One-Way Undivided",       # HPMS 6 = One-Way Couplet
}

# HPMS surface_type → VDOT Roadway Surface Type
# CRITICAL: Codes changed between 2018 and 2023 schemas.
# This mapping handles BOTH — keys that exist in both map correctly.
# 2023 schema (75K+ segments = 2023): 1=PCC, 2=AC, 3=CRCP, 4=AC/PCC,
#   5=AC Composite, 6=Other Composite, 7=Gravel, 8=Brick, 9=Dirt, 10=Other, 11=PCC+AC Overlay
# 2018 schema (19K segments = 2018): 1=Concrete, 2=Bituminous, 3=Brick, 4=Gravel, 5=Dirt
HPMS_SURFACE_MAP = {
    1:  "1. Concrete",                          # PCC (both schemas)
    2:  "2. Blacktop, Asphalt, Bituminous",     # AC / Bituminous (both)
    3:  "1. Concrete",                          # 2023: CRCP; 2018: Brick → safer to map as Concrete
    4:  "2. Blacktop, Asphalt, Bituminous",     # 2023: AC over PCC; 2018: Gravel → ambiguous
    5:  "2. Blacktop, Asphalt, Bituminous",     # 2023: AC Composite; 2018: Dirt → ambiguous
    6:  "6. Other",                             # 2023: Other Composite (NOT Brick)
    7:  "4. Slag, Gravel, Stone",               # 2023: Gravel; 2018: Dirt → use 2023 since DE=2023
    8:  "3. Brick or Block",                    # 2023: Brick (code 8, not 6)
    9:  "5. Dirt",                              # 2023: Unpaved/Dirt
    10: "6. Other",                             # 2023: Other/Unknown
    11: "1. Concrete",                          # 2023: PCC with Asphalt Overlay
}


def resolve_functional_class(df):
    """HPMS > OSM. Returns (fc_values, system_values, fc_sources)."""
    n = len(df)
    fc_values = np.full(n, "", dtype=object)
    sys_values = np.full(n, "", dtype=object)
    fc_sources = np.full(n, "", dtype=object)

    # Tier 2: OSM (lowest — set first)
    if "highway" in df.columns:
        hw = df["highway"].astype(str).str.strip().str.lower().values
        for i, h in enumerate(hw):
            # Handle semicolons (e.g. "primary;trunk")
            first = h.split(";")[0].strip()
            fc = OSM_FC_MAP.get(first, "")
            if fc:
                fc_values[i] = fc
                sys_values[i] = FC_TO_SYSTEM.get(fc, "")
                fc_sources[i] = "OSM"

    # Tier 1: HPMS (highest — always overwrites)
    if "hpms_f_system" in df.columns:
        fs = pd.to_numeric(df["hpms_f_system"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(fs):
            fc = HPMS_FC_MAP.get(code, "")
            if fc:
                fc_values[i] = fc
                sys_values[i] = FC_TO_SYSTEM.get(fc, "")
                fc_sources[i] = "HPMS"

    return fc_values, sys_values, fc_sources


def resolve_ownership(df):
    """HPMS > OSM. Returns (ownership_values, ownership_sources)."""
    n = len(df)
    values = np.full(n, "", dtype=object)
    sources = np.full(n, "", dtype=object)

    # Tier 2: OSM (heuristic from highway tag)
    if "highway" in df.columns:
        hw = df["highway"].astype(str).str.strip().str.lower().values
        for i, h in enumerate(hw):
            first = h.split(";")[0].strip()
            own = OSM_OWN_MAP.get(first, "")
            if own:
                values[i] = own
                sources[i] = "OSM"

    # Tier 1: HPMS (authoritative — always overwrites)
    if "hpms_ownership" in df.columns:
        oc = pd.to_numeric(df["hpms_ownership"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(oc):
            own = HPMS_OWN_MAP.get(code, "")
            if own:
                values[i] = own
                sources[i] = "HPMS"

    return values, sources


def resolve_intersection_type(df):
    """Derive Intersection Type from intersection_degree with correct approach count."""
    n = len(df)
    values = np.full(n, "1. Not at Intersection", dtype=object)

    if "is_intersection" not in df.columns or "intersection_degree" not in df.columns:
        return values

    is_int = df["is_intersection"].values == "Yes"
    degree = pd.to_numeric(df["intersection_degree"], errors="coerce").fillna(0).astype(int).values

    # OSM graph degree counts ALL edges (both directions of 2-way roads).
    # A typical 3-way T-intersection has degree 6 (3 roads × 2 directions).
    # A 4-way intersection has degree 8 (4 roads × 2 directions).
    # For one-way roads, degree = actual edges. Mixed is complex.
    # Best heuristic: approaches ≈ ceil(degree / 2), capped at 5
    approaches = np.ceil(degree / 2).astype(int)
    approaches = np.clip(approaches, 0, 6)

    values = np.where(~is_int, "1. Not at Intersection",
             np.where(approaches <= 2, "2. Two Approaches",
             np.where(approaches == 3, "3. Three Approaches",
             np.where(approaches == 4, "4. Four Approaches",
             "5. Five-Point, or More"))))

    return values


def resolve_facility_type(df):
    """HPMS > OSM oneway tag. Returns (values, sources)."""
    n = len(df)
    values = np.full(n, "", dtype=object)
    sources = np.full(n, "", dtype=object)

    # Tier 2: OSM (from oneway + divider tags)
    if "oneway" in df.columns:
        ow = df["oneway"].astype(str).str.strip().str.lower().values
        div = df.get("divider", pd.Series([""] * n)).astype(str).str.strip().str.lower().values
        for i in range(n):
            if ow[i] in ("yes", "true", "1", "-1"):
                values[i] = "1-One-Way Undivided"
                sources[i] = "OSM"
            elif div[i] in ("yes", "median", "barrier"):
                values[i] = "4-Two-Way Divided"
                sources[i] = "OSM"
            elif ow[i] in ("no", "false", "0", ""):
                values[i] = "3-Two-Way Undivided"
                sources[i] = "OSM"

    # Tier 1: HPMS (authoritative)
    if "hpms_facility_type" in df.columns:
        ft = pd.to_numeric(df["hpms_facility_type"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(ft):
            fac = HPMS_FACILITY_MAP.get(code, "")
            if fac:
                values[i] = fac
                sources[i] = "HPMS"

    return values, sources


def resolve_surface_type_vdot(df):
    """HPMS > OSM. Returns VDOT-standard surface type values using correct HPMS mapping."""
    n = len(df)
    values = np.full(n, "", dtype=object)
    sources = np.full(n, "", dtype=object)

    # OSM surface → VDOT
    osm_to_vdot = {
        "asphalt": "2. Blacktop, Asphalt, Bituminous",
        "concrete": "1. Concrete",
        "paved": "2. Blacktop, Asphalt, Bituminous",
        "concrete:plates": "1. Concrete",
        "concrete:lanes": "1. Concrete",
        "paving_stones": "3. Brick or Block",
        "sett": "3. Brick or Block",
        "brick": "3. Brick or Block",
        "gravel": "4. Slag, Gravel, Stone",
        "fine_gravel": "4. Slag, Gravel, Stone",
        "compacted": "4. Slag, Gravel, Stone",
        "dirt": "5. Dirt",
        "earth": "5. Dirt",
        "ground": "5. Dirt",
        "mud": "5. Dirt",
        "sand": "5. Dirt",
        "grass": "5. Dirt",
        "unpaved": "4. Slag, Gravel, Stone",
        "metal": "6. Other",
    }

    if "surface" in df.columns:
        surf = df["surface"].astype(str).str.strip().str.lower().values
        for i, s in enumerate(surf):
            v = osm_to_vdot.get(s, "")
            if v:
                values[i] = v
                sources[i] = "OSM"

    # HPMS surface_type → VDOT (using correct mapping, NOT 1:1 numbering)
    if "hpms_surface_type" in df.columns:
        st = pd.to_numeric(df["hpms_surface_type"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(st):
            v = HPMS_SURFACE_MAP.get(code, "")
            if v:
                values[i] = v
                sources[i] = "HPMS"

    return values, sources


def resolve_traffic_control(df):
    """Derive Traffic Control Type from resolved signals, stop signs, yield signs."""
    n = len(df)
    values = np.full(n, "", dtype=object)

    # Yield signs from Mapillary
    if "map_yield_sign" in df.columns:
        mask = df["map_yield_sign"].values == "Yes"
        values[mask] = "8. Yield Sign"

    # Stop signs
    has_stop = np.zeros(n, dtype=bool)
    if "map_stop_sign" in df.columns:
        has_stop |= (df["map_stop_sign"].values == "Yes")
    if "Near_PoiStopSign_100ft" in df.columns:
        has_stop |= (df["Near_PoiStopSign_100ft"].values == "Yes")
    values[has_stop] = "4. Stop Sign"

    # Traffic signals (highest priority — overwrites stop)
    has_signal = np.zeros(n, dtype=bool)
    if "resolved_has_signal" in df.columns:
        has_signal = (df["resolved_has_signal"].values == "Yes")
    elif "map_signal_present" in df.columns:
        has_signal = (df["map_signal_present"].values == "Yes")
    values[has_signal] = "3. Traffic Signal"

    # Rail crossing signals
    if "Near_RailXing_500ft" in df.columns:
        rail = (df["Near_RailXing_500ft"].values == "Yes")
        # Only set if not already signal/stop
        empty_rail = rail & (values == "")
        values[empty_rail] = "10. Railroad Crossing With Markings and Signs"

    # Fill remaining empty with "1. No Traffic Control"
    values[values == ""] = "1. No Traffic Control"

    return values


# ═══════════════════════════════════════════════════════════════
#  FRONTEND COLUMN POPULATOR
# ═══════════════════════════════════════════════════════════════

def populate_frontend_columns(df, state_abbr=""):
    """
    Map all resolved/enriched values to exact VDOT frontend column names.
    Applies statutory speed defaults, IRI pavement condition, VMT readiness.
    Drops duplicate resolved_ columns (keeps _source cols for provenance).
    """
    print("    Populating frontend-standard columns...")
    n = len(df)
    populated = 0

    # ── Speed limit with statutory defaults (#1) ──
    if "resolved_speed_limit" in df.columns:
        speed = df["resolved_speed_limit"].values.copy()
        source = df["resolved_speed_source"].values.copy()
        fc = df.get("resolved_functional_class",
                     df.get("Functional Class", pd.Series([""] * n))).astype(str).values

        # Load statutory defaults from registry
        get_statutory = None
        try:
            from states_registry import get_statutory_speed
            get_statutory = get_statutory_speed
        except ImportError:
            pass
        if not get_statutory:
            try:
                from pathlib import Path
                import importlib.util
                for p in [Path(__file__).parent / "states_registry.py", Path("states_registry.py")]:
                    if p.exists():
                        spec = importlib.util.spec_from_file_location("sr", p)
                        sr = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(sr)
                        get_statutory = sr.get_statutory_speed
                        break
            except Exception:
                pass

        if get_statutory:
            no_speed = speed == 0
            imputed = 0
            for i in range(n):
                if no_speed[i] and fc[i]:
                    default = get_statutory(fc[i], state_abbr)
                    if default > 0:
                        speed[i] = default
                        source[i] = "Statutory"
                        imputed += 1
            df["resolved_speed_limit"] = speed
            df["resolved_speed_source"] = source
            measured = ((source != "Statutory") & (source != "")).sum()
            print(f"      Speed limit:       measured={measured:,}, "
                  f"statutory={imputed:,}, total={(speed > 0).sum():,}/{n:,} "
                  f"({(speed > 0).sum()/n*100:.0f}%)")
        else:
            print(f"      Speed limit:       {(speed > 0).sum():>7,}/{n:,} (no defaults — states_registry.py not found)")

        df["Max Speed Diff"] = df["resolved_speed_limit"]
        populated += 1

    # ── Functional Class + SYSTEM ──
    if "resolved_functional_class" in df.columns:
        df["Functional Class"] = df["resolved_functional_class"]
        df["SYSTEM"] = df["resolved_system"]
        _count = (df['Functional Class'] != '').sum()
        print(f'      Functional Class:  {_count:>7,}/{n:,}')
        populated += 2

    # ── Ownership ──
    if "resolved_ownership" in df.columns:
        df["Ownership"] = df["resolved_ownership"]
        _count = (df['Ownership'] != '').sum()
        print(f'      Ownership:         {_count:>7,}/{n:,}')
        populated += 1

    # ── Facility Type ──
    if "resolved_facility_type" in df.columns:
        df["Facility Type"] = df["resolved_facility_type"]
        _count = (df['Facility Type'] != '').sum()
        print(f'      Facility Type:     {_count:>7,}/{n:,}')
        populated += 1

    # ── Roadway Surface Type ──
    if "resolved_surface_type_vdot" in df.columns:
        df["Roadway Surface Type"] = df["resolved_surface_type_vdot"]
        _count = (df['Roadway Surface Type'] != '').sum()
        print(f'      Surface Type:      {_count:>7,}/{n:,}')
        populated += 1

    # ── Traffic Control Type ──
    if "resolved_traffic_control" in df.columns:
        df["Traffic Control Type"] = df["resolved_traffic_control"]
        _count = (df['Traffic Control Type'] != '1. No Traffic Control').sum()
        print(f'      Traffic Control:   {_count:>7,}/{n:,}')
        populated += 1

    # ── Intersection Type ──
    if "resolved_intersection_type" in df.columns:
        df["Intersection Type"] = df["resolved_intersection_type"]
        _count = (df['Intersection Type'] != '1. Not at Intersection').sum()
        print(f'      Intersection Type: {_count:>7,}/{n:,}')
        populated += 1

    # ── Area Type ──
    if "geo_area_type" in df.columns:
        df["Area Type"] = df["geo_area_type"]
        populated += 1

    # ── Lanes ──
    if "resolved_lanes" in df.columns:
        df["Through_Lanes"] = df["resolved_lanes"]
        populated += 1

    # ── AADT ──
    if "hpms_aadt" in df.columns:
        df["AADT"] = pd.to_numeric(df["hpms_aadt"], errors="coerce").fillna(0).astype(int)
        populated += 1

    # ── Geography ──
    for src, dst in [("geo_dot_region","DOT District"),("geo_planning_district","Planning District"),
                     ("geo_mpo_name","MPO Name"),("geo_county_name","Physical Juris Name"),
                     ("geo_juris_code","Juris Code")]:
        if src in df.columns:
            df[dst] = df[src]
            populated += 1

    # ── Route name ──
    if "hpms_route_name" in df.columns:
        rte = df["hpms_route_name"].astype(str).str.strip()
        osm_ref = df.get("ref", pd.Series([""] * n)).astype(str).str.strip()
        rte = rte.where(rte != "", osm_ref).where(rte != "0", "")
        df["RTE Name"] = rte
        populated += 1

    # ── School Zone ──
    if "resolved_school_zone" in df.columns:
        df["School Zone"] = np.where(df["resolved_school_zone"] == "Yes", "1. Yes", "3. No")
        populated += 1

    # ── IRI Pavement Condition (#7 — new frontend column) ──
    if "hpms_iri" in df.columns:
        iri = pd.to_numeric(df["hpms_iri"], errors="coerce").fillna(0).values
        df["Roadway Condition"] = np.where(
            iri <= 0, "",
            np.where(iri <= 95, "Good",
            np.where(iri <= 170, "Fair",
            np.where(iri <= 220, "Mediocre", "Poor"))))
        has_cond = (df["Roadway Condition"] != "").sum()
        good = ((iri > 0) & (iri <= 95)).sum()
        poor = (iri > 220).sum()
        print(f"      Roadway Condition: {has_cond:>7,}/{n:,} (Good={good:,}, Poor={poor:,})")
        populated += 1

    # ── Crash Rate Readiness (#8) ──
    if "AADT" in df.columns:
        aadt_val = pd.to_numeric(df["AADT"], errors="coerce").fillna(0)
        length_mi = pd.to_numeric(df.get("hpms_length_mi", 0), errors="coerce").fillna(0)
        df["VMT_Annual"] = (aadt_val * 365 * length_mi).round(0).astype(int)
        vmt_ready = (df["VMT_Annual"] > 0).sum()
        print(f"      VMT_Annual:        {vmt_ready:>7,}/{n:,} crash-rate-ready")
        populated += 1

    # ── Drop duplicate resolved_ columns (#5 — keep _source for provenance) ──
    dupes = ["resolved_functional_class","resolved_system","resolved_ownership",
             "resolved_facility_type","resolved_surface_type_vdot",
             "resolved_traffic_control","resolved_intersection_type"]
    dropped = sum(1 for c in dupes if c in df.columns)
    df.drop(columns=[c for c in dupes if c in df.columns], inplace=True)
    if dropped:
        print(f"      Dropped {dropped} duplicate resolved_ cols (kept _source)")

    print(f"    Frontend columns populated: {populated}")


# ═══════════════════════════════════════════════════════════════
#  SANITY CHECKS
# ═══════════════════════════════════════════════════════════════

def run_sanity_checks(df, state_abbr):
    """Run data quality checks. Returns dict of {check_name: (passed, total, pct)}."""
    checks = {}

    n = len(df)

    # GPS within continental US (rough bounds)
    lat_ok = ((df["mid_lat"] >= 24.0) & (df["mid_lat"] <= 72.0)).sum()
    lon_ok = ((df["mid_lon"] >= -180.0) & (df["mid_lon"] <= -65.0)).sum()
    checks["gps_lat_in_range"] = (lat_ok, n, lat_ok/n*100)
    checks["gps_lon_in_range"] = (lon_ok, n, lon_ok/n*100)

    # Speed sanity (where resolved)
    if "resolved_speed_limit" in df.columns:
        has_speed = df["resolved_speed_limit"] > 0
        if has_speed.sum() > 0:
            valid = ((df["resolved_speed_limit"] >= 5) & (df["resolved_speed_limit"] <= 85))
            checks["speed_5_to_85"] = (valid.sum(), has_speed.sum(), valid.sum()/max(has_speed.sum(),1)*100)

    # Lanes sanity
    if "resolved_lanes" in df.columns:
        has_lanes = df["resolved_lanes"] > 0
        if has_lanes.sum() > 0:
            valid = ((df["resolved_lanes"] >= 1) & (df["resolved_lanes"] <= 12))
            checks["lanes_1_to_12"] = (valid.sum(), has_lanes.sum(), valid.sum()/max(has_lanes.sum(),1)*100)

    # HPMS AADT sanity
    if "hpms_aadt" in df.columns:
        has_aadt = df["hpms_aadt"] > 0
        if has_aadt.sum() > 0:
            valid = ((df["hpms_aadt"] >= 1) & (df["hpms_aadt"] <= 500000))
            checks["aadt_1_to_500k"] = (valid.sum(), has_aadt.sum(), valid.sum()/max(has_aadt.sum(),1)*100)

    # Bridge year sanity
    if "nearest_bridge_year_built" in df.columns:
        has_year = df["nearest_bridge_year_built"] != ""
        if isinstance(df["nearest_bridge_year_built"].iloc[0], str):
            has_year = (df["nearest_bridge_year_built"].str.strip() != "") & (df["nearest_bridge_year_built"] != "0")
        if has_year.sum() > 0:
            try:
                years = pd.to_numeric(df.loc[has_year, "nearest_bridge_year_built"], errors="coerce")
                valid = ((years >= 1800) & (years <= 2026)).sum()
                checks["bridge_year_1800_2026"] = (valid, has_year.sum(), valid/max(has_year.sum(),1)*100)
            except:
                pass

    # School enrollment sanity
    if "nearest_school_enrollment" in df.columns:
        has_enr = df["nearest_school_enrollment"] != ""
        if has_enr.sum() > 0:
            try:
                enr = pd.to_numeric(df.loc[has_enr, "nearest_school_enrollment"], errors="coerce")
                valid = ((enr >= 1) & (enr <= 10000)).sum()
                checks["enrollment_1_to_10k"] = (valid, has_enr.sum(), valid/max(has_enr.sum(),1)*100)
            except:
                pass

    # County FIPS populated
    if "geo_county_fips" in df.columns:
        has_county = (df["geo_county_fips"].astype(str).str.strip() != "").sum()
        checks["county_fips_populated"] = (has_county, n, has_county/n*100)

    # No duplicate rows (exclude HPMS orphans with negative IDs)
    if "u_node" in df.columns:
        real_segs = df[df["u_node"] >= 0] if (df["u_node"] < 0).any() else df
        dupes = real_segs.duplicated(subset=["u_node", "v_node"]).sum()
        checks["no_duplicate_segments"] = (len(real_segs) - dupes, len(real_segs),
                                           (len(real_segs)-dupes)/max(len(real_segs),1)*100)

    # HPMS match within 100m
    if "hpms_match_dist_ft" in df.columns:
        matched = df["hpms_matched"] == "Yes"
        if matched.sum() > 0:
            within_100m = (df.loc[matched, "hpms_match_dist_ft"] <= 328).sum()  # 100m in ft
            checks["hpms_within_100m"] = (within_100m, matched.sum(), within_100m/max(matched.sum(),1)*100)

    # ── Frontend column validation ──
    # Valid VDOT Functional Class values
    VALID_FC = {"1-Interstate", "2-Freeway/Expressway", "3-Principal Arterial",
                "4-Minor Arterial", "5-Major Collector", "6-Minor Collector", "7-Local", ""}
    if "Functional Class" in df.columns:
        fc_vals = set(df["Functional Class"].astype(str).unique())
        invalid_fc = fc_vals - VALID_FC
        valid_fc = (df["Functional Class"].astype(str).isin(VALID_FC)).sum()
        checks["fc_valid_values"] = (valid_fc, n, valid_fc/n*100)
        if invalid_fc - {"nan", "None"}:
            print(f"      ⚠️ Invalid FC values: {invalid_fc - {'nan', 'None'}}")

    # Valid Ownership values
    VALID_OWN = {"1. State Hwy Agency", "2. County Hwy Agency", "3. City or Town Hwy Agency",
                 "4. Federal Roads", "5. Toll Roads Maintained by Others",
                 "6. Private/Unknown Roads", ""}
    if "Ownership" in df.columns:
        own_vals = set(df["Ownership"].astype(str).unique())
        invalid_own = own_vals - VALID_OWN
        valid_own = (df["Ownership"].astype(str).isin(VALID_OWN)).sum()
        checks["ownership_valid_values"] = (valid_own, n, valid_own/n*100)
        if invalid_own - {"nan", "None"}:
            print(f"      ⚠️ Invalid Ownership values: {invalid_own - {'nan', 'None'}}")

    # Functional Class populated (should be near 100%)
    if "Functional Class" in df.columns:
        fc_filled = (df["Functional Class"].astype(str).str.strip().isin(VALID_FC - {""})).sum()
        checks["fc_populated"] = (fc_filled, n, fc_filled/n*100)

    # Ownership populated
    if "Ownership" in df.columns:
        own_filled = (df["Ownership"].astype(str).str.strip().isin(VALID_OWN - {""})).sum()
        checks["ownership_populated"] = (own_filled, n, own_filled/n*100)

    return checks


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def apply_authority_layer(df, state_abbr=""):
    """Add all resolved columns to the road database DataFrame."""
    print("    Data authority resolution...")

    # ── Functional Class + SYSTEM ──
    fc_vals, sys_vals, fc_srcs = resolve_functional_class(df)
    df["resolved_functional_class"] = fc_vals
    df["resolved_system"] = sys_vals
    df["resolved_fc_source"] = fc_srcs
    filled = (fc_vals != "").sum()
    print(f"      Func Class:   {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(fc_srcs=='HPMS').sum():,} OSM:{(fc_srcs=='OSM').sum():,}")

    # ── Ownership ──
    own_vals, own_srcs = resolve_ownership(df)
    df["resolved_ownership"] = own_vals
    df["resolved_ownership_source"] = own_srcs
    filled = (own_vals != "").sum()
    print(f"      Ownership:    {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(own_srcs=='HPMS').sum():,} OSM:{(own_srcs=='OSM').sum():,}")

    # ── Facility Type ──
    fac_vals, fac_srcs = resolve_facility_type(df)
    df["resolved_facility_type"] = fac_vals
    df["resolved_facility_source"] = fac_srcs
    filled = (fac_vals != "").sum()
    print(f"      Facility:     {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(fac_srcs=='HPMS').sum():,} OSM:{(fac_srcs=='OSM').sum():,}")

    vals, srcs = resolve_speed_limit(df)
    df["resolved_speed_limit"] = vals
    df["resolved_speed_source"] = srcs
    filled = (vals > 0).sum()
    print(f"      Speed limit:  {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(srcs=='HPMS').sum():,} Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_lanes(df)
    df["resolved_lanes"] = vals
    df["resolved_lanes_source"] = srcs
    filled = (vals > 0).sum()
    print(f"      Lanes:        {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(srcs=='HPMS').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_surface(df)
    df["resolved_surface_type"] = vals
    df["resolved_surface_source"] = srcs
    filled = (vals != "").sum()
    print(f"      Surface:      {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(srcs=='HPMS').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_signals(df)
    df["resolved_has_signal"] = vals
    df["resolved_signal_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      Signal:       {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} POI:{(srcs=='POI').sum():,} HPMS:{(srcs=='HPMS').sum():,}")

    vals, srcs = resolve_lighting(df)
    df["resolved_has_lighting"] = vals
    df["resolved_lighting_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      Lighting:     {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_bridge(df)
    df["resolved_on_bridge"] = vals
    df["resolved_bridge_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      On bridge:    {yes:>7,} Yes"
          f" — OSM:{(srcs=='OSM').sum():,} Fed:{(srcs=='Federal').sum():,}")

    vals, srcs = resolve_school_zone(df)
    df["resolved_school_zone"] = vals
    df["resolved_school_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      School zone:  {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} Fed:{(srcs=='Federal').sum():,}")

    # ── Surface Type (VDOT standard) ──
    surf_vals, surf_srcs = resolve_surface_type_vdot(df)
    df["resolved_surface_type_vdot"] = surf_vals
    df["resolved_surface_vdot_source"] = surf_srcs
    filled = (surf_vals != "").sum()
    print(f"      Surface(VDOT):{filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — HPMS:{(surf_srcs=='HPMS').sum():,} OSM:{(surf_srcs=='OSM').sum():,}")

    # ── Intersection Type ──
    int_vals = resolve_intersection_type(df)
    df["resolved_intersection_type"] = int_vals
    at_int = (int_vals != "1. Not at Intersection").sum()
    print(f"      Intersection: {at_int:>7,} at intersection ({at_int/len(df)*100:.1f}%)")

    # ── Traffic Control Type ──
    tc_vals = resolve_traffic_control(df)
    df["resolved_traffic_control"] = tc_vals
    has_ctrl = (tc_vals != "1. No Traffic Control").sum()
    print(f"      Traffic Ctrl: {has_ctrl:>7,} controlled ({has_ctrl/len(df)*100:.1f}%)")

    # ── Populate frontend columns (must be last) ──
    populate_frontend_columns(df, state_abbr=state_abbr)


def print_sanity_report(checks):
    """Print sanity check results."""
    print("\n    Sanity checks:")
    all_pass = True
    for name, (passed, total, pct) in sorted(checks.items()):
        icon = "✅" if pct >= 99.0 else ("⚠️" if pct >= 90.0 else "❌")
        if pct < 99.0:
            all_pass = False
        print(f"      {icon} {name:30s}  {passed:>8,}/{total:>8,} ({pct:.1f}%)")
    return all_pass


# ═══════════════════════════════════════════════════════════════
#  CONFIDENCE SCORING + CROSS-SOURCE VALIDATION
# ═══════════════════════════════════════════════════════════════

def compute_confidence_scores(df):
    """
    Add confidence scores for key safety attributes.
    
    Confidence = how many independent sources agree + data freshness.
    
    Scoring (0-100):
      50  = 1 source confirms
      75  = 2 sources confirm  
      90  = 3 sources confirm
      +5  = Mapillary data < 2 years old
      +3  = Mapillary data < 4 years old
      
    Columns added:
      conf_signal          0-100 confidence that segment has traffic signal
      conf_crosswalk       0-100 confidence that segment has crosswalk
      conf_stop_sign       0-100 confidence that segment has stop sign
      conf_school_zone     0-100 confidence this is a school zone
      conf_speed_limit     0-100 confidence in resolved speed limit value
      conf_bridge          0-100 confidence segment is on/near bridge
    
    Cross-validation columns:
      xval_signal_sources  Count of independent sources confirming signal
      xval_crosswalk_sources
      xval_stop_sign_sources
      xval_school_sources
    """
    print("    Confidence scoring + cross-validation...")
    n = len(df)

    # ── SIGNAL confidence ──
    sig_sources = np.zeros(n, dtype=int)
    if "hpms_signal_type" in df.columns:
        try:
            sig_sources += (pd.to_numeric(df["hpms_signal_type"], errors="coerce").fillna(0) > 0).astype(int)
        except: pass
    if "Near_PoiSignal_100ft" in df.columns:
        sig_sources += (df["Near_PoiSignal_100ft"] == "Yes").astype(int)
    if "map_signal_present" in df.columns:
        sig_sources += (df["map_signal_present"] == "Yes").astype(int)
    
    df["xval_signal_sources"] = sig_sources
    conf = np.where(sig_sources >= 3, 90, np.where(sig_sources == 2, 75, np.where(sig_sources == 1, 50, 0)))
    # Freshness bonus from Mapillary
    if "map_signal_present" in df.columns:
        conf = _add_freshness_bonus(df, conf, "map_signal_present", "Yes")
    df["conf_signal"] = conf
    multi = (sig_sources >= 2).sum()
    print(f"      Signal:     {multi:>6,} confirmed by 2+ sources, "
          f"max conf={conf.max()}")

    # ── CROSSWALK confidence ──
    xwalk_sources = np.zeros(n, dtype=int)
    if "Near_PoiCrossing_100ft" in df.columns:
        xwalk_sources += (df["Near_PoiCrossing_100ft"] == "Yes").astype(int)
    if "map_crosswalk_count" in df.columns:
        xwalk_sources += (df["map_crosswalk_count"] > 0).astype(int)
    if "map_stop_line_count" in df.columns:
        # Stop line near crosswalk = corroborating evidence
        xwalk_sources += ((df["map_stop_line_count"] > 0) & (xwalk_sources > 0)).astype(int)
    
    df["xval_crosswalk_sources"] = xwalk_sources
    conf = np.where(xwalk_sources >= 3, 90, np.where(xwalk_sources == 2, 75, np.where(xwalk_sources == 1, 50, 0)))
    df["conf_crosswalk"] = conf
    multi = (xwalk_sources >= 2).sum()
    print(f"      Crosswalk:  {multi:>6,} confirmed by 2+ sources")

    # ── STOP SIGN confidence ──
    stop_sources = np.zeros(n, dtype=int)
    if "Near_PoiStopSign_100ft" in df.columns:
        stop_sources += (df["Near_PoiStopSign_100ft"] == "Yes").astype(int)
    if "map_stop_sign" in df.columns:
        stop_sources += (df["map_stop_sign"] == "Yes").astype(int)
    if "map_stop_ahead" in df.columns:
        # Stop Ahead warning sign corroborates
        stop_sources += ((df["map_stop_ahead"] == "Yes") & (stop_sources > 0)).astype(int)
    if "hpms_num_stop_int" in df.columns:
        try:
            stop_sources += (pd.to_numeric(df["hpms_num_stop_int"], errors="coerce").fillna(0) > 0).astype(int)
        except: pass

    df["xval_stop_sign_sources"] = stop_sources
    conf = np.where(stop_sources >= 3, 90, np.where(stop_sources == 2, 75, np.where(stop_sources == 1, 50, 0)))
    df["conf_stop_sign"] = conf
    multi = (stop_sources >= 2).sum()
    print(f"      Stop sign:  {multi:>6,} confirmed by 2+ sources")

    # ── SCHOOL ZONE confidence ──
    school_sources = np.zeros(n, dtype=int)
    if "Near_School_1500ft" in df.columns:
        school_sources += (df["Near_School_1500ft"] == "Yes").astype(int)
    if "map_school_zone" in df.columns:
        school_sources += (df["map_school_zone"] == "Yes").astype(int)
    if "Near_PoiCollege_1500ft" in df.columns:
        # Don't count college as school zone — separate
        pass

    df["xval_school_sources"] = school_sources
    conf = np.where(school_sources >= 2, 90, np.where(school_sources == 1, 50, 0))
    df["conf_school_zone"] = conf
    multi = (school_sources >= 2).sum()
    print(f"      School zone:{multi:>6,} confirmed by 2+ sources")

    # ── SPEED LIMIT confidence ──
    speed_sources = np.zeros(n, dtype=int)
    if "hpms_speed_limit" in df.columns:
        try:
            has = pd.to_numeric(df["hpms_speed_limit"], errors="coerce").fillna(0)
            speed_sources += ((has >= 5) & (has <= 85)).astype(int)
        except: pass
    if "map_speed_limit_value" in df.columns:
        speed_sources += (df["map_speed_limit_value"].astype(str).str.strip() != "").astype(int)
    if "maxspeed" in df.columns:
        speed_sources += (df["maxspeed"].astype(str).str.strip() != "").astype(int)

    conf = np.where(speed_sources >= 3, 95, np.where(speed_sources == 2, 80, np.where(speed_sources == 1, 50, 0)))
    df["conf_speed_limit"] = conf
    multi = (speed_sources >= 2).sum()
    print(f"      Speed:      {multi:>6,} confirmed by 2+ sources")

    # ── BRIDGE confidence ──
    br_sources = np.zeros(n, dtype=int)
    if "bridge" in df.columns:
        br_sources += df["bridge"].astype(str).str.strip().str.lower().isin(
            ["yes","viaduct","movable","cantilever"]).astype(int)
    if "Near_Bridge_500ft" in df.columns:
        br_sources += (df["Near_Bridge_500ft"] == "Yes").astype(int)

    df["conf_bridge"] = np.where(br_sources >= 2, 90, np.where(br_sources == 1, 50, 0))
    multi = (br_sources >= 2).sum()
    print(f"      Bridge:     {multi:>6,} confirmed by 2+ sources")


def _add_freshness_bonus(df, conf_array, col, match_val):
    """Add +5 for <2yr, +3 for <4yr Mapillary data."""
    # This requires map_speed_limit_dist_ft or similar to be present
    # In practice, freshness is a global property of the Mapillary dataset
    # We can't determine per-row freshness without the first_seen date
    # which isn't carried into the road database. So this is a no-op placeholder.
    # The actual freshness scoring happens in the Mapillary source file.
    return conf_array


# ═══════════════════════════════════════════════════════════════
#  MAPILLARY FRESHNESS SCORING (when raw Mapillary data available)
# ═══════════════════════════════════════════════════════════════

def score_mapillary_freshness(mapillary_df):
    """
    Score Mapillary features by data freshness.
    Called BEFORE enrichment, filters/weights features.
    
    Returns DataFrame with freshness_score column:
      100 = seen in last year
       80 = seen in last 2 years  
       60 = seen in last 4 years
       40 = seen 4-8 years ago
       20 = seen >8 years ago
    """
    if mapillary_df is None or len(mapillary_df) == 0:
        return mapillary_df
    
    dates = pd.to_datetime(mapillary_df["first_seen"], errors="coerce")
    now = pd.Timestamp.now()
    age_days = (now - dates).dt.days.fillna(9999)
    
    scores = np.where(age_days < 365, 100,
             np.where(age_days < 730, 80,
             np.where(age_days < 1460, 60,
             np.where(age_days < 2920, 40, 20))))
    
    mapillary_df = mapillary_df.copy()
    mapillary_df["freshness_score"] = scores
    
    return mapillary_df


# ═══════════════════════════════════════════════════════════════
#  RISK INDICATORS (derived from multi-source cross-analysis)
# ═══════════════════════════════════════════════════════════════

def compute_risk_indicators(df):
    """
    Derive safety-relevant risk indicators from cross-source data.
    These are NOT raw data — they are analytical conclusions.
    
    Columns added:
      risk_speed_transition       Yes/No — 2+ different speed limits within 500ft
      risk_speed_transition_diff  Speed differential in mph (e.g., 25→45 = 20)
      risk_unsignalized_xwalk     Yes/No — crosswalk WITHOUT signal nearby
      risk_school_exposure        0-100 — school proximity × enrollment weight
      risk_bridge_condition       0-100 — bridge condition + age composite
      risk_departure_curve        Yes/No — guard rail OR curve_class > 0 OR curvature > 1.5
      risk_departure_score        0-100 — composite departure risk from multiple signals
    """
    print("    Risk indicators...")
    n = len(df)

    # ── 1. SPEED TRANSITION ZONES ──
    speed_cols = [c for c in df.columns if c.startswith("map_speed_") and c.endswith("_count")]
    if speed_cols:
        # Extract speed values from column names once
        col_speeds = []
        valid_speed_cols = []
        for col in speed_cols:
            spd_str = col.replace("map_speed_", "").replace("_count", "")
            try:
                col_speeds.append(int(spd_str))
                valid_speed_cols.append(col)
            except ValueError:
                pass

        if valid_speed_cols:
            speed_matrix = df[valid_speed_cols].values > 0  # boolean: is this speed present?
            speed_vals = np.array(col_speeds)  # speed value per column
            distinct_speeds = speed_matrix.sum(axis=1)
            is_transition = distinct_speeds >= 2
            df["risk_speed_transition"] = np.where(is_transition, "Yes", "No")

            # Vectorized: max speed - min speed where present
            # Set non-present to NaN, then take max/min per row
            speed_present = np.where(speed_matrix, speed_vals[np.newaxis, :], np.nan)
            with np.errstate(invalid="ignore"):
                max_spd = np.nanmax(speed_present, axis=1)
                min_spd = np.nanmin(speed_present, axis=1)
                diffs = np.where(is_transition, np.nan_to_num(max_spd - min_spd, nan=0).astype(int), 0)
            df["risk_speed_transition_diff"] = diffs

            tz = is_transition.sum()
            med_diff = np.median(diffs[diffs > 0]) if (diffs > 0).sum() > 0 else 0
            print(f"      Speed transition: {tz:,} zones, median Δ={med_diff:.0f} mph")
        else:
            df["risk_speed_transition"] = "No"
            df["risk_speed_transition_diff"] = 0
    else:
        df["risk_speed_transition"] = "No"
        df["risk_speed_transition_diff"] = 0

    # ── 2. UNSIGNALIZED CROSSWALK ──
    has_xwalk = np.zeros(n, dtype=bool)
    if "map_crosswalk_count" in df.columns:
        has_xwalk |= (df["map_crosswalk_count"].values > 0)
    if "Near_PoiCrossing_100ft" in df.columns:
        has_xwalk |= (df["Near_PoiCrossing_100ft"].values == "Yes")
    
    has_signal = np.zeros(n, dtype=bool)
    if "resolved_has_signal" in df.columns:
        has_signal = (df["resolved_has_signal"].values == "Yes")
    
    unsig = has_xwalk & ~has_signal
    df["risk_unsignalized_xwalk"] = np.where(unsig, "Yes", "No")
    print(f"      Unsignalized crosswalk: {unsig.sum():,} segments "
          f"({has_xwalk.sum():,} total crosswalks, {(has_xwalk & has_signal).sum():,} signalized)")

    # ── 3. SCHOOL EXPOSURE SCORE (0-100) ──
    # Combines: proximity (closer=worse) × enrollment (more students=worse) × school sign presence
    scores = np.zeros(n, dtype=int)
    
    near_school = np.zeros(n, dtype=bool)
    if "Near_School_1500ft" in df.columns:
        near_school = (df["Near_School_1500ft"].values == "Yes")
    
    if near_school.sum() > 0:
        # Distance factor: 0-500ft=40pts, 500-1000ft=25pts, 1000-1500ft=15pts
        dist_pts = np.zeros(n, dtype=int)
        if "nearest_school_dist_ft" in df.columns:
            dist = pd.to_numeric(df["nearest_school_dist_ft"], errors="coerce").fillna(9999).values
            dist_pts = np.where(dist <= 500, 40,
                       np.where(dist <= 1000, 25,
                       np.where(dist <= 1500, 15, 0)))

        # Enrollment factor: >1000=30pts, 500-1000=20pts, <500=10pts
        enr_pts = np.zeros(n, dtype=int)
        if "nearest_school_enrollment" in df.columns:
            enr = pd.to_numeric(df["nearest_school_enrollment"], errors="coerce").fillna(0).values
            enr_pts = np.where(enr > 1000, 30,
                      np.where(enr > 500, 20,
                      np.where(enr > 0, 10, 0)))

        # School zone sign bonus: +20pts
        sign_pts = np.zeros(n, dtype=int)
        if "map_school_zone" in df.columns:
            sign_pts = np.where(df["map_school_zone"].values == "Yes", 20, 0)

        # Multi-school bonus: +10 per additional school
        multi_pts = np.zeros(n, dtype=int)
        if "school_count_1500ft" in df.columns:
            cnt = pd.to_numeric(df["school_count_1500ft"], errors="coerce").fillna(0).values
            multi_pts = np.minimum((cnt - 1).clip(0) * 10, 30).astype(int)

        scores = np.minimum(dist_pts + enr_pts + sign_pts + multi_pts, 100)
        scores = np.where(near_school, scores, 0)
    
    df["risk_school_exposure"] = scores
    high = (scores >= 60).sum()
    print(f"      School exposure: {high:,} high-risk (≥60), "
          f"{(scores > 0).sum():,} any exposure")

    # ── 4. BRIDGE CONDITION SCORE (0-100) ──
    # Lower = better condition. Higher = worse/riskier.
    bridge_risk = np.zeros(n, dtype=int)
    near_bridge = np.zeros(n, dtype=bool)
    if "Near_Bridge_500ft" in df.columns:
        near_bridge = (df["Near_Bridge_500ft"].values == "Yes")
    
    if near_bridge.sum() > 0:
        # Condition: Poor=60, Fair=30, Good=10, Unknown=20
        cond_pts = np.full(n, 0, dtype=int)
        if "nearest_bridge_condition" in df.columns:
            cond = df["nearest_bridge_condition"].astype(str).str.strip().str.lower().values
            cond_pts = np.where(cond == "poor", 60,
                       np.where(cond == "fair", 30,
                       np.where(cond == "good", 10,
                       np.where((cond == "unknown") | (cond == ""), 20, 15))))

        # Age: pre-1940=30, 1940-1960=20, 1960-1990=10, post-1990=5
        age_pts = np.zeros(n, dtype=int)
        if "nearest_bridge_year_built" in df.columns:
            yr = pd.to_numeric(df["nearest_bridge_year_built"], errors="coerce").fillna(0).values
            age_pts = np.where((yr > 0) & (yr < 1940), 30,
                      np.where((yr >= 1940) & (yr < 1960), 20,
                      np.where((yr >= 1960) & (yr < 1990), 10,
                      np.where(yr >= 1990, 5, 15))))

        # Width penalty: narrow bridges (< 8m / 26ft) = +10
        width_pts = np.zeros(n, dtype=int)
        if "nearest_bridge_width_m" in df.columns:
            w = pd.to_numeric(df["nearest_bridge_width_m"], errors="coerce").fillna(0).values
            width_pts = np.where((w > 0) & (w < 8), 10, 0)
        elif "nearest_bridge_width_ft" in df.columns:
            w = pd.to_numeric(df["nearest_bridge_width_ft"], errors="coerce").fillna(0).values
            width_pts = np.where((w > 0) & (w < 26), 10, 0)

        bridge_risk = np.minimum(cond_pts + age_pts + width_pts, 100)
        bridge_risk = np.where(near_bridge, bridge_risk, 0)

    df["risk_bridge_condition"] = bridge_risk
    poor = (bridge_risk >= 70).sum()
    print(f"      Bridge risk: {poor:,} high-risk (≥70), "
          f"{(bridge_risk > 0).sum():,} any risk")

    # ── 5. ROAD DEPARTURE RISK ──
    # Multiple signals: guard rail, curve class, high curvature, grade class
    depart_signals = np.zeros(n, dtype=int)

    # Guard rail from Mapillary
    if "map_guard_rail" in df.columns:
        depart_signals += (df["map_guard_rail"].values == "Yes").astype(int)

    # HPMS curve class (>0 means curved road)
    if "hpms_curve_class" in df.columns:
        curve = pd.to_numeric(df["hpms_curve_class"], errors="coerce").fillna(0)
        depart_signals += (curve > 0).astype(int).values

    # HPMS grade class (>0 means graded road)
    if "hpms_grade_class" in df.columns:
        grade = pd.to_numeric(df["hpms_grade_class"], errors="coerce").fillna(0)
        depart_signals += (grade > 0).astype(int).values

    # High curvature from OSM (>1.5 = notably curved)
    if "curvature" in df.columns:
        curv = pd.to_numeric(df["curvature"], errors="coerce").fillna(1.0)
        depart_signals += (curv > 1.5).astype(int).values

    # Turn/curve warning signs from Mapillary
    if "map_turn_warning" in df.columns:
        depart_signals += (df["map_turn_warning"].values == "Yes").astype(int)
    if "map_winding_road" in df.columns:
        depart_signals += (df["map_winding_road"].values == "Yes").astype(int)

    df["risk_departure_curve"] = np.where(depart_signals > 0, "Yes", "No")
    
    # Score: 0-100 based on signal count + speed
    depart_score = np.minimum(depart_signals * 25, 75).astype(int)
    # High speed bonus: if speed > 45 and departure risk, add 25
    if "resolved_speed_limit" in df.columns:
        spd = pd.to_numeric(df["resolved_speed_limit"], errors="coerce").fillna(0)
        high_speed_curve = (spd > 45) & (depart_signals > 0)
        depart_score = np.where(high_speed_curve, np.minimum(depart_score + 25, 100), depart_score)
    
    df["risk_departure_score"] = depart_score
    has_risk = (depart_signals > 0).sum()
    high_risk = (depart_score >= 50).sum()
    print(f"      Departure risk: {has_risk:,} flagged, {high_risk:,} high (≥50)")

    print(f"    Risk indicators complete: 7 new columns")


# ═══════════════════════════════════════════════════════════════
#  CURVE ANALYSIS (for frontend curve-crash feature)
# ═══════════════════════════════════════════════════════════════

def compute_curve_analysis(df):
    """
    Comprehensive curve identification and risk scoring for crash analysis.
    
    Uses 4 independent signals:
      1. OSM curvature (geometric angular deflection — most granular)
      2. HPMS curve_class (federal inventory — not always populated)
      3. Mapillary W1-x signs (Turn, Curve, Winding — DOT placed signs)
      4. Mapillary advisory speed near curve signs
    
    Curve Classification (based on OSM curvature ratio):
      curvature = max(length_ratio, 1 + angular_deflection)
      1.0      = perfectly straight
      1.0-1.05 = straight (negligible deviation)
      1.05-1.2 = slight curve
      1.2-1.5  = moderate curve
      1.5-2.5  = sharp curve
      >2.5     = extreme curve (switchback, ramp loop)
    
    Columns added:
      curve_class              1-5 classification (1=Straight...5=Extreme)
      curve_class_label        Straight / Slight / Moderate / Sharp / Extreme
      curve_has_warning_sign   Yes/No — Mapillary W1-x sign within 500ft
      curve_warning_sign_type  Turn / Curve / Winding Road / ""
      curve_advisory_speed     Advisory speed from Mapillary (mph) or 0
      curve_speed_differential Speed limit minus advisory speed (danger indicator)
      curve_risk_score         0-100 composite risk (curvature × speed × signs)
      curve_is_curve           Yes/No — definitive "is this a curve?" for frontend
    """
    print("    Curve analysis...")
    n = len(df)

    # ── 1. CURVE CLASSIFICATION from OSM curvature ──
    curv = pd.to_numeric(df.get("curvature", pd.Series([1.0]*n)), errors="coerce").fillna(1.0).values
    length = pd.to_numeric(df.get("length_m", pd.Series([0.0]*n)), errors="coerce").fillna(0).values
    
    # Classification thresholds (calibrated from Delaware analysis)
    # Short segments (<20m) often have artificially high curvature — suppress
    effective_curv = np.where(length < 20, 1.0, curv)
    
    classes = np.where(effective_curv <= 1.05, 1,           # Straight
              np.where(effective_curv <= 1.2, 2,            # Slight
              np.where(effective_curv <= 1.5, 3,            # Moderate
              np.where(effective_curv <= 2.5, 4,            # Sharp
              5))))                                          # Extreme
    
    labels = np.where(classes == 1, "Straight",
             np.where(classes == 2, "Slight",
             np.where(classes == 3, "Moderate",
             np.where(classes == 4, "Sharp",
             "Extreme"))))
    
    df["curve_class"] = classes
    df["curve_class_label"] = labels
    
    for cls, lbl in [(1,"Straight"),(2,"Slight"),(3,"Moderate"),(4,"Sharp"),(5,"Extreme")]:
        cnt = (classes == cls).sum()
        print(f"      Class {cls} ({lbl:8s}): {cnt:>7,} ({cnt/n*100:.1f}%)")

    # ── 2. HPMS CURVE CLASS ENRICHMENT ──
    # When HPMS has curve_class > 0, it confirms/upgrades the classification
    hpms_curve = np.zeros(n, dtype=int)
    if "hpms_curve_class" in df.columns:
        hpms_curve = pd.to_numeric(df["hpms_curve_class"], errors="coerce").fillna(0).astype(int).values
        hpms_confirmed = (hpms_curve > 0).sum()
        if hpms_confirmed > 0:
            # HPMS curve_class: 1=A, 2=B, 3=C, 4=D, 5=E (increasing curvature)
            # If HPMS says it's curved but OSM says straight, upgrade
            upgrade = (hpms_curve >= 3) & (classes <= 2)
            classes[upgrade] = 3  # At least Moderate
            labels[upgrade] = "Moderate"
            df["curve_class"] = classes
            df["curve_class_label"] = labels
            print(f"      HPMS upgrades: {upgrade.sum():,} segments promoted to Moderate+")

    # ── 3. MAPILLARY CURVE WARNING SIGNS ──
    # W1-1=Turn, W1-2=Curve, W1-3=Reverse Turn, W1-4=Reverse Curve,
    # W1-5=Winding Road, W1-6=Large Arrow, W1-7=Double Arrow, W1-8=Chevron
    df["curve_has_warning_sign"] = "No"
    df["curve_warning_sign_type"] = ""
    
    # Check existing Mapillary columns
    sign_mapping = [
        ("map_turn_warning", "Turn"),
        ("map_curve_warning", "Curve"),
        ("map_winding_road", "Winding Road"),
    ]
    
    for col, sign_type in sign_mapping:
        if col in df.columns:
            has_sign = df[col].astype(str).values == "Yes"
            df.loc[has_sign, "curve_has_warning_sign"] = "Yes"
            # Only set type if not already set (first match wins)
            mask = has_sign & (df["curve_warning_sign_type"] == "")
            df.loc[mask, "curve_warning_sign_type"] = sign_type
    
    sign_count = (df["curve_has_warning_sign"] == "Yes").sum()
    print(f"      Warning signs: {sign_count:,} segments with W1-x signs")
    
    # Signs confirm curves — if sign present but class is Straight, upgrade
    sign_upgrade = (df["curve_has_warning_sign"] == "Yes") & (df["curve_class"] <= 2)
    if sign_upgrade.sum() > 0:
        df.loc[sign_upgrade, "curve_class"] = 3
        df.loc[sign_upgrade, "curve_class_label"] = "Moderate"
        print(f"      Sign upgrades: {sign_upgrade.sum():,} segments promoted (sign proves curve)")

    # ── 4. ADVISORY SPEED ON CURVES ──
    # Speed signs near curve warnings indicate advisory curve speed
    # Lower advisory speed = tighter curve
    df["curve_advisory_speed"] = 0
    df["curve_speed_differential"] = 0
    
    # Find segments with both a curve sign and a speed sign nearby
    # Use per-speed columns: if segment has curve sign AND speed sign count > 0
    has_curve_sign = df["curve_has_warning_sign"] == "Yes"
    
    if has_curve_sign.sum() > 0:
        speed_cols = sorted([c for c in df.columns if c.startswith("map_speed_") and c.endswith("_count")])
        
        for col in speed_cols:
            spd_str = col.replace("map_speed_", "").replace("_count", "")
            try:
                spd_val = int(spd_str)
            except ValueError:
                continue
            
            if col in df.columns:
                # Segments with curve sign AND this speed sign
                has_both = has_curve_sign & (pd.to_numeric(df[col], errors="coerce").fillna(0) > 0)
                if has_both.sum() > 0:
                    # Advisory speed = lowest speed sign near a curve sign
                    current = df.loc[has_both, "curve_advisory_speed"]
                    # Only set if not already set or if this speed is lower
                    should_set = has_both & ((df["curve_advisory_speed"] == 0) | (spd_val < df["curve_advisory_speed"]))
                    df.loc[should_set, "curve_advisory_speed"] = spd_val
    
    # Speed differential = posted speed - advisory speed
    has_advisory = df["curve_advisory_speed"] > 0
    if has_advisory.sum() > 0:
        posted = pd.to_numeric(df.get("resolved_speed_limit", 0), errors="coerce").fillna(0)
        advisory = df["curve_advisory_speed"]
        diff = posted - advisory
        df.loc[has_advisory, "curve_speed_differential"] = diff[has_advisory].clip(0).astype(int)
        
        advisory_count = has_advisory.sum()
        med_diff = df.loc[has_advisory & (diff > 0), "curve_speed_differential"].median()
        print(f"      Advisory speed: {advisory_count:,} segments, "
              f"median differential: {med_diff:.0f} mph")

    # ── 5. CURVE RISK SCORE (0-100) ──
    # Components:
    #   Curvature severity: 0-40 points
    #   Speed factor: 0-30 points (higher speed on curve = worse)
    #   Warning sign absence: 0-15 points (curve without sign = unexpected)
    #   Multi-signal confirmation: 0-15 points
    
    risk = np.zeros(n, dtype=int)
    
    # Curvature severity (from classification)
    curv_pts = np.where(classes <= 1, 0,
               np.where(classes == 2, 10,
               np.where(classes == 3, 20,
               np.where(classes == 4, 30, 40))))
    risk += curv_pts
    
    # Speed factor (resolved_speed_limit on curved segments)
    speed = pd.to_numeric(df.get("resolved_speed_limit", 0), errors="coerce").fillna(0).values
    is_curve = classes >= 3  # Moderate or sharper
    speed_pts = np.where(~is_curve, 0,
                np.where(speed >= 55, 30,
                np.where(speed >= 45, 25,
                np.where(speed >= 35, 15,
                np.where(speed >= 25, 10, 5)))))
    risk += speed_pts
    
    # Warning sign absence (curve exists but no DOT warning sign)
    no_warning = is_curve & (df["curve_has_warning_sign"].values == "No")
    risk += np.where(no_warning, 15, 0)
    
    # Multi-signal confirmation (curvature + guard rail + warning sign all agree)
    multi_signals = np.zeros(n, dtype=int)
    multi_signals += (classes >= 3).astype(int)  # OSM curvature
    multi_signals += (df["curve_has_warning_sign"].values == "Yes").astype(int)  # Mapillary sign
    if "map_guard_rail" in df.columns:
        multi_signals += (df["map_guard_rail"].values == "Yes").astype(int)
    if "hpms_curve_class" in df.columns:
        multi_signals += (hpms_curve > 0).astype(int)
    risk += np.where(multi_signals >= 3, 15, np.where(multi_signals == 2, 10, 0))
    
    risk = np.minimum(risk, 100)
    df["curve_risk_score"] = risk
    
    # ── 6. DEFINITIVE CURVE FLAG (for frontend) ──
    # A segment "is a curve" if ANY of these are true:
    #   - OSM curvature > 1.2 AND length > 20m (geometric proof)
    #   - HPMS curve_class > 0 (federal confirmation)
    #   - Mapillary curve warning sign present (DOT placed sign)
    #   - curve_class >= 3 (our classification says Moderate+)
    
    is_curve_definitive = (
        ((effective_curv > 1.2) & (length > 20)) |  # Geometry
        (hpms_curve > 0) |                           # Federal
        (df["curve_has_warning_sign"].values == "Yes")  # Sign
    )
    df["curve_is_curve"] = np.where(is_curve_definitive, "Yes", "No")
    
    curve_count = is_curve_definitive.sum()
    high_risk = (risk >= 50).sum()
    print(f"      Definitive curves: {curve_count:,} ({curve_count/n*100:.1f}%)")
    print(f"      High risk (≥50):   {high_risk:,}")
    print(f"    Curve analysis complete: 8 new columns")


# ═══════════════════════════════════════════════════════════════
#  TRAFFIC ENGINEERING METRICS (from Mapillary + multi-source)
# ═══════════════════════════════════════════════════════════════

def compute_traffic_engineering_metrics(df):
    """
    Derive traffic-engineering-relevant safety metrics from Mapillary,
    HPMS, OSM, and federal data. These are analytical columns for engineers.

    PEDESTRIAN SAFETY (5 columns):
      te_ped_signal_present       Yes/No — dedicated pedestrian signal head
      te_ped_unlit_crosswalk      Yes/No — crosswalk without street lighting
      te_ped_high_speed_xwalk     Yes/No — crosswalk on ≥40mph road
      te_ped_unprotected_xwalk    Yes/No — crosswalk without signal or stop sign
      te_ped_risk_score           0-100 composite pedestrian risk

    INTERSECTION CONTROL (4 columns):
      te_int_control_type         Signal/Stop/Yield/RR Crossing/None
      te_int_control_conflict     Yes/No — conflicting signs (stop+signal)
      te_int_stop_has_marking     Yes/No — stop sign has stop line pavement marking
      te_int_sources_agree        0-4 count of agreeing sources

    SPEED MANAGEMENT (4 columns):
      te_speed_transition         Yes/No — 2+ speed limits within 500ft
      te_speed_max_diff           Max speed differential (mph)
      te_speed_posted_vs_design   Posted speed minus HPMS design speed
      te_speed_mismatch           Yes/No — posted > design speed (overposted)

    LIGHTING (3 columns):
      te_lighting_density         None/Sparse/Adequate/Dense (lights per segment)
      te_lighting_gap             Yes/No — nearby segments lit but this one dark
      te_lit_intersection         Yes/No — intersection with lighting

    RAILROAD CROSSING (2 columns):
      te_rail_warning_coverage    Full/Partial/None — Mapillary sign + federal data
      te_rail_gates_present       Yes/No — from federal warning_level

    WRONG-WAY RISK (2 columns):
      te_wrong_way_signs          Count of DNE + one-way signs within 100ft
      te_wrong_way_on_divided     Yes/No — DNE sign on divided highway (ramp protection)
    """
    print("    Traffic engineering metrics...")
    n = len(df)
    cols_added = 0

    # ══════════════════════════════════════════════════════════
    #  PEDESTRIAN SAFETY
    # ══════════════════════════════════════════════════════════

    # Dedicated ped signal
    has_ped_sig = np.zeros(n, dtype=bool)
    if "map_signal_count_500ft" in df.columns and "map_signal_heads" in df.columns:
        heads = df["map_signal_heads"].astype(str).values
        # Signal head "2" = pedestrian front signal
        has_ped_sig = np.isin(heads, ["2", "5"])
    df["te_ped_signal_present"] = np.where(has_ped_sig, "Yes", "No")

    # Crosswalk detection (from Mapillary markings + OSM POI)
    has_xwalk = np.zeros(n, dtype=bool)
    if "map_crosswalk_count" in df.columns:
        has_xwalk |= (pd.to_numeric(df["map_crosswalk_count"], errors="coerce").fillna(0) > 0).values
    if "Near_PoiCrossing_100ft" in df.columns:
        has_xwalk |= (df["Near_PoiCrossing_100ft"].values == "Yes")

    # Lighting detection
    has_light = np.zeros(n, dtype=bool)
    if "map_street_light_count" in df.columns:
        has_light |= (pd.to_numeric(df["map_street_light_count"], errors="coerce").fillna(0) > 0).values
    if "lit" in df.columns:
        has_light |= (df["lit"].astype(str).str.lower().values == "yes")

    # Signal/stop detection
    has_signal = np.zeros(n, dtype=bool)
    if "resolved_has_signal" in df.columns:
        has_signal = (df["resolved_has_signal"].values == "Yes")
    has_stop = np.zeros(n, dtype=bool)
    if "map_stop_sign" in df.columns:
        has_stop = (df["map_stop_sign"].values == "Yes")

    # Speed
    speed = pd.to_numeric(df.get("resolved_speed_limit", 0), errors="coerce").fillna(0).values

    # Unlit crosswalk
    df["te_ped_unlit_crosswalk"] = np.where(has_xwalk & ~has_light, "Yes", "No")
    # High speed crosswalk (≥40mph — FHWA considers 40+ high-speed for ped safety)
    df["te_ped_high_speed_xwalk"] = np.where(has_xwalk & (speed >= 40), "Yes", "No")
    # Unprotected crosswalk (no signal, no stop sign)
    df["te_ped_unprotected_xwalk"] = np.where(has_xwalk & ~has_signal & ~has_stop, "Yes", "No")

    # Composite ped risk (0-100)
    ped_risk = np.zeros(n, dtype=int)
    ped_risk += np.where(has_xwalk, 20, 0)                    # Base: crosswalk present
    ped_risk += np.where(has_xwalk & ~has_light, 25, 0)       # Unlit
    ped_risk += np.where(has_xwalk & (speed >= 40), 25, 0)    # High speed
    ped_risk += np.where(has_xwalk & ~has_signal & ~has_stop, 15, 0)  # Unprotected
    ped_risk += np.where(has_xwalk & ~has_ped_sig & has_signal, 10, 0)  # Signal but no ped phase
    ped_risk += np.where(has_xwalk & (speed >= 50), 5, 0)     # Extra for very high speed
    df["te_ped_risk_score"] = np.minimum(ped_risk, 100)

    unlit = (df["te_ped_unlit_crosswalk"] == "Yes").sum()
    hi_spd = (df["te_ped_high_speed_xwalk"] == "Yes").sum()
    unp = (df["te_ped_unprotected_xwalk"] == "Yes").sum()
    hi_risk = (ped_risk >= 60).sum()
    print(f"      Ped safety: {has_xwalk.sum():,} crosswalks, {unlit:,} unlit, "
          f"{hi_spd:,} high-speed, {unp:,} unprotected, {hi_risk:,} high-risk")
    cols_added += 5

    # ══════════════════════════════════════════════════════════
    #  INTERSECTION CONTROL
    # ══════════════════════════════════════════════════════════

    has_yield = np.zeros(n, dtype=bool)
    if "map_yield_sign" in df.columns:
        has_yield = (df["map_yield_sign"].values == "Yes")

    has_rail_near = np.zeros(n, dtype=bool)
    if "Near_RailXing_500ft" in df.columns:
        has_rail_near = (df["Near_RailXing_500ft"].values == "Yes")

    # Resolved control type (priority: signal > stop > yield > RR > none)
    ctrl = np.full(n, "None", dtype=object)
    ctrl[has_rail_near] = "RR Crossing"
    ctrl[has_yield] = "Yield"
    ctrl[has_stop] = "Stop"
    ctrl[has_signal] = "Signal"
    df["te_int_control_type"] = ctrl

    # Control conflict — stop sign AND signal at same location
    df["te_int_control_conflict"] = np.where(has_stop & has_signal, "Yes", "No")

    # Stop sign with stop line marking (properly marked intersection)
    has_stop_line = np.zeros(n, dtype=bool)
    if "map_stop_line_count" in df.columns:
        has_stop_line = (pd.to_numeric(df["map_stop_line_count"], errors="coerce").fillna(0) > 0).values
    df["te_int_stop_has_marking"] = np.where(has_stop & has_stop_line, "Yes",
                                    np.where(has_stop & ~has_stop_line, "No", ""))

    # Source agreement count
    sources = np.zeros(n, dtype=int)
    if "xval_signal_sources" in df.columns:
        sources = np.maximum(sources, pd.to_numeric(df["xval_signal_sources"], errors="coerce").fillna(0).astype(int).values)
    if "xval_stop_sign_sources" in df.columns:
        sources = np.maximum(sources, pd.to_numeric(df["xval_stop_sign_sources"], errors="coerce").fillna(0).astype(int).values)
    df["te_int_sources_agree"] = sources

    conflict = (df["te_int_control_conflict"] == "Yes").sum()
    unmarked = (df["te_int_stop_has_marking"] == "No").sum()
    print(f"      Intersection: {has_signal.sum():,} signal, {has_stop.sum():,} stop, "
          f"{has_yield.sum():,} yield, {conflict:,} conflicts, {unmarked:,} unmarked stops")
    cols_added += 4

    # ══════════════════════════════════════════════════════════
    #  SPEED MANAGEMENT
    # ══════════════════════════════════════════════════════════

    # Speed transition (already partially computed in risk indicators)
    speed_cols = [c for c in df.columns if c.startswith("map_speed_") and c.endswith("_count")
                  and c != "map_speed_sign_count_500ft"]
    if speed_cols:
        col_speeds = []
        valid_cols = []
        for col in speed_cols:
            spd_str = col.replace("map_speed_", "").replace("_count", "")
            try:
                col_speeds.append(int(spd_str))
                valid_cols.append(col)
            except ValueError:
                pass

        if valid_cols:
            speed_matrix = df[valid_cols].values > 0
            speed_vals = np.array(col_speeds)
            distinct = speed_matrix.sum(axis=1)
            is_transition = distinct >= 2

            speed_present = np.where(speed_matrix, speed_vals[np.newaxis, :], np.nan)
            with np.errstate(invalid="ignore"):
                max_spd = np.nanmax(speed_present, axis=1)
                min_spd = np.nanmin(speed_present, axis=1)
                raw_diff = np.nan_to_num(max_spd - min_spd, nan=0).astype(int)
            max_diff = np.where(is_transition, raw_diff, 0)

            df["te_speed_transition"] = np.where(is_transition, "Yes", "No")
            df["te_speed_max_diff"] = max_diff
        else:
            df["te_speed_transition"] = "No"
            df["te_speed_max_diff"] = 0
    else:
        df["te_speed_transition"] = "No"
        df["te_speed_max_diff"] = 0

    # Posted vs design speed (overposted = dangerous)
    design = pd.to_numeric(df.get("hpms_design_speed", 0), errors="coerce").fillna(0).values
    posted = speed.copy()
    diff_pd = np.where((posted > 0) & (design > 0), posted - design, 0).astype(int)
    df["te_speed_posted_vs_design"] = diff_pd
    overposted = ((posted > 0) & (design > 0) & (posted > design))
    df["te_speed_mismatch"] = np.where(overposted, "Yes", "No")

    trans = (df["te_speed_transition"] == "Yes").sum()
    mismatch = overposted.sum()
    print(f"      Speed mgmt: {trans:,} transitions, {mismatch:,} overposted (posted>design)")
    cols_added += 4

    # ══════════════════════════════════════════════════════════
    #  LIGHTING ASSESSMENT
    # ══════════════════════════════════════════════════════════

    light_count = pd.to_numeric(df.get("map_street_light_count", 0), errors="coerce").fillna(0).values
    density = np.full(n, "None", dtype=object)
    density[light_count >= 1] = "Sparse"
    density[light_count >= 3] = "Adequate"
    density[light_count >= 6] = "Dense"
    df["te_lighting_density"] = density

    # Lighting gap: intersection that SHOULD be lit but isn't
    # (nearby segments have lights, this one doesn't)
    is_int = np.zeros(n, dtype=bool)
    if "is_intersection" in df.columns:
        is_int = (df["is_intersection"].values == "Yes")
    df["te_lighting_gap"] = "No"  # Would need spatial neighbor analysis for true gap detection
    # Simple heuristic: intersection with signal but no lighting = gap
    sig_no_light = is_int & has_signal & ~has_light
    df.loc[sig_no_light, "te_lighting_gap"] = "Yes"

    # Lit intersection
    df["te_lit_intersection"] = np.where(is_int & has_light, "Yes", "No")

    lit_int = (df["te_lit_intersection"] == "Yes").sum()
    dark_sig = sig_no_light.sum()
    print(f"      Lighting: {(light_count > 0).sum():,} lit segments, "
          f"{lit_int:,} lit intersections, {dark_sig:,} signalized but unlit")
    cols_added += 3

    # ══════════════════════════════════════════════════════════
    #  RAILROAD CROSSING SAFETY
    # ══════════════════════════════════════════════════════════

    has_rr_sign = np.zeros(n, dtype=bool)
    if "map_rr_crossing_warning" in df.columns:
        has_rr_sign = (df["map_rr_crossing_warning"].values == "Yes")

    # Warning coverage: both Mapillary sign + federal = Full, one = Partial
    coverage = np.full(n, "None", dtype=object)
    coverage[has_rail_near | has_rr_sign] = "Partial"
    coverage[has_rail_near & has_rr_sign] = "Full"
    df["te_rail_warning_coverage"] = coverage

    # Gates (from federal warning_level)
    has_gates = np.zeros(n, dtype=bool)
    if "nearest_rail_xing_warning_level" in df.columns:
        wl = df["nearest_rail_xing_warning_level"].astype(str).str.lower().values
        has_gates = np.isin(wl, ["gates", "gate", "flashing lights and gates"])
    df["te_rail_gates_present"] = np.where(has_rail_near & has_gates, "Yes",
                                  np.where(has_rail_near & ~has_gates, "No", ""))

    partial = (coverage == "Partial").sum()
    full = (coverage == "Full").sum()
    gates = (df["te_rail_gates_present"] == "Yes").sum()
    print(f"      Railroad: {full:,} full coverage, {partial:,} partial, {gates:,} with gates")
    cols_added += 2

    # ══════════════════════════════════════════════════════════
    #  WRONG-WAY RISK
    # ══════════════════════════════════════════════════════════

    dne_count = np.zeros(n, dtype=int)
    if "map_do_not_enter" in df.columns:
        dne_count += (df["map_do_not_enter"].values == "Yes").astype(int)
    if "map_one_way" in df.columns:
        dne_count += (df["map_one_way"].values == "Yes").astype(int)
    df["te_wrong_way_signs"] = dne_count

    # DNE on divided highway (ramp protection)
    is_divided = np.zeros(n, dtype=bool)
    if "hpms_facility_type" in df.columns:
        ft = pd.to_numeric(df["hpms_facility_type"], errors="coerce").fillna(0).astype(int).values
        is_divided = np.isin(ft, [2, 4])  # 2=one-way divided, 4=two-way divided
    elif "resolved_facility_type" in df.columns:
        is_divided = df["resolved_facility_type"].astype(str).str.contains("Divided", case=False, na=False).values
    df["te_wrong_way_on_divided"] = np.where((dne_count > 0) & is_divided, "Yes", "No")

    ww = (dne_count > 0).sum()
    print(f"      Wrong-way: {ww:,} segments with DNE/one-way signs")
    cols_added += 2

    print(f"    Traffic engineering metrics complete: {cols_added} new columns")
