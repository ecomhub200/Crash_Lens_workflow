"""
states_registry.py — CrashLens Single Source of Truth for State Metadata
=========================================================================
Import from everywhere:
    from states_registry import STATES, get_state, get_statutory_speed

Used by: build_road_inventory.py, osm_county_download.py, mapillary_county_download.py,
         generate_hpms_data.py, generate_osm_data.py, generate_federal_data.py,
         crash_enricher.py, split.py

STATES dict:  abbr → (display_name, r2_prefix, state_fips)

SPEED DEFAULTS: Per-state statutory speed limits from IIHS/FHWA (2024).
  Last-resort fallback when HPMS, Mapillary, and OSM all lack speed data.
  Source column = "Statutory" for auditability.
  Ref: https://www.iihs.org/topics/speed/speed-limit-laws
"""

# ═══════════════════════════════════════════════════════════════
#  STATE REGISTRY
# ═══════════════════════════════════════════════════════════════

STATES = {
    "al": ("Alabama",              "alabama",              "01"),
    "ak": ("Alaska",               "alaska",               "02"),
    "az": ("Arizona",              "arizona",              "04"),
    "ar": ("Arkansas",             "arkansas",             "05"),
    "ca": ("California",           "california",           "06"),
    "co": ("Colorado",             "colorado",             "08"),
    "ct": ("Connecticut",          "connecticut",          "09"),
    "de": ("Delaware",             "delaware",             "10"),
    "dc": ("District of Columbia", "district_of_columbia", "11"),
    "fl": ("Florida",              "florida",              "12"),
    "ga": ("Georgia",              "georgia",              "13"),
    "hi": ("Hawaii",               "hawaii",               "15"),
    "id": ("Idaho",                "idaho",                "16"),
    "il": ("Illinois",             "illinois",             "17"),
    "in": ("Indiana",              "indiana",              "18"),
    "ia": ("Iowa",                 "iowa",                 "19"),
    "ks": ("Kansas",               "kansas",               "20"),
    "ky": ("Kentucky",             "kentucky",             "21"),
    "la": ("Louisiana",            "louisiana",            "22"),
    "me": ("Maine",                "maine",                "23"),
    "md": ("Maryland",             "maryland",             "24"),
    "ma": ("Massachusetts",        "massachusetts",        "25"),
    "mi": ("Michigan",             "michigan",             "26"),
    "mn": ("Minnesota",            "minnesota",            "27"),
    "ms": ("Mississippi",          "mississippi",          "28"),
    "mo": ("Missouri",             "missouri",             "29"),
    "mt": ("Montana",              "montana",              "30"),
    "ne": ("Nebraska",             "nebraska",             "31"),
    "nv": ("Nevada",               "nevada",               "32"),
    "nh": ("New Hampshire",        "new_hampshire",        "33"),
    "nj": ("New Jersey",           "new_jersey",           "34"),
    "nm": ("New Mexico",           "new_mexico",           "35"),
    "ny": ("New York",             "new_york",             "36"),
    "nc": ("North Carolina",       "north_carolina",       "37"),
    "nd": ("North Dakota",         "north_dakota",         "38"),
    "oh": ("Ohio",                 "ohio",                 "39"),
    "ok": ("Oklahoma",             "oklahoma",             "40"),
    "or": ("Oregon",               "oregon",               "41"),
    "pa": ("Pennsylvania",         "pennsylvania",         "42"),
    "ri": ("Rhode Island",         "rhode_island",         "44"),
    "sc": ("South Carolina",       "south_carolina",       "45"),
    "sd": ("South Dakota",         "south_dakota",         "46"),
    "tn": ("Tennessee",            "tennessee",            "47"),
    "tx": ("Texas",                "texas",                "48"),
    "ut": ("Utah",                 "utah",                 "49"),
    "vt": ("Vermont",              "vermont",              "50"),
    "va": ("Virginia",             "virginia",             "51"),
    "wa": ("Washington",           "washington",            "53"),
    "wv": ("West Virginia",        "west_virginia",        "54"),
    "wi": ("Wisconsin",            "wisconsin",            "55"),
    "wy": ("Wyoming",              "wyoming",              "56"),
}


def get_state(abbr):
    """Returns (display_name, r2_prefix, state_fips) or raises KeyError."""
    return STATES[abbr.lower()]


def get_all_abbrs():
    """Returns sorted list of all state abbreviations."""
    return sorted(STATES.keys())


# ═══════════════════════════════════════════════════════════════
#  STATUTORY SPEED LIMITS — All 51 states + DC
# ═══════════════════════════════════════════════════════════════
#
#  Source: IIHS Speed Limit Laws (May 2024), state vehicle codes,
#          Wikipedia "Speed limits in the United States by jurisdiction"
#
#  Format: abbr → {fc: speed_mph} for all 7 functional classes.
#  Keys match CrashLens Functional Class values exactly.
#
#  Column mapping:
#    1-Interstate           = Rural interstate statutory maximum
#    2-Freeway/Expressway   = Rural freeway/expressway (non-interstate)
#    3-Principal Arterial   = Rural divided arterial / "other roads"
#    4-Minor Arterial       = Rural undivided arterial
#    5-Major Collector      = Rural collector (often = minor arterial)
#    6-Minor Collector      = Rural 2-lane road
#    7-Local                = Residential/local (MUTCD standard = 25)

_SPEED = {
    #           Interstate  Freeway  PrncArt  MinArt  MajColl  MinColl  Local
    "al": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ak": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "az": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ar": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ca": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "co": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 40,
                "5-Major Collector": 40, "6-Minor Collector": 30, "7-Local": 25},
    "ct": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 40,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "de": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 35,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "dc": {     "1-Interstate": 55, "2-Freeway/Expressway": 45,
                "3-Principal Arterial": 30, "4-Minor Arterial": 25,
                "5-Major Collector": 25, "6-Minor Collector": 25, "7-Local": 20},
    "fl": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ga": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "hi": {     "1-Interstate": 60, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 45, "4-Minor Arterial": 35,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "id": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "il": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "in": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ia": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ks": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ky": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "la": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "me": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "md": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 40,
                "5-Major Collector": 40, "6-Minor Collector": 30, "7-Local": 25},
    "ma": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 40,
                "5-Major Collector": 40, "6-Minor Collector": 30, "7-Local": 25},
    "mi": {     "1-Interstate": 75, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "mn": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 30},
    "ms": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "mo": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "mt": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ne": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "nv": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "nh": {     "1-Interstate": 70, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 55, "4-Minor Arterial": 40,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "nj": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 40,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "nm": {     "1-Interstate": 75, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ny": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 55, "4-Minor Arterial": 40,
                "5-Major Collector": 40, "6-Minor Collector": 30, "7-Local": 25},
    "nc": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "nd": {     "1-Interstate": 75, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "oh": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ok": {     "1-Interstate": 75, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "or": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 20},
    "pa": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "ri": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 40,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "sc": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "sd": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "tn": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "tx": {     "1-Interstate": 75, "2-Freeway/Expressway": 75,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 30},
    "ut": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "vt": {     "1-Interstate": 65, "2-Freeway/Expressway": 55,
                "3-Principal Arterial": 50, "4-Minor Arterial": 40,
                "5-Major Collector": 35, "6-Minor Collector": 30, "7-Local": 25},
    "va": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "wa": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "wv": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "wi": {     "1-Interstate": 70, "2-Freeway/Expressway": 65,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
    "wy": {     "1-Interstate": 80, "2-Freeway/Expressway": 70,
                "3-Principal Arterial": 55, "4-Minor Arterial": 45,
                "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25},
}

# National fallback for unknown states
_FALLBACK = {
    "1-Interstate": 70, "2-Freeway/Expressway": 65,
    "3-Principal Arterial": 55, "4-Minor Arterial": 45,
    "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25,
}


def get_statutory_speed(fc, state_abbr=""):
    """
    Get statutory default speed (mph) for a functional class in a state.

    4-tier speed resolution in CrashLens:
      HPMS (posted) → Mapillary (sign) → OSM (maxspeed tag) → Statutory (this)

    Args:
        fc: Functional class string (e.g., "7-Local", "1-Interstate")
        state_abbr: Two-letter state abbreviation (e.g., "de", "va")

    Returns:
        Speed in mph (int). Returns 25 if FC is unknown.

    Examples:
        get_statutory_speed("1-Interstate", "tx")  → 75
        get_statutory_speed("7-Local", "de")        → 25
        get_statutory_speed("7-Local", "dc")        → 20
        get_statutory_speed("7-Local")              → 25  (national fallback)
    """
    abbr = state_abbr.lower() if state_abbr else ""
    table = _SPEED.get(abbr, _FALLBACK)
    return table.get(fc, 25)


def get_speed_table(state_abbr):
    """Get complete speed table for a state (all 7 functional classes)."""
    abbr = state_abbr.lower() if state_abbr else ""
    return dict(_SPEED.get(abbr, _FALLBACK))
