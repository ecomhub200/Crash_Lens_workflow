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
#  STATUTORY SPEED LIMITS (per-state, per-area-type)
# ═══════════════════════════════════════════════════════════════
#
#  Source: IIHS Speed Limit Laws (May 2024), state vehicle codes,
#          Wikipedia "Speed limits in the United States by jurisdiction"
#
#  Format: abbr → { "rural": {fc: speed_mph}, "urban": {fc: speed_mph} }
#  "suburban" uses rural speeds for FC 1-3 and urban speeds for FC 4-7.
#  Keys match CrashLens Functional Class values exactly.
#
#  Rural = statutory max for unposted roads outside business/residential districts
#  Urban = statutory default inside business/residential/municipal limits
#
#  When area_type is not provided, rural (higher) values are used as fallback
#  to avoid understating speed on unclassified roads.

def _make_speed_entry(ri, rf, rpa, rma, rmc, rmcl, rl, ui, uf, upa, uma, umc, umcl, ul):
    """Helper: build rural+urban speed dict from positional args."""
    return {
        "rural": {
            "1-Interstate": ri, "2-Freeway/Expressway": rf,
            "3-Principal Arterial": rpa, "4-Minor Arterial": rma,
            "5-Major Collector": rmc, "6-Minor Collector": rmcl, "7-Local": rl,
        },
        "urban": {
            "1-Interstate": ui, "2-Freeway/Expressway": uf,
            "3-Principal Arterial": upa, "4-Minor Arterial": uma,
            "5-Major Collector": umc, "6-Minor Collector": umcl, "7-Local": ul,
        },
    }

#                       ── Rural ──────────────────────  ── Urban ──────────────────────
#                       Int  Fwy  PrA  MnA  MjC  MnC  Loc  Int  Fwy  PrA  MnA  MjC  MnC  Loc
_SPEED = {
    "al": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "ak": _make_speed_entry(65,  55,  55,  45,  45,  35,  25,   55,  45,  45,  35,  30,  25,  25),
    "az": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  35,  25,  25),
    "ar": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "ca": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "co": _make_speed_entry(75,  65,  55,  40,  40,  30,  25,   65,  55,  40,  30,  30,  25,  25),
    "ct": _make_speed_entry(65,  55,  50,  40,  35,  30,  25,   55,  45,  40,  30,  25,  25,  25),
    "de": _make_speed_entry(65,  55,  50,  50,  50,  50,  25,   55,  45,  35,  35,  25,  25,  25),
    "dc": _make_speed_entry(55,  45,  30,  25,  25,  25,  20,   55,  45,  30,  25,  25,  25,  20),
    "fl": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "ga": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "hi": _make_speed_entry(60,  55,  45,  35,  35,  30,  25,   55,  45,  35,  25,  25,  25,  25),
    "id": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "il": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   55,  55,  40,  30,  30,  25,  25),
    "in": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   55,  55,  45,  30,  30,  25,  25),
    "ia": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   55,  55,  45,  35,  30,  25,  25),
    "ks": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   65,  55,  45,  30,  30,  25,  25),
    "ky": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "la": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "me": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   55,  45,  45,  30,  25,  25,  25),
    "md": _make_speed_entry(70,  65,  55,  40,  40,  30,  25,   55,  55,  40,  30,  30,  25,  25),
    "ma": _make_speed_entry(65,  55,  50,  40,  40,  30,  25,   55,  45,  35,  30,  25,  25,  25),
    "mi": _make_speed_entry(75,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "mn": _make_speed_entry(70,  65,  55,  45,  45,  35,  30,   55,  55,  40,  30,  30,  25,  30),
    "ms": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "mo": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   60,  55,  45,  35,  30,  25,  25),
    "mt": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "ne": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   55,  55,  45,  35,  30,  25,  25),
    "nv": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "nh": _make_speed_entry(70,  55,  55,  40,  35,  30,  25,   55,  45,  40,  30,  25,  25,  25),
    "nj": _make_speed_entry(65,  55,  50,  40,  35,  30,  25,   55,  45,  35,  30,  25,  25,  25),
    "nm": _make_speed_entry(75,  65,  55,  45,  45,  35,  25,   65,  55,  45,  30,  30,  25,  25),
    "ny": _make_speed_entry(65,  55,  55,  40,  40,  30,  25,   55,  45,  35,  30,  25,  25,  25),
    "nc": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "nd": _make_speed_entry(75,  70,  55,  45,  45,  35,  25,   55,  55,  45,  35,  30,  25,  25),
    "oh": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "ok": _make_speed_entry(75,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "or": _make_speed_entry(70,  65,  55,  45,  45,  35,  20,   55,  45,  40,  30,  25,  25,  20),
    "pa": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   55,  55,  40,  35,  30,  25,  25),
    "ri": _make_speed_entry(65,  55,  50,  40,  35,  30,  25,   55,  45,  35,  25,  25,  25,  25),
    "sc": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "sd": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   55,  55,  45,  35,  30,  25,  25),
    "tn": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "tx": _make_speed_entry(75,  75,  55,  45,  45,  35,  30,   65,  55,  45,  35,  30,  25,  30),
    "ut": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "vt": _make_speed_entry(65,  55,  50,  40,  35,  30,  25,   50,  45,  35,  30,  25,  25,  25),
    "va": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "wa": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   60,  55,  40,  30,  25,  25,  25),
    "wv": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   55,  55,  45,  35,  30,  25,  25),
    "wi": _make_speed_entry(70,  65,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
    "wy": _make_speed_entry(80,  70,  55,  45,  45,  35,  25,   65,  55,  45,  35,  30,  25,  25),
}

# National fallback for unknown states
_FALLBACK = {
    "rural": {
        "1-Interstate": 70, "2-Freeway/Expressway": 65,
        "3-Principal Arterial": 55, "4-Minor Arterial": 45,
        "5-Major Collector": 45, "6-Minor Collector": 35, "7-Local": 25,
    },
    "urban": {
        "1-Interstate": 55, "2-Freeway/Expressway": 50,
        "3-Principal Arterial": 40, "4-Minor Arterial": 30,
        "5-Major Collector": 30, "6-Minor Collector": 25, "7-Local": 25,
    },
}


def get_statutory_speed(fc, state_abbr="", area_type=""):
    """
    Get statutory default speed (mph) for a functional class in a state.

    4-tier speed resolution in CrashLens:
      HPMS (posted) → Mapillary (sign) → OSM (maxspeed tag) → Statutory (this)

    Args:
        fc: Functional class string (e.g., "7-Local", "1-Interstate")
        state_abbr: Two-letter state abbreviation (e.g., "de", "va")
        area_type: "Urban", "Suburban", or "Rural" (default: rural)
            Suburban uses rural speeds for FC 1-3, urban for FC 4-7.

    Returns:
        Speed in mph (int). Returns 25 if FC is unknown.

    Examples:
        get_statutory_speed("1-Interstate", "tx")            → 75
        get_statutory_speed("7-Local", "de")                 → 25 (rural default)
        get_statutory_speed("7-Local", "de", "Urban")        → 25
        get_statutory_speed("5-Major Collector", "de", "Rural") → 50
        get_statutory_speed("5-Major Collector", "de", "Urban") → 25
        get_statutory_speed("7-Local", "dc")                 → 20
    """
    abbr = state_abbr.lower() if state_abbr else ""
    table = _SPEED.get(abbr, _FALLBACK)
    at = area_type.lower().strip() if area_type else "rural"

    if at == "urban":
        return table["urban"].get(fc, 25)
    elif at == "suburban":
        # Suburban: high-speed roads keep rural limits, local roads use urban
        fc_prefix = fc[:2] if fc else ""
        if fc_prefix in ("1-", "2-", "3-"):
            return table["rural"].get(fc, 25)
        else:
            return table["urban"].get(fc, 25)
    else:
        # Rural (default)
        return table["rural"].get(fc, 25)


def get_speed_table(state_abbr, area_type=""):
    """Get complete speed table for a state (all 7 functional classes).

    Args:
        state_abbr: Two-letter state abbreviation
        area_type: "Urban", "Suburban", or "Rural" (default: rural)

    Returns:
        dict of {fc_string: speed_mph} for all 7 FCs.
    """
    abbr = state_abbr.lower() if state_abbr else ""
    table = _SPEED.get(abbr, _FALLBACK)
    at = area_type.lower().strip() if area_type else "rural"

    if at == "urban":
        return dict(table["urban"])
    elif at == "suburban":
        result = {}
        for fc in table["rural"]:
            fc_prefix = fc[:2] if fc else ""
            if fc_prefix in ("1-", "2-", "3-"):
                result[fc] = table["rural"][fc]
            else:
                result[fc] = table["urban"][fc]
        return result
    else:
        return dict(table["rural"])
