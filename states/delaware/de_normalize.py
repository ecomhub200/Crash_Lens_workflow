#!/usr/bin/env python3
"""
de_normalize.py — CrashLens Delaware (DelDOT) Normalization Script  v2.6.4
State: Delaware | FIPS: 10 | DOT: DelDOT

Pipeline:
  Phase 1 — Column Mapping & Rename
  Phase 2 — State-Specific Post-Normalization Transforms
  Phase 3 — FIPS Resolution (hardcoded — DE has only 3 counties)
  Phase 3.5 — GPS Jurisdiction Validation (v2.6.4 — reassign mismatched crashes)
  Phase 4 — Composite Crash ID Generation (OBJECTID = de-0000001)
  Phase 5 — EPDO Scoring
  Phase 6 — Jurisdiction Ranking (24 columns: 4 scopes × 6 metrics)
  Phase 7 — Validation & Reporting (+ Fill Strategy Recommendations)
  Phase 8 — Enrichment (crash_enricher Tier 1 + Tier 2 OSM)
  → then prefix_extra_columns()

Output: {input_stem}_normalized_ranked.csv  (69 + 4 + 24 + extras)
        {input_stem}_validation_report.json

Usage:
    python de_normalize.py --input all_roads.csv
    python de_normalize.py --input all_roads.csv --output de_normalized.csv
    python de_normalize.py --input all_roads.csv --epdo vdot2024
    python de_normalize.py --input all_roads.csv --skip-if-normalized
    python de_normalize.py --input all_roads.csv --skip-enrichment
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
#  PATH RESOLUTION  (supports repo layout AND flat folder)
# ─────────────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = _SCRIPT_DIR.parent.parent
for p in [str(_REPO_ROOT), str(_SCRIPT_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)
_CACHE_DIR = _REPO_ROOT / "cache"
# Always use repo-root cache/ — create it if needed rather than falling back
# to states/delaware/cache/ which won't match CI R2 cache restore paths.
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG & CONSTANTS  (v2.6 naming: lowercase abbreviation, full word)
# ─────────────────────────────────────────────────────────────────────────────

STATE_FIPS          = "10"
STATE_ABBREVIATION  = "de"          # lowercase — NEVER uppercase
STATE_NAME          = "Delaware"
STATE_DOT           = "DelDOT"

# Contributing circumstance column (for Phase 8 flag derivation)
CIRCUMSTANCE_COL     = "PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION"
PRIVATE_PROPERTY_COL = "COLLISION ON PRIVATE PROPERTY"

GOLDEN_COLUMNS = [
    "OBJECTID", "Document Nbr", "Crash Year", "Crash Date", "Crash Military Time",
    "Crash Severity", "K_People", "A_People", "B_People", "C_People",
    "Persons Injured", "Pedestrians Killed", "Pedestrians Injured", "Vehicle Count",
    "Collision Type", "Weather Condition", "Light Condition", "Roadway Surface Condition",
    "Relation To Roadway", "Roadway Alignment", "Roadway Surface Type", "Roadway Defect",
    "Roadway Description", "Intersection Type", "Traffic Control Type", "Traffic Control Status",
    "Work Zone Related", "Work Zone Location", "Work Zone Type", "School Zone",
    "First Harmful Event", "First Harmful Event Loc",
    "Alcohol?", "Animal Related?", "Unrestrained?", "Bike?", "Distracted?", "Drowsy?",
    "Drug Related?", "Guardrail Related?", "Hitrun?", "Lgtruck?", "Motorcycle?", "Pedestrian?",
    "Speed?", "Max Speed Diff", "RoadDeparture Type", "Intersection Analysis",
    "Senior?", "Young?", "Mainline?", "Night?",
    "DOT District", "Juris Code", "Physical Juris Name", "Functional Class",
    "Facility Type", "Area Type", "SYSTEM", "VSP", "Ownership",
    "Planning District", "MPO Name", "RTE Name", "RNS MP", "Node", "Node Offset (ft)",
    "x", "y",
]

ENRICHMENT_COLUMNS = ["FIPS", "Place FIPS", "EPDO_Score", "Intersection Name"]

# Columns created by crash_enricher Tier 2/2b — NOT state-prefixed, kept as-is
# These are universal analysis columns that apply to any state.
ANALYSIS_COLUMNS = [
    # Road inventory columns protected from state prefix.
    # The road inventory enricher transfers 300+ columns — we protect them
    # by exact name here AND by prefix pattern in prefix_extra_columns().
    "AADT", "AADT_source", "Through_Lanes", "VMT_Annual",
    "Roadway Condition", "Roadway Alignment", "Roadway Description",
    "intersection_name", "intersection_degree", "is_intersection",
    "road_source", "length_m", "length_mi",
    "ri_matched", "ri_match_dist_ft", "ri_segment_id",
    # HPMS v2: frontend columns from road_data_authority.py (not prefixed)
    "Capacity", "Directional_Lanes", "Peak_Lanes",
    "Pct_Peak_Single_Unit", "Structure_Type", "Toll_Charged",
    "HOV_Type", "Climate_Zone", "Cracking_Pct",
    "ARNOLD_Route_ID", "ARNOLD_Begin_MP", "ARNOLD_End_MP",
    "HPMS_Section_Length_mi", "HPMS_Year",
]

# Prefixes for road inventory columns — never get state prefix (de_, va_, etc.)
ROAD_INVENTORY_PREFIXES = (
    "hpms_", "sdot_", "dot_", "te_", "curve_", "map_", "risk_",
    "nearest_", "Near_", "poi_", "conf_", "xval_", "geo_", "resolved_", "ri_",
)

RANKING_SCOPES  = ["District", "Juris", "PlanningDistrict", "MPO"]
RANKING_METRICS = [
    "total_crash", "total_ped_crash", "total_bike_crash",
    "total_fatal", "total_fatal_serious_injury", "total_epdo",
]

EPDO_PRESETS = {
    "hsm2010":  {"K": 462,  "A": 62, "B": 12, "C": 5,  "O": 1},
    "vdot2024": {"K": 1032, "A": 53, "B": 16, "C": 10, "O": 1},
    "fhwa2022": {"K": 975,  "A": 48, "B": 13, "C": 8,  "O": 1},
    "fhwa2025": {"K": 883,  "A": 94, "B": 21, "C": 11, "O": 1},
}
DEFAULT_EPDO_PRESET = "fhwa2025"

# ─────────────────────────────────────────────────────────────────────────────
#  FILL STRATEGY LOOKUP  (columns <20% filled → recommendations)
# ─────────────────────────────────────────────────────────────────────────────

FILL_STRATEGY_LOOKUP = {
    "Functional Class":       "✅ Auto-fill via OSM (Tier 2 Python pipeline) — or join with HPMS shapefile",
    "RTE Name":               "✅ Auto-fill via OSM nearest road (Tier 2 Python pipeline) — or reverse geocode GPS",
    "Ownership":              "✅ Auto-derived from Functional Class + jurisdiction (geo_resolver)",
    "Facility Type":          "✅ Auto-fill via OSM road tags (Tier 2) — or infer from Functional Class",
    "Area Type":              "✅ Auto-derived from Census Urban/Rural by FIPS (geo_resolver)",
    "SYSTEM":                 "✅ Auto-derived from Functional Class mapping",
    "Roadway Description":    "✅ Auto-fill via OSM lane tags (Tier 2) — or default by FC",
    "Intersection Type":      "✅ Auto-fill via OSM node analysis (Tier 2)",
    "Intersection Name":      "✅ Auto-fill via OSM cross-street names at nearest node (Tier 2)",
    "Node":                   "✅ Auto-fill via nearest OSM intersection node (Tier 2)",
    "Node Offset (ft)":       "✅ Auto-fill — distance to nearest OSM node in feet (Tier 2)",
    "Traffic Control Type":   "⚠️ Join with signal inventory from state/city DOT — OSM partial coverage",
    "Traffic Control Status":  "⚠️ Join with signal inventory — cannot derive from GPS alone",
    "Relation To Roadway":    "⚠️ Check source for location_type field — or infer from intersection proximity",
    "Roadway Alignment":      "⚠️ Compute curvature from OSM geometry — or default '1. Straight - Level'",
    "Roadway Surface Type":   "⚠️ OSM surface tag — or default '2. Blacktop, Asphalt, Bituminous'",
    "Roadway Defect":         "❌ Requires officer observation — leave empty",
    "First Harmful Event":    "⚠️ Map from 'manner of collision' or 'object struck' if available",
    "First Harmful Event Loc":"⚠️ Derive from First Harmful Event + roadway relation",
    "Max Speed Diff":         "⚠️ Join with speed limit inventory — or OSM maxspeed tag",
    "RoadDeparture Type":     "⚠️ Infer from collision type (Fixed Object-Off Road → departure)",
    "Intersection Analysis":  "⚠️ Cross-reference Intersection Type + Node Offset proximity",
    "VSP":                    "❌ Virginia-specific — leave empty for non-VA states",
    "RNS MP":                 "⚠️ Join with state DOT LRS shapefile using route name + GPS",
    "Persons Injured":        "⚠️ Join person/occupant table — or infer minimum from severity",
    "Pedestrians Killed":     "⚠️ Join person table — or infer from Pedestrian? + Severity=K",
    "Pedestrians Injured":    "⚠️ Join person table — or infer from Pedestrian? + Severity=A/B/C",
    "Vehicle Count":          "⚠️ Join vehicle table — or infer from collision type (Angle→2+, Single→1)",
    "Guardrail Related?":     "⚠️ Parse First Harmful Event for 'guardrail' — or default 'No'",
    "Lgtruck?":               "⚠️ Check vehicle table for vehicle type — or default 'No'",
    "Senior?":                "⚠️ Check person table for age ≥65 — or default 'No'",
    "Young?":                 "⚠️ Check person table for age ≤20 — or default 'No'",
    "Unrestrained?":          "⚠️ Invert seatbelt field if available — or check person table",
}

TIER2_COLUMNS = {
    "Functional Class", "RTE Name", "Ownership", "Facility Type",
    "Roadway Description", "Intersection Type", "Intersection Name",
    "Node", "Node Offset (ft)", "SYSTEM",
    # NOTE: "Area Type" is intentionally NOT here — it is resolved from
    # DE_COUNTIES in Phase 3 (hardcoded by county), not from OSM.
}

MANDATORY_COLUMNS = {"Physical Juris Name", "x", "y", "Crash Severity", "FIPS"}

# ─────────────────────────────────────────────────────────────────────────────
#  DELAWARE GEOGRAPHY  (authoritative — only 3 counties)
# ─────────────────────────────────────────────────────────────────────────────

DE_COUNTIES = {
    "Kent":        {"fips": "001", "geoid": "10001", "district": "Central District", "mpo": "Dover/Kent County MPO",         "area_type": "Urban",  "centlat": 39.097088, "centlon": -75.502982},
    "New Castle":  {"fips": "003", "geoid": "10003", "district": "North District",   "mpo": "WILMAPCO",                      "area_type": "Urban",  "centlat": 39.575915, "centlon": -75.644132},
    "Sussex":      {"fips": "005", "geoid": "10005", "district": "South District",   "mpo": "Salisbury-Wicomico MPO",        "area_type": "Rural",  "centlat": 38.673227, "centlon": -75.337024},
}

DE_COUNTY_CODE_MAP = {"K": "Kent", "N": "New Castle", "S": "Sussex"}

# ─────────────────────────────────────────────────────────────────────────────
#  VALUE MAPPING TABLES  (Delaware → CrashLens Standard)
# ─────────────────────────────────────────────────────────────────────────────

MAP_SEVERITY = {
    "fatality crash":           "K",
    "fatal crash":              "K",
    "personal injury crash":    "A",
    "injury crash":             "A",
    "personal injury":          "A",
    "property damage only":     "O",
    "property damage":          "O",
    "pdo":                      "O",
    "non-reportable":           "O",
    "non reportable":           "O",
}

MAP_COLLISION_TYPE = {
    "front to rear":                          "1. Rear End",
    "angle":                                  "2. Angle",
    "front to front":                         "3. Head On",
    "sideswipe, same direction":              "4. Sideswipe - Same Direction",
    "sideswipe, opposite direction":          "5. Sideswipe - Opposite Direction",
    "not a collision between two vehicles":   "8. Non-Collision",
    "rear to rear":                           "16. Other",
    "rear to side":                           "16. Other",
    "other":                                  "16. Other",
    "unknown":                                "Not Provided",
}

MAP_WEATHER = {
    "clear":                                    "1. No Adverse Condition (Clear/Cloudy)",
    "cloudy":                                   "1. No Adverse Condition (Clear/Cloudy)",
    "fog, smog, smoke":                         "3. Fog",
    "rain":                                     "5. Rain",
    "snow":                                     "6. Snow",
    "sleet, hail (freezing rain or drizzle)":   "7. Sleet/Hail",
    "blowing sand, soil, dirt":                 "10. Blowing Sand, Soil, Dirt, or Snow",
    "blowing snow":                             "10. Blowing Sand, Soil, Dirt, or Snow",
    "severe crosswinds":                        "11. Severe Crosswinds",
    "other":                                    "9. Other",
    "unknown":                                  "Not Applicable",
}

MAP_LIGHT = {
    "dawn":                    "1. Dawn",
    "daylight":                "2. Daylight",
    "dusk":                    "3. Dusk",
    "dark-lighted":            "4. Darkness - Road Lighted",
    "dark-not lighted":        "5. Darkness - Road Not Lighted",
    "dark-unknown lighting":   "6. Darkness - Unknown Road Lighting",
    "other":                   "7. Unknown",
    "unknown":                 "7. Unknown",
}

MAP_ROAD_SURFACE = {
    "dry":                      "1. Dry",
    "wet":                      "2. Wet",
    "snow":                     "3. Snowy",
    "ice/frost":                "4. Icy",
    "oil":                      "6. Oil/Other Fluids",
    "mud, dirt, gravel":        "11. Sand, Dirt, Gravel",
    "sand":                     "11. Sand, Dirt, Gravel",
    "slush":                    "10. Slush",
    "water (standing, moving)": "9. Water (Standing, Moving)",
    "other":                    "7. Other",
    "unknown":                  "Not Applicable",
}

MAP_WORK_ZONE_LOCATION = {
    "advance warning area":                    "1. Advance Warning Area",
    "before the first work zone warning sign": "1. Advance Warning Area",
    "transition area":                         "2. Transition Area",
    "activity area":                           "3. Activity Area",
    "termination area":                        "4. Termination Area",
}

MAP_WORK_ZONE_TYPE = {
    "lane closure":               "1. Lane Closure",
    "lane shift/crossover":       "2. Lane Shift/Crossover",
    "work on shoulder or median": "3. Work on Shoulder or Median",
    "intermittent or moving work":"4. Intermittent or Moving Work",
    "other":                      "5. Other",
}

MAP_SCHOOL_BUS_TO_ZONE = {
    "no":                       "3. No",
    "yes, directly involved":   "2. Yes - With School Activity",
    "yes, indirectly involved": "1. Yes",
}

YN_COLUMNS = [
    ("PEDESTRIAN INVOLVED",  "Pedestrian?"),
    ("ALCOHOL INVOLVED",     "Alcohol?"),
    ("DRUG INVOLVED",        "Drug Related?"),
    ("MOTORCYCLE INVOLVED",  "Motorcycle?"),
    ("BICYCLED INVOLVED",    "Bike?"),
]

NIGHT_KEYWORDS = {"dark-lighted", "dark-not lighted", "dark-unknown lighting", "dusk", "dawn"}

# ─────────────────────────────────────────────────────────────────────────────
#  DETECTION: Is this file already normalized?
# ─────────────────────────────────────────────────────────────────────────────

def is_already_normalized(columns: list[str]) -> bool:
    required = {"Document Nbr", "Crash Severity", "Physical Juris Name", "x", "y"}
    return required.issubset(set(columns))


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 1 — COLUMN MAPPING & RENAME
# ─────────────────────────────────────────────────────────────────────────────

COLUMN_RENAMES = {
    "CRASH DATETIME":                     "Crash Date",
    "YEAR":                               "Crash Year",
    "LATITUDE":                           "y",
    "LONGITUDE":                          "x",
    "COUNTY NAME":                        "Physical Juris Name",
    "COUNTY CODE":                        "Juris Code",
    "PEDESTRIAN INVOLVED":               "Pedestrian?",
    "BICYCLED INVOLVED":                 "Bike?",
    "ALCOHOL INVOLVED":                  "Alcohol?",
    "DRUG INVOLVED":                     "Drug Related?",
    "MOTORCYCLE INVOLVED":               "Motorcycle?",
    "SEATBELT USED":                     "Unrestrained?",
    "WEATHER 1 DESCRIPTION":             "Weather Condition",
    "LIGHTING CONDITION DESCRIPTION":    "Light Condition",
    "ROAD SURFACE DESCRIPTION":          "Roadway Surface Condition",
    "MANNER OF IMPACT DESCRIPTION":      "Collision Type",
    "SCHOOL BUS INVOLVED DESCRIPTION":   "School Zone",
    "WORK ZONE":                         "Work Zone Related",
    "WORK ZONE LOCATION DESCRIPTION":    "Work Zone Location",
    "WORK ZONE TYPE DESCRIPTION":        "Work Zone Type",
    "CRASH CLASSIFICATION DESCRIPTION":  "Crash Severity",
}

# Extra source columns not in the 69-column standard — kept and prefixed
EXTRA_COLUMNS = [
    "DAY OF WEEK CODE", "DAY OF WEEK DESCRIPTION",
    "CRASH CLASSIFICATION CODE",
    "COLLISION ON PRIVATE PROPERTY",
    "MANNER OF IMPACT CODE",
    "ROAD SURFACE CODE", "LIGHTING CONDITION CODE",
    "WEATHER 1 CODE", "WEATHER 2 CODE", "WEATHER 2 DESCRIPTION",
    "MOTORCYCLE HELMET USED", "BICYCLE HELMET USED",
    "SCHOOL BUS INVOLVED CODE",
    "WORK ZONE LOCATION CODE", "WORK ZONE TYPE CODE", "WORKERS PRESENT",
    "PRIMARY CONTRIBUTING CIRCUMSTANCE CODE",
    "PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION",
]


def apply_column_renames(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Delaware source columns to CrashLens standard names."""
    rename_map = {}
    src_cols = {c.strip().upper(): c for c in df.columns}

    for src, tgt in COLUMN_RENAMES.items():
        if src in src_cols:
            rename_map[src_cols[src]] = tgt

    df = df.rename(columns=rename_map)

    for col in GOLDEN_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 2 — STATE-SPECIFIC TRANSFORMS
# ─────────────────────────────────────────────────────────────────────────────

_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def parse_delaware_datetime(raw: str) -> tuple[str, str, str]:
    """
    Parse DelDOT datetime.  Handles multiple formats returned by Socrata:
      Format A:  '2015 Jul 17 03:15:00 PM'   (legacy text export)
      Format B:  '2015-07-17T15:15:00.000'    (ISO / SODA JSON)
      Format C:  '07/17/2015 03:15:00 PM'     (US locale CSV)
      Format D:  '2015 Jul 17 PM'             (missing time, AM/PM at pos 3)
    """
    if not raw or not raw.strip():
        return "", "", ""

    try:
        s = raw.strip()

        # ── Format B: ISO datetime (2015-07-17T15:15:00.000) ──
        if "T" in s and "-" in s[:10]:
            try:
                date_part, time_part = s.split("T", 1)
                ymd = date_part.split("-")
                year, mon, day = ymd[0], ymd[1], ymd[2]
                hms = time_part.split(".")[0].split(":")
                hour = int(hms[0]) if hms else 0
                minute = hms[1] if len(hms) > 1 else "00"
                mil_time = f"{hour:02d}{minute}"
                date_str = f"{int(mon)}/{int(day)}/{year}"
                return date_str, mil_time, year
            except (ValueError, IndexError):
                pass

        # ── Format C: '07/17/2015 03:15:00 PM' or '3/22/2026 3:15 PM' ──
        if "/" in s.split()[0]:
            try:
                date_token = s.split()[0]
                mdy = date_token.split("/")
                mon, day, year = mdy[0], mdy[1], mdy[2]
                rest = s.split(None, 1)
                hour, minute, ampm = 0, "00", ""
                if len(rest) > 1:
                    time_and_ampm = rest[1].split()
                    t_parts = time_and_ampm[0].split(":")
                    hour = int(t_parts[0])
                    minute = t_parts[1] if len(t_parts) > 1 else "00"
                    ampm = time_and_ampm[1].upper() if len(time_and_ampm) > 1 else ""
                if ampm == "PM" and hour < 12:
                    hour += 12
                elif ampm == "AM" and hour == 12:
                    hour = 0
                mil_time = f"{hour:02d}{minute}"
                date_str = f"{int(mon)}/{int(day)}/{year}"
                return date_str, mil_time, year
            except (ValueError, IndexError):
                pass

        # ── Format A/D: '2015 Jul 17 03:15:00 PM' or '2015 Jul 17 PM' ──
        parts = s.split()
        if len(parts) < 3:
            return s, "", ""

        year = parts[0]
        mon  = _MONTHS.get(parts[1].lower(), "01")
        day  = parts[2]

        hour, minute, ampm = 0, "00", ""
        if len(parts) >= 4:
            # Check if parts[3] is AM/PM (Format D: time missing)
            if parts[3].upper() in ("AM", "PM"):
                ampm = parts[3].upper()
            else:
                t_parts = parts[3].split(":")
                # Guard: t_parts[0] could be "PM" on malformed rows
                raw_hour = t_parts[0] if t_parts else "0"
                hour = int(raw_hour) if raw_hour.isdigit() else 0
                minute = t_parts[1] if len(t_parts) > 1 else "00"
                ampm = parts[4].upper() if len(parts) > 4 else ""

        if ampm == "PM" and hour < 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0

        mil_time = f"{hour:02d}{minute}"
        date_str = f"{int(mon)}/{int(day)}/{year}"
        return date_str, mil_time, year
    except Exception:
        return "", "", ""


def apply_value_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all value-level transformations using vectorised maps."""

    # ── Datetime ──
    if "Crash Date" in df.columns:
        parsed = df["Crash Date"].fillna("").apply(parse_delaware_datetime)
        df["Crash Date"]          = parsed.apply(lambda t: t[0])
        # Always 4-digit zero-padded: "240" → "0240", "21" → "0021"
        df["Crash Military Time"] = parsed.apply(lambda t: t[1]).str.zfill(4)
        year_from_dt = parsed.apply(lambda t: t[2])
        mask_no_year = df["Crash Year"].fillna("").str.strip() == ""
        df.loc[mask_no_year, "Crash Year"] = year_from_dt[mask_no_year]

    # ── Crash Year: strip commas AND .0 decimal (Socrata float/comma artifacts) ──
    if "Crash Year" in df.columns:
        df["Crash Year"] = (
            df["Crash Year"].fillna("").astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

    # ── Crash Severity → KABCO ──
    if "Crash Severity" in df.columns:
        df["Crash Severity"] = (
            df["Crash Severity"].fillna("").str.strip().str.lower()
            .map(MAP_SEVERITY).fillna("O")
        )

    # ── Collision Type ──
    if "Collision Type" in df.columns:
        df["Collision Type"] = (
            df["Collision Type"].fillna("").str.strip().str.lower()
            .map(MAP_COLLISION_TYPE).fillna("Not Provided")
        )

    # ── Weather ──
    if "Weather Condition" in df.columns:
        df["Weather Condition"] = (
            df["Weather Condition"].fillna("").str.strip().str.lower()
            .map(MAP_WEATHER).fillna("Not Applicable")
        )

    # ── Light Condition ──
    if "Light Condition" in df.columns:
        df["Light Condition"] = (
            df["Light Condition"].fillna("").str.strip().str.lower()
            .map(MAP_LIGHT).fillna("7. Unknown")
        )

    # ── Roadway Surface ──
    if "Roadway Surface Condition" in df.columns:
        df["Roadway Surface Condition"] = (
            df["Roadway Surface Condition"].fillna("").str.strip().str.lower()
            .map(MAP_ROAD_SURFACE).fillna("Not Applicable")
        )

    # ── Y / N Boolean fields → Yes / No ──
    for std_col in ["Pedestrian?", "Alcohol?", "Drug Related?", "Motorcycle?", "Bike?"]:
        if std_col in df.columns:
            df[std_col] = df[std_col].fillna("").str.strip().str.upper().map(
                {"Y": "Yes", "N": "No", "YES": "Yes", "NO": "No"}
            ).fillna("No")

    # ── Unrestrained? (inverted seatbelt) ──
    if "Unrestrained?" in df.columns:
        df["Unrestrained?"] = df["Unrestrained?"].fillna("").str.strip().str.upper().map(
            {"Y": "Belted", "N": "Unbelted"}
        ).fillna("Belted")

    # ── Work Zone Related ──
    if "Work Zone Related" in df.columns:
        df["Work Zone Related"] = df["Work Zone Related"].fillna("").str.strip().str.upper().map(
            {"Y": "1. Yes", "N": "2. No", "YES": "1. Yes", "NO": "2. No"}
        ).fillna("2. No")

    # ── Work Zone Location ──
    if "Work Zone Location" in df.columns:
        df["Work Zone Location"] = (
            df["Work Zone Location"].fillna("").str.strip().str.lower()
            .map(MAP_WORK_ZONE_LOCATION).fillna("")
        )

    # ── Work Zone Type ──
    if "Work Zone Type" in df.columns:
        df["Work Zone Type"] = (
            df["Work Zone Type"].fillna("").str.strip().str.lower()
            .map(MAP_WORK_ZONE_TYPE).fillna("")
        )

    # ── School Zone ──
    if "School Zone" in df.columns:
        df["School Zone"] = (
            df["School Zone"].fillna("").str.strip().str.lower()
            .map(MAP_SCHOOL_BUS_TO_ZONE).fillna("3. No")
        )

    # ── Night? from MAPPED Light Condition ──
    if "Light Condition" in df.columns:
        night_values = {
            "1. Dawn", "3. Dusk",
            "4. Darkness - Road Lighted",
            "5. Darkness - Road Not Lighted",
            "6. Darkness - Unknown Road Lighting",
        }
        df["Night?"] = df["Light Condition"].isin(night_values).map({True: "Yes", False: "No"})

    return df


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 3 — FIPS RESOLUTION  (hardcoded — 3 counties)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_fips(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Resolve FIPS codes for Delaware's 3 counties.
    VECTORIZED: map + fillna instead of df.apply(axis=1) (~0.05s for 566K).
    """
    fips_lookup = {}
    for county, geo in DE_COUNTIES.items():
        fips_lookup[county] = {
            "fips":            geo["fips"],
            "countyName":      county,
            "geoid":           geo["geoid"],
            "region":          geo["district"],
            "planningDistrict": geo["district"],
            "mpo":             geo["mpo"],
            "source":          "state_transform",
            "conflicts":       [],
        }

    # Normalize jurisdiction names (map county codes to names)
    juris = df["Physical Juris Name"].fillna("").astype(str).str.strip()
    juris = juris.map(lambda v: DE_COUNTY_CODE_MAP.get(v, v))

    # Build vectorized lookup dicts
    fips_map = {c: g["fips"] for c, g in DE_COUNTIES.items()}
    district_map = {c: g["district"] for c, g in DE_COUNTIES.items()}
    mpo_map = {c: g["mpo"] for c, g in DE_COUNTIES.items()}
    area_map = {c: g.get("area_type", "Rural") for c, g in DE_COUNTIES.items()}

    # Apply all at once
    df["Physical Juris Name"] = juris.where(juris.isin(DE_COUNTIES), df["Physical Juris Name"])
    df["FIPS"] = juris.map(fips_map).fillna("")
    df["Place FIPS"] = ""
    df["DOT District"] = juris.map(district_map).fillna("")
    df["Planning District"] = juris.map(district_map).fillna("")
    df["MPO Name"] = juris.map(mpo_map).fillna("")
    df["Area Type"] = juris.map(area_map).fillna("Rural")

    # Force 3-digit zero-padding
    df["FIPS"] = df["FIPS"].astype(str).str.zfill(3).replace("000", "")
    return df, fips_lookup


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 3.5 — GPS JURISDICTION VALIDATION  (v2.6.4)
#  Fallback: built-in centroid nearest-neighbor if tigerweb_pip not available
# ─────────────────────────────────────────────────────────────────────────────

def validate_gps_jurisdiction(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Phase 3.5: Cross-check crash GPS against county boundaries.

    Tier 1: BoundaryResolver (polygon PIP via geopandas.sjoin — exact)
    Tier 2: Vectorized centroid nearest-neighbor (numpy — fast fallback)

    Both are vectorized. Tier 1 is geometrically exact (polygon contains point).
    Tier 2 uses centroid distance (accurate for DE's 3 large counties).
    """
    # Tier 1: Try polygon PIP if boundary_resolver + boundaries available
    try:
        from boundary_resolver import BoundaryResolver
        resolver = BoundaryResolver(cache_dir=str(_CACHE_DIR / "boundaries"))
        if resolver.counties is not None:
            return resolver.validate_jurisdiction(
                df, state_fips=STATE_FIPS, county_dict=DE_COUNTIES)
        else:
            print("        Boundary polygons not cached — using centroid fallback")
    except ImportError:
        pass

    # Tier 2: Vectorized centroid nearest-neighbor
    centroids = {}
    for county, geo in DE_COUNTIES.items():
        if "centlat" in geo and "centlon" in geo:
            centroids[county] = (geo["centlat"], geo["centlon"])

    if not centroids:
        print("        ⚠️  No county centroids — skipping GPS validation")
        return df, {}

    county_names = list(centroids.keys())
    county_lats = np.array([centroids[c][0] for c in county_names])
    county_lons = np.array([centroids[c][1] for c in county_names])

    # Vectorized GPS extraction
    crash_lon = pd.to_numeric(df["x"], errors="coerce").fillna(0).values
    crash_lat = pd.to_numeric(df["y"], errors="coerce").fillna(0).values

    # Valid GPS mask
    valid = (crash_lat != 0) & (crash_lon != 0) & np.isfinite(crash_lat) & np.isfinite(crash_lon)

    # Vectorized distance to each county centroid (simplified — Euclidean in degrees is fine for ~50mi)
    # cos(lat) correction for longitude
    cos_lat = np.cos(np.radians(np.mean(county_lats)))
    best_county_idx = np.zeros(len(df), dtype=int)

    # Compute squared distance to each centroid (no sqrt needed for comparison)
    best_dist_sq = np.full(len(df), np.inf)
    for ci, (clat, clon) in enumerate(zip(county_lats, county_lons)):
        dlat = crash_lat - clat
        dlon = (crash_lon - clon) * cos_lat
        dist_sq = dlat * dlat + dlon * dlon
        closer = dist_sq < best_dist_sq
        best_dist_sq[closer] = dist_sq[closer]
        best_county_idx[closer] = ci

    # Map index to county name
    nearest_county = np.array(county_names)[best_county_idx]

    # Find mismatches
    stated_juris = df["Physical Juris Name"].fillna("").astype(str).str.strip().values
    mismatched = valid & (nearest_county != stated_juris)

    stats = {}
    total_checked = valid.sum()
    total_reassigned = mismatched.sum()

    if total_reassigned > 0:
        # Apply corrections vectorized
        for county_name in county_names:
            mask = mismatched & (nearest_county == county_name)
            if not mask.any():
                continue
            geo = DE_COUNTIES[county_name]
            df.loc[mask, "Physical Juris Name"] = county_name
            df.loc[mask, "FIPS"]                = geo["fips"]
            df.loc[mask, "DOT District"]        = geo.get("district", "")
            if "VDOT District" in df.columns:
                df.loc[mask, "VDOT District"]   = geo.get("district", "")
            df.loc[mask, "Planning District"]   = geo.get("district", "")
            df.loc[mask, "MPO Name"]            = geo.get("mpo", "")
            df.loc[mask, "Area Type"]           = geo.get("area_type", "Rural")

        # Collect stats
        for i in np.where(mismatched)[0]:
            pair_key = f"{stated_juris[i]} → {nearest_county[i]}"
            stats[pair_key] = stats.get(pair_key, 0) + 1

    df["FIPS"] = df["FIPS"].fillna("").astype(str).str.zfill(3).replace("000", "")

    if total_reassigned > 0:
        print(f"        ⚠️  GPS validation (vectorized): {total_reassigned:,} of {total_checked:,} reassigned")
        for pair, count in sorted(stats.items(), key=lambda x: -x[1])[:10]:
            print(f"           {pair}: {count:,} crashes")
    else:
        print(f"        ✅ All {total_checked:,} crashes match stated jurisdiction")

    return df, stats


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 4 — CRASH ID GENERATION  (v2.6 lowercase prefix)
# ─────────────────────────────────────────────────────────────────────────────

def generate_object_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Generate state-prefixed OBJECTID: {abbreviation}-{7-digit seq}."""
    df["OBJECTID"] = [f"{STATE_ABBREVIATION}-{i+1:07d}" for i in range(len(df))]
    return df


def generate_crash_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Document Nbr: {abbreviation}-{YYYYMMDD}-{HHMM}-{NNNNNNN}.
    VECTORIZED: pandas string ops instead of iterrows (~0.5s for 566K).
    """
    needs_id = df["Document Nbr"].fillna("").str.strip() == ""

    if needs_id.any():
        # Parse date parts vectorized
        date_str = df.loc[needs_id, "Crash Date"].fillna("").astype(str)

        # Handle MM/DD/YYYY format
        has_slash = date_str.str.contains("/", na=False)

        # Split and recombine for slash dates
        date_clean = pd.Series("00000000", index=date_str.index)
        if has_slash.any():
            parts = date_str[has_slash].str.split("/", expand=True)
            if len(parts.columns) >= 3:
                date_clean[has_slash] = (
                    parts[2].str.strip() +
                    parts[0].str.strip().str.zfill(2) +
                    parts[1].str.strip().str.zfill(2)
                )

        # Non-slash dates: strip non-digits, take first 8
        non_slash = ~has_slash & needs_id.loc[needs_id].reindex(date_str.index, fill_value=False)
        if non_slash.any():
            date_clean[non_slash] = (
                date_str[non_slash].str.replace(r"[^0-9]", "", regex=True)
                .str[:8].str.ljust(8, "0")
            )

        # Time
        time_str = (df.loc[needs_id, "Crash Military Time"]
                    .fillna("0000").astype(str).str.strip().str.ljust(4, "0"))

        # Sequence numbers
        seq = pd.Series([f"{i+1:07d}" for i in range(len(df))], index=df.index)

        # Build IDs
        new_ids = (STATE_ABBREVIATION + "-" + date_clean + "-" + time_str +
                   "-" + seq[needs_id])
        df.loc[needs_id, "Document Nbr"] = new_ids.values

    df = generate_object_ids(df)
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 5 — EPDO SCORING
# ─────────────────────────────────────────────────────────────────────────────

def compute_epdo(df: pd.DataFrame, weights: dict) -> pd.DataFrame:
    sev_map = {"K": weights["K"], "A": weights["A"], "B": weights["B"],
               "C": weights["C"], "O": weights["O"]}
    df["EPDO_Score"] = df["Crash Severity"].map(sev_map).fillna(weights["O"]).astype(int)
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 6 — JURISDICTION RANKING (24 columns)
# ─────────────────────────────────────────────────────────────────────────────

def compute_rankings(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Compute jurisdiction rankings using vectorized groupby.
    VECTORIZED: groupby + rank + merge instead of iterrows (~0.3s for 566K).
    """
    # Drop any pre-existing ranking columns (re-run safe)
    existing_rank = [c for c in df.columns if "_Rank_" in c]
    if existing_rank:
        df.drop(columns=existing_rank, inplace=True)

    # Build ranking key (FIPS preferred, fallback to Physical Juris Name)
    fips = df["FIPS"].fillna("").astype(str).str.strip()
    juris = df["Physical Juris Name"].fillna("").astype(str).str.strip()
    key = fips.where(fips != "", juris)
    df["_rank_key"] = key

    # Vectorized metric computation per jurisdiction
    agg_df = df.groupby("_rank_key", dropna=False).agg(
        total_crash=("_rank_key", "size"),
        total_ped_crash=("Pedestrian?", lambda s: s.isin(["Yes", "Y"]).sum()),
        total_bike_crash=("Bike?", lambda s: s.isin(["Yes", "Y"]).sum()),
        total_fatal=("Crash Severity", lambda s: (s == "K").sum()),
        total_fatal_serious_injury=("Crash Severity", lambda s: s.isin(["K", "A"]).sum()),
        total_epdo=("EPDO_Score", lambda s: pd.to_numeric(s, errors="coerce").fillna(1).sum()),
    ).reset_index()

    # Get scope columns per key (first occurrence)
    scope_cols = df.groupby("_rank_key", dropna=False).agg(
        district=("DOT District", "first"),
        mpo=("MPO Name", "first"),
        pd_col=("Planning District", "first"),
    ).reset_index()

    agg_df = agg_df.merge(scope_cols, on="_rank_key", how="left")

    # Convert to metrics dict for backward compatibility
    metrics = {}
    for _, row in agg_df.iterrows():
        k = row["_rank_key"]
        if not k:
            continue
        metrics[k] = {col: row[col] for col in agg_df.columns if col != "_rank_key"}

    # ── Rank within each scope ──
    rank_results = {}  # key → {col_name: rank_value}
    for k in agg_df["_rank_key"]:
        rank_results[k] = {}

    for metric in RANKING_METRICS:
        metric_vals = agg_df.set_index("_rank_key")[metric]

        # Juris scope — rank across all jurisdictions
        ranks = metric_vals.rank(ascending=False, method="min").astype(int)
        for k, r in ranks.items():
            rank_results[k][f"Juris_Rank_{metric}"] = r

        # District scope — rank within each district
        for dist, grp in agg_df.groupby("district", dropna=False):
            if not dist or str(dist).strip() == "":
                for k in grp["_rank_key"]:
                    rank_results[k][f"District_Rank_{metric}"] = ""
                continue
            dist_ranks = grp.set_index("_rank_key")[metric].rank(
                ascending=False, method="min").astype(int)
            for k, r in dist_ranks.items():
                rank_results[k][f"District_Rank_{metric}"] = r

        # MPO scope
        for mpo_name, grp in agg_df.groupby("mpo", dropna=False):
            if not mpo_name or str(mpo_name).strip() == "":
                for k in grp["_rank_key"]:
                    rank_results[k][f"MPO_Rank_{metric}"] = ""
                continue
            mpo_ranks = grp.set_index("_rank_key")[metric].rank(
                ascending=False, method="min").astype(int)
            for k, r in mpo_ranks.items():
                rank_results[k][f"MPO_Rank_{metric}"] = r

        # Planning District scope
        for pd_name, grp in agg_df.groupby("pd_col", dropna=False):
            if not pd_name or str(pd_name).strip() == "":
                for k in grp["_rank_key"]:
                    rank_results[k][f"PlanningDistrict_Rank_{metric}"] = ""
                continue
            pd_ranks = grp.set_index("_rank_key")[metric].rank(
                ascending=False, method="min").astype(int)
            for k, r in pd_ranks.items():
                rank_results[k][f"PlanningDistrict_Rank_{metric}"] = r

    # ── Map ranks back to every row via vectorized merge ──
    rank_df = pd.DataFrame.from_dict(rank_results, orient="index")
    rank_df.index.name = "_rank_key"
    rank_df = rank_df.reset_index()

    # Convert rank values to string (empty for None)
    for col in rank_df.columns:
        if col != "_rank_key":
            rank_df[col] = rank_df[col].apply(
                lambda v: "" if v == "" or pd.isna(v) else str(int(v)))

    # Merge into main df
    df = df.merge(rank_df, on="_rank_key", how="left")
    df.drop(columns=["_rank_key"], inplace=True)

    return df, metrics


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 7 — VALIDATION, REPORTING & FILL STRATEGIES
# ─────────────────────────────────────────────────────────────────────────────

def compute_fill_strategies(df: pd.DataFrame) -> dict:
    """Generate actionable fill strategies for missing/empty columns."""
    strategies = {}
    for col in GOLDEN_COLUMNS:
        if col not in df.columns:
            continue
        fill_pct = (df[col].fillna("").str.strip() != "").sum() / max(len(df), 1) * 100
        if fill_pct < 20 and col in FILL_STRATEGY_LOOKUP:
            strategies[col] = {
                "filled_pct": round(fill_pct, 1),
                "strategy": FILL_STRATEGY_LOOKUP[col],
                "tier": "Tier 2 (OSM)" if col in TIER2_COLUMNS else "Manual/External",
                "priority": "HIGH" if col in MANDATORY_COLUMNS else "MEDIUM",
            }
    return strategies


def print_fill_strategies(df: pd.DataFrame):
    """Print recommendations for filling empty BDOT columns."""
    print("\n  📋 Missing Column Fill Strategies:")
    found_any = False
    for col in GOLDEN_COLUMNS:
        if col not in df.columns:
            continue
        filled_pct = (df[col].fillna("").str.strip() != "").sum() / max(len(df), 1) * 100
        if filled_pct < 20 and col in FILL_STRATEGY_LOOKUP:
            strategy = FILL_STRATEGY_LOOKUP[col]
            print(f"     {col:<30} ({filled_pct:5.1f}% filled) → {strategy}")
            found_any = True
    if not found_any:
        print("     All columns have >20% coverage — no fill strategies needed.")


def build_validation_report(
    df: pd.DataFrame,
    fips_lookup: dict,
    metrics: dict,
    epdo_preset_name: str,
    epdo_weights: dict,
    column_mapping: dict,
    gps_reassign_stats: dict = None,
) -> dict:
    total = len(df)
    sev_dist = {}
    for sev in ["K", "A", "B", "C", "O"]:
        sev_dist[sev] = int((df["Crash Severity"] == sev).sum())
    sev_dist["unmapped"] = int((~df["Crash Severity"].isin(["K", "A", "B", "C", "O"])).sum())

    fips_resolved = sum(1 for v in fips_lookup.values() if v.get("fips"))
    mapped  = sum(1 for v in column_mapping.values() if v["status"] == "mapped")
    renamed = sum(1 for v in column_mapping.values() if v["status"] == "renamed")
    missing = sum(1 for v in column_mapping.values() if v["status"] == "missing")

    quality = round(
        0.5 * (mapped + renamed) / len(GOLDEN_COLUMNS) * 100
        + 0.5 * (fips_resolved / max(len(fips_lookup), 1)) * 100,
        1,
    )

    mandatory_check = {}
    for col in ["Physical Juris Name", "x", "y", "Crash Severity"]:
        pct = float((df[col].fillna("").str.strip() != "").sum()) / max(total, 1) * 100
        mandatory_check[col] = f"OK ({pct:.1f}% filled)" if pct > 90 else f"WARNING ({pct:.1f}% filled)"

    unmapped_sev = df.loc[~df["Crash Severity"].isin(["K", "A", "B", "C", "O"]), "Crash Severity"].value_counts().to_dict()

    # Fill strategies
    fill_strategies = compute_fill_strategies(df)

    # State-prefixed extras tracking
    standard_set = set(GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + ANALYSIS_COLUMNS)
    for s in RANKING_SCOPES:
        for m in RANKING_METRICS:
            standard_set.add(f"{s}_Rank_{m}")
    extra_cols_info = []
    for col in df.columns:
        if col not in standard_set and col.startswith(f"{STATE_ABBREVIATION}_"):
            extra_cols_info.append({"prefixed": col})

    return {
        "state":              STATE_NAME,
        "state_fips":         STATE_FIPS,
        "state_abbreviation": STATE_ABBREVIATION,
        "processed_at":       datetime.now(timezone.utc).isoformat(),
        "total_rows":         total,
        "total_columns":      69 + len(ENRICHMENT_COLUMNS) + len(RANKING_SCOPES) * len(RANKING_METRICS),
        "golden_columns":     69,
        "enrichment_columns": len(ENRICHMENT_COLUMNS),
        "ranking_columns":    len(RANKING_SCOPES) * len(RANKING_METRICS),
        "quality_score":      quality,
        "objectid_format":    f"{STATE_ABBREVIATION}-{{7-digit}}",
        "fips_coverage": {
            "total_jurisdictions": len(fips_lookup),
            "resolved":    fips_resolved,
            "unresolved":  len(fips_lookup) - fips_resolved,
            "coverage_pct": round(fips_resolved / max(len(fips_lookup), 1) * 100, 1),
        },
        "severity_distribution": sev_dist,
        "epdo_config": {"preset": epdo_preset_name, "weights": epdo_weights},
        "mapping_completeness": {
            "mapped":   mapped,
            "renamed":  renamed,
            "missing":  missing,
            "coverage_pct": round((mapped + renamed) / len(GOLDEN_COLUMNS) * 100, 1),
        },
        "mandatory_columns":      mandatory_check,
        "ranking_scopes":         RANKING_SCOPES,
        "ranking_metrics":        RANKING_METRICS,
        "conflicts":              [],
        "warnings":               ["Delaware does not distinguish B/C severity — all injury mapped to A"],
        "unmapped_values":        {"Crash Severity": unmapped_sev} if unmapped_sev else {},
        "fill_strategies":        fill_strategies,
        "state_prefixed_extras":  extra_cols_info,
        "intersection_name_coverage": {
            "filled": int((df.get("Intersection Name", pd.Series(dtype=str)).fillna("").str.strip() != "").sum()) if "Intersection Name" in df.columns else 0,
            "pct": round((df.get("Intersection Name", pd.Series(dtype=str)).fillna("").str.strip() != "").sum() / max(total, 1) * 100, 1) if "Intersection Name" in df.columns else 0.0,
        },
        "gps_jurisdiction_validation": {
            "reassignments": gps_reassign_stats or {},
            "total_reassigned": sum((gps_reassign_stats or {}).values()),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
#  COLUMN MAPPING RECORD  (for validation report)
# ─────────────────────────────────────────────────────────────────────────────

def build_column_mapping_record(source_cols: list[str]) -> dict:
    src_upper = {c.upper().strip() for c in source_cols}
    mapping = {}
    rename_upper = {k.upper(): v for k, v in COLUMN_RENAMES.items()}

    for tgt in GOLDEN_COLUMNS:
        if tgt in source_cols:
            mapping[tgt] = {"source": tgt, "status": "mapped"}
        elif tgt.upper() in rename_upper.values():
            for src_u, tgt_v in rename_upper.items():
                if tgt_v == tgt and src_u in src_upper:
                    orig = next((c for c in source_cols if c.upper().strip() == src_u), src_u)
                    mapping[tgt] = {"source": orig, "status": "renamed"}
                    break
            else:
                mapping[tgt] = {"source": None, "status": "missing"}
        else:
            mapping[tgt] = {"source": None, "status": "missing"}

    return mapping


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 8 — ENRICHMENT  (crash_enricher Tier 1 + Tier 2)
# ─────────────────────────────────────────────────────────────────────────────

def run_enrichment(df: pd.DataFrame, skip_enrichment: bool = False,
                   output_path: str = None) -> pd.DataFrame:
    """Run Tier 1 + Road Inventory enrichment with stream-to-disk.

    Architecture (scales to Texas 1.2M rows):
      1. Tier 1 self-enrichment on full df (flags only, fast)
      2. RoadInventorySession: load parquet + KDTree ONCE (~10s)
      3. Per-county: session.enrich(chunk) via vectorized merge (~3s/chunk)
      4. Stream each enriched chunk to CSV — peak memory = 1 county only
      5. Read back final CSV for prefix_extra_columns + output ordering

    Memory budget (GitHub Actions 7GB):
      Road inventory in session:  ~0.7 GB (stays loaded)
      One county chunk enriched:  ~0.3 GB (freed after write)
      Final read-back:            ~3.0 GB (after session freed)
      Peak:                       ~4.0 GB (well under 7GB)
    """
    if skip_enrichment:
        print("  [8/8] Phase 8: Road enrichment SKIPPED (--skip-enrichment)")
        print("         Running Tier 1 self-enrichment (flags, severity)...")
        df = _inline_tier1_enrichment(df)
        return df

    try:
        from crash_enricher import CrashEnricher
    except ImportError:
        print("  [8/8] Phase 8: crash_enricher.py not found")
        df = _inline_tier1_enrichment(df)
        return df

    import gc

    # ── Tier 1: Self-enrichment (flags, severity — no external data) ──
    enricher = CrashEnricher(
        STATE_FIPS, STATE_ABBREVIATION, STATE_NAME,
        cache_dir=str(_CACHE_DIR),
        circumstance_col=CIRCUMSTANCE_COL,
        private_property_col=PRIVATE_PROPERTY_COL,
    )
    print(f"  [8/8] Phase 8: Enrichment ({len(df):,} rows)...")
    df = enricher.enrich_tier1(df)

    # ── Road Inventory: session-based + stream-to-disk ──
    try:
        from road_inventory_enricher import RoadInventorySession
    except ImportError:
        print("    road_inventory_enricher.py not found — Tier 1 only")
        df = enricher._derive_intersection_analysis(df)
        enricher._print_fill_report(df)
        return df

    session = RoadInventorySession(STATE_ABBREVIATION, str(_CACHE_DIR))
    if not session.ready:
        print("    Road inventory not available — Tier 1 only")
        df = enricher._derive_intersection_analysis(df)
        enricher._print_fill_report(df)
        return df

    # ── Chunk by county and stream to temp CSV ──
    counties = df["FIPS"].fillna("").str.strip()
    unique_fips = sorted([f for f in counties.unique() if f])

    STREAM_THRESHOLD = 100_000
    MAX_CHUNK = 75_000

    if len(df) <= STREAM_THRESHOLD:
        # Small dataset — enrich in memory, no streaming
        print(f"    In-memory enrichment ({len(df):,} rows)")
        df = session.enrich(df)
        df = enricher._derive_intersection_analysis(df)
        enricher._print_fill_report(df)
        return df

    # Large dataset — stream enriched chunks to disk
    temp_path = str(Path(output_path or "enriched_temp.csv").with_suffix(".enriching.csv"))
    print(f"\n    Stream-to-disk: {len(df):,} rows across {len(unique_fips)} counties")
    print(f"    Temp file: {temp_path}")

    header_written = False
    total_matched = 0
    all_columns = None
    t_start = time.time()

    # First pass: enrich one small chunk to discover all columns
    first_fips = unique_fips[0]
    first_chunk = df.loc[counties == first_fips].copy()
    first_enriched = session.enrich(first_chunk)
    all_columns = list(first_enriched.columns)
    del first_chunk, first_enriched
    gc.collect()

    for i, fips in enumerate(unique_fips):
        county_mask = counties == fips
        county_df = df.loc[county_mask].copy()
        county_name = (county_df["Physical Juris Name"].iloc[0]
                       if "Physical Juris Name" in county_df.columns else fips)

        # Sub-chunk large counties
        if len(county_df) <= MAX_CHUNK:
            sub_chunks = [county_df]
        else:
            n_sub = (len(county_df) + MAX_CHUNK - 1) // MAX_CHUNK
            sub_chunks = [county_df.iloc[j*MAX_CHUNK : (j+1)*MAX_CHUNK].copy()
                          for j in range(n_sub)]

        label = f"[{i+1}/{len(unique_fips)}] {county_name} ({len(county_df):,})"
        if len(sub_chunks) > 1:
            label += f" → {len(sub_chunks)} sub-chunks"
        print(f"\n    {label}")

        for sub in sub_chunks:
            try:
                enriched = session.enrich(sub)
            except MemoryError:
                gc.collect()
                # Adaptive retry: halve the batch
                half = len(sub) // 2
                if half < 5000:
                    print(f"      ❌ OOM at {len(sub):,} rows — keeping Tier 1")
                    enriched = sub
                else:
                    print(f"      ⚠️ OOM — retrying as 2×{half:,}")
                    e1 = session.enrich(sub.iloc[:half].copy())
                    gc.collect()
                    e2 = session.enrich(sub.iloc[half:].copy())
                    gc.collect()
                    enriched = pd.concat([e1, e2], ignore_index=False)
                    del e1, e2
            except Exception as e:
                print(f"      ❌ Failed: {e} — keeping Tier 1")
                enriched = sub

            total_matched += (enriched.get("ri_matched", pd.Series("")) == "Yes").sum()

            # Ensure column consistency across chunks
            for col in all_columns:
                if col not in enriched.columns:
                    enriched[col] = ""

            # Write to disk
            enriched[all_columns].to_csv(
                temp_path,
                mode="a" if header_written else "w",
                header=not header_written,
                index=False,
            )
            header_written = True

            del enriched, sub
            gc.collect()

        del county_df, sub_chunks
        gc.collect()

        # Progress
        elapsed = time.time() - t_start
        if elapsed > 0 and i < len(unique_fips) - 1:
            rate = (i + 1) / elapsed
            remaining = (len(unique_fips) - i - 1) / max(rate, 0.01)
            print(f"      Progress: county {i+1}/{len(unique_fips)}, "
                  f"ETA {remaining/60:.0f}min")

    # ── Free session memory before reading back ──
    del session
    gc.collect()

    # ── Read back streamed CSV ──
    print(f"\n    Reading back enriched data from disk...")
    df = pd.read_csv(temp_path, dtype=str, low_memory=False)
    Path(temp_path).unlink(missing_ok=True)

    print(f"    Loaded: {len(df):,} rows × {len(df.columns)} cols")
    print(f"    Matched: {total_matched:,}/{len(df):,} "
          f"({total_matched/len(df)*100:.0f}%)")

    df = enricher._derive_intersection_analysis(df)
    enricher._print_fill_report(df)
    return df


def _inline_tier1_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal Tier 1 enrichment when crash_enricher.py is not available."""
    # Contributing circumstance → flags
    circ_col = CIRCUMSTANCE_COL if CIRCUMSTANCE_COL in df.columns else None
    if not circ_col:
        for candidate in df.columns:
            if "contributing" in candidate.lower() and "circumstance" in candidate.lower():
                circ_col = candidate
                break

    if circ_col:
        circ_lower = df[circ_col].fillna("").str.lower()

        flag_patterns = {
            "Distracted?":      r"inattent|distract|cell.phone|texting|electronic|eating|grooming|fatigue",
            "Drowsy?":          r"drowsy|asleep|fatigued",
            "Speed?":           r"speed|exceeding|too fast|racing|aggressive",
            "Animal Related?":  r"animal|deer|wildlife|elk|moose",
            "Hitrun?":          r"hit.and.run|hit-and-run|hitrun|left scene|fled|fleeing",
        }

        for flag, pattern in flag_patterns.items():
            mask_empty = df[flag].fillna("").str.strip().isin(["", "No"])
            matched = circ_lower.str.contains(pattern, regex=True, na=False)
            df.loc[mask_empty & matched, flag] = "Yes"
            df.loc[mask_empty & ~matched, flag] = "No"

    # Default all flag columns to "No" if empty
    flag_cols = ["Distracted?", "Drowsy?", "Speed?", "Animal Related?", "Hitrun?",
                 "Guardrail Related?", "Lgtruck?", "Senior?", "Young?"]
    for flag in flag_cols:
        if flag in df.columns:
            mask = df[flag].fillna("").str.strip() == ""
            df.loc[mask, flag] = "No"

    # Mainline? from Private Property
    pp_col = PRIVATE_PROPERTY_COL if PRIVATE_PROPERTY_COL in df.columns else None
    if pp_col:
        mask = df["Mainline?"].fillna("").str.strip() == ""
        pp_upper = df[pp_col].fillna("").str.strip().str.upper()
        df.loc[mask & (pp_upper == "N"), "Mainline?"] = "Yes"
        df.loc[mask & (pp_upper != "N"), "Mainline?"] = "No"
    else:
        mask = df["Mainline?"].fillna("").str.strip() == ""
        df.loc[mask, "Mainline?"] = "No"

    # K/A People cross-validation
    k_mask = (df["Crash Severity"] == "K") & (df["K_People"].fillna("").str.strip().isin(["", "0"]))
    df.loc[k_mask, "K_People"] = "1"
    a_mask = (df["Crash Severity"] == "A") & (df["A_People"].fillna("").str.strip().isin(["", "0"]))
    df.loc[a_mask, "A_People"] = "1"

    # Ped/Bike cross-validation from Collision Type
    ct_lower = df["Collision Type"].fillna("").str.lower()
    ped_from_ct = ct_lower.str.contains("ped", na=False)
    df.loc[ped_from_ct & (df["Pedestrian?"] == "No"), "Pedestrian?"] = "Yes"
    bike_from_ct = ct_lower.str.contains("bicycl|bike", regex=True, na=False)
    df.loc[bike_from_ct & (df["Bike?"] == "No"), "Bike?"] = "Yes"

    # Ensure Intersection Name exists
    if "Intersection Name" not in df.columns:
        df["Intersection Name"] = ""

    enriched_flags = sum(1 for f in flag_cols if f in df.columns and (df[f] == "Yes").any())
    print(f"         Tier 1: {enriched_flags} flag columns enriched (inline fallback)")

    return df


# ─────────────────────────────────────────────────────────────────────────────
#  STATE-PREFIXED EXTRA COLUMNS  (v2.6)
# ─────────────────────────────────────────────────────────────────────────────

def _clean_float_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fix merge()-induced float reversion before CSV write.

    pandas merge() silently converts string "2012" → float 2012.0 when columns
    mix string and NaN. This cleanup strips trailing .0 from columns that should
    be clean integer strings, preventing "2012.0" from appearing in output CSV.
    """
    for col in ["Crash Year", "Crash Military Time", "Juris Code", "K_People",
                "A_People", "B_People", "C_People", "VSP"]:
        if col in df.columns:
            df[col] = (df[col].fillna("").astype(str)
                       .str.replace(r"\.0$", "", regex=True)
                       .str.strip()
                       .replace({"nan": "", "NaN": ""}))
    return df


def prefix_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Prefix non-standard columns with lowercase state abbreviation."""
    standard_set = set(GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + ANALYSIS_COLUMNS)
    for s in RANKING_SCOPES:
        for m in RANKING_METRICS:
            standard_set.add(f"{s}_Rank_{m}")

    # Ensure Intersection Name is always present (empty if Tier 2 not run)
    if "Intersection Name" not in df.columns:
        df.insert(df.columns.get_loc("EPDO_Score") + 1, "Intersection Name", "")

    # Drop raw geometry column — it's WKT, not useful in the output CSV
    geom_cols = [c for c in df.columns if c.lower().strip() in ("the_geom", "geom", "geometry", "wkt")]
    if geom_cols:
        df = df.drop(columns=geom_cols)

    rename_map = {}
    for col in df.columns:
        if col not in standard_set and not col.startswith(ROAD_INVENTORY_PREFIXES):
            clean = "_".join(w.capitalize() for w in col.strip().split())
            rename_map[col] = f"{STATE_ABBREVIATION}_{clean}"

    if rename_map:
        df = df.rename(columns=rename_map)
        print(f"  Prefixed {len(rename_map)} extra columns with '{STATE_ABBREVIATION}_'")

    return df


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def normalize(
    input_path: str,
    output_path: str | None = None,
    epdo_preset: str = DEFAULT_EPDO_PRESET,
    skip_if_normalized: bool = False,
    skip_enrichment: bool = False,
    rerank_only: bool = False,
    report_path: str | None = None,
) -> str:
    t0 = time.time()
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input not found: {src}")

    print(f"\n{'═'*60}")
    print(f"  CrashLens Delaware Normalization v2.6.4  |  {datetime.now():%Y-%m-%d %H:%M}")
    print(f"  Input : {src.name}")
    print(f"  State : {STATE_NAME} ({STATE_ABBREVIATION}) | FIPS: {STATE_FIPS} | DOT: {STATE_DOT}")
    print(f"{'═'*60}")

    # Module status check
    print("\n  Module status:")
    for name in ["geo_resolver.py", "crash_enricher.py", "osm_road_enricher.py", "tigerweb_pip.py"]:
        found = any((Path(p) / name).exists() for p in sys.path if p)
        status = "OK" if found else "MISSING — put in same folder"
        print(f"    {name:<25} {status}")
    print()

    # [1/8] Load
    print("  [1/8] Loading CSV...")
    if str(src).endswith(('.parquet.gz', '.parquet')):
        df = pd.read_parquet(src).astype(str).replace({"nan": "", "None": "", "<NA>": ""})
    else:
        df = pd.read_csv(src, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    total_rows = len(df)
    print(f"        {total_rows:,} rows  ×  {len(df.columns)} columns")

    if skip_if_normalized and is_already_normalized(df.columns.tolist()):
        print("  ✓ Already normalized — skipping (use --force to override)")
        return str(src)

    col_mapping = build_column_mapping_record(df.columns.tolist())

    # ── RERANK-ONLY MODE ──
    # Skip Phases 1-4 and 7-8. Only recompute OBJECTIDs, EPDO, rankings.
    # Used after merging existing normalized CSV with newly normalized delta.
    if rerank_only:
        print("  ⚡ RERANK-ONLY mode — skipping normalization phases")
        print(f"     Input is already normalized ({total_rows:,} rows)")

        # Regenerate OBJECTIDs (sequential order changes after merge)
        print("     Regenerating OBJECTIDs...")
        df["OBJECTID"] = [f"{STATE_ABBREVIATION}-{i+1:07d}" for i in range(len(df))]

        # Recompute EPDO
        weights = EPDO_PRESETS.get(epdo_preset, EPDO_PRESETS[DEFAULT_EPDO_PRESET])
        print(f"     Recomputing EPDO ({epdo_preset.upper()})...")
        df = compute_epdo(df, weights)

        # Recompute rankings
        print(f"     Recomputing rankings...")
        df, metrics = compute_rankings(df)
        print(f"     Ranked {len(metrics)} jurisdictions across {len(RANKING_SCOPES)} scopes × {len(RANKING_METRICS)} metrics")

        # Determine output paths
        if output_path is None:
            output_path = str(src.parent / f"{src.stem}_normalized_ranked.parquet.gz")

        # Build output column order (same as full pipeline)
        ranking_cols = [f"{s}_Rank_{m}" for s in RANKING_SCOPES for m in RANKING_METRICS]
        analysis_cols = [c for c in ANALYSIS_COLUMNS if c in df.columns]
        ri_cols = [c for c in df.columns if c.startswith(ROAD_INVENTORY_PREFIXES) and c not in analysis_cols]
        analysis_cols = analysis_cols + ri_cols
        standard_set = set(GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + analysis_cols + ranking_cols)
        extra_cols = [c for c in df.columns if c not in standard_set]
        all_out_cols = GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + analysis_cols + ranking_cols + extra_cols
        all_out_cols = [c for c in all_out_cols if c in df.columns]
        # Deduplicate: GOLDEN and ANALYSIS overlap (Roadway Alignment, Roadway Description)
        seen = set()
        all_out_cols = [c for c in all_out_cols if c not in seen and not seen.add(c)]

        df = _clean_float_columns(df)
        df[all_out_cols].to_parquet(output_path, engine='pyarrow', compression='gzip', index=False)

        elapsed = time.time() - t0
        print(f"\n  ✅ Rerank done in {elapsed:.1f}s")
        print(f"     Output  → {output_path}")
        print(f"     Columns → {len(all_out_cols)}")
        print(f"{'═'*60}\n")
        return output_path

    # [2/8] Column renames
    print("  [2/8] Phase 1: Column renames...")
    df = apply_column_renames(df)

    # [3/8] Value transforms
    print("  [3/8] Phase 2: Value transforms (datetime, severity, Y/N, etc.)...")
    df = apply_value_transforms(df)

    # [4/8] FIPS Resolution
    print("  [4/8] Phase 3: FIPS resolution (DE hardcoded — 3 counties)...")
    df, fips_lookup = resolve_fips(df)
    resolved = sum(1 for v in fips_lookup.values() if v["fips"])
    print(f"        {resolved}/{len(fips_lookup)} jurisdictions resolved")

    # Phase 3.5: GPS Jurisdiction Validation (v2.6.4)
    print("        Phase 3.5: GPS jurisdiction cross-check...")
    df, gps_reassign_stats = validate_gps_jurisdiction(df)

    # [5/8] Crash IDs  (v2.6: lowercase prefix)
    print("  [5/8] Phase 4: Generating crash IDs...")
    df = generate_crash_ids(df)
    print(f"        OBJECTID: {STATE_ABBREVIATION}-0000001 format")
    print(f"        Document Nbr: {STATE_ABBREVIATION}-YYYYMMDD-HHMM-NNNNNNN format")

    # [6/8] EPDO + Ranking
    weights = EPDO_PRESETS.get(epdo_preset, EPDO_PRESETS[DEFAULT_EPDO_PRESET])
    print(f"  [6/8] Phase 5+6: EPDO scoring ({epdo_preset.upper()}) + Ranking...")
    df = compute_epdo(df, weights)
    df, metrics = compute_rankings(df)
    print(f"        Ranked {len(metrics)} jurisdictions across {len(RANKING_SCOPES)} scopes × {len(RANKING_METRICS)} metrics")

    # [7/8] Validation + Fill Strategies
    print("  [7/8] Phase 7: Validation report...")
    report = build_validation_report(df, fips_lookup, metrics, epdo_preset, weights, col_mapping, gps_reassign_stats)
    print(f"        Quality score: {report['quality_score']}%")
    sev = report["severity_distribution"]
    print(f"        Severity: K={sev['K']}  A={sev['A']}  B={sev['B']}  C={sev['C']}  O={sev['O']}")
    print_fill_strategies(df)

    # [8/8] Enrichment
    # Resolve output path early so stream-to-disk can use it
    if output_path is None:
        output_path = str(src.parent / f"{src.stem}_normalized_ranked.parquet.gz")
    if report_path is None:
        report_path = str(src.parent / f"{src.stem}_validation_report.json")

    df = run_enrichment(df, skip_enrichment=skip_enrichment, output_path=output_path)

    # Prefix extra columns (v2.6)
    df = prefix_extra_columns(df)

    # Build output column order: 69 standard + 4 enrichment + road inventory + 24 ranking + prefixed extras
    ranking_cols = [f"{s}_Rank_{m}" for s in RANKING_SCOPES for m in RANKING_METRICS]
    # Road inventory columns: anything in ANALYSIS_COLUMNS or with protected prefix
    analysis_cols = [c for c in ANALYSIS_COLUMNS if c in df.columns]
    ri_cols = [c for c in df.columns if c.startswith(ROAD_INVENTORY_PREFIXES) and c not in analysis_cols]
    analysis_cols = analysis_cols + ri_cols
    standard_set = set(GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + analysis_cols + ranking_cols)
    extra_cols = [c for c in df.columns if c not in standard_set]

    all_out_cols = GOLDEN_COLUMNS + ENRICHMENT_COLUMNS + analysis_cols + ranking_cols + extra_cols
    all_out_cols = [c for c in all_out_cols if c in df.columns]
    # Deduplicate: GOLDEN and ANALYSIS overlap (Roadway Alignment, Roadway Description)
    seen = set()
    all_out_cols = [c for c in all_out_cols if c not in seen and not seen.add(c)]

    # Output paths already resolved above

    df = _clean_float_columns(df)
    df[all_out_cols].to_parquet(output_path, engine='pyarrow', compression='gzip', index=False)
    with open(report_path, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2)

    elapsed = time.time() - t0
    print(f"\n  ✅ Done in {elapsed:.1f}s")
    print(f"     Output  → {output_path}")
    print(f"     Report  → {report_path}")
    print(f"     Columns → {len(all_out_cols)} ({len(GOLDEN_COLUMNS)} standard + {len(ENRICHMENT_COLUMNS)} enrich + {len(analysis_cols)} analysis + {len(ranking_cols)} rank + {len(extra_cols)} extra)")
    print(f"{'═'*60}\n")
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
#  CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CrashLens — Delaware (DelDOT) crash data normalization pipeline v2.6"
    )
    parser.add_argument("--input",  "-i", required=True,  help="Input CSV path")
    parser.add_argument("--output", "-o", default=None,   help="Output path (default: {stem}_normalized_ranked.parquet.gz)")
    parser.add_argument("--report", "-r", default=None,   help="Validation report JSON path")
    parser.add_argument(
        "--epdo", default=DEFAULT_EPDO_PRESET,
        choices=list(EPDO_PRESETS.keys()),
        help=f"EPDO weight preset (default: {DEFAULT_EPDO_PRESET})"
    )
    parser.add_argument(
        "--skip-if-normalized", action="store_true",
        help="Skip processing if file is already in CrashLens standard format"
    )
    parser.add_argument(
        "--skip-enrichment", action="store_true",
        help="Skip Phase 8 enrichment (Tier 1 + Tier 2)"
    )
    parser.add_argument(
        "--rerank-only", action="store_true",
        help="Skip normalization — only recompute EPDO + rankings + OBJECTIDs on an already-normalized CSV"
    )
    args = parser.parse_args()

    try:
        normalize(
            input_path=args.input,
            output_path=args.output,
            epdo_preset=args.epdo,
            skip_if_normalized=args.skip_if_normalized,
            skip_enrichment=args.skip_enrichment,
            rerank_only=args.rerank_only,
            report_path=args.report,
        )
    except Exception as exc:
        print(f"\n  ❌ Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
