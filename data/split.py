#!/usr/bin/env python3
"""
split.py — CrashLens Universal Jurisdiction & Road Type Splitter  v2.0
=======================================================================
Reads a statewide normalized CSV (115-column standard schema output from
{state}_normalize.py) and splits it into per-jurisdiction, per-road-type
CSV files that exactly match the Cloudflare R2 folder structure the
CrashLens frontend expects.

WHY THIS FILE EXISTS
--------------------
The normalize.py pipeline produces ONE statewide CSV (e.g. 566,762 rows
for Delaware). The CrashLens frontend loads small per-jurisdiction slices
from R2, not the full statewide file. This script bridges that gap.

It is UNIVERSAL — the same split.py works for every state because:
  1. Every state's normalize.py maps to the same 69-column standard schema
  2. Functional Class always uses the same 7 standard values post-normalization
  3. Ownership always uses the same 6 standard values post-normalization
  4. Jurisdiction columns (DOT District, MPO Name, Planning District) are
     populated by normalize.py / geo_resolver.py for all states

R2 OUTPUT STRUCTURE (mirrors create_r2_folders.py exactly)
-----------------------------------------------------------
  {state_prefix}/
    _state/
      all_roads.csv                 All crashes
      dot_roads.csv                 State DOT-maintained only
      primary_roads.csv             Interstate + Freeway only
      non_dot_roads.csv             Non-DOT (county + city + local)
      statewide_all_roads.csv       Alias for all_roads (pipeline compat)
    _region/{region_id}/
      all_roads.csv
      dot_roads.csv
      primary_roads.csv
      non_dot_roads.csv
    _mpo/{mpo_id}/
      all_roads.csv
      county_roads.csv
      city_roads.csv
      no_interstate.csv
    _planning_district/{pd_id}/
      all_roads.csv
      county_roads.csv
      city_roads.csv
      no_interstate.csv
    _city/{city_slug}/
      all_roads.csv
      county_roads.csv
      city_roads.csv
      no_interstate.csv
    {county_key}/
      all_roads.csv
      county_roads.csv
      city_roads.csv
      no_interstate.csv

TWO ROAD TYPE SETS — matching frontend getActiveRoadTypeSuffix()
----------------------------------------------------------------
  SET A — State + Region tiers (DOT perspective):
    all_roads       No filter
    dot_roads       Ownership == "1. State Hwy Agency"
    primary_roads   Functional Class starts with 1- or 2-
    non_dot_roads   Ownership != "1. State Hwy Agency"

  SET B — County / MPO / Planning District / City tiers (local view):
    all_roads       No filter
    county_roads    Ownership == "2. County Hwy Agency"
    city_roads      Ownership == "3. City or Town Hwy Agency"
    no_interstate   Functional Class does NOT start with 1- or 2-

TWO JURISDICTION SPLIT STRATEGIES — auto-detected per tier
-----------------------------------------------------------
  STRATEGY A (column): groupby on existing DOT District / MPO Name /
                       Planning District column. Used when column is
                       >= 10% populated. Works for DOT + enriched states.

  STRATEGY B (hierarchy): filter by county membership from hierarchy.json.
                          Used when jurisdiction columns are empty. Works
                          for all states with hierarchy.json.

USAGE
-----
  # Basic
  python split.py --input delaware_statewide_normalized.csv --state delaware

  # Full CI pipeline
  python split.py --input statewide.csv --state delaware \\
    --hierarchy states/delaware/hierarchy.json \\
    --output-dir splits/delaware/ \\
    --gzip --upload-r2 \\
    --r2-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com

  # Dry run (see files without writing)
  python split.py --input statewide.csv --state delaware --dry-run

  # County + state only (fastest)
  python split.py --input statewide.csv --state delaware --county-only

DEPENDENCIES: pandas>=2.0.0 only
"""

import argparse
import gzip
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
#  PATH RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = (
    _SCRIPT_DIR.parent.parent
    if (_SCRIPT_DIR.parent.parent / "states").exists()
    else _SCRIPT_DIR
)


# ─────────────────────────────────────────────────────────────────────────────
#  STANDARD OWNERSHIP VALUES  (must match frontend schema exactly)
# ─────────────────────────────────────────────────────────────────────────────
OWN_STATE   = "1. State Hwy Agency"
OWN_COUNTY  = "2. County Hwy Agency"
OWN_CITY    = "3. City or Town Hwy Agency"
OWN_FEDERAL = "4. Federal Roads"
OWN_TOLL    = "5. Toll Roads Maintained by Others"
OWN_PRIVATE = "6. Private/Unknown Roads"

# Non-DOT ownership set (everything the state DOT doesn't maintain)
OWN_NON_DOT = {OWN_COUNTY, OWN_CITY, OWN_FEDERAL, OWN_TOLL, OWN_PRIVATE}

# Functional Class prefixes for Interstate AND Freeway/Expressway
# "1-Interstate (A,1)" and "2-Principal Arterial - Other Freeways and Expressways (B)"
FC_HIGHWAY_PREFIX_PATTERN = re.compile(r"^[12]-", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
#  ROAD TYPE FILTER SETS
# ─────────────────────────────────────────────────────────────────────────────

ROAD_TYPES_STATE_REGION: Dict[str, dict] = {
    # SET A — used for _state/ and _region/ tiers
    "all_roads": {
        "description": "All crashes — no filter",
        "filter": None,
    },
    "dot_roads": {
        "description": "State DOT-maintained (Ownership == State Hwy Agency)",
        "filter": lambda df: df[df["Ownership"].fillna("") == OWN_STATE],
    },
    "primary_roads": {
        "description": "Interstate + Freeway/Expressway (FC 1 + FC 2)",
        "filter": lambda df: df[
            df["Functional Class"].fillna("").str.match(r"^[12]-", na=False)
        ],
    },
    "non_dot_roads": {
        "description": "Non-DOT roads — county, city, federal, toll, private",
        "filter": lambda df: df[df["Ownership"].fillna("").isin(OWN_NON_DOT)],
    },
}

ROAD_TYPES_LOCAL: Dict[str, dict] = {
    # SET B — used for _mpo/, _planning_district/, _city/, county/ tiers
    "all_roads": {
        "description": "All crashes — no filter",
        "filter": None,
    },
    "county_roads": {
        "description": "County-maintained roads (Ownership == County Hwy Agency)",
        "filter": lambda df: df[df["Ownership"].fillna("") == OWN_COUNTY],
    },
    "city_roads": {
        "description": "City/town-maintained roads (Ownership == City or Town Hwy Agency)",
        "filter": lambda df: df[df["Ownership"].fillna("") == OWN_CITY],
    },
    "no_interstate": {
        "description": "Excluding Interstate AND Freeway/Expressway (FC 1 + FC 2)",
        "filter": lambda df: df[
            ~df["Functional Class"].fillna("").str.match(r"^[12]-", na=False)
        ],
    },
}

# Which road type set each tier uses
TIER_ROAD_TYPES = {
    "state":             ROAD_TYPES_STATE_REGION,
    "region":            ROAD_TYPES_STATE_REGION,
    "mpo":               ROAD_TYPES_LOCAL,
    "planning_district": ROAD_TYPES_LOCAL,
    "city":              ROAD_TYPES_LOCAL,
    "county":            ROAD_TYPES_LOCAL,
}


# ─────────────────────────────────────────────────────────────────────────────
#  NAME → R2 KEY CONVERSION
#  Mirrors county_name_to_key() in create_r2_folders.py exactly
# ─────────────────────────────────────────────────────────────────────────────

def name_to_r2_key(name: str) -> str:
    """
    Convert any jurisdiction name to an R2 folder key.

    Mirrors create_r2_folders.py county_name_to_key() so R2 paths match
    the folder structure created by the folder creation script.

    Examples:
      "New Castle"              -> "new_castle"
      "Prince George's County"  -> "prince_georges_county"
      "City of Richmond"        -> "city_of_richmond"
      "Hampton Roads"           -> "hampton_roads"
      "001. Accomack County"    -> "accomack_county"  (strips numeric prefix)
      "Sussex (partial)"        -> "sussex_partial"
    """
    key = str(name).strip()
    key = re.sub(r"^\d+\.\s*", "", key)   # Strip "NNN. " prefix
    key = key.lower()
    key = key.replace("'", "")             # Remove apostrophes
    key = key.replace(".", "")             # Remove periods
    key = re.sub(r"[^a-z0-9\s_-]", " ", key)
    key = re.sub(r"[\s\-]+", "_", key)    # Spaces/hyphens -> underscores
    key = re.sub(r"_+", "_", key)
    key = key.strip("_")
    return key


def strip_juris_prefix(name: str) -> str:
    """
    Strip VDOT-style "NNN. " numeric prefix from Physical Juris Name.
    "001. Accomack County" -> "Accomack County"
    Plain names pass through unchanged.
    """
    return re.sub(r"^\d+\.\s*", "", str(name).strip())


# ─────────────────────────────────────────────────────────────────────────────
#  JURISDICTION TYPE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

_COUNTY_RE = re.compile(r"\bcounty\b|\bparish\b|\bborough\b", re.IGNORECASE)
_CITY_RE   = re.compile(r"^(city of|town of)\b|^(city|town)$", re.IGNORECASE)


def classify_juris_name(name: str) -> str:
    """
    Classify a Physical Juris Name as 'county', 'city', or 'unknown'.
    Works for both VDOT-style ("001. Accomack County") and plain names ("Kent").
    """
    clean = strip_juris_prefix(name)
    if _COUNTY_RE.search(clean):
        return "county"
    if _CITY_RE.search(clean):
        return "city"
    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
#  HIERARCHY LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_hierarchy(hierarchy_path: str) -> dict:
    """
    Load and normalize hierarchy.json.
    Handles both 'tprs' and 'mpos' key names for MPO sections.
    """
    path = Path(hierarchy_path)
    if not path.exists():
        raise FileNotFoundError(f"hierarchy.json not found: {path}")
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return {
        "state":             raw.get("state", {}),
        "regions":           raw.get("regions", {}),
        "tprs":              raw.get("tprs", raw.get("mpos", {})),
        "planningDistricts": raw.get("planningDistricts", {}),
        "allCounties":       raw.get("allCounties", {}),
    }


def build_fips_to_name(hierarchy: dict) -> Dict[str, str]:
    """Build FIPS -> clean county name map. Strips '(partial)' qualifier."""
    return {
        fips: name.replace(" (partial)", "").strip()
        for fips, name in hierarchy["allCounties"].items()
    }


def build_entity_county_map(section: dict, fips_to_name: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Build entity_id -> [county_name_list] from a hierarchy section.
    Used for Strategy B fallback for regions, MPOs, and planning districts.
    Only includes entities with at least one resolvable county name.
    """
    result = {}
    for entity_id, entity_data in section.items():
        fips_list = entity_data.get("counties", [])
        county_names_dict = entity_data.get("countyNames", {})
        names = []
        for fips in fips_list:
            name = fips_to_name.get(fips) or county_names_dict.get(fips, "")
            if name:
                names.append(name.replace(" (partial)", "").strip())
        if names:
            result[entity_id] = names
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  STRATEGY DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_strategy(df: pd.DataFrame, column: str) -> str:
    """
    Detect whether to use column-based (Strategy A) or hierarchy (Strategy B)
    split for a jurisdiction tier.

    Returns 'column' if the column exists and >= 10% of rows are populated.
    Returns 'hierarchy' otherwise.
    """
    if column not in df.columns:
        return "hierarchy"
    populated_pct = (df[column].fillna("").str.strip() != "").sum() / max(len(df), 1) * 100
    return "column" if populated_pct >= 10 else "hierarchy"


# ─────────────────────────────────────────────────────────────────────────────
#  FILE WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(
    df: pd.DataFrame,
    output_path: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> int:
    """Write DataFrame to CSV (optionally gzipped). Returns row count."""
    if dry_run:
        ext = ".gz" if gzip_output else ""
        print(f"      [DRY] {len(df):>7,} rows -> {output_path.name}{ext}")
        return len(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if gzip_output:
        gz_path = Path(str(output_path) + ".gz")
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            df.to_csv(f, index=False)
        print(f"      {len(df):>7,} rows -> {output_path.name}.gz")
    else:
        df.to_csv(output_path, index=False)
        print(f"      {len(df):>7,} rows -> {output_path.name}")

    return len(df)


def write_road_type_splits(
    df: pd.DataFrame,
    output_dir: Path,
    road_types: Dict[str, dict],
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Apply each road type filter and write files. Returns {type: row_count}."""
    counts = {}
    for road_type, spec in road_types.items():
        filtered = spec["filter"](df) if spec["filter"] is not None else df
        counts[road_type] = write_csv(
            filtered,
            output_dir / f"{road_type}.csv",
            gzip_output,
            dry_run,
        )
    return counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 1: STATE LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_state_level(
    df: pd.DataFrame,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Write _state/ files. Uses ROAD_TYPES_STATE_REGION (SET A).
    Also writes statewide_all_roads.csv alias for batch-pipeline compatibility.
    """
    print(f"\n  [STATE] _state/ — {len(df):,} total records")
    state_dir = output_dir / "_state"
    counts = write_road_type_splits(df, state_dir, ROAD_TYPES_STATE_REGION, gzip_output, dry_run)
    # Alias: batch-all-jurisdictions.yml uploads the normalized CSV as
    # statewide_all_roads.csv before triggering this pipeline.
    # We write it here so split.py is also correct when run standalone.
    counts["statewide_all_roads"] = write_csv(
        df, state_dir / "statewide_all_roads.csv", gzip_output, dry_run
    )
    return counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 2: REGION / DOT DISTRICT LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_region_level(
    df: pd.DataFrame,
    hierarchy: dict,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Write _region/{region_id}/ files. Uses ROAD_TYPES_STATE_REGION (SET A).

    Strategy A (column): groups by "DOT District" column values.
      - Virginia: "1. Bristol", "2. Salem", ..., "9. Northern Virginia"
      - Other enriched states: DOT district names from geo_resolver

    Strategy B (hierarchy): reads hierarchy["regions"], maps region FIPS
      lists to county names, filters df by Physical Juris Name membership.
    """
    strategy = detect_strategy(df, "DOT District")
    print(f"\n  [REGION] Strategy: {strategy.upper()}")
    all_counts: Dict[str, Dict[str, int]] = {}

    if strategy == "column":
        groups = df[df["DOT District"].fillna("").str.strip() != ""].groupby("DOT District")
        for region_name, region_df in groups:
            region_key = name_to_r2_key(str(region_name))
            if not region_key:
                continue
            print(f"    {str(region_name):<35} -> _region/{region_key}/ ({len(region_df):,})")
            all_counts[region_key] = write_road_type_splits(
                region_df, output_dir / "_region" / region_key,
                ROAD_TYPES_STATE_REGION, gzip_output, dry_run
            )
    else:
        regions = hierarchy.get("regions", {})
        if not regions:
            print("    No regions in hierarchy — skipping")
            return {}
        fips_to_name = build_fips_to_name(hierarchy)
        county_map = build_entity_county_map(regions, fips_to_name)
        juris_col = "Physical Juris Name"
        for region_id, county_names in county_map.items():
            region_name = regions[region_id].get("name", region_id)
            region_df = df[
                df[juris_col].fillna("").apply(strip_juris_prefix).isin(county_names)
            ]
            if region_df.empty:
                continue
            print(f"    {region_name:<35} -> _region/{region_id}/ ({len(region_df):,})")
            all_counts[region_id] = write_road_type_splits(
                region_df, output_dir / "_region" / region_id,
                ROAD_TYPES_STATE_REGION, gzip_output, dry_run
            )

    return all_counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 3: MPO LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_mpo_level(
    df: pd.DataFrame,
    hierarchy: dict,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Write _mpo/{mpo_id}/ files. Uses ROAD_TYPES_LOCAL (SET B).

    Strategy A (column): groups by "MPO Name" column values.
      - Virginia: short acronyms "HAMP", "NOVA", "RICH", "ROAN", etc.
      - Delaware: "Dover/Kent County MPO", "WILMAPCO", etc.

    Strategy B (hierarchy): reads hierarchy["tprs"] (tprs = MPOs),
      maps FIPS lists to county names, filters by Physical Juris Name.
    """
    strategy = detect_strategy(df, "MPO Name")
    print(f"\n  [MPO] Strategy: {strategy.upper()}")
    all_counts: Dict[str, Dict[str, int]] = {}

    if strategy == "column":
        groups = df[df["MPO Name"].fillna("").str.strip() != ""].groupby("MPO Name")
        for mpo_name, mpo_df in groups:
            mpo_key = name_to_r2_key(str(mpo_name))
            if not mpo_key:
                continue
            print(f"    {str(mpo_name):<35} -> _mpo/{mpo_key}/ ({len(mpo_df):,})")
            all_counts[mpo_key] = write_road_type_splits(
                mpo_df, output_dir / "_mpo" / mpo_key,
                ROAD_TYPES_LOCAL, gzip_output, dry_run
            )
    else:
        tprs = hierarchy.get("tprs", {})
        if not tprs:
            print("    No MPOs in hierarchy — skipping")
            return {}
        fips_to_name = build_fips_to_name(hierarchy)
        county_map = build_entity_county_map(tprs, fips_to_name)
        juris_col = "Physical Juris Name"
        for mpo_id, county_names in county_map.items():
            mpo_name = tprs[mpo_id].get("name", mpo_id)
            mpo_df = df[
                df[juris_col].fillna("").apply(strip_juris_prefix).isin(county_names)
            ]
            if mpo_df.empty:
                continue
            print(f"    {mpo_name[:35]:<35} -> _mpo/{mpo_id}/ ({len(mpo_df):,})")
            all_counts[mpo_id] = write_road_type_splits(
                mpo_df, output_dir / "_mpo" / mpo_id,
                ROAD_TYPES_LOCAL, gzip_output, dry_run
            )

    return all_counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 4: PLANNING DISTRICT LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_planning_district_level(
    df: pd.DataFrame,
    hierarchy: dict,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Write _planning_district/{pd_id}/ files. Uses ROAD_TYPES_LOCAL (SET B).

    Strategy A (column): groups by "Planning District" column values.
      - Virginia: "Northern Virginia", "Hampton Roads", "Thomas Jefferson", etc.

    Strategy B (hierarchy): reads hierarchy["planningDistricts"],
      maps FIPS lists to county names, filters by Physical Juris Name.
    """
    strategy = detect_strategy(df, "Planning District")
    print(f"\n  [PLANNING DISTRICT] Strategy: {strategy.upper()}")
    all_counts: Dict[str, Dict[str, int]] = {}

    if strategy == "column":
        groups = df[df["Planning District"].fillna("").str.strip() != ""].groupby("Planning District")
        for pd_name, pd_df in groups:
            pd_key = name_to_r2_key(str(pd_name))
            if not pd_key:
                continue
            print(f"    {str(pd_name):<35} -> _planning_district/{pd_key}/ ({len(pd_df):,})")
            all_counts[pd_key] = write_road_type_splits(
                pd_df, output_dir / "_planning_district" / pd_key,
                ROAD_TYPES_LOCAL, gzip_output, dry_run
            )
    else:
        pds = hierarchy.get("planningDistricts", {})
        if not pds:
            print("    No planning districts in hierarchy — skipping")
            return {}
        fips_to_name = build_fips_to_name(hierarchy)
        county_map = build_entity_county_map(pds, fips_to_name)
        juris_col = "Physical Juris Name"
        for pd_id, county_names in county_map.items():
            pd_name = pds[pd_id].get("name", pd_id)
            pd_df = df[
                df[juris_col].fillna("").apply(strip_juris_prefix).isin(county_names)
            ]
            if pd_df.empty:
                continue
            print(f"    {pd_name:<35} -> _planning_district/{pd_id}/ ({len(pd_df):,})")
            all_counts[pd_id] = write_road_type_splits(
                pd_df, output_dir / "_planning_district" / pd_id,
                ROAD_TYPES_LOCAL, gzip_output, dry_run
            )

    return all_counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 5: COUNTY LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_county_level(
    df: pd.DataFrame,
    hierarchy: dict,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Write {county_key}/ files. Uses ROAD_TYPES_LOCAL (SET B).

    Always uses Physical Juris Name groupby — no column/hierarchy choice
    needed because Physical Juris Name is populated by every normalize.py.

    County identification:
      VDOT-style:  "001. Accomack County" contains "County" -> county
      Plain names: "Kent", "New Castle" -> matched against allCounties values

    City/Town entries are EXCLUDED here — they go to split_city_level().

    R2 key:
      "001. Accomack County" -> strip prefix -> "Accomack County" -> "accomack_county"
      "New Castle"           -> "new_castle"
    """
    juris_col = "Physical Juris Name"
    if juris_col not in df.columns:
        print(f"\n  [COUNTY] '{juris_col}' missing — skipping")
        return {}

    print(f"\n  [COUNTY] Splitting by Physical Juris Name")

    fips_to_name = build_fips_to_name(hierarchy) if hierarchy.get("allCounties") else {}
    known_county_names = {name.lower() for name in fips_to_name.values()}

    all_counts: Dict[str, Dict[str, int]] = {}
    skipped = []

    for raw_name in sorted(df[juris_col].dropna().unique()):
        clean = strip_juris_prefix(str(raw_name).strip())
        if not clean:
            continue

        entity_type = classify_juris_name(raw_name)

        # For plain-name states, cross-check against known county names
        if entity_type == "unknown" and clean.lower() in known_county_names:
            entity_type = "county"

        # Skip cities — they go to split_city_level()
        if entity_type == "city":
            continue

        county_df = df[df[juris_col] == raw_name]
        if county_df.empty:
            continue

        county_key = name_to_r2_key(clean)
        if not county_key:
            skipped.append(raw_name)
            continue

        print(f"    {str(raw_name):<35} -> {county_key}/ ({len(county_df):,})")
        all_counts[county_key] = write_road_type_splits(
            county_df, output_dir / county_key,
            ROAD_TYPES_LOCAL, gzip_output, dry_run
        )

    if skipped:
        print(f"    Warning: Skipped {len(skipped)} unresolvable names: {skipped}")

    return all_counts


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 6: CITY / TOWN LEVEL
# ─────────────────────────────────────────────────────────────────────────────

def split_city_level(
    df: pd.DataFrame,
    output_dir: Path,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Write _city/{city_slug}/ files. Uses ROAD_TYPES_LOCAL (SET B).

    Virginia has ~130 independent cities and towns in Physical Juris Name:
      "100. City of Richmond", "200. Town of Leesburg", etc.

    For most non-Virginia states, this phase runs but produces no output
    because Physical Juris Name contains only county-type entries. The
    overhead is negligible (just iterates the unique values list).

    R2 key:
      "100. City of Richmond"  -> strip -> "City of Richmond" -> "city_of_richmond"
      "200. Town of Leesburg"  -> "town_of_leesburg"
    """
    juris_col = "Physical Juris Name"
    if juris_col not in df.columns:
        return {}

    city_entries = [
        n for n in df[juris_col].dropna().unique()
        if classify_juris_name(n) == "city"
    ]

    if not city_entries:
        print(f"\n  [CITY] No city/town entries in Physical Juris Name — skipping")
        return {}

    print(f"\n  [CITY] Splitting {len(city_entries)} city/town entries")
    all_counts: Dict[str, Dict[str, int]] = {}

    for raw_name in sorted(city_entries):
        clean    = strip_juris_prefix(str(raw_name).strip())
        city_df  = df[df[juris_col] == raw_name]
        if city_df.empty:
            continue
        city_key = name_to_r2_key(clean)
        if not city_key:
            continue
        print(f"    {str(raw_name):<35} -> _city/{city_key}/ ({len(city_df):,})")
        all_counts[city_key] = write_road_type_splits(
            city_df, output_dir / "_city" / city_key,
            ROAD_TYPES_LOCAL, gzip_output, dry_run
        )

    return all_counts


# ─────────────────────────────────────────────────────────────────────────────
#  R2 UPLOAD
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_r2(
    output_dir: Path,
    state_prefix: str,
    r2_bucket: str,
    r2_endpoint: str,
    gzip_output: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Upload all split files to Cloudflare R2 using aws s3 cp.
    Each file at output_dir/{path} -> s3://{bucket}/{state_prefix}/{path}.
    Retries 4 times with exponential backoff on failure.
    Returns count of successfully uploaded files.
    """
    ext   = ".csv.gz" if gzip_output else ".csv"
    files = sorted(output_dir.rglob(f"*{ext}"))

    if not files:
        print(f"  Warning: No {ext} files found in {output_dir}")
        return 0

    print(f"\n  [R2 UPLOAD] {len(files)} files -> s3://{r2_bucket}/{state_prefix}/")
    uploaded = 0

    for local_path in files:
        r2_key = f"{state_prefix}/{local_path.relative_to(output_dir)}"
        if dry_run:
            print(f"    [DRY] -> {r2_key}")
            uploaded += 1
            continue

        cmd = [
            "aws", "s3", "cp", str(local_path),
            f"s3://{r2_bucket}/{r2_key}",
            "--endpoint-url", r2_endpoint,
            "--content-type", "text/csv",
            "--only-show-errors",
        ]
        if gzip_output:
            cmd += ["--content-encoding", "gzip"]

        success = False
        for attempt in range(1, 5):
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if result.returncode == 0:
                    success = True
                    break
                delay = 2 ** attempt
                print(f"    Attempt {attempt}/4 failed for {r2_key} — retry in {delay}s")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                print(f"    TIMEOUT: {r2_key}")
                break
            except FileNotFoundError:
                print("  aws CLI not found — install awscli or skip --upload-r2")
                return uploaded

        if success:
            uploaded += 1
        else:
            print(f"    FAILED after 4 attempts: {r2_key}")

    print(f"  Uploaded {uploaded}/{len(files)} files")
    return uploaded


# ─────────────────────────────────────────────────────────────────────────────
#  MANIFEST WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_manifest(
    state_prefix: str,
    output_dir: Path,
    stats: dict,
    elapsed: float,
    total_rows: int,
    strategies: dict,
    dry_run: bool = False,
) -> dict:
    """
    Write split_manifest.json summarizing all generated files.
    Read by batch-pipeline.yml to verify split success before Stage 4 upload.
    """
    manifest = {
        "state":            state_prefix,
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "elapsed_s":        round(elapsed, 1),
        "total_input_rows": total_rows,
        "dry_run":          dry_run,
        "strategies":       strategies,
        "splits":           stats,
    }
    if not dry_run:
        p = output_dir / "split_manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest -> {p}")
    return manifest


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def split(
    input_path: str,
    state_prefix: str,
    hierarchy_path: Optional[str] = None,
    output_dir_str: Optional[str] = None,
    gzip_output: bool = False,
    upload_r2: bool = False,
    r2_bucket: str = "crash-lens-data",
    r2_endpoint: str = "",
    county_only: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Orchestrates all 6 split phases.

    Phase order:
      1. State level     _state/               SET A road types
      2. Region level    _region/{id}/         SET A road types
      3. MPO level       _mpo/{id}/            SET B road types
      4. Planning Dist   _planning_district/   SET B road types
      5. County level    {county_key}/         SET B road types
      6. City level      _city/{slug}/         SET B road types
    """
    t0  = time.time()
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"Input not found: {src}")

    output_dir = Path(output_dir_str) if output_dir_str else src.parent / f"{state_prefix}_splits"

    print(f"\n{'='*65}")
    print(f"  CrashLens split.py v2.0  |  {datetime.now():%Y-%m-%d %H:%M}")
    print(f"  Input  : {src.name}")
    print(f"  State  : {state_prefix}")
    print(f"  Output : {output_dir}")
    print(f"  Flags  : gzip={gzip_output}  upload={upload_r2}  dry_run={dry_run}")
    print(f"{'='*65}\n")

    # Auto-detect hierarchy.json
    if not hierarchy_path:
        for candidate in [
            _SCRIPT_DIR / "hierarchy.json",
            _REPO_ROOT / "states" / state_prefix / "hierarchy.json",
            src.parent / "hierarchy.json",
        ]:
            if candidate.exists():
                hierarchy_path = str(candidate)
                print(f"  hierarchy.json: {candidate} (auto-detected)")
                break

    if hierarchy_path and Path(hierarchy_path).exists():
        hierarchy = load_hierarchy(hierarchy_path)
        print(f"  Hierarchy loaded: {len(hierarchy['allCounties'])} counties, "
              f"{len(hierarchy['regions'])} regions, "
              f"{len(hierarchy['tprs'])} MPOs, "
              f"{len(hierarchy['planningDistricts'])} PDs")
    else:
        print("  No hierarchy.json — column-based split only (Strategy A)")
        hierarchy = {"state": {}, "regions": {}, "tprs": {}, "planningDistricts": {}, "allCounties": {}}

    # Load normalized CSV
    print(f"\n  Loading {src.name}...")
    df = pd.read_csv(src, dtype=str, low_memory=False)
    total_rows = len(df)
    print(f"  Loaded {total_rows:,} rows x {len(df.columns)} columns")

    # Validate standard columns exist
    required = {"Physical Juris Name", "Crash Severity", "Ownership", "Functional Class"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        print(f"  WARNING: Missing standard columns: {missing_cols}")
        print(f"  Ensure input is the output of {{state}}_normalize.py")

    # Detect strategies upfront (for manifest and logging)
    strategies = {
        "region":            detect_strategy(df, "DOT District"),
        "mpo":               detect_strategy(df, "MPO Name"),
        "planning_district": detect_strategy(df, "Planning District"),
    }
    print(f"\n  Split strategies:")
    for tier, strat in strategies.items():
        print(f"    {tier:<25} -> {strat.upper()}")

    # Run all phases
    stats = {}

    stats["state"] = split_state_level(df, output_dir, gzip_output, dry_run)

    if not county_only:
        stats["regions"]            = split_region_level(df, hierarchy, output_dir, gzip_output, dry_run)
        stats["mpos"]               = split_mpo_level(df, hierarchy, output_dir, gzip_output, dry_run)
        stats["planning_districts"] = split_planning_district_level(df, hierarchy, output_dir, gzip_output, dry_run)

    stats["counties"] = split_county_level(df, hierarchy, output_dir, gzip_output, dry_run)
    stats["cities"]   = split_city_level(df, output_dir, gzip_output, dry_run)

    elapsed = time.time() - t0

    manifest = write_manifest(state_prefix, output_dir, stats, elapsed, total_rows, strategies, dry_run)

    if upload_r2 and not dry_run:
        endpoint = r2_endpoint or os.environ.get("R2_ENDPOINT", "")
        if endpoint:
            upload_to_r2(output_dir, state_prefix, r2_bucket, endpoint, gzip_output, dry_run)
        else:
            print("\n  R2 upload skipped — set --r2-endpoint or R2_ENDPOINT env var")

    # Summary
    n_r = len(stats.get("regions", {}))
    n_m = len(stats.get("mpos", {}))
    n_p = len(stats.get("planning_districts", {}))
    n_c = len(stats.get("counties", {}))
    n_ci= len(stats.get("cities", {}))
    a   = len(ROAD_TYPES_STATE_REGION)
    b   = len(ROAD_TYPES_LOCAL)
    total_files = (a + 1) + n_r*a + n_m*b + n_p*b + n_c*b + n_ci*b

    print(f"\n{'='*65}")
    print(f"  Done in {elapsed:.1f}s")
    print(f"  Rows    : {total_rows:,}")
    print(f"  State   : 1  ({a} files + 1 alias)")
    print(f"  Regions : {n_r}  ({n_r*a} files)")
    print(f"  MPOs    : {n_m}  ({n_m*b} files)")
    print(f"  PDs     : {n_p}  ({n_p*b} files)")
    print(f"  Counties: {n_c}  ({n_c*b} files)")
    print(f"  Cities  : {n_ci}  ({n_ci*b} files)")
    print(f"  Total   : ~{total_files} CSV files")
    print(f"  Output  : {output_dir}")
    print(f"{'='*65}\n")

    return manifest


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CrashLens Universal Jurisdiction & Road Type Splitter v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Road type sets produced:
  State/Region: all_roads | dot_roads | primary_roads | non_dot_roads
  Local tiers:  all_roads | county_roads | city_roads | no_interstate

Split strategy per tier (auto-detected):
  COLUMN    - direct groupby on DOT District / MPO Name / Planning District
  HIERARCHY - fallback using hierarchy.json county membership maps

Examples:
  python split.py --input delaware_statewide_normalized.csv --state delaware
  python split.py --input virginia_statewide_normalized.csv --state virginia --gzip --upload-r2 --r2-endpoint https://xxx.r2.cloudflarestorage.com
  python split.py --input statewide.csv --state delaware --dry-run
  python split.py --input statewide.csv --state delaware --county-only
        """,
    )
    parser.add_argument("--input",        "-i", required=True,  help="Statewide normalized CSV path")
    parser.add_argument("--state",        "-s", required=True,  help="State prefix lowercase (delaware, virginia)")
    parser.add_argument("--hierarchy",          default=None,   help="hierarchy.json path (auto-detected if omitted)")
    parser.add_argument("--output-dir",   "-o", default=None,   help="Output dir (default: {input_dir}/{state}_splits/)")
    parser.add_argument("--gzip",               action="store_true", help="Write .csv.gz files")
    parser.add_argument("--upload-r2",          action="store_true", help="Upload to R2 after writing")
    parser.add_argument("--r2-bucket",          default="crash-lens-data", help="R2 bucket name")
    parser.add_argument("--r2-endpoint",        default="",     help="R2 endpoint URL (or set R2_ENDPOINT env var)")
    parser.add_argument("--county-only",        action="store_true", help="State + county phases only")
    parser.add_argument("--dry-run",            action="store_true", help="Show files without writing")

    args = parser.parse_args()

    try:
        split(
            input_path=args.input,
            state_prefix=args.state,
            hierarchy_path=args.hierarchy,
            output_dir_str=args.output_dir,
            gzip_output=args.gzip,
            upload_r2=args.upload_r2,
            r2_bucket=args.r2_bucket,
            r2_endpoint=args.r2_endpoint,
            county_only=args.county_only,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(f"\n  Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
