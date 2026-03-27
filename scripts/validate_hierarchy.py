#!/usr/bin/env python3
"""
Validate and auto-heal hierarchy.json for a given state before pipeline execution.

This script runs as Stage 0.1 in the pipeline (after cache init, before downloads).
It detects and fixes gaps that would cause downstream ranking/aggregation failures:

  1. ORPHANED COUNTIES — Counties in allCounties not assigned to any region
  2. EMPTY MPO COUNTIES — MPOs with empty counties[] arrays (can't aggregate)
  3. MISSING COUNTIES — Counties in Census data but missing from allCounties
  4. UNRESOLVABLE FIPS — FIPS codes in regions/MPOs not in allCounties
  5. MISSING MPO ASSIGNMENTS — Counties not assigned to any MPO (info only)

Auto-heal strategy:
  - Uses states/geography/ Census/BTS reference data to fill gaps
  - Assigns orphaned counties to nearest region (by centroid distance)
  - Assigns counties to MPOs using centroid-in-bbox spatial matching
  - Adds missing counties from Census data to allCounties
  - All changes are logged and can be previewed with --dry-run

Usage:
    # Validate single state (pipeline mode)
    python scripts/validate_hierarchy.py --state virginia

    # Validate all states
    python scripts/validate_hierarchy.py --all

    # Preview only
    python scripts/validate_hierarchy.py --state virginia --dry-run

    # Strict mode: exit non-zero if any issues found (for CI gating)
    python scripts/validate_hierarchy.py --state virginia --strict

Exit codes:
    0 = clean (or auto-healed successfully)
    1 = issues found in --strict mode (or --dry-run with issues)
    2 = fatal error (missing files, invalid JSON)
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATES_DIR = ROOT / "states"
GEO_DIR = STATES_DIR / "geography"

FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY"
}

FIPS_TO_STATE_DIR = {}
for fips, abbr in FIPS_TO_ABBR.items():
    names = {
        "01": "alabama", "02": "alaska", "04": "arizona", "05": "arkansas",
        "06": "california", "08": "colorado", "09": "connecticut", "10": "delaware",
        "11": "district_of_columbia", "12": "florida", "13": "georgia", "15": "hawaii",
        "16": "idaho", "17": "illinois", "18": "indiana", "19": "iowa",
        "20": "kansas", "21": "kentucky", "22": "louisiana", "23": "maine",
        "24": "maryland", "25": "massachusetts", "26": "michigan", "27": "minnesota",
        "28": "mississippi", "29": "missouri", "30": "montana", "31": "nebraska",
        "32": "nevada", "33": "new_hampshire", "34": "new_jersey", "35": "new_mexico",
        "36": "new_york", "37": "north_carolina", "38": "north_dakota", "39": "ohio",
        "40": "oklahoma", "41": "oregon", "42": "pennsylvania", "44": "rhode_island",
        "45": "south_carolina", "46": "south_dakota", "47": "tennessee", "48": "texas",
        "49": "utah", "50": "vermont", "51": "virginia", "53": "washington",
        "54": "west_virginia", "55": "wisconsin", "56": "wyoming"
    }
    FIPS_TO_STATE_DIR[fips] = names[fips]


# ─── Census reference data loaders ────────────────────────────────────────────

_census_counties = None
_census_mpos = None


def load_census_counties():
    """Load Census county reference data (cached)."""
    global _census_counties
    if _census_counties is not None:
        return _census_counties

    path = GEO_DIR / "us_counties.json"
    if not path.exists():
        print(f"  ⚠ Census reference data not found: {path}")
        _census_counties = {}
        return _census_counties

    with open(path) as f:
        data = json.load(f)
    records = data.get("records", data if isinstance(data, list) else [])

    # Index by state FIPS → county FIPS → record
    _census_counties = defaultdict(dict)
    for r in records:
        sf = r.get("STATE", "")
        cf = r.get("COUNTY", "")
        if sf and cf:
            _census_counties[sf][cf] = r

    return _census_counties


def load_census_mpos():
    """Load BTS MPO reference data (cached)."""
    global _census_mpos
    if _census_mpos is not None:
        return _census_mpos

    path = GEO_DIR / "us_mpos.json"
    if not path.exists():
        _census_mpos = {}
        return _census_mpos

    with open(path) as f:
        data = json.load(f)
    records = data.get("records", data if isinstance(data, list) else [])

    # Index by state abbreviation
    _census_mpos = defaultdict(list)
    for r in records:
        abbr = r.get("STATE", "")
        if abbr:
            _census_mpos[abbr].append(r)

    return _census_mpos


def get_county_centroid(state_fips, county_fips):
    """Get [lat, lon] for a county from Census data."""
    counties = load_census_counties()
    rec = counties.get(state_fips, {}).get(county_fips)
    if not rec:
        return None
    try:
        lat = float(rec.get("INTPTLAT", 0))
        lon = float(rec.get("INTPTLON", 0))
        return [lat, lon] if lat and lon else None
    except (ValueError, TypeError):
        return None


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


# ─── Validation checks ───────────────────────────────────────────────────────

class HierarchyIssue:
    """A single validation issue with optional auto-fix."""
    def __init__(self, level, check, message, fix_fn=None):
        self.level = level      # "error", "warning", "info"
        self.check = check      # check name
        self.message = message
        self.fix_fn = fix_fn    # callable(hierarchy) → bool (did fix?)

    def __repr__(self):
        icon = {"error": "✗", "warning": "⚠", "info": "ℹ"}.get(self.level, "?")
        return f"  {icon} [{self.check}] {self.message}"


def check_orphaned_counties(hierarchy, state_fips):
    """Find counties in allCounties not assigned to any region."""
    issues = []
    all_counties = hierarchy.get("allCounties", {})
    regions = hierarchy.get("regions", {})

    assigned = set()
    for rdata in regions.values():
        for fips in rdata.get("counties", []):
            assigned.add(fips)

    orphaned = set(all_counties.keys()) - assigned
    if not orphaned:
        return issues

    for fips in sorted(orphaned):
        name = all_counties[fips] if isinstance(all_counties[fips], str) else fips

        def make_fix(f=fips, n=name):
            def fix(h):
                return assign_to_nearest_region(h, f, state_fips)
            return fix

        issues.append(HierarchyIssue(
            "warning", "orphaned_county",
            f"County {fips} ({name}) not assigned to any region",
            fix_fn=make_fix()
        ))

    return issues


def check_empty_mpo_counties(hierarchy, state_fips):
    """Find MPOs with empty counties[] arrays."""
    issues = []
    tprs = hierarchy.get("tprs", {})

    for key, mpo in tprs.items():
        counties = mpo.get("counties", [])
        if not counties:
            name = mpo.get("name", key)
            center = mpo.get("center")

            def make_fix(k=key, c=center):
                def fix(h):
                    return assign_counties_to_mpo(h, k, state_fips)
                return fix

            issues.append(HierarchyIssue(
                "warning", "empty_mpo",
                f"MPO '{name}' ({key}) has no county assignments",
                fix_fn=make_fix()
            ))

    return issues


def check_missing_counties(hierarchy, state_fips):
    """Find counties in Census data missing from allCounties."""
    issues = []
    census = load_census_counties()
    state_counties = census.get(state_fips, {})
    all_counties = hierarchy.get("allCounties", {})

    for fips, rec in sorted(state_counties.items()):
        if fips not in all_counties:
            name = rec.get("BASENAME", rec.get("NAME", fips))

            def make_fix(f=fips, n=name):
                def fix(h):
                    ac = h.get("allCounties", {})
                    ac[f] = n
                    h["allCounties"] = dict(sorted(ac.items()))
                    return True
                return fix

            issues.append(HierarchyIssue(
                "error", "missing_county",
                f"Census county {fips} ({name}) not in allCounties",
                fix_fn=make_fix()
            ))

    return issues


def check_unresolvable_fips(hierarchy):
    """Find FIPS codes in regions/MPOs not present in allCounties."""
    issues = []
    all_counties = set(hierarchy.get("allCounties", {}).keys())

    for section_name in ("regions", "tprs", "mpos"):
        section = hierarchy.get(section_name, {})
        for key, data in section.items():
            for fips in data.get("counties", []):
                if fips not in all_counties:
                    issues.append(HierarchyIssue(
                        "error", "unresolvable_fips",
                        f"FIPS '{fips}' in {section_name}.{key} not in allCounties"
                    ))

    return issues


def check_region_completeness(hierarchy):
    """Check that regions cover all counties (info-level summary)."""
    issues = []
    all_counties = hierarchy.get("allCounties", {})
    regions = hierarchy.get("regions", {})

    if not regions:
        issues.append(HierarchyIssue(
            "info", "no_regions",
            "No regions defined — all counties are unassigned"
        ))
        return issues

    assigned = set()
    for rdata in regions.values():
        for fips in rdata.get("counties", []):
            assigned.add(fips)

    coverage = len(assigned) / len(all_counties) * 100 if all_counties else 0
    if coverage < 100:
        missing = len(all_counties) - len(assigned)
        issues.append(HierarchyIssue(
            "info", "region_coverage",
            f"Region coverage: {coverage:.0f}% ({missing} counties unassigned)"
        ))

    return issues


def check_mpo_coverage(hierarchy):
    """Check MPO county coverage (info-level — rural counties won't be in MPOs)."""
    issues = []
    all_counties = hierarchy.get("allCounties", {})
    tprs = hierarchy.get("tprs", {})
    mpos_section = hierarchy.get("mpos", {})

    mpo_counties = set()
    for data in tprs.values():
        for fips in data.get("counties", []):
            mpo_counties.add(fips)
    for data in mpos_section.values():
        for fips in data.get("counties", []):
            mpo_counties.add(fips)

    coverage = len(mpo_counties) / len(all_counties) * 100 if all_counties else 0
    issues.append(HierarchyIssue(
        "info", "mpo_coverage",
        f"MPO coverage: {coverage:.0f}% ({len(mpo_counties)}/{len(all_counties)} counties in an MPO)"
    ))

    return issues


# ─── Auto-fix functions ──────────────────────────────────────────────────────

def assign_to_nearest_region(hierarchy, county_fips, state_fips):
    """Assign an orphaned county to the nearest region by centroid distance."""
    regions = hierarchy.get("regions", {})
    if not regions:
        return False

    county_centroid = get_county_centroid(state_fips, county_fips)
    if not county_centroid:
        return False

    # Find closest region center
    best_region = None
    best_dist = float("inf")

    for rid, rdata in regions.items():
        center = rdata.get("center")
        if not center:
            continue
        # hierarchy centers are [lon, lat], convert to [lat, lon]
        rlat, rlon = center[1], center[0]
        dist = haversine_km(county_centroid[0], county_centroid[1], rlat, rlon)
        if dist < best_dist:
            best_dist = dist
            best_region = rid

    if not best_region:
        return False

    # Add county to the region
    rdata = regions[best_region]
    counties = rdata.get("counties", [])
    if county_fips not in counties:
        counties.append(county_fips)
        rdata["counties"] = sorted(counties)

    # Also add to countyNames
    all_counties = hierarchy.get("allCounties", {})
    county_name = all_counties.get(county_fips, county_fips)
    if isinstance(county_name, str):
        county_names = rdata.get("countyNames", {})
        county_names[county_fips] = county_name
        rdata["countyNames"] = county_names

    return True


def assign_counties_to_mpo(hierarchy, mpo_key, state_fips):
    """Assign counties to an MPO using centroid-in-bbox spatial matching."""
    tprs = hierarchy.get("tprs", {})
    mpo = tprs.get(mpo_key)
    if not mpo:
        return False

    mpo_center = mpo.get("center")
    if not mpo_center:
        return False

    # MPO center is [lon, lat]
    mpo_lat, mpo_lon = mpo_center[1], mpo_center[0]

    # Find all counties within ~50km of MPO center
    census = load_census_counties()
    state_counties = census.get(state_fips, {})
    all_counties = hierarchy.get("allCounties", {})

    candidates = []
    for fips, rec in state_counties.items():
        if fips not in all_counties:
            continue
        try:
            clat = float(rec.get("INTPTLAT", 0))
            clon = float(rec.get("INTPTLON", 0))
        except (ValueError, TypeError):
            continue

        dist = haversine_km(mpo_lat, mpo_lon, clat, clon)
        # MPOs typically cover a metro area — use 60km radius
        if dist < 60:
            candidates.append((fips, dist, rec.get("BASENAME", fips)))

    if not candidates:
        # Expand to 100km
        for fips, rec in state_counties.items():
            if fips not in all_counties:
                continue
            try:
                clat = float(rec.get("INTPTLAT", 0))
                clon = float(rec.get("INTPTLON", 0))
            except (ValueError, TypeError):
                continue

            dist = haversine_km(mpo_lat, mpo_lon, clat, clon)
            if dist < 100:
                candidates.append((fips, dist, rec.get("BASENAME", fips)))

    if not candidates:
        return False

    # Sort by distance, take closest counties (cap at 10 for safety)
    candidates.sort(key=lambda x: x[1])
    assigned = candidates[:min(10, max(3, len(candidates) // 2))]

    counties = []
    county_names = {}
    for fips, dist, name in assigned:
        counties.append(fips)
        county_names[fips] = name

    mpo["counties"] = sorted(counties)
    mpo["countyNames"] = county_names
    mpo["_autoAssigned"] = True  # Mark for review

    return True


# ─── Main validation orchestrator ────────────────────────────────────────────

def validate_state(state_dir_name, dry_run=False, verbose=True):
    """
    Validate and optionally auto-heal a single state's hierarchy.json.
    Returns (issues_found, issues_fixed, hierarchy_modified).
    """
    hierarchy_path = STATES_DIR / state_dir_name / "hierarchy.json"
    if not hierarchy_path.exists():
        if verbose:
            print(f"  ⚠ No hierarchy.json for {state_dir_name}")
        return 0, 0, False

    with open(hierarchy_path) as f:
        hierarchy = json.load(f)

    state_fips = hierarchy.get("state", {}).get("fips", "")
    state_name = hierarchy.get("state", {}).get("name", state_dir_name)

    if verbose:
        print(f"\n── {state_name} ({state_fips}) ──")

    # Run all checks
    all_issues = []
    all_issues.extend(check_missing_counties(hierarchy, state_fips))
    all_issues.extend(check_unresolvable_fips(hierarchy))
    all_issues.extend(check_orphaned_counties(hierarchy, state_fips))
    all_issues.extend(check_empty_mpo_counties(hierarchy, state_fips))
    all_issues.extend(check_region_completeness(hierarchy))
    all_issues.extend(check_mpo_coverage(hierarchy))

    if not all_issues:
        if verbose:
            print("  ✓ No issues found")
        return 0, 0, False

    # Display issues
    errors = [i for i in all_issues if i.level == "error"]
    warnings = [i for i in all_issues if i.level == "warning"]
    infos = [i for i in all_issues if i.level == "info"]

    if verbose:
        for issue in errors + warnings:
            print(str(issue))
        for issue in infos:
            print(str(issue))

    # Auto-fix fixable issues
    fixed = 0
    modified = False

    if not dry_run:
        for issue in all_issues:
            if issue.fix_fn:
                try:
                    if issue.fix_fn(hierarchy):
                        fixed += 1
                        modified = True
                except Exception as e:
                    if verbose:
                        print(f"  ✗ Fix failed for [{issue.check}]: {e}")

        if modified:
            with open(hierarchy_path, "w") as f:
                json.dump(hierarchy, f, indent=2)
            if verbose:
                print(f"  ✓ Fixed {fixed} issues, saved {hierarchy_path.name}")
    elif verbose and any(i.fix_fn for i in all_issues):
        fixable = sum(1 for i in all_issues if i.fix_fn)
        print(f"  [DRY RUN] {fixable} issues are auto-fixable")

    total_issues = len(errors) + len(warnings)
    return total_issues, fixed, modified


def main():
    parser = argparse.ArgumentParser(
        description="Validate and auto-heal hierarchy.json files"
    )
    parser.add_argument("--state", type=str, help="State directory name (e.g., virginia)")
    parser.add_argument("--all", action="store_true", help="Validate all states")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying")
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero if any issues found (for CI gating)")
    parser.add_argument("--quiet", action="store_true", help="Only show errors/warnings")
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.error("Either --state or --all is required")

    print("=" * 60)
    print("  CRASH LENS — Hierarchy Validation")
    if args.dry_run:
        print("  Mode: DRY RUN")
    print("=" * 60)

    total_issues = 0
    total_fixed = 0
    total_modified = 0

    if args.all:
        for fips in sorted(FIPS_TO_ABBR.keys()):
            state_dir = FIPS_TO_STATE_DIR[fips]
            issues, fixed, modified = validate_state(
                state_dir, dry_run=args.dry_run, verbose=not args.quiet
            )
            total_issues += issues
            total_fixed += fixed
            if modified:
                total_modified += 1
    else:
        issues, fixed, modified = validate_state(
            args.state, dry_run=args.dry_run, verbose=True
        )
        total_issues = issues
        total_fixed = fixed
        total_modified = 1 if modified else 0

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  Issues found: {total_issues}")
    print(f"  Auto-fixed:   {total_fixed}")
    print(f"  Files updated: {total_modified}")
    print(f"{'=' * 60}")

    if args.strict and total_issues > total_fixed:
        remaining = total_issues - total_fixed
        print(f"\n✗ {remaining} unfixed issues remain (--strict mode)")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
