#!/usr/bin/env python3
"""
Enhance CRASH LENS geographic data files using downloaded Census/BTS data.

Reads raw data from states/geography/ and enhances (never destructively overwrites):
  1. states/us_counties_db.js     — Add missing counties, update coordinates
  2. states/fips_database.js      — Update state centroids/bounds from Census
  3. docs/CDOT/us_states_dot_districts_mpos.json — Add BTS MPO data
  4. states/{state}/hierarchy.json — Add missing counties, add MPOs

IMPORTANT: This script PRESERVES all existing data. It only ADDS missing entries
and updates coordinates where the Census data is more accurate.

Usage:
    python scripts/enhance_geographic_data.py                    # All updates
    python scripts/enhance_geographic_data.py --target counties  # Just us_counties_db.js
    python scripts/enhance_geographic_data.py --target fips      # Just fips_database.js
    python scripts/enhance_geographic_data.py --target mpos      # Just MPO file
    python scripts/enhance_geographic_data.py --target hierarchy # Just hierarchy.json files
    python scripts/enhance_geographic_data.py --dry-run          # Preview changes
"""

import json
import re
import argparse
import copy
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
GEO_DIR = ROOT / "states" / "geography"
STATES_DIR = ROOT / "states"

# ─── State reference ─────────────────────────────────────────────────────────

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

FIPS_TO_STATE_NAME = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming"
}

FIPS_TO_DIR_NAME = {}
for fips, name in FIPS_TO_STATE_NAME.items():
    FIPS_TO_DIR_NAME[fips] = name.lower().replace(" ", "_")


# ─── Load geography data ─────────────────────────────────────────────────────

def load_geo(filename):
    path = GEO_DIR / filename
    if not path.exists():
        print(f"  ✗ {path} not found")
        return []
    with open(path) as f:
        data = json.load(f)
    return data.get("records", data.get("data", data if isinstance(data, list) else []))


# ─── Helper: make county key from name ────────────────────────────────────────

def make_county_key(name, lsadc="06"):
    """
    Convert a Census county name to a JS-safe key.
    Examples:
      'Henrico County' → 'henrico'
      'Alexandria city' → 'alexandria_city'
      'St. Louis County' → 'st_louis'
      "Prince George's County" → 'prince_georges'
    """
    # Remove suffix like " County", " Parish", " Borough", " Census Area", " Municipality"
    base = name
    for suffix in [" County", " Parish", " Borough", " Census Area",
                   " Municipality", " city and Borough"]:
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break

    # For VA independent cities (LSADC=25), keep " city" → "_city"
    # For other LSADC, already stripped above
    if lsadc == "25" and not base.endswith(" City") and not base.endswith(" city"):
        # Census uses "city" lowercase for VA independent cities
        pass

    key = base.lower().strip()
    key = key.replace("'", "").replace("'", "")
    key = key.replace(".", "")
    key = key.replace(" ", "_")
    key = key.replace("-", "_")
    key = re.sub(r'[^a-z0-9_]', '', key)
    key = re.sub(r'_+', '_', key)
    return key


def get_county_type(lsadc):
    """Map LSADC code to type string."""
    types = {
        "06": "county",
        "03": "city_borough",     # City and Borough (AK)
        "04": "borough",          # Borough (AK)
        "12": "parish",           # Parish (LA)
        "13": "municipality",     # Municipality (AK)
        "15": "city",             # city (VA independent cities - Census uses lowercase)
        "25": "city",             # city (other independent cities)
        "00": "county",           # Consolidated city
    }
    return types.get(lsadc, "county")


# ═══════════════════════════════════════════════════════════════════════════════
#  1. ENHANCE us_counties_db.js
# ═══════════════════════════════════════════════════════════════════════════════

def enhance_counties_db(dry_run=False):
    """Add missing counties to us_counties_db.js using Census data."""
    print("\n═══ Enhancing us_counties_db.js ═══")

    counties = load_geo("us_counties.json")
    if not counties:
        return

    js_path = STATES_DIR / "us_counties_db.js"
    js_content = js_path.read_text()

    # Parse existing entries per state by extracting FIPS codes already present
    # Find state block boundaries, then extract f:'XXX' within each block
    existing_by_state = defaultdict(set)
    state_starts = [(m.group(1), m.start()) for m in re.finditer(r"'(\d{2})':\s*\{", js_content)]

    for i, (state_fips, start) in enumerate(state_starts):
        # Block ends where next state starts (or end of file)
        end = state_starts[i + 1][1] if i + 1 < len(state_starts) else len(js_content)
        block = js_content[start:end]
        fips_matches = re.findall(r"f:'(\d{3})'", block)
        existing_by_state[state_fips].update(fips_matches)

    # Group downloaded counties by state
    counties_by_state = defaultdict(list)
    for c in counties:
        sf = c.get("STATE", "")
        if sf in FIPS_TO_ABBR:
            counties_by_state[sf].append(c)

    # Find missing counties
    total_added = 0
    additions_by_state = {}

    for sf in sorted(counties_by_state.keys()):
        existing = existing_by_state.get(sf, set())
        missing = []

        for c in counties_by_state[sf]:
            county_fips = c.get("COUNTY", "")
            if county_fips not in existing:
                missing.append(c)

        if missing:
            additions_by_state[sf] = missing
            total_added += len(missing)

    print(f"  Found {total_added} missing counties across {len(additions_by_state)} states")

    if total_added == 0:
        print("  ✓ us_counties_db.js is already complete")
        return

    # Show what's missing
    for sf in sorted(additions_by_state.keys()):
        missing = additions_by_state[sf]
        state_name = FIPS_TO_STATE_NAME.get(sf, sf)
        names = [c["NAME"] for c in missing[:5]]
        more = f" (+{len(missing)-5} more)" if len(missing) > 5 else ""
        print(f"  {state_name} ({sf}): +{len(missing)} — {', '.join(names)}{more}")

    if dry_run:
        print("  [DRY RUN] No changes written")
        return

    # Generate new JS entries and insert them
    for sf in sorted(additions_by_state.keys()):
        missing = additions_by_state[sf]
        new_entries = []
        for c in missing:
            county_fips = c.get("COUNTY", "")
            name = c.get("NAME", "")
            basename = c.get("BASENAME", "")
            lsadc = c.get("LSADC", "06")
            lat = c.get("INTPTLAT", 0)
            lon = c.get("INTPTLON", 0)

            key = make_county_key(name, lsadc)
            ctype = get_county_type(lsadc)
            display_name = name

            # Build search patterns
            patterns = [basename.upper(), basename, name]
            patterns_str = json.dumps(patterns)

            entry = (f"            '{key}': {{n:\"{display_name}\",t:'{ctype}',"
                     f"f:'{county_fips}',c:[{lat},{lon}],z:10,"
                     f"p:{patterns_str}}}")
            new_entries.append(entry)

        # Find the closing of this state's block and insert before it
        # Look for the pattern: 'XX': {\n ... \n        },
        state_pattern = re.compile(
            rf"('{sf}':\s*\{{[^}}]*(?:\{{[^}}]*\}}[^}}]*)*?)(\n\s*\}})",
            re.DOTALL
        )
        match = state_pattern.search(js_content)
        if match:
            insert_point = match.end(1)
            new_entries_str = ",\n" + ",\n".join(new_entries)
            js_content = js_content[:insert_point] + new_entries_str + js_content[insert_point:]
            print(f"    ✓ Added {len(new_entries)} entries to state {sf}")
        else:
            print(f"    ⚠ Could not find state block for {sf} — skipping")

    js_path.write_text(js_content)
    print(f"  ✓ Saved {js_path.name} with {total_added} new counties")


# ═══════════════════════════════════════════════════════════════════════════════
#  2. ENHANCE fips_database.js
# ═══════════════════════════════════════════════════════════════════════════════

def enhance_fips_database(dry_run=False):
    """Update state centroids and bounds in fips_database.js from Census data."""
    print("\n═══ Enhancing fips_database.js ═══")

    states = load_geo("us_states.json")
    if not states:
        return

    js_path = STATES_DIR / "fips_database.js"
    content = js_path.read_text()

    updates = 0
    for s in states:
        fips = s.get("GEOID", s.get("STATE", ""))
        if fips not in FIPS_TO_ABBR:
            continue

        lat = s.get("INTPTLAT")
        lon = s.get("INTPTLON")
        if not lat or not lon:
            continue

        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            continue

        # Check if state exists in STATES object
        state_pattern = rf"'{fips}':\s*\{{[^}}]*center:\s*\[([^\]]+)\]"
        match = re.search(state_pattern, content)
        if match:
            old_center = match.group(1)
            new_center = f"{lat:.6f}, {lon:.6f}"
            if old_center.strip() != new_center:
                updates += 1
                if not dry_run:
                    content = content.replace(
                        f"center: [{old_center}]",
                        f"center: [{new_center}]",
                        1
                    )

    if updates:
        if not dry_run:
            js_path.write_text(content)
        print(f"  ✓ Updated {updates} state centroids" + (" [DRY RUN]" if dry_run else ""))
    else:
        print("  ✓ All state centroids already accurate")


# ═══════════════════════════════════════════════════════════════════════════════
#  3. ENHANCE us_states_dot_districts_mpos.json with BTS MPO data
# ═══════════════════════════════════════════════════════════════════════════════

def enhance_mpo_data(dry_run=False):
    """Add BTS MPO entries to us_states_dot_districts_mpos.json."""
    print("\n═══ Enhancing us_states_dot_districts_mpos.json ═══")

    mpos = load_geo("us_mpos.json")
    if not mpos:
        return

    mpo_file = ROOT / "docs" / "CDOT" / "us_states_dot_districts_mpos.json"
    with open(mpo_file) as f:
        mpo_data = json.load(f)

    # Group BTS MPOs by state abbreviation
    bts_by_state = defaultdict(list)
    for m in mpos:
        state_abbr = m.get("STATE", "")
        if state_abbr and len(state_abbr) == 2:
            bts_by_state[state_abbr].append(m)

    total_added = 0
    total_enriched = 0

    for state_abbr, bts_mpos in sorted(bts_by_state.items()):
        state_data = mpo_data.get("states", {}).get(state_abbr)
        if not state_data:
            continue

        existing_mpos = state_data.get("mpos", [])
        existing_names = {m.get("name", "").lower() for m in existing_mpos}
        existing_names.update({m.get("acronym", "").lower() for m in existing_mpos if m.get("acronym")})

        for bts in bts_mpos:
            bts_name = bts.get("MPO_NAME", bts.get("NAME", ""))
            bts_name_lower = bts_name.lower()
            bts_id = bts.get("MPO_ID", "")
            lat = bts.get("CENTLAT") or bts.get("INTPTLAT")
            lon = bts.get("CENTLON") or bts.get("INTPTLON")

            # Check if already exists (fuzzy match on name)
            found = False
            for existing in existing_mpos:
                ex_name = existing.get("name", "").lower()
                ex_acro = existing.get("acronym", "").lower()
                if (bts_name_lower in ex_name or ex_name in bts_name_lower or
                    (ex_acro and ex_acro in bts_name_lower)):
                    found = True
                    # Enrich existing entry with BTS data
                    if bts_id and not existing.get("btsMpoId"):
                        existing["btsMpoId"] = str(bts_id)
                        total_enriched += 1
                    if lat and lon and not existing.get("mapCenter"):
                        try:
                            existing["mapCenter"] = {"lat": float(lat), "lng": float(lon)}
                        except (ValueError, TypeError):
                            pass
                    break

            if not found:
                # New MPO — add it
                new_entry = {
                    "name": bts_name,
                    "btsMpoId": str(bts_id),
                    "source": "BTS/USDOT NTAD"
                }
                if lat and lon:
                    try:
                        new_entry["mapCenter"] = {"lat": float(lat), "lng": float(lon)}
                    except (ValueError, TypeError):
                        pass
                existing_mpos.append(new_entry)
                total_added += 1

        state_data["mpos"] = existing_mpos

    print(f"  Added {total_added} new MPOs, enriched {total_enriched} existing")

    if not dry_run:
        with open(mpo_file, "w") as f:
            json.dump(mpo_data, f, indent=2)
        print(f"  ✓ Saved {mpo_file.name}")
    else:
        print("  [DRY RUN] No changes written")


# ═══════════════════════════════════════════════════════════════════════════════
#  4. ENHANCE hierarchy.json files
# ═══════════════════════════════════════════════════════════════════════════════

def enhance_hierarchies(dry_run=False):
    """Add missing counties and MPOs to each state's hierarchy.json."""
    print("\n═══ Enhancing hierarchy.json files ═══")

    counties = load_geo("us_counties.json")
    mpos = load_geo("us_mpos.json")
    places = load_geo("us_places.json")

    if not counties:
        return

    # Group data by state
    counties_by_state = defaultdict(list)
    for c in counties:
        sf = c.get("STATE", "")
        if sf in FIPS_TO_ABBR:
            counties_by_state[sf].append(c)

    mpos_by_state = defaultdict(list)
    for m in mpos:
        abbr = m.get("STATE", "")
        sf = next((k for k, v in FIPS_TO_ABBR.items() if v == abbr), "")
        if sf:
            mpos_by_state[sf].append(m)

    places_by_state = defaultdict(list)
    for p in places:
        sf = p.get("STATE", "")
        if sf in FIPS_TO_ABBR and p.get("FUNCSTAT") == "A":
            places_by_state[sf].append(p)

    total_counties_added = 0
    total_mpos_added = 0
    total_files_updated = 0

    for sf in sorted(FIPS_TO_ABBR.keys()):
        dir_name = FIPS_TO_DIR_NAME.get(sf)
        if not dir_name:
            continue

        hierarchy_path = STATES_DIR / dir_name / "hierarchy.json"
        if not hierarchy_path.exists():
            continue

        with open(hierarchy_path) as f:
            hierarchy = json.load(f)

        modified = False
        state_counties_added = 0
        state_mpos_added = 0

        # ── Add missing counties to allCounties ──
        all_counties = hierarchy.get("allCounties", {})
        for c in counties_by_state.get(sf, []):
            county_fips = c.get("COUNTY", "")
            if county_fips not in all_counties:
                basename = c.get("BASENAME", c.get("NAME", ""))
                lsadc = c.get("LSADC", "06")
                # For VA independent cities, include "City" suffix
                if lsadc == "25":
                    display = basename if basename.endswith(" City") or basename.endswith(" city") else basename
                else:
                    display = basename
                all_counties[county_fips] = display
                state_counties_added += 1
                modified = True

        if state_counties_added:
            hierarchy["allCounties"] = dict(sorted(all_counties.items()))

        # ── Add BTS MPO data to tprs section ──
        tprs = hierarchy.get("tprs", {})
        existing_mpo_names = set()
        for mpo_data in tprs.values():
            existing_mpo_names.add(mpo_data.get("name", "").lower())
            if mpo_data.get("shortName"):
                existing_mpo_names.add(mpo_data["shortName"].lower())

        for m in mpos_by_state.get(sf, []):
            mpo_name = m.get("MPO_NAME", m.get("NAME", ""))
            mpo_id = m.get("MPO_ID", "")
            lat = m.get("CENTLAT") or m.get("INTPTLAT")
            lon = m.get("CENTLON") or m.get("INTPTLON")

            # Check if already exists
            name_lower = mpo_name.lower()
            already_exists = any(
                name_lower in ex or ex in name_lower
                for ex in existing_mpo_names
            )

            if not already_exists and mpo_name:
                key = make_county_key(mpo_name).replace("_mpo", "").replace("_tpo", "")
                key = key + "_mpo" if not key.endswith("_mpo") else key

                new_mpo = {
                    "name": mpo_name,
                    "shortName": mpo_name.replace(" Metropolitan Planning Organization", "")
                                        .replace(" MPO", "")
                                        .replace(" Area", "").strip(),
                    "type": "mpo",
                    "btsMpoId": str(mpo_id),
                    "source": "BTS/USDOT NTAD",
                    "counties": [],
                    "countyNames": {}
                }
                if lat and lon:
                    try:
                        new_mpo["center"] = [float(lon), float(lat)]
                        new_mpo["zoom"] = 10
                    except (ValueError, TypeError):
                        pass

                tprs[key] = new_mpo
                existing_mpo_names.add(name_lower)
                state_mpos_added += 1
                modified = True

        if state_mpos_added:
            hierarchy["tprs"] = tprs

        # ── Add incorporated places count for reference ──
        state_places = places_by_state.get(sf, [])
        if state_places and "placesCount" not in hierarchy.get("state", {}):
            if "state" in hierarchy:
                hierarchy["state"]["placesCount"] = len(state_places)
                modified = True

        # ── Save if modified ──
        if modified:
            total_counties_added += state_counties_added
            total_mpos_added += state_mpos_added
            total_files_updated += 1

            state_name = FIPS_TO_STATE_NAME.get(sf, sf)
            changes = []
            if state_counties_added:
                changes.append(f"+{state_counties_added} counties")
            if state_mpos_added:
                changes.append(f"+{state_mpos_added} MPOs")
            print(f"  {state_name}: {', '.join(changes)}")

            if not dry_run:
                with open(hierarchy_path, "w") as f:
                    json.dump(hierarchy, f, indent=2)

    print(f"\n  Summary: {total_counties_added} counties + {total_mpos_added} MPOs added across {total_files_updated} files")
    if dry_run:
        print("  [DRY RUN] No changes written")
    else:
        print("  ✓ All hierarchy files updated")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Enhance CRASH LENS geographic data files")
    parser.add_argument("--target", choices=["all", "counties", "fips", "mpos", "hierarchy"],
                        default="all", help="Which file(s) to update")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    print("=" * 60)
    print("  CRASH LENS — Geographic Data Enhancement")
    print("  Source: states/geography/ (Census TIGERweb + BTS)")
    if args.dry_run:
        print("  Mode: DRY RUN (no files will be modified)")
    print("=" * 60)

    if args.target in ("all", "counties"):
        enhance_counties_db(dry_run=args.dry_run)

    if args.target in ("all", "fips"):
        enhance_fips_database(dry_run=args.dry_run)

    if args.target in ("all", "mpos"):
        enhance_mpo_data(dry_run=args.dry_run)

    if args.target in ("all", "hierarchy"):
        enhance_hierarchies(dry_run=args.dry_run)

    print("\n" + "=" * 60)
    print("  ✓ Enhancement complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
