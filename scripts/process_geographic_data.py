#!/usr/bin/env python3
"""
Post-process downloaded geographic data into formats ready for codebase updates.

Reads raw JSON from data/geographic/ (produced by download_geographic_data.py)
and generates update-ready files that Claude Code can use to refresh:
  - states/us_counties_db.js
  - states/fips_database.js
  - docs/CDOT/us_states_dot_districts_mpos.json
  - states/{state}/hierarchy.json

Outputs (saved to data/geographic/processed/):
  - counties_by_state.json    — Counties grouped by state FIPS, ready for us_counties_db.js
  - states_metadata.json      — State centroids/bounds, ready for fips_database.js
  - mpos_by_state.json        — MPOs grouped by state with county members
  - places_by_state.json      — Cities/towns grouped by state + parent county
  - hierarchy_updates.json    — Per-state diffs showing what to add to hierarchy.json

Usage:
    python scripts/process_geographic_data.py
    python scripts/process_geographic_data.py --state 51
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict

INPUT_DIR = Path(__file__).parent.parent / "data" / "geographic"
OUTPUT_DIR = INPUT_DIR / "processed"

# State FIPS → abbreviation (for output keys)
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

FIPS_TO_NAME = {
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


def load_json(filename):
    """Load a downloaded JSON file, returning the data array."""
    filepath = INPUT_DIR / filename
    if not filepath.exists():
        print(f"  ⚠ File not found: {filepath}")
        return []
    with open(filepath) as f:
        raw = json.load(f)
    return raw.get("data", raw) if isinstance(raw, dict) else raw


def process_counties(state_filter=None):
    """Group counties by state, format for us_counties_db.js updates."""
    print("\n── Processing Counties ──")
    counties = load_json("us_counties.json")
    if not counties:
        return {}

    by_state = defaultdict(list)
    for c in counties:
        sf = c.get("stateFips", "")
        if state_filter and sf != state_filter:
            continue
        if sf not in FIPS_TO_ABBR:
            continue

        centroid = c.get("centroid", [0, 0])
        # Convert [lon, lat] → [lat, lon] for JS convention
        lat = centroid[1] if centroid else 0
        lon = centroid[0] if centroid else 0

        by_state[sf].append({
            "fips": c.get("countyFips", ""),
            "name": c.get("name", ""),
            "lsad": c.get("lsad", ""),
            "centroid": [round(lat, 4), round(lon, 4)],  # [lat, lon]
            "bbox": c.get("bbox"),
        })

    # Sort counties within each state by FIPS
    for sf in by_state:
        by_state[sf].sort(key=lambda x: x["fips"])

    total = sum(len(v) for v in by_state.values())
    print(f"  ✓ {total} counties across {len(by_state)} states")
    return dict(by_state)


def process_states():
    """Process state data for fips_database.js updates."""
    print("\n── Processing States ──")
    states = load_json("us_states.json")
    if not states:
        return {}

    result = {}
    for s in states:
        fips = s.get("fips", "")
        centroid = s.get("centroid", [0, 0])
        bbox = s.get("bbox", [0, 0, 0, 0])

        result[fips] = {
            "fips": fips,
            "name": s.get("name", ""),
            "abbreviation": s.get("abbreviation", ""),
            "center": [round(centroid[1], 4), round(centroid[0], 4)] if centroid else None,  # [lat, lon]
            "bounds": {
                "latMin": bbox[1] if bbox else None,
                "latMax": bbox[3] if bbox else None,
                "lonMin": bbox[0] if bbox else None,
                "lonMax": bbox[2] if bbox else None,
            } if bbox else None,
        }

    print(f"  ✓ {len(result)} states processed")
    return result


def process_mpos(state_filter=None):
    """Group MPOs by state, include county members."""
    print("\n── Processing MPOs ──")
    mpos = load_json("us_mpos.json")
    if not mpos:
        return {}

    by_state = defaultdict(list)
    for m in mpos:
        # MPOs can span multiple states — assign to primary state
        sf = m.get("stateFips", "")
        if not sf:
            # Try to infer from state abbreviation
            abbr = m.get("stateAbbr", "")
            sf = next((k for k, v in FIPS_TO_ABBR.items() if v == abbr), "")

        if state_filter and sf != state_filter:
            continue
        if sf not in FIPS_TO_ABBR:
            # Multi-state MPO — check county list
            counties = m.get("counties", [])
            if counties:
                sf = counties[0].get("stateFips", "")
            if sf not in FIPS_TO_ABBR:
                continue

        centroid = m.get("centroid", [0, 0])
        mpo_entry = {
            "name": m.get("name", ""),
            "acronym": m.get("acronym", ""),
            "mpoId": m.get("mpoId", ""),
            "centroid": [round(centroid[1], 4), round(centroid[0], 4)] if centroid else None,
            "bbox": m.get("bbox"),
            "population": m.get("population"),
            "counties": m.get("counties", []),
        }

        # Preserve raw fields for debugging
        raw = m.get("_rawFields", {})
        if raw:
            mpo_entry["_rawFields"] = raw

        by_state[sf].append(mpo_entry)

    for sf in by_state:
        by_state[sf].sort(key=lambda x: x["name"])

    total = sum(len(v) for v in by_state.values())
    print(f"  ✓ {total} MPOs across {len(by_state)} states")
    return dict(by_state)


def process_places(state_filter=None):
    """Group places by state and parent county (using FIPS prefix matching)."""
    print("\n── Processing Places (Cities/Towns) ──")
    places = load_json("us_places.json")
    counties = load_json("us_counties.json")

    if not places:
        return {}

    # Build county bbox lookup for assigning places to counties
    county_bboxes = {}
    for c in counties:
        geoid = c.get("geoid", "")
        if c.get("bbox"):
            county_bboxes[geoid] = {
                "fips": c.get("countyFips", ""),
                "name": c.get("name", ""),
                "bbox": c.get("bbox"),
            }

    by_state = defaultdict(lambda: {"incorporated": [], "cdp": [], "byCounty": defaultdict(list)})

    for p in places:
        sf = p.get("stateFips", "")
        if state_filter and sf != state_filter:
            continue
        if sf not in FIPS_TO_ABBR:
            continue

        centroid = p.get("centroid", [0, 0])
        entry = {
            "placeFips": p.get("placeFips", ""),
            "name": p.get("name", ""),
            "fullName": p.get("fullName", ""),
            "type": p.get("type", "other"),
            "lsad": p.get("lsad", ""),
            "centroid": [round(centroid[1], 4), round(centroid[0], 4)] if centroid else None,
            "funcstat": p.get("funcstat", ""),
        }

        # Categorize
        if p.get("funcstat") == "S":
            by_state[sf]["cdp"].append(entry)
        else:
            by_state[sf]["incorporated"].append(entry)

        # Try to assign to parent county via centroid-in-bbox
        if centroid:
            lon, lat = centroid[0], centroid[1]
            for cgeoid, cdata in county_bboxes.items():
                if not cgeoid.startswith(sf):
                    continue
                bb = cdata["bbox"]
                if bb[0] <= lon <= bb[2] and bb[1] <= lat <= bb[3]:
                    by_state[sf]["byCounty"][cdata["fips"]].append(entry)
                    entry["parentCountyFips"] = cdata["fips"]
                    entry["parentCountyName"] = cdata["name"]
                    break

    # Sort
    for sf in by_state:
        by_state[sf]["incorporated"].sort(key=lambda x: x["name"])
        by_state[sf]["cdp"].sort(key=lambda x: x["name"])
        for cf in by_state[sf]["byCounty"]:
            by_state[sf]["byCounty"][cf].sort(key=lambda x: x["name"])
        # Convert defaultdict to dict for JSON
        by_state[sf]["byCounty"] = dict(by_state[sf]["byCounty"])

    total_inc = sum(len(v["incorporated"]) for v in by_state.values())
    total_cdp = sum(len(v["cdp"]) for v in by_state.values())
    print(f"  ✓ {total_inc} incorporated places + {total_cdp} CDPs across {len(by_state)} states")
    return {k: dict(v) for k, v in by_state.items()}


def generate_hierarchy_updates(counties_by_state, mpos_by_state, state_filter=None):
    """
    Compare downloaded data against existing hierarchy.json files.
    Generate a diff showing what needs to be added.
    """
    print("\n── Generating Hierarchy Update Report ──")
    states_dir = Path(__file__).parent.parent / "states"
    updates = {}

    for sf, abbr in sorted(FIPS_TO_ABBR.items()):
        if state_filter and sf != state_filter:
            continue

        state_name = FIPS_TO_NAME.get(sf, "")
        state_key = state_name.lower().replace(" ", "_")
        hierarchy_path = states_dir / state_key / "hierarchy.json"

        update = {
            "state": state_name,
            "fips": sf,
            "abbreviation": abbr,
            "hierarchyFile": str(hierarchy_path.relative_to(Path(__file__).parent.parent)),
            "exists": hierarchy_path.exists(),
            "missingCounties": [],
            "missingMpos": [],
            "orphanedCounties": [],
            "countyCount": {
                "downloaded": len(counties_by_state.get(sf, [])),
                "inHierarchy": 0,
            },
            "mpoCount": {
                "downloaded": len(mpos_by_state.get(sf, [])),
                "inHierarchy": 0,
            },
        }

        if hierarchy_path.exists():
            with open(hierarchy_path) as f:
                hierarchy = json.load(f)

            # Count counties in hierarchy
            all_counties = hierarchy.get("allCounties", {})
            update["countyCount"]["inHierarchy"] = len(all_counties)

            # Find counties in download but NOT in hierarchy
            downloaded_fips = {c["fips"] for c in counties_by_state.get(sf, [])}
            hierarchy_fips = set(all_counties.keys())
            missing = downloaded_fips - hierarchy_fips
            update["missingCounties"] = [
                {"fips": fips, "name": next((c["name"] for c in counties_by_state.get(sf, []) if c["fips"] == fips), "?")}
                for fips in sorted(missing)
            ]

            # Find counties not assigned to any region
            assigned = set()
            for region in hierarchy.get("regions", {}).values():
                for county_fips in region.get("counties", []):
                    assigned.add(county_fips)
            orphaned = hierarchy_fips - assigned
            update["orphanedCounties"] = [
                {"fips": fips, "name": all_counties.get(fips, "?")}
                for fips in sorted(orphaned)
            ]

            # Count MPOs in hierarchy
            tprs = hierarchy.get("tprs", {})
            mpos_section = hierarchy.get("mpos", {})
            update["mpoCount"]["inHierarchy"] = len(tprs) + len(mpos_section)

        updates[sf] = update

    # Summary
    total_missing = sum(len(u["missingCounties"]) for u in updates.values())
    total_orphaned = sum(len(u["orphanedCounties"]) for u in updates.values())
    print(f"  ✓ {len(updates)} states analyzed")
    print(f"    {total_missing} counties missing from hierarchies")
    print(f"    {total_orphaned} counties orphaned (not in any region)")
    return updates


def main():
    parser = argparse.ArgumentParser(description="Process downloaded geographic data")
    parser.add_argument("--state", type=str, default=None, help="2-digit state FIPS filter")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process each data type
    states_data = process_states()
    counties_data = process_counties(state_filter=args.state)
    mpos_data = process_mpos(state_filter=args.state)
    places_data = process_places(state_filter=args.state)

    # Generate hierarchy comparison
    hierarchy_updates = generate_hierarchy_updates(counties_data, mpos_data, state_filter=args.state)

    # Save all processed files
    def save(data, filename):
        filepath = OUTPUT_DIR / filename
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        size_kb = filepath.stat().st_size / 1024
        print(f"  💾 {filepath.name} ({size_kb:.0f} KB)")

    print("\n── Saving Processed Files ──")
    if states_data:
        save(states_data, "states_metadata.json")
    if counties_data:
        save(counties_data, "counties_by_state.json")
    if mpos_data:
        save(mpos_data, "mpos_by_state.json")
    if places_data:
        save(places_data, "places_by_state.json")
    if hierarchy_updates:
        save(hierarchy_updates, "hierarchy_updates.json")

    print(f"\n✓ All processed files saved to {OUTPUT_DIR}")
    print("\nNext steps:")
    print("  1. Review hierarchy_updates.json for gaps")
    print("  2. Use Claude Code to update hierarchy.json files")
    print("  3. Use Claude Code to regenerate us_counties_db.js and fips_database.js")


if __name__ == "__main__":
    main()
