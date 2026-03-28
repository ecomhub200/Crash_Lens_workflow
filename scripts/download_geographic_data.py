#!/usr/bin/env python3
"""
Download comprehensive US geographic/administrative data from federal APIs.

Data sources:
  1. Census TIGERweb API — States, counties, incorporated places (cities/towns/villages)
  2. BTS/USDOT — Metropolitan Planning Organizations (MPO boundaries + member counties)
  3. Census County Subdivisions — Minor Civil Divisions (towns in New England, townships, etc.)

Outputs (all saved to data/geographic/):
  - us_states.json          — All 50 states + DC with FIPS, centroid, bbox
  - us_counties.json        — All 3,200+ counties with FIPS, centroid, bbox, LSAD
  - us_places.json          — All 30,000+ incorporated places (cities/towns/CDPs) with FIPS, centroid, type
  - us_mpos.json            — All ~400 MPOs with name, member counties, centroid, bbox
  - us_county_subdivisions.json — County subdivisions (MCDs/townships) with FIPS, centroid

Usage:
    python scripts/download_geographic_data.py
    python scripts/download_geographic_data.py --layer counties
    python scripts/download_geographic_data.py --layer mpos
    python scripts/download_geographic_data.py --state 51    # Virginia only

Requires: requests (pip install requests)
"""

import json
import time
import argparse
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

# ─── ArcGIS REST API helpers ────────────────────────────────────────────────

# Census TIGERweb endpoints (ordered by preference)
# The Census Bureau periodically retires vintage services, so we try multiple.
TIGER_SERVICES = [
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2025/MapServer",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2024/MapServer",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer",
]
TIGER_BASE = None  # Resolved dynamically at startup

# Layer name → expected ArcGIS layer names (for dynamic lookup)
# Census renames layers between vintages, so we try many variants
TIGER_LAYER_NAMES = {
    "states": ["States", "Census States", "Current States", "2020 Census States",
               "ACS 2024 States", "ACS 2025 States"],
    "counties": ["Counties", "Census Counties", "Current Counties", "2020 Census Counties",
                 "ACS 2024 Counties", "ACS 2025 Counties"],
    "places": ["Places", "Incorporated Places", "Census Places", "Current Places",
               "2020 Census Places", "ACS 2024 Places", "ACS 2025 Places"],
    "county_subdivisions": ["County Subdivisions", "Census County Subdivisions",
                            "Current County Subdivisions", "2020 Census County Subdivisions",
                            "ACS 2024 County Subdivisions", "ACS 2025 County Subdivisions"],
}

# Fallback layer IDs (last known good — used only if dynamic lookup fails)
TIGER_LAYERS_FALLBACK = {
    "states": [80, 54, 82, 84, 86, 88, 90],
    "counties": [82, 86, 100, 102],
    "places": [28, 30, 150, 152],
    "county_subdivisions": [30, 32, 160, 162],
}

# BTS/USDOT National Transportation Atlas — MPO boundaries
BTS_MPO_URL = "https://geo.dot.gov/server/rest/services/NTAD/Metropolitan_Planning_Organizations/MapServer/0/query"

# Rate limiting
REQUEST_DELAY = 0.25  # seconds between paginated requests
MAX_RETRIES = 4
PAGE_SIZE = 2000  # ArcGIS max varies; 2000 is safe for TIGERweb

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "geographic"
GEOGRAPHY_DIR = Path(__file__).parent.parent / "states" / "geography"

# Canonical filenames that should be mirrored to states/geography/
_CANONICAL_GEO_FILES = {
    "us_states.json",
    "us_counties.json",
    "us_places.json",
    "us_mpos.json",
    "us_county_subdivisions.json",
}


def arcgis_query_all(url, where="1=1", out_fields="*", return_geometry=True,
                     out_sr=4326, page_size=PAGE_SIZE, extra_params=None, label="records"):
    """
    Paginate through an ArcGIS REST API FeatureServer/MapServer query endpoint.
    Returns list of all features (attributes + geometry).
    """
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' package required. Install with: pip install requests")
        sys.exit(1)

    all_features = []
    offset = 0
    supports_pagination = True  # assume yes, disable if server rejects it
    error_retries = 0  # prevent infinite retry loops

    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true" if return_geometry else "false",
            "outSR": out_sr,
            "f": "json",
        }
        # Only add pagination params if server supports them
        if supports_pagination:
            params["resultOffset"] = offset
            params["resultRecordCount"] = page_size
        if extra_params:
            params.update(extra_params)

        data = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                wait = 2 ** (attempt + 1)
                print(f"  ⚠ Request failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"    Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  ✗ Giving up after {MAX_RETRIES} attempts")
                    return all_features

        if data is None:
            break

        # Check for ArcGIS error responses (returned with HTTP 200)
        if "error" in data:
            err = data["error"]
            code = err.get("code", "?")
            msg = err.get("message", "Unknown error")
            details = err.get("details", [])

            # Try automatic recovery strategies (max 3 retries to prevent loops)
            if error_retries < 3 and offset == 0:
                error_retries += 1

                # Strategy 1: Remove pagination params
                if supports_pagination:
                    supports_pagination = False
                    print(f"  ⚠ Query failed with pagination params, retrying without...")
                    continue

                # Strategy 2: Switch to wildcard fields
                if out_fields != "*":
                    print(f"  ⚠ Query failed with specific fields, retrying with outFields=*...")
                    out_fields = "*"
                    supports_pagination = True  # re-enable and try again
                    continue

            print(f"  ✗ ArcGIS API error (code {code}): {msg}")
            if details:
                for d in details[:3]:
                    print(f"    → {d}")
            return all_features

        features = data.get("features", [])
        if not features:
            # Print diagnostic info on first page returning empty
            if offset == 0:
                keys = list(data.keys())
                print(f"  ⚠ API returned 0 features. Response keys: {keys}")
                if len(str(data)) < 500:
                    print(f"    Full response: {json.dumps(data, indent=None)[:400]}")
            break

        all_features.extend(features)
        count = len(all_features)
        print(f"  Fetched {count} {label}...", end="\r")

        # Check if there might be more
        exceeded = data.get("exceededTransferLimit", False)
        if len(features) < page_size and not exceeded:
            break

        offset += len(features)
        time.sleep(REQUEST_DELAY)

    print(f"  Fetched {len(all_features)} {label} total.     ")
    return all_features


def compute_centroid_from_rings(geometry):
    """Compute centroid and bbox from polygon rings."""
    rings = geometry.get("rings", [])
    if not rings:
        return None, None

    all_x, all_y = [], []
    for ring in rings:
        for coord in ring:
            all_x.append(coord[0])
            all_y.append(coord[1])

    if not all_x:
        return None, None

    centroid = [round(sum(all_x) / len(all_x), 6), round(sum(all_y) / len(all_y), 6)]
    bbox = [round(min(all_x), 6), round(min(all_y), 6),
            round(max(all_x), 6), round(max(all_y), 6)]
    return centroid, bbox


def compute_centroid_from_point(geometry):
    """Extract centroid from a point geometry."""
    x = geometry.get("x")
    y = geometry.get("y")
    if x is not None and y is not None:
        return [round(x, 6), round(y, 6)], None
    return None, None


# ─── Field name resolution ───────────────────────────────────────────────────

def _get_field(attr, *candidates, default=""):
    """
    Get a field value from ArcGIS attributes, trying multiple candidate names.
    Census TIGERweb periodically renames fields between vintages (e.g., NAME vs NAME20,
    STUSAB vs STUSPS, FUNCSTAT vs FUNCSTAT20).
    """
    for name in candidates:
        val = attr.get(name)
        if val is not None and val != "":
            return val
    return default


# ─── Dynamic service discovery ───────────────────────────────────────────────

def _discover_tiger_service():
    """
    Try each TIGERweb service URL until one responds with valid layer metadata.
    Returns (base_url, {layer_name: layer_id}) or raises RuntimeError.
    """
    import requests
    global TIGER_BASE

    for service_url in TIGER_SERVICES:
        service_name = service_url.split("/services/")[1].split("/MapServer")[0]
        try:
            resp = requests.get(service_url, params={"f": "json"}, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                print(f"  ⚠ {service_name}: API error — {data['error'].get('message', '?')}")
                continue

            layers = data.get("layers", [])
            if not layers:
                print(f"  ⚠ {service_name}: No layers found")
                continue

            # Build lookup: layer name → layer id
            layer_lookup = {layer["name"]: layer["id"] for layer in layers}

            # Verify we can find our required layers (exact match first, then substring)
            resolved = {}
            for key, candidate_names in TIGER_LAYER_NAMES.items():
                # Try exact match first
                for name in candidate_names:
                    if name in layer_lookup:
                        resolved[key] = layer_lookup[name]
                        break
                # If no exact match, try case-insensitive substring match
                if key not in resolved:
                    # e.g., key="states" should match "Census 2020 States" or "States (Current)"
                    search_term = key.replace("_", " ")  # "county_subdivisions" → "county subdivisions"
                    for layer_name, layer_id in layer_lookup.items():
                        if search_term in layer_name.lower():
                            resolved[key] = layer_id
                            print(f"    (fuzzy match: '{layer_name}' → {key})")
                            break

            if len(resolved) >= 2:  # At least states + counties
                # Verify a query actually works on one of the discovered layers
                test_layer_id = resolved.get("states") or list(resolved.values())[0]
                test_url = f"{service_url}/{test_layer_id}/query"
                try:
                    test_resp = requests.get(test_url, params={
                        "where": "1=1", "outFields": "*",
                        "returnGeometry": "false", "f": "json",
                        "resultRecordCount": 1
                    }, timeout=15)
                    test_data = test_resp.json()
                    if "error" in test_data:
                        # Try without pagination
                        test_resp = requests.get(test_url, params={
                            "where": "1=1", "outFields": "*",
                            "returnGeometry": "false", "f": "json",
                        }, timeout=15)
                        test_data = test_resp.json()
                    if "error" in test_data:
                        print(f"  ⚠ {service_name}: Layers found but queries fail — {test_data['error'].get('message', '?')}")
                        continue
                    if test_data.get("features"):
                        sample_fields = list(test_data["features"][0].get("attributes", {}).keys())
                        print(f"  ✓ Query verified. Sample fields: {sample_fields[:8]}")
                except Exception as e:
                    print(f"  ⚠ {service_name}: Query verification failed — {e}")
                    # Still use this service; the error might be transient
                TIGER_BASE = service_url
                print(f"  ✓ Using {service_name}")
                for key, layer_id in resolved.items():
                    print(f"    {key}: layer {layer_id}")
                return service_url, resolved
            else:
                print(f"  ⚠ {service_name}: Only matched {len(resolved)}/{len(TIGER_LAYER_NAMES)} layers")

        except requests.exceptions.RequestException as e:
            print(f"  ⚠ {service_name}: {e}")
            continue

    # All services failed — use fallback layer IDs with first service URL
    print("  ⚠ Dynamic layer discovery failed, will try fallback layer IDs")
    TIGER_BASE = TIGER_SERVICES[0]
    return TIGER_BASE, None


def _resolve_layer_url(layer_key, discovered_layers):
    """
    Get the query URL for a layer, trying discovered IDs first,
    then fallback IDs until one returns data.
    """
    if discovered_layers and layer_key in discovered_layers:
        return f"{TIGER_BASE}/{discovered_layers[layer_key]}/query"

    # Try fallback layer IDs
    import requests
    for layer_id in TIGER_LAYERS_FALLBACK.get(layer_key, []):
        url = f"{TIGER_BASE}/{layer_id}/query"
        try:
            resp = requests.get(url, params={
                "where": "1=1", "outFields": "GEOID",
                "returnGeometry": "false", "f": "json",
                "resultRecordCount": 1
            }, timeout=15)
            data = resp.json()
            if data.get("features"):
                print(f"  ✓ Found {layer_key} at layer {layer_id}")
                return url
            if "error" in data:
                continue
        except Exception:
            continue

    # Last resort: use first fallback ID
    fallback_id = TIGER_LAYERS_FALLBACK.get(layer_key, [0])[0]
    return f"{TIGER_BASE}/{fallback_id}/query"


# Module-level state for discovered layers
_discovered_layers = None


def _ensure_service():
    """Discover the TIGERweb service once per run."""
    global _discovered_layers, TIGER_BASE
    if TIGER_BASE is not None:
        return
    print("\n═══ Discovering Census TIGERweb Service ═══")
    _, _discovered_layers = _discover_tiger_service()


def _query_with_state_filter(url, state_fips, label):
    """
    Query an ArcGIS layer with optional state FIPS filter.
    Tries multiple field name variants for the state filter since Census
    renames fields between vintages (STATE vs STATEFP vs STATEFP20).
    """
    if not state_fips:
        return arcgis_query_all(url, where="1=1", out_fields="*",
                                return_geometry=True, label=label)

    # Try each state field name variant
    for field_name in ["STATE", "STATEFP", "STATEFP20", "STATEFP10"]:
        where = f"{field_name}='{state_fips}'"
        features = arcgis_query_all(url, where=where, out_fields="*",
                                    return_geometry=True, label=label)
        if features:
            return features

    # Fallback: download all and filter client-side
    print(f"  ⚠ State filter failed, downloading all and filtering locally...")
    return arcgis_query_all(url, where="1=1", out_fields="*",
                            return_geometry=True, label=label)


# ─── Layer download functions ────────────────────────────────────────────────

def download_states():
    """Download all US states + DC from TIGERweb."""
    print("\n═══ Downloading States ═══")
    _ensure_service()
    url = _resolve_layer_url("states", _discovered_layers)
    features = arcgis_query_all(
        url,
        where="1=1",
        out_fields="*",
        return_geometry=True,
        label="states"
    )

    if features:
        sample = features[0].get("attributes", {})
        print(f"  Available fields: {list(sample.keys())}")

    states = []
    for f in features:
        attr = f.get("attributes", {})
        geoid = _get_field(attr, "GEOID", "GEOID20", "GEOID10", "GEO_ID")
        # Skip territories (only 50 states + DC)
        if geoid not in _VALID_STATE_FIPS:
            continue

        centroid, bbox = compute_centroid_from_rings(f.get("geometry", {}))
        states.append({
            "fips": geoid,
            "name": _get_field(attr, "NAME", "NAME20", "NAME10", "STATE_NAME"),
            "abbreviation": _get_field(attr, "STUSAB", "STUSPS", "STUSPS20", "STATE_ABBR"),
            "centroid": centroid,  # [lon, lat]
            "bbox": bbox,         # [west, south, east, north]
            "landAreaSqM": _get_field(attr, "ALAND", "ALAND20", "ALAND10", default=None),
            "waterAreaSqM": _get_field(attr, "AWATER", "AWATER20", "AWATER10", default=None),
        })

    states.sort(key=lambda s: s["fips"])
    print(f"  ✓ {len(states)} states processed")
    return states


def download_counties(state_fips=None):
    """Download all US counties from TIGERweb."""
    print("\n═══ Downloading Counties ═══")
    _ensure_service()
    url = _resolve_layer_url("counties", _discovered_layers)

    features = _query_with_state_filter(url, state_fips, "counties")

    if features:
        sample = features[0].get("attributes", {})
        print(f"  Available fields: {list(sample.keys())}")

    counties = []
    for f in features:
        attr = f.get("attributes", {})
        state = _get_field(attr, "STATE", "STATEFP", "STATEFP20", "STATEFP10")
        if state not in _VALID_STATE_FIPS:
            continue

        centroid, bbox = compute_centroid_from_rings(f.get("geometry", {}))
        counties.append({
            "stateFips": state,
            "countyFips": _get_field(attr, "COUNTY", "COUNTYFP", "COUNTYFP20", "COUNTYFP10"),
            "geoid": _get_field(attr, "GEOID", "GEOID20", "GEOID10"),
            "name": _get_field(attr, "NAME", "NAME20", "NAME10", "BASENAME"),
            "lsad": _get_field(attr, "LSAD", "LSAD20", "LSAD10"),
            "centroid": centroid,
            "bbox": bbox,
            "landAreaSqM": _get_field(attr, "ALAND", "ALAND20", "ALAND10", default=None),
        })

    counties.sort(key=lambda c: c["geoid"])
    print(f"  ✓ {len(counties)} counties processed")
    return counties


def download_places(state_fips=None):
    """Download all incorporated places (cities, towns, villages, CDPs) from TIGERweb."""
    print("\n═══ Downloading Incorporated Places ═══")
    _ensure_service()
    url = _resolve_layer_url("places", _discovered_layers)

    features = _query_with_state_filter(url, state_fips, "places")

    if features:
        sample = features[0].get("attributes", {})
        print(f"  Available fields: {list(sample.keys())}")

    places = []
    for f in features:
        attr = f.get("attributes", {})
        state = _get_field(attr, "STATE", "STATEFP", "STATEFP20", "STATEFP10")
        if state not in _VALID_STATE_FIPS:
            continue

        funcstat = _get_field(attr, "FUNCSTAT", "FUNCSTAT20", "FUNCSTAT10")
        # A = Active, S = Statistical (CDP). Include both.
        # If FUNCSTAT is not available, include the record anyway
        if funcstat and funcstat not in ("A", "S"):
            continue

        centroid, bbox = compute_centroid_from_rings(f.get("geometry", {}))
        lsad = _get_field(attr, "LSAD", "LSAD20", "LSAD10")
        places.append({
            "stateFips": state,
            "placeFips": _get_field(attr, "PLACEFP", "PLACEFP20", "PLACEFP10"),
            "geoid": _get_field(attr, "GEOID", "GEOID20", "GEOID10"),
            "name": _get_field(attr, "NAME", "NAME20", "NAME10", "BASENAME"),
            "fullName": _get_field(attr, "NAMELSAD", "NAMELSAD20", "NAMELSAD10"),
            "lsad": lsad,
            "type": _LSAD_PLACE_TYPE.get(lsad, "other"),
            "funcstat": funcstat or "A",  # default to Active if field not available
            "centroid": centroid,
            "bbox": bbox,
            "landAreaSqM": _get_field(attr, "ALAND", "ALAND20", "ALAND10", default=None),
        })

    places.sort(key=lambda p: p["geoid"])
    print(f"  ✓ {len(places)} places processed")
    return places


def download_county_subdivisions(state_fips=None):
    """Download county subdivisions (MCDs, townships, towns) from TIGERweb."""
    print("\n═══ Downloading County Subdivisions ═══")
    _ensure_service()
    url = _resolve_layer_url("county_subdivisions", _discovered_layers)

    features = _query_with_state_filter(url, state_fips, "county subdivisions")

    if features:
        sample = features[0].get("attributes", {})
        print(f"  Available fields: {list(sample.keys())}")

    subdivisions = []
    for f in features:
        attr = f.get("attributes", {})
        state = _get_field(attr, "STATE", "STATEFP", "STATEFP20", "STATEFP10")
        if state not in _VALID_STATE_FIPS:
            continue

        funcstat = _get_field(attr, "FUNCSTAT", "FUNCSTAT20", "FUNCSTAT10")
        # If FUNCSTAT is not available, include the record anyway
        if funcstat and funcstat not in ("A", "S"):
            continue

        centroid, bbox = compute_centroid_from_rings(f.get("geometry", {}))
        subdivisions.append({
            "stateFips": state,
            "countyFips": _get_field(attr, "COUNTY", "COUNTYFP", "COUNTYFP20", "COUNTYFP10"),
            "cousubFips": _get_field(attr, "COUSUBFP", "COUSUBFP20", "COUSUBFP10"),
            "geoid": _get_field(attr, "GEOID", "GEOID20", "GEOID10"),
            "name": _get_field(attr, "NAME", "NAME20", "NAME10", "BASENAME"),
            "fullName": _get_field(attr, "NAMELSAD", "NAMELSAD20", "NAMELSAD10"),
            "lsad": _get_field(attr, "LSAD", "LSAD20", "LSAD10"),
            "funcstat": funcstat or "A",
            "centroid": centroid,
            "bbox": bbox,
        })

    subdivisions.sort(key=lambda s: s["geoid"])
    print(f"  ✓ {len(subdivisions)} county subdivisions processed")
    return subdivisions


def download_mpos():
    """Download MPO boundaries from BTS/USDOT NTAD service."""
    print("\n═══ Downloading MPOs (BTS/USDOT) ═══")

    # First try the BTS NTAD service
    features = arcgis_query_all(
        BTS_MPO_URL,
        where="1=1",
        out_fields="*",
        return_geometry=True,
        label="MPOs"
    )

    if not features:
        # Fallback: try alternate URLs (BTS/USDOT reorganizes periodically)
        alt_urls = [
            "https://geo.dot.gov/server/rest/services/NTAD/Metropolitan_Planning_Organizations/FeatureServer/0/query",
            "https://geo.dot.gov/server/rest/services/Hosted/Metropolitan_Planning_Organizations/FeatureServer/0/query",
            "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/Metropolitan_Planning_Organizations/FeatureServer/0/query",
            "https://geo.dot.gov/server/rest/services/NTAD/MPO_Boundaries/FeatureServer/0/query",
            "https://geo.dot.gov/server/rest/services/NTAD/MPO_Boundaries/MapServer/0/query",
        ]
        for alt_url in alt_urls:
            print(f"  Trying alternate endpoint: {alt_url.split('/services/')[1].split('/query')[0]}...")
            features = arcgis_query_all(
                alt_url,
                where="1=1",
                out_fields="*",
                return_geometry=True,
                label="MPOs"
            )
            if features:
                break

    mpos = []
    for f in features:
        attr = f.get("attributes", {})
        geometry = f.get("geometry", {})

        centroid, bbox = compute_centroid_from_rings(geometry)

        # Extract all available fields — field names vary across service versions
        mpo = {
            "name": attr.get("MPO_NAME") or attr.get("NAME") or attr.get("MpoName") or "",
            "acronym": attr.get("MPO_ACRONYM") or attr.get("ACRONYM") or attr.get("MpoAcronym") or "",
            "mpoId": attr.get("MPO_ID") or attr.get("OBJECTID") or attr.get("MpoId") or "",
            "stateFips": attr.get("STATE_FIPS") or attr.get("STFIPS") or attr.get("StateFips") or "",
            "stateAbbr": attr.get("STATE") or attr.get("STUSAB") or attr.get("StateAbbr") or "",
            "centroid": centroid,
            "bbox": bbox,
            "population": attr.get("POP") or attr.get("POPULATION") or attr.get("Pop") or None,
            "status": attr.get("STATUS") or attr.get("Status") or "",
        }

        # Capture all raw attributes for inspection
        mpo["_rawFields"] = {k: v for k, v in attr.items() if v is not None and v != ""}

        mpos.append(mpo)

    mpos.sort(key=lambda m: (m.get("stateAbbr", ""), m.get("name", "")))
    print(f"  ✓ {len(mpos)} MPOs processed")
    return mpos


# ─── Spatial join: assign counties to MPOs ───────────────────────────────────

def assign_counties_to_mpos(mpos, counties):
    """
    Use centroid-in-bbox to roughly assign counties to MPOs.
    This is an approximation — proper spatial join would need shapely.
    We check if the county centroid falls within an MPO's bbox.
    """
    print("\n═══ Assigning Counties to MPOs (centroid-in-bbox) ═══")

    for mpo in mpos:
        mpo["counties"] = []
        mpo_bbox = mpo.get("bbox")
        if not mpo_bbox:
            continue

        west, south, east, north = mpo_bbox
        for county in counties:
            c = county.get("centroid")
            if not c:
                continue
            lon, lat = c[0], c[1]
            if west <= lon <= east and south <= lat <= north:
                mpo["counties"].append({
                    "stateFips": county["stateFips"],
                    "countyFips": county["countyFips"],
                    "geoid": county["geoid"],
                    "name": county["name"],
                })

    assigned = sum(1 for m in mpos if m.get("counties"))
    print(f"  ✓ {assigned}/{len(mpos)} MPOs have county assignments")
    return mpos


# ─── Reference data ─────────────────────────────────────────────────────────

# 50 states + DC FIPS codes
_VALID_STATE_FIPS = {
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"
}

# LSAD codes for places → human-readable type
_LSAD_PLACE_TYPE = {
    "25": "city",
    "43": "town",
    "47": "village",
    "53": "CDP",       # Census Designated Place (unincorporated)
    "55": "CDP",
    "57": "CDP",
    "21": "borough",
    "44": "township",
    "46": "plantation",
    "49": "charter_township",
    "00": "city",      # Consolidated city
}


# ─── Main ────────────────────────────────────────────────────────────────────

def save_json(data, filename, metadata=None):
    """Save data to JSON file with metadata header.

    Also mirrors canonical nationwide files (us_counties.json, us_mpos.json, etc.)
    to states/geography/ so that enhance_geographic_data.py and the front-end
    R2 upload pipeline can consume them.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename

    output = {
        "_metadata": {
            "source": "Census TIGERweb + BTS/USDOT NTAD",
            "downloadedAt": datetime.now(timezone.utc).isoformat(),
            "script": "scripts/download_geographic_data.py",
            "recordCount": len(data),
        },
        "data": data,
    }
    if metadata:
        output["_metadata"].update(metadata)

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2)

    size_mb = filepath.stat().st_size / (1024 * 1024)
    print(f"  💾 Saved {filepath} ({size_mb:.1f} MB, {len(data)} records)")

    # Mirror canonical nationwide files to states/geography/
    if filename in _CANONICAL_GEO_FILES:
        GEOGRAPHY_DIR.mkdir(parents=True, exist_ok=True)
        geo_dest = GEOGRAPHY_DIR / filename
        shutil.copy2(filepath, geo_dest)
        print(f"  📋 Mirrored to {geo_dest}")


def main():
    parser = argparse.ArgumentParser(
        description="Download US geographic/administrative data from federal APIs"
    )
    parser.add_argument(
        "--layer", type=str, default="all",
        choices=["all", "states", "counties", "places", "subdivisions", "mpos"],
        help="Which data layer to download (default: all)"
    )
    parser.add_argument(
        "--state", type=str, default=None,
        help="2-digit state FIPS to limit download (e.g., 51 for Virginia)"
    )
    parser.add_argument(
        "--skip-subdivisions", action="store_true",
        help="Skip county subdivisions (largest dataset, ~35,000 records)"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  CRASH LENS — Geographic Data Downloader")
    print(f"  Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if args.state:
        print(f"  Scope: State FIPS {args.state}")
    else:
        print("  Scope: Nationwide (50 states + DC)")
    print("=" * 60)

    layer = args.layer
    counties_data = None

    # ── States ──
    if layer in ("all", "states") and not args.state:
        states = download_states()
        save_json(states, "us_states.json")

    # ── Counties ──
    if layer in ("all", "counties"):
        counties_data = download_counties(state_fips=args.state)
        suffix = f"_{args.state}" if args.state else ""
        save_json(counties_data, f"us_counties{suffix}.json")

    # ── Incorporated Places ──
    if layer in ("all", "places"):
        places = download_places(state_fips=args.state)
        suffix = f"_{args.state}" if args.state else ""
        save_json(places, f"us_places{suffix}.json")

    # ── County Subdivisions ──
    if layer in ("all", "subdivisions"):
        if args.skip_subdivisions and layer == "all":
            print("\n⏭ Skipping county subdivisions (--skip-subdivisions)")
        else:
            subdivisions = download_county_subdivisions(state_fips=args.state)
            suffix = f"_{args.state}" if args.state else ""
            save_json(subdivisions, f"us_county_subdivisions{suffix}.json")

    # ── MPOs ──
    if layer in ("all", "mpos"):
        mpos = download_mpos()
        # If we have counties, do spatial assignment
        if counties_data:
            mpos = assign_counties_to_mpos(mpos, counties_data)
        elif layer == "mpos":
            # Need counties for assignment — download them
            print("\n  (Downloading counties for MPO-county assignment...)")
            counties_data = download_counties(state_fips=args.state)
            mpos = assign_counties_to_mpos(mpos, counties_data)
        save_json(mpos, "us_mpos.json")

    # ── Validate results ──
    print("\n" + "=" * 60)
    total_records = 0
    failures = []
    for json_file in sorted(OUTPUT_DIR.glob("*.json")):
        try:
            with open(json_file) as jf:
                meta = json.load(jf).get("_metadata", {})
                count = meta.get("recordCount", 0)
                total_records += count
                if count == 0:
                    failures.append(json_file.name)
        except Exception:
            failures.append(json_file.name)

    if total_records == 0:
        print("  ✗ FAILED: All downloads returned 0 records!")
        print("    This likely means the Census TIGERweb API has changed.")
        print("    Check: https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb")
        print("=" * 60)
        sys.exit(1)
    elif failures:
        print(f"  ⚠ Partial success: {len(failures)} layer(s) returned 0 records:")
        for f in failures:
            print(f"    - {f}")
        print(f"  Total records across other layers: {total_records}")
    else:
        print("  ✓ Download complete!")

    print(f"  Output directory: {OUTPUT_DIR}")
    print("=" * 60)

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
