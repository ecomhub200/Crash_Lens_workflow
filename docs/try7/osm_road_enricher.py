#!/usr/bin/env python3
"""
osm_road_enricher.py — Universal CrashLens Road Network Enrichment Module
═══════════════════════════════════════════════════════════════════════════

Given only GPS coordinates (x, y), enriches crash records with up to 15+
columns by querying OpenStreetMap road network data and applying geometric
analysis.

COLUMNS ENRICHED (from GPS coordinates alone):
  ┌──────────────────────┬─────────────────────────────────────────────────┐
  │ Column               │ OSM Source                                      │
  ├──────────────────────┼─────────────────────────────────────────────────┤
  │ RTE Name             │ way.name + way.ref (route number)              │
  │ Functional Class     │ way.highway tag → FC mapping                   │
  │ Facility Type        │ highway + lanes + oneway + divided             │
  │ Roadway Description  │ lanes count + divided/undivided                │
  │ Intersection Type    │ Node analysis (ways sharing a node)            │
  │ Traffic Control Type │ Nearby traffic_signals / stop sign nodes       │
  │ SYSTEM               │ highway + ref → DOT system classification      │
  │ Ownership            │ Cascaded from FC + jurisdiction type           │
  │ Mainline?            │ FC + not ramp/service road                     │
  │ Node                 │ Nearest OSM intersection node ID               │
  │ Node Offset (ft)     │ Distance to nearest intersection               │
  │ Roadway Alignment    │ Way geometry curvature analysis                │
  │ Roadway Surface Type │ way.surface tag                                │
  │ Speed limit (posted) │ way.maxspeed tag                               │
  └──────────────────────┴─────────────────────────────────────────────────┘

ADDITIONAL DERIVATIONS (from contributing factor text):
  Hitrun?, Animal Related?, Speed?, Distracted?, Drowsy?, Guardrail Related?

ARCHITECTURE:
  1. Download road network from Overpass API (or load cached JSON)
  2. Build KDTree spatial index on road segment midpoints
  3. For each crash point, find nearest road → extract tags
  4. Identify intersections (nodes shared by 2+ ways)
  5. Map all OSM tags → CrashLens standard values

USAGE:
  # One-time download (run separately, saves cache file):
  python osm_road_enricher.py --download --state DE --output de_roads.json

  # In a state normalizer:
  from osm_road_enricher import OSMRoadEnricher
  enricher = OSMRoadEnricher.from_cache("de_roads.json", state_dot="DelDOT")
  enricher.enrich_dataframe(df)

DEPENDENCIES: requests (for download), scipy (for KDTree), pandas
"""

import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS — CrashLens Standard Values
# ─────────────────────────────────────────────────────────────────────────────

# Functional Class (exact CrashLens values)
FC_INTERSTATE   = "1-Interstate (A,1)"
FC_FREEWAY      = "2-Principal Arterial - Other Freeways and Expressways (B)"
FC_PRINCIPAL    = "3-Principal Arterial - Other (E,2)"
FC_MINOR_ART    = "4-Minor Arterial (H,3)"
FC_MAJ_COLLECT  = "5-Major Collector (I,4)"
FC_MIN_COLLECT  = "6-Minor Collector (5)"
FC_LOCAL        = "7-Local (J,6)"

# Facility Type (exact CrashLens values)
FT_1WAY_UNDIV   = "1-One-Way Undivided"
FT_1WAY_DIV     = "2-One-Way Divided"
FT_2WAY_UNDIV   = "3-Two-Way Undivided"
FT_2WAY_DIV     = "4-Two-Way Divided"

# Ownership (exact CrashLens values)
OWN_STATE       = "1. State Hwy Agency"
OWN_COUNTY      = "2. County Hwy Agency"
OWN_CITY_TOWN   = "3. City or Town Hwy Agency"
OWN_FEDERAL     = "4. Federal Roads"
OWN_TOLL        = "5. Toll Roads Maintained by Others"
OWN_PRIVATE     = "6. Private/Unknown Roads"

# OSM highway tag → Functional Class
HIGHWAY_TO_FC = {
    "motorway":       FC_INTERSTATE,
    "motorway_link":  FC_INTERSTATE,
    "trunk":          FC_FREEWAY,
    "trunk_link":     FC_FREEWAY,
    "primary":        FC_PRINCIPAL,
    "primary_link":   FC_PRINCIPAL,
    "secondary":      FC_MINOR_ART,
    "secondary_link": FC_MINOR_ART,
    "tertiary":       FC_MAJ_COLLECT,
    "tertiary_link":  FC_MAJ_COLLECT,
    "unclassified":   FC_MIN_COLLECT,
    "residential":    FC_LOCAL,
    "living_street":  FC_LOCAL,
    "service":        FC_LOCAL,
}

# OSM surface tag → CrashLens Roadway Surface Type
SURFACE_MAP = {
    "asphalt":   "Bituminous",
    "concrete":  "Portland Cement Concrete",
    "paved":     "Bituminous",
    "unpaved":   "Gravel or Stone",
    "gravel":    "Gravel or Stone",
    "dirt":      "Dirt",
    "sand":      "Dirt",
    "cobblestone": "Brick or Block",
    "brick":     "Brick or Block",
}

# Contributing factor keywords → boolean flag columns
CONTRIBUTING_FACTOR_FLAGS = {
    "Hitrun?":           ["hit and run", "hit-and-run", "left scene", "fled scene",
                          "failure to stop", "fail to stop"],
    "Animal Related?":   ["animal", "deer", "wildlife", "dog", "horse", "cow",
                          "elk", "moose", "bear"],
    "Speed?":            ["speed", "too fast", "exceeded", "racing", "excessive speed"],
    "Distracted?":       ["distract", "cell phone", "inattent", "texting", "mobile",
                          "electronic device", "not paying attention"],
    "Drowsy?":           ["drowsy", "asleep", "fatigue", "fell asleep", "fatigued",
                          "sleepy", "sleep"],
    "Guardrail Related?":["guardrail", "guard rail", "barrier", "median barrier",
                          "cable barrier", "concrete barrier"],
}

# State bounding boxes (for Overpass download)
STATE_BBOXES = {
    "DE": (38.45, -75.79, 39.84, -75.05),
    "VA": (36.54, -83.68, 39.47, -75.24),
    "MD": (37.91, -79.49, 39.72, -75.05),
    "CO": (36.99, -109.06, 41.00, -102.04),
    "FL": (24.40, -87.63, 31.00, -80.03),
    "NY": (40.50, -79.76, 45.02, -71.86),
    "CA": (32.53, -124.48, 42.01, -114.13),
    "TX": (25.84, -106.65, 36.50, -93.51),
    "PA": (39.72, -80.52, 42.27, -74.69),
    "NJ": (38.93, -75.56, 41.36, -73.89),
}

# Earth radius in feet (for distance calculations)
EARTH_RADIUS_FT = 20_902_231


# ─────────────────────────────────────────────────────────────────────────────
#  UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_ft(lat1, lon1, lat2, lon2):
    """Haversine distance in feet between two lat/lon points."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon/2)**2
    return EARTH_RADIUS_FT * 2 * math.asin(math.sqrt(min(a, 1.0)))


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Bearing in degrees from point 1 to point 2."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return math.degrees(math.atan2(x, y)) % 360


def _angle_diff(a1, a2):
    """Absolute angle difference, accounting for wraparound (0-180)."""
    diff = abs(a1 - a2) % 360
    return min(diff, 360 - diff)


# ─────────────────────────────────────────────────────────────────────────────
#  OVERPASS API DOWNLOAD
# ─────────────────────────────────────────────────────────────────────────────

def download_osm_roads(bbox, output_path, timeout=600):
    """
    Download road network from Overpass API and save as cached JSON.

    Args:
        bbox: (south, west, north, east) bounding box
        output_path: Path to save JSON cache file
        timeout: Overpass API timeout in seconds
    """
    import requests

    south, west, north, east = bbox
    query = f"""
    [out:json][timeout:{timeout}];
    (
      way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street|service|motorway_link|trunk_link|primary_link|secondary_link|tertiary_link)$"]
      ({south},{west},{north},{east});
      node(w);
    );
    out body;
    """

    print(f"  Downloading OSM roads for bbox ({south:.2f},{west:.2f})-({north:.2f},{east:.2f})...")
    print(f"  Using Overpass API (this may take 1-5 minutes for large states)...")

    url = "https://overpass-api.de/api/interpreter"
    resp = requests.post(url, data={"data": query}, timeout=timeout + 60)
    resp.raise_for_status()

    raw = resp.json()
    elements = raw.get("elements", [])

    # Separate nodes and ways
    nodes = {}
    ways = []
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = {"lat": el["lat"], "lon": el["lon"],
                               "tags": el.get("tags", {})}
        elif el["type"] == "way":
            ways.append({
                "id": el["id"],
                "nodes": el.get("nodes", []),
                "tags": el.get("tags", {}),
            })

    cache = {
        "bbox": list(bbox),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "node_count": len(nodes),
        "way_count": len(ways),
        "nodes": {str(k): v for k, v in nodes.items()},
        "ways": ways,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(cache, f)

    print(f"  Saved {len(ways):,} roads, {len(nodes):,} nodes → {output_path}")
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  File size: {size_mb:.1f} MB")
    return cache


# ─────────────────────────────────────────────────────────────────────────────
#  ROAD NETWORK INDEX
# ─────────────────────────────────────────────────────────────────────────────

class RoadNetwork:
    """
    Spatial index over an OSM road network for fast nearest-road queries.

    Uses scipy.spatial.KDTree for O(log n) nearest-neighbor lookups.
    Falls back to brute-force search if scipy is unavailable.
    """

    def __init__(self, nodes: dict, ways: list):
        self.nodes = nodes          # {node_id_str: {lat, lon, tags}}
        self.ways = ways            # [{id, nodes, tags}, ...]
        self.way_by_id = {w["id"]: w for w in ways}

        # Build node → ways index (for intersection detection)
        self.node_to_ways: dict[str, list[int]] = defaultdict(list)
        for w in ways:
            for nid in w["nodes"]:
                self.node_to_ways[str(nid)].append(w["id"])

        # Intersection nodes = nodes referenced by 2+ ways
        self.intersection_nodes: set[str] = set()
        for nid, wids in self.node_to_ways.items():
            if len(set(wids)) >= 2:
                self.intersection_nodes.add(nid)

        # Traffic control nodes (signals, stop signs)
        self.signal_nodes: set[str] = set()
        self.stop_nodes: set[str] = set()
        for nid, nd in nodes.items():
            tags = nd.get("tags", {})
            hw = tags.get("highway", "")
            if hw == "traffic_signals":
                self.signal_nodes.add(str(nid))
            elif hw in ("stop", "give_way"):
                self.stop_nodes.add(str(nid))

        # Build way-node coordinate arrays for spatial index
        self._build_spatial_index()

        print(f"    Road network loaded: {len(ways):,} ways, {len(nodes):,} nodes, "
              f"{len(self.intersection_nodes):,} intersections, "
              f"{len(self.signal_nodes):,} signals, {len(self.stop_nodes):,} stops")

    def _build_spatial_index(self):
        """Build KDTree on way midpoints for fast nearest-road lookup."""
        # For each way, compute midpoint and store
        self._way_midpoints = []   # [(lat, lon, way_id), ...]

        # Also build arrays of ALL node positions for intersection lookups
        self._intersection_coords = []  # [(lat, lon, node_id), ...]

        for w in self.ways:
            node_ids = w["nodes"]
            lats, lons = [], []
            for nid in node_ids:
                nd = self.nodes.get(str(nid))
                if nd:
                    lats.append(nd["lat"])
                    lons.append(nd["lon"])
            if lats:
                mid_lat = sum(lats) / len(lats)
                mid_lon = sum(lons) / len(lons)
                self._way_midpoints.append((mid_lat, mid_lon, w["id"]))

                # Also add individual node segments for finer matching
                for i in range(len(lats)):
                    if str(node_ids[i]) in self.intersection_nodes:
                        nd = self.nodes.get(str(node_ids[i]))
                        if nd:
                            self._intersection_coords.append(
                                (nd["lat"], nd["lon"], str(node_ids[i]))
                            )

        # Deduplicate intersection coords
        seen = set()
        unique_int = []
        for lat, lon, nid in self._intersection_coords:
            if nid not in seen:
                seen.add(nid)
                unique_int.append((lat, lon, nid))
        self._intersection_coords = unique_int

        # Build KDTree
        try:
            from scipy.spatial import KDTree
            if self._way_midpoints:
                # Convert to radians-scaled coordinates for better distance
                cos_lat = math.cos(math.radians(
                    sum(p[0] for p in self._way_midpoints) / len(self._way_midpoints)
                ))
                self._way_pts = [(p[0], p[1] * cos_lat) for p in self._way_midpoints]
                self._way_tree = KDTree(self._way_pts)
                self._cos_lat = cos_lat
            else:
                self._way_tree = None
                self._cos_lat = 1.0

            if self._intersection_coords:
                self._int_pts = [(p[0], p[1] * cos_lat) for p in self._intersection_coords]
                self._int_tree = KDTree(self._int_pts)
            else:
                self._int_tree = None

            self._use_kdtree = True
        except ImportError:
            print("    WARNING: scipy not available — using brute-force search (slower)")
            self._use_kdtree = False

    def find_nearest_road(self, lat: float, lon: float, k: int = 5) -> Optional[dict]:
        """
        Find the nearest road to a crash point.

        Returns dict with:
          way_id, distance_ft, highway, name, ref, lanes, oneway, surface,
          maxspeed, divided, tags
        """
        if not self._way_midpoints:
            return None

        if self._use_kdtree and self._way_tree:
            query_pt = (lat, lon * self._cos_lat)
            dists, idxs = self._way_tree.query(query_pt, k=min(k, len(self._way_midpoints)))
            if not hasattr(idxs, '__iter__'):
                idxs = [idxs]

            best = None
            best_dist = float('inf')
            for idx in idxs:
                wid = self._way_midpoints[idx][2]
                way = self.way_by_id[wid]
                # Compute actual distance using way nodes
                d = self._point_to_way_distance(lat, lon, way)
                if d < best_dist:
                    best_dist = d
                    best = way
        else:
            # Brute-force
            best = None
            best_dist = float('inf')
            for mid_lat, mid_lon, wid in self._way_midpoints:
                rough_d = abs(lat - mid_lat) + abs(lon - mid_lon)
                if rough_d < best_dist * 2:
                    way = self.way_by_id[wid]
                    d = self._point_to_way_distance(lat, lon, way)
                    if d < best_dist:
                        best_dist = d
                        best = way

        if best is None:
            return None

        tags = best.get("tags", {})
        return {
            "way_id":      best["id"],
            "distance_ft": best_dist,
            "highway":     tags.get("highway", ""),
            "name":        tags.get("name", ""),
            "ref":         tags.get("ref", ""),
            "lanes":       tags.get("lanes", ""),
            "oneway":      tags.get("oneway", ""),
            "surface":     tags.get("surface", ""),
            "maxspeed":    tags.get("maxspeed", ""),
            "divided":     tags.get("dual_carriageway", tags.get("divided", "")),
            "junction":    tags.get("junction", ""),
            "tags":        tags,
        }

    def find_nearest_intersection(self, lat: float, lon: float) -> Optional[dict]:
        """
        Find nearest intersection node to a crash point.

        Returns dict with: node_id, distance_ft, way_count, signal, stop
        """
        if not self._intersection_coords:
            return None

        if self._use_kdtree and self._int_tree:
            query_pt = (lat, lon * self._cos_lat)
            dist, idx = self._int_tree.query(query_pt)
            nid = self._intersection_coords[idx][2]
        else:
            best_nid = None
            best_d = float('inf')
            for nlat, nlon, nid in self._intersection_coords:
                d = _haversine_ft(lat, lon, nlat, nlon)
                if d < best_d:
                    best_d = d
                    best_nid = nid
            nid = best_nid

        if nid is None:
            return None

        nd = self.nodes.get(str(nid), {})
        dist_ft = _haversine_ft(lat, lon, nd.get("lat", lat), nd.get("lon", lon))
        way_count = len(set(self.node_to_ways.get(str(nid), [])))

        return {
            "node_id":     nid,
            "distance_ft": dist_ft,
            "way_count":   way_count,
            "has_signal":  str(nid) in self.signal_nodes,
            "has_stop":    str(nid) in self.stop_nodes,
        }

    def analyze_curvature(self, way: dict, lat: float, lon: float,
                          window: int = 5) -> str:
        """
        Analyze road geometry near crash point to determine alignment.

        Returns CrashLens Roadway Alignment value.
        """
        node_ids = way.get("nodes", [])
        coords = []
        for nid in node_ids:
            nd = self.nodes.get(str(nid))
            if nd:
                coords.append((nd["lat"], nd["lon"]))

        if len(coords) < 3:
            return ""

        # Find the segment closest to crash point
        best_idx = 0
        best_d = float('inf')
        for i, (clat, clon) in enumerate(coords):
            d = abs(lat - clat) + abs(lon - clon)
            if d < best_d:
                best_d = d
                best_idx = i

        # Compute bearing changes in a window around the crash point
        start = max(0, best_idx - window)
        end = min(len(coords) - 1, best_idx + window)

        if end - start < 2:
            return "2. Straight Level"  # Not enough geometry

        bearings = []
        for i in range(start, end):
            b = _bearing_deg(coords[i][0], coords[i][1],
                            coords[i+1][0], coords[i+1][1])
            bearings.append(b)

        # Maximum bearing change indicates curvature
        max_change = 0
        for i in range(len(bearings) - 1):
            change = _angle_diff(bearings[i], bearings[i+1])
            max_change = max(max_change, change)

        # Classify: >15° = curve, else straight
        # (We can't determine grade without elevation data — default to "Level")
        if max_change > 25:
            return "4. Curve Grade"     # Strong curve
        elif max_change > 15:
            return "5. Curve Level"     # Moderate curve
        else:
            return "2. Straight Level"  # Straight

    def _point_to_way_distance(self, lat, lon, way):
        """Minimum distance from point to any segment of a way, in feet."""
        node_ids = way.get("nodes", [])
        min_d = float('inf')
        prev = None
        for nid in node_ids:
            nd = self.nodes.get(str(nid))
            if nd is None:
                continue
            nlat, nlon = nd["lat"], nd["lon"]
            d = _haversine_ft(lat, lon, nlat, nlon)
            if d < min_d:
                min_d = d
            prev = (nlat, nlon)
        return min_d


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN ENRICHER CLASS
# ─────────────────────────────────────────────────────────────────────────────

class OSMRoadEnricher:
    """
    Universal CrashLens enrichment using OpenStreetMap road network data.

    Fills up to 15+ missing columns from GPS coordinates alone.
    Works for ANY state — just provide the cached road network JSON.
    """

    def __init__(self, network: RoadNetwork, state_dot: str = "",
                 state_abbr: str = "", max_road_dist_ft: float = 500,
                 max_intersection_dist_ft: float = 300):
        self.network = network
        self.state_dot = state_dot or "DOT"
        self.state_abbr = state_abbr or ""
        self.max_road_dist = max_road_dist_ft
        self.max_int_dist = max_intersection_dist_ft

    @classmethod
    def from_cache(cls, cache_path: str, state_dot: str = "",
                   state_abbr: str = "", **kwargs) -> "OSMRoadEnricher":
        """Load enricher from a pre-downloaded JSON cache file."""
        print(f"  Loading road network from {cache_path}...")
        with open(cache_path) as f:
            data = json.load(f)
        network = RoadNetwork(data["nodes"], data["ways"])
        return cls(network, state_dot=state_dot, state_abbr=state_abbr, **kwargs)

    @classmethod
    def from_overpass(cls, state_abbr: str, cache_dir: str = ".",
                      state_dot: str = "", **kwargs) -> "OSMRoadEnricher":
        """Download road network from Overpass API and build enricher."""
        bbox = STATE_BBOXES.get(state_abbr.upper())
        if not bbox:
            raise ValueError(f"No bounding box for state {state_abbr}. "
                             "Add it to STATE_BBOXES or provide a custom bbox.")
        cache_path = os.path.join(cache_dir, f"{state_abbr.lower()}_roads.json")

        if os.path.exists(cache_path):
            print(f"  Using cached road network: {cache_path}")
        else:
            download_osm_roads(bbox, cache_path)

        return cls.from_cache(cache_path, state_dot=state_dot,
                              state_abbr=state_abbr, **kwargs)

    # ── Core enrichment ──────────────────────────────────────────────────────

    def enrich_row(self, lat: float, lon: float,
                   existing: dict = None) -> dict:
        """
        Enrich a single crash record using GPS coordinates.

        Args:
            lat, lon: Crash GPS coordinates
            existing: Dict of already-populated columns (to avoid overwriting)

        Returns:
            Dict of enriched column values
        """
        result = {}
        existing = existing or {}

        # Skip if coordinates are missing/zero
        if not lat or not lon or (lat == 0 and lon == 0):
            return result

        # ── Find nearest road ────────────────────────────────────────────────
        road = self.network.find_nearest_road(lat, lon)
        if not road or road["distance_ft"] > self.max_road_dist:
            return result

        hw = road["highway"]
        name = road["name"]
        ref = road["ref"]
        lanes_str = road["lanes"]
        oneway = road["oneway"]
        divided = road["divided"]

        # ── RTE Name ─────────────────────────────────────────────────────────
        if not existing.get("RTE Name"):
            rte_parts = []
            if ref:
                rte_parts.append(ref)
            if name and name != ref:
                rte_parts.append(name)
            if rte_parts:
                result["RTE Name"] = " / ".join(rte_parts)

        # ── Functional Class ─────────────────────────────────────────────────
        if not existing.get("Functional Class"):
            fc = HIGHWAY_TO_FC.get(hw, "")
            if fc:
                result["Functional Class"] = fc

        # ── Facility Type ────────────────────────────────────────────────────
        if not existing.get("Facility Type"):
            result["Facility Type"] = self._derive_facility_type(
                hw, lanes_str, oneway, divided
            )

        # ── Roadway Description ──────────────────────────────────────────────
        if not existing.get("Roadway Description"):
            result["Roadway Description"] = self._derive_roadway_desc(
                lanes_str, oneway, divided
            )

        # ── Roadway Surface Type ─────────────────────────────────────────────
        if not existing.get("Roadway Surface Type"):
            surface = road.get("surface", "")
            if surface:
                result["Roadway Surface Type"] = SURFACE_MAP.get(
                    surface.lower(), "Bituminous"
                )

        # ── SYSTEM ───────────────────────────────────────────────────────────
        if not existing.get("SYSTEM"):
            result["SYSTEM"] = self._derive_system(hw, ref)

        # ── Ownership (improved with FC) ─────────────────────────────────────
        if not existing.get("Ownership"):
            fc_val = result.get("Functional Class") or existing.get("Functional Class", "")
            juris_type = existing.get("_juris_type", "county")
            result["Ownership"] = self._derive_ownership(fc_val, hw, ref, juris_type)

        # ── Mainline? ────────────────────────────────────────────────────────
        if not existing.get("Mainline?"):
            is_link = "_link" in hw
            is_service = hw == "service"
            fc_val = result.get("Functional Class") or existing.get("Functional Class", "")
            if fc_val and not is_link and not is_service:
                fc_num = fc_val[0] if fc_val else "9"
                result["Mainline?"] = "Yes" if fc_num in ("1", "2", "3", "4") else "No"

        # ── Roadway Alignment (curvature analysis) ───────────────────────────
        if not existing.get("Roadway Alignment"):
            way_obj = self.network.way_by_id.get(road["way_id"])
            if way_obj:
                alignment = self.network.analyze_curvature(way_obj, lat, lon)
                if alignment:
                    result["Roadway Alignment"] = alignment

        # ── Intersection analysis ────────────────────────────────────────────
        intersection = self.network.find_nearest_intersection(lat, lon)
        if intersection and intersection["distance_ft"] <= self.max_int_dist:
            # Node + Node Offset
            if not existing.get("Node"):
                result["Node"] = str(intersection["node_id"])
            if not existing.get("Node Offset (ft)"):
                result["Node Offset (ft)"] = str(int(intersection["distance_ft"]))

            # Intersection Type
            if not existing.get("Intersection Type"):
                wc = intersection["way_count"]
                if wc >= 5:
                    result["Intersection Type"] = "5. Five-Point or More"
                elif wc == 4:
                    result["Intersection Type"] = "2. Four-Way Intersection"
                elif wc == 3:
                    result["Intersection Type"] = "3. T-Intersection"
                elif wc == 2:
                    result["Intersection Type"] = "1. Not at Intersection"

            # Traffic Control Type
            if not existing.get("Traffic Control Type"):
                if intersection["has_signal"]:
                    result["Traffic Control Type"] = "1. Traffic Signal"
                    result["Traffic Control Status"] = "1. Working"
                elif intersection["has_stop"]:
                    result["Traffic Control Type"] = "2. Stop Sign"
                    result["Traffic Control Status"] = "1. Working"

            # Intersection Analysis (near intersection if within 150 ft)
            if not existing.get("Intersection Analysis"):
                if intersection["distance_ft"] <= 150:
                    result["Intersection Analysis"] = "At Intersection"
                else:
                    result["Intersection Analysis"] = "Non-Intersection"
        else:
            if not existing.get("Intersection Analysis"):
                result["Intersection Analysis"] = "Non-Intersection"

        return result

    def enrich_from_contributing_factor(self, text: str, existing: dict = None) -> dict:
        """
        Derive boolean flag columns from contributing factor text.

        Universal across all states — works with ANY contributing factor field.
        """
        result = {}
        existing = existing or {}
        if not text:
            return result

        text_lower = text.lower()

        for column, keywords in CONTRIBUTING_FACTOR_FLAGS.items():
            if not existing.get(column):
                if any(kw in text_lower for kw in keywords):
                    result[column] = "Yes"

        return result

    def enrich_dataframe(self, df, lat_col: str = "y", lon_col: str = "x",
                          contrib_col: str = "",
                          progress_interval: int = 5000) -> None:
        """
        Enrich an entire pandas DataFrame in-place.

        Args:
            df: pandas DataFrame with crash data (already has golden columns)
            lat_col: Column name for latitude
            lon_col: Column name for longitude
            contrib_col: Column name for contributing factor (optional)
            progress_interval: Print progress every N rows
        """
        import pandas as pd

        total = len(df)
        enriched_count = 0
        road_found = 0
        int_found = 0

        print(f"  Enriching {total:,} crash records from OSM road network...")

        for idx in range(total):
            if idx > 0 and idx % progress_interval == 0:
                print(f"    {idx:,}/{total:,} rows ({idx/total*100:.0f}%) "
                      f"— roads matched: {road_found:,}, intersections: {int_found:,}")

            lat = _safe_float(df.iloc[idx].get(lat_col))
            lon = _safe_float(df.iloc[idx].get(lon_col))

            if not lat or not lon:
                continue

            # Build existing dict from current row
            existing = {}
            for col in ["RTE Name", "Functional Class", "Facility Type",
                        "Roadway Description", "SYSTEM", "Ownership",
                        "Mainline?", "Node", "Node Offset (ft)",
                        "Intersection Type", "Traffic Control Type",
                        "Roadway Alignment", "Roadway Surface Type",
                        "Traffic Control Status", "Intersection Analysis"]:
                val = str(df.iloc[idx].get(col, "") or "").strip()
                if val:
                    existing[col] = val

            # OSM road enrichment
            enrichments = self.enrich_row(lat, lon, existing)
            if enrichments:
                enriched_count += 1
                if "RTE Name" in enrichments:
                    road_found += 1
                if "Node" in enrichments:
                    int_found += 1

                for col, val in enrichments.items():
                    df.at[df.index[idx], col] = val

            # Contributing factor enrichment
            if contrib_col and contrib_col in df.columns:
                contrib_text = str(df.iloc[idx].get(contrib_col, "") or "")
                flag_enrichments = self.enrich_from_contributing_factor(
                    contrib_text, existing
                )
                for col, val in flag_enrichments.items():
                    if not str(df.iloc[idx].get(col, "") or "").strip():
                        df.at[df.index[idx], col] = val

        print(f"  Enrichment complete: {enriched_count:,}/{total:,} rows enriched")
        print(f"    Roads matched: {road_found:,}")
        print(f"    Intersections found: {int_found:,}")

    # ── Derivation helpers ───────────────────────────────────────────────────

    def _derive_facility_type(self, highway, lanes_str, oneway, divided) -> str:
        """Derive CrashLens Facility Type from OSM tags."""
        is_oneway = oneway in ("yes", "1", "-1")
        is_divided = divided in ("yes", "1") or highway in ("motorway", "trunk")

        lanes = 0
        try:
            lanes = int(lanes_str) if lanes_str else 0
        except ValueError:
            pass

        if is_oneway:
            return FT_1WAY_UNDIV
        elif is_divided or lanes >= 4:
            return FT_2WAY_DIV
        else:
            return FT_2WAY_UNDIV

    def _derive_roadway_desc(self, lanes_str, oneway, divided) -> str:
        """Derive Roadway Description from OSM tags."""
        lanes = 0
        try:
            lanes = int(lanes_str) if lanes_str else 0
        except ValueError:
            pass

        is_oneway = oneway in ("yes", "1", "-1")
        is_divided = divided in ("yes", "1")

        if is_oneway:
            if lanes >= 2:
                return f"{lanes} Lanes, One-Way"
            return "One-Way"
        elif is_divided:
            if lanes >= 4:
                return f"{lanes} Lanes, Divided"
            return "Divided"
        else:
            if lanes >= 4:
                return f"{lanes} Lanes, Undivided"
            elif lanes == 2 or lanes == 0:
                return "Two Lanes, Undivided"
            return f"{lanes} Lanes, Undivided"

    def _derive_system(self, highway, ref) -> str:
        """Derive SYSTEM column from OSM highway type and route reference."""
        dot = self.state_dot if self.state_dot else "DOT"

        if highway in ("motorway", "motorway_link"):
            return f"{dot} Interstate"
        elif highway in ("trunk", "trunk_link", "primary", "primary_link"):
            # Check ref for US/State route designation
            if ref:
                ref_upper = ref.upper()
                if re.match(r"^(I[-\s]|IS\s)", ref_upper):
                    return f"{dot} Interstate"
                if re.match(r"^(US[-\s]|US\s)", ref_upper):
                    return f"{dot} Primary"
            return f"{dot} Primary"
        elif highway in ("secondary", "secondary_link", "tertiary", "tertiary_link"):
            return f"{dot} Secondary"
        else:
            return f"Non{dot} secondary"

    def _derive_ownership(self, fc, highway, ref, juris_type="county") -> str:
        """Derive Ownership from Functional Class + jurisdiction type."""
        # Interstate/Freeway always state
        if highway in ("motorway", "motorway_link", "trunk", "trunk_link"):
            return OWN_STATE

        # Check ref for state/federal route
        if ref:
            ref_upper = ref.upper()
            if re.match(r"^(I[-\s]|US[-\s]|SR[-\s]|DE[-\s]|" + self.state_abbr + r"[-\s])", ref_upper):
                return OWN_STATE
            if re.match(r"^(CR[-\s]|CO[-\s])", ref_upper):
                return OWN_COUNTY

        # FC-based
        if fc:
            fc_num = fc[0] if fc else "9"
            if fc_num in ("1", "2", "3"):
                return OWN_STATE
            elif fc_num == "4":
                return OWN_STATE if juris_type == "county" else OWN_CITY_TOWN
            elif fc_num == "5":
                return OWN_STATE if juris_type == "county" else OWN_CITY_TOWN
            elif fc_num in ("6", "7"):
                if juris_type == "county":
                    return OWN_COUNTY
                elif juris_type in ("city", "town"):
                    return OWN_CITY_TOWN

        # Default
        return OWN_STATE


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f != 0 else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  CLI — Download road network or test enrichment
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="CrashLens Universal OSM Road Enricher"
    )
    sub = parser.add_subparsers(dest="command")

    # Download command
    dl = sub.add_parser("download", help="Download OSM road network for a state")
    dl.add_argument("--state", "-s", required=True, help="State abbreviation (e.g. DE)")
    dl.add_argument("--output", "-o", default=None, help="Output JSON path")

    # Enrich command
    en = sub.add_parser("enrich", help="Enrich a crash CSV using cached road data")
    en.add_argument("--input", "-i", required=True, help="Input CSV")
    en.add_argument("--roads", "-r", required=True, help="Road network JSON cache")
    en.add_argument("--output", "-o", default=None, help="Output CSV")
    en.add_argument("--dot", default="DOT", help="State DOT name (e.g. DelDOT)")
    en.add_argument("--abbr", default="", help="State abbreviation")
    en.add_argument("--contrib-col", default="", help="Contributing factor column name")

    # Stats command
    st = sub.add_parser("stats", help="Show road network statistics")
    st.add_argument("--roads", "-r", required=True, help="Road network JSON cache")

    args = parser.parse_args()

    if args.command == "download":
        abbr = args.state.upper()
        output = args.output or f"{abbr.lower()}_roads.json"
        bbox = STATE_BBOXES.get(abbr)
        if not bbox:
            print(f"Error: No bounding box for {abbr}. Known states: {list(STATE_BBOXES.keys())}")
            sys.exit(1)
        download_osm_roads(bbox, output)

    elif args.command == "enrich":
        import pandas as pd
        enricher = OSMRoadEnricher.from_cache(
            args.roads, state_dot=args.dot, state_abbr=args.abbr
        )
        df = pd.read_csv(args.input, dtype=str, low_memory=False)
        enricher.enrich_dataframe(df, contrib_col=args.contrib_col)
        output = args.output or args.input.replace(".csv", "_enriched.csv")
        df.to_csv(output, index=False)
        print(f"\nSaved enriched data → {output}")

    elif args.command == "stats":
        with open(args.roads) as f:
            data = json.load(f)
        print(f"Roads: {data['way_count']:,}")
        print(f"Nodes: {data['node_count']:,}")
        print(f"BBox: {data['bbox']}")
        print(f"Downloaded: {data['downloaded_at']}")

    else:
        parser.print_help()
