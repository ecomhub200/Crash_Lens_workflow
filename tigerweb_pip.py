#!/usr/bin/env python3
"""
tigerweb_pip.py — Universal GPS Jurisdiction Validator (v2.6.2)
CrashLens Shared Module — works for ANY U.S. state.

Uses Census TIGERweb county boundaries for true point-in-polygon (PIP)
to validate and reassign crash GPS coordinates to the correct county.

Three-tier fallback strategy:
  Tier 1: Local shapely PIP on cached GeoJSON boundaries (fastest, offline)
  Tier 2: TIGERweb REST API per-point queries (accurate, needs internet)
  Tier 3: Centroid nearest-neighbor from us_counties.json (always works)

Smart batching:
  - Pre-filter: centroid flags candidates where stated ≠ nearest county
  - Grid dedupe: ~0.01° cells → 1 TIGERweb query per cell (not per crash)
  - Cache: grid cell → county result, reused for all crashes in that cell

Usage:
    from tigerweb_pip import TIGERwebValidator

    validator = TIGERwebValidator(
        state_fips="10",
        state_abbreviation="de",
        county_dict=DE_COUNTIES,         # {name: {fips, district, mpo, area_type, ...}}
        cache_dir="cache",
        counties_json_path="us_counties.json"  # for centroid fallback
    )
    df, stats = validator.validate_jurisdiction(df)

Dependencies:
  Required: pandas, requests (for Tier 2 API calls)
  Optional: shapely, geopandas (for Tier 1 local PIP — much faster)
"""

import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    pd = None

# ─────────────────────────────────────────────────────────────────────────────
#  TIGERweb API Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Counties layer — tigerWMS_Current layer 82 (same as CrashLens BoundaryService)
# Point-in-polygon queries: pass geometry={lon},{lat} → returns county containing that point
TIGERWEB_COUNTY_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/tigerWMS_Current/MapServer/82/query"
)

# Same layer for downloading full state county boundaries as GeoJSON
# (one request: where=STATE='XX'&returnGeometry=true → all county polygons)
TIGERWEB_COUNTY_GEOJSON_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/tigerWMS_Current/MapServer/82/query"
)

# Grid cell size for deduplication (~1.1 km at mid-latitudes)
GRID_CELL_DEG = 0.01

# Rate limit: max TIGERweb API calls per second
API_RATE_LIMIT = 10


# ─────────────────────────────────────────────────────────────────────────────
#  Haversine Distance (Tier 3 fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _grid_key(lat: float, lon: float) -> str:
    """Convert lat/lon to a grid cell key for deduplication."""
    return f"{round(lat / GRID_CELL_DEG)},{round(lon / GRID_CELL_DEG)}"


# ─────────────────────────────────────────────────────────────────────────────
#  TIGERweb Validator Class
# ─────────────────────────────────────────────────────────────────────────────

class TIGERwebValidator:
    """
    Universal GPS jurisdiction validator for U.S. crash data.

    Validates crash GPS coordinates against county boundaries and
    reassigns Physical Juris Name when GPS disagrees with the stated county.
    Cascades: FIPS, DOT District, Planning District, MPO Name, Area Type.
    """

    def __init__(
        self,
        state_fips: str,
        state_abbreviation: str,
        county_dict: Dict[str, Dict],
        cache_dir: str = "cache",
        counties_json_path: str = "",
    ):
        """
        Args:
            state_fips:          2-digit state FIPS (e.g. "10" for Delaware)
            state_abbreviation:  lowercase 2-letter code (e.g. "de")
            county_dict:         {county_name: {fips, district, mpo, area_type, ...}}
            cache_dir:           directory for cached boundary files
            counties_json_path:  path to us_counties.json for centroid fallback
        """
        self.state_fips = state_fips.zfill(2)
        self.state_abbreviation = state_abbreviation.lower()
        self.county_dict = county_dict
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Build reverse FIPS→county name lookup
        self.fips_to_county: Dict[str, str] = {}
        for name, geo in county_dict.items():
            fips = geo.get("fips", "").zfill(3)
            self.fips_to_county[fips] = name

        # Load centroids from us_counties.json (Tier 3 fallback)
        self.centroids: Dict[str, Tuple[float, float]] = {}  # {county_name: (lat, lon)}
        self._load_centroids(counties_json_path)

        # Load or download county boundaries (Tier 1)
        self.shapely_available = False
        self.county_polygons = None  # GeoDataFrame if available
        self._try_load_boundaries()

        # Grid cache for Tier 2 API results
        self._grid_cache: Dict[str, Optional[Dict]] = {}

        # Stats
        self.tier_used = {"shapely_pip": 0, "tigerweb_api": 0, "centroid": 0}

    # ─── Initialization helpers ───

    def _load_centroids(self, counties_json_path: str):
        """Load county centroids from us_counties.json for Tier 3 fallback."""
        # First try county_dict (if centroids embedded)
        for name, geo in self.county_dict.items():
            if "centlat" in geo and "centlon" in geo:
                self.centroids[name] = (geo["centlat"], geo["centlon"])

        if self.centroids:
            return  # Already have centroids from county_dict

        # Fall back to us_counties.json
        if not counties_json_path or not os.path.exists(counties_json_path):
            # Try common paths
            for candidate in [
                "us_counties.json",
                "states/geography/us_counties.json",
                os.path.join(os.path.dirname(__file__), "us_counties.json"),
            ]:
                if os.path.exists(candidate):
                    counties_json_path = candidate
                    break

        if not counties_json_path or not os.path.exists(counties_json_path):
            print("    ⚠️  us_counties.json not found — Tier 3 centroid fallback limited")
            return

        try:
            with open(counties_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            records = data.get("records", data) if isinstance(data, dict) else data
            for c in records:
                if c.get("STATE") != self.state_fips:
                    continue
                county_fips = (c.get("COUNTY") or "").zfill(3)
                county_name = self.fips_to_county.get(county_fips)
                if county_name:
                    lat = float(c.get("CENTLAT") or c.get("INTPTLAT") or 0)
                    lon = float(c.get("CENTLON") or c.get("INTPTLON") or 0)
                    if lat != 0 and lon != 0:
                        self.centroids[county_name] = (lat, lon)
        except Exception as e:
            print(f"    ⚠️  Failed to load centroids: {e}")

    def _try_load_boundaries(self):
        """Try to load cached county boundaries for Tier 1 shapely PIP."""
        boundary_file = self.cache_dir / f"{self.state_abbreviation}_county_boundaries.geojson"

        try:
            import shapely.geometry
            import geopandas as gpd
            self.shapely_available = True
        except ImportError:
            self.shapely_available = False
            return

        if boundary_file.exists():
            try:
                import geopandas as gpd
                self.county_polygons = gpd.read_file(str(boundary_file))
                print(f"    ✅ Loaded cached county boundaries ({len(self.county_polygons)} polygons)")
                return
            except Exception as e:
                print(f"    ⚠️  Failed to load cached boundaries: {e}")

        # Try to download from TIGERweb
        self._download_boundaries(boundary_file)

    def _download_boundaries(self, output_path: Path):
        """Download county boundaries from TIGERweb as GeoJSON."""
        try:
            import requests
            import geopandas as gpd

            print(f"    📥 Downloading county boundaries for state FIPS {self.state_fips}...")
            params = {
                "where": f"STATE = '{self.state_fips}'",
                "outFields": "GEOID,STATE,COUNTY,NAME,BASENAME,NAMELSAD",
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
            }
            resp = requests.get(TIGERWEB_COUNTY_GEOJSON_URL, params=params, timeout=60)
            resp.raise_for_status()
            geojson_data = resp.json()

            if "features" not in geojson_data or len(geojson_data["features"]) == 0:
                print(f"    ⚠️  No county boundaries returned for state FIPS {self.state_fips}")
                return

            # Save to cache
            with open(str(output_path), "w", encoding="utf-8") as f:
                json.dump(geojson_data, f)

            self.county_polygons = gpd.read_file(str(output_path))
            print(f"    ✅ Downloaded & cached {len(self.county_polygons)} county boundaries")

        except ImportError:
            print("    ⚠️  requests/geopandas not available — skipping boundary download")
        except Exception as e:
            print(f"    ⚠️  Failed to download boundaries: {e}")

    # ─── Core PIP Methods ───

    def _pip_shapely(self, lat: float, lon: float) -> Optional[Dict]:
        """Tier 1: Point-in-polygon using cached shapely boundaries."""
        if not self.shapely_available or self.county_polygons is None:
            return None

        try:
            from shapely.geometry import Point
            pt = Point(lon, lat)  # shapely uses (x, y) = (lon, lat)

            for _, row in self.county_polygons.iterrows():
                if row.geometry and row.geometry.contains(pt):
                    county_fips = str(row.get("COUNTY", "")).zfill(3)
                    county_name = self.fips_to_county.get(county_fips, "")
                    if not county_name:
                        # Try matching by BASENAME
                        basename = str(row.get("BASENAME", "")).strip()
                        for name in self.county_dict:
                            if name.lower() == basename.lower():
                                county_name = name
                                break
                    self.tier_used["shapely_pip"] += 1
                    return {
                        "county_fips": county_fips,
                        "county_name": county_name,
                        "geoid": str(row.get("GEOID", "")),
                        "source": "shapely_pip",
                    }
            return None  # Point not inside any county polygon
        except Exception:
            return None

    def _pip_tigerweb_api(self, lat: float, lon: float) -> Optional[Dict]:
        """Tier 2: Point-in-polygon via TIGERweb REST API."""
        try:
            import requests
        except ImportError:
            return None

        try:
            params = {
                "geometry": f"{lon},{lat}",
                "geometryType": "esriGeometryPoint",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "GEOID,STATE,COUNTY,NAME,BASENAME",
                "returnGeometry": "false",
                "f": "json",
            }
            resp = requests.get(TIGERWEB_COUNTY_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                return None

            attrs = features[0].get("attributes", {})
            county_fips = str(attrs.get("COUNTY", "")).zfill(3)
            county_name = self.fips_to_county.get(county_fips, "")
            if not county_name:
                basename = str(attrs.get("BASENAME", "")).strip()
                for name in self.county_dict:
                    if name.lower() == basename.lower():
                        county_name = name
                        break

            self.tier_used["tigerweb_api"] += 1
            return {
                "county_fips": county_fips,
                "county_name": county_name,
                "geoid": str(attrs.get("GEOID", "")),
                "source": "tigerweb_api",
            }
        except Exception:
            return None

    def _pip_centroid(self, lat: float, lon: float) -> Optional[Dict]:
        """Tier 3: Nearest county centroid (fallback — always works)."""
        if not self.centroids:
            return None

        best_county = None
        best_dist = float("inf")
        for county_name, (clat, clon) in self.centroids.items():
            dist = _haversine_km(lat, lon, clat, clon)
            if dist < best_dist:
                best_dist = dist
                best_county = county_name

        if best_county:
            geo = self.county_dict.get(best_county, {})
            self.tier_used["centroid"] += 1
            return {
                "county_fips": geo.get("fips", "").zfill(3),
                "county_name": best_county,
                "geoid": geo.get("geoid", ""),
                "source": "centroid",
                "distance_km": round(best_dist, 2),
            }
        return None

    def resolve_point(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Resolve a single (lat, lon) to a county using 3-tier fallback.
        Uses grid cache to avoid redundant lookups.
        """
        gk = _grid_key(lat, lon)
        if gk in self._grid_cache:
            return self._grid_cache[gk]

        # Tier 1: shapely PIP
        result = self._pip_shapely(lat, lon)
        if result:
            self._grid_cache[gk] = result
            return result

        # Tier 2: TIGERweb API
        result = self._pip_tigerweb_api(lat, lon)
        if result:
            self._grid_cache[gk] = result
            return result

        # Tier 3: centroid
        result = self._pip_centroid(lat, lon)
        if result:
            self._grid_cache[gk] = result
            return result

        self._grid_cache[gk] = None
        return None

    # ─── DataFrame-level validation ───

    def validate_jurisdiction(
        self,
        df: "pd.DataFrame",
        x_col: str = "x",
        y_col: str = "y",
        juris_col: str = "Physical Juris Name",
    ) -> Tuple["pd.DataFrame", Dict]:
        """
        Validate GPS jurisdiction for every crash row.

        For each row with valid GPS:
          1. Resolve the true county via PIP (3-tier)
          2. If true county ≠ stated jurisdiction → reassign
          3. Cascade: FIPS, DOT District, Planning District, MPO Name, Area Type

        Returns:
            df:    DataFrame with corrected jurisdictions
            stats: {reassignment_pair: count, ...}
        """
        if pd is None:
            print("    ⚠️  pandas not available — cannot validate")
            return df, {}

        stats = {}
        total_checked = 0
        total_reassigned = 0
        sample_log = []

        # Compute state bounding box from centroids (with 2% buffer)
        if self.centroids:
            lats = [c[0] for c in self.centroids.values()]
            lons = [c[1] for c in self.centroids.values()]
            lat_span = max(lats) - min(lats) or 1.0
            lon_span = max(lons) - min(lons) or 1.0
            lat_min = min(lats) - 0.02 * lat_span
            lat_max = max(lats) + 0.02 * lat_span
            lon_min = min(lons) - 0.02 * lon_span
            lon_max = max(lons) + 0.02 * lon_span
        else:
            # Generous CONUS bounds
            lat_min, lat_max = 24.0, 50.0
            lon_min, lon_max = -125.0, -66.0

        t0 = time.time()

        for idx in range(len(df)):
            try:
                x_val = float(df.iat[idx, df.columns.get_loc(x_col)]) if pd.notna(df.iat[idx, df.columns.get_loc(x_col)]) else None
                y_val = float(df.iat[idx, df.columns.get_loc(y_col)]) if pd.notna(df.iat[idx, df.columns.get_loc(y_col)]) else None
            except (ValueError, TypeError, KeyError):
                continue

            if x_val is None or y_val is None or x_val == 0.0 or y_val == 0.0:
                continue

            crash_lon, crash_lat = x_val, y_val

            # Skip points outside state bounding box
            if not (lat_min <= crash_lat <= lat_max and lon_min <= crash_lon <= lon_max):
                continue

            total_checked += 1
            stated_juris = str(df.iat[idx, df.columns.get_loc(juris_col)]).strip()

            # Resolve true county
            result = self.resolve_point(crash_lat, crash_lon)
            if not result or not result.get("county_name"):
                continue

            true_county = result["county_name"]

            # Reassign if mismatch
            if true_county != stated_juris and true_county in self.county_dict:
                old_juris = stated_juris
                new_geo = self.county_dict[true_county]

                df.at[idx, juris_col]            = true_county
                df.at[idx, "FIPS"]               = new_geo.get("fips", "").zfill(3)
                df.at[idx, "DOT District"]       = new_geo.get("district", "")
                # Legacy compat for states still using VDOT District column name
                if "VDOT District" in df.columns:
                    df.at[idx, "VDOT District"]  = new_geo.get("district", "")
                df.at[idx, "Planning District"]  = new_geo.get("district", "")
                df.at[idx, "MPO Name"]           = new_geo.get("mpo", "")
                df.at[idx, "Area Type"]          = new_geo.get("area_type", "Rural")

                total_reassigned += 1
                pair_key = f"{old_juris} → {true_county}"
                stats[pair_key] = stats.get(pair_key, 0) + 1

                if len(sample_log) < 10:
                    sample_log.append((idx, old_juris, true_county, result.get("source", "?")))

            # Progress every 10K rows
            if idx > 0 and idx % 10000 == 0:
                elapsed = time.time() - t0
                rate = idx / max(elapsed, 0.01)
                print(f"        ... {idx:,}/{len(df):,} rows ({rate:.0f} rows/sec, "
                      f"{total_reassigned:,} reassigned so far)")

        # Force re-pad FIPS
        df["FIPS"] = df["FIPS"].fillna("").astype(str).str.zfill(3).replace("000", "")

        elapsed = time.time() - t0

        # Print summary
        print(f"\n        GPS Jurisdiction Validation Complete ({elapsed:.1f}s)")
        print(f"        Tier usage: shapely={self.tier_used['shapely_pip']:,}, "
              f"API={self.tier_used['tigerweb_api']:,}, "
              f"centroid={self.tier_used['centroid']:,}")
        print(f"        Grid cache: {len(self._grid_cache):,} unique cells cached")

        if total_reassigned > 0:
            print(f"        ⚠️  {total_reassigned:,} of {total_checked:,} crashes reassigned:")
            for pair, count in sorted(stats.items(), key=lambda x: -x[1]):
                print(f"           {pair}: {count:,} crashes")
            if sample_log:
                print(f"        Sample (first {len(sample_log)}):")
                for row_idx, old, new, src in sample_log:
                    print(f"           Row {row_idx}: {old} → {new} (via {src})")
        else:
            print(f"        ✅ All {total_checked:,} GPS-validated crashes match stated jurisdiction")

        return df, stats


# ─────────────────────────────────────────────────────────────────────────────
#  Standalone CLI (for testing)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TIGERweb PIP — test a single point")
    parser.add_argument("--lat", type=float, required=True, help="Latitude")
    parser.add_argument("--lon", type=float, required=True, help="Longitude")
    parser.add_argument("--state-fips", default="10", help="State FIPS (default: 10 = Delaware)")
    args = parser.parse_args()

    # Quick test with Delaware defaults
    test_counties = {
        "Kent":       {"fips": "001", "district": "Central District", "mpo": "Dover/Kent County MPO", "area_type": "Urban",
                        "centlat": 39.097088, "centlon": -75.502982},
        "New Castle":  {"fips": "003", "district": "North District", "mpo": "WILMAPCO", "area_type": "Urban",
                        "centlat": 39.575915, "centlon": -75.644132},
        "Sussex":      {"fips": "005", "district": "South District", "mpo": "Salisbury-Wicomico MPO", "area_type": "Rural",
                        "centlat": 38.673227, "centlon": -75.337024},
    }

    validator = TIGERwebValidator(
        state_fips=args.state_fips,
        state_abbreviation="de",
        county_dict=test_counties,
    )

    result = validator.resolve_point(args.lat, args.lon)
    if result:
        print(f"\nPoint ({args.lat}, {args.lon}) → {result['county_name']} "
              f"(FIPS {result['county_fips']}, via {result['source']})")
    else:
        print(f"\nPoint ({args.lat}, {args.lon}) → Could not resolve")
