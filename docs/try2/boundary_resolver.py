"""
boundary_resolver.py — Vectorized Point-in-Polygon for CrashLens
=================================================================
Replaces row-by-row tigerweb_pip.py API calls with vectorized geopandas
sjoin against pre-downloaded boundary polygons.

Performance:
  tigerweb_pip:     566K × shapely.contains() = ~8 min + API calls
  boundary_resolver: geopandas.sjoin(566K, 3222)  = ~2 seconds

Usage:
    from boundary_resolver import BoundaryResolver

    resolver = BoundaryResolver(cache_dir="cache/boundaries")
    df = resolver.resolve_counties(df, x_col="x", y_col="y")
    df = resolver.resolve_places(df, x_col="x", y_col="y")
    df = resolver.resolve_mpos(df, x_col="x", y_col="y")

Or use in de_normalize.py Phase 3.5:
    resolver = BoundaryResolver(cache_dir="cache/boundaries")
    df, stats = resolver.validate_jurisdiction(
        df, state_fips="10", county_dict=DE_COUNTIES)
"""

import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


class BoundaryResolver:
    """Vectorized boundary resolution using pre-downloaded polygons."""

    def __init__(self, cache_dir: str = "cache/boundaries", r2_prefix: str = "_national/boundaries"):
        self.cache_dir = Path(cache_dir)
        self.r2_prefix = r2_prefix

        # Lazy-loaded GeoDataFrames
        self._counties = None
        self._places = None
        self._mpos = None
        self._county_subdivisions = None
        self._states = None
        self._urban_areas = None

    def _load_boundaries(self, filename: str):
        """Load a boundary file, trying local cache then R2."""
        import geopandas as gpd

        local = self.cache_dir / filename
        if local.exists():
            return gpd.read_parquet(local)

        # Try R2
        try:
            import boto3
            import io
            endpoint = __import__("os").environ.get("R2_ENDPOINT")
            key_id = __import__("os").environ.get("R2_ACCESS_KEY_ID")
            secret = __import__("os").environ.get("R2_SECRET_ACCESS_KEY")
            bucket = __import__("os").environ.get("R2_BUCKET", "crash-lens-data")

            if all([endpoint, key_id, secret]):
                s3 = boto3.client("s3", endpoint_url=endpoint,
                                   aws_access_key_id=key_id,
                                   aws_secret_access_key=secret)
                r2_key = f"{self.r2_prefix}/{filename}"
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, r2_key, str(local))
                return gpd.read_parquet(local)
        except Exception:
            pass

        # Try WKT fallback (if saved as regular parquet with geometry_wkt)
        if local.exists():
            df = pd.read_parquet(local)
            if "geometry_wkt" in df.columns:
                from shapely import wkt
                geoms = df["geometry_wkt"].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df.drop(columns=["geometry_wkt"]),
                                        geometry=geoms, crs="EPSG:4326")
                return gdf

        return None

    @property
    def counties(self):
        if self._counties is None:
            self._counties = self._load_boundaries("us_county_boundaries.parquet.gz")
        return self._counties

    @property
    def places(self):
        if self._places is None:
            self._places = self._load_boundaries("us_place_boundaries.parquet.gz")
        return self._places

    @property
    def mpos(self):
        if self._mpos is None:
            self._mpos = self._load_boundaries("us_mpo_boundaries.parquet.gz")
        return self._mpos

    @property
    def county_subdivisions(self):
        if self._county_subdivisions is None:
            self._county_subdivisions = self._load_boundaries(
                "us_county_subdivision_boundaries.parquet.gz")
        return self._county_subdivisions

    @property
    def urban_areas(self):
        if self._urban_areas is None:
            self._urban_areas = self._load_boundaries(
                "us_urban_area_boundaries.parquet.gz")
        return self._urban_areas

    def _state_col(self, gdf):
        """Find the state FIPS column name (STATEFP or STATE)."""
        for col in ["STATEFP", "STATE", "STATEFP20", "STATEFP10"]:
            if col in gdf.columns:
                return col
        return "STATE"  # will raise KeyError with clear message

    def _county_col(self, gdf):
        """Find the county FIPS column name (COUNTYFP or COUNTY)."""
        for col in ["COUNTYFP", "COUNTY", "COUNTYFP20", "COUNTYFP10"]:
            if col in gdf.columns:
                return col
        return "COUNTY"

    def _name_col(self, gdf):
        """Find the name column (NAME or BASENAME)."""
        for col in ["BASENAME", "NAME", "NAMELSAD"]:
            if col in gdf.columns:
                return col
        return "NAME"

    def _place_col(self, gdf):
        """Find the place FIPS column (PLACEFP or PLACE)."""
        for col in ["PLACEFP", "PLACE", "PLACEFP20"]:
            if col in gdf.columns:
                return col
        return "PLACE"

    def _make_crash_gdf(self, df, x_col="x", y_col="y"):
        """Convert crash DataFrame to GeoDataFrame with Point geometries."""
        import geopandas as gpd
        from shapely.geometry import Point

        lons = pd.to_numeric(df[x_col], errors="coerce")
        lats = pd.to_numeric(df[y_col], errors="coerce")

        valid = lons.notna() & lats.notna() & (lons != 0) & (lats != 0)
        points = gpd.GeoSeries(
            [Point(lon, lat) if v else None
             for lon, lat, v in zip(lons, lats, valid)],
            crs="EPSG:4326",
        )

        return gpd.GeoDataFrame(df, geometry=points, crs="EPSG:4326"), valid

    def resolve_counties(self, df, x_col="x", y_col="y",
                          state_fips=None) -> pd.DataFrame:
        """Resolve county for each crash via spatial join.

        Adds columns: resolved_county_fips, resolved_county_name, resolved_county_geoid
        """
        if self.counties is None:
            print("    ⚠️ County boundaries not available")
            return df

        t0 = time.time()
        import geopandas as gpd

        counties = self.counties
        if state_fips:
            counties = counties[counties[self._state_col(counties)] == state_fips].copy()

        crash_gdf, valid = self._make_crash_gdf(df, x_col, y_col)

        county_col = self._county_col(counties)
        name_col = self._name_col(counties)
        geoid_col = "GEOID" if "GEOID" in counties.columns else "GEOID20"

        # Spatial join — only valid GPS rows
        valid_gdf = crash_gdf[valid].copy()
        join_cols = ["geometry"]
        for c in [county_col, name_col, geoid_col]:
            if c in counties.columns:
                join_cols.append(c)

        joined = gpd.sjoin(valid_gdf, counties[join_cols],
                            how="left", predicate="within")

        # Handle duplicates (crash in overlapping boundaries)
        joined = joined[~joined.index.duplicated(keep="first")]

        # Transfer results back
        df["resolved_county_fips"] = ""
        df["resolved_county_name"] = ""
        df["resolved_county_geoid"] = ""
        if county_col in joined.columns:
            df.loc[joined.index, "resolved_county_fips"] = joined[county_col].fillna("").values
        if name_col in joined.columns:
            df.loc[joined.index, "resolved_county_name"] = joined[name_col].fillna("").values
        if geoid_col in joined.columns:
            df.loc[joined.index, "resolved_county_geoid"] = joined[geoid_col].fillna("").values

        matched = (df["resolved_county_fips"] != "").sum()
        elapsed = time.time() - t0
        print(f"    County PIP: {matched:,}/{valid.sum():,} resolved ({elapsed:.1f}s)")
        return df

    def resolve_places(self, df, x_col="x", y_col="y",
                        state_fips=None) -> pd.DataFrame:
        """Resolve city/place for each crash via spatial join."""
        if self.places is None:
            print("    ⚠️ Place boundaries not available")
            return df

        t0 = time.time()
        import geopandas as gpd

        places = self.places
        if state_fips:
            places = places[places[self._state_col(places)] == state_fips].copy()

        crash_gdf, valid = self._make_crash_gdf(df, x_col, y_col)
        valid_gdf = crash_gdf[valid].copy()

        place_col = self._place_col(places)
        name_col = self._name_col(places)

        join_cols = ["geometry"]
        for c in [place_col, name_col]:
            if c in places.columns:
                join_cols.append(c)

        joined = gpd.sjoin(valid_gdf, places[join_cols],
                            how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")]

        df["resolved_place_fips"] = ""
        df["resolved_place_name"] = ""
        if place_col in joined.columns:
            df.loc[joined.index, "resolved_place_fips"] = joined[place_col].fillna("").values
        if name_col in joined.columns:
            df.loc[joined.index, "resolved_place_name"] = joined[name_col].fillna("").values

        matched = (df["resolved_place_fips"] != "").sum()
        elapsed = time.time() - t0
        print(f"    Place PIP: {matched:,}/{valid.sum():,} in city limits ({elapsed:.1f}s)")
        return df

    def resolve_mpos(self, df, x_col="x", y_col="y") -> pd.DataFrame:
        """Resolve MPO for each crash via spatial join."""
        if self.mpos is None:
            print("    ⚠️ MPO boundaries not available")
            return df

        t0 = time.time()
        import geopandas as gpd

        # MPO layer field names may vary
        name_col = None
        for candidate in ["MPO_NAME", "NAME", "MPO_Name"]:
            if candidate in self.mpos.columns:
                name_col = candidate
                break

        crash_gdf, valid = self._make_crash_gdf(df, x_col, y_col)
        valid_gdf = crash_gdf[valid].copy()

        join_cols = ["geometry"]
        if name_col:
            join_cols.append(name_col)

        joined = gpd.sjoin(valid_gdf, self.mpos[join_cols], how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")]

        df["resolved_mpo"] = ""
        if name_col and name_col in joined.columns:
            df.loc[joined.index, "resolved_mpo"] = joined[name_col].fillna("").values

        matched = (df["resolved_mpo"] != "").sum()
        elapsed = time.time() - t0
        print(f"    MPO PIP: {matched:,}/{valid.sum():,} in MPO area ({elapsed:.1f}s)")
        return df

    def resolve_area_type(self, lats, lons):
        """Classify each point as Urban / Suburban / Rural using Census boundaries.

        Census Urban Area types:
          - LSAD20 = '75' → Urbanized Area (pop 50K+) → "Urban"
          - LSAD20 = '76' → Urban Cluster (pop 2,500-49,999) → "Suburban"
          - Not in any UA/UC → "Rural"

        Args:
            lats: array of latitudes
            lons: array of longitudes

        Returns:
            tuple of 3 numpy arrays:
              area_type:  "Urban" / "Suburban" / "Rural"
              ua_name:    Census urban area name (e.g., "Dover, DE Urbanized Area") or ""
              ua_geoid:   Census GEOID20 code (e.g., "24420") or ""
        """
        n = len(lats)
        area_type = np.full(n, "Rural", dtype=object)
        ua_name = np.full(n, "", dtype=object)
        ua_geoid = np.full(n, "", dtype=object)

        if self.urban_areas is None:
            print("    ⚠️ Urban area boundaries not available — defaulting to Rural")
            return area_type, ua_name, ua_geoid

        t0 = time.time()
        import geopandas as gpd
        from shapely.geometry import Point

        # Build points
        valid = np.isfinite(lats) & np.isfinite(lons) & (lats != 0) & (lons != 0)
        points = []
        for i in range(n):
            if valid[i]:
                points.append(Point(lons[i], lats[i]))
            else:
                points.append(None)

        point_gdf = gpd.GeoDataFrame(
            {"idx": range(n)}, geometry=points, crs="EPSG:4326")
        valid_gdf = point_gdf[valid].copy()

        if len(valid_gdf) == 0:
            return area_type, ua_name, ua_geoid

        # Determine column names dynamically
        ua = self.urban_areas
        lsad_col = None
        for candidate in ["LSAD20", "LSAD", "LSAD10"]:
            if candidate in ua.columns:
                lsad_col = candidate
                break

        name_col = None
        for candidate in ["NAMELSAD20", "NAMELSAD", "NAME20", "NAME"]:
            if candidate in ua.columns:
                name_col = candidate
                break

        geoid_col = None
        for candidate in ["GEOID20", "GEOID", "UACE20"]:
            if candidate in ua.columns:
                geoid_col = candidate
                break

        join_cols = ["geometry"]
        for c in [lsad_col, name_col, geoid_col]:
            if c and c in ua.columns:
                join_cols.append(c)

        # Spatial join
        joined = gpd.sjoin(valid_gdf, ua[join_cols], how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")]

        # Area type classification
        if lsad_col and lsad_col in joined.columns:
            lsad = joined[lsad_col].fillna("").astype(str).str.strip()
            is_urban = lsad == "75"
            is_suburban = lsad == "76"
            area_type[joined.index[is_urban].values] = "Urban"
            area_type[joined.index[is_suburban].values] = "Suburban"
        else:
            matched_idx = joined.index[joined["index_right"].notna()]
            area_type[matched_idx.values] = "Urban"

        # Urban area name
        if name_col and name_col in joined.columns:
            names = joined[name_col].fillna("").astype(str).values
            matched = joined["index_right"].notna()
            ua_name[joined.index[matched].values] = names[matched.values]

        # GEOID
        if geoid_col and geoid_col in joined.columns:
            geoids = joined[geoid_col].fillna("").astype(str).values
            matched = joined["index_right"].notna()
            ua_geoid[joined.index[matched].values] = geoids[matched.values]

        n_urban = (area_type == "Urban").sum()
        n_suburban = (area_type == "Suburban").sum()
        n_rural = (area_type == "Rural").sum()
        n_named = (ua_name != "").sum()
        elapsed = time.time() - t0
        print(f"    Area type from Census UA/UC: Urban={n_urban:,}, "
              f"Suburban={n_suburban:,}, Rural={n_rural:,}, named={n_named:,} ({elapsed:.1f}s)")

        return area_type, ua_name, ua_geoid

    def validate_jurisdiction(
        self,
        df: pd.DataFrame,
        state_fips: str,
        county_dict: dict,
        x_col: str = "x",
        y_col: str = "y",
        juris_col: str = "Physical Juris Name",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Phase 3.5 replacement: validate crash jurisdictions using polygon PIP.

        For each crash with valid GPS:
          1. sjoin against county boundaries → true county
          2. If true county ≠ stated jurisdiction → reassign FIPS, district, MPO, etc.

        Falls back to centroid distance if boundaries not available.

        Returns: (df, stats_dict)
        """
        t0 = time.time()

        if self.counties is None:
            print("        ⚠️ County boundaries not available — using centroid fallback")
            return self._centroid_fallback(df, county_dict, x_col, y_col, juris_col)

        import geopandas as gpd

        # Filter to state
        state_counties = self.counties[self.counties[self._state_col(self.counties)] == state_fips].copy()
        if len(state_counties) == 0:
            print(f"        ⚠️ No county boundaries for state FIPS {state_fips}")
            return self._centroid_fallback(df, county_dict, x_col, y_col, juris_col)

        # Build crash GeoDataFrame
        crash_gdf, valid = self._make_crash_gdf(df, x_col, y_col)
        valid_gdf = crash_gdf[valid].copy()

        county_col = self._county_col(state_counties)
        name_col = self._name_col(state_counties)

        join_cols = ["geometry"]
        for c in [county_col, name_col]:
            if c in state_counties.columns:
                join_cols.append(c)

        # Spatial join — find containing county
        joined = gpd.sjoin(
            valid_gdf, state_counties[join_cols],
            how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")]

        # Map BASENAME to county_dict keys
        basename_to_county = {}
        for county_name in county_dict:
            basename_to_county[county_name.lower()] = county_name
            # Also match without "County" suffix
            base = county_name.replace(" County", "").strip()
            basename_to_county[base.lower()] = county_name

        def _resolve_name(basename):
            if pd.isna(basename) or not basename:
                return ""
            b = str(basename).strip()
            return basename_to_county.get(b.lower(), b)

        true_county = joined[name_col].apply(_resolve_name) if name_col in joined.columns \
            else pd.Series("", index=joined.index)

        # Find mismatches
        stated = df.loc[joined.index, juris_col].fillna("").astype(str).str.strip()
        mismatched = (true_county != "") & (true_county != stated)

        stats = {}
        total_checked = valid.sum()
        total_reassigned = mismatched.sum()

        if total_reassigned > 0:
            mismatch_idx = joined.index[mismatched]
            true_counties = true_county[mismatched]

            for county_name in true_counties.unique():
                if not county_name or county_name not in county_dict:
                    continue
                mask = mismatch_idx[true_counties == county_name]
                geo = county_dict[county_name]

                old_juris = df.loc[mask, juris_col].values

                df.loc[mask, juris_col] = county_name
                df.loc[mask, "FIPS"] = geo.get("fips", "")
                df.loc[mask, "DOT District"] = geo.get("district", "")
                if "VDOT District" in df.columns:
                    df.loc[mask, "VDOT District"] = geo.get("district", "")
                df.loc[mask, "Planning District"] = geo.get("district", "")
                df.loc[mask, "MPO Name"] = geo.get("mpo", "")
                df.loc[mask, "Area Type"] = geo.get("area_type", "Rural")

                for old in old_juris:
                    pair_key = f"{old} → {county_name}"
                    stats[pair_key] = stats.get(pair_key, 0) + 1

        df["FIPS"] = df["FIPS"].fillna("").astype(str).str.zfill(3).replace("000", "")

        elapsed = time.time() - t0
        if total_reassigned > 0:
            print(f"        ⚠️  GPS validation (polygon PIP): "
                  f"{total_reassigned:,} of {total_checked:,} reassigned ({elapsed:.1f}s)")
            for pair, count in sorted(stats.items(), key=lambda x: -x[1])[:10]:
                print(f"           {pair}: {count:,} crashes")
        else:
            print(f"        ✅ All {total_checked:,} crashes match stated jurisdiction ({elapsed:.1f}s)")

        return df, stats

    def _centroid_fallback(self, df, county_dict, x_col, y_col, juris_col):
        """Vectorized centroid nearest-neighbor (when polygons unavailable)."""
        centroids = {}
        for county, geo in county_dict.items():
            if "centlat" in geo and "centlon" in geo:
                centroids[county] = (geo["centlat"], geo["centlon"])

        if not centroids:
            return df, {}

        county_names = list(centroids.keys())
        county_lats = np.array([centroids[c][0] for c in county_names])
        county_lons = np.array([centroids[c][1] for c in county_names])

        crash_lon = pd.to_numeric(df[x_col], errors="coerce").fillna(0).values
        crash_lat = pd.to_numeric(df[y_col], errors="coerce").fillna(0).values
        valid = (crash_lat != 0) & (crash_lon != 0) & np.isfinite(crash_lat) & np.isfinite(crash_lon)

        cos_lat = np.cos(np.radians(np.mean(county_lats)))
        best_county_idx = np.zeros(len(df), dtype=int)
        best_dist_sq = np.full(len(df), np.inf)

        for ci, (clat, clon) in enumerate(zip(county_lats, county_lons)):
            dlat = crash_lat - clat
            dlon = (crash_lon - clon) * cos_lat
            dist_sq = dlat * dlat + dlon * dlon
            closer = dist_sq < best_dist_sq
            best_dist_sq[closer] = dist_sq[closer]
            best_county_idx[closer] = ci

        nearest_county = np.array(county_names)[best_county_idx]
        stated_juris = df[juris_col].fillna("").astype(str).str.strip().values
        mismatched = valid & (nearest_county != stated_juris)

        stats = {}
        total_reassigned = mismatched.sum()

        if total_reassigned > 0:
            for county_name in county_names:
                mask = mismatched & (nearest_county == county_name)
                if not mask.any():
                    continue
                geo = county_dict[county_name]
                df.loc[mask, juris_col] = county_name
                df.loc[mask, "FIPS"] = geo.get("fips", "")
                df.loc[mask, "DOT District"] = geo.get("district", "")
                if "VDOT District" in df.columns:
                    df.loc[mask, "VDOT District"] = geo.get("district", "")
                df.loc[mask, "Planning District"] = geo.get("district", "")
                df.loc[mask, "MPO Name"] = geo.get("mpo", "")
                df.loc[mask, "Area Type"] = geo.get("area_type", "Rural")

            for i in np.where(mismatched)[0]:
                pair_key = f"{stated_juris[i]} → {nearest_county[i]}"
                stats[pair_key] = stats.get(pair_key, 0) + 1

        df["FIPS"] = df["FIPS"].fillna("").astype(str).str.zfill(3).replace("000", "")

        if total_reassigned > 0:
            print(f"        ⚠️  GPS validation (centroid fallback): "
                  f"{total_reassigned:,} of {valid.sum():,} reassigned")
        else:
            print(f"        ✅ All {valid.sum():,} crashes match (centroid)")

        return df, stats
