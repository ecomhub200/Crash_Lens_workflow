"""
spatial_matcher.py — CrashLens Spatial Matching Engine (v3)
============================================================
Tiered fallback for GPS → nearest road segment matching:

  Tier 1: DuckDB Spatial  — SQL spatial join + linestring refinement
  Tier 2: GeoPandas        — KDTree top-k + linestring refinement (BEST PATH)
  Tier 3: SciPy KDTree     — KDTree top-k + linestring refinement

All tiers share KDTree-based top-k refinement with full linestring support.
When geometry_coords is available, perpendicular distance is computed against
the full road polyline (all intermediate vertices), not just the two endpoints.

v3 changes (over v2):
  - Linestring support: stores full polyline as flat arrays with offset indexing
  - Vectorized linestring refinement: loops over sub-segment index, vectorizes across N crashes
  - Backward compatible: falls back to u/v endpoints when geometry_coords absent

Usage:
    from spatial_matcher import SpatialMatcher
    matcher = SpatialMatcher(ri_lats, ri_lons, ri_u_lats, ri_u_lons, ri_v_lats, ri_v_lons,
                             geometry_coords=ri_df.get("geometry_coords"))
    crash_idx, road_idx, dists, confidence = matcher.match(crash_lats, crash_lons)
"""

import time
import json
import numpy as np

# ── Vectorized distance helpers ──

def _vec_point_to_segment_dist_ft(plats, plons, ulats, ulons, vlats, vlons):
    """Vectorized perpendicular distance from points to line segments, in feet.

    All inputs are 1-D numpy arrays of equal length.
    Returns 1-D array of distances in feet.
    """
    M = 111320.0
    cos_lat = np.cos(np.radians(plats))

    # Project to local meters
    px = (plons - ulons) * M * cos_lat
    py = (plats - ulats) * M
    vx = (vlons - ulons) * M * cos_lat
    vy = (vlats - ulats) * M

    len_sq = vx * vx + vy * vy
    t = np.where(len_sq < 1e-10, 0.0,
                 np.clip((px * vx + py * vy) / np.maximum(len_sq, 1e-10), 0.0, 1.0))

    dx = px - t * vx
    dy = py - t * vy
    return np.sqrt(dx * dx + dy * dy) * 3.28084


def _point_to_segment_dist_ft(plat, plon, ulat, ulon, vlat, vlon):
    """Scalar perpendicular distance from point to line segment, in feet."""
    M = 111320.0
    cos_lat = np.cos(np.radians(plat))
    px = (plon - ulon) * M * cos_lat
    py = (plat - ulat) * M
    vx = (vlon - ulon) * M * cos_lat
    vy = (vlat - ulat) * M
    len_sq = vx * vx + vy * vy
    if len_sq < 1e-10:
        return np.sqrt(px * px + py * py) * 3.28084
    t = max(0.0, min(1.0, (px * vx + py * vy) / len_sq))
    return np.sqrt((px - t * vx) ** 2 + (py - t * vy) ** 2) * 3.28084


def _validate_gps(lats, lons):
    """Return boolean mask of valid US GPS coordinates."""
    return (
        np.isfinite(lats) & np.isfinite(lons) &
        (lats != 0) & (lons != 0) &
        (lats > 20) & (lats < 72) &
        (lons < -60) & (lons > -180)
    )


def _confidence_labels(dists):
    """Assign confidence labels based on match distance."""
    conf = np.full(len(dists), "low", dtype=object)
    conf[dists <= 200] = "medium"
    conf[dists <= 100] = "high"
    return conf


class SpatialMatcher:
    """Tiered spatial matcher with full linestring support.

    All tiers share:
      - KDTree built at init on road midpoints (fast candidate search)
      - Flat-array linestring storage for vectorized perpendicular refinement
    """

    def __init__(self, mid_lats, mid_lons, u_lats, u_lons, v_lats, v_lons,
                 geometry_coords=None):
        """Initialize matcher.

        Args:
            mid_lats, mid_lons: Road midpoints for KDTree candidate search
            u_lats, u_lons:     Segment start points (fallback when no linestring)
            v_lats, v_lons:     Segment end points (fallback when no linestring)
            geometry_coords:    Series/array of JSON linestring strings, or None
                                Each is a JSON array of [lon, lat] pairs.
        """
        self.mid_lats = np.asarray(mid_lats, dtype=np.float64)
        self.mid_lons = np.asarray(mid_lons, dtype=np.float64)
        self.u_lats = np.asarray(u_lats, dtype=np.float64)
        self.u_lons = np.asarray(u_lons, dtype=np.float64)
        self.v_lats = np.asarray(v_lats, dtype=np.float64)
        self.v_lons = np.asarray(v_lons, dtype=np.float64)
        self.n_roads = len(mid_lats)
        self.cos_lat = np.cos(np.radians(np.mean(self.mid_lats)))
        self.engine = None
        self._tree = None
        self._ri_xy = None

        # Linestring flat arrays
        self._ls_flat_lats = None
        self._ls_flat_lons = None
        self._ls_offsets = None   # start index per segment
        self._ls_counts = None    # point count per segment
        self._has_linestrings = False

        self._build_kdtree()
        self._parse_linestrings(geometry_coords)
        self._detect_engine()

    def _build_kdtree(self):
        """Build KDTree on road midpoints — used by all tiers for refinement."""
        try:
            from scipy.spatial import cKDTree
            M = 111320.0
            self._ri_xy = np.column_stack([
                self.mid_lats * M,
                self.mid_lons * M * self.cos_lat,
            ])
            self._tree = cKDTree(self._ri_xy)
        except ImportError:
            pass

    def _parse_linestrings(self, geometry_coords):
        """Parse JSON linestring column into flat numpy arrays for vectorized access.

        Storage layout:
          _ls_flat_lats[i], _ls_flat_lons[i] = i-th point across ALL linestrings
          _ls_offsets[seg_id] = start index in flat arrays for segment's linestring
          _ls_counts[seg_id] = number of points in segment's linestring

        Segments without linestrings get 2-point entries from u/v endpoints.
        """
        if geometry_coords is None:
            # No linestrings — use u/v endpoints (2-point degenerate linestrings)
            self._ls_flat_lats = np.empty(self.n_roads * 2, dtype=np.float64)
            self._ls_flat_lons = np.empty(self.n_roads * 2, dtype=np.float64)
            self._ls_flat_lats[0::2] = self.u_lats
            self._ls_flat_lats[1::2] = self.v_lats
            self._ls_flat_lons[0::2] = self.u_lons
            self._ls_flat_lons[1::2] = self.v_lons
            self._ls_offsets = np.arange(0, self.n_roads * 2, 2, dtype=np.int64)
            self._ls_counts = np.full(self.n_roads, 2, dtype=np.int32)
            return

        # Parse JSON strings into coordinate lists
        all_lats = []
        all_lons = []
        offsets = np.empty(self.n_roads, dtype=np.int64)
        counts = np.empty(self.n_roads, dtype=np.int32)
        cursor = 0
        n_linestring = 0

        coords_arr = np.asarray(geometry_coords)
        for i in range(self.n_roads):
            raw = coords_arr[i]
            parsed = False
            if raw is not None and isinstance(raw, str) and raw.startswith("["):
                try:
                    coords = json.loads(raw)
                    if len(coords) >= 2:
                        for lon, lat in coords:
                            all_lats.append(lat)
                            all_lons.append(lon)
                        offsets[i] = cursor
                        counts[i] = len(coords)
                        cursor += len(coords)
                        parsed = True
                        if len(coords) > 2:
                            n_linestring += 1
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

            if not parsed:
                # Fall back to u/v endpoints
                all_lats.append(self.u_lats[i])
                all_lons.append(self.u_lons[i])
                all_lats.append(self.v_lats[i])
                all_lons.append(self.v_lons[i])
                offsets[i] = cursor
                counts[i] = 2
                cursor += 2

        self._ls_flat_lats = np.array(all_lats, dtype=np.float64)
        self._ls_flat_lons = np.array(all_lons, dtype=np.float64)
        self._ls_offsets = offsets
        self._ls_counts = counts
        self._has_linestrings = n_linestring > 0

        if n_linestring > 0:
            pct = n_linestring / self.n_roads * 100
            avg_pts = counts[counts > 2].mean() if (counts > 2).any() else 0
            print(f"    Linestrings: {n_linestring:,}/{self.n_roads:,} "
                  f"({pct:.0f}%) have full geometry (avg {avg_pts:.1f} pts)")

    def _detect_engine(self):
        """Detect best available spatial engine."""
        # Tier 1: DuckDB Spatial
        try:
            import duckdb
            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial; LOAD spatial;")
            except Exception:
                con.execute("LOAD spatial;")
            con.execute("SELECT ST_Point(0,0)")
            con.close()
            self.engine = "duckdb"
            ls_tag = " + linestrings" if self._has_linestrings else ""
            print(f"    Spatial engine: DuckDB {duckdb.__version__} (Tier 1){ls_tag}")
            return
        except Exception:
            pass

        # Tier 2: GeoPandas + KDTree
        try:
            import geopandas as gpd
            self.engine = "geopandas"
            ls_tag = " + linestrings" if self._has_linestrings else ""
            print(f"    Spatial engine: GeoPandas {gpd.__version__} + KDTree (Tier 2){ls_tag}")
            return
        except Exception:
            pass

        # Tier 3: SciPy KDTree only
        if self._tree is not None:
            self.engine = "kdtree"
            ls_tag = " + linestrings" if self._has_linestrings else ""
            print(f"    Spatial engine: SciPy KDTree (Tier 3){ls_tag}")
            return

        print("    ⚠️ No spatial engine available!")

    def match(self, crash_lats, crash_lons, threshold_ft=328, k=5):
        """Match crash GPS to nearest road segments.

        Returns:
            (matched_crash_indices, matched_road_indices, distances_ft, confidence)
        """
        crash_lats = np.asarray(crash_lats, dtype=np.float64)
        crash_lons = np.asarray(crash_lons, dtype=np.float64)

        valid = _validate_gps(crash_lats, crash_lons)
        if valid.sum() == 0:
            empty = np.array([], dtype=int)
            return empty, empty, np.array([]), np.array([], dtype=object)

        if self.engine == "duckdb":
            ci, ri, d = self._match_duckdb(crash_lats, crash_lons, valid, threshold_ft)
        elif self.engine == "geopandas":
            ci, ri, d = self._match_geopandas(crash_lats, crash_lons, valid, threshold_ft, k)
        elif self.engine == "kdtree":
            ci, ri, d = self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, k)
        else:
            empty = np.array([], dtype=int)
            return empty, empty, np.array([]), np.array([], dtype=object)

        conf = _confidence_labels(d) if len(d) > 0 else np.array([], dtype=object)
        return ci, ri, d, conf

    # ══════════════════════════════════════════════════════════
    #  SHARED: Vectorized top-k linestring refinement
    # ══════════════════════════════════════════════════════════

    def _refine_topk(self, crash_lats, crash_lons, vi, threshold_ft, k=5):
        """KDTree top-k + vectorized linestring perpendicular refinement.

        For each crash, finds k nearest road midpoints via KDTree, then
        computes perpendicular distance to the full linestring of each
        candidate (all sub-segments), keeps the closest road within threshold.

        Vectorization strategy:
          - Outer loop: over k candidates (5 iterations)
          - Middle loop: over sub-segment index (max ~20 iterations for longest road)
          - Inner: fully vectorized numpy across all N crashes simultaneously
        """
        M = 111320.0
        crash_xy = np.column_stack([
            crash_lats[vi] * M,
            crash_lons[vi] * M * self.cos_lat,
        ])
        _, cand_idxs = self._tree.query(crash_xy, k=k)

        n = len(vi)
        best_seg = np.zeros(n, dtype=np.int64)
        best_dist = np.full(n, np.inf)

        c_lats = crash_lats[vi]
        c_lons = crash_lons[vi]

        for ki in range(k):
            si = cand_idxs[:, ki]  # road segment index for each crash

            # Number of sub-segments per candidate: counts - 1
            n_subseg = self._ls_counts[si] - 1  # (n,) array
            max_sub = int(n_subseg.max())

            d_ki = np.full(n, np.inf)

            for sub_i in range(max_sub):
                # Which crashes have a sub_i-th sub-segment on their candidate?
                mask = sub_i < n_subseg
                if not mask.any():
                    break

                # Get sub-segment endpoints from flat arrays
                # Clamp sub_i for short segments to avoid out-of-bounds access
                # (clamped values are ignored via mask below)
                safe_sub = np.minimum(sub_i, np.maximum(n_subseg - 1, 0))
                offsets = self._ls_offsets[si] + safe_sub
                u_lats = self._ls_flat_lats[offsets]
                u_lons = self._ls_flat_lons[offsets]
                v_lats = self._ls_flat_lats[offsets + 1]
                v_lons = self._ls_flat_lons[offsets + 1]

                sub_d = _vec_point_to_segment_dist_ft(
                    c_lats, c_lons, u_lats, u_lons, v_lats, v_lons)

                # Only update crashes that have this sub-segment
                d_ki = np.where(mask, np.minimum(d_ki, sub_d), d_ki)

            better = d_ki < best_dist
            best_dist[better] = d_ki[better]
            best_seg[better] = si[better]

        within = best_dist <= threshold_ft
        return vi[within], best_seg[within], best_dist[within]

    # ══════════════════════════════════════════════════════════
    #  TIER 1: DuckDB Spatial
    # ══════════════════════════════════════════════════════════

    def _match_duckdb(self, crash_lats, crash_lons, valid, threshold_ft):
        """DuckDB spatial join + linestring refinement."""
        import duckdb
        import pandas as pd_

        t0 = time.time()
        vi = np.where(valid)[0]
        threshold_deg = threshold_ft / 364000.0

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except Exception:
            try:
                con.execute("LOAD spatial;")
            except Exception as e:
                print(f"    DuckDB spatial unavailable: {e}")
                con.close()
                self.engine = "geopandas"
                try:
                    import geopandas
                    return self._match_geopandas(crash_lats, crash_lons, valid, threshold_ft, 5)
                except ImportError:
                    self.engine = "kdtree"
                    return self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, 5)

        crashes_df = pd_.DataFrame({
            "ci": vi, "clat": crash_lats[vi], "clon": crash_lons[vi]})
        roads_df = pd_.DataFrame({
            "ri": np.arange(self.n_roads),
            "mlat": self.mid_lats, "mlon": self.mid_lons})

        con.register("crashes", crashes_df)
        con.register("roads", roads_df)

        cos_lat = self.cos_lat
        result = con.execute(f"""
            WITH candidates AS (
                SELECT c.ci, r.ri,
                    (c.clat - r.mlat) * (c.clat - r.mlat) +
                    ((c.clon - r.mlon) * {cos_lat}) *
                    ((c.clon - r.mlon) * {cos_lat}) AS dist_sq
                FROM crashes c
                JOIN roads r ON
                    r.mlat BETWEEN c.clat - {threshold_deg} AND c.clat + {threshold_deg}
                    AND r.mlon BETWEEN c.clon - {threshold_deg} AND c.clon + {threshold_deg}
            ),
            ranked AS (
                SELECT ci, ri, dist_sq,
                    ROW_NUMBER() OVER (PARTITION BY ci ORDER BY dist_sq) as rn
                FROM candidates
            )
            SELECT ci, ri FROM ranked WHERE rn <= 5
        """).fetchdf()
        con.close()

        if len(result) == 0:
            print(f"    DuckDB: 0 candidates ({time.time()-t0:.1f}s)")
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

        # Group by crash, refine with linestring perpendicular
        result_ci = result["ci"].values.astype(int)
        result_ri = result["ri"].values.astype(int)
        unique_ci = np.unique(result_ci)
        best_ci, best_ri, best_dist = [], [], []

        for ci in unique_ci:
            mask = result_ci == ci
            cands = result_ri[mask]
            min_d = np.inf
            min_ri = cands[0]
            for ri in cands:
                d = self._dist_to_linestring(crash_lats[ci], crash_lons[ci], ri)
                if d < min_d:
                    min_d = d
                    min_ri = ri
            if min_d <= threshold_ft:
                best_ci.append(ci)
                best_ri.append(min_ri)
                best_dist.append(min_d)

        elapsed = time.time() - t0
        print(f"    DuckDB: {len(best_ci):,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return np.array(best_ci), np.array(best_ri), np.array(best_dist)

    def _dist_to_linestring(self, plat, plon, seg_id):
        """Scalar distance from point to segment's full linestring."""
        off = self._ls_offsets[seg_id]
        cnt = self._ls_counts[seg_id]
        min_d = np.inf
        for i in range(cnt - 1):
            d = _point_to_segment_dist_ft(
                plat, plon,
                self._ls_flat_lats[off + i], self._ls_flat_lons[off + i],
                self._ls_flat_lats[off + i + 1], self._ls_flat_lons[off + i + 1])
            if d < min_d:
                min_d = d
        return min_d

    # ══════════════════════════════════════════════════════════
    #  TIER 2: GeoPandas + KDTree linestring refinement
    # ══════════════════════════════════════════════════════════

    def _match_geopandas(self, crash_lats, crash_lons, valid, threshold_ft, k=5):
        """Best path: KDTree top-k with vectorized linestring refinement."""
        t0 = time.time()
        vi = np.where(valid)[0]

        if self._tree is not None:
            ci, ri, d = self._refine_topk(crash_lats, crash_lons, vi, threshold_ft, k)
            elapsed = time.time() - t0
            print(f"    GeoPandas+KDTree: {len(ci):,}/{len(vi):,} matched ({elapsed:.1f}s)")
            return ci, ri, d

        # Fallback: pure GeoPandas (no KDTree)
        import geopandas as gpd
        from shapely.geometry import Point
        import warnings

        crash_points = gpd.GeoDataFrame(
            {"ci": vi},
            geometry=[Point(crash_lons[i], crash_lats[i]) for i in vi],
            crs="EPSG:4326",
        )
        road_points = gpd.GeoDataFrame(
            {"ri": np.arange(self.n_roads)},
            geometry=[Point(self.mid_lons[i], self.mid_lats[i]) for i in range(self.n_roads)],
            crs="EPSG:4326",
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            joined = gpd.sjoin_nearest(
                crash_points, road_points, how="inner", distance_col="dist_deg")

        if len(joined) == 0:
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

        joined = joined.sort_values("dist_deg").drop_duplicates(subset=["ci"], keep="first")
        ci_arr = joined["ci"].values.astype(int)
        ri_arr = joined["ri"].values.astype(int)

        dists = _vec_point_to_segment_dist_ft(
            crash_lats[ci_arr], crash_lons[ci_arr],
            self.u_lats[ri_arr], self.u_lons[ri_arr],
            self.v_lats[ri_arr], self.v_lons[ri_arr])

        within = dists <= threshold_ft
        elapsed = time.time() - t0
        print(f"    GeoPandas: {within.sum():,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return ci_arr[within], ri_arr[within], dists[within]

    # ══════════════════════════════════════════════════════════
    #  TIER 3: SciPy KDTree only
    # ══════════════════════════════════════════════════════════

    def _match_kdtree(self, crash_lats, crash_lons, valid, threshold_ft, k=5):
        """KDTree with vectorized linestring refinement."""
        t0 = time.time()
        vi = np.where(valid)[0]
        ci, ri, d = self._refine_topk(crash_lats, crash_lons, vi, threshold_ft, k)
        elapsed = time.time() - t0
        print(f"    KDTree: {len(ci):,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return ci, ri, d
