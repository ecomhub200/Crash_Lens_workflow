"""
spatial_matcher.py — CrashLens Spatial Matching Engine
======================================================
Tiered fallback for GPS → nearest road segment matching:

  Tier 1: DuckDB Spatial  — SQL spatial join, O(log n), ~5 seconds for 566K
  Tier 2: GeoPandas        — sjoin_nearest with R-tree index, ~30 seconds
  Tier 3: SciPy KDTree     — Pure Python fallback, ~5 minutes

All tiers return the same output: (matched_crash_idx, matched_road_idx, distances_ft)

Usage:
    from spatial_matcher import SpatialMatcher
    matcher = SpatialMatcher(ri_lats, ri_lons, ri_u_lats, ri_u_lons, ri_v_lats, ri_v_lons)
    crash_idx, road_idx, dists = matcher.match(crash_lats, crash_lons, threshold_ft=328)
"""

import time
import numpy as np

# ── Haversine / distance helpers ──

def _point_to_segment_dist_ft(plat, plon, ulat, ulon, vlat, vlon):
    """Perpendicular distance from point to line segment, in feet."""
    M = 111320.0
    cos_lat = np.cos(np.radians(plat))
    px = (plon - ulon) * M * cos_lat
    py = (plat - ulat) * M
    vx = (vlon - ulon) * M * cos_lat
    vy = (vlat - ulat) * M
    len_sq = vx * vx + vy * vy
    if len_sq < 1e-10:
        dist_m = np.sqrt(px * px + py * py)
    else:
        t = max(0.0, min(1.0, (px * vx + py * vy) / len_sq))
        dist_m = np.sqrt((px - t * vx) ** 2 + (py - t * vy) ** 2)
    return dist_m * 3.28084


def _validate_gps(lats, lons):
    """Return boolean mask of valid US GPS coordinates."""
    return (
        np.isfinite(lats) & np.isfinite(lons) &
        (lats != 0) & (lons != 0) &
        (lats > 20) & (lats < 72) &
        (lons < -60) & (lons > -180)
    )


class SpatialMatcher:
    """Tiered spatial matcher: DuckDB → GeoPandas → KDTree.

    Initialize with road inventory coordinates, then call match()
    with crash coordinates. The matcher automatically selects the
    fastest available engine.

    Args:
        mid_lats, mid_lons: Road segment midpoints (for initial nearest search)
        u_lats, u_lons:     Segment start points (for perpendicular refinement)
        v_lats, v_lons:     Segment end points (for perpendicular refinement)
    """

    def __init__(self, mid_lats, mid_lons, u_lats, u_lons, v_lats, v_lons):
        self.mid_lats = np.asarray(mid_lats, dtype=np.float64)
        self.mid_lons = np.asarray(mid_lons, dtype=np.float64)
        self.u_lats = np.asarray(u_lats, dtype=np.float64)
        self.u_lons = np.asarray(u_lons, dtype=np.float64)
        self.v_lats = np.asarray(v_lats, dtype=np.float64)
        self.v_lons = np.asarray(v_lons, dtype=np.float64)
        self.n_roads = len(mid_lats)
        self.cos_lat = np.cos(np.radians(np.mean(self.mid_lats)))
        self.engine = None
        self._detect_engine()

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
            print(f"    Spatial engine: DuckDB {duckdb.__version__} (Tier 1 — fastest)")
            return
        except Exception:
            pass

        # Tier 2: GeoPandas sjoin_nearest
        try:
            import geopandas as gpd
            from shapely.geometry import Point
            self.engine = "geopandas"
            print(f"    Spatial engine: GeoPandas {gpd.__version__} (Tier 2)")
            return
        except Exception:
            pass

        # Tier 3: SciPy KDTree
        try:
            from scipy.spatial import cKDTree
            self.engine = "kdtree"
            print(f"    Spatial engine: SciPy KDTree (Tier 3 — slowest)")
            return
        except Exception:
            pass

        print("    ⚠️ No spatial engine available!")
        self.engine = None

    def match(self, crash_lats, crash_lons, threshold_ft=328, k=5):
        """Match crash GPS to nearest road segments.

        Args:
            crash_lats, crash_lons: Arrays of crash coordinates
            threshold_ft: Max match distance in feet (default 328 = 100m)
            k: Number of KDTree candidates for refinement (Tier 3 only)

        Returns:
            Tuple of (matched_crash_indices, matched_road_indices, distances_ft)
            All as numpy arrays. Only includes rows within threshold.
        """
        crash_lats = np.asarray(crash_lats, dtype=np.float64)
        crash_lons = np.asarray(crash_lons, dtype=np.float64)

        valid = _validate_gps(crash_lats, crash_lons)
        if valid.sum() == 0:
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

        if self.engine == "duckdb":
            return self._match_duckdb(crash_lats, crash_lons, valid, threshold_ft)
        elif self.engine == "geopandas":
            return self._match_geopandas(crash_lats, crash_lons, valid, threshold_ft)
        elif self.engine == "kdtree":
            return self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, k)
        else:
            print("    ❌ No spatial engine — cannot match")
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

    # ══════════════════════════════════════════════════════════
    #  TIER 1: DuckDB Spatial — SQL spatial join
    # ══════════════════════════════════════════════════════════

    def _match_duckdb(self, crash_lats, crash_lons, valid, threshold_ft):
        """DuckDB spatial join with perpendicular refinement."""
        import duckdb
        import pandas as pd_

        t0 = time.time()
        vi = np.where(valid)[0]
        threshold_deg = threshold_ft / 364000.0

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except Exception:
            # Extension may already be installed or unavailable
            try:
                con.execute("LOAD spatial;")
            except Exception as e:
                print(f"    DuckDB spatial unavailable: {e}")
                con.close()
                # Fallback to next tier
                self.engine = "geopandas"
                try:
                    import geopandas
                    print(f"    Falling back to GeoPandas {geopandas.__version__}")
                    return self._match_geopandas(crash_lats, crash_lons, valid, threshold_ft)
                except ImportError:
                    self.engine = "kdtree"
                    print(f"    Falling back to KDTree")
                    return self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, 5)

        crashes_df = pd_.DataFrame({
            "ci": vi, "clat": crash_lats[vi], "clon": crash_lons[vi]})
        roads_df = pd_.DataFrame({
            "ri": np.arange(self.n_roads),
            "mlat": self.mid_lats, "mlon": self.mid_lons})

        con.register("crashes", crashes_df)
        con.register("roads", roads_df)

        cos_lat = self.cos_lat

        # Bounding box filter + nearest per crash using window function
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

        # Perpendicular refinement on top-5 candidates per crash
        result_ci = result["ci"].values.astype(int)
        result_ri = result["ri"].values.astype(int)

        unique_ci = np.unique(result_ci)
        best_ci, best_ri, best_dist = [], [], []

        for ci in unique_ci:
            mask = result_ci == ci
            cands = result_ri[mask]
            min_d = np.inf
            min_ri = -1
            for ri in cands:
                d = _point_to_segment_dist_ft(
                    crash_lats[ci], crash_lons[ci],
                    self.u_lats[ri], self.u_lons[ri],
                    self.v_lats[ri], self.v_lons[ri])
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

    # ══════════════════════════════════════════════════════════
    #  TIER 2: GeoPandas sjoin_nearest
    # ══════════════════════════════════════════════════════════

    def _match_geopandas(self, crash_lats, crash_lons, valid, threshold_ft):
        """GeoPandas sjoin_nearest with R-tree spatial index."""
        import geopandas as gpd
        from shapely.geometry import Point

        t0 = time.time()
        vi = np.where(valid)[0]

        # Build GeoDataFrames
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

        # sjoin_nearest — uses R-tree index internally
        joined = gpd.sjoin_nearest(
            crash_points, road_points,
            how="inner",
            distance_col="dist_deg",
        )

        if len(joined) == 0:
            print(f"    GeoPandas: 0 matches ({time.time()-t0:.1f}s)")
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

        # Deduplicate: keep nearest match per crash
        joined = joined.sort_values("dist_deg").drop_duplicates(subset=["ci"], keep="first")

        # Perpendicular refinement for actual ft distance
        best_ci = []
        best_ri = []
        best_dist = []

        ci_arr = joined["ci"].values.astype(int)
        ri_arr = joined["ri"].values.astype(int)

        for idx in range(len(ci_arr)):
            ci = ci_arr[idx]
            ri = ri_arr[idx]
            d = _point_to_segment_dist_ft(
                crash_lats[ci], crash_lons[ci],
                self.u_lats[ri], self.u_lons[ri],
                self.v_lats[ri], self.v_lons[ri])
            if d <= threshold_ft:
                best_ci.append(ci)
                best_ri.append(ri)
                best_dist.append(d)

        elapsed = time.time() - t0
        print(f"    GeoPandas: {len(best_ci):,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return np.array(best_ci), np.array(best_ri), np.array(best_dist)

    # ══════════════════════════════════════════════════════════
    #  TIER 3: SciPy KDTree (current method)
    # ══════════════════════════════════════════════════════════

    def _match_kdtree(self, crash_lats, crash_lons, valid, threshold_ft, k=5):
        """SciPy cKDTree with perpendicular refinement. Pure Python fallback."""
        from scipy.spatial import cKDTree

        t0 = time.time()
        vi = np.where(valid)[0]
        M = 111320.0
        cos_lat = np.cos(np.radians(np.mean(crash_lats[vi])))

        # Build tree on road midpoints
        ri_xy = np.column_stack([
            self.mid_lats * M,
            self.mid_lons * M * cos_lat,
        ])
        tree = cKDTree(ri_xy)

        crash_xy = np.column_stack([
            crash_lats[vi] * M,
            crash_lons[vi] * M * cos_lat,
        ])
        _, cand_idxs = tree.query(crash_xy, k=k)

        best_seg = np.zeros(len(vi), dtype=np.int64)
        best_dist = np.full(len(vi), np.inf)

        for ki in range(k):
            si = cand_idxs[:, ki]
            for i in range(len(vi)):
                d = _point_to_segment_dist_ft(
                    crash_lats[vi[i]], crash_lons[vi[i]],
                    self.u_lats[si[i]], self.u_lons[si[i]],
                    self.v_lats[si[i]], self.v_lons[si[i]])
                if d < best_dist[i]:
                    best_dist[i] = d
                    best_seg[i] = si[i]

        within = best_dist <= threshold_ft
        elapsed = time.time() - t0
        mc = within.sum()
        print(f"    KDTree: {mc:,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return vi[within], best_seg[within], best_dist[within]
