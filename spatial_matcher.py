"""
spatial_matcher.py — CrashLens Spatial Matching Engine (v4)
============================================================
Tiered fallback for GPS → nearest road segment matching:

  Tier 1: DuckDB Spatial  — ST_Distance on real LineString geometries
  Tier 2: STRtree (Shapely) — R-Tree on LineString geometries, exact nearest
  Tier 3: SciPy KDTree     — top-k midpoint + perpendicular linestring refinement

Each tier is more accurate than the next:
  DuckDB:  SQL-based, true geometric distance, handles millions of rows
  STRtree: R-Tree spatial index on real LineStrings, exact nearest geometry
  KDTree:  Queries midpoints (approximate), then refines with perpendicular distance

All tiers support full road polylines (geometry_coords) for sub-segment accuracy.

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
    """Vectorized perpendicular distance from points to line segments, in feet."""
    M = 111320.0
    cos_lat = np.cos(np.radians(plats))
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
    """Tiered spatial matcher: DuckDB → STRtree → KDTree."""

    def __init__(self, mid_lats, mid_lons, u_lats, u_lons, v_lats, v_lons,
                 geometry_coords=None):
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
        self._ls_offsets = None
        self._ls_counts = None
        self._has_linestrings = False

        # STRtree index (built lazily)
        self._strtree = None
        self._shapely_lines = None

        self._build_kdtree()
        self._parse_linestrings(geometry_coords)
        self._detect_engine()

    def _build_kdtree(self):
        """Build KDTree on road midpoints — used by Tier 3."""
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
        """Parse JSON linestring column into flat numpy arrays."""
        if geometry_coords is None:
            self._ls_flat_lats = np.empty(self.n_roads * 2, dtype=np.float64)
            self._ls_flat_lons = np.empty(self.n_roads * 2, dtype=np.float64)
            self._ls_flat_lats[0::2] = self.u_lats
            self._ls_flat_lats[1::2] = self.v_lats
            self._ls_flat_lons[0::2] = self.u_lons
            self._ls_flat_lons[1::2] = self.v_lons
            self._ls_offsets = np.arange(0, self.n_roads * 2, 2, dtype=np.int64)
            self._ls_counts = np.full(self.n_roads, 2, dtype=np.int32)
            return

        all_lats, all_lons = [], []
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
        ls_tag = " + linestrings" if self._has_linestrings else ""

        # Tier 1: DuckDB Spatial (ST_Distance on real geometries)
        try:
            import duckdb
            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial; LOAD spatial;")
            except Exception:
                con.execute("LOAD spatial;")
            con.execute("SELECT ST_Distance(ST_Point(0,0), ST_Point(1,1))")
            con.close()
            self.engine = "duckdb"
            print(f"    Spatial engine: DuckDB Spatial {duckdb.__version__} (Tier 1){ls_tag}")
            return
        except Exception:
            pass

        # Tier 2: STRtree (Shapely R-Tree on LineStrings)
        try:
            from shapely import STRtree as _STR
            self.engine = "strtree"
            print(f"    Spatial engine: Shapely STRtree (Tier 2){ls_tag}")
            return
        except ImportError:
            pass
        try:
            from shapely.strtree import STRtree as _STR2
            self.engine = "strtree"
            print(f"    Spatial engine: Shapely STRtree (Tier 2){ls_tag}")
            return
        except ImportError:
            pass

        # Tier 3: SciPy KDTree
        if self._tree is not None:
            self.engine = "kdtree"
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
        elif self.engine == "strtree":
            ci, ri, d = self._match_strtree(crash_lats, crash_lons, valid, threshold_ft)
        elif self.engine == "kdtree":
            ci, ri, d = self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, k)
        else:
            empty = np.array([], dtype=int)
            return empty, empty, np.array([]), np.array([], dtype=object)

        conf = _confidence_labels(d) if len(d) > 0 else np.array([], dtype=object)
        return ci, ri, d, conf

    # ══════════════════════════════════════════════════════════
    #  SHARED: linestring distance helper
    # ══════════════════════════════════════════════════════════

    def _dist_to_linestring(self, plat, plon, seg_id):
        """Scalar distance from point to segment's full linestring, in feet."""
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

    def _refine_topk(self, crash_lats, crash_lons, vi, threshold_ft, k=5):
        """KDTree top-k + vectorized linestring perpendicular refinement."""
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
            si = cand_idxs[:, ki]
            n_subseg = self._ls_counts[si] - 1
            max_sub = int(n_subseg.max())
            d_ki = np.full(n, np.inf)

            for sub_i in range(max_sub):
                mask = sub_i < n_subseg
                if not mask.any():
                    break
                safe_sub = np.minimum(sub_i, np.maximum(n_subseg - 1, 0))
                offsets = self._ls_offsets[si] + safe_sub
                u_lats = self._ls_flat_lats[offsets]
                u_lons = self._ls_flat_lons[offsets]
                v_lats = self._ls_flat_lats[offsets + 1]
                v_lons = self._ls_flat_lons[offsets + 1]
                sub_d = _vec_point_to_segment_dist_ft(
                    c_lats, c_lons, u_lats, u_lons, v_lats, v_lons)
                d_ki = np.where(mask, np.minimum(d_ki, sub_d), d_ki)

            better = d_ki < best_dist
            best_dist[better] = d_ki[better]
            best_seg[better] = si[better]

        within = best_dist <= threshold_ft
        return vi[within], best_seg[within], best_dist[within]

    # ══════════════════════════════════════════════════════════
    #  TIER 1: DuckDB Spatial (ST_Distance on LineStrings)
    # ══════════════════════════════════════════════════════════

    def _match_duckdb(self, crash_lats, crash_lons, valid, threshold_ft):
        """DuckDB spatial: build LineStrings, ST_Distance, return nearest."""
        import duckdb
        import pandas as pd_

        t0 = time.time()
        vi = np.where(valid)[0]
        threshold_m = threshold_ft / 3.28084

        con = duckdb.connect()
        try:
            try:
                con.execute("INSTALL spatial; LOAD spatial;")
            except Exception:
                con.execute("LOAD spatial;")
        except Exception:
            con.close()
            # Fall through to next tier
            return self._fallback_match(crash_lats, crash_lons, valid, threshold_ft)

        # Build WKT LineStrings from flat arrays
        wkt_lines = []
        for i in range(self.n_roads):
            off = self._ls_offsets[i]
            cnt = self._ls_counts[i]
            coords_str = ", ".join(
                f"{self._ls_flat_lons[off+j]:.7f} {self._ls_flat_lats[off+j]:.7f}"
                for j in range(cnt))
            wkt_lines.append(f"LINESTRING({coords_str})")

        roads_df = pd_.DataFrame({
            "ri": np.arange(self.n_roads),
            "mlat": self.mid_lats,
            "mlon": self.mid_lons,
            "geom_wkt": wkt_lines,
        })
        crashes_df = pd_.DataFrame({
            "ci": vi, "clat": crash_lats[vi], "clon": crash_lons[vi],
        })

        con.register("crashes", crashes_df)
        con.register("roads", roads_df)

        # Bounding-box pre-filter + exact ST_Distance on LineString
        threshold_deg = threshold_ft / 364000.0
        cos_lat = self.cos_lat
        M = 111320.0

        result = con.execute(f"""
            WITH candidates AS (
                SELECT c.ci, r.ri, r.geom_wkt, c.clat, c.clon
                FROM crashes c
                JOIN roads r ON
                    r.mlat BETWEEN c.clat - {threshold_deg} AND c.clat + {threshold_deg}
                    AND r.mlon BETWEEN c.clon - {threshold_deg} AND c.clon + {threshold_deg}
            ),
            distances AS (
                SELECT ci, ri,
                    -- Approximate distance in meters using degree-to-meter conversion
                    ST_Distance(
                        ST_Point(clon, clat),
                        ST_GeomFromText(geom_wkt)
                    ) * {M} * {cos_lat} AS dist_m
                FROM candidates
            ),
            ranked AS (
                SELECT ci, ri, dist_m,
                    ROW_NUMBER() OVER (PARTITION BY ci ORDER BY dist_m) AS rn
                FROM distances
            )
            SELECT ci, ri, dist_m FROM ranked WHERE rn = 1 AND dist_m <= {threshold_m}
        """).fetchdf()
        con.close()

        if len(result) == 0:
            elapsed = time.time() - t0
            print(f"    DuckDB Spatial: 0/{len(vi):,} matched ({elapsed:.1f}s)")
            return np.array([], dtype=int), np.array([], dtype=int), np.array([])

        ci_arr = result["ci"].values.astype(int)
        ri_arr = result["ri"].values.astype(int)

        # Refine with exact perpendicular distance on linestring sub-segments
        dists = np.array([self._dist_to_linestring(crash_lats[c], crash_lons[c], r)
                          for c, r in zip(ci_arr, ri_arr)])
        within = dists <= threshold_ft
        ci_arr, ri_arr, dists = ci_arr[within], ri_arr[within], dists[within]

        elapsed = time.time() - t0
        print(f"    DuckDB Spatial: {len(ci_arr):,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return ci_arr, ri_arr, dists

    # ══════════════════════════════════════════════════════════
    #  TIER 2: STRtree (Shapely R-Tree on real LineStrings)
    # ══════════════════════════════════════════════════════════

    def _build_strtree(self):
        """Build Shapely LineString geometries + STRtree index (lazy, once)."""
        if self._strtree is not None:
            return

        from shapely.geometry import LineString, Point

        t0 = time.time()
        lines = []
        for i in range(self.n_roads):
            off = self._ls_offsets[i]
            cnt = self._ls_counts[i]
            coords = [(self._ls_flat_lons[off + j], self._ls_flat_lats[off + j])
                       for j in range(cnt)]
            if len(coords) >= 2:
                lines.append(LineString(coords))
            else:
                # Degenerate — make a tiny line so STRtree doesn't break
                lines.append(Point(self.mid_lons[i], self.mid_lats[i]).buffer(0.00001))

        self._shapely_lines = lines

        # Build R-Tree index
        try:
            from shapely import STRtree
            self._strtree = STRtree(lines)
        except ImportError:
            from shapely.strtree import STRtree
            self._strtree = STRtree(lines)

        elapsed = time.time() - t0
        print(f"    STRtree built: {len(lines):,} geometries ({elapsed:.1f}s)")

    def _match_strtree(self, crash_lats, crash_lons, valid, threshold_ft):
        """STRtree: exact nearest geometry on real LineStrings."""
        from shapely.geometry import Point

        t0 = time.time()
        self._build_strtree()

        vi = np.where(valid)[0]
        n = len(vi)

        # Build crash points
        crash_points = [Point(crash_lons[i], crash_lats[i]) for i in vi]

        # Query nearest geometry for each crash
        ci_list, ri_list, dist_list = [], [], []

        M_FT = 111320.0 * 3.28084  # degrees to feet (approximate at equator)
        cos_lat = self.cos_lat

        # Batch: query nearest for all points
        try:
            # Shapely 2.x API: nearest() returns indices
            nearest_idx = self._strtree.nearest(crash_points)
            for j in range(n):
                ri = int(nearest_idx[j])
                d = self._dist_to_linestring(crash_lats[vi[j]], crash_lons[vi[j]], ri)
                if d <= threshold_ft:
                    ci_list.append(vi[j])
                    ri_list.append(ri)
                    dist_list.append(d)
        except (TypeError, AttributeError):
            # Shapely 1.x fallback: query one by one
            for j in range(n):
                pt = crash_points[j]
                try:
                    ri = self._strtree.nearest(pt)
                    if isinstance(ri, (list, np.ndarray)):
                        ri = int(ri[0])
                    else:
                        ri = int(ri)
                except Exception:
                    continue
                d = self._dist_to_linestring(crash_lats[vi[j]], crash_lons[vi[j]], ri)
                if d <= threshold_ft:
                    ci_list.append(vi[j])
                    ri_list.append(ri)
                    dist_list.append(d)

        elapsed = time.time() - t0
        print(f"    STRtree: {len(ci_list):,}/{n:,} matched ({elapsed:.1f}s)")
        return np.array(ci_list, dtype=int), np.array(ri_list, dtype=int), np.array(dist_list)

    # ══════════════════════════════════════════════════════════
    #  TIER 3: SciPy KDTree + linestring refinement
    # ══════════════════════════════════════════════════════════

    def _match_kdtree(self, crash_lats, crash_lons, valid, threshold_ft, k=5):
        """KDTree top-k with vectorized linestring refinement."""
        t0 = time.time()
        vi = np.where(valid)[0]
        ci, ri, d = self._refine_topk(crash_lats, crash_lons, vi, threshold_ft, k)
        elapsed = time.time() - t0
        print(f"    KDTree: {len(ci):,}/{len(vi):,} matched ({elapsed:.1f}s)")
        return ci, ri, d

    # ══════════════════════════════════════════════════════════
    #  Fallback when DuckDB spatial extension fails to load
    # ══════════════════════════════════════════════════════════

    def _fallback_match(self, crash_lats, crash_lons, valid, threshold_ft):
        """Try STRtree, then KDTree."""
        try:
            self.engine = "strtree"
            from shapely import STRtree as _s
            return self._match_strtree(crash_lats, crash_lons, valid, threshold_ft)
        except ImportError:
            pass
        try:
            from shapely.strtree import STRtree as _s2
            return self._match_strtree(crash_lats, crash_lons, valid, threshold_ft)
        except ImportError:
            pass
        if self._tree is not None:
            self.engine = "kdtree"
            return self._match_kdtree(crash_lats, crash_lons, valid, threshold_ft, 5)
        return np.array([], dtype=int), np.array([], dtype=int), np.array([])
