"""
road_inventory_enricher.py — CrashLens Road Inventory Enricher (v3)
====================================================================
Single spatial join: crash GPS → nearest road segment → transfer ALL columns.

v3: Uses spatial_matcher.py for tiered matching (DuckDB → GeoPandas → KDTree).
    Column transfer via vectorized pandas (no Python cell loops).

Architecture:
    spatial_matcher.py  → (crash_idx, road_idx, dist_ft)
    road_inventory_enricher.py → column transfer + derived columns
"""

import gzip
import io
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── Constants ──

RI_FC_TO_VDOT_FC = {
    "1-Interstate": "1-Interstate (A,1)",
    "2-Freeway/Expressway": "2-Principal Arterial - Other Freeways and Expressways (B)",
    "3-Principal Arterial": "3-Principal Arterial - Other (E,2)",
    "4-Minor Arterial": "4-Minor Arterial (H,3)",
    "5-Major Collector": "5-Major Collector (I,4)",
    "6-Minor Collector": "6-Minor Collector (I,5)",
    "7-Local": "7-Local (J,6)",
}
FC_TRANSLATE_COLUMNS = {"Functional Class"}

EXCLUDE_COLUMNS = {
    "mid_lat", "mid_lon", "u_lat", "u_lon", "v_lat", "v_lon",
    "u_node", "v_node", "road_source",
    "highway", "oneway", "lanes", "maxspeed", "surface", "name", "ref",
    "bridge", "tunnel", "lit", "sidewalk", "cycleway",
    "access", "service", "junction",
}

OVERWRITE_COLUMNS = {
    "Functional Class", "Ownership", "SYSTEM", "Facility Type",
    "Roadway Surface Type", "Area Type",
    "DOT District", "Physical Juris Name", "Planning District", "MPO Name",
    "Juris Code",
}

FILL_COLUMNS = {
    "Traffic Control Type", "Intersection Type", "RTE Name",
    "Max Speed Diff", "Through_Lanes", "School Zone",
    "Roadway Alignment", "Roadway Surface Condition",
}


def _is_empty(val):
    if val is None:
        return True
    s = str(val).strip()
    return s in ("", "nan", "None", "0", "0.0")


class RoadInventorySession:
    """Load road inventory ONCE, match many chunks via best available spatial engine.

    Tier 1: DuckDB Spatial  — ~5 seconds for 566K crashes
    Tier 2: GeoPandas       — ~30 seconds
    Tier 3: SciPy KDTree    — ~5 minutes

    Usage:
        session = RoadInventorySession("de", "cache")
        if session.ready:
            chunk = session.enrich(chunk)
    """

    def __init__(self, state_abbr, cache_dir="cache",
                 match_threshold_ft=328, k_candidates=5):
        self.state_abbr = state_abbr.lower()
        self.cache_dir = Path(cache_dir)
        self.threshold = match_threshold_ft
        self.k = k_candidates
        self.ready = False
        self.ri = None
        self.matcher = None
        self.transfer_cols = []
        self._load()

    def _load(self):
        abbr = self.state_abbr
        ri_path = self.cache_dir / f"{abbr}_road_inventory.parquet.gz"
        if not ri_path.exists():
            ri_path = self.cache_dir / f"{abbr}_road_inventory.parquet"
        if not ri_path.exists():
            print(f"\n  [Road Inventory] Not found: {ri_path}")
            return

        t0 = time.time()
        print(f"\n  [Road Inventory Session] Loading {ri_path.name}...")
        if str(ri_path).endswith(".gz"):
            with gzip.open(ri_path, "rb") as f:
                self.ri = pd.read_parquet(io.BytesIO(f.read()))
        else:
            self.ri = pd.read_parquet(ri_path)

        # Proximity threshold cleanup — dist_ft=0 when beyond range, blank detail cols only
        _ASSET_THRESHOLDS = {
            "nearest_bridge_": 500, "nearest_rail_xing_": 500,
            "nearest_school_": 2000, "nearest_transit_": 500,
            "nearest_poi_bar_": 2000, "nearest_poi_clinic_": 2000,
            "nearest_poi_college_": 2000, "nearest_poi_crossing_": 500,
            "nearest_poi_fuel_": 1000, "nearest_poi_hospital_": 2000,
            "nearest_poi_parking_": 500, "nearest_poi_rest_area_": 1000,
            "nearest_poi_restaurant_": 1000, "nearest_poi_signal_": 500,
            "nearest_poi_stop_sign_": 500,
        }
        cleaned = 0
        for prefix, threshold in _ASSET_THRESHOLDS.items():
            dist_col = f"{prefix}dist_ft"
            if dist_col not in self.ri.columns:
                continue
            dists = pd.to_numeric(self.ri[dist_col], errors="coerce").fillna(0)
            beyond = dists > threshold
            if beyond.any():
                # dist_ft → 0 (keeps column, signals "none within range")
                self.ri.loc[beyond, dist_col] = 0
                # Blank detail cols only (name, id, lat, lon, etc.)
                detail_cols = [c for c in self.ri.columns
                               if c.startswith(prefix) and c != dist_col]
                for col in detail_cols:
                    dt = self.ri[col].dtype
                    if dt in (np.float32, np.float64, np.int32, np.int64, float, int):
                        self.ri.loc[beyond, col] = 0
                    else:
                        self.ri.loc[beyond, col] = None
                cleaned += beyond.sum()

        # Drop columns that add no analytical value
        _DROP_COLS = set()

        # Duplicate geography (geo_ repeats frontend columns)
        _DROP_COLS.update([
            "geo_area_type", "geo_county_basename", "geo_county_fips",
            "geo_county_name", "geo_dot_region", "geo_dot_region_id",
            "geo_juris_code", "geo_mpo_id", "geo_mpo_name",
            "geo_planning_district", "geo_planning_district_id",
        ])

        # HPMS raw duplicates (already resolved into frontend columns)
        _DROP_COLS.update([
            "hpms_aadt", "hpms_f_system", "hpms_facility_type",
            "hpms_match_dist_ft", "hpms_matched", "hpms_ownership",
            "hpms_speed_limit", "hpms_surface_type", "hpms_through_lanes",
        ])

        # Single-value subcategory columns (always same value → no signal)
        for c in self.ri.columns:
            if c.endswith("_subcategory") and c.startswith("nearest_"):
                vals = self.ri[c].dropna().unique()
                non_empty = [v for v in vals if str(v).strip() not in ("", "nan", "None")]
                if len(non_empty) <= 1:
                    _DROP_COLS.add(c)

        # Drop columns that exist in the DataFrame
        actual_drop = [c for c in _DROP_COLS if c in self.ri.columns]
        if actual_drop:
            self.ri.drop(columns=actual_drop, inplace=True)

        self.transfer_cols = [c for c in self.ri.columns if c not in EXCLUDE_COLUMNS]
        msg = f"    {len(self.ri):,} segments x {len(self.transfer_cols)} transfer cols"
        if cleaned:
            msg += f" (proximity: {cleaned:,} → 0ft)"
        if actual_drop:
            msg += f" (dropped {len(actual_drop)} cols)"
        print(msg)

        # Data quality validation + auto-fix
        try:
            from road_inventory_validator import validate_and_fix
            report = validate_and_fix(self.ri, verbose=True)
            # Refresh transfer_cols after validator may have modified data
            self.transfer_cols = [c for c in self.ri.columns if c not in EXCLUDE_COLUMNS]
        except ImportError:
            pass  # Validator not available — skip silently

        # Initialize spatial matcher (auto-detects best engine)
        try:
            from spatial_matcher import SpatialMatcher
            geo_coords = self.ri.get("geometry_coords")
            self.matcher = SpatialMatcher(
                self.ri["mid_lat"].values, self.ri["mid_lon"].values,
                self.ri["u_lat"].values, self.ri["u_lon"].values,
                self.ri["v_lat"].values, self.ri["v_lon"].values,
                geometry_coords=geo_coords,
            )
        except ImportError:
            print("    spatial_matcher.py not found — using inline KDTree")
            self.matcher = None

        elapsed = time.time() - t0
        print(f"    Session ready in {elapsed:.1f}s")
        self.ready = True

    def enrich(self, df, x_col="x", y_col="y"):
        """Enrich crash chunk: spatial match + vectorized column transfer."""
        if not self.ready:
            return df

        # Reset index to 0-based so matched_ci (positional) aligns with df.loc[]
        orig_index = df.index
        df = df.reset_index(drop=True)

        t0 = time.time()
        lons = pd.to_numeric(df.get(x_col, 0), errors="coerce").values
        lats = pd.to_numeric(df.get(y_col, 0), errors="coerce").values

        # ── Spatial matching (tiered: DuckDB+STRtree → STRtree → KDTree) ──
        confidence = None
        match_method = None
        if self.matcher:
            result = self.matcher.match(
                lats, lons, threshold_ft=self.threshold, k=self.k)
            if len(result) == 5:
                matched_ci, matched_ri, dists, confidence, match_method = result
            elif len(result) == 4:
                matched_ci, matched_ri, dists, confidence = result
            else:
                matched_ci, matched_ri, dists = result
        else:
            matched_ci, matched_ri, dists = self._fallback_kdtree(lats, lons)

        if len(matched_ci) == 0:
            df.index = orig_index
            return df

        # ── Vectorized column transfer ──
        ri_slice = self.ri.iloc[matched_ri][self.transfer_cols].copy()
        ri_slice.index = matched_ci

        for col in ri_slice.columns:
            ri_slice[col] = ri_slice[col].astype(str).replace(
                {"nan": "", "None": "", "none": "", "<NA>": ""})

        for col in FC_TRANSLATE_COLUMNS:
            if col in ri_slice.columns:
                ri_slice[col] = ri_slice[col].map(
                    lambda v: RI_FC_TO_VDOT_FC.get(v, v))

        # OVERWRITE
        for col in OVERWRITE_COLUMNS:
            if col in ri_slice.columns:
                if col not in df.columns:
                    df[col] = ""
                vals = ri_slice[col].values
                non_empty = np.array([bool(v.strip()) for v in vals])
                if non_empty.any():
                    df.loc[matched_ci[non_empty], col] = vals[non_empty]

        # FILL
        for col in FILL_COLUMNS:
            if col in ri_slice.columns:
                if col not in df.columns:
                    df[col] = ""
                crash_vals = df.loc[matched_ci, col].fillna("").astype(str).str.strip()
                ri_vals = ri_slice[col].values
                fill_mask = crash_vals.isin(["", "nan", "None", "0"]).values & \
                            np.array([bool(v.strip()) for v in ri_vals])
                if fill_mask.any():
                    df.loc[matched_ci[fill_mask], col] = ri_vals[fill_mask]

        # ── Intersection Type from road inventory node proximity ──
        # (replaces GPS clustering — uses actual OSM intersection nodes)
        #
        # OSM intersection_degree counts DIRECTED edges from a MultiDiGraph.
        # Each two-way road contributes 2 edges per node, so a 3-road
        # T-intersection has degree 6, not 3. The MIRE-correct metric is
        # streets_per_node (undirected physical street count): spn>=3 is a
        # real intersection. When streets_per_node is present on the road
        # inventory slice (Phase 2), prefer it; otherwise fall back to the
        # directed-degree mapping (deg>=6 ≈ T-intersection).
        if "intersection_degree" in self.ri.columns:
            ri_slice = self.ri.iloc[matched_ri]
            seg_int_deg = pd.to_numeric(
                ri_slice["intersection_degree"], errors="coerce"
            ).fillna(0).astype(int).values
            has_spn = "streets_per_node" in ri_slice.columns
            if has_spn:
                seg_spn = pd.to_numeric(
                    ri_slice["streets_per_node"], errors="coerce"
                ).fillna(0).astype(int).values
            else:
                seg_spn = None
            seg_u_lat = ri_slice["u_lat"].values.astype(float)
            seg_u_lon = ri_slice["u_lon"].values.astype(float)
            seg_v_lat = ri_slice["v_lat"].values.astype(float)
            seg_v_lon = ri_slice["v_lon"].values.astype(float)

            c_lat = lats[matched_ci]
            c_lon = lons[matched_ci]

            # Longitude scale factor for approximate distance
            valid_lats = c_lat[c_lat != 0]
            mean_lat = np.nanmean(valid_lats) if len(valid_lats) > 0 else 39.0
            lon_scale = np.cos(np.radians(mean_lat))

            # Squared distance to u-node and v-node (in meters)
            d_u_sq = ((c_lat - seg_u_lat) * 111000)**2 + \
                     ((c_lon - seg_u_lon) * 111000 * lon_scale)**2
            d_v_sq = ((c_lat - seg_v_lat) * 111000)**2 + \
                     ((c_lon - seg_v_lon) * 111000 * lon_scale)**2
            min_dist_sq = np.minimum(d_u_sq, d_v_sq)

            # At intersection = real intersection node within 30m.
            # streets_per_node >= 3 (preferred) or directed degree >= 6 (fallback).
            if has_spn:
                node_is_real = seg_spn >= 3
            else:
                node_is_real = seg_int_deg >= 6
            at_intersection = node_is_real & (min_dist_sq <= 900)

            # Only fill empty Intersection Type
            if "Intersection Type" not in df.columns:
                df["Intersection Type"] = ""
            crash_it = df.loc[matched_ci, "Intersection Type"] \
                .fillna("").astype(str).str.strip()
            needs_fill = crash_it.isin(["", "nan", "None", "Not Applicable"]).values

            # Map degree/spn → Intersection Type string. Prefer spn (direct
            # physical street count) when available; otherwise use directed
            # degree with ~2:1 ratio for two-way streets.
            if has_spn:
                int_type_arr = np.where(
                    seg_spn >= 5, "5. Five-Point, or More",
                    np.where(seg_spn >= 4, "4. Four Approaches",
                    np.where(seg_spn >= 3, "3. Three Approaches",
                             "1. Not at Intersection")))
            else:
                int_type_arr = np.where(
                    seg_int_deg >= 10, "5. Five-Point, or More",
                    np.where(seg_int_deg >= 8, "4. Four Approaches",
                    np.where(seg_int_deg >= 6, "3. Three Approaches",
                             "1. Not at Intersection")))

            fill_as_int = needs_fill & at_intersection
            fill_as_not = needs_fill & ~at_intersection

            if fill_as_int.any():
                df.loc[matched_ci[fill_as_int], "Intersection Type"] = \
                    int_type_arr[fill_as_int]
            if fill_as_not.any():
                df.loc[matched_ci[fill_as_not], "Intersection Type"] = \
                    "1. Not at Intersection"

            int_count = fill_as_int.sum()
            not_count = fill_as_not.sum()
            total_matched = len(matched_ci)
            if int_count or not_count:
                print(f"    Intersection Type (node proximity): "
                      f"{int_count:,} at intersection "
                      f"({int_count/total_matched*100:.1f}%), "
                      f"{not_count:,} not at intersection")

        # NEW columns (bulk assign)
        new_cols = [c for c in self.transfer_cols
                    if c not in OVERWRITE_COLUMNS and c not in FILL_COLUMNS]
        if new_cols:
            for col in new_cols:
                if col not in df.columns:
                    df[col] = ""
            df.loc[matched_ci, new_cols] = ri_slice[new_cols].values

        # ── Derived columns ──
        mm = np.zeros(len(df), dtype=bool)
        mm[matched_ci] = True

        if "Intersection Type" in df.columns and "Ownership" in df.columns:
            if "Intersection Analysis" not in df.columns:
                df["Intersection Analysis"] = ""
            it = df["Intersection Type"].astype(str)
            ow = df["Ownership"].astype(str)
            not_int = it.str.contains("Not at Intersection", na=False)
            state = ow.str.contains("State", na=False)
            ia = df["Intersection Analysis"].copy()
            ia[mm & not_int] = "Not Intersection"
            ia[mm & ~not_int & state] = "DOT Intersection"
            ia[mm & ~not_int & ~state] = "Urban Intersection"
            df["Intersection Analysis"] = ia

        if "Functional Class" in df.columns:
            if "Mainline?" not in df.columns:
                df["Mainline?"] = ""
            fc = df["Functional Class"].astype(str)
            mfcs = {"1-Interstate (A,1)",
                    "2-Principal Arterial - Other Freeways and Expressways (B)",
                    "3-Principal Arterial - Other (E,2)"}
            df.loc[mm, "Mainline?"] = np.where(fc[mm].isin(mfcs), "Yes", "No")

        if "Facility Type" in df.columns:
            if "Roadway Description" not in df.columns:
                df["Roadway Description"] = ""
            dm = {"1-One-Way Undivided": "4. One-Way, Not Divided",
                  "2-One-Way Divided": "4. One-Way, Not Divided",
                  "3-Two-Way Undivided": "1. Two-Way, Not Divided",
                  "4-Two-Way Divided": "2. Two-Way, Divided, Unprotected Median"}
            df.loc[mm, "Roadway Description"] = \
                df.loc[mm, "Facility Type"].astype(str).map(dm).fillna("")

        # ── Ramp derivation (from is_ramp/ramp_type in road inventory) ──
        if "is_ramp" in df.columns:
            on_ramp = mm & (df["is_ramp"].astype(str).str.strip() == "Yes")

            if on_ramp.any():
                # Roadway Alignment → "10. On/Off Ramp" (Tier C fill)
                if "Roadway Alignment" not in df.columns:
                    df["Roadway Alignment"] = ""
                ra = df["Roadway Alignment"].fillna("").astype(str).str.strip()
                ra_empty = on_ramp & ra.isin(["", "nan", "None"])
                if ra_empty.any():
                    df.loc[ra_empty, "Roadway Alignment"] = "10. On/Off Ramp"

                # Relation To Roadway → ramp-specific values (Tier C fill)
                if "Relation To Roadway" not in df.columns:
                    df["Relation To Roadway"] = ""
                rtr = df["Relation To Roadway"].fillna("").astype(str).str.strip()
                rtr_empty = on_ramp & rtr.isin(["", "nan", "None", "Not Provided",
                                                 "Not Applicable"])
                if rtr_empty.any() and "ramp_type" in df.columns:
                    rt_map = {"Exit": "5. On Entrance/Exit Ramp",
                              "Entrance": "5. On Entrance/Exit Ramp",
                              "Connector": "3. Gore Area (b/w Ramp and Highway Edgelines)",
                              "Ramp": "5. On Entrance/Exit Ramp"}
                    rt = df.loc[rtr_empty, "ramp_type"].astype(str).str.strip()
                    mapped = rt.map(rt_map)
                    has_val = mapped.notna() & (mapped != "")
                    if has_val.any():
                        df.loc[has_val[has_val].index, "Relation To Roadway"] = \
                            mapped[has_val].values

        # Metadata
        df["ri_matched"] = "No"
        df.loc[matched_ci, "ri_matched"] = "Yes"
        df["ri_match_dist_ft"] = ""
        df.loc[matched_ci, "ri_match_dist_ft"] = dists.astype(int).astype(str)
        df["ri_confidence"] = ""
        if confidence is not None and len(confidence) > 0:
            df.loc[matched_ci, "ri_confidence"] = confidence
        else:
            # Derive from distance if matcher didn't provide
            conf = np.where(dists <= 100, "high",
                   np.where(dists <= 200, "medium", "low"))
            df.loc[matched_ci, "ri_confidence"] = conf
        df["ri_segment_id"] = ""
        df.loc[matched_ci, "ri_segment_id"] = matched_ri.astype(str)
        df["ri_match_method"] = ""
        if match_method is not None and len(match_method) > 0:
            df.loc[matched_ci, "ri_match_method"] = match_method

        elapsed = time.time() - t0
        mc = len(matched_ci)
        md = int(np.median(dists)) if len(dists) > 0 else 0
        print(f"    Chunk: {mc:,}/{len(df):,} "
              f"({mc/len(df)*100:.0f}%, med={md}ft, {elapsed:.1f}s)")
        df.index = orig_index
        return df

    def _fallback_kdtree(self, crash_lats, crash_lons):
        """Inline KDTree fallback if spatial_matcher.py missing."""
        from spatial_matcher import _validate_gps, _point_to_segment_dist_ft
        from scipy.spatial import cKDTree

        valid = _validate_gps(crash_lats, crash_lons)
        vi = np.where(valid)[0]
        if len(vi) == 0:
            return np.array([],dtype=int), np.array([],dtype=int), np.array([])

        M = 111320.0
        cos_lat = np.cos(np.radians(np.mean(crash_lats[vi])))
        ri_xy = np.column_stack([self.ri["mid_lat"].values*M, self.ri["mid_lon"].values*M*cos_lat])
        tree = cKDTree(ri_xy)
        cxy = np.column_stack([crash_lats[vi]*M, crash_lons[vi]*M*cos_lat])
        _, cand = tree.query(cxy, k=self.k)

        best_seg = np.zeros(len(vi), dtype=np.int64)
        best_dist = np.full(len(vi), np.inf)
        ul, ulo = self.ri["u_lat"].values, self.ri["u_lon"].values
        vl, vlo = self.ri["v_lat"].values, self.ri["v_lon"].values

        for ki in range(self.k):
            si = cand[:, ki]
            for i in range(len(vi)):
                d = _point_to_segment_dist_ft(
                    crash_lats[vi[i]], crash_lons[vi[i]],
                    ul[si[i]], ulo[si[i]], vl[si[i]], vlo[si[i]])
                if d < best_dist[i]:
                    best_dist[i] = d
                    best_seg[i] = si[i]

        within = best_dist <= self.threshold
        return vi[within], best_seg[within], best_dist[within]


def enrich_from_road_inventory(df, state_abbr, cache_dir="cache",
                                match_threshold_ft=328, k_candidates=5,
                                x_col="x", y_col="y"):
    """One-shot enrichment. Use RoadInventorySession for batches."""
    session = RoadInventorySession(state_abbr, cache_dir, match_threshold_ft, k_candidates)
    if not session.ready:
        return df
    return session.enrich(df, x_col, y_col)
