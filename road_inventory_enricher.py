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

        self.transfer_cols = [c for c in self.ri.columns if c not in EXCLUDE_COLUMNS]
        print(f"    {len(self.ri):,} segments x {len(self.transfer_cols)} transfer cols")

        # Initialize spatial matcher (auto-detects best engine)
        try:
            from spatial_matcher import SpatialMatcher
            self.matcher = SpatialMatcher(
                self.ri["mid_lat"].values, self.ri["mid_lon"].values,
                self.ri["u_lat"].values, self.ri["u_lon"].values,
                self.ri["v_lat"].values, self.ri["v_lon"].values,
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

        t0 = time.time()
        lons = pd.to_numeric(df.get(x_col, 0), errors="coerce").values
        lats = pd.to_numeric(df.get(y_col, 0), errors="coerce").values

        # ── Spatial matching (tiered: DuckDB → GeoPandas → KDTree) ──
        if self.matcher:
            matched_ci, matched_ri, dists = self.matcher.match(
                lats, lons, threshold_ft=self.threshold, k=self.k)
        else:
            matched_ci, matched_ri, dists = self._fallback_kdtree(lats, lons)

        if len(matched_ci) == 0:
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
                crash_vals = df.loc[matched_ci, col].astype(str).str.strip()
                ri_vals = ri_slice[col].values
                fill_mask = crash_vals.isin(["", "nan", "None", "0"]).values & \
                            np.array([bool(v.strip()) for v in ri_vals])
                if fill_mask.any():
                    df.loc[matched_ci[fill_mask], col] = ri_vals[fill_mask]

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

        # Metadata
        df["ri_matched"] = "No"
        df.loc[matched_ci, "ri_matched"] = "Yes"
        df["ri_match_dist_ft"] = ""
        df.loc[matched_ci, "ri_match_dist_ft"] = dists.astype(int).astype(str)
        df["ri_segment_id"] = ""
        df.loc[matched_ci, "ri_segment_id"] = matched_ri.astype(str)

        elapsed = time.time() - t0
        mc = len(matched_ci)
        md = int(np.median(dists)) if len(dists) > 0 else 0
        print(f"    Chunk: {mc:,}/{len(df):,} "
              f"({mc/len(df)*100:.0f}%, med={md}ft, {elapsed:.1f}s)")
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
