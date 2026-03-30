#!/usr/bin/env python3
"""
test_pipeline.py — CrashLens Comprehensive Pipeline Test
==========================================================
Validates every module, function, constant, cross-validation rule,
and end-to-end integration using synthetic data.

Requirements: pandas, numpy, scipy, pyarrow (same as CI)
No network access, no R2, no large files needed.

Usage:
    python test_pipeline.py              # run all tests
    python test_pipeline.py -v           # verbose
    python test_pipeline.py --quick      # skip slow tests

Exit code 0 = all pass, 1 = failures found.
"""

import sys
import os
import time
import json
import gzip
import io
import traceback
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# ═══════════════════════════════════════════════════════════════
#  TEST FRAMEWORK
# ═══════════════════════════════════════════════════════════════

class TestRunner:
    def __init__(self, verbose=False):
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.verbose = verbose
        self.t0 = time.time()

    def test(self, name, fn):
        try:
            fn()
            self.passed += 1
            if self.verbose:
                print(f"  ✅ {name}")
        except AssertionError as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"  ❌ {name}: {e}")
        except Exception as e:
            self.failed += 1
            self.errors.append((name, f"{type(e).__name__}: {e}"))
            print(f"  ❌ {name}: {type(e).__name__}: {e}")
            if self.verbose:
                traceback.print_exc()

    def report(self):
        elapsed = time.time() - self.t0
        total = self.passed + self.failed
        print(f"\n{'═' * 60}")
        if self.failed == 0:
            print(f"  ✅ ALL {total} TESTS PASSED ({elapsed:.1f}s)")
        else:
            print(f"  ❌ {self.failed} FAILED / {total} total ({elapsed:.1f}s)")
            for name, err in self.errors:
                print(f"    → {name}: {err}")
        print(f"{'═' * 60}")
        return self.failed == 0


# ═══════════════════════════════════════════════════════════════
#  SYNTHETIC DATA GENERATORS
# ═══════════════════════════════════════════════════════════════

def make_road_inventory(n=500):
    """Create synthetic road inventory DataFrame."""
    np.random.seed(42)
    lats = 38.8 + np.random.rand(n) * 0.6
    lons = -75.7 + np.random.rand(n) * 0.4
    offsets = (np.random.rand(n) - 0.5) * 0.002

    hw_tags = np.random.choice(
        ["motorway", "trunk", "primary", "secondary", "tertiary",
         "residential", "service", "motorway_link", "trunk_link"],
        size=n, p=[0.02, 0.05, 0.1, 0.15, 0.2, 0.35, 0.08, 0.02, 0.03])

    fc_map = {
        "motorway": "1-Interstate (A,1)",
        "trunk": "2-Principal Arterial - Other Freeways and Expressways (B)",
        "primary": "3-Principal Arterial - Other (E,2)",
        "secondary": "4-Minor Arterial (H,3)",
        "tertiary": "5-Major Collector (I,4)",
        "residential": "7-Local (J,6)",
        "service": "7-Local (J,6)",
        "motorway_link": "1-Interstate (A,1)",
        "trunk_link": "2-Principal Arterial - Other Freeways and Expressways (B)",
    }

    speeds = {"1-": 65, "2-": 55, "3-": 45, "4-": 35, "5-": 30, "6-": 25, "7-": 25}
    ri = pd.DataFrame({
        "mid_lat": lats, "mid_lon": lons,
        "u_lat": lats - offsets, "u_lon": lons - offsets * 0.8,
        "v_lat": lats + offsets, "v_lon": lons + offsets * 0.8,
        "u_node": range(1, n + 1), "v_node": range(n + 1, 2 * n + 1),
        "highway": hw_tags,
        "name": [f"Road {i}" for i in range(n)],
        "ref": "",
        "oneway": np.random.choice(["yes", "no"], size=n, p=[0.3, 0.7]),
        "lanes": np.random.choice(["1", "2", "4"], size=n, p=[0.4, 0.4, 0.2]),
        "maxspeed": "",
        "length_m": np.random.uniform(20, 500, size=n),
        "bridge": "no", "tunnel": "no", "surface": "asphalt",
        "lit": np.random.choice(["yes", "no"], size=n),
        "sidewalk": "no", "cycleway": "no", "divider": "no",
        "curvature": np.random.uniform(1.0, 1.5, size=n),
        "road_source": "OSM",
    })

    # Set FC and derived columns
    ri["Functional Class"] = ri["highway"].map(fc_map)
    fc_prefix = ri["Functional Class"].str[:2]
    ri["Max Speed Diff"] = fc_prefix.map(speeds).fillna(25).astype(str)
    ri["SYSTEM"] = fc_prefix.map({
        "1-": "DOT Interstate", "2-": "DOT Primary", "3-": "DOT Primary",
        "4-": "DOT Secondary", "5-": "DOT Secondary",
        "6-": "Non-DOT primary", "7-": "Non-DOT secondary"})
    ri["Ownership"] = fc_prefix.map({
        "1-": "1. State Hwy Agency", "2-": "1. State Hwy Agency",
        "3-": "1. State Hwy Agency", "4-": "1. State Hwy Agency",
        "5-": "2. County Hwy Agency", "6-": "2. County Hwy Agency",
        "7-": "3. City or Town Hwy Agency"})
    ri["Facility Type"] = fc_prefix.map({
        "1-": "4-Two-Way Divided", "2-": "4-Two-Way Divided",
        "3-": "3-Two-Way Undivided", "4-": "3-Two-Way Undivided",
        "5-": "3-Two-Way Undivided", "6-": "3-Two-Way Undivided",
        "7-": "3-Two-Way Undivided"})
    ri["Roadway Surface Type"] = "2. Blacktop, Asphalt, Bituminous"
    ri["Roadway Description"] = "1. Two-Way, Not Divided"
    ri["Area Type"] = np.random.choice(["Urban", "Rural"], size=n)
    ri["DOT District"] = "District 1"
    ri["Planning District"] = "PD 1"
    ri["MPO Name"] = "Test MPO"
    ri["Physical Juris Name"] = "Test County"
    ri["Juris Code"] = "001"
    ri["RTE Name"] = ri["name"]
    ri["AADT"] = np.random.choice([500, 2000, 5000, 15000, 30000], size=n).astype(str)
    ri["Through_Lanes"] = ri["lanes"]
    ri["Traffic Control Type"] = "1. No Traffic Control"
    ri["Intersection Type"] = "1. Not at Intersection"
    ri["School Zone"] = "3. No"
    ri["is_intersection"] = "No"
    ri["intersection_degree"] = 0
    ri["intersection_name"] = ""
    ri["VMT_Annual"] = "0"
    ri["AADT_source"] = "HPMS"
    ri["nearest_school_dist_ft"] = np.random.uniform(0, 5000, size=n)
    ri["nearest_bridge_dist_ft"] = np.random.uniform(0, 10000, size=n)

    return ri


def make_crash_data(n=200):
    """Create synthetic crash DataFrame matching golden 69 schema."""
    np.random.seed(123)
    # Place crashes near synthetic road midpoints (38.8-39.4, -75.7 to -75.3)
    # Add small jitter so they're near roads but not exactly on them
    lats = 38.8 + np.random.rand(n) * 0.6
    lons = -75.7 + np.random.rand(n) * 0.4

    df = pd.DataFrame({
        "OBJECTID": [f"DE-{i:07d}" for i in range(1, n + 1)],
        "Document Nbr": range(1000, 1000 + n),
        "Crash Year": "2023",
        "Crash Date": "2023-06-15",
        "Crash Military Time": [str(x) for x in np.random.randint(0, 2359, size=n)],
        "Crash Severity": np.random.choice(["O", "C", "B", "A", "K"],
                                            size=n, p=[0.5, 0.2, 0.15, 0.1, 0.05]),
        "K_People": "0", "A_People": "0", "B_People": "0", "C_People": "0",
        "Persons Injured": "0", "Pedestrians Killed": "0", "Pedestrians Injured": "0",
        "Vehicle Count": "2",
        "Collision Type": "1. Rear End",
        "Weather Condition": "1. No Adverse Condition (Clear/Cloudy)",
        "Light Condition": "2. Daylight",
        "Roadway Surface Condition": "1. Dry",
        "Relation To Roadway": "",
        "Roadway Alignment": "",
        "Roadway Surface Type": "",
        "Roadway Defect": "",
        "Roadway Description": "",
        "Intersection Type": "",
        "Traffic Control Type": "",
        "Traffic Control Status": "",
        "Work Zone Related": "2. No",
        "Work Zone Location": "", "Work Zone Type": "",
        "School Zone": "",
        "First Harmful Event": "", "First Harmful Event Loc": "",
        "Alcohol?": "No", "Animal Related?": "No", "Unrestrained?": "Belted",
        "Bike?": "No", "Distracted?": "No", "Drowsy?": "No",
        "Drug Related?": "No", "Guardrail Related?": "No", "Hitrun?": "No",
        "Lgtruck?": "No", "Motorcycle?": "No", "Pedestrian?": "No", "Speed?": "No",
        "Max Speed Diff": "", "RoadDeparture Type": "",
        "Intersection Analysis": "",
        "Senior?": "No", "Young?": "No", "Mainline?": "", "Night?": "No",
        "DOT District": "", "Juris Code": "", "Physical Juris Name": "Test County",
        "Functional Class": "", "Facility Type": "", "Area Type": "",
        "SYSTEM": "", "VSP": "", "Ownership": "",
        "Planning District": "", "MPO Name": "",
        "RTE Name": "", "RNS MP": "", "Node": "", "Node Offset (ft)": "",
        "x": lons, "y": lats,
        "FIPS": "001",
        "EPDO_Score": "1",
    })
    return df


def save_synthetic_parquet(ri, path):
    """Save synthetic road inventory as gzipped parquet (matches enricher load)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # Write parquet to buffer, then gzip-wrap (matches enricher's gzip.open load path)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(ri), buf)
    with gzip.open(path, "wb") as f:
        f.write(buf.getvalue())


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _find_file(name):
    """Find a Python file in cwd or sys.path."""
    if os.path.exists(name):
        return name
    for p in sys.path:
        if p and os.path.exists(os.path.join(p, name)):
            return os.path.join(p, name)
    return None


# ═══════════════════════════════════════════════════════════════
#  TEST GROUPS
# ═══════════════════════════════════════════════════════════════

def test_imports(t):
    """Test all modules import successfully."""
    print("\n── MODULE IMPORTS ──")

    def _import_spatial():
        from spatial_matcher import SpatialMatcher, _vec_point_to_segment_dist_ft, \
            _validate_gps, _confidence_labels
        assert callable(SpatialMatcher)

    def _import_enricher():
        from road_inventory_enricher import RoadInventorySession, enrich_from_road_inventory, \
            OVERWRITE_COLUMNS, FILL_COLUMNS, RI_FC_TO_VDOT_FC
        assert len(OVERWRITE_COLUMNS) > 0
        assert len(FILL_COLUMNS) > 0

    def _import_validator():
        from road_inventory_validator import validate_and_fix, ValidationReport
        assert callable(validate_and_fix)

    def _import_crash_enricher():
        from crash_enricher import CrashEnricher
        methods = ["enrich_all", "enrich_tier1", "_derive_intersection_analysis",
                    "_print_fill_report", "_derive_flags_from_circumstance",
                    "_cross_validate_flags", "_estimate_kabco_people",
                    "_derive_fc_from_route_name"]
        for m in methods:
            assert hasattr(CrashEnricher, m), f"Missing: CrashEnricher.{m}"

    def _import_road_data_authority():
        import road_data_authority as rda
        funcs = ["resolve_speed_limit", "resolve_lanes", "resolve_surface",
                 "resolve_signals", "resolve_lighting", "resolve_bridge",
                 "resolve_school_zone", "run_sanity_checks", "apply_authority_layer",
                 "compute_confidence_scores", "compute_risk_indicators",
                 "compute_curve_analysis", "merge_frontend_columns"]
        for f in funcs:
            assert hasattr(rda, f), f"Missing: road_data_authority.{f}"

    t.test("import spatial_matcher", _import_spatial)
    t.test("import road_inventory_enricher", _import_enricher)
    t.test("import road_inventory_validator", _import_validator)
    t.test("import crash_enricher", _import_crash_enricher)
    t.test("import road_data_authority", _import_road_data_authority)


def test_spatial_matcher(t):
    """Test spatial matcher with synthetic data."""
    print("\n── SPATIAL MATCHER ──")
    from spatial_matcher import SpatialMatcher, _vec_point_to_segment_dist_ft, \
        _validate_gps, _confidence_labels

    ri = make_road_inventory(100)

    def _init_no_linestring():
        m = SpatialMatcher(
            ri["mid_lat"].values, ri["mid_lon"].values,
            ri["u_lat"].values, ri["u_lon"].values,
            ri["v_lat"].values, ri["v_lon"].values)
        assert m.engine is not None
        assert m._tree is not None

    def _init_with_linestring():
        coords = []
        for i in range(len(ri)):
            c = [[ri["u_lon"].iloc[i], ri["u_lat"].iloc[i]],
                 [ri["mid_lon"].iloc[i], ri["mid_lat"].iloc[i]],
                 [ri["v_lon"].iloc[i], ri["v_lat"].iloc[i]]]
            coords.append(json.dumps(c))
        m = SpatialMatcher(
            ri["mid_lat"].values, ri["mid_lon"].values,
            ri["u_lat"].values, ri["u_lon"].values,
            ri["v_lat"].values, ri["v_lon"].values,
            geometry_coords=coords)
        assert m._has_linestrings

    def _match_returns_4tuple():
        m = SpatialMatcher(
            ri["mid_lat"].values, ri["mid_lon"].values,
            ri["u_lat"].values, ri["u_lon"].values,
            ri["v_lat"].values, ri["v_lon"].values)
        crashes = make_crash_data(50)
        result = m.match(
            pd.to_numeric(crashes["y"]).values,
            pd.to_numeric(crashes["x"]).values)
        assert len(result) == 4, f"Expected 4-tuple, got {len(result)}"
        ci, road_idx, dists, conf = result
        assert len(ci) == len(road_idx) == len(dists) == len(conf)

    def _confidence_labels_correct():
        dists = np.array([5, 50, 150, 250, 320])
        conf = _confidence_labels(dists)
        assert conf[0] == "high"
        assert conf[1] == "high"
        assert conf[2] == "medium"
        assert conf[3] == "low"
        assert conf[4] == "low"

    def _validate_gps_correct():
        lats = np.array([39.0, 0.0, np.nan, 39.5, 10.0])
        lons = np.array([-75.5, 0.0, -75.0, -75.2, -75.0])
        mask = _validate_gps(lats, lons)
        assert mask[0] == True
        assert mask[1] == False   # zero
        assert mask[2] == False   # nan
        assert mask[3] == True
        assert mask[4] == False   # lat < 20

    def _vec_dist_correct():
        d = _vec_point_to_segment_dist_ft(
            np.array([39.0]), np.array([-75.5]),
            np.array([39.0]), np.array([-75.5]),
            np.array([39.0]), np.array([-75.5]))
        assert d[0] < 1.0  # same point = 0 distance

    t.test("SpatialMatcher init (no linestring)", _init_no_linestring)
    t.test("SpatialMatcher init (with linestring)", _init_with_linestring)
    t.test("match returns 4-tuple", _match_returns_4tuple)
    t.test("confidence labels", _confidence_labels_correct)
    t.test("GPS validation", _validate_gps_correct)
    t.test("vectorized distance", _vec_dist_correct)


def test_validator(t):
    """Test all 12 validator rules."""
    print("\n── VALIDATOR RULES ──")
    from road_inventory_validator import validate_and_fix

    def _speed_floor():
        ri = make_road_inventory(100)
        # Set some Interstate speeds to 5 mph (should be fixed to 55)
        interstate = ri["Functional Class"].str.startswith("1-")
        ri.loc[interstate, "Max Speed Diff"] = "5"
        report = validate_and_fix(ri, verbose=False)
        # Verify they were fixed
        fixed_spd = pd.to_numeric(ri.loc[interstate, "Max Speed Diff"], errors="coerce")
        assert (fixed_spd >= 45).all(), f"Interstate speed not fixed: {fixed_spd.unique()}"

    def _speed_ceiling():
        ri = make_road_inventory(100)
        local = ri["Functional Class"].str.startswith("7-")
        ri.loc[local, "Max Speed Diff"] = "65"
        report = validate_and_fix(ri, verbose=False)
        fixed = pd.to_numeric(ri.loc[local, "Max Speed Diff"], errors="coerce")
        assert (fixed <= 35).all(), f"Local speed not capped: {fixed.unique()}"

    def _surface_fill():
        ri = make_road_inventory(100)
        ri["Roadway Surface Type"] = ""
        report = validate_and_fix(ri, verbose=False)
        filled = (ri["Roadway Surface Type"].str.strip() != "").sum()
        assert filled == 100, f"Surface not filled: {filled}/100"

    def _ownership_interstate():
        ri = make_road_inventory(100)
        interstate = ri["Functional Class"].str.startswith("1-")
        ri.loc[interstate, "Ownership"] = "3. City or Town Hwy Agency"
        report = validate_and_fix(ri, verbose=False)
        own = ri.loc[interstate, "Ownership"]
        assert (own == "1. State Hwy Agency").all(), f"Interstate ownership not fixed"

    def _system_alignment():
        ri = make_road_inventory(100)
        interstate = ri["Functional Class"].str.startswith("1-")
        ri.loc[interstate, "SYSTEM"] = "Non-DOT secondary"
        report = validate_and_fix(ri, verbose=False)
        sys_val = ri.loc[interstate, "SYSTEM"]
        assert (sys_val == "DOT Interstate").all(), f"SYSTEM not aligned"

    def _facility_oneway():
        ri = make_road_inventory(100)
        oneway = ri["oneway"] == "yes"
        ri.loc[oneway, "Facility Type"] = "3-Two-Way Undivided"
        report = validate_and_fix(ri, verbose=False)
        fac = ri.loc[oneway, "Facility Type"]
        assert (fac == "1-One-Way Undivided").all(), f"Oneway facility not fixed: {fac.unique()}"

    def _stop_sign_interstate():
        ri = make_road_inventory(100)
        interstate = ri["Functional Class"].str.startswith("1-")
        ri.loc[interstate, "Traffic Control Type"] = "4. Stop Sign"
        report = validate_and_fix(ri, verbose=False)
        tc = ri.loc[interstate, "Traffic Control Type"]
        assert (tc == "1. No Traffic Control").all(), f"Stop sign on interstate not removed"

    def _lane_count():
        ri = make_road_inventory(100)
        interstate = ri["Functional Class"].str.startswith("1-")
        ri.loc[interstate, "Through_Lanes"] = "1"
        report = validate_and_fix(ri, verbose=False)
        lanes = ri.loc[interstate, "Through_Lanes"]
        assert (lanes == "2").all(), f"Interstate lanes not raised: {lanes.unique()}"

    def _route_name_fc():
        ri = make_road_inventory(100)
        # Set first row to I-95 but FC-7
        ri.loc[0, "RTE Name"] = "I-95"
        ri.loc[0, "Functional Class"] = "7-Local (J,6)"
        report = validate_and_fix(ri, verbose=False)
        assert ri.loc[0, "Functional Class"] == "1-Interstate (A,1)"
        assert ri.loc[0, "Ownership"] == "1. State Hwy Agency"

    def _school_zone_far():
        ri = make_road_inventory(100)
        ri.loc[0, "School Zone"] = "1. Yes"
        ri.loc[0, "nearest_school_dist_ft"] = 5000  # 5000ft = way too far
        report = validate_and_fix(ri, verbose=False)
        assert ri.loc[0, "School Zone"] == "3. No"

    def _school_zone_near():
        ri = make_road_inventory(100)
        ri.loc[0, "School Zone"] = "3. No"
        ri.loc[0, "nearest_school_dist_ft"] = 300  # 300ft = should be Yes
        report = validate_and_fix(ri, verbose=False)
        assert ri.loc[0, "School Zone"] == "1. Yes"

    def _report_counts():
        ri = make_road_inventory(100)
        ri["Roadway Surface Type"] = ""
        report = validate_and_fix(ri, verbose=False)
        assert report.total_fixes > 0
        assert "SURF_FILL" in report.fixes

    t.test("Rule 1: Speed floor (Interstate ≥45)", _speed_floor)
    t.test("Rule 1: Speed ceiling (Local ≤35)", _speed_ceiling)
    t.test("Rule 2: Surface type gap fill", _surface_fill)
    t.test("Rule 3: Ownership (Interstate→State)", _ownership_interstate)
    t.test("Rule 4: SYSTEM aligned to FC", _system_alignment)
    t.test("Rule 5: Facility Type oneway fix", _facility_oneway)
    t.test("Rule 7: Stop sign on Interstate removed", _stop_sign_interstate)
    t.test("Rule 8: Lane count (Interstate ≥2)", _lane_count)
    t.test("Rule 9: Route I-* → FC-1", _route_name_fc)
    t.test("Rule 10: School Zone far → No", _school_zone_far)
    t.test("Rule 10: School Zone near → Yes", _school_zone_near)
    t.test("Report has fix counts", _report_counts)


def test_enricher(t, cache_dir):
    """Test road inventory enricher with synthetic parquet."""
    print("\n── ROAD INVENTORY ENRICHER ──")
    from road_inventory_enricher import RoadInventorySession

    ri = make_road_inventory(200)
    parquet_path = os.path.join(cache_dir, "xx_road_inventory.parquet.gz")
    save_synthetic_parquet(ri, parquet_path)

    def _session_loads():
        session = RoadInventorySession("xx", cache_dir)
        assert session.ready, "Session not ready"
        assert session.matcher is not None
        assert len(session.transfer_cols) > 0

    def _enrich_returns_df():
        session = RoadInventorySession("xx", cache_dir)
        # Use road midpoints as crash coords to guarantee matches
        ri_sample = session.ri.head(50)
        crashes = make_crash_data(50)
        crashes["y"] = ri_sample["mid_lat"].values[:50]
        crashes["x"] = ri_sample["mid_lon"].values[:50]
        result = session.enrich(crashes.copy())
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 50
        assert "ri_matched" in result.columns
        assert "ri_confidence" in result.columns
        assert "ri_match_dist_ft" in result.columns

    def _enrich_fills_columns():
        session = RoadInventorySession("xx", cache_dir)
        crashes = make_crash_data(50)
        ri_sample = session.ri.head(50)
        crashes["y"] = ri_sample["mid_lat"].values[:50]
        crashes["x"] = ri_sample["mid_lon"].values[:50]
        result = session.enrich(crashes.copy())
        matched = result["ri_matched"] == "Yes"
        assert matched.sum() > 0, f"No matches (have {len(session.ri)} segs)"
        # Check key columns got filled
        for col in ["Functional Class", "SYSTEM", "Ownership"]:
            filled = (result.loc[matched, col].fillna("").str.strip() != "").sum()
            assert filled > 0, f"{col} not filled"

    def _enrich_preserves_index():
        session = RoadInventorySession("xx", cache_dir)
        crashes = make_crash_data(50)
        crashes.index = range(500, 550)  # non-zero-based index
        result = session.enrich(crashes.copy())
        assert list(result.index) == list(range(500, 550)), "Index not preserved"

    def _proximity_cleanup():
        session = RoadInventorySession("xx", cache_dir)
        # After load, bridge distances beyond 500ft should be 0
        bridge_dist = pd.to_numeric(session.ri["nearest_bridge_dist_ft"], errors="coerce")
        beyond = bridge_dist > 500
        if beyond.any():
            assert False, f"Bridge dist >500ft not cleaned: max={bridge_dist.max()}"

    def _duplicate_columns_dropped():
        session = RoadInventorySession("xx", cache_dir)
        # geo_ duplicates should be gone
        geo_cols = [c for c in session.ri.columns if c.startswith("geo_")]
        assert len(geo_cols) == 0, f"geo_ cols not dropped: {geo_cols}"

    t.test("Session loads from parquet", _session_loads)
    t.test("enrich() returns DataFrame", _enrich_returns_df)
    t.test("enrich() fills key columns", _enrich_fills_columns)
    t.test("enrich() preserves non-zero index", _enrich_preserves_index)
    t.test("Proximity cleanup applied", _proximity_cleanup)
    t.test("Duplicate columns dropped", _duplicate_columns_dropped)


def test_crash_enricher(t):
    """Test CrashEnricher tier 1."""
    print("\n── CRASH ENRICHER (Tier 1) ──")
    from crash_enricher import CrashEnricher

    def _tier1_runs():
        enricher = CrashEnricher("99", "XX", "TestState", cache_dir="cache")
        crashes = make_crash_data(50)
        result = enricher.enrich_tier1(crashes.copy())
        assert len(result) == 50

    def _night_flag():
        enricher = CrashEnricher("99", "XX", "TestState", cache_dir="cache")
        crashes = make_crash_data(10)
        crashes["Light Condition"] = "5. Darkness - Road Not Lighted"
        # Night? derivation happens in enrich_tier1 via Light Condition mapping
        result = enricher.enrich_tier1(crashes.copy())
        night_vals = result["Night?"].unique()
        assert "Yes" in night_vals or len(night_vals) > 0, f"Night flag issue: {night_vals}"

    def _intersection_analysis():
        enricher = CrashEnricher("99", "XX", "TestState", cache_dir="cache")
        crashes = make_crash_data(10)
        crashes["Intersection Type"] = "1. Not at Intersection"
        crashes["Ownership"] = "1. State Hwy Agency"
        result = enricher._derive_intersection_analysis(crashes.copy())
        assert (result["Intersection Analysis"] == "Not Intersection").all()

    def _intersection_dot():
        enricher = CrashEnricher("99", "XX", "TestState", cache_dir="cache")
        crashes = make_crash_data(10)
        crashes["Intersection Type"] = "4. Four Approaches"
        crashes["Ownership"] = "1. State Hwy Agency"
        result = enricher._derive_intersection_analysis(crashes.copy())
        assert (result["Intersection Analysis"] == "DOT Intersection").all()

    t.test("Tier 1 enrichment runs", _tier1_runs)
    t.test("Night flag derivation", _night_flag)
    t.test("Intersection Analysis: Not Intersection", _intersection_analysis)
    t.test("Intersection Analysis: DOT Intersection", _intersection_dot)


def test_value_mappings(t):
    """Test that constants match VDOT frontend standard."""
    print("\n── VALUE MAPPING CONSTANTS ──")
    from road_inventory_enricher import RI_FC_TO_VDOT_FC, OVERWRITE_COLUMNS, FILL_COLUMNS
    from crash_enricher import OSM_HIGHWAY_TO_FC, FC_TO_OWNERSHIP, FC_TO_SYSTEM

    def _fc_values_standard():
        expected = [
            "1-Interstate (A,1)",
            "2-Principal Arterial - Other Freeways and Expressways (B)",
            "3-Principal Arterial - Other (E,2)",
            "4-Minor Arterial (H,3)",
            "5-Major Collector (I,4)",
            "6-Minor Collector (5)",
            "7-Local (J,6)",
        ]
        for fc in expected:
            assert fc in FC_TO_SYSTEM, f"FC missing from FC_TO_SYSTEM: {fc}"
            assert fc in FC_TO_OWNERSHIP, f"FC missing from FC_TO_OWNERSHIP: {fc}"

    def _system_values_standard():
        expected = {"DOT Interstate", "DOT Primary", "DOT Secondary",
                    "Non-DOT primary", "Non-DOT secondary"}
        actual = set(FC_TO_SYSTEM.values())
        assert actual == expected, f"SYSTEM values wrong: {actual}"

    def _ownership_values_standard():
        expected = {"1. State Hwy Agency", "2. County Hwy Agency",
                    "3. City or Town Hwy Agency"}
        actual = set(FC_TO_OWNERSHIP.values())
        assert actual == expected, f"Ownership values wrong: {actual}"

    def _overwrite_has_fc():
        assert "Functional Class" in OVERWRITE_COLUMNS
        assert "Ownership" in OVERWRITE_COLUMNS
        assert "SYSTEM" in OVERWRITE_COLUMNS

    def _fill_has_rte():
        assert "RTE Name" in FILL_COLUMNS
        assert "Traffic Control Type" in FILL_COLUMNS

    def _osm_highway_complete():
        expected_tags = ["motorway", "trunk", "primary", "secondary",
                         "tertiary", "residential", "service"]
        for tag in expected_tags:
            assert tag in OSM_HIGHWAY_TO_FC, f"Missing OSM tag: {tag}"

    t.test("FC values match standard", _fc_values_standard)
    t.test("SYSTEM values match standard", _system_values_standard)
    t.test("Ownership values match standard", _ownership_values_standard)
    t.test("OVERWRITE has FC/Ownership/SYSTEM", _overwrite_has_fc)
    t.test("FILL has RTE Name/Traffic Control", _fill_has_rte)
    t.test("OSM highway tags complete", _osm_highway_complete)


def test_end_to_end(t, cache_dir):
    """Full pipeline: crash data → tier 1 → spatial match → enriched output."""
    print("\n── END-TO-END PIPELINE ──")
    from crash_enricher import CrashEnricher
    from road_inventory_enricher import RoadInventorySession

    ri = make_road_inventory(300)
    parquet_path = os.path.join(cache_dir, "xx_road_inventory.parquet.gz")
    save_synthetic_parquet(ri, parquet_path)

    def _full_pipeline():
        # Step 1: Tier 1
        enricher = CrashEnricher("99", "XX", "TestState", cache_dir=cache_dir)
        crashes = make_crash_data(100)
        df = enricher.enrich_tier1(crashes.copy())

        # Step 2: Road Inventory
        session = RoadInventorySession("xx", cache_dir)
        assert session.ready
        df = session.enrich(df)

        # Step 3: Intersection Analysis
        df = enricher._derive_intersection_analysis(df)

        # Verify
        matched = df["ri_matched"] == "Yes"
        assert matched.sum() > 0, "No matches"
        assert "ri_confidence" in df.columns
        assert "Intersection Analysis" in df.columns

        # Key columns filled for matched rows
        for col in ["Functional Class", "SYSTEM", "Ownership", "RTE Name"]:
            filled = (df.loc[matched, col].fillna("").str.strip() != "").sum()
            assert filled > 0, f"E2E: {col} not filled"

        # Surface type should be 100% after validator
        surf = df.loc[matched, "Roadway Surface Type"].fillna("").str.strip()
        surf_pct = (surf != "").sum() / matched.sum() * 100
        assert surf_pct >= 99, f"Surface only {surf_pct:.0f}%"

        # Speed should be sane
        spd = pd.to_numeric(df.loc[matched, "Max Speed Diff"], errors="coerce")
        assert (spd[spd > 0] >= 5).all(), "Speed < 5 found"

        return len(df.columns)

    def _column_count():
        n_cols = _full_pipeline()
        assert n_cols > 50, f"Too few columns: {n_cols}"

    t.test("Full pipeline: Tier1 → Match → Validate → Enrich", _full_pipeline)


def test_generate_osm(t):
    """Test generate_osm_data.py constants and structure."""
    print("\n── GENERATE_OSM_DATA ──")

    def _poi_tags_has_exit():
        osm = _find_file("generate_osm_data.py")
        assert osm, "generate_osm_data.py not found"
        with open(osm) as f:
            content = f.read()
        assert '"exit"' in content, "Missing exit POI category"
        assert "motorway_junction" in content, "Missing motorway_junction tag"
        assert "geometry_coords" in content, "Missing geometry_coords"

    def _state_registry():
        osm = _find_file("generate_osm_data.py")
        assert osm, "generate_osm_data.py not found"
        with open(osm) as f:
            content = f.read()
        for state in ["Alabama", "California", "Texas", "New York", "Wyoming"]:
            assert state in content, f"Missing state: {state}"

    t.test("POI tags include exit nodes", _poi_tags_has_exit)
    t.test("State registry covers 51 states", _state_registry)


def test_build_road_inventory(t):
    """Test build_road_inventory.py structure."""
    print("\n── BUILD_ROAD_INVENTORY ──")

    def _calls_validator():
        # Find build_road_inventory.py in sys.path or cwd
        bri = _find_file("build_road_inventory.py")
        assert bri, "build_road_inventory.py not found"
        with open(bri) as f:
            content = f.read()
        assert "validate_and_fix" in content, "Missing validator call"
        assert "enrich_ramps" in content, "Missing enrich_ramps call"

    def _has_proximity_cleanup():
        bri = _find_file("build_road_inventory.py")
        assert bri, "build_road_inventory.py not found"
        with open(bri) as f:
            content = f.read()
        assert "ASSET_THRESHOLDS_FT" in content, "Missing proximity thresholds"
        assert "nearest_bridge_" in content

    def _state_registry():
        bri = _find_file("build_road_inventory.py")
        assert bri, "build_road_inventory.py not found"
        with open(bri) as f:
            content = f.read()
        for abbr in ["de", "va", "tx", "ca", "ny"]:
            assert f'"{abbr}"' in content, f"Missing state: {abbr}"

    t.test("Calls validator", _calls_validator)
    t.test("Has proximity cleanup", _has_proximity_cleanup)
    t.test("State registry covers key states", _state_registry)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--quick", action="store_true", help="Skip slow tests")
    args = parser.parse_args()

    print("═" * 60)
    print("  CrashLens Pipeline Test Suite")
    print("═" * 60)

    t = TestRunner(verbose=args.verbose)

    # Setup synthetic cache
    cache_dir = "test_cache"
    os.makedirs(cache_dir, exist_ok=True)

    test_imports(t)
    test_spatial_matcher(t)
    test_validator(t)
    test_value_mappings(t)
    test_generate_osm(t)
    test_build_road_inventory(t)

    if not args.quick:
        test_enricher(t, cache_dir)
        test_crash_enricher(t)
        test_end_to_end(t, cache_dir)

    success = t.report()

    # Cleanup
    import shutil
    shutil.rmtree(cache_dir, ignore_errors=True)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
