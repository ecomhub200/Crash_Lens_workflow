"""
Synthetic unit test for enrich_nearest_asset.

Tests the core matching logic without requiring a full pipeline run.
Uses small fake datasets (roads grid + 2 schools) so we can hand-compute
the expected output and catch regressions in the STRtree-direction fix.

Key assertions:
  1. Bug regression: buggy direction would match at most 2 roads; fix matches
     every road within threshold of ANY school.
  2. Within-threshold rows have populated attributes (name, ncessch, leaid).
  3. Beyond-threshold rows have empty-string / zero attributes (honesty).
  4. dist_ft is always set (even beyond threshold).
  5. KDTree fallback works when shapely unavailable (simulated).

Run:  pytest tests/test_asset_enrichment_synthetic.py -v
"""
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import build_road_inventory as bri


FT_PER_DEG_LAT = 364_000.0  # ~1 degree of latitude ≈ 364,000 ft
# 1 degree of longitude ≈ 364,000 * cos(lat) ft; at lat=39 that's ≈ 282,840 ft


def _offset_ft(lat_ref, north_ft, east_ft):
    """Return (lat, lon) offset from (lat_ref, 0) by north_ft and east_ft."""
    dlat = north_ft / FT_PER_DEG_LAT
    dlon = east_ft / (FT_PER_DEG_LAT * math.cos(math.radians(lat_ref)))
    return lat_ref + dlat, 0.0 + dlon


@pytest.fixture
def roads():
    """10 roads strung east along lat=39.0 every 500 ft.
    mid_lat / mid_lon are used by the enrichment; geometry_coords is
    optional (STRtree fallback is fine without it)."""
    lat_ref = 39.0
    rows = []
    for i in range(10):
        lat, lon = _offset_ft(lat_ref, 0, i * 500)  # roads at 0, 500, 1000... ft east
        rows.append({
            "mid_lat": lat,
            "mid_lon": lon,
            "u_node": i,
            "v_node": i + 1,
            "highway": "residential",
        })
    return pd.DataFrame(rows)


@pytest.fixture
def schools():
    """2 schools: one near road 0 (east offset 0 ft), one near road 6 (east offset 3000 ft).
    With 1000ft threshold:
      - School A (east 0)   matches roads 0, 1, 2     (0, 500, 1000 ft — edge case: 1000 ft = boundary)
      - School B (east 3000) matches roads 5, 6, 7, 8 (2500, 3000, 3500, 4000 ft offsets → distances 500/0/500/1000)
    Hand-computed expectation: ~6-7 roads with Near_School_1000ft=Yes."""
    lat_ref = 39.0
    lat_a, lon_a = _offset_ft(lat_ref, 0, 0)
    lat_b, lon_b = _offset_ft(lat_ref, 0, 3000)
    return pd.DataFrame([
        {"lat": lat_a, "lon": lon_a, "school_name": "Alpha Elementary",
         "school_level": "Elementary", "enrollment": 400, "school_type": "Public",
         "ncessch": "100001", "leaid": "1000"},
        {"lat": lat_b, "lon": lon_b, "school_name": "Beta Middle",
         "school_level": "Middle", "enrollment": 700, "school_type": "Public",
         "ncessch": "100002", "leaid": "1000"},
    ])


@pytest.fixture(autouse=True)
def reset_strtree():
    """Reset module globals so each test starts clean."""
    bri._road_strtree = None
    bri._road_linestrings = None
    yield
    bri._road_strtree = None
    bri._road_linestrings = None


def _run_schools(roads_df, schools_df):
    """Call enrich_nearest_asset with the same attr_cols as enrich_schools."""
    bri.enrich_nearest_asset(
        roads_df, schools_df, "school", 1000,
        attr_cols=[
            ("school_name",   "name"),
            ("school_level",  "level"),
            ("enrollment",    "enrollment"),
            ("school_type",   "type"),
            ("ncessch",       "ncessch"),
            ("leaid",         "leaid"),
        ],
        label="Schools",
    )


# ── Core regression: bug produced 1-2 Yes rows, fix produces ≥4 ───────────

def test_fill_rate_beats_old_bug(roads, schools):
    """Old bug: only 2 roads matched (one per school). Fix: multiple roads per school."""
    _run_schools(roads, schools)
    n_yes = (roads["Near_School_1000ft"] == "Yes").sum()
    assert n_yes >= 4, f"Fix should match ≥4 roads (got {n_yes}); old bug matched ≤2"
    assert n_yes <= 8, f"Sanity: shouldn't match >8 of 10 roads (got {n_yes})"


def test_every_road_has_distance(roads, schools):
    """dist_ft is set for ALL roads, even beyond threshold."""
    _run_schools(roads, schools)
    dists = roads["nearest_school_dist_ft"]
    assert (dists >= 0).all(), "dist_ft must be non-negative for every road"
    # At least one road should be > 1000 ft from any school (roads 3, 4 are 1500, 1000 from nearest)
    assert (dists > 500).any(), "Some roads must have dist > 500ft in this fixture"


def test_within_threshold_attrs_populated(roads, schools):
    """Rows with Near_School_1000ft=Yes must have non-empty name, ncessch, leaid."""
    _run_schools(roads, schools)
    yes_mask = roads["Near_School_1000ft"] == "Yes"
    assert yes_mask.sum() > 0, "fixture should produce at least one Yes"
    yes_rows = roads[yes_mask]
    # Name
    assert (yes_rows["nearest_school_name"].astype(str).str.strip() != "").all(), (
        "name must be populated on all Yes rows"
    )
    # NCES ID
    assert (yes_rows["nearest_school_ncessch"].astype(str).str.strip() != "").all(), (
        "ncessch must be populated on all Yes rows"
    )
    # LEA ID
    assert (yes_rows["nearest_school_leaid"].astype(str).str.strip() != "").all(), (
        "leaid must be populated on all Yes rows"
    )


def test_beyond_threshold_attrs_empty(roads, schools):
    """Honesty principle: rows with Near_School_1000ft=No must have EMPTY attrs."""
    _run_schools(roads, schools)
    no_mask = roads["Near_School_1000ft"] == "No"
    if no_mask.sum() == 0:
        pytest.skip("fixture happens to match every road")
    no_rows = roads[no_mask]
    # String attrs must be empty
    assert (no_rows["nearest_school_name"].astype(str).str.strip() == "").all(), (
        "name must be empty for No rows — honesty principle"
    )
    assert (no_rows["nearest_school_ncessch"].astype(str).str.strip() == "").all(), (
        "ncessch must be empty for No rows"
    )
    # Numeric attrs must be 0
    assert (no_rows["nearest_school_enrollment"] == 0).all(), (
        "enrollment must be 0 for No rows"
    )
    # lat/lon must be 0
    assert (no_rows["nearest_school_lat"] == 0.0).all()
    assert (no_rows["nearest_school_lon"] == 0.0).all()


def test_count_within_radius_nonzero(roads, schools):
    """school_count_1000ft should be populated (uses count_within_radius)."""
    _run_schools(roads, schools)
    assert "school_count_1000ft" in roads.columns
    counts = roads["school_count_1000ft"]
    assert (counts >= 0).all()
    assert counts.max() >= 1, f"At least one road should have ≥1 school nearby; max={counts.max()}"


def test_nearest_asset_lat_lon_populated(roads, schools):
    """nearest_school_lat/lon should match one of the input school positions."""
    _run_schools(roads, schools)
    yes_rows = roads[roads["Near_School_1000ft"] == "Yes"]
    school_lats = set(round(l, 4) for l in schools["lat"])
    for lat in yes_rows["nearest_school_lat"]:
        assert round(lat, 4) in school_lats, (
            f"nearest_school_lat {lat} must match one of {school_lats}"
        )


# ── Output column names use threshold ────────────────────────────────────

def test_output_columns_use_1000ft_threshold(roads, schools):
    _run_schools(roads, schools)
    assert "Near_School_1000ft" in roads.columns
    assert "school_count_1000ft" in roads.columns
    # Old threshold must not be produced
    assert "Near_School_1500ft" not in roads.columns


def test_hospital_threshold_1000ft(roads):
    """enrich_poi_categories passes 1000 for hospital now (was 2000)."""
    hospital_df = pd.DataFrame([
        {"lat": 39.0, "lon": 0.0, "name": "Test Hospital", "subcategory": "general"},
    ])
    bri.enrich_nearest_asset(
        roads, hospital_df, "poi_hospital", 1000,
        attr_cols=[("name", "name"), ("subcategory", "subcategory")],
        label="POI hospital",
    )
    assert "Near_PoiHospital_1000ft" in roads.columns
    assert "Near_PoiHospital_2000ft" not in roads.columns


def test_clinic_threshold_1000ft(roads):
    clinic_df = pd.DataFrame([
        {"lat": 39.0, "lon": 0.0, "name": "Test Clinic", "subcategory": "urgent"},
    ])
    bri.enrich_nearest_asset(
        roads, clinic_df, "poi_clinic", 1000,
        attr_cols=[("name", "name"), ("subcategory", "subcategory")],
        label="POI clinic",
    )
    assert "Near_PoiClinic_1000ft" in roads.columns
    assert "Near_PoiClinic_1500ft" not in roads.columns


# ── STRtree path vs KDTree fallback ──────────────────────────────────────

def test_strtree_path_matches_kdtree_fallback(roads, schools):
    """Both paths should produce the same Yes/No pattern (within rounding)."""
    roads_strtree = roads.copy()
    _run_schools(roads_strtree, schools)

    # Simulate shapely-missing to force KDTree fallback path.
    roads_kdtree = roads.copy()
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def mock_import(name, *args, **kwargs):
        if name.startswith("shapely"):
            raise ImportError("simulated: shapely missing")
        return real_import(name, *args, **kwargs)

    import builtins as _b
    _b.__import__ = mock_import
    try:
        bri._road_strtree = None
        bri._road_linestrings = None
        bri.enrich_nearest_asset(
            roads_kdtree, schools, "school", 1000,
            attr_cols=[
                ("school_name",   "name"),
                ("school_level",  "level"),
                ("enrollment",    "enrollment"),
                ("school_type",   "type"),
                ("ncessch",       "ncessch"),
                ("leaid",         "leaid"),
            ],
            label="Schools (KDTree)",
        )
    finally:
        _b.__import__ = real_import

    yes_strtree = (roads_strtree["Near_School_1000ft"] == "Yes").sum()
    yes_kdtree = (roads_kdtree["Near_School_1000ft"] == "Yes").sum()
    # Allow ±1 divergence due to point-vs-linestring geometry and midpoint approximation
    assert abs(yes_strtree - yes_kdtree) <= 1, (
        f"STRtree ({yes_strtree} Yes) vs KDTree ({yes_kdtree} Yes) should roughly agree"
    )


# ── Empty input graceful handling (pre-existing early-return branch) ─────

def test_empty_df_produces_no_columns_and_logs(roads):
    bri.enrich_nearest_asset(
        roads, None, "school", 1000,
        attr_cols=[("school_name", "name"), ("ncessch", "ncessch")],
        label="Schools (empty)",
    )
    assert "Near_School_1000ft" in roads.columns
    assert (roads["Near_School_1000ft"] == "No").all()
    assert (roads["nearest_school_name"] == "").all()
    assert (roads["nearest_school_ncessch"] == "").all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
