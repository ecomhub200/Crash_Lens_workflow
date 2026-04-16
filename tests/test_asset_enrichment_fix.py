"""
Bug test for enrich_nearest_asset fix.
Verifies:
  1. Fill rates correct (was 0.4%, now 10-20% within threshold)
  2. Attributes EMPTY beyond threshold (honest — not fake data)
  3. Renamed columns present, old columns absent
  4. New school attrs (ncessch, leaid) populated

Pre-req: python build_road_inventory.py --state de --local-only
Run:     pytest tests/test_asset_enrichment_fix.py -v
"""
import pandas as pd
import pytest
from pathlib import Path

CACHE = Path("cache/de_road_inventory.parquet.gz")

@pytest.fixture(scope="module")
def df():
    assert CACHE.exists(), f"Run build_road_inventory.py --state de --local-only first"
    return pd.read_parquet(CACHE)


# ── Primary flag columns (renamed) ─────────────────────────────────────────

def test_near_school_1000ft_present(df):
    assert 'Near_School_1000ft' in df.columns, "Missing Near_School_1000ft"

def test_near_school_1500ft_absent(df):
    assert 'Near_School_1500ft' not in df.columns, (
        "Old Near_School_1500ft should NOT be produced after rename"
    )

def test_near_poi_hospital_1000ft_present(df):
    assert 'Near_PoiHospital_1000ft' in df.columns

def test_near_poi_hospital_2000ft_absent(df):
    assert 'Near_PoiHospital_2000ft' not in df.columns

def test_near_poi_clinic_1000ft_present(df):
    assert 'Near_PoiClinic_1000ft' in df.columns

def test_near_poi_clinic_1500ft_absent(df):
    assert 'Near_PoiClinic_1500ft' not in df.columns


# ── Unchanged primary flags ────────────────────────────────────────────────

@pytest.mark.parametrize("col", [
    'Near_Bridge_500ft', 'Near_RailXing_500ft', 'Near_Transit_500ft',
    'Near_PoiBar_1500ft', 'Near_PoiCollege_1500ft', 'Near_PoiCrossing_100ft',
    'Near_PoiFuel_500ft', 'Near_PoiParking_500ft', 'Near_PoiRestArea_1000ft',
    'Near_PoiRestaurant_500ft', 'Near_PoiSignal_100ft', 'Near_PoiStopSign_100ft',
])
def test_unchanged_flags_present(df, col):
    assert col in df.columns, f"{col} should still be produced (threshold unchanged)"


# ── Fill rates: bug fixed ──────────────────────────────────────────────────

@pytest.mark.parametrize("asset,flag,name_col,expected_min,expected_max", [
    ("school",   "Near_School_1000ft",       "nearest_school_name",            3, 25),
    ("bridge",   "Near_Bridge_500ft",        "nearest_bridge_structure_id",    0.5, 30),
    ("rail",     "Near_RailXing_500ft",      "nearest_rail_xing_id",           0.1, 10),
    ("transit",  "Near_Transit_500ft",       "nearest_transit_stop_name",      1, 30),
    ("hospital", "Near_PoiHospital_1000ft",  "nearest_poi_hospital_name",      0.1, 15),
    ("clinic",   "Near_PoiClinic_1000ft",    "nearest_poi_clinic_name",        0.1, 20),
])
def test_fill_rate_in_realistic_range(df, asset, flag, name_col, expected_min, expected_max):
    """Tested against Delaware: school 11.2%, bridges ~5-15%, etc."""
    pct = (df[flag] == 'Yes').sum() / len(df) * 100
    assert expected_min < pct < expected_max, (
        f"{flag} at {pct:.1f}% — expected {expected_min}-{expected_max}% (was 0.4% before fix)"
    )


# ── Within-threshold semantics: populated if flag=Yes, empty if flag=No ────

@pytest.mark.parametrize("flag,name_col", [
    ("Near_School_1000ft",       "nearest_school_name"),
    ("Near_PoiHospital_1000ft",  "nearest_poi_hospital_name"),
    ("Near_PoiClinic_1000ft",    "nearest_poi_clinic_name"),
    ("Near_Transit_500ft",       "nearest_transit_stop_name"),
    ("Near_RailXing_500ft",      "nearest_rail_xing_id"),
])
def test_attrs_populated_within_threshold(df, flag, name_col):
    """When flag=Yes, the name attribute must be populated."""
    if name_col not in df.columns:
        pytest.skip(f"{name_col} not in columns")
    yes_mask = df[flag] == 'Yes'
    names_when_yes = df.loc[yes_mask, name_col].fillna('').astype(str).str.strip()
    empty_ratio = (names_when_yes == '').sum() / max(yes_mask.sum(), 1)
    assert empty_ratio < 0.05, (
        f"When {flag}=Yes, {empty_ratio*100:.1f}% of {name_col} is empty — should be ~0%"
    )

@pytest.mark.parametrize("flag,name_col", [
    ("Near_School_1000ft",       "nearest_school_name"),
    ("Near_PoiHospital_1000ft",  "nearest_poi_hospital_name"),
])
def test_attrs_empty_beyond_threshold(df, flag, name_col):
    """When flag=No, the name attribute must be empty (honesty principle)."""
    if name_col not in df.columns:
        pytest.skip(f"{name_col} not in columns")
    no_mask = df[flag] == 'No'
    names_when_no = df.loc[no_mask, name_col].fillna('').astype(str).str.strip()
    populated_ratio = (names_when_no != '').sum() / max(no_mask.sum(), 1)
    assert populated_ratio < 0.02, (
        f"When {flag}=No, {populated_ratio*100:.1f}% of {name_col} is populated — "
        f"should be ~0% (honest). Bug: attributes leaking beyond threshold."
    )


# ── New school attributes ──────────────────────────────────────────────────

def test_ncessch_column_added(df):
    assert 'nearest_school_ncessch' in df.columns

def test_leaid_column_added(df):
    assert 'nearest_school_leaid' in df.columns

def test_ncessch_populated_within_threshold(df):
    """ncessch must be populated on every Near_School_1000ft=Yes row."""
    yes_rows = df[df['Near_School_1000ft'] == 'Yes']
    filled = (yes_rows['nearest_school_ncessch'].fillna('').astype(str).str.strip() != '').sum()
    assert filled / max(len(yes_rows), 1) > 0.95, (
        f"ncessch only {filled}/{len(yes_rows)} on school-zone rows"
    )

def test_ncessch_empty_outside_threshold(df):
    """ncessch must be EMPTY beyond 1000ft."""
    no_rows = df[df['Near_School_1000ft'] == 'No']
    populated = (no_rows['nearest_school_ncessch'].fillna('').astype(str).str.strip() != '').sum()
    assert populated / max(len(no_rows), 1) < 0.02, (
        f"ncessch leaking: {populated}/{len(no_rows)} rows outside threshold have values"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
