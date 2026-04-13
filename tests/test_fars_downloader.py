#!/usr/bin/env python3
"""
Bug tests for generate_fars_data.py — CrashLens FARS downloader.

Covers:
- Helper correctness (state loading, numeric coercion)
- Aggregation semantics (person/vehicle row-level flags → crash level)
- GPS sanitization (sentinels AND Hawaii/Alaska bounds)
- End-to-end process_state() with a mocked FARS API
- Multi-state nationwide rollup
- Final schema matches the documented 44-column contract

Run with: python -m pytest tests/test_fars_downloader.py -v
"""

import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import requests

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

import generate_fars_data as gfd


# ═══════════════════════════════════════════════════════════════
#  State registry
# ═══════════════════════════════════════════════════════════════

def test_load_states_count():
    """51 states total: 50 states + DC. Puerto Rico filtered out (not in FARS)."""
    assert len(gfd.ALL_STATES) == 51
    assert "pr" not in gfd.ABBR_LOOKUP


def test_load_states_delaware():
    de = gfd.ABBR_LOOKUP["de"]
    assert de["name"] == "Delaware"
    assert de["abbreviation"] == "de"
    assert de["fips"] == "10"
    assert de["r2_prefix"] == "delaware"


def test_load_states_dc_prefix():
    """DC's r2_prefix must be underscore-joined: 'district_of_columbia'."""
    dc = gfd.ABBR_LOOKUP["dc"]
    assert dc["name"] == "District of Columbia"
    assert dc["fips"] == "11"
    assert dc["r2_prefix"] == "district_of_columbia"


def test_load_states_unique_fips():
    """Every state must have a unique FIPS code."""
    fips = [s["fips"] for s in gfd.ALL_STATES]
    assert len(fips) == len(set(fips))


# ═══════════════════════════════════════════════════════════════
#  _num helper
# ═══════════════════════════════════════════════════════════════

def test_num_coerces_series():
    s = pd.Series(["1", "2", "abc", None, "4.5"])
    out = gfd._num(s)
    assert out.iloc[0] == 1
    assert out.iloc[1] == 2
    assert pd.isna(out.iloc[2])  # "abc" → NaN
    assert pd.isna(out.iloc[3])  # None → NaN
    assert out.iloc[4] == 4.5


def test_num_handles_none_scalar():
    """_num(None) must not crash — it's used via df.get('MISSING_COL')."""
    # This is what happens when aggregate_persons is handed a df
    # that's missing an expected column.
    try:
        result = gfd._num(None)
    except Exception as e:
        pytest.fail(f"_num(None) raised {type(e).__name__}: {e}")
    # Either NaN or None is acceptable; just must not crash.
    assert result is None or (isinstance(result, float) and np.isnan(result))


# ═══════════════════════════════════════════════════════════════
#  GPS sanitization
# ═══════════════════════════════════════════════════════════════

def test_sanitize_gps_delaware():
    """Delaware coordinates (~38.9°N, -75.4°E) must be preserved."""
    df = pd.DataFrame({"latitude": [38.9], "longitude": [-75.4]})
    valid, total = gfd._sanitize_gps(df)
    assert valid == 1
    assert total == 1
    assert df["latitude"].iloc[0] == 38.9
    assert df["longitude"].iloc[0] == -75.4


def test_sanitize_gps_alaska_far_north():
    """Alaska's Utqiagvik at 71.3°N must be preserved."""
    df = pd.DataFrame({"latitude": [71.3], "longitude": [-156.8]})
    valid, _ = gfd._sanitize_gps(df)
    assert valid == 1
    assert df["latitude"].iloc[0] == 71.3


def test_sanitize_gps_hawaii():
    """CRITICAL: Hawaii (Big Island ~19.5°N) must NOT be masked as invalid.
    FARS covers fatal crashes in Hawaii — a too-tight lat lower bound would
    silently drop every HI crash. This is the Hawaii-bounds regression test.
    """
    df = pd.DataFrame({
        "latitude": [19.5, 20.8, 21.3],     # Big Island, Maui, Oahu
        "longitude": [-155.6, -156.3, -157.8],
    })
    valid, total = gfd._sanitize_gps(df)
    assert valid == 3, f"Hawaii coords were masked: only {valid}/{total} kept"
    assert not df["latitude"].isna().any()


def test_sanitize_gps_sentinels():
    """FARS unknown-location sentinels must become NaN."""
    df = pd.DataFrame({
        "latitude": [77.7777, 88.8888, 99.9999],
        "longitude": [777.7777, 888.8888, -200.0],
    })
    valid, total = gfd._sanitize_gps(df)
    assert valid == 0
    assert df["latitude"].isna().all()
    assert df["longitude"].isna().all()


def test_sanitize_gps_mixed():
    df = pd.DataFrame({
        "latitude":  [38.9, 77.7777, 19.5, 45.0],
        "longitude": [-75.4, 777.7777, -155.6, 0.0],  # last lon is out of range
    })
    valid, total = gfd._sanitize_gps(df)
    assert total == 4
    assert valid == 2  # DE + HI
    assert df["latitude"].iloc[0] == 38.9
    assert pd.isna(df["latitude"].iloc[1])
    assert df["latitude"].iloc[2] == 19.5
    assert pd.isna(df["latitude"].iloc[3])  # lon was 0, outside range


# ═══════════════════════════════════════════════════════════════
#  Person aggregation
# ═══════════════════════════════════════════════════════════════

def _person_row(**kw):
    base = dict(ST_CASE=1, YEAR=2020, DRINKING=0, ALC_RES=0, REST_USE=3,
                PER_TYP=1, INJ_SEV=0, AGE=30)
    base.update(kw)
    return base


def test_aggregate_persons_empty():
    out = gfd.aggregate_persons(pd.DataFrame())
    assert list(out.columns) == [
        "ST_CASE", "YEAR", "any_drunk", "any_unrestrained",
        "ped_involved", "bike_involved", "ped_fatals", "bike_fatals",
        "total_fatalities", "youngest_driver_age", "oldest_driver_age",
    ]
    assert len(out) == 0


def test_aggregate_persons_drunk_by_alc_res():
    """ALC_RES in [8, 94] → any_drunk=True regardless of DRINKING flag."""
    df = pd.DataFrame([_person_row(ALC_RES=12)])
    out = gfd.aggregate_persons(df)
    assert out["any_drunk"].iloc[0] == True  # noqa: E712


def test_aggregate_persons_drunk_by_drinking_flag():
    df = pd.DataFrame([_person_row(DRINKING=1, ALC_RES=0)])
    out = gfd.aggregate_persons(df)
    assert out["any_drunk"].iloc[0] == True  # noqa: E712


def test_aggregate_persons_not_drunk():
    df = pd.DataFrame([_person_row(DRINKING=0, ALC_RES=4)])  # under 8
    out = gfd.aggregate_persons(df)
    assert out["any_drunk"].iloc[0] == False  # noqa: E712


def test_aggregate_persons_pedestrian_fatal():
    df = pd.DataFrame([
        _person_row(ST_CASE=1, PER_TYP=1, INJ_SEV=0),   # driver, not fatal
        _person_row(ST_CASE=1, PER_TYP=5, INJ_SEV=4),   # ped fatal
    ])
    out = gfd.aggregate_persons(df)
    assert out["ped_involved"].iloc[0] == True         # noqa: E712
    assert out["ped_fatals"].iloc[0] == 1
    assert out["bike_fatals"].iloc[0] == 0
    assert out["total_fatalities"].iloc[0] == 1


def test_aggregate_persons_driver_age_masks_passengers():
    """youngest/oldest driver age must exclude passengers (PER_TYP != 1)."""
    df = pd.DataFrame([
        _person_row(ST_CASE=1, PER_TYP=1, AGE=40),   # driver
        _person_row(ST_CASE=1, PER_TYP=2, AGE=16),   # passenger (would be young if counted)
        _person_row(ST_CASE=1, PER_TYP=2, AGE=80),   # passenger
    ])
    out = gfd.aggregate_persons(df)
    assert out["youngest_driver_age"].iloc[0] == 40
    assert out["oldest_driver_age"].iloc[0] == 40


def test_aggregate_persons_restraint():
    """REST_USE in {3, 7} → restrained. Anything else → unrestrained."""
    df = pd.DataFrame([
        _person_row(ST_CASE=1, REST_USE=3),   # belted
        _person_row(ST_CASE=2, REST_USE=1),   # no restraint
    ])
    out = gfd.aggregate_persons(df).sort_values("ST_CASE").reset_index(drop=True)
    assert out["any_unrestrained"].iloc[0] == False  # noqa: E712
    assert out["any_unrestrained"].iloc[1] == True   # noqa: E712


# ═══════════════════════════════════════════════════════════════
#  Vehicle aggregation
# ═══════════════════════════════════════════════════════════════

def _vehicle_row(**kw):
    base = dict(ST_CASE=1, YEAR=2020, SPEEDREL=0, BODY_TYP=4,
                MDRDSTRD=0, HIT_RUN=0)
    base.update(kw)
    return base


def test_aggregate_vehicles_empty():
    out = gfd.aggregate_vehicles(pd.DataFrame())
    assert len(out) == 0
    assert "any_speeding" in out.columns


def test_aggregate_vehicles_speeding():
    df = pd.DataFrame([_vehicle_row(SPEEDREL=2)])
    out = gfd.aggregate_vehicles(df)
    assert out["any_speeding"].iloc[0] == True  # noqa: E712


def test_aggregate_vehicles_large_truck():
    df = pd.DataFrame([_vehicle_row(BODY_TYP=65)])
    out = gfd.aggregate_vehicles(df)
    assert out["any_large_truck"].iloc[0] == True   # noqa: E712
    assert out["any_motorcycle"].iloc[0] == False   # noqa: E712


def test_aggregate_vehicles_motorcycle():
    df = pd.DataFrame([_vehicle_row(BODY_TYP=85)])
    out = gfd.aggregate_vehicles(df)
    assert out["any_motorcycle"].iloc[0] == True  # noqa: E712


def test_aggregate_vehicles_distracted_safe_on_nan():
    """NaN MDRDSTRD must not crash and must NOT count as distracted."""
    df = pd.DataFrame([_vehicle_row(MDRDSTRD=np.nan)])
    out = gfd.aggregate_vehicles(df)
    assert out["any_distracted"].iloc[0] == False  # noqa: E712


def test_aggregate_vehicles_distracted_positive():
    df = pd.DataFrame([_vehicle_row(MDRDSTRD=5)])
    out = gfd.aggregate_vehicles(df)
    assert out["any_distracted"].iloc[0] == True  # noqa: E712


def test_aggregate_vehicles_hit_and_run():
    df = pd.DataFrame([_vehicle_row(HIT_RUN=1)])
    out = gfd.aggregate_vehicles(df)
    assert out["hit_and_run"].iloc[0] == True  # noqa: E712


# ═══════════════════════════════════════════════════════════════
#  Final schema (44 columns per task spec)
# ═══════════════════════════════════════════════════════════════

# These 44 column names are the documented output contract (see the script
# epilog and wiki/log.md 2026-04-13 entry). Keep in sync with RENAME_MAP +
# person/vehicle aggregation column names.
EXPECTED_FINAL_COLS = {
    # Identification (7)
    "case_id", "state_fips", "state_name", "county_fips", "county_name",
    "city_fips", "city_name",
    # When (5)
    "crash_year", "crash_month", "crash_day", "crash_hour", "crash_minute",
    # Where (2)
    "latitude", "longitude",
    # Severity (4)
    "fatalities", "drunk_drivers", "total_vehicles", "total_persons",
    # Road context — text labels (10)
    "functional_class", "road_ownership", "route_type", "rural_urban",
    "lighting", "weather", "manner_of_collision", "first_harmful_event",
    "relation_to_road", "intersection_type",
    # Route names (2)
    "route_name_1", "route_name_2",
    # Person aggregated flags (9)
    "any_drunk", "any_unrestrained", "ped_involved", "bike_involved",
    "ped_fatals", "bike_fatals", "total_fatalities",
    "youngest_driver_age", "oldest_driver_age",
    # Vehicle aggregated flags (5)
    "any_speeding", "any_large_truck", "any_motorcycle",
    "any_distracted", "hit_and_run",
}


def _synthetic_accident_row(case=1, year=2020, **kw):
    base = {
        "ST_CASE": case,
        "STATE": 10,
        "STATENAME": "Delaware",
        "COUNTY": 1,
        "COUNTYNAME": "New Castle",
        "CITY": 0,
        "CITYNAME": "Rural",
        "YEAR": year,
        "MONTH": 6,
        "DAY": 15,
        "HOUR": 12,
        "MINUTE": 30,
        "LATITUDE": 38.9,
        "LONGITUD": -75.4,
        "FATALS": 1,
        "DRUNK_DR": 0,
        "TOTALVEHICLES": 1,
        "PERSONS": 2,
        "FUNC_SYSNAME": "Local",
        "RD_OWNERNAME": "State",
        "ROUTENAME": "County",
        "RUR_URBNAME": "Rural",
        "LGT_CONDNAME": "Daylight",
        "WEATHERNAME": "Clear",
        "MAN_COLLNAME": "Not Collision",
        "HARM_EVNAME": "Pedestrian",
        "REL_ROADNAME": "On Roadway",
        "TYP_INTNAME": "Not an Intersection",
        "TWAY_ID": "US-13",
        "TWAY_ID2": "",
    }
    base.update(kw)
    return base


def test_build_final_df_column_set_matches_spec():
    """Final DF must have exactly the documented 44 columns — no more, no less.

    Uses a realistic accident row that includes FARS code columns the real
    API returns (FUNC_SYS code, WRK_ZONE, SCH_BUS, NHS, notification/arrival
    times, etc.). The pipeline must drop everything not in the 44-col spec.
    """
    raw = _synthetic_accident_row()
    # Inject extra FARS fields the real CrashAPI returns — these must NOT
    # leak into the final output (they aren't in the documented schema).
    raw.update({
        "FUNC_SYS": 5,      # code counterpart to FUNC_SYSNAME
        "RD_OWNER": 2,
        "ROUTE": 3,
        "RUR_URB": 2,
        "LGT_COND": 1,
        "WEATHER": 1,
        "MAN_COLL": 0,
        "HARM_EV": 8,
        "REL_ROAD": 1,
        "TYP_INT": 1,
        "WRK_ZONE": 0,
        "SCH_BUS": 0,
        "NHS": 1,
        "SP_JUR": 0,
        "SP_JURNAME": "No Special Jurisdiction",
        "NOT_HOUR": 12,
        "NOT_MIN": 32,
        "ARR_HOUR": 12,
        "ARR_MIN": 40,
        "HOSP_HR": 13,
        "HOSP_MN": 5,
    })
    accident = pd.DataFrame([raw])
    persons = pd.DataFrame([
        _person_row(ST_CASE=1, YEAR=2020, PER_TYP=1, AGE=30),
        _person_row(ST_CASE=1, YEAR=2020, PER_TYP=5, INJ_SEV=4),
    ])
    vehicles = pd.DataFrame([_vehicle_row(ST_CASE=1, YEAR=2020)])
    out = gfd.build_final_df(accident, persons, vehicles)
    got = set(out.columns)
    missing = EXPECTED_FINAL_COLS - got
    extra = got - EXPECTED_FINAL_COLS
    assert not missing, f"missing expected columns: {sorted(missing)}"
    assert not extra, f"unexpected extra columns: {sorted(extra)}"
    assert len(out.columns) == 44


def test_build_final_df_empty_accident():
    out = gfd.build_final_df(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert out.empty


def test_build_final_df_backfills_missing_text_labels():
    """Older FARS years (pre-2015) sometimes ship bulk CSVs without the
    *NAME text-label columns. The final DF must still have all 44 columns,
    with the missing text labels filled in as NaN — downstream consumers
    rely on the fixed schema.
    """
    raw = _synthetic_accident_row()
    # Strip every text-label column to simulate an older-year bulk CSV
    for col in ("STATENAME", "COUNTYNAME", "CITYNAME", "FUNC_SYSNAME",
                "RD_OWNERNAME", "ROUTENAME", "RUR_URBNAME", "LGT_CONDNAME",
                "WEATHERNAME", "MAN_COLLNAME", "HARM_EVNAME", "REL_ROADNAME",
                "TYP_INTNAME"):
        raw.pop(col, None)
    accident = pd.DataFrame([raw])
    persons = pd.DataFrame([_person_row(ST_CASE=1, YEAR=2020, PER_TYP=1, AGE=30)])
    vehicles = pd.DataFrame([_vehicle_row(ST_CASE=1, YEAR=2020)])
    out = gfd.build_final_df(accident, persons, vehicles)
    # Contract holds even when the source is missing text-label columns
    assert set(out.columns) == EXPECTED_FINAL_COLS
    assert len(out.columns) == 44
    # The missing labels are now NaN rather than KeyError
    assert pd.isna(out["state_name"].iloc[0])
    assert pd.isna(out["functional_class"].iloc[0])
    # Required columns (latitude, case_id, fatalities) still populated
    assert out["case_id"].iloc[0] == 1
    assert out["latitude"].iloc[0] == 38.9


def test_build_final_df_flags_join():
    """End-to-end flag propagation across accident/person/vehicle joins."""
    accident = pd.DataFrame([
        _synthetic_accident_row(case=1, year=2020, LATITUDE=77.7777),  # sentinel
        _synthetic_accident_row(case=2, year=2020, LATITUDE=19.5, LONGITUD=-155.6),  # Hawaii
    ])
    persons = pd.DataFrame([
        _person_row(ST_CASE=1, YEAR=2020, DRINKING=1, PER_TYP=1, INJ_SEV=4),
        _person_row(ST_CASE=2, YEAR=2020, PER_TYP=6, INJ_SEV=4),  # bike fatal
    ])
    vehicles = pd.DataFrame([
        _vehicle_row(ST_CASE=1, YEAR=2020, SPEEDREL=1),
        _vehicle_row(ST_CASE=2, YEAR=2020, BODY_TYP=85, HIT_RUN=1),
    ])
    out = gfd.build_final_df(accident, persons, vehicles).sort_values("case_id").reset_index(drop=True)

    # Case 1: drunk + speeding, sentinel GPS masked
    assert out.loc[out["case_id"] == 1, "any_drunk"].iloc[0] == True   # noqa: E712
    assert out.loc[out["case_id"] == 1, "any_speeding"].iloc[0] == True  # noqa: E712
    assert pd.isna(out.loc[out["case_id"] == 1, "latitude"].iloc[0])

    # Case 2: Hawaii coords preserved, bike fatal, motorcycle, hit and run
    hi_row = out.loc[out["case_id"] == 2].iloc[0]
    assert hi_row["latitude"] == 19.5
    assert hi_row["longitude"] == -155.6
    assert hi_row["bike_involved"] == True         # noqa: E712
    assert hi_row["bike_fatals"] == 1
    assert hi_row["any_motorcycle"] == True        # noqa: E712
    assert hi_row["hit_and_run"] == True           # noqa: E712


# ═══════════════════════════════════════════════════════════════
#  Bulk CSV ZIP path (replaced the CrashAPI — see Fix 2)
# ═══════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def _reset_bulk_cache():
    """Clear the bulk-ZIP cache before every test to prevent cross-test leakage."""
    gfd.clear_bulk_cache()
    yield
    gfd.clear_bulk_cache()


def _make_nationwide_year(year, include_other_state=True):
    """Build synthetic nationwide Accident/Person/Vehicle DataFrames for one year.

    Contains 2 crashes in Delaware (FIPS=10) and optionally 1 crash in
    Virginia (FIPS=51) so state filtering has real work to do.
    """
    rows_a, rows_p, rows_v = [], [], []
    for seq in (1, 2):
        case = year * 100 + seq
        rows_a.append(_synthetic_accident_row(
            case=case, year=year,
            LATITUDE=38.9 + seq * 0.01,
            LONGITUD=-75.4 - seq * 0.01,
            STATE=10, STATENAME="Delaware",
        ))
        rows_p.append(_person_row(
            ST_CASE=case, YEAR=year,
            DRINKING=1 if seq == 1 else 0,
            PER_TYP=1, INJ_SEV=4, AGE=25 + seq,
        ))
        rows_p[-1]["STATE"] = 10  # filter key
        rows_v.append(_vehicle_row(
            ST_CASE=case, YEAR=year,
            SPEEDREL=1 if seq == 2 else 0,
            BODY_TYP=4,
        ))
        rows_v[-1]["STATE"] = 10
    if include_other_state:
        # One Virginia crash to verify filtering drops non-target rows
        case_va = year * 100 + 99
        rows_a.append(_synthetic_accident_row(
            case=case_va, year=year,
            LATITUDE=37.4, LONGITUD=-78.7,
            STATE=51, STATENAME="Virginia",
        ))
        rows_p.append(_person_row(
            ST_CASE=case_va, YEAR=year, PER_TYP=1, INJ_SEV=4, AGE=50,
        ))
        rows_p[-1]["STATE"] = 51
        rows_v.append(_vehicle_row(ST_CASE=case_va, YEAR=year))
        rows_v[-1]["STATE"] = 51
    return {
        "Accident": pd.DataFrame(rows_a),
        "Person": pd.DataFrame(rows_p),
        "Vehicle": pd.DataFrame(rows_v),
    }


def _preseed_bulk_cache(years):
    """Populate the module-level cache so download_fars_year_bulk is a no-op
    and no HTTP request is made.
    """
    for year in years:
        gfd._FARS_BULK_CACHE[year] = _make_nationwide_year(year)


def test_filter_to_state_basic():
    """_filter_to_state must keep only rows matching the FIPS int."""
    df = pd.DataFrame({
        "STATE": [10, 10, 51, 36],
        "ST_CASE": [1, 2, 3, 4],
    })
    de_rows = gfd._filter_to_state(df, 10)
    assert len(de_rows) == 2
    assert set(de_rows["ST_CASE"]) == {1, 2}


def test_filter_to_state_missing_column():
    """Missing STATE column must not crash — return empty."""
    df = pd.DataFrame({"ST_CASE": [1, 2]})
    out = gfd._filter_to_state(df, 10)
    assert out.empty


def test_filter_to_state_string_fips():
    """Older years ship STATE as strings — must coerce to int."""
    df = pd.DataFrame({"STATE": ["10", "10", "51"], "ST_CASE": [1, 2, 3]})
    out = gfd._filter_to_state(df, 10)
    assert len(out) == 2


def test_download_fars_year_bulk_cache_reuse():
    """A pre-seeded cache entry is returned without any network call."""
    gfd._FARS_BULK_CACHE[2020] = _make_nationwide_year(2020)
    # If this touched the network, the call would fail in the sandbox.
    result = gfd.download_fars_year_bulk(2020)
    assert result is not None
    assert not result["Accident"].empty
    # Returns the exact cached dict (identity, not a copy)
    assert result is gfd._FARS_BULK_CACHE[2020]


def test_download_fars_year_bulk_http_retry_and_failure():
    """3-attempt retry, then None + cache negative on persistent failure."""
    gfd.clear_bulk_cache()
    calls = []

    class Boom:
        def raise_for_status(self):
            raise requests.RequestException("simulated network error")

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        return Boom()

    with patch.object(gfd.requests, "get", side_effect=fake_get), \
         patch.object(gfd.time, "sleep", lambda *a, **kw: None):
        result = gfd.download_fars_year_bulk(2020)

    assert result is None
    assert len(calls) == 3
    # Negative result cached so a subsequent call doesn't hammer the CDN
    assert gfd._FARS_BULK_CACHE[2020] is None


def test_download_fars_year_bulk_extracts_csvs_from_zip():
    """Build a synthetic ZIP in memory and verify extraction + parsing."""
    gfd.clear_bulk_cache()

    acc_df = pd.DataFrame([_synthetic_accident_row(case=1, year=2020, STATE=10)])
    per_df = pd.DataFrame([_person_row(ST_CASE=1, YEAR=2020)])
    veh_df = pd.DataFrame([_vehicle_row(ST_CASE=1, YEAR=2020)])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ACCIDENT.CSV", acc_df.to_csv(index=False))
        zf.writestr("PERSON.CSV", per_df.to_csv(index=False))
        zf.writestr("VEHICLE.CSV", veh_df.to_csv(index=False))
        zf.writestr("DISTRACT.CSV", "ST_CASE,VEH_NO,MDRDSTRD\n1,1,0\n")  # noise file
    buf.seek(0)

    class FakeResponse:
        def __init__(self, content):
            self.content = content
        def raise_for_status(self):
            pass

    def fake_get(url, headers=None, timeout=None):
        assert "static.nhtsa.gov" in url
        assert "2020" in url
        assert "CrashLens" in (headers or {}).get("User-Agent", "")
        return FakeResponse(buf.getvalue())

    with patch.object(gfd.requests, "get", side_effect=fake_get):
        result = gfd.download_fars_year_bulk(2020)

    assert result is not None
    assert len(result["Accident"]) == 1
    assert len(result["Person"]) == 1
    assert len(result["Vehicle"]) == 1
    assert result["Accident"]["STATE"].iloc[0] == 10


def test_download_fars_year_bulk_handles_lowercase_filenames():
    """CSV filename matching is case-insensitive — lowercase filenames must work."""
    gfd.clear_bulk_cache()

    acc_df = pd.DataFrame([_synthetic_accident_row(case=1, year=2021, STATE=10)])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("accident.csv", acc_df.to_csv(index=False))
    buf.seek(0)

    class FakeResponse:
        content = buf.getvalue()
        def raise_for_status(self): pass

    with patch.object(gfd.requests, "get", return_value=FakeResponse()):
        result = gfd.download_fars_year_bulk(2021)

    assert result is not None
    assert len(result["Accident"]) == 1
    assert result["Person"].empty  # no person.csv in the zip


def test_download_all_datasets_filters_to_state():
    """Full end-to-end per-state slice: pre-seed cache, call the public API,
    and verify only target-state rows come back.
    """
    _preseed_bulk_cache([2020, 2021, 2022])
    result = gfd.download_all_datasets(fips="10", abbr="de",
                                       from_year=2020, to_year=2022)
    # 3 years × 2 Delaware rows = 6 accident rows (Virginia row filtered out)
    assert len(result["Accident"]) == 6
    # All rows are actually Delaware
    assert set(result["Accident"]["STATE"]) == {10}


def test_download_all_datasets_different_states_share_cache():
    """--all efficiency: per-state calls for DE then VA reuse the same year
    download, so cache_hits == 1 (only DE triggered the load).
    """
    gfd.clear_bulk_cache()
    _preseed_bulk_cache([2020])  # no network at all

    de_result = gfd.download_all_datasets(fips="10", abbr="de",
                                          from_year=2020, to_year=2020)
    va_result = gfd.download_all_datasets(fips="51", abbr="va",
                                          from_year=2020, to_year=2020)
    assert len(de_result["Accident"]) == 2
    assert len(va_result["Accident"]) == 1
    assert set(de_result["Accident"]["STATE"]) == {10}
    assert set(va_result["Accident"]["STATE"]) == {51}


# ═══════════════════════════════════════════════════════════════
#  End-to-end: process_state with a mocked bulk cache
# ═══════════════════════════════════════════════════════════════

def test_process_state_mocked_end_to_end(tmp_path, monkeypatch):
    """Exercises the full process_state() path with a pre-seeded bulk cache.

    Verifies: parquet file is written, row count matches, columns match spec,
    any_drunk/any_speeding flags reflect the synthetic input.
    """
    _preseed_bulk_cache([2020, 2021, 2022])
    monkeypatch.setattr(gfd.time, "sleep", lambda *a, **kw: None)

    de = gfd.ABBR_LOOKUP["de"]
    tag, df = gfd.process_state(
        de, tmp_path, s3=None, bucket="test-bucket",
        from_year=2020, to_year=2022,
        force=False, local_only=True,
    )
    assert tag == "completed"
    assert df is not None
    # 3 years × 2 Delaware crashes/yr = 6 rows
    assert len(df) == 6
    # Schema matches the 44-col contract
    assert set(df.columns) == EXPECTED_FINAL_COLS
    # Parquet file on disk
    pq = tmp_path / "de_fars.parquet.gz"
    assert pq.exists()
    round_trip = gfd.read_gz_parquet(pq)
    assert len(round_trip) == 6
    assert set(round_trip.columns) == EXPECTED_FINAL_COLS
    # Flag correctness: half the rows are drunk (seq==1), half speeding (seq==2)
    assert round_trip["any_drunk"].sum() == 3
    assert round_trip["any_speeding"].sum() == 3


def test_process_state_skip_local_cached(tmp_path, monkeypatch):
    """If --local-only and file already exists, skip without re-fetching."""
    _preseed_bulk_cache([2020])
    monkeypatch.setattr(gfd.time, "sleep", lambda *a, **kw: None)

    de = gfd.ABBR_LOOKUP["de"]
    gfd.process_state(de, tmp_path, s3=None, bucket="x",
                      from_year=2020, to_year=2020,
                      force=False, local_only=True)

    # Clear cache so any reload attempt would have to go to the network
    gfd.clear_bulk_cache()

    def boom(*a, **kw):
        raise AssertionError("download_fars_year_bulk should not be called on skip")
    monkeypatch.setattr(gfd, "download_fars_year_bulk", boom)

    tag, df = gfd.process_state(de, tmp_path, s3=None, bucket="x",
                                from_year=2020, to_year=2020,
                                force=False, local_only=True)
    assert tag == "skipped"
    assert df is not None
    assert len(df) == 2


def test_build_nationwide_multi_state(tmp_path):
    """Nationwide rollup concatenates per-state DataFrames and writes a file."""
    a = pd.DataFrame({"case_id": [1, 2], "crash_year": [2020, 2021]})
    b = pd.DataFrame({"case_id": [3], "crash_year": [2022]})
    gfd.build_nationwide([a, b], tmp_path, s3=None, bucket="x", local_only=True)
    pq = tmp_path / "fars_nationwide.parquet.gz"
    assert pq.exists()
    combined = gfd.read_gz_parquet(pq)
    assert len(combined) == 3
    assert set(combined["case_id"]) == {1, 2, 3}


def test_build_nationwide_not_enough_states(tmp_path, capsys):
    """Single-state list → no rollup written (needs ≥2)."""
    a = pd.DataFrame({"case_id": [1]})
    gfd.build_nationwide([a], tmp_path, s3=None, bucket="x", local_only=True)
    assert not (tmp_path / "fars_nationwide.parquet.gz").exists()


# ═══════════════════════════════════════════════════════════════
#  CLI smoke test (argparse wiring)
# ═══════════════════════════════════════════════════════════════

def test_cli_help_runs():
    """--help must exit 0 and print a usage banner."""
    import subprocess
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "generate_fars_data.py"), "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert "FARS" in result.stdout or "FARS" in result.stderr
    assert "--state" in result.stdout
    assert "--from-year" in result.stdout


def test_cli_rejects_invalid_year_range():
    """--from-year > --to-year must exit non-zero."""
    import subprocess
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "generate_fars_data.py"),
         "--state", "de", "--local-only", "--from-year", "2023", "--to-year", "2020"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode != 0


# (The User-Agent test for the old CrashAPI path was removed in Fix 2 —
# see test_download_fars_year_bulk_extracts_csvs_from_zip for the
# equivalent header assertion on the bulk-CSV path.)
