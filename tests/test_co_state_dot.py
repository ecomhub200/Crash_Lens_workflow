"""
Bug test for states/colorado/co_state_dot.py — CDOT Highways normalizer.

Covers the Colorado-specific divergences from the Delaware reference that
could silently regress if `normalize()` is touched without care:

  1. FUNCCLASS text lookup + substring fallback (CDOT uses text, not codes).
  2. PRISURF "Composite" → Blacktop — NOT brick (regression guard for the
     SURFACE_LABELS[7] fix recorded in wiki/log.md 2026-04-18).
  3. SPEEDLIM direct passthrough (CDOT's big win over DelDOT's FC+area
     estimate) — actual posted speeds must NOT be overwritten.
  4. Layer-7-is-CDOT-only → Ownership hardcoded to State Hwy Agency.
  5. ISDIVIDED Y/N → Facility Type (no median-code heuristic).
  6. Zero-padded route numbers ("025" → "I 25").
  7. Through_Lanes cap at 12, Lane_Width_ft clip 8-16 ft.
  8. Paved-override guard: Interstates/Freeways/Arterials never "Dirt".
  9. FIELD_MAP / constant sanity (SPEEDLIM present, FC_TEXT_MAP non-empty).

Run:   pytest tests/test_co_state_dot.py -v
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add project root to path so `states.colorado.co_state_dot` imports
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from states.colorado import co_state_dot as co


# ═══════════════════════════════════════════════════════════════
#  HELPERS — build a single-row DataFrame with CDOT-shaped inputs
# ═══════════════════════════════════════════════════════════════

_DEFAULT_ROW = {
    "dot_fc_text": "Rural Minor Arterial",
    "dot_route_sign": "SH",
    "dot_nhs": "",
    "dot_access_control": "",
    "dot_route_number": "50",
    "dot_road_name": "",
    "dot_description": "",
    "dot_county_name": "Mesa",
    "dot_county_fips": "077",
    "dot_city": "",
    "dot_city_fips": "",
    "dot_region_code": "3",
    "dot_tpr_id": "",
    "dot_terrain": "Flat",
    "dot_lanes": "2",
    "dot_lane_width": "12",
    "dot_surface_type": "Asphalt",
    "dot_is_divided": "N",
    "dot_median_type": "",
    "dot_median_width": "0",
    "dot_shoulder_type": "",
    "dot_shoulder_width": "4",
    "dot_aadt": "",
    "dot_aadt_single": "",
    "dot_aadt_combo": "",
    "dot_speed_limit": "55",
    "dot_vmt": "",
    "dot_vc_ratio": "",
    "dot_beg_mp": "0",
    "dot_end_mp": "1",
    "dot_seg_length": "1",
}


def _mk(**overrides):
    """Build a 1-row DataFrame with CDOT-shaped columns, applying overrides."""
    row = dict(_DEFAULT_ROW)
    row.update(overrides)
    return pd.DataFrame([row])


def _norm(**overrides):
    """Build row, run normalize(), return the 1-row result."""
    return co.normalize(_mk(**overrides))


# ═══════════════════════════════════════════════════════════════
#  1. FUNCCLASS text lookup + substring fallback
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("fc_txt,expected", [
    ("Rural Interstate",                  "1-Interstate"),
    ("Urban Interstate",                  "1-Interstate"),
    ("Rural Freeway and Expressway",      "2-Freeway/Expressway"),
    ("Urban Freeway and Expressway",      "2-Freeway/Expressway"),
    ("Rural Principal Arterial - Other",  "3-Principal Arterial"),
    ("Urban Principal Arterial - Other",  "3-Principal Arterial"),
    ("Rural Minor Arterial",              "4-Minor Arterial"),
    ("Urban Minor Arterial",              "4-Minor Arterial"),
    ("Rural Major Collector",             "5-Major Collector"),
    ("Urban Major Collector",             "5-Major Collector"),
    ("Rural Minor Collector",             "6-Minor Collector"),
    ("Urban Minor Collector",             "6-Minor Collector"),
    ("Rural Local",                       "7-Local"),
    ("Urban Local",                       "7-Local"),
])
def test_fc_text_exact_matches(fc_txt, expected):
    out = _norm(dot_fc_text=fc_txt)
    assert out["Functional Class"].iloc[0] == expected


@pytest.mark.parametrize("fc_txt,expected", [
    # CDOT occasionally emits variants not in FC_TEXT_MAP — substring fallback
    # must still resolve them to the correct CrashLens class.
    ("Urban Other Freeway",               "2-Freeway/Expressway"),
    ("Rural Principal Arterial",          "3-Principal Arterial"),
    ("Urban Minor Arterial - Divided",    "4-Minor Arterial"),
    ("SOME URBAN LOCAL ROAD",             "7-Local"),
])
def test_fc_text_substring_fallback(fc_txt, expected):
    out = _norm(dot_fc_text=fc_txt)
    assert out["Functional Class"].iloc[0] == expected, \
        f"substring fallback failed for {fc_txt!r}"


def test_fc_empty_remains_empty():
    out = _norm(dot_fc_text="")
    assert out["Functional Class"].iloc[0] == ""


def test_system_derived_from_fc():
    assert _norm(dot_fc_text="Rural Interstate")["SYSTEM"].iloc[0] == "DOT Interstate"
    assert _norm(dot_fc_text="Urban Minor Arterial")["SYSTEM"].iloc[0] == "DOT Secondary"
    assert _norm(dot_fc_text="Rural Local")["SYSTEM"].iloc[0] == "Non-DOT secondary"


# ═══════════════════════════════════════════════════════════════
#  2. PRISURF "Composite" → Blacktop (SURFACE_LABELS[7] regression)
# ═══════════════════════════════════════════════════════════════

def test_composite_surface_maps_to_blacktop_not_brick():
    """CDOT 'Composite' = PCC with asphalt overlay. Must NOT be Brick.
    Mirrors the SURFACE_LABELS[7] fix in wiki/log.md [2026-04-18]."""
    out = _norm(dot_surface_type="Composite")
    assert out["Roadway Surface Type"].iloc[0] == "2. Blacktop, Asphalt, Bituminous"
    # Positive guard: never the word 'Brick' anywhere in the resolved value.
    assert "Brick" not in out["Roadway Surface Type"].iloc[0]


@pytest.mark.parametrize("raw,expected", [
    ("Asphalt",    "2. Blacktop, Asphalt, Bituminous"),
    ("Bituminous", "2. Blacktop, Asphalt, Bituminous"),
    ("Concrete",   "1. Concrete"),
    ("Composite",  "2. Blacktop, Asphalt, Bituminous"),
    ("Gravel",     "4. Slag, Gravel, Stone"),
    ("Stone",      "4. Slag, Gravel, Stone"),
    ("Dirt",       "5. Dirt"),
    ("Unpaved",    "5. Dirt"),
    ("Other",      "6. Other"),
])
def test_surface_exact_matches(raw, expected):
    # Use a local FC so the "paved override" guard doesn't fire.
    out = _norm(dot_fc_text="Rural Local", dot_surface_type=raw)
    assert out["Roadway Surface Type"].iloc[0] == expected


@pytest.mark.parametrize("raw,expected", [
    ("Asphalt Concrete Pavement", "2. Blacktop, Asphalt, Bituminous"),
    ("Bituminous Surface",        "2. Blacktop, Asphalt, Bituminous"),
    ("Portland Cement Concrete",  "1. Concrete"),
    ("Gravel Surface",            "4. Slag, Gravel, Stone"),
    ("Native Dirt",               "5. Dirt"),
])
def test_surface_substring_fallback(raw, expected):
    out = _norm(dot_fc_text="Rural Local", dot_surface_type=raw)
    assert out["Roadway Surface Type"].iloc[0] == expected


def test_surface_empty_defaults_to_blacktop():
    out = _norm(dot_fc_text="Rural Local", dot_surface_type="")
    assert out["Roadway Surface Type"].iloc[0] == "2. Blacktop, Asphalt, Bituminous"


def test_paved_override_interstate_never_dirt():
    """FC 1/2/3 segments with an inventory error (PRISURF='Dirt') must be
    forced to Blacktop. Matches Delaware's equivalent guard."""
    out = _norm(dot_fc_text="Rural Interstate", dot_surface_type="Dirt")
    assert out["Roadway Surface Type"].iloc[0] == "2. Blacktop, Asphalt, Bituminous"


def test_paved_override_principal_arterial_never_gravel():
    out = _norm(dot_fc_text="Urban Principal Arterial - Other",
                dot_surface_type="Gravel")
    assert out["Roadway Surface Type"].iloc[0] == "2. Blacktop, Asphalt, Bituminous"


def test_paved_override_does_not_touch_local_roads():
    """FC 7 (Local) on Gravel stays Gravel — the override is FC 1-3 only."""
    out = _norm(dot_fc_text="Rural Local", dot_surface_type="Gravel")
    assert out["Roadway Surface Type"].iloc[0] == "4. Slag, Gravel, Stone"


# ═══════════════════════════════════════════════════════════════
#  3. SPEEDLIM direct passthrough
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("raw,expected", [
    ("75", "75"),
    ("65", "65"),
    ("55", "55"),
    ("45", "45"),
    ("25", "25"),
])
def test_speed_limit_uses_actual_posted_value(raw, expected):
    """CDOT's big win over DelDOT: use SPEEDLIM directly, don't overwrite
    it with an FC+area-derived estimate."""
    out = _norm(dot_speed_limit=raw)
    assert out["Speed_Limit_Est"].iloc[0] == expected


def test_speed_limit_interstate_with_low_posted_is_preserved():
    """Even on an Interstate (where DelDOT would return 65), the actual CDOT
    posted value must be honored — it's ACTUAL, not estimated."""
    out = _norm(dot_fc_text="Rural Interstate", dot_speed_limit="55")
    assert out["Speed_Limit_Est"].iloc[0] == "55"


def test_speed_limit_empty_stays_empty():
    out = _norm(dot_speed_limit="")
    assert out["Speed_Limit_Est"].iloc[0] == ""


def test_speed_limit_zero_stays_empty():
    out = _norm(dot_speed_limit="0")
    assert out["Speed_Limit_Est"].iloc[0] == ""


# ═══════════════════════════════════════════════════════════════
#  4. Ownership — hardcoded State Hwy Agency
# ═══════════════════════════════════════════════════════════════

def test_ownership_always_state_hwy_agency():
    """Layer 7 is CDOT-maintained only; Ownership must be 100% State Hwy Agency."""
    df = pd.concat([
        _mk(dot_fc_text="Rural Interstate"),
        _mk(dot_fc_text="Urban Local"),
        _mk(dot_fc_text="Rural Major Collector"),
    ], ignore_index=True)
    out = co.normalize(df)
    assert (out["Ownership"] == "1. State Hwy Agency").all()


# ═══════════════════════════════════════════════════════════════
#  5. Facility Type from ISDIVIDED
# ═══════════════════════════════════════════════════════════════

def test_facility_type_divided_yes():
    out = _norm(dot_is_divided="Y")
    assert out["Facility Type"].iloc[0] == "4-Two-Way Divided"


def test_facility_type_divided_no():
    out = _norm(dot_is_divided="N")
    assert out["Facility Type"].iloc[0] == "3-Two-Way Undivided"


def test_facility_type_divided_lowercase_y():
    """CDOT sometimes lowercases — our normalizer uppercases before lookup."""
    out = _norm(dot_is_divided="y")
    assert out["Facility Type"].iloc[0] == "4-Two-Way Divided"


def test_facility_type_empty_defaults_to_undivided():
    out = _norm(dot_is_divided="")
    assert out["Facility Type"].iloc[0] == "3-Two-Way Undivided"


def test_roadway_description_follows_facility_type():
    out_div = _norm(dot_is_divided="Y")
    out_und = _norm(dot_is_divided="N")
    assert out_div["Roadway Description"].iloc[0] == \
        "2. Two-Way, Divided, Unprotected Median"
    assert out_und["Roadway Description"].iloc[0] == \
        "1. Two-Way, Not Divided"


# ═══════════════════════════════════════════════════════════════
#  6. RTE Name — sign + zero-stripped route number
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("sign,num,expected", [
    ("I",  "025",  "I 25"),       # classic I-25
    ("I",  "070",  "I 70"),       # I-70
    ("US", "050",  "US 50"),
    ("US", "6",    "US 6"),       # already single digit
    ("SH", "83",   "SH 83"),
    ("SH", "009",  "SH 9"),
])
def test_rte_name_strips_leading_zeros(sign, num, expected):
    out = _norm(dot_route_sign=sign, dot_route_number=num)
    assert out["RTE Name"].iloc[0] == expected


def test_rte_name_empty_when_sign_missing():
    out = _norm(dot_route_sign="", dot_route_number="025")
    assert out["RTE Name"].iloc[0] == ""


def test_rte_name_empty_when_number_missing():
    out = _norm(dot_route_sign="I", dot_route_number="")
    assert out["RTE Name"].iloc[0] == ""


def test_rte_name_all_zeros_preserves_zero():
    """"000" must not collapse to empty — render as "SH 0" so the row
    still produces a RTE Name (handles the SH 0 edge case)."""
    out = _norm(dot_route_sign="SH", dot_route_number="000")
    assert out["RTE Name"].iloc[0] == "SH 0"


# ═══════════════════════════════════════════════════════════════
#  7. Through_Lanes cap + Lane_Width_ft clip
# ═══════════════════════════════════════════════════════════════

def test_through_lanes_cap_at_12():
    out = _norm(dot_lanes="99")
    assert out["Through_Lanes"].iloc[0] == "12"


def test_through_lanes_normal_value():
    out = _norm(dot_lanes="4")
    assert out["Through_Lanes"].iloc[0] == "4"


def test_through_lanes_zero_becomes_empty():
    out = _norm(dot_lanes="0")
    assert out["Through_Lanes"].iloc[0] == ""


def test_lane_width_clipped_low():
    out = _norm(dot_lane_width="3")
    assert out["Lane_Width_ft"].iloc[0] == 8


def test_lane_width_clipped_high():
    out = _norm(dot_lane_width="50")
    assert out["Lane_Width_ft"].iloc[0] == 16


def test_lane_width_zero_stays_zero():
    out = _norm(dot_lane_width="0")
    assert out["Lane_Width_ft"].iloc[0] == 0


def test_lane_width_in_range_passthrough():
    out = _norm(dot_lane_width="12")
    assert out["Lane_Width_ft"].iloc[0] == 12


# ═══════════════════════════════════════════════════════════════
#  8. Segment length — prefer SEG_LENGTH over end-begin
# ═══════════════════════════════════════════════════════════════

def test_segment_length_prefers_seg_length_field():
    out = _norm(dot_seg_length="2.5", dot_beg_mp="0", dot_end_mp="9999")
    assert out["Segment_Length_mi"].iloc[0] == 2.5


def test_segment_length_falls_back_to_end_minus_begin():
    out = _norm(dot_seg_length="", dot_beg_mp="10", dot_end_mp="12.5")
    assert out["Segment_Length_mi"].iloc[0] == 2.5


def test_segment_length_negative_end_minus_begin_clipped_to_zero():
    """If end < begin (shouldn't happen but guard anyway), clip to 0."""
    out = _norm(dot_seg_length="", dot_beg_mp="100", dot_end_mp="50")
    assert out["Segment_Length_mi"].iloc[0] == 0


# ═══════════════════════════════════════════════════════════════
#  9. Area Type derivation from Rural/Urban prefix
# ═══════════════════════════════════════════════════════════════

def test_area_type_urban_prefix():
    assert _norm(dot_fc_text="Urban Minor Arterial")["Area Type"].iloc[0] == "Urban"


def test_area_type_rural_prefix():
    assert _norm(dot_fc_text="Rural Interstate")["Area Type"].iloc[0] == "Rural"


def test_area_type_unknown_defaults_to_rural():
    """Colorado is predominantly rural off the Front Range — safe default."""
    assert _norm(dot_fc_text="")["Area Type"].iloc[0] == "Rural"


# ═══════════════════════════════════════════════════════════════
# 10. DOT District from CDOT region code
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("code,expected_substring", [
    ("1", "Denver Metro"),
    ("2", "Pueblo"),
    ("3", "Grand Junction"),
    ("4", "Greeley"),
    ("5", "Durango"),
])
def test_dot_district_region_lookup(code, expected_substring):
    out = _norm(dot_region_code=code)
    assert expected_substring in out["DOT District"].iloc[0]


def test_dot_district_handles_trailing_dot_zero():
    """CDOT sometimes returns region as '1.0' from a numeric field —
    our normalizer strips trailing '.0' before lookup."""
    out = _norm(dot_region_code="1.0")
    assert "Denver Metro" in out["DOT District"].iloc[0]


# ═══════════════════════════════════════════════════════════════
# 11. NHS / source tracking
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("raw,expected", [
    ("Y",       "Yes"),
    ("Yes",     "Yes"),
    ("1",       "Yes"),
    ("NHS",     "Yes"),
    ("",        "No"),
    ("N",       "No"),
    ("0",       "No"),
])
def test_is_nhs(raw, expected):
    out = _norm(dot_nhs=raw)
    assert out["Is_NHS"].iloc[0] == expected


def test_source_tracking_fields_set():
    out = _norm()
    assert out["dot_source"].iloc[0] == "CDOT Highways Inventory"
    assert out["dot_source_url"].iloc[0] == co.ENDPOINT_URL


def test_sidewalk_guardrail_bikelane_left_empty():
    """CDOT Layer 7 doesn't expose these — should NOT invent values."""
    out = _norm()
    assert out["Has_Sidewalk"].iloc[0] == ""
    assert out["Guardrail Related?"].iloc[0] == ""
    assert out["Has_Bike_Lane"].iloc[0] == ""


# ═══════════════════════════════════════════════════════════════
# 12. FIELD_MAP / constant sanity
# ═══════════════════════════════════════════════════════════════

def test_constants_expected_shape():
    assert co.STATE_ABBR == "co"
    assert co.STATE_FIPS == "08"
    assert co.STATE_NAME == "Colorado"
    assert co.STATE_DOT == "CDOT"
    assert co.MAX_RECORD_COUNT == 1000
    assert co.OUT_SR == 4326
    assert co.OUT_FIELDS is None


def test_endpoint_url_points_at_cdot_layer_7():
    assert "coloradodot.info" in co.ENDPOINT_URL
    assert co.ENDPOINT_URL.rstrip("/").endswith("/7")


def test_field_map_covers_critical_cdot_fields():
    """The fields we actually transform in normalize() must be in FIELD_MAP."""
    required = {
        "FUNCCLASS", "SPEEDLIM", "ISDIVIDED", "PRISURF",
        "ROUTESIGN", "ROUTE", "REGION", "COUNTY",
        "THRULNQTY", "THRULNWD", "REFPT", "ENDREFPT",
    }
    missing = required - set(co.FIELD_MAP)
    assert not missing, f"FIELD_MAP missing required CDOT fields: {missing}"


def test_field_map_targets_have_dot_prefix():
    """Every target column must start with 'dot_' per the column naming rule."""
    bad = [v for v in co.FIELD_MAP.values() if not v.startswith("dot_")]
    assert not bad, f"FIELD_MAP has non-dot_ target columns: {bad}"


def test_fc_text_map_has_all_fourteen_prefixes():
    """Rural + Urban × 7 functional classes = 14 exact matches."""
    assert len(co.FC_TEXT_MAP) == 14


def test_fc_to_system_matches_delaware_standard():
    """This dict is supposed to be the CrashLens cross-state standard,
    identical to Delaware's. If it drifts, enrichment breaks."""
    expected = {
        "1-Interstate":           "DOT Interstate",
        "2-Freeway/Expressway":   "DOT Primary",
        "3-Principal Arterial":   "DOT Primary",
        "4-Minor Arterial":       "DOT Secondary",
        "5-Major Collector":      "DOT Secondary",
        "6-Minor Collector":      "Non-DOT primary",
        "7-Local":                "Non-DOT secondary",
    }
    assert co.FC_TO_SYSTEM == expected


def test_surface_type_map_has_no_brick_value():
    """Regression guard for SURFACE_LABELS[7] fix (wiki/log.md 2026-04-18)."""
    for k, v in co.SURFACE_TYPE_MAP.items():
        assert "Brick" not in v, \
            f"SURFACE_TYPE_MAP[{k!r}]={v!r} — must not contain 'Brick'"


# ═══════════════════════════════════════════════════════════════
# 13. Multi-row sanity — no row-level leakage between states
# ═══════════════════════════════════════════════════════════════

def test_multi_row_independent_normalization():
    df = pd.concat([
        _mk(dot_fc_text="Rural Interstate",    dot_speed_limit="75", dot_is_divided="Y"),
        _mk(dot_fc_text="Urban Local",         dot_speed_limit="25", dot_is_divided="N"),
        _mk(dot_fc_text="Rural Major Collector", dot_speed_limit="55", dot_is_divided="N"),
    ], ignore_index=True)
    out = co.normalize(df)

    assert out["Functional Class"].tolist() == [
        "1-Interstate", "7-Local", "5-Major Collector",
    ]
    assert out["Speed_Limit_Est"].tolist() == ["75", "25", "55"]
    assert out["Facility Type"].tolist() == [
        "4-Two-Way Divided", "3-Two-Way Undivided", "3-Two-Way Undivided",
    ]
    assert (out["Ownership"] == "1. State Hwy Agency").all()
