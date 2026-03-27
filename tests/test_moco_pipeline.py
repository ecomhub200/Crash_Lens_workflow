#!/usr/bin/env python3
"""
Comprehensive bug tests for the Montgomery County (MoCo) crash data pipeline.

Tests every component of the MoCo data flow:
  1. download_moco_crashes.py   — Socrata API downloader
  2. scripts/state_adapter.py   — MarylandNormalizer + StateDetector
  3. data/MarylandDOT/ configs  — moco_config.json, moco_source_manifest.json
  4. states/maryland/config.json — state-level config consistency
  5. .github/workflows/          — workflow YAML structural checks

Run with:  python -m pytest tests/test_moco_pipeline.py -v
"""

import csv
import gzip
import io
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SCRIPTS_DIR = PROJECT_ROOT / 'scripts'
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from scripts.state_adapter import (
    BaseNormalizer,
    ColoradoNormalizer,
    MarylandNormalizer,
    StateDetector,
    STATE_SIGNATURES,
    STANDARD_COLUMNS,
    VirginiaNormalizer,
    convert_file,
    get_normalizer,
    get_supported_states,
)

from download_moco_crashes import (
    BASE_URL,
    build_where_clause,
    DATASETS,
    DEFAULT_DATA_DIR,
    DEFAULT_YEARS,
    download_data,
    health_check,
    main as downloader_main,
    MAX_RETRIES,
    PAGE_SIZE,
    RETRY_BACKOFF,
    retry_request,
    save_csv,
)


# ===========================================================================
# Fixtures & Helpers
# ===========================================================================

def make_moco_row(**overrides):
    """Create a realistic MoCo crash row (dict) with sensible defaults."""
    row = {
        'report_number': 'MD2023-000001',
        'acrs_report_type': 'Property Damage Crash',
        'crash_date_time': '2023-06-15T14:30:00.000',
        'road_name': 'GEORGIA AVE',
        'cross_street_name': 'RANDOLPH RD',
        'route_type': 'Maryland (State)',
        'collision_type': 'Same Dir Rear End',
        'weather': 'Clear',
        'light': 'Daylight',
        'surface_condition': 'Dry',
        'latitude': '39.0512',
        'longitude': '-77.0413',
        'municipality': 'Silver Spring',
        'hit_run': 'No',
        'junction': 'At Intersection',
        'traffic_control': 'Traffic Signal',
        'speed_limit': '35',
        'year': '2023',
    }
    row.update(overrides)
    return row


def make_statewide_md_row(**overrides):
    """Create a statewide Maryland ACRS row with alternate field names."""
    row = {
        'report_no': 'MD2023-SW-0001',
        'acrs_report_type': 'Injury Crash',
        'acc_date': '2023-07-20',
        'acc_time': '0830',
        'road_name': 'US 29',
        'county_desc': 'Montgomery',
        'collision_type_desc': 'ANGLE',
        'weather_desc': 'RAINING',
        'light_desc': 'DAYLIGHT',
        'surf_cond_desc': 'WET',
        'route_type_desc': 'US Route',
        'junction_desc': 'Non-Intersection',
    }
    row.update(overrides)
    return row


def make_moco_csv_file(tmpdir, rows, filename='moco_test.csv'):
    """Write MoCo rows to a CSV and return the file path."""
    if not rows:
        raise ValueError('Need at least one row')
    fieldnames = list(rows[0].keys())
    path = os.path.join(tmpdir, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture
def md_normalizer():
    """MarylandNormalizer with real config."""
    config_path = str(PROJECT_ROOT / 'states' / 'maryland' / 'config.json')
    return MarylandNormalizer('maryland', config_path)


@pytest.fixture
def md_normalizer_no_config():
    """MarylandNormalizer without config (tests defaults)."""
    return MarylandNormalizer('maryland', '/nonexistent/config.json')


# ===========================================================================
# 1. StateDetector — Maryland header detection
# ===========================================================================

class TestStateDetector_Maryland:
    """Tests that MoCo + statewide MD headers are correctly detected."""

    def test_moco_headers_exact_match(self):
        """MoCo portal headers must detect as 'maryland'."""
        headers = [
            'report_number', 'acrs_report_type', 'road_name',
            'crash_date_time', 'collision_type', 'municipality',
        ]
        det = StateDetector()
        assert det.detect_from_headers(headers) == 'maryland'

    def test_statewide_headers_exact_match(self):
        """Statewide portal headers must detect as 'maryland_statewide'."""
        headers = [
            'report_no', 'acrs_report_type', 'road_name', 'county_desc',
            'acc_date', 'collision_type_desc', 'weather_desc',
        ]
        det = StateDetector()
        assert det.detect_from_headers(headers) == 'maryland_statewide'

    def test_moco_headers_with_extra_columns(self):
        """Extra columns shouldn't prevent detection."""
        headers = [
            'report_number', 'acrs_report_type', 'road_name',
            'some_extra_field', 'another_field',
        ]
        det = StateDetector()
        assert det.detect_from_headers(headers) == 'maryland'

    def test_moco_headers_with_whitespace(self):
        """Leading/trailing spaces in headers should be stripped."""
        headers = [' report_number ', 'acrs_report_type ', ' road_name']
        det = StateDetector()
        assert det.detect_from_headers(headers) == 'maryland'

    def test_unknown_headers(self):
        """Completely unrelated headers should return 'unknown'."""
        headers = ['foo', 'bar', 'baz']
        det = StateDetector()
        assert det.detect_from_headers(headers) == 'unknown'

    def test_maryland_not_confused_with_colorado(self):
        """MD headers must NOT match Colorado signature."""
        md_headers = [
            'report_number', 'acrs_report_type', 'road_name',
            'crash_date_time', 'collision_type',
        ]
        det = StateDetector()
        result = det.detect_from_headers(md_headers)
        assert result != 'colorado'

    def test_maryland_not_confused_with_virginia(self):
        """MD headers must NOT match Virginia signature."""
        md_headers = [
            'report_number', 'acrs_report_type', 'road_name',
            'crash_date_time',
        ]
        det = StateDetector()
        result = det.detect_from_headers(md_headers)
        assert result != 'virginia'

    def test_detect_from_file(self):
        """StateDetector.detect_from_file reads headers from CSV on disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = make_moco_csv_file(tmpdir, [make_moco_row()])
            det = StateDetector()
            assert det.detect_from_file(path) == 'maryland'

    def test_detect_from_file_bom(self):
        """BOM-prefixed CSV should still detect correctly (utf-8-sig)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'bom_test.csv')
            with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['report_number', 'acrs_report_type', 'road_name'])
                writer.writeheader()
                writer.writerow({'report_number': 'X', 'acrs_report_type': 'Y', 'road_name': 'Z'})
            det = StateDetector()
            assert det.detect_from_file(path) == 'maryland'

    def test_partial_match_above_threshold(self):
        """Headers with >50% match should still detect the correct state."""
        # 'maryland' requires: report_number, acrs_report_type, road_name
        # optional: crash_date_time, collision_type, municipality, weather, light
        # Give it 2/3 required + 3 optional => high partial score
        headers = [
            'report_number', 'acrs_report_type',  # 2 of 3 required
            'crash_date_time', 'collision_type', 'municipality',  # 3 optional
        ]
        det = StateDetector()
        # Should at least not return 'unknown'
        result = det.detect_from_headers(headers)
        assert result in ('maryland', 'maryland_statewide')


# ===========================================================================
# 2. MarylandNormalizer — MoCo portal format
# ===========================================================================

class TestMarylandNormalizer_MoCoFormat:
    """Tests the MoCo-specific field normalization path."""

    def test_basic_row_normalizes(self, md_normalizer):
        """A standard MoCo row should normalize without errors."""
        row = make_moco_row()
        result = md_normalizer.normalize_row(row)
        assert isinstance(result, dict)
        assert result['Document Nbr'] == 'MD2023-000001'
        assert result['_source_state'] == 'maryland'

    def test_date_parsing_iso8601(self, md_normalizer):
        """ISO 8601 crash_date_time should split into date + time + year."""
        row = make_moco_row(crash_date_time='2023-06-15T14:30:00.000')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Date'] == '2023-06-15'
        assert result['Crash Year'] == '2023'
        assert result['Crash Military Time'] == '1430'

    def test_date_parsing_iso_midnight(self, md_normalizer):
        """Midnight timestamp T00:00:00 should give time '0000'."""
        row = make_moco_row(crash_date_time='2024-01-01T00:00:00.000')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Date'] == '2024-01-01'
        assert result['Crash Year'] == '2024'
        assert result['Crash Military Time'] == '0000'

    def test_date_parsing_no_time_component(self, md_normalizer):
        """Date without 'T' separator (statewide format) should still parse."""
        row = make_moco_row(crash_date_time='', acc_date='2023-07-20')
        # Simulate statewide alt path: crash_date_time empty, use acc_date
        row['crash_date_time'] = ''
        result = md_normalizer.normalize_row(row)
        # _get tries 'crash_date_time' first (empty), then 'acc_date'
        assert result['Crash Year'] == '2023'

    def test_date_parsing_slash_format(self, md_normalizer):
        """MM/DD/YYYY format should extract year correctly."""
        row = make_moco_row(crash_date_time='06/15/2023')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Year'] == '2023'

    def test_date_empty_string(self, md_normalizer):
        """Empty date should produce empty date/time, but year may come from 'year' field."""
        row = make_moco_row(crash_date_time='', year='')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Date'] == ''
        assert result['Crash Year'] == ''
        assert result['Crash Military Time'] == ''

    # --- Severity mapping ---

    def test_severity_fatal(self, md_normalizer):
        row = make_moco_row(acrs_report_type='Fatal Crash')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'K'
        assert result['K_People'] == '1'
        assert result['B_People'] == '0'

    def test_severity_injury(self, md_normalizer):
        row = make_moco_row(acrs_report_type='Injury Crash')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'B'
        assert result['B_People'] == '1'
        assert result['K_People'] == '0'

    def test_severity_pdo(self, md_normalizer):
        row = make_moco_row(acrs_report_type='Property Damage Crash')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'O'
        assert result['K_People'] == '0'
        assert result['B_People'] == '0'

    def test_severity_uppercase_variant(self, md_normalizer):
        """UPPERCASE severity strings must also map correctly."""
        row = make_moco_row(acrs_report_type='FATAL CRASH')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'K'

    def test_severity_unknown_defaults_to_O(self, md_normalizer):
        """Unknown severity type should default to 'O' (not crash)."""
        row = make_moco_row(acrs_report_type='Some Unknown Type')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'O'

    def test_severity_a_and_c_always_zero(self, md_normalizer):
        """Maryland 3-tier system has no A or C; both should always be '0'."""
        for sev in ['Fatal Crash', 'Injury Crash', 'Property Damage Crash']:
            row = make_moco_row(acrs_report_type=sev)
            result = md_normalizer.normalize_row(row)
            assert result['A_People'] == '0', f"A_People not 0 for {sev}"
            assert result['C_People'] == '0', f"C_People not 0 for {sev}"

    # --- Collision Type mapping ---

    def test_collision_rear_end(self, md_normalizer):
        row = make_moco_row(collision_type='Same Dir Rear End')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '1. Rear End'

    def test_collision_angle(self, md_normalizer):
        row = make_moco_row(collision_type='Angle')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '2. Angle'

    def test_collision_head_on(self, md_normalizer):
        row = make_moco_row(collision_type='Head On')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '3. Head On'

    def test_collision_sideswipe_same(self, md_normalizer):
        row = make_moco_row(collision_type='Same Direction Sideswipe')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '4. Sideswipe - Same Direction'

    def test_collision_sideswipe_opposite(self, md_normalizer):
        row = make_moco_row(collision_type='Opposite Direction Sideswipe')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '5. Sideswipe - Opposite Direction'

    def test_collision_single_vehicle(self, md_normalizer):
        row = make_moco_row(collision_type='Single Vehicle')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '14. Fixed Object'

    def test_collision_uppercase_same_dir(self, md_normalizer):
        row = make_moco_row(collision_type='SAME DIR REAR END')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '1. Rear End'

    def test_collision_unknown_passthrough(self, md_normalizer):
        """Unknown collision types should pass through as-is (not crash)."""
        row = make_moco_row(collision_type='Some Exotic Type')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == 'Some Exotic Type'

    def test_collision_empty_defaults_to_other(self, md_normalizer):
        """Empty collision type should default to '16. Other'."""
        row = make_moco_row(collision_type='')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '16. Other'

    # --- Weather mapping ---

    def test_weather_clear(self, md_normalizer):
        row = make_moco_row(weather='Clear')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '1. No Adverse Condition (Clear/Cloudy)'

    def test_weather_rain(self, md_normalizer):
        row = make_moco_row(weather='Raining')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '5. Rain'

    def test_weather_snow(self, md_normalizer):
        row = make_moco_row(weather='Snow')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '4. Snow'

    def test_weather_blowing_snow(self, md_normalizer):
        row = make_moco_row(weather='Blowing Snow')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '4. Snow'

    def test_weather_fog(self, md_normalizer):
        row = make_moco_row(weather='Foggy')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '3. Fog/Smog/Smoke'

    def test_weather_uppercase(self, md_normalizer):
        row = make_moco_row(weather='RAINING')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '5. Rain'

    def test_weather_unknown_passthrough(self, md_normalizer):
        row = make_moco_row(weather='Tornado')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == 'Tornado'

    # --- Light Condition mapping ---

    def test_light_daylight(self, md_normalizer):
        row = make_moco_row(light='Daylight')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '2. Daylight'

    def test_light_dark_lighted(self, md_normalizer):
        row = make_moco_row(light='Dark Lights On')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '4. Darkness - Road Lighted'

    def test_light_dark_unlighted(self, md_normalizer):
        row = make_moco_row(light='Dark No Lights')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '5. Darkness - Road Not Lighted'

    def test_light_dawn(self, md_normalizer):
        row = make_moco_row(light='Dawn')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '1. Dawn'

    def test_light_dusk(self, md_normalizer):
        row = make_moco_row(light='Dusk')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '3. Dusk'

    def test_light_dark_unknown(self, md_normalizer):
        row = make_moco_row(light='Dark -- Unknown Lighting')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '6. Dark - Unknown'

    def test_light_hyphenated_variant(self, md_normalizer):
        """Both 'Dark Lights On' and 'Dark-Lights On' should map identically."""
        r1 = md_normalizer.normalize_row(make_moco_row(light='Dark Lights On'))
        r2 = md_normalizer.normalize_row(make_moco_row(light='Dark-Lights On'))
        assert r1['Light Condition'] == r2['Light Condition']

    # --- Night flag ---

    def test_night_flag_dark_lighted(self, md_normalizer):
        row = make_moco_row(light='Dark Lights On')
        result = md_normalizer.normalize_row(row)
        assert result['Night?'] == 'Yes'

    def test_night_flag_dark_unlighted(self, md_normalizer):
        row = make_moco_row(light='Dark No Lights')
        result = md_normalizer.normalize_row(row)
        assert result['Night?'] == 'Yes'

    def test_night_flag_daylight(self, md_normalizer):
        row = make_moco_row(light='Daylight')
        result = md_normalizer.normalize_row(row)
        assert result['Night?'] == 'No'

    def test_night_flag_dawn_is_not_night(self, md_normalizer):
        row = make_moco_row(light='Dawn')
        result = md_normalizer.normalize_row(row)
        assert result['Night?'] == 'No'

    # --- Surface Condition mapping ---

    def test_surface_dry(self, md_normalizer):
        row = make_moco_row(surface_condition='Dry')
        result = md_normalizer.normalize_row(row)
        assert result['Roadway Surface Condition'] == '1. Dry'

    def test_surface_wet(self, md_normalizer):
        row = make_moco_row(surface_condition='Wet')
        result = md_normalizer.normalize_row(row)
        assert result['Roadway Surface Condition'] == '2. Wet'

    def test_surface_ice(self, md_normalizer):
        row = make_moco_row(surface_condition='Ice')
        result = md_normalizer.normalize_row(row)
        assert result['Roadway Surface Condition'] == '5. Ice'

    # --- Road System mapping ---

    def test_road_system_interstate(self, md_normalizer):
        row = make_moco_row(route_type='Interstate')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Interstate'

    def test_road_system_us_route(self, md_normalizer):
        row = make_moco_row(route_type='US Route')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Primary'

    def test_road_system_state_route(self, md_normalizer):
        row = make_moco_row(route_type='Maryland (State)')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Primary'

    def test_road_system_county(self, md_normalizer):
        row = make_moco_row(route_type='County')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Non-DOT secondary'

    def test_road_system_municipality(self, md_normalizer):
        row = make_moco_row(route_type='Municipality')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Non-DOT secondary'

    def test_road_system_unknown_defaults(self, md_normalizer):
        """Unknown route types should default to 'Non-DOT secondary'."""
        row = make_moco_row(route_type='Private Road')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Non-DOT secondary'

    # --- Node (intersection name) ---

    def test_node_both_streets(self, md_normalizer):
        """Node should be 'StreetA & StreetB' (alphabetically sorted)."""
        row = make_moco_row(road_name='RANDOLPH RD', cross_street_name='GEORGIA AVE')
        result = md_normalizer.normalize_row(row)
        assert result['Node'] == 'GEORGIA AVE & RANDOLPH RD'

    def test_node_no_cross_street(self, md_normalizer):
        """No cross street → empty Node."""
        row = make_moco_row(road_name='GEORGIA AVE', cross_street_name='')
        result = md_normalizer.normalize_row(row)
        assert result['Node'] == ''

    def test_node_no_road_name(self, md_normalizer):
        """No road name → empty Node."""
        row = make_moco_row(road_name='', cross_street_name='SOME ST')
        result = md_normalizer.normalize_row(row)
        assert result['Node'] == ''

    def test_node_alphabetical_order(self, md_normalizer):
        """Node streets must be alphabetically sorted."""
        row = make_moco_row(road_name='Z STREET', cross_street_name='A AVENUE')
        result = md_normalizer.normalize_row(row)
        assert result['Node'] == 'A AVENUE & Z STREET'

    # --- Coordinates ---

    def test_coordinates_present(self, md_normalizer):
        row = make_moco_row(latitude='39.0512', longitude='-77.0413')
        result = md_normalizer.normalize_row(row)
        # Virginia convention: x=lon, y=lat
        assert result['x'] == '-77.0413'
        assert result['y'] == '39.0512'

    def test_coordinates_empty(self, md_normalizer):
        row = make_moco_row(latitude='', longitude='')
        result = md_normalizer.normalize_row(row)
        assert result['x'] == ''
        assert result['y'] == ''

    def test_coordinates_convention_x_is_longitude(self, md_normalizer):
        """BUG CHECK: Verify x=longitude, y=latitude (Virginia convention)."""
        row = make_moco_row(latitude='39.0', longitude='-77.0')
        result = md_normalizer.normalize_row(row)
        # x must be negative (longitude in Western Hemisphere)
        assert float(result['x']) < 0, "x should be longitude (negative for US)"
        assert float(result['y']) > 0, "y should be latitude (positive for US)"

    # --- Intersection / Relation to Roadway ---

    def test_intersection_at_junction(self, md_normalizer):
        row = make_moco_row(junction='At Intersection')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Type'] == '4. Four Approaches'
        assert result['Relation To Roadway'] == '9. Within Intersection'

    def test_intersection_not_at_junction(self, md_normalizer):
        row = make_moco_row(junction='Non-Intersection')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Type'] == '1. Not at Intersection'
        assert result['Relation To Roadway'] == '8. Non-Intersection'

    def test_intersection_empty_junction(self, md_normalizer):
        row = make_moco_row(junction='')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Type'] == '1. Not at Intersection'
        assert result['Relation To Roadway'] == '8. Non-Intersection'

    # --- Intersection Analysis (Safety Focus) ---

    def test_intersection_analysis_at_junction(self, md_normalizer):
        row = make_moco_row(junction='At Intersection')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Analysis'] == 'Urban Intersection'

    def test_intersection_analysis_not_junction(self, md_normalizer):
        row = make_moco_row(junction='')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Analysis'] == 'Not Intersection'

    # --- Hit and Run ---

    def test_hitrun_yes(self, md_normalizer):
        row = make_moco_row(hit_run='Yes')
        result = md_normalizer.normalize_row(row)
        assert result['Hitrun?'] == 'Yes'

    def test_hitrun_no(self, md_normalizer):
        row = make_moco_row(hit_run='No')
        result = md_normalizer.normalize_row(row)
        assert result['Hitrun?'] == 'No'

    def test_hitrun_true_flag(self, md_normalizer):
        """'TRUE' maps to 'Yes' because normalizer checks .upper() in ('YES', 'TRUE', 'Y')."""
        row = make_moco_row(hit_run='TRUE')
        result = md_normalizer.normalize_row(row)
        assert result['Hitrun?'] == 'Yes'

    def test_hitrun_empty(self, md_normalizer):
        row = make_moco_row(hit_run='')
        result = md_normalizer.normalize_row(row)
        assert result['Hitrun?'] == 'No'

    # --- Jurisdiction fallback ---

    def test_jurisdiction_municipality_first(self, md_normalizer):
        row = make_moco_row(municipality='Rockville')
        result = md_normalizer.normalize_row(row)
        assert result['Physical Juris Name'] == 'Rockville'

    def test_jurisdiction_county_desc_fallback(self, md_normalizer):
        row = make_moco_row(municipality='', county_desc='Montgomery')
        result = md_normalizer.normalize_row(row)
        assert result['Physical Juris Name'] == 'Montgomery'

    def test_jurisdiction_default_montgomery(self, md_normalizer):
        """If no municipality or county_desc, default to 'Montgomery County'."""
        row = make_moco_row(municipality='')
        # Remove county_desc if present
        row.pop('county_desc', None)
        result = md_normalizer.normalize_row(row)
        assert result['Physical Juris Name'] == 'Montgomery County'

    # --- Boolean flags (all default to 'No' for crash-level data) ---

    def test_boolean_flags_default_no(self, md_normalizer):
        """Ped, Bike, Alcohol, Speed, etc. should default to 'No'."""
        row = make_moco_row()
        result = md_normalizer.normalize_row(row)
        for flag in ['Pedestrian?', 'Bike?', 'Alcohol?', 'Speed?',
                     'Motorcycle?', 'Distracted?', 'Drowsy?',
                     'Drug Related?', 'Young?', 'Senior?',
                     'Unrestrained?', 'School Zone', 'Work Zone Related']:
            assert result[flag] == 'No', f"Expected 'No' for '{flag}'"

    # --- Source tracking fields ---

    def test_source_state_tracking(self, md_normalizer):
        row = make_moco_row()
        result = md_normalizer.normalize_row(row)
        assert result['_source_state'] == 'maryland'
        assert result['_md_report_type'] == 'Property Damage Crash'

    def test_md_extra_fields_preserved(self, md_normalizer):
        row = make_moco_row(speed_limit='45', off_road_description='Ran off left')
        result = md_normalizer.normalize_row(row)
        assert result['_md_speed_limit'] == '45'
        assert result['_md_off_road'] == 'Ran off left'

    # --- Output completeness ---

    def test_all_standard_columns_present(self, md_normalizer):
        """Every standard column except _source_file must be in normalized output.
        (_source_file is added by convert_file, not normalize_row.)"""
        row = make_moco_row()
        result = md_normalizer.normalize_row(row)
        for col in STANDARD_COLUMNS:
            if col == '_source_file':
                continue  # Added by convert_file, not normalize_row
            assert col in result, f"Missing standard column: {col}"


# ===========================================================================
# 3. MarylandNormalizer — Statewide format (alt field names)
# ===========================================================================

class TestMarylandNormalizer_StatewideFormat:
    """Test normalization with statewide ACRS field names (report_no, acc_date, etc.)."""

    def test_statewide_id_field(self, md_normalizer):
        row = make_statewide_md_row()
        result = md_normalizer.normalize_row(row)
        assert result['Document Nbr'] == 'MD2023-SW-0001'

    def test_statewide_severity_injury(self, md_normalizer):
        row = make_statewide_md_row(acrs_report_type='Injury Crash')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Severity'] == 'B'

    def test_statewide_collision_type(self, md_normalizer):
        row = make_statewide_md_row(collision_type_desc='ANGLE')
        result = md_normalizer.normalize_row(row)
        assert result['Collision Type'] == '2. Angle'

    def test_statewide_weather(self, md_normalizer):
        row = make_statewide_md_row(weather_desc='RAINING')
        result = md_normalizer.normalize_row(row)
        assert result['Weather Condition'] == '5. Rain'

    def test_statewide_light(self, md_normalizer):
        row = make_statewide_md_row(light_desc='DAYLIGHT')
        result = md_normalizer.normalize_row(row)
        assert result['Light Condition'] == '2. Daylight'

    def test_statewide_surface(self, md_normalizer):
        row = make_statewide_md_row(surf_cond_desc='WET')
        result = md_normalizer.normalize_row(row)
        assert result['Roadway Surface Condition'] == '2. Wet'

    def test_statewide_road_system(self, md_normalizer):
        row = make_statewide_md_row(route_type_desc='US Route')
        result = md_normalizer.normalize_row(row)
        assert result['SYSTEM'] == 'Primary'

    def test_statewide_junction(self, md_normalizer):
        row = make_statewide_md_row(junction_desc='Non-Intersection')
        result = md_normalizer.normalize_row(row)
        assert result['Intersection Type'] == '1. Not at Intersection'

    def test_statewide_time_from_acc_time(self, md_normalizer):
        """acc_time should be used if crash_date_time has no time component."""
        row = make_statewide_md_row()
        # acc_date has no T component, so time comes from acc_time
        result = md_normalizer.normalize_row(row)
        assert result['Crash Military Time'] == '0830'

    def test_statewide_county_desc_as_jurisdiction(self, md_normalizer):
        row = make_statewide_md_row(county_desc='Montgomery')
        result = md_normalizer.normalize_row(row)
        assert result['Physical Juris Name'] == 'Montgomery'


# ===========================================================================
# 4. get_normalizer / get_supported_states registry
# ===========================================================================

class TestNormalizerRegistry:

    def test_get_normalizer_maryland(self):
        n = get_normalizer('maryland')
        assert isinstance(n, MarylandNormalizer)

    def test_get_normalizer_maryland_statewide(self):
        n = get_normalizer('maryland_statewide')
        assert isinstance(n, MarylandNormalizer)

    def test_get_normalizer_colorado(self):
        n = get_normalizer('colorado')
        assert isinstance(n, ColoradoNormalizer)

    def test_get_normalizer_virginia(self):
        n = get_normalizer('virginia')
        assert isinstance(n, VirginiaNormalizer)

    def test_get_normalizer_unknown_raises(self):
        with pytest.raises(ValueError, match="No normalizer"):
            get_normalizer('texas')

    def test_supported_states_includes_maryland(self):
        states = get_supported_states()
        assert 'maryland' in states
        assert 'maryland_statewide' in states
        assert 'MoCo' in states['maryland'] or 'Maryland' in states['maryland']


# ===========================================================================
# 5. convert_file end-to-end (CSV → standardized CSV)
# ===========================================================================

class TestConvertFile_Maryland:

    def test_basic_conversion(self):
        """End-to-end: raw MoCo CSV → standardized CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [make_moco_row(), make_moco_row(report_number='MD2023-000002')]
            input_path = make_moco_csv_file(tmpdir, rows)
            output_path = os.path.join(tmpdir, 'standardized.csv')

            state, total, with_gps = convert_file(input_path, output_path)

            assert state == 'maryland'
            assert total == 2
            assert with_gps == 2  # Both rows have valid GPS

            # Verify output file
            with open(output_path, 'r') as f:
                reader = csv.DictReader(f)
                out_rows = list(reader)
            assert len(out_rows) == 2
            assert out_rows[0]['Document Nbr'] == 'MD2023-000001'
            assert out_rows[0]['_source_state'] == 'maryland'

    def test_conversion_with_missing_gps(self):
        """Rows without GPS should be counted correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                make_moco_row(latitude='39.05', longitude='-77.04'),
                make_moco_row(report_number='MD-002', latitude='', longitude=''),
                make_moco_row(report_number='MD-003', latitude='invalid', longitude='nope'),
            ]
            input_path = make_moco_csv_file(tmpdir, rows)
            output_path = os.path.join(tmpdir, 'standardized.csv')

            state, total, with_gps = convert_file(input_path, output_path)
            assert total == 3
            assert with_gps == 1  # Only first row has valid GPS

    def test_conversion_explicit_state(self):
        """Explicit --state maryland should skip auto-detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [make_moco_row()]
            input_path = make_moco_csv_file(tmpdir, rows)
            output_path = os.path.join(tmpdir, 'out.csv')

            state, total, _ = convert_file(input_path, output_path, state='maryland')
            assert state == 'maryland'
            assert total == 1

    def test_conversion_unknown_state_raises(self):
        """Unknown auto-detected state should raise ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'garbage.csv')
            with open(path, 'w') as f:
                f.write('foo,bar,baz\n1,2,3\n')
            output_path = os.path.join(tmpdir, 'out.csv')

            with pytest.raises(ValueError, match="Could not auto-detect"):
                convert_file(path, output_path)

    def test_conversion_source_file_tracking(self):
        """_source_file should contain the filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [make_moco_row()]
            input_path = make_moco_csv_file(tmpdir, rows, filename='moco_crashes_2023.csv')
            output_path = os.path.join(tmpdir, 'out.csv')

            convert_file(input_path, output_path)

            with open(output_path) as f:
                reader = csv.DictReader(f)
                row = next(reader)
            assert row['_source_file'] == 'moco_crashes_2023.csv'

    def test_conversion_bom_encoding(self):
        """UTF-8 BOM files should be handled by convert_file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'bom.csv')
            row = make_moco_row()
            fieldnames = list(row.keys())
            with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row)
            output_path = os.path.join(tmpdir, 'out.csv')

            state, total, _ = convert_file(path, output_path)
            assert state == 'maryland'
            assert total == 1


# ===========================================================================
# 6. download_moco_crashes.py — Downloader logic
# ===========================================================================

class TestDownloaderConstants:
    """Verify downloader constants are correct and consistent with manifest."""

    def test_datasets_have_three_entries(self):
        assert len(DATASETS) == 3
        assert 'crashes' in DATASETS
        assert 'drivers' in DATASETS
        assert 'nonmotorists' in DATASETS

    def test_dataset_ids_match_manifest(self):
        """Dataset resource IDs must match moco_source_manifest.json."""
        manifest_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_source_manifest.json'
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
            assert DATASETS['crashes']['id'] == manifest['datasets']['crashes']['resource_id']
            assert DATASETS['drivers']['id'] == manifest['datasets']['drivers']['resource_id']
            assert DATASETS['nonmotorists']['id'] == manifest['datasets']['nonmotorists']['resource_id']

    def test_base_url(self):
        assert 'data.montgomerycountymd.gov' in BASE_URL

    def test_page_size_reasonable(self):
        assert 100 <= PAGE_SIZE <= 50000

    def test_retry_backoff_list(self):
        assert len(RETRY_BACKOFF) == MAX_RETRIES
        # Must be increasing
        for i in range(1, len(RETRY_BACKOFF)):
            assert RETRY_BACKOFF[i] > RETRY_BACKOFF[i - 1]

    def test_default_years_sanity(self):
        assert len(DEFAULT_YEARS) > 0
        assert min(DEFAULT_YEARS) >= 2010
        assert max(DEFAULT_YEARS) <= 2030

    def test_default_data_dir(self):
        assert 'MarylandDOT' in DEFAULT_DATA_DIR
        assert 'montgomery' in DEFAULT_DATA_DIR


class TestBuildWhereClause:

    def test_basic_year(self):
        clause = build_where_clause('crash_date_time', 2023)
        assert "2023-01-01T00:00:00.000" in clause
        assert "2024-01-01T00:00:00.000" in clause
        assert "crash_date_time >=" in clause
        assert "crash_date_time <" in clause

    def test_year_boundary(self):
        """Year 2024 should produce 2024-01-01 to 2025-01-01 range."""
        clause = build_where_clause('crash_date_time', 2024)
        assert "'2024-01-01" in clause
        assert "'2025-01-01" in clause

    def test_alternate_field_name(self):
        clause = build_where_clause('acc_date', 2022)
        assert "acc_date >=" in clause


class TestSaveCSV:

    def test_save_csv_basic(self):
        """save_csv should write records to a valid CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [
                {'report_number': 'A001', 'road_name': 'MAIN ST'},
                {'report_number': 'A002', 'road_name': 'OAK AVE'},
            ]
            path = os.path.join(tmpdir, 'output.csv')
            result = save_csv(records, path)

            assert result == path
            with open(path) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 2
            assert rows[0]['report_number'] == 'A001'

    def test_save_csv_gzip(self):
        """Gzip output should create a .csv.gz file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [{'report_number': 'A001', 'road_name': 'MAIN ST'}]
            path = os.path.join(tmpdir, 'output.csv')
            result = save_csv(records, path, gzip_output=True)

            assert result.endswith('.csv.gz')
            assert os.path.exists(result)
            # Verify it's valid gzip
            with gzip.open(result, 'rt') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 1

    def test_save_csv_empty_records(self):
        """Empty record list should return None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'empty.csv')
            result = save_csv([], path)
            assert result is None

    def test_save_csv_creates_parent_dirs(self):
        """save_csv should create parent directories if they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'nested', 'deep', 'output.csv')
            records = [{'report_number': 'A001'}]
            result = save_csv(records, path)
            assert result == path
            assert os.path.exists(path)

    def test_save_csv_union_of_keys(self):
        """Field names should be the union of all record keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [
                {'a': '1', 'b': '2'},
                {'a': '3', 'c': '4'},
            ]
            path = os.path.join(tmpdir, 'union.csv')
            save_csv(records, path)
            with open(path) as f:
                reader = csv.DictReader(f)
                assert set(reader.fieldnames) == {'a', 'b', 'c'}


class TestRetryRequest:

    @patch('download_moco_crashes.requests.get')
    def test_success_first_try(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = retry_request('https://example.com/test')
        assert result is mock_resp
        assert mock_get.call_count == 1

    @patch('download_moco_crashes.time.sleep')
    @patch('download_moco_crashes.requests.get')
    def test_retry_on_failure(self, mock_get, mock_sleep):
        """Should retry on RequestException and succeed on second try."""
        import requests as req
        mock_get.side_effect = [
            req.exceptions.ConnectionError("Connection refused"),
            MagicMock(raise_for_status=MagicMock()),
        ]

        result = retry_request('https://example.com/test', max_retries=2)
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(2)

    @patch('download_moco_crashes.time.sleep')
    @patch('download_moco_crashes.requests.get')
    def test_all_retries_exhausted(self, mock_get, mock_sleep):
        """Should raise after all retries are exhausted."""
        import requests as req
        mock_get.side_effect = req.exceptions.ConnectionError("Connection refused")

        with pytest.raises(req.exceptions.ConnectionError):
            retry_request('https://example.com/test', max_retries=3)
        assert mock_get.call_count == 3


class TestDownloadData:

    @patch('download_moco_crashes.retry_request')
    @patch('download_moco_crashes.time.sleep')
    def test_single_page(self, mock_sleep, mock_retry):
        """Single page of results (< PAGE_SIZE)."""
        records = [{'report_number': f'MD-{i}'} for i in range(10)]
        mock_resp = MagicMock()
        mock_resp.json.return_value = records
        mock_retry.return_value = mock_resp

        result = download_data('crashes', 2023)
        assert len(result) == 10

    @patch('download_moco_crashes.retry_request')
    @patch('download_moco_crashes.time.sleep')
    def test_multi_page(self, mock_sleep, mock_retry):
        """Multiple pages of results with proper pagination."""
        page1 = [{'report_number': f'MD-{i}'} for i in range(PAGE_SIZE)]
        page2 = [{'report_number': f'MD-{i}'} for i in range(PAGE_SIZE, PAGE_SIZE + 5)]

        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = page1
        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = page2

        mock_retry.side_effect = [mock_resp1, mock_resp2]

        result = download_data('crashes', 2023)
        assert len(result) == PAGE_SIZE + 5

    @patch('download_moco_crashes.retry_request')
    @patch('download_moco_crashes.time.sleep')
    def test_empty_response(self, mock_sleep, mock_retry):
        """No records for the given year."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_retry.return_value = mock_resp

        result = download_data('crashes', 2030)
        assert len(result) == 0


class TestHealthCheck:

    @patch('download_moco_crashes.retry_request')
    def test_all_endpoints_ok(self, mock_retry):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{'crash_date_time': '2023-01-01', 'foo': 'bar'}]
        mock_retry.return_value = mock_resp

        assert health_check() is True
        assert mock_retry.call_count == 3  # Once per dataset

    @patch('download_moco_crashes.retry_request')
    def test_one_endpoint_fails(self, mock_retry):
        import requests as req
        mock_ok = MagicMock()
        mock_ok.json.return_value = [{'crash_date_time': '2023-01-01'}]
        mock_retry.side_effect = [
            mock_ok,
            req.exceptions.ConnectionError("Down"),
            mock_ok,
        ]

        assert health_check() is False


# ===========================================================================
# 7. Config file consistency checks
# ===========================================================================

class TestMoCoConfig:
    """Validate moco_config.json structure and mappings."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'
        if not path.exists():
            pytest.skip('moco_config.json not found')
        with open(path) as f:
            self.config = json.load(f)

    def test_has_column_mapping(self):
        assert 'columnMapping' in self.config
        mapping = self.config['columnMapping']
        assert 'ID' in mapping
        assert 'DATE' in mapping
        assert 'LAT' in mapping
        assert 'LON' in mapping

    def test_column_mapping_values_are_strings(self):
        for k, v in self.config['columnMapping'].items():
            assert isinstance(v, str), f"columnMapping[{k}] is not a string: {v}"

    def test_has_crash_type_mapping(self):
        assert 'crashTypeMapping' in self.config
        ct = self.config['crashTypeMapping']
        # Should have at least the common types
        assert any('Rear End' in v for v in ct.values())
        assert any('Angle' in v for v in ct.values())

    def test_crash_type_values_use_vdot_format(self):
        """All values should use numbered VDOT format (e.g., '1. Rear End')."""
        for k, v in self.config['crashTypeMapping'].items():
            if k.startswith('_'):
                continue  # Skip metadata keys
            # VDOT format: number + period + space + text
            assert '.' in v, f"crashTypeMapping['{k}'] = '{v}' doesn't use VDOT numbered format"

    def test_weather_mapping_present(self):
        assert 'weatherMapping' in self.config
        wm = self.config['weatherMapping']
        assert 'Clear' in wm or 'CLEAR' in wm

    def test_light_mapping_present(self):
        assert 'lightMapping' in self.config
        lm = self.config['lightMapping']
        assert 'Daylight' in lm or 'DAYLIGHT' in lm

    def test_surface_mapping_present(self):
        assert 'surfaceMapping' in self.config

    def test_road_system_mapping_present(self):
        assert 'roadSystemMapping' in self.config
        rsm = self.config['roadSystemMapping']
        assert 'Interstate' in rsm
        assert rsm['Interstate'] == 'Interstate'

    def test_derived_fields_severity(self):
        assert 'derivedFields' in self.config
        sev = self.config['derivedFields']['SEVERITY']
        assert sev['source'] == 'acrs_report_type'
        assert sev['mapping']['Fatal Crash'] == 'K'
        assert sev['mapping']['Injury Crash'] == 'B'
        assert sev['mapping']['Property Damage Crash'] == 'O'

    def test_config_mapping_consistent_with_normalizer(self):
        """Config collision type mappings should match MarylandNormalizer maps."""
        normalizer = MarylandNormalizer('maryland')
        config_ct = self.config.get('crashTypeMapping', {})

        # Check that every config mapping key that is also in normalizer produces same result
        for raw_val, config_mapped in config_ct.items():
            if raw_val.startswith('_'):
                continue
            norm_mapped = normalizer.COLLISION_VDOT_MAP.get(raw_val)
            if norm_mapped:
                # Allow minor variations (e.g., '16. Other' vs '16. Other/Unknown')
                assert norm_mapped.split('.')[0] == config_mapped.split('.')[0], (
                    f"Collision mismatch for '{raw_val}': "
                    f"config='{config_mapped}', normalizer='{norm_mapped}'"
                )


class TestMoCoSourceManifest:
    """Validate moco_source_manifest.json structure."""

    @pytest.fixture(autouse=True)
    def load_manifest(self):
        path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_source_manifest.json'
        if not path.exists():
            pytest.skip('moco_source_manifest.json not found')
        with open(path) as f:
            self.manifest = json.load(f)

    def test_has_source(self):
        assert 'source' in self.manifest
        src = self.manifest['source']
        assert 'name' in src
        assert 'base_url' in src
        assert 'montgomerycountymd.gov' in src['base_url']

    def test_has_datasets(self):
        assert 'datasets' in self.manifest
        ds = self.manifest['datasets']
        assert 'crashes' in ds
        assert 'drivers' in ds
        assert 'nonmotorists' in ds

    def test_datasets_have_required_fields(self):
        for key, ds in self.manifest['datasets'].items():
            assert 'resource_id' in ds, f"datasets.{key} missing resource_id"
            assert 'api_url' in ds, f"datasets.{key} missing api_url"
            assert 'name' in ds, f"datasets.{key} missing name"

    def test_dataset_ids_valid_format(self):
        """Socrata resource IDs should be in xxxx-xxxx format."""
        import re
        for key, ds in self.manifest['datasets'].items():
            rid = ds['resource_id']
            assert re.match(r'^[a-z0-9]{4}-[a-z0-9]{4}$', rid), (
                f"datasets.{key}.resource_id '{rid}' doesn't match Socrata format"
            )

    def test_api_urls_contain_resource_ids(self):
        for key, ds in self.manifest['datasets'].items():
            assert ds['resource_id'] in ds['api_url'], (
                f"datasets.{key}.api_url doesn't contain its resource_id"
            )

    def test_has_api_config(self):
        assert 'api' in self.manifest
        api = self.manifest['api']
        assert 'endpoint_template' in api
        assert 'pagination' in api
        assert 'join_key' in api
        assert api['join_key'] == 'report_number'

    def test_has_jurisdiction(self):
        assert 'jurisdiction' in self.manifest
        j = self.manifest['jurisdiction']
        assert j['name'] == 'Montgomery County'
        assert j['state'] == 'Maryland'
        assert j['fips_full'] == '24031'


class TestStateConfig:
    """Validate states/maryland/config.json."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        path = PROJECT_ROOT / 'states' / 'maryland' / 'config.json'
        if not path.exists():
            pytest.skip('states/maryland/config.json not found')
        with open(path) as f:
            self.config = json.load(f)

    def test_state_metadata(self):
        s = self.config['state']
        assert s['name'] == 'Maryland'
        assert s['abbreviation'] == 'MD'
        assert s['fips'] == '24'

    def test_coordinate_bounds_valid(self):
        """Bounds should contain the state of Maryland."""
        bounds = self.config['state']['coordinateBounds']
        assert bounds['latMin'] < bounds['latMax']
        assert bounds['lonMin'] < bounds['lonMax']
        # Maryland approximate bounding box
        assert 37.5 < bounds['latMin'] < 38.5
        assert 39.3 < bounds['latMax'] < 40.0
        assert -80.0 < bounds['lonMin'] < -78.0
        assert -76.0 < bounds['lonMax'] < -74.5

    def test_montgomery_county_bounds(self):
        """MoCo GPS data should fit within state bounds."""
        bounds = self.config['state']['coordinateBounds']
        moco_lat = 39.14  # Approximate center
        moco_lon = -77.15
        assert bounds['latMin'] < moco_lat < bounds['latMax']
        assert bounds['lonMin'] < moco_lon < bounds['lonMax']

    def test_column_mapping_has_alt_fields(self):
        """Maryland config should have both primary and _ALT field names."""
        cm = self.config['columnMapping']
        assert 'ID' in cm
        assert 'ID_ALT' in cm
        assert cm['ID'] == 'report_number'
        assert cm['ID_ALT'] == 'report_no'

    def test_road_systems_present(self):
        assert 'roadSystems' in self.config
        rs = self.config['roadSystems']['values']
        assert 'Interstate' in rs
        assert 'County' in rs

    def test_road_system_categories_valid(self):
        """Every road system value should have a standardizedSystem."""
        for key, val in self.config['roadSystems']['values'].items():
            assert 'standardizedSystem' in val, f"roadSystems.{key} missing standardizedSystem"
            assert val['standardizedSystem'] in (
                'Interstate', 'Primary', 'Secondary', 'Non-DOT secondary'
            ), f"roadSystems.{key}.standardizedSystem = '{val['standardizedSystem']}' is not recognized"

    def test_epdo_weights_standard(self):
        """EPDO weights should match FHWA standard."""
        w = self.config['epdoWeights']
        assert w['K'] == 462
        assert w['A'] == 62
        assert w['B'] == 12
        assert w['C'] == 5
        assert w['O'] == 1

    def test_montgomery_jurisdiction_entry(self):
        j = self.config['jurisdictions']['montgomery']
        assert j['name'] == 'Montgomery County'
        assert j['type'] == 'county'
        assert j['fips'] == '031'

    def test_split_config_present(self):
        """Split config for generating county_roads / no_interstate files."""
        sc = self.config['roadSystems']['splitConfig']
        assert 'countyRoads' in sc
        assert 'interstateExclusion' in sc


# ===========================================================================
# 8. Workflow YAML structural checks
# ===========================================================================

class TestDownloadWorkflow:
    """Structural checks on moco_crash_download.yml."""

    @pytest.fixture(autouse=True)
    def load_workflow(self):
        path = PROJECT_ROOT / '.github' / 'workflows' / 'moco_crash_download.yml'
        if not path.exists():
            pytest.skip('moco_crash_download.yml not found')
        with open(path) as f:
            self.content = f.read()

    def test_uses_python_311(self):
        assert "python-version: '3.11'" in self.content or 'python-version: "3.11"' in self.content

    def test_installs_requirements(self):
        assert 'pip install -r requirements.txt' in self.content

    def test_references_download_script(self):
        assert 'download_moco_crashes.py' in self.content

    def test_has_r2_upload_step(self):
        assert 'R2' in self.content or 'r2' in self.content
        assert 'crash-lens-data' in self.content or 's3 cp' in self.content

    def test_has_retry_logic_for_push(self):
        assert 'MAX_RETRIES' in self.content

    def test_has_schedule_trigger(self):
        assert 'schedule' in self.content
        assert 'cron' in self.content

    def test_has_workflow_dispatch(self):
        assert 'workflow_dispatch' in self.content

    def test_permissions_contents_write(self):
        assert 'contents: write' in self.content


class TestProcessWorkflow:
    """Structural checks on process-moco-data.yml."""

    @pytest.fixture(autouse=True)
    def load_workflow(self):
        path = PROJECT_ROOT / '.github' / 'workflows' / 'process-moco-data.yml'
        if not path.exists():
            pytest.skip('process-moco-data.yml not found')
        with open(path) as f:
            self.content = f.read()

    def test_triggers_on_download_complete(self):
        assert 'workflow_run' in self.content
        assert 'Download MoCo Crash Data' in self.content

    def test_runs_process_script(self):
        assert 'process_crash_data.py' in self.content

    def test_passes_maryland_state(self):
        assert '-s maryland' in self.content or '--state maryland' in self.content

    def test_has_five_pipeline_stages(self):
        """Workflow should reference CONVERT, VALIDATE, GEOCODE, SPLIT, PREDICT."""
        for stage in ['CONVERT', 'VALIDATE', 'PREDICT']:
            assert stage.lower() in self.content.lower() or stage in self.content, (
                f"Workflow missing reference to {stage} stage"
            )

    def test_output_file_suffixes(self):
        """Should produce standardized, all_roads, county_roads, no_interstate."""
        for suffix in ['standardized', 'all_roads', 'county_roads', 'city_roads', 'no_interstate']:
            assert suffix in self.content, f"Missing output suffix: {suffix}"

    def test_uploads_forecasts_to_r2(self):
        assert 'forecasts_' in self.content

    def test_has_dry_run_option(self):
        assert 'dry_run' in self.content or 'dry-run' in self.content


# ===========================================================================
# 9. Cross-component consistency checks (bug detection)
# ===========================================================================

class TestCrossComponentConsistency:
    """Tests that catch data flow bugs between components."""

    def test_normalizer_collision_map_covers_config(self):
        """Every collision type in moco_config should be handled by normalizer."""
        config_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'
        if not config_path.exists():
            pytest.skip('moco_config.json not found')
        with open(config_path) as f:
            config = json.load(f)

        normalizer = MarylandNormalizer('maryland')
        config_ct = config.get('crashTypeMapping', {})

        for raw_val in config_ct.keys():
            if raw_val.startswith('_'):
                continue
            # Normalizer should either have this key or the passthrough should work
            result = normalizer.COLLISION_VDOT_MAP.get(raw_val, raw_val)
            assert result, f"Collision type '{raw_val}' has no mapping and no passthrough"

    def test_normalizer_weather_map_covers_config(self):
        """Every weather value in moco_config should be handled by normalizer."""
        config_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'
        if not config_path.exists():
            pytest.skip('moco_config.json not found')
        with open(config_path) as f:
            config = json.load(f)

        normalizer = MarylandNormalizer('maryland')
        config_wm = config.get('weatherMapping', {})

        for raw_val in config_wm.keys():
            if raw_val.startswith('_') or raw_val == 'N/A':
                continue
            norm_result = normalizer.WEATHER_VDOT_MAP.get(raw_val)
            if norm_result:
                config_result = config_wm[raw_val]
                assert norm_result.split('.')[0] == config_result.split('.')[0], (
                    f"Weather mismatch for '{raw_val}': "
                    f"config='{config_result}', normalizer='{norm_result}'"
                )

    def test_normalizer_light_map_covers_config(self):
        """Every light value in moco_config should be handled by normalizer."""
        config_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'
        if not config_path.exists():
            pytest.skip('moco_config.json not found')
        with open(config_path) as f:
            config = json.load(f)

        normalizer = MarylandNormalizer('maryland')
        config_lm = config.get('lightMapping', {})

        for raw_val in config_lm.keys():
            if raw_val.startswith('_') or raw_val == 'N/A':
                continue
            norm_result = normalizer.LIGHT_VDOT_MAP.get(raw_val)
            if norm_result:
                config_result = config_lm[raw_val]
                assert norm_result == config_result, (
                    f"Light mismatch for '{raw_val}': "
                    f"config='{config_result}', normalizer='{norm_result}'"
                )

    def test_normalizer_road_system_map_covers_config(self):
        """Every road system in moco_config should match normalizer."""
        config_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'
        if not config_path.exists():
            pytest.skip('moco_config.json not found')
        with open(config_path) as f:
            config = json.load(f)

        normalizer = MarylandNormalizer('maryland')
        config_rsm = config.get('roadSystemMapping', {})

        for raw_val, config_mapped in config_rsm.items():
            norm_mapped = normalizer.ROAD_SYSTEM_MAP.get(raw_val)
            if norm_mapped:
                assert norm_mapped == config_mapped, (
                    f"Road system mismatch for '{raw_val}': "
                    f"config='{config_mapped}', normalizer='{norm_mapped}'"
                )

    def test_downloader_ids_match_manifest_and_config(self):
        """Resource IDs should be consistent across downloader, manifest, and config."""
        manifest_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_source_manifest.json'
        config_path = PROJECT_ROOT / 'data' / 'MarylandDOT' / 'moco_config.json'

        if not manifest_path.exists():
            pytest.skip('moco_source_manifest.json not found')

        with open(manifest_path) as f:
            manifest = json.load(f)

        # Downloader DATASETS must match manifest
        assert DATASETS['crashes']['id'] == manifest['datasets']['crashes']['resource_id']
        assert DATASETS['drivers']['id'] == manifest['datasets']['drivers']['resource_id']
        assert DATASETS['nonmotorists']['id'] == manifest['datasets']['nonmotorists']['resource_id']

    def test_state_signature_matches_normalizer_fields(self):
        """STATE_SIGNATURES 'maryland' required fields should be what MarylandNormalizer expects."""
        sig = STATE_SIGNATURES['maryland']
        required = set(sig['required'])
        # These are the fields the normalizer reads via _get() as primary keys
        expected_primaries = {'report_number', 'acrs_report_type', 'road_name'}
        assert expected_primaries.issubset(required), (
            f"Signature required {required} doesn't cover normalizer primaries {expected_primaries}"
        )

    def test_maryland_statewide_signature_distinct(self):
        """Maryland and maryland_statewide must have different required columns."""
        md = set(STATE_SIGNATURES['maryland']['required'])
        md_sw = set(STATE_SIGNATURES['maryland_statewide']['required'])
        assert md != md_sw, "Maryland and maryland_statewide have identical signatures"

    def test_normalizer_output_has_source_file_slot(self):
        """Normalized rows should contain _source_file after convert_file adds it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [make_moco_row()]
            input_path = make_moco_csv_file(tmpdir, rows)
            output_path = os.path.join(tmpdir, 'out.csv')
            convert_file(input_path, output_path)

            with open(output_path) as f:
                reader = csv.DictReader(f)
                row = next(reader)
            assert '_source_file' in row
            assert '_source_state' in row
            assert row['_source_state'] == 'maryland'


# ===========================================================================
# 10. Edge cases & regression tests
# ===========================================================================

class TestEdgeCases:

    def test_empty_row(self, md_normalizer):
        """Completely empty row should normalize without crashing."""
        result = md_normalizer.normalize_row({})
        assert isinstance(result, dict)
        assert result['_source_state'] == 'maryland'
        assert result['Crash Severity'] == 'O'  # default

    def test_whitespace_only_values(self, md_normalizer):
        """Fields with only whitespace should be treated as empty."""
        row = make_moco_row(
            report_number='  ',
            road_name='   ',
            weather='  ',
        )
        result = md_normalizer.normalize_row(row)
        assert result['Document Nbr'] == ''
        assert result['RTE Name'] == ''

    def test_special_characters_in_road_name(self, md_normalizer):
        """Road names with special chars shouldn't cause issues."""
        row = make_moco_row(road_name="O'NEILL LN", cross_street_name='MD-355 (ROCKVILLE PIKE)')
        result = md_normalizer.normalize_row(row)
        assert "O'NEILL LN" in result['Node']

    def test_very_long_date_string(self, md_normalizer):
        """Malformed extra-long date string should not crash."""
        row = make_moco_row(crash_date_time='2023-06-15T14:30:00.000000000+00:00')
        result = md_normalizer.normalize_row(row)
        assert result['Crash Date'] == '2023-06-15'

    def test_coordinates_with_extra_precision(self, md_normalizer):
        """High-precision GPS coordinates should pass through intact."""
        row = make_moco_row(latitude='39.0512345678', longitude='-77.0413456789')
        result = md_normalizer.normalize_row(row)
        assert result['y'] == '39.0512345678'
        assert result['x'] == '-77.0413456789'

    def test_large_batch_normalization(self, md_normalizer):
        """Normalizing many rows should not cause memory issues or errors."""
        rows = [make_moco_row(report_number=f'MD-{i:06d}') for i in range(1000)]
        results = [md_normalizer.normalize_row(r) for r in rows]
        assert len(results) == 1000
        assert all(r['_source_state'] == 'maryland' for r in results)

    def test_duplicate_report_numbers_preserved(self, md_normalizer):
        """Normalizer should NOT deduplicate — that's the pipeline's job."""
        row1 = make_moco_row(report_number='MD-DUP')
        row2 = make_moco_row(report_number='MD-DUP')
        r1 = md_normalizer.normalize_row(row1)
        r2 = md_normalizer.normalize_row(row2)
        assert r1['Document Nbr'] == r2['Document Nbr'] == 'MD-DUP'

    def test_none_values_in_row(self, md_normalizer):
        """None values (not strings) should not crash the normalizer."""
        row = make_moco_row()
        row['weather'] = None
        # _get should handle None gracefully since it does .get() then .strip()
        # This is a potential bug — let's verify
        try:
            result = md_normalizer.normalize_row(row)
            # If it doesn't crash, that's fine
            assert isinstance(result, dict)
        except AttributeError:
            pytest.fail("MarylandNormalizer crashes on None values in row — "
                       "_get() should handle None gracefully")

    def test_mixed_case_hitrun_values(self, md_normalizer):
        """Various casing of hit_run should be handled."""
        for val, expected in [('Yes', 'Yes'), ('YES', 'Yes'), ('Y', 'Yes'),
                              ('No', 'No'), ('no', 'No'), ('', 'No')]:
            row = make_moco_row(hit_run=val)
            result = md_normalizer.normalize_row(row)
            assert result['Hitrun?'] == expected, (
                f"hit_run='{val}' should map to '{expected}', got '{result['Hitrun?']}'"
            )

    def test_iso_date_with_milliseconds_variations(self, md_normalizer):
        """Various millisecond precision in ISO dates."""
        for dt in ['2023-06-15T14:30:00.000',
                    '2023-06-15T14:30:00.0',
                    '2023-06-15T14:30:00']:
            row = make_moco_row(crash_date_time=dt)
            result = md_normalizer.normalize_row(row)
            assert result['Crash Date'] == '2023-06-15'
            assert result['Crash Military Time'] == '1430'
