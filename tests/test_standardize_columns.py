#!/usr/bin/env python3
"""
Tests for standardize_columns() in download_crash_data.py.

Verifies that raw ArcGIS FeatureServer coded values are correctly decoded
to match the previous VDOT data format expected by the frontend.

Covers:
  1. Header renaming (raw ArcGIS → human-readable)
  2. Collision Type coded value decoding
  3. Weather/Light/Roadway condition decoding
  4. Boolean flag decoding (0/1 → Yes/No)
  5. Unrestrained? special decode (0→Belted, 1→Unbelted)
  6. SYSTEM decode with VDOT prefix
  7. Ownership decode (correct labels for codes 5, 6)
  8. Functional Class decode
  9. Facility Type decode with numbered prefixes
  10. DOT District decode
  11. Area Type decode (0→Rural, 1→Urban)
  12. Physical Juris Name decode
  13. Planning District decode
  14. Intersection Analysis decode
  15. RoadDeparture Type decode
  16. Crash Date epoch ms → formatted date
  17. Work Zone Location/Type decode
  18. School Zone decode
  19. First Harmful Event decode
  20. Idempotency (already-decoded values pass through unchanged)
  21. Full-row integration test against reference data
"""

import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(REPO_ROOT))

from download_crash_data import standardize_columns


def _make_df(overrides=None):
    """Create a single-row DataFrame with raw ArcGIS coded values."""
    base = {
        'OBJECTID': '1',
        'DOCUMENT_NBR': '170785244',
        'LOCAL_CASE_CD': '20170613',
        'CRASH_YEAR': '2017',
        'CRASH_DT': '1489554000000',
        'CRASH_MILITARY_TM': '845',
        'CRASH_SEVERITY': 'O',
        'K_PEOPLE': '0', 'A_PEOPLE': '0', 'B_PEOPLE': '0', 'C_PEOPLE': '0',
        'PERSONS_INJURED': '0',
        'PEDESTRIANS_KILLED': '0', 'PEDESTRIANS_INJURED': '0',
        'VEH_COUNT': '2',
        'COLLISION_TYPE': '4',
        'WEATHER_CONDITION': '1',
        'LIGHT_CONDITION': '2',
        'ROADWAY_SURFACE_COND': '1',
        'RELATION_TO_ROADWAY': '9',
        'ROADWAY_ALIGNMENT': '1',
        'ROADWAY_SURFACE_TYPE': '2',
        'ROADWAY_DEFECT': '1',
        'ROADWAY_DESCRIPTION': '2',
        'INTERSECTION_TYPE': '4',
        'TRAFFIC_CONTROL_TYPE': '3',
        'TRFC_CTRL_STATUS_TYPE': '1',
        'WORK_ZONE_RELATED': '2',
        'WORK_ZONE_LOCATION': '0',
        'WORK_ZONE_TYPE': '0',
        'SCHOOL_ZONE': '1',
        'FIRST_HARMFUL_EVENT': '20',
        'FIRST_HARMFUL_EVENT_LOC': '1',
        'ROUTE_OR_STREET_NM': '16TH STREET',
        'ALCOHOL_NOTALCOHOL': '0',
        'BELTED_UNBELTED': '0',
        'BIKE_NONBIKE': '0',
        'DISTRACTED_NOTDISTRACTED': '1',
        'ANIMAL': '0',
        'DROWSY_NOTDROWSY': '0',
        'DRUG_NODRUG': '0',
        'GR_NOGR': '0',
        'HITRUN_NOT_HITRUN': '0',
        'LGTRUCK_NONLGTRUCK': '0',
        'MOTOR_NONMOTOR': '0',
        'PED_NONPED': '0',
        'SPEED_DIFF_MAX': '',
        'SPEED_NOTSPEED': '0',
        'RD_TYPE': '0',
        'INTERSECTION_ANALYSIS': '2',
        'SENIOR_NOTSENIOR': '1',
        'YOUNG_NOTYOUNG': '0',
        'MAINLINE_YN': '1',
        'NIGHT': '0',
        'VDOT_DISTRICT': '5',
        'JURIS_CODE': '121',
        'PHYSICAL_JURIS': '121',
        'FUN': 'LOC',
        'FAC': 'TUD',
        'AREA_TYPE': '1',
        'SYSTEM': '5',
        'VSP': '5',
        'OWNERSHIP': '3',
        'PLAN_DISTRICT': '23',
        'MPO_NAME': 'HAMP',
        'RTE_NM': 'S-VA121PR IVY AVE',
        'RNS_MP': '0.59',
        'NODE': '731559',
        'OFFSET': '48.57',
        'LAT': '36.97593488388',
        'LON': '-76.41507013793',
        'x': '-76.4150701379999',
        'y': '36.975934884',
    }
    if overrides:
        base.update(overrides)
    return pd.DataFrame([base])


class TestHeaderRenaming(unittest.TestCase):
    """Verify raw ArcGIS column names are renamed to human-readable format."""

    def test_core_headers_renamed(self):
        df = standardize_columns(_make_df())
        expected = [
            'Document Nbr', 'Crash Year', 'Crash Date', 'Crash Severity',
            'K_People', 'A_People', 'B_People', 'C_People',
            'Collision Type', 'Weather Condition', 'Light Condition',
            'Roadway Surface Condition', 'Intersection Type',
            'Traffic Control Type', 'Traffic Control Status',
            'DOT District', 'Juris Code', 'Physical Juris Name',
            'Functional Class', 'Facility Type', 'Area Type',
            'SYSTEM', 'Ownership', 'Planning District', 'MPO Name',
            'RTE Name', 'RNS MP', 'Node', 'Node Offset (ft)',
        ]
        for col in expected:
            self.assertIn(col, df.columns, f"Missing column: {col}")

    def test_raw_headers_removed(self):
        df = standardize_columns(_make_df())
        raw_should_not_exist = [
            'DOCUMENT_NBR', 'CRASH_YEAR', 'CRASH_DT', 'CRASH_SEVERITY',
            'COLLISION_TYPE', 'WEATHER_CONDITION', 'VDOT_DISTRICT',
            'FUN', 'FAC', 'OWNERSHIP', 'PLAN_DISTRICT',
        ]
        for col in raw_should_not_exist:
            self.assertNotIn(col, df.columns, f"Raw header still present: {col}")


class TestCollisionTypeDecode(unittest.TestCase):

    def test_collision_type_rear_end(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '1'}))
        self.assertEqual(df['Collision Type'].iloc[0], '1. Rear End')

    def test_collision_type_angle(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '2'}))
        self.assertEqual(df['Collision Type'].iloc[0], '2. Angle')

    def test_collision_type_sideswipe(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '4'}))
        self.assertEqual(df['Collision Type'].iloc[0], '4. Sideswipe - Same Direction')

    def test_collision_type_deer(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '10'}))
        self.assertEqual(df['Collision Type'].iloc[0], '10. Deer')


class TestWeatherLightConditionDecode(unittest.TestCase):

    def test_weather_clear(self):
        df = standardize_columns(_make_df({'WEATHER_CONDITION': '1'}))
        self.assertEqual(df['Weather Condition'].iloc[0], '1. No Adverse Condition (Clear/Cloudy)')

    def test_weather_rain(self):
        df = standardize_columns(_make_df({'WEATHER_CONDITION': '5'}))
        self.assertEqual(df['Weather Condition'].iloc[0], '5. Rain')

    def test_light_daylight(self):
        df = standardize_columns(_make_df({'LIGHT_CONDITION': '2'}))
        self.assertEqual(df['Light Condition'].iloc[0], '2. Daylight')

    def test_light_darkness_not_lighted(self):
        df = standardize_columns(_make_df({'LIGHT_CONDITION': '5'}))
        self.assertEqual(df['Light Condition'].iloc[0], '5. Darkness - Road Not Lighted')

    def test_roadway_surface_dry(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_COND': '1'}))
        self.assertEqual(df['Roadway Surface Condition'].iloc[0], '1. Dry')

    def test_roadway_surface_wet(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_COND': '2'}))
        self.assertEqual(df['Roadway Surface Condition'].iloc[0], '2. Wet')


class TestBooleanFlagDecode(unittest.TestCase):

    def test_alcohol_no(self):
        df = standardize_columns(_make_df({'ALCOHOL_NOTALCOHOL': '0'}))
        self.assertEqual(df['Alcohol?'].iloc[0], 'No')

    def test_alcohol_yes(self):
        df = standardize_columns(_make_df({'ALCOHOL_NOTALCOHOL': '1'}))
        self.assertEqual(df['Alcohol?'].iloc[0], 'Yes')

    def test_distracted_yes(self):
        df = standardize_columns(_make_df({'DISTRACTED_NOTDISTRACTED': '1'}))
        self.assertEqual(df['Distracted?'].iloc[0], 'Yes')

    def test_pedestrian_no(self):
        df = standardize_columns(_make_df({'PED_NONPED': '0'}))
        self.assertEqual(df['Pedestrian?'].iloc[0], 'No')

    def test_speed_yes(self):
        df = standardize_columns(_make_df({'SPEED_NOTSPEED': '1'}))
        self.assertEqual(df['Speed?'].iloc[0], 'Yes')

    def test_senior_yes(self):
        df = standardize_columns(_make_df({'SENIOR_NOTSENIOR': '1'}))
        self.assertEqual(df['Senior?'].iloc[0], 'Yes')

    def test_young_no(self):
        df = standardize_columns(_make_df({'YOUNG_NOTYOUNG': '0'}))
        self.assertEqual(df['Young?'].iloc[0], 'No')

    def test_night_no(self):
        df = standardize_columns(_make_df({'NIGHT': '0'}))
        self.assertEqual(df['Night?'].iloc[0], 'No')

    def test_mainline_yes(self):
        df = standardize_columns(_make_df({'MAINLINE_YN': '1'}))
        self.assertEqual(df['Mainline?'].iloc[0], 'Yes')

    def test_hitrun_no(self):
        df = standardize_columns(_make_df({'HITRUN_NOT_HITRUN': '0'}))
        self.assertEqual(df['Hitrun?'].iloc[0], 'No')

    def test_motorcycle_no(self):
        df = standardize_columns(_make_df({'MOTOR_NONMOTOR': '0'}))
        self.assertEqual(df['Motorcycle?'].iloc[0], 'No')

    def test_guardrail_no(self):
        df = standardize_columns(_make_df({'GR_NOGR': '0'}))
        self.assertEqual(df['Guardrail Related?'].iloc[0], 'No')

    def test_drug_no(self):
        df = standardize_columns(_make_df({'DRUG_NODRUG': '0'}))
        self.assertEqual(df['Drug Related?'].iloc[0], 'No')

    def test_drowsy_no(self):
        df = standardize_columns(_make_df({'DROWSY_NOTDROWSY': '0'}))
        self.assertEqual(df['Drowsy?'].iloc[0], 'No')

    def test_bike_no(self):
        df = standardize_columns(_make_df({'BIKE_NONBIKE': '0'}))
        self.assertEqual(df['Bike?'].iloc[0], 'No')

    def test_animal_no(self):
        df = standardize_columns(_make_df({'ANIMAL': '0'}))
        self.assertEqual(df['Animal Related?'].iloc[0], 'No')

    def test_lgtruck_yes(self):
        df = standardize_columns(_make_df({'LGTRUCK_NONLGTRUCK': '1'}))
        self.assertEqual(df['Lgtruck?'].iloc[0], 'Yes')


class TestUnrestrainedDecode(unittest.TestCase):

    def test_belted(self):
        df = standardize_columns(_make_df({'BELTED_UNBELTED': '0'}))
        self.assertEqual(df['Unrestrained?'].iloc[0], 'Belted')

    def test_unbelted(self):
        df = standardize_columns(_make_df({'BELTED_UNBELTED': '1'}))
        self.assertEqual(df['Unrestrained?'].iloc[0], 'Unbelted')


class TestSystemDecode(unittest.TestCase):

    def test_vdot_interstate(self):
        df = standardize_columns(_make_df({'SYSTEM': '1'}))
        self.assertEqual(df['SYSTEM'].iloc[0], 'DOT Interstate')

    def test_vdot_primary(self):
        df = standardize_columns(_make_df({'SYSTEM': '2'}))
        self.assertEqual(df['SYSTEM'].iloc[0], 'DOT Primary')

    def test_vdot_secondary(self):
        df = standardize_columns(_make_df({'SYSTEM': '3'}))
        self.assertEqual(df['SYSTEM'].iloc[0], 'DOT Secondary')

    def test_nonvdot_primary(self):
        df = standardize_columns(_make_df({'SYSTEM': '4'}))
        self.assertEqual(df['SYSTEM'].iloc[0], 'Non-DOT primary')

    def test_nonvdot_secondary(self):
        df = standardize_columns(_make_df({'SYSTEM': '5'}))
        self.assertEqual(df['SYSTEM'].iloc[0], 'Non-DOT secondary')


class TestOwnershipDecode(unittest.TestCase):

    def test_state_hwy(self):
        df = standardize_columns(_make_df({'OWNERSHIP': '1'}))
        self.assertEqual(df['Ownership'].iloc[0], '1. State Hwy Agency')

    def test_county_hwy(self):
        df = standardize_columns(_make_df({'OWNERSHIP': '2'}))
        self.assertEqual(df['Ownership'].iloc[0], '2. County Hwy Agency')

    def test_city_hwy(self):
        df = standardize_columns(_make_df({'OWNERSHIP': '3'}))
        self.assertEqual(df['Ownership'].iloc[0], '3. City or Town Hwy Agency')

    def test_toll_roads(self):
        df = standardize_columns(_make_df({'OWNERSHIP': '5'}))
        self.assertEqual(df['Ownership'].iloc[0], '5. Toll Roads Maintained by Others')

    def test_private_unknown(self):
        df = standardize_columns(_make_df({'OWNERSHIP': '6'}))
        self.assertEqual(df['Ownership'].iloc[0], '6. Private/Unknown Roads')


class TestFunctionalClassDecode(unittest.TestCase):

    def test_interstate(self):
        df = standardize_columns(_make_df({'FUN': 'INT'}))
        self.assertEqual(df['Functional Class'].iloc[0], '1-Interstate (A,1)')

    def test_minor_arterial(self):
        df = standardize_columns(_make_df({'FUN': 'MIA'}))
        self.assertEqual(df['Functional Class'].iloc[0], '4-Minor Arterial (H,3)')

    def test_local(self):
        df = standardize_columns(_make_df({'FUN': 'LOC'}))
        self.assertEqual(df['Functional Class'].iloc[0], '7-Local (J,6)')


class TestFacilityTypeDecode(unittest.TestCase):

    def test_two_way_undivided(self):
        df = standardize_columns(_make_df({'FAC': 'TUD'}))
        self.assertEqual(df['Facility Type'].iloc[0], '3-Two-Way Undivided')

    def test_two_way_divided(self):
        df = standardize_columns(_make_df({'FAC': 'TWD'}))
        self.assertEqual(df['Facility Type'].iloc[0], '4-Two-Way Divided')

    def test_one_way_undivided(self):
        df = standardize_columns(_make_df({'FAC': 'OUD'}))
        self.assertEqual(df['Facility Type'].iloc[0], '1-One-Way Undivided')


class TestVDOTDistrictDecode(unittest.TestCase):

    def test_hampton_roads(self):
        df = standardize_columns(_make_df({'VDOT_DISTRICT': '5'}))
        self.assertEqual(df['DOT District'].iloc[0], '5. Hampton Roads')

    def test_northern_virginia(self):
        df = standardize_columns(_make_df({'VDOT_DISTRICT': '9'}))
        self.assertEqual(df['DOT District'].iloc[0], '9. Northern Virginia')

    def test_bristol(self):
        df = standardize_columns(_make_df({'VDOT_DISTRICT': '1'}))
        self.assertEqual(df['DOT District'].iloc[0], '1. Bristol')

    def test_richmond(self):
        df = standardize_columns(_make_df({'VDOT_DISTRICT': '4'}))
        self.assertEqual(df['DOT District'].iloc[0], '4. Richmond')


class TestAreaTypeDecode(unittest.TestCase):

    def test_urban(self):
        df = standardize_columns(_make_df({'AREA_TYPE': '1'}))
        self.assertEqual(df['Area Type'].iloc[0], 'Urban')

    def test_rural(self):
        df = standardize_columns(_make_df({'AREA_TYPE': '0'}))
        self.assertEqual(df['Area Type'].iloc[0], 'Rural')


class TestPhysicalJurisDecode(unittest.TestCase):

    def test_newport_news(self):
        df = standardize_columns(_make_df({'PHYSICAL_JURIS': '121'}))
        self.assertEqual(df['Physical Juris Name'].iloc[0], '121. City of Newport News')

    def test_arlington(self):
        df = standardize_columns(_make_df({'PHYSICAL_JURIS': '0'}))
        self.assertEqual(df['Physical Juris Name'].iloc[0], '000. Arlington County')

    def test_fairfax_county(self):
        df = standardize_columns(_make_df({'PHYSICAL_JURIS': '29'}))
        self.assertEqual(df['Physical Juris Name'].iloc[0], '029. Fairfax County')

    def test_virginia_beach(self):
        df = standardize_columns(_make_df({'PHYSICAL_JURIS': '134'}))
        self.assertEqual(df['Physical Juris Name'].iloc[0], '134. City of Virginia Beach')


class TestPlanningDistrictDecode(unittest.TestCase):

    def test_hampton_roads(self):
        df = standardize_columns(_make_df({'PLAN_DISTRICT': '23'}))
        self.assertEqual(df['Planning District'].iloc[0], 'Hampton Roads')

    def test_northern_virginia(self):
        df = standardize_columns(_make_df({'PLAN_DISTRICT': '8'}))
        self.assertEqual(df['Planning District'].iloc[0], 'Northern Virginia')

    def test_combo_district(self):
        df = standardize_columns(_make_df({'PLAN_DISTRICT': '15,19'}))
        self.assertEqual(df['Planning District'].iloc[0], 'Richmond Regional, Crater')


class TestIntersectionAnalysisDecode(unittest.TestCase):

    def test_not_intersection(self):
        df = standardize_columns(_make_df({'INTERSECTION_ANALYSIS': '0'}))
        self.assertEqual(df['Intersection Analysis'].iloc[0], 'Not Intersection')

    def test_urban_intersection(self):
        df = standardize_columns(_make_df({'INTERSECTION_ANALYSIS': '1'}))
        self.assertEqual(df['Intersection Analysis'].iloc[0], 'Urban Intersection')

    def test_vdot_intersection(self):
        df = standardize_columns(_make_df({'INTERSECTION_ANALYSIS': '2'}))
        self.assertEqual(df['Intersection Analysis'].iloc[0], 'DOT Intersection')


class TestRoadDepartureTypeDecode(unittest.TestCase):

    def test_not_rd(self):
        df = standardize_columns(_make_df({'RD_TYPE': '0'}))
        self.assertEqual(df['RoadDeparture Type'].iloc[0], 'NOT_RD')

    def test_rd_left(self):
        df = standardize_columns(_make_df({'RD_TYPE': '1'}))
        self.assertEqual(df['RoadDeparture Type'].iloc[0], 'RD_LEFT')

    def test_rd_right(self):
        df = standardize_columns(_make_df({'RD_TYPE': '2'}))
        self.assertEqual(df['RoadDeparture Type'].iloc[0], 'RD_RIGHT')


class TestCrashDateDecode(unittest.TestCase):

    def test_epoch_to_date(self):
        df = standardize_columns(_make_df({'CRASH_DT': '1489554000000'}))
        # Should be March 15, 2017 in UTC
        self.assertIn('3/15/2017', df['Crash Date'].iloc[0])

    def test_epoch_date_format(self):
        df = standardize_columns(_make_df({'CRASH_DT': '1489554000000'}))
        date_val = df['Crash Date'].iloc[0]
        # Format should be M/D/YYYY H:MM:SS AM (12-hour with seconds and AM/PM)
        self.assertRegex(date_val, r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M$')

    def test_non_epoch_passthrough(self):
        """Already-formatted dates should pass through unchanged."""
        df = standardize_columns(_make_df({'CRASH_DT': '3/15/2017 5:00:00 AM'}))
        self.assertEqual(df['Crash Date'].iloc[0], '3/15/2017 5:00:00 AM')


class TestWorkZoneDecode(unittest.TestCase):

    def test_work_zone_location_zero_empty(self):
        df = standardize_columns(_make_df({'WORK_ZONE_LOCATION': '0'}))
        self.assertEqual(df['Work Zone Location'].iloc[0], '')

    def test_work_zone_location_advance_warning(self):
        df = standardize_columns(_make_df({'WORK_ZONE_LOCATION': '1'}))
        self.assertEqual(df['Work Zone Location'].iloc[0], '1. Advance Warning Area')

    def test_work_zone_location_activity(self):
        df = standardize_columns(_make_df({'WORK_ZONE_LOCATION': '3'}))
        self.assertEqual(df['Work Zone Location'].iloc[0], '3. Activity Area')

    def test_work_zone_type_zero_empty(self):
        df = standardize_columns(_make_df({'WORK_ZONE_TYPE': '0'}))
        self.assertEqual(df['Work Zone Type'].iloc[0], '')

    def test_work_zone_type_shoulder(self):
        df = standardize_columns(_make_df({'WORK_ZONE_TYPE': '3'}))
        self.assertEqual(df['Work Zone Type'].iloc[0], '3. Work on Shoulder or Median')

    def test_work_zone_related_no(self):
        df = standardize_columns(_make_df({'WORK_ZONE_RELATED': '2'}))
        self.assertEqual(df['Work Zone Related'].iloc[0], '2. No')


class TestSchoolZoneDecode(unittest.TestCase):

    def test_yes(self):
        df = standardize_columns(_make_df({'SCHOOL_ZONE': '1'}))
        self.assertEqual(df['School Zone'].iloc[0], '1. Yes')

    def test_no(self):
        df = standardize_columns(_make_df({'SCHOOL_ZONE': '3'}))
        self.assertEqual(df['School Zone'].iloc[0], '3. No')


class TestFirstHarmfulEventDecode(unittest.TestCase):

    def test_motor_vehicle(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '20'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '20. Motor Vehicle In Transport')

    def test_tree(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '2'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '2. Trees')

    def test_ped(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '19'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '19. Ped')

    def test_first_harmful_loc_on_roadway(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT_LOC': '1'}))
        self.assertEqual(df['First Harmful Event Loc'].iloc[0], '1. On Roadway')

    def test_first_harmful_loc_roadside(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT_LOC': '4'}))
        self.assertEqual(df['First Harmful Event Loc'].iloc[0], '4. Roadside')


class TestColumnAliases(unittest.TestCase):
    """Verify VDOT website download column renames."""

    def test_hit_and_run_renamed(self):
        df = pd.DataFrame([{'Hit & Run?': '1', 'SYSTEM': 'DOT Primary'}])
        df = standardize_columns(df)
        self.assertIn('Hitrun?', df.columns)
        self.assertNotIn('Hit & Run?', df.columns)
        self.assertEqual(df['Hitrun?'].iloc[0], 'Yes')

    def test_large_vehicle_renamed(self):
        df = pd.DataFrame([{'Large Vehicle?': '0', 'SYSTEM': 'DOT Primary'}])
        df = standardize_columns(df)
        self.assertIn('Lgtruck?', df.columns)
        self.assertEqual(df['Lgtruck?'].iloc[0], 'No')

    def test_unbelted_renamed(self):
        df = pd.DataFrame([{'UnBelted?': '1', 'SYSTEM': 'DOT Primary'}])
        df = standardize_columns(df)
        self.assertIn('Unrestrained?', df.columns)
        self.assertEqual(df['Unrestrained?'].iloc[0], 'Unbelted')

    def test_senior_driver_renamed(self):
        df = pd.DataFrame([{'Senior Driver?': '1', 'SYSTEM': 'DOT Primary'}])
        df = standardize_columns(df)
        self.assertIn('Senior?', df.columns)
        self.assertEqual(df['Senior?'].iloc[0], 'Yes')

    def test_young_driver_renamed(self):
        df = pd.DataFrame([{'Young Driver?': '0', 'SYSTEM': 'DOT Primary'}])
        df = standardize_columns(df)
        self.assertIn('Young?', df.columns)
        self.assertEqual(df['Young?'].iloc[0], 'No')


class TestSentinelValues(unittest.TestCase):
    """Verify 0 and 99 sentinel codes are decoded correctly."""

    def test_collision_type_not_applicable(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '0'}))
        self.assertEqual(df['Collision Type'].iloc[0], 'Not Applicable')

    def test_collision_type_not_provided(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '99'}))
        self.assertEqual(df['Collision Type'].iloc[0], 'Not Provided')

    def test_weather_not_applicable(self):
        df = standardize_columns(_make_df({'WEATHER_CONDITION': '99'}))
        self.assertEqual(df['Weather Condition'].iloc[0], 'Not Applicable')

    def test_light_not_applicable(self):
        df = standardize_columns(_make_df({'LIGHT_CONDITION': '99'}))
        self.assertEqual(df['Light Condition'].iloc[0], 'Not Applicable')

    def test_surface_cond_zero_passthrough(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_COND': '0'}))
        self.assertEqual(df['Roadway Surface Condition'].iloc[0], '0')

    def test_surface_cond_not_applicable(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_COND': '99'}))
        self.assertEqual(df['Roadway Surface Condition'].iloc[0], 'Not Applicable')

    def test_relation_roadway_not_applicable(self):
        df = standardize_columns(_make_df({'RELATION_TO_ROADWAY': '0'}))
        self.assertEqual(df['Relation To Roadway'].iloc[0], 'Not Applicable')

    def test_relation_roadway_not_provided(self):
        df = standardize_columns(_make_df({'RELATION_TO_ROADWAY': '99'}))
        self.assertEqual(df['Relation To Roadway'].iloc[0], 'Not Provided')

    def test_roadway_alignment_not_applicable(self):
        df = standardize_columns(_make_df({'ROADWAY_ALIGNMENT': '99'}))
        self.assertEqual(df['Roadway Alignment'].iloc[0], 'Not Applicable')

    def test_surface_type_not_applicable(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_TYPE': '99'}))
        self.assertEqual(df['Roadway Surface Type'].iloc[0], 'Not Applicable')

    def test_roadway_defect_not_applicable(self):
        df = standardize_columns(_make_df({'ROADWAY_DEFECT': '0'}))
        self.assertEqual(df['Roadway Defect'].iloc[0], 'Not Applicable')

    def test_roadway_defect_not_provided(self):
        df = standardize_columns(_make_df({'ROADWAY_DEFECT': '99'}))
        self.assertEqual(df['Roadway Defect'].iloc[0], 'Not Provided')

    def test_roadway_desc_not_applicable(self):
        df = standardize_columns(_make_df({'ROADWAY_DESCRIPTION': '0'}))
        self.assertEqual(df['Roadway Description'].iloc[0], 'Not Applicable')

    def test_roadway_desc_not_provided(self):
        df = standardize_columns(_make_df({'ROADWAY_DESCRIPTION': '99'}))
        self.assertEqual(df['Roadway Description'].iloc[0], 'Not Provided')

    def test_intersection_type_zero_passthrough(self):
        df = standardize_columns(_make_df({'INTERSECTION_TYPE': '0'}))
        self.assertEqual(df['Intersection Type'].iloc[0], '0')

    def test_intersection_type_not_applicable(self):
        df = standardize_columns(_make_df({'INTERSECTION_TYPE': '99'}))
        self.assertEqual(df['Intersection Type'].iloc[0], 'Not Applicable')

    def test_traffic_control_not_applicable(self):
        df = standardize_columns(_make_df({'TRAFFIC_CONTROL_TYPE': '99'}))
        self.assertEqual(df['Traffic Control Type'].iloc[0], 'Not Applicable')

    def test_traffic_ctrl_status_zero_passthrough(self):
        df = standardize_columns(_make_df({'TRFC_CTRL_STATUS_TYPE': '0'}))
        self.assertEqual(df['Traffic Control Status'].iloc[0], '0')

    def test_traffic_ctrl_status_not_applicable(self):
        df = standardize_columns(_make_df({'TRFC_CTRL_STATUS_TYPE': '99'}))
        self.assertEqual(df['Traffic Control Status'].iloc[0], 'Not Applicable')

    def test_work_zone_related_not_applicable(self):
        df = standardize_columns(_make_df({'WORK_ZONE_RELATED': '0'}))
        self.assertEqual(df['Work Zone Related'].iloc[0], 'Not Applicable')

    def test_school_zone_not_applicable(self):
        df = standardize_columns(_make_df({'SCHOOL_ZONE': '0'}))
        self.assertEqual(df['School Zone'].iloc[0], 'Not Applicable')

    def test_first_harmful_loc_zero_passthrough(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT_LOC': '0'}))
        self.assertEqual(df['First Harmful Event Loc'].iloc[0], '0')

    def test_first_harmful_loc_not_applicable(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT_LOC': '99'}))
        self.assertEqual(df['First Harmful Event Loc'].iloc[0], 'Not Applicable')


class TestCorrectedLabels(unittest.TestCase):
    """Verify corrected label text matches frontend expectations."""

    def test_collision_type_train(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '7'}))
        self.assertEqual(df['Collision Type'].iloc[0], '7. Train')

    def test_collision_type_bicyclist(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '13'}))
        self.assertEqual(df['Collision Type'].iloc[0], '13. Bicyclist')

    def test_collision_type_motorcyclist(self):
        df = standardize_columns(_make_df({'COLLISION_TYPE': '14'}))
        self.assertEqual(df['Collision Type'].iloc[0], '14. Motorcyclist')

    def test_weather_smoke_dust(self):
        df = standardize_columns(_make_df({'WEATHER_CONDITION': '8'}))
        self.assertEqual(df['Weather Condition'].iloc[0], '8. Smoke/Dust')

    def test_weather_blowing_sand(self):
        df = standardize_columns(_make_df({'WEATHER_CONDITION': '10'}))
        self.assertEqual(df['Weather Condition'].iloc[0], '10. Blowing Sand, Soil, Dirt, or Snow')

    def test_surface_oil_other_fluids(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_COND': '6'}))
        self.assertEqual(df['Roadway Surface Condition'].iloc[0], '6. Oil/Other Fluids')

    def test_surface_type_dirt(self):
        df = standardize_columns(_make_df({'ROADWAY_SURFACE_TYPE': '5'}))
        self.assertEqual(df['Roadway Surface Type'].iloc[0], '5. Dirt')

    def test_traffic_control_rr_markings_signs(self):
        df = standardize_columns(_make_df({'TRAFFIC_CONTROL_TYPE': '10'}))
        self.assertEqual(df['Traffic Control Type'].iloc[0], '10. Railroad Crossing With Markings and Signs')

    def test_traffic_control_rr_signals(self):
        df = standardize_columns(_make_df({'TRAFFIC_CONTROL_TYPE': '11'}))
        self.assertEqual(df['Traffic Control Type'].iloc[0], '11. Railroad Crossing With Signals')

    def test_traffic_ctrl_status_missing(self):
        df = standardize_columns(_make_df({'TRFC_CTRL_STATUS_TYPE': '5'}))
        self.assertEqual(df['Traffic Control Status'].iloc[0], '5. Yes - Missing')

    def test_relation_railway_grade(self):
        df = standardize_columns(_make_df({'RELATION_TO_ROADWAY': '14'}))
        self.assertEqual(df['Relation To Roadway'].iloc[0], '14. Railway Grade Crossing')

    def test_relation_other_crossing(self):
        df = standardize_columns(_make_df({'RELATION_TO_ROADWAY': '15'}))
        self.assertEqual(df['Relation To Roadway'].iloc[0], '15. Other Crossing (Crossing for Bikes, School, etc.)')

    def test_first_harmful_other_traffic_barrier(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '16'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '16. Other Traffic Barrier')

    def test_first_harmful_train(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '21'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '21. Train')

    def test_first_harmful_work_zone_equipment(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '24'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '24. Work Zone Maintenance Equipment')

    def test_first_harmful_downhill_runaway(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '31'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '31. Downhill Runaway')

    def test_first_harmful_cargo_loss(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '32'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '32. Cargo Loss or Shift')

    def test_first_harmful_explosion_fire(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '33'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '33. Explosion or Fire')

    def test_first_harmful_separation(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '34'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '34. Separation of Units')

    def test_first_harmful_cross_median(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '35'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '35. Cross Median')

    def test_first_harmful_immersion(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '38'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '38. Immersion')

    def test_first_harmful_non_collision_unknown(self):
        df = standardize_columns(_make_df({'FIRST_HARMFUL_EVENT': '41'}))
        self.assertEqual(df['First Harmful Event'].iloc[0], '41. Non-Collision Unknown')

    def test_work_zone_type_intermittent(self):
        df = standardize_columns(_make_df({'WORK_ZONE_TYPE': '4'}))
        self.assertEqual(df['Work Zone Type'].iloc[0], '4. Intermittent or Moving Work')


class TestFacilityTypeNewCodes(unittest.TestCase):
    """Verify new Facility Type codes."""

    def test_one_way_divided(self):
        df = standardize_columns(_make_df({'FAC': 'OWD'}))
        self.assertEqual(df['Facility Type'].iloc[0], '2-One-Way Divided')

    def test_reversible(self):
        df = standardize_columns(_make_df({'FAC': 'REX'}))
        self.assertEqual(df['Facility Type'].iloc[0], '5-Reversible Exclusively (e.g. 395R)')


class TestIdempotency(unittest.TestCase):
    """Running standardize_columns on already-decoded data should not change it."""

    def test_already_decoded_passes_through(self):
        # Build a df with already-decoded values (human-readable)
        decoded = {
            'OBJECTID': '1',
            'Document Nbr': '170785244',
            'Crash Year': '2017',
            'Crash Date': '3/15/2017 5:00:00 AM',
            'Crash Severity': 'O',
            'Collision Type': '4. Sideswipe - Same Direction',
            'Weather Condition': '1. No Adverse Condition (Clear/Cloudy)',
            'SYSTEM': 'Non-DOT secondary',
            'Ownership': '3. City or Town Hwy Agency',
            'Functional Class': '7-Local (J,6)',
            'Facility Type': '3-Two-Way Undivided',
            'Alcohol?': 'No',
            'Unrestrained?': 'Belted',
            'DOT District': '5. Hampton Roads',
            'Area Type': 'Urban',
            'Physical Juris Name': '121. City of Newport News',
            'Planning District': 'Hampton Roads',
            'Intersection Analysis': 'DOT Intersection',
            'RoadDeparture Type': 'NOT_RD',
        }
        df = pd.DataFrame([decoded])
        df2 = standardize_columns(df)

        for col, expected_val in decoded.items():
            if col in df2.columns:
                self.assertEqual(
                    df2[col].iloc[0], expected_val,
                    f"Idempotency failed for {col}: got '{df2[col].iloc[0]}'"
                )


class TestFullRowIntegration(unittest.TestCase):
    """Integration test: full row decode matches expected output."""

    def test_full_row_decode(self):
        df = standardize_columns(_make_df())
        row = df.iloc[0]

        expected = {
            'Document Nbr': '170785244',
            'Crash Year': '2017',
            'Crash Severity': 'O',
            'K_People': '0',
            'Vehicle Count': '2',
            'Collision Type': '4. Sideswipe - Same Direction',
            'Weather Condition': '1. No Adverse Condition (Clear/Cloudy)',
            'Light Condition': '2. Daylight',
            'Roadway Surface Condition': '1. Dry',
            'Relation To Roadway': '9. Within Intersection',
            'Roadway Alignment': '1. Straight - Level',
            'Roadway Surface Type': '2. Blacktop, Asphalt, Bituminous',
            'Roadway Defect': '1. No Defects',
            'Roadway Description': '2. Two-Way, Divided, Unprotected Median',
            'Intersection Type': '4. Four Approaches',
            'Traffic Control Type': '3. Traffic Signal',
            'Traffic Control Status': '1. Yes - Working',
            'Work Zone Related': '2. No',
            'School Zone': '1. Yes',
            'First Harmful Event': '20. Motor Vehicle In Transport',
            'First Harmful Event Loc': '1. On Roadway',
            'Alcohol?': 'No',
            'Unrestrained?': 'Belted',
            'Bike?': 'No',
            'Distracted?': 'Yes',
            'Animal Related?': 'No',
            'Drowsy?': 'No',
            'Drug Related?': 'No',
            'Guardrail Related?': 'No',
            'Hitrun?': 'No',
            'Lgtruck?': 'No',
            'Motorcycle?': 'No',
            'Pedestrian?': 'No',
            'Speed?': 'No',
            'RoadDeparture Type': 'NOT_RD',
            'Intersection Analysis': 'DOT Intersection',
            'Senior?': 'Yes',
            'Young?': 'No',
            'Mainline?': 'Yes',
            'Night?': 'No',
            'DOT District': '5. Hampton Roads',
            'Juris Code': '121',
            'Physical Juris Name': '121. City of Newport News',
            'Functional Class': '7-Local (J,6)',
            'Facility Type': '3-Two-Way Undivided',
            'Area Type': 'Urban',
            'SYSTEM': 'Non-DOT secondary',
            'Ownership': '3. City or Town Hwy Agency',
            'Planning District': 'Hampton Roads',
            'MPO Name': 'HAMP',
            'RTE Name': 'S-VA121PR IVY AVE',
            'Node': '731559',
        }
        for col, expected_val in expected.items():
            self.assertEqual(
                str(row[col]), expected_val,
                f"Full row mismatch for {col}: got '{row[col]}', expected '{expected_val}'"
            )


class TestReferenceDataComparison(unittest.TestCase):
    """Compare standardize_columns() output against reference CSV if available."""

    @unittest.skipUnless(
        (Path(__file__).parent.parent / 'docs' / 'data_compare' /
         'previous version dataset with details column name and attributes.csv').exists()
        and (Path(__file__).parent.parent / 'docs' / 'data_compare' /
             'downloaded from r2 after batch workflow done.xlsx').exists(),
        "Reference data files not available"
    )
    def test_against_reference_data(self):
        """Decode R2 data and compare against previous version reference."""
        import openpyxl

        ref_dir = REPO_ROOT / 'docs' / 'data_compare'
        r2_path = ref_dir / 'downloaded from r2 after batch workflow done.xlsx'
        prev_path = ref_dir / 'previous version dataset with details column name and attributes.csv'

        # Load R2 data
        wb = openpyxl.load_workbook(str(r2_path), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        headers = [str(h) for h in rows[0]]
        data = [[str(v) if v is not None else '' for v in row] for row in rows[1:]]
        wb.close()
        df = pd.DataFrame(data, columns=headers)
        df2 = standardize_columns(df)

        # Load reference
        with open(str(prev_path), 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            prev_headers = next(reader)
            all_prev = list(reader)

        prev_idx = {h: i for i, h in enumerate(prev_headers)}
        prev_by_doc = {row[prev_idx['Document Nbr']]: row for row in all_prev}

        # Compare (excluding x/y precision, Crash Date format, and SYSTEM
        # which had incorrect mapping in the previous version dataset)
        checked = 0
        field_mismatches = {}
        for _, row in df2.iterrows():
            doc = str(row['Document Nbr'])
            if doc in prev_by_doc:
                prev_row = prev_by_doc[doc]
                checked += 1
                for col in prev_headers:
                    if col in ('x', 'y', 'Crash Date', 'SYSTEM'):
                        continue
                    if col in df2.columns:
                        new_val = str(row[col]).strip()
                        prev_val = prev_row[prev_idx[col]].strip()
                        if new_val != prev_val:
                            field_mismatches[col] = field_mismatches.get(col, 0) + 1

        self.assertGreater(checked, 100, f"Only matched {checked} records")
        self.assertEqual(
            len(field_mismatches), 0,
            f"Mismatches in {checked} records: {field_mismatches}"
        )


if __name__ == '__main__':
    unittest.main()
