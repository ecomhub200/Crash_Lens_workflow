#!/usr/bin/env python3
"""
Multi-State Crash Data Adapter (Python)

Detects the source state from CSV headers and converts raw crash data
to the standardized CRASH LENS internal format (Virginia-compatible columns).

This is the Python counterpart to states/state_adapter.js - it performs
the same normalization but as a persistent pre-processing step, writing
standardized CSV files that the validation system can then process.

Supported states:
  - Virginia (TREDS) - passthrough, already in internal format
  - Colorado (CDOT) - full column mapping + severity derivation
  - Maryland (ACRS) - MoCo county portal + statewide portal normalization

Adding a new state:
  1. Create states/{state}/config.json with columnMapping + derivedFields
  2. Add a detection signature in STATE_SIGNATURES
  3. Add a normalizer class inheriting from BaseNormalizer

Usage:
    from state_adapter import StateDetector, get_normalizer

    detector = StateDetector()
    state = detector.detect_from_headers(csv_headers)
    normalizer = get_normalizer(state)
    standardized_row = normalizer.normalize_row(raw_row)
"""

import csv
import json
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# --- State detection signatures ---
STATE_SIGNATURES = {
    'colorado': {
        'required': ['CUID', 'System Code', 'Injury 00', 'Injury 04'],
        'optional': ['Rd_Number', 'Rd_Section', 'MHE'],
        'display_name': 'Colorado (CDOT)',
        'config_dir': 'colorado'
    },
    'virginia': {
        'required': ['Document Nbr', 'Crash Severity', 'RTE Name', 'SYSTEM'],
        'optional': ['K_People', 'A_People', 'Node', 'Physical Juris Name'],
        'display_name': 'Virginia (TREDS)',
        'config_dir': 'virginia'
    },
    'maryland': {
        'required': ['report_number', 'acrs_report_type', 'road_name'],
        'optional': ['crash_date_time', 'collision_type', 'municipality', 'weather', 'light'],
        'display_name': 'Maryland (ACRS - MoCo)',
        'config_dir': 'maryland'
    },
    'maryland_statewide': {
        'required': ['report_no', 'acrs_report_type', 'road_name', 'county_desc'],
        'optional': ['acc_date', 'collision_type_desc', 'weather_desc', 'light_desc'],
        'display_name': 'Maryland (ACRS - Statewide)',
        'config_dir': 'maryland'
    },
    'delaware': {
        'required': ['crash_datetime', 'crash_classification_description', 'latitude', 'longitude'],
        'optional': ['manner_of_impact_description', 'weather_1_description', 'county_name',
                     'alcohol_involved', 'drug_involved', 'motorcycle_involved'],
        'display_name': 'Delaware (DelDOT)',
        'config_dir': 'delaware'
    },
    'delaware_csv': {
        'required': ['CRASH DATETIME', 'CRASH CLASSIFICATION DESCRIPTION', 'LATITUDE', 'LONGITUDE'],
        'optional': ['MANNER OF IMPACT DESCRIPTION', 'WEATHER 1 DESCRIPTION', 'COUNTY NAME',
                     'ALCOHOL INVOLVED', 'DRUG INVOLVED', 'MOTORCYCLE INVOLVED'],
        'display_name': 'Delaware (DelDOT CSV)',
        'config_dir': 'delaware'
    }
}

# Standardized output columns (Virginia-compatible format)
STANDARD_COLUMNS = [
    'Document Nbr', 'Crash Date', 'Crash Year', 'Crash Military Time',
    'Crash Severity', 'K_People', 'A_People', 'B_People', 'C_People',
    'Collision Type', 'Weather Condition', 'Light Condition',
    'Roadway Surface Condition', 'Roadway Alignment',
    'Roadway Description', 'Intersection Type',
    'RTE Name', 'SYSTEM', 'Node', 'RNS MP',
    'x', 'y',  # longitude, latitude (Virginia convention: x=lon, y=lat)
    'Physical Juris Name',
    'Pedestrian?', 'Bike?', 'Alcohol?', 'Speed?', 'Hitrun?',
    'Motorcycle?', 'Night?', 'Distracted?', 'Drowsy?', 'Drug Related?',
    'Young?', 'Senior?', 'Unrestrained?', 'School Zone', 'Work Zone Related',
    'Traffic Control Type', 'Traffic Control Status',
    'Functional Class', 'Area Type', 'Facility Type', 'Ownership',
    'First Harmful Event', 'First Harmful Event Loc',
    'Relation To Roadway',  # intersection location relationship
    'Vehicle Count',         # number of vehicles involved
    'Persons Injured', 'Pedestrians Killed', 'Pedestrians Injured',
    # Original state columns preserved with prefix
    '_source_state', '_source_file'
]


class StateDetector:
    """Detect which state's crash data format a CSV uses."""

    def detect_from_headers(self, headers: List[str]) -> str:
        """
        Detect state from CSV column headers.

        Returns state key (e.g., 'colorado', 'virginia') or 'unknown'.
        """
        normalized = set(h.strip() for h in headers)

        # Exact match first
        for state_key, sig in STATE_SIGNATURES.items():
            if all(col in normalized for col in sig['required']):
                return state_key

        # Partial match fallback
        best_match = None
        best_score = 0.0
        for state_key, sig in STATE_SIGNATURES.items():
            all_cols = sig['required'] + sig.get('optional', [])
            score = sum(1 for c in all_cols if c in normalized) / len(all_cols)
            if score > best_score:
                best_score = score
                best_match = state_key

        if best_score > 0.5 and best_match:
            return best_match

        return 'unknown'

    def detect_from_file(self, filepath: str) -> str:
        """Detect state from CSV file headers."""
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)
        return self.detect_from_headers(headers)


class BaseNormalizer(ABC):
    """Base class for state-specific data normalizers."""

    def __init__(self, state_key: str, config_path: Optional[str] = None):
        self.state_key = state_key
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)

    @abstractmethod
    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """Convert a raw CSV row dict to standardized format."""
        pass

    def get_state_bounds(self) -> Optional[Dict]:
        """Get coordinate bounds for this state."""
        state_cfg = self.config.get('state', {})
        bounds = state_cfg.get('coordinateBounds', {})
        if bounds:
            return {
                'minLat': bounds.get('latMin', 0),
                'maxLat': bounds.get('latMax', 0),
                'minLon': bounds.get('lonMin', 0),
                'maxLon': bounds.get('lonMax', 0)
            }
        return None

    def get_road_system_column(self) -> str:
        """Get the column name used for road system classification."""
        mapping = self.config.get('columnMapping', {})
        return mapping.get('ROAD_SYSTEM', 'SYSTEM')

    def get_agency_column(self) -> str:
        """Get the column name used for agency identification."""
        mapping = self.config.get('columnMapping', {})
        return mapping.get('AGENCY', 'Agency Id')


class VirginiaNormalizer(BaseNormalizer):
    """Virginia TREDS data normalizer.

    Handles two data formats:
    - OLD format (text labels): passes through unchanged
    - NEW format (numeric codes): decodes back to text labels the frontend expects

    Idempotent: detects format and only normalizes if needed.
    """

    # Column renames: VDOT website renamed some columns
    COLUMN_RENAMES = {
        'Senior Driver?': 'Senior?',
        'Young Driver?': 'Young?',
        'Hit & Run?': 'Hitrun?',
        'Large Vehicle?': 'Lgtruck?',
        'UnBelted?': 'Unrestrained?',
        # ArcGIS ALL_CAPS format
        'DOCUMENT_NBR': 'Document Nbr', 'Document_Nbr': 'Document Nbr',
        'CRASH_YEAR': 'Crash Year', 'Crash_Year': 'Crash Year',
        'CRASH_DT': 'Crash Date', 'Crash_Date': 'Crash Date',
        'CRASH_MILITARY_TM': 'Crash Military Time', 'Crash_Military_Time': 'Crash Military Time',
        'CRASH_SEVERITY': 'Crash Severity', 'Crash_Severity': 'Crash Severity',
        'K_PEOPLE': 'K_People', 'A_PEOPLE': 'A_People',
        'B_PEOPLE': 'B_People', 'C_PEOPLE': 'C_People',
        'PERSONS_INJURED': 'Persons Injured', 'Persons_Injured': 'Persons Injured',
        'PEDESTRIANS_KILLED': 'Pedestrians Killed', 'Pedestrians_Killed': 'Pedestrians Killed',
        'PEDESTRIANS_INJURED': 'Pedestrians Injured', 'Pedestrians_Injured': 'Pedestrians Injured',
        'VEH_COUNT': 'Vehicle Count', 'Vehicle_Count': 'Vehicle Count',
        'COLLISION_TYPE': 'Collision Type', 'Collision_Type': 'Collision Type',
        'WEATHER_CONDITION': 'Weather Condition', 'Weather_Condition': 'Weather Condition',
        'LIGHT_CONDITION': 'Light Condition', 'Light_Condition': 'Light Condition',
        'ROADWAY_SURFACE_COND': 'Roadway Surface Condition', 'Roadway_Surface_Condition': 'Roadway Surface Condition',
        'RELATION_TO_ROADWAY': 'Relation To Roadway', 'Relation_To_Roadway': 'Relation To Roadway',
        'ROADWAY_ALIGNMENT': 'Roadway Alignment', 'Roadway_Alignment': 'Roadway Alignment',
        'ROADWAY_SURFACE_TYPE': 'Roadway Surface Type', 'Roadway_Surface_Type': 'Roadway Surface Type',
        'ROADWAY_DEFECT': 'Roadway Defect', 'Roadway_Defect': 'Roadway Defect',
        'ROADWAY_DESCRIPTION': 'Roadway Description', 'Roadway_Description': 'Roadway Description',
        'INTERSECTION_TYPE': 'Intersection Type', 'Intersection_Type': 'Intersection Type',
        'TRAFFIC_CONTROL_TYPE': 'Traffic Control Type', 'Traffic_Control_Type': 'Traffic Control Type',
        'TRFC_CTRL_STATUS_TYPE': 'Traffic Control Status', 'Traffic_Control_Status': 'Traffic Control Status',
        'WORK_ZONE_RELATED': 'Work Zone Related', 'Work_Zone_Related': 'Work Zone Related',
        'WORK_ZONE_LOCATION': 'Work Zone Location', 'Work_Zone_Location': 'Work Zone Location',
        'WORK_ZONE_TYPE': 'Work Zone Type', 'Work_Zone_Type': 'Work Zone Type',
        'SCHOOL_ZONE': 'School Zone', 'School_Zone': 'School Zone',
        'FIRST_HARMFUL_EVENT': 'First Harmful Event', 'First_Harmful_Event': 'First Harmful Event',
        'FIRST_HARMFUL_EVENT_LOC': 'First Harmful Event Loc', 'First_Harmful_Event_Loc': 'First Harmful Event Loc',
        'JURIS_CODE': 'Juris Code', 'Juris_Code': 'Juris Code',
        'PHYSICAL_JURIS': 'Physical Juris Name', 'Physical_Juris_Name': 'Physical Juris Name',
        'FUN': 'Functional Class', 'Functional_Class': 'Functional Class',
        'FAC': 'Facility Type', 'Facility_Type': 'Facility Type',
        'AREA_TYPE': 'Area Type', 'Area_Type': 'Area Type',
        'OWNERSHIP': 'Ownership',
        'PLAN_DISTRICT': 'Planning District', 'Planning_District': 'Planning District',
        'MPO_NAME': 'MPO Name', 'MPO_Name': 'MPO Name',
        'VDOT_DISTRICT': 'DOT District', 'VDOT_District': 'DOT District',
        'RTE_NM': 'RTE Name', 'RTE_NAME': 'RTE Name', 'RTE_Name': 'RTE Name',
        'NODE': 'Node', 'OFFSET': 'Node Offset (ft)', 'Node_Offset': 'Node Offset (ft)',
        'ALCOHOL_NOTALCOHOL': 'Alcohol?', 'BIKE_NONBIKE': 'Bike?',
        'PED_NONPED': 'Pedestrian?', 'SPEED_NOTSPEED': 'Speed?',
        'DISTRACTED_NOTDISTRACTED': 'Distracted?', 'DROWSY_NOTDROWSY': 'Drowsy?',
        'HITRUN_NOT_HITRUN': 'Hitrun?', 'SENIOR_NOTSENIOR': 'Senior?',
        'YOUNG_NOTYOUNG': 'Young?', 'NIGHT': 'Night?',
        'BELTED_UNBELTED': 'Unrestrained?', 'MOTOR_NONMOTOR': 'Motorcycle?',
        'DRUG_NODRUG': 'Drug Related?', 'GR_NOGR': 'Guardrail Related?',
        'LGTRUCK_NONLGTRUCK': 'Lgtruck?', 'MAINLINE_YN': 'Mainline?',
        'SPEED_DIFF_MAX': 'Max Speed Diff', 'RD_TYPE': 'RoadDeparture Type',
        'LOCAL_CASE_CD': 'Local Case CD', 'ROUTE_OR_STREET_NM': 'Route or Street Name',
        'INTERSECTION_ANALYSIS': 'Intersection Analysis', 'ANIMAL': 'Animal Related?',
    }

    # Coded value decode maps: column → {code → label}
    DECODE_MAPS = {
        'Collision Type': {
            '0': 'Not Applicable', '1': '1. Rear End', '2': '2. Angle',
            '3': '3. Head On', '4': '4. Sideswipe - Same Direction',
            '5': '5. Sideswipe - Opposite Direction', '6': '6. Fixed Object in Road',
            '7': '7. Train', '8': '8. Non-Collision', '9': '9. Fixed Object - Off Road',
            '10': '10. Deer', '11': '11. Other Animal', '12': '12. Ped',
            '13': '13. Bicyclist', '14': '14. Motorcyclist', '15': '15. Backed Into',
            '16': '16. Other', '99': 'Not Provided',
        },
        'Weather Condition': {
            '1': '1. No Adverse Condition (Clear/Cloudy)', '3': '3. Fog',
            '4': '4. Mist', '5': '5. Rain', '6': '6. Snow', '7': '7. Sleet/Hail',
            '8': '8. Smoke/Dust', '9': '9. Other',
            '10': '10. Blowing Sand, Soil, Dirt, or Snow', '11': '11. Severe Crosswinds',
            '99': 'Not Applicable',
        },
        'Light Condition': {
            '1': '1. Dawn', '2': '2. Daylight', '3': '3. Dusk',
            '4': '4. Darkness - Road Lighted', '5': '5. Darkness - Road Not Lighted',
            '6': '6. Darkness - Unknown Road Lighting', '7': '7. Unknown',
            '99': 'Not Applicable',
        },
        'Roadway Surface Condition': {
            '1': '1. Dry', '2': '2. Wet', '3': '3. Snowy',
            '4': '4. Icy', '5': '5. Muddy', '6': '6. Oil/Other Fluids', '7': '7. Other',
            '8': '8. Natural Debris', '9': '9. Water (Standing, Moving)',
            '10': '10. Slush', '11': '11. Sand, Dirt, Gravel', '99': 'Not Applicable',
        },
        'Relation To Roadway': {
            '0': 'Not Applicable', '1': '1. Main-Line Roadway',
            '2': '2. Acceleration/Deceleration Lanes',
            '3': '3. Gore Area (b/w Ramp and Highway Edgelines)',
            '4': '4. Collector/Distributor Road', '5': '5. On Entrance/Exit Ramp',
            '6': '6. Intersection at end of Ramp',
            '7': '7. Other location not listed above within an interchange area (median, shoulder , roadside)',
            '8': '8. Non-Intersection', '9': '9. Within Intersection',
            '10': '10. Intersection Related - Within 150 Feet',
            '11': '11. Intersection Related - Outside 150 Feet',
            '12': '12. Crossover Related', '13': '13. Driveway, Alley-Access - Related',
            '14': '14. Railway Grade Crossing',
            '15': '15. Other Crossing (Crossing for Bikes, School, etc.)',
            '99': 'Not Provided',
        },
        'Roadway Alignment': {
            '1': '1. Straight - Level', '2': '2. Curve - Level',
            '3': '3. Grade - Straight', '4': '4. Grade - Curve',
            '5': '5. Hillcrest - Straight', '6': '6. Hillcrest - Curve',
            '7': '7. Dip - Straight', '8': '8. Dip - Curve', '9': '9. Other',
            '10': '10. On/Off Ramp', '99': 'Not Applicable',
        },
        'Roadway Surface Type': {
            '1': '1. Concrete', '2': '2. Blacktop, Asphalt, Bituminous',
            '3': '3. Brick or Block', '4': '4. Slag, Gravel, Stone',
            '5': '5. Dirt', '6': '6. Other', '99': 'Not Applicable',
        },
        'Roadway Defect': {
            '0': 'Not Applicable', '1': '1. No Defects', '2': '2. Holes, Ruts, Bumps',
            '3': '3. Soft or Low Shoulder', '4': '4. Under Repair',
            '5': '5. Loose Material', '6': '6. Restricted Width',
            '7': '7. Slick Pavement', '8': '8. Roadway Obstructed', '9': '9. Other',
            '10': '10. Edge Pavement Drop Off', '99': 'Not Provided',
        },
        'Roadway Description': {
            '0': 'Not Applicable', '1': '1. Two-Way, Not Divided',
            '2': '2. Two-Way, Divided, Unprotected Median',
            '3': '3. Two-Way, Divided, Positive Median Barrier',
            '4': '4. One-Way, Not Divided', '5': '5. Unknown', '99': 'Not Provided',
        },
        'Intersection Type': {
            '1': '1. Not at Intersection',
            '2': '2. Two Approaches', '3': '3. Three Approaches',
            '4': '4. Four Approaches', '5': '5. Five-Point, or More',
            '6': '6. Roundabout', '99': 'Not Applicable',
        },
        'Traffic Control Type': {
            '1': '1. No Traffic Control', '2': '2. Officer or Flagger',
            '3': '3. Traffic Signal', '4': '4. Stop Sign',
            '5': '5. Slow or Warning Sign', '6': '6. Traffic Lanes Marked',
            '7': '7. No Passing Lines', '8': '8. Yield Sign',
            '9': '9. One Way Road or Street',
            '10': '10. Railroad Crossing With Markings and Signs',
            '11': '11. Railroad Crossing With Signals',
            '12': '12. Railroad Crossing With Gate and Signals',
            '13': '13. Other', '14': '14. Ped Crosswalk',
            '15': '15. Reduced Speed - School Zone',
            '16': '16. Reduced Speed - Work Zone',
            '17': '17. Highway Safety Corridor', '99': 'Not Applicable',
        },
        'Traffic Control Status': {
            '1': '1. Yes - Working',
            '2': '2. Yes - Working and Obscured', '3': '3. Yes - Not Working',
            '4': '4. Yes - Not Working and Obscured', '5': '5. Yes - Missing',
            '6': '6. No Traffic Control Device Present', '99': 'Not Applicable',
        },
        'Work Zone Related': {
            '0': 'Not Applicable', '1': '1. Yes', '2': '2. No', '99': 'Not Provided',
        },
        'Work Zone Location': {
            '0': '', '1': '1. Advance Warning Area', '2': '2. Transition Area',
            '3': '3. Activity Area', '4': '4. Termination Area', '99': '',
        },
        'Work Zone Type': {
            '0': '', '1': '1. Lane Closure', '2': '2. Lane Shift/Crossover',
            '3': '3. Work on Shoulder or Median', '4': '4. Intermittent or Moving Work',
            '5': '5. Other', '99': '',
        },
        'School Zone': {
            '0': 'Not Applicable', '1': '1. Yes', '2': '2. Yes - With School Activity',
            '3': '3. No', '99': 'Not Provided',
        },
        'First Harmful Event': {
            '1': '1. Bank Or Ledge', '2': '2. Trees', '3': '3. Utility Pole',
            '4': '4. Fence Or Post', '5': '5. Guard Rail', '6': '6. Parked Vehicle',
            '7': '7. Tunnel, Bridge, Underpass, Culvert, etc.',
            '8': '8. Sign, Traffic Signal', '9': '9. Impact Cushioning Device',
            '10': '10. Other', '11': '11. Jersey Wall', '12': '12. Building/Structure',
            '13': '13. Curb', '14': '14. Ditch', '15': '15. Other Fixed Object',
            '16': '16. Other Traffic Barrier', '17': '17. Traffic Sign Support',
            '18': '18. Mailbox', '19': '19. Ped', '20': '20. Motor Vehicle In Transport',
            '21': '21. Train', '22': '22. Bicycle', '23': '23. Animal',
            '24': '24. Work Zone Maintenance Equipment', '25': '25. Other Movable Object',
            '26': '26. Unknown Movable Object', '27': '27. Other',
            '28': '28. Ran Off Road', '29': '29. Jack Knife',
            '30': '30. Overturn (Rollover)', '31': '31. Downhill Runaway',
            '32': '32. Cargo Loss or Shift', '33': '33. Explosion or Fire',
            '34': '34. Separation of Units', '35': '35. Cross Median',
            '36': '36. Cross Centerline', '37': '37. Equipment Failure (Tire, etc)',
            '38': '38. Immersion', '39': '39. Fell/Jumped From Vehicle',
            '40': '40. Thrown or Falling Object', '41': '41. Non-Collision Unknown',
            '42': '42. Other Non-Collision',
        },
        'First Harmful Event Loc': {
            '1': '1. On Roadway', '2': '2. Shoulder',
            '3': '3. Median', '4': '4. Roadside', '5': '5. Gore',
            '6': '6. Separator', '7': '7. In Parking Lane or Zone',
            '8': '8. Off Roadway, Location Unknown', '9': '9. Outside Right-of-Way',
            '99': 'Not Applicable',
        },
        'DOT District': {
            '1': '1. Bristol', '2': '2. Salem', '3': '3. Lynchburg',
            '4': '4. Richmond', '5': '5. Hampton Roads', '6': '6. Fredericksburg',
            '7': '7. Culpeper', '8': '8. Staunton', '9': '9. Northern Virginia',
        },
        'Ownership': {
            '1': '1. State Hwy Agency', '2': '2. County Hwy Agency',
            '3': '3. City or Town Hwy Agency', '4': '4. Federal Roads',
            '5': '5. Toll Roads Maintained by Others', '6': '6. Private/Unknown Roads',
        },
        'SYSTEM': {
            '1': 'DOT Interstate', '2': 'DOT Primary', '3': 'DOT Secondary',
            '4': 'Non-DOT primary', '5': 'Non-DOT secondary',
        },
        'Functional Class': {
            'INT': '1-Interstate (A,1)',
            'OFE': '2-Principal Arterial - Other Freeways and Expressways (B)',
            'OPA': '3-Principal Arterial - Other (E,2)',
            'MIA': '4-Minor Arterial (H,3)', 'MAC': '5-Major Collector (I,4)',
            'MIC': '6-Minor Collector (5)', 'LOC': '7-Local (J,6)',
        },
        'Facility Type': {
            'OUD': '1-One-Way Undivided', 'OWD': '2-One-Way Divided',
            'TUD': '3-Two-Way Undivided', 'TWD': '4-Two-Way Divided',
            'REX': '5-Reversible Exclusively (e.g. 395R)',
        },
        'Area Type': {
            '0': 'Rural', '1': 'Urban',
        },
        'RoadDeparture Type': {
            '0': 'NOT_RD', '1': 'RD_LEFT', '2': 'RD_RIGHT', '3': 'RD_UNKNOWN',
        },
        'Intersection Analysis': {
            '0': 'Not Intersection', '1': 'Urban Intersection', '2': 'DOT Intersection',
        },
    }

    # Planning District decode map
    PLANNING_DISTRICT_MAP = {
        '1': 'Lenowisco', '2': 'Cumberland Plateau',
        '3': 'Mount Rogers', '4': 'New River Valley',
        '5': 'Roanoke Valley-Alleghany', '6': 'Central Shenandoah',
        '7': 'Northern Shenandoah Valley', '8': 'Northern Virginia',
        '9': 'Rappahannock - Rapidan', '10': 'Thomas Jefferson',
        '11': 'Region 2000', '12': 'West Piedmont',
        '13': 'Southside', '14': 'Commonwealth Regional',
        '15': 'Richmond Regional', '16': 'George Washington Regional',
        '17': 'Northern Neck', '18': 'Middle Peninsula',
        '19': 'Crater', '20': 'Piedmont',
        '21': 'Rappahannock Area', '22': 'Accomack-Northampton',
        '23': 'Hampton Roads',
        '5,12': 'Roanoke Valley-Alleghany, West Piedmont',
        '15,19': 'Richmond Regional, Crater',
        '18,23': 'Middle Peninsula, Hampton Roads',
        '19,23': 'Crater, Hampton Roads',
    }

    # Complete VDOT jurisdiction table (324 entries)
    JURIS_MAP = {
        '0': '000. Arlington County', '1': '001. Accomack County',
        '2': '002. Albemarle County', '3': '003. Alleghany County',
        '4': '004. Amelia County', '5': '005. Amherst County',
        '6': '006. Appomattox County', '7': '007. Augusta County',
        '8': '008. Bath County', '9': '009. Bedford County',
        '10': '010. Bland County', '11': '011. Botetourt County',
        '12': '012. Brunswick County', '13': '013. Buchanan County',
        '14': '014. Buckingham County', '15': '015. Campbell County',
        '16': '016. Caroline County', '17': '017. Carroll County',
        '18': '018. Charles City County', '19': '019. Charlotte County',
        '20': '020. Chesterfield County', '21': '021. Clarke County',
        '22': '022. Craig County', '23': '023. Culpeper County',
        '24': '024. Cumberland County', '25': '025. Dickenson County',
        '26': '026. Dinwiddie County',
        '28': '028. Essex County', '29': '029. Fairfax County',
        '30': '030. Fauquier County', '31': '031. Floyd County',
        '32': '032. Fluvanna County', '33': '033. Franklin County',
        '34': '034. Frederick County', '35': '035. Giles County',
        '36': '036. Gloucester County', '37': '037. Goochland County',
        '38': '038. Grayson County', '39': '039. Greene County',
        '40': '040. Greensville County', '41': '041. Halifax County',
        '42': '042. Hanover County', '43': '043. Henrico County',
        '44': '044. Henry County', '45': '045. Highland County',
        '46': '046. Isle of Wight County', '47': '047. James City County',
        '48': '048. King George County', '49': '049. King & Queen County',
        '50': '050. King William County', '51': '051. Lancaster County',
        '52': '052. Lee County', '53': '053. Loudoun County',
        '54': '054. Louisa County', '55': '055. Lunenburg County',
        '56': '056. Madison County', '57': '057. Mathews County',
        '58': '058. Mecklenburg County', '59': '059. Middlesex County',
        '60': '060. Montgomery County',
        '62': '062. Nelson County', '63': '063. New Kent County',
        '65': '065. Northampton County',
        '66': '066. Northumberland County', '67': '067. Nottoway County',
        '68': '068. Orange County', '69': '069. Page County',
        '70': '070. Patrick County', '71': '071. Pittsylvania County',
        '72': '072. Powhatan County', '73': '073. Prince Edward County',
        '74': '074. Prince George County',
        '76': '076. Prince William County', '77': '077. Pulaski County',
        '78': '078. Rappahannock County', '79': '079. Richmond County',
        '80': '080. Roanoke County', '81': '081. Rockbridge County',
        '82': '082. Rockingham County', '83': '083. Russell County',
        '84': '084. Scott County', '85': '085. Shenandoah County',
        '86': '086. Smyth County', '87': '087. Southampton County',
        '88': '088. Spotsylvania County', '89': '089. Stafford County',
        '90': '090. Surry County', '91': '091. Sussex County',
        '92': '092. Tazewell County', '93': '093. Warren County',
        '95': '095. Washington County',
        '96': '096. Westmoreland County', '97': '097. Wise County',
        '98': '098. Wythe County', '99': '099. York County',
        '100': '100. City of Alexandria', '101': '101. Town of Big Stone Gap',
        '102': '102. City of Bristol', '103': '103. City of Buena Vista',
        '104': '104. City of Charlottesville', '105': '105. Town of Clifton Forge',
        '106': '106. City of Colonial Heights', '107': '107. City of Covington',
        '108': '108. City of Danville', '109': '109. City of Emporia',
        '110': '110. City of Falls Church', '111': '111. City of Fredericksburg',
        '112': '112. Town of Front Royal', '113': '113. City of Galax',
        '114': '114. City of Hampton', '115': '115. City of Harrisonburg',
        '116': '116. City of Hopewell', '117': '117. City of Lexington',
        '118': '118. City of Lynchburg', '119': '119. Town of Marion',
        '120': '120. City of Martinsville', '121': '121. City of Newport News',
        '122': '122. City of Norfolk', '123': '123. City of Petersburg',
        '124': '124. City of Portsmouth', '125': '125. Town of Pulaski',
        '126': '126. City of Radford', '127': '127. City of Richmond',
        '128': '128. City of Roanoke', '129': '129. City of Salem',
        '130': '130. Town of South Boston', '131': '131. City of Chesapeake',
        '132': '132. City of Staunton', '133': '133. City of Suffolk',
        '134': '134. City of Virginia Beach',
        '136': '136. City of Waynesboro', '137': '137. City of Williamsburg',
        '138': '138. City of Winchester', '139': '139. Town of Wytheville',
        '140': '140. Town of Abingdon', '141': '141. Town of Bedford',
        '142': '142. Town of Blackstone', '143': '143. Town of Bluefield',
        '144': '144. Town of Farmville', '145': '145. City of Franklin',
        '146': '146. City of Norton', '147': '147. City of Poquoson',
        '148': '148. Town of Richlands', '149': '149. Town of Vinton',
        '150': '150. Town of Blacksburg', '151': '151. City of Fairfax',
        '152': '152. City of Manassas Park', '153': '153. Town of Vienna',
        '154': '154. Town of Christiansburg', '155': '155. City of Manassas',
        '156': '156. Town of Warrenton', '157': '157. Town of Rocky Mount',
        '158': '158. Town of Tazewell', '159': '159. Town of Luray',
        '160': '160. Town of Accomac', '161': '161. Town of Alberta',
        '162': '162. Town of Altavista', '163': '163. Town of Amherst',
        '164': '164. Town of Appalachia', '165': '165. Town of Appomattox',
        '166': '166. Town of Ashland', '167': '167. Town of Belle Haven',
        '168': '168. Town of Berryville', '169': '169. Town of Bloxom',
        '170': '170. Town of Boones Mill', '171': '171. Town of Bowling Green',
        '172': '172. Town of Boyce', '173': '173. Town of Boydton',
        '174': '174. Town of Boykins', '175': '175. Town of Branchville',
        '176': '176. Town of Bridgewater', '177': '177. Town of Broadway',
        '178': '178. Town of Brodnax', '179': '179. Town of Brookneal',
        '180': '180. Town of Buchanan', '181': '181. Town of Burkeville',
        '182': '182. Town of Cape Charles', '183': '183. Town of Capron',
        '184': '184. Town of Cedar Bluff', '185': '185. Town of Charlotte C.H.',
        '186': '186. Town of Chase City', '187': '187. Town of Chatham',
        '188': '188. Town of Cheriton', '189': '189. Town of Chilhowie',
        '190': '190. Town of Chincoteague', '191': '191. Town of Claremont',
        '192': '192. Town of Clarksville', '193': '193. Town of Cleveland',
        '194': '194. Town of Clifton', '195': '195. Town of Clinchport',
        '196': '196. Town of Clintwood',
        '198': '198. Town of Coeburn', '199': '199. Town of Colonial Beach',
        '200': '200. Town of Columbia', '201': '201. Town of Courtland',
        '202': '202. Town of Craigsville', '203': '203. Town of Crewe',
        '204': '204. Town of Culpeper', '205': '205. Town of Damascus',
        '206': '206. Town of Dayton', '207': '207. Town of Dendron',
        '208': '208. Town of Dillwyn', '209': '209. Town of Drakes Branch',
        '210': '210. Town of Dublin', '211': '211. Town of Duffield',
        '212': '212. Town of Dumfries', '213': '213. Town of Dungannon',
        '214': '214. Town of Eastville', '215': '215. Town of Edinburg',
        '216': '216. Town of Elkton', '217': '217. Town of Exmore',
        '218': '218. Town of Fincastle', '219': '219. Town of Floyd',
        '220': '220. Town of Fries', '221': '221. Town of Gate City',
        '222': '222. Town of Glade Spring', '223': '223. Town of Glasgow',
        '224': '224. Town of Glen Lyn', '225': '225. Town of Gordonsville',
        '226': '226. Town of Goshen', '227': '227. Town of Gretna',
        '228': '228. Town of Grottoes', '229': '229. Town of Grundy',
        '230': '230. Town of Halifax', '231': '231. Town of Hallwood',
        '232': '232. Town of Hamilton', '233': '233. Town of Haymarket',
        '234': '234. Town of Haysi', '235': '235. Town of Herndon',
        '236': '236. Town of Hillsboro', '237': '237. Town of Hillsville',
        '239': '239. Town of Honaker', '240': '240. Town of Independence',
        '241': '241. Town of Iron Gate', '242': '242. Town of Irvington',
        '243': '243. Town of Ivor', '244': '244. Town of Jarratt',
        '245': '245. Town of Jonesville', '246': '246. Town of Keller',
        '247': '247. Town of Kenbridge', '248': '248. Town of Keysville',
        '249': '249. Town of Kilmarnock', '250': '250. Town of LaCrosse',
        '251': '251. Town of Lawrenceville', '252': '252. Town of Lebanon',
        '253': '253. Town of Leesburg', '254': '254. Town of Louisa',
        '255': '255. Town of Lovettsville', '256': '256. Town of Madison',
        '257': '257. Town of McKenney', '258': '258. Town of Melfa',
        '259': '259. Town of Middleburg', '260': '260. Town of Middletown',
        '261': '261. Town of Mineral', '262': '262. Town of Monterey',
        '263': '263. Town of Montross', '264': '264. Town of Mount Crawford',
        '265': '265. Town of Mount Jackson', '266': '266. Town of Narrows',
        '267': '267. Town of Nassawadox', '268': '268. Town of New Castle',
        '269': '269. Town of New Market', '270': '270. Town of Newsoms',
        '271': '271. Town of Nickelsville', '272': '272. Town of Occoquan',
        '273': '273. Town of Onancock', '274': '274. Town of Onley',
        '275': '275. Town of Orange', '276': '276. Town of Painter',
        '277': '277. Town of Pamplin City', '278': '278. Town of Parksley',
        '279': '279. Town of Pearisburg', '280': '280. Town of Pembroke',
        '281': '281. Town of Pennington Gap', '282': '282. Town of Phenix',
        '283': '283. Town of Pocahontas', '284': '284. Town of Port Royal',
        '285': '285. Town of Pound', '286': '286. Town of Purcellville',
        '287': '287. Town of Quantico', '288': '288. Town of Remington',
        '289': '289. Town of Rich Creek', '290': '290. Town of Ridgeway',
        '291': '291. Town of Round Hill', '292': '292. Town of Rural Retreat',
        '293': '293. Town of St. Charles', '294': '294. Town of Saint Paul',
        '295': '295. Town of Saltville', '296': '296. Town of Saxis',
        '297': '297. Town of Scottsburg', '298': '298. Town of Scottsville',
        '299': '299. Town of Shenandoah', '300': '300. Town of Smithfield',
        '301': '301. Town of South Hill', '302': '302. Town of Stanardsville',
        '303': '303. Town of Stanley', '304': '304. Town of Stephens City',
        '305': '305. Town of Stony Creek', '306': '306. Town of Strasburg',
        '307': '307. Town of Stuart', '308': '308. Town of Surry',
        '309': '309. Town of Tangier', '310': '310. Town of Tappahannock',
        '311': '311. Town of The Plains', '312': '312. Town of Timberville',
        '313': '313. Town of Toms Brook', '314': '314. Town of Troutdale',
        '315': '315. Town of Troutville', '316': '316. Town of Urbanna',
        '317': '317. Town of Victoria', '318': '318. Town of Virgilina',
        '319': '319. Town of Wachapreague', '320': '320. Town of Wakefield',
        '321': '321. Town of Warsaw', '322': '322. Town of Washington',
        '323': '323. Town of Waverly', '324': '324. Town of Weber City',
        '325': '325. Town of West Point', '327': '327. Town of White Stone',
        '328': '328. Town of Windsor', '329': '329. Town of Wise',
        '330': '330. Town of Woodstock', '331': '331. Town of Hurt',
        '339': '339. Town of Clinchco',
    }

    # Boolean Yes/No fields
    BOOL_YES_NO_FIELDS = [
        'Alcohol?', 'Bike?', 'Pedestrian?', 'Speed?', 'Distracted?',
        'Drowsy?', 'Drug Related?', 'Guardrail Related?', 'Hitrun?',
        'Lgtruck?', 'Motorcycle?', 'Animal Related?', 'Senior?',
        'Young?', 'Mainline?', 'Night?',
    ]

    # Idempotency markers: if a column value contains this string, it's already decoded
    _SKIP_MARKERS = {
        'Collision Type': 'Rear End',
        'Weather Condition': 'Adverse',
        'Light Condition': 'Dawn',
        'Roadway Surface Condition': 'Dry',
        'Relation To Roadway': 'Main-Line',
        'Roadway Alignment': 'Straight',
        'Roadway Surface Type': 'Concrete',
        'Roadway Defect': 'No Defects',
        'Roadway Description': 'Two-Way',
        'Intersection Type': 'Not at Intersection',
        'Traffic Control Type': 'No Traffic Control',
        'Traffic Control Status': 'Working',
        'Work Zone Related': 'Yes',
        'Work Zone Location': 'Warning',
        'Work Zone Type': 'Lane Closure',
        'School Zone': 'Yes',
        'First Harmful Event': 'Bank Or Ledge',
        'First Harmful Event Loc': 'On Roadway',
        'DOT District': 'Bristol',
        'Ownership': 'Hwy Agency',
        'SYSTEM': 'VDOT',
        'Functional Class': 'Interstate',
        'Facility Type': 'Way',
        'Area Type': 'Rural',
        'RoadDeparture Type': 'NOT_RD',
        'Intersection Analysis': 'Intersection',
    }

    def __init__(self, state_key: str, config_path: Optional[str] = None):
        super().__init__(state_key, config_path)
        # Track format detection per column to avoid re-checking every row
        self._format_detected = False
        self._needs_decode = {}  # col → bool

    def _detect_format(self, row: Dict[str, str]) -> None:
        """Detect whether this data uses old (text) or new (numeric) format."""
        self._format_detected = True

        # Check each decode-map column
        for col, code_map in self.DECODE_MAPS.items():
            val = row.get(col, '').strip()
            if not val:
                self._needs_decode[col] = False
                continue
            skip_marker = self._SKIP_MARKERS.get(col)
            if skip_marker and skip_marker in val:
                self._needs_decode[col] = False  # already decoded
            elif val in code_map:
                self._needs_decode[col] = True  # numeric code found
            else:
                self._needs_decode[col] = False

        # Check Planning District
        pd_val = row.get('Planning District', '').strip()
        if pd_val and pd_val in self.PLANNING_DISTRICT_MAP and not re.search(r'[a-zA-Z]', pd_val):
            self._needs_decode['Planning District'] = True
        else:
            self._needs_decode['Planning District'] = False

        # Check Physical Juris Name
        pjn_val = row.get('Physical Juris Name', '').strip()
        if pjn_val and pjn_val in self.JURIS_MAP and not re.search(r'County|City|Town', pjn_val):
            self._needs_decode['Physical Juris Name'] = True
        else:
            self._needs_decode['Physical Juris Name'] = False

        # Check boolean fields
        # Sample one boolean field to decide
        for bf in self.BOOL_YES_NO_FIELDS:
            bv = row.get(bf, '').strip()
            if bv in ('0', '1'):
                self._needs_decode['_booleans'] = True
                break
            elif bv in ('Yes', 'No', ''):
                self._needs_decode['_booleans'] = False
                break
        else:
            self._needs_decode['_booleans'] = False

        # Check Unrestrained?
        uv = row.get('Unrestrained?', '').strip()
        if uv in ('0', '1'):
            self._needs_decode['Unrestrained?'] = True
        else:
            self._needs_decode['Unrestrained?'] = False

    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        normalized = dict(row)

        # Step 1: Apply column renames
        for old_name, new_name in self.COLUMN_RENAMES.items():
            if old_name in normalized and old_name != new_name:
                normalized[new_name] = normalized.pop(old_name)

        # Step 2: Detect format on first row
        if not self._format_detected:
            self._detect_format(normalized)

        # Step 3: Decode coded columns
        for col, code_map in self.DECODE_MAPS.items():
            if self._needs_decode.get(col):
                val = normalized.get(col, '').strip()
                if val in code_map:
                    normalized[col] = code_map[val]

        # Step 4: Decode Planning District
        if self._needs_decode.get('Planning District'):
            val = normalized.get('Planning District', '').strip()
            if val in self.PLANNING_DISTRICT_MAP:
                normalized['Planning District'] = self.PLANNING_DISTRICT_MAP[val]

        # Step 5: Decode Physical Juris Name
        if self._needs_decode.get('Physical Juris Name'):
            val = normalized.get('Physical Juris Name', '').strip()
            if val in self.JURIS_MAP:
                normalized['Physical Juris Name'] = self.JURIS_MAP[val]

        # Step 6: Boolean 0/1 → Yes/No
        if self._needs_decode.get('_booleans'):
            for col in self.BOOL_YES_NO_FIELDS:
                val = normalized.get(col, '').strip()
                if val == '1':
                    normalized[col] = 'Yes'
                elif val == '0':
                    normalized[col] = 'No'

        # Step 7: Unrestrained? special case (0→Belted, 1→Unbelted)
        if self._needs_decode.get('Unrestrained?'):
            val = normalized.get('Unrestrained?', '').strip()
            if val == '1':
                normalized['Unrestrained?'] = 'Unbelted'
            elif val == '0':
                normalized['Unrestrained?'] = 'Belted'

        # Step 8: Epoch date conversion
        crash_date = normalized.get('Crash Date', '').strip()
        if crash_date and re.match(r'^\d{10,}$', crash_date):
            try:
                from datetime import datetime, timezone
                ts = int(float(crash_date)) / 1000
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                normalized['Crash Date'] = dt.strftime('%-m/%-d/%Y %-I:%M:%S %p')
            except (ValueError, TypeError, OSError):
                pass

        normalized['_source_state'] = 'virginia'
        return normalized


class ColoradoNormalizer(BaseNormalizer):
    """Colorado CDOT data normalizer - converts to Virginia-compatible VDOT format.

    Produces output with numbered-prefix values matching the Henrico VDOT reference
    format used by CRASH LENS (e.g., '1. Rear End', '2. Daylight').
    """

    # ----------------------------------------------------------------
    # Step 1: Colorado MHE/Crash Type → intermediate collision category
    # ----------------------------------------------------------------
    COLLISION_MAP = {
        'Rear-End': 'Rear End', 'Front to Rear': 'Rear End',
        'Broadside': 'Angle', 'Front to Side': 'Angle',
        'Rear to Side': 'Angle', 'Approach Turn': 'Angle',
        'Overtaking Turn': 'Angle',
        'Head-On': 'Head On', 'Front to Front': 'Head On',
        'Sideswipe Same Direction': 'Sideswipe - Same Direction',
        'Side to Side-Same Direction': 'Sideswipe - Same Direction',
        'Sideswipe Opposite Direction': 'Sideswipe - Opposite Direction',
        'Side to Side-Opposite Direction': 'Sideswipe - Opposite Direction',
        'Overturning/Rollover': 'Non-Collision',
        'Other Non-Collision': 'Non-Collision',
        'Fell from Motor Vehicle': 'Non-Collision', 'Ground': 'Non-Collision',
        'Pedestrian': 'Pedestrian',
        'School Age To/From School': 'Pedestrian',
        'Bicycle/Motorized Bicycle': 'Bicyclist',
        'Wild Animal': 'Other Animal', 'Domestic Animal': 'Other Animal',
        # Fixed objects
        'Light Pole/Utility Pole': 'Fixed Object - Off Road',
        'Traffic Signal Pole': 'Fixed Object - Off Road',
        'Concrete Highway Barrier': 'Fixed Object - Off Road',
        'Guardrail Face': 'Fixed Object - Off Road',
        'Guardrail End': 'Fixed Object - Off Road',
        'Cable Rail': 'Fixed Object - Off Road',
        'Tree': 'Fixed Object - Off Road', 'Fence': 'Fixed Object - Off Road',
        'Sign': 'Fixed Object - Off Road', 'Curb': 'Fixed Object - Off Road',
        'Embankment': 'Fixed Object - Off Road', 'Ditch': 'Fixed Object - Off Road',
        'Large Rocks or Boulder': 'Fixed Object - Off Road',
        'Electrical/Utility Box': 'Fixed Object - Off Road',
        'Electical/Utility Box': 'Fixed Object - Off Road',  # typo in source
        'Crash Cushion/Traffic Barrel': 'Fixed Object - Off Road',
        'Mailbox': 'Fixed Object - Off Road',
        'Delineator/Milepost': 'Fixed Object - Off Road',
        'Culvert or Headwall': 'Fixed Object - Off Road',
        'Wall or Building': 'Fixed Object - Off Road',
        'Barricade': 'Fixed Object - Off Road',
        'Bridge Structure (Not Overhead)': 'Fixed Object - Off Road',
        'Overhead Structure (Not Bridge)': 'Fixed Object - Off Road',
        'Railroad Crossing Equipment': 'Fixed Object - Off Road',
        'Other Fixed Object (Describe in Narrative)': 'Fixed Object - Off Road',
        'Vehicle Debris or Cargo': 'Fixed Object in Road',
        'Parked Motor Vehicle': 'Other',
        'Other Non-Fixed Object (Describe in Narrative)': 'Other',
        'Other Non-Fixed Object Describe in Narrative)': 'Other',
    }

    # ----------------------------------------------------------------
    # Step 2: Intermediate category → VDOT numbered-prefix format
    # ----------------------------------------------------------------
    COLLISION_VDOT_MAP = {
        'Rear End': '1. Rear End',
        'Angle': '2. Angle',
        'Head On': '3. Head On',
        'Sideswipe - Same Direction': '4. Sideswipe - Same Direction',
        'Sideswipe - Opposite Direction': '5. Sideswipe - Opposite Direction',
        'Non-Collision': '8. Non-Collision',
        'Fixed Object - Off Road': '9. Fixed Object - Off Road',
        'Other Animal': '10. Deer/Animal',
        'Fixed Object in Road': '11. Fixed Object in Road',
        'Pedestrian': '12. Ped',
        'Bicyclist': '13. Bicycle',
        'Other': '16. Other',
        'Unknown': '16. Other',
    }

    # ----------------------------------------------------------------
    # Weather: Colorado raw → VDOT numbered format
    # ----------------------------------------------------------------
    WEATHER_VDOT_MAP = {
        'Clear': '1. No Adverse Condition (Clear/Cloudy)',
        'Cloudy': '1. No Adverse Condition (Clear/Cloudy)',
        'Rain': '5. Rain',
        'Snow': '4. Snow',
        'Blowing Snow': '4. Snow',
        'Sleet, Hail (Freezing Rain or Drizzle)': '6. Sleet/Hail/Freezing',
        'Freezing Rain or Freezing Drizzle': '6. Sleet/Hail/Freezing',
        'Sleet or Hail': '6. Sleet/Hail/Freezing',
        'Fog': '3. Fog/Smog/Smoke',
        'Blowing Sand, Soil, Dirt': '7. Blowing Sand/Dust',
        'Dust': '7. Blowing Sand/Dust',
        'Severe Crosswinds': '8. Severe Crosswinds',
        'Wind': '8. Severe Crosswinds',
    }

    # ----------------------------------------------------------------
    # Light: Colorado raw → VDOT numbered format
    # ----------------------------------------------------------------
    LIGHT_VDOT_MAP = {
        'Daylight': '2. Daylight',
        'Dark \u2013 Lighted': '4. Darkness - Road Lighted',
        'Dark \u2013 Unlighted': '5. Darkness - Road Not Lighted',
        'Dark - Lighted': '4. Darkness - Road Lighted',
        'Dark - Unlighted': '5. Darkness - Road Not Lighted',
        'Dawn or Dusk': '1. Dawn',  # refined by time in normalize_row
    }

    # ----------------------------------------------------------------
    # Roadway Surface: Colorado raw → VDOT numbered format
    # ----------------------------------------------------------------
    SURFACE_VDOT_MAP = {
        'Dry': '1. Dry',
        'Wet': '2. Wet',
        'Snow': '3. Snow', 'Snowy': '3. Snow',
        'Snowy W/Visible Icy Road Treatment': '3. Snow',
        'Ice': '5. Ice', 'Icy': '5. Ice',
        'Icy W/Visible Icy Road Treatment': '5. Ice',
        'Slush': '4. Slush', 'Slushy': '4. Slush',
        'Slushy W/Visible Icy Road Treatment': '4. Slush',
        'Dry W/Visible Icy Road Treatment': '1. Dry',
        'Wet W/Visible Icy Road Treatment': '2. Wet',
        'Sand, Mud, Dirt, Oil, Gravel': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Sand/Gravel': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Muddy': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Water (Standing, Moving)': '7. Water',
        'Roto-Milled': '16. Other',
        'Other': '16. Other',
    }

    # ----------------------------------------------------------------
    # Roadway Description: derive road geometry from System Code
    # (CO "Road Description" is actually intersection relation, not geometry)
    # ----------------------------------------------------------------
    ROAD_DESC_FROM_SYSTEM = {
        'Interstate Highway': '3. Two-Way, Divided, Positive Median Barrier',
        'State Highway': '2. Two-Way, Divided, Unprotected Median',
        'County Road': '1. Two-Way, Not Divided',
        'City Street': '1. Two-Way, Not Divided',
        'Frontage Road': '1. Two-Way, Not Divided',
    }

    # ----------------------------------------------------------------
    # Intersection Type: CO Road Description → VDOT approach counts
    # ----------------------------------------------------------------
    INTERSECTION_VDOT_MAP = {
        'Non-Intersection': '1. Not at Intersection',
        'At Intersection': '4. Four Approaches',
        'Intersection Related': '4. Four Approaches',
        'Driveway Access Related': '2. Two Approaches',
        'Ramp': '1. Not at Intersection',
        'Ramp-related': '1. Not at Intersection',
        'Roundabout': '5. Roundabout',
        'Crossover-Related': '1. Not at Intersection',
        'Crossover-Related ': '1. Not at Intersection',
        'Express/Managed/HOV Lane': '1. Not at Intersection',
        'Auxiliary Lane': '1. Not at Intersection',
        'Alley Related': '4. Four Approaches',
        'Railroad Crossing Related': '2. Two Approaches',
        'Mid-Block Crosswalk': '1. Not at Intersection',
    }

    # ----------------------------------------------------------------
    # Relation To Roadway: CO Road Description → VDOT relation codes
    # ----------------------------------------------------------------
    RELATION_TO_ROADWAY_MAP = {
        'Non-Intersection': '8. Non-Intersection',
        'At Intersection': '9. Within Intersection',
        'Intersection Related': '10. Intersection Related - Within 150 Feet',
        'Ramp': '2. Acceleration/Deceleration Lanes',
        'Ramp-related': '2. Acceleration/Deceleration Lanes',
        'Driveway Access Related': '8. Non-Intersection',
        'Crossover-Related': '1. Main-Line Roadway',
        'Crossover-Related ': '1. Main-Line Roadway',
        'Roundabout': '9. Within Intersection',
        'Express/Managed/HOV Lane': '1. Main-Line Roadway',
        'Auxiliary Lane': '1. Main-Line Roadway',
        'Alley Related': '8. Non-Intersection',
        'Railroad Crossing Related': '8. Non-Intersection',
        'Mid-Block Crosswalk': '8. Non-Intersection',
    }

    # ----------------------------------------------------------------
    # First Harmful Event: CO raw → VDOT numbered codes
    # ----------------------------------------------------------------
    FIRST_HE_VDOT_MAP = {
        # Motor vehicle collisions
        'Front to Rear': '20. Motor Vehicle In Transport',
        'Front to Side': '20. Motor Vehicle In Transport',
        'Rear to Side': '20. Motor Vehicle In Transport',
        'Front to Front': '20. Motor Vehicle In Transport',
        'Side to Side-Same Direction': '20. Motor Vehicle In Transport',
        'Side to Side-Opposite Direction': '20. Motor Vehicle In Transport',
        'Rear to Rear': '20. Motor Vehicle In Transport',
        'Other Non-Collision': '38. Other Non-Collision',
        'Other Non-Fixed Object (Describe in Narrative)': '37. Other Object (Not Fixed)',
        'Other Non-Fixed Object Describe in Narrative)': '37. Other Object (Not Fixed)',
        # Fixed objects
        'Light Pole/Utility Pole': '3. Utility Pole',
        'Traffic Signal Pole': '3. Utility Pole',
        'Electrical/Utility Box': '3. Utility Pole',
        'Electical/Utility Box': '3. Utility Pole',  # typo in source
        'Tree': '2. Trees',
        'Guardrail Face': '5. Guard Rail',
        'Guardrail End': '5. Guard Rail',
        'Cable Rail': '5. Guard Rail',
        'Fence': '8. Fence',
        'Curb': '27. Curb',
        'Ditch': '14. Ditch',
        'Embankment': '13. Embankment',
        'Sign': '4. Traffic Sign Support',
        'Concrete Highway Barrier': '15. Concrete Traffic Barrier',
        'Crash Cushion/Traffic Barrel': '16. Impact Attenuator/Crash Cushion',
        'Bridge Structure (Not Overhead)': '17. Bridge Pier or Abutment',
        'Overhead Structure (Not Bridge)': '18. Overhead Sign Post',
        'Wall or Building': '12. Building/Structure',
        'Large Rocks or Boulder': '24. Other Fixed Object',
        'Delineator/Milepost': '24. Other Fixed Object',
        'Mailbox': '7. Mailbox',
        'Culvert or Headwall': '10. Culvert',
        'Barricade': '24. Other Fixed Object',
        'Railroad Crossing Equipment': '24. Other Fixed Object',
        'Other Fixed Object (Describe in Narrative)': '24. Other Fixed Object',
        # Rollover / non-collision
        'Overturning/Rollover': '30. Overturn (Rollover)',
        'Ground': '30. Overturn (Rollover)',
        'Fell from Motor Vehicle': '39. Fell/Jumped From Vehicle',
        # Non-motorist
        'Pedestrian': '19. Ped',
        'School Age To/From School': '19. Ped',
        'Bicycle/Motorized Bicycle': '22. Bicycle',
        # Animals
        'Wild Animal': '21. Animal',
        'Domestic Animal': '21. Animal',
        # Parked vehicles
        'Parked Motor Vehicle': '6. Parked Vehicle',
        'Vehicle Debris or Cargo': '37. Other Object (Not Fixed)',
        # Fire
        'Fire/Explosion': '34. Fire/Explosion',
    }

    # ----------------------------------------------------------------
    # First Harmful Event Location: CO raw → VDOT numbered codes
    # ----------------------------------------------------------------
    FIRST_HE_LOC_VDOT_MAP = {
        'On Roadway': '1. On Roadway',
        'Center median/ Island': '3. Median',
        'Ran off left side': '4. Roadside',
        'Ran off right side': '4. Roadside',
        'Shoulder': '2. Shoulder',
        'In Parking Lane': '1. On Roadway',
        'Gore': '5. Gore',
        'Vehicle crossed center median into opposing lanes': '3. Median',
        'Ran off "T" intersection': '4. Roadside',
        'On private property': '9. Outside Right-of-Way',
    }

    # ----------------------------------------------------------------
    # Ownership: derive from System Code
    # ----------------------------------------------------------------
    OWNERSHIP_FROM_SYSTEM = {
        'Interstate Highway': '1. State Hwy Agency',
        'State Highway': '1. State Hwy Agency',
        'County Road': '2. County Hwy Agency',
        'City Street': '3. City or Town Hwy Agency',
        'Frontage Road': '1. State Hwy Agency',
    }

    ROAD_SYSTEM_MAP = {
        'City Street': 'Non-DOT secondary',
        'County Road': 'Non-DOT secondary',
        'State Highway': 'Primary',
        'Interstate Highway': 'Interstate',
        'Frontage Road': 'Secondary',
        'Non Crash': 'Non-DOT secondary',
    }

    SPEED_ACTIONS = {
        'Too Fast for Conditions', 'Exceeded Speed Limit',
        'Exceeded Safe/Posted Speed', 'Speeding'
    }

    DISTRACTED_VALUES = {
        'Distracted', 'Cell Phone', 'Inattention',
        'Distracted - Cell Phone/Electronic Device',
        'Distracted - Other', 'Inattentive/Distracted'
    }

    DROWSY_VALUES = {
        'Asleep or Fatigued', 'Asleep/Fatigued', 'Fatigued/Asleep',
        'Ill/Asleep/Fatigued', 'Fatigued', 'Asleep'
    }

    ALCOHOL_POSITIVE = {
        'Yes - SFST', 'Yes - BAC', 'Yes - Both', 'Yes - Observation'
    }

    DRUG_POSITIVE = {
        'Yes - Observation', 'Yes - SFST', 'Yes - Both', 'Yes - Test Results'
    }

    DARKNESS_VALUES = {
        'Dark \u2013 Lighted', 'Dark \u2013 Unlighted',
        'Dark - Lighted', 'Dark - Unlighted'
    }

    # Colorado-specific detail columns to preserve (output_name, raw_col_name)
    CO_EXTRA_COLUMNS = [
        ('_co_total_vehicles', 'Total Vehicles'),
        ('_co_mhe', 'MHE'),
        ('_co_crash_type', 'Crash Type'),
        ('_co_link', 'Link'),
        ('_co_second_he', 'Second HE'),
        ('_co_third_he', 'Third HE'),
        ('_co_wild_animal', 'Wild Animal'),
        ('_co_secondary_crash', 'Secondary Crash'),
        ('_co_weather2', 'Weather Condition 2'),
        ('_co_lane_position', 'Lane Position'),
        ('_co_injury00_uninjured', 'Injury 00'),
        # TU-1 fields
        ('_co_tu1_direction', 'TU-1 Direction'),
        ('_co_tu1_movement', 'TU-1 Movement'),
        ('_co_tu1_vehicle_type', 'TU-1 Type'),
        ('_co_tu1_speed_limit', 'TU-1 Speed Limit'),
        ('_co_tu1_estimated_speed', 'TU-1 Estimated Speed'),
        ('_co_tu1_stated_speed', 'TU-1 Speed'),
        ('_co_tu1_driver_action', 'TU-1 Driver Action'),
        ('_co_tu1_human_factor', 'TU-1 Human Contributing Factor'),
        ('_co_tu1_age', 'TU-1 Age'),
        ('_co_tu1_sex', 'TU-1 Sex '),
        # TU-2 fields
        ('_co_tu2_direction', 'TU-2 Direction'),
        ('_co_tu2_movement', 'TU-2 Movement'),
        ('_co_tu2_vehicle_type', 'TU-2 Type'),
        ('_co_tu2_speed_limit', 'TU-2 Speed Limit'),
        ('_co_tu2_estimated_speed', 'TU-2 Estimated Speed'),
        ('_co_tu2_stated_speed', 'TU-2 Speed'),
        ('_co_tu2_driver_action', 'TU-2 Driver Action'),
        ('_co_tu2_human_factor', 'TU-2 Human Contributing Factor'),
        ('_co_tu2_age', 'TU-2 Age'),
        ('_co_tu2_sex', 'TU-2 Sex'),
        # Non-motorist (ped/bike) fields
        ('_co_nm1_type', 'TU-1 NM Type'),
        ('_co_nm1_age', 'TU-1 NM Age '),
        ('_co_nm1_sex', 'TU-1 NM Sex '),
        ('_co_nm1_action', 'TU-1 NM Action '),
        ('_co_nm1_movement', 'TU-1 NM Movement'),
        ('_co_nm1_location', 'TU-1 NM Location '),
        ('_co_nm1_facility', 'TU-1 NM Facility Available'),
        ('_co_nm1_contributing_factor', 'TU-1 NM Human Contributing Factor '),
        ('_co_nm2_type', 'TU-2 NM Type'),
        ('_co_nm2_age', 'TU-2 NM Age '),
        ('_co_nm2_sex', 'TU-2 NM Sex '),
        ('_co_nm2_action', 'TU-2 NM Action '),
        ('_co_nm2_movement', 'TU-2 NM Movement'),
        ('_co_nm2_location', 'TU-2 NM Location '),
        ('_co_nm2_facility', 'TU-2 NM Facility Available'),
        ('_co_nm2_contributing_factor', 'TU-2 NM Human Contributing Factor '),
    ]

    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        n = {}

        # --- ID ---
        n['Document Nbr'] = row.get('CUID', '').strip()

        # --- Date/Time ---
        raw_date = row.get('Crash Date', '').strip()
        n['Crash Date'] = raw_date
        n['Crash Year'] = self._extract_year(raw_date)
        mil_time = row.get('Crash Time', '').replace(':', '').strip()[:4]
        n['Crash Military Time'] = mil_time

        # --- Severity (derived from injury counts) ---
        inj_k = self._int(row.get('Injury 04', '0'))
        inj_a = self._int(row.get('Injury 03', '0'))
        inj_b = self._int(row.get('Injury 02', '0'))
        inj_c = self._int(row.get('Injury 01', '0'))

        if inj_k > 0:
            n['Crash Severity'] = 'K'
        elif inj_a > 0:
            n['Crash Severity'] = 'A'
        elif inj_b > 0:
            n['Crash Severity'] = 'B'
        elif inj_c > 0:
            n['Crash Severity'] = 'C'
        else:
            n['Crash Severity'] = 'O'

        n['K_People'] = str(inj_k)
        n['A_People'] = str(inj_a)
        n['B_People'] = str(inj_b)
        n['C_People'] = str(inj_c)

        # --- Collision Type (two-step: CO raw → category → VDOT numbered) ---
        crash_type = row.get('Crash Type', '').strip() or row.get('MHE', '').strip()
        intermediate = self.COLLISION_MAP.get(crash_type, crash_type or 'Unknown')
        n['Collision Type'] = self.COLLISION_VDOT_MAP.get(intermediate, '16. Other')

        # --- Weather Condition (VDOT numbered format) ---
        raw_weather = row.get('Weather Condition', '').strip()
        n['Weather Condition'] = self.WEATHER_VDOT_MAP.get(raw_weather, raw_weather)

        # --- Light Condition (VDOT numbered format + Dawn/Dusk split by time) ---
        raw_light = row.get('Lighting Conditions', '').strip()
        mapped_light = self.LIGHT_VDOT_MAP.get(raw_light, raw_light)
        if mapped_light == '1. Dawn' and mil_time:
            try:
                if int(mil_time) >= 1200:
                    mapped_light = '3. Dusk'
            except ValueError:
                pass
        n['Light Condition'] = mapped_light

        # --- Roadway Surface Condition (VDOT numbered format) ---
        raw_surface = row.get('Road Condition', '').strip()
        n['Roadway Surface Condition'] = self.SURFACE_VDOT_MAP.get(raw_surface, raw_surface)

        # --- Roadway Alignment (VDOT 4-category system) ---
        n['Roadway Alignment'] = self._map_alignment(
            row.get('Road Contour Curves', '').strip(),
            row.get('Road Contour Grade', '').strip()
        )

        # --- Road Description / Intersection / Relation To Roadway ---
        # CO "Road Description" is actually intersection relation, NOT road geometry
        road_desc = row.get('Road Description', '').strip()
        system = row.get('System Code', '').strip()

        # Roadway Description = road geometry (derived from system code)
        n['Roadway Description'] = self.ROAD_DESC_FROM_SYSTEM.get(
            system, '1. Two-Way, Not Divided')

        # Intersection Type = VDOT approach-count format (derived from CO Road Description)
        n['Intersection Type'] = self.INTERSECTION_VDOT_MAP.get(
            road_desc, '1. Not at Intersection')

        # Relation To Roadway = VDOT location relationship (from CO Road Description)
        n['Relation To Roadway'] = self.RELATION_TO_ROADWAY_MAP.get(road_desc, road_desc)

        # --- Route & Location ---
        n['RTE Name'] = self._build_route_name(row)
        n['SYSTEM'] = self.ROAD_SYSTEM_MAP.get(system, 'Non-DOT secondary')
        n['Node'] = self._build_node_id(row)
        n['RNS MP'] = row.get('Rd_Section', '').strip() if system in (
            'State Highway', 'Interstate Highway'
        ) else ''

        # --- Coordinates (x=longitude, y=latitude per Virginia convention) ---
        n['x'] = row.get('Longitude', '').strip()
        n['y'] = row.get('Latitude', '').strip()

        # --- Jurisdiction ---
        n['Physical Juris Name'] = row.get('County', '').strip()

        # --- Boolean flags (derived) ---
        n['Pedestrian?'] = 'Yes' if self._check_pedestrian(row) else 'No'
        n['Bike?'] = 'Yes' if self._check_bicycle(row) else 'No'
        n['Alcohol?'] = 'Yes' if self._check_alcohol(row) else 'No'
        n['Speed?'] = 'Yes' if self._check_speed(row) else 'No'
        n['Hitrun?'] = 'Yes' if self._check_hitrun(row) else 'No'
        n['Motorcycle?'] = 'Yes' if self._check_vehicle_type(row, 'Motorcycle') else 'No'
        n['Night?'] = 'Yes' if self._is_nighttime(row.get('Lighting Conditions', '')) else 'No'
        n['Distracted?'] = 'Yes' if self._check_distracted(row) else 'No'
        n['Drowsy?'] = 'Yes' if self._check_drowsy(row) else 'No'
        n['Drug Related?'] = 'Yes' if self._check_drugs(row) else 'No'
        n['Young?'] = 'Yes' if self._check_age(row, 16, 20) else 'No'
        n['Senior?'] = 'Yes' if self._check_age(row, 65, 999) else 'No'
        n['Unrestrained?'] = 'Yes' if self._check_unrestrained(row) else 'No'
        n['School Zone'] = 'Yes' if row.get('School Zone', '').strip() in ('TRUE', 'True') else 'No'
        n['Work Zone Related'] = 'Yes' if row.get('Construction Zone', '').strip() in ('TRUE', 'True') else 'No'

        # --- Derived safety fields (for Safety Focus tab) ---
        n['Animal Related?'] = 'Yes' if self._check_animal(row) else 'No'
        n['Guardrail Related?'] = 'Yes' if self._check_guardrail(row) else 'No'
        n['Lgtruck?'] = 'Yes' if self._check_large_truck(row) else 'No'
        n['RoadDeparture Type'] = self._derive_road_departure_type(row)
        n['Intersection Analysis'] = self._derive_intersection_analysis(row)
        n['Max Speed Diff'] = self._calc_speed_diff(row)

        # --- Traffic Control (not available in CDOT data) ---
        n['Traffic Control Type'] = ''
        n['Traffic Control Status'] = ''

        # --- Infrastructure fields ---
        n['Functional Class'] = ''
        n['Area Type'] = ''
        n['Facility Type'] = ''
        n['Ownership'] = self.OWNERSHIP_FROM_SYSTEM.get(system, '')

        # --- First Harmful Event (VDOT numbered codes) ---
        raw_fhe = row.get('First HE', '').strip()
        n['First Harmful Event'] = self.FIRST_HE_VDOT_MAP.get(raw_fhe, raw_fhe)

        # --- First Harmful Event Location (VDOT numbered codes) ---
        raw_fhe_loc = row.get('Location', '').strip()
        n['First Harmful Event Loc'] = self.FIRST_HE_LOC_VDOT_MAP.get(
            raw_fhe_loc, raw_fhe_loc)

        # --- Vehicle Count ---
        n['Vehicle Count'] = row.get('Total Vehicles', '').strip()

        # --- Injury counts ---
        n['Persons Injured'] = row.get('Number Injured', '0').strip()

        # --- Pedestrians Killed/Injured (derived from NM data) ---
        pk, pi = self._derive_ped_killed_injured(row)
        n['Pedestrians Killed'] = str(pk)
        n['Pedestrians Injured'] = str(pi)

        # --- Source tracking ---
        n['_source_state'] = 'colorado'
        n['_co_system_code'] = system
        n['_co_agency_id'] = row.get('Agency Id', '').strip()
        n['_co_rd_number'] = row.get('Rd_Number', '').strip()
        n['_co_location1'] = row.get('Location 1', '').strip()
        n['_co_location2'] = row.get('Location 2', '').strip()
        n['_co_city'] = row.get('City', '').strip()

        # --- Colorado-specific detail columns ---
        for col_name, raw_col in self.CO_EXTRA_COLUMNS:
            n[col_name] = row.get(raw_col, '').strip()

        return n

    # --- Helpers ---

    def _int(self, val: str) -> int:
        try:
            return int(val.strip()) if val.strip() else 0
        except (ValueError, TypeError):
            return 0

    def _extract_year(self, date_str: str) -> str:
        if not date_str:
            return ''
        parts = date_str.split('/')
        if len(parts) == 3:
            return parts[2][:4]
        parts = date_str.split('-')
        if len(parts) == 3 and len(parts[0]) == 4:
            return parts[0]
        return ''

    def _build_route_name(self, row: Dict[str, str]) -> str:
        system = row.get('System Code', '').strip()
        rd_num = row.get('Rd_Number', '').strip()
        loc1 = row.get('Location 1', '').strip()

        if system == 'Interstate Highway':
            num = re.sub(r'^0+', '', rd_num).rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')
            return f'I-{num}' if num else loc1 or f'I-{rd_num}'
        if system == 'State Highway':
            if loc1:
                return loc1
            num = re.sub(r'^0+', '', rd_num).rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')
            return f'CO-{num}' if num else f'CO-{rd_num}'
        if system == 'Frontage Road':
            return f'{loc1} (Frontage)' if loc1 else f'Frontage Rd {rd_num}'
        return loc1 or f'Road {rd_num}'

    def _build_node_id(self, row: Dict[str, str]) -> str:
        rd = row.get('Road Description', '').strip()
        loc1 = row.get('Location 1', '').strip()
        loc2 = row.get('Location 2', '').strip()

        if rd in ('At Intersection', 'Intersection Related', 'Roundabout'):
            if loc1 and loc2:
                roads = sorted([loc1, loc2])
                return f'{roads[0]} & {roads[1]}'
        return ''

    def _map_alignment(self, curves: str, grade: str) -> str:
        """Map CO Road Contour fields to VDOT 4-category alignment."""
        # Only recognized curve values count (Unknown, empty, Straight do not)
        curve_values = {'Curve Right', 'Curve Left'}
        grade_values = {'Uphill', 'Downhill', 'Hill Crest'}
        has_curve = curves in curve_values
        has_grade = grade in grade_values
        if has_curve and has_grade:
            return '4. Grade - Curve'
        if has_curve:
            return '2. Curve - Level'
        if has_grade:
            return '3. Grade - Straight'
        return '1. Straight - Level'

    def _derive_ped_killed_injured(self, row: Dict[str, str]):
        """Derive pedestrian killed/injured from NM type + injury counts."""
        ped_killed = 0
        ped_injured = 0
        for tu in ['TU-1', 'TU-2']:
            nm_type = row.get(f'{tu} NM Type', '').strip()
            if 'Pedestrian' in nm_type:
                if self._int(row.get('Injury 04', '0')) > 0:
                    ped_killed = max(ped_killed, 1)
                if (self._int(row.get('Injury 03', '0')) > 0 or
                        self._int(row.get('Injury 02', '0')) > 0 or
                        self._int(row.get('Injury 01', '0')) > 0):
                    ped_injured = max(ped_injured, 1)
        return ped_killed, ped_injured

    def _check_nm_type(self, row: Dict[str, str], nm_type: str) -> bool:
        tu1 = row.get('TU-1 NM Type', '').strip()
        tu2 = row.get('TU-2 NM Type', '').strip()
        return nm_type in tu1 or nm_type in tu2

    def _check_pedestrian(self, row: Dict[str, str]) -> bool:
        if self._check_nm_type(row, 'Pedestrian'):
            return True
        ct = row.get('Crash Type', '').strip()
        mhe = row.get('MHE', '').strip()
        return ct in ('Pedestrian', 'School Age To/From School') or \
               mhe in ('Pedestrian', 'School Age To/From School')

    def _check_bicycle(self, row: Dict[str, str]) -> bool:
        if self._check_nm_type(row, 'Bicycle'):
            return True
        ct = row.get('Crash Type', '').strip()
        mhe = row.get('MHE', '').strip()
        return 'Bicycle' in ct or 'Bicycle' in mhe

    def _check_alcohol(self, row: Dict[str, str]) -> bool:
        tu1 = row.get('TU-1 Alcohol Suspected', '').strip()
        tu2 = row.get('TU-2 Alcohol Suspected', '').strip()
        return tu1 in self.ALCOHOL_POSITIVE or tu2 in self.ALCOHOL_POSITIVE

    def _check_speed(self, row: Dict[str, str]) -> bool:
        fields = [
            row.get('TU-1 Driver Action', '').strip(),
            row.get('TU-2 Driver Action', '').strip(),
            row.get('TU-1 Human Contributing Factor', '').strip(),
            row.get('TU-2 Human Contributing Factor', '').strip(),
        ]
        return any(f in self.SPEED_ACTIONS for f in fields)

    def _check_hitrun(self, row: Dict[str, str]) -> bool:
        tu1 = row.get('TU-1 Hit And Run', '').strip()
        tu2 = row.get('TU-2 Hit And Run', '').strip()
        return tu1 == 'TRUE' or tu2 == 'TRUE'

    def _check_vehicle_type(self, row: Dict[str, str], vtype: str) -> bool:
        tu1 = row.get('TU-1 Type', '').strip()
        tu2 = row.get('TU-2 Type', '').strip()
        return vtype in tu1 or vtype in tu2

    def _is_nighttime(self, lighting: str) -> bool:
        return lighting.strip() in self.DARKNESS_VALUES

    def _check_distracted(self, row: Dict[str, str]) -> bool:
        fields = [
            row.get('TU-1 Driver Action', '').strip(),
            row.get('TU-2 Driver Action', '').strip(),
            row.get('TU-1 Human Contributing Factor', '').strip(),
            row.get('TU-2 Human Contributing Factor', '').strip(),
        ]
        return any(
            any(d in f for d in self.DISTRACTED_VALUES)
            for f in fields if f
        )

    def _check_drowsy(self, row: Dict[str, str]) -> bool:
        tu1 = row.get('TU-1 Human Contributing Factor', '').strip()
        tu2 = row.get('TU-2 Human Contributing Factor', '').strip()
        return any(v in tu1 or v in tu2 for v in self.DROWSY_VALUES)

    def _check_drugs(self, row: Dict[str, str]) -> bool:
        fields = [
            row.get('TU-1  Marijuana Suspected', '').strip(),
            row.get('TU-2 Marijuana Suspected', '').strip(),
            row.get('TU-1 Other Drugs Suspected ', '').strip(),
            row.get('TU-2 Other Drugs Suspected ', '').strip(),
        ]
        return any(f in self.DRUG_POSITIVE for f in fields)

    def _check_age(self, row: Dict[str, str], min_age: int, max_age: int) -> bool:
        for col in ['TU-1 Age', 'TU-2 Age']:
            try:
                age = int(row.get(col, '0').strip())
                if min_age <= age <= max_age:
                    return True
            except (ValueError, TypeError):
                pass
        return False

    def _check_unrestrained(self, row: Dict[str, str]) -> bool:
        unrestrained = {'Not Used', 'Improperly Used'}
        tu1 = row.get('TU-1 Safety restraint Use', '').strip()
        tu2 = row.get('TU-2 Safety restraint Use', '').strip()
        return tu1 in unrestrained or tu2 in unrestrained

    def _check_animal(self, row: Dict[str, str]) -> bool:
        wild = row.get('Wild Animal', '').strip()
        if wild:
            return True
        ct = (row.get('Crash Type', '') or row.get('MHE', '')).strip()
        return ct in ('Wild Animal', 'Domestic Animal')

    def _check_guardrail(self, row: Dict[str, str]) -> bool:
        for f in ('MHE', 'Crash Type', 'First HE'):
            if 'Guardrail' in row.get(f, ''):
                return True
        return False

    def _check_large_truck(self, row: Dict[str, str]) -> bool:
        truck_keywords = ('Medium/Heavy Truck', 'Truck/Tractor', 'Truck Tractor',
                          'Semi-Trailer', 'Bus', 'Working Vehicle', 'Farm Equipment')
        tu1 = row.get('TU-1 Type', '').strip()
        tu2 = row.get('TU-2 Type', '').strip()
        return any(k in tu1 or k in tu2 for k in truck_keywords)

    def _derive_road_departure_type(self, row: Dict[str, str]) -> str:
        mhe = row.get('MHE', '').strip()
        first_he = row.get('First HE', '').strip()
        indicators = ('Tree', 'Utility Pole', 'Guard Rail', 'Guardrail', 'Fence',
                      'Embankment', 'Ditch', 'Concrete Highway Barrier', 'Cable Rail',
                      'Culvert', 'Overturning', 'Rollover', 'Large Rocks', 'Sign',
                      'Mailbox', 'Crash Cushion', 'Wall or Building', 'Barricade',
                      'Bridge Structure')
        if any(ind in mhe or ind in first_he for ind in indicators):
            return 'RD_UNKNOWN'
        return 'NOT_RD'

    def _derive_intersection_analysis(self, row: Dict[str, str]) -> str:
        rd = row.get('Road Description', '').strip()
        if rd in ('At Intersection', 'Intersection Related', 'Roundabout',
                  'Alley Related', 'Mid-Block Crosswalk'):
            return 'Urban Intersection'
        return 'Not Intersection'

    def _calc_speed_diff(self, row: Dict[str, str]) -> str:
        limit = self._int(row.get('TU-1 Speed Limit', ''))
        est = self._int(row.get('TU-1 Estimated Speed', ''))
        if limit > 0 and est > 0:
            return str(est - limit)
        return ''


class MarylandNormalizer(BaseNormalizer):
    """Maryland ACRS data normalizer — converts to Virginia-compatible VDOT format.

    Handles both MoCo county portal fields (report_number, crash_date_time,
    collision_type) and statewide portal fields (report_no, acc_date,
    collision_type_desc). Auto-detects which field names are present.
    """

    # Collision Type → VDOT numbered format
    COLLISION_VDOT_MAP = {
        # MoCo portal values
        'Same Dir Rear End': '1. Rear End',
        'SAME DIR REAR END': '1. Rear End',
        'SAME DIRECTION REAR END': '1. Rear End',
        'Rear End': '1. Rear End',
        'Angle': '2. Angle',
        'ANGLE': '2. Angle',
        'Angle Meets Left Head On': '2. Angle',
        'Angle Meets Left Turn': '2. Angle',
        'Angle Meets Right Turn': '2. Angle',
        'Head On': '3. Head On',
        'HEAD ON': '3. Head On',
        'Head On Left Turn': '3. Head On',
        'HEAD ON LEFT TURN': '3. Head On',
        'Opposite Direction Both Left Turn': '3. Head On',
        'Same Direction Sideswipe': '4. Sideswipe - Same Direction',
        'SAME DIRECTION SIDESWIPE': '4. Sideswipe - Same Direction',
        'Same Direction Both Left Turn': '4. Sideswipe - Same Direction',
        'Same Direction Both Right Turn': '4. Sideswipe - Same Direction',
        'Same Direction Left Turn': '4. Sideswipe - Same Direction',
        'Same Direction Right Turn': '4. Sideswipe - Same Direction',
        'Opposite Direction Sideswipe': '5. Sideswipe - Opposite Direction',
        'OPPOSITE DIRECTION SIDESWIPE': '5. Sideswipe - Opposite Direction',
        'Single Vehicle': '14. Fixed Object',
        'SINGLE VEHICLE': '14. Fixed Object',
        'Other': '16. Other',
        'OTHER': '16. Other',
        'UNKNOWN': '16. Other',
    }

    # Weather → VDOT numbered format
    WEATHER_VDOT_MAP = {
        'Clear': '1. No Adverse Condition (Clear/Cloudy)',
        'CLEAR': '1. No Adverse Condition (Clear/Cloudy)',
        'Cloudy': '1. No Adverse Condition (Clear/Cloudy)',
        'CLOUDY': '1. No Adverse Condition (Clear/Cloudy)',
        'Raining': '5. Rain',
        'RAINING': '5. Rain',
        'Foggy': '3. Fog/Smog/Smoke',
        'FOGGY': '3. Fog/Smog/Smoke',
        'Snow': '4. Snow',
        'SNOW': '4. Snow',
        'Blowing Snow': '4. Snow',
        'BLOWING SNOW': '4. Snow',
        'Sleet': '6. Sleet/Hail/Freezing',
        'SLEET': '6. Sleet/Hail/Freezing',
        'Blowing Sand/Dirt/Snow': '7. Blowing Sand/Dust',
        'Blowing Sand, Soil, Dirt': '7. Blowing Sand/Dust',
        'Severe Crosswinds': '8. Severe Crosswinds',
        'SEVERE CROSSWINDS': '8. Severe Crosswinds',
    }

    # Light → VDOT numbered format
    LIGHT_VDOT_MAP = {
        'Daylight': '2. Daylight',
        'DAYLIGHT': '2. Daylight',
        'Dark Lights On': '4. Darkness - Road Lighted',
        'Dark-Lights On': '4. Darkness - Road Lighted',
        'DARK LIGHTS ON': '4. Darkness - Road Lighted',
        'DARK-LIGHTS ON': '4. Darkness - Road Lighted',
        'Dark No Lights': '5. Darkness - Road Not Lighted',
        'Dark-No Lights': '5. Darkness - Road Not Lighted',
        'DARK NO LIGHTS': '5. Darkness - Road Not Lighted',
        'DARK-NO LIGHTS': '5. Darkness - Road Not Lighted',
        'Dawn': '1. Dawn',
        'DAWN': '1. Dawn',
        'Dusk': '3. Dusk',
        'DUSK': '3. Dusk',
        'Dark - Unknown Lighting': '6. Dark - Unknown',
        'Dark -- Unknown Lighting': '6. Dark - Unknown',
        'DARK -- UNKNOWN LIGHTING': '6. Dark - Unknown',
    }

    # Surface Condition → VDOT numbered format
    SURFACE_VDOT_MAP = {
        'Dry': '1. Dry', 'DRY': '1. Dry',
        'Wet': '2. Wet', 'WET': '2. Wet',
        'Snow': '3. Snow', 'SNOW': '3. Snow',
        'Slush': '4. Slush', 'SLUSH': '4. Slush',
        'Ice': '5. Ice', 'ICE': '5. Ice',
        'Mud, Dirt, Gravel': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Sand': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Oil': '6. Sand/Mud/Dirt/Oil/Gravel',
        'Water (Standing, Moving)': '7. Water',
    }

    # Route Type → Road System classification
    ROAD_SYSTEM_MAP = {
        'Interstate': 'Interstate',
        'US Route': 'Primary',
        'US (State)': 'Primary',
        'Maryland (State)': 'Primary',
        'State Route': 'Primary',
        'County': 'Non-DOT secondary',
        'Municipality': 'Non-DOT secondary',
        'Other Public Roadway': 'Non-DOT secondary',
        'Government': 'Non-DOT secondary',
        'Service Road': 'Secondary',
        'Ramp': 'Secondary',
    }

    # Severity: 3-tier ACRS crash-level classification
    SEVERITY_MAP = {
        'Fatal Crash': 'K',
        'FATAL CRASH': 'K',
        'Injury Crash': 'B',
        'INJURY CRASH': 'B',
        'Property Damage Crash': 'O',
        'PROPERTY DAMAGE CRASH': 'O',
    }

    DARKNESS_VALUES = {
        'Dark Lights On', 'Dark-Lights On', 'Dark No Lights', 'Dark-No Lights',
        'Dark - Unknown Lighting', 'Dark -- Unknown Lighting',
        'DARK LIGHTS ON', 'DARK-LIGHTS ON', 'DARK NO LIGHTS', 'DARK-NO LIGHTS',
        'DARK -- UNKNOWN LIGHTING',
    }

    def _get(self, row: Dict[str, str], primary: str, alt: str = '') -> str:
        """Get field value trying primary name first, then alternate."""
        val = (row.get(primary) or '').strip()
        if not val and alt:
            val = (row.get(alt) or '').strip()
        return val

    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        n = {}

        # --- ID ---
        n['Document Nbr'] = self._get(row, 'report_number', 'report_no')

        # --- Date/Time ---
        # MoCo: crash_date_time (ISO 8601: 2023-01-15T14:30:00.000)
        # Statewide: acc_date (calendar_date type)
        raw_datetime = self._get(row, 'crash_date_time', 'acc_date')
        date_part = ''
        time_part = ''
        year_part = ''
        if 'T' in raw_datetime:
            # ISO format: 2023-01-15T14:30:00.000
            parts = raw_datetime.split('T')
            date_part = parts[0]
            time_str = parts[1].split('.')[0] if len(parts) > 1 else ''
            time_part = time_str.replace(':', '')[:4]
            year_part = date_part[:4] if len(date_part) >= 4 else ''
        elif raw_datetime:
            # Fallback: try various date formats
            date_part = raw_datetime.split(' ')[0]
            if '/' in date_part:
                dparts = date_part.split('/')
                if len(dparts) == 3:
                    year_part = dparts[2][:4]
            elif '-' in date_part:
                year_part = date_part[:4]

        n['Crash Date'] = date_part
        n['Crash Year'] = year_part or self._get(row, 'year', '')
        n['Crash Military Time'] = time_part or self._get(row, 'acc_time', '').replace(':', '')[:4]

        # --- Severity ---
        acrs_type = self._get(row, 'acrs_report_type', 'report_type')
        severity = self.SEVERITY_MAP.get(acrs_type, 'O')
        n['Crash Severity'] = severity
        n['K_People'] = '1' if severity == 'K' else '0'
        n['A_People'] = '0'
        n['B_People'] = '1' if severity == 'B' else '0'
        n['C_People'] = '0'

        # --- Collision Type ---
        raw_collision = self._get(row, 'collision_type', 'collision_type_desc')
        n['Collision Type'] = self.COLLISION_VDOT_MAP.get(raw_collision, raw_collision or '16. Other')

        # --- Weather ---
        raw_weather = self._get(row, 'weather', 'weather_desc')
        n['Weather Condition'] = self.WEATHER_VDOT_MAP.get(raw_weather, raw_weather)

        # --- Light ---
        raw_light = self._get(row, 'light', 'light_desc')
        n['Light Condition'] = self.LIGHT_VDOT_MAP.get(raw_light, raw_light)

        # --- Surface Condition ---
        raw_surface = self._get(row, 'surface_condition', 'surf_cond_desc')
        n['Roadway Surface Condition'] = self.SURFACE_VDOT_MAP.get(raw_surface, raw_surface)

        # --- Road Alignment ---
        n['Roadway Alignment'] = '1. Straight - Level'

        # --- Roadway Description ---
        n['Roadway Description'] = ''

        # --- Intersection Type ---
        junction = self._get(row, 'junction', 'junction_desc')
        jl = junction.lower()
        is_intersection = (junction and 'intersection' in jl
                           and 'non-intersection' not in jl
                           and 'non intersection' not in jl)
        if is_intersection:
            n['Intersection Type'] = '4. Four Approaches'
        else:
            n['Intersection Type'] = '1. Not at Intersection'

        # --- Relation to Roadway ---
        if is_intersection:
            n['Relation To Roadway'] = '9. Within Intersection'
        else:
            n['Relation To Roadway'] = '8. Non-Intersection'

        # --- Route & Location ---
        n['RTE Name'] = self._get(row, 'road_name', '')
        route_type = self._get(row, 'route_type', 'route_type_desc')
        n['SYSTEM'] = self.ROAD_SYSTEM_MAP.get(route_type, 'Non-DOT secondary')

        # Node: intersection name
        road = self._get(row, 'road_name', '')
        cross = self._get(row, 'cross_street_name', '')
        if road and cross:
            roads = sorted([road, cross])
            n['Node'] = f'{roads[0]} & {roads[1]}'
        else:
            n['Node'] = ''

        n['RNS MP'] = ''

        # --- Coordinates (x=longitude, y=latitude per Virginia convention) ---
        n['x'] = self._get(row, 'longitude', '')
        n['y'] = self._get(row, 'latitude', '')

        # --- Jurisdiction ---
        n['Physical Juris Name'] = (
            self._get(row, 'municipality', '') or
            self._get(row, 'county_desc', '') or
            'Montgomery County'
        )

        # --- Boolean flags ---
        # Most not available at crash level (need driver/NM dataset join)
        n['Pedestrian?'] = 'No'
        n['Bike?'] = 'No'
        n['Alcohol?'] = 'No'
        n['Speed?'] = 'No'
        hit_run = self._get(row, 'hit_run', '')
        n['Hitrun?'] = 'Yes' if hit_run.upper() in ('YES', 'TRUE', 'Y') else 'No'
        n['Motorcycle?'] = 'No'
        n['Night?'] = 'Yes' if raw_light in self.DARKNESS_VALUES else 'No'
        n['Distracted?'] = 'No'
        n['Drowsy?'] = 'No'
        n['Drug Related?'] = 'No'
        n['Young?'] = 'No'
        n['Senior?'] = 'No'
        n['Unrestrained?'] = 'No'
        n['School Zone'] = 'No'
        n['Work Zone Related'] = 'No'

        # --- Safety fields (not derivable from crash-level data) ---
        n['Animal Related?'] = 'No'
        n['Guardrail Related?'] = 'No'
        n['Lgtruck?'] = 'No'
        n['RoadDeparture Type'] = 'NOT_RD'
        n['Intersection Analysis'] = (
            'Urban Intersection' if is_intersection
            else 'Not Intersection'
        )
        n['Max Speed Diff'] = ''

        # --- Traffic Control ---
        n['Traffic Control Type'] = self._get(row, 'traffic_control', 'traf_control_desc')
        n['Traffic Control Status'] = ''

        # --- Infrastructure fields ---
        n['Functional Class'] = ''
        n['Area Type'] = ''
        n['Facility Type'] = ''
        n['Ownership'] = ''

        # --- First Harmful Event ---
        n['First Harmful Event'] = ''
        n['First Harmful Event Loc'] = ''

        # --- Vehicle Count ---
        n['Vehicle Count'] = ''

        # --- Injury counts ---
        n['Persons Injured'] = ''
        n['Pedestrians Killed'] = '0'
        n['Pedestrians Injured'] = '0'

        # --- Source tracking ---
        n['_source_state'] = 'maryland'
        n['_md_report_type'] = acrs_type
        n['_md_route_type'] = route_type
        n['_md_municipality'] = self._get(row, 'municipality', '')
        n['_md_cross_street'] = cross
        n['_md_speed_limit'] = self._get(row, 'speed_limit', '')
        n['_md_off_road'] = self._get(row, 'off_road_description', 'off_road_desc')
        n['_md_road_condition'] = self._get(row, 'road_condition', 'rd_cond_desc')

        return n


# =============================================================================
# Delaware Normalizer
# =============================================================================

class DelawareNormalizer(BaseNormalizer):
    """
    Delaware DelDOT crash data normalizer.

    Data source: Socrata SODA API (data.delaware.gov, dataset 827n-m6xc)
    Crash-level dataset — no person-level injury detail available.

    Handles TWO field name formats:
    - Socrata JSON API: lowercase_with_underscores (crash_datetime)
    - CSV/Excel export: UPPERCASE WITH SPACES (CRASH DATETIME)

    Key limitations:
    - Severity: 3 levels only (Fatal/Injury/PDO) → mapped to K/A/O per FHWA guidance
    - No route/road name field → RTE Name empty (deferred to geocoding pipeline)
    - No crash ID → composite key from datetime + coordinates
    - No node/intersection ID → empty (deferred)
    """

    # Map from canonical field names to UPPERCASE CSV/Excel names AND abbreviated
    # Socrata JSON API names. The JSON API underwent a schema change where field
    # names were shortened (e.g. crash_classification_description → crash_class_desc).
    # We support all three formats: canonical, UPPERCASE, and abbreviated.
    _FIELD_ALIASES = {
        'crash_datetime': 'CRASH DATETIME',
        'crash_classification_description': 'CRASH CLASSIFICATION DESCRIPTION',
        'crash_classification_code': 'CRASH CLASSIFICATION CODE',
        'manner_of_impact_description': 'MANNER OF IMPACT DESCRIPTION',
        'weather_1_description': 'WEATHER 1 DESCRIPTION',
        'lighting_condition_description': 'LIGHTING CONDITION DESCRIPTION',
        'road_surface_description': 'ROAD SURFACE DESCRIPTION',
        'latitude': 'LATITUDE',
        'longitude': 'LONGITUDE',
        'county_name': 'COUNTY NAME',
        'pedestrian_involved': 'PEDESTRIAN INVOLVED',
        'bicycled_involved': 'BICYCLED INVOLVED',
        'alcohol_involved': 'ALCOHOL INVOLVED',
        'drug_involved': 'DRUG INVOLVED',
        'motorcycle_involved': 'MOTORCYCLE INVOLVED',
        'seatbelt_used': 'SEATBELT USED',
        'work_zone': 'WORK ZONE',
        'primary_contributing_circumstance_code': 'PRIMARY CONTRIBUTING CIRCUMSTANCE CODE',
        'day_of_week_description': 'DAY OF WEEK DESCRIPTION',
        'year': 'YEAR',
        'school_bus_involved_code': 'SCHOOL BUS INVOLVED CODE',
    }

    # Abbreviated Socrata JSON API field names (post-schema-change) mapped to
    # the canonical names used by _get(). This allows the normalizer to work
    # with data from the JSON API even after the field rename.
    _ABBREVIATED_ALIASES = {
        'crash_class_desc': 'crash_classification_description',
        'crash_class': 'crash_classification_code',
        'impact_desc': 'manner_of_impact_description',
        'impact': 'manner_of_impact_code',
        'weather_1_desc': 'weather_1_description',
        'light_cond_desc': 'lighting_condition_description',
        'light_cond': 'lighting_condition_code',
        'road_surface_desc': 'road_surface_description',
        'road_surface': 'road_surface_code',
        'county_desc': 'county_name',
        'county': 'county_code',
        'ped_involved': 'pedestrian_involved',
        'bike_involved': 'bicycled_involved',
        'mc_involved': 'motorcycle_involved',
        'mc_helmet_used': 'motorcycle_helmet_used',
        'bike_helmet_used': 'bicycle_helmet_used',
        'pri_contrib_circum': 'primary_contributing_circumstance_code',
        'pri_contrib_circum_desc': 'primary_contributing_circumstance_description',
        'priv_prop_coll': 'collision_on_private_property',
        'day_of_week_desc': 'day_of_week_description',
        'day_of_week': 'day_of_week_code',
    }

    # Crash classification → KABCO severity (case-insensitive via .strip().lower())
    # Delaware only reports 3 crash-level categories. For Fatal and PDO, mapping is
    # direct. For "Personal Injury", we use proportional A/B/C split based on NHTSA
    # national averages (FHWA HSIP Manual Sec 4.2, NHTSA Traffic Safety Facts):
    #   A (Suspected Serious):  ~8% of injury crashes
    #   B (Suspected Minor):    ~32% of injury crashes
    #   C (Possible Injury):    ~60% of injury crashes
    # This prevents EPDO inflation from mapping all injury → A.
    SEVERITY_MAP = {
        'fatal crash': 'K',
        'fatality crash': 'K',       # actual value in Delaware data
        'personal injury crash': 'INJURY',  # split into A/B/C proportionally
        'property damage crash': 'O',
        'property damage only': 'O',  # alternate wording
        'non-reportable': 'O',        # below-threshold crashes
    }

    # NHTSA national average proportional split for "Injury" crashes
    # Thresholds for deterministic hash-based assignment (reproducible per crash)
    INJURY_SPLIT_A_THRESHOLD = 0.08   # 0.00 - 0.08 → A (8%)
    INJURY_SPLIT_B_THRESHOLD = 0.40   # 0.08 - 0.40 → B (32%)
    # remainder 0.40 - 1.00 → C (60%)

    # Lighting values that indicate darkness (for Night? flag)
    # Compared case-insensitively
    DARKNESS_KEYWORDS = {'dark'}  # any lighting value containing 'dark'

    # Delaware contributing circumstance codes that indicate speed-related
    SPEED_CODES = {
        '50', '51', '52', '53',  # speed too fast, exceeded limit, etc.
    }

    # Delaware contributing circumstance codes that indicate distraction
    DISTRACTED_CODES = {
        '60', '61', '62', '63', '64', '65', '66',  # distraction-related codes
    }

    def _get(self, row: Dict[str, str], primary: str, alt: str = '') -> str:
        """Get field value trying multiple name formats.

        Lookup order for each field name:
        1. Canonical lowercase (e.g. 'crash_classification_description')
        2. UPPERCASE CSV alias (e.g. 'CRASH CLASSIFICATION DESCRIPTION')
        3. Abbreviated API alias (e.g. 'crash_class_desc')
        4. Alt field name (same lookup order)
        """
        val = self._try_field(row, primary)
        if not val and alt:
            val = self._try_field(row, alt)
        # Treat sentinel values as empty
        if val in ('NA', 'None', 'nan', 'N/A', 'null'):
            return ''
        return val

    def _try_field(self, row: Dict[str, str], field_name: str) -> str:
        """Try to get a field value using all known name formats."""
        # 1. Try the canonical name directly
        val = (row.get(field_name) or '').strip()
        if val:
            return val
        # 2. Try UPPERCASE CSV alias
        upper_alias = self._FIELD_ALIASES.get(field_name, '')
        if upper_alias:
            raw = row.get(upper_alias)
            val = str(raw).strip() if raw is not None else ''
            if val:
                return val
        # 3. Try abbreviated API names (reverse lookup: find any abbreviated
        #    key that maps to this canonical name)
        for abbrev, canonical in self._ABBREVIATED_ALIASES.items():
            if canonical == field_name:
                raw = row.get(abbrev)
                val = str(raw).strip() if raw is not None else ''
                if val:
                    return val
        return ''

    def _is_truthy(self, value: str) -> bool:
        """Check if a Delaware boolean field value is truthy (Y/N or Yes/No)."""
        return value.upper() in ('YES', 'TRUE', 'Y', '1') if value else False

    def _parse_datetime(self, raw_datetime: str) -> tuple:
        """Parse Delaware datetime into (date_part, time_part, year_part).

        Handles multiple formats:
        - ISO: 2023-01-15T14:30:00.000 (Socrata JSON API)
        - Named month: 2012 Apr 29 05:32:00 PM (Excel/CSV export)
        - Fallback: space-separated or slash-separated dates
        """
        from datetime import datetime as dt

        date_part = ''
        time_part = ''
        year_part = ''

        if not raw_datetime:
            return date_part, time_part, year_part

        # Format 1: ISO (from Socrata JSON API)
        if 'T' in raw_datetime:
            parts = raw_datetime.split('T')
            date_part = parts[0]
            time_str = parts[1].split('.')[0] if len(parts) > 1 else ''
            time_part = time_str.replace(':', '')[:4]
            year_part = date_part[:4] if len(date_part) >= 4 else ''
            return date_part, time_part, year_part

        # Format 2: Named month with AM/PM (from Excel/CSV export)
        # "2012 Apr 29 05:32:00 PM"
        try:
            parsed = dt.strptime(raw_datetime, '%Y %b %d %I:%M:%S %p')
            date_part = parsed.strftime('%Y-%m-%d')
            time_part = parsed.strftime('%H%M')
            year_part = str(parsed.year)
            return date_part, time_part, year_part
        except (ValueError, TypeError):
            pass

        # Format 3: Other named month variants
        for fmt in ('%Y %B %d %I:%M:%S %p', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p'):
            try:
                parsed = dt.strptime(raw_datetime, fmt)
                date_part = parsed.strftime('%Y-%m-%d')
                time_part = parsed.strftime('%H%M')
                year_part = str(parsed.year)
                return date_part, time_part, year_part
            except (ValueError, TypeError):
                continue

        # Fallback: try to extract what we can
        tokens = raw_datetime.split()
        if tokens:
            first = tokens[0]
            if '-' in first:
                date_part = first
                year_part = first[:4] if len(first) >= 4 else ''
            elif '/' in first:
                date_part = first
                dparts = first.split('/')
                if len(dparts) == 3:
                    year_part = dparts[2][:4]
            elif first.isdigit() and len(first) == 4:
                year_part = first
                date_part = raw_datetime.split(' ')[0:3]
                date_part = ' '.join(date_part) if len(date_part) >= 3 else first

        return date_part, time_part, year_part

    def _build_composite_id(self, row: Dict[str, str], date_str: str, time_str: str) -> str:
        """Generate composite crash ID from datetime + coordinates."""
        lat = self._get(row, 'latitude')
        lon = self._get(row, 'longitude')
        lat_part = lat.replace('.', '').replace('-', '')[:6] if lat else '000000'
        lon_part = lon.replace('.', '').replace('-', '')[:6] if lon else '000000'
        date_compact = date_str.replace('-', '').replace('/', '').replace(' ', '')[:8] if date_str else '00000000'
        time_compact = time_str[:4] if time_str else '0000'
        return f"DE-{date_compact}-{time_compact}-{lat_part}-{lon_part}"

    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        n = {}

        # --- Date/Time ---
        raw_datetime = self._get(row, 'crash_datetime')
        date_part, time_part, year_part = self._parse_datetime(raw_datetime)

        n['Crash Date'] = date_part
        n['Crash Year'] = year_part or self._get(row, 'year', '')
        n['Crash Military Time'] = time_part

        # --- ID (composite key) ---
        n['Document Nbr'] = self._build_composite_id(row, date_part, time_part)

        # --- Severity (case-insensitive lookup with proportional injury split) ---
        raw_severity = self._get(row, 'crash_classification_description')
        severity = self.SEVERITY_MAP.get(raw_severity.lower(), 'O')

        if severity == 'INJURY':
            # Proportional A/B/C split using deterministic hash for reproducibility.
            # Same crash (same composite ID) always gets the same severity.
            import hashlib
            crash_key = n.get('Document Nbr', '') or f"{raw_datetime}-{self._get(row, 'latitude')}-{self._get(row, 'longitude')}"
            hash_val = int(hashlib.md5(crash_key.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
            if hash_val < self.INJURY_SPLIT_A_THRESHOLD:
                severity = 'A'
            elif hash_val < self.INJURY_SPLIT_B_THRESHOLD:
                severity = 'B'
            else:
                severity = 'C'

        n['Crash Severity'] = severity
        n['K_People'] = '1' if severity == 'K' else '0'
        n['A_People'] = '1' if severity == 'A' else '0'
        n['B_People'] = '1' if severity == 'B' else '0'
        n['C_People'] = '1' if severity == 'C' else '0'

        # --- Collision Type ---
        n['Collision Type'] = self._get(row, 'manner_of_impact_description')

        # --- Weather ---
        n['Weather Condition'] = self._get(row, 'weather_1_description')

        # --- Light ---
        raw_light = self._get(row, 'lighting_condition_description')
        n['Light Condition'] = raw_light

        # --- Surface Condition ---
        n['Roadway Surface Condition'] = self._get(row, 'road_surface_description')

        # --- Road Alignment (not available) ---
        n['Roadway Alignment'] = ''

        # --- Roadway Description (not available) ---
        n['Roadway Description'] = ''

        # --- Intersection Type (not available) ---
        n['Intersection Type'] = ''

        # --- Relation to Roadway ---
        n['Relation To Roadway'] = ''

        # --- Route & Location (NOT available in Delaware public data) ---
        n['RTE Name'] = ''
        n['SYSTEM'] = ''
        n['Node'] = ''
        n['RNS MP'] = ''

        # --- Coordinates (x=longitude, y=latitude per Virginia convention) ---
        n['x'] = self._get(row, 'longitude')
        n['y'] = self._get(row, 'latitude')

        # --- Jurisdiction ---
        n['Physical Juris Name'] = self._get(row, 'county_name')

        # --- Boolean flags (available from Delaware dataset) ---
        n['Pedestrian?'] = 'Yes' if self._is_truthy(self._get(row, 'pedestrian_involved')) else 'No'
        n['Bike?'] = 'Yes' if self._is_truthy(self._get(row, 'bicycled_involved')) else 'No'
        n['Alcohol?'] = 'Yes' if self._is_truthy(self._get(row, 'alcohol_involved')) else 'No'
        n['Drug Related?'] = 'Yes' if self._is_truthy(self._get(row, 'drug_involved')) else 'No'
        n['Motorcycle?'] = 'Yes' if self._is_truthy(self._get(row, 'motorcycle_involved')) else 'No'

        # Unrestrained = inverse of seatbelt_used
        seatbelt = self._get(row, 'seatbelt_used')
        if seatbelt:
            n['Unrestrained?'] = 'No' if self._is_truthy(seatbelt) else 'Yes'
        else:
            n['Unrestrained?'] = 'No'

        # Work Zone
        n['Work Zone Related'] = 'Yes' if self._is_truthy(self._get(row, 'work_zone')) else 'No'

        # Night (derived from lighting condition — keyword-based for robustness)
        n['Night?'] = 'Yes' if 'dark' in raw_light.lower() else 'No'

        # Speed & Distracted (derived from contributing circumstance code)
        contrib_code = self._get(row, 'primary_contributing_circumstance_code')
        n['Speed?'] = 'Yes' if contrib_code in self.SPEED_CODES else 'No'
        n['Distracted?'] = 'Yes' if contrib_code in self.DISTRACTED_CODES else 'No'

        # Not available in Delaware public data
        n['Hitrun?'] = 'No'
        n['Drowsy?'] = 'No'
        n['Young?'] = 'No'
        n['Senior?'] = 'No'
        n['School Zone'] = 'No'

        # --- Safety fields ---
        n['Animal Related?'] = 'No'
        n['Guardrail Related?'] = 'No'
        n['Lgtruck?'] = 'No'
        n['RoadDeparture Type'] = ''
        n['Intersection Analysis'] = ''
        n['Max Speed Diff'] = ''

        # --- Traffic Control (not available) ---
        n['Traffic Control Type'] = ''
        n['Traffic Control Status'] = ''

        # --- Infrastructure fields (not available) ---
        n['Functional Class'] = ''
        n['Area Type'] = ''
        n['Facility Type'] = ''
        n['Ownership'] = ''

        # --- First Harmful Event (not available) ---
        n['First Harmful Event'] = ''
        n['First Harmful Event Loc'] = ''

        # --- Vehicle Count (not available) ---
        n['Vehicle Count'] = ''

        # --- Injury counts ---
        n['Persons Injured'] = ''
        n['Pedestrians Killed'] = '0'
        n['Pedestrians Injured'] = '0'

        # --- Source tracking ---
        n['_source_state'] = 'delaware'
        n['_de_classification_code'] = self._get(row, 'crash_classification_code')
        n['_de_contrib_code'] = contrib_code
        n['_de_day_of_week'] = self._get(row, 'day_of_week_description')

        return n


# --- Normalizer Registry ---
_NORMALIZERS = {
    'colorado': ColoradoNormalizer,
    'virginia': VirginiaNormalizer,
    'maryland': MarylandNormalizer,
    'maryland_statewide': MarylandNormalizer,
    'delaware': DelawareNormalizer,
    'delaware_csv': DelawareNormalizer,
}


def get_normalizer(state_key: str, config_path: Optional[str] = None) -> BaseNormalizer:
    """
    Get a normalizer instance for the given state.

    Args:
        state_key: State identifier (e.g., 'colorado', 'virginia')
        config_path: Optional path to state config JSON

    Returns:
        BaseNormalizer subclass instance
    """
    if not config_path:
        states_dir = Path(__file__).parent.parent / 'states'
        sig = STATE_SIGNATURES.get(state_key, {})
        config_dir = sig.get('config_dir', state_key)
        config_path = str(states_dir / config_dir / 'config.json')

    cls = _NORMALIZERS.get(state_key)
    if not cls:
        raise ValueError(
            f"No normalizer for state '{state_key}'. "
            f"Available: {', '.join(_NORMALIZERS.keys())}"
        )
    return cls(state_key, config_path)


def get_supported_states() -> Dict[str, str]:
    """Return dict of state_key -> display_name for all supported states."""
    return {k: v['display_name'] for k, v in STATE_SIGNATURES.items()}


def convert_file(
    input_path: str,
    output_path: str,
    state: Optional[str] = None,
    source_filename: Optional[str] = None,
) -> Tuple[str, int, int]:
    """
    Convert a raw crash data CSV to standardized format.

    Args:
        input_path: Path to raw CSV
        output_path: Path for standardized output CSV
        state: State key (auto-detected if None)
        source_filename: Original filename for tracking

    Returns:
        Tuple of (detected_state, total_rows, rows_with_gps)
    """
    detector = StateDetector()

    # Auto-detect state if not specified
    if not state:
        state = detector.detect_from_file(input_path)
        if state == 'unknown':
            raise ValueError(
                f"Could not auto-detect state from {input_path}. "
                f"Please specify --state explicitly."
            )

    normalizer = get_normalizer(state)
    src_name = source_filename or os.path.basename(input_path)

    total = 0
    with_gps = 0

    with open(input_path, 'r', encoding='utf-8-sig') as fin:
        reader = csv.DictReader(fin)

        # Build output columns from first normalized row
        first_row = next(reader)
        normalized_first = normalizer.normalize_row(first_row)
        normalized_first['_source_file'] = src_name

        # Use all keys from normalized row as output columns
        output_cols = list(normalized_first.keys())

        with open(output_path, 'w', newline='', encoding='utf-8') as fout:
            writer = csv.DictWriter(fout, fieldnames=output_cols, extrasaction='ignore')
            writer.writeheader()

            # Write first row
            writer.writerow(normalized_first)
            total = 1
            if normalized_first.get('x') and normalized_first.get('y'):
                try:
                    float(normalized_first['x'])
                    float(normalized_first['y'])
                    with_gps += 1
                except ValueError:
                    pass

            # Process remaining rows
            for row in reader:
                normalized = normalizer.normalize_row(row)
                normalized['_source_file'] = src_name
                writer.writerow(normalized)
                total += 1

                if normalized.get('x') and normalized.get('y'):
                    try:
                        float(normalized['x'])
                        float(normalized['y'])
                        with_gps += 1
                    except ValueError:
                        pass

    return state, total, with_gps


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for state_adapter.py."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Convert raw state crash data CSV to Virginia-standard format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Supported states: {', '.join(get_supported_states().keys())}"
    )
    parser.add_argument('--input', '-i', required=True, help='Path to raw CSV')
    parser.add_argument('--output', '-o', required=True, help='Path for normalized output CSV')
    parser.add_argument('--state', '-s', default=None,
                        help='State key (auto-detected if omitted)')
    args = parser.parse_args()

    state, total, with_gps = convert_file(args.input, args.output, state=args.state)
    print(f"Normalized {total:,} rows ({with_gps:,} with GPS) from {state}")


if __name__ == '__main__':
    main()
