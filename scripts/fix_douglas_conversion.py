#!/usr/bin/env python3
"""
Fix Douglas County Conversion - Column-by-Column Quality Corrections

Reads the original Douglas_County.csv and the current converted files,
applies value mappings to match the Henrico VDOT reference format, and
appends missing Colorado-specific columns at the end.

Fixes applied:
  1. Collision Type - add numbered prefixes to match Henrico vocabulary
  2. Weather Condition - map to Henrico's numbered categories
  3. Light Condition - map to Henrico's numbered categories
  4. Roadway Surface Condition - map to Henrico's numbered categories
  5. Roadway Alignment - map to Henrico's 4-category system
  6. Roadway Description - fix semantic mismatch (was intersection type, should be road geometry)
  7. Intersection Type - map to Henrico's approach-count system
  8. First Harmful Event - map to Henrico's numbered codes
  9. First Harmful Event Loc - map to Henrico's numbered codes
  10. SYSTEM - keep as-is (tool already handles both prefixed/unprefixed)
  11. Pedestrians Killed/Injured - derive from original NM data
  12. Vehicle Count - map from original Total Vehicles
  13. Ownership - derive from System Code
  14. Append ~30 Colorado-specific columns from original data
"""

import csv
import os
import sys

# ============================================================
# VALUE MAPPING TABLES (Colorado → Henrico VDOT format)
# ============================================================

COLLISION_TYPE_MAP = {
    'Rear End': '1. Rear End',
    'Angle': '2. Angle',
    'Head On': '3. Head On',
    'Sideswipe - Same Direction': '4. Sideswipe - Same Direction',
    'Sideswipe - Opposite Direction': '5. Sideswipe - Opposite Direction',
    'Fixed Object in Road': '11. Fixed Object in Road',
    'Fixed Object - Off Road': '9. Fixed Object - Off Road',
    'Non-Collision': '8. Non-Collision',
    'Pedestrian': '12. Ped',
    'Bicyclist': '13. Bicycle',
    'Other Animal': '10. Deer/Animal',
    'Other': '16. Other',
    'Unknown': '16. Other',
}

WEATHER_MAP = {
    'Clear': '1. No Adverse Condition (Clear/Cloudy)',
    'Cloudy': '1. No Adverse Condition (Clear/Cloudy)',
    'Rain': '5. Rain',
    'Snow': '4. Snow',
    'Blowing Snow': '4. Snow',
    'Sleet, Hail (Freezing Rain or Drizzle)': '6. Sleet/Hail/Freezing',
    'Fog': '3. Fog/Smog/Smoke',
    'Blowing Sand, Soil, Dirt': '7. Blowing Sand/Dust',
    'Dust': '7. Blowing Sand/Dust',
    'Severe Crosswinds': '8. Severe Crosswinds',
    'Freezing Rain or Freezing Drizzle': '6. Sleet/Hail/Freezing',
    'Sleet or Hail': '6. Sleet/Hail/Freezing',
    'Wind': '8. Severe Crosswinds',
    '': '',
}

LIGHT_MAP = {
    'Daylight': '2. Daylight',
    'Dark \u2013 Lighted': '4. Darkness - Road Lighted',
    'Dark – Lighted': '4. Darkness - Road Lighted',
    'Dark - Lighted': '4. Darkness - Road Lighted',
    'Dark \u2013 Unlighted': '5. Darkness - Road Not Lighted',
    'Dark – Unlighted': '5. Darkness - Road Not Lighted',
    'Dark - Unlighted': '5. Darkness - Road Not Lighted',
    'Dawn or Dusk': '1. Dawn',  # default to Dawn; will refine by time below
    '': '',
}

SURFACE_MAP = {
    'Dry': '1. Dry',
    'Wet': '2. Wet',
    'Snow': '3. Snow',
    'Snowy': '3. Snow',
    'Ice': '5. Ice',
    'Icy': '5. Ice',
    'Slush': '4. Slush',
    'Slushy': '4. Slush',
    'Sand, Mud, Dirt, Oil, Gravel': '6. Sand/Mud/Dirt/Oil/Gravel',
    'Water (Standing, Moving)': '7. Water',
    'Snowy W/Visible Icy Road Treatment': '3. Snow',
    'Icy W/Visible Icy Road Treatment': '5. Ice',
    'Slushy W/Visible Icy Road Treatment': '4. Slush',
    'Dry W/Visible Icy Road Treatment': '1. Dry',
    'Wet W/Visible Icy Road Treatment': '2. Wet',
    'Sand/Gravel': '6. Sand/Mud/Dirt/Oil/Gravel',
    'Muddy': '6. Sand/Mud/Dirt/Oil/Gravel',
    'Roto-Milled': '16. Other',
    'Other': '16. Other',
    '': '',
}

# Henrico alignment: 1=Straight-Level, 2=Curve-Level, 3=Grade-Straight, 4=Grade-Curve
def map_alignment(val):
    """Map compound Colorado alignment to Henrico's 4-category system."""
    if not val:
        return ''
    v = val.lower()
    has_curve = 'curve' in v
    has_grade = any(w in v for w in ['uphill', 'downhill', 'hill crest', 'grade'])
    if has_curve and has_grade:
        return '4. Grade - Curve'
    if has_curve:
        return '2. Curve - Level'
    if has_grade:
        return '3. Grade - Straight'
    return '1. Straight - Level'


# Roadway Description: Henrico describes road cross-section geometry
# Since Colorado doesn't have this data, we derive from System Code
ROAD_DESC_FROM_SYSTEM = {
    'Interstate Highway': '3. Two-Way, Divided, Positive Median Barrier',
    'State Highway': '2. Two-Way, Divided, Unprotected Median',
    'County Road': '1. Two-Way, Not Divided',
    'City Street': '1. Two-Way, Not Divided',
    'Frontage Road': '1. Two-Way, Not Divided',
}

# Intersection Type: Henrico uses approach counts
INTERSECTION_TYPE_MAP = {
    'Non-Intersection': '1. Not at Intersection',
    'Intersection': '4. Four Approaches',  # default for generic intersections
    'Ramp': '1. Not at Intersection',
    'Ramp-related': '1. Not at Intersection',
    'Driveway': '2. Two Approaches',
    'Driveway Access Related': '2. Two Approaches',
    'Roundabout': '5. Roundabout',
    'Crossover-Related': '1. Not at Intersection',
    'Crossover-Related ': '1. Not at Intersection',
    'Railroad Crossing': '2. Two Approaches',
    'Mid-Block Crosswalk': '1. Not at Intersection',
    'Unknown': '1. Not at Intersection',
    '': '',
}

# First Harmful Event: Colorado raw → Henrico numbered codes
FIRST_HE_MAP = {
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
    # Typo in source data
    'Electical/Utility Box': '3. Utility Pole',
}

FIRST_HE_LOC_MAP = {
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
    '': '',
}

# Relation To Roadway - derived from Colorado's Road Description field
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

OWNERSHIP_FROM_SYSTEM = {
    'Interstate Highway': '1. State Hwy Agency',
    'State Highway': '1. State Hwy Agency',
    'County Road': '2. County Hwy Agency',
    'City Street': '3. City or Town Hwy Agency',
    'Frontage Road': '1. State Hwy Agency',
}

# ============================================================
# COLORADO-SPECIFIC COLUMNS TO APPEND
# ============================================================

# These are columns from the original Douglas_County.csv that provide
# rich analytical detail beyond what the boolean flags capture
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


def load_original_data(filepath):
    """Load original Douglas_County.csv into a dict keyed by CUID."""
    print(f"  Loading original data from {filepath}...")
    lookup = {}
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cuid = row.get('CUID', '').strip()
            if cuid:
                lookup[cuid] = row
    print(f"  Loaded {len(lookup)} original records")
    return lookup


def refine_dawn_dusk(light_val, military_time):
    """If 'Dawn or Dusk', refine based on time: before 1200 = Dawn, after = Dusk."""
    if light_val == '1. Dawn' and military_time:
        try:
            t = int(military_time)
            if t >= 1200:
                return '3. Dusk'
        except ValueError:
            pass
    return light_val


def derive_ped_killed_injured(orig_row):
    """Derive Pedestrians Killed/Injured from original NM and injury data."""
    ped_killed = 0
    ped_injured = 0

    # Check if any NM type is Pedestrian
    for tu in ['TU-1', 'TU-2']:
        nm_type = orig_row.get(f'{tu} NM Type', '').strip()
        if 'Pedestrian' in nm_type:
            # Check severity from injury counts
            inj_k = safe_int(orig_row.get('Injury 04', '0'))
            inj_a = safe_int(orig_row.get('Injury 03', '0'))
            inj_b = safe_int(orig_row.get('Injury 02', '0'))
            inj_c = safe_int(orig_row.get('Injury 01', '0'))

            # If fatal crash with ped, assume ped killed
            if inj_k > 0:
                ped_killed = max(ped_killed, 1)
            if inj_a > 0 or inj_b > 0 or inj_c > 0:
                ped_injured = max(ped_injured, 1)

    return str(ped_killed), str(ped_injured)


def safe_int(val):
    try:
        return int(val.strip()) if val.strip() else 0
    except (ValueError, TypeError):
        return 0


def fix_row(row, orig_row):
    """Apply all column corrections to a single converted row."""
    # --- 1. Collision Type ---
    ct = row.get('Collision Type', '').strip()
    row['Collision Type'] = COLLISION_TYPE_MAP.get(ct, ct)

    # --- 2. Weather Condition ---
    wc = row.get('Weather Condition', '').strip()
    row['Weather Condition'] = WEATHER_MAP.get(wc, wc)

    # --- 3. Light Condition ---
    lc = row.get('Light Condition', '').strip()
    mapped_light = LIGHT_MAP.get(lc, lc)
    # Refine Dawn/Dusk by time
    mil_time = row.get('Crash Military Time', '')
    row['Light Condition'] = refine_dawn_dusk(mapped_light, mil_time)

    # --- 4. Roadway Surface Condition ---
    sc = row.get('Roadway Surface Condition', '').strip()
    row['Roadway Surface Condition'] = SURFACE_MAP.get(sc, sc)

    # --- 5. Roadway Alignment ---
    ra = row.get('Roadway Alignment', '').strip()
    row['Roadway Alignment'] = map_alignment(ra)

    # --- 6. Roadway Description (fix semantic mismatch) ---
    # The original CO "Road Description" describes intersection relation (Non-Intersection,
    # At Intersection, etc.) — NOT road geometry. Use original data for the mapping.
    co_road_desc = ''
    co_system = ''
    if orig_row:
        co_road_desc = orig_row.get('Road Description', '').strip()
        co_system = orig_row.get('System Code', '').strip()
    else:
        co_system = row.get('_co_system_code', '').strip()
    row['Roadway Description'] = ROAD_DESC_FROM_SYSTEM.get(co_system, '1. Two-Way, Not Divided')

    # --- 7. Intersection Type ---
    # Use original Road Description to derive intersection type (since the converted
    # Intersection Type may already be mapped on re-runs)
    if co_road_desc:
        it = co_road_desc
    else:
        it = row.get('Intersection Type', '').strip()
    # Map from CO Road Description values to Henrico approach-count format
    # First check our map, then fall back to INTERSECTION_MAP from state_adapter
    from_road_desc = {
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
    row['Intersection Type'] = from_road_desc.get(it, INTERSECTION_TYPE_MAP.get(it, it))

    # --- 8. First Harmful Event ---
    fhe = row.get('First Harmful Event', '').strip()
    row['First Harmful Event'] = FIRST_HE_MAP.get(fhe, fhe)

    # --- 9. First Harmful Event Loc ---
    fhel = row.get('First Harmful Event Loc', '').strip()
    row['First Harmful Event Loc'] = FIRST_HE_LOC_MAP.get(fhel, fhel)

    # --- 10. Relation To Roadway (new column from the old Roadway Description values) ---
    row['Relation To Roadway'] = RELATION_TO_ROADWAY_MAP.get(co_road_desc, co_road_desc)

    # --- 11. Pedestrians Killed/Injured ---
    if orig_row:
        pk, pi = derive_ped_killed_injured(orig_row)
        row['Pedestrians Killed'] = pk
        row['Pedestrians Injured'] = pi
    else:
        if not row.get('Pedestrians Killed'):
            row['Pedestrians Killed'] = '0'
        if not row.get('Pedestrians Injured'):
            row['Pedestrians Injured'] = '0'

    # --- 12. Vehicle Count ---
    if orig_row:
        row['Vehicle Count'] = orig_row.get('Total Vehicles', '').strip()
    else:
        row['Vehicle Count'] = ''

    # --- 13. Ownership ---
    row['Ownership'] = OWNERSHIP_FROM_SYSTEM.get(co_system, '')

    # --- 14. Append Colorado-specific columns ---
    if orig_row:
        for col_name, orig_col in CO_EXTRA_COLUMNS:
            row[col_name] = orig_row.get(orig_col, '').strip()

    return row


def build_output_columns(sample_row):
    """Build the complete column order for output files."""
    # Start with the standard VDOT columns (in Henrico order), adding new ones
    standard = [
        'Document Nbr', 'Crash Date', 'Crash Year', 'Crash Military Time',
        'Crash Severity', 'K_People', 'A_People', 'B_People', 'C_People',
        'Collision Type', 'Weather Condition', 'Light Condition',
        'Roadway Surface Condition', 'Roadway Alignment',
        'Roadway Description', 'Intersection Type',
        'RTE Name', 'SYSTEM', 'Node', 'RNS MP', 'x', 'y',
        'Physical Juris Name',
        'Pedestrian?', 'Bike?', 'Alcohol?', 'Speed?', 'Hitrun?',
        'Motorcycle?', 'Night?', 'Distracted?', 'Drowsy?', 'Drug Related?',
        'Young?', 'Senior?', 'Unrestrained?', 'School Zone', 'Work Zone Related',
        'Traffic Control Type', 'Traffic Control Status',
        'Functional Class', 'Area Type', 'Facility Type', 'Ownership',
        'First Harmful Event', 'First Harmful Event Loc',
        'Relation To Roadway',  # NEW - from the old Roadway Description values
        'Vehicle Count',         # NEW - from original Total Vehicles
        'Persons Injured', 'Pedestrians Killed', 'Pedestrians Injured',
        # Source tracking
        '_source_state',
        '_co_system_code', '_co_agency_id', '_co_rd_number',
        '_co_location1', '_co_location2', '_co_city', '_source_file',
    ]
    # Append Colorado-specific detail columns
    for col_name, _ in CO_EXTRA_COLUMNS:
        standard.append(col_name)

    return standard


def process_file(input_path, output_path, original_lookup):
    """Process a single converted CSV file and write corrected output."""
    print(f"\nProcessing: {input_path}")
    rows = []
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))

    print(f"  Read {len(rows)} rows")

    matched = 0
    unmatched = 0
    for row in rows:
        doc_nbr = row.get('Document Nbr', '').strip()
        orig_row = original_lookup.get(doc_nbr)
        if orig_row:
            matched += 1
        else:
            unmatched += 1
        fix_row(row, orig_row)

    print(f"  Matched to original: {matched}, unmatched: {unmatched}")

    # Build output columns from the first corrected row
    output_cols = build_output_columns(rows[0] if rows else {})

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_cols, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"  Written {len(rows)} rows to {output_path}")


def verify_output(filepath, henrico_path):
    """Quick verification: compare column value samples with Henrico."""
    print(f"\n=== VERIFICATION: {os.path.basename(filepath)} ===")
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    check_cols = [
        'Collision Type', 'Weather Condition', 'Light Condition',
        'Roadway Surface Condition', 'Roadway Alignment',
        'Roadway Description', 'Intersection Type',
        'First Harmful Event', 'First Harmful Event Loc',
        'Relation To Roadway', 'Vehicle Count', 'Ownership',
        'Pedestrians Killed', 'Pedestrians Injured',
    ]

    for col in check_cols:
        vals = set()
        empty = 0
        for r in rows:
            v = r.get(col, '')
            if v:
                vals.add(v)
            else:
                empty += 1
        has_prefix = sum(1 for v in vals if v and v[0].isdigit() and '. ' in v[:5])
        print(f"  {col}: {len(vals)} unique, {empty} empty, {has_prefix} prefixed")
        # Show first 5 values
        for v in sorted(vals)[:5]:
            print(f"    '{v}'")

    # Check Colorado-specific columns
    co_cols = [c for c in rows[0].keys() if c.startswith('_co_')]
    non_empty_co = 0
    for col in co_cols:
        ne = sum(1 for r in rows if r.get(col, '').strip())
        if ne > 0:
            non_empty_co += 1
    print(f"  Colorado-specific columns: {len(co_cols)} total, {non_empty_co} with data")


def main():
    base_dir = '/home/user/Douglas_County_2/data'
    cdot_dir = os.path.join(base_dir, 'CDOT')
    original_file = os.path.join(cdot_dir, 'Douglas_County.csv')

    # Load original data for lookups
    original_lookup = load_original_data(original_file)

    # Files to process
    files = [
        ('douglas_standardized.csv', 'douglas_standardized.csv'),
        ('douglas_all_roads.csv', 'douglas_all_roads.csv'),
        ('douglas_county_roads.csv', 'douglas_county_roads.csv'),
        ('douglas_no_interstate.csv', 'douglas_no_interstate.csv'),
    ]

    for input_name, output_name in files:
        input_path = os.path.join(cdot_dir, input_name)
        if not os.path.exists(input_path):
            print(f"SKIP: {input_path} not found")
            continue
        output_path = os.path.join(cdot_dir, output_name)
        process_file(input_path, output_path, original_lookup)

    # Also update crashes.csv in data root if it's Douglas data
    crashes_path = os.path.join(base_dir, 'crashes.csv')
    if os.path.exists(crashes_path):
        with open(crashes_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            first = next(reader, {})
            if first.get('_source_state', '').strip() == 'colorado' or \
               first.get('Physical Juris Name', '').strip() == 'DOUGLAS':
                print(f"\ncrashes.csv is Douglas data - processing it too")
                process_file(crashes_path, crashes_path, original_lookup)
            else:
                print(f"\ncrashes.csv is NOT Douglas data - skipping")

    # Verify
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)
    verify_output(
        os.path.join(cdot_dir, 'douglas_all_roads.csv'),
        os.path.join(base_dir, 'henrico_all_roads.csv')
    )

    print("\nDone! All files corrected.")


if __name__ == '__main__':
    main()
