#!/usr/bin/env python3
"""
Generate representative synthetic Henrico County crash data in Virginia TREDS format.

Creates 3 road-type CSV files matching the schema expected by generate_forecast.py.
Uses realistic distributions based on typical Virginia county crash patterns.
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(2024)

# Output directory
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "virginia", "henrico")

# Virginia TREDS column names (matching StateAdapter normalized output)
COLUMNS = [
    "Document Nbr", "Crash Date", "Crash Year", "Crash Military Time",
    "Crash Severity", "K_People", "A_People", "B_People", "C_People",
    "Collision Type", "Weather Condition", "Light Condition",
    "Roadway Surface Condition", "Roadway Alignment", "Roadway Description",
    "Intersection Type", "RTE Name", "SYSTEM", "Node", "RNS MP",
    "x", "y", "Physical Juris Name",
    "Pedestrian?", "Bike?", "Alcohol?", "Speed?", "Hitrun?",
    "Motorcycle?", "Night?", "Distracted?", "Drowsy?", "Drug Related?",
    "Young?", "Senior?", "Unrestrained?", "School Zone", "Work Zone Related",
    "Functional Class", "Area Type", "Facility Type", "Ownership",
    "Traffic Control Type", "Traffic Control Status",
    "First Harmful Event", "First Harmful Event Loc",
]

# Henrico County major corridors (real route names)
HENRICO_ROUTES = {
    "county_roads": [
        "BROAD ST", "W BROAD ST", "E BROAD ST", "STAPLES MILL RD",
        "THREE CHOPT RD", "PUMP RD", "PATTERSON AVE", "LAUDERDALE DR",
        "HUNGARY RD", "BROOK RD", "MECHANICSVILLE TPKE", "NINE MILE RD",
        "WILLIAMSBURG RD", "LABURNUM AVE", "PARHAM RD", "GASKINS RD",
        "NUCKOLS RD", "HUNGARY SPRING RD", "CREIGHTON RD", "CHARLES CITY RD",
        "COX RD", "RIDGE RD", "MILL RD", "PEMBERTON RD", "CHURCH RD",
    ],
    "state_roads": [
        "US-250", "US-33", "US-1", "US-360", "US-301",
        "VA-150", "VA-156", "VA-5", "VA-73", "VA-157",
    ],
    "interstates": [
        "I-64", "I-95", "I-295",
    ],
}

# Severity distribution (typical Virginia county)
SEVERITY_WEIGHTS = {"K": 0.008, "A": 0.035, "B": 0.11, "C": 0.20, "O": 0.647}

# Collision type distribution
COLLISION_TYPES = {
    "Rear End": 0.30,
    "Angle": 0.20,
    "Fixed Object - Off Road": 0.12,
    "Sideswipe - Same Direction": 0.10,
    "Head On": 0.03,
    "Pedestrian": 0.02,
    "Bicyclist": 0.01,
    "Other Animal": 0.04,
    "Non-Collision": 0.05,
    "Sideswipe - Opposite Direction": 0.03,
    "Fixed Object in Road": 0.02,
    "Backed Into": 0.02,
    "Other": 0.06,
}

# Intersection type distribution
INTERSECTION_TYPES = {
    "Non-Intersection": 0.55,
    "Intersection": 0.35,
    "Driveway": 0.05,
    "Roundabout": 0.02,
    "Ramp": 0.03,
}

# Weather distribution
WEATHER = {
    "Clear": 0.65, "Cloudy": 0.15, "Rain": 0.12,
    "Snow": 0.03, "Fog": 0.02, "Other": 0.03,
}

# Light condition distribution
LIGHT = {
    "Daylight": 0.60, "Dark - Lighted": 0.18,
    "Dark - Not Lighted": 0.10, "Dusk": 0.06, "Dawn": 0.06,
}

# Road system
SYSTEMS = {
    "county_roads": "Non-DOT secondary",
    "state_roads": "Primary",
    "interstates": "Interstate",
}

# Seasonal pattern (monthly multiplier, 1-indexed)
SEASONAL = {
    1: 0.85, 2: 0.82, 3: 0.95, 4: 1.00, 5: 1.08, 6: 1.10,
    7: 1.05, 8: 1.08, 9: 1.05, 10: 1.10, 11: 1.02, 12: 0.90,
}


def weighted_choice(distribution):
    """Pick a random key from a {key: probability} dict."""
    keys = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(keys, weights=weights, k=1)[0]


def generate_crashes(road_types, monthly_base, years, output_path):
    """Generate synthetic crash records."""
    rows = []
    doc_counter = 100000

    for year in years:
        for month in range(1, 13):
            # Skip future months
            if year == 2025 and month > 11:
                continue

            # Monthly crash count with seasonality and year-over-year trend
            year_factor = 1.0 + (year - 2019) * 0.015  # slight upward trend
            n_crashes = int(monthly_base * SEASONAL[month] * year_factor
                           + random.gauss(0, monthly_base * 0.08))
            n_crashes = max(10, n_crashes)

            for _ in range(n_crashes):
                doc_counter += 1
                road_cat = weighted_choice(
                    {k: len(v) for k, v in road_types.items()}
                )
                route = random.choice(road_types[road_cat])
                system = SYSTEMS[road_cat]

                severity = weighted_choice(SEVERITY_WEIGHTS)
                collision = weighted_choice(COLLISION_TYPES)
                intersection = weighted_choice(INTERSECTION_TYPES)
                weather = weighted_choice(WEATHER)
                light = weighted_choice(LIGHT)

                day = random.randint(1, 28)
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                crash_date = f"{month}/{day}/{year}"
                crash_time = f"{hour:02d}{minute:02d}"

                # Henrico County approximate coordinates
                lat = round(37.55 + random.uniform(-0.12, 0.12), 6)
                lon = round(-77.45 + random.uniform(-0.2, 0.2), 6)

                # Contributing factors (probabilistic)
                is_night = "Y" if light.startswith("Dark") else "N"
                is_ped = "Y" if collision == "Pedestrian" else "N"
                is_bike = "Y" if collision == "Bicyclist" else "N"
                is_alcohol = "Y" if random.random() < 0.05 else "N"
                is_speed = "Y" if random.random() < 0.08 else "N"
                is_hitrun = "Y" if random.random() < 0.06 else "N"
                is_distracted = "Y" if random.random() < 0.12 else "N"
                is_drowsy = "Y" if random.random() < 0.02 else "N"
                is_drug = "Y" if random.random() < 0.03 else "N"
                is_young = "Y" if random.random() < 0.15 else "N"
                is_senior = "Y" if random.random() < 0.12 else "N"
                is_unrestrained = "Y" if random.random() < 0.04 else "N"
                is_motorcycle = "Y" if random.random() < 0.03 else "N"
                is_school = "N"
                is_workzone = "Y" if random.random() < 0.03 else "N"

                # Severity people counts
                k_ppl = 1 if severity == "K" else 0
                a_ppl = 1 if severity == "A" else 0
                b_ppl = 1 if severity == "B" else 0
                c_ppl = 1 if severity == "C" else 0

                # Node (intersection ID)
                node = ""
                if intersection == "Intersection":
                    cross = random.choice(
                        list(road_types["county_roads"])
                        if "county_roads" in road_types
                        else ["MAIN ST"]
                    )
                    node = f"{route} & {cross}"

                row = {
                    "Document Nbr": str(doc_counter),
                    "Crash Date": crash_date,
                    "Crash Year": str(year),
                    "Crash Military Time": crash_time,
                    "Crash Severity": severity,
                    "K_People": k_ppl,
                    "A_People": a_ppl,
                    "B_People": b_ppl,
                    "C_People": c_ppl,
                    "Collision Type": collision,
                    "Weather Condition": weather,
                    "Light Condition": light,
                    "Roadway Surface Condition": "Dry" if weather == "Clear" else "Wet",
                    "Roadway Alignment": "Straight/Level",
                    "Roadway Description": intersection,
                    "Intersection Type": intersection,
                    "RTE Name": route,
                    "SYSTEM": system,
                    "Node": node,
                    "RNS MP": "",
                    "x": lon,
                    "y": lat,
                    "Physical Juris Name": "HENRICO",
                    "Pedestrian?": is_ped,
                    "Bike?": is_bike,
                    "Alcohol?": is_alcohol,
                    "Speed?": is_speed,
                    "Hitrun?": is_hitrun,
                    "Motorcycle?": is_motorcycle,
                    "Night?": is_night,
                    "Distracted?": is_distracted,
                    "Drowsy?": is_drowsy,
                    "Drug Related?": is_drug,
                    "Young?": is_young,
                    "Senior?": is_senior,
                    "Unrestrained?": is_unrestrained,
                    "School Zone": is_school,
                    "Work Zone Related": is_workzone,
                    "Functional Class": "",
                    "Area Type": "",
                    "Facility Type": "",
                    "Ownership": "",
                    "Traffic Control Type": "",
                    "Traffic Control Status": "",
                    "First Harmful Event": "",
                    "First Harmful Event Loc": "",
                }
                rows.append(row)

    # Write CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Written {len(rows):,} records to {output_path}")
    return len(rows)


def main():
    print("=" * 60)
    print("  Generating synthetic Henrico County crash data")
    print("  Format: Virginia TREDS (normalized)")
    print("=" * 60)

    years = list(range(2019, 2026))  # 2019-2025

    # 1. County roads only (~600 crashes/month)
    county_routes = {"county_roads": HENRICO_ROUTES["county_roads"]}
    path = os.path.join(DATA_DIR, "henrico_county_roads.csv")
    n = generate_crashes(county_routes, 600, years, path)
    print(f"\n  county_roads: {n:,} records")

    # 2. No interstate (county + state roads, ~900 crashes/month)
    no_int_routes = {
        "county_roads": HENRICO_ROUTES["county_roads"],
        "state_roads": HENRICO_ROUTES["state_roads"],
    }
    path = os.path.join(DATA_DIR, "henrico_no_interstate.csv")
    n = generate_crashes(no_int_routes, 900, years, path)
    print(f"  no_interstate: {n:,} records")

    # 3. All roads (county + state + interstate, ~1100 crashes/month)
    all_routes = {
        "county_roads": HENRICO_ROUTES["county_roads"],
        "state_roads": HENRICO_ROUTES["state_roads"],
        "interstates": HENRICO_ROUTES["interstates"],
    }
    path = os.path.join(DATA_DIR, "henrico_all_roads.csv")
    n = generate_crashes(all_routes, 1100, years, path)
    print(f"  all_roads: {n:,} records")

    print("\n" + "=" * 60)
    print("  Done! Data files are ready for forecast generation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
