#!/usr/bin/env python3
"""
CDOT Data Pipeline: Rebuild Road Type CSVs
==========================================
Rebuilds douglas_county_roads.csv, douglas_no_interstate.csv, and
douglas_all_roads.csv from douglas_standardized.csv with strict filtering.

Fixes applied:
  1. Strict road type filtering (no system code leakage)
  2. Removes ghost rows (empty Crash Year / Crash Date)
  3. Removes duplicate Document Nbr records (keeps first occurrence)
  4. Ensures county_roads ⊂ no_interstate ⊂ all_roads

Run: python3 scripts/rebuild_road_type_csvs.py
"""

import csv
import os
import sys
from collections import Counter

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'CDOT')
SOURCE = os.path.join(DATA_DIR, 'douglas_standardized.csv')

# Road system filter definitions based on _co_system_code
FILTERS = {
    'county_roads': {
        'include': {'County Road'},
        'output': 'douglas_county_roads.csv',
        'description': 'County Roads Only (county-maintained)'
    },
    'city_roads': {
        'include': {'City Street'},
        'output': 'douglas_city_roads.csv',
        'description': 'City Roads Only (city/town-maintained)'
    },
    'no_interstate': {
        'include': {'City Street', 'County Road', 'State Highway', 'Frontage Road'},
        'output': 'douglas_no_interstate.csv',
        'description': 'All Roads except Interstate'
    },
    'all_roads': {
        'include': {'City Street', 'County Road', 'State Highway', 'Frontage Road', 'Interstate Highway'},
        'output': 'douglas_all_roads.csv',
        'description': 'All Roads including Interstate'
    },
}


def rebuild():
    if not os.path.exists(SOURCE):
        print(f'ERROR: Source file not found: {SOURCE}')
        sys.exit(1)

    # Phase 1: Read all rows, filter ghost rows and duplicates
    print(f'Reading source: {SOURCE}')
    all_rows = []
    headers = None
    ghost_count = 0
    dup_count = 0
    seen_docs = set()

    with open(SOURCE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        for row in reader:
            # Skip ghost rows (empty year AND empty date)
            year = (row.get('Crash Year', '') or '').strip()
            date = (row.get('Crash Date', '') or '').strip()
            if not year and not date:
                ghost_count += 1
                continue

            # Skip duplicates (keep first occurrence)
            doc = (row.get('Document Nbr', '') or '').strip()
            if doc in seen_docs:
                dup_count += 1
                continue
            seen_docs.add(doc)

            all_rows.append(row)

    print(f'  Total source rows: {len(all_rows) + ghost_count + dup_count}')
    print(f'  Ghost rows removed: {ghost_count}')
    print(f'  Duplicate rows removed: {dup_count}')
    print(f'  Clean rows: {len(all_rows)}')

    # Phase 2: Write filtered files
    for filter_key, filt in FILTERS.items():
        output_path = os.path.join(DATA_DIR, filt['output'])
        include_systems = filt['include']

        filtered = []
        sys_counts = Counter()
        for row in all_rows:
            sys_code = (row.get('_co_system_code', '') or '').strip()
            if sys_code in include_systems:
                filtered.append(row)
                sys_counts[sys_code] += 1

        print(f'\n  Writing {filt["output"]}: {len(filtered)} rows')
        print(f'    Filter: {filt["description"]}')
        print(f'    Systems: {dict(sorted(sys_counts.items(), key=lambda x:-x[1]))}')

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(filtered)

    # Phase 3: Verify subset relationships
    print('\n  Verifying subset relationships...')
    county_ids = set()
    city_ids = set()
    noint_ids = set()
    all_ids = set()

    for filt_key, id_set in [('county_roads', county_ids), ('city_roads', city_ids), ('no_interstate', noint_ids), ('all_roads', all_ids)]:
        path = os.path.join(DATA_DIR, FILTERS[filt_key]['output'])
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                id_set.add(row.get('Document Nbr', ''))

    county_not_in_noint = county_ids - noint_ids
    noint_not_in_all = noint_ids - all_ids
    city_not_in_all = city_ids - all_ids
    county_city_overlap = county_ids & city_ids

    if county_not_in_noint:
        print(f'  WARNING: {len(county_not_in_noint)} county records NOT in no_interstate')
    else:
        print(f'  OK: county_roads ({len(county_ids)}) ⊂ no_interstate ({len(noint_ids)})')

    if city_not_in_all:
        print(f'  WARNING: {len(city_not_in_all)} city records NOT in all_roads')
    else:
        print(f'  OK: city_roads ({len(city_ids)}) ⊂ all_roads ({len(all_ids)})')

    if county_city_overlap:
        print(f'  WARNING: {len(county_city_overlap)} records in BOTH county_roads and city_roads')
    else:
        print(f'  OK: county_roads ∩ city_roads = ∅ (disjoint)')

    if noint_not_in_all:
        print(f'  WARNING: {len(noint_not_in_all)} no_interstate records NOT in all_roads')
    else:
        print(f'  OK: no_interstate ({len(noint_ids)}) ⊂ all_roads ({len(all_ids)})')

    print('\nDone!')


if __name__ == '__main__':
    rebuild()
