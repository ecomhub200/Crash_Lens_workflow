# Deep Dive Tab — Claude Code Instructions for New State Onboarding

## Purpose

When the user says something like "onboard [State] crash data for Deep Dive" or "set up unused columns for [State]", follow these instructions step by step.

The Deep Dive tab in `app/index.html` displays analysis panels for state-specific crash data columns that aren't used by the core tool. Each new state's data will have different unused columns. Your job is to identify them, map them to panels, and update the code so the Deep Dive tab works for that state.

---

## Step 1: Identify Unused Columns

Read the new state's validated CSV file header:

```bash
head -1 data/<STATE_FOLDER>/<filename>.csv | tr ',' '\n' | cat -n
```

Then read the `COL` object in `app/index.html` (search for `const COL = {`). This lists every column the core tool uses.

**Compare the two lists.** Any CSV column NOT in the COL object is an "unused column" — a candidate for Deep Dive.

**Skip these columns** (metadata, not useful for analysis):
- `_source_state`
- `_source_file`

Report the findings to the user: how many unused columns found, grouped by category.

---

## Step 2: Classify Each Unused Column

Map every unused column into one of these 9 Deep Dive panel categories:

| Panel ID | Category | Look for columns containing these keywords |
|----------|----------|-------------------------------------------|
| `DriverDemo` | Driver Demographics | `age`, `sex`, `gender`, `dob` for transport units (`tu1`, `tu2`, etc.) |
| `Speed` | Speed Intelligence | `speed_limit`, `estimated_speed`, `stated_speed`, `travel_speed` |
| `Behavior` | Driver Behavior | `driver_action`, `human_factor`, `contributing`, `driver_condition` |
| `Vehicle` | Vehicle Fleet | `vehicle_type`, `vehicle_make`, `vehicle_model` |
| `NonMotorist` | Non-Motorist | `nm1_type`, `nm_action`, `nm_location`, `nm_facility`, `nm_contributing` |
| `Sequence` | Crash Sequence | `mhe`, `most_harmful`, `second_he`, `third_he`, `secondary_crash` |
| `Location` | Location & Community | `city`, `community`, `lane_position`, `location` (not lat/lon) |
| `Animal` | Animal Specifics | `wild_animal`, `animal_type`, `animal_species` |
| `Agency` | Agency Metadata | `system_code`, `agency_id`, `reporting_agency` |

If a column doesn't fit any category, add it to the closest match or ask the user if a new panel is needed.

---

## Step 3: Check Data Quality

For each unused column, check how much data it actually has:

```bash
python3 -c "
import csv
with open('data/<STATE_FOLDER>/<filename>.csv', 'r') as f:
    rows = list(csv.DictReader(f))
    print(f'Total rows: {len(rows)}')
    for col in [<LIST_OF_UNUSED_COLUMNS>]:
        non_empty = sum(1 for r in rows if r.get(col, '').strip())
        unique = set(r.get(col, '').strip() for r in rows if r.get(col, '').strip())
        sample = list(unique)[:5]
        print(f'{col}: {non_empty}/{len(rows)} non-empty, samples: {sample}')
"
```

**Flag these issues and fix them in the render functions:**

| Issue | How to detect | How to fix |
|-------|--------------|------------|
| Outlier values | Speed > 200 mph, Age > 120 | Add max threshold filter in render function |
| Coded values | Lane = "N01", "WLS" | Add a decoder function (like `ddDecodeLanePos`) |
| Low population (< 10%) | Column has data for very few rows | Add context note in the insight text ("Note: Only X% of crashes have this data") |
| Cross-column gaps | Facility data exists but NM type doesn't | Query each column independently, never pre-filter one column by another |
| Boolean format | "TRUE"/"FALSE" vs "Yes"/"No" vs "1"/"0" | Use `.toUpperCase()` and check all variants |

---

## Step 4: Update `panelConfig` in `index.html`

Find the `deepDiveState` object in `app/index.html` (search for `const deepDiveState`).

Update the `panelConfig` — replace the column names with the new state's columns:

```javascript
panelConfig: {
    DriverDemo: { columns: ['_<st>_tu1_age','_<st>_tu1_sex','_<st>_tu2_age','_<st>_tu2_sex'], label: 'Driver Demographics', icon: '👤' },
    Speed:      { columns: ['_<st>_tu1_speed_limit','_<st>_tu1_estimated_speed'], label: 'Speed Intelligence', icon: '🏎️' },
    // ... only include panels that have matching columns in the data
}
```

Where `<st>` is the state abbreviation prefix used in the CSV columns (e.g., `_co_` for Colorado, `_nc_` for North Carolina).

**Important:** Only include panel entries where the data actually has matching columns. Remove panel entries that have no matching data.

---

## Step 5: Update Render Functions

Each panel has a render function: `renderDDDriverDemo(rows)`, `renderDDSpeed(rows)`, etc.

Inside each render function, find all hardcoded column references like `r['_co_tu1_age']` and update them to the new state's column names.

**Search pattern:** Search `app/index.html` for `'_co_` to find all Colorado-specific column references in the Deep Dive section.

**Critical rules for render functions:**
1. Never pre-filter rows by one column when rendering a different column's chart (the NM facility bug)
2. Add outlier filters for any numeric data (speed, age)
3. Use `.toUpperCase()` when checking boolean/flag values
4. Add a "no data" message if a chart's data array is empty
5. Scale heat map colors to actual data max, not a hardcoded number

---

## Step 6: Update the `enhancements.json`

Create `data/<STATE_FOLDER>/enhancements.json` documenting which panels are available:

```json
{
  "_description": "Deep Dive panel configuration for <State> data",
  "stateEnhancements": {
    "driverDemographics": {
      "enabled": true,
      "columns": ["_<st>_tu1_age", "_<st>_tu1_sex"],
      "label": "Driver Demographics",
      "description": "Age and gender analysis of drivers involved in crashes"
    }
  },
  "standardUnused": {
    "vehicleCount": { "column": "Vehicle Count", "enriches": "Dashboard" },
    "personsInjured": { "column": "Persons Injured", "enriches": "Dashboard" },
    "pedestriansKilled": { "column": "Pedestrians Killed", "enriches": "Ped/Bike" },
    "pedestriansInjured": { "column": "Pedestrians Injured", "enriches": "Ped/Bike" }
  }
}
```

---

## Step 7: Update State `config.json`

If the new state's CSV has these standard columns, add them to `data/<STATE_FOLDER>/config.json` under `columnMapping`:

```json
"VEHICLE_COUNT": "<actual CSV column header>",
"PERSONS_INJURED": "<actual CSV column header>",
"PED_KILLED": "<actual CSV column header>",
"PED_INJURED": "<actual CSV column header>"
```

---

## Step 8: Verify Everything Works

After making all changes, verify:

1. **All panels that should appear DO appear** — check `deepDiveState.detectedColumns` has entries for each category with data
2. **No charts are empty** — every visible chart has data bars/slices
3. **KPI values are reasonable** — no NaN, no obviously wrong numbers
4. **Date filter works** — applying 1Y filter reduces crash counts
5. **Export works** — CSV download has the state-specific columns; PDF has insight text
6. **No console errors** — open browser dev tools, check for JavaScript errors

---

## Reference: Files You'll Edit

| File | What to change |
|------|---------------|
| `app/index.html` — `deepDiveState.panelConfig` | Column names for the new state |
| `app/index.html` — `renderDD*()` functions | Column name references (`r['_co_...']` → `r['_<st>_...']`) |
| `app/index.html` — `ddDecodeLanePos()` | Add state-specific value decoders if needed |
| `data/<STATE_FOLDER>/enhancements.json` | Create — documents available panels |
| `data/<STATE_FOLDER>/config.json` | Add standard unused column mappings if available |

## Reference: Existing Panel Render Functions

| Function | What it renders | Key columns it reads |
|----------|----------------|---------------------|
| `renderDDDriverDemo(rows)` | Age histogram, age×severity, gender doughnut, age×crash type matrix | `*_tu1_age`, `*_tu1_sex`, `*_tu2_age`, `*_tu2_sex` |
| `renderDDSpeed(rows)` | Speed scatter, differential bar, severity bar, route table | `*_tu1_speed_limit`, `*_tu1_estimated_speed`, `*_tu1_stated_speed` |
| `renderDDBehavior(rows)` | Driver actions bar, human factors bar, action×crash type matrix | `*_tu1_driver_action`, `*_tu1_human_factor` |
| `renderDDVehicle(rows)` | Vehicle type doughnut, type×severity stacked bar | `*_tu1_vehicle_type`, `*_tu2_vehicle_type` |
| `renderDDNonMotorist(rows)` | NM type doughnut, action bar, facility bar, contributing factors bar | `*_nm1_type`, `*_nm1_action`, `*_nm1_facility`, `*_nm1_contributing_factor` |
| `renderDDSequence(rows)` | Event count doughnut, secondary crash doughnut, event chain table | `*_mhe`, `*_second_he`, `*_third_he`, `*_secondary_crash` |
| `renderDDLocation(rows)` | City horizontal bar, lane position doughnut | `*_city`, `*_lane_position` |
| `renderDDAnimal(rows)` | Animal type doughnut, monthly seasonality bar | `*_wild_animal` |
| `renderDDAgency(rows)` | Agency/system table with K/A/EPDO columns | `*_system_code`, `*_agency_id` |

## Reference: Colorado Example (current implementation)

The current implementation uses Colorado CDOT data with prefix `_co_`. Column examples:
- `_co_tu1_age`, `_co_tu1_sex`, `_co_tu1_speed_limit`, `_co_tu1_estimated_speed`
- `_co_tu1_driver_action`, `_co_tu1_human_factor`, `_co_tu1_vehicle_type`
- `_co_nm1_type`, `_co_nm1_action`, `_co_nm1_facility`
- `_co_mhe`, `_co_second_he`, `_co_third_he`, `_co_secondary_crash`
- `_co_city`, `_co_lane_position`, `_co_wild_animal`
- `_co_system_code`, `_co_agency_id`

When onboarding a new state, replace `_co_` with the new state's prefix everywhere in the Deep Dive code section.
