# Onboarding a New State — Step-by-Step Playbook

> **Audience**: Claude Code (or any developer) adding a new US state to the Crash Lens pipeline.
> **Goal**: Follow this document exactly to onboard a new state without discovering patterns from scratch.
> Every file path, field name, and code pattern referenced below comes from the working Virginia and Colorado implementations.

---

## Prerequisites — What Already Exists

Before you start, confirm these exist for the target state:

| File | Location | Status |
|------|----------|--------|
| `hierarchy.json` | `states/{state}/hierarchy.json` | **Already exists** for all 50 states + DC |
| State entry in `config.json` | Root `config.json` → `states.{state}` | **Already exists** (stub with `fips`, `dotName`, `r2Prefix`; `dataDir` and `defaultJurisdiction` are `null`) |

If these don't exist, something is wrong. Stop and investigate.

---

## Overview — The 8 Files/Changes You Must Create

| # | What | Path | Purpose |
|---|------|------|---------|
| 1 | State config | `states/{state}/config.json` | Column mappings, road systems, EPDO weights, jurisdictions |
| 2 | Boundaries config | `states/{state}/boundaries.json` | ArcGIS endpoints for DOT district/county boundaries on map |
| 3 | State adapter entry | `scripts/state_adapter.py` | Detection signature + normalizer class |
| 4 | Cache registration | `scripts/init_cache.py` | Add to `ACTIVE_STATES` list |
| 5 | Download script | `download_{state}_crash_data.py` (root) or reuse existing | State DOT data portal downloader |
| 6 | Download workflow | `.github/workflows/download-{state}.yml` | GitHub Actions download + trigger pipeline |
| 7 | Pipeline registration | `.github/workflows/pipeline.yml` | Add state to the `options` list |
| 8 | Root config update | `config.json` → `states.{state}` | Set `dataDir` and `defaultJurisdiction` |

---

## Step 1: Create `states/{state}/config.json`

This is the most critical file. Every pipeline script reads it. Copy the structure from Virginia (simpler) or Colorado (derived severity), depending on which pattern matches.

### Template

```json
{
  "state": {
    "name": "Texas",
    "abbreviation": "TX",
    "fips": "48",
    "dotName": "TxDOT",
    "dotFullName": "Texas Department of Transportation",
    "dataSystemName": "CRIS (Crash Records Information System)",
    "dataPortalUrl": "https://cris.dot.state.tx.us",
    "coordinateBounds": {
      "latMin": 25.8,
      "latMax": 36.5,
      "lonMin": -106.6,
      "lonMax": -93.5
    }
  },

  "columnMapping": {
    "_description": "Maps raw CSV column headers from {STATE DOT} to internal field codes",
    "ID": "{raw_column_for_crash_id}",
    "DATE": "{raw_column_for_date}",
    "TIME": "{raw_column_for_time}",
    "YEAR": "{raw_column_for_year_or_null_if_derived}",
    "SEVERITY": "{raw_column_or_null_if_derived}",
    "K": "{raw_column_for_fatal_count_or_null}",
    "A": "{raw_column_for_incapacitating_or_null}",
    "B": "{raw_column_for_non_incapacitating_or_null}",
    "C": "{raw_column_for_possible_injury_or_null}",
    "O": "{raw_column_for_pdo_or_null}",
    "COLLISION": "{raw_column_for_collision_type}",
    "WEATHER": "{raw_column_for_weather}",
    "LIGHT": "{raw_column_for_lighting}",
    "SURFACE": "{raw_column_for_road_surface}",
    "ALIGNMENT": "{raw_column_for_road_alignment_or_null}",
    "ROAD_DESC": "{raw_column_for_road_description_or_null}",
    "INT_TYPE": "{raw_column_for_intersection_type_or_null}",
    "ROUTE": "{raw_column_for_route_name}",
    "ROAD_SYSTEM": "{raw_column_for_road_classification}",
    "X": "{raw_column_for_longitude}",
    "Y": "{raw_column_for_latitude}",
    "JURISDICTION": "{raw_column_for_county_or_jurisdiction}",
    "NODE": "{raw_column_for_intersection_node_or_null}",
    "PED": "{raw_column_for_pedestrian_flag_or_null}",
    "BIKE": "{raw_column_for_bicycle_flag_or_null}",
    "ALCOHOL": "{raw_column_for_alcohol_flag_or_null}",
    "SPEED": "{raw_column_for_speed_flag_or_null}",
    "HITRUN": "{raw_column_for_hitrun_flag_or_null}"
  },

  "derivedFields": {
    "_description": "Fields that must be derived from other columns",
    "SEVERITY": {
      "method": "direct | injury_hierarchy | report_type_map",
      "source": "column_name_or_null",
      "columns": ["only_if_injury_hierarchy"],
      "labels": ["K", "A", "B", "C", "O"]
    }
  },

  "roadSystems": {
    "values": {
      "{raw_value_1}": {
        "category": "local | state | interstate",
        "isStateDOT": false,
        "isInterstate": false,
        "displayName": "Local Road",
        "standardizedSystem": "Non-DOT secondary | Primary | Secondary | Interstate"
      }
    },
    "splitConfig": {
      "countyRoads": {
        "method": "ownership | system_column | agency_id | column_value",
        "column": "{column_to_filter_on}",
        "includeValues": ["{values_for_county_roads}"],
        "agencyMap": "{only_for_agency_id_method}"
      },
      "interstateExclusion": {
        "method": "functional_class | system_column | column_value",
        "column": "{column_to_filter_on}",
        "excludeValues": ["{interstate_values}"]
      }
    },
    "filterProfiles": {
      "countyOnly": {
        "name": "County/City Roads Only",
        "description": "Only locally-maintained roads",
        "systemValues": ["{standardized_local_values}"]
      },
      "countyPlusVDOT": {
        "name": "All Roads (No Interstate)",
        "description": "Local + state roads, excluding interstates",
        "systemValues": ["{local + state values}"]
      },
      "allRoads": {
        "name": "All Roads (Including Interstate)",
        "description": "All road types",
        "systemValues": ["{all values}"]
      }
    }
  },

  "epdoWeights": {
    "_source": "Source citation (e.g., HSM Standard, state-specific memo)",
    "K": 883,
    "A": 94,
    "B": 21,
    "C": 11,
    "O": 1
  },

  "jurisdictions": {
    "{jurisdiction_key}": {
      "name": "Full County Name",
      "type": "county",
      "fips": "XXX",
      "namePatterns": ["PATTERN1", "Pattern2"],
      "mapCenter": [lat, lon],
      "mapZoom": 11,
      "bbox": [west, south, east, north],
      "maintainsOwnRoads": true
    }
  },

  "cache_config": {
    "update_frequency": "daily | monthly | biannual",
    "typical_new_records_per_update": 150,
    "data_retention_years": 5,
    "stale_threshold_days": 30,
    "geocode_ttl_days": 365,
    "max_cache_size_mb": 200
  }
}
```

### How to Determine Values

**Column mapping**: You MUST obtain a sample CSV from the state's crash data portal. Read the header row and map each raw column name to the internal code. If a field doesn't exist in the data, set it to `null`.

**Severity derivation methods** (pick one):
- `"direct"` — Virginia pattern: severity is a single column with K/A/B/C/O values directly
- `"injury_hierarchy"` — Colorado pattern: multiple injury count columns (Injury 04=K, Injury 03=A, etc.), derive highest severity
- `"report_type_map"` — Maryland pattern: map report types like "Fatal Crash" → K, "Injury Crash" → B, "Property Damage Crash" → O

**Road systems**: Look at the raw data's road classification columns. Map each unique value to one of the 3 standardized categories: `"Non-DOT secondary"` (local), `"Primary"` or `"Secondary"` (state), `"Interstate"`.

**splitConfig** (CRITICAL — determines how `county_roads.csv` and `no_interstate.csv` are produced):

1. Open a sample CSV in a spreadsheet. Filter to one jurisdiction. Confirm you see ALL road types (Interstate, State, County, City).
2. **For county_roads**: Find the column that best identifies county/locally-owned roads:
   - **Ownership column** (e.g., "2. County Hwy Agency") → use `method: "ownership"`. This is the most accurate when available (Virginia pattern).
   - **SYSTEM column** (e.g., "Non-DOT secondary") → use `method: "system_column"`. Simpler but may not perfectly match ownership semantics.
   - **Agency ID column** (e.g., "DSO" for Douglas Sheriff's Office) → use `method: "agency_id"` with an `agencyMap` (Colorado pattern).
3. **For interstate exclusion**: Find the column that identifies Interstate roads:
   - **Functional Class column** (e.g., "1-Interstate (A,1)") → use `method: "functional_class"`. Most precise for excluding interstates (Virginia pattern).
   - **SYSTEM column** (e.g., "Interstate") → use `method: "system_column"`. Works when SYSTEM reliably identifies interstates.
4. **Validate**: Run the split script and compare output file sizes against manually filtered reference files from a spreadsheet. If sizes don't match, the wrong column/values are being used.

**Common mistake**: The SYSTEM column may seem correct but can give different results than Ownership or Functional Class. Always verify against manual filtering from the state DOT's raw data.

**EPDO weights**: Use FHWA 2025 (K:883, A:94, B:21, C:11, O:1) unless the state DOT publishes their own crash cost weights.

**Coordinate bounds**: Use the state's geographic bounding box. Find at `states/{state}/hierarchy.json` → `state.bbox` if available, or look up the state's lat/lon bounds.

### Reference Implementations

| Pattern | Example State | Key Difference |
|---------|---------------|----------------|
| Direct severity | `states/virginia/config.json` | `SEVERITY` maps to a single column |
| Derived severity | `states/colorado/config.json` | `SEVERITY` is null, derived from injury counts |
| Dual data source | `states/maryland/config.json` | Has `ID_ALT`, `DATE_ALT` for alternate field names |

### splitConfig Reference Implementations

| Pattern | Example State | countyRoads Method | interstateExclusion Method |
|---------|---------------|--------------------|-----------------------------|
| Ownership + Functional Class | Virginia (`states/virginia/config.json`) | `ownership` on `Ownership` column, value `"2. County Hwy Agency"` | `functional_class` on `Functional Class` column, exclude `"1-Interstate (A,1)"` |
| Agency ID + System Code | Colorado (`states/colorado/config.json`) | `agency_id` on `_co_agency_id` column with `agencyMap` per jurisdiction | `column_value` on `_co_system_code` column, exclude `"Interstate Highway"` |
| SYSTEM column (simple) | Default fallback | `system_column` on `SYSTEM` column, include `["Non-DOT secondary"]` | `system_column` on `SYSTEM` column, exclude `["Interstate"]` |

**When in doubt**: Use the Ownership-based approach if the state's data has an Ownership column. It is the most semantically accurate for identifying county-owned roads. If not available, use the SYSTEM column approach but **always validate against manually filtered reference data**.

---

## Step 2: Create `states/{state}/boundaries.json`

Controls boundary layers on the map. Copy from Colorado and adjust.

### Template

```json
{
  "state": "{state}",
  "stateFips": "{XX}",
  "stateAbbrev": "{XX}",

  "dotDistricts": {
    "name": "{DOT Name} {Region Type Plural}",
    "term": "{Region}",
    "termPlural": "{Regions}",
    "count": 5,
    "source": "arcgis_rest",
    "endpoint": "{ArcGIS REST endpoint for DOT district boundaries}",
    "nameField": "{field_name_for_district_name}",
    "codeField": "{field_name_for_district_id}",
    "style": {
      "color": "#2563eb",
      "weight": 3,
      "fillOpacity": 0.05,
      "dashArray": "8, 4"
    },
    "fallbackGeojson": "shared/boundaries/{state}_dot_districts.geojson"
  },

  "mpo": {
    "source": "bts_ntad",
    "stateFilter": "{XX}",
    "endpoint": "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0",
    "style": {
      "color": "#7c3aed",
      "weight": 2,
      "fillOpacity": 0.08,
      "dashArray": "6, 3"
    },
    "fallbackGeojson": "shared/boundaries/{state}_mpos.geojson"
  },

  "counties": {
    "source": "census_tiger",
    "tigerwebLayer": 82,
    "tigerwebQuery": "STATE='{state_fips}'",
    "geojson": "shared/boundaries/{state}_counties.geojson",
    "style": {
      "color": "#334155",
      "weight": 1.5,
      "fillOpacity": 0.0
    }
  },

  "stateOutline": {
    "source": "census_tiger",
    "tigerwebLayer": 80,
    "tigerwebQuery": "STATE='{state_fips}'",
    "style": {
      "color": "#0f172a",
      "weight": 2.5,
      "fillOpacity": 0.0,
      "dashArray": "none"
    }
  }
}
```

### Notes
- The MPO endpoint is **the same for all states** (BTS NTAD national dataset), just change `stateFilter`.
- The `counties` and `stateOutline` sections use Census TIGERweb — only the FIPS code changes.
- The `dotDistricts` endpoint is state-specific. Find the state DOT's ArcGIS REST services. If unavailable, set `"source": "none"` and the map will skip that layer.

---

## Step 3: Add to `scripts/state_adapter.py`

This is the data conversion engine. You must add 3 things:

### 3a. Detection Signature

Add to `STATE_SIGNATURES` dict (line ~40):

```python
'{state}': {
    'required': ['{col1}', '{col2}', '{col3}', '{col4}'],
    'optional': ['{col5}', '{col6}'],
    'display_name': '{State} ({DOT System Name})',
    'config_dir': '{state}'
},
```

**Required columns** = 3-5 columns that **uniquely identify** this state's CSV format. Choose columns with distinctive names that no other state uses. The `StateDetector.detect_from_headers()` method checks if ALL required columns are present in the CSV headers.

### 3b. Normalizer Class

Create a class inheriting from `BaseNormalizer`. This is the most code-intensive step (~100-400 lines depending on complexity).

**Pattern to follow** (see `ColoradoNormalizer` at line 178 or `MarylandNormalizer` at line 915):

```python
class {State}Normalizer(BaseNormalizer):
    """{State} {DOT} data normalizer — converts to Virginia-compatible VDOT format."""

    # Value mapping dicts: raw state values → VDOT numbered format
    COLLISION_VDOT_MAP = {
        '{raw_value}': '{N}. {VDOT Standard Name}',
        # ... map ALL collision types from the state's data
    }

    WEATHER_VDOT_MAP = { ... }
    LIGHT_VDOT_MAP = { ... }
    SURFACE_VDOT_MAP = { ... }
    ROAD_SYSTEM_MAP = { ... }

    def normalize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        n = {}

        # --- ID ---
        n['Document Nbr'] = row.get('{id_column}', '').strip()

        # --- Date/Time ---
        raw_date = row.get('{date_column}', '').strip()
        n['Crash Date'] = raw_date
        n['Crash Year'] = self._extract_year(raw_date)
        n['Crash Military Time'] = row.get('{time_column}', '').strip()

        # --- Severity ---
        # Use ONE of these patterns:
        # Pattern A (direct): n['Crash Severity'] = row.get('{severity_col}', 'O')
        # Pattern B (injury hierarchy): derive from injury count columns
        # Pattern C (report type map): map report type string to K/A/B/C/O

        # --- Collision Type (map to VDOT numbered format) ---
        raw = row.get('{collision_column}', '').strip()
        n['Collision Type'] = self.COLLISION_VDOT_MAP.get(raw, '16. Other')

        # --- Weather, Light, Surface (same pattern) ---
        # ... map each to VDOT numbered format

        # --- Route & Location ---
        n['RTE Name'] = row.get('{route_column}', '').strip()
        n['SYSTEM'] = self.ROAD_SYSTEM_MAP.get(
            row.get('{system_column}', '').strip(), 'Non-DOT secondary')
        n['Node'] = ''  # Build intersection node ID if data supports it

        # --- Coordinates (CRITICAL: x=longitude, y=latitude) ---
        n['x'] = row.get('{longitude_column}', '').strip()
        n['y'] = row.get('{latitude_column}', '').strip()

        # --- Jurisdiction ---
        n['Physical Juris Name'] = row.get('{county_column}', '').strip()

        # --- Boolean flags ---
        # Set each to 'Yes' or 'No' based on available columns
        n['Pedestrian?'] = 'Yes' if ... else 'No'
        n['Bike?'] = 'Yes' if ... else 'No'
        n['Alcohol?'] = 'Yes' if ... else 'No'
        n['Speed?'] = 'Yes' if ... else 'No'
        n['Hitrun?'] = 'Yes' if ... else 'No'
        n['Motorcycle?'] = 'No'  # Default if not available
        n['Night?'] = 'Yes' if raw_light in self.DARKNESS_VALUES else 'No'
        n['Distracted?'] = 'No'
        n['Drowsy?'] = 'No'
        n['Drug Related?'] = 'No'
        n['Young?'] = 'No'
        n['Senior?'] = 'No'
        n['Unrestrained?'] = 'No'
        n['School Zone'] = 'No'
        n['Work Zone Related'] = 'No'

        # --- Fields not available in this state's data ---
        n['Roadway Alignment'] = ''
        n['Roadway Description'] = ''
        n['Intersection Type'] = ''
        n['Relation To Roadway'] = ''
        n['Traffic Control Type'] = ''
        n['Traffic Control Status'] = ''
        n['Functional Class'] = ''
        n['Area Type'] = ''
        n['Facility Type'] = ''
        n['Ownership'] = ''
        n['First Harmful Event'] = ''
        n['First Harmful Event Loc'] = ''
        n['Vehicle Count'] = ''
        n['Persons Injured'] = ''
        n['Pedestrians Killed'] = '0'
        n['Pedestrians Injured'] = '0'
        n['RNS MP'] = ''

        # --- Source tracking ---
        n['_source_state'] = '{state}'

        return n
```

**CRITICAL**: The output dict keys MUST use the Virginia standard column names exactly. See `STANDARD_COLUMNS` at line 68 for the full list.

### VDOT Numbered Format Reference

These are the standard numbered-prefix values the app expects. Map your state's raw values to these:

**Collision Types**: `1. Rear End`, `2. Angle`, `3. Head On`, `4. Sideswipe - Same Direction`, `5. Sideswipe - Opposite Direction`, `8. Non-Collision`, `9. Fixed Object - Off Road`, `10. Deer/Animal`, `11. Fixed Object in Road`, `12. Ped`, `13. Bicycle`, `14. Fixed Object`, `16. Other`

**Weather**: `1. No Adverse Condition (Clear/Cloudy)`, `3. Fog/Smog/Smoke`, `4. Snow`, `5. Rain`, `6. Sleet/Hail/Freezing`, `7. Blowing Sand/Dust`, `8. Severe Crosswinds`

**Light**: `1. Dawn`, `2. Daylight`, `3. Dusk`, `4. Darkness - Road Lighted`, `5. Darkness - Road Not Lighted`, `6. Dark - Unknown`

**Surface**: `1. Dry`, `2. Wet`, `3. Snow`, `4. Slush`, `5. Ice`, `6. Sand/Mud/Dirt/Oil/Gravel`, `7. Water`, `16. Other`

### 3c. Register in `_NORMALIZERS` Dict

At line ~1225:

```python
_NORMALIZERS = {
    'colorado': ColoradoNormalizer,
    'virginia': VirginiaNormalizer,
    'maryland': MarylandNormalizer,
    'maryland_statewide': MarylandNormalizer,
    '{state}': {State}Normalizer,         # <-- ADD THIS
}
```

---

## Step 4: Add to `scripts/init_cache.py` — `ACTIVE_STATES`

At line 46, add the new state:

```python
ACTIVE_STATES = ['virginia', 'colorado', 'maryland', '{state}']
```

This ensures `python scripts/init_cache.py --all` initializes cache for this state.

The pipeline's Stage 0 will auto-create the cache structure when run:
```
.cache/{state}/
  cache_manifest.json
  validation/
    validated_hashes.json
    last_run.json
  geocode/
    geocode_cache.json
    geocoded_records.json
    cache_stats.json
```

---

## Step 5: Create or Wire Up the Download Script

### Option A: State Has an Open Data API (ArcGIS, Socrata, etc.)

Create a new download script or adapt the existing one:

| Data API Type | Reference Script | Used By |
|---------------|-----------------|---------|
| ArcGIS REST | `download_crash_data.py` (root) | Virginia |
| Web portal + Playwright | `download_cdot_crash_data.py` (root) | Colorado |
| Socrata SODA | `download_moco_crashes.py` (root) | Maryland |

The script must:
1. Accept `--output-dir`, `--jurisdiction`, `--force-download` flags
2. Output a CSV file at a predictable path: `{data_dir}/{jurisdiction}_all_roads.csv` or `{data_dir}/{state}_statewide_all_roads.csv`
3. The CSV must contain the raw columns as they come from the DOT (the adapter normalizes later)

### Option B: Manually Uploaded CSV

If the state's data isn't programmatically downloadable, skip the download script. Place the raw CSV manually in `data/{DataDir}/` and run the pipeline with `data_source` pointing to it.

### Conversion Step

If the state is not Virginia (passthrough), the download workflow must call `state_adapter.py` to convert:

```bash
python -c "
from scripts.state_adapter import convert_file
state, total, gps = convert_file(
    '{raw_csv_path}',
    '{output_standardized_csv_path}',
    state='{state}'
)
print(f'Converted {total} rows ({gps} with GPS) from {state}')
"
```

This produces a Virginia-compatible CSV that the pipeline can process.

---

## Step 6: Create `.github/workflows/download-{state}.yml`

Copy from `download-colorado.yml` (the most complete template) and customize.

### Template Structure

```yaml
name: "Download: {State Name}"

on:
  schedule:
    - cron: '0 11 1 * *'  # Adjust frequency based on state data updates

  workflow_dispatch:
    inputs:
      scope:
        description: 'Processing scope'
        required: true
        type: choice
        default: 'jurisdiction'
        options:
          - jurisdiction
          - region
          - mpo
          - statewide

      selection:
        description: 'Jurisdiction/Region/MPO name'
        required: false
        default: '{default_jurisdiction}'
        type: string

      region:
        description: 'DOT Region (if scope=region)'
        required: false
        type: choice
        default: ''
        options:
          - ''
          # List all region IDs from states/{state}/hierarchy.json → regions
          - {region_1_id}
          - {region_2_id}

      mpo:
        description: 'MPO (if scope=mpo)'
        required: false
        type: choice
        default: ''
        options:
          - ''
          # List all MPO IDs from states/{state}/hierarchy.json → tprs (where type=mpo)
          - {mpo_1_id}
          - {mpo_2_id}

      force_download:
        description: 'Force re-download'
        required: false
        type: boolean
        default: false

      skip_pipeline:
        description: 'Skip pipeline trigger'
        required: false
        type: boolean
        default: false

permissions:
  contents: write
  actions: write

env:
  STATE: {state}
  DOT_NAME: {DotAbbrev}
  DATA_DIR: data/{DataDir}  # Must match config.json states.{state}.dataDir

jobs:
  download:
    name: "Download {State} (${{ github.event.inputs.scope || 'jurisdiction' }})"
    runs-on: ubuntu-latest
    timeout-minutes: 120

    outputs:
      scope: ${{ steps.params.outputs.scope }}
      selection: ${{ steps.params.outputs.selection }}
      csv_path: ${{ steps.download.outputs.csv_path }}

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install pandas requests openpyxl

      - name: Resolve parameters
        id: params
        run: |
          SCOPE="${{ github.event.inputs.scope || 'jurisdiction' }}"
          SELECTION="${{ github.event.inputs.selection || '{default_jurisdiction}' }}"

          if [ "$SCOPE" = "region" ] && [ -n "${{ github.event.inputs.region }}" ]; then
            SELECTION="${{ github.event.inputs.region }}"
          elif [ "$SCOPE" = "mpo" ] && [ -n "${{ github.event.inputs.mpo }}" ]; then
            SELECTION="${{ github.event.inputs.mpo }}"
          elif [ "$SCOPE" = "statewide" ]; then
            SELECTION=""
          fi

          echo "scope=$SCOPE" >> $GITHUB_OUTPUT
          echo "selection=$SELECTION" >> $GITHUB_OUTPUT

          python scripts/resolve_scope.py \
            --state ${{ env.STATE }} --scope "$SCOPE" --selection "$SELECTION" --json

      - name: Download crash data
        id: download
        run: |
          # Call state-specific download script
          # CRITICAL: Always use --filter allRoads to ensure all_roads.csv
          # contains ALL road types (Interstate, State Hwy, County, City, Federal).
          # Road-type splitting happens in Stage 2 (split_road_type.py), NOT here.
          #
          # Output must be at: ${{ env.DATA_DIR }}/{jurisdiction}_all_roads.csv
          # or: ${{ env.DATA_DIR }}/{state}_statewide_all_roads.csv
          #
          # If raw data needs conversion, also run state_adapter.py:
          # python -c "from scripts.state_adapter import convert_file; ..."

          CSV_PATH="..."  # Set to the final output path
          echo "csv_path=$CSV_PATH" >> $GITHUB_OUTPUT

      - name: Commit downloaded data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add ${{ env.DATA_DIR }}/ 2>/dev/null || true
          if git diff --cached --quiet; then
            echo "No new data to commit"
          else
            git commit -m "data: ${{ env.STATE }} download (${{ steps.params.outputs.scope }}/${{ steps.params.outputs.selection || 'all' }})"
            git push origin main
          fi

  trigger-pipeline:
    name: "Trigger pipeline"
    needs: download
    if: github.event.inputs.skip_pipeline != 'true'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/github-script@v7
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          script: |
            await github.rest.actions.createWorkflowDispatch({
              owner: context.repo.owner,
              repo: context.repo.repo,
              workflow_id: 'pipeline.yml',
              ref: 'main',
              inputs: {
                state: '${{ env.STATE }}',
                scope: '${{ needs.download.outputs.scope }}',
                selection: '${{ needs.download.outputs.selection }}',
                data_source: '${{ needs.download.outputs.csv_path }}',
                skip_forecasts: 'true'
              }
            });
```

### Existing Scaffold Workflows

Some states already have disabled scaffold workflows at `.github/workflows/download-{state}-crash-data.yml`. These are **old-format** (don't use scope/pipeline trigger pattern). You should either:
- **Replace** the scaffold with the new template above, OR
- **Delete** the scaffold and create the new `download-{state}.yml`

The new naming convention is `download-{state}.yml` (not `download-{state}-crash-data.yml`).

---

## Step 7: Add State to `pipeline.yml`

In `.github/workflows/pipeline.yml`, add the state to the `inputs.state.options` list (line ~24):

```yaml
      state:
        description: 'State to process'
        required: true
        type: choice
        options:
          - virginia
          - colorado
          - maryland
          - {state}        # <-- ADD THIS
```

**No other changes needed** — the pipeline is fully generic. All 8 stages read from `states/{state}/config.json` and `states/{state}/hierarchy.json` dynamically.

---

## Step 8: Update Root `config.json`

Find the state's entry in `config.json` → `states.{state}` and set the two null fields:

```json
"{state}": {
  "fips": "XX",
  "name": "State Name",
  "abbreviation": "XX",
  "dotName": "XXDOT",
  "defaultJurisdiction": "{most_common_jurisdiction_key}",
  "dataDir": "{DataDirName}",
  "r2Prefix": "{state}",
  "appSubtitle": "{State} Crash Analysis Tool"
}
```

| Field | What to Set | Example (Colorado) |
|-------|-------------|-------------------|
| `defaultJurisdiction` | The jurisdiction to use as default in dropdowns | `"douglas"` |
| `dataDir` | Subdirectory name under `data/` for this state's files. Use DOT abbreviation or state name. If `null`, data goes to `data/` root (Virginia pattern). | `"CDOT"` |

---

## Verification Checklist

After completing all 8 steps, verify each component works:

### Config Verification

```bash
# 1. Scope resolver works with hierarchy.json
python scripts/resolve_scope.py --state {state} --list
python scripts/resolve_scope.py --state {state} --scope statewide --json

# 2. Cache initializes from config.json
python scripts/init_cache.py --state {state}
ls -la .cache/{state}/

# 3. State adapter detects and normalizes
python -c "
from scripts.state_adapter import StateDetector, get_normalizer
print('Supported:', get_normalizer('{state}').state_key)
"
```

### Pipeline Dry Run

```bash
# If you have a sample CSV, test the full flow locally:

# Validate
python scripts/validate_data.py --state {state} --input {csv_path}

# Convert (if not Virginia format)
python -c "
from scripts.state_adapter import convert_file
state, total, gps = convert_file('{raw_csv}', '{output_csv}', state='{state}')
print(f'{state}: {total} rows, {gps} with GPS')
"

# Split jurisdictions
python scripts/split_jurisdictions.py --state {state} --input {csv_path} --output-dir data/{DataDir} --dry-run

# Split road types
python scripts/split_road_type.py --state {state} --data-dir data/{DataDir} --auto --dry-run
```

### Workflow Test

Trigger the download workflow manually from GitHub Actions:
1. Go to Actions → "Download: {State}"
2. Select `scope: jurisdiction`, `selection: {test_county}`
3. Check `skip_pipeline: true` for first test
4. Verify CSV is downloaded and committed
5. Then run pipeline manually: Actions → "Pipeline: Process Crash Data" → state: {state}

---

## Common Patterns by Data Source Type

### Pattern 1: ArcGIS REST API (like Virginia)

- Data is queried via HTTP GET with pagination (`resultOffset`, `resultRecordCount`)
- Returns JSON features, converted to CSV
- See `download_crash_data.py` for the full pattern
- Usually supports filtering by date range and jurisdiction

### Pattern 2: Web Portal + Playwright (like Colorado CDOT)

- Data is behind a web portal requiring browser automation
- Playwright downloads files year-by-year
- See `download_cdot_crash_data.py` for the full pattern
- Add `playwright` to `requirements.txt` if not already present

### Pattern 3: Socrata Open Data API (like Maryland)

- Data is on an open data portal (e.g., `opendata.maryland.gov`)
- Uses SODA API with `$where` filters and `$limit`/`$offset` pagination
- See `download_moco_crashes.py` for the full pattern
- Simple HTTP GET requests, no browser automation needed

### Pattern 4: Direct CSV Download

- State provides a downloadable CSV link
- Simplest pattern: just `requests.get(url)` and save
- May require authentication or form submission first

---

## R2 Storage Path Convention

The pipeline uploads processed CSVs to Cloudflare R2 following this structure:

```
crash-lens-data/
  {state}/
    {jurisdiction}/
      all_roads.csv
      no_interstate.csv
      county_roads.csv
      forecasts_all_roads.json
      forecasts_no_interstate.json
      forecasts_county_roads.json
    _region/
      {region_id}/
        {region_id}_all_roads.csv
        {region_id}_no_interstate.csv
        {region_id}_county_roads.csv
    _mpo/
      {mpo_id}/
        {mpo_id}_all_roads.csv
        {mpo_id}_no_interstate.csv
        {mpo_id}_county_roads.csv
    _state/
      statewide_all_roads.csv.gz
```

The R2 prefix comes from `config.json` → `states.{state}.r2Prefix` (usually the lowercase state name).

---

## Files Modified Summary

When onboarding is complete, you should have touched exactly these files:

| Action | File |
|--------|------|
| **Created** | `states/{state}/config.json` |
| **Created** | `states/{state}/boundaries.json` |
| **Created** | `.github/workflows/download-{state}.yml` |
| **Created** (maybe) | `download_{state}_crash_data.py` or reused existing |
| **Modified** | `scripts/state_adapter.py` (signature + normalizer + registry) |
| **Modified** | `scripts/init_cache.py` (ACTIVE_STATES) |
| **Modified** | `.github/workflows/pipeline.yml` (state options list) |
| **Modified** | `config.json` (dataDir + defaultJurisdiction) |
| **Already existed** | `states/{state}/hierarchy.json` (no changes needed) |

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `resolve_scope.py` fails with "not in config.json" | `config.json` → `states.{state}` missing `dataDir` | Set `dataDir` (Step 8) |
| `validate_data.py` reports "out of bounds" | Wrong `coordinateBounds` in config | Check lat/lon bounds for the state |
| `split_road_type.py` puts everything in "all_roads" | `splitConfig` column doesn't match normalized output | Check that `splitConfig.column` matches the VDOT-standardized column name (e.g., `Ownership`, `Functional Class`, `SYSTEM`) |
| `all_roads.csv` is too small (missing Interstate/State roads) | Download script used `countyOnly` filter instead of `allRoads` | Always pass `--filter allRoads` in download workflow. Check `download_crash_data.py` default filter. |
| `county_roads.csv` has wrong row count | SYSTEM column doesn't match Ownership semantics | Switch splitConfig to use `ownership` method with the state's ownership column values |
| `no_interstate.csv` includes/excludes wrong roads | SYSTEM column doesn't reliably identify interstates | Switch splitConfig to use `functional_class` method with the state's functional class interstate value |
| ArcGIS API returns partial jurisdiction data | WHERE clause tries `Juris_Code` first, which may only match local roads | Reorder WHERE clauses to try `Physical_Juris_Name` first (captures ALL road types within jurisdiction) |
| State adapter detects wrong state | Detection signature columns overlap with another state | Make `required` columns more specific (add more unique columns) |
| Pipeline Stage 3 skips splitting | `download_mode` is `individual` not `statewide` | Only triggers when >3 jurisdictions. Single jurisdiction runs skip this stage. |
| Geocoding hits rate limit | Too many missing coordinates | Set `cache_config.geocode_ttl_days` higher, ensure GPS columns are populated |
| CSV endpoint returns JSON `"status":"Pending"` | ArcGIS Hub generates large CSV files asynchronously | `download_from_fallback()` polls every 30s for up to 3 minutes. If still pending, falls back to ArcGIS API. |
| ArcGIS API has <50k records for a statewide dataset | Endpoint is district-specific, not statewide | Use the CSV endpoint as primary source. The script warns when record count is suspiciously low. Verify the ArcGIS endpoint covers the whole state before relying on it. |
