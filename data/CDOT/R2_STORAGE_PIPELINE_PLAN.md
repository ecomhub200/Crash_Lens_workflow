# R2 Storage & Pipeline Architecture for Multi-Tier Views

## Context

The Crash Lens app supports 7 view tiers: **Federal > State > Region > MPO > County > City > Corridor**.
This plan covers how data flows from the CI/CD pipeline into R2 storage and back to the
frontend app for each tier. It is designed to be **reusable for any new state** added to the system.

**Current scale:**
- Virginia: **133 jurisdictions** (96 counties + 37 independent cities) — all configured in `config.json`
- Colorado: **64 counties** — all configured in `data/CDOT/source_manifest.json`

## Approach

1. **Bottom-up**: Aggregate existing county-level CSVs → form Region and MPO datasets
2. **Top-down**: Download a separate copy of the statewide dataset → save as gzip in R2
3. **County data**: 3 road-type CSVs per jurisdiction, all uploaded to R2
4. **Federal**: Cross-state aggregation of statewide aggregates
5. **Batch**: Download statewide data ONCE, split into ALL jurisdictions locally

**Key constraint**: Use the existing pipeline architecture. The statewide data download
already gets ALL jurisdictions in a single API call — we split locally, not per-jurisdiction.

---

## Multi-Jurisdiction Architecture (Download Once, Split All)

### The Key Insight

Both Virginia and Colorado download **all statewide data** in a single API call:

- **Virginia**: `download_from_fallback()` fetches the full statewide CSV from Virginia Roads
  portal (~200K+ records). Currently filters to 1 jurisdiction, discards the rest.
- **Colorado**: `download_cdot_crash_data.py` downloads per-year Excel files from CDOT OnBase.
  Each file contains **all 64 counties**. Currently filters to 1 county per run.

**The waste**: Running the pipeline 133 times for Virginia = 133 identical downloads of the
same statewide dataset, each keeping only ~1/133 of the data.

**The solution**: Download once → split locally into all jurisdictions → upload all to R2.

### Data Volume Estimates

| State | Statewide Records | Jurisdictions | Per-Jurisdiction Avg | 3 CSVs × All |
|-------|------------------|---------------|---------------------|---------------|
| Virginia | ~200K | 133 | ~1,500 records | 399 CSVs (~400 MB total) |
| Colorado | ~150K | 64 | ~2,300 records | 192 CSVs (~200 MB total) |

R2 storage is cheap ($0.015/GB/month). 600 MB total ≈ $0.01/month.

### Split Script (`scripts/split_jurisdictions.py`)

The script takes a statewide CSV and splits it into all jurisdictions with 3 road-type variants:

```bash
# Virginia: Split into all 133 jurisdictions
python scripts/split_jurisdictions.py \
  --state virginia \
  --input data/virginia_statewide_all_roads.csv \
  --output-dir data

# Colorado: Split into all 64 counties
python scripts/split_jurisdictions.py \
  --state colorado \
  --input data/CDOT/colorado_statewide_all_roads.csv \
  --output-dir data/CDOT

# Specific jurisdictions only
python scripts/split_jurisdictions.py \
  --state virginia \
  --input data/virginia_statewide_all_roads.csv \
  --jurisdictions henrico chesterfield fairfax_county

# List all jurisdictions
python scripts/split_jurisdictions.py --state virginia --list

# Dry run (report sizes without writing)
python scripts/split_jurisdictions.py --state virginia --input data/va.csv --dry-run

# Generate R2 upload manifest JSON
python scripts/split_jurisdictions.py --state virginia --r2-manifest --output-dir data
```

**Output per jurisdiction:**
```
data/henrico_county_roads.csv      # County/city roads only
data/henrico_no_interstate.csv     # All roads except interstate
data/henrico_all_roads.csv         # All roads including interstate
```

**Filtering logic:**
- Virginia: Uses `jurisCode`, `namePatterns`, and `fips` from config.json
- Colorado: Uses `County` column matched against CDOT source manifest

### Batch Workflow (`.github/workflows/batch-all-jurisdictions.yml`)

Manual-trigger workflow that processes ALL jurisdictions for a state:

```
Stage 1: Download statewide data (single API call)
Stage 2: Split into all jurisdictions (split_jurisdictions.py)
Stage 3: Validate all jurisdiction CSVs (batch)
Stage 4: Geocode all jurisdiction CSVs (batch)
Stage 5: Upload all CSVs to R2 (batched, 20 files per batch)
Stage 6: Generate forecasts for all jurisdictions (optional)
Stage 7: Generate region/MPO/federal aggregates
Stage 8: Commit metadata
```

**Inputs:**
| Input | Description | Default |
|-------|-------------|---------|
| `state` | virginia or colorado | required |
| `batch_size` | Files per R2 upload batch | 20 |
| `jurisdictions` | Specific jurisdictions (comma-separated) | all |
| `skip_validation` | Skip validation step | false |
| `skip_geocode` | Skip geocoding step | false |
| `skip_forecasts` | Skip forecast generation | true |
| `dry_run` | Report sizes only | false |

**Usage examples:**
```bash
# Full Virginia batch (133 jurisdictions × 3 = 399 CSVs)
gh workflow run batch-all-jurisdictions.yml -f state=virginia

# Full Colorado batch (64 counties × 3 = 192 CSVs)
gh workflow run batch-all-jurisdictions.yml -f state=colorado

# Just a few jurisdictions
gh workflow run batch-all-jurisdictions.yml -f state=virginia -f jurisdictions="henrico,chesterfield,richmond_city"

# Dry run (estimate sizes without uploading)
gh workflow run batch-all-jurisdictions.yml -f state=virginia -f dry_run=true
```

**R2 result after batch run:**
```
crash-lens-data/
  virginia/
    accomack/county_roads.csv, no_interstate.csv, all_roads.csv
    albemarle/county_roads.csv, no_interstate.csv, all_roads.csv
    ...
    henrico/county_roads.csv, no_interstate.csv, all_roads.csv
    ...
    winchester_city/county_roads.csv, no_interstate.csv, all_roads.csv
    (133 jurisdictions × 3 = 399 CSVs)

  colorado/
    adams/county_roads.csv, no_interstate.csv, all_roads.csv
    ...
    douglas/county_roads.csv, no_interstate.csv, all_roads.csv
    ...
    yuma/county_roads.csv, no_interstate.csv, all_roads.csv
    (64 counties × 3 = 192 CSVs)
```

### Single vs Batch Pipeline Comparison

| Aspect | Single Jurisdiction (current) | Batch All Jurisdictions (new) |
|--------|------------------------------|-------------------------------|
| **API calls** | 1 download per jurisdiction | 1 download total |
| **Network** | ~50 MB × 133 = 6.5 GB transferred | ~50 MB once |
| **Time** | ~5 min × 133 = 11 hours | ~20 min total |
| **R2 uploads** | 3 CSVs | 399 CSVs (batched) |
| **Trigger** | Scheduled monthly | Manual (initial + periodic refresh) |
| **Use case** | Update single jurisdiction | Populate all jurisdictions |

### When to Use Each Pipeline

| Scenario | Use |
|----------|-----|
| **Initial population** of a new state | `batch-all-jurisdictions.yml` |
| **Monthly refresh** of a single county | `download-data.yml` or `download-cdot-crash-data.yml` |
| **Full state refresh** (e.g., new year's data added) | `batch-all-jurisdictions.yml` |
| **Add a few new jurisdictions** | `batch-all-jurisdictions.yml` with `--jurisdictions` |

---

## Adding a New State — Checklist

To onboard a new state (e.g., Texas), follow these steps:

### 1. State Configuration Files

```
states/{state_key}/
  config.json           # Column mappings, EPDO weights, state metadata
  hierarchy.json        # Regions, MPOs, corridors, county memberships
```

**`config.json`** must include:
```json
{
  "state": { "abbreviation": "TX", "name": "Texas", "fips": "48" },
  "columnMapping": { ... },
  "epdoWeights": { "K": 883, "A": 94, "B": 21, "C": 11, "O": 1 }
}
```

**`hierarchy.json`** must include:
```json
{
  "state": { "abbreviation": "TX", "name": "Texas" },
  "regions": [ { "id": "region_1", "name": "...", "counties": ["county_a", ...] } ],
  "mpos": [ { "id": "mpo_1", "name": "...", "counties": ["county_a", ...] } ]
}
```

### 2. Jurisdiction Registry

Add all jurisdictions to `config.json` under `jurisdictions`:
```json
{
  "tx_harris": {
    "name": "Harris County",
    "type": "county",
    "fips": "201",
    "jurisCode": "...",
    "namePatterns": ["harris"],
    "mapCenter": [29.76, -95.36],
    "mapZoom": 10
  }
}
```

For states with separate manifest files (like Colorado), create:
```json
// data/TXDOT/source_manifest.json
{
  "jurisdiction_filters": {
    "harris": { "county": "HARRIS", "fips": "48201", "display_name": "Harris County" },
    ...
  }
}
```

### 3. State Adapter (if non-Virginia format)

If the state's raw CSV format differs from Virginia's, create a normalizer in
`scripts/state_adapter.py` (pattern: `ColoradoNormalizer`). The normalizer maps
the state's columns to the standard format used by the app.

### 4. Download Script

Create `download_{state}_crash_data.py` following the pattern of:
- `download_crash_data.py` (Virginia — ArcGIS/Virginia Roads API)
- `download_cdot_crash_data.py` (Colorado — Hyland OnBase)

Must support:
- `--save-statewide` or `--save-statewide-gzip` flag
- Calls `scripts/process_crash_data.py` with `--skip-split` for statewide processing
- Outputs `{state}_statewide_all_roads.csv.gz`

### 5. Add State to split_jurisdictions.py

Add the state to the split script's jurisdiction filtering:
- Add state-specific filter function (`filter_jurisdiction_{state}()`)
- Add state to `get_jurisdictions()` lookup
- Add default output directory mapping

### 6. GitHub Actions Workflows

**Per-jurisdiction workflow** (monthly updates):
Create `.github/workflows/download-{state}-crash-data.yml`

**Batch workflow** (initial population):
The generic `batch-all-jurisdictions.yml` already supports new states —
just add the state to the `choices` list in the workflow inputs.

### 7. Frontend Registration

In `config.json`, add the state to the `states` registry:
```json
{
  "texas": {
    "fips": "48",
    "name": "Texas",
    "abbreviation": "TX",
    "dotName": "TxDOT",
    "defaultJurisdiction": "harris",
    "dataDir": "TXDOT",
    "r2Prefix": "texas"
  }
}
```

### 8. Run Initial Batch

```bash
# Populate all jurisdictions for the new state
gh workflow run batch-all-jurisdictions.yml -f state=texas

# Verify
gh run list --workflow=batch-all-jurisdictions.yml
```

After the first batch run, the state will have:
```
crash-lens-data/
  texas/
    _state/
      dot_roads.csv, non_dot_roads.csv, statewide_all_roads.csv
      forecasts_dot_roads.json, forecasts_non_dot_roads.json, forecasts_statewide_all_roads.json
    _statewide/aggregates.json           # Bottom-up aggregates
    _statewide/county_summary.json
    _region/{id}/
      dot_roads.csv, non_dot_roads.csv, all_roads.csv
      forecasts_dot_roads.json, forecasts_non_dot_roads.json, forecasts_all_roads.json
      aggregates.json
    _mpo/{id}/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
      aggregates.json
    harris/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    dallas/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    ...  (×254 counties)
```

---

## Processing Requirements Per State

### Generic — Full Pipeline

States with non-Virginia CSV formats require the full pipeline:

| Stage | What It Does | Applied to Statewide? | Applied to Split CSVs? |
|-------|-------------|----------------------|----------------------|
| **MERGE** | Combine per-year/per-source CSVs into single file | Yes | No (already merged) |
| **CONVERT** | State normalizer transforms columns → standard format | **Yes — REQUIRED** | No (done on statewide) |
| **VALIDATE** | Quality checks, bounds validation, auto-correction | **Yes** | Optional per-jurisdiction |
| **GEOCODE** | Fill missing GPS coordinates | **Yes** (1800s timeout) | Optional per-jurisdiction |
| **SPLIT** | Split statewide → per-jurisdiction + road-type variants | N/A | **Yes — split_jurisdictions.py** |
| **GZIP** | Compress statewide for R2 storage | Yes | No (county CSVs are small) |

### Virginia — Standardize Only

Virginia data from `virginiaroads.org` is **already in the standard format**.
Only column renaming via `standardize_columns()` is needed before splitting.

---

## Frontend: R2 → App Data Flow

### How the App Resolves R2 URLs

`resolveDataUrl(localPath)` uses 3 strategies in order:

| Strategy | Pattern | Example |
|----------|---------|---------|
| **1. Manifest lookup** | Exact match in `r2-manifest.json` `localPathMapping` | `data/henrico_all_roads.csv` → `virginia/henrico/all_roads.csv` |
| **2. Dynamic construction** | `{r2Prefix}/{jurisdiction}/{filter}.{ext}` | `data/CDOT/douglas_all_roads.csv` → `colorado/douglas/all_roads.csv` |
| **3. Tier path passthrough** | Direct R2 key for `_state/`, `_statewide/`, `_region/`, `_mpo/`, `_federal/`, `_national/` paths | `colorado/_state/dot_roads.csv` → R2 URL directly |

**Strategy 2** is critical for multi-jurisdiction: it dynamically constructs R2 URLs
from the state's `r2Prefix` + jurisdiction ID + filename, so **any jurisdiction works
without being listed in the manifest**. As long as the CSV exists in R2, the frontend
will find it.

### How the App Loads Data Per Tier

| Tier | Data Source | Loading Mechanism |
|------|-------------|-------------------|
| **County** | `{state}/{jurisdiction}/{road_type}.csv` | `autoLoadCrashData()` → `getDataFilePath()` → `resolveDataUrl()` → `fetch()` → `Papa.parse()` |
| **State** | Aggregates: `{state}/_statewide/aggregates.json` | `AggregateLoader.loadStatewide(stateKey)` → `_fetch()` → JSON |
| **State** | CSV: `{state}/_state/{dot_roads\|non_dot_roads\|statewide_all_roads}.csv` | `autoLoadCrashData()` → `getDataFilePath()` → `resolveDataUrl()` Strategy 3 → `fetch()` → `Papa.parse()` |
| **Region** | Aggregates: `{state}/_region/{id}/aggregates.json` | `AggregateLoader.loadRegion(stateKey, regionId)` |
| **Region** | CSV: `{state}/_region/{id}/{road_type}.csv` | `autoLoadCrashData()` → `getDataFilePath()` → `resolveDataUrl()` Strategy 3 → `fetch()` |
| **MPO** | Aggregates: `{state}/_mpo/{id}/aggregates.json` | `AggregateLoader.loadMPO(stateKey, mpoId)` |
| **MPO** | CSV: `{state}/_mpo/{id}/{road_type}.csv` | `autoLoadCrashData()` → `getDataFilePath()` → `resolveDataUrl()` Strategy 3 → `fetch()` |
| **Federal** | Aggregates: `_national/aggregates.json` | `AggregateLoader.loadNational()` |
| **Federal** | CSV: `_national/{dot_roads\|non_dot_roads\|all_roads}.csv` | `autoLoadCrashData()` → `getDataFilePath()` → `resolveDataUrl()` Strategy 3 → `fetch()` |

### Jurisdiction Switching in the Frontend

When a user selects a different jurisdiction in the dropdown:
1. `getActiveJurisdictionId()` returns the new jurisdiction ID (e.g., `fairfax_county`)
2. `getDataFilePath()` builds `data/{stateDir}/fairfax_county_all_roads.csv`
3. `resolveDataUrl()` (Strategy 2) maps to `virginia/fairfax_county/all_roads.csv` in R2
4. `autoLoadCrashData()` fetches and parses the CSV
5. All tabs update with the new jurisdiction's data

**No frontend code changes needed** for multi-jurisdiction — the dynamic URL construction
already handles any jurisdiction that has data in R2.

### Gzip Transparent Decompression

Files uploaded to R2 with `Content-Encoding: gzip` header are **automatically decompressed
by the browser** when fetched via `fetch()`. No frontend decompression library (like pako) is needed.

### Key Frontend Functions

| Function | Purpose | File |
|----------|---------|------|
| `resolveDataUrl(localPath)` | Maps local/tier paths to R2 URLs (6 tier prefixes) | `app/index.html` |
| `getDataFilePath()` | Returns tier-aware R2 path (federal/state/region/mpo/county) | `app/index.html` |
| `getActiveRoadTypeSuffix(tier)` | Maps radio filter value to road type file suffix per tier | `app/index.html` |
| `updateRoadTypeLabels(tier)` | Dynamically updates road type radio labels per tier | `app/index.html` |
| `AggregateLoader.loadStatewideCSV(stateKey, roadType)` | Fetches + caches statewide CSV from R2 | `app/index.html` |
| `handleTierChange(tier)` | Orchestrates tier switch: labels, boundary, aggregates, CSV | `app/index.html` |
| `getPredictionForecastFile()` | Returns tier-aware forecast filename | `app/index.html` |

---

## Pipeline Stages Reference

### Single-Jurisdiction Pipeline (Monthly Updates)

#### Virginia (`download-data.yml`)

```
Stage 0:   Pre-flight (determine jurisdiction, health check)
Stage 1:   Download statewide CSV (filters to 1 jurisdiction)
Stage 1.5: Save statewide copy as gzip (standardize_columns only)
Stage 2:   Validate & Auto-Correct county data
Stage 3:   Geocode county data
Stage 4:   Upload 3 county CSVs to R2
Stage 4.5: Upload statewide gzip to R2
Stage 5:   Generate Forecasts
Stage 6:   Upload Forecasts to R2
Stage 6.5: Generate & upload region/MPO/federal aggregates
Stage 7:   Commit metadata to Git
```

#### Colorado (`download-cdot-crash-data.yml` → `process-cdot-data.yml`)

```
Stage 1:    Download per-year Excel from CDOT OnBase
Stage 1.5:  Assemble statewide: merge → CONVERT → VALIDATE → GEOCODE → gzip
Stage 2a:   Upload raw year CSVs to R2
Stage 2a.5: Upload statewide gzip to R2
Stage 2b:   Commit metadata
            ─── auto-triggers process-cdot-data.yml ───
Stage 3:    Process county data (Merge → Convert → Validate → Geocode → Split)
Stage 4:    Upload processed county CSVs to R2
Stage 5:    Generate Forecasts
Stage 6:    Upload Forecasts
Stage 6.5:  Generate & upload region/MPO/federal aggregates
Stage 7:    Commit metadata
```

### Batch Pipeline (Initial Population / Full Refresh)

#### `batch-all-jurisdictions.yml`

```
Stage 1: Download statewide data (SINGLE API call)
         VA: download_crash_data.py → Virginia Roads CSV (~200K records)
         CO: download_cdot_crash_data.py → OnBase Excel (~150K records)

Stage 2: Split into ALL jurisdictions (split_jurisdictions.py)
         VA: 133 jurisdictions × 3 road-type variants = 399 CSVs
         CO:  64 counties × 3 road-type variants      = 192 CSVs

Stage 3: Validate all jurisdiction CSVs (batch, non-fatal)

Stage 4: Geocode all jurisdiction CSVs (batch, 45 min timeout)

Stage 5: Upload ALL CSVs to R2
         VA: 399 files → virginia/{jurisdiction}/{road_type}.csv
         CO: 192 files → colorado/{jurisdiction}/{road_type}.csv

Stage 6: Generate forecasts (optional, 60 min timeout)

Stage 7: Generate aggregates
         - {state}/_statewide/aggregates.json (from all county CSVs)
         - {state}/_region/{id}/aggregates.json
         - {state}/_mpo/{id}/aggregates.json
         - _federal/aggregates.json

Stage 8: Commit metadata
```

---

## Uniform Naming Convention

### Road Type File Names Per View Level

| View Level | Road Type 1 | Road Type 2 | Road Type 3 |
|-----------|-------------|-------------|-------------|
| **Federal** | `dot_roads.csv` | `non_dot_roads.csv` | `all_roads.csv` |
| **State** | `dot_roads.csv` | `non_dot_roads.csv` | `statewide_all_roads.csv` |
| **Region** | `dot_roads.csv` | `non_dot_roads.csv` | `all_roads.csv` |
| **MPO** | `county_roads.csv` | `no_interstate.csv` | `all_roads.csv` |
| **County** | `county_roads.csv` | `no_interstate.csv` | `all_roads.csv` |

### Forecast File Names (same pattern with `forecasts_` prefix)

| View Level | Forecast 1 | Forecast 2 | Forecast 3 |
|-----------|-----------|-----------|-----------|
| **Federal** | `forecasts_dot_roads.json` | `forecasts_non_dot_roads.json` | `forecasts_all_roads.json` |
| **State** | `forecasts_dot_roads.json` | `forecasts_non_dot_roads.json` | `forecasts_statewide_all_roads.json` |
| **Region** | `forecasts_dot_roads.json` | `forecasts_non_dot_roads.json` | `forecasts_all_roads.json` |
| **MPO** | `forecasts_county_roads.json` | `forecasts_no_interstate.json` | `forecasts_all_roads.json` |
| **County** | `forecasts_county_roads.json` | `forecasts_no_interstate.json` | `forecasts_all_roads.json` |

### UI Label Mapping

| View Level | Radio 1 Label | Radio 2 Label | Radio 3 Label |
|-----------|---------------|---------------|---------------|
| **Federal/State/Region** | DOT Roads Only | Non-DOT Roads | All Roads / Statewide All Roads |
| **MPO/County** | County/City Roads Only | All Roads (No Interstate) | All Roads |

### R2 Path Patterns

| View Level | R2 Path Pattern |
|-----------|----------------|
| **Federal** | `_national/{road_type}.csv` |
| **State** | `{state}/_state/{road_type}.csv` |
| **Region** | `{state}/_region/{region_key}/{road_type}.csv` |
| **MPO** | `{state}/_mpo/{mpo_key}/{road_type}.csv` |
| **County** | `{state}/{county_key}/{road_type}.csv` |

---

## R2 Storage Layout (Full Scale)

```
crash-lens-data/
  _national/                                   # Cross-state (Federal tier)
    dot_roads.csv                              #   DOT-maintained roads nationwide
    non_dot_roads.csv                          #   Non-DOT roads nationwide
    all_roads.csv                              #   All roads combined
    forecasts_dot_roads.json                   #   Forecast: DOT roads
    forecasts_non_dot_roads.json               #   Forecast: Non-DOT roads
    forecasts_all_roads.json                   #   Forecast: All roads
    aggregates.json                            #   National totals
    state_summary.json                         #   Per-state ranking

  virginia/                                    # 133 jurisdictions
    _state/
      dot_roads.csv                            #   VDOT-maintained roads statewide
      non_dot_roads.csv                        #   Non-DOT roads statewide
      statewide_all_roads.csv                  #   All roads combined
      forecasts_dot_roads.json
      forecasts_non_dot_roads.json
      forecasts_statewide_all_roads.json
    _statewide/
      aggregates.json, county_summary.json, mpo_summary.json
    _region/{district_id}/
      dot_roads.csv, non_dot_roads.csv, all_roads.csv
      forecasts_dot_roads.json, forecasts_non_dot_roads.json, forecasts_all_roads.json
      aggregates.json, hotspots.json           #   9 VDOT districts
    _mpo/{mpo_id}/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
      aggregates.json, hotspots.json           #   8 MPOs
    accomack/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    ...
    henrico/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    ...
    winchester_city/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json

  colorado/                                    # 64 counties
    _state/
      dot_roads.csv                            #   CDOT-maintained roads statewide
      non_dot_roads.csv                        #   Non-CDOT roads statewide
      statewide_all_roads.csv                  #   All roads combined
      forecasts_dot_roads.json
      forecasts_non_dot_roads.json
      forecasts_statewide_all_roads.json
    _statewide/
      aggregates.json, county_summary.json, mpo_summary.json
    _region/{region_id}/
      dot_roads.csv, non_dot_roads.csv, all_roads.csv
      forecasts_dot_roads.json, forecasts_non_dot_roads.json, forecasts_all_roads.json
      aggregates.json, hotspots.json           #   5 engineering regions
    _mpo/{mpo_id}/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
      aggregates.json, hotspots.json           #   9 MPOs/TPRs
    adams/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    ...
    douglas/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
    ...
    yuma/
      county_roads.csv, no_interstate.csv, all_roads.csv
      forecasts_county_roads.json, forecasts_no_interstate.json, forecasts_all_roads.json
```

**Total R2 objects per state**: ~(jurisdictions × 6 files) + (regions × 8 files) + (MPOs × 8 files) + (state-level × 9 files)
**Virginia**: 133×6 + 9×8 + 8×8 = 798 + 72 + 64 + 9 = **943 objects**
**Colorado**: 64×6 + 5×8 + 9×8 = 384 + 40 + 72 + 9 = **505 objects**

---

## Data Flow Diagram

```
  SINGLE JURISDICTION (existing)              BATCH ALL JURISDICTIONS (new)
  ══════════════════════════════              ═══════════════════════════════

  download_{state}_crash_data.py              download_{state}_crash_data.py
  --jurisdiction {single county}              (same download, full statewide)
        │                                           │
        ▼                                           ▼
  Statewide CSV (downloaded)                  Statewide CSV (same data)
        │                                           │
        ├── filter to 1 jurisdiction          split_jurisdictions.py
        │                                     --state {state}
        ▼                                           │
  3 CSVs for 1 jurisdiction                   ┌─────▼───────────────────────┐
        │                                     │ Filter to ALL jurisdictions  │
        ▼                                     │ × 3 road-type variants      │
  Validate → Geocode                          └─────┬───────────────────────┘
        │                                           │
        ▼                                     VA: 133 × 3 = 399 CSVs
  R2: {state}/{jurisdiction}/                 CO:  64 × 3 = 192 CSVs
      county_roads.csv                              │
      no_interstate.csv                       Validate (batch) → Geocode (batch)
      all_roads.csv                                 │
                                                    ▼
                                              R2: {state}/{each jurisdiction}/
                                                  county_roads.csv
                                                  no_interstate.csv
                                                  all_roads.csv
                                                    │
                                                    ▼
                                              generate_aggregates.py
                                              (reads ALL county CSVs from R2)
                                                    │
                                              ┌─────┴──────────────────────┐
                                              │ _statewide/aggregates.json │
                                              │ _region/{id}/aggregates    │
                                              │ _mpo/{id}/aggregates       │
                                              │ _federal/aggregates        │
                                              └────────────────────────────┘
```

---

## Files Changed

| File | Change | Purpose |
|------|--------|---------|
| `scripts/split_jurisdictions.py` | **NEW** — Split statewide CSV into all jurisdictions | Multi-jurisdiction splitting |
| `.github/workflows/batch-all-jurisdictions.yml` | **NEW** — Batch workflow for all jurisdictions | Orchestration |
| `download_crash_data.py` | `--save-statewide` flag; keep uncompressed CSV for batch splitting | VA statewide save |
| `download_cdot_crash_data.py` | `--save-statewide-gzip` flag; keep uncompressed CSV for batch splitting | CO statewide save |
| `.github/actions/upload-r2/action.yml` | Gzip Content-Encoding support (existing) | R2 upload |
| `.github/workflows/download-data.yml` | Stage 4.5 + 6.5 (existing) | VA single-jurisdiction |
| `.github/workflows/download-cdot-crash-data.yml` | Stage 2a.5 (existing) | CO single-jurisdiction |
| `.github/workflows/process-cdot-data.yml` | Stage 6.5 (existing) | CO processing |
| `scripts/generate_aggregates.py` | `--federal` flag (existing) | Aggregate generation |
| `data/r2-manifest.json` | Version 3 with gzip metadata (existing) | R2 manifest |
| `app/index.html` | `AggregateLoader.loadStatewideCSV()`, `resolveDataUrl()` Strategy 3, `loadStatewideCSVForTier()`, tier-aware `getDataFilePath()` | Frontend |

---

## Bug Fixes Applied (Post-Review)

| # | Severity | Bug | Fix |
|---|----------|-----|-----|
| 1 | **CRITICAL** | `hashFiles()` evaluated at parse time — never matches runtime-generated gzip files. | Replaced with runtime `[ -f "$GZ" ]` check. |
| 2 | **CRITICAL** | `--output-dir data` causes aggregates to write wrong path. | Changed to `--output-dir data/$STATE`. |
| 3 | **CRITICAL** | Frontend had no code to fetch statewide gzip CSV from R2. | Added `AggregateLoader.loadStatewideCSV()`, `resolveDataUrl()` Strategy 3, `loadStatewideCSVForTier()`. |
| 4 | **CRITICAL** | `resolveDataUrl()` Strategy 2 split at first underscore — 23+ VA jurisdictions with underscores (fairfax_county, richmond_city, etc.) got wrong R2 keys. | Match known suffixes from end of filename first. |
| 5 | **CRITICAL** | Both download scripts deleted uncompressed statewide CSV after gzipping (`os.remove(statewide_path)`). Batch splitting workflow needs uncompressed CSV. | Removed `os.remove()` — keep both `.csv` and `.csv.gz`. |
| 6 | **MODERATE** | `uncompressed_size` never passed to upload-r2 action. | Added gzip footer read. |
| 7 | **MODERATE** | `_get_col_mapping()` hardcoded to VA/CO. | Rewrote to scan `states/` dynamically. |
| 8 | **USER-REQ** | Colorado statewide skipped geocoding. | Removed `--skip-geocode`; timeout 1800s. |

---

## Decisions

1. **Federal view**: Cross-state aggregation from statewide aggregate JSONs + 3 road-type CSVs + forecasts
2. **Region/MPO storage**: Aggregate JSONs + 3 road-type CSVs + 3 forecast JSONs per region/MPO
3. **Injection point**: Stage 1.5 (post-download, pre-validate) for statewide save
4. **Non-VA processing**: Full CONVERT + VALIDATE + GEOCODE pipeline (same as county)
5. **VA processing**: standardize_columns() only (already in standard format)
6. **Gzip serving**: `Content-Encoding: gzip` header on R2 → browser auto-decompresses
7. **Frontend tier-aware**: `getDataFilePath()` returns R2 tier paths for state view
8. **Multi-jurisdiction**: Download once, split locally — not 133 separate downloads
9. **Batch pipeline**: Separate workflow for initial population and full refresh
10. **Dynamic R2 URLs**: Strategy 2 in `resolveDataUrl()` handles any jurisdiction without manifest entries
