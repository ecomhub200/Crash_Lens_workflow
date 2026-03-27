# Delaware Crash Data Pipeline

## Overview

Delaware uses a dedicated two-workflow pipeline that adds a normalization step to transform raw crash data from data.delaware.gov into the CrashLens 69-column standard format before processing.

**Why a separate pipeline?** Delaware's raw data has non-standard column names, text-based severity descriptions (instead of KABCO codes), no crash IDs, and a unique datetime format. The normalization step handles all of this automatically.

## Architecture

```
delaware-batch-all-jurisdictions.yml
├── 1. Resolve Registry (download-registry.json)
├── 2. Download from data.delaware.gov (Socrata API)
├── 3. 🔄 NORMALIZE (de_normalize.py) ← Delaware-specific step
├── 4. Upload normalized CSV to R2
└── Trigger delaware-batch-pipeline.yml
    ├── Stage 0: Init Cache
    ├── Stage 1: Split by Jurisdiction (3 counties)
    ├── Stage 2: Split by Road Type (4 CSVs per county)
    ├── Stage 3: Aggregate by Scope (CSV)
    ├── Stage 4: Upload to R2
    ├── Stage 4.5: Headless Validation & Auto-Correct
    ├── Stage 5: Generate Forecasts
    ├── Stage 5b-5d: Upload Forecasts to R2
    └── Stage 6: Commit Manifest
```

## Files

| File | Purpose |
|------|---------|
| `states/delaware/config.json` | Column mappings, EPDO weights, data source config |
| `states/delaware/hierarchy.json` | Counties, regions (DOT Districts), MPOs, geography |
| `states/delaware/de_normalize.py` | Raw → CrashLens 69-column normalizer with FIPS, EPDO, ranking (Python) |
| `.github/workflows/delaware-batch-all-jurisdictions.yml` | Download + normalize + upload workflow |
| `.github/workflows/delaware-batch-pipeline.yml` | Processing pipeline (Stage 0-6) |

## Data Source

- **Portal:** data.delaware.gov (Socrata Open Data)
- **Dataset ID:** 827n-m6xc
- **API:** `https://data.delaware.gov/resource/827n-m6xc.json`
- **Format:** JSON/CSV via Socrata SODA API
- **Pagination:** offset-based, 50,000 records per request
- **Records:** ~566,000 (as of 2026)

## Geography

Delaware has 3 counties, 3 DOT districts, and 3 MPOs:

| County | FIPS | DOT District | MPO |
|--------|------|-------------|-----|
| Kent | 10001 | Central District | Dover/Kent County MPO |
| New Castle | 10003 | North District | WILMAPCO |
| Sussex | 10005 | South District | Salisbury-Wicomico MPO |

## Normalization Details

All normalization is handled by **`de_normalize.py`** (Python), which runs as Step 3 in `delaware-batch-all-jurisdictions.yml`. It performs column mapping, severity mapping, crash ID generation, datetime parsing, geography assignment (FIPS), EPDO scoring, and jurisdiction ranking in a single pass. The output is a fully normalized CrashLens 69-column CSV with enrichment and ranking columns.

The `de_normalize.py` script transforms raw Delaware data:

**Severity Mapping** (Delaware → KABCO):
- "Fatality Crash" / "Fatal Crash" → K
- "Personal Injury Crash" / "Injury Crash" → A (DE doesn't distinguish A/B/C; mapped to A per FHWA guidance)
- "Property Damage Only" → O
- "Non-Reportable" → O

**Crash ID Generation:**
Format: `DE-{YYYYMMDD}-{HHMM}-{NNNNNNN}` (e.g., `DE-20230715-1515-0000001`)

**Datetime Parsing:**
- Input: `2015 Jul 17 03:15:00 PM`
- Crash Date: `7/17/2015`
- Crash Military Time: `1515`
- Crash Year: `2015`

**Jurisdiction Names:**
County name only (e.g., `Kent`, `New Castle`, `Sussex`)

**Known Limitations:**
- No A/B/C injury severity distinction (all injuries mapped to A per FHWA guidance)
- Functional Class not in source data (empty — affects road-type filters)
- Ownership not in source data (empty — affects county/city road filters)
- No route/road name fields available
- No intersection/junction type data

## EPDO Weights

From `config.json` (source: FHWA-SA-25-021, Oct 2025, 2024 dollars):

| Severity | Weight |
|----------|--------|
| K (Fatal) | 883 |
| A (Serious Injury) | 94 |
| B (Minor Injury) | 21 |
| C (Possible Injury) | 11 |
| O (PDO) | 1 |

## R2 Storage Structure

```
crash-lens-data/
└── delaware/
    ├── _state/
    │   └── statewide_all_roads.csv.gz
    ├── statewide/
    │   └── delaware_statewide_all_roads.csv
    ├── kent/
    │   ├── all_roads.csv
    │   ├── county_roads.csv
    │   ├── city_roads.csv
    │   ├── no_interstate.csv
    │   └── forecasts_*.json
    ├── new_castle/
    │   └── (same structure)
    ├── sussex/
    │   └── (same structure)
    ├── _region/
    │   └── (aggregated by DOT District)
    └── _mpo/
        └── (aggregated by MPO)
```

## Running the Pipeline

### Full Pipeline (Download + Normalize + Process)
Go to **Actions → Delaware: Batch All Jurisdictions → Run workflow**

| Input | Default | Notes |
|-------|---------|-------|
| scope | statewide | Process all 3 counties |
| force_download | false | Force re-download from data.delaware.gov |
| skip_pipeline | false | Download + normalize only |
| skip_forecasts | false | Skip forecast generation |
| incremental | false | Skip if data unchanged |
| dry_run | false | No uploads or commits |

### Reprocess Without Re-downloading
Go to **Actions → Delaware: Batch Pipeline → Run workflow**

Provide the R2 key from the last download (e.g., `delaware/statewide/delaware_statewide_all_roads.csv`).

## Troubleshooting

**Normalization fails:** Check `states/delaware/de_normalize.py` — the Socrata API may have changed column names. Compare with `states/delaware/config.json` column mappings.

**Stage 0.5 validation warnings:** These are non-fatal. Common warnings include low coordinate coverage (some records lack lat/lon) and boundary checks (records outside Delaware's bounding box).

**No data from Socrata API:** Check if dataset 827n-m6xc is still active at data.delaware.gov. The API has a 50,000 record limit per request — the download script handles pagination.

**Functional Class empty:** This is a known limitation. Delaware's public crash data doesn't include road functional classification. To enable road-type filtering, you'd need to join with HPMS or OSM road data using coordinates.

## Admin Notes

- Only repository administrators can modify workflow files
- The original `batch-all-jurisdictions.yml` and `batch-pipeline.yml` are NOT modified
- Delaware-specific workflows are prefixed with `delaware-` for clear identification
- The normalization script (`de_normalize.py`) is idempotent — re-running on already-normalized data is safe
- Progress is tracked in `data/batch-progress-manifest.json` with `pipeline: "delaware-batch-all-jurisdictions"` tag
