# Crash Lens — Universal Data Pipeline: Download to R2 Storage

**Version:** 3.1
**Last Updated:** March 13, 2026
**Purpose:** Single reference document for building a complete state data pipeline from raw crash data download through Cloudflare R2 storage upload. Follow this document step-by-step when onboarding any new state.

**Working Implementations:** Colorado (CDOT), Virginia (VDOT)

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Prerequisites — What You Need Before Starting](#2-prerequisites)
3. [Folder Structure — Files to Create Per State](#3-folder-structure)
4. [Stage 0: Download Raw Data](#4-stage-0-download)
5. [Stage 1: Merge (Optional)](#5-stage-1-merge)
6. [Stage 2: Convert / Normalize](#6-stage-2-convert)
7. [Stage 3: Split by Jurisdiction](#7-stage-3-split-jurisdiction)
8. [Stage 4: Split by Road Type](#8-stage-4-split-road-type)
9. [Stage 5: Aggregate by Scope (CSV)](#9-stage-5-aggregate)
10. [Stage 6: Upload to R2 Storage](#10-stage-6-upload-to-r2)
11. [Stage 7: Predict (Forecast Generation)](#11-stage-7-predict)
12. [Stage 8: Manifest Update & Git Commit](#12-stage-8-manifest-update)
13. [Cloudflare Workers — Server-Side Validation & Geocoding](#13-cloudflare-workers)
14. [GitHub Actions Workflow (pipeline.yml v6)](#14-github-actions-workflow)
15. [Front-End Connection — How the Browser Loads Data](#15-front-end-connection)
16. [State Onboarding Checklist](#16-state-onboarding-checklist)
17. [Appendix A: Standardized Column Format](#appendix-a)
18. [Appendix B: API Types Across States](#appendix-b)
19. [Appendix C: R2 Bucket Structure](#appendix-c)
20. [Appendix D: Reference Implementations](#appendix-d)

---

## 1. Pipeline Overview

Every state follows a **hybrid pipeline**. GitHub Actions handles data processing (download, convert, split, aggregate, upload, predict, manifest), while **Cloudflare Workers handle validation and geocoding server-side** after CSVs are uploaded to R2. This eliminates redundant CI/CD compute and enables real-time re-validation.

```
┌──────────────────────────────────────────────────────────────────────┐
│                     CRASH LENS DATA PIPELINE v3                      │
│             (Hybrid: GitHub Actions + Cloudflare Workers)            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─── GITHUB ACTIONS (CI/CD) ─────────────────────────────────────┐ │
│  │                                                                 │ │
│  │  Stage 0: DOWNLOAD                                              │ │
│  │  ├─ Fetch raw crash data from state DOT source                  │ │
│  │  ├─ API type: Socrata, ArcGIS, REST, Bulk Portal, etc.         │ │
│  │  └─ Output: raw CSV/Excel in data/{DOT_NAME}/                  │ │
│  │                          ↓                                      │ │
│  │  Stage 1: MERGE (optional)                                      │ │
│  │  ├─ Combine multiple yearly or partial files                    │ │
│  │  └─ Output: single merged CSV per jurisdiction                  │ │
│  │                          ↓                                      │ │
│  │  Stage 2: CONVERT / NORMALIZE                                   │ │
│  │  ├─ Auto-detect state format from CSV headers                   │ │
│  │  ├─ Map raw columns → standardized format                       │ │
│  │  └─ Output: {jurisdiction}_standardized.csv                     │ │
│  │                          ↓                                      │ │
│  │  Stage 3: SPLIT BY JURISDICTION (statewide mode only)           │ │
│  │  └─ Output: per-county CSVs                                     │ │
│  │                          ↓                                      │ │
│  │  Stage 4: SPLIT BY ROAD TYPE                                    │ │
│  │  ├─ county_roads.csv / no_interstate.csv / all_roads.csv        │ │
│  │  └─ Output: 3 filtered CSVs per jurisdiction                    │ │
│  │                          ↓                                      │ │
│  │  Stage 5: AGGREGATE BY SCOPE                                    │ │
│  │  └─ Output: region/MPO/federal aggregate CSVs                   │ │
│  │                          ↓                                      │ │
│  │  Stage 6: UPLOAD TO R2                                          │ │
│  │  ├─ Upload CSVs + JSONs to Cloudflare R2 bucket                 │ │
│  │  └─ Path: crash-lens-data/{state}/{jurisdiction}/               │ │
│  │                          ↓                                      │ │
│  │  Stage 7: PREDICT (forecast generation)                         │ │
│  │  ├─ SageMaker Chronos-2 time-series prediction                  │ │
│  │  └─ Output: 3 forecast JSON files per jurisdiction              │ │
│  │                          ↓                                      │ │
│  │  Stage 8: MANIFEST UPDATE                                       │ │
│  │  ├─ Update data/r2-manifest.json with file metadata             │ │
│  │  └─ Commit manifest to git                                      │ │
│  │                                                                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                          ↓                                           │
│  ┌─── CLOUDFLARE WORKERS (Server-Side, Post-Upload) ──────────────┐ │
│  │                                                                 │ │
│  │  AUTO-VALIDATE (crash-validator-worker)                          │ │
│  │  ├─ Triggered automatically when CSV uploaded to R2              │ │
│  │  ├─ R2 upload worker detects main CSV → service binding          │ │
│  │  ├─ Coordinate bounds check, KABCO validation                    │ │
│  │  ├─ Auto-correction: route-median snap, FC/FT inference          │ │
│  │  ├─ Corrections ledger (persistent across runs)                  │ │
│  │  └─ Writes corrected CSV back to R2 (via R2 binding)            │ │
│  │                                                                 │ │
│  │  AUTO-GEOCODE (Cloudflare worker pipeline)                       │ │
│  │  ├─ Fill missing GPS via geocoding APIs                          │ │
│  │  └─ Runs post-validation on Cloudflare edge                     │ │
│  │                                                                 │ │
│  │  CRON SAFETY NET (weekly Monday 6 AM UTC)                        │ │
│  │  └─ Re-validates stale CSVs if report older than data            │ │
│  │                                                                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Why the Hybrid Approach?

| Concern | GitHub Actions (before) | Cloudflare Workers (now) |
|---------|------------------------|--------------------------|
| **Validation compute** | 5-10 min per run, consumes CI/CD minutes | Runs on Cloudflare edge, free on paid plan |
| **Geocoding compute** | 15-60 min for Nominatim rate limits | Runs async on Cloudflare, no CI timeout |
| **Re-validation** | Must re-run full pipeline | Auto-triggers on any CSV upload |
| **Weekly safety net** | None | Cron checks for stale data every Monday |
| **Corrections ledger** | Lost between runs | Persistent on R2, cumulative fixes |

---

## 2. Prerequisites

Before building a pipeline for a new state, you need:

### 2.1 Infrastructure (Already Exists)

These are shared across all states — do NOT recreate:

| Resource | Value | Notes |
|----------|-------|-------|
| R2 Bucket | `crash-lens-data` | Cloudflare R2 storage |
| R2 Public URL | `https://data.aicreatesai.com` | CDN endpoint |
| R2 CORS Policy | `AllowedOrigins: [*], Methods: [GET, HEAD]` | Already configured |
| SageMaker Endpoint | `crashlens-chronos2-endpoint` | Chronos-2 forecasting |
| Manifest File | `data/r2-manifest.json` (version 3) | Shared across all states |
| State Adapter | `scripts/state_adapter.py` | Multi-state detection engine |
| Main Orchestrator | `scripts/process_crash_data.py` | Runs processing stages |
| R2 Uploader | `scripts/upload-to-r2.py` | Handles R2 upload + manifest |
| R2 Upload Worker | `r2-upload-worker/src/index.js` | Cloudflare Worker: routes uploads, triggers validation |
| Crash Validator Worker | `data-pipeline/crash-validator-worker/crash-validator-worker.js` | Cloudflare Worker: server-side validation + auto-correction |

### 2.2 Credentials (GitHub Secrets)

These must be configured in the repo's GitHub Actions secrets:

| Secret | Purpose |
|--------|---------|
| `CF_ACCOUNT_ID` | Cloudflare account ID |
| `CF_R2_ACCESS_KEY_ID` | R2 API access key |
| `CF_R2_SECRET_ACCESS_KEY` | R2 API secret key |
| `AWS_ACCESS_KEY_ID` | SageMaker access (for forecasting) |
| `AWS_SECRET_ACCESS_KEY` | SageMaker secret (for forecasting) |

### 2.3 Information to Gather for the New State

Before writing any code, research and collect:

| Item | Example (Colorado) | Where to Find |
|------|-------------------|---------------|
| State name & abbreviation | Colorado / CO | — |
| State FIPS code | 08 | Census Bureau |
| DOT name/abbreviation | CDOT | State DOT website |
| Data source URL | `https://oitco.hylandcloud.com/CDOTRMPop/...` | State DOT open data |
| API type | OnBase Portal (custom) | See Appendix B |
| Data format | Excel (.xlsx) | Download a sample |
| Coordinate bounds | lat [36.9, 41.1], lon [-109.1, -101.9] | State boundary |
| Column names in raw data | CUID, Crash Date, Injury 04, etc. | Download sample, inspect headers |
| Severity field | Derived from Injury 00-04 | Check if direct or derived |
| Road system classification | Interstate Highway, State Highway, County Road, City Street | Raw data values |
| County/jurisdiction list | 64 counties with FIPS codes | Census Bureau |
| EPDO weights | K=883, A=94, B=21, C=11, O=1 (FHWA 2025) | State DOT or use FHWA 2025 |
| Target jurisdictions | Douglas, Arapahoe, Jefferson, etc. | Start with 1-3, expand later |

---

## 3. Folder Structure

For each new state, create these files. Use the Colorado (`data/CDOT/`) or Virginia structure as templates.

### 3.1 Files to Create

```
project-root/
├── data/{DOT_NAME}/                        ← State data folder
│   ├── CLAUDE.md                           ← State overview + pipeline instructions
│   ├── config.json                         ← Column mappings, road systems, EPDO
│   ├── source_manifest.json                ← Data source IDs/URLs per county
│   ├── jurisdictions.json                  ← County definitions (bbox, cities, routes)
│   ├── enhancements.json                   ← Deep Dive panel mappings (optional)
│   └── .validation/                        ← Created automatically by validation stage
│       └── pipeline_report.json
│
├── states/{state_key}/                     ← State adapter config
│   ├── config.json                         ← Column mapping for state_adapter.py
│   └── hierarchy.json                      ← Regions, MPOs, counties hierarchy
│
├── download_{state}_crash_data.py          ← Download script (at project root)
│
├── states/
│   └── download-registry.json              ← State download registry (all states)
│
└── .github/workflows/
    ├── download-state-crash-data.yml       ← Batch Download for Any State (registry-driven)
    └── download-{state}-crash-data.yml     ← Per-state single-jurisdiction workflows
```

### 3.2 Config.json Structure (data/{DOT_NAME}/config.json)

This is the heart of the state configuration. Template:

```json
{
  "state": "{state_name}",
  "dotName": "{DOT_NAME}",
  "fips": "{state_fips}",
  "coordinateBounds": {
    "latMin": 0.0, "latMax": 0.0,
    "lonMin": 0.0, "lonMax": 0.0
  },
  "columnMapping": {
    "ID": "{raw_id_column}",
    "DATE": "{raw_date_column}",
    "TIME": "{raw_time_column}",
    "SEVERITY": "{raw_severity_column_or_DERIVED}",
    "LAT": "{raw_lat_column}",
    "LON": "{raw_lon_column}",
    "ROUTE": "{raw_route_column_or_DERIVED}",
    "ROAD_SYSTEM": "{raw_system_column}",
    "COLLISION_TYPE": "{raw_collision_column}",
    "WEATHER": "{raw_weather_column}",
    "LIGHT": "{raw_light_column}",
    "JUNCTION": "{raw_junction_column}",
    "JURISDICTION": "{raw_county_column}"
  },
  "derivedFields": {
    "SEVERITY": {
      "method": "injury_hierarchy | direct",
      "columns": ["fatal_col", "serious_col", "minor_col", "possible_col", "pdo_col"],
      "labels": ["K", "A", "B", "C", "O"]
    }
  },
  "crashTypeMapping": {
    "{state_value}": "{vdot_numbered_value}"
  },
  "weatherMapping": {
    "{state_value}": "{vdot_numbered_value}"
  },
  "lightMapping": {
    "{state_value}": "{vdot_numbered_value}"
  },
  "roadSystems": {
    "splitConfig": {
      "countyRoads": {
        "method": "system_column | agency_id",
        "column": "{column_name}",
        "includeValues": ["list", "of", "local", "road", "types"]
      },
      "interstateExclusion": {
        "method": "column_value",
        "column": "{column_name}",
        "excludeValues": ["Interstate"]
      }
    }
  },
  "epdoWeights": {
    "K": 883, "A": 94, "B": 21, "C": 11, "O": 1
  },
  "booleanFlags": {
    "PED": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "BIKE": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "ALCOHOL": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "SPEED": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "HITRUN": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "MOTORCYCLE": {"column": "{col}", "trueValues": ["Y", "Yes", "1", "TRUE"]},
    "NIGHT": {"derivedFrom": "LIGHT", "trueValues": ["Dark"]},
    "DISTRACTED": {"column": "{col}", "trueValues": ["Y"]},
    "DROWSY": {"column": "{col}", "trueValues": ["Y"]},
    "DRUG": {"column": "{col}", "trueValues": ["Y"]},
    "YOUNG": {"derivedFrom": "AGE", "range": [16, 25]},
    "SENIOR": {"derivedFrom": "AGE", "range": [65, 999]},
    "UNRESTRAINED": {"column": "{col}", "trueValues": ["Y"]}
  }
}
```

### 3.3 State Adapter Signature (states/{state_key}/config.json)

Register the new state in the state adapter detection system. Add a signature to `scripts/state_adapter.py`:

```python
'{state_key}': {
    'required': ['col1', 'col2', 'col3'],   # 3-4 unique header columns
    'display_name': '{State Name} ({DOT_NAME})',
    'config_dir': '{state_key}'
}
```

The `required` columns must be unique to this state — columns that no other state's CSV would have.

### 3.4 Main Config Registration (config.json at project root)

Add the state entry under `states`:

```json
"{state_key}": {
  "fips": "{fips}",
  "name": "{State Name}",
  "abbreviation": "{XX}",
  "dotName": "{DOT_NAME}",
  "defaultJurisdiction": "{default_county}",
  "dataDir": "{DOT_NAME}",
  "r2Prefix": "{state_key}",
  "appSubtitle": "{State Name} Crash Analysis Tool"
}
```

The `r2Prefix` MUST match the folder name used in R2 storage (lowercase state name).

---

## 4. Stage 0: Download Raw Data

### 4.1 Purpose

Fetch raw crash data from the state DOT's data portal and save it locally as CSV.

### 4.2 API Types (Choose One)

| API Type | States Using It | Python Library | Pagination |
|----------|----------------|---------------|------------|
| **Socrata SODA** | MD, CT, DE, NY, NYC, HI | `requests` (REST) | `$offset` + `$limit` |
| **ArcGIS GeoServices** | IA, IL, FL, OR, PA, MA, OH, WI, AK, NV, UT, ID, WA, GA, SC, AR, MT, MS, OK, LA, WV | `requests` (REST) | `resultOffset` + `resultRecordCount` |
| **Custom REST** | VT | `requests` | Varies |
| **Bulk Download Portal** | TX | `requests` (direct file download) | N/A (full file) |
| **OnBase Document Portal** | CO | `requests` + browser headers | N/A (document by ID) |

### 4.3 Download Script Template

Create `download_{state}_crash_data.py` at the project root. Every download script must:

1. Accept `--jurisdiction` to filter by county
2. Accept `--list` to show available jurisdictions
3. Accept `--years` to specify year range
4. Implement retry logic with exponential backoff (at least 3 retries)
5. Use persistent sessions with connection pooling
6. Validate downloaded file size (minimum 100 KB)
7. Save output to `data/{DOT_NAME}/`
8. Print download statistics (rows, size, duration)

**Minimal structure:**

```python
#!/usr/bin/env python3
"""Download {STATE_NAME} crash data from {SOURCE_NAME}."""

import argparse, requests, time, os, sys
from pathlib import Path

# ── Configuration ──
STATE_KEY = '{state_key}'
DOT_NAME = '{DOT_NAME}'
API_BASE_URL = '{api_url}'
DATA_DIR = Path(f'data/{DOT_NAME}')
MIN_VALID_FILE_SIZE = 100 * 1024  # 100 KB

def download_data(jurisdiction, years, output_dir):
    """Download crash data with pagination and retry."""
    session = requests.Session()
    session.headers.update({'User-Agent': 'CrashLens-Pipeline/1.0'})

    # ... API-specific pagination loop ...
    # ... Retry logic with exponential backoff ...
    # ... County/jurisdiction filtering ...
    # ... Save to CSV ...

    return output_path

def list_jurisdictions():
    """Print available jurisdictions from source_manifest.json."""
    manifest = json.load(open(DATA_DIR / 'source_manifest.json'))
    for jur_id, info in manifest['jurisdictions'].items():
        print(f"  {jur_id}: {info['name']} (FIPS: {info['fips']})")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=f'Download {DOT_NAME} crash data')
    parser.add_argument('--jurisdiction', '-j', default=None)
    parser.add_argument('--years', nargs='+', type=int, default=None)
    parser.add_argument('--list', action='store_true', help='List jurisdictions')
    parser.add_argument('--output', '-o', default=str(DATA_DIR))
    args = parser.parse_args()

    if args.list:
        list_jurisdictions()
    else:
        download_data(args.jurisdiction, args.years, args.output)
```

### 4.4 Reference Implementations

| State | Script | API Type | Key Differences |
|-------|--------|----------|----------------|
| Virginia | `download_crash_data.py` | ArcGIS FeatureServer | 2,000 records/page, jurisdiction filter via county FIPS |
| Colorado | `download_cdot_crash_data.py` | OnBase Portal | Document ID-based, Excel format, statewide then filter |

---

## 5. Stage 1: Merge (Optional)

### 5.1 When to Use

Only needed when the state provides data as multiple files (e.g., one Excel file per year, like Colorado). Skip if the API returns a single combined dataset (like Virginia).

### 5.2 What It Does

1. Read multiple CSV/Excel files matching a glob pattern
2. Concatenate all rows into a single DataFrame
3. Remove exact duplicate rows
4. Save as a single merged CSV

### 5.3 Command

```bash
python scripts/process_crash_data.py \
  --input 'data/{DOT_NAME}/2021*.csv' 'data/{DOT_NAME}/2022*.csv' \
  --jurisdiction {jurisdiction} \
  --merge
```

### 5.4 Output

`data/{DOT_NAME}/{jurisdiction}_merged.csv` — single file containing all years.

---

## 6. Stage 2: Convert / Normalize

### 6.1 Purpose

Transform state-specific raw CSV columns into the standardized Virginia-compatible format that the front-end expects. This is the most state-specific stage.

### 6.2 How Auto-Detection Works

The state adapter (`scripts/state_adapter.py`) examines the CSV header row and matches against registered state signatures:

```python
# Detection flow:
headers = csv_file.columns.tolist()
for state_key, signature in STATE_SIGNATURES.items():
    if all(col in headers for col in signature['required']):
        return state_key  # Found match
```

### 6.3 What Gets Normalized

| Category | Action | Example |
|----------|--------|---------|
| **Column Renaming** | Raw column → standardized name | `CUID` → `Document Nbr` |
| **Severity** | Direct passthrough OR derived from injury counts | CO: `Injury 04 > 0` → `K` |
| **Route Name** | Direct passthrough OR derived from components | CO: `System Code + Rd_Number` → `RTE Name` |
| **Value Mapping** | State values → VDOT numbered vocabulary | `Clear` → `1. No Adverse Condition` |
| **Boolean Flags** | Derive 13 Yes/No flags from raw fields | See flag list below |
| **State Detail Columns** | Preserve originals with `_{xx}_` prefix | `Speed Limit` → `_co_tu1_speed_limit` |
| **Coordinates** | Standardize to `x` (longitude) and `y` (latitude) | Various column names → `x`, `y` |

### 6.4 The 13 Boolean Flags

Every state must derive these flags (Yes/No):

| Flag | Meaning | Common Raw Sources |
|------|---------|-------------------|
| `Pedestrian?` | Pedestrian involved | Pedestrian count, crash type contains "ped" |
| `Bike?` | Bicyclist involved | Bicycle count, crash type contains "bik" |
| `Motorcycle?` | Motorcycle involved | Vehicle type column |
| `Alcohol?` | Alcohol involvement | DUI flag, alcohol-related column |
| `Speed?` | Speed-related | Speed-related flag |
| `Hitrun?` | Hit and run | Hit-run flag |
| `Distracted?` | Distracted driving | Driver action column |
| `Drowsy?` | Drowsy driving | Driver action or factor column |
| `Drug Related?` | Drug involvement | Drug-related flag |
| `Young?` | Driver age 16-25 | Driver age column |
| `Senior?` | Driver age 65+ | Driver age column |
| `Unrestrained?` | Seatbelt not used | Restraint column |
| `Night?` | Dark conditions | Derived from Light Condition |

### 6.5 Severity Derivation Methods

**Method 1: Direct Passthrough** (Virginia, most states)
The raw data has a single severity column with K/A/B/C/O values.

**Method 2: Injury Hierarchy** (Colorado, some states)
No single severity column. Derive from injury count columns:
```
If Fatal_Count > 0        → K
Elif Serious_Inj_Count > 0 → A
Elif Minor_Inj_Count > 0   → B
Elif Possible_Inj_Count > 0 → C
Else                        → O (Property Damage Only)
```

Configure the method in `data/{DOT_NAME}/config.json` → `derivedFields.SEVERITY`.

### 6.6 Command

```bash
python scripts/process_crash_data.py \
  --input data/{DOT_NAME}/{jurisdiction}_merged.csv \
  --state {state_key} \
  --jurisdiction {jurisdiction} \
  --convert-only   # Run only Stage 2
```

### 6.7 Output

`data/{DOT_NAME}/{jurisdiction}_standardized.csv` — standardized column format.

---

## 7. Stage 3: Split by Jurisdiction

### 7.1 Purpose

Split a statewide standardized CSV into per-county files. Only needed in statewide processing mode (when you download the entire state at once). Skipped for single-jurisdiction runs.

### 7.2 Command

```bash
python scripts/split_jurisdictions.py \
  --state {state_key} \
  --input data/{DOT_NAME}/{state}_statewide_standardized.csv \
  --output-dir data/{DOT_NAME}/
```

### 7.3 Output

Per-county CSVs in `data/{DOT_NAME}/`: one file per jurisdiction containing only that county's crashes.

---

## 8. Stage 4: Split by Road Type

### 8.1 Purpose

Create 3 filtered CSV files from the standardized data. Each corresponds to a radio button in the front-end UI.

### 8.2 Three Output Files

| File | UI Label | What's Included | What's Excluded |
|------|----------|----------------|-----------------|
| `county_roads.csv` | "County Roads Only" | Local/county-maintained roads | State highways, interstates |
| `no_interstate.csv` | "No Interstate" | Everything except interstates | Interstate highways |
| `all_roads.csv` | "All Roads" | Complete dataset | Nothing |

### 8.3 Filtering Methods

The filter logic varies by state — configure in `data/{DOT_NAME}/config.json` → `roadSystems.splitConfig`:

**Method A: System Column (Virginia)**
```json
"countyRoads": {
  "method": "system_column",
  "column": "SYSTEM",
  "includeValues": ["Non-DOT secondary", "Non-DOT", "Non-DOT"]
}
```

**Method B: Agency ID (Colorado)**
```json
"countyRoads": {
  "method": "agency_id",
  "column": "_co_agency_id",
  "agencyMap": {
    "douglas": ["DSO"],
    "arapahoe": ["ASO"]
  }
}
```

**Method C: Road Class (Generic)**
```json
"countyRoads": {
  "method": "column_value",
  "column": "Functional_Class",
  "includeValues": ["Local", "Minor Collector", "Major Collector"]
}
```

### 8.4 Command

```bash
python scripts/split_road_type.py \
  --state {state_key} \
  --jurisdictions {jurisdiction} \
  --data-dir data/{DOT_NAME}/
```

### 8.5 Output

Three files per jurisdiction in `data/{DOT_NAME}/`:
- `{jurisdiction}_county_roads.csv`
- `{jurisdiction}_no_interstate.csv`
- `{jurisdiction}_all_roads.csv`

---

## 9. Stage 5: Aggregate by Scope (CSV)

### 9.1 Purpose

Generate region-level, MPO-level, and federal cross-state aggregate CSVs based on the processing scope.

### 9.2 Command

```bash
python scripts/aggregate_by_scope.py \
  --state {state_key} \
  --scope {jurisdiction|region|mpo|statewide} \
  --selection {name} \
  --data-dir data/{DOT_NAME}/ \
  --output-format csv \
  --output-dir data/{DOT_NAME}/
```

### 9.3 Output

Aggregate CSVs in `data/{DOT_NAME}/_region/`, `data/{DOT_NAME}/_mpo/`, and `data/_federal/` directories.

---

## 10. Stage 6: Upload to R2 Storage

### 10.1 Purpose

Upload all processed CSVs and forecast JSONs to Cloudflare R2 cloud storage so the browser can load them. **When main CSV files are uploaded, the R2 upload worker automatically triggers server-side validation via Cloudflare Workers** (see Section 13).

### 10.2 R2 Bucket Path Convention

Every state follows this exact folder structure inside the `crash-lens-data` bucket:

```
crash-lens-data/
  {state_key}/                          ← e.g., colorado/
    _statewide/                         ← State rollup aggregates
      aggregates.json
      county_summary.json
      mpo_summary.json
    _state/                             ← State-tier road data
      dot_roads.csv
      non_dot_roads.csv
      statewide_all_roads.csv
    _region/{regionId}/                 ← Region-tier data
      aggregates.json
      dot_roads.csv
      all_roads.csv
    _mpo/{mpoId}/                       ← MPO-tier data
      aggregates.json
      dot_roads.csv
      all_roads.csv
    {jurisdiction}/                     ← e.g., douglas/
      county_roads.csv                  ← Road type 1
      no_interstate.csv                 ← Road type 2
      all_roads.csv                     ← Road type 3
      standardized.csv                  ← Full standardized dataset
      forecasts_county_roads.json
      forecasts_no_interstate.json
      forecasts_all_roads.json
      corrections_ledger_*.json         ← Auto-generated by validator worker
      validation_report_*.json          ← Auto-generated by validator worker
      backups/                          ← Auto-generated by R2 upload worker
```

### 10.3 Upload File List Per Jurisdiction

| File | Source Stage | Required? |
|------|-------------|-----------|
| `county_roads.csv` | Stage 4 (Split Road Type) | Yes |
| `no_interstate.csv` | Stage 4 (Split Road Type) | Yes |
| `all_roads.csv` | Stage 4 (Split Road Type) | Yes |
| `standardized.csv` | Stage 2 (Convert) | Optional |
| `forecasts_county_roads.json` | Stage 7 (Predict) | If forecasting enabled |
| `forecasts_no_interstate.json` | Stage 7 (Predict) | If forecasting enabled |
| `forecasts_all_roads.json` | Stage 7 (Predict) | If forecasting enabled |

### 10.4 What Happens After Upload (Automatic)

When a main CSV (`all_roads.csv`, `county_roads.csv`, or `no_interstate.csv`) is uploaded to R2:

1. The **R2 Upload Worker** (`r2-upload-worker/src/index.js`) receives the PUT request
2. It detects the file matches the pattern `{state}/{county}/(all_roads|county_roads|no_interstate).csv`
3. It creates a backup of the existing file (full CSV or metadata backup, auto-cleaned after 90 days)
4. It writes the new file to R2
5. If no `X-Skip-Validation` header is present, it **triggers the crash-validator-worker** via service binding
6. The validator reads, validates, auto-corrects, and writes back the corrected CSV (see Section 13)

### 10.5 Command

```bash
# Upload specific jurisdiction:
python scripts/upload-to-r2.py --state {state_key} --jurisdiction {jurisdiction}

# Dry run (preview only):
python scripts/upload-to-r2.py --dry-run

# With explicit credentials:
CF_ACCOUNT_ID=xxx CF_R2_ACCESS_KEY_ID=xxx CF_R2_SECRET_ACCESS_KEY=xxx \
  python scripts/upload-to-r2.py --state {state_key} --jurisdiction {jurisdiction}
```

---

## 11. Stage 7: Predict (Forecast Generation)

### 11.1 Purpose

Generate 12-month crash forecasts using Amazon SageMaker Chronos-2, a foundation model for time-series prediction.

### 11.2 Six Prediction Matrices

| Matrix | Description | Input |
|--------|-------------|-------|
| **M01** | Total crash frequency (monthly) | All crashes grouped by month |
| **M02** | Severity-level multivariate (K/A/B/C/O by month) | Crashes by severity × month |
| **M03** | Corridor cross-learning (top 10 routes) | Top routes by crash volume |
| **M04** | Crash type distribution | Crash types by month |
| **M05** | Contributing factor trends | Speed, alcohol, ped, bike by month |
| **M06** | Intersection vs. segment | Location type by month |

### 11.3 Temporal Embedding

**For high-count series (≥10 crashes/month, 2+ years history):**
- Seasonal decomposition (additive, 12-month cycle)
- Removes cyclical pattern; model focuses on trend + irregular

**For low-count series (<10 crashes/month):**
- Log1p variance stabilization
- Compresses range for proportional treatment of rare events (e.g., fatalities)

### 11.4 Output Format

```json
{
  "jurisdiction": "douglas",
  "state": "colorado",
  "roadType": "county_roads",
  "generatedAt": "2026-02-07T04:32:35Z",
  "horizon": 12,
  "quantiles": [0.1, 0.25, 0.5, 0.75, 0.9],
  "matrices": {
    "M01_total": { "monthly": [...] },
    "M02_severity": { "K": [...], "A": [...], "B": [...], "C": [...], "O": [...] },
    "M03_corridors": { "I-25": [...], "CO-83": [...] },
    "M04_types": { "Rear End": [...], "Angle": [...] },
    "M05_factors": { "speed": [...], "alcohol": [...] },
    "M06_location": { "intersection": [...], "segment": [...] }
  }
}
```

### 11.5 Execution Modes

| Mode | Flag | When to Use | AWS Required? |
|------|------|------------|---------------|
| **Live** | (default) | CI/CD with AWS secrets | Yes |
| **Dry-run** | `--dry-run` | Local dev, no AWS credentials | No (synthetic data) |
| **Skip** | omit `--predict` flag | Data corrections, no forecast needed | No |

Forecasting is **non-fatal** — if SageMaker is down or credentials are missing, stages 0-6 still produce valid CSVs. The pipeline logs a warning and continues.

### 11.6 Command

```bash
python scripts/generate_forecast.py \
  --data data/{DOT_NAME}/{jurisdiction}_county_roads.csv \
  --horizon 12

# Dry run (no AWS needed):
python scripts/generate_forecast.py --dry-run
```

### 11.7 Output

Three JSON files per jurisdiction:
- `forecasts_county_roads.json`
- `forecasts_no_interstate.json`
- `forecasts_all_roads.json`

---

## 12. Stage 8: Manifest Update & Git Commit

### 12.1 Purpose

After uploading files to R2, update the shared manifest file so the browser knows what's available and where to find it.

### 12.2 Manifest File

**Location:** `data/r2-manifest.json` (version 3)

**Structure:**

```json
{
  "version": 3,
  "r2BaseUrl": "https://data.aicreatesai.com",
  "updated": "2026-02-16T00:45:07.223028+00:00",
  "files": {
    "{state_key}/{jurisdiction}/county_roads.csv": {
      "size": 9500000,
      "md5": "abc123...",
      "uploaded": "2026-02-13T03:32:40.216522+00:00"
    }
  },
  "localPathMapping": {
    "data/{DOT_NAME}/{jurisdiction}_county_roads.csv": "{state_key}/{jurisdiction}/county_roads.csv"
  }
}
```

### 12.3 What Gets Updated

1. **`files` object:** Add/update entries for each uploaded file (size, md5, timestamp)
2. **`localPathMapping` object:** Map legacy local paths → R2 keys (for backwards compatibility)
3. **`updated` timestamp:** Set to current UTC time

### 12.4 Git Commit

After manifest update, commit to git so the deployed app can access the updated manifest:

```bash
git add data/r2-manifest.json
git commit -m "Update R2 manifest: {state_key}/{jurisdiction} data refresh"
git push
```

In GitHub Actions, this is automated with the `actions/checkout` token.

---

## 13. Cloudflare Workers — Server-Side Validation & Geocoding

Validation and geocoding are handled **entirely on Cloudflare Workers**, not in GitHub Actions. This is the primary processing path — the pipeline uploads raw (normalized, split) CSVs to R2, and Cloudflare Workers validate and geocode them automatically.

### 13.1 Architecture Overview

```
GitHub Actions uploads CSV to R2 (Stage 6)
        ↓
R2 Upload Worker (r2-upload-worker/src/index.js)
  ├─ Receives HTTP PUT request
  ├─ Routes to CRASH_DATA bucket (via hostname or X-Bucket header)
  ├─ Creates backup of existing file (full CSV or metadata)
  ├─ Writes new CSV to R2
  └─ Detects main CSV pattern → triggers crash-validator-worker
        ↓
Crash Validator Worker (crash-validator-worker.js)
  ├─ Reads CSV from R2 via binding
  ├─ Parses with PapaParse
  ├─ Loads previous corrections ledger (persistent across runs)
  ├─ Applies accumulated corrections to empty fields
  ├─ Validates: coordinate bounds, KABCO severity, missing FC/FT, GPS precision
  ├─ Auto-corrects: route-median snap, nearest-neighbor, FC crosswalk, route prefix
  ├─ Writes corrected CSV back to R2 (via R2 binding — NOT HTTP)
  ├─ Saves updated corrections ledger JSON
  └─ Saves validation report JSON
        ↓
Loop prevention:
  - Validator writes via R2 binding (bucket.put) → does NOT trigger upload worker
  - HTML validator push → includes X-Skip-Validation: true → skips trigger
  - GitHub Actions upload → no skip header → TRIGGERS validation
```

### 13.2 Workers

| Worker | File Location | Purpose | Trigger |
|--------|--------------|---------|---------|
| **R2 Upload Worker** | `r2-upload-worker/src/index.js` | Routes uploads to correct R2 bucket, creates backups, triggers validation | HTTP PUT to `data.aicreatesai.com` |
| **Crash Validator Worker** | `data-pipeline/crash-validator-worker/crash-validator-worker.js` | Validates + auto-corrects CSVs on R2, maintains corrections ledger | Service binding from R2 worker, weekly cron |

### 13.3 R2 Upload Worker Details

The R2 upload worker (`r2-upload-worker/src/index.js`) is the gateway for all file uploads to R2. Key behaviors:

**Routing:**
- `data.aicreatesai.com/*` → `CRASH_DATA` bucket (all 50 states, crash-lens-data)
- `r2-upload.ecomhub200.workers.dev/*` → `BUCKET` (Mapillary / asset inventory)
- Also supports `X-Bucket: CRASH_DATA` header for explicit routing

**Authentication:**
- All mutating operations (PUT, DELETE, LIST) require `X-Upload-Secret` header
- GET requests for file download are public (no auth)

**Backup System (for main CSVs only):**
- Pattern: `{state}/{county}/(all_roads|county_roads|no_interstate).csv`
- Full backup: copies existing CSV to `{state}/{county}/backups/{basename}_{date}.csv`
- Metadata backup: saves size/etag/date to `{state}/{county}/backups/{basename}_{date}.meta.json`
- Controlled by `X-Backup-Mode` header: `full` (default), `diff`/`metadata`, or `skip`
- Auto-cleanup: removes backups older than 90 days

**Delete Protection:**
- Cannot delete protected data files: `all_roads.csv`, `county_roads.csv`, `no_interstate.csv`, `traffic-inventory.csv`

**API Endpoints:**
- `PUT /{key}` — Upload file
- `GET /{key}` — Download file (public)
- `DELETE /{key}` — Delete file (with protection check)
- `GET ?list=1&prefix=...` — List files by prefix
- `DELETE ?purge=1&prefix=...&before={ISO date}` — Purge old files

### 13.4 Crash Validator Worker Details

The crash-validator-worker (`crash-validator-worker.js`) performs comprehensive validation and auto-correction:

**Validation Checks:**

| Check | What It Does | Auto-Correctable? |
|-------|-------------|-------------------|
| **Coordinate bounds** | Verify lat/lon within state and county bounds | Yes (swap detection) |
| **KABCO severity** | Must be K, A, B, C, or O | No (flagged) |
| **Missing Functional Class** | FC field empty or invalid | Yes (FC crosswalk, route majority vote) |
| **Missing Facility Type** | FT field empty | Yes (FC-to-FT crosswalk) |
| **GPS precision** | Coordinates have sufficient decimal places | Yes (nearest-neighbor snap) |
| **Route name consistency** | Route names match expected patterns | Yes (route prefix inference) |

**Auto-Correction Strategies:**

| Strategy | What It Does |
|----------|-------------|
| **Route-median snap** | Corrects coordinates to the median lat/lon of all crashes on the same route |
| **Nearest-neighbor grid** | Snaps low-precision GPS to nearest known crash location |
| **FC crosswalk** | Fills missing Functional Class using route name → FC lookup table |
| **FT crosswalk** | Fills missing Facility Type from Functional Class |
| **Route majority vote** | If most crashes on a route have the same FC, apply it to those missing FC |
| **Route prefix inference** | Derives route name from Roadway Description field |

**Corrections Ledger:**

The worker maintains a persistent `corrections_ledger_{fileKey}.json` on R2 for each validated file. This ledger:
- Accumulates corrections across multiple pipeline runs
- Keyed by `Document Nbr` (unique crash ID)
- Stores the corrected values for each field
- Applied to empty fields on subsequent validations (corrections are never overwritten)

### 13.5 Worker Bindings (wrangler.toml)

**R2 Upload Worker:**

| Binding | Resource | Purpose |
|---------|----------|---------|
| `CRASH_DATA` | `crash-lens-data` R2 bucket | All 50 states crash data |
| `BUCKET` | Mapillary R2 bucket | Asset inventory |
| `VALIDATOR` | Service binding → crash-validator-worker | Auto-validation trigger |

**Crash Validator Worker:**

| Binding | Resource | Purpose |
|---------|----------|---------|
| `CRASH_DATA` | `crash-lens-data` R2 bucket | Read/write CSVs + reports |

**Environment Variables:**

| Variable | Worker | Purpose |
|----------|--------|---------|
| `UPLOAD_SECRET` | R2 Upload Worker | Auth key for write operations |
| `CRASH_DOMAIN` | R2 Upload Worker | Hostname for crash data routing (`data.aicreatesai.com`) |

### 13.6 Validation Outputs on R2

For each validated file, the worker writes:

| R2 Key | Contents |
|--------|----------|
| `{state}/{county}/{file}.csv` | Corrected CSV (overwrites original) |
| `{state}/{county}/corrections_ledger_{fileKey}.json` | Cumulative corrections by Document Nbr |
| `{state}/{county}/validation_report_{fileKey}.json` | Stats: issues found, auto-fixed, remaining |

### 13.7 Cron Safety Net

The crash-validator-worker runs a weekly cron (`0 6 * * 1` — Monday 6 AM UTC) that:

1. Iterates all registered jurisdictions (defined in the `JURISDICTIONS` object in the worker)
2. For each jurisdiction, checks all 3 main CSV files
3. Compares CSV last-modified timestamp vs. validation report timestamp
4. Re-validates any file where the CSV is newer than its report
5. Logs results to console for monitoring

### 13.8 Adding a New Jurisdiction to the Validator

To register a new county for automatic validation, add it to the `JURISDICTIONS` object in `crash-validator-worker.js`:

```javascript
'{state_key}/{county_name}': {
  state: '{state_key}',
  county: '{county_name}',
  bounds: {
    latMin: 0.0, latMax: 0.0,
    lonMin: -0.0, lonMax: -0.0
  },
  files: ['all_roads.csv', 'county_roads.csv', 'no_interstate.csv']
}
```

The bounds should match the county's bounding box from `jurisdictions.json`.

---

## 14. GitHub Actions Workflow (pipeline.yml v6)

The unified pipeline workflow (`pipeline.yml v6`) orchestrates all GitHub Actions stages. Validation and geocoding stages are retained as fallbacks but **skipped by default** — they now run on Cloudflare Workers.

### 14.1 Workflow Structure

The pipeline has two jobs:

**Job 1: Prepare** — Resolves scope, locates input CSV, outputs metadata for Job 2.

**Job 2: Process** — Runs stages 0-8:

| pipeline.yml Stage | Description | Skipped? |
|--------------------|-------------|----------|
| Stage 0 | Initialize state-isolated cache | No |
| Stage 1 | Validate (fallback) | **Yes by default** — Cloudflare handles this |
| Stage 2 | Geocode (fallback) | **Yes by default** — Cloudflare handles this |
| Stage 3 | Split by jurisdiction (statewide mode only) | No |
| Stage 4 | Split by road type | No |
| Stage 5 | Aggregate by scope (CSV) | No |
| Stage 6 | Upload to R2 (triggers Cloudflare validation) | No |
| Stage 7 | Generate forecasts + upload forecast JSONs | No (unless `skip_forecasts=true`) |
| Stage 8 | Commit manifest and metadata | No |

### 14.2 Workflow Inputs

| Input | Default | Purpose |
|-------|---------|---------|
| `state` | (required) | State to process: virginia, colorado, maryland |
| `scope` | (required) | Processing scope: jurisdiction, region, mpo, statewide |
| `selection` | (optional) | Jurisdiction/region/MPO name |
| `data_source` | (optional) | Path to input CSV |
| `skip_validation` | `true` | Skip validation (Cloudflare handles it) |
| `skip_geocode` | `true` | Skip geocoding (Cloudflare handles it) |
| `skip_forecasts` | `false` | Skip forecast generation |
| `dry_run` | `false` | No uploads, no commits |

### 14.3 Batch Download for Any State (Registry-Driven)

A single workflow (`download-state-crash-data.yml` / "Batch Download for Any State") replaces the need for per-state download workflows. It uses `states/download-registry.json` to resolve the correct script, data directory, and R2 prefix for each state.

#### Architecture

```
"Batch Download for Any State"                   "Batch Pipeline: Process Statewide Data"
(download-state-crash-data.yml)                   (batch-pipeline.yml)
┌───────────────────────────────┐                 ┌──────────────────────────────────┐
│ 1. Resolve from registry      │                 │ Download statewide CSV from R2   │
│ 2. Download statewide data    │                 │         ↓                        │
│ 3. Merge (if multi-file)      │   triggers      │ Split by jurisdiction            │
│ 4. Convert to standard format │ ──────────────► │         ↓                        │
│ 5. Upload statewide CSV to R2 │                 │ Split road type (3 CSVs/county)  │
└───────────────────────────────┘                 │         ↓                        │
                                                  │ Aggregate → Upload → Forecasts   │
                                                  │         ↓                        │
                                                  │ Manifest commit                  │
                                                  └──────────────────────────────────┘
```

**Every state follows the same flow:** download statewide → merge → convert → upload to R2 → trigger batch-pipeline.yml which splits into per-county, 3-road-type CSVs.

#### Registry Entry Format (`states/download-registry.json`)

```json
{
  "florida": {
    "displayName": "Florida",
    "dotDir": "data/FloridaDOT",
    "script": "data/FloridaDOT/download_florida_crash_data.py",
    "requiresPlaywright": false,
    "r2Prefix": "florida"
  }
}
```

| Field | Purpose |
|-------|---------|
| `displayName` | Human-readable state name |
| `dotDir` | Path to data directory |
| `script` | Path to download script |
| `requiresPlaywright` | Install Playwright for browser-based downloads (Colorado) |
| `r2Prefix` | R2 bucket prefix for this state |

**To add a new state:** add an entry to `states/download-registry.json` + write the download script. No new workflow file needed.

#### Workflow Inputs

| Input | Default | Description |
|-------|---------|-------------|
| `state` | (required) | State key from registry dropdown |
| `filter` | `countyOnly` | Road type filter (Virginia only) |
| `force_download` | `false` | Skip cache |
| `skip_pipeline` | `false` | Download + R2 upload only |
| `skip_forecasts` | `false` | Skip forecast generation in batch pipeline |
| `dry_run` | `false` | No uploads, no commits |

#### Workflow Steps (Job 1: Download)

1. **Resolve from registry** — reads `states/download-registry.json` for script, dotDir, r2Prefix
2. **Install dependencies** — `requirements.txt` + Playwright if needed + awscli
3. **Download statewide data** — Virginia/Colorado use custom root scripts; all others use `data/{DotDir}/download_{state}_crash_data.py --data-dir {dotDir} --gzip`
4. **Merge** — combines multi-file data if needed (skipped for Virginia/Colorado)
5. **Convert** — normalizes to standardized format via `scripts/process_crash_data.py` (skipped for Virginia)
6. **Upload statewide CSV to R2** — `crash-lens-data/{state}/statewide/{state}_statewide_all_roads.csv`

#### Workflow Steps (Job 2: Trigger Pipeline)

7. **Trigger `batch-pipeline.yml`** — passes R2 data key, state, scope=statewide
   - batch-pipeline downloads from R2 → splits by jurisdiction → splits road type (3 CSVs per county) → aggregates → uploads per-county to R2 → generates forecasts → commits manifest

### 14.4 Legacy Per-State Workflows

The existing per-state `download-{state}-crash-data.yml` files are **not deleted** — they still work for single-jurisdiction downloads. The batch download workflow handles statewide processing.

| Workflow | Purpose | Pipeline |
|----------|---------|----------|
| `download-state-crash-data.yml` | **Batch statewide** for any state (registry-driven) | `batch-pipeline.yml` |
| `batch-all-jurisdictions.yml` | Batch statewide (hardcoded, all 50 states) | `batch-pipeline.yml` |
| `download-virginia.yml` | Virginia single jurisdiction/region/mpo | `pipeline.yml` |
| `download-colorado.yml` | Colorado single jurisdiction/region/mpo | `pipeline.yml` |
| `download-{state}-crash-data.yml` | Per-state single jurisdiction (R2 only, no pipeline) | None |

---

## 15. Front-End Connection

### 15.1 How the Browser Loads Data from R2

Once data is in R2, the browser app (`app/index.html`) loads it through a three-layer URL resolution system:

```
User selects: {State} > {County} > {Road Type}
        ↓
Layer 1: getDataFilePath()
  - Reads current tier, state r2Prefix, jurisdiction ID, road type
  - Returns R2-native path: "{state_key}/{jurisdiction}/{road_type}.csv"
        ↓
Layer 2: resolveDataUrl(path)
  - Strategy 1: Exact lookup in r2-manifest.json localPathMapping
  - Strategy 2: Dynamic construction from state r2Prefix
  - Strategy 3: R2-native path detection (prepend R2 base URL)
  - Returns: "https://data.aicreatesai.com/{state_key}/{jurisdiction}/{road_type}.csv"
        ↓
Layer 3: fetch()
  - Browser HTTP GET to R2 CDN
  - Parse CSV with PapaParse
  - StateAdapter.detect(headers) identifies the state format
  - Normalize rows, populate dashboard
```

### 15.2 What the Pipeline Must Ensure

For the front-end to work, the pipeline must guarantee:

1. **Files exist in R2** at the correct paths (Stage 6)
2. **r2-manifest.json is updated** with file metadata (Stage 8)
3. **config.json has `r2Prefix`** for the state (Section 3.4)
4. **State adapter detects the format** from CSV headers (Section 6)
5. **Column names are standardized** to Virginia-compatible format (Stage 2)

### 15.3 Road Type to Filename Mapping (Front-End)

| Radio Button | Filter Value | County-Level File | State-Level File |
|-------------|-------------|-------------------|-----------------|
| County Roads Only **(default)** | `countyOnly` | `county_roads.csv` | `dot_roads.csv` |
| No Interstate | `countyPlusVDOT` | `no_interstate.csv` | `non_dot_roads.csv` |
| All Roads | `allRoads` | `all_roads.csv` | `statewide_all_roads.csv` |

---

## 16. State Onboarding Checklist

Use this checklist when adding a new state. Check off each item as completed.

### Phase 1: Research & Setup

- [ ] Identify state DOT data source (URL, API type, format)
- [ ] Download a sample file and inspect column headers
- [ ] Document column mapping (raw → standardized)
- [ ] Determine severity method (direct or derived)
- [ ] Identify road system classification values
- [ ] Get coordinate bounds for the state
- [ ] Get FIPS codes for target counties
- [ ] Determine EPDO weights (use HSM standard if state-specific unavailable)

### Phase 2: Configuration Files

- [ ] Create `data/{DOT_NAME}/` folder
- [ ] Create `data/{DOT_NAME}/CLAUDE.md` — state overview
- [ ] Create `data/{DOT_NAME}/config.json` — column mappings, road systems, EPDO
- [ ] Create `data/{DOT_NAME}/source_manifest.json` — data source registry
- [ ] Create `data/{DOT_NAME}/jurisdictions.json` — county definitions
- [ ] Create `states/{state_key}/config.json` — state adapter configuration
- [ ] Create `states/{state_key}/hierarchy.json` — region/county hierarchy
- [ ] Add state signature to `scripts/state_adapter.py`
- [ ] Add state entry to root `config.json` under `states` (with `r2Prefix`)

### Phase 3: Download Script

- [ ] Create `download_{state}_crash_data.py`
- [ ] Implement API pagination
- [ ] Add retry logic with exponential backoff
- [ ] Add `--jurisdiction`, `--years`, `--list` arguments
- [ ] Test download for at least 1 jurisdiction
- [ ] Verify file size > 100 KB

### Phase 4: Pipeline Processing

- [ ] Run Stage 2 (Convert) — verify standardized output
- [ ] Run Stage 4 (Split Road Type) — verify 3 output CSVs are correct
- [ ] Run Stage 6 (Upload to R2) — verify files in R2 bucket
- [ ] Verify Cloudflare validation auto-triggered (check validation report on R2)
- [ ] Run Stage 7 (Predict) — verify 3 forecast JSONs (or dry-run)

### Phase 5: Cloudflare Worker Registration

- [ ] Add jurisdiction to `JURISDICTIONS` in `crash-validator-worker.js` (bounds, files)
- [ ] Deploy updated worker: `wrangler deploy`
- [ ] Trigger a test upload and verify validation report appears on R2
- [ ] Verify corrections ledger is created/updated

### Phase 6: R2 Upload & Integration

- [ ] Create R2 folder: `crash-lens-data/{state_key}/`
- [ ] Upload test data with `scripts/upload-to-r2.py --dry-run`
- [ ] Upload real data with `scripts/upload-to-r2.py`
- [ ] Verify `data/r2-manifest.json` updated correctly
- [ ] Test in browser: select new state → data loads from R2

### Phase 7: Automation

- [ ] Add state entry to `states/download-registry.json` (see Section 14.3)
- [ ] Test via Actions → "Batch Download for Any State" → select state
- [ ] (Optional) Create a dedicated `.github/workflows/download-{state}-crash-data.yml` if custom UI is needed
- [ ] Enable scheduled trigger (monthly) if applicable
- [ ] Verify end-to-end: download → process → upload → (CF validate) → manifest commit

### Phase 8: Expansion

- [ ] Add more jurisdictions (counties) to source_manifest.json
- [ ] Register new jurisdictions in crash-validator-worker.js
- [ ] Configure Deep Dive panels in enhancements.json (optional)
- [ ] Add state-specific hierarchy (regions, MPOs) to hierarchy.json
- [ ] Generate aggregate data (`scripts/generate_aggregates.py`)

---

## Appendix A: Standardized Column Format

All states normalize to this column set (Virginia-compatible):

```
Document Nbr          — Unique crash identifier
Crash Date            — Date (YYYY-MM-DD or M/D/YYYY)
Crash Year            — Year (derived from date)
Crash Military Time   — Time (HHMM format)
Crash Severity        — K / A / B / C / O
K_People              — Fatal count
A_People              — Serious injury count
B_People              — Minor injury count
C_People              — Possible injury count
Collision Type        — Numbered VDOT format (e.g., "1. Rear End")
Weather Condition     — Numbered VDOT format
Light Condition       — Numbered VDOT format
Roadway Surface Condition
Roadway Alignment
Roadway Description
Intersection Type
RTE Name              — Route name
SYSTEM                — Road system (Interstate, US Route, etc.)
Node                  — Intersection node ID (if available)
RNS MP                — Route milepost (if available)
x                     — Longitude (decimal degrees, negative for US)
y                     — Latitude (decimal degrees, positive for US)
Physical Juris Name   — Jurisdiction/county name
Pedestrian?           — Y/N boolean flag
Bike?                 — Y/N boolean flag
Alcohol?              — Y/N boolean flag
Speed?                — Y/N boolean flag
Hitrun?               — Y/N boolean flag
Motorcycle?           — Y/N boolean flag
Night?                — Y/N boolean flag
Distracted?           — Y/N boolean flag
Drowsy?               — Y/N boolean flag
Drug Related?         — Y/N boolean flag
Young?                — Y/N boolean flag
Senior?               — Y/N boolean flag
Unrestrained?         — Y/N boolean flag
School Zone           — Y/N
Work Zone Related     — Y/N
Traffic Control Type
Traffic Control Status
Functional Class
Area Type
Facility Type
Ownership
First Harmful Event
First Harmful Event Loc
Relation To Roadway
Vehicle Count
Persons Injured
Pedestrians Killed
Pedestrians Injured
_source_state         — State key (e.g., "colorado")
_source_file          — Original filename
```

---

## Appendix B: API Types Across 30+ States

| API Type | States | Base URL Pattern | Pagination |
|----------|--------|-----------------|------------|
| **Socrata SODA** | MD, CT, DE, NY, NYC, HI | `https://{domain}/resource/{dataset_id}.csv` | `$offset`, `$limit` |
| **ArcGIS GeoServices** | IA, IL, FL, OR, PA, MA, OH, WI, AK, NV, UT, ID, WA, GA, SC, AR, MT, MS, OK, LA, WV | `https://services.arcgis.com/.../FeatureServer/0/query` | `resultOffset`, `resultRecordCount` |
| **Custom REST** | VT | `https://{domain}/api/...` | Varies |
| **Bulk Download** | TX | Direct file URL | N/A |
| **OnBase Portal** | CO | `https://{domain}/docpop/GetDoc.aspx?docid={id}` | N/A |

---

## Appendix C: R2 Bucket Complete Structure

```
crash-lens-data/
├── _national/
│   ├── state_comparison.json
│   ├── dot_roads.csv
│   ├── non_dot_roads.csv
│   └── statewide_all_roads.csv
├── colorado/
│   ├── _statewide/
│   │   ├── aggregates.json
│   │   ├── county_summary.json
│   │   └── mpo_summary.json
│   ├── _state/
│   │   ├── dot_roads.csv
│   │   ├── non_dot_roads.csv
│   │   └── statewide_all_roads.csv
│   ├── _region/{regionId}/
│   │   ├── aggregates.json
│   │   └── all_roads.csv
│   ├── _mpo/{mpoId}/
│   │   ├── aggregates.json
│   │   └── all_roads.csv
│   ├── douglas/
│   │   ├── county_roads.csv
│   │   ├── no_interstate.csv
│   │   ├── all_roads.csv
│   │   ├── standardized.csv
│   │   ├── forecasts_county_roads.json
│   │   ├── forecasts_no_interstate.json
│   │   ├── forecasts_all_roads.json
│   │   ├── corrections_ledger_all_roads.json      ← validator worker
│   │   ├── corrections_ledger_county_roads.json   ← validator worker
│   │   ├── corrections_ledger_no_interstate.json  ← validator worker
│   │   ├── validation_report_all_roads.json       ← validator worker
│   │   ├── validation_report_county_roads.json    ← validator worker
│   │   ├── validation_report_no_interstate.json   ← validator worker
│   │   └── backups/                               ← R2 upload worker
│   │       ├── all_roads_2026-02-28.csv
│   │       └── all_roads_2026-02-28.meta.json
│   ├── arapahoe/
│   │   └── (same structure)
│   └── ... (other counties)
├── virginia/
│   ├── _statewide/
│   ├── henrico/
│   └── ... (other counties)
└── {new_state}/
    └── (same structure)
```

---

## Appendix D: Reference Implementations

### Colorado (CDOT) — Key Files

| File | Purpose |
|------|---------|
| `download_cdot_crash_data.py` | Download from OnBase portal |
| `data/CDOT/config.json` | Column mapping, severity derivation, road systems |
| `data/CDOT/source_manifest.json` | OnBase doc IDs for all 64 counties |
| `data/CDOT/CLAUDE.md` | State overview |
| `states/colorado/config.json` | State adapter configuration |
| `states/colorado/hierarchy.json` | Regions, MPOs, 64 counties |
| `states/download-registry.json` (colorado entry) | Universal download registry |
| `.github/workflows/download-cdot-crash-data.yml` | Legacy download workflow |
| `.github/workflows/download-state-crash-data.yml` | Batch Download for Any State |
| `.github/workflows/process-cdot-data.yml` | Processing workflow |

### Virginia (VDOT) — Key Files

| File | Purpose |
|------|---------|
| `download_crash_data.py` | Download from ArcGIS API |
| `states/virginia/config.json` | Column mapping (passthrough), road systems |
| `states/virginia/hierarchy.json` | Construction districts, 133 jurisdictions |
| `states/download-registry.json` (virginia entry) | Universal download registry |
| `.github/workflows/download-virginia.yml` | Virginia download workflow (default: `countyOnly`) |
| `.github/workflows/download-state-crash-data.yml` | Batch Download for Any State |

### Key Differences Between the Two

| Aspect | Colorado | Virginia |
|--------|----------|---------|
| Data source | OnBase document portal (Excel) | ArcGIS FeatureServer (CSV) |
| Severity | Derived from injury columns | Direct passthrough |
| Route name | Derived from System Code + Rd_Number | Direct passthrough |
| Value mapping | Extensive (Colorado → VDOT vocabulary) | Minimal (already VDOT format) |
| Road split | By agency ID | By SYSTEM column |
| Counties | 64 (6 configured) | 95 + 38 cities (133 total) |
| Download format | Excel (.xlsx) per year | CSV via API query |
| Merge needed? | Yes (multiple year files) | No (single query) |
| EPDO weights | HSM 2010 standard | VDOT 2024 state-specific |

---

## Quick Reference: Full Pipeline Command Sequence

For a new state with everything configured:

```bash
# 1. Download
python download_{state}_crash_data.py --jurisdiction {jurisdiction}

# 2. Process (convert + split in one command)
python scripts/process_crash_data.py \
  --input data/{DOT_NAME}/*.csv \
  --jurisdiction {jurisdiction} \
  --merge

# 3. Upload to R2 (triggers Cloudflare validation automatically)
python scripts/upload-to-r2.py --state {state_key} --jurisdiction {jurisdiction}

# 4. Forecast (optional)
python scripts/generate_forecast.py --dry-run

# 5. Verify in browser
# Open app/index.html → Select new state → Data should load from R2
# Check R2 for validation_report_*.json to confirm Cloudflare validation ran
```

---

## Appendix E: Additional File Templates

### E.1 source_manifest.json Template

Registry of data source identifiers per jurisdiction (URLs, doc IDs, API endpoints):

```json
{
  "state": "{state_name}",
  "dotName": "{DOT_NAME}",
  "dataSource": {
    "type": "arcgis | socrata | onbase | rest | bulk",
    "baseUrl": "{api_base_url}",
    "notes": "Brief description of how to access data"
  },
  "years": {
    "2025": { "status": "preliminary" },
    "2024": { "status": "final" },
    "2023": { "status": "final" }
  },
  "jurisdictions": {
    "{jurisdiction_id}": {
      "name": "{County Name} County",
      "fips": "{county_fips}",
      "apiFilter": "{api_specific_filter_value}"
    }
  }
}
```

For Colorado (OnBase), each year also has a `docid` field. For ArcGIS states, `apiFilter` is the FIPS or name used in API queries.

### E.2 jurisdictions.json Template

County definitions with map boundaries and metadata:

```json
{
  "{jurisdiction_id}": {
    "name": "{County Name} County",
    "type": "county",
    "fips": "{county_fips}",
    "state": "{XX}",
    "center": { "lat": 0.0, "lon": 0.0 },
    "zoom": 11,
    "bbox": [-0.0, 0.0, -0.0, 0.0],
    "cities": ["{City1}", "{City2}"],
    "majorRoutes": ["I-XX", "US-XX", "SR-XX"],
    "reportingAgencies": ["{agency_code}"]
  }
}
```

### E.3 CLAUDE.md Template

State overview for pipeline context (used by Claude Code to understand state-specific rules):

```markdown
# {DOT_NAME} ({State Full Name}) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | {State Name} |
| **Abbreviation** | {XX} |
| **FIPS** | {fips} |
| **DOT Name** | {DOT_NAME} |
| **Counties** | {count} |

## Data Source

- **System:** {source_system_name}
- **URL:** `{source_url}`
- **Format:** {CSV/Excel/JSON}
- **Refresh:** {Annual/Monthly/Quarterly}

## Column Mapping Summary

| CRASH LENS Field | Raw Column | Notes |
|-----------------|-----------|-------|
| ID | `{raw_id}` | Unique crash identifier |
| DATE | `{raw_date}` | Date of crash |
| SEVERITY | `{raw_severity}` or DERIVED | {Notes on derivation} |
| ROUTE | `{raw_route}` or DERIVED | {Notes on derivation} |
| JURISDICTION | `{raw_county}` | County/jurisdiction name |

## Severity Mapping

{Direct passthrough OR injury hierarchy derivation rules}

## Road Systems

| Raw Value | Classification |
|-----------|---------------|
| {value1} | Local |
| {value2} | State-maintained |
| {value3} | Interstate |

## Key Files in This Folder

| File | Purpose |
|------|---------|
| `config.json` | Column mappings, road systems, EPDO weights |
| `source_manifest.json` | Data source registry |
| `jurisdictions.json` | County definitions |
```

### E.4 hierarchy.json Template

Region/MPO/county hierarchy for multi-tier navigation:

```json
{
  "state": "{state_key}",
  "regions": [
    {
      "id": "{region_id}",
      "name": "{Region Name}",
      "shortName": "{Short}",
      "counties": ["{county1}", "{county2}"]
    }
  ],
  "mpos": [
    {
      "id": "{mpo_id}",
      "name": "{MPO Full Name}",
      "shortName": "{Acronym}",
      "counties": ["{county1}", "{county2}"]
    }
  ],
  "counties": {
    "{county_id}": {
      "name": "{County Name}",
      "fips": "{county_fips}",
      "region": "{region_id}",
      "mpo": "{mpo_id_or_null}",
      "center": [0.0, 0.0],
      "zoom": 11
    }
  }
}
```

### E.5 enhancements.json Template (Optional)

Deep Dive tab panel configuration for state-specific analysis columns:

```json
{
  "panels": [
    {
      "id": "driver_demographics",
      "title": "Driver Demographics",
      "description": "Age and gender analysis",
      "columns": ["_{xx}_driver_age", "_{xx}_driver_sex"],
      "chartType": "bar"
    },
    {
      "id": "speed_intelligence",
      "title": "Speed Intelligence",
      "description": "Posted vs actual speed analysis",
      "columns": ["_{xx}_speed_limit", "_{xx}_estimated_speed"],
      "chartType": "scatter"
    }
  ]
}
```

Only create this file if the state's raw data has unique columns worth analyzing beyond the standard set.

---

*End of Document — Crash Lens Universal Data Pipeline v3: Download to R2 Storage*
