# Crash Lens — Unified Pipeline Architecture

**Version:** 7.0
**Last Updated:** March 2026
**Purpose:** Replace 40+ separate workflows with a two-workflow system: state-specific download/convert workflows (with scope dropdowns) that automatically trigger a single unified processing pipeline. The conversion step produces a Virginia-compatible CSV with standardized columns plus any unmapped state-specific columns appended at the end with a `_{state}_` prefix. Stages 1-3 (jurisdiction split, road type split, scope aggregation) are unified into a single `scripts/split.py` call that outputs to `data/{state}/`. Validation is triggered from the frontend, not from the pipeline.

**Design Decision:** State-specific download + merge + convert workflows (with state/scope/jurisdiction dropdowns) → auto-trigger unified `pipeline.yml` starting at Init Cache.

**Architecture Boundary:** Everything needed to produce a Virginia-compatible CSV is state-specific. Everything after that is unified.

**Working Implementations:** Colorado (CDOT), Virginia (VDOT), Montgomery County (MD)

---

## Table of Contents

1. [Why a Unified Pipeline](#1-why-a-unified-pipeline)
2. [Architecture Overview](#2-architecture-overview)
3. [Design Principles](#3-design-principles)
4. [Layer 1: State-Specific Download + Convert Workflows](#4-layer-1-state-specific-workflows)
5. [State-Specific Workflow Template](#5-state-specific-workflow-template)
6. [Layer 2: Scope Resolver](#6-layer-2-scope-resolver)
7. [Layer 3: Unified Processing Pipeline](#7-layer-3-unified-processing)
8. [Validation (Frontend-Triggered)](#8-validation)
9. [Unified Stage 0: Init Cache](#9-stage-0-init-cache)
10. [Unified Stages 1-3: Split (scripts/split.py)](#10-stages-1-3-split)
11. [Unified Stage 4: Upload to R2](#11-stage-4-upload-to-r2)
12. [Unified Stage 5: Predict (Forecast)](#12-stage-5-predict)
12b. [Unified Stage 5b: Upload Forecast JSONs](#12b-stage-5b-upload-forecasts)
13. [Unified Stage 6: Manifest Update & Git Commit](#13-stage-6-manifest-update)
16. [Unified Workflow YAML (pipeline.yml)](#16-unified-workflow-yaml)
17. [Scope Resolver Script](#17-scope-resolver-script)
18. [Configuration Per State](#18-configuration-per-state)
19. [Migration Plan: From 40 Workflows to 2](#19-migration-plan)
20. [State Onboarding Checklist](#20-onboarding-checklist)
21. [Appendix A: Current vs Unified Comparison](#appendix-a)
22. [Appendix B: Download Script Interface Contract](#appendix-b)
23. [Appendix C: Scope Resolution Examples](#appendix-c)
24. [Appendix D: State-Specific Script Examples](#appendix-d)
25. [Appendix E: Aggregation by Scope (CSV)](#appendix-e)
26. [Appendix F: Conversion Algorithm — Virginia Standard + Unmapped Columns](#appendix-f)
27. [Appendix G: Incremental Validation & Geocoding Cache](#appendix-g)

---

## 1. Why a Unified Pipeline

### The Current Problem

The codebase has **40 workflow files** and growing:

| Category | Count | Examples |
|----------|-------|---------|
| Active state workflows | 3 | download-data.yml (VA), download-cdot-crash-data.yml (CO), moco_crash_download.yml (MD) |
| Active processing workflows | 2 | process-cdot-data.yml, process-moco-data.yml |
| Batch workflow | 1 | batch-all-jurisdictions.yml |
| Disabled state frameworks | 28 | download-alaska-crash-data.yml through download-wisconsin-crash-data.yml |
| Infrastructure/utility | 6 | create-r2-folders.yml, seed-r2.yml, etc. |

When the processing pipeline changes (add a stage, fix upload logic, change a flag), you have to update **every workflow file**. The 28 disabled state workflows are copy-paste scaffolds that will diverge over time.

### The Solution

**State-specific workflows** (one per state) handle Download + Merge + Convert and then **auto-trigger** `pipeline.yml`.
**Unified pipeline.yml** handles Init Cache → Split Jurisdiction → Split Road Type → Aggregate (CSV) → Upload → Predict → Manifest.

| Before | After |
|--------|-------|
| 40 workflow files | 1 per state (download/convert) + 1 unified (pipeline.yml) |
| download-{state}-crash-data.yml × 30 | `download-{state}.yml` (download + merge + convert + auto-trigger) |
| process-{state}-data.yml × 3 | `pipeline.yml` (unified, starts at Init Cache) |
| batch-all-jurisdictions.yml × 1 | — (merged into pipeline.yml) |
| Adding a new state = write workflow + download script + config | Adding a new state = copy workflow template + write download/convert script + config |

### What We Keep From Each Existing Pipeline

| Source | What We Take | Why |
|--------|-------------|-----|
| `batch-all-jurisdictions.yml` | Split-first pattern, batch upload via AWS CLI, aggregate generation | Proven at scale (399 CSVs for Virginia) |
| `download-data.yml` (Virginia) | Single-jurisdiction mode, scheduled triggers | Daily operations |
| `process-cdot-data.yml` (Colorado) | Auto-trigger from download, convert → split → upload → validate (Stage 4.5) chain | Multi-stage orchestration |
| `data-pipeline-download-to-R2-storage-pipeline.md` | 9-stage conceptual model, config templates, onboarding checklist | Documentation foundation |

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                    UNIFIED PIPELINE ARCHITECTURE v6                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  USER INPUT (via state-specific workflow dropdown)                    │
│  ├─ state:      virginia | colorado | maryland | ...   (auto-set)    │
│  ├─ scope:      jurisdiction | region | mpo | statewide              │
│  ├─ selection:  dropdown populated from hierarchy.json               │
│  └─ options:    skip_forecasts, dry_run                              │
│                          ↓                                           │
│  ╔══════════════════════════════════════════════════════════════╗     │
│  ║  LAYER 1: STATE-SPECIFIC WORKFLOW                           ║     │
│  ║  File: .github/workflows/download-{state}.yml               ║     │
│  ║                                                             ║     │
│  ║  1. Resolve scope → jurisdiction list                       ║     │
│  ║  2. Download raw data from state API                        ║     │
│  ║  3. Merge multi-file sources (if needed)                    ║     │
│  ║  4. Convert columns to Virginia-compatible format           ║     │
│  ║                                                             ║     │
│  ║  Output: STANDARDIZED Virginia-format CSV                   ║     │
│  ║  Action: AUTO-TRIGGER pipeline.yml via workflow_dispatch     ║     │
│  ╚══════════════════════════════════════════════════════════════╝     │
│                          ↓ (automatic)                               │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  LAYER 2: UNIFIED PIPELINE (pipeline.yml)                    │    │
│  │  Input: Virginia-compatible CSV + scope metadata             │    │
│  │                                                              │    │
│  │                                                              │    │
│  │  Stage 0:    INIT CACHE (state-isolated cache setup)          │    │
│  │  Stages 1-3: SPLIT (scripts/split.py — unified)              │    │
│  │              jurisdiction + road type + aggregate in 1 pass   │    │
│  │              Output: data/{state}/ (mirrors R2 structure)     │    │
│  │  Stage 4:    UPLOAD all CSVs to R2                            │    │
│  │  Stage 5:    PREDICT (SageMaker Chronos-2 — optional)         │    │
│  │  Stage 5b:   UPLOAD forecast JSONs to R2                      │    │
│  │  Stage 6:    MANIFEST UPDATE + GIT COMMIT                     │    │
│  │                                                               │    │
│  │  NOTE: Validation triggered from frontend, not pipeline       │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### The Key Boundary

**State-specific workflow** = Download + Merge + Convert → produce Virginia-compatible CSV → auto-trigger pipeline.yml.
**Unified pipeline.yml** = Init Cache → Split (unified) → Upload → Predict → Upload Forecasts → Manifest.

### Validation

Validation is triggered **from the frontend** (Upload tab), not from the pipeline. The `crash-data-validator-v13.html` engine runs in the browser when the user opens the Upload tab, performing interactive review, auto-correction, and push to R2.

**Stage 4.5 was removed** from both `pipeline.yml` and `batch-pipeline.yml` to keep the pipeline focused on data processing and uploading. Validation happens after data is available in R2, triggered by the user from the frontend.

### Two Processing Modes

**Mode A: Single Jurisdiction**
User picks one county from the dropdown. The state workflow downloads and converts data for that county. Pipeline initializes cache → splits by road type → uploads.

**Mode B: Batch (Region / MPO / Statewide)**
User picks a region, MPO, or "statewide." The state workflow downloads the full statewide dataset, merges and converts to Virginia format. Pipeline initializes cache → splits (all tiers in one pass via `scripts/split.py`) → uploads to R2.

---

## 3. Design Principles

### 3.1 Download + Merge + Convert Are State-Specific

Every state has a different data source — ArcGIS, Socrata, OnBase, REST, bulk download. Each has unique authentication, pagination, retry logic, output formats, column names, severity classifications, date formats, and road system codes. Some states (like Colorado) provide data as multiple files that need merging.

All of this variation lives in the state-specific download script. The script handles three responsibilities:

1. **Download** — fetch raw data from the state's API/portal
2. **Merge** — combine multi-file sources if the state provides split data (optional)
3. **Convert** — normalize columns to produce a Virginia-compatible CSV

**The user will use Claude Code** to build and modify these state-specific conversion pipelines when onboarding new states.

### 3.2 State Workflows Have Scope Dropdowns

Each state-specific workflow includes dropdown inputs for:
- **scope** — jurisdiction, region, mpo, statewide
- **selection** — populated from `states/{state}/hierarchy.json` (county names, region names, MPO names)

This means users pick their scope directly in the GitHub Actions UI and the workflow resolves it to jurisdiction IDs.

### 3.3 State Workflows Auto-Trigger pipeline.yml

After the state workflow finishes downloading and converting, it uses `workflow_dispatch` to trigger `pipeline.yml` with the state, scope, selection, and path to the Virginia-compatible CSV. No manual step between download and processing.

### 3.4 Config-Driven, Not Code-Driven

State behavior is controlled by configuration files:
- `states/{state}/config.json` — column mappings, severity derivation, road system classification
- `states/{state}/hierarchy.json` — regions, MPOs, counties with FIPS codes
- `data/{DOT_NAME}/source_manifest.json` — data source URLs, doc IDs, API filters
- `data/{DOT_NAME}/jurisdictions.json` — county definitions, bounding boxes

### 3.5 Every Stage Is Skippable and Non-Fatal

Each processing stage can be skipped via flag (`--skip-forecasts`). Stages that fail log warnings but don't kill the pipeline. The `dry_run` flag prevents uploads and commits.

### 3.6 Download Script Interface Contract

Every state download script MUST implement this interface:

```bash
python download_{state}_crash_data.py \
  --jurisdiction {jurisdiction_id}       # Filter to single county
  --output-dir {path}                    # Where to save output
  --save-statewide                       # Save full statewide CSV + gzip
  --list                                 # Print available jurisdictions
  --years {year1} {year2}               # Year range filter
  --force-download                       # Skip cache, re-download

# CRITICAL: Output CSV MUST be Virginia-compatible standardized format.
# Script handles download + merge + convert internally.
# Print "statewide_path={path}" to stdout for statewide mode.
# Exit 0 on success, exit 1 on failure.
```

**Virginia-Compatible Standardized Columns (actual column names from Virginia TREDS data):**

The output CSV from `state_adapter.py` uses these exact column names. The first 51 columns are the **standard columns** that every state MUST map. Columns 52-69 are Virginia-specific infrastructure/location columns (states that can't map these leave them empty). After these, any unmapped state-specific columns are appended with a `_{state}_` prefix.

**Group A — Core Columns (1-51, all states MUST map):**

| # | Column | Type | Description |
|---|--------|------|-------------|
| 1 | `Document Nbr` | string | Unique crash ID |
| 2 | `Crash Date` | string | Date of crash (state format, validated on Cloudflare) |
| 3 | `Crash Year` | string | Year extracted from crash date |
| 4 | `Crash Military Time` | string (HHMM) | 24h time of crash |
| 5 | `Crash Severity` | string | K / A / B / C / O (KABCO scale) |
| 6 | `K_People` | int | Fatalities |
| 7 | `A_People` | int | Incapacitating injuries |
| 8 | `B_People` | int | Non-incapacitating injuries |
| 9 | `C_People` | int | Possible injuries |
| 10 | `Collision Type` | string | VDOT numbered format (e.g., "1. Rear End") |
| 11 | `Weather Condition` | string | VDOT numbered format (e.g., "5. Rain") |
| 12 | `Light Condition` | string | VDOT numbered format (e.g., "2. Daylight") |
| 13 | `Roadway Surface Condition` | string | VDOT numbered format (e.g., "1. Dry") |
| 14 | `Roadway Alignment` | string | VDOT numbered format (e.g., "1. Straight - Level") |
| 15 | `Roadway Description` | string | Road geometry (e.g., "1. Two-Way, Not Divided") |
| 16 | `Intersection Type` | string | VDOT approach format (e.g., "4. Four Approaches") |
| 17 | `Relation To Roadway` | string | VDOT relation (e.g., "9. Within Intersection") |
| 18 | `RTE Name` | string | Route/road name (e.g., "I-64", "Main St") |
| 19 | `SYSTEM` | string | Road system: Interstate / Primary / Secondary / Non-DOT secondary |
| 20 | `Node` | string | Intersection node ID (e.g., "Main St & Broad St") |
| 21 | `RNS MP` | string | Route milepost |
| 22 | `x` | float | Longitude (Virginia convention: x=lon) |
| 23 | `y` | float | Latitude (Virginia convention: y=lat) |
| 24 | `Physical Juris Name` | string | County/jurisdiction name |
| 25 | `Pedestrian?` | Yes/No | Pedestrian involved |
| 26 | `Bike?` | Yes/No | Bicycle involved |
| 27 | `Alcohol?` | Yes/No | Alcohol-related |
| 28 | `Speed?` | Yes/No | Speed-related |
| 29 | `Hitrun?` | Yes/No | Hit and run |
| 30 | `Motorcycle?` | Yes/No | Motorcycle involved |
| 31 | `Night?` | Yes/No | Nighttime crash |
| 32 | `Distracted?` | Yes/No | Distracted driving |
| 33 | `Drowsy?` | Yes/No | Drowsy driving |
| 34 | `Drug Related?` | Yes/No | Drug-related |
| 35 | `Young?` | Yes/No | Young driver (16-20) |
| 36 | `Senior?` | Yes/No | Senior driver (65+) |
| 37 | `Unrestrained?` | Yes/No | Unrestrained occupant |
| 38 | `School Zone` | Yes/No | In school zone |
| 39 | `Work Zone Related` | Yes/No | Work zone crash |
| 40 | `Traffic Control Type` | string | Traffic control device type |
| 41 | `Traffic Control Status` | string | Traffic control status |
| 42 | `Functional Class` | string | Road functional classification |
| 43 | `Area Type` | string | Urban/rural classification |
| 44 | `Facility Type` | string | Facility type |
| 45 | `Ownership` | string | Road ownership (e.g., "1. State Hwy Agency") |
| 46 | `First Harmful Event` | string | VDOT numbered format (e.g., "20. Motor Vehicle In Transport") |
| 47 | `First Harmful Event Loc` | string | VDOT numbered format (e.g., "1. On Roadway") |
| 48 | `Vehicle Count` | int | Number of vehicles involved |
| 49 | `Persons Injured` | int | Total persons injured |
| 50 | `Pedestrians Killed` | int | Pedestrians killed |
| 51 | `Pedestrians Injured` | int | Pedestrians injured |

**Group B — Virginia Infrastructure Columns (52-69, Virginia-only — other states leave empty):**

| # | Column | Type | Description |
|---|--------|------|-------------|
| 52 | `OBJECTID` | int | Virginia TREDS internal ID |
| 53 | `Roadway Surface Type` | string | Surface material type |
| 54 | `Roadway Defect` | string | Road defect type |
| 55 | `Work Zone Location` | string | Work zone location detail |
| 56 | `Work Zone Type` | string | Work zone type |
| 57 | `Animal Related?` | Yes/No | Animal-related crash |
| 58 | `Guardrail Related?` | Yes/No | Guardrail involvement |
| 59 | `Lgtruck?` | Yes/No | Large truck involved |
| 60 | `Max Speed Diff` | int | Speed over limit differential |
| 61 | `RoadDeparture Type` | string | Road departure classification |
| 62 | `Intersection Analysis` | string | Intersection analysis result |
| 63 | `Mainline?` | Yes/No | Mainline roadway |
| 64 | `DOT District` | string | DOT District name |
| 65 | `Juris Code` | string | Jurisdiction FIPS code |
| 66 | `VSP` | string | Virginia State Police division |
| 67 | `Planning District` | string | Planning district |
| 68 | `MPO Name` | string | MPO name |
| 69 | `Node Offset (ft)` | int | Offset from node in feet |

**Group C — Source Tracking + Unmapped State Columns (appended at end):**

| Column Pattern | Description |
|---------------|-------------|
| `_source_state` | Always present — identifies the source state (e.g., "colorado") |
| `_{state}_{field}` | Unmapped state-specific columns with state abbreviation prefix |
| `_source_file` | Always present — original source filename |

**Example: Colorado unmapped columns appended after standard columns:**
```
_source_state, _co_system_code, _co_agency_id, _co_rd_number, _co_location1,
_co_location2, _co_city, _co_total_vehicles, _co_mhe, _co_crash_type, _co_link,
_co_second_he, _co_third_he, _co_wild_animal, _co_secondary_crash, _co_weather2,
_co_lane_position, _co_injury00_uninjured, _co_tu1_direction, _co_tu1_movement,
_co_tu1_vehicle_type, _co_tu1_speed_limit, _co_tu1_estimated_speed,
_co_tu1_stated_speed, _co_tu1_driver_action, _co_tu1_human_factor, _co_tu1_age,
_co_tu1_sex, _co_tu2_direction, _co_tu2_movement, _co_tu2_vehicle_type, ...
_co_nm1_type, _co_nm1_age, _co_nm1_sex, _co_nm1_action, ... _source_file
```

**The rule:** All standard Virginia columns come first (in the exact order above). Then `_source_state`. Then any state-specific columns that couldn't be mapped, prefixed with `_{state_abbreviation}_`. Then `_source_file` last. The pipeline processes ONLY the standard columns (Group A). Unmapped columns pass through untouched for potential future analysis.

### 3.7 Scope-Aware Aggregation (CSV, Not JSON)

Aggregation produces **CSV files** (not JSON). A region or MPO aggregate CSV contains the actual crash records from all member counties concatenated together — same Virginia-standard columns, same format as any county CSV. This means the frontend and analysts can run the exact same analysis on a region CSV as on a county CSV.

| User Selected Scope | What Aggregate CSVs Are Generated |
|---------------------|-------------------------------|
| `statewide` | Statewide CSV (already exists after geocoding) + ALL region CSVs + ALL MPO CSVs |
| `region` (e.g., hampton_roads) | Region CSV (concat member county rows) with 3 road-type splits |
| `mpo` (e.g., drcog) | MPO CSV (concat member county rows) with 3 road-type splits |
| `jurisdiction` (e.g., henrico) | County CSVs only (no rollup needed) |

This means if a user processes just the Hampton Roads region, they get `hampton_roads_all_roads.csv`, `hampton_roads_no_interstate.csv`, and `hampton_roads_county_roads.csv` — each containing all crash records from the 11 member counties. Same format as any county CSV, just bigger.

### 3.8 Statewide CSV as Input Artifact

The state-specific download workflow produces a **statewide Virginia-compatible CSV** (or a single jurisdiction CSV). This is the input to the pipeline. All county, region, and MPO CSVs are derived from it:

```
Download + Convert → Virginia-compatible statewide CSV
    ↓
Init Cache → Split by Jurisdiction → county CSVs
Split by Road Type → 3 files per county
Aggregate by Scope → region/MPO CSVs (concat county rows)
```

For **Virginia** (bulk download): The download script produces one statewide CSV directly. Simple.

For **Colorado** (yearly archives): The download script downloads year-by-year (2021.csv, 2022.csv, etc.), merges them into one statewide CSV, then converts.

For states where users download only one county at a time: No statewide CSV is created; the single county CSV goes directly through the pipeline.

**Note:** Validation and auto-correction run in Stage 4.5 after upload to R2, using the headless validator engine.

### 3.9 Hierarchy Drives Scope Resolution

Every state has a `states/{state}/hierarchy.json` that defines the region → county and MPO → county mappings. The scope resolver reads this file to translate user selections into jurisdiction lists.

**Virginia example:**
```
regions:
  hampton_roads → [norfolk, virginia_beach, chesapeake, hampton, ...]
  richmond → [henrico, chesterfield, hanover, richmond_city, ...]
mpos:
  hrtpo → [norfolk, virginia_beach, chesapeake, ...]
  rrtpo → [henrico, chesterfield, hanover, ...]
```

**Colorado example:**
```
regions:
  region_1 → [adams, arapahoe, boulder, broomfield, denver, douglas, ...]
  region_2 → [el_paso, pueblo, fremont, ...]
mpos:
  drcog → [adams, arapahoe, boulder, broomfield, denver, douglas, jefferson]
  ppacg → [el_paso, teller]
```

---

## 4. Layer 1: State-Specific Download + Convert Workflows

### 4.1 What Each State Workflow Does

Each state has its own GitHub Actions workflow file (e.g., `download-virginia.yml`, `download-colorado.yml`) that:

1. **Presents scope dropdowns** — user picks scope (jurisdiction/region/mpo/statewide) and a selection from a dropdown populated by hierarchy.json
2. **Resolves scope** — translates the selection into a list of jurisdiction IDs
3. **Runs the state-specific download script** — handles Download + Merge + Convert
4. **Auto-triggers pipeline.yml** — passes state, scope, selection, and the path to the Virginia-compatible CSV

### 4.2 Current Scripts

| Script | State | API Type | Merge Needed? | Convert Complexity |
|--------|-------|----------|---------------|-------------------|
| `download_crash_data.py` | Virginia | ArcGIS FeatureServer | No (single download) | Low (near 1:1 mapping) |
| `download_cdot_crash_data.py` | Colorado | OnBase Document Portal | Yes (multi-year Excel) | High (different severity model) |
| `download_moco_crashes.py` | Maryland | Socrata SODA | Yes (3 datasets: crashes, drivers, non-motorists) | Medium (field renaming + join) |

### 4.3 Why Download + Merge + Convert Are Together

1. **Download is state-specific** — every state has a different API with unique authentication, pagination, and retry logic.
2. **Merge is state-specific** — some states provide a single CSV; others provide one file per year, or multiple datasets that need joining.
3. **Convert is state-specific** — every state has unique column headers, severity classifications, date formats, road system codes. The conversion uses `state_adapter.py` which maps raw columns to the 51 standard Virginia columns (see Section 3.6 for full spec). **Any unmapped state-specific columns are automatically appended at the end with a `_{state}_` prefix**, preserving all original data. See Appendix F for the full algorithm.
4. **User builds these with Claude Code** — when onboarding a new state, the user will use Claude Code to inspect sample data and build the conversion logic.

### 4.4 API Types for Future States

| API Type | Template Script | States That Use It |
|----------|----------------|-------------------|
| ArcGIS GeoServices | `download_crash_data.py` | VA, IA, IL, FL, OR, PA, MA, OH, WI, AK, NV, UT, ID, WA, GA, SC, AR, MT, MS, OK, LA, WV |
| Socrata SODA | `download_moco_crashes.py` | MD, CT, DE, NY, NYC, HI |
| OnBase Portal | `download_cdot_crash_data.py` | CO |
| Custom REST | (create new) | VT |
| Bulk Download | (create new) | TX |

---

## 5. State-Specific Workflow Template

### File: `.github/workflows/download-{state}.yml`

Each state gets a workflow file with **scope dropdowns** that auto-trigger `pipeline.yml` after download/convert completes.

```yaml
# ==============================================================================
# Crash Lens — {STATE_NAME} Download + Convert
# ==============================================================================
# Downloads raw crash data from {STATE_NAME}'s API, merges if needed,
# converts to Virginia-compatible format, then auto-triggers pipeline.yml.
#
# User selects scope (jurisdiction/region/mpo/statewide) and a specific
# selection from the dropdown. The workflow resolves this to jurisdiction IDs.
# ==============================================================================

name: "Download: {State Name}"

on:
  workflow_dispatch:
    inputs:
      scope:
        description: 'Processing scope'
        required: true
        type: choice
        options:
          - jurisdiction
          - region
          - mpo
          - statewide

      # ── JURISDICTION DROPDOWN ──
      # Populated from states/{state}/hierarchy.json → counties
      jurisdiction:
        description: 'County/jurisdiction (if scope=jurisdiction)'
        required: false
        type: choice
        options:
          - ''            # blank = not applicable
          # ── Add all counties from hierarchy.json ──
          # Virginia example:
          # - henrico
          # - fairfax
          # - virginia_beach
          # - norfolk
          # ... (all 133 jurisdictions)

      # ── REGION DROPDOWN ──
      # Populated from states/{state}/hierarchy.json → regions
      region:
        description: 'Region (if scope=region)'
        required: false
        type: choice
        options:
          - ''            # blank = not applicable
          # ── Add all regions from hierarchy.json ──
          # Virginia example:
          # - bristol
          # - salem
          # - lynchburg
          # - staunton
          # - culpeper
          # - fredericksburg
          # - richmond
          # - hampton_roads
          # - nova

      # ── MPO DROPDOWN ──
      # Populated from states/{state}/hierarchy.json → mpos
      mpo:
        description: 'MPO (if scope=mpo)'
        required: false
        type: choice
        options:
          - ''            # blank = not applicable
          # ── Add all MPOs from hierarchy.json ──
          # Virginia example:
          # - hrtpo
          # - rrtpo
          # - gwrc
          # - tjpdc
          # - rrpdc

      skip_forecasts:
        description: 'Skip forecast generation in pipeline'
        required: false
        default: true
        type: boolean

      dry_run:
        description: 'Dry run (no uploads, no commits)'
        required: false
        default: false
        type: boolean

  # ── Scheduled trigger ──
  schedule:
    - cron: '0 11 1 * *'    # 1st of every month, 11:00 UTC

env:
  STATE: "{state_key}"          # e.g., virginia, colorado
  DOT_NAME: "{DOT_NAME}"        # e.g., VDOT, CDOT
  DATA_DIR: "data/{DOT_NAME}"

jobs:
  download-convert:
    name: "Download + Convert ({State Name})"
    runs-on: ubuntu-latest
    timeout-minutes: 120
    outputs:
      scope: ${{ steps.resolve.outputs.scope }}
      selection: ${{ steps.resolve.outputs.selection }}
      jurisdiction_count: ${{ steps.resolve.outputs.jurisdiction_count }}
      statewide_path: ${{ steps.download.outputs.statewide_path }}

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pandas requests openpyxl
          # Add state-specific dependencies here:
          # pip install playwright && playwright install chromium  # (Colorado)

      # ── Resolve scope from dropdowns ──
      - name: Resolve scope
        id: resolve
        run: |
          SCOPE="${{ github.event.inputs.scope || 'statewide' }}"

          # Determine selection from the correct dropdown
          case "$SCOPE" in
            jurisdiction) SELECTION="${{ github.event.inputs.jurisdiction }}" ;;
            region)       SELECTION="${{ github.event.inputs.region }}" ;;
            mpo)          SELECTION="${{ github.event.inputs.mpo }}" ;;
            statewide)    SELECTION="" ;;
          esac

          # Resolve to jurisdiction list
          RESOLVED=$(python scripts/resolve_scope.py \
            --state "$STATE" --scope "$SCOPE" --selection "$SELECTION" --json)

          DOWNLOAD_MODE=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['download_mode'])")
          JCOUNT=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['jurisdiction_count'])")
          JURISDICTIONS=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['jurisdictions']))")

          echo "scope=$SCOPE" >> $GITHUB_OUTPUT
          echo "selection=$SELECTION" >> $GITHUB_OUTPUT
          echo "download_mode=$DOWNLOAD_MODE" >> $GITHUB_OUTPUT
          echo "jurisdiction_count=$JCOUNT" >> $GITHUB_OUTPUT
          echo "jurisdictions_json=$JURISDICTIONS" >> $GITHUB_OUTPUT

          echo "=========================================="
          echo "State: $STATE"
          echo "Scope: $SCOPE → $SELECTION"
          echo "Mode: $DOWNLOAD_MODE"
          echo "Jurisdictions: $JCOUNT"
          echo "=========================================="

      # ── Download + Merge + Convert ──
      - name: Download and convert crash data
        id: download
        timeout-minutes: 60
        run: |
          DOWNLOAD_MODE="${{ steps.resolve.outputs.download_mode }}"
          JURISDICTIONS='${{ steps.resolve.outputs.jurisdictions_json }}'

          echo "=========================================="
          echo "Downloading $STATE crash data ($DOWNLOAD_MODE mode)"
          echo "=========================================="

          if [ "$DOWNLOAD_MODE" = "statewide" ]; then
            # Download full state, merge, convert to Virginia format
            python download_${STATE}_crash_data.py \
              --save-statewide \
              --output-dir "$DATA_DIR"

            STATEWIDE=$(ls "$DATA_DIR"/${STATE}_statewide_all_roads.csv 2>/dev/null | head -1)
            if [ -z "$STATEWIDE" ]; then
              echo "::error::No statewide CSV found after download"
              exit 1
            fi
            echo "statewide_path=$STATEWIDE" >> $GITHUB_OUTPUT

          else
            # Download individual jurisdictions
            echo "$JURISDICTIONS" | python3 -c "
            import json, sys, subprocess
            jurisdictions = json.load(sys.stdin)
            state = '$STATE'
            data_dir = '$DATA_DIR'
            for j in jurisdictions:
                print(f'Downloading + converting: {j}')
                subprocess.run([
                    'python', f'download_{state}_crash_data.py',
                    '--jurisdiction', j,
                    '--output-dir', data_dir
                ], check=True)
            "
            echo "statewide_path=" >> $GITHUB_OUTPUT
          fi

      # ── Commit converted CSVs to repo ──
      - name: Commit converted data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add "$DATA_DIR"/*.csv "$DATA_DIR"/*.csv.gz 2>/dev/null || true
          if ! git diff --cached --quiet; then
            git commit -m "data: download + convert $STATE ($(date -u '+%Y-%m-%d'))"
            git push origin main || true
          fi

  # ============================================================
  # JOB 2: Auto-trigger unified pipeline
  # ============================================================
  trigger-pipeline:
    name: "Trigger Pipeline"
    needs: download-convert
    runs-on: ubuntu-latest
    if: github.event.inputs.dry_run != 'true'

    steps:
      - name: Trigger pipeline.yml
        uses: actions/github-script@v7
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
                scope: '${{ needs.download-convert.outputs.scope }}',
                selection: '${{ needs.download-convert.outputs.selection }}',
                data_source: '${{ needs.download-convert.outputs.statewide_path }}',
                skip_forecasts: '${{ github.event.inputs.skip_forecasts }}',
                dry_run: 'false'
              }
            });
            console.log('Triggered pipeline.yml for ${{ env.STATE }}');
```

### 5.1 Virginia Workflow Example

**File: `.github/workflows/download-virginia.yml`**

Key differences from template:
- `STATE: "virginia"`, `DOT_NAME: "VDOT"`
- Jurisdiction dropdown: all 133 Virginia counties/cities
- Region dropdown: bristol, salem, lynchburg, staunton, culpeper, fredericksburg, richmond, hampton_roads, nova
- MPO dropdown: hrtpo, rrtpo, gwrc, tjpdc, rrpdc, etc.
- No special dependencies (ArcGIS uses standard `requests`)

### 5.2 Colorado Workflow Example

**File: `.github/workflows/download-colorado.yml`**

Key differences from template:
- `STATE: "colorado"`, `DOT_NAME: "CDOT"`
- Jurisdiction dropdown: all 64 Colorado counties
- Region dropdown: region_1 through region_5
- MPO dropdown: drcog, ppacg, nfrmpo, pacog
- Extra dependency: `playwright` + `chromium` (OnBase portal requires browser automation)
- Merge phase: combines multi-year Excel files before convert

### 5.3 Download Script Checklist (For New States)

**Download Phase:**
- [ ] Accept `--jurisdiction`, `--output-dir`, `--save-statewide`, `--list`, `--years`, `--force-download`
- [ ] Implement retry logic with exponential backoff (minimum 3 retries)
- [ ] Validate downloaded file size (minimum 100 KB)

**Merge Phase (if needed):**
- [ ] Handle multi-file sources (one per year, multiple datasets, etc.)
- [ ] Join related datasets (crashes + drivers + non-motorists for Maryland)
- [ ] Deduplicate records after merge

**Convert Phase:**
- [ ] Map ALL state columns to Virginia standardized column names
- [ ] Map severity values → Fatal / Injury / Property Damage Only (PDO)
- [ ] Map road system values → Interstate / US Route / State Route / County Road / City Street
- [ ] Standardize dates to YYYY-MM-DD format
- [ ] Standardize jurisdiction names to lowercase_underscore
- [ ] Derive boolean flags (is_intersection, is_alcohol_related, etc.)

**Output:**
- [ ] Output CSV in Virginia-compatible standardized format
- [ ] Print `statewide_path={path}` to stdout when `--save-statewide` is used
- [ ] Exit 0 on success, exit 1 on failure

---

## 6. Layer 2: Scope Resolver

### 6.1 Purpose

Translate the user's dropdown selection (state + scope + selection) into a concrete list of jurisdiction IDs to process. Used by both the state-specific workflow and the unified pipeline.

### 6.2 How It Works

```
Input:  state=virginia, scope=region, selection=hampton_roads
                    ↓
Read:   states/virginia/hierarchy.json
                    ↓
Lookup: regions.hampton_roads.counties = ["norfolk", "virginia_beach", ...]
                    ↓
Output: {
  "jurisdictions": ["norfolk", "virginia_beach", "chesapeake", ...],
  "jurisdiction_count": 11,
  "download_mode": "statewide",
  "scope": "region",
  "selection": "hampton_roads"
}
```

### 6.3 Scope Types

| Scope | What Happens | Dropdown Shows | Example |
|-------|-------------|----------------|---------|
| `jurisdiction` | Process single county | All counties from hierarchy.json | `henrico` → 1 jurisdiction |
| `region` | Resolve region → counties | All regions from hierarchy.json | `hampton_roads` → 11 counties |
| `mpo` | Resolve MPO → counties | All MPOs from hierarchy.json | `drcog` → 7 counties |
| `statewide` | All counties in state | (no selection needed) | → all 133 (VA) or 64 (CO) |

### 6.4 Download Mode Selection

| Resolved Count | Download Strategy | Why |
|----------------|------------------|-----|
| ≤ 3 jurisdictions | Download each individually | Faster, less data |
| > 3 jurisdictions | Download statewide + split | Avoid many separate API calls |

---

## 7. Layer 3: Unified Processing Pipeline

### 7.1 Stage Sequence

The unified pipeline has **5 stages** (0, 1-3 unified, 4, 5/5b, 6). It receives Virginia-compatible CSVs from the state-specific workflow and handles all generic processing.

**New in v7:** Stages 1-3 (jurisdiction split, road type split, scope aggregation) merged into a single `scripts/split.py` call. Stage 4.5 (validation) removed — validation is now triggered from the frontend. Split output goes to `data/{state}/` which mirrors the R2 folder structure directly.

```
┌─────────────────────────────────────────────────────────────────────┐
│          UNIFIED PROCESSING PIPELINE v7 (pipeline.yml)              │
│          Input: Virginia-compatible CSV from state workflow          │
│          Triggered automatically by state workflow                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Stage 0:    INIT CACHE (state-isolated cache directory setup)      │
│       ↓                                                             │
│  Stages 1-3: SPLIT (scripts/split.py — unified)                    │
│       ├─ Jurisdiction split (counties + cities)                     │
│       ├─ Road type split (SET A & SET B per tier)                   │
│       ├─ Scope aggregation (state, region, MPO, planning district)  │
│       └─ Output: data/{state}/ (mirrors R2 folder structure)        │
│       ↓                                                             │
│  Stage 4:    UPLOAD ALL CSVs to R2 (gzip-compressed)                │
│       ↓                                                             │
│  Stage 5:    PREDICT (SageMaker Chronos-2 forecasts — optional)     │
│       ↓                                                             │
│  Stage 5b:   UPLOAD FORECAST JSONs to R2                            │
│       ↓                                                             │
│  Stage 6:    MANIFEST UPDATE + GIT COMMIT                           │
│                                                                     │
│  NOTE: Validation triggered from frontend, not pipeline             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.2 What Changed From v6 → v7

| v6 (8 stages) | v7 (5 stages — unified split, no pipeline validation) |
|----------------|---------------------------|
| Stage 0: Init Cache | Stage 0: Init Cache (unchanged) |
| Stage 1: Split by Jurisdiction | Stages 1-3: Unified split.py |
| Stage 2: Split by Road Type | (merged into single pass) |
| Stage 3: Aggregate by Scope | (merged into single pass) |
| Stage 4: Upload CSVs to R2 | Stage 4: Upload CSVs to R2 (unchanged) |
| Stage 4.5: Validate & Auto-Correct | — (removed, triggered from frontend) |
| Stage 5: Predict | Stage 5: Predict (unchanged) |
| Stage 5b: Upload Forecast JSONs | Stage 5b: Upload Forecast JSONs (unchanged) |
| Stage 6: Manifest + Git Commit | Stage 6: Manifest + Git Commit (unchanged) |

**Key changes in v7:** `scripts/split.py` replaces `split_jurisdictions.py` + `split_road_type.py` + `aggregate_by_scope.py`. Two road type sets: SET A (state/region: all_roads, dot_roads, primary_roads, non_dot_roads) and SET B (county/MPO/PD/city: all_roads, county_roads, city_roads, no_interstate). Output directory changed from `$DATA_DIR/splits/` to `data/{state}/`. Stage 4.5 validation removed from pipeline (now frontend-triggered). `skip_validation` input removed from workflow YAMLs.

---

## 8. Validation (Frontend-Triggered)

Validation is **no longer part of the pipeline**. It was removed in v7.

The `crash-data-validator-v13.html` engine runs in the browser when the user opens the Upload tab in the frontend. It performs interactive review, auto-correction, and pushes corrected data back to R2.

**Previous pipeline validation (v6 Stage 4.5)** used headless Playwright to run the same engine autonomously. This was removed because:
1. Frontend validation provides the same coverage
2. Pipeline validation added significant runtime (Playwright install + Chromium + per-jurisdiction validation)
3. Keeping validation frontend-triggered gives users control over when corrections are applied

---

## 9. Unified Stage 0: Init Cache

### What It Does

Initializes a state-isolated cache directory for the pipeline run. Each state's cache is fully independent.

```bash
python scripts/init_cache.py --state ${STATE}
```

This ensures cache directories exist and are properly structured before subsequent stages run.

---

## 10. Unified Stage 1: Split by Jurisdiction

### When It Runs

Only in batch mode (scope = region, mpo, or statewide). Skipped in single jurisdiction mode.

### What It Does

Takes the statewide CSV and splits it into per-jurisdiction files.

```bash
python scripts/split_jurisdictions.py \
  --state ${STATE} \
  --input data/${DOT_NAME}/${STATE}_statewide_all_roads.csv \
  --output-dir data/${DOT_NAME}/ \
  --jurisdictions ${JURISDICTION_LIST}
```

### Output

- `data/{DOT_NAME}/{jurisdiction}_all_roads.csv` — per jurisdiction
- `data/{DOT_NAME}/.split_report.json` — statistics

---

## 11. Unified Stage 2: Split by Road Type

Produces 3 filtered CSVs per jurisdiction using `scripts/split_road_type.py`:

| File | Contents |
|------|----------|
| `{jurisdiction}_county_roads.csv` | Local/county roads only |
| `{jurisdiction}_no_interstate.csv` | Everything except interstates |
| `{jurisdiction}_all_roads.csv` | Complete dataset (no filtering) |

Config-driven — reads from `states/{state}/config.json` → `roadSystems.splitConfig`.

### How Road-Type Filtering Works

The `all_roads.csv` file must contain **ALL roads** for a jurisdiction (Interstate, State Hwy, County, City, Federal). The download workflow must always use `--filter allRoads` to ensure this. The road-type splitting then happens in Stage 2, producing `county_roads.csv` and `no_interstate.csv` from the complete `all_roads.csv`.

### splitConfig — Filtering Methods

Each state defines its own `splitConfig` in `states/{state}/config.json` → `roadSystems.splitConfig`. The config specifies which column and method to use for each filter. **Each state may use different columns and methods** depending on their DOT data structure.

#### Available Methods

| Method | Description | Used By |
|--------|-------------|---------|
| `system_column` | Filters by a road classification column (e.g., SYSTEM). Include/exclude by exact value match. | Default fallback |
| `ownership` | Filters by road ownership column (e.g., "2. County Hwy Agency"). Best when the data has an explicit ownership field. | Virginia (county_roads) |
| `functional_class` | Filters by functional classification column (e.g., exclude "1-Interstate (A,1)"). Best when SYSTEM column doesn't distinguish Interstate vs non-Interstate reliably. | Virginia (interstate exclusion) |
| `agency_id` | Filters by agency ID codes specific to each jurisdiction (e.g., "DSO" for Douglas County). Used when road ownership is encoded as agency codes. | Colorado |
| `column_value` | Generic include/exclude by any column value. Alias for `system_column` behavior but can target any column. | Generic |

#### Virginia Example (Ownership + Functional Class)

```json
"splitConfig": {
  "countyRoads": {
    "method": "ownership",
    "column": "Ownership",
    "includeValues": ["2. County Hwy Agency"]
  },
  "interstateExclusion": {
    "method": "functional_class",
    "column": "Functional Class",
    "excludeValues": ["1-Interstate (A,1)"]
  }
}
```

- **county_roads**: Keeps only rows where Ownership = "2. County Hwy Agency"
- **no_interstate**: Keeps all rows EXCEPT where Functional Class = "1-Interstate (A,1)" (blanks are kept)

#### Colorado Example (Agency ID + System Code)

```json
"splitConfig": {
  "countyRoads": {
    "method": "agency_id",
    "column": "_co_agency_id",
    "agencyMap": {
      "douglas": ["DSO"],
      "arapahoe": ["ASO"]
    }
  },
  "interstateExclusion": {
    "method": "column_value",
    "column": "_co_system_code",
    "excludeValues": ["Interstate Highway"]
  }
}
```

#### How to Determine the Right Method for a New State

1. **Get sample data** from the state DOT and open it in a spreadsheet
2. **Identify the jurisdiction column** (e.g., Physical Juris Name, County) — filter to one jurisdiction and confirm you get ALL road types
3. **For county_roads filter**: Find the column that distinguishes county-owned roads from state/federal roads:
   - If there's an **Ownership** column → use `ownership` method
   - If there's a **SYSTEM** column with values like "Non-DOT", "Local" → use `system_column` method
   - If there's an **agency ID** column → use `agency_id` method
4. **For interstate exclusion**: Find the column that identifies Interstate roads:
   - If there's a **Functional Class** column with an Interstate value → use `functional_class` method
   - If the **SYSTEM** column has "Interstate" → use `system_column` method
5. **Validate** by comparing automated output file sizes against manually filtered reference files

### Critical Pitfalls

| Pitfall | What Goes Wrong | How to Avoid |
|---------|----------------|--------------|
| Download filter defaults to `countyOnly` | `all_roads.csv` only contains county roads (~50% of data missing) | Always pass `--filter allRoads` in download workflow |
| SYSTEM column doesn't match Ownership semantics | County roads filter includes/excludes wrong roads | Use the column that the state DOT uses for road ownership classification |
| ArcGIS API WHERE clause stops at first partial match | Jurisdiction filter misses Interstate/State Hwy roads because `Juris_Code` only covers local roads | Order WHERE clauses to try `Physical_Juris_Name` first (broadest match) |
| Old scripts used different filtering logic | Inconsistent results depending on pipeline path | Unified into `scripts/split.py` which handles all filtering in one pass |

### Scripts Involved

| Script | Role | Config Source |
|--------|------|---------------|
| `scripts/split.py` | **v7 unified splitter** — jurisdiction split + road type split + scope aggregation in one pass | `states/{state}/hierarchy.json`, auto-detected column strategies |
| `scripts/split_road_type.py` | Legacy — splits individual jurisdiction's `all_roads.csv` → 3 variants (no longer called by pipeline) | `states/{state}/config.json` → `roadSystems.splitConfig` |
| `scripts/split_jurisdictions.py` | Legacy — splits statewide CSV → per-jurisdiction CSVs (no longer called by pipeline) | Same `splitConfig` (preferred) or falls back to `config.json` → `filterProfiles` |
| `scripts/aggregate_by_scope.py` | Legacy — builds region/MPO aggregates (no longer called by pipeline) | `states/{state}/hierarchy.json` |
| `download_crash_data.py` | Downloads and optionally filters during download | `config.json` → `filterProfiles` (must use `allRoads` for `_all_roads.csv`) |

---

## 12. Unified Stage 3: Aggregate by Scope (CSV)

### What This Stage Does

After splitting into county CSVs and road-type CSVs, this stage builds **region and MPO aggregate CSVs** by concatenating the rows from member county CSVs. The output is a CSV with the exact same Virginia-standard columns — just containing rows from multiple counties.

### If scope = statewide

Generate region + MPO CSVs for ALL regions and ALL MPOs:

```bash
python scripts/aggregate_by_scope.py \
  --state ${STATE} \
  --scope statewide \
  --data-dir data/${DOT_NAME}/ \
  --output-dir data/${DOT_NAME}/
```

Produces (for each region and MPO, 3 road-type splits each):
- `data/{DOT_NAME}/_region/{region_id}/{region_id}_all_roads.csv`
- `data/{DOT_NAME}/_region/{region_id}/{region_id}_no_interstate.csv`
- `data/{DOT_NAME}/_region/{region_id}/{region_id}_county_roads.csv`
- `data/{DOT_NAME}/_mpo/{mpo_id}/{mpo_id}_all_roads.csv`
- `data/{DOT_NAME}/_mpo/{mpo_id}/{mpo_id}_no_interstate.csv`
- `data/{DOT_NAME}/_mpo/{mpo_id}/{mpo_id}_county_roads.csv`

The statewide CSV already exists as the input from the state-specific download workflow. No need to regenerate it.

### If scope = region

Generate CSV for the selected region only:

```bash
python scripts/aggregate_by_scope.py \
  --state ${STATE} \
  --scope region \
  --selection ${SELECTION} \
  --data-dir data/${DOT_NAME}/ \
  --output-dir data/${DOT_NAME}/
```

Produces:
- `data/{DOT_NAME}/_region/{selection}/{selection}_all_roads.csv`
- `data/{DOT_NAME}/_region/{selection}/{selection}_no_interstate.csv`
- `data/{DOT_NAME}/_region/{selection}/{selection}_county_roads.csv`

Each file = concatenation of the corresponding road-type CSV from all member counties. For example, `hampton_roads_all_roads.csv` = norfolk_all_roads.csv + virginia_beach_all_roads.csv + chesapeake_all_roads.csv + ... (11 counties concatenated).

### If scope = mpo

Generate CSV for the selected MPO only:

```bash
python scripts/aggregate_by_scope.py \
  --state ${STATE} \
  --scope mpo \
  --selection ${SELECTION} \
  --data-dir data/${DOT_NAME}/ \
  --output-dir data/${DOT_NAME}/
```

Produces:
- `data/{DOT_NAME}/_mpo/{selection}/{selection}_all_roads.csv`
- `data/{DOT_NAME}/_mpo/{selection}/{selection}_no_interstate.csv`
- `data/{DOT_NAME}/_mpo/{selection}/{selection}_county_roads.csv`

### If scope = jurisdiction

No aggregate needed — county CSVs from Stage 2 are the final output.

### How Aggregation Works (Algorithm)

```python
def generate_aggregate_csv(hierarchy, group_name, member_counties, road_type, data_dir):
    """Concatenate member county CSVs into one aggregate CSV."""
    output_rows = []
    header = None

    for county in member_counties:
        county_csv = f"{data_dir}/{county}_{road_type}.csv"
        with open(county_csv) as f:
            reader = csv.reader(f)
            if header is None:
                header = next(reader)  # Use header from first county
            else:
                next(reader)  # Skip header for subsequent counties
            for row in reader:
                output_rows.append(row)

    # Write aggregate CSV
    output_path = f"{data_dir}/_{group_type}/{group_name}/{group_name}_{road_type}.csv"
    with open(output_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(output_rows)
```

This is a simple concatenation — same columns, just more rows. The frontend can load a region CSV exactly like it loads a county CSV.

---

## 13. Unified Stage 4: Upload to R2

The upload stage uses the existing **reusable action** at `.github/actions/upload-r2/action.yml`, which provides: 3-attempt retry with exponential backoff, automatic content-type detection (text/csv, application/json, application/gzip), gzip `Content-Encoding` for transparent browser decompression, MD5 hashing, and automatic `data/r2-manifest.json` updates. For local/manual uploads, `scripts/upload-to-r2.py` provides the same functionality via boto3.

**Note:** In v6, the statewide gzip upload was removed — validation and geocoding are handled on Cloudflare Workers.

### 4a: Upload County Jurisdiction CSVs

```bash
# Generate the file manifest (local_path → r2_key pairs)
MANIFEST=$(python scripts/split_jurisdictions.py \
  --state ${STATE} --r2-manifest --r2-prefix ${R2_PREFIX} --output-dir data/${DOT_NAME}/)

# Upload via the reusable action (in pipeline.yml, uses upload-r2/action.yml)
# Or via direct aws s3 cp with retry logic:
echo "$MANIFEST" | python3 -c "
import json, sys, subprocess, os
files = json.load(sys.stdin)
endpoint = os.environ['R2_ENDPOINT']
bucket = 'crash-lens-data'
success = failed = 0
for entry in files:
    local = entry['local_path']
    r2_key = entry['r2_key']
    if not os.path.exists(local):
        continue
    for attempt in range(3):
        result = subprocess.run([
            'aws', 's3', 'cp', local, f's3://{bucket}/{r2_key}',
            '--endpoint-url', endpoint,
            '--content-type', 'text/csv',
            '--only-show-errors'
        ], capture_output=True)
        if result.returncode == 0:
            success += 1; break
        import time; time.sleep(2 ** attempt)
    else:
        failed += 1
print(f'County CSVs: {success} ok, {failed} fail')
"
```

### 4b: Upload Region/MPO Aggregate CSVs

```bash
# Upload region aggregate CSVs (concat of member county rows)
for DIR in data/${DOT_NAME}/_region/*/; do
  REGION_ID=$(basename "$DIR")
  for CSV in "$DIR"*.csv; do
    FILENAME=$(basename "$CSV")
    aws s3 cp "$CSV" \
      "s3://crash-lens-data/${STATE}/_region/${REGION_ID}/${FILENAME}" \
      --endpoint-url ${R2_ENDPOINT} \
      --content-type "text/csv" \
      --only-show-errors || true
  done
done

# Upload MPO aggregate CSVs (same structure)
for DIR in data/${DOT_NAME}/_mpo/*/; do
  MPO_ID=$(basename "$DIR")
  for CSV in "$DIR"*.csv; do
    FILENAME=$(basename "$CSV")
    aws s3 cp "$CSV" \
      "s3://crash-lens-data/${STATE}/_mpo/${MPO_ID}/${FILENAME}" \
      --endpoint-url ${R2_ENDPOINT} \
      --content-type "text/csv" \
      --only-show-errors || true
  done
done
```

### 4c: Upload Federal Aggregate CSVs (Statewide Scope Only)

```bash
# Federal cross-state aggregate CSVs
if [ "$SCOPE" = "statewide" ]; then
  find "data/_federal" -name "*.csv" | while read f; do
    R2_KEY="_federal/${f#data/_federal/}"
    aws s3 cp "$f" "s3://crash-lens-data/$R2_KEY" \
      --endpoint-url ${R2_ENDPOINT} \
      --content-type "text/csv" \
      --only-show-errors || true
  done
fi
```

### 4d: Upload Raw Annual CSVs (State-Specific Workflows)

```bash
# Colorado: raw annual CSVs uploaded to {state}/{jurisdiction}/raw/{year}.csv
# This happens in state-specific download workflows, not in pipeline.yml
for f in data/CDOT/20*.csv; do
  YEAR=$(echo "$f" | grep -oP '^\d{4}')
  aws s3 cp "$f" "s3://crash-lens-data/${STATE}/${JURISDICTION}/raw/${YEAR}.csv" \
    --endpoint-url "$R2_ENDPOINT" \
    --content-type "text/csv"
done
```

### R2 Bucket Path Convention

**Bucket:** `crash-lens-data` — Public CDN: `https://data.aicreatesai.com`

The folder hierarchy is created by `scripts/create_r2_folders.py` (reads `states/{state}/hierarchy.json` for regions, MPOs, counties) and the upload uses the reusable `.github/actions/upload-r2/action.yml` action (with retry logic, MD5 hashing, content-type detection, gzip `Content-Encoding`, and automatic `data/r2-manifest.json` updates).

```
crash-lens-data/
  ── Top-Level (cross-state) ──────────────────────────────────────────
  _federal/                                    ← Federal cross-state aggregates
  _national/                                   ← National aggregate data
    snapshots/                                 ← National data snapshots
  shared/                                      ← Shared resources
    boundaries/                                ← Boundary/GIS data
    mutcd/                                     ← MUTCD-related resources

  ── Per-State (×51: 50 states + DC; also NYC as special sub-state) ──
  {state}/                                     ← e.g., colorado/, virginia/, maryland/
    _state/                                    ← Statewide raw data
      statewide_all_roads.csv.gz               ← full state CSV (gzip, legacy — no longer uploaded by pipeline v6)
    _statewide/                                ← Statewide aggregate metadata
      snapshots/                               ← Statewide data snapshots
    _region/{region_id}/                       ← DOT region/district aggregates
      {region_id}_all_roads.csv                ← concat of member county rows
      {region_id}_no_interstate.csv
      {region_id}_county_roads.csv
    _mpo/{mpo_id}/                             ← MPO/TPR aggregates
      {mpo_id}_all_roads.csv                   ← concat of member county rows
      {mpo_id}_no_interstate.csv
      {mpo_id}_county_roads.csv
    {jurisdiction}/                             ← Per-county data (atomic unit)
      standardized.csv                         ← Full Virginia-standard CSV (all columns)
      county_roads.csv                         ← Road-type split: county roads only
      no_interstate.csv                        ← Road-type split: no interstate
      all_roads.csv                            ← Road-type split: all roads
      forecasts_county_roads.json              ← SageMaker prediction matrices
      forecasts_no_interstate.json
      forecasts_all_roads.json
      raw/                                     ← Raw annual source CSVs (pre-conversion)
        {year}.csv                             ← e.g., 2021.csv, 2022.csv, 2024.csv
```

### Existing Infrastructure (Already Built)

| Component | File | Purpose |
|-----------|------|---------|
| Folder creator | `scripts/create_r2_folders.py` | Creates zero-byte marker objects for the full hierarchy (all 51 states) |
| Reusable upload action | `.github/actions/upload-r2/action.yml` | Upload with retry, MD5, manifest update, gzip support |
| R2 manifest | `data/r2-manifest.json` | Version 3 — tracks every file's size, MD5, upload time, gzip metadata |
| CLI upload tool | `scripts/upload-to-r2.py` | Local boto3-based upload for manual seeding |
| Folder creation workflow | `.github/workflows/create-r2-folders.yml` | One-time R2 hierarchy setup (manual dispatch) |
| Seed workflow | `.github/workflows/seed-r2.yml` | Upload existing local CSVs to R2 (manual dispatch) |
| Key rename workflow | `.github/workflows/rename-r2-keys.yml` | Fix legacy naming (e.g., remove redundant jurisdiction prefixes) |

### Local Path → R2 Key Mapping Examples

```
Local File                                          R2 Key
──────────────────────────────────────────          ──────────────────────────────────────
data/CDOT/douglas_standardized.csv              →   colorado/douglas/standardized.csv
data/CDOT/douglas_all_roads.csv                 →   colorado/douglas/all_roads.csv
data/CDOT/douglas_county_roads.csv              →   colorado/douglas/county_roads.csv
data/CDOT/douglas_no_interstate.csv             →   colorado/douglas/no_interstate.csv
data/CDOT/forecasts_county_roads.json           →   colorado/douglas/forecasts_county_roads.json
data/CDOT/2024 douglas.csv                      →   colorado/douglas/raw/2024.csv
data/CDOT/colorado_statewide_all_roads.csv.gz   →   colorado/_state/statewide_all_roads.csv.gz
data/henrico_all_roads.csv                      →   virginia/henrico/all_roads.csv
data/henrico_county_roads.csv                   →   virginia/henrico/county_roads.csv
data/henrico_no_interstate.csv                  →   virginia/henrico/no_interstate.csv
data/forecasts_county_roads.json                →   virginia/henrico/forecasts_county_roads.json
data/MarylandDOT/montgomery_standardized.csv    →   maryland/montgomery/standardized.csv
```

### State Prefix Naming Convention

State R2 prefixes are lowercase with underscores, derived from `scripts/create_r2_folders.py` `STATE_MAP`:
- Simple: `colorado`, `virginia`, `maryland`, `florida`, `texas`
- Multi-word: `new_york`, `north_carolina`, `south_carolina`, `west_virginia`, `district_of_columbia`
- Special: `nyc` (New York City — 5 boroughs, no `hierarchy.json`)

Jurisdiction keys are snake_case derived from county display names:
- `"Adams"` → `adams`, `"El Paso"` → `el_paso`, `"Prince George's"` → `prince_georges`, `"St. Mary's"` → `st_marys`
- Virginia independent cities keep suffix: `"Alexandria City"` → `alexandria_city`

---

## 14. Unified Stage 5: Predict (Forecast)

### Overview

Optional. Non-fatal. Produces 3 forecast JSON files per jurisdiction (one per road type) with 6 prediction matrices (M01-M06). Always uses real **Chronos-2 via AWS SageMaker** — there is no synthetic/dry-run mode.

Forecasting runs **per county**. Region/MPO-level forecasts are not generated in this version — the region/MPO aggregate CSVs provide the raw data for future region-level forecasting if needed.

### CLI Interface

```bash
python scripts/generate_forecast.py \
  --state ${STATE} \
  --all-road-types \
  --jurisdiction ${JURISDICTION} \
  --data-dir data/${DOT_NAME}/ \
  --source r2
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--state` | Yes | — | State name (e.g., `virginia`, `colorado`). Loads EPDO weights from `states/{state}/config.json` |
| `--all-road-types` | No | — | Generate forecasts for all 3 road types (county_roads, no_interstate, all_roads) |
| `--jurisdiction` | Yes (with `--all-road-types`) | — | Jurisdiction name prefix for data files |
| `--data-dir` | No | `data` | Directory containing road-type CSV files |
| `--source` | No | `local` | `local` reads from data-dir; `r2` downloads validated CSVs from R2 CDN first |
| `--data` | No | `data/crash_data.csv` | Path to crash CSV (single-file mode) |
| `--output` | No | `data/forecasts.json` | Output JSON path (single-file mode) |
| `--horizon` | No | `12` | Forecast horizon in months |

### Jurisdiction-Agnostic Design

The forecast script is fully state/jurisdiction-agnostic:

1. **EPDO weights** are loaded from `states/{state}/config.json` at runtime (not hardcoded). Falls back to FHWA 2025 weights (`K=883, A=94, B=21, C=11, O=1`) if the state config is missing.
2. **Top corridors** are auto-detected from the data via `auto_detect_top_corridors()` — no hardcoded route names.
3. **Data paths** are constructed from `--data-dir` and `--jurisdiction` arguments, not from fixed directory names.

### R2 Data Sourcing (`--source r2`)

When `--source r2` is specified, the script downloads the 3 road-type CSVs from the R2 CDN before processing:

```
https://data.aicreatesai.com/{state}/{jurisdiction}/county_roads.csv
https://data.aicreatesai.com/{state}/{jurisdiction}/no_interstate.csv
https://data.aicreatesai.com/{state}/{jurisdiction}/all_roads.csv
```

Files are saved locally as `{data-dir}/{jurisdiction}_{road_type}.csv` for processing. This ensures forecasts always use the latest validated data from R2 storage.

### SageMaker Endpoint Pre-Check

Before generating any forecasts, the script verifies the SageMaker endpoint (`crashlens-chronos2-endpoint`) is `InService`. If the endpoint is unavailable or not ready, the script exits with an error immediately rather than failing mid-forecast.

### Pipeline Integration (Stage 5 in `pipeline.yml`)

```python
cmd = ['python', 'scripts/generate_forecast.py',
       '--all-road-types', '--jurisdiction', j, '--data-dir', data_dir,
       '--state', state, '--source', 'r2']
```

The pipeline passes `--state` from `needs.prepare.outputs.state` and uses `--source r2` to ensure forecasts are generated from the latest validated R2 data. Failures are non-fatal — the pipeline continues with remaining jurisdictions.

### Output Files

| Road Type | Output Filename | R2 Upload Key |
|-----------|----------------|---------------|
| County/City Roads | `forecasts_county_roads.json` | `{state}/{jurisdiction}/forecasts_county_roads.json` |
| No Interstate | `forecasts_no_interstate.json` | `{state}/{jurisdiction}/forecasts_no_interstate.json` |
| All Roads | `forecasts_all_roads.json` | `{state}/{jurisdiction}/forecasts_all_roads.json` |

Each forecast JSON contains: `model: "amazon/chronos-2"`, state-specific `epdoWeights`, 6 prediction matrices (M01-M06), derived metrics, and backtesting results.

### Example Commands

```bash
# Virginia — Henrico (R2 source)
python scripts/generate_forecast.py \
  --state virginia --all-road-types --jurisdiction henrico \
  --data-dir data/VDOT --source r2

# Colorado — Douglas (local source)
python scripts/generate_forecast.py \
  --state colorado --all-road-types --jurisdiction douglas \
  --data-dir data/CDOT --source local
```

---

## 14b. Unified Stage 5b: Upload Forecast JSONs to R2

After forecast generation, the forecast JSON files are uploaded to R2 in a separate step from the main CSV upload (Stage 4). This separation allows the forecast upload to be skipped independently via `skip_forecasts`.

```bash
# For each jurisdiction and road type, upload forecast JSON
for j in ${JURISDICTIONS}; do
  for rt in county_roads no_interstate all_roads; do
    aws s3 cp "${DATA_DIR}/${j}/forecasts_${rt}.json" \
      "s3://crash-lens-data/${R2_PREFIX}/${j}/forecasts_${rt}.json" \
      --endpoint-url ${R2_ENDPOINT} \
      --content-type "application/json" \
      --only-show-errors || true
  done
done
```

---

## 15. Unified Stage 6: Manifest Update & Git Commit

### What Gets Committed

| File | Purpose |
|------|---------|
| `data/r2-manifest.json` | Version 3 manifest — tracks every R2 file's size, MD5, upload time, gzip metadata, local path mapping. The frontend uses `r2BaseUrl` + file key to construct CDN URLs |
| `data/{DOT_NAME}/.split_report.json` | Split statistics |

### R2 Manifest Structure (`data/r2-manifest.json`)

```json
{
  "version": 3,
  "r2BaseUrl": "https://data.aicreatesai.com",
  "updated": "2026-02-16T00:45:07.223028+00:00",
  "files": {
    "colorado/douglas/standardized.csv": {
      "size": 18447410,
      "md5": "908abfa0d4fcb690a28a25b38b8e625b",
      "uploaded": "2026-02-13T03:32:38.196289+00:00"
    },
    "colorado/_state/statewide_all_roads.csv.gz": {
      "size": 12345678,
      "md5": "...",
      "uploaded": "...",
      "contentEncoding": "gzip",
      "uncompressedSize": 98765432
    }
  },
  "localPathMapping": {
    "data/CDOT/douglas_standardized.csv": "colorado/douglas/standardized.csv",
    "data/henrico_all_roads.csv": "virginia/henrico/all_roads.csv"
  }
}
```

Version 3 added: `contentEncoding` and `uncompressedSize` fields for gzipped statewide files. The `localPathMapping` maps local repo file paths to R2 keys so the upload action can track provenance.

### Commit + Push with Retry

```bash
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

git add data/r2-manifest.json 2>/dev/null || true
git add data/*/.split_report.json 2>/dev/null || true

if ! git diff --cached --quiet; then
  SCOPE="${SCOPE}"
  SELECTION="${SELECTION:-all}"
  git commit -m "pipeline: ${STATE} ${SCOPE}/${SELECTION} — ${JURISDICTION_COUNT} jurisdictions ($(date -u '+%Y-%m-%d'))"

  for i in 1 2 3 4; do
    git push origin main && break
    sleep $((2 ** i))
    git fetch origin main
    git rebase origin/main || git pull --no-edit origin main
  done
fi
```

---

## 16. Unified Workflow YAML

### File: `.github/workflows/pipeline.yml`

```yaml
# ==============================================================================
# Crash Lens — Unified Pipeline v6
# ==============================================================================
# Processes Virginia-compatible CSVs through the generic pipeline.
# Auto-triggered by state-specific download workflows.
#
# Stage order: Init Cache → Split Jurisdiction → Split Road Type →
#              Aggregate (CSV) → Upload → Predict → Manifest
#
# NOTE: Validation runs in Stage 4.5 via headless Playwright (crash-data-validator-v13.html).
#
# Scope-aware: generates region/MPO aggregate CSVs based on user selection.
# ==============================================================================

name: "Pipeline: Process Crash Data"

on:
  workflow_dispatch:
    inputs:
      state:
        description: 'State to process'
        required: true
        type: choice
        options:
          - virginia
          - colorado
          - maryland

      scope:
        description: 'Processing scope'
        required: true
        type: choice
        options:
          - jurisdiction
          - region
          - mpo
          - statewide

      selection:
        description: 'Jurisdiction/region/MPO name (blank for statewide)'
        required: false
        default: ''
        type: string

      data_source:
        description: 'Path to Virginia-compatible CSV (leave blank for default location)'
        required: false
        default: ''
        type: string

      skip_forecasts:
        description: 'Skip forecast generation'
        required: false
        default: 'false'
        type: string

      dry_run:
        description: 'Dry run (no uploads, no commits)'
        required: false
        default: 'false'
        type: string

env:
  NOTIFICATION_EMAIL: ecomhub200@gmail.com

jobs:
  # ============================================================
  # JOB 1: Resolve scope + Prepare
  # ============================================================
  prepare:
    name: "Prepare (${{ github.event.inputs.state }})"
    runs-on: ubuntu-latest
    timeout-minutes: 30
    outputs:
      state: ${{ steps.resolve.outputs.state }}
      scope: ${{ steps.resolve.outputs.scope }}
      selection: ${{ steps.resolve.outputs.selection }}
      dot_name: ${{ steps.resolve.outputs.dot_name }}
      data_dir: ${{ steps.resolve.outputs.data_dir }}
      r2_prefix: ${{ steps.resolve.outputs.r2_prefix }}
      download_mode: ${{ steps.resolve.outputs.download_mode }}
      jurisdiction_count: ${{ steps.resolve.outputs.jurisdiction_count }}
      jurisdictions_json: ${{ steps.resolve.outputs.jurisdictions_json }}
      input_csv: ${{ steps.locate.outputs.input_csv }}

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install pandas requests openpyxl awscli

      - name: Resolve scope
        id: resolve
        run: |
          STATE="${{ github.event.inputs.state }}"
          SCOPE="${{ github.event.inputs.scope }}"
          SELECTION="${{ github.event.inputs.selection }}"

          RESOLVED=$(python scripts/resolve_scope.py \
            --state "$STATE" --scope "$SCOPE" --selection "$SELECTION" --json)

          DOT_NAME=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['dot_name'])")
          DATA_DIR=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['data_dir'])")
          R2_PREFIX=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['r2_prefix'])")
          DOWNLOAD_MODE=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['download_mode'])")
          JCOUNT=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.load(sys.stdin)['jurisdiction_count'])")
          JURISDICTIONS=$(echo "$RESOLVED" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['jurisdictions']))")

          echo "state=$STATE" >> $GITHUB_OUTPUT
          echo "scope=$SCOPE" >> $GITHUB_OUTPUT
          echo "selection=$SELECTION" >> $GITHUB_OUTPUT
          echo "dot_name=$DOT_NAME" >> $GITHUB_OUTPUT
          echo "data_dir=$DATA_DIR" >> $GITHUB_OUTPUT
          echo "r2_prefix=$R2_PREFIX" >> $GITHUB_OUTPUT
          echo "download_mode=$DOWNLOAD_MODE" >> $GITHUB_OUTPUT
          echo "jurisdiction_count=$JCOUNT" >> $GITHUB_OUTPUT
          echo "jurisdictions_json=$JURISDICTIONS" >> $GITHUB_OUTPUT

      - name: Locate input CSV
        id: locate
        run: |
          STATE="${{ steps.resolve.outputs.state }}"
          DATA_DIR="${{ steps.resolve.outputs.data_dir }}"
          DATA_SOURCE="${{ github.event.inputs.data_source }}"
          DOWNLOAD_MODE="${{ steps.resolve.outputs.download_mode }}"

          if [ -n "$DATA_SOURCE" ] && [ -f "$DATA_SOURCE" ]; then
            echo "input_csv=$DATA_SOURCE" >> $GITHUB_OUTPUT
          elif [ "$DOWNLOAD_MODE" = "statewide" ]; then
            CSV=$(ls "$DATA_DIR"/${STATE}_statewide_all_roads.csv 2>/dev/null | head -1)
            echo "input_csv=$CSV" >> $GITHUB_OUTPUT
          else
            # Single jurisdiction — find the specific CSV
            SELECTION="${{ steps.resolve.outputs.selection }}"
            CSV=$(ls "$DATA_DIR"/${SELECTION}_all_roads.csv 2>/dev/null | head -1)
            echo "input_csv=$CSV" >> $GITHUB_OUTPUT
          fi

  # ============================================================
  # JOB 2: Process (Stages 0-6)
  # ============================================================
  process:
    name: "Process (${{ needs.prepare.outputs.state }})"
    needs: prepare
    runs-on: ubuntu-latest
    timeout-minutes: 180

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pandas requests openpyxl awscli

      # ── Stage 0: Initialize Cache ──
      - name: "Stage 0: Initialize cache"
        run: |
          STATE="${{ needs.prepare.outputs.state }}"

          echo "=========================================="
          echo "Stage 0: Initialize state-isolated cache"
          echo "State: $STATE"
          echo "=========================================="

          python scripts/init_cache.py --state "$STATE"

      # ── Stage 1: Split by Jurisdiction (batch mode only) ──
      # NOTE: Validation runs as Stage 4.5 via headless Playwright. Old Stage 1/2 are now
      #       handled separately on Cloudflare Workers.
      - name: "Stage 1: Split by jurisdiction"
        if: needs.prepare.outputs.download_mode == 'statewide'
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"
          INPUT_CSV="${{ needs.prepare.outputs.input_csv }}"
          JURISDICTIONS='${{ needs.prepare.outputs.jurisdictions_json }}'

          echo "=========================================="
          echo "Stage 1: Split CSV into jurisdictions"
          echo "=========================================="

          SPLIT_INPUT="$INPUT_CSV"

          if [ -z "$SPLIT_INPUT" ] || [ ! -f "$SPLIT_INPUT" ]; then
            echo "WARNING: No input CSV found for splitting"
            exit 0
          fi

          ARGS="--state $STATE --input $SPLIT_INPUT --output-dir $DATA_DIR"

          JCOUNT="${{ needs.prepare.outputs.jurisdiction_count }}"
          TOTAL_STATE=$(python scripts/resolve_scope.py --state $STATE --scope statewide --json | \
                        python3 -c "import json,sys; print(json.load(sys.stdin)['jurisdiction_count'])")

          if [ "$JCOUNT" != "$TOTAL_STATE" ]; then
            JURIS_LIST=$(echo "$JURISDICTIONS" | python3 -c "import json,sys; print(' '.join(json.load(sys.stdin)))")
            ARGS="$ARGS --jurisdictions $JURIS_LIST"
          fi

          python scripts/split_jurisdictions.py $ARGS

      # ── Stage 2: Split by Road Type ──
      - name: "Stage 2: Split by road type"
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"
          JURISDICTIONS='${{ needs.prepare.outputs.jurisdictions_json }}'

          echo "=========================================="
          echo "Stage 2: Split by road type (3 CSVs per jurisdiction)"
          echo "=========================================="

          JURIS_LIST=$(echo "$JURISDICTIONS" | python3 -c "import json,sys; print(' '.join(json.load(sys.stdin)))")

          python scripts/split_road_type.py \
            --state "$STATE" \
            --jurisdictions $JURIS_LIST \
            --data-dir "$DATA_DIR" || {
            echo "WARNING: Road-type split had issues (non-fatal)"
          }

      # ── Stage 3: Aggregate by Scope (CSV) ──
      - name: "Stage 3: Aggregate by scope (CSV)"
        if: github.event.inputs.dry_run != 'true'
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          SCOPE="${{ needs.prepare.outputs.scope }}"
          SELECTION="${{ needs.prepare.outputs.selection }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"

          echo "=========================================="
          echo "Stage 3: Aggregate by scope (scope=$SCOPE, format=CSV)"
          echo "=========================================="

          # Scope-aware CSV aggregation
          python scripts/aggregate_by_scope.py \
            --state "$STATE" \
            --scope "$SCOPE" \
            --selection "$SELECTION" \
            --data-dir "$DATA_DIR" \
            --output-format csv \
            --output-dir "$DATA_DIR" || {
            echo "WARNING: State aggregates failed (non-fatal)"
          }

          # Federal cross-state aggregates (only for statewide scope)
          if [ "$SCOPE" = "statewide" ]; then
            python scripts/aggregate_by_scope.py \
              --federal \
              --output-format csv \
              --data-dir "$DATA_DIR" \
              --output-dir data || {
              echo "WARNING: Federal aggregates failed (non-fatal)"
            }
          fi

      # ── Stage 4: Upload to R2 ──
      - name: "Stage 4: Upload to R2"
        if: github.event.inputs.dry_run != 'true'
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: auto
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"
          R2_PREFIX="${{ needs.prepare.outputs.r2_prefix }}"
          SCOPE="${{ needs.prepare.outputs.scope }}"
          R2_ENDPOINT="https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com"
          R2_BUCKET="crash-lens-data"

          echo "=========================================="
          echo "Stage 4: Upload to R2 (all CSVs)"
          echo "=========================================="

          # 4a: Upload jurisdiction CSVs (county-level road-type splits)
          MANIFEST=$(python scripts/split_jurisdictions.py \
            --state "$STATE" --r2-manifest --r2-prefix "$R2_PREFIX" --output-dir "$DATA_DIR")

          echo "$MANIFEST" | python3 -c "
          import json, sys, subprocess, os
          files = json.load(sys.stdin)
          endpoint = '$R2_ENDPOINT'
          bucket = '$R2_BUCKET'
          success = failed = skipped = 0
          for i, entry in enumerate(files):
              local = entry['local_path']
              r2_key = entry['r2_key']
              if not os.path.exists(local):
                  skipped += 1
                  continue
              try:
                  subprocess.run([
                      'aws', 's3', 'cp', local, f's3://{bucket}/{r2_key}',
                      '--endpoint-url', endpoint,
                      '--content-type', 'text/csv',
                      '--only-show-errors'
                  ], check=True, capture_output=True, timeout=120)
                  success += 1
              except Exception as e:
                  print(f'  FAIL: {r2_key}: {e}')
                  failed += 1
              if (i + 1) % 50 == 0 or (i + 1) == len(files):
                  print(f'  Progress: {i+1}/{len(files)} ({success} ok, {failed} fail, {skipped} skip)')
          print(f'County CSV upload: {success} ok, {failed} fail, {skipped} skip')
          "

          # 4b: Upload region/MPO aggregate CSVs
          find "$DATA_DIR" -path "*/_region/*" -name "*.csv" -o -path "*/_mpo/*" -name "*.csv" 2>/dev/null | while read f; do
            REL="${f#$DATA_DIR/}"
            R2_KEY="$R2_PREFIX/$REL"
            aws s3 cp "$f" "s3://$R2_BUCKET/$R2_KEY" \
              --endpoint-url "$R2_ENDPOINT" \
              --content-type "text/csv" \
              --only-show-errors || echo "  WARN: failed to upload $R2_KEY"
          done

          # 4c: Upload federal aggregate CSVs (statewide scope only)
          if [ "$SCOPE" = "statewide" ]; then
            find "data/_federal" -name "*.csv" 2>/dev/null | while read f; do
              R2_KEY="_federal/${f#data/_federal/}"
              aws s3 cp "$f" "s3://$R2_BUCKET/$R2_KEY" \
                --endpoint-url "$R2_ENDPOINT" \
                --content-type "text/csv" \
                --only-show-errors || true
            done
          fi

          # NOTE: Statewide gzip upload removed — validation runs in Stage 4.5

      # ── Stage 5: Predict (Forecasts) ──
      - name: "Stage 5: Generate forecasts"
        if: github.event.inputs.skip_forecasts != 'true' && github.event.inputs.dry_run != 'true'
        timeout-minutes: 60
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_REGION: ${{ secrets.AWS_REGION }}
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"
          JURISDICTIONS='${{ needs.prepare.outputs.jurisdictions_json }}'

          echo "=========================================="
          echo "Stage 5: Forecast generation (per county only)"
          echo "=========================================="

          echo "$JURISDICTIONS" | python3 -c "
          import json, sys, subprocess
          jurisdictions = json.load(sys.stdin)
          state = '$STATE'
          data_dir = '$DATA_DIR'
          success = 0
          for j in jurisdictions:
              cmd = ['python', 'scripts/generate_forecast.py',
                     '--all-road-types', '--jurisdiction', j, '--data-dir', data_dir,
                     '--state', state, '--source', 'r2']
              result = subprocess.run(cmd, capture_output=True)
              if result.returncode == 0:
                  success += 1
              else:
                  print(f'    WARNING: Forecast failed for {j} (non-fatal)')
          print(f'Forecasts: {success}/{len(jurisdictions)} jurisdictions')
          "

      # ── Stage 5b: Upload forecast JSONs to R2 ──
      - name: "Stage 5b: Upload forecast JSONs to R2"
        if: github.event.inputs.skip_forecasts != 'true' && github.event.inputs.dry_run != 'true'
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: auto
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          DATA_DIR="${{ needs.prepare.outputs.data_dir }}"
          R2_PREFIX="${{ needs.prepare.outputs.r2_prefix }}"
          JURISDICTIONS='${{ needs.prepare.outputs.jurisdictions_json }}'
          R2_ENDPOINT="https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com"
          R2_BUCKET="crash-lens-data"

          echo "=========================================="
          echo "Stage 5b: Upload forecast JSONs to R2"
          echo "=========================================="

          UPLOADED=0
          SKIPPED=0

          echo "$JURISDICTIONS" | python3 -c "
          import json, sys, subprocess, os

          jurisdictions = json.load(sys.stdin)
          data_dir = '$DATA_DIR'
          r2_prefix = '$R2_PREFIX'
          endpoint = '$R2_ENDPOINT'
          bucket = '$R2_BUCKET'
          uploaded = 0
          skipped = 0

          for j in jurisdictions:
              for rt in ['county_roads', 'no_interstate', 'all_roads']:
                  local_path = os.path.join(data_dir, f'forecasts_{rt}.json')
                  # Also check jurisdiction-prefixed path
                  if not os.path.exists(local_path):
                      local_path = os.path.join(data_dir, j, f'forecasts_{rt}.json')
                  if not os.path.exists(local_path):
                      skipped += 1
                      continue

                  r2_key = f'{r2_prefix}/{j}/forecasts_{rt}.json'
                  try:
                      subprocess.run([
                          'aws', 's3', 'cp', local_path, f's3://{bucket}/{r2_key}',
                          '--endpoint-url', endpoint,
                          '--content-type', 'application/json',
                          '--only-show-errors'
                      ], check=True, capture_output=True, timeout=120)
                      size = os.path.getsize(local_path)
                      print(f'  Uploaded: {r2_key} ({size:,} bytes)')
                      uploaded += 1
                  except Exception as e:
                      print(f'  FAIL: {r2_key}: {e}')

          print(f'Forecast upload: {uploaded} uploaded, {skipped} skipped')
          "

      # ── Stage 6: Commit ──
      - name: "Stage 6: Commit manifest and metadata"
        if: github.event.inputs.dry_run != 'true'
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          SCOPE="${{ needs.prepare.outputs.scope }}"
          SELECTION="${{ needs.prepare.outputs.selection || 'all' }}"
          JCOUNT="${{ needs.prepare.outputs.jurisdiction_count }}"

          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add data/r2-manifest.json 2>/dev/null || true
          git add data/*/.split_report.json 2>/dev/null || true

          if git diff --cached --quiet; then
            echo "No metadata changes to commit"
          else
            git commit -m "pipeline: ${STATE} ${SCOPE}/${SELECTION} — ${JCOUNT} jurisdictions ($(date -u '+%Y-%m-%d'))"

            for i in 1 2 3 4; do
              if git push origin main; then
                echo "Push succeeded"
                break
              fi
              sleep $((2 ** i))
              git fetch origin main
              git rebase origin/main || git pull --no-edit origin main
            done
          fi

      # ── Summary ──
      - name: Job summary
        if: always()
        run: |
          STATE="${{ needs.prepare.outputs.state }}"
          SCOPE="${{ needs.prepare.outputs.scope }}"
          SELECTION="${{ needs.prepare.outputs.selection || 'all' }}"
          echo "## Pipeline Summary" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "| Metric | Value |" >> $GITHUB_STEP_SUMMARY
          echo "|--------|-------|" >> $GITHUB_STEP_SUMMARY
          echo "| State | $STATE |" >> $GITHUB_STEP_SUMMARY
          echo "| Scope | $SCOPE ($SELECTION) |" >> $GITHUB_STEP_SUMMARY
          echo "| Jurisdictions | ${{ needs.prepare.outputs.jurisdiction_count }} |" >> $GITHUB_STEP_SUMMARY
          echo "| Dry Run | ${{ github.event.inputs.dry_run }} |" >> $GITHUB_STEP_SUMMARY
          echo "| Stages | Init Cache → Split Jurisdiction → Split Road Type → Aggregate (CSV) → Upload → Predict → Manifest |" >> $GITHUB_STEP_SUMMARY
```

---

## 17. Scope Resolver Script

### File: `scripts/resolve_scope.py`

```python
#!/usr/bin/env python3
"""
Resolve pipeline scope to a concrete list of jurisdiction IDs.

Translates user selection (state + scope + selection) into:
  - A list of jurisdiction IDs to process
  - The download mode (statewide vs per-jurisdiction)
  - State config metadata (dot_name, data_dir, r2_prefix)

Usage:
    python scripts/resolve_scope.py --state virginia --scope jurisdiction --selection henrico
    python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads
    python scripts/resolve_scope.py --state colorado --scope mpo --selection drcog
    python scripts/resolve_scope.py --state virginia --scope statewide
    python scripts/resolve_scope.py --state virginia --list
    python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads --json
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
STATEWIDE_DOWNLOAD_THRESHOLD = 3


def load_hierarchy(state):
    path = PROJECT_ROOT / 'states' / state / 'hierarchy.json'
    if not path.exists():
        print(f"ERROR: No hierarchy.json for '{state}' at {path}", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def load_state_config(state):
    config_path = PROJECT_ROOT / 'config.json'
    with open(config_path) as f:
        config = json.load(f)
    states = config.get('states', {})
    if state not in states:
        for key, val in states.items():
            if key.lower() == state.lower():
                return val
        print(f"ERROR: State '{state}' not in config.json", file=sys.stderr)
        sys.exit(1)
    return states[state]


def get_all_counties(hierarchy):
    return sorted(hierarchy.get('counties', {}).keys())


def resolve_region(hierarchy, region_id):
    regions = hierarchy.get('regions', {})
    if region_id not in regions:
        for key in regions:
            if key.lower().replace('_', '') == region_id.lower().replace('_', ''):
                region_id = key
                break
        else:
            print(f"ERROR: Region '{region_id}' not found. Available: {', '.join(regions.keys())}", file=sys.stderr)
            sys.exit(1)
    county_codes = regions[region_id].get('counties', [])
    counties = hierarchy.get('counties', {})
    return [jid for code in county_codes for jid, jdata in counties.items()
            if jdata.get('fips') == code or jid == code]


def resolve_mpo(hierarchy, mpo_id):
    mpos = hierarchy.get('mpos', {})
    if mpo_id not in mpos:
        for key in mpos:
            if key.lower().replace('_', '') == mpo_id.lower().replace('_', ''):
                mpo_id = key
                break
        else:
            print(f"ERROR: MPO '{mpo_id}' not found. Available: {', '.join(mpos.keys())}", file=sys.stderr)
            sys.exit(1)
    county_codes = mpos[mpo_id].get('counties', [])
    counties = hierarchy.get('counties', {})
    return [jid for code in county_codes for jid, jdata in counties.items()
            if jdata.get('fips') == code or jid == code]


def resolve_scope(state, scope, selection, hierarchy):
    if scope == 'jurisdiction':
        if not selection:
            print("ERROR: --selection required for scope=jurisdiction", file=sys.stderr)
            sys.exit(1)
        return [selection]
    elif scope == 'region':
        if not selection:
            print("ERROR: --selection required for scope=region", file=sys.stderr)
            sys.exit(1)
        return resolve_region(hierarchy, selection)
    elif scope == 'mpo':
        if not selection:
            print("ERROR: --selection required for scope=mpo", file=sys.stderr)
            sys.exit(1)
        return resolve_mpo(hierarchy, selection)
    elif scope == 'statewide':
        return get_all_counties(hierarchy)
    else:
        print(f"ERROR: Unknown scope '{scope}'", file=sys.stderr)
        sys.exit(1)


def list_scopes(state, hierarchy):
    print(f"\n=== Available scopes for {state} ===\n")
    for section, label in [('regions', 'Regions'), ('mpos', 'MPOs')]:
        items = hierarchy.get(section, {})
        if items:
            print(f"{label}:")
            for rid, rdata in sorted(items.items()):
                name = rdata.get('name', rdata.get('shortName', rid))
                count = len(rdata.get('counties', []))
                print(f"  {rid}: {name} ({count} counties)")
    print(f"\nTotal counties: {len(hierarchy.get('counties', {}))}")


def main():
    parser = argparse.ArgumentParser(description='Resolve pipeline scope')
    parser.add_argument('--state', required=True)
    parser.add_argument('--scope', choices=['jurisdiction', 'region', 'mpo', 'statewide'])
    parser.add_argument('--selection', default='')
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()

    hierarchy = load_hierarchy(args.state)
    state_config = load_state_config(args.state)

    if args.list:
        list_scopes(args.state, hierarchy)
        return

    if not args.scope:
        print("ERROR: --scope required (or use --list)", file=sys.stderr)
        sys.exit(1)

    jurisdictions = resolve_scope(args.state, args.scope, args.selection, hierarchy)
    download_mode = 'statewide' if len(jurisdictions) > STATEWIDE_DOWNLOAD_THRESHOLD else 'individual'

    dot_name = state_config.get('dotName', state_config.get('dataDir', args.state.upper()))
    data_dir = f"data/{state_config.get('dataDir', dot_name)}"
    r2_prefix = state_config.get('r2Prefix', args.state)

    result = {
        'state': args.state,
        'scope': args.scope,
        'selection': args.selection,
        'download_mode': download_mode,
        'jurisdictions': jurisdictions,
        'jurisdiction_count': len(jurisdictions),
        'dot_name': dot_name,
        'data_dir': data_dir,
        'r2_prefix': r2_prefix
    }

    if args.json:
        print(json.dumps(result))
    else:
        print(f"State: {args.state}, Scope: {args.scope} ({args.selection})")
        print(f"Mode: {download_mode}, Jurisdictions ({len(jurisdictions)}): {', '.join(jurisdictions)}")


if __name__ == '__main__':
    main()
```

---

## 18. Configuration Per State

### 18.1 Files Required Per State

| File | Purpose |
|------|---------|
| `states/{state}/config.json` | Column mappings, severity derivation, road systems, cache config (update_frequency, stale_threshold) |
| `states/{state}/hierarchy.json` | Regions → counties, MPOs → counties (populates dropdowns) |
| `data/{DOT_NAME}/config.json` | Coordinate bounds, jurisdiction config |
| `data/{DOT_NAME}/source_manifest.json` | Data source URLs/IDs per county |
| `data/{DOT_NAME}/jurisdictions.json` | County definitions, bounding boxes |
| `data/{DOT_NAME}/CLAUDE.md` | State overview for Claude Code context |
| `download_{state}_crash_data.py` | State-specific download + merge + convert script |
| `.github/workflows/download-{state}.yml` | State workflow with dropdown UI |

### 18.2 Root Config Registration

```json
"states": {
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
}
```

### 18.3 Add State to pipeline.yml Dropdown

```yaml
state:
  type: choice
  options:
    - virginia
    - colorado
    - maryland
    - {new_state}    # ← Add here
```

---

## 19. Migration Plan: From 40 Workflows to 2

### Phase 1: Build (No Deletion)

1. Create `scripts/resolve_scope.py`
2. Update existing download scripts to include merge + convert phases
3. Create `.github/workflows/pipeline.yml` (unified processing)
4. Create `.github/workflows/download-virginia.yml` (with dropdowns)
5. Create `.github/workflows/download-colorado.yml` (with dropdowns)
6. Test Virginia: scope=jurisdiction, selection=henrico
7. Test Colorado: scope=statewide

### Phase 2: Validate

8. Run for Virginia statewide — compare output to `batch-all-jurisdictions.yml`
9. Run for Colorado statewide — compare output
10. Verify region aggregates (hampton_roads, richmond)
11. Verify MPO aggregates (drcog, hrtpo)
12. Verify R2 uploads match existing data

### Phase 3: Switch Over

13. Disable scheduled triggers on old workflows
14. Enable scheduled triggers on new state workflows
15. Run in production for 1 month alongside old workflows

### Phase 4: Cleanup (Future — User Decision)

16. Delete 28 disabled state framework workflows
17. Delete old per-state processing workflows
18. Archive `batch-all-jurisdictions.yml`

**Note:** Per user request, the 28 disabled state workflow files are NOT deleted now.

---

## 20. State Onboarding Checklist

### Step 1: Research (1-2 hours)
- [ ] Identify data source URL and API type
- [ ] Download sample file, inspect column headers
- [ ] Document column mapping → Virginia standardized columns
- [ ] Determine severity method (direct or derived)
- [ ] Determine if merge is needed (single file vs multi-file)
- [ ] Get coordinate bounds, FIPS codes, road system values

### Step 2: Configuration Files (1-2 hours)
- [ ] Create `states/{state}/config.json` (include `cache_config` with `update_frequency`, `typical_new_records_per_update`, `stale_threshold_days`, `geocode_ttl_days`)
- [ ] Create `states/{state}/hierarchy.json` (regions, MPOs, counties)
- [ ] Create `data/{DOT_NAME}/` folder with config.json, source_manifest.json, jurisdictions.json, CLAUDE.md
- [ ] Add state entry to root `config.json`
- [ ] Add state to `pipeline.yml` dropdown

### Step 3: Download + Convert Script (2-4 hours, with Claude Code)
- [ ] Copy closest API type template
- [ ] Configure API endpoint, authentication, pagination
- [ ] Implement merge phase (if multi-file state)
- [ ] Build column mapping, severity mapping, road system mapping
- [ ] Test: download sample → verify output matches Virginia format

### Step 4: State Workflow (30 min)
- [ ] Copy `download-{state}.yml` template
- [ ] Populate jurisdiction dropdown from hierarchy.json
- [ ] Populate region dropdown from hierarchy.json
- [ ] Populate MPO dropdown from hierarchy.json
- [ ] Set STATE, DOT_NAME, DATA_DIR env vars

### Step 5: Test Pipeline (1 hour)
- [ ] Run state workflow: scope=jurisdiction, selection={test_county}
- [ ] Verify pipeline.yml auto-triggered
- [ ] Verify all 7 stages (0-6) complete
- [ ] Test in browser: select new state → data loads

### Step 6: Go Live
- [ ] Run: scope=statewide
- [ ] Verify region aggregates generated
- [ ] Verify MPO aggregates generated
- [ ] Enable scheduled trigger

**Total estimated time per state: 6-10 hours.**

---

## Appendix A: Current vs Unified Comparison

| Aspect | Current (40 Workflows) | Unified v6 |
|--------|----------------------|------------|
| Workflow files | 40 | 1 per state + 1 pipeline.yml |
| User interface | Manual workflow_dispatch | Dropdown: scope + jurisdiction/region/MPO |
| Conversion logic | Scattered across files | In each state's download script |
| Adding a new state | Write workflow + download + config | Copy template + write download/convert + config |
| Changing processing | Update 30+ files | Update 1 file (pipeline.yml) |
| Scope selection | Hardcoded per workflow | Dynamic dropdowns from hierarchy.json |
| Aggregation | Only statewide | Scope-aware: statewide, region, MPO, jurisdiction |
| Auto-trigger | Manual or separate trigger | State workflow auto-triggers pipeline.yml |
| Who builds converter? | Developer | User with Claude Code |

## Appendix B: Download Script Interface Contract

```
Required arguments:
  --jurisdiction JURISDICTION    Filter to single county
  --output-dir DIR               Where to save Virginia-format CSVs
  --save-statewide               Download full state + save gzip

Optional:
  --list                         Print available jurisdictions and exit
  --years YEAR [YEAR ...]        Filter by year range
  --force-download               Skip cache, force re-download

Output: Virginia-compatible standardized CSV (see Section 3.6)
Stdout: Print "statewide_path={path}" when --save-statewide succeeds
Exit:   0 = success, 1 = failure
```

## Appendix C: Scope Resolution Examples

### Example 1: Single County
```
Input:  state=virginia, scope=jurisdiction, selection=henrico
Output: { jurisdictions: ["henrico"], download_mode: "individual" }
Aggregates: county-level only
```

### Example 2: Region
```
Input:  state=virginia, scope=region, selection=hampton_roads
Output: { jurisdictions: ["norfolk", "virginia_beach", ...], download_mode: "statewide" }
Aggregates: hampton_roads region aggregate + 11 member county summaries
```

### Example 3: MPO
```
Input:  state=colorado, scope=mpo, selection=drcog
Output: { jurisdictions: ["adams", "arapahoe", ...], download_mode: "statewide" }
Aggregates: DRCOG MPO aggregate + 7 member county summaries
```

### Example 4: Statewide
```
Input:  state=virginia, scope=statewide, selection=""
Output: { jurisdictions: [...all 133...], download_mode: "statewide" }
Aggregates: state totals + ALL regions + ALL MPOs + federal cross-state
```

## Appendix D: State-Specific Script Examples

### Virginia (Low Complexity)
```
Download: ArcGIS FeatureServer, single API call with pagination
Merge: Not needed
Convert: Rename columns, standardize dates
```

### Colorado (High Complexity)
```
Download: OnBase Document Portal, one Excel file per year
Merge: YES — combine multi-year Excel files
Convert: Complex severity derivation (injury hierarchy → crash severity)
```

### Maryland (Medium Complexity)
```
Download: Socrata SODA API, three separate dataset queries
Merge: YES — join crashes + drivers + non_motorists on crash_id
Convert: Field renaming, severity mapping, boolean derivation
```

## Appendix E: Aggregation by Scope (CSV)

### What Gets Generated Per Scope

All aggregates are **CSV files** — they contain the same Virginia-standard columns as county CSVs. A region or MPO CSV is a concatenation of all member county rows. The frontend reads them exactly like it reads a county CSV (same parser, same columns, just more rows).

| Scope | Aggregate CSV Files Generated | R2 Path |
|-------|-------------------------------|---------|
| `statewide` | All region CSVs (3 road types × N regions) | `{state}/_region/{id}/{id}_all_roads.csv` etc. |
| `statewide` | All MPO CSVs (3 road types × N MPOs) | `{state}/_mpo/{id}/{id}_all_roads.csv` etc. |
| `statewide` | Federal cross-state CSV | `_federal/all_states_all_roads.csv` |
| `region` | Region CSV (3 road types) | `{state}/_region/{selection}/{selection}_all_roads.csv` etc. |
| `region` | Region member county CSVs | `{state}/{county}/...` (already uploaded) |
| `mpo` | MPO CSV (3 road types) | `{state}/_mpo/{selection}/{selection}_all_roads.csv` etc. |
| `mpo` | MPO member county CSVs | `{state}/{county}/...` (already uploaded) |
| `jurisdiction` | County CSVs (3 road types) | `{state}/{county}/{county}_all_roads.csv` etc. |

### How Scope Flows Through the Pipeline

```
User picks: scope=region, selection=hampton_roads
     ↓
State workflow: downloads statewide, converts to Virginia format
     ↓
Pipeline Stage 0 (Init Cache): initializes state-isolated cache
Pipeline Stage 1 (Split Jurisdiction): splits into 11 Hampton Roads counties
Pipeline Stage 2 (Split Road Type): 3 CSVs × 11 = 33 county files
Pipeline Stage 3 (Aggregate CSV):
  → concat 11 counties into hampton_roads_all_roads.csv
  → concat 11 counties into hampton_roads_no_interstate.csv
  → concat 11 counties into hampton_roads_county_roads.csv
  → does NOT generate other regions or full statewide aggregates
Pipeline Stage 4 (Upload): uploads 33 county CSVs + 3 region CSVs to R2
Pipeline Stage 5 (Predict): generates forecasts for 11 counties (not region)
Pipeline Stage 5b (Upload Forecasts): uploads forecast JSONs to R2
Pipeline Stage 6 (Manifest): commits manifest + metadata
```

---

## Appendix F: Conversion Algorithm — Virginia Standard + Unmapped Columns

### The Conversion Contract

Every state's download script MUST produce a CSV where:

1. **Columns 1-51** (Group A) are the standard Virginia columns, in the exact order defined in Section 3.6. Every state MUST map these. If a state lacks data for a column, leave it empty (the pipeline handles empty values gracefully).

2. **Columns 52-69** (Group B) are Virginia infrastructure columns. Other states typically leave these empty. Colorado maps a few (e.g., `Ownership` from `System Code`). These columns are optional for non-Virginia states.

3. **After Group B**, append `_source_state` (always required), then any unmapped state-specific columns with the prefix `_{state_abbreviation}_`, then `_source_file` last.

### How `state_adapter.py` Implements This

The existing `scripts/state_adapter.py` already implements this pattern. Here is the algorithm:

```
STANDARD_COLUMNS = [
    'Document Nbr', 'Crash Date', 'Crash Year', 'Crash Military Time',
    'Crash Severity', 'K_People', 'A_People', 'B_People', 'C_People',
    'Collision Type', 'Weather Condition', 'Light Condition',
    'Roadway Surface Condition', 'Roadway Alignment',
    'Roadway Description', 'Intersection Type', 'Relation To Roadway',
    'RTE Name', 'SYSTEM', 'Node', 'RNS MP', 'x', 'y',
    'Physical Juris Name',
    'Pedestrian?', 'Bike?', 'Alcohol?', 'Speed?', 'Hitrun?',
    'Motorcycle?', 'Night?', 'Distracted?', 'Drowsy?', 'Drug Related?',
    'Young?', 'Senior?', 'Unrestrained?', 'School Zone', 'Work Zone Related',
    'Traffic Control Type', 'Traffic Control Status',
    'Functional Class', 'Area Type', 'Facility Type', 'Ownership',
    'First Harmful Event', 'First Harmful Event Loc',
    'Vehicle Count', 'Persons Injured', 'Pedestrians Killed', 'Pedestrians Injured',
    '_source_state', '_source_file'
]
```

### Conversion Algorithm (per row):

```python
def convert_row(raw_row, state_normalizer, state_key):
    # Step 1: Map standard columns using state normalizer
    normalized = state_normalizer.normalize_row(raw_row)
    # Returns dict with all STANDARD_COLUMNS mapped
    # e.g., Colorado: 'CUID' → 'Document Nbr', 'County' → 'Physical Juris Name'

    # Step 2: Set source tracking
    normalized['_source_state'] = state_key

    # Step 3: Identify unmapped columns from raw data
    mapped_raw_columns = set()  # track which raw columns were consumed
    for standard_col, raw_col in state_normalizer.COLUMN_MAPPING.items():
        mapped_raw_columns.add(raw_col)

    # Step 4: Append unmapped columns with _{state}_ prefix
    state_abbrev = STATE_ABBREVIATIONS[state_key]  # e.g., 'co' for colorado
    for raw_col in raw_row.keys():
        if raw_col not in mapped_raw_columns:
            prefixed_name = f'_{state_abbrev}_{sanitize(raw_col)}'
            normalized[prefixed_name] = raw_row[raw_col]

    # Step 5: Set source file
    normalized['_source_file'] = source_filename

    return normalized
```

### Real Example: Colorado CDOT → Virginia Standard

**Raw Colorado row (115 columns):**
```
CUID: 20240001234
Crash Date: 01/15/2024
County: Douglas
System Code: State Highway
MHE: Rear-End
Injury 04: 0
Injury 03: 0
Injury 02: 1
Injury 01: 0
TU-1 Direction: North
TU-1 Movement: Straight
TU-1 Type: Passenger Car
...
```

**Converted output (51 standard + unmapped):**
```
Document Nbr: 20240001234          ← mapped from CUID
Crash Date: 01/15/2024             ← passthrough
Crash Year: 2024                   ← extracted from date
Crash Severity: B                  ← derived from injury hierarchy (Injury 02 > 0)
K_People: 0                        ← from Injury 04
A_People: 0                        ← from Injury 03
B_People: 1                        ← from Injury 02
C_People: 0                        ← from Injury 01
Collision Type: 1. Rear End        ← mapped from MHE via 2-step lookup
Physical Juris Name: Douglas       ← from County
SYSTEM: Primary                    ← mapped from System Code
...
_source_state: colorado
_co_system_code: State Highway     ← unmapped: preserved raw System Code
_co_mhe: Rear-End                  ← unmapped: preserved raw MHE value
_co_tu1_direction: North           ← unmapped: unit-level detail not in standard
_co_tu1_movement: Straight         ← unmapped: unit-level detail not in standard
_co_tu1_vehicle_type: Passenger Car ← unmapped: unit-level detail
...
_source_file: cdot_2024.csv
```

### Why Unmapped Columns Matter

1. **Lossless conversion** — no state data is lost during conversion. Analysts can access the full raw data via the `_{state}_` columns.
2. **Future analysis** — if the tool later adds support for unit-level analysis (e.g., vehicle type breakdown), the data is already there.
3. **Debugging** — when a conversion looks wrong, the raw value is right there in the same row for comparison.
4. **State-specific dashboards** — Colorado users can build views that use `_co_tu1_vehicle_type` even though it's not a standard column.

### Adding a New State's Conversion

When onboarding a new state with Claude Code:

1. Download sample CSV from the state's data portal.
2. Inspect column headers — `StateDetector.detect_from_headers()` checks for signature columns.
3. Build the mapping: for each of the 51 standard columns, identify which raw column(s) provide the data.
4. For value translations (e.g., severity codes, weather codes), create lookup dictionaries that map raw values → VDOT numbered format.
5. Any raw columns that don't map to a standard column get the `_{state}_` prefix automatically.
6. Add the state to `STATE_SIGNATURES` and `_NORMALIZERS` in `state_adapter.py`.

---

## Appendix G: State-Isolated Incremental Cache Architecture

### Why Caching

A typical statewide dataset has 50,000-200,000 crash records. When new data arrives, only a fraction are new. Re-validating and re-geocoding 200,000 records when only 500 are new is wasteful. But since this pipeline serves **multiple states**, the cache must be intelligent enough to:

1. **Isolate by state** — running Colorado never touches Virginia's cache, and vice versa.
2. **Know update frequency** — Virginia updates daily, Colorado every 6 months, some states annually. The cache adapts.
3. **Persist across runs** — each state's cache grows over time and survives pipeline runs for other states.

### Cache Directory Structure (State-Isolated)

```
.cache/                                    ← Root cache directory (in repo root)
  _cache_registry.json                     ← Global registry of all state caches
  virginia/                                ← Virginia's cache (NEVER touched by other states)
    cache_manifest.json                    ← State cache metadata + update schedule
    validation/
      validated_hashes.json                ← { "Document Nbr": "row_hash", ... }
      validation_rules_hash.txt            ← sha256 of config.json validation rules
      last_run.json                        ← { timestamp, total, new, cached, errors }
    geocode/
      geocode_cache.json                   ← { "location_key": { x, y, method, confidence, cached_at }, ... }
      geocoded_records.json                ← { "Document Nbr": "location_key", ... }
      cache_stats.json                     ← { total_lookups, cache_hits, api_calls, hit_rate }
  colorado/                                ← Colorado's cache (completely independent)
    cache_manifest.json
    validation/
      validated_hashes.json
      validation_rules_hash.txt
      last_run.json
    geocode/
      geocode_cache.json
      geocoded_records.json
      cache_stats.json
  maryland/                                ← Maryland's cache
    cache_manifest.json
    validation/ ...
    geocode/ ...
```

**Key principle:** The `--state` parameter determines which `.cache/{state}/` directory is used. No state can read, write, or delete another state's cache. This is enforced by directory path — there is no shared cache file.

### Cache Manifest (Per State)

Each state has a `cache_manifest.json` that tracks metadata about the cache AND the state's data update patterns:

```json
{
  "state": "virginia",
  "dot_name": "VDOT",
  "cache_version": "1.0",
  "created_at": "2026-01-15T10:00:00Z",

  "update_schedule": {
    "frequency": "daily",
    "typical_new_records_per_update": 150,
    "typical_total_records": 200000,
    "data_retention_years": 5,
    "last_data_update": "2026-02-18T06:00:00Z",
    "next_expected_update": "2026-02-19T06:00:00Z"
  },

  "validation_cache": {
    "total_cached_records": 198500,
    "last_run": "2026-02-18T06:30:00Z",
    "last_run_new_records": 150,
    "last_run_changed_records": 12,
    "last_run_skipped_records": 198338,
    "config_rules_hash": "a3f8c2..."
  },

  "geocode_cache": {
    "total_cached_locations": 45000,
    "total_cached_records": 198500,
    "last_run": "2026-02-18T06:35:00Z",
    "last_run_api_calls": 30,
    "last_run_cache_hits": 120,
    "cumulative_api_calls": 52000,
    "cache_hit_rate": 0.96
  },

  "cache_health": {
    "status": "healthy",
    "disk_size_mb": 45,
    "oldest_entry": "2021-01-01",
    "stale_threshold_days": 365,
    "stale_entries_count": 0
  }
}
```

### Update Frequency Intelligence

Different states update their crash data at very different intervals. The cache adapts its behavior:

| State | Update Frequency | Typical New Records | Cache Strategy |
|-------|-----------------|--------------------|--------------------|
| Virginia | Daily | 100-200/day | Keep cache warm; expect small increments. Short stale threshold (30 days). |
| Colorado | Every 6 months | 20,000-30,000/batch | Large batch expected; pre-allocate memory. Long stale threshold (365 days). |
| Maryland | Monthly | 2,000-3,000/month | Medium batches. Moderate stale threshold (90 days). |
| Texas | Annually | 100,000+/batch | Full rebuild expected once/year. Very long stale threshold (548 days). |

**How this is configured:** The `update_schedule` section of `cache_manifest.json` is set during state onboarding (Step 2 in the Onboarding Checklist). The pipeline reads this to:

1. **Estimate expected new records** — if Virginia runs daily and the cache was updated yesterday, expect ~150 new records. If Colorado runs after 6 months, expect ~25,000 new records.
2. **Optimize memory allocation** — for small daily updates, stream records. For large batch updates, load cache into memory first.
3. **Detect anomalies** — if Virginia suddenly has 50,000 new records on a daily run, something may be wrong (data format changed, historical data re-sent). Log a warning.
4. **Set stale thresholds** — records older than `stale_threshold_days` (relative to the state's update frequency) are flagged for potential cleanup.
5. **Smart re-geocoding** — if a state hasn't been updated in 2× its expected interval, proactively re-geocode a sample of records to check if road network changes affect results.

### The Cache Manifest Update Frequency Configuration

When onboarding a new state, set the `update_schedule` in `states/{state}/config.json`:

```json
{
  "state": "new_state",
  "cache_config": {
    "update_frequency": "monthly",
    "typical_new_records_per_update": 2500,
    "data_retention_years": 5,
    "stale_threshold_days": 90,
    "geocode_ttl_days": 365,
    "max_cache_size_mb": 200
  }
}
```

| Field | Description |
|-------|-------------|
| `update_frequency` | One of: `daily`, `weekly`, `monthly`, `quarterly`, `biannual`, `annual` |
| `typical_new_records_per_update` | Estimated new records per update cycle |
| `data_retention_years` | How many years of data the state provides |
| `stale_threshold_days` | After this many days without update, warn about stale cache |
| `geocode_ttl_days` | How long to trust a geocoded location before re-checking |
| `max_cache_size_mb` | Maximum disk space for this state's cache before pruning oldest entries |

### Global Cache Registry

The `_cache_registry.json` at the root provides a dashboard view of all state caches:

```json
{
  "cache_version": "1.0",
  "states": {
    "virginia": {
      "status": "healthy",
      "last_run": "2026-02-18T06:30:00Z",
      "update_frequency": "daily",
      "validation_records": 198500,
      "geocode_locations": 45000,
      "disk_size_mb": 45,
      "next_expected": "2026-02-19T06:00:00Z"
    },
    "colorado": {
      "status": "healthy",
      "last_run": "2026-01-10T12:00:00Z",
      "update_frequency": "biannual",
      "validation_records": 24700,
      "geocode_locations": 8200,
      "disk_size_mb": 12,
      "next_expected": "2026-07-10T12:00:00Z"
    },
    "maryland": {
      "status": "stale",
      "last_run": "2025-11-01T08:00:00Z",
      "update_frequency": "monthly",
      "validation_records": 15000,
      "geocode_locations": 5500,
      "disk_size_mb": 8,
      "next_expected": "2025-12-01T08:00:00Z",
      "warning": "Cache not updated in 110 days (expected: monthly)"
    }
  }
}
```

This registry is updated automatically at the end of every pipeline run. It provides a single place to see which states have healthy caches, which are stale, and which need attention.

### Validation Cache Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  INPUT: statewide CSV (e.g., 200,000 rows)                       │
│  STATE: virginia                                                  │
│  CACHE: .cache/virginia/validation/validated_hashes.json          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Load cache_manifest.json → check update_frequency             │
│     If daily state + last_run was yesterday → expect ~150 new     │
│     If biannual state + last_run was 6 months ago → expect ~25K   │
│                                                                  │
│  2. For each row:                                                 │
│     a. Compute row_hash = sha256(standard columns only)           │
│        (exclude _{state}_ columns — raw format changes don't      │
│         invalidate validation of standard fields)                 │
│     b. Look up Document Nbr in validated_hashes.json              │
│     c. If hash matches → SKIP (already validated)                 │
│     d. If hash differs → RE-VALIDATE (data changed)               │
│     e. If not found → VALIDATE (new record)                       │
│                                                                  │
│  3. After all rows:                                               │
│     a. Update validated_hashes.json with new/changed hashes       │
│     b. Remove entries for Document Nbrs no longer in dataset      │
│     c. Update cache_manifest.json with run stats                  │
│     d. Update _cache_registry.json with state summary             │
│                                                                  │
│  OUTPUT: "[virginia] Validated 150 new + 12 changed records"      │
│          "[virginia] Skipped 198,338 cached records"              │
│          "[virginia] Cache: 198,500 total | 45 MB | healthy"      │
└──────────────────────────────────────────────────────────────────┘
```

### Geocoding Cache Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  INPUT: validated CSV (200,000 rows)                              │
│  STATE: virginia                                                  │
│  CACHE: .cache/virginia/geocode/geocode_cache.json                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Load cache_manifest.json → get geocode_ttl_days               │
│                                                                  │
│  2. For each row:                                                 │
│     a. If x (lon) and y (lat) already have valid values           │
│        → SKIP geocoding (coordinates from source data)            │
│                                                                  │
│     b. If coordinates missing:                                    │
│        i.  Build location_key from:                               │
│            "{RTE Name}|{Node}|{Physical Juris Name}|{RNS MP}"    │
│        ii. Check .cache/virginia/geocode/geocode_cache.json       │
│            → If found AND cached_at < geocode_ttl_days:           │
│              use cached x, y (CACHE HIT)                          │
│            → If found BUT cached_at > geocode_ttl_days:           │
│              re-geocode and update cache (STALE REFRESH)          │
│            → If not found: run 3-strategy geocoding               │
│              and store result in virginia's cache                  │
│                                                                  │
│     c. Update geocoded_records.json with Document Nbr → key       │
│                                                                  │
│  3. Update cache_manifest.json + _cache_registry.json              │
│                                                                  │
│  OUTPUT: "[virginia] Geocoded 30 new records (API calls: 30)"     │
│          "[virginia] Cache hits: 120 | Stale refreshed: 5"        │
│          "[virginia] Already had coordinates: 199,845"            │
└──────────────────────────────────────────────────────────────────┘
```

### Cache Performance by State Update Pattern

| State | Frequency | First Run | Subsequent Runs | Cache Size |
|-------|-----------|-----------|-----------------|------------|
| Virginia (daily) | Daily | 4 hours (200K records, 50K API calls) | 30 seconds (150 new, 30 API calls) | ~45 MB |
| Colorado (biannual) | Every 6 months | 2 hours (25K records, 8K API calls) | 1.5 hours (25K new, 6K API calls) | ~12 MB |
| Maryland (monthly) | Monthly | 1 hour (15K records, 4K API calls) | 10 min (3K new, 600 API calls) | ~8 MB |
| Texas (annual) | Annually | 8 hours (300K records, 80K API calls) | 8 hours (rebuilt each year) | ~100 MB |
| Re-run (any state, no changes) | N/A | N/A | 15 seconds (0 new, 0 API calls) | No change |

### Cache Invalidation Rules

| Trigger | Scope | Validation Cache | Geocoding Cache |
|---------|-------|-----------------|-----------------|
| `--force-validate` flag | Single state only | Cleared | Kept |
| `--force-geocode` flag | Single state only | Kept | Cleared |
| `config.json` rules change | Single state only | Auto-cleared | Kept |
| Road network data update | Single state only | Kept | Auto-cleared |
| `geocode_ttl_days` exceeded | Per-entry | Kept | Entry refreshed on next access |
| `stale_threshold_days` exceeded | Single state only | Warning logged | Warning logged |
| `max_cache_size_mb` exceeded | Single state only | Oldest entries pruned | Oldest entries pruned |
| Manual: delete `.cache/{state}/` | Single state only | Cleared | Cleared |
| Manual: delete `.cache/` | ALL states | All cleared | All cleared |

**Critical:** Every invalidation action is scoped to a single state. There is no operation that accidentally clears another state's cache (except deleting the entire `.cache/` root).

### How Cache Identifies "New" Records

The `Document Nbr` (crash ID) is the primary key. The cache tracks per state:

1. **New record**: `Document Nbr` not in `.cache/{state}/` → full validation + geocoding.
2. **Changed record**: `Document Nbr` found but row hash differs → re-validate, check if coordinates changed.
3. **Unchanged record**: `Document Nbr` found and row hash matches → skip entirely.
4. **Deleted record**: `Document Nbr` in cache but not in new dataset → remove from cache (cleanup).
5. **Stale geocode**: `Document Nbr` found, hash matches, but geocode `cached_at` exceeds `geocode_ttl_days` → re-geocode only.

This means even if a state re-sends the entire dataset (common with bulk downloads and annual states), only genuinely new or modified records are processed. And it never interferes with any other state's cache.

### Anomaly Detection

The cache uses `update_schedule` to detect unexpected situations:

| Anomaly | Detection | Action |
|---------|-----------|--------|
| Daily state has 50K new records | `new_records > 10 × typical_new_records_per_update` | Log WARNING: "Virginia: 50,000 new records detected (expected ~150). Possible data format change or historical data re-send." |
| Biannual state has 0 new records | `new_records == 0 AND days_since_last_update > expected_interval` | Log INFO: "Colorado: No new records. Last update was 7 months ago (expected: 6 months)." |
| Cache growing beyond limit | `disk_size_mb > max_cache_size_mb` | Auto-prune: remove validation hashes for records not seen in last `data_retention_years`. Remove geocode entries not accessed in last `geocode_ttl_days × 2`. |
| Geocode hit rate dropping | `hit_rate < 0.5 AND previous_hit_rate > 0.8` | Log WARNING: "Virginia geocode cache hit rate dropped from 96% to 45%. Road network may have changed." |

---

*End of Document — Crash Lens Unified Pipeline Architecture v6.0*
