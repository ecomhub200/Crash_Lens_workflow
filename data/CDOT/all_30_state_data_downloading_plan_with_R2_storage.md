# All 30 State Data Downloading Plan with R2 Storage

## Context

CRASH LENS currently supports Virginia and Colorado crash data pipelines. The goal is to expand to **30 states** by creating per-state data pipeline folders under `data/`, each containing pipeline documentation, configuration, download scripts, and a CLAUDE.md for autonomous AI agent work. All data normalizes to Virginia VDOT reference format and stores in Cloudflare R2 as gzip-compressed CSV.

**Scope:** 29 new state DOT folders + 1 CLAUDE.md for existing CDOT = ~176+ new files

**Key decisions (confirmed by user):**
- **Folder naming:** Full DOT names (e.g., `data/OhioDOT/`, `data/OregonDOT/`, `data/MarylandDOT/`)
- **Download scripts:** One standalone script per state (not shared templates)
- **GitHub Actions:** Each state gets its **own dedicated workflow** file (following Virginia `download-data.yml` and Colorado `download-cdot-crash-data.yml` patterns)
- **Column mappings:** Best-effort real mappings by finding and using each state's **data dictionary** for their crash dataset
- **R2 storage:** All CSVs gzip-compressed (`.csv.gz`) per R2 integration plan in `data/CDOT/r2-integration-plan.md`

---

## State Roster (30 States by API Type)

### Socrata SODA API (6 states)
| State | DOT | Folder | API Endpoint |
|-------|-----|--------|-------------|
| Maryland | MDOT SHA | `data/MarylandDOT/` | `opendata.maryland.gov/resource/65du-s3qu.json` |
| Connecticut | CTDOT | `data/ConnecticutDOT/` | `data.ct.gov` |
| New York | NYSDOT | `data/NewYorkDOT/` | `data.ny.gov` |
| NYC | NYC DOT | `data/NYCDOT/` | `data.cityofnewyork.us` |
| Delaware | DelDOT | `data/DelawareDOT/` | `data.delaware.gov` |
| Hawaii | HDOT | `data/HawaiiDOT/` | `data.hawaii.gov` |

### ArcGIS GeoServices API (20 states)
| State | DOT | Folder | Portal |
|-------|-----|--------|--------|
| Iowa | Iowa DOT | `data/IowaDOT/` | `data.iowadot.gov` |
| Illinois | IDOT | `data/IllinoisDOT/` | `gis-idot.opendata.arcgis.com` |
| Louisiana | LADOTD | `data/LouisianaDOT/` | `data-ladotd.opendata.arcgis.com` |
| Alaska | AKDOT | `data/AlaskaDOT/` | `data-soa-akdot.opendata.arcgis.com` |
| Massachusetts | MassDOT | `data/MassachusettsDOT/` | `massdot-impact-crashes-vhb.opendata.arcgis.com` |
| Pennsylvania | PennDOT | `data/PennsylvaniaDOT/` | `crashinfo.penndot.pa.gov` |
| Florida | FDOT | `data/FloridaDOT/` | `gis-fdot.opendata.arcgis.com` |
| Georgia | GDOT | `data/GeorgiaDOT/` | `gdot.aashtowaresafety.net` |
| South Carolina | SCDOT | `data/SouthCarolinaDOT/` | `fatality-count-scdps.hub.arcgis.com` |
| Ohio | ODOT | `data/OhioDOT/` | `gis.dot.state.oh.us/tims` |
| Wisconsin | WisDOT | `data/WisconsinDOT/` | `data-wisdot.opendata.arcgis.com` |
| Nevada | NDOT | `data/NevadaDOT/` | `data-ndot.opendata.arcgis.com` |
| Utah | UDOT | `data/UtahDOT/` | `data-uplan.opendata.arcgis.com` |
| Oregon | ODOT | `data/OregonDOT/` | Oregon TransGIS |
| Washington | WSDOT | `data/WashingtonDOT/` | `geo.wa.gov` |
| Idaho | ITD | `data/IdahoDOT/` | `data-iplan.opendata.arcgis.com` |
| Montana | MDT | `data/MontanaDOT/` | `gis-mdt.opendata.arcgis.com` |
| West Virginia | WVDOT | `data/WestVirginiaDOT/` | `data-wvdot.opendata.arcgis.com` |
| Mississippi | MDOT | `data/MississippiDOT/` | `gis-mdot.opendata.arcgis.com` |
| Oklahoma | OKDOT | `data/OklahomaDOT/` | `gis-okdot.opendata.arcgis.com` |

### Custom REST API (1 state)
| State | DOT | Folder | API |
|-------|-----|--------|-----|
| Vermont | VTrans | `data/VermontDOT/` | `apps.vtrans.vermont.gov/crashdata` |

### CRIS Bulk Download (1 state)
| State | DOT | Folder | Portal |
|-------|-----|--------|--------|
| Texas | TxDOT | `data/TexasDOT/` | `cris.dot.state.tx.us` |

### Already Exists (update only)
| State | DOT | Folder | Action |
|-------|-----|--------|--------|
| Colorado | CDOT | `data/CDOT/` | Add CLAUDE.md only |

---

## Per-State Deliverables (5 files + 1 workflow each)

### 1. `CLAUDE.md` — AI Agent Instructions
State-specific instructions for Claude Code agents working in that folder:
- DOT name, abbreviation, state FIPS code
- API endpoints and auth requirements
- Column mapping summary (raw → VDOT standardized) based on state data dictionary
- Severity mapping method (direct column vs derived from injury counts)
- VDOT value mapping key differences from that state's raw data
- Jurisdiction filtering approach (county FIPS codes, jurisdiction codes)
- Data format notes (CSV/Excel/JSON, date formats, coordinate system)
- Gzip compression requirement for R2 storage
- Pipeline stage notes specific to that state

### 2. `config.json` — Column Mappings & State Config
Following `data/CDOT/config.json` structure exactly. **Column names sourced from each state's data dictionary:**
```json
{
  "state": { "name", "abbreviation", "fips", "dotName", "dotFullName", "coordinateBounds", "dataDir" },
  "columnMapping": { "ID": "actual_state_col_name", "DATE": "actual_date_col", ... },
  "derivedFields": { "SEVERITY": { "method": "...", "sources": [...] }, ... },
  "roadSystems": { "values": {...}, "filterProfiles": {...} },
  "crashTypeMapping": { "StateValue": "StandardValue", ... },
  "epdoWeights": { "K": 883, "A": 94, "B": 21, "C": 11, "O": 1 },
  "validValues": { "severity": {...}, "roadCondition": [...] },
  "dataSource": { "name", "url", "apiUrl", "fileFormat", "dataDictionary": "url_or_reference" }
}
```

### 3. `source_manifest.json` — Dataset & Jurisdiction Registry
Following `data/CDOT/source_manifest.json` structure:
```json
{
  "_description": "...",
  "source": { "name", "base_url", "file_format" },
  "jurisdiction_filters": {
    "county_key": { "county": "NAME", "fips": "XXXXX", "display_name": "Name County" }
  },
  "api": { "endpoint", "resource_id", "pagination", "max_records" }
}
```
All counties with FIPS codes for each state.

### 4. `PIPELINE_ARCHITECTURE.md` — Pipeline Documentation
Following CDOT `PIPELINE_ARCHITECTURE.md` pattern:
- State-specific data download method and API details
- Data dictionary reference and where to find it
- Column mapping reference table (from data dictionary)
- Value mapping tables (raw state values → VDOT vocabulary)
- Derived fields computation rules
- Validation rules and state-specific bounds
- Geocoding strategy and caching
- R2 storage paths with gzip compression
- GitHub Actions workflow reference

### 5. `download_{state}_crash_data.py` — Standalone Download Script
One self-contained script per state with:
- `--jurisdiction`, `--years`, `--data-dir`, `--output-format` arguments
- `--health-check` flag for API connectivity testing
- `--gzip` flag for compressed output (`.csv.gz`) for R2 storage
- County/FIPS filtering built into API query
- Retry logic with exponential backoff (2s, 4s, 8s, 16s)
- Logging with state-specific context
- CSV output compatible with pipeline Stage 1 (CONVERT)

### 6. `.github/workflows/download-{state}-crash-data.yml` — Dedicated Workflow
Each state gets its own GitHub Actions workflow file following the CDOT pattern:
- Schedule (monthly or as appropriate for data refresh cadence)
- `workflow_dispatch` with jurisdiction dropdown for that state's counties
- State-specific `R2_STATE_PREFIX` and `STATE_DISPLAY` env vars
- Steps: checkout → setup Python → install deps → build args → download → stats → R2 upload → verify → commit manifest → summary
- R2 upload step using `aws s3 cp` with `--endpoint-url` (same pattern as CDOT workflow)
- Git push with retry logic (exponential backoff, 4 attempts)

---

## Column Mapping Strategy: Data Dictionary Research

For each state, the column mapping in `config.json` must be based on the state's actual **data dictionary**. The research approach per API type:

### Socrata States
- Use `GET https://{domain}/api/views/{resource_id}.json` to get dataset metadata
- Field names come from `columns[].fieldName` in the metadata response
- Data dictionary often available as a companion dataset or PDF on the portal

### ArcGIS States
- Use `GET {featureServerUrl}?f=json` to get layer metadata
- Field names come from `fields[].name` in the response
- Some portals publish data dictionaries alongside the data

### Custom/Bulk States
- Vermont: API documentation at `apps.vtrans.vermont.gov/crashdata`
- Texas: CRIS data dictionary available at `cris.dot.state.tx.us` (requires login)

### Column Mapping Priority
For each state, map these critical fields (in order of importance):
1. **ID** — Crash record identifier
2. **DATE** — Crash date
3. **LATITUDE/LONGITUDE** — GPS coordinates
4. **SEVERITY** — KABCO severity (direct or derived)
5. **ROUTE** — Road/route name
6. **COLLISION** — Collision type
7. **WEATHER** — Weather conditions
8. **LIGHT** — Lighting conditions
9. **PED/BIKE** — Pedestrian/bicycle flags
10. **COUNTY/FIPS** — Jurisdiction identifier

---

## Implementation Phases

### Phase 1: Foundation (7 files)
1. **`data/CDOT/CLAUDE.md`** — Create CLAUDE.md for existing CDOT folder
   - Serves as reference template for all other state CLAUDE.md files

2. **Maryland (first new state)** — 6 files total:
   - `data/MarylandDOT/CLAUDE.md`
   - `data/MarylandDOT/config.json` (columns from MD Socrata data dictionary)
   - `data/MarylandDOT/source_manifest.json` (24 MD counties + Baltimore City)
   - `data/MarylandDOT/PIPELINE_ARCHITECTURE.md`
   - `data/MarylandDOT/download_maryland_crash_data.py`
   - `.github/workflows/download-maryland-crash-data.yml`

   Maryland is done first as it has the best-documented Socrata API.

### Phase 2: Remaining Socrata States (5 states = 30 files)
Connecticut → Delaware → New York → NYC → Hawaii

Each state gets 6 files (5 in data folder + 1 workflow). Socrata states first because:
- SODA API is well-documented with discoverable schemas
- Column names readily available via metadata endpoints
- Simplest pagination model (`$limit`/`$offset`)

### Phase 3: ArcGIS States — Tier 1 (4 states = 24 files)
Iowa → Illinois → Louisiana → Alaska

These have the most complete ArcGIS Feature Server APIs with full public access.
Column mappings from Feature Server `?f=json` metadata + state data dictionaries.

### Phase 4: ArcGIS States — HIGH Priority (4 states = 24 files)
Massachusetts → Florida → Washington → Nevada

### Phase 5: ArcGIS States — MEDIUM Priority (5 states = 30 files)
Pennsylvania → Georgia → South Carolina → Ohio → Wisconsin

### Phase 6: ArcGIS States — Remaining (7 states = 42 files)
Utah → Oregon → Idaho → Montana → West Virginia → Mississippi → Oklahoma

### Phase 7: Custom API States (2 states = 12 files)
Vermont (Custom REST API) → Texas (CRIS Bulk Download)

### Phase 8: Gzip Compression Integration (modify existing files)
- Update `.github/actions/upload-r2/action.yml` to handle `.csv.gz` files
- All download scripts include `--gzip` flag for compressed output
- Update `data/r2-manifest.json` to document `.csv.gz` path patterns

---

## Per-State Workflow Pattern

Each state's workflow (`.github/workflows/download-{state}-crash-data.yml`) follows the existing CDOT pattern from `.github/workflows/download-cdot-crash-data.yml`:

```yaml
name: Download {State} Crash Data

# Separate workflow for {State DOT} crash data downloads.
# Modeled after download-cdot-crash-data.yml

on:
  schedule:
    # 1st of every month at 11:00 AM UTC — checks for new data
    - cron: '0 11 1 * *'
  workflow_dispatch:
    inputs:
      jurisdiction:
        description: 'County to filter to'
        required: false
        default: '{default_county}'
        type: choice
        options:
          - {list of all state counties}
      years:
        description: 'Comma-separated years to download (e.g., "2024,2025"). Leave empty for latest only.'
        required: false
        default: ''
        type: string
      force_download:
        description: 'Force re-download even if data already exists'
        required: false
        type: boolean
        default: false

permissions:
  contents: write

env:
  R2_STATE_PREFIX: {state_lowercase}
  STATE_DISPLAY: {State Name}

jobs:
  download-{state}-data:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Build download arguments
        id: build-args
        run: |
          ARGS="--data-dir data/{StateDOT}"
          JURISDICTION="${{ github.event.inputs.jurisdiction || '{default_county}' }}"
          ARGS="$ARGS --jurisdiction $JURISDICTION"
          YEARS="${{ github.event.inputs.years }}"
          if [ -n "$YEARS" ]; then
            YEARS_SPACED=$(echo "$YEARS" | tr ',' ' ')
            ARGS="$ARGS --years $YEARS_SPACED"
          fi
          FORCE="${{ github.event.inputs.force_download }}"
          if [ "$FORCE" = "true" ]; then
            ARGS="$ARGS --force"
          fi
          ARGS="$ARGS --gzip"
          echo "args=$ARGS" >> $GITHUB_OUTPUT
          echo "jurisdiction=$JURISDICTION" >> $GITHUB_OUTPUT

      - name: Download {State} crash data
        id: download
        run: |
          echo "=========================================="
          echo "Downloading ${STATE_DISPLAY} Crash Data"
          echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
          echo "=========================================="
          python data/{StateDOT}/download_{state}_crash_data.py ${{ steps.build-args.outputs.args }}

      - name: Generate download statistics
        id: stats
        run: |
          echo "Files in data/{StateDOT}/:"
          ls -lh data/{StateDOT}/*.csv* 2>/dev/null || echo "  No CSV files found"
          CHANGED=$(git diff --name-only data/{StateDOT}/ 2>/dev/null | wc -l | tr -d ' ')
          UNTRACKED=$(git ls-files --others --exclude-standard data/{StateDOT}/ 2>/dev/null | wc -l | tr -d ' ')
          echo "changed=$CHANGED" >> $GITHUB_OUTPUT
          echo "untracked=$UNTRACKED" >> $GITHUB_OUTPUT

      - name: Upload CSVs to R2 (gzip compressed)
        id: r2-upload
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          R2_ENDPOINT: "https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com"
        run: |
          JURISDICTION="${{ steps.build-args.outputs.jurisdiction }}"
          UPLOADED=0
          for f in data/{StateDOT}/*.csv.gz; do
            [ -f "$f" ] || continue
            BASENAME=$(basename "$f" .csv.gz)
            if aws s3 cp "$f" "s3://crash-lens-data/${R2_STATE_PREFIX}/${JURISDICTION}/${BASENAME}.csv.gz" \
              --endpoint-url "$R2_ENDPOINT" \
              --content-type "application/gzip"; then
              echo "  Uploaded: $f -> ${R2_STATE_PREFIX}/${JURISDICTION}/${BASENAME}.csv.gz"
              UPLOADED=$((UPLOADED + 1))
            fi
          done
          echo "uploaded=$UPLOADED" >> $GITHUB_OUTPUT

      - name: Verify R2 state
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          R2_ENDPOINT: "https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com"
        run: |
          JURISDICTION="${{ steps.build-args.outputs.jurisdiction }}"
          aws s3 ls "s3://crash-lens-data/${R2_STATE_PREFIX}/${JURISDICTION}/" \
            --endpoint-url "$R2_ENDPOINT" 2>/dev/null || echo "  Could not list R2 bucket"

      - name: Commit manifest and metadata
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "github-actions[bot]"
          git add data/r2-manifest.json
          git add data/{StateDOT}/.validation/ 2>/dev/null || true
          if git diff --staged --quiet; then
            echo "No changes to commit"
            exit 0
          fi
          JURISDICTION="${{ steps.build-args.outputs.jurisdiction }}"
          git commit -m "chore: update $STATE_DISPLAY crash data ($JURISDICTION) -> R2 [$(date +'%Y-%m-%d')]"
          MAX_RETRIES=4
          for i in $(seq 1 $MAX_RETRIES); do
            if git push; then echo "Push successful"; exit 0; fi
            sleep $((2 ** i))
            git fetch origin main
            git rebase origin/main || { git rebase --abort 2>/dev/null || true; git pull --no-edit origin main; }
          done
          echo "ERROR: Push failed after $MAX_RETRIES attempts"
          exit 1

      - name: Job summary
        if: always()
        run: |
          JURISDICTION="${{ steps.build-args.outputs.jurisdiction }}"
          UPLOADED="${{ steps.r2-upload.outputs.uploaded || '0' }}"
          cat >> $GITHUB_STEP_SUMMARY <<EOF
          ## ${STATE_DISPLAY} Crash Data Download — ${JURISDICTION}
          | Metric | Value |
          |--------|-------|
          | Jurisdiction | ${JURISDICTION} |
          | CSVs uploaded to R2 | ${UPLOADED} |
          | Run time | $(date -u '+%Y-%m-%d %H:%M:%S UTC') |
          EOF
```

---

## Critical Files to Create/Modify

| File | Action | Count |
|------|--------|-------|
| `data/CDOT/CLAUDE.md` | **CREATE** | 1 |
| `data/{29 StateDOTs}/CLAUDE.md` | **CREATE** | 29 |
| `data/{29 StateDOTs}/config.json` | **CREATE** | 29 |
| `data/{29 StateDOTs}/source_manifest.json` | **CREATE** | 29 |
| `data/{29 StateDOTs}/PIPELINE_ARCHITECTURE.md` | **CREATE** | 29 |
| `data/{29 StateDOTs}/download_{state}_crash_data.py` | **CREATE** | 29 |
| `.github/workflows/download-{state}-crash-data.yml` | **CREATE** | 29 |
| `.github/actions/upload-r2/action.yml` | **MODIFY** | 1 |
| `data/r2-manifest.json` | **MODIFY** | 1 |

**Total new files: ~176** (1 CDOT CLAUDE.md + 145 state data files + 29 workflows + 1 gzip integration)

---

## Key Patterns to Reuse

| Pattern | Source File | Reuse For |
|---------|-----------|-----------|
| Config structure | `data/CDOT/config.json` | All 29 state config.json files |
| Manifest structure | `data/CDOT/source_manifest.json` | All 29 source_manifest.json files |
| Pipeline docs | `data/CDOT/PIPELINE_ARCHITECTURE.md` | All 29 PIPELINE_ARCHITECTURE.md files |
| Virginia download script | `download_crash_data.py` | ArcGIS state download scripts |
| Colorado download script | `download_cdot_crash_data.py` | Custom/bulk download scripts |
| Virginia workflow | `.github/workflows/download-data.yml` | State workflow structure |
| Colorado workflow | `.github/workflows/download-cdot-crash-data.yml` | State workflow structure (simpler) |
| R2 upload pattern | CDOT workflow R2 upload step | All state workflow R2 upload steps |
| R2 integration plan | `data/CDOT/r2-integration-plan.md` | Gzip compression, bucket structure |

---

## R2 Storage Path Convention (gzip)

Per `data/CDOT/r2-integration-plan.md`, R2 bucket structure:

```
crash-lens-data/
  {state_lowercase}/
    {jurisdiction}/
      all_roads.csv.gz              ← gzip compressed CSV
      county_roads.csv.gz           ← gzip compressed CSV
      no_interstate.csv.gz          ← gzip compressed CSV
      standardized.csv.gz           ← gzip compressed CSV
      raw/
        {year}.csv.gz               ← gzip compressed annual data
      forecasts_all_roads.json
      forecasts_county_roads.json
      forecasts_no_interstate.json
```

---

## Verification Plan

### 1. File Structure (all 29 new folders + CDOT CLAUDE.md)
```bash
for dir in data/MarylandDOT data/ConnecticutDOT data/NewYorkDOT data/NYCDOT \
  data/DelawareDOT data/HawaiiDOT data/IowaDOT data/IllinoisDOT data/LouisianaDOT \
  data/AlaskaDOT data/MassachusettsDOT data/PennsylvaniaDOT data/FloridaDOT \
  data/GeorgiaDOT data/SouthCarolinaDOT data/OhioDOT data/WisconsinDOT \
  data/NevadaDOT data/UtahDOT data/OregonDOT data/WashingtonDOT data/IdahoDOT \
  data/MontanaDOT data/WestVirginiaDOT data/MississippiDOT data/OklahomaDOT \
  data/VermontDOT data/TexasDOT; do
  echo "$dir: $(ls $dir 2>/dev/null | wc -l) files"
done
ls data/CDOT/CLAUDE.md
ls .github/workflows/download-*-crash-data.yml | wc -l  # Should be 29+1(CDOT)
```

### 2. JSON Syntax Validation
```bash
find data/*/config.json data/*/source_manifest.json -exec python -m json.tool {} > /dev/null \;
```

### 3. Python Script Validation
```bash
find data/*/download_*.py -exec python -m py_compile {} \;
```

### 4. Workflow Syntax Validation
```bash
# Verify all workflow YAML files parse correctly
for f in .github/workflows/download-*-crash-data.yml; do
  python -c "import yaml; yaml.safe_load(open('$f'))" && echo "OK: $f" || echo "FAIL: $f"
done
```

### 5. Consistency Checks
- Every `config.json` has: `state`, `columnMapping`, `derivedFields`, `roadSystems`, `epdoWeights`, `dataSource`
- Every `config.json` `dataSource` includes `dataDictionary` reference
- Every `source_manifest.json` has: `source`, `jurisdiction_filters` with FIPS codes
- Every `CLAUDE.md` references the state's API endpoint and data dictionary
- Every download script accepts `--jurisdiction`, `--years`, `--data-dir`, `--gzip`, `--health-check`
- Every workflow has `R2_STATE_PREFIX`, R2 upload step, manifest commit step
- CDOT folder has CLAUDE.md alongside existing files

### 6. Integration Smoke Test
- `python data/MarylandDOT/download_maryland_crash_data.py --help`
- `python data/IowaDOT/download_iowa_crash_data.py --health-check`
- Verify gzip output produces valid `.csv.gz`

---

## Arkansas Note

Arkansas is listed as "ArcGIS Dashboard | Feature Service" in the research but was not included in the original 20 ArcGIS states. If it should be included, it would bring the total to 31 states. Currently the roster matches the user's original specification of 30 states (6 Socrata + 20 ArcGIS + 1 Custom REST + 1 CRIS Bulk + 1 existing CDOT + Virginia already integrated).
