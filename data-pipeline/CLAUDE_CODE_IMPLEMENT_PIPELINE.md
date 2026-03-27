# Claude Code — Implement the Unified Pipeline Architecture

## YOUR TASK

You are implementing the Crash Lens unified data pipeline. **Read the architecture plan FIRST**, then build it phase by phase.

**MANDATORY FIRST STEP:** Before writing ANY code, read the full plan:

```bash
cat data-pipeline/Unified-Pipeline-Architecture.md
```

That document is v5.0 and contains EVERYTHING — the 8-stage pipeline design, exact column specifications, YAML templates, R2 upload paths, caching architecture, scope resolver, aggregation algorithm, and state onboarding checklist. **Follow it as your blueprint.** Do NOT deviate from the plan unless you find a bug in the existing codebase that requires adaptation.

---

## WHAT ALREADY EXISTS (DO NOT RECREATE)

Before you build anything, understand what's already built. Read these files:

```bash
# Existing scripts (READ THESE — they work)
cat scripts/state_adapter.py          # Conversion engine — STANDARD_COLUMNS, StateDetector, normalizers
cat scripts/create_r2_folders.py      # R2 folder hierarchy creator (all 51 states)
cat scripts/upload-to-r2.py           # Local CLI upload tool (boto3)

# Existing workflows (READ THESE — they work)
cat .github/actions/upload-r2/action.yml        # Reusable R2 upload action (retry, MD5, manifest)
cat .github/workflows/create-r2-folders.yml     # R2 folder setup
cat .github/workflows/seed-r2.yml               # Initial data seeding
cat .github/workflows/download-cdot-crash-data.yml  # Colorado download (REFERENCE for state workflows)

# Existing configs (READ THESE — they define the data model)
cat data/r2-manifest.json                       # Version 3 manifest (file metadata, local→R2 mapping)
cat states/colorado/hierarchy.json              # Colorado regions, MPOs/TPRs, allCounties
cat states/virginia/hierarchy.json              # Virginia regions, MPOs, counties
cat data/CDOT/source_manifest.json              # Colorado jurisdiction filters + OnBase doc IDs
cat config.json                                 # Root config with jurisdiction definitions

# Existing data (READ column headers to verify your work)
head -1 data/no_interstate.csv                  # Virginia standard — 69 columns
head -1 data/CDOT/colorado_douglas_all_roads.csv  # Colorado converted — 106 columns (51 standard + _co_* unmapped)
```

**Key existing infrastructure you MUST use (not rebuild):**
- `.github/actions/upload-r2/action.yml` — Reusable upload action with retry, MD5, manifest update
- `scripts/state_adapter.py` — Conversion engine with `STANDARD_COLUMNS`, `CO_EXTRA_COLUMNS`, normalizers
- `scripts/create_r2_folders.py` — R2 folder hierarchy (`STATE_MAP` with all 51 states)
- `data/r2-manifest.json` — Version 3 manifest tracking all R2 files

---

## IMPLEMENTATION PHASES

### Phase 1: Core Scripts (Build These First)

**1.1 Create `scripts/resolve_scope.py`**

The plan (Section 17) has the COMPLETE source code for this script. Copy it exactly. It:
- Reads `states/{state}/hierarchy.json`
- Resolves scope (jurisdiction/region/mpo/statewide) into a list of county keys
- Outputs JSON with: state, scope, selection, jurisdictions, jurisdiction_count, dot_name, data_dir, r2_prefix
- Has a `--list` mode to show available scopes

```bash
# Test it:
python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads --json
python scripts/resolve_scope.py --state colorado --list
```

**1.2 Create `scripts/validate_data.py`**

See plan Section 8. Must support:
- `--state STATE` — which state (for state-isolated cache)
- `--input FILE` — path to Virginia-compatible CSV
- `--cache-dir .cache/{state}/validation` — state-isolated cache directory
- `--force-validate` — ignore cache, revalidate everything
- **Incremental caching** — uses `validated_hashes.json` to skip already-validated rows. Hash each row by `Document Nbr`. Only validate NEW or CHANGED rows.
- **Cache invalidation** — automatically invalidate if `states/{state}/config.json` validation rules change (hash the rules section)
- Validation checks: required fields present, latitude/longitude within state bounds, crash_date parseable, severity values valid
- Output: validated CSV (same columns, bad rows removed or flagged) + validation report

**1.3 Create `scripts/geocode_data.py`**

See plan Section 9. Must support:
- `--state STATE`
- `--input FILE` — path to validated CSV
- `--cache-dir .cache/{state}/geocode` — state-isolated cache directory
- `--output FILE` — statewide validated+geocoded CSV (KEY INTERMEDIATE ARTIFACT)
- **Incremental caching** — uses `geocode_cache.json` (location_key → {x, y, method, confidence, cached_at}) and `geocoded_records.json` (Document Nbr → location_key)
- **Geocode TTL** — stale entries re-geocoded based on `geocode_ttl_days` from `cache_manifest.json`
- 3 strategies: node lookup, Nominatim/OSM reverse geocode, persistent cache
- **IMPORTANT:** After geocoding, save the statewide validated+geocoded CSV as intermediate artifact: `data/{DOT_NAME}/{state}_statewide_validated_geocoded.csv`

**1.4 Create `scripts/aggregate_by_scope.py`**

See plan Section 12. This is the CSV aggregation script. Must support:
- `--state STATE` --scope SCOPE --selection SELECTION
- `--data-dir DIR` — where the split county CSVs are
- `--output-format csv` — ALWAYS CSV, never JSON
- `--output-dir DIR`
- `--federal` — cross-state federal aggregates

**Algorithm for CSV aggregation (CRITICAL — this is NOT a summary, it's a concatenation):**
```
For scope=region, selection=hampton_roads:
  1. Read hierarchy.json → get list of 11 member counties
  2. For each road type (all_roads, no_interstate, county_roads):
     a. Read each county's {county}_{road_type}.csv
     b. Concatenate all rows (same columns, just more rows)
     c. Write to _region/hampton_roads/hampton_roads_{road_type}.csv
```

A region/MPO CSV is just a concatenation of member county rows. Same Virginia-standard columns. The frontend reads it exactly like a county CSV.

**1.5 Update `scripts/split_jurisdictions.py`** (if needed)

Check if the existing split script already handles the road-type split (county_roads, no_interstate, all_roads). If not, create `scripts/split_road_type.py` per plan Section 11. Must produce 3 CSVs per jurisdiction based on `states/{state}/config.json` → `roadSystems.splitConfig`.

---

### Phase 2: Pipeline Workflow

**2.1 Create `.github/workflows/pipeline.yml`**

The plan (Section 16) has the COMPLETE YAML. Copy it, adapting as needed for the actual script names you created in Phase 1. The stage order is:

```
Stage 1: Validate          → scripts/validate_data.py --cache-dir .cache/$STATE/validation
Stage 2: Geocode           → scripts/geocode_data.py --cache-dir .cache/$STATE/geocode
         (saves statewide validated+geocoded CSV as intermediate artifact)
Stage 3: Split Jurisdiction → scripts/split_jurisdictions.py
Stage 4: Split Road Type   → scripts/split_road_type.py (or existing split logic)
Stage 5: Aggregate (CSV)   → scripts/aggregate_by_scope.py --output-format csv
Stage 6: Upload to R2      → aws s3 cp (county CSVs + region/MPO CSVs + statewide gzip)
Stage 7: Predict            → scripts/generate_forecast.py (per county, optional)
Stage 8: Manifest           → git commit r2-manifest.json + metadata
```

**CRITICAL DETAILS for pipeline.yml:**
- Trigger: `workflow_dispatch` with inputs: state (dropdown), scope, selection, dry_run, skip_forecasts
- Uses `scripts/resolve_scope.py --json` in the prepare job to resolve jurisdictions
- Stage 6 uploads county CSVs, region/MPO aggregate CSVs, federal CSVs, AND statewide gzip
- Stage 6 must use the EXISTING `.github/actions/upload-r2/action.yml` reusable action OR direct `aws s3 cp` with the same retry logic
- Stage 7 (Predict) is optional, non-fatal — forecasts per county only
- Stage 8 commits `data/r2-manifest.json` with retry on push

---

### Phase 3: State-Specific Download Workflows

**3.1 Create `.github/workflows/download-virginia.yml`**

See plan Section 5 for the template. Must include:
- Scope dropdown: jurisdiction / region / mpo / statewide
- Selection dropdown: populated from `states/virginia/hierarchy.json`
- Downloads Virginia data (bulk — entire state at once)
- Converts to Virginia-standard format (Virginia is already standard, so minimal conversion)
- **Auto-triggers pipeline.yml** via `actions/github-script@v7` → `createWorkflowDispatch`

**3.2 Create `.github/workflows/download-colorado.yml`**

Same template but adapted for Colorado:
- Uses the existing `download-cdot-crash-data.yml` as reference for the download/OnBase logic
- Colorado downloads year-by-year archives (2021.csv, 2022.csv, etc.) and merges them
- Runs `state_adapter.py` to convert to Virginia-compatible format
- Auto-triggers pipeline.yml

---

### Phase 4: Cache Infrastructure

**4.1 Create cache directory structure:**

```
.cache/
  _cache_registry.json          ← Global registry of all state caches
  virginia/
    cache_manifest.json         ← State cache metadata + update schedule
    validation/
      validated_hashes.json
      validation_rules_hash.txt
      last_run.json
    geocode/
      geocode_cache.json
      geocoded_records.json
      cache_stats.json
  colorado/
    cache_manifest.json
    validation/
      ...
    geocode/
      ...
```

See plan Appendix G for the complete cache architecture. Key rules:
- **State-isolated** — running Colorado NEVER reads/writes Virginia's cache
- **Update frequency aware** — Virginia=daily, Colorado=biannual (configured in `cache_manifest.json`)
- **Cache invalidation** — force flag OR config.json rules change

**4.2 Add `cache_config` to state configs:**

In `states/{state}/config.json`, add:
```json
{
  "cache_config": {
    "update_frequency": "biannual",
    "typical_new_records_per_update": 25000,
    "stale_threshold_days": 200,
    "geocode_ttl_days": 365
  }
}
```

---

### Phase 5: Testing & Validation

**5.1 Test Colorado end-to-end (smallest dataset):**

```bash
# 1. Validate
python scripts/validate_data.py --state colorado --input data/CDOT/colorado_douglas_all_roads.csv --cache-dir .cache/colorado/validation

# 2. Geocode
python scripts/geocode_data.py --state colorado --input data/CDOT/colorado_douglas_validated.csv --output data/CDOT/colorado_statewide_validated_geocoded.csv --cache-dir .cache/colorado/geocode

# 3. Split jurisdiction (should already work)
python scripts/split_jurisdictions.py --state colorado --data-dir data/CDOT/

# 4. Aggregate
python scripts/aggregate_by_scope.py --state colorado --scope statewide --data-dir data/CDOT/ --output-format csv --output-dir data/CDOT/

# 5. Verify output files exist
ls -la data/CDOT/_region/*/
ls -la data/CDOT/_mpo/*/
```

**5.2 Test Virginia (larger dataset):**

```bash
python scripts/validate_data.py --state virginia --input data/all_roads.csv --cache-dir .cache/virginia/validation
```

**5.3 Test scope resolver:**

```bash
python scripts/resolve_scope.py --state virginia --scope region --selection hampton_roads --json
python scripts/resolve_scope.py --state colorado --scope mpo --selection drcog --json
python scripts/resolve_scope.py --state virginia --scope statewide --json
```

**5.4 Verify R2 paths match convention:**

After aggregation, verify the output files follow the R2 bucket path convention documented in Section 13 of the plan:
```
{state}/_region/{region_id}/{region_id}_{road_type}.csv
{state}/_mpo/{mpo_id}/{mpo_id}_{road_type}.csv
{state}/{jurisdiction}/{road_type}.csv
{state}/_state/statewide_all_roads.csv.gz
```

---

## RULES

1. **Read the plan first.** `data-pipeline/Unified-Pipeline-Architecture.md` is your single source of truth.
2. **Don't break existing functionality.** The Colorado pipeline, Virginia pipeline, and R2 upload all work today. Your changes must be additive.
3. **Use existing infrastructure.** Don't recreate the upload action, state adapter, or folder creator.
4. **State-isolated caching.** `.cache/{state}/` — running one state must NEVER touch another state's cache.
5. **CSV aggregation, NOT JSON.** Region/MPO aggregates are CSV concatenations of member county rows. Same columns as county CSVs.
6. **Save statewide CSV.** After geocoding (Stage 2), save the full statewide validated+geocoded CSV as an intermediate artifact before splitting.
7. **Follow the stage order.** Validate → Geocode → [Save Statewide] → Split Jurisdiction → Split Road Type → Aggregate CSV → Upload → Predict → Manifest.
8. **Test after each phase.** Don't move to Phase 2 until Phase 1 scripts work. Don't move to Phase 3 until pipeline.yml runs.
9. **Commit incrementally.** One commit per phase, with descriptive messages.
10. **Check column counts.** Virginia has 69 columns. Colorado converted has 106 columns (51 standard + _source_state + 53 _co_* + _source_file). Verify after conversion.

---

## R2 BUCKET STRUCTURE (Already Created)

The R2 folder hierarchy already exists (created by `scripts/create_r2_folders.py`). Your uploads must follow this exact path convention:

```
crash-lens-data/
  _federal/                                     ← Federal cross-state aggregates
  _national/                                    ← National aggregate data
  shared/                                       ← Shared resources (boundaries, MUTCD)
  {state}/                                      ← e.g., colorado/, virginia/
    _state/
      statewide_all_roads.csv.gz                ← validated+geocoded full state (gzip)
    _statewide/
      snapshots/                                ← Statewide data snapshots
    _region/{region_id}/
      {region_id}_all_roads.csv                 ← concat of member county rows
      {region_id}_no_interstate.csv
      {region_id}_county_roads.csv
    _mpo/{mpo_id}/
      {mpo_id}_all_roads.csv                   ← concat of member county rows
      {mpo_id}_no_interstate.csv
      {mpo_id}_county_roads.csv
    {jurisdiction}/
      standardized.csv                          ← Full Virginia-standard CSV
      county_roads.csv                          ← Road-type split
      no_interstate.csv
      all_roads.csv
      forecasts_county_roads.json               ← SageMaker predictions
      forecasts_no_interstate.json
      forecasts_all_roads.json
      raw/
        {year}.csv                              ← Raw annual source CSVs
```

**Upload action:** Use `.github/actions/upload-r2/action.yml` — it handles retry, MD5, content-type, gzip Content-Encoding, and auto-updates `data/r2-manifest.json` (Version 3).

**Local → R2 key mapping:**
```
data/CDOT/douglas_standardized.csv           →  colorado/douglas/standardized.csv
data/CDOT/douglas_all_roads.csv              →  colorado/douglas/all_roads.csv
data/CDOT/colorado_statewide_all_roads.csv.gz → colorado/_state/statewide_all_roads.csv.gz
data/henrico_all_roads.csv                   →  virginia/henrico/all_roads.csv
```

**State prefixes:** lowercase with underscores (from `STATE_MAP` in `create_r2_folders.py`)
- `colorado`, `virginia`, `maryland`, `new_york`, `west_virginia`, `district_of_columbia`
- Jurisdiction keys: snake_case — `"El Paso"` → `el_paso`, `"Prince George's"` → `prince_georges`

---

## DELIVERABLES CHECKLIST

When done, you should have created or modified:

**New Scripts:**
- [ ] `scripts/resolve_scope.py` — Scope resolver (plan Section 17 has full source)
- [ ] `scripts/validate_data.py` — Validation with incremental cache
- [ ] `scripts/geocode_data.py` — Geocoding with incremental cache + statewide CSV save
- [ ] `scripts/aggregate_by_scope.py` — CSV aggregation (concat county rows)
- [ ] `scripts/split_road_type.py` — Road-type splitting (if not already in split_jurisdictions.py)

**New Workflows:**
- [ ] `.github/workflows/pipeline.yml` — Unified 8-stage processing pipeline
- [ ] `.github/workflows/download-virginia.yml` — Virginia download + auto-trigger
- [ ] `.github/workflows/download-colorado.yml` — Colorado download + auto-trigger

**New/Modified Configs:**
- [ ] `states/virginia/config.json` — add `cache_config` section
- [ ] `states/colorado/config.json` — add `cache_config` section

**Cache Infrastructure:**
- [ ] `.cache/` directory structure with state isolation
- [ ] `_cache_registry.json` template

**Verification:**
- [ ] Colorado: validate → geocode → split → aggregate → verify CSVs
- [ ] Virginia: validate → geocode → split → aggregate → verify CSVs
- [ ] Scope resolver works for all 4 scope types
- [ ] Region/MPO aggregate CSVs have same columns as county CSVs
- [ ] R2 upload paths match the convention above
- [ ] Running Colorado does NOT create/modify anything in `.cache/virginia/`

---

## QUICK START

```bash
# Step 1: Read the plan
cat data-pipeline/Unified-Pipeline-Architecture.md

# Step 2: Read existing code
cat scripts/state_adapter.py
cat .github/actions/upload-r2/action.yml
cat data/r2-manifest.json

# Step 3: Build Phase 1 (core scripts)
# Start with resolve_scope.py since the plan has the full source code

# Step 4: Test each script individually before building the workflow
```

Good luck. The plan has everything you need.
