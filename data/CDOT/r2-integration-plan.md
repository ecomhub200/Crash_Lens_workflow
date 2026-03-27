# Cloudflare R2 Integration for CRASH LENS Data Pipeline

## Context

CRASH LENS currently stores all crash data CSVs (~197 MB) directly in the Git repo. As the tool scales to 64 Colorado counties and multi-state, this will exceed GitHub's limits. The user wants to migrate large data files to **Cloudflare R2** so that:
- Monthly GitHub Actions workflows automatically upload processed data to R2 (no manual intervention)
- The app fetches large files from R2 at runtime instead of from the repo
- Small files (grants, CMF JSON, configs, forecasts) stay in Git
- No custom domain needed initially (free `r2.dev` URL)

---

## Files to Modify

| File | Change |
|------|--------|
| `app/index.html` | Add R2 manifest loader + `resolveDataUrl()` wrapper around data fetch calls |
| `.github/workflows/download-data.yml` | Replace `git add/commit/push` of large crash CSVs with R2 upload (crash job only) |
| `.github/workflows/download-cdot-crash-data.yml` | Replace `git add/commit/push` of raw CSVs with R2 upload |
| `.github/workflows/process-cdot-data.yml` | Change trigger to `workflow_run`, add R2 download of inputs + R2 upload of outputs |
| `.gitignore` | Add patterns to exclude large data files from Git |

## New Files to Create

| File | Purpose |
|------|---------|
| `.github/actions/upload-r2/action.yml` | Reusable composite action for R2 uploads (used by all workflows) |
| `data/r2-manifest.json` | Maps local file paths to R2 URLs; app reads this at startup |
| `docs/r2-setup.md` | One-time Cloudflare account/bucket/token setup instructions |

---

## Step 1: Create Composite Action `.github/actions/upload-r2/action.yml`

A reusable composite action that any workflow can call as a step. It:
- Accepts a JSON array of `{local_path, r2_key}` pairs
- Uses AWS CLI (S3-compatible, pre-installed on GitHub runners) to upload each file to R2
- Updates `data/r2-manifest.json` with file metadata (size, MD5, upload timestamp)

Requires these GitHub Secrets (set once by user):
- `CF_ACCOUNT_ID` - Cloudflare Account ID
- `CF_R2_ACCESS_KEY_ID` - R2 API Access Key
- `CF_R2_SECRET_ACCESS_KEY` - R2 API Secret Key

And one GitHub Variable:
- `R2_PUBLIC_URL` - e.g. `https://pub-XXXXXXXX.r2.dev`

---

## Step 2: Create `data/r2-manifest.json`

Initial empty manifest committed to Git:

```json
{
  "version": 1,
  "r2BaseUrl": "",
  "updated": "",
  "files": {},
  "localPathMapping": {}
}
```

After the first R2 upload, workflows update it with entries like:
```json
{
  "localPathMapping": {
    "data/CDOT/douglas_all_roads.csv": "colorado/douglas/all_roads.csv",
    "data/CDOT/douglas_county_roads.csv": "colorado/douglas/county_roads.csv",
    "data/CDOT/douglas_no_interstate.csv": "colorado/douglas/no_interstate.csv",
    "data/henrico_all_roads.csv": "virginia/henrico/all_roads.csv"
  }
}
```

The `localPathMapping` translates the app's existing relative paths (e.g. `../data/CDOT/douglas_all_roads.csv`) to R2 object keys. The `{state}/{jurisdiction}/{file_type}` structure makes it trivial to add new states and counties.

---

## Step 3: Add R2 Integration to `app/index.html` (after line ~19985)

Add 3 things after the existing `APP_PATHS` object:

1. **`r2State`** object - holds the loaded manifest
2. **`loadR2Manifest()`** - fetches `../data/r2-manifest.json` once at startup
3. **`resolveDataUrl(localPath)`** - checks the manifest and returns an R2 URL if the file is tracked there, otherwise returns the original local path (graceful fallback)

Then wrap the existing `fetch()` calls with `resolveDataUrl()` at these locations:
- Line ~29795: `fetch(dataFilePath)` -> `fetch(resolveDataUrl(dataFilePath))`
- Line ~29801: `fetch(fallbackPath)` -> `fetch(resolveDataUrl(fallbackPath))`
- Any other data file fetch that loads large CSVs

Files that stay local (no wrapping needed):
- `../data/grants.csv` (3.4 KB)
- `../data/cmf_processed.json` (245 KB)
- `../data/va_mutcd/*` (static reference data)
- `../data/CDOT/forecasts*.json` (~237 KB each)

Call `loadR2Manifest()` early in the startup sequence, before `autoLoadCrashData()`.

---

## Step 4: Modify `.github/workflows/download-data.yml`

### Crash data job (lines 342-388):
**Replace** the `git add data/ && git commit && git push` step with:
1. Call the upload-r2 composite action with the crash CSV files
2. Commit only `data/r2-manifest.json` and `data/.validation/` to Git

### CMF data job:
**No changes** - `cmfclearinghouse_raw.csv`, `cmf_processed.json`, and `cmf_metadata.json` stay in Git.

### Grants data job:
**No changes** - `grants.csv` stays in Git.

---

## Step 5: Modify `.github/workflows/download-cdot-crash-data.yml`

**Replace** the "Commit and push if changed" step (lines 133-166) with:
1. Upload raw CDOT CSVs to R2 under the `colorado/{jurisdiction}/raw/` prefix
2. Commit only `data/r2-manifest.json` and metadata files to Git

---

## Step 6: Modify `.github/workflows/process-cdot-data.yml`

Two changes:

### A. Change trigger
The current trigger is `push: paths: data/CDOT/*.csv`. Since raw CSVs won't be pushed to Git anymore, change to:
```yaml
on:
  workflow_run:
    workflows: ["Download CDOT Crash Data"]
    types: [completed]
    branches: [main]
  workflow_dispatch: # keep existing manual inputs
```

### B. Add R2 download step early in the job
Before the pipeline runs, download raw CSVs from R2 so the processing scripts have input data:
```yaml
- name: Download raw CSVs from R2
  run: |
    aws s3 sync "s3://crash-lens-data/colorado/{jurisdiction}/raw/" "data/CDOT/" \
      --endpoint-url "$R2_ENDPOINT"
```

### C. Replace git commit with R2 upload
Upload the processed output files to R2, commit only manifest + validation metadata to Git.

---

## Step 7: Update `.gitignore`

Add patterns to stop tracking large crash data files:
```gitignore
# Large crash data files (stored on Cloudflare R2)
data/*_county_roads.csv
data/*_no_interstate.csv
data/*_all_roads.csv
data/crashes.csv
data/CDOT/*_standardized.csv
data/CDOT/*_all_roads.csv
data/CDOT/*_county_roads.csv
data/CDOT/*_no_interstate.csv
data/CDOT/crashes.csv
data/CDOT/2*.csv
data/CDOT/*_merged_raw.csv
```

Then `git rm --cached` the already-tracked large files.

---

## Step 8: Create `docs/r2-setup.md`

One-time setup instructions for the user:
1. Create free Cloudflare account
2. Create R2 bucket named `crash-lens-data`
3. Enable the free `r2.dev` public subdomain
4. Set CORS policy (Allow GET/HEAD from `*`)
5. Create R2 API token (Object Read & Write, scoped to bucket)
6. Add 3 GitHub Secrets + 1 GitHub Variable to the repo

---

## R2 Bucket Structure (Multi-State Extensible)

Organized by `{state}/{jurisdiction}/` so adding a new state or county is just a new folder:

```
crash-lens-data/
  colorado/
    douglas/
      all_roads.csv
      county_roads.csv
      no_interstate.csv
      standardized.csv
      raw/
        2021.csv
        2022.csv
        2023.csv
        2024.csv
        2025.csv
    arapahoe/
      all_roads.csv
      county_roads.csv
      ...
    jefferson/
      ...
  virginia/
    henrico/
      all_roads.csv
      county_roads.csv
      no_interstate.csv
    fairfax_county/
      ...
    chesterfield/
      ...
```

**Why this structure:**
- Adding a new Colorado county = upload files to `colorado/{county}/`
- Adding a new state (e.g., Texas) = create `texas/{county}/` folders
- Each jurisdiction has a consistent set of files (all_roads, county_roads, no_interstate)
- Raw annual source files stored under `raw/` subfolder
- The R2 key pattern is: `{state}/{jurisdiction}/{file_type}.csv`

---

## What Stays in Git (everything except large crash CSVs)

- `data/r2-manifest.json` (the URL manifest)
- `data/grants.csv` (3.4 KB)
- `data/cmfclearinghouse_raw.csv` (23 MB - stays in Git for now)
- `data/cmf_processed.json` (245 KB)
- `data/cmf_metadata.json` (351 B)
- `data/va_mutcd/*` (static reference data)
- `data/CDOT/forecasts*.json` (~237 KB each)
- `data/CDOT/config.json`, `source_manifest.json`, `jurisdictions.json` (small configs)
- `data/CDOT/.geocode_cache.json` (pipeline cache)
- `data/.validation/*`, `data/CDOT/.validation/*` (pipeline metadata)
- All config files, docs, and app code

## What Moves to R2 (large crash data CSVs only)

- `data/{jurisdiction}_county_roads.csv` -> `{state}/{jurisdiction}/county_roads.csv`
- `data/{jurisdiction}_no_interstate.csv` -> `{state}/{jurisdiction}/no_interstate.csv`
- `data/{jurisdiction}_all_roads.csv` -> `{state}/{jurisdiction}/all_roads.csv`
- `data/CDOT/{jurisdiction}_standardized.csv` -> `{state}/{jurisdiction}/standardized.csv`
- `data/CDOT/{jurisdiction}_county_roads.csv` -> `{state}/{jurisdiction}/county_roads.csv`
- `data/CDOT/{jurisdiction}_all_roads.csv` -> `{state}/{jurisdiction}/all_roads.csv`
- `data/CDOT/{jurisdiction}_no_interstate.csv` -> `{state}/{jurisdiction}/no_interstate.csv`
- `data/CDOT/crashes.csv` -> `{state}/{jurisdiction}/county_roads.csv` (same as county_roads)
- Raw annual CSVs -> `{state}/{jurisdiction}/raw/{year}.csv`

---

## Verification

1. **Before R2 setup**: App works identically (manifest empty, `resolveDataUrl()` returns local paths)
2. **After R2 setup**: Run any workflow manually, confirm:
   - R2 bucket has the uploaded files
   - `data/r2-manifest.json` is updated with correct mappings
   - App loads data from R2 URLs (check browser DevTools Network tab)
   - Console shows `[R2] Resolved: ../data/CDOT/douglas_all_roads.csv -> https://pub-XXX.r2.dev/colorado/douglas/all_roads.csv`
3. **Fallback test**: Temporarily clear the manifest `r2BaseUrl` -- app should fall back to local paths
4. **Trigger each workflow** and verify end-to-end: download -> process -> R2 upload -> manifest update
