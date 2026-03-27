# CRASH LENS — Data Pipeline Implementation Plan

## R2 Storage to Front-End Connection

**Complete Fix Guide for Claude Code**

**March 2026 — Version 2.0**

> Updated to reflect modular upload tab architecture, Virginia bug fixes,
> forecast data for all road types, and current R2 base URL.

---

## 1. Problem Statement

The front-end must load crash data from Cloudflare R2 storage. The URL
resolution chain converts local file paths into R2 URLs at runtime. If any
step in this chain breaks, data never reaches the browser.

### 1.1 Current Architecture

The app uses a three-layer URL resolution system:

```
getDataFilePath()  →  resolveDataUrl()  →  fetch()
     ↓                      ↓                  ↓
  Build R2 key       Apply 3 strategies     HTTP request
  (tier/state/       (manifest, dynamic,    to R2 or
   jurisdiction/      R2-native)            local fallback
   roadType.csv)
```

### 1.2 Known Failure Points (Resolved)

| Issue | Root Cause | Fix Applied |
|-------|-----------|-------------|
| Virginia loads only one road type | `getFallbackData()` hardcoded to `all_roads.csv` | Now uses `getActiveRoadTypeSuffix()` (line ~21835) |
| R2 case sensitivity | Jurisdiction not lowercased in path construction | Added `.toLowerCase()` in 4 locations |
| Forecast data not loading per road type | Forecast path used wrong jurisdiction case | Added `.toLowerCase()` in `initPredictionTab()` |

---

## 2. Complete File Map

Every file involved in the R2 data pipeline, organized by function.

### 2.1 R2 Connection Files (Primary)

| File | Purpose |
|------|---------|
| `data/r2-manifest.json` | R2 file inventory with `r2BaseUrl`, `localPathMapping`, file metadata |
| `config.json` | State configs with `r2Prefix` values (e.g., `"colorado"`, `"virginia"`) |
| `app/index.html` | Main app — contains inline data loading functions |

### 2.2 Modular Upload Tab (NEW)

| File | Purpose |
|------|---------|
| `app/modules/loader.js` | CL namespace initialization — registers `CL.upload` |
| `app/modules/upload/upload-tab.js` | Main upload module: R2 data loading, filter management, EPDO, UI helpers |
| `app/modules/upload/upload-pipeline.js` | 4-stage CSV upload pipeline (Detect, Validate, GPS Check, Load) |
| `app/modules/upload/api-connector.js` | External data source connector (Roads, Signals, Speed, BikePed via ArcGIS) |

### 2.3 State/County Selection Files

| File | Purpose |
|------|---------|
| `config.json` | State entries with `r2Prefix`, `defaultJurisdiction`, FIPS codes |
| `states/{state}/config.json` | Per-state column mappings, EPDO weights, road system definitions |

### 2.4 Data Fetch and Processing Files

| File | Purpose |
|------|---------|
| `app/index.html` | `autoLoadCrashData()`, `resolveDataUrl()`, `getDataFilePath()` |
| `app/modules/upload/upload-tab.js` | Modular versions of the same functions (facade pattern) |

### 2.5 Test Files

| File | Purpose |
|------|---------|
| `tests/test_upload_tab_bug.js` | 55 tests covering Virginia bug fixes, road type switching, case normalization |
| `tests/test_r2_integration.js` | 82+ tests for R2 URL resolution, manifest loading, path construction |

---

## 3. R2 Storage Details

### 3.1 Production R2 Endpoint

```
Base URL: https://data.aicreatesai.com
Bucket:   crash-lens-data
```

> **Note:** The old URL `https://pub-3334b656e3c74ea28eb4165b32499843.r2.dev`
> is deprecated. All code and tests now use `https://data.aicreatesai.com`.

### 3.2 R2 Folder Structure

```
crash-lens-data/
  _national/                              # Federal-level data
    state_comparison.json
    dot_roads.csv | non_dot_roads.csv | statewide_all_roads.csv
  {state}/                                # e.g., colorado/, virginia/
    _statewide/aggregates.json            # State rollup stats
    _state/                               # State-tier road data
      dot_roads.csv | non_dot_roads.csv | statewide_all_roads.csv
    _region/{regionId}/                   # Region-tier data
      aggregates.json | dot_roads.csv | all_roads.csv
    _mpo/{mpoId}/                         # MPO-tier data
      aggregates.json | dot_roads.csv | all_roads.csv
    {county}/                             # e.g., douglas/, henrico/
      county_roads.csv                    # Road type 1: County only
      no_interstate.csv                   # Road type 2: No interstate
      all_roads.csv                       # Road type 3: All roads
      standardized.csv                    # Full standardized dataset
      forecasts_county_roads.json         # Forecast: county roads
      forecasts_no_interstate.json        # Forecast: no interstate
      forecasts_all_roads.json            # Forecast: all roads
```

**Important:** All folder and file names are **lowercase** in R2 (e.g.,
`virginia/henrico/`, not `Virginia/Henrico/`).

### 3.3 Road Type Mapping (3 Types)

| Radio Button | Filter Value | County-Tier File | Forecast File |
|-------------|-------------|-----------------|---------------|
| County Roads Only | `countyOnly` | `county_roads.csv` | `forecasts_county_roads.json` |
| No Interstate | `countyPlusVDOT` | `no_interstate.csv` | `forecasts_no_interstate.json` |
| All Roads | `allRoads` | `all_roads.csv` | `forecasts_all_roads.json` |

The `getActiveRoadTypeSuffix()` function (line ~23109) maps the radio button
value to the file suffix:

```javascript
function getActiveRoadTypeSuffix() {
    var selected = document.querySelector('input[name="roadTypeFilter"]:checked');
    if (!selected) return 'all_roads';
    switch (selected.value) {
        case 'countyOnly':      return 'county_roads';
        case 'countyPlusVDOT':  return 'no_interstate';
        case 'allRoads':        return 'all_roads';
        default:                return 'all_roads';
    }
}
```

---

## 4. Complete Data Flow (End-to-End)

### 4.1 Startup Sequence

1. `validateAppPaths()` — detect broken file references
2. `loadR2Manifest()` — fetch `../data/r2-manifest.json`, populate `r2State.manifest`
3. `loadAppConfig()` — load `config.json` with state `r2Prefix` values
4. `loadAppSettings()` — load saved jurisdiction/filter preferences
5. `populateStateDropdown()` — fill state selector (50 states + DC)
6. `populateJurisdictionDropdown()` — fill county selector for active state
7. `window.load` fires `autoLoadCrashData()` after 3-second delay

### 4.2 Data Fetch Chain (`autoLoadCrashData`, line ~30544)

1. Check IndexedDB cache. If hit and valid → use cached data, stop.
2. Call `getDataFilePath()` (line ~23177) which reads current tier, state
   `r2Prefix`, jurisdiction ID, and road type suffix to build an R2 key:
   `colorado/douglas/county_roads.csv`
3. Call `resolveDataUrl(path)` (line ~22055) which applies 3 strategies:
   - **Strategy 1:** Exact match in `r2State.manifest.localPathMapping`
   - **Strategy 2:** Dynamic construction from state `r2Prefix` + parsed filename
   - **Strategy 3:** R2-native path detection (prepend R2 base URL)
   - **Fallback:** Return original local path unchanged
4. `fetch()` the resolved URL. If it fails, build a fallback path using
   `APP_PATHS.getFallbackData()` (line ~21835) and retry.
5. Parse CSV with PapaParse (chunked, `header: true`). On first chunk, call
   `StateAdapter.detect(headers)` to identify state format.
6. Normalize each row with `StateAdapter.normalizeRow()`.
7. Cache final data in IndexedDB.

### 4.3 Fallback Path Construction (Bug Fix)

The `APP_PATHS.getFallbackData()` function now uses the active road type
instead of a hardcoded `all_roads.csv`:

```javascript
getFallbackData: () => {
    const roadType = (typeof getActiveRoadTypeSuffix === 'function')
        ? getActiveRoadTypeSuffix() : 'all_roads';
    return `${r2Prefix}/${jurisdiction.toLowerCase()}/${roadType}.csv`;
}
```

### 4.4 Forecast Data Loading (`initPredictionTab`, line ~143184)

Forecast files follow the same road-type pattern. The `getPredictionForecastFile()`
function maps the active road type to the correct forecast JSON file. The
jurisdiction is lowercased to match R2 folder structure.

---

## 5. Modular Upload Tab Architecture

### 5.1 Module Namespace

All upload modules attach to `window.CL.upload`:

```javascript
// app/modules/loader.js
window.CL = window.CL || {};
CL.upload = CL.upload || {};
```

### 5.2 Module Files

#### `app/modules/upload/upload-tab.js` (~480 lines)

Main upload tab module. Exposes via `CL.upload`:

| Function | Purpose |
|----------|---------|
| `getActiveRoadTypeSuffix()` | Map radio button to file suffix |
| `getDataFilePath()` | Build R2 path from tier/state/jurisdiction/road type |
| `resolveDataUrl()` | Apply 3-strategy URL resolution |
| `buildLocalFallbackPaths()` | Generate fallback path list |
| `loadR2Manifest()` | Fetch and parse R2 manifest |
| `checkR2DataAvailability()` | Verify R2 data exists for jurisdiction |
| `saveFilterProfile()` | Save road type selection and reload data |
| `saveUserPreferences()` | Persist user preferences to localStorage |
| `clearUserPreferences()` | Reset saved preferences |
| `forceRefreshAllData()` | Clear cache and reload from R2 |
| `showFilterLoadingState()` | Show loading spinner in filter UI |
| `showRefreshButton()` | Show manual refresh button |
| `updateCurrentSelectionDisplay()` | Update selection summary text |
| `updateRoadTypeLabels()` | Update road type radio labels |
| `toggleEPDOSection()` | Toggle EPDO weights panel |
| `loadEPDOPreset()` | Load state-specific EPDO weights |
| `saveCustomEPDOWeights()` | Save user-customized weights |
| `applyStateDefaultEPDO()` | Apply default weights for state |
| `getR2DataAvailabilitySummary()` | Return R2 availability status |

#### `app/modules/upload/upload-pipeline.js` (~280 lines)

4-stage CSV upload pipeline. Exposes via `CL.upload.pipeline`:

| Function | Purpose |
|----------|---------|
| `handleFileSelect()` | Handle file input change |
| `handleFileDrop()` | Handle drag-and-drop upload |
| `handleStateChange()` | Handle state dropdown change |
| `startPipeline()` | Begin 4-stage processing |

**Stages:**
1. Detect & Convert — auto-detect state format via `StateAdapter`
2. Validate & Normalize — parse/normalize all rows
3. GPS Check — calculate GPS coverage percentage
4. Load — feed rows into `processRow()` and `finalizeData()`

#### `app/modules/upload/api-connector.js` (~280 lines)

External data source connector. Exposes via `CL.upload.apiConnector`:

| Function | Purpose |
|----------|---------|
| `toggle()` | Toggle connector card expansion |
| `testConnection()` | Test ArcGIS REST endpoint |
| `clearSource()` | Clear a data source |
| `applyPreset()` | Apply state preset URLs |
| `handleDataDict()` | Upload data dictionary file |
| `setMappingMode()` | Set auto/manual field mapping |

State presets available: Virginia, Colorado, Maryland.

### 5.3 Script Loading Order

In `app/index.html` (after `loader.js`):

```html
<script src="modules/loader.js"></script>
<script src="modules/upload/upload-tab.js"></script>
<script src="modules/upload/upload-pipeline.js"></script>
<script src="modules/upload/api-connector.js"></script>
```

Inline functions in `app/index.html` remain as the primary implementations.
The modules serve as a facade/reference layer for the modular architecture.
Global aliases provide backward compatibility.

---

## 6. Bug Fixes Applied

### 6.1 Virginia Henrico Data Loading (Critical)

**Problem:** Switching road types in Virginia/Henrico always loaded the same
dataset. Colorado/Douglas worked correctly.

**Root Cause:** `APP_PATHS.getFallbackData()` (line ~21835) was hardcoded to
return `all_roads.csv` regardless of the selected road type filter.

**Fix:** Changed to use `getActiveRoadTypeSuffix()`:

```javascript
// BEFORE (bug):
return `${r2Prefix}/${jurisdiction}/all_roads.csv`;

// AFTER (fix):
const roadType = (typeof getActiveRoadTypeSuffix === 'function')
    ? getActiveRoadTypeSuffix() : 'all_roads';
return `${r2Prefix}/${jurisdiction.toLowerCase()}/${roadType}.csv`;
```

### 6.2 Case Normalization (Defensive)

Added `.toLowerCase()` in 4 locations to prevent R2 case-sensitivity issues:

| Location | Line | Fix |
|----------|------|-----|
| `getDataFilePath()` | ~23225 | `r2Jurisdiction = r2Jurisdiction.toLowerCase()` |
| `autoLoadCrashData()` | ~30705 | `r2JurisdictionPath = r2JurisdictionPath.toLowerCase()` |
| `resolveDataUrl()` | ~22055 | Lowercase normalization for R2-native paths |
| `initPredictionTab()` | ~143233 | Lowercase jurisdiction in forecast path |

### 6.3 Improved Logging

Added trace logging in `saveFilterProfile()` (line ~24328) to show which road
type is being loaded when the user switches filters:

```javascript
console.log('[Config] Switching road type to:', selected.value, '→ file:', getDataFilePath());
```

---

## 7. Implementation Tasks

### Task 1: Verify r2-manifest.json

**File:** `data/r2-manifest.json` | **Priority:** CRITICAL

The manifest must contain:
- `r2BaseUrl` set to `https://data.aicreatesai.com`
- `localPathMapping` with Douglas County and Henrico entries
- `files` object with file metadata (size, md5, uploaded timestamp)
- `version` field set to 3

### Task 2: Fix loadR2Manifest() Fetch Path

**File:** `app/index.html` | **Line:** ~23881 | **Priority:** CRITICAL

Add robust path resolution with multiple fallbacks:

```javascript
const manifestPaths = [
    '../data/r2-manifest.json',
    './data/r2-manifest.json',
    'data/r2-manifest.json',
    '/data/r2-manifest.json'
];
```

### Task 3: Ensure Startup Load Order

**Priority:** CRITICAL

The startup sequence must be:
1. `await loadR2Manifest()` — must complete first
2. `await loadAppConfig()` — may trigger state selection
3. `await loadAppSettings()` — loads saved preferences
4. `populateStateDropdown()` and `populateJurisdictionDropdown()`
5. `autoLoadCrashData()` — only after all above resolve

### Task 4: Diagnostic Logging in resolveDataUrl()

**File:** `app/index.html` | **Line:** ~22055 | **Priority:** HIGH

```javascript
console.log('[R2-DEBUG] resolveDataUrl called with:', localPath);
console.log('[R2-DEBUG] r2State.manifest loaded:', !!r2State.manifest);
console.log('[R2-DEBUG] r2BaseUrl:', r2State.manifest?.r2BaseUrl);
```

### Task 5: Validate config.json State Entries

**File:** `config.json` | **Priority:** HIGH

Every state must have `r2Prefix`:

```json
"colorado": { "fips": "08", "r2Prefix": "colorado", ... }
"virginia": { "fips": "51", "r2Prefix": "virginia", ... }
```

### Task 6: Fix getDataFilePath() for All Tiers

**File:** `app/index.html` | **Line:** ~23177 | **Priority:** HIGH

Must return clean R2-native paths (not legacy `data/` prefixed paths).
Must apply `.toLowerCase()` to jurisdiction.

### Task 7: Ensure resolveDataUrl() Strategy 3 Works

**File:** `app/index.html` | **Line:** ~22055 | **Priority:** HIGH

R2-native path detection must match all `getDataFilePath()` output patterns:

```javascript
const isR2NativePath = !normalizedPath.startsWith('data/')
    && normalizedPath.includes('/')
    && (normalizedPath.endsWith('.csv')
        || normalizedPath.endsWith('.json')
        || normalizedPath.endsWith('.csv.gz'));
```

Resulting URL: `https://data.aicreatesai.com/colorado/douglas/county_roads.csv`

### Task 8: Verify AggregateLoader Uses R2 URLs

**File:** `app/index.html` | **Line:** ~21751 | **Priority:** MEDIUM

`AggregateLoader._resolveR2Url()` checks three sources:
1. `r2State.manifest.r2BaseUrl` (primary — should be `https://data.aicreatesai.com`)
2. `appConfig.r2.publicUrl` (secondary fallback)
3. `../data` (local fallback — means R2 not connected)

### Task 9: R2 Connection Health Check

**Priority:** MEDIUM

Console diagnostic function:

```javascript
async function diagR2Connection() {
    console.group('[R2 Diagnostics]');
    console.log('1. r2State.loaded:', r2State.loaded);
    console.log('2. r2State.error:', r2State.error);
    console.log('3. Manifest present:', !!r2State.manifest);
    console.log('4. r2BaseUrl:', r2State.manifest?.r2BaseUrl);
    console.log('5. Files count:', Object.keys(r2State.manifest?.files || {}).length);
    console.log('6. LocalPathMapping count:',
        Object.keys(r2State.manifest?.localPathMapping || {}).length);
    const testPath = 'colorado/douglas/county_roads.csv';
    const testUrl = resolveDataUrl(testPath);
    console.log('7. Test resolve:', testPath, '->', testUrl);
    try {
        const resp = await fetch(testUrl, { method: 'HEAD' });
        console.log('8. HEAD response:', resp.status, resp.statusText);
    } catch (e) { console.error('8. Fetch failed:', e.message); }
    console.groupEnd();
}
```

### Task 10: Fallback Config with R2 Support

**Priority:** LOW

`getMinimalFallbackConfig()` should include:

```json
"r2": {
    "publicUrl": "https://data.aicreatesai.com"
}
```

---

## 8. Verification Checklist

### 8.1 Unit Tests

- [ ] Run `tests/test_upload_tab_bug.js` — all 55 tests must pass
- [ ] Run `tests/test_r2_integration.js` — all 82+ tests must pass
- [ ] `resolveDataUrl()` returns R2 URLs (not local paths) for all county paths
- [ ] `getDataFilePath()` output correct for each tier (federal, state, region, mpo, county)

### 8.2 Integration Tests

- [ ] Open `app/index.html` in browser, open DevTools Console
- [ ] Run `diagR2Connection()` — all 8 checks should pass
- [ ] Select **Colorado > Douglas County > County Roads Only**
  - Network tab shows: `https://data.aicreatesai.com/colorado/douglas/county_roads.csv`
  - Response: 200 OK with CSV content
- [ ] Switch to **All Roads** — new request to `/all_roads.csv`
- [ ] Switch to **Virginia > Henrico** — path uses `/virginia/henrico/`
- [ ] Switch road types for Virginia — each loads a **different** record count

### 8.3 Forecast Data Tests

- [ ] Select **County Roads Only** → Prediction tab loads `forecasts_county_roads.json`
- [ ] Select **No Interstate** → Prediction tab loads `forecasts_no_interstate.json`
- [ ] Select **All Roads** → Prediction tab loads `forecasts_all_roads.json`
- [ ] Forecast data includes p10/p50/p90 confidence bands

### 8.4 Console Error Check

Zero errors matching:
- `[R2] Manifest not found`
- `[R2] No mapping found for:`
- `Failed to fetch` / `net::ERR_`
- `CORS` / `Access-Control-Allow-Origin`
- `404 Not Found` on any R2 URL

---

## 9. Quick Reference

### Fix Priority Order

| Priority | Task | Status |
|----------|------|--------|
| CRITICAL | r2-manifest.json exists and valid | Verified |
| CRITICAL | loadR2Manifest() fetch path | Implemented |
| CRITICAL | Startup load order | Verified |
| CRITICAL | getFallbackData() road type fix | **Fixed** |
| HIGH | resolveDataUrl() diagnostic logging | Implemented |
| HIGH | config.json r2Prefix entries | Verified |
| HIGH | getDataFilePath() all tiers + `.toLowerCase()` | **Fixed** |
| HIGH | resolveDataUrl() Strategy 3 | Verified |
| MEDIUM | AggregateLoader R2 URLs | Verified |
| MEDIUM | diagR2Connection() health check | Implemented |
| LOW | Fallback config R2 block | Implemented |

### Key Function Locations (app/index.html)

| Function | Line |
|----------|------|
| `APP_PATHS` | ~21810 |
| `getFallbackData()` | ~21835 |
| `loadR2Manifest()` | ~23881 |
| `checkR2DataAvailability()` | ~21938 |
| `resolveDataUrl()` | ~22055 |
| `getActiveRoadTypeSuffix()` | ~23109 |
| `getDataFilePath()` | ~23177 |
| `saveFilterProfile()` | ~24321 |
| `autoLoadCrashData()` | ~30544 |
| `fetchWithR2Retry()` | ~30744 |
| `showUploadSummary()` | ~31371 |
| `initPredictionTab()` | ~143184 |

### R2 URL Format

```
https://data.aicreatesai.com/{state}/{jurisdiction}/{roadType}.csv
https://data.aicreatesai.com/{state}/{jurisdiction}/forecasts_{roadType}.json
```

Examples:
```
https://data.aicreatesai.com/colorado/douglas/county_roads.csv
https://data.aicreatesai.com/virginia/henrico/no_interstate.csv
https://data.aicreatesai.com/virginia/henrico/forecasts_all_roads.json
```

---

*Crash Lens — Data Pipeline for R2 to Front-End Connection*
*Version 2.0 — March 2026*
