# Plan: State-Specific Grants Tab

## Context

The Grants tab is currently hardcoded for Virginia throughout — UI labels say "Virginia State Grants", agency filters list VDOT/VAHSO, AI prompts reference "Henrico County, Virginia", and the grant data (`VIRGINIA_GRANTS` constant + `data/grants.csv`) only contains Virginia and federal programs.

The app already has a robust multi-state architecture: `FIPSDatabase` provides `dotName`/`dotFullName` for all 50 states, `jurisdictionContext` tracks the active state/county, and `handleStateSelection()` dispatches `jurisdictionChanged` events. The Grants tab simply doesn't participate in this system yet.

**Goal**: When a user selects a state from the Upload tab, the Grants tab dynamically updates to show that state's grant programs, agencies, contacts, and URLs.

---

## Approach: Template-Based Grant Generation

Since every US state participates in the same federal programs (HSIP via state DOT, 402/405 via state Highway Safety Office, SS4A/RAISE/INFRA via USDOT), the key difference between states is **who administers each program** — not the programs themselves. We use a template approach:

1. A `STATE_HSO_REGISTRY` maps state FIPS codes to Highway Safety Office details (the one data point `FIPSDatabase` doesn't have)
2. A `getStateGrantPrograms(stateFips)` function generates state-specific grants by combining `FIPSDatabase` (for DOT info) with `STATE_HSO_REGISTRY` (for HSO info)
3. The UI and AI prompts read from `jurisdictionContext` instead of hardcoded strings

---

## Changes

### 1. Add State Highway Safety Office Registry
**File**: `app/index.html` (~line 31406, before `VIRGINIA_GRANTS`)

Add a `STATE_HSO_REGISTRY` object mapping FIPS codes to HSO details. Start with Virginia (`51`) and Colorado (`08`) with full details, plus a `_default` entry for all other states:

```javascript
const STATE_HSO_REGISTRY = {
    '51': { name: 'VAHSO', fullName: 'Virginia Highway Safety Office',
            agency: 'Virginia DMV', url: 'https://www.dmv.virginia.gov/safety/grants-management' },
    '08': { name: 'OEHS', fullName: 'Office of Transportation Safety',
            agency: 'CDOT', url: 'https://www.codot.gov/safety' },
    '_default': { name: 'Highway Safety Office', fullName: 'State Highway Safety Office',
                  agency: null, url: 'https://www.nhtsa.gov/highway-safety-grants-program' }
};
```

The `_default.agency` of `null` signals to use the `dotName` from `FIPSDatabase` instead.

### 2. Add `getStateGrantPrograms(stateFips)` Function
**File**: `app/index.html` (~line 33588, after `loadGrantsCSV`)

New function that generates grant program entries for any state:
- Looks up DOT info via `FIPSDatabase.getState(stateFips)` (already has `dotName`, `dotFullName`, `name`, `abbr` for all 50 states)
- Looks up HSO info via `STATE_HSO_REGISTRY[stateFips]` (falls back to `_default`)
- Returns array of grant objects: HSIP (state DOT), 402 (state HSO), 405 (state HSO), SS4A (USDOT), RAISE (USDOT), INFRA (USDOT)
- Each grant uses the same structure as `VIRGINIA_GRANTS` entries

### 3. Make `getAllGrants()` State-Aware
**File**: `app/index.html` (line 33590)

Modify to accept optional `stateFips` parameter:
- If CSV grants are loaded and contain entries for the current state, filter and return those
- Otherwise, fall back to `getStateGrantPrograms(stateFips)`
- Add `stateCode` field when parsing CSV grants in `loadGrantsCSV()` (line 33555)

### 4. Rename `displayVirginiaGrants()` to `displayStateGrants()`
**File**: `app/index.html` (line 33670)

- Rename function, keep backward-compatible alias: `const displayVirginiaGrants = displayStateGrants;`
- Use `getAllGrants(jurisdictionContext.stateFips)` instead of `getAllGrants()`
- Update `initGrantModule()` (line 33610) to call `displayStateGrants()`
- Update `applyGrantFilters()` (line 33728) to call `displayStateGrants()`

### 5. Dynamic UI Labels and Elements
**File**: `app/index.html`

| Line | Current | Change |
|------|---------|--------|
| 12171 | `"Virginia State Grants"` button text | Add `id="stateGrantsTabBtn"`, update text dynamically |
| 12171 | `onclick="showGrantTab('virginia')"` | Change to `onclick="showGrantTab('stateGrants')"` |
| 12204 | `id="grantTab-virginia"` | Change to `id="grantTab-stateGrants"` |
| 12205 | `"Showing: Open and Forecasted Virginia grants"` | Dynamic via `updateGrantFilterInfo()` |
| 12186-12191 | Hardcoded VDOT/USDOT/NHTSA agency filter | Populate dynamically via `updateGrantAgencyFilter()` |
| 12222-12225 | Virginia DMV + VDOT HSIP quick links | Wrap in `<div id="grantQuickLinks">`, populate dynamically |
| 33656 | `if (tabId === 'virginia')` in `showGrantTab()` | Change to `if (tabId === 'stateGrants')` |
| 33753 | `"Virginia state and federal programs"` | Use `jurisdictionContext.stateName` |

Add new function `updateGrantsTabForState()` that updates all the above elements based on `jurisdictionContext`.

### 6. Wire Up `jurisdictionChanged` Event
**File**: `app/index.html` (near other `jurisdictionChanged` listeners, ~line 126233)

Add event listener following the same pattern as Transit Safety and School Safety:
```javascript
document.addEventListener('jurisdictionChanged', function(e) {
    const newFips = e.detail?.stateFips;
    if (newFips && newFips !== grantState._lastStateFips) {
        grantState._lastStateFips = newFips;
        updateGrantsTabForState();
    }
});
```

Add `_lastStateFips: null` to `grantState` (line 27294) to track state changes.

### 7. Dynamic AI Prompts
**File**: `app/index.html`

Convert the 3 constant strings to functions that use `jurisdictionContext`:

| Line | Constant | Change |
|------|----------|--------|
| 38217 | `GRANT_AI_SYSTEM_PROMPT` | `getGrantAISystemPrompt()` — replace "Virginia" with `jurisdictionContext.stateName` |
| 38230 | `GRANT_SEARCH_SYSTEM_PROMPT` | `getGrantSearchSystemPrompt()` — replace "VDOT", "Virginia DMV" with state DOT/HSO names |
| 38239 | `FULL_APPLICATION_SYSTEM_PROMPT` | `getFullApplicationSystemPrompt()` — replace "Virginia DOT" with state DOT, "Henrico County, Virginia" with dynamic county/state |

Update callers (search for each constant name and change to function call).

### 8. Dynamic Full Application Prompt
**File**: `app/index.html` (line 36234-36302)

| Line | Current | Change |
|------|---------|--------|
| 36239 | `"Henrico County, Virginia - Department of Public Works"` | `${jurisdictionContext.jurisdictionName}, ${jurisdictionContext.stateName}` |
| 36245 | `"VDOT 2024"` crash cost label | Use DOT name from FIPSDatabase |
| 36286 | `"Henrico County, Virginia"` | Dynamic county/state |
| 36309 | `FULL_APPLICATION_SYSTEM_PROMPT` constant | Call `getFullApplicationSystemPrompt()` |

### 9. Scoring Profile Description
**File**: `app/index.html` (line 27372)

Change `'VDOT HSIP - Focus on systemic infrastructure improvements'` to `'HSIP - Focus on systemic infrastructure improvements'` (state-agnostic, since the scoring math doesn't vary by state).

### 10. Update `data/grants.csv`
**File**: `data/grants.csv`

Add a `state_code` column to enable filtering. Virginia grants get `VA`, federal grants get empty/`FED`. Optionally add Colorado grant entries (CO-HSIP, CO-402, etc.) with CDOT/OEHS details.

### 11. Update `download_grants_data.py`
**File**: `download_grants_data.py`

- Rename `VIRGINIA_STATIC_GRANTS` to a parameterized function `get_state_static_grants(state_abbr, dot_name, hso_name, ...)`
- Add `state_code` column to CSV output
- Accept state parameter (default to current behavior for backward compatibility)

### 12. State-Specific EPDO Weights & Crash Costs in Grants

The app already has `STATE_EPDO_WEIGHTS` (line 20055) with per-state EPDO weight ratios for all 50 states, and the Upload tab's "EPDO Weight System" auto-updates when state changes via `loadEPDOPreset('stateDefault')`. However, the Grants tab's **crash cost dollar values** and **EPDO formula in AI prompts** are hardcoded to Virginia/HSM defaults.

**Existing infrastructure to reuse:**
- `STATE_EPDO_WEIGHTS` (line 20055): Maps FIPS → EPDO ratios (e.g., VA: K=1032, CA: K=1100). Already has entries for all 50 states.
- `EPDO_WEIGHTS` (line 20032): The active global EPDO weights. Updated by `loadEPDOPreset()`.
- `getStateEPDOWeights(stateFips)` (line 20141): Looks up state's EPDO weights.
- `CRASH_COST_PRESETS` (line 36137): Only has `vdot2024` and `fhwa2022` — needs expansion.
- `grantState.crashCosts` (used at lines 35900, 36120, 49080): Dollar amounts for B/C calculation.

**Changes needed:**

#### 12a. Add `STATE_CRASH_COSTS` database
**File**: `app/index.html` (near `CRASH_COST_PRESETS`, ~line 36137)

Add a per-state crash cost database (actual dollar amounts per severity), similar to `STATE_EPDO_WEIGHTS`. Derived from the same DOT crash cost data:

```javascript
const STATE_CRASH_COSTS = {
    '51': { name: 'VDOT 2024', costs: { K: 12800000, A: 655000, B: 198000, C: 125000, O: 12400 },
            source: 'VDOT 2024 crash cost memo' },
    '08': { name: 'CDOT 2023', costs: { K: 5740000, A: 770000, B: 149000, C: 62100, O: 12400 },
            source: 'CDOT crash cost estimates' },
    '06': { name: 'Caltrans 2023', costs: { K: 13640000, A: 719000, B: 211000, C: 136000, O: 12400 },
            source: 'Caltrans TASAS methodology' },
    // ... more states with published data ...
    '_default': { name: 'FHWA 2022', costs: { K: 11600000, A: 571000, B: 155000, C: 99000, O: 11900 },
                  source: 'FHWA national average crash costs' }
};
```

Add helper: `function getStateCrashCosts(stateFips)` — returns state entry or `_default`.

#### 12b. Dynamic crash cost preset button
**File**: `app/index.html` (line 12507)

Replace hardcoded "VDOT 2024 Values" button:
```html
<!-- Before -->
<button id="btnVDOT2024" onclick="loadVDOTCrashCosts()">📊 VDOT 2024 Values</button>
<!-- After -->
<button id="btnStateDOT" onclick="loadStateCrashCosts()">📊 State DOT Values</button>
```

The button label updates dynamically via `updateGrantsTabForState()` to show e.g. "CDOT 2023 Values" or "VDOT 2024 Values" based on current state.

#### 12c. Add `loadStateCrashCosts()` function
**File**: `app/index.html` (near `loadVDOTCrashCosts`, ~line 36142)

New function that reads `jurisdictionContext.stateFips`, looks up `STATE_CRASH_COSTS[fips]`, and populates the crash cost input fields. Replaces `loadVDOTCrashCosts()`:

```javascript
function loadStateCrashCosts() {
    const fips = jurisdictionContext.stateFips || '08';
    const stateData = STATE_CRASH_COSTS[fips] || STATE_CRASH_COSTS['_default'];
    document.getElementById('crashCostK').value = stateData.costs.K;
    // ... A, B, C, O ...
    saveCrashCosts();
}
```

Keep `loadVDOTCrashCosts()` as an alias for backward compatibility.

#### 12d. Auto-apply state crash costs on state change
In the `jurisdictionChanged` listener for grants (from step 6), also update crash costs:

```javascript
// Inside the jurisdictionChanged handler for grants:
loadStateCrashCosts();  // Auto-apply state's crash costs
```

#### 12e. Use active EPDO weights in grant AI prompt
**File**: `app/index.html` (line 36284, 38298)

Replace hardcoded EPDO formula `(K × 462) + (A × 62) + (B × 12) + (C × 5) + (O × 1)` with dynamic values from the active `EPDO_WEIGHTS`:

```javascript
// Line 36284 - full application prompt EPDO formula
`EPDO = (${location.K} × ${EPDO_WEIGHTS.K}) + (${location.A} × ${EPDO_WEIGHTS.A}) + ...`

// Line 38298 - system prompt EPDO formula
`EPDO = (K × ${EPDO_WEIGHTS.K}) + (A × ${EPDO_WEIGHTS.A}) + ...`
```

#### 12f. Update crash cost label in full application prompt
**File**: `app/index.html` (line 36245)

Replace `crashCostK === 12800000 ? 'VDOT 2024' : 'Custom'` with dynamic state DOT label:

```javascript
const stateCostData = STATE_CRASH_COSTS[jurisdictionContext.stateFips];
const costLabel = stateCostData ? stateCostData.name : 'Custom';
```

---

## Files Modified

| File | Nature of Change |
|------|-----------------|
| `app/index.html` | Primary — UI, JS logic, AI prompts (all changes above) |
| `data/grants.csv` | Add `state_code` column |
| `download_grants_data.py` | Parameterize state-specific grant generation |

---

## Deliverable

Save a copy of this plan to `data-pipeline/State specific grant data.md` for project documentation.

---

## Verification

1. **State switch test**: Select Colorado in Upload tab, open Grants tab — verify tab says "Colorado State Grants", agency filter shows "CDOT", quick links point to CDOT URLs
2. **Switch to Virginia**: Verify it shows "Virginia State Grants" with VDOT/VAHSO details
3. **Switch to unsupported state** (e.g., Texas): Verify it shows "Texas State Grants" with TxDOT details from FIPSDatabase and generic HSO defaults
4. **Grant cards**: Verify HSIP grant shows correct state DOT as agency, 402/405 show correct HSO
5. **AI prompts**: Open grant AI chat, verify system prompts reference correct state/county
6. **Full application**: Generate a grant application, verify it references correct state/county (not Henrico/Virginia)
7. **Federal grants**: Verify SS4A, RAISE, INFRA always appear regardless of selected state
8. **Scoring/ranking**: Verify location ranking still works after changes (scoring math unchanged)
9. **EPDO weights in grants**: Verify the EPDO formula in AI prompts uses the active state's EPDO weights (not hardcoded 462/62/12/5/1)
10. **Crash cost auto-update**: Switch state → verify crash cost input fields update to state-specific dollar values (e.g., CO crash costs differ from VA)
11. **Crash cost button label**: Verify the "State DOT Values" button label reflects the current state (e.g., "CDOT 2023 Values" for Colorado, "VDOT 2024 Values" for Virginia)
12. **B/C calculation**: Generate a B/C analysis → verify it uses the state's crash costs, not hardcoded VDOT values
13. **No regressions**: Verify other tabs (Dashboard, Map, CMF, etc.) are unaffected
