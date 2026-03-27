# Dynamic EPDO Scoring System — Implementation Plan

## Context

The app now uses FHWA 2025 (FHWA-SA-25-021) as the default EPDO weights (K=883, A=94, B=21, C=11, O=1), derived from October 2025 comprehensive crash unit costs in 2024 dollars ($16.0M K / $18.1K O). As the tool supports multiple states, EPDO weights are dynamic — **each state/federal agency can define different EPDO weights from their own crash cost data** (e.g., VDOT 2024: K=1032). The config files define `epdoWeights` per state, and the app reads them via `getStateEPDOWeights()`. EPDO weights are fully configurable with a UI preset selector, and all calculations use the centralized `calcEPDO()` function.

---

## Research: Do States Have Different EPDO Weights?

**Yes.** EPDO weights are derived by dividing each severity's societal crash cost by the PDO crash cost. Since every state DOT calculates their own crash costs, the resulting weights vary significantly:

| Source / Agency | K (Fatal) | A (Serious) | B (Minor) | C (Possible) | O (PDO) |
|-----------------|-----------|-------------|-----------|---------------|---------|
| **HSM Standard (current tool)** | 462 | 62 | 12 | 5 | 1 |
| **VDOT 2024 crash costs** | ~1,032 | ~53 | ~16 | ~10 | 1 |
| **FHWA 2022 crash costs** | ~975 | ~48 | ~13 | ~8 | 1 |
| **North Carolina DOT** | 76.8 (K+A) | — | 8.4 (B+C) | — | 1 |
| **New Mexico DOT** | 567 | 33 (all injury) | — | — | 1 |
| **Massachusetts DOT** | 21 (all F+I) | — | — | — | 1 |
| **Illinois DOT** | 25 | 10 | 1 | — | — |

**Key insight**: The VDOT 2024 crash costs already in the tool ($12.8M K / $12.4K O) yield K=1032 — more than double the current 462.

### How EPDO Weights Are Derived

```
EPDO Weight = Crash Cost for Severity Level / Crash Cost for PDO

Example (VDOT 2024):
  K weight = $12,800,000 / $12,400 = 1,032
  A weight = $655,000 / $12,400 = 53
  B weight = $198,000 / $12,400 = 16
  C weight = $125,000 / $12,400 = 10
  O weight = $12,400 / $12,400 = 1
```

Sources:
- FHWA Highway Safety Manual (HSM), AASHTO, 2010 — Table 4-7
- FHWA HSIP Manual: https://safety.fhwa.dot.gov/hsip/resources/fhwasa09029/sec2.cfm
- FHWA Network Screening Training: https://safety.fhwa.dot.gov/local_rural/training/fhwasa14072/sec4.cfm

---

## Files to Modify

| File | Changes |
|------|---------|
| `app/index.html` | Core: presets, dynamic weights, UI selector, fix 14 hardcoded calcs |
| `scripts/generate_forecast.py` | Load weights from config instead of hardcoding |
| `send_notifications.py` | Load weights from config instead of hardcoding |

**Unchanged (intentionally separate methodology):**
- Streetlight analysis (line 106360): K=1500, A=240 — warrant-specific, NOT standard EPDO
- Roundabout analysis (line 96525): K=1500, A=240 — warrant-specific
- School zone analysis (lines 119030, 119197): K=1500, A=240 — warrant-specific
- Grant PDF report (lines 60512, 61542): K=1500, A=240 — grant application-specific

---

## Implementation Steps

### Step 1: Make EPDO_WEIGHTS mutable + add presets
**File:** `app/index.html:19916`

Change `const` to `let`, add `EPDO_ACTIVE_PRESET`, add `EPDO_PRESETS` constant.
See `epdo_presets.js` for full reference code.

### Step 2: Add preset switching + recalculation functions
**File:** `app/index.html` (insert after EPDO_PRESETS definition)

New functions:
- `loadEPDOPreset(presetKey)` — sets EPDO_WEIGHTS, saves to localStorage, triggers recalc cascade
- `loadSavedEPDOPreset()` — restores from localStorage on startup
- `saveCustomEPDOWeights()` — handles custom weight input changes
- `updateEPDOPresetButtons()` — toggles active radio button
- `updateEPDOWeightLabels()` — updates dashboard label + glossary dynamically
- `recalculateAllEPDO()` — cascades recalculation across ALL tabs

See `epdo_presets.js` for full reference code.

### Step 3: Fix ALL 14 inline hardcoded EPDO calculations
**File:** `app/index.html` — replace each `*462 + *62 + *12 + *5 +` with `calcEPDO()`

| Line | Current Code | Replacement |
|------|-------------|-------------|
| 53437 | `epdo: d.K*462 + d.A*62 + d.B*12 + d.C*5 + d.O,` | `epdo: calcEPDO(d),` |
| 53684 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 54213 | `epdo: d.K*462 + d.A*62 + d.B*12 + d.C*5 + d.O,` | `epdo: calcEPDO(d),` |
| 54453 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 54903 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 54953 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 55101 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 55151 | `const epdo = severity.K*462 + ...` | `const epdo = calcEPDO(severity);` |
| 55444 | `const epdo = stats.K*462 + ...` | `const epdo = calcEPDO(stats);` |
| 55996 | `const epdo = sevCounts.K * 462 + ...` | `const epdo = calcEPDO(sevCounts);` |
| 61922 | `const epdo = severities.K * 462 + ...` | `const epdo = calcEPDO(severities);` |
| 76112 | `(profile.severity?.K \|\| 0) * 462 + ...` | `calcEPDO(profile.severity \|\| {})` |
| 80782 | `calculateEPDO(severity) { return (severity.K * 462) + ...; }` | `calculateEPDO(severity) { return calcEPDO(severity); }` |
| 126554 | `kSum * 462 + aSum * 62` | `kSum * EPDO_WEIGHTS.K + aSum * EPDO_WEIGHTS.A` |

### Step 4: Remove unused EPDO_WEIGHTS_AD
**File:** `app/index.html:69187-69188` — Delete the unused duplicate constant.

### Step 5: Make UI labels dynamic
- Line 5447: Add `id="epdoWeightsLabel"` to the weights display div
- Line 19734: Add `id="epdoGlossaryDef"` to the glossary definition

### Step 6: Add EPDO preset selector UI in Upload Data tab
**File:** `app/index.html` — Insert after Road Type Filter (~line 4700)

Radio button group matching the existing State/Jurisdiction/Road Type pattern.
See `epdo_presets.js` for full HTML reference.

### Step 7: Initialize saved preset on app load
Call `loadSavedEPDOPreset()` in a `DOMContentLoaded` handler, BEFORE the first `updateDashboard()` call.

### Step 8: Python scripts — read from config
See `epdo_config_loader.py` for full reference code.

---

## What Stays Unchanged (Intentionally Different Weights)

These use K=1500, A=240 for warrant/grant analyses — a separate methodology, NOT standard EPDO:

| Line | Function | Weights | Reason |
|------|----------|---------|--------|
| 106360 | `streetlight_analyzeCrashesByLight()` | K=1500,A=240 | Streetlight warrant methodology |
| 96525 | Roundabout warrant analysis | K=1500,A=240 | Roundabout warrant methodology |
| 119030, 119197 | School zone analysis | K=1500,A=240 | School zone warrant methodology |
| 60512, 61542 | Grant application PDF | K=1500,A=240 | Grant application standard |

These functions use local `const` declarations that shadow the global — they will NOT be affected by changes to the global `EPDO_WEIGHTS`.

---

## Verification Plan

1. **Dashboard:** Load app, note EPDO, switch to VDOT 2024, verify EPDO increases (K: 883→1032), switch back, verify original restores
2. **Dynamic labels:** Verify "Weights: K=..." text updates in dashboard breakdown and glossary
3. **All tabs:** Switch preset, verify Hotspots, Grants, CMF, Safety Focus tabs all update
4. **Persistence:** Set VDOT 2024, refresh page, verify VDOT 2024 still active
5. **Custom weights:** Select Custom, enter K=500, verify calculations use K=500, refresh, verify persists
6. **Warrant independence:** Change to VDOT 2024, run streetlight warrant, verify it still uses K=1500
7. **No inline hardcoding:** Search `*462` in app/index.html — should return 0 results (except warrant sections)
8. **Python scripts:** Run `python scripts/generate_forecast.py`, verify it reads from config
9. **Console:** No errors on preset switch, `console.log(EPDO_WEIGHTS)` shows correct values
