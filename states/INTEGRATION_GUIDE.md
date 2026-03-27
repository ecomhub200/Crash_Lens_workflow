# Multi-State Adapter - Integration Guide

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                    User uploads CSV                   │
│           (Virginia OR Colorado format)               │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│            StateAdapter.detect(headers)               │
│                                                       │
│  Checks CSV columns against state signatures:         │
│  • "CUID" + "System Code" + "Injury 00" → Colorado   │
│  • "Document Nbr" + "SYSTEM" + "Crash Severity" → VA │
└──────────────────────┬──────────────────────────────┘
                       │
            ┌──────────┴──────────┐
            │                     │
     Colorado detected     Virginia detected
            │                     │
            ▼                     ▼
┌───────────────────┐  ┌──────────────────┐
│ StateAdapter       │  │ Pass-through     │
│ .normalizeRow()   │  │ (no changes)     │
│                   │  │                  │
│ • Derive severity │  │ Data already in  │
│   from Injury 00- │  │ internal format  │
│   04 counts       │  │                  │
│ • Map crash types │  │                  │
│ • Build route     │  │                  │
│   names           │  │                  │
│ • Derive boolean  │  │                  │
│   flags (ped,     │  │                  │
│   bike, alcohol)  │  │                  │
│ • Map road system │  │                  │
│   codes           │  │                  │
└────────┬──────────┘  └────────┬─────────┘
         │                      │
         └──────────┬───────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────┐
│        Normalized Row (Virginia-compatible)           │
│                                                       │
│  Same column names regardless of source state:        │
│  • 'Crash Severity' → K/A/B/C/O                     │
│  • 'Collision Type' → Standardized types             │
│  • 'RTE Name' → Route name                          │
│  • 'SYSTEM' → Road classification                    │
│  • 'Pedestrian?' → Y/N                              │
│  • etc.                                              │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│          Existing processRow() function               │
│       (NO CHANGES NEEDED - works as-is)              │
└─────────────────────────────────────────────────────┘
```

## How to Integrate into app/index.html

### Step 1: Include the adapter

Add before the main app script:
```html
<script src="../states/state_adapter.js"></script>
```

### Step 2: Modify CSV parsing

In the Papa.parse configuration, add state detection and row normalization:

```javascript
// BEFORE (current Virginia-only code):
Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true,
    chunk: function(results) {
        results.data.forEach(row => {
            processRow(row);
        });
    },
    // ...
});

// AFTER (multi-state):
Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true,
    beforeFirstChunk: function(chunk) {
        // Auto-detect state from first row headers
        const firstLine = chunk.split('\n')[0];
        const headers = firstLine.split(',').map(h => h.trim().replace(/"/g, ''));
        StateAdapter.detect(headers);

        // Show detection result in UI
        const stateName = StateAdapter.getStateName();
        console.log('[Data Load] Detected state: ' + stateName);
    },
    chunk: function(results) {
        results.data.forEach(row => {
            // Normalize row BEFORE processing
            const normalized = StateAdapter.normalizeRow(row);
            processRow(normalized);
        });
    },
    // ...
});
```

### Step 3: Update road system filter UI (optional)

If Colorado is detected, the filter profile labels should update:

```javascript
if (StateAdapter.getDetectedState() === 'colorado') {
    // Update filter radio labels
    document.querySelector('label[for="countyOnly"]').textContent = 'County/City Roads Only';
    document.querySelector('label[for="countyPlusVDOT"]').textContent = 'All Roads (No Interstate)';
    document.querySelector('label[for="allRoads"]').textContent = 'All Roads (Including Interstate)';
}
```

---

## Key Design Decisions

### Why normalize to Virginia format?

The existing app has ~70,000 lines of code built around Virginia column names. Instead of modifying thousands of references, we normalize incoming data to match the expected format. This is:

1. **Safer** - Zero risk of breaking existing features
2. **Faster** - No app refactoring needed
3. **Reversible** - Remove the adapter and Virginia still works
4. **Extensible** - Adding Texas means just adding another normalizer

### How severity is derived for Colorado

Virginia has a direct `Crash Severity` column (K/A/B/C/O).
Colorado has **injury count columns** instead:

| Colorado Column | Meaning | KABCO |
|----------------|---------|-------|
| `Injury 04` | Persons Killed | K |
| `Injury 03` | Suspected Serious Injury | A |
| `Injury 02` | Suspected Minor Injury | B |
| `Injury 01` | Possible Injury | C |
| `Injury 00` | No Apparent Injury | O |

**Derivation rule:** Use the highest severity level present:
```
If Injury 04 > 0 → Severity = K
Else if Injury 03 > 0 → Severity = A
Else if Injury 02 > 0 → Severity = B
Else if Injury 01 > 0 → Severity = C
Else → Severity = O
```

### How road system filtering works for Colorado

Colorado's `System Code` is mapped to Virginia's `SYSTEM` values so existing filter logic works:

| Colorado System Code | Mapped To | Filter Profile |
|---------------------|-----------|----------------|
| City Street | Non-DOT secondary | countyOnly ✓ |
| County Road | Non-DOT secondary | countyOnly ✓ |
| State Highway | Primary | countyPlusState ✓ |
| Frontage Road | Secondary | countyPlusState ✓ |
| Interstate Highway | Interstate | allRoads only ✓ |

**User's road filter choices:**

- **County/City Roads Only**: City Streets + County Roads (no state routes)
- **All Roads (No Interstate)**: Above + State Highways + Frontage Roads
- **All Roads**: Everything including I-25, I-70, etc.

### How routes are named for Colorado

Virginia has a single `RTE Name` column (e.g., `R-VA US00250WB`).
Colorado constructs route names from multiple fields:

| System Code | Route Name Logic | Example |
|-------------|-----------------|---------|
| Interstate Highway | `I-{number}` | `I-25` |
| State Highway | Location 1 or `CO-{number}` | `S PARKER RD` or `CO-83` |
| Frontage Road | `{Location 1} (Frontage)` | `I-25 FRONTAGE (Frontage)` |
| County Road | Location 1 | `CASTLE PINES PKWY` |
| City Street | Location 1 | `PLUM CREEK BLVD` |

### How intersections are identified

Virginia has a `Node` column with numeric node IDs.
Colorado constructs intersection IDs from location fields:

```
If Road Description = "At Intersection" or "Intersection Related" or "Roundabout":
    Node = "{Location 1} & {Location 2}" (alphabetically sorted)
Else:
    Node = "" (non-intersection)
```

Example: `CASTLE PINES PKWY & MONARCH BLVD`

---

## Road System Include/Exclude Logic

### For Douglas County specifically:

**County/City Roads Only** (excludes state routes):
- Includes: All City Street and County Road crashes
- Excludes: State Highway (CO-83, CO-86, etc.), Interstate (I-25), Frontage Roads
- Use case: Analyzing only locally-maintained roads

**All Roads No Interstate** (includes state routes):
- Includes: Everything above + State Highways + Frontage Roads
- Excludes: Interstate Highway (I-25)
- Use case: Analyzing all non-interstate roads

**All Roads** (includes everything):
- Includes: All road types including I-25
- Use case: Complete county crash picture

### Douglas County Route Distribution (from data):
```
City Street:        1,142 crashes (26.4%)
County Road:        1,037 crashes (24.0%)
State Highway:      1,143 crashes (26.4%)
Interstate Highway:   992 crashes (22.9%)
Frontage Road:          8 crashes (0.2%)
```

---

## Files Created

```
states/
├── INTEGRATION_GUIDE.md          ← This file
├── state_adapter.js              ← Core normalization module
├── colorado/
│   ├── config.json               ← Full Colorado state config
│   └── jurisdictions.json        ← Douglas County + neighboring counties
└── virginia/
    └── config.json               ← Virginia config (reference)
```

---

## Adding a New State

To add Texas, for example:

1. **Get a sample CSV** from TxDOT/CRIS
2. **Create** `states/texas/config.json` with column mappings
3. **Add detection signature** in `state_adapter.js`:
   ```javascript
   texas: {
       requiredColumns: ['Crash ID', 'Crash Severity', 'County'],
       displayName: 'Texas (CRIS)',
       configPath: 'states/texas/config.json'
   }
   ```
4. **Add normalizer** in `state_adapter.js`:
   ```javascript
   const TEXAS_NORMALIZER = {
       normalizeRow(row) {
           // Map Texas columns to internal format
           return { ... };
       }
   };
   ```
5. **Register** in NORMALIZERS:
   ```javascript
   texas: TEXAS_NORMALIZER
   ```
