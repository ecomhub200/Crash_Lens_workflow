# CRASH LENS — Comprehensive Implementation Plan
# Multi-Jurisdiction Expansion: Unified Roadmap

**Version:** 1.0
**Date:** February 14, 2026
**Status:** READY FOR IMPLEMENTATION
**Synthesized from:**
- `JURISDICTION_EXPANSION_PLAN.md` (Part 1 — Architecture & Infrastructure)
- `JURISDICTION_EXPANSION_PLAN_2.md` (Part 2 — Tab-Level Views & Scope Restrictions)
- `JURISDICTION_PIPELINE_PLAN.md` (Part 3 — Data Pipeline & EPDO System)

---

## Table of Contents

1. [Vision & Scope](#1-vision--scope)
2. [Architecture Summary](#2-architecture-summary)
3. [Critical Design Decisions](#3-critical-design-decisions)
4. [Implementation Phases](#4-implementation-phases)
   - Phase 1: Foundation — Config, Hierarchy & EPDO
   - Phase 2: Data Pipeline — Multi-County Processing & Aggregates
   - Phase 3: Boundary System — APIs, GeoJSON & Caching
   - Phase 4: UI Foundation — Tier Selector & Scope State
   - Phase 5: State View — Dashboard, Map & Explore Tabs
   - Phase 6: Region/MPO View — Cross-County Analysis
   - Phase 7: Federal View — National Overview
   - Phase 8: Tab Scope Restrictions & Non-Explore Behavior
   - Phase 9: Advanced Features — DuckDB-WASM & Optimization
   - Phase 10: New State Onboarding & Production Readiness
5. [Files To Create](#5-files-to-create)
6. [Files To Modify](#6-files-to-modify)
7. [Dependency Graph](#7-dependency-graph)
8. [Risk Register](#8-risk-register)
9. [Testing Strategy](#9-testing-strategy)

---

## 1. Vision & Scope

### What We're Building

Transform CRASH LENS from a **single-jurisdiction tool** (1 county at a time) into a **multi-tier hierarchical analysis platform** supporting views at:

| Tier | Scope | Example | Data Source |
|------|-------|---------|-------------|
| **Federal** | Cross-state comparison | All states with data | Pre-aggregated JSON + FARS |
| **State** | Statewide, 64 counties | Colorado | Pre-aggregated JSON from R2 |
| **Region/MPO** | 3-15 counties | CDOT Region 1, DRCOG | Pre-aggregated JSON + lazy-loaded CSVs |
| **County** | Single county (existing) | Douglas County | Full CSV (unchanged) |
| **City/Place** | Sub-county filtered | Castle Rock | Filtered from county CSV |
| **Corridor** | Multi-county route | I-25 | Pre-aggregated + lazy-loaded |

### What Does NOT Change

- **County view** remains 100% unchanged — same functionality, same data loading
- **Solutions section** (CMF, Warrants, MUTCD AI, Domain Knowledge) — stays single-location
- **Grants section** — stays single-county
- **Reports section** — stays single-county
- **Single-file architecture** — `app/index.html` remains the SPA

### The Key Principle

> **Only the EXPLORE section participates in multi-jurisdiction expansion.**
> Solutions, Grants, and Reports always operate at county level.
> When the user is at a higher scope, these sections prompt for county selection.

---

## 2. Architecture Summary

### New Components to Build

```
┌─────────────────────────────────────────────────────────────┐
│                     CRASH LENS APPLICATION                   │
│                                                               │
│  ┌── UI LAYER ──────────────────────────────────────────┐   │
│  │  Tier Selector (Federal/State/Region/MPO/County)      │   │
│  │  Scope State Manager (viewTier, selectedScope)        │   │
│  │  Adaptive View Renderer (tab visibility by tier)      │   │
│  │  Progressive Map Renderer (choropleth → markers)      │   │
│  │  Inline County Selector (for Solutions/Grants/Reports)│   │
│  │  EPDO Preset System (HSM/VDOT/FHWA/Custom)           │   │
│  └───────────────────────┬──────────────────────────────┘   │
│                           │                                   │
│  ┌── DATA LAYER ─────────▼──────────────────────────────┐   │
│  │  HierarchyRegistry (region→county, MPO→county)        │   │
│  │  AggregationEngine (multi-county combining)            │   │
│  │  BoundaryService (TIGERweb + BTS NTAD auto-discovery) │   │
│  │  SpatialClipService (bbox→polygon precision)           │   │
│  │  IndexedDB Cache (boundaries, aggregates, CSVs)        │   │
│  └───────────────────────┬──────────────────────────────┘   │
│                           │                                   │
│  ┌── CONFIG LAYER ────────▼──────────────────────────────┐  │
│  │  states/{fips}/config.json      (state-specific)       │  │
│  │  states/{fips}/hierarchy.json   (region/MPO/county)    │  │
│  │  states/{fips}/boundaries.json  (DOT district APIs)    │  │
│  │  R2: {state}/_statewide/aggregates.json                │  │
│  │  R2: {state}/_mpo/{mpo}/aggregates.json                │  │
│  │  R2: {state}/{county}/*.csv                            │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌── PIPELINE LAYER (Python, runs monthly) ──────────────┐  │
│  │  download_state_crash_data.py  (statewide download)    │  │
│  │  split_by_jurisdiction.py      (per-county split)      │  │
│  │  generate_aggregates.py        (state/region/MPO JSON) │  │
│  │  upload-to-r2.py              (batch gzipped upload)   │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow by Tier

```
FEDERAL:  R2 _national/state_comparison.json ──► Dashboard + Choropleth
             (pre-aggregated, ~50 KB)              Zero CSV loading

STATE:    R2 {state}/_statewide/aggregates.json ──► Dashboard + Choropleth
             (pre-aggregated, ~200 KB)                Zero CSV loading

REGION:   R2 {state}/_region/{region}/aggregates.json ──► Dashboard + Choropleth
          + lazy-load county CSVs on demand              + Top-N markers
             (~300 KB JSON + on-demand CSVs)

MPO:      R2 {state}/_mpo/{mpo}/aggregates.json ──► Dashboard + Choropleth
          + lazy-load member county CSVs                 + Heatmap overlay
             (~100 KB JSON + on-demand CSVs)

COUNTY:   R2 {state}/{county}/*.csv ──► Full existing UI (unchanged)
             (~6-7 MB gzipped transfer)
```

### Memory Budget by Tier

| Tier | Max CSV in Memory | JSON Summaries | GeoJSON | Total Target |
|------|-------------------|----------------|---------|-------------|
| Federal | 0 | ~50 KB | ~200 KB (US states) | <1 MB |
| State | 0 | ~200-400 KB | ~600 KB (state+counties) | <5 MB |
| Region/MPO | 0-3 CSVs on demand | ~50-100 KB | ~400 KB | <150 MB |
| County | 1 CSV (~15-50 MB) | ~5 KB | ~30 KB | ~50 MB |

---

## 3. Critical Design Decisions

### Decision 1: Explore-Only Expansion

Solutions, Grants, and Reports sections do NOT expand to multi-jurisdiction views.

| Section | Behavior at Region/State | Rationale |
|---------|--------------------------|-----------|
| Solutions → CMF | Requires county+location selection | CMF analysis is per-intersection |
| Solutions → Warrants | Requires county+location selection | MUTCD warrants need specific traffic data |
| Solutions → MUTCD AI | Works (knowledge-based Q&A) | Not data-dependent |
| Solutions → Domain Knowledge | Works (reference material) | Not data-dependent |
| Grants | Requires county selection | HSIP applications are per-county |
| Reports | Requires county selection | Reports have jurisdiction-specific headers |

### Decision 2: Progressive Detail Rendering

| Tier | Map Visualization | Why |
|------|-------------------|-----|
| State | Choropleth only (counties colored by metric) | 140K+ crashes = no markers |
| Region | Choropleth + optional heatmap + Top-N markers | ~85K crashes = limited markers |
| County | Individual markers / clusters / heatmap (existing) | ~8K-25K = full detail |
| City | Filtered markers from county | Subset = faster |

### Decision 3: Pre-Aggregated JSON Over Runtime Computation

State and Region dashboards MUST render in <1 second from pre-computed JSON.
Zero raw CSV processing at State level. This is enforced by the monthly pipeline
producing `aggregates.json` files.

### Decision 4: Live API Boundaries Over Static GeoJSON

| Boundary Type | Source | Static File? |
|---------------|--------|-------------|
| State outline | TIGERweb layer 80 | No |
| County boundaries | TIGERweb layer 82 | No |
| City/Place boundaries | TIGERweb layers 28+30 | No |
| MPO boundaries | BTS NTAD MPO API | No (cached fallback on R2) |
| State DOT regions/districts | State DOT ArcGIS API | Fallback GeoJSON on R2 |
| Census tracts | TIGERweb layer 8 | No |
| Corridor routes | Static from CDOT/OSM | Yes (only static files needed) |

### Decision 5: Dynamic EPDO Preset System

| Tier | Default Preset | Rationale |
|------|---------------|-----------|
| Federal | HSM 2010 (K=462) | Uniform national weights for cross-state comparison |
| State (VA) | VDOT 2024 (K=1032) | State DOT staff expect their own weights |
| State (CO) | HSM 2010 (fallback) | Until CDOT-specific preset added |
| MPO/County | Inherit from parent state | Consistent within state |

Users can override any preset to Custom (K/A/B/C/O inputs), persisted via localStorage.

---

## 4. Implementation Phases

### Phase 1: Foundation — Config, Hierarchy & EPDO

**Goal:** Establish the configuration infrastructure that all subsequent phases depend on.

**Duration estimate:** Foundation work

#### 1.1 Create Colorado hierarchy.json

**File:** `states/08/hierarchy.json` (new)

Contains:
- State metadata (fips: "08", name: "Colorado", abbreviation: "CO")
- 5 CDOT regions with county FIPS arrays, map centers, key corridors
- 15 TPRs/MPOs with county FIPS arrays, BTS acronyms, types (mpo/rural_tpr)
- 4+ corridors (I-25, I-70, I-76, US-285) with county traversal lists
- regionType and tprType labels for dynamic UI

**Data:** All mappings are fully specified in `JURISDICTION_EXPANSION_PLAN.md` Section 8.

#### 1.2 Create Virginia hierarchy.json

**File:** `states/51/hierarchy.json` (new)

Contains:
- State metadata (fips: "51", name: "Virginia", abbreviation: "VA")
- 9 VDOT districts with jurisdiction mappings
- 14+ Virginia MPOs with BTS acronyms (HRTPO, NVTA, FAMPO, etc.)
- Major corridors (I-64, I-95, I-81, etc.)

#### 1.3 Create Boundary Config Files

**Files:**
- `states/virginia/boundaries.json` (new)
- `states/colorado/boundaries.json` (new)

Contains per-state:
- DOT district/region ArcGIS REST endpoint URL
- Field names (VDOT: `DISTRICT_NAME`, CDOT: `REGION`)
- Styling (color, weight, dashArray)
- Fallback GeoJSON path on R2

#### 1.4 Implement EPDO Preset System

**File:** `app/index.html` (modify)

Changes (from `JURISDICTION_PIPELINE_PLAN.md` Section 6):
1. Change `const EPDO_WEIGHTS` to `let EPDO_WEIGHTS`
2. Add `EPDO_PRESETS` object with 4 presets (HSM 2010, VDOT 2024, FHWA 2022, Custom)
3. Add preset switching functions: `loadEPDOPreset()`, `loadSavedEPDOPreset()`, `saveCustomEPDOWeights()`, `recalculateAllEPDO()`
4. Fix all 14 inline hardcoded EPDO calculations (replace `*462 + *62 + ...` with `calcEPDO()`)
5. Remove unused `EPDO_WEIGHTS_AD` at line ~69187
6. Add EPDO preset selector UI (radio group + custom inputs) in Upload Data tab
7. Add dynamic label IDs for dashboard + glossary EPDO display
8. Call `loadSavedEPDOPreset()` on DOMContentLoaded before first `updateDashboard()`

**Verification:** Run 9-point test plan (Section 6.7 of Pipeline Plan).

#### 1.5 Create HierarchyRegistry Module

**File:** `app/index.html` (add module inline)

```javascript
const HierarchyRegistry = (() => {
    let hierarchyData = null;
    return {
        async load(stateFips) { ... },
        getCountiesInRegion(regionId) { ... },
        getCountiesInTPR(tprId) { ... },
        getCountyMemberships(countyFips) { ... },
        getAllRegions() { ... },
        getAllTPRs() { ... },
        getCountiesOnCorridor(corridorId) { ... }
    };
})();
```

**Loads from:** `states/{fips}/hierarchy.json`

---

### Phase 2: Data Pipeline — Multi-County Processing & Aggregates

**Goal:** Build the backend pipeline that produces per-county CSVs and aggregate JSONs for all tiers.

#### 2.1 Enable Gzip Compression on R2

**File:** `scripts/upload-to-r2.py` (modify)

- Compress CSVs with gzip before upload
- Set `Content-Encoding: gzip` and `Content-Type: text/csv` headers
- Verify browser auto-decompresses correctly

**Impact:** ~70-80% transfer size reduction. No client-side code changes needed.

#### 2.2 Multi-County Download & Split

**Virginia:**
- **File:** `download_crash_data.py` (modify) — add `--all-jurisdictions` flag
- Downloads full statewide CSV from Virginia Roads ArcGIS API
- Splits into 133 per-jurisdiction CSVs by `jurisCode`

**Colorado:**
- **File:** `scripts/split_cdot_data.py` (modify) — extend to emit all 64 counties
- Process statewide CDOT data into per-county CSVs

#### 2.3 Create Aggregate Generator

**File:** `scripts/generate_aggregates.py` (new)

Reads per-county CSVs and produces:

```
R2 Structure:
├── {state}/_statewide/
│   ├── aggregates.json          (statewide totals, trends, YoY)
│   ├── county_summary.json      (per-county ranking by road type)
│   ├── mpo_summary.json         (per-MPO aggregates by road type)
│   └── snapshots/               (monthly archive of aggregates)
│
├── {state}/_region/{region_id}/
│   ├── aggregates.json          (region totals)
│   ├── member_counties.json     (per-county stats within region)
│   └── hotspots.json            (cross-county top locations)
│
├── {state}/_mpo/{mpo_id}/
│   ├── aggregates.json          (MPO totals)
│   ├── member_counties.json     (per-county stats within MPO)
│   └── hotspots.json            (cross-county top locations)
```

Each aggregate JSON includes:
- Road-type breakdowns (all_roads, county_roads, no_interstate)
- Severity counts (K/A/B/C/O) per road type
- EPDO scores (with `epdoWeights` and `epdoSource` recorded)
- Year-over-year trends
- Top collision types
- Top hotspot locations (for region/MPO)

#### 2.4 Batch Upload Pipeline

**File:** `scripts/upload-to-r2.py` (modify)

- Batch upload all jurisdictions (gzipped)
- Upload all aggregate JSONs
- Update `r2-manifest.json`

#### 2.5 Update GitHub Actions Workflows

**File:** `.github/workflows/download-data.yml` (modify)

Add workflow inputs for:
- `scope`: county / region / statewide
- `state`: colorado / virginia
- Stage: download → split → validate → aggregate → upload

---

### Phase 3: Boundary System — APIs, GeoJSON & Caching

**Goal:** Implement the BoundaryService that dynamically loads boundaries from federal APIs.

#### 3.1 Implement BoundaryService Module

**File:** `app/index.html` (add module inline)

```javascript
const BoundaryService = (() => {
    const TIGERWEB_BASE = '...';
    const BTS_MPO_BASE = '...';

    return {
        async discoverState(stateFips, stateAbbrev) { ... },
        async getPlaces(stateFips, countyFips) { ... },
        async getCensusTracts(stateFips, countyFips) { ... },
        async getUrbanAreas(stateFips) { ... },
        async getSchoolDistricts(stateFips) { ... },
        LAYERS: { states: 80, counties: 82, ... }
    };
})();
```

**Key methods:**
- `discoverState()` — parallel fetch of state outline + all counties + all MPOs
- `getPlaces()` — city/CDP boundaries for drill-down
- All results cached in IndexedDB with annual expiration (Census) or quarterly (BTS)

#### 3.2 Implement SpatialClipService

**File:** `app/index.html` (add module inline)

```javascript
const SpatialClipService = (() => {
    return {
        clipPoints(features, jurisdictionId) { ... },
        clipLines(features, jurisdictionId) { ... },
        clipPolygons(features, jurisdictionId) { ... },
        getJurisdictionPolygon(jurisdictionId) { ... }
    };
})();
```

**Integration points:**
- `transitTryStatewideData()` — clip transit stops to jurisdiction polygon
- Overpass road/signal fetches — clip to polygon
- Mapillary asset fetches — clip to polygon
- Graceful fallback: if polygon unavailable, return unclipped with UI warning

#### 3.3 Add BTS MPO Boundary Layer

**File:** `app/index.html` (modify `BTS_ENDPOINTS`)

Add `btsMPOBoundaries` endpoint configuration:
- Purple dashed polygon style
- State-filtered queries
- Auto-load on MPO dropdown selection
- Cache per-state in `geojsonCache`

#### 3.4 Add DOT District Boundary Layer

**File:** `app/index.html` (add new function)

`loadDOTDistrictBoundary(stateKey)`:
- Read endpoint from `states/{state}/boundaries.json`
- Try live ArcGIS REST API first
- Fallback to cached GeoJSON from R2
- Toggle in Asset Layers Panel at State view

#### 3.5 Pre-Download Boundary Fallbacks

**Script:** One-time download + upload to R2:
- State/county GeoJSONs from Census TIGER/Line
- MPO boundaries from BTS NTAD per state
- DOT district boundaries from state DOT ArcGIS APIs
- Upload to `shared/boundaries/` in R2

---

### Phase 4: UI Foundation — Tier Selector & Scope State

**Goal:** Build the UI scaffolding for multi-tier views without implementing per-tier content yet.

#### 4.1 Add Tier Selector Control

**File:** `app/index.html` (modify Upload Data tab)

Add segmented control above the existing State/Jurisdiction selector:

```
[Federal] [State] [Region/MPO] [County (default)]
```

When tier changes:
- Show/hide relevant dropdowns (State/Region/MPO/County)
- Update `jurisdictionContext.viewTier`
- Toggle tab visibility per tier matrix
- Update AI context awareness

#### 4.2 Implement Scope State Manager

**File:** `app/index.html` (add to global state)

```javascript
const jurisdictionContext = {
    viewTier: 'county',          // federal | state | region | mpo | county | city | corridor
    state: null,                 // { fips, name, abbreviation }
    region: null,                // { id, name, counties[] }
    mpo: null,                   // { id, name, btsAcronym, counties[] }
    county: null,                // { fips, name }
    city: null,                  // { name, placeFips }
    corridor: null,              // { id, name, counties[] }
    roadType: 'all_roads',       // all_roads | county_roads | no_interstate
    solutionsScopeCounty: null,  // Temporary county for Solutions/Grants at higher scope
    hierarchyLoaded: false
};
```

#### 4.3 Tab Visibility Controller

**File:** `app/index.html` (add function)

`updateTabVisibility(tier)` — show/hide tabs based on the tier matrix:

| Tab | Federal | State | Region/MPO | County |
|-----|---------|-------|------------|--------|
| Dashboard | Show | Show | Show | Show |
| Map | Show | Show | Show | Show |
| Crash Tree | Hide | Show | Show | Show |
| Safety Focus | Show | Show | Show | Show |
| Fatal & Speeding | Hide | Show | Show | Show |
| Hot Spots | Show | Show | Show | Show |
| Intersections | Hide | Summary | Show | Show |
| Ped/Bike | Hide | Hide | Show | Show |
| Analysis | Show | Show | Show | Show |
| Deep Dive | Hide | Hide | Hide | Show |
| Crash Prediction | Hide | Hide | Hide | Show |
| **CMF** | Needs County | Needs County | Needs County | Show |
| **Warrants** | Needs County | Needs County | Needs County | Show |
| **Grants** | Needs County | Needs County | Needs County | Show |
| **Reports** | Needs County | Needs County | Needs County | Show |

#### 4.4 Add Road Type Selector to All Tiers

Extend the existing road type radio (County Roads / No Interstate / All Roads) to appear in Federal, State, and MPO views. The selection filters which aggregate data is displayed.

#### 4.5 Aggregate Data Loader

**File:** `app/index.html` (add function)

`loadTierData(tier, scope)`:
- Federal → fetch `_national/state_comparison.json` from R2
- State → fetch `{state}/_statewide/aggregates.json` from R2
- Region → fetch `{state}/_region/{region}/aggregates.json` from R2
- MPO → fetch `{state}/_mpo/{mpo}/aggregates.json` from R2
- County → existing CSV loading (unchanged)

---

### Phase 5: State View — Dashboard, Map & Explore Tabs

**Goal:** Implement the statewide view for CDOT HQ / VDOT HQ users.

#### 5.1 State Dashboard

**Data source:** `{state}/_statewide/county_summary.json` (pre-aggregated)

Renders:
- KPI cards: total crashes, fatalities, EPDO, 5-year trend (statewide)
- Per-region breakdown table (for CO: 5 CDOT regions; for VA: 9 VDOT districts)
- Per-county ranking table with sorting (crashes, EPDO, KA rate, trend)
- County comparison bar charts
- Drill-down: click county row → switch to County view

#### 5.2 State Map — County Choropleth

**Data source:** Pre-aggregated JSON + GeoJSON boundaries

Renders:
- County polygons colored by selected metric (crash rate, EPDO, KA count, trend)
- Metric selector dropdown above map
- Group-by toggle (Region / County / TPR)
- DOT district overlay toggle
- MPO boundary overlay toggle
- Hover county → tooltip with summary stats
- Click county → drill down to County view
- NO individual crash markers (memory: ~200 KB JSON only)

**Implementation:**
- Use `L.geoJSON` with `style` function reading summary stats
- `onEachFeature` with `mouseover`/`mouseout` handlers
- Click handler changes scope and triggers re-render

**Color scales:**
| Metric | Scale |
|--------|-------|
| Total Crashes | Sequential blue |
| EPDO Score | Sequential red-orange |
| KA Crash Count | Sequential red |
| Crash Rate | Sequential green→red |
| YoY Trend | Diverging green-gray-red |

#### 5.3 State Hot Spots

**Data source:** `{state}/_statewide/county_summary.json` hotspots array

- Top 50 statewide hotspot locations
- Grouped by region (default) or ungrouped (flat ranking)
- County column identifies which county each location is in
- Filter by crash type, severity, route type

#### 5.4 State Safety Focus

- Statewide emphasis area summary
- Per-region comparison for each category (Ped, Bike, Speed, Impaired, etc.)
- Aligned with SHSP emphasis areas

#### 5.5 State Crash Tree

- Statewide crash tree with region attribution
- "Broadside → Dark → Wet → 60% from Region 1, 20% from Region 2"
- Toggle: aggregate tree vs. by-region comparison

#### 5.6 State Analysis Tab

- County comparison charts (bar, radar, sparklines)
- School safety: disabled (message: "available at county/region level")
- Transit safety: disabled
- Mapillary/Road Inventory: disabled
- Infrastructure assets: disabled

---

### Phase 6: Region/MPO View — Cross-County Analysis

**Goal:** Implement the regional view for CDOT Region engineers and MPO planners.

#### 6.1 Region Dashboard

**Data source:** `{state}/_region/{region}/aggregates.json`

Renders:
- Region header: "CDOT Region 1 — Denver Metro / Central, 10 Counties"
- KPI cards: region aggregate totals
- Per-county breakdown table with drill-down
- County comparison bar charts + sparklines

#### 6.2 Region Map — Choropleth + Heatmap Hybrid

**Data source:** JSON aggregates + GeoJSON boundaries + optional lazy-loaded CSVs

Renders:
- Region boundary as primary outline
- County polygons within region colored by metric
- OPTIONAL heatmap overlay (toggle on/off) from aggregated centroids
- Top 20 hotspot locations as labeled markers
- Hover county → tooltip with county stats
- Click county → zoom to county view

**Layer controls:**
| Control | Region |
|---------|--------|
| Marker type toggle | Heatmap only |
| Choropleth metric | Shown |
| Year filter | Shown |
| Route filter | Shown |
| Severity filter | Shown |
| Hotspot markers (Top N) | Top 20 |

#### 6.3 MPO Dashboard + Map

Same pattern as Region but:
- Auto-load BTS NTAD boundary on MPO selection
- Member county fills + crash heatmap
- Progressive detail: zoom in → transition to markers

#### 6.4 Cross-County Hot Spots

**THE most valuable feature of jurisdiction expansion.**

- Hotspots from different counties ranked TOGETHER using same EPDO scoring
- County column identifies source county
- Group-by options: None / By County / By Route
- Filter: route, severity, crash type

```
Rank │ Location                  │ County    │ EPDO  │ K+A
─────┼───────────────────────────┼───────────┼───────┼────
  1  │ I-25 & CO-470            │ Douglas   │ 4,231 │ 12
  2  │ I-70 & Wadsworth Blvd    │ Jefferson │ 3,876 │  9
  3  │ Colfax Ave & Federal Blvd│ Denver    │ 3,654 │  8
```

#### 6.5 Region Crash Tree

- Aggregate tree combining all counties
- County attribution column on each node
- "Rear End → Daylight → Dry → 85% from Denver/Arapahoe/Jefferson"

#### 6.6 Region Safety Focus

- Aggregate categories across all counties
- Per-county comparison within each category
- County-level comparison charts per emphasis area

#### 6.7 Region Ped/Bike Tab

- Combined ped/bike across all counties
- Per-county comparison (crashes per capita)
- Regional heatmap overlay

#### 6.8 Region Analysis Sub-Tabs

| Sub-Tab | Region Behavior |
|---------|----------------|
| Infrastructure Assets | Limited (ArcGIS + School + Transit only) |
| School Safety | Aggregated (multiple LEAs from hierarchy) |
| Transit Safety | Aggregated (region bbox from BTS) |
| Mapillary Assets | Disabled (county quick-select buttons) |
| Road Inventory | Disabled (county quick-select buttons) |

---

### Phase 7: Federal View — National Overview

**Goal:** Implement cross-state comparison for FHWA analysts.

#### 7.1 National Summary Generator

**File:** `scripts/generate_national_summary.py` (new)

- Roll up state aggregates
- Integrate FARS fatality data by state
- HSIP performance targets by state

#### 7.2 Federal Dashboard

- State comparison table (states with data)
- National totals + per-state breakdown
- Click state row → drill down to State view

#### 7.3 Federal Map — US State Choropleth

- US map with state polygons colored by metric
- Metric selector: crashes, fatalities, KA rate, EPDO, trend
- Hover → tooltip, Click → drill to State view
- Only Bridges layer available (clustered)

#### 7.4 Federal Safety Focus + Hot Spots

- National emphasis area summary
- Top locations across all states
- Comparison between states

---

### Phase 8: Tab Scope Restrictions & Non-Explore Behavior

**Goal:** Implement the inline county selector for Solutions/Grants/Reports at higher scopes.

#### 8.1 Inline County Selector Component

When user is at Region/State scope and navigates to Solutions/Grants/Reports:

```
┌────────────────────────────────────────────────────────┐
│ COUNTERMEASURES / CMF ANALYSIS                          │
│                                                          │
│ Current scope: Region 1 (10 counties)                   │
│                                                          │
│ Countermeasure analysis requires a specific location.    │
│ Select a location to begin:                             │
│                                                          │
│ Option 1: [Select from Region Hot Spots ▶]              │
│ Option 2: [Select a County ▼] then choose a location    │
│ Option 3: [Search by route/intersection name...]        │
└────────────────────────────────────────────────────────┘
```

#### 8.2 Solutions Scope County Persistence

- Store selected county in `jurisdictionContext.solutionsScopeCounty`
- If user selects "Douglas County" in CMF, it persists to Warrants tab
- Resets when main scope selector changes
- Breadcrumb: "Region 1 > Douglas County > Countermeasures"

#### 8.3 Disabled Sub-Tab Messages

For Mapillary Assets and Road Inventory at Region/State:

```
⚠️ Mapillary asset analysis is available at county and city levels only.
   Current scope: Region 1 (10 counties)
   [Select a County ▼] to use Mapillary Assets
   Quick select: [Douglas] [Denver] [Arapahoe] [Jefferson] ...
```

#### 8.4 Grants at Higher Scope

```
HSIP grant analysis is performed per county.
[Douglas County] [Denver] [Arapahoe] [Jefferson] ...
💡 Tip: Use Hot Spots tab to see locations ranked across the entire region.
```

---

### Phase 9: Advanced Features — DuckDB-WASM & Optimization

**Goal:** Add advanced analytical capabilities for power users.

#### 9.1 IndexedDB Caching Layer

- Cache parsed CSV data in IndexedDB for instant repeat visits
- LRU eviction when approaching 2GB limit
- Service Worker checks monthly for updated data
- Cache boundary GeoJSON (annual expiration for Census, quarterly for BTS)

#### 9.2 DuckDB-WASM Integration (Future)

- Load DuckDB-WASM (~3-5 MB, browser-cached)
- Query remote Parquet files via HTTP range requests
- Cross-county SQL: `SELECT county, route, SUM(epdo) FROM '*.parquet' GROUP BY ...`
- Ad-hoc analysis without loading all CSVs

#### 9.3 WebWorker Processing

- Move heavy data processing off main thread
- CSV parsing, aggregation, spatial clipping in WebWorkers
- Keep UI responsive during region-level data loading

#### 9.4 Parquet Conversion

- Convert CSVs to Apache Parquet in pipeline (10x compression)
- Column-selective reading for specific analysis needs
- Compatible with DuckDB-WASM queries

---

### Phase 10: New State Onboarding & Production Readiness

**Goal:** Make adding new states a zero-code config-only process.

#### 10.1 Create State Template

**Files:**
- `states/template/config.json.template`
- `states/template/hierarchy.json.template`
- `states/template/boundaries.json.template`
- `states/template/README.md` (onboarding instructions)

#### 10.2 Onboarding Script

**File:** `scripts/onboard_jurisdiction.py` (new)

```bash
# Onboard a single county
python onboard_jurisdiction.py --state colorado --county "El Paso"

# Onboard entire region
python onboard_jurisdiction.py --state colorado --region 2

# Onboard all counties in a state
python onboard_jurisdiction.py --state colorado --all

# Setup new state from scratch
python onboard_jurisdiction.py --state texas --setup
```

Steps:
1. Validate county in FIPS database
2. Download crash data (state-specific)
3. Process through pipeline
4. Generate aggregates
5. Upload to R2
6. Fetch boundary from Census
7. Update hierarchy.json
8. Update manifest

#### 10.3 Boundary Auto-Discovery

When adding a new state, `BoundaryService.discoverState(fips, abbrev)` auto-fetches:
- State outline (TIGERweb)
- All counties (TIGERweb)
- All MPOs (BTS NTAD)
- Census tracts, urban areas, school districts (TIGERweb, on demand)

**Only manual work:**
- Create `hierarchy.json` with DOT region→county mapping
- Create `boundaries.json` with DOT district API endpoint
- Add column mapping to `state_adapter.js`

#### 10.4 First New State Onboard

Target: Maryland or North Carolina (both have public ArcGIS crash data APIs).

Verify complete pipeline:
- Download → Validate → Geocode → Split → Aggregate → Upload → Display
- DOT districts render on map
- BTS MPOs connect correctly
- EPDO weights applied correctly

---

## 5. Files To Create

| File | Phase | Purpose |
|------|-------|---------|
| `states/08/hierarchy.json` | 1.1 | Colorado region/MPO/county mappings |
| `states/51/hierarchy.json` | 1.2 | Virginia VDOT districts/MPO mappings |
| `states/virginia/boundaries.json` | 1.3 | VA DOT district API config |
| `states/colorado/boundaries.json` | 1.3 | CO DOT region API config |
| `scripts/generate_aggregates.py` | 2.3 | State/region/MPO aggregate JSON generator |
| `scripts/generate_national_summary.py` | 7.1 | National cross-state summary |
| `scripts/onboard_jurisdiction.py` | 10.2 | Automated jurisdiction onboarding |
| `states/template/config.json.template` | 10.1 | Template for new states |
| `states/template/hierarchy.json.template` | 10.1 | Template for new states |
| `states/template/boundaries.json.template` | 10.1 | Template for new states |
| `states/template/README.md` | 10.1 | Onboarding instructions |

---

## 6. Files To Modify

| File | Phase | Changes |
|------|-------|---------|
| `app/index.html` | 1.4-8.4 | EPDO presets, HierarchyRegistry, BoundaryService, SpatialClipService, tier selector, scope state, tab visibility, choropleth map, tier-specific dashboards, inline county selector, disabled sub-tab messages |
| `scripts/upload-to-r2.py` | 2.1 | Gzip compression, batch upload |
| `download_crash_data.py` | 2.2 | `--all-jurisdictions` flag |
| `scripts/split_cdot_data.py` | 2.2 | Extend to all 64 counties |
| `.github/workflows/download-data.yml` | 2.5 | Multi-scope workflow |
| `config.json` | 3.3 | Add TIGERweb layer IDs, BTS MPO config |
| `states/virginia/config.json` | 1.4 | Verify EPDO weights |
| `states/colorado/config.json` | 1.4 | Verify EPDO weights |

---

## 7. Dependency Graph

```
Phase 1: Foundation
├── 1.1 CO hierarchy.json ─────────────────────┐
├── 1.2 VA hierarchy.json ─────────────────────┤
├── 1.3 Boundary configs ──────────────────────┤
├── 1.4 EPDO Preset System ───────────────────┤ (independent)
└── 1.5 HierarchyRegistry ────────────────────┤
                                                │
Phase 2: Pipeline ──────────────── depends on ──┤
├── 2.1 R2 Gzip ──────────────────────────────┤
├── 2.2 Multi-county download/split ──────────┤
├── 2.3 Aggregate generator ──── needs 1.1,1.2 ┤
├── 2.4 Batch upload ─────────── needs 2.1,2.3 │
└── 2.5 Workflow updates ─────── needs 2.2,2.4 │
                                                │
Phase 3: Boundaries ─────────── depends on ─────┤
├── 3.1 BoundaryService ──────── needs 1.3     │
├── 3.2 SpatialClipService ──── needs 3.1      │
├── 3.3 BTS MPO layer ────────── needs 3.1     │
├── 3.4 DOT district layer ──── needs 1.3, 3.1 │
└── 3.5 Pre-download fallbacks ─ needs 3.1     │
                                                │
Phase 4: UI Foundation ─────── depends on ──────┤
├── 4.1 Tier selector ────────── needs 1.5     │
├── 4.2 Scope state manager ──── needs 4.1     │
├── 4.3 Tab visibility ────────── needs 4.2    │
├── 4.4 Road type at all tiers ── needs 4.1    │
└── 4.5 Aggregate data loader ── needs 2.3, 4.2│
                                                │
Phase 5: State View ────────── depends on ──────┤
├── 5.1 State Dashboard ─────── needs 4.5      │
├── 5.2 State Map ────────────── needs 3.1, 4.5│
├── 5.3 State Hot Spots ─────── needs 4.5      │
├── 5.4 State Safety Focus ──── needs 4.5      │
├── 5.5 State Crash Tree ────── needs 4.5      │
└── 5.6 State Analysis ─────── needs 4.5       │
                                                │
Phase 6: Region/MPO ───────── depends on ───────┤
├── 6.1 Region Dashboard ────── needs 4.5      │
├── 6.2 Region Map ──────────── needs 3.1, 4.5 │
├── 6.3 MPO Dashboard+Map ──── needs 3.3, 4.5  │
├── 6.4 Cross-County Hot Spots  needs 4.5      │
├── 6.5 Region Crash Tree ──── needs 4.5       │
├── 6.6 Region Safety Focus ─── needs 4.5      │
├── 6.7 Region Ped/Bike ────── needs 4.5       │
└── 6.8 Region Analysis ────── needs 4.5       │
                                                │
Phase 7: Federal View ─────── depends on ───────┤
├── 7.1 National summary script  needs 2.3     │
├── 7.2 Federal Dashboard ───── needs 4.5, 7.1 │
├── 7.3 Federal Map ─────────── needs 3.1, 4.5 │
└── 7.4 Federal Safety+Hotspots needs 4.5      │
                                                │
Phase 8: Scope Restrictions ─── needs Phase 4 ──┤
├── 8.1 Inline county selector                  │
├── 8.2 Solutions scope persistence             │
├── 8.3 Disabled sub-tab messages               │
└── 8.4 Grants at higher scope                  │
                                                │
Phase 9: Advanced ──────────── needs Phase 5+ ──┤
├── 9.1 IndexedDB caching                       │
├── 9.2 DuckDB-WASM                             │
├── 9.3 WebWorker processing                    │
└── 9.4 Parquet conversion                      │
                                                │
Phase 10: Onboarding ─────── needs Phase 1-7 ───┘
├── 10.1 State template
├── 10.2 Onboarding script
├── 10.3 Boundary auto-discovery
└── 10.4 First new state
```

### Critical Path

```
1.1/1.2 (hierarchy) → 2.3 (aggregates) → 4.5 (loader) → 5.1 (state dashboard)
                                                        → 6.1 (region dashboard)
                                                        → 7.2 (federal dashboard)
```

The **aggregate generator** (2.3) is the key bottleneck — all tier dashboards depend on it.
Boundary work (Phase 3) and UI scaffolding (Phase 4) can proceed in parallel.

---

## 8. Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **TIGERweb API downtime** | Boundaries don't load | Medium | IndexedDB cache + R2 fallback GeoJSON |
| **BTS NTAD API changes** | MPO boundaries break | Low | Cache + static fallback, quarterly check |
| **Memory overflow at Region** | Browser crashes with 10-county data | Medium | Progressive loading, LRU cache eviction |
| **State choropleth performance** | Slow rendering with 64 county polygons | Low | Simplify with `turf.simplify()` at state zoom |
| **Duplicate function names** | JS silently overwrites functions | High | Search before creating; use unique prefixes |
| **Data scope confusion** | Showing county data as statewide | High | Strict tier awareness in all rendering functions |
| **EPDO recalculation lag** | Slow when switching presets with 100K+ rows | Low | EPDO is 5-term multiply; <100ms for 100K rows |
| **Breaking existing county view** | Regression in current functionality | Critical | Phase 4 tier selector defaults to County; no changes to county rendering path |

---

## 9. Testing Strategy

### Per-Phase Testing

| Phase | Test |
|-------|------|
| 1 | EPDO: 9-point verification plan. Hierarchy: load + query all regions/MPOs |
| 2 | Aggregate JSONs validate against raw CSVs. Gzip decompresses in browser |
| 3 | Boundaries load from TIGERweb/BTS. Fallback to cached. SpatialClip correct |
| 4 | Tier selector toggles tabs correctly. Road type filters all tiers |
| 5 | State dashboard matches aggregate JSON. Choropleth renders, drill-down works |
| 6 | Region cross-county hotspots ranked correctly. MPO auto-loads boundary |
| 7 | Federal choropleth renders. State comparison accurate |
| 8 | Inline county selector persists. Solutions/Grants show correct prompts |
| 9 | IndexedDB caches and evicts correctly. DuckDB queries produce correct results |
| 10 | New state onboards end-to-end. All boundaries auto-discover |

### Cross-Cutting Tests

- [ ] Verify crash counts match across related views (county sum = region total = state total)
- [ ] Test with location selected AND without at each tier
- [ ] Test with date filter applied AND without
- [ ] Verify no duplicate function names introduced
- [ ] Console shows expected data flow (no errors)
- [ ] UI indicators reflect actual data tier being used
- [ ] County view is 100% unchanged (regression test)
- [ ] Memory stays within budget for each tier
- [ ] Road type filter works at all tiers
- [ ] EPDO preset switching recalculates all visible tabs

---

## Summary

This plan synthesizes three detailed planning documents into a **10-phase implementation roadmap**:

1. **Foundation** — Config files, hierarchy, EPDO presets
2. **Pipeline** — Multi-county processing, aggregate generation, R2 gzip
3. **Boundaries** — BoundaryService, SpatialClipService, API layers
4. **UI Foundation** — Tier selector, scope state, tab visibility
5. **State View** — Dashboard, choropleth, hotspots, safety focus
6. **Region/MPO View** — Cross-county analysis, heatmap, drill-down
7. **Federal View** — National overview, state comparison
8. **Scope Restrictions** — Inline county selector for Solutions/Grants/Reports
9. **Advanced** — IndexedDB, DuckDB-WASM, WebWorkers, Parquet
10. **Onboarding** — Templates, scripts, first new state

The critical path runs through **hierarchy → aggregates → data loader → dashboards**.
The design preserves the existing county view, adds no backend servers,
and targets $0 infrastructure cost through Phase 9.
