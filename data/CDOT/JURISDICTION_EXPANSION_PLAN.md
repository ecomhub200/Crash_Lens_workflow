# CRASH LENS — Comprehensive Multi-Jurisdiction Expansion Plan

## For CDOT (Colorado Department of Transportation) & Beyond

**Version:** 1.0
**Date:** February 9, 2026
**Status:** PLANNING ONLY — Not Yet Implemented

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current Architecture Audit](#2-current-architecture-audit)
3. [CDOT Organizational Structure](#3-cdot-organizational-structure)
4. [Colorado MPOs & Regional Planning Organizations](#4-colorado-mpos--regional-planning-organizations)
5. [User Persona Matrix](#5-user-persona-matrix)
6. [Hierarchical Jurisdiction Model](#6-hierarchical-jurisdiction-model)
7. [Dynamic Multi-Tier Architecture](#7-dynamic-multi-tier-architecture)
8. [State-Level Configuration System](#8-state-level-configuration-system)
9. [Data Pipeline Expansion](#9-data-pipeline-expansion)
10. [UI/UX Redesign for Multi-Jurisdiction](#10-uiux-redesign-for-multi-jurisdiction)
11. [Automated Jurisdiction Onboarding](#11-automated-jurisdiction-onboarding)
12. [CDOT District Office Implementation](#12-cdot-district-office-implementation)
13. [MPO/Regional Aggregation Engine](#13-mporegional-aggregation-engine)
14. [Statewide View (CDOT HQ)](#14-statewide-view-cdot-hq)
15. [Multi-State Scaling Strategy](#15-multi-state-scaling-strategy)
16. [Spatial Accuracy: Bbox-Fetch + Polygon-Clip Strategy](#16-spatial-accuracy-bbox-fetch--polygon-clip-strategy)
17. [Database & Storage Strategy](#17-database--storage-strategy)
18. [Data Pipeline & Query Architecture (Zero-Backend)](#18-data-pipeline--query-architecture-zero-backend)
19. [Phased Implementation Roadmap](#19-phased-implementation-roadmap)
20. [Risk Assessment & Mitigations](#20-risk-assessment--mitigations)
21. [Appendix A: Complete Colorado County-to-Region Mapping](#appendix-a-complete-colorado-county-to-region-mapping)
22. [Appendix B: Configuration Schema Reference](#appendix-b-configuration-schema-reference)

---

## 1. Executive Summary

### The Problem

CRASH LENS currently operates as a **single-jurisdiction tool** — one county at a time (Douglas County, CO or Henrico County, VA). However, real-world transportation safety work operates at multiple overlapping scales:

- A **county engineer** needs their county's data
- A **CDOT Region engineer** needs data across 8-15 counties in their region
- An **MPO planner** needs data across the 3-9 counties in their planning area
- A **CDOT HQ analyst** needs statewide data subdivided by their 5 engineering regions
- A **FHWA Division** needs cross-state comparisons

These are not separate tools — they are **different views of the same data** with different aggregation levels.

### The Solution

Build a **hierarchical jurisdiction model** with a **dynamic aggregation engine** that lets any user see crash data at their level — from a single intersection up to the entire state — with automatic roll-up and drill-down capabilities.

### Key Design Principles

1. **Zero-code onboarding** — Adding a new county should require ONLY a config file, not code changes
2. **Hierarchical by nature** — County → Region/District → MPO → State are first-class concepts
3. **Data flows up, context flows down** — Raw data lives at the county level; aggregation is computed dynamically
4. **One tool, many views** — The same CRASH LENS instance serves county engineers, regional planners, and state HQ
5. **Zero-backend by design** — Pre-processed Parquet files + DuckDB-WASM in-browser SQL eliminates the need for backend servers; IndexedDB provides offline caching; edge SQLite available only if auth is needed

---

## 2. Current Architecture Audit

### What Already Exists (Strengths)

| Component | Status | Location |
|-----------|--------|----------|
| StateAdapter (auto-detect CSV format) | ✅ Working | `states/state_adapter.js` |
| FIPSDatabase (all 50 states + counties) | ✅ Working | `states/fips_database.js` |
| US Counties DB (embedded) | ✅ Working | `states/us_counties_db.js` |
| Colorado column mapping | ✅ Working | `data/CDOT/config.json` |
| Virginia 133 jurisdictions | ✅ Working | `config.json` |
| Colorado 6 jurisdictions (hardcoded) | ✅ Working | `state_adapter.js` |
| Dynamic jurisdiction dropdown | ✅ Working | `app/index.html` |
| Data processing pipeline | ✅ Working | `scripts/process_crash_data.py` |
| CDOT data downloader | ✅ Working | `download_cdot_crash_data.py` |

### What's Missing (Gaps)

| Gap | Impact | Priority |
|-----|--------|----------|
| No region/district concept | Cannot aggregate counties into CDOT regions | P0 - Critical |
| No MPO concept | Cannot group counties into planning organizations | P0 - Critical |
| No statewide view | CDOT HQ cannot see all 64 counties at once | P0 - Critical |
| Single-county data loading | Must reload entire app to switch jurisdictions | P1 - High |
| No multi-county selection | Cannot compare or combine adjacent counties | P1 - High |
| Hardcoded Colorado jurisdictions | Only 6 of 64 CO counties defined | P1 - High |
| No role-based views | Everyone sees the same UI regardless of role | P2 - Medium |
| No automated data pipeline for all counties | Only Douglas County downloads automatically | P2 - Medium |
| No cross-jurisdiction hotspot analysis | Hotspots stop at county borders | P2 - Medium |
| No jurisdiction boundary GeoJSON | Map doesn't show county/region outlines | P3 - Nice to have |

---

## 3. CDOT Organizational Structure

### CDOT Engineering Regions

CDOT divides Colorado into **5 Engineering Regions** plus a headquarters. Each region has a Regional Transportation Director (RTD) and manages highway operations, maintenance, and safety within their geographic area.

```
┌─────────────────────────────────────────────────────┐
│                   CDOT HEADQUARTERS                  │
│              4201 E. Arkansas Ave, Denver             │
│                                                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐             │
│  │ Region 1 │ │ Region 2 │ │ Region 3 │             │
│  │ (Denver  │ │ (Pueblo/ │ │ (Grand   │             │
│  │  Metro)  │ │  SE CO)  │ │ Junction)│             │
│  └──────────┘ └──────────┘ └──────────┘             │
│  ┌──────────┐ ┌──────────┐                           │
│  │ Region 4 │ │ Region 5 │                           │
│  │ (Greeley/│ │ (Durango/│                           │
│  │  NE CO)  │ │  SW CO)  │                           │
│  └──────────┘ └──────────┘                           │
└─────────────────────────────────────────────────────┘
```

### Region 1 — Denver Metro / Central

- **Office:** 2000 S. Holly St., Denver, CO 80222
- **Counties (approximately 10):** Adams, Arapahoe, Boulder, Broomfield, Clear Creek, Denver, Douglas, Gilpin, Jefferson, Summit
- **Key Corridors:** I-25 (Denver Metro), I-70 (Mountain Corridor), I-76, I-225, I-270, US-36, US-6, CO-470, C-470, E-470

### Region 2 — Southeast / Pueblo

- **Office:** 905 Erie Ave., Pueblo, CO 81001
- **Counties (approximately 17):** Baca, Bent, Chaffee, Crowley, Custer, El Paso, Elbert, Fremont, Huerfano, Kiowa, Lake, Las Animas, Lincoln, Otero, Park, Prowers, Pueblo, Teller
- **Key Corridors:** I-25 (south of Denver), US-24, US-50, US-285, US-160, CO-115

### Region 3 — Grand Junction / Western Slope

- **Office:** 222 S. 6th St., Grand Junction, CO 81501
- **Counties (approximately 14):** Delta, Eagle, Garfield, Grand, Gunnison, Hinsdale, Jackson, Mesa, Moffat, Montrose, Ouray, Pitkin, Rio Blanco, Routt, San Miguel
- **Key Corridors:** I-70 (Glenwood Canyon to Utah), US-40, US-50, US-550, CO-82, CO-133

### Region 4 — Greeley / Northeast

- **Office:** 1420 2nd St., Greeley, CO 80631
- **Counties (approximately 12):** Cheyenne, Kit Carson, Larimer, Logan, Morgan, Phillips, Sedgwick, Washington, Weld, Yuma
- **Key Corridors:** I-25 (north of Denver), I-76 (east), US-34, US-85, US-287, CO-14

### Region 5 — Durango / Southwest & San Luis Valley

- **Office:** 3803 N. Main Ave., Durango, CO 81301
- **Counties (approximately 13):** Alamosa, Archuleta, Conejos, Costilla, Dolores, La Plata, Mineral, Montezuma, Rio Grande, Saguache, San Juan
- **Key Corridors:** US-160, US-550, US-285 (south), CO-17, CO-145, CO-149

> **Note:** Some counties may overlap between regions, especially in boundary areas. The exact mapping should be confirmed against the latest CDOT region boundary GIS shapefile available at https://dtdapps.coloradodot.info/otis.

---

## 4. Colorado MPOs & Regional Planning Organizations

Colorado is divided into **15 Transportation Planning Regions (TPRs)**: 5 MPOs (urban) and 10 rural TPRs.

### 5 Metropolitan Planning Organizations (MPOs)

| # | MPO | Abbreviation | Counties Covered |
|---|-----|-------------|-----------------|
| 1 | Denver Regional Council of Governments | DRCOG | Adams, Arapahoe, Boulder, Broomfield, Clear Creek, Denver, Douglas, Gilpin, Jefferson, SW Weld |
| 2 | North Front Range MPO | NFRMPO | Larimer, Weld (partial) |
| 3 | Pikes Peak Area Council of Governments | PPACG | El Paso, Teller |
| 4 | Pueblo Area Council of Governments | PACOG | Pueblo |
| 5 | Grand Valley MPO | GVMPO | Mesa |

### 10 Rural Transportation Planning Regions (TPRs)

| # | TPR Name | Abbreviation | Counties Covered | Administering Organization |
|---|----------|-------------|-----------------|---------------------------|
| 6 | Central Front Range | CFR | Custer, Fremont, Park (+ parts of El Paso, Teller) | Upper Arkansas Area COG (UAACOG) |
| 7 | Eastern | EA | Cheyenne, Elbert, Kit Carson, Lincoln, Logan, Phillips, Sedgwick, Washington, Yuma | Yuma County |
| 8 | Gunnison Valley | GV | Delta, Gunnison, Hinsdale, Montrose, Ouray, San Miguel | Region 10 LEAP |
| 9 | Intermountain | IM | Eagle, Garfield, Lake, Pitkin, Summit | NW CO COG (NWCCOG) |
| 10 | Northwest | NW | Grand, Jackson, Moffat, Rio Blanco, Routt | Town of Fraser |
| 11 | San Luis Valley | SLV | Alamosa, Chaffee, Conejos, Costilla, Mineral, Rio Grande, Saguache | SLV Development Resources Group |
| 12 | South Central | SC | Huerfano, Las Animas | South Central COG (SCCOG) |
| 13 | Southeast | SE | Baca, Bent, Crowley, Kiowa, Otero, Prowers | SE CO Enterprise Development |
| 14 | Southwest | SW | Archuleta, Dolores, La Plata, Montezuma, San Juan | SW CO COG (SWCCOG) |
| 15 | Upper Front Range | UFR | Morgan (+ parts of Larimer, Weld) | CDOT Planning Liaison |

### Federal MPO Boundary Data Source (BTS/NTAD)

The Bureau of Transportation Statistics provides **official MPO boundary polygons** via a national ArcGIS Feature Service. This dataset should be the primary source for dynamically loading MPO boundaries rather than maintaining static GeoJSON files.

**API Endpoint:**
```
https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0/query
```

| Property | Value |
|----------|-------|
| **Source** | BTS National Transportation Atlas Database (NTAD) |
| **Compiled** | November 10, 2025 (from FHWA data) |
| **Scope** | National — all US MPOs (~400+) |
| **Geometry** | `esriGeometryPolygon` — full MPO boundary polygons |
| **Update Frequency** | Quarterly (as part of NTAD updates) |
| **NGDA Status** | Recognized as National Geospatial Data Asset by FGDC |
| **Cost** | Free, no authentication required |

**Key Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `MPO_ID` | String | Unique MPO identifier |
| `MPO_NAME` | String | Full MPO name (e.g., "Denver Regional Council of Governments") |
| `ACRONYM` | String | MPO abbreviation (e.g., "DRCOG") |
| `MPO_URL` | String | Official MPO website |
| `STATE` | String | Primary state (e.g., "CO") |
| `STATE_2` | String | Secondary state (for cross-state MPOs) |
| `STATE_3` | String | Tertiary state (for tri-state MPOs) |
| `POP` | Integer | Population served by the MPO |
| `DESIGNATION_DATE` | Date | Date of MPO designation |

**Query Example — Get All Colorado MPOs:**
```
?where=STATE='CO' OR STATE_2='CO' OR STATE_3='CO'
&outFields=MPO_ID,MPO_NAME,ACRONYM,STATE,POP,DESIGNATION_DATE
&outSR=4326
&f=geojson
```

**Why This Matters for Jurisdiction Expansion:**

1. **Dynamic boundary loading** — Instead of maintaining static `tprs.geojson` files per state, query the BTS API to get official MPO polygons on demand
2. **Multi-state support** — Works for any US state, eliminating the need to manually source MPO boundaries when onboarding new states
3. **Always current** — Quarterly NTAD updates ensure boundary changes (MPO expansions, mergers, redesignations) are automatically reflected
4. **Cross-state MPOs** — The `STATE_2`/`STATE_3` fields correctly capture MPOs that span state boundaries (e.g., Portland Metro spans OR/WA)
5. **Population data** — `POP` field enables population-weighted analysis without separate Census lookups
6. **Point-in-polygon ready** — The polygon geometry enables precise crash-to-MPO assignment, solving the partial-county problem (Section 13) more accurately than county-level approximations

**Integration with Existing Architecture:**

These endpoints should be added to `API_AVAILABILITY` in `index.html`:
```javascript
// Add to existing API_AVAILABILITY object:
btsMPOBoundaries:     { scope: 'national', label: 'MPO Boundaries (BTS NTAD)' },
tigerwebPlaces:       { scope: 'national', label: 'City/Place Boundaries (TIGERweb)' },
tigerwebTracts:       { scope: 'national', label: 'Census Tracts (TIGERweb)' },
tigerwebUrbanAreas:   { scope: 'national', label: 'Urban Areas (TIGERweb)' },
tigerwebSchoolDist:   { scope: 'national', label: 'School District Boundaries (TIGERweb)' },
tigerwebStates:       { scope: 'national', label: 'State Boundaries (TIGERweb)' }
// Note: tigerwebCounties and tigerwebCouSub already implicitly used via existing tigerweb config
```

And to the `config.json` boundary configuration for runtime use (see Section 8 for full layer catalog).

### Key Insight: Overlapping Hierarchies

**CDOT Regions and TPRs/MPOs are NOT the same hierarchy.** They overlap:

```
CDOT Region 1 (Denver Metro)     ←→  DRCOG MPO (mostly overlaps)
                                       + parts of NFRMPO
                                       + Intermountain TPR (Summit)

CDOT Region 2 (Pueblo/SE)        ←→  PPACG MPO (El Paso/Teller)
                                       + PACOG MPO (Pueblo)
                                       + Central Front Range TPR
                                       + Eastern TPR (partial)
                                       + Southeast TPR
                                       + South Central TPR

CDOT Region 4 (Greeley/NE)       ←→  NFRMPO (Larimer/Weld)
                                       + Eastern TPR (partial)
                                       + Upper Front Range TPR
```

**This means our data model MUST support a county belonging to multiple groupings simultaneously.**

---

## 5. User Persona Matrix

### Who Uses This Tool and What Do They Need?

| Persona | Organization Level | Data Scope | Primary Use Cases | View Type |
|---------|-------------------|------------|-------------------|-----------|
| **County Traffic Engineer** | County (e.g., Douglas County) | Single county | Hotspot analysis, CMF selection, grant applications, warrant studies | County View |
| **City Traffic Engineer** | Municipality (e.g., Castle Rock) | Single city within county | City-specific crashes, intersection analysis | City View (filtered) |
| **CDOT Region Engineer** | CDOT Region (e.g., Region 1) | 8-15 counties | Regional safety priorities, corridor analysis across counties, resource allocation | Region View |
| **CDOT Region Traffic Safety Manager** | CDOT Region | 8-15 counties | HSIP project selection, systemic safety, before/after studies | Region View + Hotspots |
| **MPO Transportation Planner** | MPO (e.g., DRCOG) | 3-10 counties | Regional transportation plan, TIP programming, safety targets | MPO View |
| **Rural TPR Coordinator** | TPR | 2-9 counties | Rural safety needs, regional transit plans | TPR View |
| **CDOT HQ Safety Analyst** | Statewide | All 64 counties | Statewide HSIP, Strategic Highway Safety Plan (SHSP), performance targets | State View by Region |
| **CDOT HQ Traffic Engineer** | Statewide | All 64 counties | Systemic safety analysis, policy development | State View by Category |
| **FHWA Division Safety Engineer** | Federal/State | One or more states | HSIP oversight, performance measure review | Multi-State View |
| **Consultant/Researcher** | Varies | Varies | Safety studies, corridor studies, crash modification factors | Custom View |

### View Hierarchy Diagram

```
                    ┌──────────────────────┐
                    │    STATEWIDE VIEW     │
                    │   (CDOT HQ / FHWA)   │
                    │  All 64 counties      │
                    └──────────┬───────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼──────┐ ┌──────▼───────┐ ┌──────▼───────┐
    │  CDOT REGION   │ │  MPO / TPR   │ │  CORRIDOR    │
    │  VIEW          │ │  VIEW        │ │  VIEW        │
    │  (5 regions)   │ │  (15 TPRs)   │ │  (e.g. I-25) │
    └────────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                │
    ┌────────▼───────────────▼────────────────▼───────┐
    │              COUNTY VIEW                         │
    │           (64 counties)                          │
    │   Douglas, El Paso, Denver, Arapahoe, etc.      │
    └────────────────────┬────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │    CITY / PLACE     │
              │    VIEW (optional)  │
              │  Castle Rock, etc.  │
              └─────────────────────┘
```

---

## 6. Hierarchical Jurisdiction Model

### The Core Data Model

The key insight is that **jurisdictions form a DAG (Directed Acyclic Graph), not a simple tree**. A county can belong to:
- Exactly 1 CDOT Engineering Region
- Exactly 1 TPR/MPO
- Exactly 1 State
- Optionally 1+ corridors

```json
{
  "hierarchyModel": {
    "levels": [
      {
        "id": "state",
        "name": "State",
        "description": "Top-level: entire state",
        "example": "Colorado",
        "parentOf": ["cdot_region", "tpr", "corridor"]
      },
      {
        "id": "cdot_region",
        "name": "CDOT Engineering Region",
        "description": "CDOT's 5 operational regions",
        "example": "Region 1 (Denver Metro)",
        "parentOf": ["county"],
        "siblingOf": ["tpr"]
      },
      {
        "id": "tpr",
        "name": "Transportation Planning Region",
        "description": "15 TPRs (5 MPO + 10 rural)",
        "example": "DRCOG, NFRMPO, Eastern TPR",
        "parentOf": ["county"],
        "siblingOf": ["cdot_region"],
        "subtypes": ["mpo", "rural_tpr"]
      },
      {
        "id": "corridor",
        "name": "Corridor",
        "description": "Major route spanning multiple counties",
        "example": "I-25 (Wyoming to New Mexico)",
        "parentOf": ["county"],
        "crossCuts": ["cdot_region", "tpr"]
      },
      {
        "id": "county",
        "name": "County",
        "description": "Primary data unit - where crash data lives",
        "example": "Douglas County",
        "parentOf": ["city"],
        "belongsTo": ["cdot_region", "tpr", "state"]
      },
      {
        "id": "city",
        "name": "City/Place",
        "description": "Municipalities within counties",
        "example": "Castle Rock, Lone Tree",
        "parentOf": [],
        "belongsTo": ["county"]
      }
    ]
  }
}
```

### Why This Matters

When a CDOT Region 1 engineer opens the tool, they should see:
1. **Dashboard** → Aggregate stats for ALL counties in Region 1
2. **Hotspots** → Top crash locations across ALL Region 1 counties, ranked together
3. **Map** → All Region 1 counties visible, with county boundaries
4. **CMF** → Select any location across any Region 1 county
5. **Grants** → HSIP-eligible locations across the entire region

This requires **dynamic data aggregation** — loading multiple county CSVs and combining them in the browser.

---

## 7. Dynamic Multi-Tier Architecture

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     CRASH LENS APPLICATION                   │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                  UI LAYER (index.html)                  │  │
│  │                                                          │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐   │  │
│  │  │ Scope       │  │ View        │  │ Comparison   │   │  │
│  │  │ Selector    │  │ Renderer    │  │ Engine       │   │  │
│  │  │ (Cascading) │  │ (Adaptive)  │  │ (Side-by-    │   │  │
│  │  │             │  │             │  │  side/overlay)│   │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘   │  │
│  │         │                │                │             │  │
│  │  ┌──────▼────────────────▼────────────────▼───────┐    │  │
│  │  │          AGGREGATION ENGINE (NEW)               │    │  │
│  │  │                                                  │    │  │
│  │  │  ┌────────────┐  ┌──────────┐  ┌────────────┐  │    │  │
│  │  │  │ County     │  │ Region   │  │ State      │  │    │  │
│  │  │  │ Aggregator │  │ Combiner │  │ Roll-up    │  │    │  │
│  │  │  └────────────┘  └──────────┘  └────────────┘  │    │  │
│  │  └──────────────────────┬─────────────────────────┘    │  │
│  │                         │                               │  │
│  │  ┌──────────────────────▼─────────────────────────┐    │  │
│  │  │            DATA LAYER                           │    │  │
│  │  │                                                  │    │  │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │    │  │
│  │  │  │ County   │  │ State    │  │ Hierarchy    │  │    │  │
│  │  │  │ CSV      │  │ Adapter  │  │ Registry     │  │    │  │
│  │  │  │ Loader   │  │ (exist.) │  │ (NEW)        │  │    │  │
│  │  │  └──────────┘  └──────────┘  └──────────────┘  │    │  │
│  │  └────────────────────────────────────────────────┘    │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                CONFIG LAYER (file system)               │  │
│  │                                                          │  │
│  │  config.json          → App settings, API keys           │  │
│  │  states/{state}/      → State-specific configs           │  │
│  │  data/{STATE_DIR}/    → State crash data CSVs            │  │
│  │  hierarchy.json       → Region/MPO/County relationships  │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### New Component: Hierarchy Registry

This is the **single source of truth** for which counties belong to which regions, MPOs, and TPRs.

```javascript
// Proposed: hierarchy_registry.js

const HierarchyRegistry = (() => {
    'use strict';

    // Loaded from hierarchy.json at startup
    let hierarchyData = null;

    return {
        /**
         * Load hierarchy config for a state
         * @param {string} stateFips - e.g., '08' for Colorado
         */
        async load(stateFips) {
            const resp = await fetch(`states/${stateFips}/hierarchy.json`);
            hierarchyData = await resp.json();
        },

        /**
         * Get all counties in a CDOT region
         * @param {string} regionId - e.g., 'region_1'
         * @returns {string[]} Array of county FIPS codes
         */
        getCountiesInRegion(regionId) { /* ... */ },

        /**
         * Get all counties in an MPO/TPR
         * @param {string} tprId - e.g., 'drcog', 'eastern_tpr'
         * @returns {string[]} Array of county FIPS codes
         */
        getCountiesInTPR(tprId) { /* ... */ },

        /**
         * Get all groupings a county belongs to
         * @param {string} countyFips - e.g., '035' for Douglas
         * @returns {Object} { cdotRegion, tpr, corridors[] }
         */
        getCountyMemberships(countyFips) { /* ... */ },

        /**
         * Get all regions for the state
         * @returns {Object[]} Array of region definitions
         */
        getAllRegions() { /* ... */ },

        /**
         * Get all TPRs/MPOs for the state
         * @returns {Object[]}
         */
        getAllTPRs() { /* ... */ },

        /**
         * Get counties that share a corridor
         * @param {string} corridorId - e.g., 'I-25'
         * @returns {string[]} Array of county FIPS codes
         */
        getCountiesOnCorridor(corridorId) { /* ... */ }
    };
})();
```

### New Component: Aggregation Engine

Handles combining data from multiple counties.

```javascript
// Proposed: aggregation_engine.js

const AggregationEngine = (() => {
    'use strict';

    // Cache of loaded county data
    const countyDataCache = new Map(); // countyFips → crashRows[]

    return {
        /**
         * Load crash data for a single county
         * Returns cached if already loaded
         */
        async loadCounty(countyFips, stateDataDir) { /* ... */ },

        /**
         * Load and combine crash data for multiple counties
         * Used when viewing a region/MPO/state
         */
        async loadMultipleCounties(countyFipsList, stateDataDir) { /* ... */ },

        /**
         * Build aggregates across multiple counties
         * Returns same structure as crashState.aggregates but combined
         */
        buildMultiCountyAggregates(combinedRows) { /* ... */ },

        /**
         * Build per-county breakdowns within a multi-county view
         * Returns { countyFips: { aggregates } } for comparison
         */
        buildPerCountyBreakdowns(combinedRows) { /* ... */ },

        /**
         * Get cross-county hotspots (rank locations across all loaded counties)
         */
        getCrossCountyHotspots(combinedRows, topN) { /* ... */ },

        /**
         * Memory management: evict least-recently-used county data
         */
        evictCache(maxCounties) { /* ... */ }
    };
})();
```

---

## 8. State-Level Configuration System

### TIGERweb API — Complete Boundary Layer Catalog

The tool already uses TIGERweb for county boundaries (layer 82) and county subdivisions (magisterial districts). TIGERweb provides **many more boundary layers** that are critical for the jurisdiction expansion — all free, national, and queryable via the same ArcGIS REST pattern we already use.

**Base URLs (already in `config.json`):**
```
Census 2020:  https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer
Current 2025: https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer
Places:       https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer
```

**Important:** TIGERweb does **NOT** have MPO boundary polygons. MPOs are federal DOT planning designations, not Census geographies. Use BTS NTAD for MPO boundaries (see Section 4).

#### HIGH VALUE — Required for Jurisdiction Expansion

| Layer Name | ID (Current) | ID (Census2020) | Geometry | Use Case in Expansion Plan |
|------------|-------------|-----------------|----------|---------------------------|
| **States** | 80 | 80 | Polygon | State outline for statewide view choropleth. Replaces static `state_outline.geojson` |
| **Counties** | 82 | 82 | Polygon | Already using. Core boundary for every county view |
| **County Subdivisions** | 22 | 20 | Polygon | Already using (`countySubdivisionsUrl`). VA magisterial districts, CO precincts |
| **Incorporated Places** | 28 | 26 | Polygon | **City/town boundaries — unlocks City/Place level in hierarchy model.** Castle Rock, Lone Tree, Parker boundaries for Douglas Co. |
| **Census Designated Places** | 30 | 28 | Polygon | Unincorporated community boundaries (e.g., Highlands Ranch, Roxborough Park). Complements Incorporated Places |
| **Census Tracts** | 8 | 6 | Polygon | Equity analysis (EJScreen, CDC SVI, Justice40). Crash rate per capita at sub-county level. Required for MPO-specific feature "Equity analysis" (Section 13) |
| **Urban Areas (2020 Corrected)** | 88 | 88 | Polygon | Official Census urban/rural classification. Required for urban vs. rural crash pattern comparisons at state level, transit safety relevance filtering, ped/bike analysis |

#### MEDIUM VALUE — Useful for Future Features

| Layer Name | ID (Current) | ID (Census2020) | Geometry | Use Case |
|------------|-------------|-----------------|----------|----------|
| **Metropolitan Statistical Areas** | 93 | 76 | Polygon | MSA boundaries (Census-defined metro areas, NOT the same as MPOs). Useful for federal statistical comparisons |
| **Combined Statistical Areas** | 97 | 72 | Polygon | Larger metro groupings (e.g., Denver-Aurora CSA). Corridor-level analysis spanning MSAs |
| **119th Congressional Districts** | 54 | 52 (116th) | Polygon | Legislative advocacy — "crashes in your district, Representative Smith" |
| **2024 State Legislative Districts (Upper)** | 56 | 54 (2018) | Polygon | State-level legislative advocacy |
| **2024 State Legislative Districts (Lower)** | 58 | 56 (2018) | Polygon | State-level legislative advocacy |
| **ZIP Code Tabulation Areas** | 2 | 84 | Polygon | Crash analysis by ZIP code (common user request) |
| **School Districts (Unified)** | 14 | 12 | Polygon | School district boundary overlay for school safety analysis. Complements LEA ID school location data |
| **School Districts (Secondary)** | 16 | 14 | Polygon | Secondary school district boundaries |
| **School Districts (Elementary)** | 18 | 16 | Polygon | Elementary school district boundaries |
| **Census Block Groups** | 10 | 8 | Polygon | Fine-grained equity analysis (EJScreen uses block groups) |

#### Query Pattern (Same as Existing County Boundary Query)

All layers use the same ArcGIS REST query pattern already implemented in `addJurisdictionBoundaryLayer()`:

```javascript
// Example: Get all Incorporated Places in Douglas County, CO
const apiUrl = `${tigerwebConfig.baseUrl}/28/query?` +
    `where=${encodeURIComponent("STATE='08' AND COUNTY='035'")}` +
    `&outFields=NAME,PLACEFP,LSAD,FUNCSTAT` +
    `&returnGeometry=true&outSR=4326&f=geojson`;

// Example: Get Colorado state outline
const apiUrl = `${tigerwebConfig.baseUrl}/80/query?` +
    `where=${encodeURIComponent("STATE='08'")}` +
    `&outFields=NAME,STATE,GEOID` +
    `&returnGeometry=true&outSR=4326&f=geojson`;

// Example: Get Census Tracts in Douglas County for equity analysis
const apiUrl = `${tigerwebConfig.baseUrl}/8/query?` +
    `where=${encodeURIComponent("STATE='08' AND COUNTY='035'")}` +
    `&outFields=NAME,TRACT,GEOID` +
    `&returnGeometry=true&outSR=4326&f=geojson`;

// Example: Get 2020 Urban Areas intersecting Colorado
const apiUrl = `${tigerwebConfig.baseUrl}/88/query?` +
    `where=1=1` +
    `&geometry=${encodeURIComponent(JSON.stringify(coloradoBbox))}` +
    `&geometryType=esriGeometryEnvelope` +
    `&spatialRel=esriSpatialRelIntersects` +
    `&outFields=NAME10,UATYP10,GEOID10` +
    `&returnGeometry=true&outSR=4326&f=geojson`;
```

#### Proposed `config.json` Layer Configuration (Enhanced)

```json
{
  "apis": {
    "tigerweb": {
      "enabled": true,
      "baseUrl": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer",
      "census2020Url": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer",
      "placesUrl": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer",
      "stateFips": "08",
      "layers": {
        "states": 80,
        "counties": 82,
        "countySubdivisions": 22,
        "incorporatedPlaces": 28,
        "censusDesignatedPlaces": 30,
        "censusTracts": 8,
        "censusBlockGroups": 10,
        "urbanAreas": 88,
        "metroStatisticalAreas": 93,
        "combinedStatisticalAreas": 97,
        "congressionalDistricts": 54,
        "stateLegislativeUpper": 56,
        "stateLegislativeLower": 58,
        "schoolDistrictsUnified": 14,
        "schoolDistrictsSecondary": 16,
        "schoolDistrictsElementary": 18,
        "zipCodeTabAreas": 2
      }
    }
  }
}
```

#### Which Layers Replace Static GeoJSON Files

| Static File | TIGERweb Replacement | Query |
|-------------|---------------------|-------|
| `state_outline.geojson` | Layer 80 (States) | `STATE='08'` |
| `counties.geojson` | Layer 82 (Counties) | `STATE='08'` → returns all 64 |
| `counties_full/{fips}.geojson` | Layer 82 (Counties) | `STATE='08' AND COUNTY='{fips}'` |
| `tpr_boundaries.geojson` | **BTS NTAD MPO API** (not TIGERweb) | `STATE='CO'` |
| `regions.geojson` | **No TIGERweb equivalent** — CDOT regions are custom | Must remain static |
| City/Place boundaries (new) | Layer 28 + 30 (Places + CDPs) | `STATE='08' AND COUNTY='{fips}'` |

> **Note:** TIGERweb replaces the need for most static GeoJSON files in the boundary directory. Only CDOT Region boundaries and corridor geometries must remain as static files since they are DOT-specific administrative boundaries that Census does not define.

### Proposed Directory Structure (Updated)

```
states/
├── state_adapter.js              # Existing: CSV format detection & normalization
├── fips_database.js              # Existing: All US states & counties
├── us_counties_db.js             # Existing: Embedded county data
├── hierarchy_registry.js         # NEW: Region/MPO/TPR relationships
├── aggregation_engine.js         # NEW: Multi-county data combining
│
├── 08/                           # Colorado (by FIPS code)
│   ├── config.json               # State config (move from data/CDOT/config.json)
│   ├── hierarchy.json            # NEW: Region/MPO/county mappings
│   ├── regions.geojson           # CDOT region boundaries (static — no Census equivalent)
│   └── corridors.json            # NEW: Major corridor definitions
│   # NOTE: County, state, place, tract boundaries all fetched live from TIGERweb API
│   # NOTE: MPO/TPR boundaries fetched live from BTS NTAD MPO API (see Section 4)
│
├── 51/                           # Virginia (by FIPS code)
│   ├── config.json               # Existing (moved from states/virginia/)
│   ├── hierarchy.json            # NEW: VDOT district/MPO mappings
│   ├── regions.geojson           # VDOT district boundaries (static — no Census equivalent)
│   └── corridors.json            # Major corridor definitions
│
└── template/                     # NEW: Template for new states
    ├── config.json.template
    ├── hierarchy.json.template
    └── README.md                 # Instructions for onboarding a new state
```

### Colorado hierarchy.json (Proposed Schema)

```json
{
  "state": {
    "fips": "08",
    "name": "Colorado",
    "abbreviation": "CO"
  },

  "regionType": {
    "label": "CDOT Engineering Region",
    "labelPlural": "CDOT Engineering Regions",
    "shortLabel": "Region",
    "description": "CDOT's 5 operational engineering regions"
  },

  "regions": {
    "region_1": {
      "id": "region_1",
      "name": "Region 1",
      "fullName": "CDOT Region 1 — Denver Metro / Central",
      "office": "2000 S. Holly St., Denver, CO 80222",
      "counties": ["001", "005", "013", "014", "019", "031", "035", "047", "059", "117"],
      "countyNames": ["Adams", "Arapahoe", "Boulder", "Broomfield", "Clear Creek", "Denver", "Douglas", "Gilpin", "Jefferson", "Summit"],
      "mapCenter": [39.65, -105.15],
      "mapZoom": 9,
      "keyCorridors": ["I-25", "I-70", "I-76", "I-225", "I-270", "US-36", "US-6", "CO-470"]
    },
    "region_2": {
      "id": "region_2",
      "name": "Region 2",
      "fullName": "CDOT Region 2 — Southeast / Pueblo",
      "office": "905 Erie Ave., Pueblo, CO 81001",
      "counties": ["003", "011", "015", "025", "027", "041", "039", "043", "055", "061", "065", "071", "073", "089", "093", "099", "101"],
      "countyNames": ["Baca", "Bent", "Chaffee", "Crowley", "Custer", "El Paso", "Elbert", "Fremont", "Huerfano", "Kiowa", "Lake", "Las Animas", "Lincoln", "Otero", "Park", "Prowers", "Pueblo", "Teller"],
      "mapCenter": [38.5, -104.7],
      "mapZoom": 8,
      "keyCorridors": ["I-25", "US-24", "US-50", "US-285", "US-160"]
    },
    "region_3": {
      "id": "region_3",
      "name": "Region 3",
      "fullName": "CDOT Region 3 — Grand Junction / Western Slope",
      "office": "222 S. 6th St., Grand Junction, CO 81501",
      "counties": ["029", "037", "045", "049", "051", "053", "057", "077", "081", "085", "091", "097", "107", "113"],
      "countyNames": ["Delta", "Eagle", "Garfield", "Grand", "Gunnison", "Hinsdale", "Jackson", "Mesa", "Moffat", "Montrose", "Ouray", "Pitkin", "Rio Blanco", "Routt", "San Miguel"],
      "mapCenter": [39.2, -107.5],
      "mapZoom": 8,
      "keyCorridors": ["I-70", "US-40", "US-50", "US-550", "CO-82"]
    },
    "region_4": {
      "id": "region_4",
      "name": "Region 4",
      "fullName": "CDOT Region 4 — Greeley / Northeast",
      "office": "1420 2nd St., Greeley, CO 80631",
      "counties": ["017", "063", "069", "075", "087", "095", "115", "121", "123"],
      "countyNames": ["Cheyenne", "Kit Carson", "Larimer", "Logan", "Morgan", "Phillips", "Sedgwick", "Washington", "Weld", "Yuma"],
      "mapCenter": [40.3, -104.5],
      "mapZoom": 8,
      "keyCorridors": ["I-25", "I-76", "US-34", "US-85", "US-287"]
    },
    "region_5": {
      "id": "region_5",
      "name": "Region 5",
      "fullName": "CDOT Region 5 — Durango / Southwest & San Luis Valley",
      "office": "3803 N. Main Ave., Durango, CO 81301",
      "counties": ["003", "007", "021", "023", "033", "067", "079", "083", "105", "109", "111"],
      "countyNames": ["Alamosa", "Archuleta", "Conejos", "Costilla", "Dolores", "La Plata", "Mineral", "Montezuma", "Rio Grande", "Saguache", "San Juan"],
      "mapCenter": [37.5, -106.5],
      "mapZoom": 8,
      "keyCorridors": ["US-160", "US-550", "US-285", "CO-17", "CO-145"]
    }
  },

  "tprType": {
    "label": "Transportation Planning Region",
    "labelPlural": "Transportation Planning Regions (TPRs)",
    "shortLabel": "TPR"
  },

  "tprs": {
    "drcog": {
      "id": "drcog",
      "name": "DRCOG",
      "fullName": "Denver Regional Council of Governments",
      "type": "mpo",
      "counties": ["001", "005", "013", "014", "019", "031", "035", "047", "059"],
      "countyNames": ["Adams", "Arapahoe", "Boulder", "Broomfield", "Clear Creek", "Denver", "Douglas", "Gilpin", "Jefferson"],
      "partialCounties": { "123": "Southwest Weld County" },
      "mapCenter": [39.74, -104.99],
      "mapZoom": 10,
      "website": "https://drcog.org"
    },
    "nfrmpo": {
      "id": "nfrmpo",
      "name": "NFRMPO",
      "fullName": "North Front Range MPO",
      "type": "mpo",
      "counties": ["069", "123"],
      "countyNames": ["Larimer", "Weld"],
      "mapCenter": [40.45, -105.0],
      "mapZoom": 10,
      "website": "https://nfrmpo.org"
    },
    "ppacg": {
      "id": "ppacg",
      "name": "PPACG",
      "fullName": "Pikes Peak Area Council of Governments",
      "type": "mpo",
      "counties": ["041", "119"],
      "countyNames": ["El Paso", "Teller"],
      "mapCenter": [38.83, -104.82],
      "mapZoom": 10,
      "website": "https://ppacg.org"
    },
    "pacog": {
      "id": "pacog",
      "name": "PACOG",
      "fullName": "Pueblo Area Council of Governments",
      "type": "mpo",
      "counties": ["101"],
      "countyNames": ["Pueblo"],
      "mapCenter": [38.25, -104.61],
      "mapZoom": 11,
      "website": "https://pacog.net"
    },
    "gvmpo": {
      "id": "gvmpo",
      "name": "GVMPO",
      "fullName": "Grand Valley MPO",
      "type": "mpo",
      "counties": ["077"],
      "countyNames": ["Mesa"],
      "mapCenter": [39.06, -108.55],
      "mapZoom": 11
    },
    "central_front_range": {
      "id": "central_front_range",
      "name": "Central Front Range TPR",
      "fullName": "Central Front Range Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["027", "043", "093"],
      "countyNames": ["Custer", "Fremont", "Park"],
      "partialCounties": { "041": "Part of El Paso", "119": "Part of Teller" },
      "adminOrg": "Upper Arkansas Area COG (UAACOG)"
    },
    "eastern": {
      "id": "eastern",
      "name": "Eastern TPR",
      "fullName": "Eastern Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["017", "039", "063", "073", "075", "095", "115", "121", "125"],
      "countyNames": ["Cheyenne", "Elbert", "Kit Carson", "Lincoln", "Logan", "Phillips", "Sedgwick", "Washington", "Yuma"],
      "adminOrg": "Yuma County"
    },
    "gunnison_valley": {
      "id": "gunnison_valley",
      "name": "Gunnison Valley TPR",
      "fullName": "Gunnison Valley Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["029", "051", "053", "085", "091", "113"],
      "countyNames": ["Delta", "Gunnison", "Hinsdale", "Montrose", "Ouray", "San Miguel"],
      "adminOrg": "Region 10 LEAP"
    },
    "intermountain": {
      "id": "intermountain",
      "name": "Intermountain TPR",
      "fullName": "Intermountain Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["037", "045", "065", "097", "117"],
      "countyNames": ["Eagle", "Garfield", "Lake", "Pitkin", "Summit"],
      "adminOrg": "NW CO COG (NWCCOG)"
    },
    "northwest": {
      "id": "northwest",
      "name": "Northwest TPR",
      "fullName": "Northwest Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["049", "057", "081", "103", "107"],
      "countyNames": ["Grand", "Jackson", "Moffat", "Rio Blanco", "Routt"],
      "adminOrg": "Town of Fraser"
    },
    "san_luis_valley": {
      "id": "san_luis_valley",
      "name": "San Luis Valley TPR",
      "fullName": "San Luis Valley Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["003", "015", "021", "023", "079", "105", "109"],
      "countyNames": ["Alamosa", "Chaffee", "Conejos", "Costilla", "Mineral", "Rio Grande", "Saguache"],
      "adminOrg": "SLV Development Resources Group"
    },
    "south_central": {
      "id": "south_central",
      "name": "South Central TPR",
      "fullName": "South Central Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["055", "071"],
      "countyNames": ["Huerfano", "Las Animas"],
      "adminOrg": "South Central COG (SCCOG)"
    },
    "southeast": {
      "id": "southeast",
      "name": "Southeast TPR",
      "fullName": "Southeast Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["009", "011", "025", "061", "089", "099"],
      "countyNames": ["Baca", "Bent", "Crowley", "Kiowa", "Otero", "Prowers"],
      "adminOrg": "SE CO Enterprise Development (SECED)"
    },
    "southwest": {
      "id": "southwest",
      "name": "Southwest TPR",
      "fullName": "Southwest Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["007", "033", "067", "083", "111"],
      "countyNames": ["Archuleta", "Dolores", "La Plata", "Montezuma", "San Juan"],
      "adminOrg": "SW CO COG (SWCCOG)"
    },
    "upper_front_range": {
      "id": "upper_front_range",
      "name": "Upper Front Range TPR",
      "fullName": "Upper Front Range Transportation Planning Region",
      "type": "rural_tpr",
      "counties": ["087"],
      "countyNames": ["Morgan"],
      "partialCounties": { "069": "Part of Larimer", "123": "Part of Weld" },
      "adminOrg": "CDOT Planning Liaison"
    }
  },

  "corridors": {
    "I-25": {
      "name": "I-25 Corridor",
      "description": "Primary north-south interstate through Colorado",
      "counties": ["071", "055", "101", "041", "035", "005", "031", "001", "069", "123"],
      "regions": ["region_5", "region_2", "region_1", "region_4"],
      "length_miles": 300
    },
    "I-70": {
      "name": "I-70 Corridor",
      "description": "Primary east-west interstate through Colorado",
      "counties": ["063", "073", "039", "005", "031", "059", "019", "117", "037", "045", "077"],
      "regions": ["region_4", "region_2", "region_1", "region_3"],
      "length_miles": 451
    },
    "I-76": {
      "name": "I-76 Corridor",
      "description": "Northeast diagonal interstate",
      "counties": ["001", "123", "087", "075", "095"],
      "regions": ["region_1", "region_4"],
      "length_miles": 187
    },
    "US-285": {
      "name": "US-285 Corridor",
      "description": "North-south route through central Colorado mountains",
      "counties": ["059", "093", "015", "109", "105"],
      "regions": ["region_1", "region_2", "region_5"],
      "length_miles": 305
    }
  }
}
```

---

## 9. Data Pipeline Expansion

### Current Pipeline (Single County)

```
CDOT Excel File
    → download_cdot_crash_data.py (extract one county)
        → scripts/process_crash_data.py (normalize, validate, split)
            → data/CDOT/douglas_all_roads.csv
            → data/CDOT/douglas_county_roads.csv
            → data/CDOT/douglas_no_interstate.csv
```

### Proposed Pipeline (Multi-County / Statewide)

```
CDOT Statewide Data Source
    │
    ▼
┌─────────────────────────────────────────┐
│  STAGE 1: DOWNLOAD                       │
│  download_state_crash_data.py            │
│                                           │
│  Options:                                 │
│  --state colorado                         │
│  --scope statewide        (all counties) │
│  --scope region --region 1 (Region 1)    │
│  --scope county --county douglas         │
│  --scope mpo --mpo drcog  (DRCOG area)  │
│  --years 2021 2022 2023 2024             │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  STAGE 2: SPLIT BY COUNTY               │
│  split_by_jurisdiction.py                │
│                                           │
│  Input: statewide CSV                     │
│  Output: Per-county CSVs                  │
│                                           │
│  data/CDOT/counties/                      │
│    ├── adams_all_roads.csv                │
│    ├── adams_county_roads.csv             │
│    ├── adams_no_interstate.csv            │
│    ├── arapahoe_all_roads.csv             │
│    ├── ...                                │
│    ├── douglas_all_roads.csv              │
│    ├── ...                                │
│    └── yuma_all_roads.csv                 │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  STAGE 3: NORMALIZE & VALIDATE           │
│  process_crash_data.py (existing)        │
│                                           │
│  For each county:                         │
│  - Auto-detect state format              │
│  - Normalize to internal format          │
│  - Validate coordinates                  │
│  - Fill missing geocodes                 │
│  - Split by road type                    │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  STAGE 4: PRE-AGGREGATE (optional)       │
│  pre_aggregate.py (NEW)                  │
│                                           │
│  Build summary JSONs for fast loading:   │
│  data/CDOT/aggregates/                    │
│    ├── region_1_summary.json             │
│    ├── region_2_summary.json             │
│    ├── drcog_summary.json                │
│    ├── statewide_summary.json            │
│    └── county_summaries.json             │
│                                           │
│  Each summary contains:                   │
│  - Total crashes by year, severity       │
│  - EPDO scores                           │
│  - Top hotspots                          │
│  - Trend data                            │
│  (So the UI can show dashboards without  │
│   loading all raw CSVs)                  │
└─────────────────────────────────────────┘
```

### Data Directory Restructure

```
data/
├── CDOT/                          # Colorado state data
│   ├── config.json                # State-specific config (existing)
│   ├── source_manifest.json       # Data source tracking (existing)
│   │
│   ├── counties/                  # Per-county CSVs (NEW structure)
│   │   ├── adams/
│   │   │   ├── all_roads.csv
│   │   │   ├── county_roads.csv
│   │   │   └── no_interstate.csv
│   │   ├── arapahoe/
│   │   │   ├── all_roads.csv
│   │   │   ├── county_roads.csv
│   │   │   └── no_interstate.csv
│   │   ├── douglas/
│   │   │   ├── all_roads.csv      # (existing files moved here)
│   │   │   ├── county_roads.csv
│   │   │   └── no_interstate.csv
│   │   └── .../                   # All 64 counties
│   │
│   ├── aggregates/                # Pre-computed summaries (NEW)
│   │   ├── statewide.json
│   │   ├── by_region/
│   │   │   ├── region_1.json
│   │   │   ├── region_2.json
│   │   │   └── ...
│   │   ├── by_tpr/
│   │   │   ├── drcog.json
│   │   │   ├── nfrmpo.json
│   │   │   └── ...
│   │   └── by_county/
│   │       ├── adams.json
│   │       ├── douglas.json
│   │       └── ...
│   │
│   └── boundaries/                # Static GeoJSON boundaries (reduced — most come from TIGERweb API)
│       ├── regions.geojson        # CDOT region boundaries (static — no Census/TIGERweb equivalent)
│       └── corridors/             # Major route geometries (static — from CDOT/OSM)
│           ├── I-25.geojson
│           └── I-70.geojson
│       # County, state, place, tract, urban area boundaries → fetched live from TIGERweb API
│       # MPO/TPR boundaries → fetched live from BTS NTAD MPO API (see Section 4)
│       # All cached in IndexedDB after first fetch
│
├── VA/                            # Virginia state data (restructured)
│   ├── counties/
│   │   ├── henrico/
│   │   │   ├── all_roads.csv
│   │   │   └── ...
│   │   └── .../
│   ├── aggregates/
│   └── boundaries/
│       └── regions.geojson        # VDOT district boundaries (static)
│
└── README.md                      # Data directory documentation
```

### GitHub Actions Workflow Expansion

```yaml
# .github/workflows/download_crash_data.yml (proposed)

name: Download Crash Data

on:
  schedule:
    - cron: '0 6 * * 1'  # Every Monday at 6 AM UTC
  workflow_dispatch:
    inputs:
      state:
        description: 'State to download'
        required: true
        default: 'colorado'
        type: choice
        options: [colorado, virginia]
      scope:
        description: 'Download scope'
        required: true
        default: 'county'
        type: choice
        options: [county, region, statewide]
      target:
        description: 'County/region name (for county/region scope)'
        required: false
        default: 'douglas'

jobs:
  download:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Download crash data
        run: |
          python download_state_crash_data.py \
            --state ${{ inputs.state }} \
            --scope ${{ inputs.scope }} \
            --target ${{ inputs.target }}

      - name: Process and split data
        run: |
          python scripts/process_crash_data.py \
            --state ${{ inputs.state }} \
            --scope ${{ inputs.scope }}

      - name: Pre-aggregate summaries
        run: |
          python scripts/pre_aggregate.py \
            --state ${{ inputs.state }}

      - name: Commit and push
        run: |
          git add data/
          git commit -m "Update ${{ inputs.state }} crash data (${{ inputs.scope }})"
          git push
```

---

## 10. UI/UX Redesign for Multi-Jurisdiction

### Scope Selector (New Primary Navigation Element)

Replace the current simple jurisdiction dropdown with a **cascading scope selector**:

```
┌─────────────────────────────────────────────────────────┐
│  CRASH LENS — Colorado Crash Analysis Tool               │
│                                                           │
│  ┌─ Scope ──────────────────────────────────────────┐   │
│  │                                                    │   │
│  │  State: [Colorado ▼]                               │   │
│  │                                                    │   │
│  │  View Level: ○ Statewide                           │   │
│  │              ● CDOT Region  [Region 1 ▼]           │   │
│  │              ○ MPO/TPR      [DRCOG ▼]              │   │
│  │              ○ County       [Douglas County ▼]     │   │
│  │              ○ Corridor     [I-25 ▼]               │   │
│  │              ○ Custom       [Select Counties...]   │   │
│  │                                                    │   │
│  │  Road Filter: [All Roads ▼]                        │   │
│  │  Date Range:  [2021-01-01] to [2025-12-31]        │   │
│  │                                                    │   │
│  └────────────────────────────────────────────────────┘   │
│                                                           │
│  [Dashboard] [Analysis] [Map] [Hotspots] [CMF] ...       │
└─────────────────────────────────────────────────────────┘
```

### Adaptive Behavior by Scope Level

| Feature | County View | Region View | State View |
|---------|------------|-------------|------------|
| Dashboard | Single county stats | Region aggregate + per-county breakdown | Statewide + per-region breakdown |
| Map | County boundary, all crashes | Region boundary, county boundaries, crashes | State map, region coloring, county boundaries |
| Hotspots | Top locations in county | Top locations across region, flagged by county | Top locations statewide, grouped by region |
| CMF | Select intersection in county | Select intersection across any county in region | Select intersection anywhere in state |
| AI Assistant | County context | Region context (which counties, cross-county patterns) | Statewide context (regional comparisons) |
| Grants | County HSIP ranking | Regional HSIP project prioritization | Statewide HSIP allocation by region |

### County-Level View (Current, Enhanced)

```
┌──────────────────────────────────────────────────────────┐
│ Douglas County — CRASH LENS                               │
│ CDOT Region 1 │ DRCOG MPO                                │
│                                                            │
│ ┌────────────────────────────────────────────────────┐    │
│ │ DASHBOARD                                          │    │
│ │                                                    │    │
│ │  Total Crashes: 12,456    Fatal (K): 42            │    │
│ │  EPDO Score: 84,221       Serious (A): 187         │    │
│ │  5-Year Trend: ↑ 3.2%     KA Rate: 1.84%          │    │
│ │                                                    │    │
│ │  [Severity Pie] [Trend Line] [Collision Types]     │    │
│ └────────────────────────────────────────────────────┘    │
│                                                            │
│ Context: ℹ️ Viewing Douglas County (1 of 10 counties in    │
│ Region 1). Switch to [Region View] to see all Region 1.   │
└──────────────────────────────────────────────────────────┘
```

### Region-Level View (New)

```
┌──────────────────────────────────────────────────────────┐
│ CDOT Region 1 (Denver Metro) — CRASH LENS                 │
│ 10 Counties │ Population: ~3.2M                           │
│                                                            │
│ ┌────────────────────────────────────────────────────┐    │
│ │ REGION DASHBOARD                                   │    │
│ │                                                    │    │
│ │  Region Totals:  85,234 crashes │ 312 fatal         │    │
│ │  EPDO Score:     621,445                            │    │
│ │                                                    │    │
│ │  ┌─ Per-County Breakdown ──────────────────────┐   │    │
│ │  │ County      │ Crashes │ Fatal │ EPDO   │ %  │   │    │
│ │  │─────────────┼─────────┼───────┼────────┼────│   │    │
│ │  │ Denver      │ 24,521  │  98   │ 182K   │ 29%│   │    │
│ │  │ Arapahoe    │ 18,432  │  67   │ 134K   │ 22%│   │    │
│ │  │ Jefferson   │ 14,876  │  52   │ 108K   │ 17%│   │    │
│ │  │ Adams       │ 12,345  │  48   │  92K   │ 14%│   │    │
│ │  │ Douglas     │  8,234  │  28   │  61K   │ 10%│   │    │
│ │  │ Boulder     │  4,567  │  12   │  34K   │  5%│   │    │
│ │  │ Others (4)  │  2,259  │   7   │  10K   │  3%│   │    │
│ │  └─────────────────────────────────────────────┘   │    │
│ │                                                    │    │
│ │  [Regional Heatmap] [County Comparison Bar Chart]  │    │
│ │  [Click any county to drill down]                  │    │
│ └────────────────────────────────────────────────────┘    │
│                                                            │
│ ┌────────────────────────────────────────────────────┐    │
│ │ CROSS-COUNTY HOTSPOTS (Top 20 across Region 1)    │    │
│ │                                                    │    │
│ │  1. I-25 & CO-470 (Douglas)     EPDO: 4,231       │    │
│ │  2. I-70 & Wadsworth (Jefferson) EPDO: 3,876       │    │
│ │  3. Colfax & Federal (Denver)   EPDO: 3,654       │    │
│ │  ... ranked across ALL 10 counties                 │    │
│ └────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

### Statewide View (New)

```
┌──────────────────────────────────────────────────────────┐
│ Colorado Statewide — CRASH LENS                           │
│ 5 CDOT Regions │ 64 Counties │ All Roads                  │
│                                                            │
│ ┌────────────────────────────────────────────────────┐    │
│ │ STATEWIDE DASHBOARD                                │    │
│ │                                                    │    │
│ │  Total: 142,567 crashes │ 698 fatal │ EPDO: 1.2M   │    │
│ │                                                    │    │
│ │  ┌─ By CDOT Region ───────────────────────────┐   │    │
│ │  │ Region   │ Crashes │ Fatal │ EPDO    │ %    │   │    │
│ │  │──────────┼─────────┼───────┼─────────┼──────│   │    │
│ │  │ Region 1 │  85,234 │  312  │ 621K    │  60% │   │    │
│ │  │ Region 2 │  28,456 │  178  │ 284K    │  20% │   │    │
│ │  │ Region 4 │  15,234 │   98  │ 156K    │  11% │   │    │
│ │  │ Region 3 │   8,432 │   67  │  89K    │   6% │   │    │
│ │  │ Region 5 │   5,211 │   43  │  52K    │   4% │   │    │
│ │  └────────────────────────────────────────────┘   │    │
│ │                                                    │    │
│ │  [CO State Map with Region Coloring]               │    │
│ │  [Click any region to drill down]                  │    │
│ │                                                    │    │
│ │  Toggle: [By Region] [By MPO/TPR] [By Corridor]  │    │
│ └────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

---

## 11. Automated Jurisdiction Onboarding

### Zero-Code Onboarding Pipeline

The goal: to add a new county (e.g., El Paso County), the admin runs ONE command and the county appears in the tool.

```
┌──────────────────────────────────────────────────────────────┐
│                  AUTOMATED ONBOARDING FLOW                    │
│                                                                │
│  STEP 1: Admin runs command                                   │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  python onboard_jurisdiction.py \                       │   │
│  │    --state colorado \                                   │   │
│  │    --county "El Paso" \                                 │   │
│  │    --fips 041                                           │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  STEP 2: Script automatically:                                │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  1. Validates county exists in FIPSDatabase             │   │
│  │  2. Downloads crash data from CDOT source               │   │
│  │  3. Filters for El Paso County only                     │   │
│  │  4. Normalizes using StateAdapter                       │   │
│  │  5. Splits into road-type CSVs                          │   │
│  │  6. Generates pre-aggregated summary JSON               │   │
│  │  7. Updates hierarchy.json with county→region mapping   │   │
│  │  8. Fetches county boundary GeoJSON from Census         │   │
│  │  9. Updates data manifest                               │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  STEP 3: County appears in UI automatically                   │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  - App reads hierarchy.json → finds new county          │   │
│  │  - Dropdown shows "El Paso County"                      │   │
│  │  - Region 2 view now includes El Paso data              │   │
│  │  - PPACG MPO view now includes El Paso data             │   │
│  │  - State totals updated automatically                   │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  NO CODE CHANGES REQUIRED.                                    │
└──────────────────────────────────────────────────────────────┘
```

### onboard_jurisdiction.py (Proposed Script)

```python
"""
Automated jurisdiction onboarding script.

Usage:
    # Onboard a single county
    python onboard_jurisdiction.py --state colorado --county "El Paso"

    # Onboard an entire CDOT region (all counties)
    python onboard_jurisdiction.py --state colorado --region 2

    # Onboard all counties in a state
    python onboard_jurisdiction.py --state colorado --all

    # Onboard a state that doesn't exist yet
    python onboard_jurisdiction.py --state texas --setup
"""

# Key functions (pseudocode):
#
# def onboard_county(state, county_name_or_fips):
#     1. Look up county in FIPS database
#     2. Check if data already exists
#     3. Download crash data (state-specific downloader)
#     4. Process through pipeline
#     5. Update hierarchy.json
#     6. Fetch boundary GeoJSON
#     7. Generate summary statistics
#     8. Update manifest
#
# def onboard_region(state, region_id):
#     1. Look up all counties in region from hierarchy.json
#     2. For each county: onboard_county()
#     3. Generate region-level aggregate
#
# def setup_new_state(state_name):
#     1. Create state directory structure
#     2. Generate config.json template
#     3. Generate hierarchy.json template
#     4. Populate FIPS data from database
#     5. Prompt user for data source configuration
```

### Dynamic County Discovery

For states where we don't have a pre-built hierarchy, the tool can **auto-discover** counties from the crash data itself:

```javascript
// Auto-discovery from CSV data
function discoverJurisdictions(crashRows) {
    const counties = new Map();

    for (const row of crashRows) {
        const countyName = row['Physical Juris Name'] || row['County'] || '';
        if (countyName && !counties.has(countyName)) {
            // Calculate centroid from crash coordinates
            const crashes = crashRows.filter(r =>
                (r['Physical Juris Name'] || r['County']) === countyName
            );
            const avgLat = crashes.reduce((s, r) => s + (parseFloat(r.y) || 0), 0) / crashes.length;
            const avgLon = crashes.reduce((s, r) => s + (parseFloat(r.x) || 0), 0) / crashes.length;

            counties.set(countyName, {
                name: countyName,
                crashCount: crashes.length,
                centroid: [avgLat, avgLon],
                autoDiscovered: true
            });
        }
    }

    return counties;
}
```

---

## 12. CDOT District Office Implementation

### Configuration for CDOT Region-Based View

Each CDOT region gets a dedicated configuration section that defines:

1. **Geographic scope** — Which counties are included
2. **Key corridors** — Which routes are priorities for the region
3. **Office info** — For display in UI headers
4. **Performance targets** — Region-specific safety goals (if different from state)
5. **Data sources** — Any region-specific supplemental data

### Region View Features (Specific to CDOT)

#### A. Regional Dashboard
- Aggregate crash statistics for all counties in the region
- Per-county bar chart comparison
- Year-over-year trend by county
- Regional safety performance vs. targets

#### B. Regional Map
- Region boundary outline
- County boundaries within region
- Color-coded counties by crash rate or EPDO score
- Click county to drill down
- Corridor highlighting (e.g., highlight all of I-25 through Region 1)

#### C. Regional Hotspots
- Cross-county hotspot ranking
- "This intersection in Douglas County is the #3 worst in all of Region 1"
- Filter by route (all I-25 hotspots across region)
- Filter by crash type (all pedestrian hotspots across region)

#### D. Regional HSIP Programming
- Project ranking across all counties
- Cost-benefit analysis at region level
- Budget allocation recommendations by county
- Systemic safety analysis (e.g., all curves across Region 1)

#### E. Region-Level AI Assistant
- "Compare Douglas County vs. Arapahoe County pedestrian safety"
- "What are the top 5 systemic issues across Region 1?"
- "Which county in my region has the worst trend for KA crashes?"

---

## 13. MPO/Regional Aggregation Engine

### How MPO/TPR Views Differ from CDOT Region Views

| Aspect | CDOT Region View | MPO/TPR View |
|--------|-----------------|--------------|
| Purpose | Operations & maintenance | Long-range planning |
| Focus | Safety, mobility, infrastructure | Land use, transit, multimodal |
| Funding | HSIP, state safety funds | STP, CMAQ, TAP, federal formula |
| Key Output | Project lists, performance measures | TIP, Regional Transportation Plan |
| Typical User | CDOT engineers | MPO planners, local agencies |
| Special Features | Corridor analysis, winter operations | Mode share, VMT, transit integration |

### MPO-Specific Features

#### A. TIP (Transportation Improvement Program) Integration
- Map HSIP-eligible locations to TIP funding categories
- Track which projects are already in the TIP
- Identify gaps (high-crash locations NOT in TIP)

#### B. Safety Performance Targets
- Show progress toward federally-required safety targets
- Compare to state targets
- Per-county contribution to MPO targets

#### C. Multi-Modal Analysis
- Pedestrian crash density maps (critical for urban MPOs like DRCOG)
- Bicycle crash hotspots overlaid on bike network
- Transit stop safety analysis

#### D. Planning-Level Analysis
- Crash rates by functional class (for system-level planning)
- Growth area safety projections
- Equity analysis (crashes in disadvantaged communities)

### MPO Boundary Data Source for Aggregation

The BTS NTAD MPO Boundary API (documented in Section 4) is the recommended data source for the aggregation engine's boundary needs:

```
https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0/query
```

**How the Aggregation Engine Uses MPO Boundaries:**

1. **Crash-to-MPO assignment** — Use `turf.booleanPointInPolygon()` with the official BTS polygon to determine which MPO a crash belongs to. This is more accurate than county-level approximation for split counties like Weld.
2. **Boundary rendering** — Display the official MPO polygon on the map when in MPO view mode, replacing the need for manually-created `tprs.geojson`.
3. **Population normalization** — Use the `POP` field from the API to calculate crash rates per capita at the MPO level without a separate Census lookup.
4. **Auto-discovery** — When onboarding a new state, query `?where=STATE='XX'&outFields=*&f=geojson` to automatically discover all MPOs in that state along with their boundaries, names, and designations.

**Caching Strategy:**
- Query once per state and cache in IndexedDB (MPO boundaries rarely change)
- Invalidate cache quarterly (aligned with BTS NTAD update schedule)
- Fallback to static `tprs.geojson` if API is unavailable

### Partial County Handling

Some TPRs include only PART of a county (e.g., "Southwest Weld County" is in DRCOG, while most of Weld is in NFRMPO). The BTS MPO boundary polygons solve this more accurately than county-level approximation:

```javascript
// For partial counties, use BTS MPO polygon for precise assignment
async function filterPartialCounty(countyFips, mpoBoundaryPolygon, crashRows) {
    // mpoBoundaryPolygon sourced from BTS NTAD MPO Feature Service
    // Use turf.js point-in-polygon to determine which crashes
    // within Weld County fall inside the DRCOG boundary
    return crashRows.filter(row => {
        const point = turf.point([parseFloat(row.x), parseFloat(row.y)]);
        return turf.booleanPointInPolygon(point, mpoBoundaryPolygon);
    });
}
```

Alternatively, define partial counties by city or census tract boundaries for simpler filtering.

---

## 14. Statewide View (CDOT HQ)

### What CDOT Headquarters Needs

CDOT HQ users need a **statewide safety picture** for:
1. Strategic Highway Safety Plan (SHSP) development
2. HSIP funding allocation across regions
3. Performance measure reporting to FHWA
4. Systemic safety initiative identification
5. Legislative reporting and advocacy

### Statewide Dashboard Design

```
┌──────────────────────────────────────────────────────────────┐
│ COLORADO STATEWIDE — CRASH LENS                               │
│ 5 CDOT Regions │ 15 TPRs │ 64 Counties                       │
│                                                                │
│ ┌─ KEY METRICS ─────────────────────────────────────────┐     │
│ │  142,567 Total Crashes  │  698 Fatalities  │  1,842 KA │     │
│ │  EPDO: 1,247,832        │  5-Year Trend: ↑ 2.1%       │     │
│ └────────────────────────────────────────────────────────┘     │
│                                                                │
│ ┌─ STATE MAP ────────────────────────────────────────────┐    │
│ │                                                          │    │
│ │  [Choropleth map of Colorado]                            │    │
│ │  Color by: [Crash Rate ▼] [EPDO ▼] [KA Count ▼]        │    │
│ │  Group by: [CDOT Region ▼] [County ▼] [TPR ▼]          │    │
│ │                                                          │    │
│ │  Region 3        Region 4                                │    │
│ │  (Western)       (Northeast)                             │    │
│ │    ████             ████                                 │    │
│ │   ██████    Region 1 ███                                 │    │
│ │    ████    (Denver) ██                                   │    │
│ │             █████████                                    │    │
│ │  Region 5    Region 2                                    │    │
│ │  (Southwest) (Southeast)                                 │    │
│ │    ████       ████████                                   │    │
│ │                                                          │    │
│ └──────────────────────────────────────────────────────────┘    │
│                                                                │
│ ┌─ REGION COMPARISON ────────────────────────────────────┐    │
│ │  [Stacked bar: crashes by region by severity]           │    │
│ │  [Radar chart: region performance across 6 metrics]     │    │
│ │  [Sparklines: 5-year trends per region]                 │    │
│ └────────────────────────────────────────────────────────┘    │
│                                                                │
│ ┌─ EMPHASIS AREAS (SHSP) ────────────────────────────────┐    │
│ │  Impaired Driving:  18,432 crashes (13%)  → Region 1   │    │
│ │  Speed:             14,567 crashes (10%)  → Region 2   │    │
│ │  Pedestrians:        3,211 crashes (2%)   → Region 1   │    │
│ │  Intersections:     42,345 crashes (30%)  → Statewide  │    │
│ │  Young Drivers:     21,876 crashes (15%)  → Region 4   │    │
│ │  Lane Departure:    28,765 crashes (20%)  → Region 3   │    │
│ └────────────────────────────────────────────────────────┘    │
│                                                                │
│ ┌─ SYSTEMIC SAFETY ─────────────────────────────────────┐    │
│ │  [Top 50 statewide hotspots across all regions]        │    │
│ │  [Systemic issues: curves, intersections, etc.]        │    │
│ │  [Click any to drill down to county-level detail]      │    │
│ └────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

### Performance Loading Strategy for Statewide

Loading 64 county CSVs simultaneously would be too slow. Strategy:

1. **Pre-aggregated summaries** (JSON) load instantly for dashboard/comparisons
2. **On-demand county loading** when user drills down
3. **Region-level CSVs** as intermediate — one CSV per region (pre-combined)
4. **WebWorker processing** for large datasets (keep UI responsive)
5. **IndexedDB caching** — once a county's data is loaded, cache in browser

```javascript
// Loading strategy pseudocode
async function loadStateView() {
    // FAST: Load pre-aggregated state summary (~50KB JSON)
    const stateSummary = await fetch('data/CDOT/aggregates/statewide.json');
    renderStateDashboard(stateSummary);

    // FAST: Load region summaries (~10KB each, 5 regions = 50KB)
    const regionSummaries = await Promise.all(
        ['region_1', 'region_2', 'region_3', 'region_4', 'region_5']
            .map(r => fetch(`data/CDOT/aggregates/by_region/${r}.json`))
    );
    renderRegionComparison(regionSummaries);

    // LAZY: Only load raw CSV when user drills into a specific county
    // Use IndexedDB to cache after first load
}
```

---

## 15. Multi-State Scaling Strategy

### How to Expand Beyond Colorado

The architecture described above is **state-agnostic**. Here's how to add a new state:

```
┌─────────────────────────────────────────────────────────────┐
│              ADDING A NEW STATE (e.g., Texas)                │
│                                                               │
│  STEP 1: Create state config                                 │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  states/48/config.json     → Column mappings            │ │
│  │  states/48/hierarchy.json  → TxDOT districts, MPOs,     │ │
│  │                              counties                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  STEP 2: Add StateAdapter signature (if CSV format differs)  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  In state_adapter.js, add:                              │ │
│  │  texas: {                                               │ │
│  │    requiredColumns: ['Crash ID', 'Crash Severity', ...],│ │
│  │    displayName: 'Texas (CRIS)',                         │ │
│  │    configPath: 'states/48/config.json'                  │ │
│  │  }                                                      │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  STEP 3: Create data downloader                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  download_txdot_crash_data.py                           │ │
│  │  (or add Texas to generic download_state_crash_data.py) │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  STEP 4: Run onboarding                                      │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  python onboard_jurisdiction.py --state texas --setup    │ │
│  │  python onboard_jurisdiction.py --state texas --all      │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ZERO changes to app/index.html needed!                      │
│  The Hierarchy Registry and Aggregation Engine handle it.    │
└─────────────────────────────────────────────────────────────┘
```

> **Note:** MPO boundaries for any new state are automatically available from the BTS NTAD MPO API (see Section 4). Query `?where=STATE='TX'&f=geojson` to get all Texas MPOs with official boundary polygons, names, populations, and designation dates. No manual GeoJSON creation required for the MPO/TPR layer.

### Generic State Hierarchy Template

Every state DOT has a similar organizational pattern, but with different names:

| Concept | Colorado | Virginia | Texas | California |
|---------|----------|----------|-------|------------|
| State DOT | CDOT | VDOT | TxDOT | Caltrans |
| District/Region | 5 Engineering Regions | 9 Construction Districts | 25 Districts | 12 Districts |
| MPO equivalent | 5 MPOs + 10 TPRs | 14 MPOs + PDCs | 25 MPOs | 18 MPOs + RTPAs |
| County equivalent | 64 Counties | 95 Counties + 38 Cities | 254 Counties | 58 Counties |
| Sub-county | Cities/Places | Cities/CDPs | Cities/Places | Cities/Places |

The `hierarchy.json` schema handles all of these:

```json
{
  "state": { "fips": "XX", "name": "..." },
  "regionType": {
    "label": "District",
    "labelPlural": "Districts"
  },
  "regions": { },
  "tprType": {
    "label": "MPO",
    "labelPlural": "MPOs"
  },
  "tprs": { },
  "corridors": { }
}
```

### Zero-Code Dynamic Boundary System

When a user (or admin) switches to a new state, the tool should **automatically discover and load ALL boundaries** from federal APIs — no coding, no manual GeoJSON creation, no state-specific boundary logic.

**The only input needed:** A 2-digit state FIPS code (e.g., `"48"` for Texas).

Everything else is derived dynamically from TIGERweb + BTS NTAD:

```
User selects new state: Texas (FIPS: 48)
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│  AUTOMATIC BOUNDARY DISCOVERY ENGINE                     │
│                                                           │
│  Step 1: State Outline                                   │
│  TIGERweb layer 80 → STATE='48'                          │
│  → Returns Texas state polygon                           │
│                                                           │
│  Step 2: All Counties                                    │
│  TIGERweb layer 82 → STATE='48'                          │
│  → Returns all 254 Texas county polygons with            │
│    NAME, COUNTY (FIPS), GEOID                            │
│  → Auto-populates county dropdown                        │
│                                                           │
│  Step 3: All MPOs                                        │
│  BTS NTAD MPO API → STATE='TX' OR STATE_2='TX'           │
│  → Returns all 25 Texas MPO polygons with                │
│    MPO_NAME, ACRONYM, POP, DESIGNATION_DATE              │
│  → Auto-populates MPO/TPR dropdown                       │
│                                                           │
│  Step 4: Places (per county, on demand)                  │
│  TIGERweb layer 28+30 → STATE='48' AND COUNTY='{fips}'  │
│  → Returns all cities/CDPs within selected county        │
│  → Auto-populates city/place dropdown                    │
│                                                           │
│  Step 5: Urban Areas (on demand)                         │
│  TIGERweb layer 88 → spatial query within state bbox     │
│  → Returns all urban area polygons intersecting state    │
│                                                           │
│  Step 6: Census Tracts (per county, on demand)           │
│  TIGERweb layer 8 → STATE='48' AND COUNTY='{fips}'      │
│  → Returns all tracts for equity analysis overlay        │
│                                                           │
│  Step 7: School Districts (on demand)                    │
│  TIGERweb layer 14 → STATE='48'                          │
│  → Returns all school district boundaries in state       │
│                                                           │
│  All results cached in IndexedDB for instant repeat use  │
└─────────────────────────────────────────────────────────┘
```

**What CANNOT be auto-discovered (requires `hierarchy.json`):**

| Item | Why Manual | Effort |
|------|----------|--------|
| State DOT region/district boundaries | Custom administrative boundaries — no federal API | One static GeoJSON file per state |
| Region-to-county mapping | DOT-specific organizational grouping | Part of `hierarchy.json` config |
| Corridor definitions | Route groupings are state-specific | Part of `hierarchy.json` config |
| Crash data column mapping | Each state's CSV has different column names | Part of `state_adapter.js` (already exists) |

**What IS fully automatic (zero config):**

| Boundary | Source | Auto-Discovery Query |
|----------|--------|---------------------|
| State outline | TIGERweb layer 80 | `STATE='{fips}'` |
| All counties (with names, FIPS) | TIGERweb layer 82 | `STATE='{fips}'` |
| All MPOs (with names, populations, boundaries) | BTS NTAD MPO API | `STATE='{abbrev}'` |
| Cities/Places per county | TIGERweb layer 28 + 30 | `STATE='{fips}' AND COUNTY='{countyFips}'` |
| Census Tracts per county | TIGERweb layer 8 | `STATE='{fips}' AND COUNTY='{countyFips}'` |
| Urban Areas | TIGERweb layer 88 | Spatial query within state bbox |
| School Districts | TIGERweb layer 14/16/18 | `STATE='{fips}'` |
| Transit Stops | BTS NTAD Transit Stops API | Spatial query within county/region bbox |

**Implementation: `BoundaryService` Module**

```javascript
// Proposed: boundary_service.js
const BoundaryService = (() => {
    'use strict';

    const TIGERWEB_BASE = 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer';
    const BTS_MPO_BASE = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0';
    const BTS_TRANSIT_BASE = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NTAD_National_Transit_Map_Stops/FeatureServer/0';

    const LAYERS = {
        states: 80, counties: 82, countySubdivisions: 22,
        incorporatedPlaces: 28, censusDesignatedPlaces: 30,
        censusTracts: 8, censusBlockGroups: 10, urbanAreas: 88,
        schoolDistrictsUnified: 14, schoolDistrictsSecondary: 16,
        schoolDistrictsElementary: 18, congressionalDistricts: 54,
        stateLegislativeUpper: 56, stateLegislativeLower: 58,
        metroStatisticalAreas: 93, zipCodeTabAreas: 2
    };

    const cache = {}; // IndexedDB-backed cache

    async function queryTigerWeb(layerId, where, outFields = '*') {
        const cacheKey = `tw_${layerId}_${where}`;
        if (cache[cacheKey]) return cache[cacheKey];

        const url = `${TIGERWEB_BASE}/${layerId}/query?` +
            `where=${encodeURIComponent(where)}` +
            `&outFields=${outFields}&returnGeometry=true&outSR=4326&f=geojson`;
        const resp = await fetch(url);
        const data = await resp.json();
        cache[cacheKey] = data;
        // Also persist to IndexedDB for offline/repeat use
        return data;
    }

    async function queryBtsMpo(where) {
        const cacheKey = `mpo_${where}`;
        if (cache[cacheKey]) return cache[cacheKey];

        const url = `${BTS_MPO_BASE}/query?` +
            `where=${encodeURIComponent(where)}` +
            `&outFields=*&outSR=4326&f=geojson`;
        const resp = await fetch(url);
        const data = await resp.json();
        cache[cacheKey] = data;
        return data;
    }

    return {
        /**
         * Auto-discover all boundaries for a state. Called once on state selection.
         * @param {string} stateFips - 2-digit FIPS (e.g., '48')
         * @param {string} stateAbbrev - 2-letter abbreviation (e.g., 'TX')
         * @returns {Object} { stateOutline, counties, mpos }
         */
        async discoverState(stateFips, stateAbbrev) {
            const [stateOutline, counties, mpos] = await Promise.all([
                queryTigerWeb(LAYERS.states, `STATE='${stateFips}'`, 'NAME,STATE,GEOID'),
                queryTigerWeb(LAYERS.counties, `STATE='${stateFips}'`, 'NAME,COUNTY,STATE,GEOID'),
                queryBtsMpo(`STATE='${stateAbbrev}' OR STATE_2='${stateAbbrev}' OR STATE_3='${stateAbbrev}'`)
            ]);
            return { stateOutline, counties, mpos };
        },

        /**
         * Load places (cities + CDPs) for a specific county. Called on county drill-down.
         */
        async getPlaces(stateFips, countyFips) {
            const [places, cdps] = await Promise.all([
                queryTigerWeb(LAYERS.incorporatedPlaces, `STATE='${stateFips}' AND COUNTY='${countyFips}'`, 'NAME,PLACEFP,LSAD'),
                queryTigerWeb(LAYERS.censusDesignatedPlaces, `STATE='${stateFips}' AND COUNTY='${countyFips}'`, 'NAME,PLACEFP,LSAD')
            ]);
            return { places, cdps };
        },

        /**
         * Load census tracts for equity analysis overlay.
         */
        async getCensusTracts(stateFips, countyFips) {
            return queryTigerWeb(LAYERS.censusTracts, `STATE='${stateFips}' AND COUNTY='${countyFips}'`, 'NAME,TRACT,GEOID');
        },

        /**
         * Load urban area boundaries for urban/rural classification.
         */
        async getUrbanAreas(stateFips) {
            return queryTigerWeb(LAYERS.urbanAreas, `STATE='${stateFips}'`, 'NAME10,UATYP10,GEOID10');
        },

        /**
         * Load school district boundaries.
         */
        async getSchoolDistricts(stateFips) {
            return queryTigerWeb(LAYERS.schoolDistrictsUnified, `STATE='${stateFips}'`, 'NAME,SDLEA,GEOID');
        },

        /** Expose layer IDs for custom queries */
        LAYERS
    };
})();
```

**Key Design Principle:** The `BoundaryService` is completely state-agnostic. It takes a FIPS code and returns boundaries. No Colorado-specific or Virginia-specific logic. When someone wants to add Texas, they:

1. Set `stateFips: '48'` in the state config
2. `BoundaryService.discoverState('48', 'TX')` auto-fetches state outline, all 254 counties, and all 25 MPOs
3. Selecting any county auto-fetches its cities/places via `getPlaces('48', countyFips)`
4. Transit stops auto-load from BTS API using county bounding box (existing `transitLoadStops()` is already state-agnostic)

**The only manual work per new state:**
- Create `hierarchy.json` with DOT region→county mapping (this IS state-specific)
- Create `regions.geojson` with DOT region boundaries (no federal API for these)
- Add column mapping to `state_adapter.js` (crash CSV format varies by state)

Everything else — county names, county boundaries, city boundaries, MPO boundaries, census tracts, urban areas, school districts, transit stops — comes from federal APIs automatically.

### Cross-State Comparison Mode (Future)

For FHWA or multi-state organizations:

```
┌──────────────────────────────────────────────────────────────┐
│  CROSS-STATE COMPARISON                                       │
│                                                                │
│  ┌─ Select States ─────────────────────────────────────────┐  │
│  │  [✓] Colorado    [✓] Virginia    [ ] Texas              │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌─ Side-by-Side ──────────────────────────────────────────┐  │
│  │  Metric          │ Colorado    │ Virginia               │  │
│  │──────────────────┼─────────────┼────────────────────────│  │
│  │  Total Crashes   │ 142,567     │ 128,345                │  │
│  │  Fatal Crashes   │ 698         │ 845                    │  │
│  │  KA Rate         │ 1.29%       │ 1.52%                  │  │
│  │  Ped Crashes     │ 3,211       │ 2,876                  │  │
│  │  Impaired        │ 18,432      │ 15,234                 │  │
│  └─────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## 16. Spatial Accuracy: Bbox-Fetch + Polygon-Clip Strategy

### The Problem

All external spatial API queries in CRASH LENS use **bounding box (bbox) envelopes** — rectangular queries that fetch data within a geographic rectangle. However, actual jurisdiction boundaries are **irregular polygons** (counties have winding borders along rivers, mountain ridges, historical survey lines). The bbox rectangle always includes area **outside** the actual jurisdiction, meaning API results contain data from neighboring jurisdictions.

**Impact of bbox overshoot by jurisdiction shape:**

| Jurisdiction Shape | Bbox Overshoot | Example |
|---|---|---|
| Compact/square county | ~15-25% | Arapahoe County, CO |
| Elongated county | ~40-60% | Augusta County, VA; Grand County, CO |
| Independent city (irregular) | ~50-70% | Richmond City, VA; Denver City/County, CO |
| Coastal/river boundary | ~30-50% | Any county bounded by rivers |

A county with 40% bbox overshoot shows **40% false data** — transit stops, traffic signals, road segments, and Mapillary features from neighboring jurisdictions that do not belong to the selected jurisdiction.

### Current API Calls Using Bbox

**14 distinct bbox-based API call patterns** across 3 API families:

#### ArcGIS Feature Services (`esriGeometryEnvelope`)

| API | Purpose | Current Code |
|-----|---------|--------------|
| BTS National Transit Map | Transit stops | `transitTryStatewideData()` — `esriGeometryEnvelope` spatial query |
| BTS Transit fallback | Transit stops (spatial) | Fallback spatial query — `esriGeometryEnvelope` |

#### Overpass/OSM API (`bbox` parameter)

| API | Purpose | Current Code |
|-----|---------|--------------|
| Road network | Road centerlines | Overpass `way["highway"]({bbox})` |
| Traffic signals | Signal locations | Overpass `node["highway"="traffic_signals"]({bbox})` |
| Speed limits | Speed data | Overpass `way["maxspeed"]({bbox})` |
| Bike/ped facilities | Multimodal infrastructure | Overpass `way["cycleway"]({bbox})` etc. |
| Road Inventory | Full road inventory | Overpass with chunked bbox |
| Segment Analysis | Road segments for analysis | Overpass bbox query |

#### Mapillary Graph API (`bbox` parameter)

| API | Purpose | Current Code |
|-----|---------|--------------|
| Traffic signs | Sign detection | `mapillary/map_features?bbox=` |
| Map features | Road features | `mapillary/map_features?bbox=` |
| Location assets | Intersection assets | `mapillary/map_features?bbox=` |
| Custom selection assets | User-selected area | `mapillary/map_features?bbox=` |
| Street signs | Street signs | `mapillary/map_features?bbox=` |

#### Config-based bbox matching

| API | Purpose | Current Code |
|-----|---------|--------------|
| Jurisdiction crash counting | Multi-jurisdiction counts | `config.bbox` rectangle matching |

### The Solution: Two-Step Bbox-Fetch + Polygon-Clip

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPATIAL ACCURACY PIPELINE                      │
│                                                                   │
│  STEP 1: Bbox Fetch (Server-Side — FAST)                         │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │  API Request: esriGeometryEnvelope / OSM bbox / Mapillary│     │
│  │  Returns: All features in RECTANGULAR area               │     │
│  │  Speed: Hits server's spatial index — milliseconds        │     │
│  │  Result: 100% recall, but ~15-60% false positives         │     │
│  └─────────────────────────────────────────────────────────┘     │
│                          │                                        │
│                          ▼                                        │
│  STEP 2: Polygon Clip (Client-Side — PRECISE)                    │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │  Jurisdiction polygon: TIGERweb county boundary (cached)  │     │
│  │  Filter: pointInFeature(lng, lat, jurisdictionPolygon)    │     │
│  │  For lines: turf.booleanIntersects(line, polygon)         │     │
│  │  For areas: turf.intersect(area, polygon)                 │     │
│  │  Result: 100% recall, 100% precision                      │     │
│  └─────────────────────────────────────────────────────────┘     │
│                          │                                        │
│                          ▼                                        │
│  DISPLAY: Only features within actual jurisdiction boundary       │
└─────────────────────────────────────────────────────────────────┘
```

### Existing Infrastructure (Already Built)

The codebase already has **all components needed** for this pipeline:

| Component | Location | Status |
|-----------|----------|--------|
| **turf.js library** | Line 52: `<script src="turf.min.js">` | Already loaded |
| **`pointInFeature(lng, lat, feature)`** | Line 113584 | Already works for Polygon + MultiPolygon |
| **`pointInPolygon(point, ring)`** | Line 113505 | Ray-casting algorithm, proven |
| **`computeFeatureBoundingBox(feature)`** | Line 113541 | Pre-computes bbox for fast rejection |
| **`pointInBoundingBox(lng, lat, bbox)`** | Line 113574 | Fast rectangular pre-filter |
| **TIGERweb polygon cache** | `builtInLayersState.jurisdictionBoundary.geojsonCache[id]` | Cached after first fetch |
| **District crash assignment** | Line 113617: `computeDistrictCrashStatistics()` | Already uses this exact pipeline on 100K+ rows |

### Three-Tier Clipping Strategy

Different geometry types require different clipping methods:

#### Tier 1: Point Data (transit stops, signals, crossings, Mapillary features)

```javascript
// After bbox API fetch returns pointFeatures[]
const jurisdictionPolygon = builtInLayersState.jurisdictionBoundary
    .geojsonCache[currentJurisdictionId]?.features?.[0];

const clippedPoints = pointFeatures.filter(f => {
    const [lng, lat] = f.geometry.coordinates || [f.geometry.x, f.geometry.y];
    return pointInFeature(lng, lat, jurisdictionPolygon);
});
// clippedPoints contains ONLY points inside the actual jurisdiction boundary
```

**Performance:** Sub-millisecond for typical datasets (<5,000 points). The bbox pre-filter inside `pointInFeature()` rejects 30-50% of candidates before the expensive ray-casting test.

#### Tier 2: Line Data (road segments, speed limit ways, bike/ped facilities)

```javascript
// After bbox API fetch returns lineFeatures[]
const jurisdictionTurf = turf.feature(jurisdictionPolygon.geometry);

const clippedLines = lineFeatures.filter(f => {
    return turf.booleanIntersects(turf.feature(f.geometry), jurisdictionTurf);
});
// For partial overlap, optionally clip to boundary:
// const clippedLine = turf.lineSplit(lineFeature, jurisdictionTurf);
```

**Performance:** 10-50ms for typical road networks (1,000-5,000 segments). `booleanIntersects` is fast; `lineSplit` is slower but only needed for display precision.

#### Tier 3: Polygon Data (urban areas, school districts, census tracts)

```javascript
// After bbox API fetch returns polygonFeatures[]
const jurisdictionTurf = turf.feature(jurisdictionPolygon.geometry);

const clippedPolygons = polygonFeatures.map(f => {
    const intersection = turf.intersect(
        turf.featureCollection([turf.feature(f.geometry), jurisdictionTurf])
    );
    return intersection ? { ...f, geometry: intersection.geometry } : null;
}).filter(Boolean);
// clippedPolygons shows ONLY the portions within the jurisdiction boundary
```

**Performance:** 50-200ms for typical polygon datasets (10-50 features). `turf.intersect` is the most expensive operation but polygon datasets are small.

### Why NOT Server-Side Polygon Query

ArcGIS REST API supports `esriGeometryPolygon` as an alternative to `esriGeometryEnvelope`, which would let the server do the clipping. However:

| Factor | Server-Side Polygon | Client-Side Bbox+Clip |
|--------|---------------------|----------------------|
| **Request size** | 5,000-20,000 coordinate pairs per county polygon | 4 numbers (bbox) |
| **Server processing** | Slower — complex polygon intersection | Fast — spatial index envelope lookup |
| **Network payload** | Large POST body | Small GET URL |
| **Caching** | Cannot cache (unique polygon per request) | Bbox queries are highly cacheable |
| **OSM/Mapillary support** | NOT supported (bbox only) | Works for ALL APIs |
| **Recommendation** | Not recommended | **Recommended** |

The client-side approach works **universally** across all three API families (ArcGIS, Overpass, Mapillary), while server-side polygon queries only work for ArcGIS.

### Implementation Priority

| Priority | API Target | Data Type | Clip Method | Impact |
|----------|-----------|-----------|-------------|--------|
| **P0 (Critical)** | BTS Transit Stops | Points | `pointInFeature()` | Transit tab shows wrong jurisdiction stops |
| **P0 (Critical)** | Road Inventory (OSM) | Lines | `turf.booleanIntersects()` | Road stats include neighbor roads |
| **P1 (High)** | Traffic Signals (OSM) | Points | `pointInFeature()` | Signal counts inflated |
| **P1 (High)** | Mapillary Traffic Signs | Points | `pointInFeature()` | Sign inventory includes neighbors |
| **P2 (Medium)** | Bike/Ped Facilities (OSM) | Lines | `turf.booleanIntersects()` | Facility counts inflated |
| **P2 (Medium)** | Speed Limit Ways (OSM) | Lines | `turf.booleanIntersects()` | Speed data includes neighbor roads |
| **P3 (Enhancement)** | Jurisdiction Crash Counting | Points | `pointInFeature()` | Multi-jurisdiction selector accuracy |

### Proposed `SpatialClipService` Module

```javascript
/**
 * SpatialClipService — Clips API results to actual jurisdiction polygon
 * Uses bbox pre-filter for fast rejection + precise polygon test
 *
 * Depends on:
 *   - turf.js (already loaded)
 *   - pointInFeature() (already exists)
 *   - builtInLayersState.jurisdictionBoundary.geojsonCache (TIGERweb polygon)
 */
const SpatialClipService = (() => {

    /**
     * Get the cached jurisdiction polygon (from TIGERweb)
     * Must be called AFTER addJurisdictionBoundaryLayer() has completed
     */
    function getJurisdictionPolygon(jurisdictionId) {
        const geojson = builtInLayersState?.jurisdictionBoundary
            ?.geojsonCache?.[jurisdictionId];
        if (!geojson?.features?.[0]) return null;
        return geojson.features[0]; // GeoJSON Feature with Polygon/MultiPolygon geometry
    }

    /**
     * Clip point features to jurisdiction boundary
     * @param {Array} features - Array of GeoJSON/ArcGIS features with point geometry
     * @param {string} jurisdictionId - Jurisdiction ID for polygon lookup
     * @returns {Array} Features inside the jurisdiction polygon only
     */
    function clipPoints(features, jurisdictionId) {
        const polygon = getJurisdictionPolygon(jurisdictionId);
        if (!polygon) {
            console.warn('[SpatialClip] No polygon cached — returning unclipped results');
            return features; // Graceful fallback: return all if no polygon
        }

        return features.filter(f => {
            const geom = f.geometry || {};
            let lng, lat;

            // Handle ArcGIS format (x/y)
            if (geom.x !== undefined && geom.y !== undefined) {
                lng = geom.x; lat = geom.y;
            }
            // Handle GeoJSON format (coordinates)
            else if (geom.coordinates) {
                [lng, lat] = geom.coordinates;
            }
            // Handle flat attributes
            else {
                const attrs = f.attributes || f.properties || f;
                lng = parseFloat(attrs.longitude || attrs.lng || attrs.LON || attrs.x);
                lat = parseFloat(attrs.latitude || attrs.lat || attrs.LAT || attrs.y);
            }

            if (isNaN(lng) || isNaN(lat)) return false;
            return pointInFeature(lng, lat, polygon);
        });
    }

    /**
     * Clip line features to jurisdiction boundary
     * @param {Array} features - Array of GeoJSON features with LineString geometry
     * @param {string} jurisdictionId - Jurisdiction ID for polygon lookup
     * @returns {Array} Features that intersect the jurisdiction polygon
     */
    function clipLines(features, jurisdictionId) {
        const polygon = getJurisdictionPolygon(jurisdictionId);
        if (!polygon) return features;

        const polyTurf = turf.feature(polygon.geometry);
        return features.filter(f => {
            try {
                return turf.booleanIntersects(turf.feature(f.geometry), polyTurf);
            } catch (e) {
                return true; // Keep feature if test fails
            }
        });
    }

    /**
     * Clip polygon features to jurisdiction boundary (geometric intersection)
     * @param {Array} features - Array of GeoJSON features with Polygon geometry
     * @param {string} jurisdictionId - Jurisdiction ID for polygon lookup
     * @returns {Array} Features clipped to jurisdiction boundary
     */
    function clipPolygons(features, jurisdictionId) {
        const polygon = getJurisdictionPolygon(jurisdictionId);
        if (!polygon) return features;

        const polyTurf = turf.feature(polygon.geometry);
        return features.map(f => {
            try {
                const intersection = turf.intersect(
                    turf.featureCollection([turf.feature(f.geometry), polyTurf])
                );
                if (!intersection) return null;
                return { ...f, geometry: intersection.geometry };
            } catch (e) {
                return f; // Keep original if intersection fails
            }
        }).filter(Boolean);
    }

    return { clipPoints, clipLines, clipPolygons, getJurisdictionPolygon };
})();
```

### Integration Points

Where to add `SpatialClipService` calls in existing code:

| Function | File Location | Change |
|----------|--------------|--------|
| `transitTryStatewideData()` | After bbox fetch returns | `SpatialClipService.clipPoints(data.features, jurisdictionId)` |
| `transitValidateLocation()` | Replace bbox+distance heuristic | Use `pointInFeature()` with cached polygon |
| Overpass road network fetch | After OSM response parsed | `SpatialClipService.clipLines(ways, jurisdictionId)` |
| Overpass signal/crossing fetch | After OSM response parsed | `SpatialClipService.clipPoints(nodes, jurisdictionId)` |
| Mapillary graph API fetch | After features received | `SpatialClipService.clipPoints(features, jurisdictionId)` |
| Road Inventory `roadInv_fetchChunked()` | After chunks merged | `SpatialClipService.clipLines(allWays, jurisdictionId)` |

### Loading Sequence Dependency

```
┌──────────────────────────────────────────────────────────────┐
│  ON JURISDICTION SELECT:                                       │
│                                                                │
│  1. Fetch TIGERweb county polygon ──► Cache in geojsonCache   │
│     (addJurisdictionBoundaryLayer)     [MUST complete first]   │
│                                                                │
│  2. Fetch data via bbox APIs ──────► Raw results (with noise)  │
│     (transit, OSM, Mapillary)         [Can run in parallel]    │
│                                                                │
│  3. Clip results to polygon ───────► Clean, accurate results   │
│     (SpatialClipService)              [After BOTH 1 & 2 done]  │
│                                                                │
│  4. Display on map / in tables ────► Precise jurisdiction data │
└──────────────────────────────────────────────────────────────┘
```

**Critical dependency:** The TIGERweb polygon fetch (step 1) must complete before clipping (step 3) can run. However, steps 1 and 2 can run **in parallel** — fetch the polygon and the data simultaneously, then clip when both are ready.

### Graceful Fallback

If the TIGERweb polygon is not available (API down, cache miss, timeout):
- **Fallback to current bbox behavior** — show all bbox results without clipping
- Display a UI indicator: "Approximate area shown — boundary data unavailable"
- This ensures the tool never breaks; it just loses precision temporarily

---

## 17. Database & Storage Strategy

### Design Rationale

CRASH LENS crash data is **static and read-only**, with new data added **once per month**. This eliminates the need for traditional server-side databases (PostgreSQL, Supabase, etc.) which are designed for frequently-mutating, real-time, multi-writer workloads. Instead, we invest in **build-time pre-processing** during the monthly data pipeline and keep the runtime architecture **zero-backend** — no servers to manage, no databases to maintain, no ongoing hosting costs.

### Data Profile

| Metric | Value |
|--------|-------|
| Crash records per county (5 years) | ~20,000-50,000 rows |
| Raw CSV size per county | ~15-50 MB |
| Colorado total (64 counties, 5yr) | ~1.3-3.2 GB raw CSV |
| Update frequency | **Once per month** |
| Data access pattern | **Read-only** |
| Users per deployment | 1-50 concurrent |

### Tier 1: Pre-Processed Static Files — "Smarter CSVs" (Current + Enhanced)

```
Approach: CSV → Parquet conversion + pre-aggregated JSON summaries
Best for: Single county to ~10 counties
Performance: Instant dashboards (pre-computed), fast drill-down
Storage: ~2-5MB per county (Parquet, 10x smaller than CSV)
Deployment: Static hosting (GitHub Pages, S3, Netlify, Cloudflare Pages)
Cost: $0
```

**What happens during the monthly data pipeline (Python scripts):**

1. Download raw CSV from CDOT
2. Convert to **Apache Parquet** format (columnar, compressed, 10x smaller)
3. Pre-compute per-county aggregates: severity counts, EPDO, by-route breakdowns, hotspot rankings
4. Pre-compute per-region rollups: Region 1-5 summaries
5. Pre-compute statewide totals
6. Output compact **JSON summary files** alongside Parquet data
7. Deploy static files to hosting

**What happens at runtime (browser):**

1. App loads small JSON summary (~50KB) → instant dashboard
2. User drills into county → lazy-load that county's Parquet/CSV on demand
3. No server queries, no API calls, no database connections

**Why Parquet over CSV:**

| Feature | CSV | Parquet |
|---------|-----|---------|
| File size (Douglas County 5yr) | ~17 MB | ~1.7 MB |
| Column-selective reading | No (must parse entire row) | Yes (read only needed columns) |
| Typed data | No (everything is strings) | Yes (dates, numbers, booleans) |
| Compression | None | Snappy/GZIP built-in |
| Browser support | Native (Papa Parse) | Via DuckDB-WASM or Apache Arrow JS |

### Tier 2: IndexedDB + Service Worker — Browser-Side Caching

```
Approach: Load Parquet/CSV once from CDN, cache parsed data in browser IndexedDB
Best for: 10-64 counties (full state), repeat users
Performance: First load moderate, all subsequent loads instant
Storage: Up to ~2GB in IndexedDB per browser origin
Deployment: Static hosting + Service Worker for offline capability
Cost: $0
```

**How it works:**

1. First visit: fetch county Parquet from static hosting, parse, store in IndexedDB
2. Subsequent visits: load directly from IndexedDB (instant, works offline)
3. Service Worker checks monthly for updated data (ETag / Last-Modified headers)
4. User switches counties → load from IndexedDB if cached, fetch if not
5. LRU eviction when approaching storage limits (~40-100 counties fit in 2GB)

**Recommended libraries:**

| Library | Size | Notes |
|---------|------|-------|
| **Dexie.js** | ~45 KB | Clean IndexedDB wrapper, excellent query API, widely used |
| **idb** | ~1.2 KB | Minimal Promise wrapper by Jake Archibald (Google Chrome team) |
| **localForage** | ~8 KB | Simple key-value API, automatic fallback to localStorage |

Recommendation: **Dexie.js** for its query capabilities and developer ergonomics.

### Tier 3: DuckDB-WASM — In-Browser Analytical Database

```
Approach: Full SQL analytical engine running in the browser via WebAssembly
Best for: Statewide analysis (64 counties), ad-hoc cross-county queries, multi-state
Performance: Sub-second analytical queries on millions of rows, directly on Parquet files
Storage: Queries remote Parquet files via HTTP range requests (no full download needed)
Deployment: Static hosting (same as Tier 1) + DuckDB-WASM library (~3-5MB, cached)
Cost: $0
```

**Why DuckDB-WASM is the ideal fit for CRASH LENS:**

1. **Runs entirely in the browser** — no server, no API, no database to manage
2. **Queries remote Parquet files directly** — HTTP range requests fetch only the columns/rows needed
3. **Full SQL support** — complex analytical queries that would be impossible with plain JavaScript:
   ```sql
   SELECT route, county,
          COUNT(*) as total,
          SUM(CASE WHEN severity IN ('K','A') THEN 1 ELSE 0 END) as ka_crashes,
          SUM(epdo) as epdo_score
   FROM 'data/CDOT/counties/*/crashes.parquet'
   WHERE crash_year >= 2022
   GROUP BY route, county
   ORDER BY epdo_score DESC
   LIMIT 50
   ```
4. **Handles millions of rows** efficiently (columnar execution engine, vectorized processing)
5. **No data duplication** — Parquet files on CDN are the single source of truth
6. **Composable with IndexedDB** — cache query results for instant repeat access

**DuckDB-WASM integration pattern:**

```javascript
// Initialize DuckDB-WASM (one-time, cached by browser)
import * as duckdb from '@duckdb/duckdb-wasm';

const db = await duckdb.createWorker();

// Query remote Parquet files directly (HTTP range requests)
const result = await db.query(`
    SELECT route, COUNT(*) as crashes, SUM(epdo) as epdo
    FROM 'https://crashlens.org/data/CDOT/counties/035/crashes.parquet'
    WHERE severity IN ('K', 'A', 'B')
    GROUP BY route
    ORDER BY epdo DESC
`);

// Cross-county analysis (glob pattern queries multiple files)
const regionHotspots = await db.query(`
    SELECT county_name, route, node, SUM(epdo) as epdo
    FROM 'https://crashlens.org/data/CDOT/regions/region_1/*.parquet'
    GROUP BY county_name, route, node
    ORDER BY epdo DESC
    LIMIT 25
`);
```

**Trade-off:** DuckDB-WASM bundle is ~3-5 MB (one-time download, browser-cached). For a professional tool used repeatedly by traffic engineers, this is negligible.

### Tier 4 (Emergency Only): Edge SQLite — Lightweight Serverless Backend

```
Approach: SQLite at the edge via Cloudflare D1 or Turso (only if Tiers 1-3 prove insufficient)
Best for: Multi-state with 500+ concurrent users, or if auth/access control is required
Performance: Sub-10ms queries at the edge (CDN-level latency)
Storage: 5-10 GB free tier
Deployment: Serverless (no containers, no servers)
Cost: $0-5/month (generous free tiers)
```

**When to consider this (and only then):**
- You need **server-side authentication** (e.g., role-based access for CDOT vs. public)
- You have **500+ concurrent users** and CDN bandwidth costs become a concern
- You need **write capabilities** (user annotations, saved analyses, shared reports)

| Service | Free Tier | Technology | Managed |
|---------|-----------|------------|---------|
| **Cloudflare D1** | 5 GB storage, 5M reads/day | SQLite at edge | Yes |
| **Turso** | 9 GB storage, 1B reads/month | LibSQL (SQLite fork) | Yes |

Both are **dramatically simpler** than PostgreSQL — no connection pools, no migrations, no Docker containers. SQLite is a single file, deployed to CDN edge nodes.

### Recommended Progression

```
Phase 1 (Now)        → Tier 1: Pre-processed Parquet + JSON on static hosting ($0)
Phase 2 (3-6 months) → Tier 2: Add IndexedDB caching for multi-county repeat visits ($0)
Phase 3 (6-12 months)→ Tier 3: Add DuckDB-WASM for statewide ad-hoc SQL analysis ($0)
Phase 4 (Only if needed) → Tier 4: Edge SQLite for auth/multi-state at scale ($0-5/mo)
```

**Total infrastructure cost through Phase 3: $0**

---

## 18. Data Pipeline & Query Architecture (Zero-Backend)

### Design Philosophy: Compute at Build Time, Not at Runtime

Since crash data is static and updates monthly, the **monthly Python data pipeline** does all the heavy lifting. The browser receives pre-computed results and only performs computation when users request ad-hoc analysis.

```
┌─────────────────────────────────────────────────────────────────┐
│              MONTHLY DATA PIPELINE (Python)                       │
│              Runs once/month on developer machine or CI           │
│                                                                   │
│  ┌──────────┐    ┌───────────────┐    ┌──────────────────────┐  │
│  │ Raw CSV  │───▶│ Process &     │───▶│ OUTPUT:              │  │
│  │ from CDOT│    │ Standardize   │    │                      │  │
│  │          │    │ (existing     │    │ ├─ counties/          │  │
│  │          │    │  scripts)     │    │ │  ├─ 035/            │  │
│  │          │    │               │    │ │  │  ├─ crashes.parquet │
│  │          │    │               │    │ │  │  └─ summary.json │  │
│  │          │    │               │    │ │  ├─ 041/            │  │
│  │          │    │               │    │ │  │  ├─ crashes.parquet │
│  │          │    │               │    │ │  │  └─ summary.json │  │
│  │          │    │               │    │ │  └─ ...             │  │
│  │          │    │               │    │ ├─ regions/           │  │
│  │          │    │               │    │ │  ├─ region_1.json   │  │
│  │          │    │               │    │ │  ├─ region_2.json   │  │
│  │          │    │               │    │ │  └─ ...             │  │
│  │          │    │               │    │ ├─ statewide.json     │  │
│  │          │    │               │    │ └─ manifest.json      │  │
│  └──────────┘    └───────────────┘    └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                    Deploy to static hosting
                    (GitHub Pages / S3 / Netlify)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              BROWSER RUNTIME (Zero Backend)                       │
│                                                                   │
│  ┌────────────┐   ┌─────────────────┐   ┌──────────────────┐   │
│  │ Load JSON  │──▶│ Render Dashboard│──▶│ User drills into │   │
│  │ summaries  │   │ (instant)       │   │ specific county  │   │
│  │ (~50 KB)   │   │                 │   │                  │   │
│  └────────────┘   └─────────────────┘   └────────┬─────────┘   │
│                                                    │             │
│                    ┌───────────────────────────────▼──────────┐  │
│                    │ Tier 2: Load from IndexedDB (if cached)  │  │
│                    │ OR fetch Parquet from CDN                 │  │
│                    │ → Cache in IndexedDB for next time       │  │
│                    └───────────────────────────────┬──────────┘  │
│                                                    │             │
│                    ┌───────────────────────────────▼──────────┐  │
│                    │ Tier 3: DuckDB-WASM for ad-hoc SQL      │  │
│                    │ (cross-county queries, custom filters)   │  │
│                    └─────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Pre-Computed Output File Structure

```
data/CDOT/
├── manifest.json                    # Data freshness metadata
│   {
│     "lastUpdated": "2026-02-01",
│     "dataVersion": "2026.02",
│     "counties": { "035": { "rows": 24598, "years": [2021,2022,2023,2024,2025] } },
│     "checksums": { "035/crashes.parquet": "sha256:abc123..." }
│   }
│
├── statewide.json                   # ~20 KB — statewide aggregates
│   {
│     "totalCrashes": 142567,
│     "bySeverity": { "K": 698, "A": 2134, "B": 8901, "C": 42567, "O": 88267 },
│     "epdo": 512340,
│     "byYear": { "2021": {...}, "2022": {...}, ... },
│     "topCounties": [...]
│   }
│
├── regions/
│   ├── region_1.json                # ~10 KB — Region 1 aggregate
│   ├── region_2.json
│   ├── region_3.json
│   ├── region_4.json
│   └── region_5.json
│
├── tprs/
│   ├── drcog.json                   # ~10 KB — DRCOG MPO aggregate
│   ├── nfrmpo.json
│   └── ...
│
├── counties/
│   ├── 035/                         # Douglas County
│   │   ├── crashes.parquet          # ~1.7 MB (vs ~17 MB CSV)
│   │   ├── summary.json             # ~5 KB — pre-computed aggregates
│   │   └── hotspots.json            # ~3 KB — top 25 hotspot locations
│   ├── 041/                         # El Paso County
│   │   ├── crashes.parquet
│   │   ├── summary.json
│   │   └── hotspots.json
│   └── .../
│
└── corridors/
    ├── I-25.json                    # ~5 KB — I-25 corridor summary
    ├── I-70.json
    └── ...
```

### Pre-Aggregated JSON Schema (summary.json per county)

```json
{
  "county": { "fips": "035", "name": "Douglas County" },
  "period": { "startYear": 2021, "endYear": 2025, "totalMonths": 49 },
  "totals": {
    "crashes": 24598,
    "K": 42, "A": 187, "B": 2103, "C": 5432, "O": 17734,
    "epdo": 67891,
    "pedestrian": 234, "bicycle": 89, "impaired": 1567
  },
  "byYear": {
    "2021": { "crashes": 5000, "K": 8, "A": 35, "B": 240, "C": 1100, "O": 3617 },
    "2022": { "crashes": 4638, "K": 7, "A": 32, "B": 220, "C": 980, "O": 3399 }
  },
  "byRoute": {
    "I 25": { "total": 3456, "K": 12, "A": 45, "epdo": 12340 },
    "LINCOLN AVE": { "total": 890, "K": 3, "A": 18, "epdo": 3210 }
  },
  "topHotspots": [
    { "route": "I 25", "node": "CASTLE PINES PKWY", "total": 234, "epdo": 4560 }
  ],
  "collisionTypes": { "Rear End": 8900, "Broadside": 3400, "Sideswipe": 2100 },
  "weatherDist": { "Clear": 18000, "Rain": 3200, "Snow": 2100 },
  "lightDist": { "Daylight": 16000, "Dark - Lighted": 4500, "Dark - Not Lighted": 2100 }
}
```

### DuckDB-WASM Query Layer (Tier 3)

When users need analysis beyond pre-computed summaries (ad-hoc cross-county queries, custom filters), DuckDB-WASM provides full SQL in the browser:

```javascript
// queryEngine.js — Thin wrapper around DuckDB-WASM

const QueryEngine = (() => {
    'use strict';

    let db = null;

    return {
        /**
         * Initialize DuckDB-WASM (lazy — only loaded when first query requested)
         * Bundle is ~3-5MB, cached by browser after first load
         */
        async init() {
            if (db) return;
            const duckdb = await import('@duckdb/duckdb-wasm');
            db = await duckdb.createWorker();
            // Register remote Parquet file paths
            await db.query(`CREATE VIEW crashes AS
                SELECT * FROM read_parquet('data/CDOT/counties/*/crashes.parquet',
                                           hive_partitioning=true)`);
        },

        /**
         * Run analytical SQL query on crash data
         * Works on remote Parquet files via HTTP range requests
         */
        async query(sql) {
            await this.init();
            return await db.query(sql);
        },

        /**
         * Pre-built queries for common operations
         */
        async getRegionHotspots(regionId, topN = 25) {
            const counties = HierarchyRegistry.getCountiesInRegion(regionId);
            const paths = counties.map(c =>
                `'data/CDOT/counties/${c}/crashes.parquet'`
            ).join(', ');
            return await this.query(`
                SELECT route, node, county_fips,
                       COUNT(*) as total,
                       SUM(CASE WHEN severity = 'K' THEN 462
                                WHEN severity = 'A' THEN 62
                                WHEN severity = 'B' THEN 12
                                WHEN severity = 'C' THEN 5
                                ELSE 1 END) as epdo
                FROM read_parquet([${paths}])
                GROUP BY route, node, county_fips
                ORDER BY epdo DESC
                LIMIT ${topN}
            `);
        },

        async getCorridorAnalysis(routePattern, yearStart, yearEnd) {
            return await this.query(`
                SELECT county_fips, crash_year,
                       COUNT(*) as total,
                       SUM(CASE WHEN severity IN ('K','A') THEN 1 ELSE 0 END) as ka,
                       SUM(CASE WHEN pedestrian THEN 1 ELSE 0 END) as ped
                FROM read_parquet('data/CDOT/counties/*/crashes.parquet')
                WHERE route LIKE '${routePattern}%'
                  AND crash_year BETWEEN ${yearStart} AND ${yearEnd}
                GROUP BY county_fips, crash_year
                ORDER BY county_fips, crash_year
            `);
        }
    };
})();
```

### Why NOT a Traditional Backend API

| Concern | Traditional Backend | Our Zero-Backend Approach |
|---------|--------------------|-----------------------------|
| Server costs | $25-200+/month | $0 (static hosting) |
| Operational burden | Database backups, migrations, monitoring, security patches | None |
| Latency | Network round-trip to server | Local computation (instant) |
| Offline support | No | Yes (IndexedDB + Service Worker) |
| Scalability | Must provision for peak load | Each browser is its own "server" |
| Data freshness | Real-time (unnecessary — data is monthly) | Monthly deploy (matches data cadence) |
| Deployment | Docker, CI/CD, cloud provider | `git push` to GitHub Pages |
| Single point of failure | Server goes down → app is down | CDN-distributed, no SPOF |

A backend API becomes justified **only** when:
- You need **server-side authentication** with role-based access control
- You have **500+ concurrent users** where CDN bandwidth costs exceed $50/month
- You need **real-time write operations** (user annotations, collaborative features)
- You expand to **10+ states** where even Parquet files exceed practical CDN size

At that point, use **Cloudflare D1** or **Turso** (edge SQLite) — not PostgreSQL. SQLite at the edge provides sub-10ms queries with zero infrastructure management.

---

## 19. Phased Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)

**Goal:** All 64 Colorado counties loadable with zero code changes per county.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 1.1 | Create `states/08/hierarchy.json` with all CDOT regions, TPRs, counties | 3 days | Research (done) |
| 1.2 | Build `hierarchy_registry.js` — load and query hierarchy data | 3 days | 1.1 |
| 1.3 | Enhance `download_cdot_crash_data.py` to support all 64 counties | 2 days | — |
| 1.4 | Build `split_by_jurisdiction.py` to split statewide CSV by county | 2 days | — |
| 1.5 | Restructure `data/CDOT/` to per-county subdirectories | 1 day | 1.4 |
| 1.6 | Update `APP_PATHS.getDataFile()` to use new directory structure | 1 day | 1.5 |
| 1.7 | Populate all 64 county configs from FIPSDatabase automatically | 1 day | 1.1 |
| 1.8 | Test: Load Douglas County from new structure, verify no regressions | 1 day | 1.5, 1.6 |

**Deliverable:** Any Colorado county's data can be downloaded, processed, and loaded by changing only a config value.

### Phase 2: Multi-County Aggregation (Weeks 5-8)

**Goal:** View crash data for multiple counties at once (region view).

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 2.1 | Build `aggregation_engine.js` — load and combine multiple county CSVs | 5 days | 1.2 |
| 2.2 | Build WebWorker wrapper for heavy aggregation (keep UI responsive) | 2 days | 2.1 |
| 2.3 | Add IndexedDB caching layer for loaded county data | 3 days | 2.1 |
| 2.4 | Build `pre_aggregate.py` to generate summary JSONs per region/county | 3 days | 1.4 |
| 2.5 | Implement cascading scope selector UI | 3 days | 1.2 |
| 2.6 | Wire scope selector to aggregation engine | 2 days | 2.1, 2.5 |
| 2.7 | Test: Load Region 1 (10 counties), verify aggregation correctness | 2 days | 2.1-2.6 |

**Deliverable:** Users can select "CDOT Region 1" and see combined data from all 10 counties.

### Phase 3: Region & Statewide Views (Weeks 9-14)

**Goal:** Full region dashboard and statewide overview.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 3.1 | Design & implement Region Dashboard tab (per-county breakdown) | 5 days | 2.1 |
| 3.2 | Implement cross-county hotspot ranking algorithm | 3 days | 2.1 |
| 3.3 | Add county boundary GeoJSON to map view | 3 days | — |
| 3.4 | Add region boundary GeoJSON to map view | 2 days | 3.3 |
| 3.5 | Implement choropleth coloring (counties by crash rate/EPDO) | 3 days | 3.3 |
| 3.6 | Build statewide summary dashboard | 4 days | 2.4 |
| 3.7 | Implement drill-down navigation (state → region → county) | 3 days | 3.1, 3.6 |
| 3.8 | Update AI Assistant for multi-county context awareness | 3 days | 2.1 |
| 3.9 | Test: Full statewide view with all 5 regions | 3 days | All above |

**Deliverable:** CDOT HQ can view statewide dashboard with drill-down to any region/county.

### Phase 4: MPO/TPR Views (Weeks 15-18)

**Goal:** MPO planners can use the tool for their planning area.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 4.1 | Implement MPO/TPR scope selection in UI | 2 days | 2.5 |
| 4.2 | Handle partial county membership (point-in-polygon or city filter) | 4 days | 3.3 |
| 4.3 | Build TPR boundary GeoJSON layers | 2 days | 3.3 |
| 4.4 | Implement MPO-specific dashboard features | 3 days | 3.1 |
| 4.5 | Add safety performance target tracking | 3 days | 2.4 |
| 4.6 | Test: DRCOG view with 9+ counties | 2 days | All above |

**Deliverable:** MPO planners see their planning area with multi-county aggregation.

### Phase 5: Corridor Analysis (Weeks 19-22)

**Goal:** Analyze crash patterns along a corridor spanning multiple counties.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 5.1 | Define corridor routes in hierarchy.json | 2 days | 1.1 |
| 5.2 | Implement corridor scope selection | 2 days | 2.5 |
| 5.3 | Build corridor-specific filtering (match route name across counties) | 3 days | 2.1 |
| 5.4 | Linear referencing view (milepost-based crash plotting) | 5 days | 5.3 |
| 5.5 | Corridor safety metrics (crash rate per mile, etc.) | 3 days | 5.3 |
| 5.6 | Test: I-25 corridor from Pueblo to Fort Collins | 2 days | All above |

**Deliverable:** Engineers can analyze I-25 from end to end across all counties it passes through.

### Phase 6: Automated Onboarding & Multi-State (Weeks 23-30)

**Goal:** One command adds a new county or state.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 6.1 | Build `onboard_jurisdiction.py` script | 5 days | 1.3, 1.4 |
| 6.2 | Build state setup wizard (interactive CLI) | 3 days | 6.1 |
| 6.3 | Auto-fetch county boundaries from Census TIGERweb | 2 days | — |
| 6.4 | Create new-state template directory | 1 day | — |
| 6.5 | Test: Onboard Texas (new state, new CSV format) | 5 days | All above |
| 6.6 | Test: Onboard remaining 58 Colorado counties | 3 days | 6.1 |
| 6.7 | Documentation and usage guide | 3 days | All above |

**Deliverable:** `python onboard_jurisdiction.py --state texas --setup` creates a working Texas instance.

### Phase 7: DuckDB-WASM + Parquet Query Layer (Weeks 31-36)

**Goal:** Enable ad-hoc cross-county and statewide SQL analysis entirely in the browser — no backend needed.

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 7.1 | Add CSV → Parquet conversion to monthly data pipeline (`pyarrow`) | 3 days | 1.4 |
| 7.2 | Integrate DuckDB-WASM library into app (lazy-loaded, ~3-5MB cached) | 3 days | — |
| 7.3 | Build `QueryEngine` wrapper with pre-built queries (hotspots, corridors, comparisons) | 5 days | 7.2 |
| 7.4 | Add "Custom Query" UI for power users (SQL input with safety constraints) | 3 days | 7.3 |
| 7.5 | Connect cross-county hotspot analysis to DuckDB-WASM queries | 3 days | 7.3, 3.2 |
| 7.6 | Performance testing: 64-county statewide queries in DuckDB-WASM | 2 days | 7.1, 7.3 |
| 7.7 | Add query result caching (IndexedDB) for repeat cross-county queries | 2 days | 2.3, 7.3 |

**Deliverable:** Full statewide analytical SQL capability running entirely in the browser with zero backend infrastructure.

### Phase 8: Edge Database (Weeks 37+) — Only If Required

**Goal:** Add server-side capabilities only if specific triggers are met.

**Triggers (implement Phase 8 only if ANY of these become true):**
- Need for **server-side authentication** (role-based access: CDOT HQ vs. public)
- **500+ concurrent users** where CDN bandwidth exceeds $50/month
- **Write capabilities** needed (user annotations, saved analyses, shared reports)
- Expansion to **10+ states** where Parquet files exceed practical CDN hosting size

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 8.1 | Set up Cloudflare D1 or Turso (edge SQLite) | 2 days | — |
| 8.2 | Data import: Parquet → edge SQLite (monthly automated) | 2 days | 8.1 |
| 8.3 | Lightweight API routes via Cloudflare Workers or Turso HTTP API | 3 days | 8.1 |
| 8.4 | Frontend: detect API availability, use API or Parquet files transparently | 3 days | 8.3 |
| 8.5 | Optional: Add authentication via Cloudflare Access or simple API keys | 2 days | 8.3 |

**Deliverable:** Lightweight edge-hosted query layer with zero Docker, zero PostgreSQL, $0-5/month cost.

---

## 20. Risk Assessment & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Browser memory limits** with 64-county data | High | Data won't load | Pre-aggregated JSONs for overview; lazy-load raw CSV only on drill-down |
| **CDOT data access** restricted or format changes | Medium | Pipeline breaks | Multiple download methods; format detection in StateAdapter auto-adapts |
| **Overlapping TPR/Region boundaries** cause double-counting | Medium | Incorrect stats | Clearly document: each crash belongs to exactly one county; aggregation is county-based |
| **Partial county TPR membership** hard to implement | Medium | Incorrect MPO stats | Start with full-county approximation; add point-in-polygon for precision later |
| **Performance degradation** at scale | Medium | Poor UX | Pre-aggregated JSON for dashboards, Parquet for 10x smaller files, IndexedDB caching, DuckDB-WASM for efficient SQL, WebWorkers for heavy computation |
| **Stale data** across many counties | Low | Outdated analysis | Automated weekly GitHub Actions for scheduled counties; manifest tracks freshness |
| **Cross-county intersection naming** inconsistent | Medium | Node matching fails | Normalize intersection names; use coordinates for proximity matching |
| **GeoJSON boundaries** too large for browser | Low | Slow map | Simplify geometries (turf.simplify); use vector tiles at scale |

---

## Appendix A: Complete Colorado County-to-Region Mapping

### All 64 Colorado Counties → CDOT Region → TPR/MPO

| County | FIPS | CDOT Region | TPR/MPO | Pop. (est.) |
|--------|------|-------------|---------|-------------|
| Adams | 001 | Region 1 | DRCOG (MPO) | 519,572 |
| Alamosa | 003 | Region 5 | San Luis Valley TPR | 17,141 |
| Arapahoe | 005 | Region 1 | DRCOG (MPO) | 655,070 |
| Archuleta | 007 | Region 5 | Southwest TPR | 14,029 |
| Baca | 009 | Region 2 | Southeast TPR | 3,556 |
| Bent | 011 | Region 2 | Southeast TPR | 5,577 |
| Boulder | 013 | Region 1 | DRCOG (MPO) | 330,758 |
| Broomfield | 014 | Region 1 | DRCOG (MPO) | 74,112 |
| Chaffee | 015 | Region 2/5 | San Luis Valley TPR | 20,477 |
| Cheyenne | 017 | Region 4 | Eastern TPR | 1,735 |
| Clear Creek | 019 | Region 1 | DRCOG (MPO) | 9,700 |
| Conejos | 021 | Region 5 | San Luis Valley TPR | 7,720 |
| Costilla | 023 | Region 5 | San Luis Valley TPR | 3,887 |
| Crowley | 025 | Region 2 | Southeast TPR | 6,012 |
| Custer | 027 | Region 2 | Central Front Range TPR | 5,068 |
| Delta | 029 | Region 3 | Gunnison Valley TPR | 31,024 |
| Denver | 031 | Region 1 | DRCOG (MPO) | 713,252 |
| Dolores | 033 | Region 5 | Southwest TPR | 2,286 |
| Douglas | 035 | Region 1 | DRCOG (MPO) | 357,978 |
| Eagle | 037 | Region 3 | Intermountain TPR | 55,731 |
| Elbert | 039 | Region 2 | Eastern TPR | 27,440 |
| El Paso | 041 | Region 2 | PPACG (MPO) | 730,395 |
| Fremont | 043 | Region 2 | Central Front Range TPR | 48,004 |
| Garfield | 045 | Region 3 | Intermountain TPR | 62,213 |
| Gilpin | 047 | Region 1 | DRCOG (MPO) | 6,243 |
| Grand | 049 | Region 3 | Northwest TPR | 15,734 |
| Gunnison | 051 | Region 3 | Gunnison Valley TPR | 17,462 |
| Hinsdale | 053 | Region 3 | Gunnison Valley TPR | 820 |
| Huerfano | 055 | Region 2 | South Central TPR | 6,459 |
| Jackson | 057 | Region 3 | Northwest TPR | 1,359 |
| Jefferson | 059 | Region 1 | DRCOG (MPO) | 582,881 |
| Kiowa | 061 | Region 2 | Southeast TPR | 1,406 |
| Kit Carson | 063 | Region 4 | Eastern TPR | 7,096 |
| Lake | 065 | Region 2/3 | Intermountain TPR | 8,327 |
| La Plata | 067 | Region 5 | Southwest TPR | 56,221 |
| Larimer | 069 | Region 4 | NFRMPO (MPO) | 359,066 |
| Las Animas | 071 | Region 2 | South Central TPR | 14,506 |
| Lincoln | 073 | Region 2/4 | Eastern TPR | 5,608 |
| Logan | 075 | Region 4 | Eastern TPR | 22,409 |
| Mesa | 077 | Region 3 | GVMPO (MPO) | 157,502 |
| Mineral | 079 | Region 5 | San Luis Valley TPR | 803 |
| Moffat | 081 | Region 3 | Northwest TPR | 13,283 |
| Montezuma | 083 | Region 5 | Southwest TPR | 26,266 |
| Montrose | 085 | Region 3 | Gunnison Valley TPR | 47,082 |
| Morgan | 087 | Region 4 | Upper Front Range TPR | 29,068 |
| Otero | 089 | Region 2 | Southeast TPR | 18,278 |
| Ouray | 091 | Region 3 | Gunnison Valley TPR | 5,044 |
| Park | 093 | Region 2 | Central Front Range TPR | 18,845 |
| Phillips | 095 | Region 4 | Eastern TPR | 4,285 |
| Pitkin | 097 | Region 3 | Intermountain TPR | 17,767 |
| Prowers | 099 | Region 2 | Southeast TPR | 11,722 |
| Pueblo | 101 | Region 2 | PACOG (MPO) | 168,424 |
| Rio Blanco | 103 | Region 3 | Northwest TPR | 6,352 |
| Rio Grande | 105 | Region 5 | San Luis Valley TPR | 11,180 |
| Routt | 107 | Region 3 | Northwest TPR | 26,090 |
| Saguache | 109 | Region 5 | San Luis Valley TPR | 6,824 |
| San Juan | 111 | Region 5 | Southwest TPR | 737 |
| San Miguel | 113 | Region 3 | Gunnison Valley TPR | 8,179 |
| Sedgwick | 115 | Region 4 | Eastern TPR | 2,248 |
| Summit | 117 | Region 1 | Intermountain TPR | 31,011 |
| Teller | 119 | Region 2 | PPACG (MPO) | 25,388 |
| Washington | 121 | Region 4 | Eastern TPR | 4,830 |
| Weld | 123 | Region 4 | NFRMPO (MPO) / DRCOG (partial) / UFR (partial) | 324,492 |
| Yuma | 125 | Region 4 | Eastern TPR | 10,019 |

> **Note:** Weld County spans three TPRs: NFRMPO, DRCOG (southwest portion), and Upper Front Range (northeast portion). This is the most complex case in Colorado.

---

## Appendix B: Configuration Schema Reference

### hierarchy.json Schema

```jsonc
{
  // State identification
  "state": {
    "fips": "string (2-digit)",      // Required: "08"
    "name": "string",                 // Required: "Colorado"
    "abbreviation": "string (2-char)" // Required: "CO"
  },

  // DOT region/district configuration
  "regionType": {
    "label": "string",               // Display label: "CDOT Engineering Region"
    "labelPlural": "string",         // Plural: "CDOT Engineering Regions"
    "shortLabel": "string",          // Short: "Region"
    "description": "string"          // Description for tooltips
  },

  // Region definitions (keyed by region ID)
  "regions": {
    "<region_id>": {
      "id": "string",                // Unique ID: "region_1"
      "name": "string",              // Short name: "Region 1"
      "fullName": "string",          // Full: "CDOT Region 1 — Denver Metro"
      "office": "string",            // Office address
      "phone": "string",             // Optional contact
      "director": "string",          // Optional director name
      "counties": ["string"],        // Array of 3-digit county FIPS
      "countyNames": ["string"],     // Human-readable county names
      "mapCenter": [number, number], // [lat, lon]
      "mapZoom": "number",           // Mapbox zoom level
      "keyCorridors": ["string"],    // Major routes in this region
      "config": {}                   // Optional region-specific overrides
    }
  },

  // TPR/MPO configuration
  "tprType": {
    "label": "string",               // "Transportation Planning Region"
    "labelPlural": "string",
    "shortLabel": "string"
  },

  // TPR definitions (keyed by TPR ID)
  "tprs": {
    "<tpr_id>": {
      "id": "string",                // Unique ID: "drcog"
      "name": "string",              // Short: "DRCOG"
      "fullName": "string",          // Full: "Denver Regional Council of Governments"
      "type": "string",              // "mpo" or "rural_tpr"
      "counties": ["string"],        // Full-coverage county FIPS codes
      "countyNames": ["string"],
      "partialCounties": {           // Counties with partial coverage
        "<county_fips>": "string"    // Description of partial coverage
      },
      "adminOrg": "string",          // Administering organization
      "website": "string",           // Optional URL
      "mapCenter": [number, number],
      "mapZoom": "number"
    }
  },

  // Corridor definitions
  "corridors": {
    "<corridor_id>": {
      "name": "string",              // "I-25 Corridor"
      "description": "string",
      "routePattern": "string",      // Regex to match route names
      "counties": ["string"],        // County FIPS codes along corridor
      "regions": ["string"],         // Region IDs crossed
      "length_miles": "number"
    }
  }
}
```

### State config.json Schema (Enhanced)

```jsonc
{
  // State identification
  "state": {
    "name": "string",
    "abbreviation": "string",
    "fips": "string",
    "dotName": "string",
    "dotFullName": "string",
    "dataDir": "string"              // Subdirectory in data/
  },

  // Column mapping (state CSV → internal format)
  "columnMapping": {
    "ID": "string",                  // Crash ID column
    "DATE": "string",
    "SEVERITY": "string",            // or null if derived
    // ... (existing schema from data/CDOT/config.json)
  },

  // Fields computed from other columns
  "derivedFields": {
    "SEVERITY": { "method": "...", "sources": ["..."] },
    // ... (existing schema)
  },

  // Road classification
  "roadSystems": {
    "values": {},
    "filterProfiles": {}
  },

  // EPDO weights (may vary by state/agency policy)
  "epdoWeights": {
    "K": 883, "A": 94, "B": 21, "C": 11, "O": 1
  },

  // Data source configuration
  "dataSource": {
    "name": "string",
    "url": "string",
    "apiUrl": "string",
    "fileFormat": "string",
    "downloadMethod": "string"       // "api", "file", "scrape"
  },

  // NEW: Multi-jurisdiction data configuration
  "multiJurisdiction": {
    "enabled": true,
    "hierarchyFile": "states/08/hierarchy.json",
    "boundaryFiles": {
      "regions": "states/08/regions.geojson"  // Only static file — CDOT-specific
      // All other boundaries fetched from TIGERweb/BTS APIs:
      // Counties, state outline, places, tracts → TIGERweb API (see Section 8)
      // MPO/TPR boundaries → BTS NTAD MPO API (see Section 4)
    },
    "boundaryApis": {
      "tigerweb": {
        "stateOutline": { "layer": 80, "where": "STATE='{stateFips}'" },
        "counties": { "layer": 82, "where": "STATE='{stateFips}'" },
        "places": { "layer": 28, "where": "STATE='{stateFips}' AND COUNTY='{countyFips}'" },
        "cdps": { "layer": 30, "where": "STATE='{stateFips}' AND COUNTY='{countyFips}'" },
        "censusTracts": { "layer": 8, "where": "STATE='{stateFips}' AND COUNTY='{countyFips}'" },
        "urbanAreas": { "layer": 88, "spatialQuery": true },
        "schoolDistricts": { "layer": 14, "where": "STATE='{stateFips}'" }
      },
      "btsMPO": {
        "endpoint": "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0/query",
        "where": "STATE='{stateAbbrev}' OR STATE_2='{stateAbbrev}' OR STATE_3='{stateAbbrev}'"
      }
    },
    "aggregateDir": "data/CDOT/aggregates",
    "countyDataDir": "data/CDOT/counties",
    "maxConcurrentCountyLoads": 5,
    "preAggregateEnabled": true
  }
}
```

---

## Summary: What Makes This Design "Automatic and Dynamic"

1. **Auto-detection** — StateAdapter reads CSV headers and automatically knows which state's data it's processing. No manual configuration needed per dataset.

2. **Auto-discovery** — If crash data contains a "County" column, the tool can automatically discover all counties present in the data without pre-configuration.

3. **Config-driven hierarchy** — Adding a new county, region, or MPO requires ONLY editing a JSON file. The UI, aggregation, and navigation automatically adapt.

4. **Cascading scope** — Selecting "Region 1" automatically resolves to all counties in that region, loads their data, and aggregates. The user doesn't need to know which counties are in Region 1.

5. **Lazy loading** — Statewide views use pre-aggregated JSON (tiny, instant). Raw CSV is loaded only when drilling into a specific county. Browser memory stays manageable.

6. **Template-based onboarding** — `onboard_jurisdiction.py --state texas --setup` creates all necessary config files from a template, populated with FIPS data already embedded in the codebase.

7. **Multiple overlapping groupings** — A county simultaneously belongs to a CDOT Region AND a TPR/MPO AND corridors. The hierarchy model supports this natively via the DAG (not tree) structure.

8. **Progressive enhancement** — Start with pre-processed Parquet + JSON files (Phase 1, $0), add IndexedDB caching for repeat visits (Phase 2, $0), add DuckDB-WASM for ad-hoc statewide SQL (Phase 3, $0). Optionally add edge SQLite only if auth or 500+ concurrent users are needed (Phase 4, $0-5/mo). The frontend code works identically in all modes with zero backend infrastructure through Phase 3.

---

*This plan was generated based on analysis of the CRASH LENS codebase, CDOT organizational structure research, and Colorado TPR/MPO documentation. FIPS codes and county assignments should be verified against the latest CDOT region boundary shapefile before implementation.*

**Sources:**
- [CDOT Regions](https://www.codot.gov/about/regions)
- [CDOT TPR/MPO Planning Partners](https://www.codot.gov/programs/planning/planning-partners/tpr-mpo)
- [TPR at a Glance (Jan 2026)](https://www.codot.gov/programs/planning/assets/planning-partners/january2026tpr)
- [DRCOG Member Governments](https://www.drcog.org/about-drcog/member-governments)
- [NFRMPO Partners](https://nfrmpo.org/partners/)
- [BTS NTAD Metropolitan Planning Organizations (Boundary Polygons)](https://geodata.bts.gov/datasets/usdot::metropolitan-planning-organizations/about)
- [BTS NTAD MPO ArcGIS Feature Service](https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0)
- [BTS National Transportation Atlas Database](https://www.bts.gov/ntad)
- [TIGERweb Census 2020 MapServer (Layer Catalog)](https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer)
- [TIGERweb Current (2025) MapServer (Layer Catalog)](https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer)
- [TIGERweb REST Services](https://tigerweb.geo.census.gov/tigerwebmain/TIGERweb_restmapservice.html)
- [TIGER/Line Shapefiles Documentation](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
