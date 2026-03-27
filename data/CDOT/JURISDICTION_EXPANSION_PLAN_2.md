# CRASH LENS — Jurisdiction Expansion Plan (Part 2)

## Detailed Tab-Level View Architecture & Data Strategy

**Version:** 1.0
**Date:** February 12, 2026
**Status:** PLANNING ONLY — Not Yet Implemented
**Companion to:** `JURISDICTION_EXPANSION_PLAN.md` (Part 1 — Architecture, Data Model, Infrastructure)

---

## Table of Contents

1. [Scope of This Document](#1-scope-of-this-document)
2. [Critical Scoping Decision: Explore-Only Expansion](#2-critical-scoping-decision-explore-only-expansion)
3. [Tab Classification Matrix](#3-tab-classification-matrix)
4. [Map Tab — Multi-Jurisdiction Display Plan](#4-map-tab--multi-jurisdiction-display-plan)
5. [Dashboard Tab — View Specifications](#5-dashboard-tab--view-specifications)
6. [Crash Tree Tab — View Specifications](#6-crash-tree-tab--view-specifications)
7. [Safety Focus Tab — View Specifications](#7-safety-focus-tab--view-specifications)
8. [Fatal & Speeding Tab — View Specifications](#8-fatal--speeding-tab--view-specifications)
9. [Hot Spots Tab — View Specifications](#9-hot-spots-tab--view-specifications)
10. [Intersections Tab — View Specifications](#10-intersections-tab--view-specifications)
11. [Ped/Bike Tab — View Specifications](#11-pedbike-tab--view-specifications)
12. [Analysis Tab — Infrastructure Assets Sub-Tab](#12-analysis-tab--infrastructure-assets-sub-tab)
13. [Analysis Tab — School Safety Strategy](#13-analysis-tab--school-safety-strategy)
14. [Analysis Tab — Transit Safety Strategy](#14-analysis-tab--transit-safety-strategy)
15. [Analysis Tab — Mapillary Assets (County/City Only)](#15-analysis-tab--mapillary-assets-countycity-only)
16. [Analysis Tab — Road Inventory (County/City Only)](#16-analysis-tab--road-inventory-countycity-only)
17. [Solutions Section — No Expansion (Single-Location Only)](#17-solutions-section--no-expansion-single-location-only)
18. [Grants Section — No Expansion (Single-Jurisdiction Only)](#18-grants-section--no-expansion-single-jurisdiction-only)
19. [Reports Section — No Expansion (Single-Jurisdiction Only)](#19-reports-section--no-expansion-single-jurisdiction-only)
20. [Complete View Adaptation Matrix](#20-complete-view-adaptation-matrix)
21. [Boundary & GeoJSON Loading Strategy](#21-boundary--geojson-loading-strategy)
22. [Spatial Accuracy: Bbox-Fetch + Polygon-Clip](#22-spatial-accuracy-bbox-fetch--polygon-clip)
23. [Progressive Detail Rendering Philosophy](#23-progressive-detail-rendering-philosophy)
24. [Scope Selector Interaction with Non-Explore Tabs](#24-scope-selector-interaction-with-non-explore-tabs)
25. [Performance Constraints by Jurisdiction Level](#25-performance-constraints-by-jurisdiction-level)
26. [Amendments to Part 1](#26-amendments-to-part-1)

---

## 1. Scope of This Document

Part 1 (`JURISDICTION_EXPANSION_PLAN.md`) covers:
- Architecture, data model, hierarchy, infrastructure
- Data pipeline, storage strategy (Parquet, DuckDB-WASM, IndexedDB)
- High-level UI/UX (scope selector, cascading views)
- Phased implementation roadmap

**This document (Part 2) covers what Part 1 was missing:**
- **Exactly how each tab under Explore behaves** at county, region, and state levels
- **Which tabs are excluded** from jurisdiction expansion entirely
- **Detailed Map tab** rendering strategy (progressive detail, layer controls, drill-down)
- **School & Transit data** expansion strategy (how to load, aggregate, display)
- **Mapillary & Road Inventory** scope restrictions (county/city/municipal only)
- **Why Solutions, Grants, and Reports do NOT expand** (rationale and UI behavior)

---

## 2. Critical Scoping Decision: Explore-Only Expansion

### The Rule

**Only the EXPLORE section participates in multi-jurisdiction expansion.**

The Solutions, Grants, and Reports sections **remain single-jurisdiction / single-location tools** regardless of what scope level the user has selected.

### Rationale

| Section | Why It Does NOT Expand | What It Needs Instead |
|---------|----------------------|----------------------|
| **Solutions → Countermeasures** | CMF analysis is inherently **per-intersection**. You select one location, one crash pattern, and find matching countermeasures. Aggregating countermeasures across a region is meaningless — each location has different crash patterns requiring different treatments. | When user is at region/state scope, prompt them to select a specific county + location first. |
| **Solutions → Warrant Analyzer** | Signal/stop warrant analysis (MUTCD) requires **specific traffic volumes, crash counts, and geometry** at one intersection. A region-wide warrant analysis doesn't exist in engineering practice. | Same — require location-level selection. |
| **Solutions → MUTCD AI** | MUTCD guidance is applied to **specific situations** (one intersection, one sign placement, one road segment). Regional MUTCD doesn't make sense. | Works at any level since it's knowledge-based Q&A, but answers are per-situation. |
| **Solutions → Domain Knowledge** | Reference material — not data-dependent. | No change needed; already scope-independent. |
| **Grants** | HSIP grant applications are submitted by **a specific county or agency**. A region doesn't apply for HSIP funds — individual counties within that region do. The ranking/scoring is per-county. | When user is at region/state scope, require county selection before entering Grants tab. |
| **Reports** | Reports are generated for **a specific jurisdiction** with that jurisdiction's header, data, and branding. A "Region 1 Report" is a different concept than a "Douglas County Report." | When user is at region/state scope, either (a) require county selection, or (b) offer "Region Summary Report" as a separate report type in a future enhancement. |

### What Part 1 Got Wrong

Part 1, Section 10 (line 1044-1051) shows a table where **CMF, AI Assistant, and Grants** all adapt to region/state scope:

> | CMF | Select intersection in county | Select intersection across any county in region | Select intersection anywhere in state |
> | Grants | County HSIP ranking | Regional HSIP project prioritization | Statewide HSIP allocation by region |

**This should be corrected.** CMF and Grants do NOT expand. The cross-county intersection selection behavior described for CMF at region level is actually part of the **Hot Spots** or **Intersections** tab under Explore, not the CMF/Solutions tab. See [Section 26: Amendments to Part 1](#26-amendments-to-part-1) for the corrections.

### AI Assistant Exception

The AI Assistant (which lives as a panel/overlay, not a dedicated section tab) **does** adapt to context. It should be aware of whatever scope the user is viewing:
- County scope → AI uses county-level crash profile
- Region scope → AI uses region-level aggregated data
- State scope → AI uses statewide summary

This is not a "section expansion" — it's context awareness, which is already designed in Part 1's `getAIAnalysisContext()` pattern.

---

## 3. Tab Classification Matrix

### Definitive Scope Behavior for Every Tab

| Section | Tab | County | City/Place | Region | MPO/TPR | State | Corridor |
|---------|-----|--------|------------|--------|---------|-------|----------|
| **EXPLORE** | Dashboard | ✅ Full | ✅ Filtered | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate |
| **EXPLORE** | Map | ✅ Full | ✅ Filtered | ✅ Adapted | ✅ Adapted | ✅ Adapted | ✅ Adapted |
| **EXPLORE** | Crash Tree | ✅ Full | ✅ Filtered | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate |
| **EXPLORE** | Safety Focus | ✅ Full | ✅ Filtered | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate |
| **EXPLORE** | Fatal & Speeding | ✅ Full | ✅ Filtered | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate |
| **EXPLORE** | Hot Spots | ✅ Full | ✅ Filtered | ✅ Cross-County | ✅ Cross-County | ✅ Cross-County | ✅ Along Route |
| **EXPLORE** | Intersections | ✅ Full | ✅ Filtered | ✅ Cross-County | ✅ Cross-County | ⚠️ Summary Only | ⚠️ Along Route |
| **EXPLORE** | Ped/Bike | ✅ Full | ✅ Filtered | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate | ✅ Aggregate |
| **EXPLORE** | Analysis → Infrastructure | ✅ Full | ✅ Full | ⚠️ Limited | ⚠️ Limited | ❌ Disabled | ❌ Disabled |
| **EXPLORE** | Analysis → Mapillary | ✅ Full | ✅ Full | ❌ Disabled | ❌ Disabled | ❌ Disabled | ❌ Disabled |
| **EXPLORE** | Analysis → Road Inventory | ✅ Full | ✅ Full | ❌ Disabled | ❌ Disabled | ❌ Disabled | ❌ Disabled |
| **SOLUTIONS** | Countermeasures | ✅ Full | ✅ Full | ❌ Requires County | ❌ Requires County | ❌ Requires County | ❌ Requires County |
| **SOLUTIONS** | Warrant Analyzer | ✅ Full | ✅ Full | ❌ Requires County | ❌ Requires County | ❌ Requires County | ❌ Requires County |
| **SOLUTIONS** | MUTCD AI | ✅ Full | ✅ Full | ✅ Works (Q&A) | ✅ Works (Q&A) | ✅ Works (Q&A) | ✅ Works (Q&A) |
| **SOLUTIONS** | Domain Knowledge | ✅ Full | ✅ Full | ✅ Works (Ref) | ✅ Works (Ref) | ✅ Works (Ref) | ✅ Works (Ref) |
| — | Grants | ✅ Full | ❌ Requires County | ❌ Requires County | ❌ Requires County | ❌ Requires County | ❌ Requires County |
| — | Reports | ✅ Full | ✅ Full | ❌ Requires County | ❌ Requires County | ❌ Requires County | ❌ Requires County |

**Legend:**
- ✅ Full: Tab works fully at this scope
- ✅ Filtered: Same as full but filtered to geographic subset
- ✅ Aggregate: Data aggregated across multiple counties with per-county breakdown
- ✅ Cross-County: Rankings/comparisons across county boundaries
- ⚠️ Limited: Reduced functionality (explanation in relevant section)
- ⚠️ Summary Only: High-level summary only, no individual location detail
- ❌ Disabled: Tab hidden or shows "not available at this scope" message
- ❌ Requires County: User must select a specific county before using this tab

---

## 4. Map Tab — Multi-Jurisdiction Display Plan

### The Core Problem

The Map tab currently renders **individual crash markers** (or clusters/heatmaps) for a single county (~5K-50K crashes). At regional level (10 counties, ~50K-200K crashes) or state level (64 counties, ~140K+ crashes), individual markers are:
- **Too numerous** to render performantly
- **Too dense** to provide useful visual information
- **Too expensive** on memory (each marker is a DOM element or canvas draw)

### Solution: Progressive Detail Rendering

The Map tab uses a **different visualization mode at each jurisdiction level**, chosen automatically based on scope:

```
┌──────────────────────────────────────────────────────────────┐
│                  MAP PROGRESSIVE DETAIL MODEL                  │
│                                                                │
│  STATE LEVEL                                                   │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  CHOROPLETH MAP                                        │   │
│  │  - State outline with county boundaries as polygons    │   │
│  │  - Counties colored by selected metric                 │   │
│  │    (crash rate, EPDO, KA count, trend %)               │   │
│  │  - Region boundaries as thick overlay lines            │   │
│  │  - NO individual crash markers                         │   │
│  │  - Hover county → tooltip with summary stats           │   │
│  │  - Click county → zoom to county (switch to County)    │   │
│  │  - Legend shows color scale + metric selector           │   │
│  └────────────────────────────────────────────────────────┘   │
│                          │ click                               │
│                          ▼                                     │
│  REGION / MPO LEVEL                                            │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  CHOROPLETH + HEATMAP HYBRID                           │   │
│  │  - Region boundary as primary outline                  │   │
│  │  - County boundaries within region as polygons         │   │
│  │  - Counties colored by metric (lighter than state)     │   │
│  │  - OPTIONAL heatmap overlay (toggle on/off)            │   │
│  │  - Top N hotspot locations shown as labeled markers    │   │
│  │    (e.g., top 20 EPDO locations across region)         │   │
│  │  - Hover county → tooltip with county stats            │   │
│  │  - Click county → zoom to county view                  │   │
│  │  - Click hotspot marker → show crash details           │   │
│  │  - Corridor routes highlighted as colored lines        │   │
│  └────────────────────────────────────────────────────────┘   │
│                          │ click                               │
│                          ▼                                     │
│  COUNTY LEVEL (current behavior, enhanced)                     │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  INDIVIDUAL MARKERS / CLUSTERS / HEATMAP               │   │
│  │  - County boundary as outline                          │   │
│  │  - All individual crash markers (existing behavior)    │   │
│  │  - Toggle: Markers / Clusters / Heatmap                │   │
│  │  - Filter: Year, Route, Severity (existing)            │   │
│  │  - Click marker → crash details popup                  │   │
│  │  - NEW: Breadcrumb showing drill-down path:            │   │
│  │    "Colorado > Region 1 > Douglas County"              │   │
│  │  - NEW: "Back to Region View" button                   │   │
│  └────────────────────────────────────────────────────────┘   │
│                          │ click                               │
│                          ▼                                     │
│  CITY / PLACE LEVEL                                            │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  INDIVIDUAL MARKERS (filtered to city boundary)        │   │
│  │  - City boundary as outline                            │   │
│  │  - Same as county but filtered to city geography       │   │
│  │  - "Back to County View" button                        │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  CORRIDOR LEVEL                                                │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  LINEAR ROUTE MAP                                      │   │
│  │  - Corridor route highlighted as thick colored line    │   │
│  │  - County boundaries along corridor as faded polygons  │   │
│  │  - Crash clusters along the route (buffer zone)        │   │
│  │  - Milepost markers (if data available)                │   │
│  │  - Hotspot segments highlighted (high crash density)   │   │
│  │  - Crash rate per mile visualization                   │   │
│  └────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### Map Layer Controls by Jurisdiction Level

| Control | State | Region | County | City | Corridor |
|---------|-------|--------|--------|------|----------|
| **Marker type toggle** (Marker/Cluster/Heatmap) | Hidden | Heatmap only | Full (all 3) | Full (all 3) | Cluster only |
| **Choropleth metric** (Crash Rate/EPDO/KA/Trend) | Shown | Shown | Hidden | Hidden | Hidden |
| **Choropleth group by** (Region/County/TPR) | Shown | Hidden (fixed to county) | Hidden | Hidden | Hidden |
| **Year filter** | Shown | Shown | Shown | Shown | Shown |
| **Route filter** | Hidden | Shown | Shown | Shown | Fixed to corridor route |
| **Severity filter** | Shown | Shown | Shown | Shown | Shown |
| **Boundary toggle** (counties/regions/TPRs) | Shown | Partial (county bounds) | Hidden (single county) | Hidden | Partial |
| **Hotspot markers** (Top N) | Hidden | Shown (Top 20) | N/A (all markers) | N/A | Shown (Top 10 segments) |
| **Geocoder search bar** | Statewide bbox | Region bbox | County bbox | City bbox | Corridor bbox |
| **Corridor highlighting** | All major corridors | Region corridors | County corridors | Hidden | N/A (is the corridor) |

### Map Drill-Down Interaction Model

```
User opens Map at STATE level
  → Sees Colorado choropleth (counties colored by EPDO)
  → Clicks on Douglas County polygon
    → Map zooms to Douglas County bounds
    → Scope selector changes to "County: Douglas"
    → Map switches to individual crash markers
    → Breadcrumb: "Colorado > Region 1 > Douglas County"
  → User clicks "Back to Region 1"
    → Map zooms out to Region 1 bounds
    → Scope selector changes to "CDOT Region: Region 1"
    → Map switches to choropleth + hotspot markers
```

### Map Boundary GeoJSON Loading Strategy

Loading all boundary geometries upfront would be wasteful. Strategy:

| Level | What to Pre-Load | When to Load |
|-------|-----------------|-------------|
| State view | State outline + all 64 county boundaries (simplified) + 5 region boundaries | On state view selection |
| Region view | Region boundary + constituent county boundaries (full detail) | On region selection |
| County view | County boundary (full detail) | Already loaded from region/state parent |
| City view | City/Place boundary (from Census Places) | On city selection (lazy fetch) |
| Corridor view | Corridor route geometry (from OpenStreetMap or CDOT) | On corridor selection |

**GeoJSON Size Management:**
- Simplify county boundaries for state-level view using `turf.simplify()` with tolerance ~0.005
- Full-detail boundaries only loaded when user is at that level
- Total estimated GeoJSON size for Colorado: ~2-3 MB (simplified), ~15-20 MB (full detail)
- Cache in IndexedDB after first load

### Choropleth Color Scales

For state and region views, counties are colored using a **sequential or diverging color scale**:

| Metric | Color Scale | Example |
|--------|------------|---------|
| Total Crashes | Sequential blue (light → dark) | Light blue = low crashes, dark blue = high |
| EPDO Score | Sequential red-orange | Light orange = low, dark red = high |
| KA Crash Count | Sequential red | Light red = few KA, dark red = many |
| Crash Rate (per VMT or per capita) | Sequential green-red | Green = safe, red = dangerous |
| Year-over-Year Trend | Diverging green-red | Green = improving, gray = flat, red = worsening |

### Map Technical Implementation Notes

- **Leaflet Choropleth:** Use `L.geoJSON` with `style` function that reads summary stats and returns fill color
- **Leaflet Hover:** `onEachFeature` with `mouseover` / `mouseout` handlers for tooltip + highlight
- **Leaflet Click Drill-Down:** `click` handler changes scope selector and triggers re-render
- **Heatmap at Region Level:** Use existing Leaflet.heat plugin but with **pre-aggregated centroids** (not individual crash points) for performance — each hotspot location becomes one weighted point
- **Memory:** At state level, ZERO crash point data is loaded. Only JSON summaries + GeoJSON boundaries. This is critical for performance.

---

## 5. Dashboard Tab — View Specifications

### County Level (Current, Enhanced)

No significant changes. Current dashboard shows:
- Total crashes, severity breakdown, EPDO
- Year-over-year trend
- Charts: severity pie, trend line, collision types, weather, light conditions

**Enhancement:** Add context badge: "Douglas County (Region 1, DRCOG MPO)"

### Region Level (New)

```
┌────────────────────────────────────────────────────────────┐
│ REGION 1 DASHBOARD — Denver Metro / Central                 │
│ 10 Counties │ Population: ~3.2M                             │
│                                                              │
│ ┌─ REGION TOTALS ────────────────────────────────────────┐  │
│ │  Total Crashes: 85,234   │  Fatal (K): 312              │  │
│ │  EPDO Score: 621,445     │  Serious (A): 1,089          │  │
│ │  5-Year Trend: ↑ 2.8%   │  KA Rate: 1.64%              │  │
│ └─────────────────────────────────────────────────────────┘  │
│                                                              │
│ ┌─ PER-COUNTY BREAKDOWN TABLE ───────────────────────────┐  │
│ │  County      │ Crashes │ K  │ A   │ EPDO   │ % of Rgn  │  │
│ │──────────────┼─────────┼────┼─────┼────────┼───────────│  │
│ │  Denver      │ 24,521  │ 98 │ 312 │ 182K   │ 29%       │  │
│ │  Arapahoe    │ 18,432  │ 67 │ 245 │ 134K   │ 22%       │  │
│ │  Jefferson   │ 14,876  │ 52 │ 198 │ 108K   │ 17%       │  │
│ │  Adams       │ 12,345  │ 48 │ 156 │  92K   │ 14%       │  │
│ │  Douglas     │  8,234  │ 28 │  98 │  61K   │ 10%       │  │
│ │  Boulder     │  4,567  │ 12 │  52 │  34K   │  5%       │  │
│ │  Broomfield  │    987  │  3 │  12 │   5K   │  1%       │  │
│ │  Clear Creek │    456  │  2 │   8 │   3K   │  1%       │  │
│ │  Gilpin      │    123  │  1 │   3 │   1K   │  <1%      │  │
│ │  Summit      │    693  │  1 │   5 │   2K   │  1%       │  │
│ │──────────────┼─────────┼────┼─────┼────────┼───────────│  │
│ │  TOTAL       │ 85,234  │312 │1089 │ 621K   │ 100%      │  │
│ │  Click any county row to drill down ▶                   │  │
│ └─────────────────────────────────────────────────────────┘  │
│                                                              │
│ ┌─ CHARTS ───────────────────────────────────────────────┐  │
│ │  [County Comparison Bar] [Trend Sparklines] [Severity]  │  │
│ └─────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

**Data Source:** Pre-aggregated region JSON (`data/CDOT/regions/region_1.json`)
**No raw CSV loaded.** Dashboard renders entirely from summary JSON.

### State Level (New)

Same pattern as Region but with per-region breakdown instead of per-county:
- 5-row table (Region 1-5) with totals
- State-level charts with region coloring
- Click region row → drill down to region dashboard

**Data Source:** Pre-aggregated statewide JSON (`data/CDOT/statewide.json`)

---

## 6. Crash Tree Tab — View Specifications

### County Level

No changes. Hierarchical crash pattern breakdown for one county.

### Region Level

- Aggregate crash tree combining all counties in the region
- **New feature:** County attribution column — for each crash tree node, show which counties contribute the most
- Example: "Rear End → Daylight → Dry → 85% from Denver/Arapahoe/Jefferson"
- Data source: Combined county CSVs (loaded on demand) or pre-aggregated tree structure

### State Level

- Statewide crash tree with region attribution
- Example: "Broadside → Dark → Wet → 60% from Region 1, 20% from Region 2"
- Option to toggle between "aggregate tree" and "by-region comparison" view

---

## 7. Safety Focus Tab — View Specifications

### County Level

No changes. Category-based crash analysis (Pedestrian, Bicycle, Speed, Impaired, etc.)

### Region Level

- Aggregate safety focus categories across all counties in region
- **Per-county comparison within each category:**
  - "Pedestrian crashes: Denver 45%, Arapahoe 18%, Douglas 8%..."
- County-level comparison charts for each emphasis area

### State Level

- Statewide emphasis area summary
- Per-region comparison for each category
- Aligned with SHSP (Strategic Highway Safety Plan) emphasis areas
- "Impaired Driving: 18,432 statewide — Region 1: 8,234 (45%)"

---

## 8. Fatal & Speeding Tab — View Specifications

### County Level

No changes. Dedicated severe crash and speed-related analysis.

### Region Level

- Combined fatal/serious injury crashes across all counties
- Fatal crash location map (showing all KA crashes in region)
- Per-county fatal crash comparison table
- Speed-related crash distribution by county

### State Level

- Statewide fatality tracking vs. performance targets
- Region comparison for fatal and speed crashes
- Year-over-year fatality trend by region
- FHWA performance measure reporting alignment

---

## 9. Hot Spots Tab — View Specifications

### County Level

No changes. Ranked dangerous locations within one county.

### Region Level — Cross-County Ranking

This is one of the **most valuable** features of jurisdiction expansion.

```
┌────────────────────────────────────────────────────────────┐
│ REGION 1 — CROSS-COUNTY HOT SPOTS (Top 25)                 │
│                                                              │
│ Rank │ Location                    │ County    │ EPDO  │ K+A│
│──────┼─────────────────────────────┼───────────┼───────┼────│
│  1   │ I-25 & CO-470               │ Douglas   │ 4,231 │ 12 │
│  2   │ I-70 & Wadsworth Blvd       │ Jefferson │ 3,876 │  9 │
│  3   │ Colfax Ave & Federal Blvd   │ Denver    │ 3,654 │  8 │
│  4   │ I-225 & Parker Rd           │ Arapahoe  │ 3,421 │  7 │
│  5   │ US-36 & Table Mesa Dr       │ Boulder   │ 2,987 │  5 │
│  ... │                             │           │       │    │
│      │                             │           │       │    │
│ [Filter: All Routes ▼] [Severity: All ▼] [Type: All ▼]    │
│ [Group By: None ▼]  ← options: None, By County, By Route   │
└────────────────────────────────────────────────────────────┘
```

**Key Design Decision:** Hotspots from different counties are ranked **together** using the same EPDO scoring. This allows a Region engineer to see which intersection in the entire region is worst, regardless of which county it's in.

### State Level — Statewide Hot Spots

- Top 50 statewide hotspot locations
- Grouped by region (default) or ungrouped (flat ranking)
- Useful for CDOT HQ systemic safety analysis
- Filter by crash type, severity, route type

---

## 10. Intersections Tab — View Specifications

### County Level

No changes. Intersection-specific analysis within one county.

### Region Level — Cross-County Intersection Ranking

- All intersections across the region ranked by crash frequency/EPDO
- County column for attribution
- Click intersection → drill down to county-level intersection detail
- **Does NOT require loading all raw CSVs** — uses pre-aggregated hotspot data per county

### State Level — Summary Only

At state level, showing individual intersections is impractical (tens of thousands). Instead:
- Summary statistics: "Total signalized intersections with 5+ crashes: 1,245 statewide"
- Per-region intersection safety summary
- "View region detail" links to drill down

---

## 11. Ped/Bike Tab — View Specifications

### County Level

No changes. Pedestrian and bicycle crash analysis for one county.

### Region Level

- Combined ped/bike crashes across all counties
- Per-county comparison: "Pedestrian crashes per capita: Denver 4.2, Boulder 3.1, Douglas 1.8..."
- Regional pedestrian/bicycle crash density heatmap (if map component is shared)
- Countermeasure recommendations based on aggregate regional patterns

### State Level

- Statewide pedestrian/bicycle safety dashboard
- Per-region comparison
- Alignment with state ped/bike safety action plan
- Urban vs. rural ped/bike crash comparison

---

## 12. Analysis Tab — Infrastructure Assets Sub-Tab

### County/City Level

Full functionality — no changes:
- Upload CSV/Excel of infrastructure assets
- Connect to ArcGIS Feature Service
- School Safety sub-tab
- Transit Safety sub-tab

### Region Level — Limited Functionality

At region level, the Infrastructure Assets sub-tab shows **limited** capabilities:
- **Upload File:** Disabled (assets are location-specific)
- **ArcGIS Service:** Available (user can query regional ArcGIS services)
- **School Safety:** Available with aggregated data (see Section 13)
- **Transit Safety:** Available with aggregated data (see Section 14)

### State Level — Disabled

At state level, the Infrastructure Assets sub-tab is **disabled** with message:

> "Infrastructure asset analysis is available at county and region levels. Please select a specific county or region to use this feature."

**Rationale:** Infrastructure asset analysis requires proximity analysis (crashes near assets). At state level with 140K+ crashes and potentially millions of infrastructure points, this becomes computationally intractable in the browser and provides no actionable insights.

---

## 13. Analysis Tab — School Safety Strategy

### The Challenge

School Safety currently loads school locations using a single `education.leaId` from the county config. This fetches schools from one school district. At regional/state level, multiple school districts must be combined.

### County Level — No Changes

- Load schools using `education.leaId` from county config in `config.json`
- Existing school safety analysis (crash proximity, school day vs. off-hours, etc.)
- County config already has `leaId` and `districtName`

### Region Level — Aggregated School Data

**Data Loading Strategy:**

1. Each county in `hierarchy.json` already has (or will have) its `education.leaId` in `config.json`
2. When user selects Region view and navigates to School Safety:
   - Load school lists for ALL constituent counties' LEA IDs
   - Combine into single school dataset with county attribution
   - Perform proximity analysis using region's combined crash data

```
Region 1 School Safety:
┌────────────────────────────────────────────────────────────┐
│ REGION 1 SCHOOL SAFETY ANALYSIS                             │
│ 10 School Districts │ 847 Schools │ 2,345 Crashes Near Schools│
│                                                              │
│ District             │ Schools │ Crashes │ KA │ High-Risk   │
│──────────────────────┼─────────┼─────────┼────┼─────────────│
│ Denver Public Schools│ 207     │ 876     │ 12 │ 23 schools  │
│ Cherry Creek SD 5    │  67     │ 345     │  5 │  8 schools  │
│ Jeffco Public Schools│ 155     │ 456     │  7 │ 15 schools  │
│ Douglas County SD    │  89     │ 234     │  3 │  5 schools  │
│ Adams 12 Five Star   │  54     │ 198     │  4 │  6 schools  │
│ Boulder Valley SD    │  56     │ 167     │  2 │  4 schools  │
│ ...                  │         │         │    │             │
│                                                              │
│ [Click district to drill into county-level school analysis]  │
└────────────────────────────────────────────────────────────┘
```

**Performance Consideration:**
- Schools are lightweight (name, lat, lon, type) — loading 800 schools across 10 counties is trivial
- Crash proximity analysis for 800 schools × 85K crashes is computationally expensive
- Solution: Pre-compute school proximity crash counts during the monthly data pipeline
  - `data/CDOT/counties/{fips}/school_crashes.json` → pre-computed crash counts per school
  - Region view loads these lightweight JSONs instead of re-computing proximity

### State Level — Disabled

School safety analysis at state level is **disabled**. Rationale:
- 2,000+ schools × 140K+ crashes = intractable browser computation
- No school district administrator operates at the state level
- CDOT HQ interested in school zone safety would use the region view or run a targeted analysis

### Config Requirement

Each county entry in `config.json` must have `education.leaId` populated. For Colorado's 64 counties, this means mapping all Colorado school districts to their respective counties. Some counties share a school district; some counties have multiple districts.

```json
{
  "douglas": {
    "education": {
      "leaId": "0805580",
      "districtName": "Douglas County School District RE-1"
    }
  },
  "denver": {
    "education": {
      "leaIds": ["0803360"],
      "districtName": "Denver Public Schools"
    }
  },
  "arapahoe": {
    "education": {
      "leaIds": ["0804560", "0800870", "0805160"],
      "districtNames": ["Cherry Creek SD 5", "Littleton SD 6", "Englewood SD 1"]
    }
  }
}
```

**Note:** Some counties have multiple school districts. The config should support arrays of LEA IDs for these cases.

---

## 14. Analysis Tab — Transit Safety Strategy

### The Challenge

Transit Safety currently loads transit stops from the National BTS (Bureau of Transportation Statistics) database, filtered by geography. At regional/state level, the number of transit stops increases significantly, especially in urban areas like Denver metro.

### County Level — No Changes

- Load transit stops from BTS API filtered by county bbox
- Existing transit safety analysis (crashes near stops, peak hours, etc.)

### Region Level — Bounded Loading

**Data Loading Strategy:**

1. Use the region's bounding box to query BTS transit stops
2. Load transit stops for ALL counties in the region in one API call
3. Perform proximity analysis with combined regional crash data

**Performance Constraint:**
- Region 1 (Denver metro) may have 5,000-10,000 transit stops (RTD is a major system)
- Loading this many stops is feasible but proximity analysis needs optimization
- Recommendation: Use spatial indexing (e.g., geohash grid) to speed up proximity matching
- Alternative: Pre-compute transit stop crash counts in monthly pipeline

```
Region 1 Transit Safety:
┌────────────────────────────────────────────────────────────┐
│ REGION 1 TRANSIT SAFETY ANALYSIS                            │
│ 3 Transit Agencies │ 8,234 Stops │ 1,567 Crashes Near Stops │
│                                                              │
│ Agency              │ Stops │ Ped Crashes │ Bike │ High-Risk│
│─────────────────────┼───────┼─────────────┼──────┼──────────│
│ RTD (Denver Metro)  │ 7,890 │ 1,234       │  89  │ 45 stops │
│ Via Mobility (Bld.) │   234 │    23        │  12  │  3 stops │
│ Bustang (State)     │   110 │     8        │   2  │  1 stop  │
│                                                              │
│ Transit Zone Analysis:                                       │
│  Peak Hours (6-9 AM, 4-7 PM): 67% of transit-area crashes  │
│  Most dangerous corridor: E Colfax Ave (RTD Route 15)       │
└────────────────────────────────────────────────────────────┘
```

### State Level — Disabled

Transit safety analysis at state level is **disabled**. Rationale:
- Rural counties have few or no transit stops — analysis is meaningless for 40+ of 64 counties
- Urban transit safety is better analyzed at county/region level where transit systems operate
- Loading all transit stops statewide is a large API request for minimal analytical value

---

## 15. Analysis Tab — Mapillary Assets (County/City Only)

### Scope Restriction: County and City/Municipal Level ONLY

Mapillary Assets are **disabled at all levels above county** (region, MPO/TPR, state, corridor).

### Rationale

1. **Tile volume explosion:** Mapillary works by downloading tiles of street-level imagery for traffic sign detection. A county-level area might have 500-2,000 tiles. A region (10 counties) would have 5,000-20,000 tiles. A state would have 50,000+ tiles. This is impractical.

2. **Use case mismatch:** Mapillary asset detection serves **local traffic engineers** who need to inventory signs, signals, and infrastructure in their jurisdiction. A regional planner doesn't need to know where every stop sign is in 10 counties.

3. **API rate limits:** Mapillary API has rate limits that would be exceeded by large-area queries.

4. **Data size:** Downloaded Mapillary asset data for one county is already ~50-200 MB. Multiplying by 10-64x is impractical for browser storage.

### UI Behavior at Region/State/Corridor Level

When user is at any scope above county and clicks the "Mapillary Assets" sub-tab:

```
┌────────────────────────────────────────────────────────────┐
│ MAPILLARY ASSETS                                            │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                                                        │   │
│  │  ⚠️  Mapillary asset analysis is available at         │   │
│  │     county and city levels only.                       │   │
│  │                                                        │   │
│  │     Street-level imagery and traffic sign detection    │   │
│  │     require a focused geographic area for practical    │   │
│  │     analysis and manageable data volumes.              │   │
│  │                                                        │   │
│  │     Current scope: Region 1 (10 counties)              │   │
│  │                                                        │   │
│  │     [Select a County ▼]  to use Mapillary Assets       │   │
│  │                                                        │   │
│  │     Quick select:                                      │   │
│  │     [Douglas] [Denver] [Arapahoe] [Jefferson] ...      │   │
│  │                                                        │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

The quick-select buttons show the counties within the current region/MPO for easy drill-down.

---

## 16. Analysis Tab — Road Inventory (County/City Only)

### Scope Restriction: County and City/Municipal Level ONLY

Road Inventory is **disabled at all levels above county** (region, MPO/TPR, state, corridor).

### Rationale

1. **OSM data volume:** Extracting road network data from OpenStreetMap for a single county produces a manageable dataset (~1,000-10,000 road segments). A state would have 200,000+ segments — too large for in-browser processing and rendering.

2. **Processing time:** Road Inventory includes data enrichment (functional class estimation, missing value imputation, quality scoring). This is CPU-intensive and scales linearly with segment count.

3. **Use case mismatch:** Road inventory data serves **county/city engineers** who need to understand their road network attributes (lanes, speed limits, surface type). Regional planners use the state DOT's official road inventory (e.g., CDOT's OTIS system), not OSM extraction.

4. **Map rendering:** Rendering 200K road segments on a Leaflet map would be extremely slow and visually unusable.

### UI Behavior at Region/State/Corridor Level

Same pattern as Mapillary — show disabled message with county quick-select buttons:

```
┌────────────────────────────────────────────────────────────┐
│ ROAD INVENTORY                                              │
│                                                              │
│  ⚠️  Road inventory extraction is available at              │
│     county and city levels only.                             │
│                                                              │
│     OpenStreetMap road network extraction and analysis       │
│     require a focused geographic area. For regional/state    │
│     road inventory data, use CDOT's OTIS system:             │
│     https://dtdapps.coloradodot.info/otis                   │
│                                                              │
│     [Select a County ▼]  to use Road Inventory               │
└────────────────────────────────────────────────────────────┘
```

**Bonus recommendation:** At region/state level, provide a link to the state DOT's official road inventory system (CDOT OTIS, VDOT RNS, etc.) as a helpful redirect.

---

## 17. Solutions Section — No Expansion (Single-Location Only)

### Countermeasures Tab

**Remains single-location.** User must select a specific intersection to analyze crash patterns and find matching CMFs.

When user navigates to Countermeasures while at region/state scope:

```
┌────────────────────────────────────────────────────────────┐
│ COUNTERMEASURES / CMF ANALYSIS                              │
│                                                              │
│  Current scope: Region 1 (10 counties)                      │
│                                                              │
│  Countermeasure analysis requires a specific location.       │
│  Select a location to begin:                                │
│                                                              │
│  Option 1: [Select from Region Hot Spots ▶]                │
│            (Opens hot spots ranked across Region 1)          │
│                                                              │
│  Option 2: [Select a County ▼] then choose a location       │
│                                                              │
│  Option 3: [Search by route/intersection name...]           │
│            (Searches across all Region 1 counties)           │
└────────────────────────────────────────────────────────────┘
```

**Key UX pattern:** Don't make the user go back to change scope. Instead, provide inline location selection that narrows to a county automatically.

### Warrant Analyzer Tab

Same pattern as Countermeasures. Requires specific intersection.

### MUTCD AI Tab

Works at any level — it's knowledge-based Q&A, not data-dependent. No changes needed.

### Domain Knowledge Tab

Works at any level — it's reference material. No changes needed.

---

## 18. Grants Section — No Expansion (Single-Jurisdiction Only)

### Why Grants Stay Per-County

HSIP (Highway Safety Improvement Program) applications are submitted by **individual counties or agencies**, not by regions or the state. The grant ranking/scoring system evaluates locations within one jurisdiction. A "regional grant view" doesn't exist in practice.

### UI Behavior at Region/State Level

```
┌────────────────────────────────────────────────────────────┐
│ GRANTS — HSIP PROJECT IDENTIFICATION                        │
│                                                              │
│  Current scope: Region 1 (10 counties)                      │
│                                                              │
│  HSIP grant analysis is performed per county.                │
│  Select a county to view its HSIP-eligible locations:        │
│                                                              │
│  [Douglas County]  [Denver]  [Arapahoe]  [Jefferson]        │
│  [Adams]  [Boulder]  [Broomfield]  [Clear Creek]            │
│  [Gilpin]  [Summit]                                         │
│                                                              │
│  💡 Tip: Use the Hot Spots tab (under Explore) to see       │
│  dangerous locations ranked across the entire region.        │
└────────────────────────────────────────────────────────────┘
```

### Future Enhancement (Not in Current Scope)

A potential future enhancement could show "Regional HSIP Portfolio" — which counties in a region have submitted HSIP applications and how the region's safety funding is allocated. This would be a separate feature, not part of the jurisdiction expansion scope.

---

## 19. Reports Section — No Expansion (Single-Jurisdiction Only)

### Why Reports Stay Per-County

Reports are generated with jurisdiction-specific headers, branding, and data. A "Douglas County Crash Report" is a fundamentally different document from a "Region 1 Crash Report."

### UI Behavior at Region/State Level

```
┌────────────────────────────────────────────────────────────┐
│ REPORTS                                                      │
│                                                              │
│  Current scope: Region 1 (10 counties)                      │
│                                                              │
│  Report generation is available per county.                  │
│  Select a county to generate its crash report:               │
│                                                              │
│  [Select County ▼]                                          │
│                                                              │
│  Available report types:                                     │
│  • County Crash Summary Report                               │
│  • Intersection Safety Report                                │
│  • Pedestrian/Bicycle Safety Report                          │
│  • HSIP Project Justification Report                         │
└────────────────────────────────────────────────────────────┘
```

### Future Enhancement (Not in Current Scope)

A "Region Summary Report" or "Statewide Safety Performance Report" could be added later as a separate report type. This would use pre-aggregated JSON data and produce a different report format than county-level reports.

---

## 20. Complete View Adaptation Matrix

### Detailed Behavior for Every Tab × Every Level

This is the definitive reference for how each tab behaves at each jurisdiction level.

#### EXPLORE — Dashboard

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Data source** | `crashState.aggregates` | Pre-aggregated region JSON | Pre-aggregated statewide JSON |
| **Header** | "Douglas County" | "CDOT Region 1 — 10 Counties" | "Colorado Statewide — 64 Counties" |
| **KPI cards** | Single county totals | Region aggregate totals | State aggregate totals |
| **Breakdown table** | By route (existing) | By county (new) | By region (new) |
| **Charts** | Single county charts | County comparison + region aggregate | Region comparison + state aggregate |
| **Drill-down** | Click route → Map | Click county → County Dashboard | Click region → Region Dashboard |
| **Raw CSV needed?** | Yes (existing) | No (uses JSON summary) | No (uses JSON summary) |

#### EXPLORE — Map

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Visualization** | Markers/Clusters/Heatmap | Choropleth + Heatmap + Top-N markers | Choropleth only |
| **Boundaries** | County outline | County polygons + region outline | County polygons + region outlines |
| **Crash points** | All individual crashes | Top 20 hotspot centroids | None (choropleth only) |
| **Controls** | Year/Route/Severity + marker toggle | Year/Severity + metric selector | Year/Severity + metric + group-by |
| **Geocoder bbox** | County bbox | Region bbox | State bbox |
| **Drill-down** | Click marker → details | Click county → County Map | Click county/region → zoom in |
| **Raw CSV needed?** | Yes (for markers) | No (JSON summaries + GeoJSON) | No (JSON summaries + GeoJSON) |

#### EXPLORE — Crash Tree

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Tree data** | County crashes | Combined county crashes | Pre-aggregated state tree |
| **Attribution** | None needed | County column on each node | Region column on each node |
| **Drill-down** | Expand nodes | Click county → County Crash Tree | Click region → Region Crash Tree |
| **Raw CSV needed?** | Yes | Yes (lazy-loaded per county) | No (pre-aggregated) |

#### EXPLORE — Safety Focus

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Categories** | Same categories | Same categories + county comparison | Same categories + region comparison |
| **Data scope** | County crashes by category | Region aggregate + per-county breakdown | State aggregate + per-region breakdown |
| **SHSP alignment** | N/A | Optional | Primary view (emphasis areas) |
| **Raw CSV needed?** | Yes | Yes (lazy-loaded) | No (pre-aggregated) |

#### EXPLORE — Fatal & Speeding

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Fatal map** | County fatal locations | Region fatal locations (all counties) | Regional choropleth (fatals per county) |
| **Speed analysis** | County speed crashes | Region speed comparison by county | State speed trends by region |
| **Performance targets** | N/A | Regional targets | State targets (FHWA PM1) |
| **Raw CSV needed?** | Yes | Partial (fatal/speed only) | No (pre-aggregated) |

#### EXPLORE — Hot Spots

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Ranking** | Within county | Cross-county (all counties ranked together) | Cross-county (statewide Top 50) |
| **County column** | Hidden (single county) | Shown (identifies which county) | Shown + Region column |
| **Group-by** | None / Route | None / County / Route | Region / County / Route |
| **Data source** | `crashState.aggregates.byRoute` | Pre-aggregated hotspot JSONs per county | Pre-aggregated hotspot JSONs |
| **Raw CSV needed?** | No (uses aggregates) | No (uses hotspot JSONs) | No (uses hotspot JSONs) |

#### EXPLORE — Intersections

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Intersection list** | All county intersections | Cross-county intersection ranking | Summary only (counts, top 10) |
| **Detail level** | Full (crash history, pattern, etc.) | Click to drill to county detail | Region-level summary only |
| **Raw CSV needed?** | Yes | Partial (hotspot JSONs + on-demand) | No |

#### EXPLORE — Ped/Bike

| Aspect | County | Region | State |
|--------|--------|--------|-------|
| **Analysis** | County ped/bike crashes | Region aggregate + county comparison | State aggregate + region comparison |
| **Density map** | County heatmap of ped/bike | Region heatmap (combined) | Choropleth (ped/bike rate by county) |
| **Raw CSV needed?** | Yes | Yes (lazy-loaded) | No (pre-aggregated) |

#### EXPLORE — Analysis Sub-Tabs

| Sub-Tab | County | Region | State |
|---------|--------|--------|-------|
| **Infrastructure Assets** | Full | Limited (ArcGIS + School + Transit) | Disabled |
| **School Safety** | Full (single LEA) | Aggregated (multiple LEAs) | Disabled |
| **Transit Safety** | Full (county bbox) | Aggregated (region bbox) | Disabled |
| **Mapillary Assets** | Full | Disabled | Disabled |
| **Road Inventory** | Full | Disabled | Disabled |

---

## 21. Boundary & GeoJSON Loading Strategy

### Design Philosophy: Live APIs Over Static Files

The original design assumed static GeoJSON files for all boundaries. With TIGERweb (already integrated) and BTS NTAD APIs available, **most boundaries should be fetched live from federal APIs** rather than maintained as static files. This eliminates manual boundary file creation when onboarding new states and ensures boundaries stay current.

**API Sources for Boundaries:**

| Boundary Type | API Source | Why Not the Other? |
|---------------|-----------|-------------------|
| State outline | **TIGERweb** layer 80 | Census is authoritative source for state boundaries |
| County boundaries | **TIGERweb** layer 82 | Already using this. Census is authoritative |
| County subdivisions | **TIGERweb** layer 22 | Already using this (`countySubdivisionsUrl`) |
| City/Place boundaries | **TIGERweb** layer 28 + 30 | Census Incorporated Places + CDPs |
| Census Tracts | **TIGERweb** layer 8 | For equity analysis overlays |
| Urban Areas | **TIGERweb** layer 88 | For urban/rural classification |
| School Districts | **TIGERweb** layer 14/16/18 | Boundary complement to LEA ID school locations |
| MPO/TPR boundaries | **BTS NTAD** MPO API | TIGERweb does NOT have MPO layers — MPOs are DOT planning designations, not Census geographies |
| DOT Regions (CDOT/VDOT) | **Static GeoJSON** | State DOT regions are custom administrative boundaries — no federal API |
| Corridors | **Static GeoJSON** | Route geometries from state DOT or OSM |

### Static Files (Reduced — Only DOT-Specific Boundaries)

```
data/CDOT/boundaries/
├── regions.geojson                 # ~100 KB — 5 CDOT region boundaries (static — no federal API)
└── corridors/                      # Major route geometries
    ├── I-25.geojson
    ├── I-70.geojson
    └── .../
```

> **Why so few static files?** County boundaries, state outlines, city/place boundaries, census tracts, urban areas, and school districts are all available from TIGERweb. MPO boundaries come from BTS NTAD. Only CDOT-specific region boundaries and corridor route geometries have no federal API source. This means onboarding a new state requires only **one static GeoJSON file** (the state DOT's region/district boundaries) plus the corridor routes.

### Loading Sequence (Updated with API Sources)

| User Action | Boundary Data Source | API / File | Estimated Size |
|------------|---------------------|-----------|----------------|
| Opens state view | State outline | TIGERweb layer 80: `STATE='08'` | ~50 KB |
| | All 64 county boundaries (simplified) | TIGERweb layer 82: `STATE='08'` + simplify client-side | ~500 KB |
| | 5 CDOT region outlines | Static `regions.geojson` | ~100 KB |
| Selects Region 1 | 10 county boundaries (full detail) | TIGERweb layer 82: `STATE='08' AND COUNTY IN (...)` | ~300 KB |
| Drills into Douglas County | County boundary | Already cached from Region step | 0 additional |
| | City/Place boundaries | TIGERweb layer 28: `STATE='08' AND COUNTY='035'` | ~40 KB |
| | Census tracts (for equity overlay) | TIGERweb layer 8: `STATE='08' AND COUNTY='035'` | ~80 KB |
| Selects TPR/MPO view | All CO MPO boundaries | BTS NTAD MPO API: `STATE='CO'` | ~200 KB |
| Selects specific MPO (DRCOG) | DRCOG polygon | Cached from previous step | 0 additional |
| Selects corridor I-25 | Corridor route geometry | Static `corridors/I-25.geojson` | ~50 KB |
| Toggles Urban Areas overlay | Urban area polygons | TIGERweb layer 88: spatial query within state bbox | ~150 KB |
| Toggles School Districts overlay | School district boundaries | TIGERweb layer 14: `STATE='08'` | ~200 KB |
| Onboards new state (e.g., Texas) | All TX boundaries | TIGERweb + BTS NTAD — zero manual GeoJSON needed | Auto |

### TIGERweb Query Pattern (Same as Existing Implementation)

All boundary layers use the identical ArcGIS REST query pattern already implemented for county boundaries in `addJurisdictionBoundaryLayer()`:

```javascript
// Generic boundary loader — works for any TIGERweb layer
async function loadTigerWebBoundary(layerId, whereClause, outFields) {
    const apiUrl = `${tigerwebConfig.baseUrl}/${layerId}/query?` +
        `where=${encodeURIComponent(whereClause)}` +
        `&outFields=${outFields}` +
        `&returnGeometry=true&outSR=4326&f=geojson`;
    const response = await fetch(apiUrl);
    return await response.json();
}

// State outline
loadTigerWebBoundary(80, "STATE='08'", "NAME,STATE,GEOID");

// All places in Douglas County
loadTigerWebBoundary(28, "STATE='08' AND COUNTY='035'", "NAME,PLACEFP,LSAD");

// Census tracts for equity analysis
loadTigerWebBoundary(8, "STATE='08' AND COUNTY='035'", "NAME,TRACT,GEOID");

// Urban areas (spatial query for state bbox)
loadTigerWebBoundary(88, "1=1", "NAME10,UATYP10,GEOID10");  // + geometry envelope
```

### BTS NTAD MPO Boundary Service

For MPO/TPR boundaries — **NOT available from TIGERweb** (MPOs are DOT planning designations, not Census geographies):

**API Endpoint:**
```
https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0/query
```

| Property | Value |
|----------|-------|
| **Scope** | National — all US MPOs |
| **Geometry** | `esriGeometryPolygon` — official MPO boundary polygons |
| **Key Fields** | `MPO_ID`, `MPO_NAME`, `ACRONYM`, `STATE`, `POP`, `DESIGNATION_DATE` |
| **Update Frequency** | Quarterly (BTS NTAD schedule) |
| **Cost** | Free, no authentication |

**Loading Strategy for MPO Boundaries:**

| User Action | Data Source | Fallback |
|------------|------------|----------|
| Opens state view | Static `regions.geojson` (CDOT regions) | Always available |
| Selects TPR/MPO view | **BTS MPO API** → query `?where=STATE='CO'&f=geojson` | Generate from county boundaries |
| Selects specific MPO (e.g., DRCOG) | **BTS MPO API** → query `?where=ACRONYM='DRCOG'&f=geojson` | Cached from previous query |
| Onboards new state | **BTS MPO API** → auto-discover all MPOs for that state | Manual GeoJSON creation |

### Caching Strategy

- **TIGERweb boundaries:** Cache in IndexedDB with **annual** expiration (Census boundaries change only on decennial or annual BAS updates)
- **BTS MPO boundaries:** Cache in IndexedDB with **quarterly** expiration (aligned with NTAD updates)
- **Static GeoJSON** (regions, corridors): Cache in IndexedDB, invalidate when `hierarchy.json` version changes
- All cached boundaries are loaded instantly on repeat visits — API calls only on first visit or cache expiration
- State-level view requires ~650 KB total on first load — instant on any connection after caching

---

## 22. Spatial Accuracy: Bbox-Fetch + Polygon-Clip

### Problem Statement

All external spatial API calls (BTS Transit, Overpass/OSM, Mapillary) use **bounding box envelopes** — rectangular queries that always include area outside the actual jurisdiction polygon. For irregularly-shaped jurisdictions, this means 15-60% of returned data belongs to **neighboring jurisdictions**.

This is especially problematic for tabs that display counts or statistics (Road Inventory mileage, transit stop counts, signal counts), where bbox overshoot directly inflates reported numbers.

### Solution: Client-Side Polygon Clipping

**Strategy:** Keep the bbox for the API query (fast, universally supported), then clip results client-side using the actual TIGERweb polygon that is already fetched and cached by `addJurisdictionBoundaryLayer()`.

### How It Applies to Each Tab

| Tab | Data Source | Bbox API | Clip Method | Impact |
|-----|------------|----------|-------------|--------|
| **Transit** (Analysis) | BTS National Transit Map | `esriGeometryEnvelope` | `pointInFeature()` per stop | Removes stops from neighboring counties |
| **Road Inventory** (Analysis) | Overpass/OSM | `bbox` in Overpass QL | `turf.booleanIntersects()` per way | Removes road segments outside boundary |
| **School Safety** (Analysis) | NCES/CCD API | Potential bbox query | `pointInFeature()` per school | Removes schools outside boundary |
| **Infrastructure Assets** (Analysis) | Mapillary Graph | `bbox` parameter | `pointInFeature()` per feature | Removes signs/features outside boundary |
| **Mapillary Assets** (Analysis) | Mapillary Graph | `bbox` parameter | `pointInFeature()` per feature | Removes assets outside boundary |
| **Map Tab** (signals, roads) | Overpass/OSM | `bbox` in Overpass QL | `pointInFeature()` for nodes, `turf.booleanIntersects()` for ways | Clean signal/road display |

### Required Loading Sequence

```
1. User selects jurisdiction
       │
       ├──► Fetch TIGERweb polygon (if not cached)     ──► Cache in geojsonCache
       │         [addJurisdictionBoundaryLayer()]
       │
       └──► Fetch data from bbox API (in parallel)      ──► Raw results
                  [transitTryStatewideData(), etc.]
       │
       ▼
2. BOTH responses received
       │
       ▼
3. Clip raw results to polygon
       │   pointInFeature() for points
       │   turf.booleanIntersects() for lines
       │   turf.intersect() for polygons
       │
       ▼
4. Display clipped results (accurate to jurisdiction boundary)
```

**Key dependency:** Steps 1a and 1b run in **parallel** (no waiting). Step 3 waits for both. If polygon fetch fails, fallback to unclipped bbox results with a UI warning.

### Existing Codebase Infrastructure

All required components **already exist** in `index.html`:

- `turf.js` loaded at line 52
- `pointInFeature(lng, lat, feature)` at line 113584 — handles Polygon + MultiPolygon with bbox pre-filter
- `computeFeatureBoundingBox(feature)` at line 113541 — caches bbox for fast rejection
- `builtInLayersState.jurisdictionBoundary.geojsonCache[id]` — stores TIGERweb county polygon
- `computeDistrictCrashStatistics()` at line 113617 — already uses this exact pipeline (100K+ crash points assigned to district polygons)

### Cross-Reference

Full technical specification including `SpatialClipService` module design, three-tier clipping strategy, performance benchmarks, and integration points: **See Part 1, Section 16 — Spatial Accuracy: Bbox-Fetch + Polygon-Clip Strategy**.

---

## 23. Progressive Detail Rendering Philosophy

### The Core Principle

**The higher the jurisdiction level, the more abstract the visualization.**

```
STATE LEVEL:
  → Polygons (choropleth), no individual data points
  → Pre-computed aggregates only (JSON)
  → Zero raw CSV data in memory
  → Everything renders in <1 second

REGION LEVEL:
  → Polygons + limited data points (Top N hotspots)
  → Mix of pre-computed JSON + on-demand loading
  → Optional heatmap from aggregated centroids
  → Renders in 1-3 seconds

COUNTY LEVEL:
  → Individual data points (markers, clusters, heatmap)
  → Full raw CSV in memory
  → All existing functionality preserved
  → Current rendering performance (unchanged)

CITY LEVEL:
  → Filtered subset of county data
  → Faster than county (fewer points)
```

### Why This Matters for Performance

| Level | Crashes in Scope | Rendering Approach | Memory Usage |
|-------|------------------|--------------------|--------------|
| State (64 counties) | ~142,000 | Choropleth from 64 summary values | ~200 KB (JSON) |
| Region (10 counties) | ~85,000 | Choropleth from 10 summaries + 20 hotspot markers | ~300 KB (JSON) |
| County (1 county) | ~8,000-25,000 | Individual markers / clusters / heatmap | ~15-50 MB (CSV) |
| City (within county) | ~2,000-8,000 | Filtered markers | Same as county (subset) |

The **100x+ memory difference** between state and county views is why progressive detail is essential. Loading 64 county CSVs simultaneously would require 1-3 GB of browser memory — unacceptable.

---

## 24. Scope Selector Interaction with Non-Explore Tabs

### Problem

When user is at Region/State scope and clicks on a Solutions/Grants/Reports tab (which requires county-level selection), what happens?

### Solution: Inline Scope Narrowing

The Solutions/Grants/Reports tabs do NOT change the scope selector. Instead, they show an inline "select county" prompt that:

1. Lists counties available in the current scope (e.g., the 10 counties in Region 1)
2. Provides a search box for quick finding
3. Once user selects a county, the tab loads with that county's data
4. The main scope selector remains at the region/state level
5. A breadcrumb shows: "Region 1 > Douglas County > Countermeasures"
6. A "Back to Region" link returns to the Explore section at the region level

### State Persistence

- If user selects "Douglas County" in Countermeasures, then switches to Warrants, Douglas County should still be pre-selected
- This is stored in a temporary `solutionsScopeCounty` state variable
- It resets if the user changes the main scope selector

```
┌── Scope Selector ────────────────────────────────────────┐
│  State: [Colorado ▼]   View: [CDOT Region ▼] [Region 1]  │
└───────────────────────────────────────────────────────────┘

When in Solutions tab:
┌── Tab Content ───────────────────────────────────────────┐
│  COUNTERMEASURES                                          │
│  ┌─ County Context ─────────────────────────────────┐    │
│  │  Analyzing: Douglas County (within Region 1)      │    │
│  │  [Change County ▼]                                │    │
│  └───────────────────────────────────────────────────┘    │
│                                                           │
│  [Normal countermeasures interface for Douglas County]     │
└───────────────────────────────────────────────────────────┘
```

---

## 25. Performance Constraints by Jurisdiction Level

### Memory Budget

| Level | Max CSV in Memory | JSON Summaries | GeoJSON | Total Target |
|-------|-------------------|----------------|---------|-------------|
| County | 1 CSV (~15-50 MB) | 1 county summary (~5 KB) | County boundary (~30 KB) | ~50 MB max |
| Region | 0-3 CSVs (on demand) | Region + N county summaries (~50 KB) | Region + county boundaries (~400 KB) | ~150 MB max |
| State | 0 CSVs | State + 5 region + 64 county summaries (~400 KB) | State + simplified counties (~650 KB) | ~5 MB max |

### Key Constraint: State View Must Be Instant

At state level, the dashboard and map must render **immediately** from pre-computed JSON. Zero raw CSV processing. If a user selects "Statewide" and has to wait 30 seconds for 64 CSVs to load, the tool is unusable.

This is why the monthly data pipeline must produce:
- `statewide.json` (~20 KB)
- `regions/region_N.json` (~10 KB each, 50 KB total)
- `counties/{fips}/summary.json` (~5 KB each, 320 KB total)

Total: **~390 KB** for full statewide dashboard rendering. Instant on any connection.

---

## 26. Amendments to Part 1

The following corrections should be applied to `JURISDICTION_EXPANSION_PLAN.md` (Part 1):

### Amendment 1: Section 10, Adaptive Behavior Table (Line 1044-1051)

**Remove** the CMF and Grants rows from the multi-jurisdiction adaptation table. Replace with:

| Feature | County View | Region View | State View |
|---------|------------|-------------|------------|
| Dashboard | Single county stats | Region aggregate + per-county breakdown | Statewide + per-region breakdown |
| Map | County boundary, all crashes | Choropleth + heatmap + Top-N markers | Choropleth (counties colored by metric) |
| Hotspots | Top locations in county | **Cross-county ranking** across region | **Statewide Top 50**, grouped by region |
| Crash Tree | County tree | Aggregate tree with county attribution | State tree with region attribution |
| Safety Focus | County categories | Region aggregate with county comparison | State emphasis areas by region |
| Intersections | County intersections | Cross-county intersection ranking | Summary only |
| Ped/Bike | County ped/bike | Region aggregate + county comparison | State aggregate + region comparison |
| AI Assistant | County context | Region context | Statewide context |
| **CMF** | **County only** | **Requires county selection** | **Requires county selection** |
| **Grants** | **County only** | **Requires county selection** | **Requires county selection** |
| **Analysis** | **Full** | **Limited (no Mapillary/Road Inv)** | **Disabled** |

### Amendment 2: Section 12, Regional Map (Line 1292-1296)

**Enhance** with progressive detail rendering strategy and layer control specifications from Section 4 of this document.

### Amendment 3: Add Note to Section 6 (Hierarchical Jurisdiction Model)

Add a note after line 335:

> **SCOPE LIMITATION:** The jurisdiction hierarchy applies to the EXPLORE section only. Solutions, Grants, and Reports tabs always operate at county level. When the user is at a higher scope and navigates to these tabs, they are prompted to select a specific county within the current scope. See Part 2, Section 2 for detailed rationale.

### Amendment 4: Phase 3 Task Additions (Line 2016-2032)

Add these tasks to Phase 3:

| # | Task | Effort | Dependencies |
|---|------|--------|--------------|
| 3.10 | Implement tab-level scope restrictions (disable Mapillary/Road Inventory at region+) | 2 days | 2.5 |
| 3.11 | Implement inline county selector for Solutions/Grants/Reports at higher scopes | 3 days | 2.5 |
| 3.12 | Build progressive detail rendering for Map tab (choropleth at state, hybrid at region) | 5 days | 3.3, 3.4, 3.5 |
| 3.13 | Pre-compute school proximity crash counts in data pipeline | 2 days | 2.4 |

---

## Summary

This document resolves the five gaps identified in Part 1:

1. **Map Tab Display** — Fully specified with progressive detail rendering (choropleth → hybrid → markers), layer controls per level, drill-down interaction model, boundary loading strategy, and choropleth color scales.

2. **School & Transit Data** — County level unchanged. Region level aggregates multiple LEA IDs / BTS bounding boxes. State level disabled. Pre-computed proximity data recommended for performance.

3. **Mapillary & Road Inventory** — Explicitly restricted to county/city/municipal level. Disabled with helpful messages and county quick-select buttons at higher levels. Clear rationale documented (tile volume, API limits, use case mismatch).

4. **Explore-Only Expansion** — Solutions, Grants, and Reports are definitively excluded from jurisdiction expansion. Inline county selection provided when users navigate to these tabs at higher scopes. Part 1 amendments specified.

5. **Per-Tab View Specifications** — Complete view adaptation matrix covering all 13+ tabs × 6 jurisdiction levels with specific data source, visualization type, controls, and performance characteristics.

---

*This document should be read alongside `JURISDICTION_EXPANSION_PLAN.md` (Part 1). Part 1 covers architecture, data model, hierarchy, infrastructure, and phased roadmap. Part 2 covers tab-level behavior, scope restrictions, and visual design specifications.*
