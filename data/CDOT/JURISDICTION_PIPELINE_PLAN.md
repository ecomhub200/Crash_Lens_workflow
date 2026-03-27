# Jurisdiction Pipeline Plan: Federal → State → MPO → County/City

## Executive Summary

This plan defines how to extend CRASH LENS from its current 2-jurisdiction setup (Henrico County VA + Douglas County CO) to a multi-tier system supporting **Federal, State, and MPO** views — while keeping the **existing County/City view completely untouched**. All new views build on pre-computed aggregate data loaded from R2, using the existing pipeline infrastructure.

**Scope:** Federal, State, and MPO tiers only. County view remains as-is.

---

## 1. Current State Assessment

### What We Have Today

| Asset | Details |
|-------|---------|
| **States** | Virginia (133 jurisdictions in config.json), Colorado (64 counties in jurisdictions.json) |
| **Active Data** | Henrico County VA (34 MB all_roads), Douglas County CO (18 MB all_roads) |
| **R2 Bucket** | `crash-lens-data/` with `{state}/{jurisdiction}/{file}.csv` structure |
| **Pipelines** | VA: `download_crash_data.py` (ArcGIS API — full state download, replaces previous), CO: `download_cdot_crash_data.py` (OnBase — updated year only, replaces previous) |
| **Validation** | Full system with auto-correction, geocoding, spatial validation |
| **Forecasting** | SageMaker Chronos-2 integration |
| **UI** | Single-jurisdiction view with Upload Data tab (state/jurisdiction/road-type dropdowns) |
| **Map Asset Layers** | Mapillary (coverage, signs, features), TIGERweb (county boundary, magisterial districts), BTS/NTAD (AADT, bridges, railroad crossings, transit stops, transit routes), user-uploaded assets |
| **State Adapter** | `scripts/state_adapter.py` — normalizes to VDOT format |

### What Each New Tier Needs

| Tier | User Persona | What They Need |
|------|-------------|---------------|
| **Federal** | FHWA analysts, NHTSA researchers | Cross-state comparisons, national safety trends, HSIP performance |
| **State** | VDOT HQ, CDOT HQ, State Safety Engineers | Statewide crash totals, county rankings, regional comparisons, road type filtering |
| **MPO** | DRCOG, Hampton Roads TPO, FAMPO planners | Multi-county aggregation within planning boundary, cross-county hotspot ranking, road type filtering |

---

## 2. Data Pipeline Architecture

### 2.1 State-Specific Download Strategies

**Each state has fundamentally different data sources and update patterns. The pipeline must respect these differences.**

| State | Download Strategy | What Gets Replaced | Frequency |
|-------|-------------------|-------------------|-----------|
| **Virginia** | Full statewide CSV download from Virginia Roads ArcGIS API | Entire dataset replaced each run | Monthly (1st Monday) |
| **Colorado** | Updated year's Excel from CDOT OnBase portal | Only the new/updated year file replaces the previous version of that year | Monthly (1st of month) |

#### Virginia Pipeline (Replace-All Strategy)

```
download_crash_data.py --all-jurisdictions
    │
    ├─ 1. Download full statewide CSV (~500 MB) from Virginia Roads
    │     (replaces any previous statewide file entirely)
    │
    ├─ 2. Split into 133 jurisdictions by jurisCode
    │     Each jurisdiction's CSV replaces previous version
    │
    ├─ 3. Validate + Geocode (batch, shared cache)
    │
    ├─ 4. Generate road-type variants per county
    │     county_roads.csv, no_interstate.csv, all_roads.csv
    │
    ├─ 5. Upload all county CSVs to R2 (overwrite)
    │
    └─ 6. Generate state + MPO aggregates → Upload to R2
```

#### Colorado Pipeline (Year-Update Strategy)

```
download_cdot_crash_data.py --latest
    │
    ├─ 1. Download latest year's Excel from OnBase
    │     (only the updated year, e.g., 2025.xlsx replaces previous 2025.xlsx)
    │
    ├─ 2. Merge updated year into county's multi-year dataset
    │     Existing prior years remain untouched
    │
    ├─ 3. Process through 5-stage pipeline per county
    │     Stage 1: Standardize → Stage 2: Validate → Stage 3: Geocode
    │     → Stage 4: Split by road type → Stage 5: Forecast
    │
    ├─ 4. Upload county CSVs to R2 (overwrite)
    │
    └─ 5. Generate state + MPO/region aggregates → Upload to R2
```

#### Key Design Rule: No Historical Snapshots of Raw Data

Since both states use a **replace** strategy (VA replaces entire dataset, CO replaces the updated year), we do NOT store monthly snapshots of raw CSVs. The data in R2 is always the **current** version. Historical trend analysis comes from the crash dates within the dataset itself, not from multiple snapshot files.

What we DO archive monthly: the small aggregate JSONs (`aggregates.json`, `county_summary.json`) so we can track how our aggregated metrics change over time (e.g., did a county's crash count go up after a data correction?).

```
R2 Archive (aggregates only, not raw CSVs):
  {state}/_statewide/
    aggregates.json              ← Current
    county_summary.json          ← Current
    snapshots/
      2026-01_aggregates.json    ← Monthly aggregate archive (~200 KB each)
      2026-02_aggregates.json
```

### 2.2 Dataset Size Reduction Strategy

**Current sizes:**
| Dataset | Files | Total Size |
|---------|-------|------------|
| Colorado / Douglas (4 CSVs) | standardized + 3 road types | 57.9 MB |
| Virginia / Henrico (3 CSVs) | 3 road types | 76.5 MB |
| **Total** | | **134.4 MB** |

At scale: 133 VA jurisdictions × 3 road types × ~25 MB avg = **~10 GB**. Transfer size reduction is critical.

**Decision: Keep all columns. Use Gzip compression on R2 only.**

All 112 columns (including the 26 currently unused by the UI) are preserved in the CSV. These columns may be needed by future features (e.g., Deep Dive panels for new states, research exports, AI analysis context). Dropping them saves ~15% on disk but creates fragility — any new feature that needs `_co_tu2_speed_limit` or `_co_nm1_age` would require a full re-download from the source.

#### Gzip/Brotli Compression on R2 (Savings: ~70-80% transfer)

CSV data compresses extremely well due to repetitive categorical values and whitespace. R2 can serve pre-compressed files that the browser decompresses transparently.

**How it works:**
1. Pipeline uploads `county_roads.csv.gz` to R2 with `Content-Encoding: gzip`
2. Browser requests the file normally
3. R2 serves the compressed version; browser decompresses automatically
4. **No user-side impact** — the browser handles gzip/brotli decompression natively. All modern browsers support it. The user sees no difference except faster load times.

**Action:** Configure R2 upload script to compress CSVs before upload. Set `Content-Encoding: gzip` and `Content-Type: text/csv` headers.

**User-Side Impact Assessment:**

| Concern | Impact | Details |
|---------|--------|---------|
| Browser compatibility | None | All modern browsers (Chrome, Firefox, Safari, Edge) handle gzip transparently since ~2005 |
| JavaScript processing | None | `fetch()` and `XMLHttpRequest` auto-decompress. Papa Parse receives the same uncompressed CSV string |
| Data accuracy | None | Gzip is lossless compression. Bit-for-bit identical after decompression |
| Memory usage | None | Same in-memory footprint after decompression |
| Load time | Improved | ~70-80% less data over the network = significantly faster loads |
| Offline/cache | No change | Browser caches the decompressed version in its HTTP cache |

**Projected sizes at Virginia scale (133 jurisdictions):**

| Metric | Raw CSV | Gzipped Transfer | Savings |
|--------|---------|-------------------|---------|
| Per-county CSV | ~25 MB avg | ~5-7 MB transfer | ~72% |
| State total (133 × 3) | ~10 GB storage | ~2.5-3 GB transfer | ~72% |
| R2 storage (gzipped files) | ~3 GB | — | ~70% vs raw |

### 2.3 Automated Multi-County Processing

**Recommendation: Chunked Pipeline (Option D)**

```
Step 1: Download statewide dataset once
        VA: full CSV from Virginia Roads (replaces previous)
        CO: latest year Excel from OnBase (replaces that year only)
Step 2: Split into per-jurisdiction CSVs (fast, CPU-only)
Step 3: Batch validate (shared geocode cache across all counties — massive speedup)
Step 4: Generate road-type variants per county (all_roads, county_roads, no_interstate)
Step 5: Batch upload to R2 (all files, overwrite)
Step 6: Generate state + MPO aggregates from county CSVs
Step 7: Upload aggregates to R2
Step 8: Commit updated r2-manifest.json
```

**Estimated processing (Virginia, 133 jurisdictions):**
| Step | Time |
|------|------|
| Download statewide CSV | ~2-5 min |
| Split into 133 jurisdictions | ~1-2 min |
| Batch validate (cached geocodes) | ~10-20 min |
| Road-type split (133 × 3 = 399 files) | ~5 min |
| Upload 399 + aggregates to R2 | ~15-30 min |
| Generate aggregates | ~5 min |
| **Total** | **~40-65 min** (within GitHub Actions free tier) |

---

## 3. R2 Folder Structure

### 3.1 Recommended R2 Key Hierarchy

```
crash-lens-data/
│
├── _national/                              ← FEDERAL TIER
│   ├── state_comparison.json               ← Cross-state metrics (our data + FARS)
│   ├── fars_summary.json                   ← FARS fatality data by state
│   ├── hsip_performance.json               ← HSIP performance targets by state
│   └── snapshots/
│       └── 2026-02_state_comparison.json
│
├── virginia/                               ← STATE TIER (Virginia)
│   ├── _statewide/                         ← State-level aggregates
│   │   ├── aggregates.json                 ← Statewide totals, trends, YoY
│   │   ├── county_summary.json             ← Per-county ranking table (by road type)
│   │   ├── mpo_summary.json                ← Per-MPO aggregates (by road type)
│   │   └── snapshots/
│   │       ├── 2026-01_aggregates.json
│   │       └── 2026-02_aggregates.json
│   │
│   ├── _mpo/                               ← MPO TIER (Virginia)
│   │   ├── hrtpo/
│   │   │   ├── aggregates.json             ← MPO-level aggregates (by road type)
│   │   │   ├── member_counties.json        ← County list + per-county stats
│   │   │   └── hotspots.json               ← Cross-county hotspot ranking
│   │   ├── nvta/
│   │   │   ├── aggregates.json
│   │   │   ├── member_counties.json
│   │   │   └── hotspots.json
│   │   └── ... (other MPOs)
│   │
│   ├── henrico/                            ← COUNTY TIER (existing, untouched)
│   │   ├── all_roads.csv
│   │   ├── county_roads.csv
│   │   ├── no_interstate.csv
│   │   ├── forecasts_all_roads.json
│   │   ├── forecasts_county_roads.json
│   │   └── forecasts_no_interstate.json
│   │
│   └── ... (132 more jurisdictions)
│
├── colorado/                               ← STATE TIER (Colorado)
│   ├── _statewide/
│   │   ├── aggregates.json
│   │   ├── county_summary.json
│   │   ├── region_summary.json             ← CDOT 5-region breakdown
│   │   └── snapshots/
│   │
│   ├── _region/                            ← CDOT REGION TIER (CO-specific)
│   │   ├── region_1/
│   │   │   ├── aggregates.json
│   │   │   ├── member_counties.json
│   │   │   └── hotspots.json
│   │   └── ... (4 more regions)
│   │
│   ├── _mpo/                               ← MPO TIER (Colorado)
│   │   ├── drcog/
│   │   │   ├── aggregates.json
│   │   │   └── member_counties.json
│   │   └── ... (other MPOs/TPRs)
│   │
│   ├── douglas/                            ← COUNTY TIER (existing, untouched)
│   │   ├── all_roads.csv
│   │   ├── county_roads.csv
│   │   ├── no_interstate.csv
│   │   └── standardized.csv
│   │
│   └── ... (63 more counties)
│
├── shared/                                 ← CROSS-CUTTING DATA
│   ├── grants.csv                          ← Federal + state grants
│   ├── cmf_processed.json                  ← CMF clearinghouse data
│   ├── cmf_metadata.json
│   ├── value_labels.json                   ← Categorical value decode table (~5 KB)
│   ├── boundaries/                         ← GeoJSON boundaries for choropleth + overlay maps
│   │   ├── us_states.geojson               ← Federal tier map
│   │   ├── virginia_counties.geojson       ← VA state tier map
│   │   ├── colorado_counties.geojson       ← CO state tier map
│   │   ├── virginia_mpos.geojson           ← VA MPO boundaries (cached from BTS NTAD)
│   │   ├── colorado_mpos.geojson           ← CO MPO boundaries (cached from BTS NTAD)
│   │   ├── virginia_vdot_districts.geojson ← VDOT 9-district boundaries (cached from VDOT ArcGIS)
│   │   └── colorado_cdot_regions.geojson   ← CDOT 5-region boundaries (cached from CDOT ArcGIS)
│   └── mutcd/                              ← MUTCD reference data
│       └── *.json
│
└── manifest.json                           ← Master manifest of all files
```

### 3.2 Aggregate JSONs Include Road-Type Breakdowns

Since State and MPO views will support road type filtering (same as County), aggregates must include per-road-type stats:

```json
// county_summary.json schema
{
  "state": "virginia",
  "period": "2020-2025",
  "generated": "2026-02-13T00:00:00Z",
  "roadTypes": {
    "all_roads": {
      "statewide": {
        "total": 450000, "K": 890, "A": 4120, "B": 21050,
        "C": 38900, "O": 394040, "epdo": 1234567,
        "pedCrashes": 5600, "bikeCrashes": 1200,
        "yearlyTrend": [
          {"year": 2020, "total": 85000, "K": 170},
          {"year": 2021, "total": 88000, "K": 185}
        ],
        "topCollisionTypes": [
          {"type": "Rear End", "count": 156000, "pct": 34.7},
          {"type": "Angle", "count": 98000, "pct": 21.8}
        ]
      },
      "counties": {
        "henrico": {
          "name": "Henrico County", "fips": "087",
          "total": 28500, "K": 89, "A": 412, "B": 2105,
          "epdo": 112340, "crashRate": 4.2,
          "trend": -0.03, "rank": 5,
          "center": [-77.45, 37.55]
        }
      }
    },
    "county_roads": {
      "statewide": { ... },
      "counties": { ... }
    },
    "no_interstate": {
      "statewide": { ... },
      "counties": { ... }
    }
  },
  "mpos": {
    "hrtpo": {
      "name": "Hampton Roads TPO",
      "memberCounties": ["norfolk", "virginia_beach", "chesapeake", ...],
      "all_roads": { "total": 45000, "K": 156, "epdo": 267890 },
      "county_roads": { "total": 18000, "K": 42, "epdo": 98000 },
      "no_interstate": { "total": 38000, "K": 120, "epdo": 210000 }
    }
  }
}
```

---

## 4. UI Architecture: Tier Selector + Map Strategy

### 4.1 Tier Selector in Upload Data Tab

**Location:** Inside the existing Upload Data tab, above the current State/Jurisdiction selector.

```
┌──────────────────────────────────────────────────┐
│  📊 View Level                                    │
│                                                   │
│  ┌─────────┐ ┌─────────┐ ┌──────┐ ┌──────────┐  │
│  │ Federal │ │  State  │ │ MPO  │ │ County ● │  │
│  └─────────┘ └─────────┘ └──────┘ └──────────┘  │
│  (Segmented control — County is default/existing) │
│                                                   │
│  ╔═══ When "Federal" selected ═══════════════╗    │
│  ║ Showing: National Overview                ║    │
│  ║ States with data: Virginia, Colorado      ║    │
│  ║ Road Type: ○ County ○ No Interstate ● All ║    │
│  ║ [Load National View]                      ║    │
│  ╚═══════════════════════════════════════════╝    │
│                                                   │
│  ╔═══ When "State" selected ═════════════════╗    │
│  ║ State: [Virginia ▾]                       ║    │
│  ║ Road Type: ○ County ○ No Interstate ● All ║    │
│  ║ [Load State View]                         ║    │
│  ╚═══════════════════════════════════════════╝    │
│                                                   │
│  ╔═══ When "MPO" selected ═══════════════════╗    │
│  ║ State: [Virginia ▾]                       ║    │
│  ║ MPO:   [Hampton Roads TPO ▾]              ║    │
│  ║ Road Type: ○ County ○ No Interstate ● All ║    │
│  ║ Member Counties: Norfolk, VB, Ches...     ║    │
│  ║ [Load MPO View]                           ║    │
│  ╚═══════════════════════════════════════════╝    │
│                                                   │
│  ╔═══ When "County" selected (EXISTING) ═════╗    │
│  ║ State: [Virginia ▾]                       ║    │
│  ║ County: [Henrico ▾]                       ║    │
│  ║ Road Type: ○ County ○ No Interstate ● All ║    │
│  ║ [Load County Data] (existing behavior)    ║    │
│  ╚═══════════════════════════════════════════╝    │
│                                                   │
└──────────────────────────────────────────────────┘
```

**Road type selector at every tier:** The existing 3-option radio (County Roads / No Interstate / All Roads) appears for Federal, State, MPO, and County views. This filters which aggregate data is displayed at higher tiers, and which CSV variant loads at county tier.

### 4.2 Map Strategy for Each View Level

#### Current County Map (Untouched)

The existing Leaflet.js map with:
- **Base layers:** OpenStreetMap (street) / Mapbox Satellite
- **Crash markers:** Clustered markers, heatmap, individual pins
- **Asset Layers Panel** with toggles for:
  - Mapillary: Coverage (green lines), Traffic Signs, Map Features
  - TIGERweb: County Boundary (dashed blue), Magisterial Districts (colored subdivisions)
  - BTS/NTAD: AADT Network, Bridge Inventory, Railroad Crossings, Transit Stops, Transit Routes
  - User-uploaded assets

#### Federal Map — US State Choropleth

| Aspect | Design |
|--------|--------|
| **Base** | Same Leaflet instance, zoomed to CONUS (center: [-98.5, 39.8], zoom: 4) |
| **Primary Layer** | State-level choropleth polygons colored by selected metric |
| **Boundary Source** | `shared/boundaries/us_states.geojson` from R2 (pre-downloaded from Census TIGER/Line, simplified to ~2 MB) |
| **Color Metric** | Selectable: Total Crashes, Fatalities (K), KA Rate, EPDO, Crash Trend (dropdown above map) |
| **Interaction** | Hover: tooltip with state name + metrics. Click: drills down to State view for that state |
| **Legend** | Color ramp legend showing min/max values for selected metric |
| **Road Type** | Switches which set of aggregate numbers powers the choropleth coloring |

**Asset Layers at Federal Level:**

| Layer | Available? | Behavior |
|-------|-----------|----------|
| Mapillary Coverage | ❌ Hidden | Too zoomed out, not useful |
| Mapillary Signs | ❌ Hidden | Too zoomed out |
| Mapillary Features | ❌ Hidden | Too zoomed out |
| TIGERweb Boundaries | ❌ Hidden | State boundaries come from GeoJSON choropleth |
| Magisterial Districts | ❌ Hidden | County-level only |
| BTS AADT Network | ❌ Hidden | Too much data nationally |
| BTS Bridges | ✅ Available | National bridge condition overview (clustered) |
| BTS Railroad Crossings | ❌ Hidden | Too much data nationally |
| BTS Transit Stops | ❌ Hidden | Too much data nationally |
| BTS Transit Routes | ❌ Hidden | Too much data nationally |
| User Assets | ✅ Available | If user uploaded national-scope assets |

#### State Map — County Choropleth

| Aspect | Design |
|--------|--------|
| **Base** | Same Leaflet instance, zoomed to state bounds (from config.json state bbox) |
| **Primary Layer** | County-level choropleth polygons colored by selected metric |
| **Boundary Source** | `shared/boundaries/{state}_counties.geojson` from R2 (pre-downloaded from Census, ~2-3 MB per state) |
| **DOT District Overlay** | Optional toggle: DOT district/region boundary outlines (bold dashed, from state DOT ArcGIS — see Section 5.4) |
| **MPO Overlay** | Optional toggle: MPO boundary outlines (dashed purple, from BTS NTAD MPO FeatureServer — see Section 5) overlaid on county choropleth |
| **Color Metric** | Selectable dropdown: Total Crashes, K Fatalities, KA Rate, EPDO, Ped/Bike Crashes, Crash Trend |
| **Interaction** | Hover: tooltip with county name + metrics. Click: drills down to County view |
| **Road Type** | Radio selection switches which road-type aggregate set colors the choropleth |
| **Legend** | Color ramp legend |

**Asset Layers at State Level:**

| Layer | Available? | Behavior |
|-------|-----------|----------|
| Mapillary Coverage | ❌ Hidden | Too zoomed out for state view |
| Mapillary Signs | ❌ Hidden | Too zoomed out |
| Mapillary Features | ❌ Hidden | Too zoomed out |
| TIGERweb Boundaries | ✅ Adapted | County boundaries come from choropleth GeoJSON |
| Magisterial Districts | ❌ Hidden | County-level only |
| BTS AADT Network | ⚠️ Toggle | Show NHS network overlay (filter to NHS routes only for performance) |
| BTS Bridges | ✅ Available | Clustered at state zoom, shows condition ratings |
| BTS Railroad Crossings | ✅ Available | Clustered at state zoom |
| BTS Transit Stops | ❌ Hidden | Too dense at state zoom |
| BTS Transit Routes | ✅ Available | Major routes visible at state zoom |
| **NEW: BTS MPO Boundaries** | ✅ Available | Toggle to show all MPO boundaries overlaid (see Section 5) |
| **NEW: DOT District Boundaries** | ✅ Available | Toggle to show state DOT administrative district/region outlines (see Section 5.4) |
| User Assets | ✅ Available | If user uploaded state-scope assets |

#### MPO Map — Hybrid Choropleth + Heatmap

| Aspect | Design |
|--------|--------|
| **Base** | Same Leaflet instance, zoomed to MPO bounding box |
| **Primary Layer** | MPO boundary polygon (from BTS NTAD, bold outline) with member county sub-boundaries |
| **Boundary Source** | BTS NTAD MPO boundary (Section 5) + `shared/boundaries/{state}_counties.geojson` filtered to member counties |
| **County Fill** | Member counties colored by metric (mini-choropleth within MPO) |
| **Crash Layer** | Heatmap or clustered crash markers aggregated from all member county CSVs (lazy-loaded) |
| **Color Metric** | Same dropdown as State view |
| **Interaction** | Hover county: tooltip. Click county: drills down to County view. Click cluster: zoom in |
| **Road Type** | Radio switches aggregate data + which crash CSVs to load for heatmap |
| **Progressive Detail** | At higher zoom levels, transition from heatmap to individual crash markers (same as current county behavior) |

**Asset Layers at MPO Level:**

| Layer | Available? | Behavior |
|-------|-----------|----------|
| Mapillary Coverage | ✅ Available | Visible at MPO zoom level |
| Mapillary Signs | ✅ Available | Visible when zoomed to street level within MPO |
| Mapillary Features | ✅ Available | Visible when zoomed to street level within MPO |
| TIGERweb Boundaries | ✅ Adapted | Member county boundaries from choropleth |
| Magisterial Districts | ⚠️ On drill | Available when user hovers/clicks into a specific county |
| BTS AADT Network | ✅ Available | Full AADT network within MPO bounds |
| BTS Bridges | ✅ Available | All bridges within MPO |
| BTS Railroad Crossings | ✅ Available | All crossings within MPO |
| BTS Transit Stops | ✅ Available | All transit stops within MPO |
| BTS Transit Routes | ✅ Available | All transit routes within MPO |
| **NEW: BTS MPO Boundary** | ✅ Always On | Bold MPO boundary outline always visible |
| User Assets | ✅ Available | If user uploaded MPO-scope assets |

#### Map Rendering Summary

| View | Crash Display | Boundaries | Zoom Level | Asset Layer Availability |
|------|---------------|-----------|------------|--------------------------|
| **Federal** | None (aggregates only) | State choropleth polygons | ~4 (CONUS) | Bridges only |
| **State** | None (aggregates only) | County choropleth polygons + MPO outlines | ~6-8 (state) | Bridges, Railroad, Transit Routes, AADT (NHS), MPO boundaries |
| **MPO** | Heatmap → Clusters → Markers (progressive) | MPO boundary + member county fills | ~8-12 (region) | All layers available |
| **County** | Clusters → Markers (existing) | County boundary + districts (existing) | ~10-14 (county) | All layers available (existing, untouched) |

---

## 5. BTS MPO Boundary API Integration

### 5.1 BTS NTAD MPO FeatureServer

The Bureau of Transportation Statistics publishes official MPO boundaries via a **free, public ArcGIS REST API** — no API key required:

**Primary Endpoint:**
```
https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0
```

**Query for a specific state (e.g., Virginia):**
```
/query?where=STATE='VA'&outFields=MPO_ID,MPO_NAME,ACRONYM,STATE,POP&returnGeometry=true&f=geojson
```

**Available Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `MPO_ID` | String | Unique MPO identifier |
| `MPO_NAME` | String | Full MPO name |
| `ACRONYM` | String | Short name (e.g., "HRTPO") |
| `STATE` | String | Primary state (e.g., "VA") |
| `STATE_2` | String | Secondary state (multi-state MPOs) |
| `STATE_3` | String | Third state |
| `POP` | Integer | Population served |
| `MPO_URL` | String | MPO website |
| `Shape` | Polygon | Geographic boundary geometry |

### 5.2 Integration into Asset Layers

Add BTS MPO Boundaries as a new layer in the existing **Asset Layers Panel**, alongside the existing BTS layers:

```
Asset Layers Panel (updated):
  ├── Mapillary Layers
  │   ├── 📷 Street Imagery Coverage
  │   ├── 🚦 Traffic Signs
  │   └── 🛤️ Map Features
  │
  ├── BTS Federal Data
  │   ├── 🛣️ AADT Road Network       (existing)
  │   ├── 🌉 Bridge Inventory         (existing)
  │   ├── 🚂 Railroad Grade Crossings (existing)
  │   ├── 🚏 Transit Stops            (existing)
  │   ├── 🚌 Transit Routes           (existing)
  │   └── 🗺️ MPO Boundaries          ← NEW
  │
  ├── Jurisdiction Boundaries
  │   ├── 📍 County/City Boundary     (existing)
  │   └── 🗺️ Magisterial Districts   (existing)
  │
  └── User Uploaded Assets            (existing)
```

**Implementation Details:**

```javascript
// Add to BTS_ENDPOINTS object (line ~110061 in index.html)
btsMPOBoundaries: {
  name: 'MPO Boundaries',
  description: 'Metropolitan Planning Organization boundaries from BTS/NTAD',
  icon: '🗺️',
  color: '#7c3aed',  // Purple (matches district styling)
  url: 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0',
  geometryType: 'polygon',
  minZoom: 0,
  pageSize: 500,
  maxPages: 5,
  popupFields: ['MPO_NAME', 'ACRONYM', 'STATE', 'POP', 'MPO_URL'],
  style: {
    color: '#7c3aed',
    weight: 2,
    fillOpacity: 0.08,
    dashArray: '6, 3'
  }
}
```

**Query Strategy:**
- **Federal view:** Load all MPOs nationally (`where=1=1`)
- **State view:** Filter by state (`where=STATE='VA' OR STATE_2='VA' OR STATE_3='VA'`)
- **MPO view:** Load the selected MPO boundary only
- **County view:** Load MPOs that intersect the county bbox (spatial query)

**Caching:** Cache the GeoJSON per-state in `builtInLayersState.btsMPOBoundaries.geojsonCache[stateKey]` — same pattern as existing BTS layers.

### 5.3 Additional BTS Layers to Consider

The NTAD provides ~90 datasets. Beyond the 5 already integrated + MPO Boundaries, these are relevant for a crash analysis tool:

| Dataset | Endpoint | Relevance | Priority |
|---------|----------|-----------|----------|
| **Counties** | `NTAD_Counties/FeatureServer` | Alternate county boundaries for choropleth | Low (TIGERweb already provides this) |
| **Urbanized Areas** | `Urbanized_Areas/MapServer` | Context for urban/rural crash analysis | Medium |
| **Freight Analysis Framework** | `Freight_Analysis_Framework_Network/MapServer` | Truck crash context | Medium |
| **NHS (National Highway System)** | In HPMS dataset | Filter AADT to NHS-only for state view | High |
| **Airports** | `Airports/MapServer` | Context for crashes near airports | Low |
| **Bikeshare Stations** | Available in NTAD | Context for bike crash analysis | Medium |

**Recommendation:** Add MPO Boundaries now (directly supports the MPO tier). Add Urbanized Areas and Freight Network later as optional layers.

### 5.4 State DOT District/Region Boundary Integration

#### The Problem

State DOT users (VDOT HQ, CDOT HQ) organize their operations by **administrative districts/regions**. The State view map needs to show these boundaries as an overlay so users can see which district "owns" which counties. There is **no national dataset** of state DOT district boundaries — each state publishes their own via separate ArcGIS REST APIs.

#### State DOT Boundary APIs

| State | Term | Count | ArcGIS REST API |
|-------|------|-------|-----------------|
| **Virginia (VDOT)** | Districts | 9 | `https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/VDOTAdministrativeBoundaries/FeatureServer/2` |
| **Colorado (CDOT)** | Engineering Regions | 5 | `https://dtdapps.coloradodot.info/arcgis/rest/services/CPLAN/open_data_sde/FeatureServer/3` |
| **Texas (TxDOT)** | Districts | 25 | `https://maps.dot.state.tx.us/arcgis/rest/services/Boundaries/MapServer` |
| **Florida (FDOT)** | Districts | 7 | Via `gis-fdot.opendata.arcgis.com` |
| **California (Caltrans)** | Districts | 12 | Via `gisdata-caltrans.opendata.arcgis.com` |
| **N. Carolina (NCDOT)** | Highway Divisions | 14 | Via `connect.ncdot.gov` |

All use ArcGIS, so the query pattern is consistent: `/query?where=1=1&outFields=*&f=geojson`. But the URLs, layer IDs, and field names differ per state.

#### Configuration: Per-State `boundaries.json`

**Yes — create a dedicated boundary configuration file for each state DOT.** This file lives in `states/{state}/boundaries.json` and defines all boundary layer endpoints for that state.

```json
// states/virginia/boundaries.json
{
  "state": "virginia",
  "dotDistricts": {
    "name": "DOT Districts",
    "term": "District",
    "count": 9,
    "source": "arcgis_rest",
    "endpoint": "https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/VDOTAdministrativeBoundaries/FeatureServer/2",
    "nameField": "DISTRICT_NAME",
    "codeField": "DISTRICT_CODE",
    "style": {
      "color": "#dc2626",
      "weight": 3,
      "fillOpacity": 0.05,
      "dashArray": "8, 4"
    },
    "fallbackGeojson": "shared/boundaries/virginia_vdot_districts.geojson"
  },
  "mpo": {
    "source": "bts_ntad",
    "stateFilter": "VA"
  },
  "counties": {
    "source": "census_tiger",
    "geojson": "shared/boundaries/virginia_counties.geojson"
  }
}
```

```json
// states/colorado/boundaries.json
{
  "state": "colorado",
  "dotDistricts": {
    "name": "CDOT Engineering Regions",
    "term": "Region",
    "count": 5,
    "source": "arcgis_rest",
    "endpoint": "https://dtdapps.coloradodot.info/arcgis/rest/services/CPLAN/open_data_sde/FeatureServer/3",
    "nameField": "REGION",
    "codeField": "REGION_ID",
    "style": {
      "color": "#2563eb",
      "weight": 3,
      "fillOpacity": 0.05,
      "dashArray": "8, 4"
    },
    "fallbackGeojson": "shared/boundaries/colorado_cdot_regions.geojson"
  },
  "mpo": {
    "source": "bts_ntad",
    "stateFilter": "CO"
  },
  "counties": {
    "source": "census_tiger",
    "geojson": "shared/boundaries/colorado_counties.geojson"
  }
}
```

#### Why Per-State Config Files?

| Reason | Detail |
|--------|--------|
| **Field names differ** | VDOT uses `DISTRICT_NAME`, CDOT uses `REGION`, TxDOT uses `DIST_NM` |
| **Terminology differs** | "Districts" vs "Regions" vs "Divisions" — the UI label must match what state DOT staff expect |
| **Endpoint URLs differ** | Each state DOT has their own ArcGIS server with unique URLs |
| **Styling may differ** | Different colors/weights to distinguish from other boundary layers |
| **Some states have sub-districts** | VDOT has Districts > Residencies. Config can optionally include sub-layers |
| **Fallback strategy** | Pre-downloaded GeoJSON in R2 in case the live API is down |

#### Dual-Source Strategy: Live API + Cached Fallback

1. **At onboarding:** Run a one-time script to download the state's district boundaries as GeoJSON → upload to R2 as `shared/boundaries/{state}_dot_districts.geojson`
2. **At runtime:** Try the live ArcGIS REST API first (gives freshest data). If the API fails (timeout, CORS, server down), fall back to the cached GeoJSON from R2
3. **Periodic refresh:** A scheduled workflow (annually) re-downloads from the live API and updates the R2 cache. District boundaries change very rarely (CDOT last changed in 2013)

```javascript
// Pseudocode for boundary loading
async function loadDOTDistrictBoundary(stateKey) {
  const cfg = stateBoundaryConfigs[stateKey].dotDistricts;
  try {
    // Try live API first
    const url = `${cfg.endpoint}/query?where=1=1&outFields=${cfg.nameField},${cfg.codeField}&f=geojson`;
    const resp = await fetch(url);
    return await resp.json();
  } catch (err) {
    // Fall back to cached GeoJSON from R2
    const fallbackUrl = resolveDataUrl(cfg.fallbackGeojson);
    const resp = await fetch(fallbackUrl);
    return await resp.json();
  }
}
```

#### New State Onboarding Checklist (Boundary Edition)

When onboarding a new state, the boundary setup process is:

1. [ ] Find the state DOT's ArcGIS REST API for district/region boundaries (most have one)
2. [ ] Identify the field names for district name and code
3. [ ] Create `states/{state}/boundaries.json` with endpoint, fields, styling
4. [ ] Run the download script to cache boundaries as GeoJSON in R2
5. [ ] Test: district overlay renders on the State Map, labels are correct
6. [ ] If no ArcGIS API exists: manually obtain Shapefile from state DOT, convert to GeoJSON, upload to R2 as static fallback

### 5.5 MPO Boundary Auto-Load from Dropdown Selection

When the user selects an MPO from the dropdown in the Upload Data tab, the map should **automatically** fetch and display that MPO's boundary from BTS NTAD — no manual toggle needed.

#### Connection Flow

```
User selects "State: Virginia" in tier selector
    ↓
User selects "MPO: Hampton Roads TPO" from dropdown
    ↓
1. UI reads hierarchy.json to get MPO acronym ("HRTPO")
    ↓
2. Fetch from BTS NTAD:
   /query?where=ACRONYM='HRTPO'&outFields=*&returnGeometry=true&f=geojson
    ↓
3. Add GeoJSON polygon to map as bold MPO boundary layer
    ↓
4. Zoom map to MPO polygon bounds (fitBounds)
    ↓
5. Load member county boundaries (filtered from state counties GeoJSON)
    ↓
6. Load MPO aggregate data from R2
```

#### Mapping MPO Dropdown to BTS NTAD

The `hierarchy.json` must include the BTS NTAD `ACRONYM` or `MPO_ID` for each MPO so the correct boundary can be fetched:

```json
// states/virginia/hierarchy.json — MPO entries with BTS NTAD mapping
{
  "mpos": {
    "hrtpo": {
      "name": "Hampton Roads Transportation Planning Organization",
      "btsAcronym": "HRTPO",
      "btsMpoId": "VA-125",
      "counties": ["norfolk", "virginia_beach", "chesapeake", ...],
      "center": [-76.28, 36.85],
      "zoom": 10
    },
    "nvta": {
      "name": "Northern Virginia Transportation Authority",
      "btsAcronym": "NVTC",
      "btsMpoId": "VA-150",
      "counties": ["arlington", "fairfax", "loudoun", ...],
      "center": [-77.35, 38.85],
      "zoom": 10
    }
  }
}
```

#### Implementation in the UI

```javascript
// When MPO dropdown changes:
async function onMPOSelected(stateKey, mpoKey) {
  const mpoConfig = hierarchy[stateKey].mpos[mpoKey];

  // 1. Fetch MPO boundary from BTS NTAD
  const btsQuery = `https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/` +
    `NTAD_Metropolitan_Planning_Organizations/FeatureServer/0/query` +
    `?where=ACRONYM='${mpoConfig.btsAcronym}'&outFields=*&returnGeometry=true&f=geojson`;

  const mpoBoundary = await fetch(btsQuery).then(r => r.json());

  // 2. Add to map with bold styling
  mpoBoundaryLayer = L.geoJSON(mpoBoundary, {
    style: { color: '#7c3aed', weight: 3, fillOpacity: 0.06, dashArray: '6,3' },
    onEachFeature: (f, layer) => {
      layer.bindPopup(`<b>${f.properties.MPO_NAME}</b><br>Pop: ${f.properties.POP?.toLocaleString()}`);
    }
  }).addTo(map);

  // 3. Zoom to MPO bounds
  map.fitBounds(mpoBoundaryLayer.getBounds(), { padding: [20, 20] });

  // 4. Load member county fills + aggregate data
  loadMPOCountyFills(stateKey, mpoConfig.counties);
  loadMPOAggregates(stateKey, mpoKey);
}
```

---

## 6. Dynamic EPDO Scoring System

> **Reference Implementation:** See `data/CDOT/epdo/` for complete code:
> - `EPDO_PLAN.md` — Detailed implementation plan with line-by-line changes
> - `epdo_presets.js` — Frontend preset system, UI HTML, recalculation cascade
> - `epdo_config_loader.py` — Python config loader for backend scripts

### 6.1 Current State & Problem

EPDO (Equivalent Property Damage Only) weights are **hardcoded identically** in 14+ locations across the codebase:

| Location | Weights | Purpose |
|----------|---------|---------|
| `app/index.html` line 19910 | `{ K:462, A:62, B:12, C:5, O:1 }` | Primary `EPDO_WEIGHTS` constant |
| `app/index.html` line 28795 | `calcEPDO()` function | Used in 60+ places across all tabs |
| `app/index.html` — **14 inline calculations** | `d.K*462 + d.A*62 + d.B*12 + d.C*5 + d.O` | Bypass `calcEPDO()` entirely |
| `app/index.html` line 69187 | `EPDO_WEIGHTS_AD` (unused duplicate) | Dead code to remove |
| `states/virginia/config.json` | `"epdoWeights": { K:462, ... }` | Config exists but is **never read by the app** |
| `states/colorado/config.json` | `"epdoWeights": { K:462, ... }` | Config exists but is **never read by the app** |
| `scripts/generate_forecast.py` line 88 | `EPDO_WEIGHTS = load_epdo_weights()` | **Already updated** — reads from config |
| `send_notifications.py` line 131 | `epdo_weights = _load_epdo_weights()` | **Already updated** — reads from config |

**Separate warrant-specific weights (intentionally NOT standard EPDO):**

| Line | Function | Weights | Methodology |
|------|----------|---------|-------------|
| 106360 | Streetlight warrant | K=1500, A=240 | Warrant-specific, NOT standard EPDO |
| 96525 | Roundabout warrant | K=1500, A=240 | Warrant-specific |
| 119030, 119197 | School zone analysis | K=1500, A=240 | Warrant-specific |
| 60512, 61542 | Grant application PDF | K=1500, A=240 | Grant application standard |

These warrant-specific weights use local `const` declarations that shadow the global — they will NOT be affected by changes to the global `EPDO_WEIGHTS` and must remain unchanged.

### 6.2 Research: State EPDO Weights Vary Significantly

**EPDO weights are derived from crash cost ratios:** `Weight = CrashCost(severity) / CrashCost(PDO)`

Since every state DOT calculates their own crash costs, the resulting weights vary significantly:

| Source / Agency | K (Fatal) | A (Serious) | B (Minor) | C (Possible) | O (PDO) | Year |
|-----------------|-----------|-------------|-----------|---------------|---------|------|
| **HSM Standard (current tool default)** | 462 | 62 | 12 | 5 | 1 | 2010 |
| **VDOT 2024 crash costs** | **1,032** | 53 | 16 | 10 | 1 | 2024 |
| **FHWA 2022 crash costs** | **975** | 48 | 13 | 8 | 1 | 2022 |
| **North Carolina DOT** | 76.8 (K+A) | — | 8.4 (B+C) | — | 1 | — |
| **New Mexico DOT** | 567 | 33 (all injury) | — | — | 1 | — |
| **Massachusetts DOT** | 21 (all F+I) | — | — | — | 1 | — |
| **Illinois DOT** | 25 | 10 | 1 | — | — | — |

**Critical finding:** The VDOT 2024 crash costs already in the tool ($12.8M K / $12.4K O) yield **K=1032 — more than double the current 462**. This means all EPDO scores in the Virginia view are currently understated by ~2x.

#### How EPDO Weights Are Derived

```
EPDO Weight = Crash Cost for Severity Level / Crash Cost for PDO

Example (VDOT 2024):
  K weight = $12,800,000 / $12,400 = 1,032
  A weight = $655,000 / $12,400 = 53
  B weight = $198,000 / $12,400 = 16
  C weight = $125,000 / $12,400 = 10
  O weight = $12,400 / $12,400 = 1
```

**Sources:**
- FHWA Highway Safety Manual (HSM), AASHTO, 2010 — Table 4-7
- FHWA HSIP Manual: https://safety.fhwa.dot.gov/hsip/resources/fhwasa09029/sec2.cfm
- FHWA Network Screening Training: https://safety.fhwa.dot.gov/local_rural/training/fhwasa14072/sec4.cfm

### 6.3 Preset-Based Configuration Architecture

Instead of a simple config cascade, the system uses **named presets** with a custom option. Presets persist via `localStorage` (survive page refresh) while custom weights also persist to `localStorage`.

#### Built-In Presets

```javascript
const EPDO_PRESETS = {
    hsm2010: {
        name: 'HSM Standard (2010)',
        weights: { K: 462, A: 62, B: 12, C: 5, O: 1 },
        description: 'Highway Safety Manual standard weights (AASHTO/FHWA)'
    },
    vdot2024: {
        name: 'VDOT 2024',
        weights: { K: 1032, A: 53, B: 16, C: 10, O: 1 },
        description: 'Derived from VDOT 2024 crash cost ratios ($12.8M K / $12.4K O)'
    },
    fhwa2022: {
        name: 'FHWA 2022',
        weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
        description: 'Derived from FHWA 2022 crash cost ratios ($11.6M K / $11.9K O)'
    },
    custom: {
        name: 'Custom',
        weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
        description: 'User-defined custom weights'
    }
};
```

> See `data/CDOT/epdo/epdo_presets.js` for full implementation reference.

#### Which Preset Applies at Each Tier?

| Tier | Default Preset | Why |
|------|---------------|-----|
| **Federal** | `hsm2010` (HSM Standard) | Cross-state comparisons need uniform national weights for apples-to-apples |
| **State (Virginia)** | `vdot2024` (VDOT 2024) | Virginia DOT staff expect their own state's official weights |
| **State (Colorado)** | `hsm2010` (HSM Standard) — until CDOT-specific preset added | Falls back to HSM if no state-specific preset exists |
| **MPO** | Inherits from parent state | MPOs operate within a state's framework |
| **County** | Inherits from parent state | Counties operate within a state's framework |

**Key insight:** At the Federal tier, we use **uniform national weights** so that comparing Virginia EPDO to Colorado EPDO is meaningful. At State/MPO/County tiers, we use the **state's own weights** to match what state DOT staff use in their own analyses. Users can override any preset at any tier via the "Custom" option.

#### Persistence Strategy

| Storage | Scope | Survives Refresh? | Purpose |
|---------|-------|-------------------|---------|
| `localStorage('epdoActivePreset')` | Per browser | Yes | Remembers which preset (hsm2010, vdot2024, fhwa2022, custom) |
| `localStorage('epdoCustomWeights')` | Per browser | Yes | Stores custom K/A/B/C/O values |
| State config files (`states/{state}/config.json`) | Per deployment | Yes | Server-side default per state |
| Aggregate JSONs (`epdoWeights` field) | Per R2 file | Yes | Records which weights generated the pre-computed EPDO |

### 6.4 Implementation: 8-Step Plan

> Full line-by-line details in `data/CDOT/epdo/EPDO_PLAN.md`

#### Step 1: Make `EPDO_WEIGHTS` mutable + add presets
**File:** `app/index.html:19916`

Change `const` to `let`, add `EPDO_ACTIVE_PRESET`, add `EPDO_PRESETS` constant (see `epdo_presets.js` for code).

#### Step 2: Add preset switching + recalculation functions
**File:** `app/index.html` (insert after EPDO_PRESETS)

New functions from `epdo_presets.js`:
- `loadEPDOPreset(presetKey)` — sets `EPDO_WEIGHTS`, saves to `localStorage`, triggers recalc cascade
- `loadSavedEPDOPreset()` — restores from `localStorage` on startup
- `saveCustomEPDOWeights()` — handles custom weight input changes
- `updateEPDOPresetUI()` — toggles active radio button
- `updateEPDOWeightLabels()` — updates dashboard label + glossary dynamically
- `recalculateAllEPDO()` — cascades recalculation across ALL tabs (Dashboard, Hotspots, Grants, CMF, Safety Focus, Before/After, Map stats, AI context)

#### Step 3: Fix ALL 14 inline hardcoded EPDO calculations
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

#### Step 4: Remove unused `EPDO_WEIGHTS_AD`
**File:** `app/index.html:69187-69188` — Delete the unused duplicate constant.

#### Step 5: Make UI labels dynamic
- Line 5447: Add `id="epdoWeightsLabel"` to the weights display div
- Line 19734: Add `id="epdoGlossaryDef"` to the glossary definition

#### Step 6: Add EPDO preset selector UI in Upload Data tab
**File:** `app/index.html` — Insert after Road Type Filter (~line 4700)

Radio button group with 4 options (HSM 2010, VDOT 2024, FHWA 2022, Custom). Custom option reveals 5 number inputs (K/A/B/C/O). See `epdo_presets.js` for full HTML reference.

```
┌──────────────────────────────────────────────────────┐
│  ⚖ EPDO Weight System                                │
│                                                       │
│  ○ HSM Standard (2010) - K=462, A=62, B=12, C=5, O=1│
│  ○ VDOT 2024 - K=1032, A=53, B=16, C=10, O=1        │
│  ○ FHWA 2022 - K=975, A=48, B=13, C=8, O=1          │
│  ○ Custom                                             │
│    ┌─────┬─────┬─────┬─────┬─────┐                   │
│    │  K  │  A  │  B  │  C  │  O  │  (shown when      │
│    │[   ]│[   ]│[   ]│[   ]│[   ]│   Custom selected) │
│    └─────┴─────┴─────┴─────┴─────┘                   │
│                                                       │
│  EPDO weights affect severity scoring across all tabs │
└──────────────────────────────────────────────────────┘
```

#### Step 7: Initialize saved preset on app load
Call `loadSavedEPDOPreset()` in a `DOMContentLoaded` handler, BEFORE the first `updateDashboard()` call.

#### Step 8: Python scripts — read from config
**Already implemented** in `generate_forecast.py` (line 69-88) and `send_notifications.py` (line 122-131). Both now call `load_epdo_weights()` which reads `epdoWeights` from the state's `config.json`. See `data/CDOT/epdo/epdo_config_loader.py` for the reference loader with full validation, auto-detection, and fallback.

### 6.5 Recalculation Cascade

When the user switches presets, ALL tabs recalculate EPDO scores in real-time:

```
User clicks preset radio button
    ↓
loadEPDOPreset(presetKey)
    ├─ 1. Update global EPDO_WEIGHTS object
    ├─ 2. Persist to localStorage
    ├─ 3. Update UI radio buttons + weight labels
    └─ 4. recalculateAllEPDO()
           ├─ updateDashboard()           — Dashboard totals
           ├─ analyzeHotspots()           — Hotspot rankings (cache cleared)
           ├─ rankLocationsForGrants()    — Grant location scores (cache cleared)
           ├─ buildCMFCrashProfile()      — CMF tab profile
           ├─ updateSafetyCategory()      — Safety Focus active category
           ├─ updateBAStudy()             — Before/After study
           ├─ updateMapStats()            — Map stats panel
           └─ updateAIContextIndicator()  — AI context
```

**Performance:** EPDO is a 5-term multiplication — recalculating even 100K crash records takes <100ms. No performance concern.

**Key insight:** Because `calcEPDO()` reads from the global `EPDO_WEIGHTS`, any function that calls `calcEPDO()` at render-time automatically picks up the new weights. The cascade function just triggers re-rendering of each affected component.

### 6.6 Aggregate JSONs and EPDO

Pre-computed aggregate JSONs (`county_summary.json`, `aggregates.json`) include EPDO scores calculated with the pipeline's default weights. The `epdoWeights` used are recorded in the JSON for traceability:

```json
{
  "epdoWeights": { "K": 1032, "A": 53, "B": 16, "C": 10, "O": 1 },
  "epdoSource": "VDOT 2024 crash costs",
  "counties": {
    "henrico": { "total": 28500, "K": 89, "A": 412, "B": 2105, "C": 3800, "O": 23094, "epdo": 136790 }
  }
}
```

When the user selects a different preset in the UI, the browser **recalculates** EPDO from the raw K/A/B/C/O counts in the aggregate JSON — it does NOT re-fetch data. The raw severity counts are always available in the aggregate, so client-side recalculation is instant.

### 6.7 Verification Plan

After implementation, verify all 9 items:

| # | Test | Expected Result |
|---|------|-----------------|
| 1 | **Dashboard:** Load app, note EPDO, switch to VDOT 2024 | EPDO increases (K: 883→1032). Switch back → original restores |
| 2 | **Dynamic labels:** Switch preset | "Weights: K=..." text updates in dashboard breakdown AND glossary |
| 3 | **All tabs:** Switch preset | Hotspots, Grants, CMF, Safety Focus tabs all update |
| 4 | **Persistence:** Set VDOT 2024, refresh page | VDOT 2024 still active |
| 5 | **Custom weights:** Select Custom, enter K=500 | Calculations use K=500. Refresh → persists |
| 6 | **Warrant independence:** Change to VDOT 2024, run streetlight warrant | Warrant still uses K=1500 (local const shadows global) |
| 7 | **No inline hardcoding:** Search `*462` in `app/index.html` | 0 results (except warrant sections which use `*1500`) |
| 8 | **Python scripts:** Run `python scripts/generate_forecast.py` | Reads from config, prints loaded weights |
| 9 | **Console:** Switch presets | No errors. `console.log(EPDO_WEIGHTS)` shows correct values |

### 6.8 How to Add New State Presets

When onboarding a new state:

1. **Research:** Find the state DOT's current crash costs from their SHSP or HSIP manual
2. **Calculate weights:** Divide each severity cost by the PDO cost (e.g., K = $KFatalCost / $PDOCost)
3. **Add to `EPDO_PRESETS`:** Add a new entry in `epdo_presets.js` with the state key, name, weights, and description
4. **Add to state config:** Set `epdoWeights` in `states/{state}/config.json` for Python scripts
5. **Update aggregates:** Re-generate aggregate JSONs with the new state's weights. Include `epdoWeights` and `epdoSource` fields

| Scenario | Who Updates | Where | Process |
|----------|-------------|-------|---------|
| **New state onboarded** | Dev team | `EPDO_PRESETS` in `index.html` + `states/{state}/config.json` | Calculate from state crash costs |
| **FHWA publishes new crash costs** | Dev team | Add new preset (e.g., `fhwa2026`) to `EPDO_PRESETS` | Derive weights, add preset, update aggregates |
| **State updates crash costs** | Dev team | Update existing preset weights | Derive new weights, note source + year |
| **User wants to test different weights** | End user | UI → Custom preset | Real-time in browser. Persists to `localStorage` |

---

## 7. Explore Section: Tab Behavior Per Tier

### 7.1 Tab Visibility Matrix

**County view is completely untouched.** The following shows behavior for the 3 new tiers only:

| Tab | Federal | State | MPO |
|-----|---------|-------|-----|
| Dashboard | ✅ State comparison table | ✅ County ranking table | ✅ Member county table |
| Map | ✅ State choropleth | ✅ County choropleth + MPO outlines | ✅ MPO boundary + county fills + heatmap |
| Crash Tree | ❌ Hidden | ✅ Statewide taxonomy | ❌ Hidden |
| Safety Focus | ✅ National emphasis | ✅ State emphasis | ✅ MPO emphasis |
| Fatal & Speeding | ❌ Hidden | ✅ Statewide stats | ❌ Hidden |
| Hot Spots | ✅ Top locations nationally | ✅ Top locations statewide | ✅ Cross-county ranking |
| Intersections | ❌ Hidden | ⚠️ Top N summary | ✅ Cross-county |
| Ped/Bike | ❌ Hidden | ❌ Hidden | ❌ Hidden |
| Analysis | ✅ Cross-state charts | ✅ County comparison | ✅ Inter-county |
| Crash Prediction | ❌ Hidden | ❌ Hidden | ❌ Hidden |
| Deep Dive | ❌ Hidden | ❌ Hidden | ❌ Hidden |

### 7.2 Drill-Down Navigation

Every higher-tier view supports drill-down to County:

| From | Action | Result |
|------|--------|--------|
| Federal Dashboard | Click state row | Switch to State view for that state |
| Federal Map | Click state polygon | Switch to State view for that state |
| State Dashboard | Click county row | Switch to County view for that county |
| State Map | Click county polygon | Switch to County view for that county |
| MPO Dashboard | Click county row | Switch to County view for that county |
| MPO Map | Click county polygon | Switch to County view for that county |

Drill-down loads the county CSV from R2 (existing behavior), sets the road type filter, and switches the tier selector to "County."

---

## 8. Complete Phased Roadmap

### Phase 1 — Pipeline: Gzip + Multi-County (Build First)
- [ ] Enable gzip compression on R2 uploads (configure `upload-to-r2.py` to compress CSVs before upload, set `Content-Encoding: gzip`)
- [ ] Add `--all-jurisdictions` flag to `download_crash_data.py` (VA: split from statewide download)
- [ ] Extend `split_cdot_data.py` to emit all 64 CO counties
- [ ] Extend `scripts/upload-to-r2.py` to batch-upload all jurisdictions
- [ ] Test: verify browser auto-decompresses gzipped CSVs from R2 correctly

### Phase 2 — Pipeline: Aggregates + Hierarchy + EPDO Config
- [ ] Create `scripts/generate_aggregates.py` (reads county CSVs → produces aggregates with road-type breakdowns)
- [ ] Update `states/virginia/config.json` with VDOT 2024 EPDO weights (K=1032, A=53, B=16, C=10, O=1)
- [ ] Update `states/colorado/config.json` with CDOT-specific EPDO weights (research needed from CDOT SHSP)
- [ ] Include `epdoWeights` + `epdoSource` in generated aggregate JSONs for traceability
- [ ] Create `states/virginia/hierarchy.json` (all MPOs with `btsAcronym`/`btsMpoId`, VDOT districts, PDCs)
- [ ] Complete `states/colorado/hierarchy.json` (CDOT regions, MPOs/TPRs with BTS mapping)
- [ ] Generate and upload state + MPO aggregates for VA and CO
- [ ] Archive monthly aggregate snapshots in `_statewide/snapshots/`
- [ ] Update `download-data.yml` workflow for batch processing + aggregate generation

### Phase 3 — Config: State DOT Boundary Files
- [ ] Create `states/virginia/boundaries.json` (VDOT districts endpoint, fields, styling, fallback)
- [ ] Create `states/colorado/boundaries.json` (CDOT regions endpoint, fields, styling, fallback)
- [ ] Run one-time download of VDOT district + CDOT region boundaries → upload to R2 `shared/boundaries/`
- [ ] Pre-download state/county boundary GeoJSONs from Census TIGER/Line → upload to R2
- [ ] Pre-download MPO boundaries from BTS NTAD per state → upload to R2 as fallback cache

### Phase 4 — Map: Boundaries + BTS Layers
- [ ] Add BTS MPO Boundaries as new layer in `BTS_ENDPOINTS` object
- [ ] Implement `addBTSMPOBoundaryLayer()` with state-filtered queries
- [ ] Add DOT District Boundary layer (reads from `states/{state}/boundaries.json`)
- [ ] Add MPO Boundaries + DOT Districts toggles to Asset Layers Panel
- [ ] Implement MPO auto-load on dropdown selection (fetch BTS by `btsAcronym` from hierarchy.json)
- [ ] Test: VA MPOs load correctly, VDOT districts overlay, CO MPOs + CDOT regions

### Phase 5 — UI: Tier Selector + Dynamic EPDO System
- [ ] Add tier selector segmented control (Federal / State / MPO / County) to Upload Data tab
- [ ] Add road type selector to Federal, State, MPO tiers (reuse existing radio component)
- [ ] **EPDO Step 1:** Change `const EPDO_WEIGHTS` to `let` + add `EPDO_PRESETS` (4 presets: HSM 2010, VDOT 2024, FHWA 2022, Custom) — see `data/CDOT/epdo/epdo_presets.js`
- [ ] **EPDO Step 2:** Add preset switching functions (`loadEPDOPreset`, `loadSavedEPDOPreset`, `recalculateAllEPDO`, etc.)
- [ ] **EPDO Step 3:** Fix all 14 inline hardcoded EPDO calculations → use `calcEPDO()` — see line table in Section 6.4
- [ ] **EPDO Step 4:** Remove unused `EPDO_WEIGHTS_AD` at line 69187
- [ ] **EPDO Step 5:** Add dynamic `id` attributes to EPDO labels (dashboard + glossary)
- [ ] **EPDO Step 6:** Add EPDO preset selector UI (radio group + custom inputs) in Upload Data tab
- [ ] **EPDO Step 7:** Call `loadSavedEPDOPreset()` in `DOMContentLoaded` BEFORE first `updateDashboard()`
- [ ] **EPDO Verify:** Run 9-point verification plan (Section 6.7) — dashboard, persistence, warrant independence, no inline `*462`
- [ ] Add `viewTier` to global `jurisdictionContext` state object
- [ ] Implement data loading logic per tier (aggregate JSONs from R2)
- [ ] Implement tab visibility toggling based on tier (hide/show tabs per matrix above)

### Phase 6 — UI: State View
- [ ] Implement State Dashboard (county ranking table with road-type switching)
- [ ] Implement State Map (county choropleth + DOT district overlay + MPO overlay toggles)
- [ ] Adapt Hot Spots for cross-county ranking
- [ ] Adapt Safety Focus for statewide emphasis areas
- [ ] Adapt Analysis for county comparison charts
- [ ] Implement drill-down: click county → load County view

### Phase 7 — UI: MPO View
- [ ] Implement MPO Dashboard (member county breakdown table)
- [ ] Implement MPO Map (auto-loaded BTS boundary + county fills + progressive crash heatmap/clusters)
- [ ] Implement lazy-loading of member county CSVs for crash-level heatmap
- [ ] Adapt Hot Spots for cross-county hotspot ranking within MPO
- [ ] Implement drill-down: click county → load County view

### Phase 8 — UI: Federal View
- [ ] Create `scripts/generate_national_summary.py` (roll up state aggregates + FARS data)
- [ ] Implement Federal Dashboard (state comparison table)
- [ ] Implement Federal Map (US state choropleth with metric dropdown)
- [ ] Adapt Safety Focus for national emphasis areas
- [ ] Implement drill-down: click state → load State view

### Phase 9 — New State Onboarding
- [ ] Document onboarding checklist (states/{state}/config.json + boundaries.json + hierarchy.json)
- [ ] Create state onboarding script (column mapping wizard)
- [ ] Onboard first new state (e.g., Maryland or North Carolina)
- [ ] Verify full pipeline: download → validate → geocode → split → aggregate → upload → display
- [ ] Verify boundary config: DOT districts render, BTS MPOs connect, EPDO weights correct

---

## 9. Key Recommendations

### 9.1 Storage Strategy

| Data Tier | Store in R2? | Store in Git? | Format | Update |
|-----------|-------------|---------------|--------|--------|
| National aggregates | ✅ | ❌ | JSON | Monthly |
| State aggregates | ✅ | ❌ | JSON | Monthly (archive snapshots) |
| MPO aggregates | ✅ | ❌ | JSON | Monthly |
| County crash CSVs | ✅ (gzipped) | ❌ | CSV.gz | Monthly (replace, no snapshots) |
| County forecasts | ✅ | ✅ (small) | JSON | Monthly |
| Boundary GeoJSONs (cached) | ✅ | ❌ | GeoJSON | Annually (Census updates) |
| DOT district boundaries | ✅ (fallback) | ❌ | GeoJSON | Annually (rare changes) |
| Grants | ❌ | ✅ | CSV | Weekly |
| CMF data | ❌ | ✅ | JSON | Quarterly |
| State config (EPDO, columns) | ❌ | ✅ | JSON | As needed |
| State hierarchy (MPOs, districts) | ❌ | ✅ | JSON | As needed |
| State boundaries config | ❌ | ✅ | JSON | Per state onboarding |
| R2 manifest | ❌ | ✅ | JSON | After each upload |

### 9.2 R2 Cost Projection (gzipped CSVs)

| Scale | Storage (gzipped) | Raw Equivalent | Monthly Cost |
|-------|-------------------|----------------|-------------|
| Current (2 counties) | ~35 MB | ~134 MB | Free |
| Virginia (133 counties) | ~3 GB | ~10 GB | Free (10 GB free tier) |
| VA + CO (133 + 64) | ~4.5 GB | ~15 GB | Free |
| 5 states | ~15-25 GB | ~50-80 GB | ~$0.22-$0.38/month |
| All 50 states | ~200-400 GB | ~700 GB-1.3 TB | ~$3-$6/month |

### 9.3 Performance Strategy

| Tier | Data Load (raw) | Transfer (gzipped) | Load Time |
|------|----------------|-------------------|-----------|
| Federal | ~50 KB aggregate JSON | ~15 KB | Instant |
| State | ~200-500 KB summary JSON | ~60-150 KB | Instant |
| MPO | ~50-100 KB aggregate JSON | ~15-30 KB | Instant |
| MPO (with crash heatmap) | ~30-90 MB member county CSVs | ~8-25 MB | Progressive (2-5s per county) |
| County | ~25 MB CSV | ~6-7 MB (gzipped) | 1-3 seconds |

### 9.4 Asset Layer Adaptations Summary

| Asset Layer | Federal | State | MPO | County |
|-------------|---------|-------|-----|--------|
| Mapillary Coverage | ❌ | ❌ | ✅ | ✅ |
| Mapillary Signs | ❌ | ❌ | ✅ | ✅ |
| Mapillary Features | ❌ | ❌ | ✅ | ✅ |
| BTS AADT Network | ❌ | ⚠️ NHS only | ✅ Full | ✅ Full |
| BTS Bridges | ✅ Clustered | ✅ Clustered | ✅ | ✅ |
| BTS Railroad Crossings | ❌ | ✅ Clustered | ✅ | ✅ |
| BTS Transit Stops | ❌ | ❌ | ✅ | ✅ |
| BTS Transit Routes | ❌ | ✅ Major | ✅ | ✅ |
| **BTS MPO Boundaries** | ✅ All | ✅ State-filtered | ✅ Auto-loaded | ⚠️ Intersecting |
| **DOT District Boundaries** | ❌ | ✅ Toggle | ❌ | ❌ |
| TIGERweb Boundary | ❌ | Via choropleth | Via choropleth | ✅ Existing |
| Magisterial Districts | ❌ | ❌ | ⚠️ On drill | ✅ Existing |
| User Assets | ✅ | ✅ | ✅ | ✅ |

### 9.5 BTS MPO Boundary Queries Per Tier

| Tier | Query | Expected Records |
|------|-------|-----------------|
| Federal | `where=1=1` (all MPOs nationally) | ~400 MPOs |
| State | `where=STATE='VA' OR STATE_2='VA' OR STATE_3='VA'` | ~10-15 per state |
| MPO | `where=MPO_ID='{selected}'` | 1 |
| County | Spatial query: `geometry={county_bbox}&spatialRel=esriSpatialRelIntersects` | 0-3 |

### 9.6 Additional Federal Data Sources

| Source | What It Provides | Download | Integration |
|--------|-----------------|----------|-------------|
| **FARS** (NHTSA) | Fatality data for all 50 states | Annual CSV from NHTSA | Script: `download_fars_data.py` |
| **HSIP Performance** (FHWA) | Safety targets by state | FHWA reports | Manual or scrape |
| **GES/CRSS** (NHTSA) | National crash estimates (sampled) | Annual from NHTSA | Optional supplement |
| **BTS NTAD Counties** | County boundaries for all states | Free ArcGIS REST | For choropleth maps |
| **BTS NTAD Urbanized Areas** | Urban/rural boundaries | Free ArcGIS REST | Context layer |
| **BTS Freight Analysis Framework** | Freight network for truck crashes | Free ArcGIS REST | Optional layer |
