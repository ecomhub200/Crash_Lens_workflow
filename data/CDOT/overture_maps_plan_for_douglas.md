# Overture Maps STAC Integration Plan for Douglas County

**Date:** 2026-02-16 (Updated: Real-Time PMTiles Approach)
**STAC Catalog:** https://stac.overturemaps.org
**Latest Release:** 2026-01-21.0 (Schema v1.15.0)
**PMTiles:** https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/{RELEASE_DATE}/{THEME}.pmtiles
**License:** ODbL (transportation, buildings, base), CDLA Permissive 2.0 (places)
**Douglas County Bbox:** [-105.0543, 39.1298, -104.6014, 39.5624]
**Implementation:** Real-time fetch via PMTiles (no pre-saved files, no Python pipeline needed)

---

## 1. What Overture Maps STAC Provides

Overture Maps Foundation publishes a monthly, open-licensed, global dataset via STAC catalog containing **4.2 billion features** across 6 themes. The data is stored as cloud-native GeoParquet on S3 and can be extracted for any bounding box via CLI.

### Relevant Datasets for Crash Analysis

| Theme/Type | Features (Global) | What It Contains | Format |
|---|---|---|---|
| `transportation/segment` | ~740M | Road centerlines with speed limits, road class, surface type, names, GERS IDs | LineString |
| `transportation/connector` | Included above | Intersection/junction points linking road segments | Point |
| `base/infrastructure` | ~144M | Traffic signals, crosswalks, barriers, poles | Point |
| `places/place` | ~72M | POIs: schools, hospitals, bars, gas stations, parks, etc. | Point |
| `buildings/building` | ~2.5B | Building footprints with height data | Polygon |

### Data Access

```bash
# Python CLI extraction for Douglas County
pip install overturemaps
overturemaps download --bbox=-105.0543,39.1298,-104.6014,39.5624 -f geojson --type=segment -o segments.geojson
overturemaps download --bbox=-105.0543,39.1298,-104.6014,39.5624 -f geojson --type=connector -o connectors.geojson
overturemaps download --bbox=-105.0543,39.1298,-104.6014,39.5624 -f geojson --type=infrastructure -o infrastructure.geojson
overturemaps download --bbox=-105.0543,39.1298,-104.6014,39.5624 -f geojson --type=place -o places.geojson
```

S3 (no credentials): `s3://overturemaps-us-west-2/release/2026-01-21.0/theme=transportation/type=segment/*.parquet`

**Browser limitation:** DuckDB-WASM cannot query S3 directly. Must pre-extract GeoJSON via Python pipeline, then load in browser.

---

## 2. Current Gaps Overture Fills

### Gap Analysis: What We Have vs What Overture Adds

| Capability | Current State | With Overture |
|---|---|---|
| **Road network geometry** | OSM Overpass API (slow, rate-limited, often fails) | Pre-cached road centerlines, instant, offline-capable |
| **Speed limit context** | None — crash records rarely include speed data | Posted speed limit for every road segment via linear referencing |
| **Intersection topology** | Tabular aggregation by route name string | Actual spatial connector points linking road segments |
| **Crash rate per mile** | Cannot calculate — no road length data | True crashes/mile using Overture segment geometry lengths |
| **Road classification** | BTS HPMS at zoom 12+ only | Full FHWA-mappable road class for every segment |
| **Traffic infrastructure** | Mapillary traffic signs (image detection) | Crosswalks and barriers (guard rails, cable barriers) — not covered by Mapillary |
| **Land use / POI context** | Schools only (Urban Institute API) | 9+ crash-relevant POI categories (bars, hospitals, daycare, parks, etc.) |
| **Crash Tree risk factors** | "High Speed >=55 mph" from crash records (unreliable) | Data-driven from Overture posted speed limits snapped to crash locations |

---

## 3. How This Makes the Tool More Accurate

### 3a. Speed Limit Enrichment (Biggest Accuracy Gain)

**Problem:** The Crash Tree risk factor "High Speed (>=55 mph)" currently relies on the crash record's speed field, which is often empty or unreliable in CDOT data.

**Solution:** Snap each crash point to the nearest Overture road segment using `turf.nearestPointOnLine()`. The segment's `speed_limit_mph` property provides the **posted speed limit** at the crash location. This makes the Crash Tree risk factor **data-driven** rather than estimated.

**Impact:** More accurate identification of speed-related crash patterns, better countermeasure targeting (e.g., speed management vs. access management).

### 3b. True Crash Rates per Mile (Hotspot Analysis)

**Problem:** Hotspot analysis currently aggregates crashes by route name string. A road with 50 crashes over 20 miles looks the same as a road with 50 crashes over 2 miles.

**Solution:** Overture provides actual road geometry with `length_miles` per segment. Sum segment lengths per route to calculate **true crashes per mile** — the standard safety metric.

**Impact:** Identifies corridors with genuinely high crash density vs. those that simply carry more traffic over longer distances.

### 3c. Segment Analysis Reliability

**Problem:** `fetchOSMCenterlineData()` queries 5 Overpass API endpoints with fallback. These are rate-limited, slow (5-15 seconds), and frequently fail.

**Solution:** Overture data is pre-cached locally. `buildRouteMapFromOverture()` returns instant results with no network calls needed.

**Impact:** Segment analysis becomes instant and 100% reliable instead of depending on external API availability.

### 3d. POI-Correlated Crash Patterns

**Problem:** No way to identify crash patterns near alcohol-serving establishments, senior facilities, or pedestrian generators (except schools).

**Solution:** Spatial join between crash locations and Overture POIs within configurable radius. Enables pattern analysis like "23% of nighttime K+A crashes within 500ft of bars/nightclubs."

**Impact:** Supports evidence-based countermeasure selection and grant applications with quantified land-use context.

---

## 4. Map Asset Layer Recommendations

### Layers to ADD

| Layer | Geometry | Visualization | Min Zoom | Why |
|---|---|---|---|---|
| **Overture Road Network** | LineString | Color-coded by class (motorway=red, primary=orange, secondary=yellow, residential=blue), weight by importance | 12 | No existing road geometry layer. Shows classification + speed limits in popup |
| **Overture Intersection Topology** | Point | Small circles at junction points | 14 | No intersection layer exists. Shows junction density and connectivity |
| **Overture Crosswalks & Barriers** | Point | Icons by type (crosswalk=blue, guard rail=gray) | 13 | Mapillary covers traffic signs but NOT crosswalks/barriers well |
| **Overture Crash-Relevant POIs** | Point | Categorized markers (see Section 5) | 12 | Only schools covered today; 9+ other categories needed |

### Layers to SKIP (Already Covered)

| Overture Data | Existing Coverage | Why Skip |
|---|---|---|
| Schools | Urban Institute API with enrollment, time-of-day, crash association radius | Far richer than Overture's basic POI point |
| Traffic signals | Mapillary traffic sign detection + OSM Overpass signals | Already two sources; Overture adds marginal value |
| Transit stops | BTS NTAD Transit Stops layer | Federal authoritative data already integrated |
| Road AADT | BTS HPMS layer | BTS HPMS includes AADT volumes which Overture doesn't have |

### Asset Panel UI Location

Add "Overture Maps Data" section in the Map tab's Asset Panel, after the existing "Federal Data Layers (BTS)" section. Each layer gets a checkbox toggle with feature count badge. Attribution line: "Overture Maps 2026-01-21.0 (ODbL)".

---

## 5. Crash-Relevant POI Categories (Beyond Schools)

Schools are already covered by Urban Institute API. These are the **high-value crash-relevant POI categories** to extract from Overture's 72M+ places dataset:

### Tier 1 — Highest Crash Relevance

| Category | Overture Filter | Crash Relevance | Analysis Application |
|---|---|---|---|
| **Bars / Nightclubs / Breweries** | `bar`, `pub`, `nightclub`, `brewery`, `winery` | DUI/impaired driving hotspots | Correlate with nighttime K+A crashes, alcohol-related crash patterns |
| **Gas Stations / Convenience Stores** | `gas_station`, `convenience_store` | Access management — frequent turning movements across traffic | Driveway-related crashes, sight distance issues, angle crashes |
| **Senior Living / Nursing Homes** | `nursing_home`, `assisted_living`, `senior_center` | Older driver + pedestrian vulnerability zones | Age-related crash patterns, pedestrian safety for elderly |
| **Daycare / Childcare Centers** | `daycare`, `childcare`, `preschool` | Similar to schools but NOT in Urban Institute data | Child pedestrian safety near non-school child facilities |

### Tier 2 — Significant Crash Relevance

| Category | Overture Filter | Crash Relevance | Analysis Application |
|---|---|---|---|
| **Parks / Recreation Centers** | `park`, `recreation_center`, `playground`, `sports_complex` | Pedestrian + bicycle activity generators | VRU (Vulnerable Road User) crash correlation, bike/ped safety |
| **Places of Worship** | `church`, `mosque`, `synagogue`, `temple` | Periodic high-volume traffic generators (weekends/evenings) | Special event traffic patterns, parking-related conflicts |
| **Shopping Centers / Big-box Retail** | `shopping_mall`, `department_store`, `supermarket` | High pedestrian activity, complex access, parking lots | Pedestrian crash generators, turning movement conflicts |
| **Hotels / Motels** | `hotel`, `motel`, `resort` | Unfamiliar drivers, tourist corridors | Driver familiarity as crash factor, wayfinding-related crashes |

### Tier 3 — Supporting Context

| Category | Overture Filter | Crash Relevance | Analysis Application |
|---|---|---|---|
| **Hospitals / Urgent Care** | `hospital`, `urgent_care`, `medical_center` | EMS response time analysis + high-traffic generators | Crash proximity to care, ambulance route safety |
| **Construction / Industrial** | `construction_supply`, `warehouse`, `industrial_park` | Heavy vehicle traffic generators | Truck-related crash patterns, sight distance issues |
| **Event Venues** | `stadium`, `arena`, `concert_hall`, `convention_center` | Intermittent traffic surges | Event-related crash spikes, temporary traffic management needs |

---

## 6. Features Affected by Integration

### 6a. Crash Tree (Significant Impact)

**Current state:** Risk factor "High Speed (>=55 mph)" uses crash record speed field (often empty).

**With Overture:**
- Snap each crash to nearest Overture road segment
- Use segment's `speed_limit_mph` as the posted speed at crash location
- Risk factor becomes data-driven: "X% of crashes occurred on roads with posted speed >= 55 mph"
- Facility tree's "Urban vs Rural" split enriched by Overture road class (motorway/primary = arterial, residential = neighborhood)

**Implementation:** In `analyzeRiskFactors()` (~line 83990), replace speed check with Overture speed limit lookup:
```javascript
// Before: relies on crash record speed field
// After: uses Overture-enriched speed limit
const enrichment = crashState.overtureEnrichment?.get(rowIndex);
const speedLimit = enrichment?.speedLimitMph || null;
if (speedLimit && speedLimit >= 55) highSpeedCount++;
```

### 6b. Segment Analysis (Major Improvement)

**Current state:** Calls OSM Overpass API (5 endpoints, 5-15 second wait, frequent failures).

**With Overture:**
- Add Overture as priority source in `API_FALLBACK_CHAINS.roads` (line 25849)
- `buildRouteMapFromOverture()` converts pre-cached segments to the existing `osmRouteData` format
- Instant results, no network call, includes speed limit data
- OSM Overpass remains as fallback if Overture data isn't loaded

**Implementation:** Modify `analyzeOverRepSegments()` (~line 50896):
```javascript
if (overtureState.loaded && overtureState.segmentsByName.size > 0) {
    segmentAnalysisState.osmRouteData = buildRouteMapFromOverture();
} else {
    segmentAnalysisState.osmRouteData = await fetchOSMCenterlineData(bounds); // existing fallback
}
```

### 6c. Hotspot Analysis (New Capability)

**Current state:** Aggregates by route name string. Cannot calculate crashes per mile.

**With Overture:**
- After `analyzeHotspots()` runs, call `enrichHotspotsWithOvertureRoadLength()`
- Each hotspot gets: `overtureRoadLengthMi`, `overtureCrashRate` (crashes/mile), `overtureSpeedLimit`, `overtureRoadClass`
- New columns in hotspot table: Road Class, Speed Limit, Crashes/Mi

### 6d. AI Assistant Context

**With Overture:**
- `getAIAnalysisContext()` (~line 69555) includes road class, speed limit, and nearby POIs
- AI can factor "This is a 55 mph arterial near 3 bars and a senior living facility" into its safety recommendations
- More targeted countermeasure suggestions

### 6e. CMF/Countermeasures Tab

**With Overture:**
- `buildCMFCrashProfile()` (~line 75984) includes Overture road context
- Road class and speed limit data helps recommend appropriate countermeasures
- Example: "Install pedestrian hybrid beacon" recommended specifically because crashes are near senior facility on 45 mph road

---

## 7. Implementation Architecture (UPDATED: Real-Time PMTiles)

### 7a. Data Flow (No Pre-Saved Files)

```
[Overture Maps PMTiles on S3]       <-- Official cloud-hosted vector tiles
        |  (HTTP range requests — only tiles for jurisdiction viewport)
        v
[pmtiles.js library]                <-- Loaded via CDN in browser
        |  (fetches specific z/x/y tiles for bbox)
        v
[OvertureVTDecoder]                 <-- Inline MVT protobuf decoder
        |  (decode vector tiles → GeoJSON features)
        v
[app/index.html]                    <-- Real-time display, same as BTS layers
  ├── OVERTURE_ENDPOINTS config      (3 layer definitions)
  ├── builtInLayersState entries     (per-jurisdiction caching)
  ├── Asset Panel checkboxes         ("Overture Maps Data" section)
  ├── L.geoJSON display              (interactive popups, boundary clipping)
  └── Jurisdiction-change auto-reload
```

**Key change from original plan:** No Python pipeline, no pre-saved GeoJSON files,
no R2 storage needed. Data is fetched real-time from Overture's S3 PMTiles via
HTTP range requests when the user enables a layer. Cached per jurisdiction in memory.

### 7b. New State Object: `overtureState`

```javascript
const overtureState = {
    loaded: false,
    isLoading: false,
    metadata: null,                    // metadata.json contents
    segments: null,                    // GeoJSON FeatureCollection
    connectors: null,                  // GeoJSON FeatureCollection
    infrastructure: null,              // GeoJSON FeatureCollection (crosswalks + barriers)
    places: null,                      // GeoJSON FeatureCollection (13 safety categories)

    // Derived indexes (built after loading)
    segmentsByName: new Map(),         // road name -> [features]
    segmentsByClass: new Map(),        // road class -> [features]
    speedLimitIndex: new Map(),        // segment ID -> speed limit mph
    connectorIndex: new Map(),         // connector ID -> {lat, lng, segmentIds}
    spatialGrid: new Map(),            // grid cell -> [segment features] (for fast snapping)

    // Map layer references
    layers: { segments: null, connectors: null, infrastructure: null, places: null },
    visibility: { segments: false, connectors: false, infrastructure: false, places: false },

    loadError: null
};
```

### 7c. Key New Functions (all `overture` prefixed per CLAUDE.md)

| Function | Purpose | Uses |
|---|---|---|
| `loadOvertureData()` | Load all GeoJSON files in parallel, build indexes | Called after crash data loads |
| `buildOvertureIndexes()` | Build name/class/speed/grid lookup indexes | Called by loadOvertureData |
| `overtureSnapCrashToRoad(lat, lng)` | Snap crash to nearest segment, return speed/class | turf.nearestPointOnLine |
| `overtureEnrichAllCrashes()` | Batch-enrich all crashes (batches of 500) | Stores in crashState.overtureEnrichment |
| `overtureNearbyPOIs(lat, lng, radius)` | Find POIs within radius | turf.buffer + pointsWithinPolygon |
| `overtureAnalyzePOIProximity(crashes)` | Aggregate POI counts for crash set | Used by AI context + CMF |
| `buildRouteMapFromOverture()` | Convert segments to osmRouteData format | Used by segment analysis |
| `enrichHotspotsWithOvertureRoadLength()` | Add crashes/mile to hotspots | Called after analyzeHotspots |
| `toggleOvertureLayer(key, show)` | Toggle map layer on/off | Asset Panel checkboxes |
| `displayOvertureLayer(key)` / `removeOvertureLayer(key)` | Render/remove Leaflet layers | L.geoJSON with styled features |

### 7d. Files to Create/Modify

| File | Action | Purpose |
|---|---|---|
| `download_overture_data.py` | **Create** | Python extraction pipeline (follows download_cdot_crash_data.py pattern) |
| `app/index.html` | **Modify** | All browser-side integration |
| `data/r2-manifest.json` | **Modify** | Add Overture file entries for R2 CDN |
| `.github/workflows/download-overture-data.yml` | **Create** | Monthly automated extraction (cron: 25th of month) |
| `data/overture/` | **Create directory** | Local Overture GeoJSON storage |

---

## 8. Python Pipeline Detail: `download_overture_data.py`

### CLI Interface
```bash
python download_overture_data.py                          # Default: Douglas County
python download_overture_data.py --jurisdiction elpaso    # Different county
python download_overture_data.py --types segments places  # Specific types only
python download_overture_data.py --release 2026-01-21.0   # Pin release version
python download_overture_data.py --simplify 0.00005       # Geometry simplification tolerance
python download_overture_data.py --list                   # Show available jurisdictions
```

### Processing Steps

**Segments:**
1. Download via `overturemaps download --type=segment`
2. Filter to `subtype == 'road'` (exclude rail, water)
3. Flatten: `names.primary` -> `name`, `speed_limits[0].max_speed.value` -> `speed_limit_mph`
4. Map `class` to FHWA functional classification
5. Calculate `length_miles` per segment via shapely
6. Simplify geometry (tolerance ~5m)

**Infrastructure:**
- Filter to: `traffic_signal`, `crossing`/`pedestrian_crossing`, `guard_rail`, `cable_barrier`, `jersey_barrier`
- Skip traffic signals if noted as already covered by Mapillary

**Places:**
- Filter to 13 safety-relevant categories from Tiers 1-3 (Section 5)
- Include: `name`, `category`, `geometry` (point)

**Metadata:**
```json
{
  "overtureRelease": "2026-01-21.0",
  "extractionDate": "2026-02-16T...",
  "jurisdiction": "douglas",
  "state": "colorado",
  "bbox": [-105.0543, 39.1298, -104.6014, 39.5624],
  "featureCounts": {
    "segments": 12450,
    "connectors": 8230,
    "infrastructure": 1560,
    "places": 3210
  },
  "totalSizeMB": 14.2,
  "simplificationTolerance": 0.00005,
  "placeCategories": ["bar", "pub", "nightclub", "brewery", "gas_station", "convenience_store",
    "nursing_home", "assisted_living", "senior_center", "daycare", "childcare", "preschool",
    "park", "recreation_center", "playground", "church", "mosque", "synagogue",
    "shopping_mall", "supermarket", "hotel", "motel", "hospital", "urgent_care",
    "stadium", "arena"]
}
```

### Dependencies
```
overturemaps>=0.18.0
geopandas>=0.14.0
shapely>=2.0.0
```

---

## 9. Phased Rollout

| Phase | Scope | Deliverable |
|---|---|---|
| **Phase 1** | Python data pipeline | `download_overture_data.py` + `data/overture/*.geojson` for Douglas County |
| **Phase 2** | R2 storage + manifest | Overture files on CDN, `r2-manifest.json` updated |
| **Phase 3** | Browser data loading | `overtureState`, `loadOvertureData()`, indexes built |
| **Phase 4** | Map layers | 4 toggleable layers in Asset Panel (Road Network, Intersections, Crosswalks/Barriers, Safety POIs) |
| **Phase 5** | Crash enrichment | Speed limit + road class snapped to every crash point |
| **Phase 6** | Crash Tree accuracy | Risk factor "High Speed" uses Overture speed limits |
| **Phase 7** | Segment analysis | Overture replaces OSM Overpass as primary road data source |
| **Phase 8** | Hotspot enhancement | Crashes/mile column using actual road geometry |
| **Phase 9** | POI proximity | Bar/senior/daycare/park proximity analysis for crash sets |
| **Phase 10** | AI + CMF context | Speed, class, and nearby POIs in AI recommendations |

---

## 10. Verification Plan

1. **Python pipeline:** Run for Douglas County. Verify 4 GeoJSON files created with expected feature counts
2. **Data loading:** Console shows `[Overture] Data loaded` with feature counts
3. **Map layers:** Toggle each on/off. Verify color-coded roads, POI markers with category icons, crosswalk/barrier markers
4. **Crash snapping:** Click crash marker, verify popup shows speed limit and road class
5. **Crash Tree:** Verify "High Speed" risk factor uses Overture speed limits (compare old vs new)
6. **Segment analysis:** Verify "Using Overture Maps road network" status, instant results, crash rates match
7. **Hotspots:** Verify Crashes/Mi column populated, Road Class and Speed Limit columns visible
8. **POI proximity:** Select a location, verify AI context includes nearby bars/senior facilities/parks
9. **Fallback:** Delete `data/overture/`, verify all features still work via existing OSM/Mapillary fallbacks
10. **No regression:** Navigate all tabs, verify nothing broken
