# CrashLens Workflow — Python File Descriptions

This document describes every root-level Python file in the CrashLens workflow repository. Use it as a reference when building workflow automations or Claude chat projects.

---

## Data Download Scripts

### `download_crash_data.py`
Downloads crash data from Virginia Roads ArcGIS API. Filters by jurisdiction and road type. Supports pagination (2000 records/request) and retry with exponential backoff. Reads jurisdiction configs from `config.json`.

**Usage:** `python download_crash_data.py --jurisdiction henrico`

### `download_virginia_crash_data.py`
Downloads Virginia crash data (CrashData_Basic) from virginiaroads.org via ArcGIS FeatureServer using Playwright browser automation to bypass VDOT bot detection. Discovers the live FeatureServer URL from network traffic, paginates all records via in-browser fetch(), and streams results to CSV.

**Usage:** `python download_virginia_crash_data.py --data-dir data --jurisdiction henrico --force`

### `download_cdot_crash_data.py`
Downloads statewide crash data from Colorado DOT's Hyland OnBase document management system. Filters to a specific Colorado county and saves as CSV. Doc IDs for each year are stored in `data/CDOT/source_manifest.json`.

**Usage:** `python download_cdot_crash_data.py --latest` or `--years 2023 2024` or `--jurisdiction elpaso`

### `download_moco_crashes.py`
Downloads Montgomery County, Maryland crash data (crash incidents, drivers, non-motorists) from the county's Open Data Portal via Socrata SODA API. Supports year filtering, per-dataset selection, gzip compression, and health check mode.

**Usage:** `python download_moco_crashes.py --dataset crashes --year 2024 --gzip`

### `download_cmf_data.py`
Downloads and transforms Crash Modification Factor (CMF) data from FHWA Clearinghouse. Normalizes 209 crash type values to ~15 standard tags, adds Virginia relevance scoring, and outputs optimized JSON for the web frontend.

**Usage:** `python download_cmf_data.py` or `--transform-only` or `--stats`

### `download_grants_data.py`
Downloads traffic safety grants from Grants.gov. Filters by transportation category, safety-related CFDA numbers (NHTSA 402, HSIP, SS4A, RAISE, etc.), and keywords. Supports multiple states with state-specific HSIP/402/405 programs.

**Usage:** `python download_grants_data.py`

---

## Data Generation / Processing Scripts

### `generate_osm_data.py`
Downloads ALL OpenStreetMap data needed for crash enrichment in one run: road network (FC, speed, surface, lighting, sidewalk, bike lanes), intersections (node degree), and Points of Interest (bars, schools, signals, hospitals, crosswalks). Outputs three parquet files per state.

**Usage:** `python generate_osm_data.py --state de` or `--all` or `--roads-only` or `--pois-only`

### `generate_hpms_data.py`
Downloads Highway Performance Monitoring System (HPMS) data from FHWA — the federal government's authoritative road database. Provides AADT, official Functional Class, Speed Limit, Through Lanes, Median Type, Surface Type, Ownership, Access Control. Uses a 3-source pipeline: geo.dot.gov ArcGIS (primary), FHWA shapefile (fallback), HM-10 ownership audit (validation). Outputs 63-column parquet per state.

**Usage:** `python generate_hpms_data.py --state de` or `--all`

### `generate_federal_data.py`
Downloads authoritative federal infrastructure data for crash enrichment from four sources: Schools (Urban Institute), Bridges (NBI via BTS), Rail Crossings (FRA via BTS), and Transit Stops (NTM via BTS). Produces per-state parquet caches. These upgrade OSM POI data with enrollment counts, bridge conditions, rail warning devices, and transit proximity.

**Usage:** `python generate_federal_data.py --state de --source schools` or `--all`

### `generate_state_dot_data.py`
Generic ArcGIS FeatureServer downloader for State DOT road inventory data. Works for any state via per-state config files at `states/{state}/{abbr}_state_dot.py`. Handles paginated download, geometry extraction (polyline to midpoint), field renaming, and state-specific normalization. Output is used as Tier A data in `build_road_inventory.py`.

**Usage:** `python generate_state_dot_data.py --state de`

### `generate_boundaries.py`
Downloads US boundary polygon geometries from Census TIGERweb and BTS/FHWA ArcGIS services: States (52), Counties (3,222), Places (~30,000), County Subdivisions (~36,000), and MPOs (~400). Enables vectorized point-in-polygon via geopandas.sjoin instead of row-by-row API calls. Outputs geoparquet files.

**Usage:** `python generate_boundaries.py --layer counties` or `--upload`

### `generate_road_database.py`
Unified Road Segment Database Builder (v3.0). Consolidates 8+ data sources (OSM Roads, HPMS, Intersections, POIs, Federal, Mapillary) into one parquet file per state. All spatial joins use KDTree batch queries. Builds in ~30s for Delaware.

**Usage:** `python generate_road_database.py --state de --cache-dir cache`

### `build_road_inventory.py`
Consolidates all cache files into one road database. Joins roads + intersections + State DOT + HPMS + POIs + bridges + rail + schools + transit + Mapillary into a single statewide parquet file, spatially linked by GPS. Follows a 4-tier data authority hierarchy: State DOT (Tier A, optional) > HPMS (Tier B) > OSM (Tier C) > POI Proximity (Tier D). Uses STRtree with KDTree fallback for spatial matching.

**Usage:** `python build_road_inventory.py --state de --upload`

---

## Enrichment & Matching Modules

### `crash_enricher.py`
Universal crash data enrichment module. Enriches ANY normalized crash dataset by deriving missing columns from GPS coordinates, contributing circumstances, temporal data, HPMS, and OSM. Follows a 3-tier data authority hierarchy: HPMS overwrites FC/Ownership (Tier A), State data preserved (Tier B), first-available fill (Tier C). Uses DuckDB spatial grid engine for 1M+ datasets to avoid RAM issues, with KDTree fallback.

**Imported as:** `from crash_enricher import CrashEnricher`

### `osm_road_enricher.py`
Given only GPS coordinates, enriches crash records with 15+ columns by querying OSM road network data: RTE Name, Functional Class, Facility Type, Intersection Type, Traffic Control, SYSTEM, Ownership, Alignment, Surface Type, Speed Limit, and more. Also derives hit-and-run, animal, speed, distracted, drowsy, and guardrail flags from contributing factor text. Uses KDTree spatial index on road segment midpoints.

**Imported as:** `from osm_road_enricher import OSMRoadEnricher`

### `road_inventory_enricher.py`
Single spatial join: crash GPS to nearest road segment, then transfers ALL road inventory columns. Uses `spatial_matcher.py` for tiered matching (DuckDB > GeoPandas > KDTree). Column transfer via vectorized pandas. Translates road inventory FC codes to VDOT-compatible format.

**Imported as:** `from road_inventory_enricher import ...`

### `spatial_matcher.py`
CrashLens Spatial Matching Engine (v5). Two-pass matching for speed + accuracy: Pass 1 uses DuckDB spatial SQL on LineStrings, Pass 2 uses STRtree (Shapely) to validate/correct. Results are tagged as "confirmed", "corrected", "duckdb", "strtree", or "kdtree". Fallback chain: DuckDB+STRtree > STRtree only > KDTree only.

**Imported as:** `from spatial_matcher import SpatialMatcher`

### `crash_road_join.py`
Spatial join: crash CSV + road inventory parquet. The ONLY slow step in the pipeline. Loads road inventory once via RoadInventorySession, runs Tier 1 self-enrichment, then per-county spatial join via KDTree + perpendicular refinement. Memory-budgeted for GitHub Actions (7GB). Designed to be run independently or on a faster machine.

**Usage:** `python crash_road_join.py --crashes normalized.csv --state de`

### `road_data_authority.py`
Data authority resolution layer for the road database. When multiple sources provide the same attribute, resolves using a 5-tier hierarchy: State DOT > HPMS > Mapillary > OSM > Federal Point Data. Produces resolved columns (speed_limit, lanes, surface_type, has_signal, has_lighting, on_bridge, school_zone) with source tracking. Includes sanity checks (speed 5-85, lanes 1-12, AADT 0-500K, etc.).

**Imported as:** `from road_data_authority import ...`

---

## Validation & Post-Processing

### `road_inventory_validator.py`
Quality gate for road inventory data BEFORE crash enrichment. Validates and fixes road inventory data using 12 rule groups: Speed (SPD), Surface (SURF), Ownership (OWN), SYSTEM (SYS), Facility Type (FAC), Description (DESC), Traffic Control (CTRL), Geometry (GEOM), Lanes (LANE), Route Name (RTE), AADT, and School Zone (SCHZ). Runs automatically in `build_road_inventory.py` and `road_inventory_enricher.py`.

**Usage:** `python road_inventory_validator.py --state de --fix`

### `road_inventory_postprocess.py`
State-agnostic post-processor applied AFTER build + validation. Fixes 13 systemic issues discovered in audit v12: RTE Name ranking, Ownership remapping, Intersection Type degree mapping, Alignment grading, Surface Type defaults, School Zone formatting, Jurisdiction resolution, Through Lanes sentinels, AADT propagation, sentinel cleanup, deduplication, geography cross-validation, and speed limit merging.

**Usage:** Called from `build_road_inventory.py` or standalone: `python road_inventory_postprocess.py --state de`

### `test_pipeline.py`
Comprehensive pipeline test suite. Validates every module, function, constant, cross-validation rule, and end-to-end integration using synthetic data. No network access or large files needed. Returns exit code 0 on success.

**Usage:** `python test_pipeline.py -v` or `--quick`

---

## Geography & Boundary Resolution

### `geo_resolver.py`
Universal geography resolution module for crash data normalization. Any state's normalize.py imports this to derive: Physical Juris Name, Juris Code, FIPS, Place FIPS, DOT District, Planning District, MPO Name, Ownership, and Area Type. Uses JSON geography data files (us_counties.json, us_places.json, us_mpos.json, etc.) and per-state hierarchy.json.

**Imported as:** `from geo_resolver import GeoResolver`

### `boundary_resolver.py`
Vectorized point-in-polygon resolution using pre-downloaded boundary polygons. Replaces row-by-row TIGERweb API calls with geopandas sjoin. Performance: 566K crashes x 3,222 counties in ~2 seconds (vs ~8 min with API calls). Resolves counties, places, MPOs, county subdivisions, states, and urban areas. Loads boundaries from local cache or R2.

**Imported as:** `from boundary_resolver import BoundaryResolver`

### `tigerweb_pip.py`
Universal GPS jurisdiction validator. Uses Census TIGERweb county boundaries for true point-in-polygon to validate and reassign crash GPS coordinates to the correct county. Three-tier fallback: local shapely PIP (fastest), TIGERweb REST API (accurate), centroid nearest-neighbor (always works). Smart batching with grid deduplication.

**Imported as:** `from tigerweb_pip import TIGERwebValidator`

---

## County-Level Downloaders

### `osm_county_download.py`
County-by-county OSM downloader. Downloads roads, intersections, and POIs one county at a time to avoid OOM. Supports resume, parallel workers, exponential backoff, and big-counties-first ordering. Saves checkpoints per county to R2 temp folder for crash recovery.

**Usage:** `python osm_county_download.py --state oh --resume --parallel 3`

### `mapillary_county_download.py`
County-by-county Mapillary traffic inventory downloader (signs, signals, lights). Parallel workers with resume support, exponential backoff, and big-counties-first ordering. Requires MAPILLARY_TOKEN. Outputs per-county and consolidated statewide parquet files.

**Usage:** `python mapillary_county_download.py --state va --parallel 2 --resume`

---

## Splitting & Output

### `split.py`
Universal jurisdiction and road type splitter (v3.0). Reads a statewide normalized CSV (115-column standard schema) and splits it into per-jurisdiction, per-road-type parquet.gz files matching the Cloudflare R2 folder structure the CrashLens frontend expects. Splits by: state, DOT region, MPO, planning district, city, and county — each with road type variants (all_roads, dot_roads, primary_roads, non_dot_roads, county_roads, city_roads, no_interstate).

**Usage:** `python split.py --state de --input normalized.csv`

---

## Templates

### `state_dot_template.py`
Template for adding a new State DOT road inventory downloader. Copy to `states/{state_name}/{abbr}_state_dot.py` and fill in the endpoint URL, field mappings, and value transforms. Called by `generate_state_dot_data.py`. Includes known ArcGIS endpoints for Delaware, Florida, Massachusetts, Texas, Virginia, and Washington.

### `state_normalize_template.py`
Template for adding a new state crash data normalizer. Copy to `states/{state}/{abbr}_normalize.py`. Handles the full pipeline: column mapping (source to 69 golden standard), state-specific transforms, crash ID generation, geography resolution (via geo_resolver.py), EPDO scoring, validation & auto-correction, and jurisdiction ranking.

---

## Notifications & Marketing

### `send_notifications.py`
Email notification system for scheduled reports, grant alerts, and weekly digests. Sends via Brevo (API mode preferred, SMTP fallback). Supports report, grant, digest, and test email types.

**Usage:** `python send_notifications.py --type reports` or `--type grants` or `--type digest`

### `send_marketing.py`
Email marketing campaign system for government agency contacts via Brevo's campaign API. Supports product launch, feature update, and demo invitation campaigns with open/click tracking, unsubscribe management, and analytics. Can sync subscribers to Brevo contact lists.

**Usage:** `python send_marketing.py --campaign product-launch` or `--sync-contacts`

---

## Registry & Configuration

### `states_registry.py`
Single source of truth for state metadata. Provides the STATES dict (`abbr -> (display_name, r2_prefix, state_fips)`) for all 51 states + DC. Also contains per-state statutory speed limit defaults from IIHS/FHWA (2024) as last-resort fallback. Imported by nearly every other module.

**Imported as:** `from states_registry import STATES, get_state, get_statutory_speed`

### `config.json`
Application configuration file containing API keys and settings for Mapbox, Google Maps, Mapillary, Firebase, and other services used by CrashLens.

---

## Pipeline Workflow Summary

```
1. DOWNLOAD DATA
   download_crash_data.py / download_virginia_crash_data.py / download_cdot_crash_data.py
   download_moco_crashes.py / download_cmf_data.py / download_grants_data.py

2. GENERATE ROAD DATA
   generate_osm_data.py        -> {abbr}_roads, _intersections, _pois parquet
   generate_hpms_data.py       -> {abbr}_hpms parquet (63 cols)
   generate_federal_data.py    -> {abbr}_schools, _bridges, _rail, _transit parquet
   generate_state_dot_data.py  -> {abbr}_state_dot parquet
   generate_boundaries.py     -> boundary polygons (counties, places, MPOs)
   mapillary_county_download.py -> traffic-inventory parquet
   osm_county_download.py     -> county-level OSM (for large states)

3. BUILD ROAD INVENTORY
   build_road_inventory.py     -> joins all sources into one road DB
   road_data_authority.py      -> resolves conflicting values
   road_inventory_validator.py -> quality gate (12 rules)
   road_inventory_postprocess.py -> fixes 13 systemic issues

4. NORMALIZE CRASH DATA
   state_normalize_template.py -> per-state normalizer
   geo_resolver.py             -> geography resolution
   boundary_resolver.py        -> vectorized PIP
   tigerweb_pip.py             -> GPS jurisdiction validation

5. ENRICH & JOIN
   crash_enricher.py           -> enrich crashes with road/POI/federal data
   osm_road_enricher.py        -> OSM-based enrichment
   crash_road_join.py          -> spatial join crashes to roads
   road_inventory_enricher.py  -> transfer road columns to crashes
   spatial_matcher.py          -> matching engine (DuckDB/STRtree/KDTree)

6. SPLIT & DEPLOY
   split.py                    -> split to R2 folder structure

7. NOTIFY
   send_notifications.py       -> email reports/alerts
   send_marketing.py           -> marketing campaigns

8. TEST
   test_pipeline.py            -> comprehensive pipeline validation
```
