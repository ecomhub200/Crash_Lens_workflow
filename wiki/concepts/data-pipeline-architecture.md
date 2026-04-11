---
title: Data Pipeline Architecture
type: concept
tags: [pipeline, architecture, etl, unified-pipeline]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Data Pipeline Architecture

The **Unified Pipeline Architecture (v7)** is the core design of [[crash-lens-workflow]]. It standardizes how crash data flows from 30+ state DOT APIs into the [[douglas-county-frontend|web application]].

## Seven Stages

```
1. Download  →  2. Merge  →  3. Convert  →  4. Init Cache
                                                    ↓
7. Manifest  ←  6. Predict  ←  5. Upload  ←  Split by jurisdiction
```

### Stage 1: Download
- State-specific Python scripts call DOT APIs
- Virginia: ArcGIS REST API
- Colorado: OnBase document management system
- Maryland: Socrata SODA API
- Others: Various APIs, scraping (Playwright), manual uploads

### Stage 2: Merge
- Combine multi-file downloads into single dataset per state
- Handle year-over-year file formats

### Stage 3: Convert (Normalize)
- Map state-specific columns to standardized schema
- Derive severity levels from state-specific codes
- Standardize date/time formats, coordinate systems
- Config-driven via `states/{state}/config.json`

### Stage 4: Init Cache
- Build spatial indexes using DuckDB
- Create grid-based spatial partitions for efficient processing
- Memory-efficient: handles 1M+ crash records

### Stage 5: Split
- Partition by jurisdiction (county, city, MPO)
- Uses `states/{state}/hierarchy.json` for jurisdiction definitions

### Stage 6: Upload
- Push processed CSVs and Parquets to [[cloudflare-r2]]
- Uses custom GitHub Action (`upload-r2`)

### Stage 7: Predict (Optional)
- Generate crash forecasts using AWS SageMaker Chronos-2
- Produces time-series predictions per jurisdiction

## Orchestration

All stages are orchestrated via [[github-actions-ci]]:
- State download workflows trigger the unified pipeline
- Can run per-state or batch across all jurisdictions
- Manual and scheduled trigger support

## Related Pages

- [[crash-lens-workflow]] — The repo implementing this pipeline
- [[crash-enrichment]] — How records are enriched with road data
- [[state-onboarding]] — How new states are added
- [[cloudflare-r2]] — Where processed data lands


## Stage 4.5: Supabase Sync (v2.8)

Added in pipeline v2.8. Runs inline after R2 upload in each state's workflow:

```
Stage 4:   Upload to R2 (parquet.gz per jurisdiction)
Stage 4.5: SSH tunnel → supabase_sync.py → PostgreSQL
Stage 5:   Generate forecasts
```

`supabase_sync.py` reads the statewide parquet already on disk (no R2 re-download), classifies all columns into 3 tiers (111 explicit + 312 road_data JSONB + state_extras JSONB + 76 ranking_data JSONB), and bulk-inserts into the `crashes_{state}` partition via COPY or REST API.

**Requires GitHub secrets:** `SUPABASE_DB_PASSWORD`, `SUPABASE_SSH_KEY`

See [[supabase-schema-v3]] for schema, [[supabase-sync-ci]] for standalone workflow.


## Complete Pipeline Architecture v2.9 (2026-04-09)

### Pipeline Overview (5 phases, 8 shared modules)

```
Phase 0: Cache Generation (6 generators, monthly CI refresh)
  → generate_osm_data.py      (roads + intersections + 13 POI categories)
  → generate_hpms_data.py     (FHWA road inventory, 46 cols per segment)
  → generate_state_dot_data.py (state-specific road attributes)
  → generate_federal_data.py  (bridges, rail crossings, schools, transit)
  → generate_boundaries.py    (Census TIGER Urban/Suburban/Rural)
  → mapillary_county_download.py (traffic signs/signals imagery)

Phase 1: Build Road Inventory (run once, rebuild after cache updates)
  → build_road_inventory.py --state {abbr} --upload
  → Merges ALL caches, applies 4-tier Data Authority Hierarchy
  → Output: {abbr}_road_inventory.parquet.gz (151K segments × 394 cols for DE)

Phase 2: Download + Normalize (batch-all-jurisdictions.yml)
  → download_crash_data.py (registry-driven, Socrata/ArcGIS/CSV)
  → {abbr}_normalize.py (7 phases: rename, transforms, FIPS, GPS, IDs, EPDO, ranking)
  → Output: 167 cols to R2 _statewide/

Phase 3: Batch Pipeline (delaware-batch-pipeline.yml)
  → Stage 0.5: Download RI + crash_enricher.py (167 → 517 cols, 4-tier enrichment)
  → Stage 0.5b: Re-upload enriched statewide to R2 _statewide/
  → Stages 1-3: split.py (6 tiers × 2 road type sets)
  → Stage 4: Upload splits to R2 (SNAPPY parquet)

Phase 4: Supabase Sync (supabase-sync.yml, batched matrix)
  → Plan: count rows, create partition
  → Matrix: N batches × 25K rows, max-parallel:1
  → Finalize: geom, crash_date_parsed, matviews, states table
```

### Key Files Per State
| File | Location | Purpose |
|------|----------|---------|
| `hierarchy.json` | `states/{state}/` | Regions, MPOs, counties, cities, planning districts |
| `{abbr}_normalize.py` | `states/{state}/` | Column rename + value transforms to CrashLens standard |
| `download-registry.json` | `states/` | Download script, args, output patterns per state |
| `states_registry.py` | repo root | Abbreviation, name, FIPS lookup |
| 6 cache files | R2 `{state}/cache/` | OSM/HPMS/State DOT/Federal/Boundaries/Mapillary |
| Road inventory | R2 `{state}/cache/` | Consolidated `{abbr}_road_inventory.parquet.gz` |
| Enriched statewide | R2 `{state}/_statewide/` | `statewide_all_roads.parquet.gz` (517 cols) |
| Split files | R2 `{state}/{tier}/` | 368 parquet files per jurisdiction × road type |

### Data Authority Hierarchy (per column)
| Tier | Behavior | Columns |
|------|----------|---------|
| A: HPMS OVERWRITE | Always wins | FC, Ownership, SYSTEM, Facility Type, Surface Type |
| B: STATE WINS | Never overwritten | RTE Name, Node, Node Offset, RNS MP |
| C: FILL ONLY | First available | Speed, AADT, Lanes, Alignment, Description, Traffic Control |
| 2b: Federal POI | Proximity flags | Near_Bridge, Near_School, Near_Transit, Near_RailXing |

### Enrichment Order (crash_enricher.py)
| Tier | Source | Match Rate | What it fills |
|------|--------|-----------|---------------|
| 1 | Self | 100% | Boolean flags, K/A cross-validation, GPS clustering |
| 3 | HPMS | ~95% | FC, AADT, lanes, speed, ownership (OVERWRITE Tier A) |
| 2 | OSM | ~79% | Names, lighting, sidewalk, bridge (fills gaps) |
| 2b | POI | varies | Bar/school/crossing proximity, Traffic Control from signals |
| Post | Derived | 100% | Intersection Analysis (Not Intersection / Urban / DOT) |

### Column Count Progression
| Stage | Columns | Format |
|-------|---------|--------|
| Raw download | varies | CSV |
| After normalize | 167 | parquet.gz |
| After enrich | 517 | parquet.gz |
| Supabase Tier 1 | 111 | explicit Postgres cols |
| Supabase JSONB | +312 +18 +76 | road_data + state_extras + ranking_data |

### 13-Step New State Onboarding
1. Add to `states_registry.py` (abbr, name, FIPS)
2. Create `states/{state}/hierarchy.json`
3. Run `generate_osm_data.py --state {abbr}`
4. Run `generate_hpms_data.py --state {abbr}`
5. Run `generate_state_dot_data.py --state {abbr}`
6. Run `generate_federal_data.py --state {abbr}`
7. Run `generate_boundaries.py --state {abbr}`
8. Run `build_road_inventory.py --state {abbr} --upload`
9. Create `{abbr}_normalize.py` (from `state_normalize_template.py`)
10. Add entry to `states/download-registry.json`
11. Run `batch-all-jurisdictions.yml` (download + normalize + upload)
12. Run batch-pipeline (enrich + split + upload)
13. Supabase sync auto-runs (partition auto-created in plan job)
