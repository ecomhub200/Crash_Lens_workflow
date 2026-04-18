---
title: Delaware Pipeline
type: entity
tags: [delaware, pipeline, reference-state, socrata]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Delaware Pipeline

**Reference state implementation** for the CrashLens data pipeline. Delaware serves as the proving ground for all pipeline modules before multi-state expansion.

## State Profile

| Attribute | Value |
|-----------|-------|
| Abbreviation | DE |
| FIPS Code | 10 |
| Counties | 3 (New Castle, Kent, Sussex) |
| Data Source | Socrata SODA API |
| Total Crashes | ~566,000 rows |
| Year Range | 2012–2024 |
| Pipeline Status | Fully operational |

## Data Source

Delaware crash data comes from a Socrata open data portal. Key characteristics:
- API type: SODA2 paginated CSV (`$limit`/`$offset` batching)
- Known issue: Socrata exports Crash Year with commas ("2,012" instead of "2012") — normalizer must strip commas after parse
- All road-attribute columns arrive empty — 100% of enrichment comes from HPMS, OSM, and POI tiers

## Pipeline Stages

1. **Download**: `download-delaware-crash-data.yml` fetches from Socrata API
2. **Normalize**: `de_normalize.py` — 86/86 bug tests passing, ranking engine v2 with 76 columns
3. **Enrich**: `crash_enricher.py` — 4-tier enrichment (Self → HPMS → OSM → POI), 98.6% fill rate on 566K rows
4. **Split**: `split.py` — 6 jurisdiction tiers, 2 road type sets
5. **Upload**: R2 parquet.gz files per jurisdiction
6. **Sync**: `supabase_sync.py` → `crashes_delaware` partition (Phase 1)

## Enrichment Results

| Tier | Source | Match Rate | Notes |
|------|--------|-----------|-------|
| Tier 1 | Self-enrichment | 100% | Boolean flags, severity cross-validation |
| Tier 3 | HPMS (2023) | ~95% | 75K segments including FC-7 local roads |
| Tier 2 | OSM | ~79% | Fills gaps left by HPMS |
| Tier 2b | POI | varies | 22,034 POIs across 13 categories |
| **Combined** | **All tiers** | **98.6%** | **26/26 local assertions passing** |

## Key Delaware-Specific Facts

- **FC-7 AADT at 72% zero is correct** — HPMS never collected traffic counts on residential/local streets; honest unknown data, not a bug
- **FC-7 state ownership at 44.6% is correct** — reflects DelDOT's atypical road maintenance model where the state maintains local roads that other states leave to counties
- **Road inventory**: 151,270 segments × 261 columns after `road_inventory_postprocess.py`
- **Non-DOT roads split**: all 5 FC→SYSTEM mappings verified

## Cache Files (R2)

| File | Path | Size |
|------|------|------|
| OSM Roads | `delaware/cache/de_roads.parquet.gz` | — |
| OSM Intersections | `delaware/cache/de_intersections.parquet.gz` | — |
| OSM POIs | `delaware/cache/de_pois.parquet.gz` | 22K POIs |
| HPMS Federal | `delaware/cache/de_hpms.parquet.gz` | 75K segments, 1.8 MB |

## CI Workflows

- `generate-osm-cache-delaware.yml` (timeout 360 min)
- `generate-hpms-cache-delaware.yml` (timeout 360 min)
- `download-delaware-crash-data.yml`

## Related Pages

- [[crash-lens-workflow]] — The pipeline repo
- [[data-pipeline-architecture]] — How the pipeline processes state data
- [[crash-enrichment]] — GPS-based enrichment methodology
- [[state-onboarding]] — The pattern Delaware established
- [[state-coverage]] — Multi-state expansion status


## Supabase Sync Results (2026-04-07)

Delaware was the first state synced to Supabase PostgreSQL. 566,762 rows loaded into `crashes_delaware` partition.

**Verification (24/24 queries passed):**
- Row count: 566,762
- Severity: O=476,563 / A=88,425 / K=1,774
- Year range: 2009–2025 (17 years)
- DOT Districts: North=335,725 / South=133,428 / Central=97,609
- Boolean flags: exact match (alcohol=21,733, speed=6,163, distracted=129,289)
- JSONB: road_data (312 keys), state_extras (18 de_* keys), ranking_data (76 keys)
- Federal summary: 1,726 aggregation rows

**Known issues:**
- min_lat outlier at 8.78 (geocoding error in source data, ~few rows)
- road_data key mangling via itertuples (`_NNN` names) — cosmetic, Tier 1 unaffected
- Direct psycopg2 blocked by Supavisor — used REST API workaround (`rest_sync.py`)

See [[supabase-schema-v3]] for full verification query results.


## Pipeline v2.8 First Run (2026-04-08)

| Stage | Status | Details |
|-------|--------|---------|
| 0: Init Cache | ✅ | Cache initialized |
| 0.5: Download RI | ✅ | de_road_inventory.parquet.gz (20 MB) |
| 0.5: Enrich | ✅ | 558,771/566,762 matched (99%), 777.9s |
| 1-3: Split | ✅ | 368 parquet files, 149.1s |
| 4: R2 Upload | ✅ | 368/368 uploaded |
| 4.5: SSH Tunnel | ✅ | Tunnel established |
| 4.5: Supabase Sync | ❌ | `Tenant or user not found` (port 5432=Supavisor) |

**Fix queued:** Tunnel port 5432→6543 (direct Postgres). Also: R2 `.parquet.gz` cleanup, intersection_name derivation in build_road_inventory.py.


## Data Quality Audit Results (2026-04-09)

Full 30-query audit of crashes_delaware (566,762 rows). Key findings:

### Healthy (✅)
FC, Ownership, SYSTEM, Facility Type, Surface Type, Alignment, Road Description, Intersection Type, Area Type, AADT, Through Lanes — all at 98.6%. Pedestrian, Bike, Alcohol, Speed, Distracted, Night, Motorcycle, Guardrail, Animal, Work Zone flags all correctly populated.

### Fixes Needed
| Column | Issue | Root Cause | Fix |
|--------|-------|-----------|-----|
| Intersection Name | 0% | Case mismatch: road inventory `intersection_name` vs crash data `Intersection Name` | Rename in build_road_inventory.py |
| Node | 0% | DE has no LRS nodes; osm_u_node available but not mapped | Derive from osm_u_node in crash_enricher.py |
| Unrestrained? | 0% Yes | Values are "Belted"/"Unbelted" not "Yes"/"No" | Map in de_normalize.py |
| School Zone | 0% Yes | Values are "1. Yes"/"2. Yes - With School Activity" | Normalize in de_normalize.py |
| Weather | 100% bug | "1. No Adverse Condition..." doesn't match "Clear" filter | Normalize in de_normalize.py |

### Correct DE Behavior (not bugs)
Drowsy, Senior, Young, Hitrun, Lgtruck, RoadDeparture Type — all 0% because Delaware Socrata source doesn't track these attributes. Will have real values when Virginia/Colorado data loads.

### Spatial
- 1 coordinate outlier (lat=8.78) — geocoding error in source
- 7,991 crashes without geom — null lat/lon in source data
- MPO name duplicates: "Dover/Kent County MPO" vs "Dover / Kent County MPO"


## Batched Sync Results — Updated Numbers (2026-04-09)

**IMPORTANT: Numbers changed from previous sync due to updated source data.**

| Metric | Previous (v3.1) | Current (batched) | Delta |
|--------|----------------|-------------------|-------|
| Total rows | 566,762 | 569,829 | +3,067 |
| Geom | 558,771 (98.6%) | 569,829 (100%) | All geocoded |
| crash_date_parsed | 566,759 | 569,826 | 3 null dates |
| Severity K | 1,774 | 1,791 | +17 |
| Severity A | 88,425 | 88,872 | +447 |
| Severity O | 476,563 | 479,166 | +2,603 |
| Year range | 2009–2023 | 2009–2025 | +2 years |
| federal_summary | 1,726 rows | 103 rows | Changed GROUP BY? |
| Columns in parquet | 517 | 167 | ⚠️ Missing road inventory? |

### ⚠️ INVESTIGATION NEEDED: road_data JSONB
Batch logs show `Tier 2 (road_data): 0 keys` and only 167 columns total. Previous sync had 312 road_data keys from road inventory enrichment. Need to verify:
1. Is `road_data` JSONB empty (`{}`) for all rows?
2. Did the pipeline skip road inventory enrichment?
3. Was the statewide parquet built without `crash_enricher.py` Stage 8?

### ⚠️ INVESTIGATION NEEDED: federal_summary dropped from 1,726 → 103 rows
Could be because the matview definition changed, or because road attributes (FC, Ownership, Area Type) are now NULL (no road inventory = no FC enrichment).


## Data Quality Audit v2 (2026-04-10) — Schema Truth Document

Full 40-query audit. 569,829 rows. Created PDF Schema Truth Document as canonical column reference.

### Summary
| Metric | Value |
|--------|-------|
| Total rows | 569,829 |
| Year range | 2009–2025 |
| Severity | O=479,166 / A=88,872 / K=1,791 |
| Geom | 569,829 (100%) |
| Road inventory match | 561,791 (98.6%) |
| JSONB road_data | 195+ keys, avg 7,072 chars |

### Previous Fixes Confirmed ✅
Weather (1. Clear), Unrestrained (Yes/No), School Zone (Yes/No)

### Critical Issues → 9 Fixes Created
1. MPO name duplicates (WILMAPCO vs Wilmington Area Planning Council)
2. RTE Name 53.6% → fallback from road inventory
3. Place FIPS 0% → Census centroid lookup
4. Persons Injured / Vehicle Count 0% → derive from severity/collision type
5. Ped Killed/Injured 0% → derive from ped flag + severity
6. Road Departure Type 0% → derive from collision type + alignment
7. Surface Type Brick 31% bug → HPMS code 3 = Composite, not Brick
8. Node Offset 0% → transfer from ri_match_dist_ft
9. Relation To Roadway 3.8% → default "1. On Roadway"

### Not Fixable (source limitation)
first_harmful_event, first_harmful_event_loc, roadway_defect, traffic_control_status, b/c_people


## Correction: Intersection Name IS Populated (2026-04-08)

Earlier analysis showed 0% fill for Intersection Name — this was based on an older parquet file (`non_dot_roads` subset). The full Supabase dataset shows **96.5% Intersection Name fill** and **98.6% Node fill**. The road inventory enrichment pipeline works correctly. No fix needed to `build_road_inventory.py`.

### Current Supabase Status
- **519,829 rows** loaded (partial — 47K gap from earlier `rest_sync.py` load)
- **Pending**: Re-sync with corrected SSH tunnel port (5433) to load full 566,762 rows
- All severity, boolean flags, JSONB, and geographic columns verified correct (proportional to row count)

### Column Dictionary
Delaware's column dictionary will be at `states/de_columns.md` (per new convention: `states/{abbr}_columns.md`). Regenerate from statewide parquet (569K rows × 550 cols) after v2.7.3 pipeline run with Brick fix + curvature threshold applied.
