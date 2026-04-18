---
title: Column Registry (COLUMNS.md)
type: concept
tags: [columns, schema, naming, registry, truth-source]
created: 2026-04-15
updated: 2026-04-18
sources: [COLUMNS.md, road_data_authority.py, crash_enricher.py, build_road_inventory.py]
status: active
---

# Column Registry --- COLUMNS.md

Single source of truth for **every column name** in the CrashLens pipeline. Lives at the repo root as `COLUMNS.md` and is generated from a canonical Delaware run (`non_dot_roads`, 121,733 rows x 532 cols, dated 2026-04-15).

**Purpose**: prevent column-name mismatch bugs (e.g. the `sdot_Max Speed Diff` vs `sdot_Speed_Limit_Est` class of bug). Every pipeline script MUST reference `COLUMNS.md` instead of hardcoding or guessing column names.

## Relationship to Other Schema Docs

| Doc | Scope | Use For |
|---|---|---|
| `COLUMNS.md` (repo root) | **All 532 raw pipeline columns** by name, type, fill % | Naming reference, cross-references in code |
| `wiki/concepts/supabase-schema-v3.md` | 3-tier Supabase mapping (Tier 1 explicit / Tier 2 `road_data` JSONB / Tier 3 `state_extras` JSONB) | Database writes, TIER1_MAP |
| `wiki/entities/schema-truth-document.md` | Validated fill % / data issues on Delaware `crashes_delaware` | Data-quality reference |

`COLUMNS.md` is the naming/registry layer. `supabase-schema-v3.md` is the storage/mapping layer. They should agree on names --- if they diverge, `COLUMNS.md` is authoritative for the pipeline parquet, and `supabase-schema-v3.md` is authoritative for the Postgres table.

## Column Name Rules (from COLUMNS.md)

1. **Golden Schema (1-69)**: Title Case with spaces --- `Crash Severity`, `Max Speed Diff`
2. **Enrichment/resolved**: `snake_case` --- `resolved_speed_limit`, `ri_matched`
3. **HPMS**: `hpms_` prefix --- `hpms_speed_limit`, `hpms_aadt`
4. **State DOT raw**: `dot_` prefix --- `dot_road_name`, `dot_lanes`
5. **State DOT resolved**: `sdot_` prefix --- `sdot_Speed_Limit_Est`, `sdot_Through_Lanes`
6. **Mapillary**: `map_` prefix --- `map_signal_present`, `map_speed_limit_value`
7. **Rankings**: `{Tier}_Rank_{metric}` --- `Juris_Rank_total_crash`
8. **State extras**: `{abbr}_` prefix --- `de_Day_Of_Week_Code`
9. **POI proximity**: `Near_Poi{Type}_{radius}` --- `Near_PoiSignal_100ft`
10. **Federal proximity**: `Near_{Asset}_{radius}` --- `Near_Bridge_500ft`

Note that `sdot_*` columns intentionally keep Title Case with underscores/spaces (e.g. `sdot_Speed_Limit_Est`, `sdot_Functional Class`) to mirror their golden-schema counterparts. These are NOT typos --- code must match exactly.

## Cross-Reference Map (CRITICAL)

Columns that reference the same logical attribute across pipeline stages. Renaming one of these WITHOUT updating the other references will silently break enrichment.

| Road Inventory Column | Code Reference | Used In |
|---|---|---|
| `sdot_Speed_Limit_Est` | `road_data_authority.py` `resolve_speed_limit()` | Tier A speed |
| `sdot_Through_Lanes` | `road_data_authority.py` `resolve_lanes()` | Tier A lanes |
| `sdot_Roadway Surface Type` | `road_data_authority.py` `resolve_surface()` | Tier A surface |
| `sdot_RTE Name` | `road_data_authority.py` `merge_frontend_columns()` | Tier A route name |
| `sdot_Functional Class` | `road_data_authority.py` `merge_frontend_columns()` | Tier A FC |
| `sdot_Ownership` | `road_data_authority.py` `merge_frontend_columns()` | Tier A ownership |
| `map_signal_present` | `road_data_authority.py` `resolve_signals()` | Tier C signal |
| `map_speed_limit_value` | `road_data_authority.py` `resolve_speed_limit()` | Tier C speed |
| `hpms_speed_limit` | `road_data_authority.py` `resolve_speed_limit()` | Tier B speed |
| `maxspeed` | `road_data_authority.py` `resolve_speed_limit()` | Tier D speed (OSM) |
| `resolved_speed_limit` | `crash_enricher.py` -> `Max Speed Diff` | Frontend speed |
| `Intersection Name` | `crash_enricher.py` -> `RTE Name` fallback | Frontend route |

## Section Summary (532 columns total)

| Section | Col Range | Count | Source |
|---|---|---|---|
| Golden Schema: Identity | 1-5 | 5 | State adapter |
| Golden Schema: Severity | 6-14 | 9 | State adapter |
| Golden Schema: Crash Characteristics | 15-22 | 8 | State adapter |
| Golden Schema: Road Description | 23-26 | 4 | State adapter |
| Golden Schema: Work/School Zone | 27-30 | 4 | State adapter |
| Golden Schema: Harmful Events | 31-32 | 2 | State adapter |
| Golden Schema: Safety Flags | 33-45 | 13 | State adapter |
| Golden Schema: Analysis Fields | 46-52 | 7 | `crash_enricher.py` |
| Golden Schema: Geography | 53-63 | 11 | State adapter + FIPS |
| Golden Schema: Route/Node | 64-69 | 6 | State adapter |
| Enrichment Core | 70-76 | 7 | `crash_enricher.py` |
| Rankings | 77-152 | 76 | `crash_enricher.py` ranking module (4 scopes x 19 metrics) |
| State Extras (de_*) | 153-171 | 19 | `de_normalize.py` |
| Geometry & Matching | 172-232 | 11 | `build_road_inventory.py` |
| Resolved Attributes | 179-195 | 16 | `road_data_authority.py` |
| Confidence Scores | 196-201 | 6 | Cross-source validation |
| Cross-Validation Counts | 202-205 | 4 | Cross-source validation |
| Risk Indicators | 206-212 | 7 | Derived safety scores |
| Curve Analysis | 213-220 | 8 | Curvature classifier |
| Traffic Engineering | 221-228 | 8 | HPMS-derived |
| HPMS Federal Road Inventory | 233-286 | 54 | `generate_hpms_data.py` |
| Federal: Bridges | 287-297 | 11 | NBI/BTS, `generate_federal_data.py` |
| Federal: Rail Crossings | 298-305 | 8 | FRA/BTS, `generate_federal_data.py` |
| Federal: Schools | 306-311 | 6 | Urban Institute API |
| Federal: Transit | 312-316 | 5 | NTM/BTS |
| Proximity Flags | 317-327, 510-513 | 15 | `build_road_inventory.py` |
| POI Details | 328-368 | 41 | OSM, `build_road_inventory.py` |
| Mapillary Street-Level | 369-425 | 57 | `mapillary_county_download.py` |
| OSM Graph Nodes | 426-428 | 3 | `osmnx`, `generate_osm_data.py` |
| State DOT Raw (dot_*) — Delaware | 429-478 | 50 | DelDOT shapefile, `generate_state_dot_data.py` |
| State DOT Raw (dot_*) — Colorado | CO-1..CO-25 | 25 | CDOT Highways Layer 7, `states/colorado/co_state_dot.py` |
| State DOT Resolved (sdot_*) | 479-502 | 24 | `build_road_inventory.py` `enrich_state_dot()` |
| Frontend Merged | 503-522 | 16 | `merge_frontend_columns()` |
| Road & Node Matching (ri_*) | 523-532 | 10 | `crash_enricher.py` spatial match |

## Rules for Pipeline Code

1. **Never hardcode column names in module-level string constants** that don't reference `COLUMNS.md`. Prefer importing from a shared constants module that is itself generated/validated against `COLUMNS.md`.
2. **When renaming a column**: grep the repo for the old name, update `COLUMNS.md`, update `wiki/concepts/supabase-schema-v3.md` TIER1_MAP if applicable, update `wiki/log.md`.
3. **When adding a new column**: append to `COLUMNS.md` with section, type, and expected fill %, then update the section-summary table in this page.
4. **sdot_ columns are case-sensitive and contain spaces** --- do not normalize them to snake_case in enrichment code. The pipeline parquet preserves the exact names from `sdot_*` as shown in COLUMNS.md rows 479-502.
5. **Fill % is informational, not a contract** --- it represents Delaware non_dot_roads and will differ per state/jurisdiction.

## When to Update COLUMNS.md

- A new state adapter adds new `{abbr}_*` state extras
- A new data source adds a new prefix (e.g. future `nhtsa_*`, `faa_*`)
- An enrichment step adds, renames, or removes a column
- A pipeline upgrade changes column types or fill expectations

After updating `COLUMNS.md`, update this page's section-summary table and add a log entry.

## See Also

- `COLUMNS.md` (repo root) --- the registry itself
- [[supabase-schema-v3]] --- 3-tier Postgres mapping
- [[schema-truth-document]] --- validated Delaware fill %
- [[build-road-inventory]] --- how `dot_*` -> `sdot_*` resolution happens
- [[crash-enrichment]] --- how `resolved_*` and `ri_*` columns are produced
