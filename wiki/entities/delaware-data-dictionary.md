---
type: entity
tags: [delaware, data-dictionary, schema, reference]
created: 2026-04-11
updated: 2026-04-19
status: active
---

# Delaware Data Dictionary

Reference implementation — first state onboarded to CrashLens.

> **Column registry files:** The authoritative column lists for Delaware now live in two places:
> - `states/de_columns.md` — pipeline-parquet `dot_*` columns (50 cols, Per-State Columns Rule).
> - `states/delaware/de_columns.md` — deployed-Supabase registry for `crashes_delaware` (118 Tier 1 cols + 268 `road_data` / 15 `state_extras` / 76 `ranking_data` JSONB keys, 569,829 rows).
>
> Prefer those files as the source of truth for column names and fill %; the summary tables below are kept for readability but can lag.

## State Info
| Field | Value |
|-------|-------|
| State | Delaware |
| Abbreviation | de |
| FIPS | 10 |
| Source Portal | https://data.delaware.gov |
| Source Format | Socrata (SODA2 API) |
| Year Range | 2009–2025 |
| Total Rows | 569,829 |
| Road Inventory Match | 98.6% (561,791/569,829) |

## Data Source Characteristics
| Characteristic | Value |
|----------------|-------|
| Download method | Socrata SODA2 ($limit/$offset batching) |
| Update frequency | Monthly |
| Severity system | KA+O (no B/C) |
| B/C people available | No |
| Pedestrian detail | Flag only (ped killed/injured always 0) |
| Vehicle count available | No (always 0) |
| GPS coordinates | Yes (100%) |
| Unique ID field | OBJECTID (Socrata auto-increment) |

## Not-Tracked Fields (NULL, not "No")
- Drowsy?: Delaware source doesn't track drowsiness
- Senior?: Age-based flags not in Socrata export
- Young?: Age-based flags not in Socrata export
- Hitrun?: Not tracked separately
- Lgtruck?: Not tracked separately

## State Extras (18 de_* columns)
| Column | Fill Rate | Description |
|--------|-----------|-------------|
| de_Day_Of_Week_Code | 100% | Day of week numeric |
| de_Day_Of_Week_Description | 100% | Day of week text |
| de_Workers_Present | 100% | Workers present flag |
| de_Collision_On_Private_Property | 100% | Private property flag |
| de_Motorcycle_Helmet_Used | 100% | Helmet flag |
| de_Bicycle_Helmet_Used | 100% | Helmet flag |
| de_Crash_Classification_Code | 100% | Classification |
| de_School_Bus_Involved_Code | 100% | School bus flag |
| de_Primary_Contributing_Circumstance_Code | 97% | Contributing factor |
| de_Primary_Contributing_Circumstance_Description | 97% | Contributing factor text |
| de_Manner_Of_Impact_Code | 96% | Impact type |
| de_Lighting_Condition_Code | 96% | Original lighting |
| de_Road_Surface_Code | 96% | Original surface |
| de_Weather_1_Code | 96% | Primary weather |
| de_Weather_2_Code | 3.8% | Secondary weather |
| de_Weather_2_Description | 3.8% | Secondary weather text |
| de_Work_Zone_Type_Code | 0.7% | Conditional on WZ=Yes |
| de_Work_Zone_Location_Code | 0.7% | Conditional on WZ=Yes |

## Value Mappings (Delaware-specific)
| Column | Source Value | Standard Value |
|--------|-------------|----------------|
| Weather | 1. No Adverse Condition (Clear/Cloudy) | 1. Clear |
| Unrestrained | Unbelted | Yes |
| Unrestrained | Belted | No |
| School Zone | 1. Yes | Yes |
| School Zone | 2. Yes - With School Activity | Yes |
| Crash Year | 2,012 (Socrata comma) | 2012 |

## Fill Rate Summary
| Column | Fill % | Notes |
|--------|--------|-------|
| Functional Class | 98.6% | From road inventory |
| Ownership | 98.6% | From road inventory |
| AADT | 98.6% | From road inventory |
| RTE Name | 82.1% | Post-enrichment |
| Intersection Name | 96.6% | From road inventory |
| Through Lanes | 98.6% | From road inventory |
| Traffic Control Type | 32.9% | Source limitation |
| Lane Width | 77.1% | Secondary match tier |
| Median Width | 36.7% | Divided roads only |
| Shoulder Width | 62.1% | Moderate gap |

## Known Data Issues
| Issue | Severity | Details |
|-------|----------|---------|
| persons_injured always 0 | CRITICAL | Source doesn't provide, derive as A+B+C |
| vehicle_count always 0 | CRITICAL | Not in Socrata export |
| ped_killed/injured always 0 | CRITICAL | Flag exists but counts don't |
| mpo_name duplicates | MEDIUM | WILMAPCO vs Wilmington Area Planning Council |
| surface_type 31% Brick | MEDIUM | HPMS code 3 mapped as Brick not Composite |
| rte_name 53.6% | MEDIUM | Nearly half missing |
| node_offset_ft 0% | LOW | Cannot derive (lateral != longitudinal) |

## Supabase Sync
- Partition: `crashes_delaware`
- Sync method: VPS webhook (batched, 23 x 25K rows)
- 3-Tier: 111 explicit + road_data JSONB (195 keys) + state_extras JSONB (18 keys) + ranking_data JSONB (76 keys)
- geom: PostGIS Point from x/y coordinates (finalize step)
- Matviews: federal_summary, jurisdiction_baselines

## Pipeline Notes
- Normalizer: `de_normalize.py`
- Hierarchy: `states/delaware/hierarchy.json`
- R2 prefix: `delaware/`
- Partition: `crashes_delaware`
- Special: Socrata commas in Crash Year, "Unbelted" for restraint, 3 DOT districts (North/Central/South), 81 jurisdictions

## Related Pages
- [[schema-truth-document]]
- [[delaware-pipeline]]
- [[state-data-dictionary-template]]
- [[pipeline-architecture-v29]]
- [[webhook-sync]]
- [[column-registry]]
- `states/de_columns.md` — pipeline-parquet per-state registry
- `states/delaware/de_columns.md` — deployed-Supabase per-state registry
