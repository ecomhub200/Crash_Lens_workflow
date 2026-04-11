---
title: Frontend Data Inventory — Complete Column Mapping
type: analysis
tags: [frontend, columns, supabase, data-mapping, critical]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo, source-pipeline-repo]
---

# Frontend Data Inventory

Complete inventory of every column used by the CrashLens frontend, mapped to Supabase schema.

## Tier 1 Columns — Explicit Postgres Columns (Queryable)

These are the 69 "Golden" columns + enrichment/analysis columns. The frontend accesses them via CSV headers (left), which map to Postgres column names (right) via TIER1_MAP in `supabase_sync.py`.

| COL Constant | CSV Header (JS) | Postgres Column | Used By | Usage |
|---|---|---|---|---|
| ID | Document Nbr | `document_nbr` | All modules | Row identifier |
| YEAR | Crash Year | `crash_year` | Dashboard, Trends, All | Filter, GROUP BY |
| DATE | Crash Date | `crash_date` | Baselines, Trends, B/A | Filter, temporal |
| TIME | Crash Military Time | `crash_military_time` | Temporal analysis | Hour extraction |
| SEVERITY | Crash Severity | `crash_severity` | All modules | Filter, GROUP BY, COUNT |
| K | K_People | `k_people` | EPDO, Profile, All | SUM, EPDO calc |
| A | A_People | `a_people` | EPDO, Profile, All | SUM, EPDO calc |
| B | B_People | `b_people` | EPDO, Profile, All | SUM, EPDO calc |
| C | C_People | `c_people` | EPDO, Profile, All | SUM, EPDO calc |
| COLLISION | Collision Type | `collision_type` | Profile, CMF, Dashboard | GROUP BY, pattern match |
| WEATHER | Weather Condition | `weather_condition` | Profile, Dashboard, Safety | GROUP BY, filter |
| LIGHT | Light Condition | `light_condition` | Profile, Dashboard, Baselines | GROUP BY, filter |
| SURFACE | Roadway Surface Condition | `roadway_surface_cond` | Profile (extended) | GROUP BY |
| ALIGNMENT | Roadway Alignment | `roadway_alignment` | Safety (curves), Profile | Filter (curve detection) |
| ROAD_DESC | Roadway Description | `roadway_description` | — | Display |
| INT_TYPE | Intersection Type | `intersection_type` | Intersection tab, Safety | Filter, GROUP BY |
| TRAFFIC_CTRL | Traffic Control Type | `traffic_control_type` | Intersection tab | GROUP BY, filter |
| CTRL_STATUS | Traffic Control Status | `traffic_control_status` | Profile | Display |
| WORKZONE | Work Zone Related | `work_zone_related` | Safety, Profile | Filter (Y/1/Yes) |
| SCHOOL | School Zone | `school_zone` | Safety, Profile | Filter |
| ALCOHOL | Alcohol? | `alcohol` | Safety, Baselines, Profile, Grants | Filter (Y/1/Yes) |
| BIKE | Bike? | `bike` | Ped/Bike tab, Safety, All | Filter, COUNT |
| PED | Pedestrian? | `pedestrian` | Ped/Bike tab, Safety, All | Filter, COUNT |
| SPEED | Speed? | `speed` | Fatal/Speed tab, Safety, Grants | Filter (Y/1/Yes) |
| DISTRACTED | Distracted? | `distracted` | Safety, Profile | Filter (Y/1/Yes) |
| DROWSY | Drowsy? | `drowsy` | Safety, Profile | Filter (Y/1/Yes) |
| HITRUN | Hitrun? | `hitrun` | Safety, Profile | Filter (Y/1/Yes) |
| SENIOR | Senior? | `senior` | Safety, Profile | Filter (Y/1/Yes) |
| YOUNG | Young? | `young` | Safety, Profile, Worker | Filter (Y/1/Yes) |
| NIGHT | Night? | `night` | Safety, Baselines | Filter (Y/1/Yes) |
| UNRESTRAINED | Unrestrained? | `unrestrained` | Safety, Profile | Filter (Y/1/Yes/Unbelted) |
| MOTORCYCLE | Motorcycle? | `motorcycle` | Safety, Profile | Filter (Y/1/Yes) |
| DRUG | Drug Related? | `drug_related` | Safety, Profile, Baselines | Filter (Y/1/Yes) |
| GUARDRAIL | Guardrail Related? | `guardrail_related` | Safety, Profile | Filter (Y/1/Yes) |
| LGTRUCK | Lgtruck? | `lgtruck` | Safety, Profile | Filter (Y/1/Yes) |
| ROAD_DEPARTURE | RoadDeparture Type | `road_departure_type` | Safety, Profile | Filter (non-empty) |
| MAX_SPEED_DIFF | Max Speed Diff | `max_speed_diff` | Deep Dive, Profile | Numeric, AVG |
| FIRST_HARMFUL | First Harmful Event | `first_harmful_event` | Profile, Safety (animal) | Pattern match |
| FIRST_HARMFUL_LOC | First Harmful Event Loc | `first_harmful_event_loc` | Profile (extended) | GROUP BY |
| ROAD_DEFECT | Roadway Defect | `roadway_defect` | Profile (extended) | GROUP BY |
| RELATION_TO_ROAD | Relation To Roadway | `relation_to_roadway` | Profile (extended) | GROUP BY |
| ANIMAL | Animal Related? | `animal_related` | Safety (animal) | Filter (Y/1/Yes) |
| VEHICLE_COUNT | Vehicle Count | `vehicle_count` | Worker, Profile | COUNT |
| PERSONS_INJURED | Persons Injured | `persons_injured` | Worker | SUM |
| PED_KILLED | Pedestrians Killed | `pedestrians_killed` | Worker | COUNT |
| PED_INJURED | Pedestrians Injured | `pedestrians_injured` | Worker | COUNT |
| FUNC_CLASS | Functional Class | `functional_class` | Dashboard, Filter | GROUP BY, filter |
| AREA_TYPE | Area Type | `area_type` | CMF area filter | Filter |
| FACILITY_TYPE | Facility Type | `facility_type` | — | Display |
| ROAD_SYSTEM | SYSTEM | `system` | Filter profiles, Adapter | Filter (road type) |
| OWNERSHIP | Ownership | `ownership` | — | Display |
| ROUTE | RTE Name | `rte_name` | All modules | GROUP BY location |
| NODE | Node | `node` | All modules | GROUP BY location |
| MP | RNS MP | `rns_mp` | — | Display |
| NODE_OFFSET | Node Offset | `node_offset_ft` | — | Display |
| X | x | `x` | Map, B/A (spatial) | Coordinates |
| Y | y | `y` | Map, B/A (spatial) | Coordinates |
| JURISDICTION | Physical Juris Name | `physical_juris_name` | Filter, Aggregates | Filter, GROUP BY |

**Total Tier 1 columns used by frontend: 56 of 111 available**

## Additional Columns — Deep Dive Tab (State-Specific)

The Deep Dive tab uses Colorado-specific `_co_*` provenance columns stored in `state_extras` JSONB:

| Deep Dive Chart | Column (JS) | Supabase Location | Key |
|---|---|---|---|
| Driver Actions | _co_tu1_driver_action | state_extras | `de_*` / `_co_*` varies |
| Human Factors | _co_tu1_human_factor | state_extras | State-specific |
| Speed Scatter | _co_tu1_speed_limit, _co_tu1_estimated_speed | state_extras | State-specific |
| Non-Motorist Type | NM Type (CO-specific) | state_extras | State-specific |
| Age Distribution | TU-1 Age, TU-2 Age | state_extras | State-specific |
| Gender | TU-1 Gender, TU-2 Gender | state_extras | State-specific |
| Vehicle Type | TU-1 Vehicle Type | state_extras | State-specific |

## Tier 2 — road_data JSONB Fields Used

| Key | Used By | Purpose |
|-----|---------|---------|
| hpms_aadt | Infrastructure tools | AADT display |
| hpms_iri | Infrastructure tools | Road quality |
| hpms_design_speed | Infrastructure tools | Design speed |

*Note: Most road_data keys (312 total) are not directly accessed by the frontend UI — they're available for AI/MCP tool queries.*

## Tier 4 — ranking_data JSONB Fields

| Key Pattern | Used By | Purpose |
|---|---|---|
| `{Scope}_Rank_total_crash` | Grant ranking, Prediction | Location ranking |
| `{Scope}_Rank_total_epdo` | Grant ranking | EPDO-based ranking |
| `{Scope}_Rank_trend_ksi` | Grant ranking | KSI trend ranking |
| `{Scope}_Rank_safety_score` | Grant ranking | Composite safety score |

Scopes: `Juris`, `MPO`, `District`, `State`

## Boolean Flag Convention

The frontend uses `isYes(val)` which returns true for: `'Yes'`, `'Y'`, `'1'`, `'true'`

In Supabase, these are stored as TEXT. SQL equivalent: `WHERE alcohol = 'Yes'`

## Related Pages
- [[data-loader]] — How data is loaded
- [[frontend-supabase-migration]] — Migration plan
- [[supabase-schema-v3]] — Database schema


## Cross-Reference Results (2026-04-08)

Systematic comparison of frontend `COL` constants vs Supabase `TIER1_MAP`:

**57/58 columns match perfectly.** One mismatch:

| Frontend COL | CSV Header (frontend) | Pipeline CSV Header | TIER1_MAP Key | Postgres Column |
|---|---|---|---|---|
| NODE_OFFSET | `Node Offset` | `Node Offset (ft)` | `Node Offset (ft)` | `node_offset_ft` |

**Fix needed (low priority):** Either update frontend `COL.NODE_OFFSET` to `'Node Offset (ft)'`, or update pipeline golden column to `'Node Offset'` without the unit suffix. Impact: display-only column, not used for filtering or aggregation.

**26 Supabase columns not in frontend COL** (available for future features):
OBJECTID, Roadway Surface Type, Work Zone Location/Type, Intersection Analysis, Mainline?, DOT District, Juris Code, VSP, Planning District, MPO Name, FIPS, Place FIPS, EPDO_Score, Intersection Name, Through_Lanes, AADT, AADT_source, Lane_Width_ft, Median_Width_ft, Shoulder_Width_ft, Has_Sidewalk, Has_Bike_Lane, Urban_Area_Name, Urban_Area_GEOID.
