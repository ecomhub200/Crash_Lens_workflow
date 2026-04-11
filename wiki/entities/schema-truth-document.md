---
type: entity
tags: [supabase, schema, reference, delaware]
created: 2026-04-11
status: active
---

# Schema Truth Document

![[crashlens_schema_truth_document.pdf]]

Canonical reference for the `crashes_delaware` table schema. Generated April 11, 2026.

## Key Facts
- Total rows: 569,829
- Year range: 2009–2025
- Road inventory match rate: 98.6% (561,791/569,829)
- Tier 1: 111 explicit Postgres columns
- Tier 2: road_data JSONB (~195 keys, avg 7,072 chars)
- Tier 3: state_extras JSONB (18 keys, avg 530 chars)
- Ranking: ranking_data JSONB (avg 2,844 chars)

## Critical 0% Fill Columns
persons_injured, vehicle_count, pedestrians_killed, pedestrians_injured, place_fips, first_harmful_event, first_harmful_event_loc, road_departure_type, roadway_defect, traffic_control_status, node_offset_ft

## Known Data Issues
- mpo_name has duplicate values (WILMAPCO vs Wilmington Area Planning Council)
- roadway_surface_type shows 31% Brick or Block (suspiciously high — HPMS code 3 = Composite, not Brick)
- rte_name at 53.6% fill (nearly half missing)
- traffic_control_type at 32.9% (only controlled intersections)

## JSONB Key Groups (road_data)

| Group | Prefix | Key Count |
|-------|--------|-----------|
| Road Inventory Match | ri_* | 8 |
| State DOT | sdot_* | 22 |
| State DOT Raw | dot_* | 43 |
| HPMS Federal | hpms_* | 49 |
| Map Signs | map_* | 57 |
| POI Detail | poi_*, nearest_poi_* | 36 |
| Bridge | nearest_bridge_* | 11 |
| School | nearest_school_* | 6 |
| Rail Crossing | nearest_rail_xing_* | 8 |
| Transit | nearest_transit_* | 5 |
| Curve Analysis | curve_* | 8 |
| Confidence | conf_* | 6 |
| Resolved Sources | resolved_* | 9 |
| Risk Scores | risk_* | 7 |
| Cross-Validation | xval_* | 4 |
| Traffic Engineering | te_* | 7 |

## Related Pages
- [[supabase-schema-v3]]
- [[pipeline-architecture-v29]]
- [[delaware-pipeline]]
