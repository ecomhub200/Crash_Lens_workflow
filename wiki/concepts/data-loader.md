---
title: Data Loader Architecture
type: concept
tags: [frontend, data-loader, r2, supabase, data-flow, critical]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Data Loader Architecture

## Purpose
The data loader is the single entry point for all crash data in the CrashLens frontend. It fetches CSV files from Cloudflare R2, parses them, normalizes state-specific formats, and builds in-memory aggregates that every tab/module consumes. **This is the file that must change for the Supabase migration.**

## Key Files

| File | Role |
|------|------|
| `mcp-server/lib/data-loader.js` (23K) | MCP server data loading (Node.js) |
| `app/modules/upload/upload-pipeline.js` (31K) | Browser upload pipeline + R2 path construction |
| `app/modules/worker/csv-worker.js` (15K) | Web Worker for off-thread CSV parsing |
| `states/state_adapter.js` (46K) | State detection + column normalization |

## COL Constants — The Column Name Registry

Every data access in the frontend goes through the `COL` object. These are the CSV header names:

```javascript
COL = {
  ID:              'Document Nbr',
  YEAR:            'Crash Year',
  DATE:            'Crash Date',
  TIME:            'Crash Military Time',
  SEVERITY:        'Crash Severity',        // K, A, B, C, O
  K:               'K_People',
  A:               'A_People',
  B:               'B_People',
  C:               'C_People',
  COLLISION:       'Collision Type',
  WEATHER:         'Weather Condition',
  LIGHT:           'Light Condition',
  SURFACE:         'Roadway Surface Condition',
  ALIGNMENT:       'Roadway Alignment',
  ROAD_DESC:       'Roadway Description',
  INT_TYPE:        'Intersection Type',
  TRAFFIC_CTRL:    'Traffic Control Type',
  CTRL_STATUS:     'Traffic Control Status',
  WORKZONE:        'Work Zone Related',
  SCHOOL:          'School Zone',
  ALCOHOL:         'Alcohol?',
  BIKE:            'Bike?',
  PED:             'Pedestrian?',
  SPEED:           'Speed?',
  DISTRACTED:      'Distracted?',
  DROWSY:          'Drowsy?',
  HITRUN:          'Hitrun?',
  SENIOR:          'Senior?',
  YOUNG:           'Young?',
  NIGHT:           'Night?',
  UNRESTRAINED:    'Unrestrained?',
  MOTORCYCLE:      'Motorcycle?',
  DRUG:            'Drug Related?',
  GUARDRAIL:       'Guardrail Related?',
  LGTRUCK:         'Lgtruck?',
  ROAD_DEPARTURE:  'RoadDeparture Type',
  MAX_SPEED_DIFF:  'Max Speed Diff',
  FIRST_HARMFUL:   'First Harmful Event',
  FIRST_HARMFUL_LOC:'First Harmful Event Loc',
  ROAD_DEFECT:     'Roadway Defect',
  RELATION_TO_ROAD:'Relation To Roadway',
  ANIMAL:          'Animal Related?',
  VEHICLE_COUNT:   'Vehicle Count',
  PERSONS_INJURED: 'Persons Injured',
  PED_KILLED:      'Pedestrians Killed',
  PED_INJURED:     'Pedestrians Injured',
  FUNC_CLASS:      'Functional Class',
  AREA_TYPE:       'Area Type',
  FACILITY_TYPE:   'Facility Type',
  ROAD_SYSTEM:     'SYSTEM',
  OWNERSHIP:       'Ownership',
  ROUTE:           'RTE Name',
  NODE:            'Node',
  MP:              'RNS MP',
  NODE_OFFSET:     'Node Offset',
  X:               'x',
  Y:               'y',
  JURISDICTION:    'Physical Juris Name',
}
```

## R2 URL Patterns

**Base URL:** `https://data.aicreatesai.com`

### Current R2 Path Structure (by tier)

| Tier | R2 Path Pattern | Example |
|------|----------------|---------|
| Federal | `_national/all_roads.csv.gz` | `_national/all_roads.csv.gz` |
| State | `{state}/_state/statewide_all_roads.csv.gz` | `delaware/_state/statewide_all_roads.csv.gz` |
| Region | `{state}/_region/{entityId}/all_roads.csv.gz` | `virginia/_region/hampton_roads/all_roads.csv.gz` |
| MPO | `{state}/_mpo/{entityId}/all_roads.csv.gz` | `virginia/_mpo/hrtpo/all_roads.csv.gz` |
| Planning District | `{state}/_planning_district/{entityId}/all_roads.csv.gz` | `virginia/_planning_district/pd23/all_roads.csv.gz` |
| County | `{state}/{entityId}/all_roads.csv.gz` | `colorado/douglas/all_roads.csv.gz` |
| City/Town | `{state}/_city/{entityId}/all_roads.csv.gz` | `virginia/_city/richmond/all_roads.csv.gz` |

**Road type variants:** `all_roads.csv`, `county_roads.csv`, `no_interstate.csv`

### MCP Server R2 Download
```
GET https://data.aicreatesai.com/{state}/{jurisdiction}/{roadType}.csv
→ Saved to: ~/.crashlens/{state}/{jurisdiction}/data/{roadType}.csv
```

## Data Aggregation Structure

After CSV parsing, data is pre-aggregated into:

```javascript
aggregates = {
  bySeverity:   { K: n, A: n, B: n, C: n, O: n },
  byRoute:      { [routeName]: { total, K, A, B, C, O, collisions: {}, jurisdiction } },
  byNode:       { [nodeId]:    { total, K, A, B, C, O, collisions: {}, routes: Set, jurisdiction } },
  byCollision:  { [type]: count },
  byWeather:    { [condition]: count },
  byLight:      { [condition]: count },
  byYear:       { [year]: count },
  byHour:       { [0-23]: count },
  byDOW:        { [Sun-Sat]: count },
  byMonth:      { [Jan-Dec]: count },
  byFuncClass:  { [class]: count },
  byIntType:    { [type]: count },
  byTrafficCtrl:{ [type]: count },
  ped:          { total: n },
  bike:         { total: n },
  intersection: { total: n },
  totalRows:    n
}
```

## Map Points Structure

Each crash generates a map point:
```javascript
mapPoint = {
  lat, lng, sev, route, node, collision, date, time,
  isPed, isBike, isInt, weather, light, isSpeed, isYoung, isNight, docNum
}
```

## Filter System

`filterCrashes(options)` supports:
- `route` — partial match on RTE Name
- `node` — partial match on Node
- `severity` — array of K/A/B/C/O
- `date_start`, `date_end` — YYYY-MM-DD range
- `collision_type` — collision type match
- `weather` — weather condition match
- `factors` — array: ped, bike, alcohol, speed, distracted, night, hitrun

## Data Flow: Current (R2) vs Future (Supabase)

### Current Flow
```
User selects tier + jurisdiction + road type
  → buildR2DestinationPath() constructs R2 key
  → fetch() downloads CSV.GZ from R2
  → pako decompresses gzip
  → PapaParse/csv-worker.js parses CSV
  → StateAdapter normalizes columns
  → buildAggregates() pre-computes stats
  → Each tab reads from aggregates + raw rows
```

### Future Flow (Supabase)
```
User selects tier + jurisdiction + road type
  → PostgREST query with WHERE clauses
  → Server-side aggregation (GROUP BY, COUNT, SUM)
  → JSON response → directly into tab rendering
  → Heavy aggregation moves from browser → Postgres
```

## Related Pages
- [[frontend-supabase-migration]] — Migration plan
- [[frontend-data-inventory]] — Complete column inventory
- [[state-adapter]] — State detection/normalization
- [[upload-pipeline]] — Upload flow
- [[supabase-schema-v3]] — Database schema
