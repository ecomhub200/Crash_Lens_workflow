---
title: Frontend Supabase Migration Plan
type: concept
tags: [frontend, supabase, migration, architecture, critical]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo, source-pipeline-repo]
---

# Frontend → Supabase Migration Plan

## Overview
Migrate the CrashLens frontend from R2 CSV file fetching to Supabase PostgREST queries. This moves heavy aggregation from browser JavaScript to PostgreSQL server-side.

## What Changes

### Before (R2)
```
User picks jurisdiction → R2 URL built → CSV downloaded (5-50MB)
→ PapaParse/Web Worker parses all rows → buildAggregates() in browser
→ Each tab filters/groups in JavaScript
```

### After (Supabase)
```
User picks jurisdiction → PostgREST query with WHERE/GROUP BY
→ JSON response (1-100KB) → directly render charts/tables
→ Detail queries on demand (lazy loading)
```

## R2 Path → SQL WHERE Clause Mapping

| R2 Concept | R2 Path Component | SQL Equivalent |
|---|---|---|
| State | `{state}/` folder | `WHERE state = 'delaware'` |
| County | `{state}/{county}/` folder | `WHERE state = 'delaware' AND physical_juris_name = '...'` |
| Region | `{state}/_region/{id}/` | `WHERE state = 'virginia' AND planning_district IN (...)` |
| MPO | `{state}/_mpo/{id}/` | `WHERE state = 'virginia' AND mpo_name = '...'` |
| Planning District | `{state}/_planning_district/{id}/` | `WHERE state = 'virginia' AND planning_district = '...'` |
| City | `{state}/_city/{id}/` | `WHERE state = 'virginia' AND physical_juris_name LIKE '%city%'` |
| Road Type: all_roads | `all_roads.csv` | No additional filter |
| Road Type: county_roads | `county_roads.csv` | `AND system = 'Non-DOT secondary'` |
| Road Type: no_interstate | `no_interstate.csv` | `AND system != 'Interstate'` |

## Module-by-Module Migration

### Dashboard Tab — 8 charts + tables
**Current:** Reads pre-built aggregates from `buildAggregates()` on full dataset.
**Future queries:**
```sql
-- Severity distribution (pie)
SELECT crash_severity, COUNT(*) FROM crashes WHERE state=... AND physical_juris_name=... GROUP BY crash_severity;

-- Year over Year
SELECT crash_year, COUNT(*) FROM crashes WHERE state=... GROUP BY crash_year ORDER BY crash_year;

-- K+A by Year
SELECT crash_year, SUM(k_people) as k, SUM(a_people) as a FROM crashes WHERE state=... GROUP BY crash_year;

-- Collision types (top 10)
SELECT collision_type, COUNT(*) as cnt FROM crashes WHERE state=... GROUP BY collision_type ORDER BY cnt DESC LIMIT 10;

-- Weather/Light (pie charts)
SELECT weather_condition, COUNT(*) FROM crashes WHERE state=... GROUP BY weather_condition;
SELECT light_condition, COUNT(*) FROM crashes WHERE state=... GROUP BY light_condition;

-- Day of Week / Monthly / Functional Class
SELECT EXTRACT(DOW FROM crash_date::date), COUNT(*) FROM crashes WHERE state=... GROUP BY 1;
SELECT EXTRACT(MONTH FROM crash_date::date), COUNT(*) FROM crashes WHERE state=... GROUP BY 1;
SELECT functional_class, COUNT(*) FROM crashes WHERE state=... GROUP BY functional_class;
```

### Hotspot Tab
**Current:** `scoreAndRank()` on `byNode` / `byRoute` aggregates.
**Future:**
```sql
-- Top intersections by EPDO
SELECT node, rte_name, COUNT(*) as total,
  SUM(k_people) as k, SUM(a_people) as a, SUM(b_people) as b, SUM(c_people) as c,
  (SUM(k_people)*883 + SUM(a_people)*94 + SUM(b_people)*21 + SUM(c_people)*11 + COUNT(*)) as epdo
FROM crashes WHERE state=... AND node IS NOT NULL
GROUP BY node, rte_name HAVING COUNT(*) >= 5
ORDER BY epdo DESC LIMIT 20;
```

### Safety Focus — 21 categories
**Current:** Each category has a JS filter function checking boolean flags.
**Future:** Each becomes a WHERE clause:
```sql
-- Pedestrian crashes
SELECT crash_year, crash_severity, COUNT(*) FROM crashes
WHERE state=... AND pedestrian = 'Yes' GROUP BY crash_year, crash_severity;

-- Impaired (alcohol OR drug)
WHERE (alcohol = 'Yes' OR drug_related = 'Yes')

-- Curves
WHERE roadway_alignment LIKE '%Curve%'

-- Work zones
WHERE work_zone_related LIKE '%Yes%'
```

### CMF / Countermeasures
**Current:** Loads crash profile, matches to local CMF JSON database.
**No Supabase change needed** — CMF database is a static JSON file (808 countermeasures), not crash data.
**Crash profile query** feeds into CMF matching:
```sql
SELECT collision_type, COUNT(*) as cnt,
  SUM(CASE WHEN crash_severity='K' THEN 1 ELSE 0 END) as k,
  SUM(CASE WHEN crash_severity='A' THEN 1 ELSE 0 END) as a
FROM crashes WHERE state=... AND rte_name=... GROUP BY collision_type;
```

### Before/After Study
**Current:** Spatial filter (Haversine) on all mapPoints.
**Future:** PostGIS spatial query:
```sql
SELECT * FROM crashes
WHERE state=... AND ST_DWithin(
  ST_Point(x, y)::geography,
  ST_Point(-75.5, 39.7)::geography,
  150  -- radius in meters
) AND crash_date BETWEEN '2020-01-01' AND '2023-12-31';
```

### Grant Ranking
**Current:** Calculates ORI, PSI, significance from baselines.
**Future:** ranking_data JSONB provides pre-computed rankings:
```sql
SELECT ranking_data->>'Juris_Rank_total_crash' as crash_rank,
       ranking_data->>'Juris_Rank_total_epdo' as epdo_rank
FROM crashes WHERE state=... AND node=... LIMIT 1;
```

### Trends
**Current:** `calculateSeverityTrend()` splits years into halves.
**Future:**
```sql
SELECT crash_year, COUNT(*) as total,
  SUM(k_people) + SUM(a_people) as ka
FROM crashes WHERE state=... GROUP BY crash_year ORDER BY crash_year;
```

## Performance Gains

| Operation | R2 (Browser) | Supabase (Server) |
|---|---|---|
| Initial load | 5-50MB CSV download + parse | ~1KB aggregate query |
| Severity pie | Full scan of 500K rows in JS | `GROUP BY` on indexed column |
| Hotspot ranking | Sort 10K locations in memory | `ORDER BY ... LIMIT` with index |
| Spatial filter (B/A) | Haversine on every mapPoint | PostGIS `ST_DWithin` with spatial index |
| Year trend | Build `byYear` from scratch | `GROUP BY crash_year` on indexed column |

## Migration Phases

1. **Phase 1:** Add Supabase client to data-loader, keep R2 as fallback
2. **Phase 2:** Replace aggregate queries (Dashboard, Hotspots, Trends)
3. **Phase 3:** Replace detail queries (crash profile, map points)
4. **Phase 4:** Replace spatial queries (B/A study)
5. **Phase 5:** Remove R2 dependency, deprecate CSV parsing

## Related Pages
- [[data-loader]] — Current data loading architecture
- [[frontend-data-inventory]] — Column mapping
- [[supabase-schema-v3]] — Database schema
