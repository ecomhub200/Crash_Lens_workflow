---
title: Safety Focus Categories
type: concept
tags: [frontend, safety, categories, filter, data-flow]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Safety Focus — 21 Categories

## Purpose
Analyzes crashes within specific safety focus areas (pedestrians, impaired driving, curves, etc.). Each category has a JS filter function that translates to a SQL WHERE clause. Used by Safety Focus tab and MCP `analyze_safety_category` tool.

## Category → Column → SQL Mapping

| Category | JS Filter Column(s) | SQL WHERE Clause |
|----------|---------------------|-----------------|
| **pedestrian** | COL.PED | `pedestrian = 'Yes'` |
| **bicycle** | COL.BIKE | `bike = 'Yes'` |
| **speed** | COL.SPEED | `speed = 'Yes'` |
| **impaired** | COL.ALCOHOL, COL.DRUG | `alcohol = 'Yes' OR drug_related = 'Yes'` |
| **intersection** | COL.INT_TYPE | `intersection_type IS NOT NULL AND intersection_type NOT LIKE '%not at%'` |
| **nighttime** | COL.NIGHT | `night = 'Yes'` |
| **distracted** | COL.DISTRACTED | `distracted = 'Yes'` |
| **curves** | COL.ALIGNMENT | `roadway_alignment LIKE '%Curve%'` |
| **workzone** | COL.WORKZONE | `work_zone_related LIKE '%Yes%'` |
| **school** | COL.SCHOOL | `school_zone IN ('Yes', 'working', 'obscured')` |
| **senior** | COL.SENIOR | `senior = 'Yes'` |
| **young** | COL.YOUNG | `young = 'Yes'` |
| **motorcycle** | COL.MOTORCYCLE | `motorcycle = 'Yes'` |
| **hitrun** | COL.HITRUN | `hitrun = 'Yes'` |
| **unrestrained** | COL.UNRESTRAINED | `unrestrained IN ('Yes', 'Y', '1', 'Unbelted')` |
| **drowsy** | COL.DROWSY | `drowsy = 'Yes'` |
| **guardrail** | COL.GUARDRAIL | `guardrail_related = 'Yes'` |
| **roaddeparture** | COL.ROAD_DEPARTURE | `road_departure_type IS NOT NULL AND road_departure_type != 'N/A'` |
| **lgtruck** | COL.LGTRUCK | `lgtruck = 'Yes'` |
| **animal** | COL.ANIMAL, COL.FIRST_HARMFUL | `animal_related = 'Yes' OR first_harmful_event ILIKE '%animal%'` |
| **weather** | COL.WEATHER | `weather_condition NOT IN ('Clear', 'Unknown', '')` |

## Per-Category Analysis Output

For each category, `analyzeSafetyCategory()` returns:
- Total crashes in category
- Severity distribution (K/A/B/C/O)
- EPDO score
- % of all crashes
- By-year breakdown
- Top 10 routes by EPDO
- Top 10 intersections by EPDO
- CMF keywords for countermeasure matching

## UI Components
- Safety location table (`safetyLocationBody`)
- Empty state: `safetyEmptyState`
- Exports: CSV, PDF

## Supabase Query Pattern
```sql
-- Example: Pedestrian safety overview
SELECT crash_year, crash_severity, COUNT(*) as cnt,
  SUM(k_people) as k, SUM(a_people) as a
FROM crashes
WHERE state = 'delaware' AND pedestrian = 'Yes'
GROUP BY crash_year, crash_severity
ORDER BY crash_year;
```

## Related Pages
- [[crash-profile]] — Detailed crash profiling
- [[cmf-countermeasures]] — CMF keyword matching
- [[frontend-data-inventory]] — Column mapping
