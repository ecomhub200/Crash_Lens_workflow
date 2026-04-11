---
title: Hotspot Analysis
type: concept
tags: [hotspots, risk, analysis, safety, locations]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Hotspot Analysis

**High-risk location identification** — finds road segments and intersections with statistically significant crash concentrations.

## How It Works

The Hotspots tab in the [[douglas-county-frontend|frontend app]]:
1. Aggregates crashes by location (road segment or intersection)
2. Applies EPDO (Equivalent Property Damage Only) severity weighting
3. Ranks locations by crash frequency, severity, and rate
4. Identifies statistically significant clusters vs. random variation
5. Displays results on an interactive map with heatmap overlays

## EPDO Weighting

EPDO converts mixed-severity crash counts into a single weighted metric:
- Fatal crash = high weight multiplier
- Serious injury = medium weight
- Minor injury = lower weight
- Property damage only = baseline (1.0)

This ensures locations with fewer but more severe crashes are properly prioritized.

## Output

- Ranked list of high-risk locations
- Map visualization with severity-weighted clusters
- Feeds directly into [[safety-countermeasures]] matching
- Supports [[before-after-studies]] by establishing baseline crash rates

## Related Pages

- [[douglas-county-frontend]] — App with Hotspots tab
- [[safety-countermeasures]] — Treatments for identified hotspots
- [[crash-enrichment]] — Road data that enriches hotspot context
---
title: Hotspot Analysis
type: concept
tags: [frontend, hotspots, analysis, ranking, data-flow]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Hotspot Analysis

## Purpose
Identifies crash concentration locations (intersections and road segments) ranked by severity, EPDO, or frequency.

## UI Components
- Hotspot ranking table (`hotspotBody`)
- Segment table (`segmentBody`)
- Sort control: `hsSortBy` (epdo/total/ka/perYear)
- Group control: `hsGroupBy`
- Threshold: `segThreshold`, `segFuncClass`

### Exports
- `exportHotspotsCSV()`, `exportHotspotsPDF()`, `exportHotspotsToKML()`
- `generateHotspotReport()` — full report
- Detail exports: CSV, PDF, KML per selected hotspot

## Core Algorithm — `scoreAndRank()`

**File:** `app/modules/analysis/hotspots.js`

**Logic:**
1. Input: `byRoute` or `byNode` aggregate objects
2. Filter: WHERE total >= minCrashes
3. For each location: calculate EPDO, K+A count, per-year rate, top collision type
4. Sort descending by selected metric (epdo/total/ka/perYear)
5. Return ranked array

**Output per location:**
```javascript
{ loc, total, K, A, B, C, O, epdo, ka, perYear, topType, route, county }
```

## Data Requirements

### Columns Used
| Column | Usage |
|--------|-------|
| rte_name | GROUP BY for route-level hotspots |
| node | GROUP BY for intersection-level hotspots |
| crash_severity | Severity counts (K/A/B/C/O) |
| k_people, a_people, b_people, c_people | EPDO calculation |
| collision_type | Top collision type per location |
| physical_juris_name | County/jurisdiction display |

### Supabase Query
```sql
SELECT node, rte_name, physical_juris_name, COUNT(*) as total,
  SUM(k_people) as k, SUM(a_people) as a, SUM(b_people) as b,
  SUM(c_people) as c,
  (SUM(k_people)*883 + SUM(a_people)*94 + SUM(b_people)*21 + SUM(c_people)*11 + COUNT(*)) as epdo
FROM crashes
WHERE state = 'delaware' AND node IS NOT NULL
GROUP BY node, rte_name, physical_juris_name
HAVING COUNT(*) >= 5
ORDER BY epdo DESC LIMIT 20;
```

## Related Pages
- [[data-loader]] — Data loading
- [[crash-profile]] — Location analysis
- [[grant-ranking]] — Grant scoring
- [[dashboard-tab]] — Dashboard
