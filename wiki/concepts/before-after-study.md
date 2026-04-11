---
title: Before/After Study (Batch)
type: concept
tags: [frontend, before-after, analysis, cmf, spatial]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Before/After Study — Batch Analysis

## Purpose
Evaluates effectiveness of safety treatments (countermeasures) by comparing crash counts before and after installation. Supports multiple locations with configurable study periods.

## Module Files (8)
- `batch-ba-engine.js` — Core analysis (spatial filter, CMF/CRF calc, significance)
- `batch-ba-duration.js` — Study period configuration UI
- `batch-ba-charts.js` — 5 Chart.js visualizations
- `batch-ba-export-csv.js` — CSV export (30+ columns)
- `batch-ba-export-kml.js` — GIS KML export with color-coded zones
- `batch-ba-export-pdf.js` — Multi-page jsPDF report
- `batch-ba-export-pdf-details.js` — Detail tables + methodology appendix

## Analysis Engine

### Spatial Filtering
Uses Haversine distance to find crashes within radius of treatment location:
```javascript
_findCrashesInRadius(lat, lng, radiusMeters)
// Filters mapPoints by distance, using Earth radius 6371km
```

### Period Splitting
- **Before period**: beforeStart → (install_date - construction_buffer)
- **After period**: (install_date + construction_buffer) → afterEnd
- Min 12 months per FHWA recommendation

### CMF Calculation
```
expectedAfter = beforeTotal × (afterYears / beforeYears)
CMF = afterTotal / expectedAfter
CRF = (1 - CMF) × 100%
```

### Significance Testing
Poisson two-tailed z-test with configurable confidence level.

## Charts (5)
| Canvas | Type | Shows |
|--------|------|-------|
| Before/After Bar | Horizontal bar | Crash count comparison per location |
| CMF Distribution | Histogram | 11 bins from <0.5 to >1.5 |
| Severity Shift | Stacked bar | K/A/B/C/O before vs after |
| Scatter Plot | Scatter | Before (X) vs After (Y) with effectiveness zones |
| CMF by Type | Grouped bar | CMF grouped by countermeasure type |

## CSV Export Columns (30+)
location_name, lat, lng, countermeasure_type, install_date, radius_ft, before_start, before_end, after_start, after_end, before_years, after_years, before_total, after_total, change_pct, before_K/A/B/C/O/U, after_K/A/B/C/O/U, before_epdo, after_epdo, epdo_change_pct, cmf, crf, p_value, significant, effectiveness_rating

## Data Requirements
| Column | Usage |
|--------|-------|
| x, y | Spatial filtering (Haversine / PostGIS) |
| crash_date | Period splitting (before/after) |
| crash_severity | Severity stats |
| k_people, a_people, b_people, c_people | EPDO calculation |
| pedestrian, bike | Ped/bike flag tracking |

## Supabase Migration
```sql
-- Replace Haversine JS with PostGIS
SELECT * FROM crashes
WHERE state = 'delaware'
  AND ST_DWithin(ST_Point(x, y)::geography, ST_Point(?, ?)::geography, ?)
  AND crash_date BETWEEN ? AND ?;
```

## Related Pages
- [[crash-profile]] — Severity profiling
- [[cmf-countermeasures]] — CMF matching
- [[epdo-scoring]] — EPDO weights
