---
title: Dashboard Tab
type: concept
tags: [frontend, dashboard, ui, charts, data-flow]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Dashboard Tab

## Purpose
County-wide crash overview — the landing page after data loads. Shows aggregate statistics, charts, and comparison tables.

## UI Components

### Charts (8 Chart.js canvases)
| Canvas ID | Type | Data Source |
|-----------|------|-------------|
| `chartYoY` | Bar | Year-over-year crash count change |
| `chartKAYear` | Bar | K+A crashes by year (from bySeverity per year) |
| `chartDOW` | Bar | Day-of-week distribution |
| `chartMonth` | Bar | Monthly distribution |
| `chartFuncClass` | Bar | Functional class breakdown |
| `chartCollision` | Bar | Top 10 collision types |
| `chartWeather` | Pie | Weather conditions |
| `chartLight` | Pie | Light conditions |

### District Charts (4)
| Canvas ID | Type | Data Source |
|-----------|------|-------------|
| `chartDistrictTotal` | Bar | Total crashes per DOT district |
| `chartDistrictSeverity` | Stacked Bar | Severity by district |
| `chartDistrictDoughnut` | Doughnut | District proportions |
| `chartDistrictEPDO` | Bar | EPDO score per district |

### Tables
- Year-over-year stats table (`dashYearlyBody`)
- District matrix table (`districtMatrixBody`)
- Search results table (`dashSearchBody`)
- Functional class summary table (`funcClassBody`)

### Filter Controls
- `dashSearchYear` — Filter by year
- `dashSearchSeverity` — Filter by severity
- `dashSearchPedBike` — Ped/bike flag filter

### Export Buttons
- `dashExportSearchCSV()` — Export search results
- `exportDistrictMatrixCSV()` — Export district matrix

## Data Requirements

### Columns Used (Tier 1)
| Column | Usage |
|--------|-------|
| crash_year | GROUP BY for YoY, KA trend |
| crash_severity | GROUP BY for severity dist, filter |
| crash_date | DOW/month extraction |
| collision_type | Top 10 collision chart |
| weather_condition | Weather pie chart |
| light_condition | Light pie chart |
| functional_class | Func class bar chart |
| dot_district | District breakdown |
| k_people, a_people | K+A trend, EPDO |
| b_people, c_people | EPDO calculation |

### Supabase Queries
```
GET /crashes?state=eq.delaware&select=crash_year,count()&group_by=crash_year
GET /crashes?state=eq.delaware&select=crash_severity,count()&group_by=crash_severity
GET /crashes?state=eq.delaware&select=collision_type,count()&group_by=collision_type&order=count.desc&limit=10
```

## Related Pages
- [[data-loader]] — Data loading
- [[frontend-supabase-migration]] — Migration plan
- [[hotspot-analysis]] — Hotspot tab
