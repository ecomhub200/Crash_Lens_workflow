---
title: Trends & Temporal Analysis
type: concept
tags: [frontend, trends, temporal, year-over-year, analysis]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Trends & Temporal Analysis

## Purpose
Analyzes crash trends over time — severity direction, hourly/daily/monthly patterns, and year-over-year changes.

## Functions

### `calculateSeverityTrend(crashesByYear)`
Splits years into halves (older/recent), calculates avg crashes per year for each half.
- % change = (recentAvg - olderAvg) / olderAvg × 100
- Direction: **worsening** (>5%), **stable** (±5%), **improving** (<-5%)

### `analyzeTemporalPatterns(crashes)`
Returns: hourly (0-23), dayOfWeek (Sun-Sat), monthly (Jan-Dec), peaks, weekday/weekend stats.

### `calculateYearOverYearChange(crashesByYear)`
Per-year change from previous: `{year, count, change, pctChange}`

## Columns Used
| Column | Usage |
|--------|-------|
| crash_year | GROUP BY for yearly aggregation |
| crash_military_time | Hour extraction (first 2 digits) |
| crash_date | Date parsing for DOW, month |

## Supabase Queries
```sql
-- Yearly trend
SELECT crash_year, COUNT(*) FROM crashes WHERE state=... GROUP BY crash_year ORDER BY crash_year;

-- Hourly pattern
SELECT SUBSTRING(crash_military_time, 1, 2)::int AS hour, COUNT(*)
FROM crashes WHERE state=... GROUP BY 1 ORDER BY 1;
```

## Related Pages
- [[dashboard-tab]] — Displays trend charts
- [[baselines-analysis]] — Uses trend in severity trend multiplier
- [[grant-ranking]] — Trend direction affects grant scoring (×1.15 worsening)
