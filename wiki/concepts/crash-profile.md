---
title: Crash Profile Analysis
type: concept
tags: [frontend, analysis, crash-profile, data-flow]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Crash Profile Analysis

## Purpose
Builds detailed statistical profiles for locations (routes, intersections) or county-wide aggregates. Used by Analysis tab, CMF recommendations, grant scoring, and MCP tools.

## Functions (4 variants)

### 1. `buildCountyWideCrashProfile(aggregates, totalRows)`
County-level aggregate profile. Returns: totalCrashes, fatalCount, seriousCount, kaPercent, anglePercent, pedPercent, bikePercent, nightPercent, speedPercent, etc.

### 2. `buildLocationCrashProfile(crashes)`
Simple location profile. Returns: total, K, A, B, C, O, anglePercent, pedCount, bikeCount, epdo.

### 3. `buildDetailedLocationProfile(crashes)`
Comprehensive profile with distributions. Returns nested object with:
- **severityDist**: K/A/B/C/O counts with EPDO weighting
- **collisionTypes**: { type: count } (excludes "Unknown")
- **contributingFactors**: { alcohol, speed, distracted, drowsy, unrestrained } counts
- **weatherDist**: { condition: count }
- **lightDist**: { condition: count }
- **pedInvolved / bikeInvolved**: counts
- **extended**: surface, alignment, road defects, harmful event location, driver factors, speed diffs, temporal peaks, weekday/weekend, year tracking, work zones, school zones

### 4. `buildCMFCrashProfile(cmfStateRef)`
Extended profile for CMF matching with all core + extended attributes.

## All Columns Referenced

| Column | Profile Level | Usage |
|--------|-------------|-------|
| crash_severity | All | COUNT GROUP BY |
| collision_type | All | COUNT GROUP BY |
| light_condition | All | COUNT GROUP BY, night detection |
| weather_condition | Detailed+ | COUNT GROUP BY |
| pedestrian | All | isYes() count |
| bike | All | isYes() count |
| alcohol | Detailed+ | isYes() count |
| speed | Detailed+ | isYes() count |
| distracted | Detailed+ | isYes() count |
| drowsy | Detailed+ | isYes() count |
| unrestrained | Detailed+ | isYes() count |
| drug_related | Extended | isYes() count |
| hitrun | Extended | isYes() count |
| senior | Extended | isYes() count |
| young | Extended | isYes() count |
| motorcycle | Extended | isYes() count |
| lgtruck | Extended | isYes() count |
| guardrail_related | Extended | isYes() count |
| road_departure_type | Extended | non-empty count |
| max_speed_diff | Extended | Numeric array, AVG |
| roadway_surface_cond | Extended | GROUP BY |
| roadway_alignment | Extended | GROUP BY |
| roadway_defect | Extended | GROUP BY |
| relation_to_roadway | Extended | GROUP BY |
| first_harmful_event_loc | Extended | GROUP BY |
| crash_military_time | Extended | Hour extraction, peak periods |
| crash_date | Extended | Weekday/weekend, by-year |
| crash_year | Extended | By-year aggregation |
| intersection_type | Extended | Intersection detection |
| work_zone_related | Extended | isYes() count |
| school_zone | Extended | isYes() count |
| k_people, a_people, b_people, c_people | All | EPDO calculation |

**Total: 32 distinct columns** — the most column-intensive module.

## Related Pages
- [[data-loader]] — Data loading
- [[baselines-analysis]] — Baseline rates
- [[cmf-countermeasures]] — CMF matching
- [[safety-focus]] — Safety categories
