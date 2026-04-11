---
title: Baselines & Statistical Analysis
type: concept
tags: [frontend, baselines, statistics, ori, psi, analysis]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Baselines & Statistical Analysis

## Purpose
Calculates county-wide baseline crash rates, then compares individual locations against those baselines using Over-Representation Index (ORI), Potential for Safety Improvement (PSI), and statistical significance testing.

## Functions

### `calculateCountyBaselines(sampleRows, aggregates)`
**Returns:**
- Percentages: pctK, pctA, pctKA, pctPed, pctBike, pctVRU, pctNight, pctImpaired, pctSpeed, pctAngle, pctHeadOn, pctRearEnd, pctRunOff, pctWet
- Per-location stats: avgCrashesPerIntersection, stdCrashesPerIntersection, avgEPDOPerIntersection, avgCrashesPerSegment, stdCrashesPerSegment
- crashesByYear, yearCount

### `calculateORI(patterns, baselines)`
Over-Representation Index: `ORI = (local_count / total) / baseline_rate`
- ORI > 1.0 = over-represented
- ORI > 1.5 = significantly over-represented

### `testPatternSignificance(patterns, baselines, alpha=0.10)`
Binomial test with continuity correction.
Returns p-value, significant flag, confidence level (high/medium/low/none).

### `calculatePSI(locationData, baselines)`
Empirical Bayes estimate of safety improvement potential.
- Weight w = overdispersion / (overdispersion + expected)
- EB = w × expected + (1-w) × observed
- PSI = EB - expected
- Critical threshold = expected + 1.645√expected + 0.5

## Columns Used
| Column | Usage |
|--------|-------|
| crash_severity | K/A/B/C/O counting |
| pedestrian, bike | VRU baseline rate |
| light_condition | Night detection (regex: dark\|night) |
| collision_type | Angle/headon/rearend/runoff (regex) |
| weather_condition | Wet surface detection (regex: wet\|rain\|snow\|ice) |
| alcohol, drug_related | Impaired rate |
| speed | Speed rate |
| crash_date | Year extraction for trend |

## Crash Pattern Regex
```javascript
nightLight:       /dark|night/i
angleCollision:   /angle/i
headOnCollision:  /head.?on/i
rearEndCollision: /rear/i
runOffRoad:       /run.?off|fixed.?object|overturn/i
wetSurface:       /wet|rain|snow|ice|sleet/i
```

## Related Pages
- [[crash-profile]] — Provides patterns for ORI/PSI
- [[grant-ranking]] — Uses baselines for grant scoring
- [[hotspot-analysis]] — Baseline comparison
