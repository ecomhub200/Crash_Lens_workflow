---
title: CMF Countermeasures
type: concept
tags: [frontend, cmf, countermeasures, safety, analysis]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# CMF / Countermeasures Module

## Purpose
Searches the FHWA CMF Clearinghouse (808 countermeasures) and recommends treatments based on a location's crash profile. CMF = Crash Modification Factor (1.0 = no change, <1.0 = fewer crashes).

## Key Functions

### `searchCMF(records, params)`
Multi-phase search: filter → score → deduplicate → sort.
- **Filter**: min_rating, location_type, area_type, category, proven_only, hsm_only, keywords
- **Score**: rating×10, crash type match×20, proven +25, HSM +15, Virginia relevance bonus
- Returns top N by searchScore

### `recommendCountermeasures(records, crashProfile, options)`
Auto-recommends based on location crash patterns:
1. `identifyDominantPatterns(profile)` — maps collision types to CMF vocabulary
2. Searches with derived parameters
3. Re-scores against location's specific patterns

### `calculateCombinedCMF(cmfValues)`
FHWA successive multiplication: `combined = product of all CMFs`
CRF = (1 - combined) × 100

## CMF Record Fields
```javascript
{ id, n: name, c: category, sc: subcategory, cmf, crf, r: rating,
  ct: [crash_types], sev: severity, loc: location_type, at: area_type,
  psc: proven_safety_countermeasure, hsm: highway_safety_manual, va: virginia_relevance, cost }
```

## Crash Type Vocabulary Mapping
| Frontend Collision Type | CMF Vocabulary |
|------------------------|----------------|
| Angle | angle |
| Rear End | rear_end |
| Head On | head_on |
| Sideswipe | sideswipe |
| Fixed Object / Run Off Road | run_off_road |
| (pedestrian flag) | pedestrian |
| (bicycle flag) | bicycle |
| (speed flag) | speed |
| (≥20% nighttime) | nighttime |

## UI Controls
- `cmfLocationSelect` — Location selector
- `cmfAreaType` — Urban/Rural/All
- `cmfMinRating` — Minimum star rating (1-5)
- `cmfLanes` — Lane count

## Data Requirements
Uses [[crash-profile]] output — no direct column access. The CMF database is a static JSON file (`cmf_processed.json`), not crash data. **No Supabase change needed for CMF records.** Only the crash profile query that feeds into CMF matching needs migration.

## Related Pages
- [[crash-profile]] — Feeds crash patterns into CMF search
- [[before-after-study]] — Uses CMF for effectiveness evaluation
- [[safety-focus]] — CMF keywords per safety category
- [[grant-ranking]] — CMF in grant scoring
