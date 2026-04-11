---
title: EPDO Scoring System
type: concept
tags: [frontend, epdo, scoring, severity, analysis]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# EPDO — Equivalent Property Damage Only

## Purpose
Weights crash severity levels to a single score. A fatal crash (K=883) counts 883× more than a PDO crash (O=1). Used throughout the app for ranking, hotspot detection, grant scoring, and trend analysis.

## Formula
```
EPDO = (K × K_weight) + (A × A_weight) + (B × B_weight) + (C × C_weight) + (O × O_weight)
```

## Default Weights (FHWA 2025)
```javascript
{ K: 883, A: 94, B: 21, C: 11, O: 1 }
```

## State-Specific Weights

| State | K | A | B | C | O | Source |
|-------|---|---|---|---|---|--------|
| Virginia (51) | 1032 | 53 | 16 | 10 | 1 | VDOT 2024 |
| California (06) | 1100 | 58 | 17 | 11 | 1 | Caltrans 2023 |
| Texas (48) | 920 | 55 | 14 | 9 | 1 | TxDOT 2023 |
| Florida (12) | 985 | 50 | 15 | 9 | 1 | FDOT 2023 |
| New York (36) | 1050 | 55 | 15 | 10 | 1 | NYSDOT 2023 |
| Massachusetts (25) | 1200 | 60 | 18 | 12 | 1 | MassDOT 2024 |
| North Carolina (37) | 770 | 77 | 8 | 8 | 1 | NCDOT 2023 |
| All others | 883 | 94 | 21 | 11 | 1 | FHWA 2025 |

## Presets Available
- `hsm2010`: HSM Standard 2010 (K:462, A:62, B:12, C:5)
- `vdot2024`: VDOT 2024 (K:1032, A:53, B:16, C:10)
- `fhwa2022`: FHWA 2022 (K:975, A:48, B:13, C:8)
- `fhwa2025`: FHWA 2025 (K:883, A:94, B:21, C:11)

## Supabase Calculation
```sql
SELECT node,
  (SUM(k_people)*883 + SUM(a_people)*94 + SUM(b_people)*21 + SUM(c_people)*11 + COUNT(*)) as epdo
FROM crashes WHERE state = 'delaware'
GROUP BY node ORDER BY epdo DESC;
```

## Columns Required
- `k_people`, `a_people`, `b_people`, `c_people` — severity counts per crash
- `crash_severity` — K/A/B/C/O letter code (used for grouping)

## Related Pages
- [[crash-profile]] — Uses EPDO in profiles
- [[hotspot-analysis]] — Ranks by EPDO
- [[grant-ranking]] — EPDO in grant scoring
