---
title: Grant Ranking & Eligibility
type: concept
tags: [frontend, grants, ranking, scoring, hsip, ss4a]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Grant Ranking & Eligibility

## Purpose
Scores locations for federal safety grant programs (HSIP, SS4A, 402, 405d) using crash data, statistical analysis, and pattern matching.

## Scoring Profiles
| Profile | Focus |
|---------|-------|
| `balanced` | Equal weight across need, pattern, grant fit |
| `hsip` | HSIP — Infrastructure (severity + angle/head-on) |
| `ss4a` | Safe Streets — VRU crashes + speed |
| `402` | Section 402 — Behavioral (impaired + speed + night) |
| `405d` | Section 405d — Impaired driving focus |

## Composite Score Formula
**Balanced:** `(needScore×3) + (patternScore×2) + (feasibility×2) + (bestGrant×3)`
**Profile-specific:** `(needScore×2) + (patternScore×2) + (feasibility×1) + (targetGrant×5)`
With severity trend multiplier: ×1.15 worsening, ×0.9 improving.

## Sub-Scores

### Need Score (0-100)
- PSI exceeds critical: +10 to +40 based on ratio
- K/A severity: min(40, ka×12 + B×2)
- Total crashes: min(20, total×0.5)

### Pattern Score (0-100)
- Significant patterns: +15 per pattern
- Strong ORI (>1.5): +8 per pattern
- Total ≥20: +10; ≥10: +5

### Grant Fit (per program)
- **HSIP**: Critical threshold + K/A + angle/head-on patterns
- **SS4A**: Ped/bike + speed + K/A
- **402**: Impaired + speed + night + total
- **405d**: Impaired + night + impaired count + K/A

## Statistical Components
- **ORI** (Over-Representation Index): `(local_rate / baseline_rate)` — values >1.5 flag over-representation
- **PSI** (Potential for Safety Improvement): Empirical Bayes estimate minus expected — positive = location worse than expected
- **Significance**: Binomial test at α=0.10, confidence levels: high (<0.01), medium (<0.05), low (<0.10)

## Data Requirements
Uses pre-computed patterns and baselines from [[baselines-analysis]]. Indirectly requires all columns used by [[crash-profile]].

### ranking_data JSONB Fields
| Key | Usage |
|-----|-------|
| `Juris_Rank_total_crash` | Jurisdiction crash ranking |
| `Juris_Rank_total_epdo` | EPDO-based ranking |
| `MPO_Rank_trend_ksi` | KSI trend ranking |
| `District_Rank_safety_score` | District safety score |

## UI Components
- Grant location table (`grantLocationBody`)
- Scoring profile selector
- Location comparison

## Related Pages
- [[baselines-analysis]] — Baseline rates for ORI/PSI
- [[crash-profile]] — Input crash patterns
- [[hotspot-analysis]] — Hotspot detection feeds ranking
- [[epdo-scoring]] — EPDO in need score
