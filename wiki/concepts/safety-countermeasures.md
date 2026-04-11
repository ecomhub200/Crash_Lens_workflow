---
title: Safety Countermeasures
type: concept
tags: [cmf, countermeasures, safety, hsip, treatments]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Safety Countermeasures

**Evidence-based safety treatments** matched to crash patterns using Crash Modification Factors (CMFs).

## What Are CMFs?

A **Crash Modification Factor** quantifies the expected change in crashes after applying a specific treatment:
- CMF = 0.70 means a 30% expected reduction in crashes
- CMFs are published by FHWA and sourced from research studies
- Crash Lens includes **500+ CMFs** in its database

## How Matching Works

The [[douglas-county-frontend|frontend app]]'s CMF/Countermeasures tab:
1. Analyzes crash patterns at a selected location (crash types, severity, road features)
2. Matches patterns against the CMF database
3. Ranks applicable countermeasures by expected crash reduction
4. Estimates benefit-cost ratios for HSIP funding applications

## Examples of Countermeasures

- Roundabout installation (intersection crashes)
- Road diet / lane reconfiguration (mid-block crashes)
- Pedestrian hybrid beacon (pedestrian crashes)
- Cable median barrier (cross-median crashes)
- Enhanced curve signage (curve-related crashes)

## Why It Matters

Countermeasure matching is central to **HSIP grant applications** — agencies must demonstrate that proposed treatments have evidence-based effectiveness to receive federal funding.

## Related Pages

- [[douglas-county-frontend]] — The app with CMF matching UI
- [[crash-enrichment]] — Road attributes needed for CMF matching
- [[hotspot-analysis]] — Identifies locations that need countermeasures
- [[before-after-studies]] — Evaluates countermeasure effectiveness post-installation
