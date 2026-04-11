---
title: Before/After Studies
type: concept
tags: [before-after, evaluation, safety, effectiveness]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Before/After Studies

**Safety improvement evaluation methodology** — measures whether a countermeasure actually reduced crashes after installation.

## Purpose

After a safety treatment is installed (see [[safety-countermeasures]]), agencies need to verify it worked. Before/After studies compare:
- **Before period**: Crash frequency and severity in years preceding treatment
- **After period**: Crash frequency and severity in years following treatment
- **Control**: Account for background crash trends (regression to mean)

## Implementation

The Before/After tab in [[douglas-county-frontend]]:
- Select a location and treatment installation date
- System automatically partitions crash data into before/after periods
- Calculates observed vs. expected crash reduction
- Generates statistical significance metrics
- Produces report-ready output (PDF/DOCX)

## Why It Matters

- Required for HSIP grant accountability
- Validates CMF-based predictions
- Builds local evidence base for future countermeasure selection
- Demonstrates ROI of safety investments to stakeholders

## Related Pages

- [[safety-countermeasures]] — The treatments being evaluated
- [[hotspot-analysis]] — Identifies where treatments are needed
- [[douglas-county-frontend]] — App with Before/After tab
