---
title: State Coverage
type: analysis
tags: [states, coverage, expansion, multi-state]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# State Coverage

Current status of multi-state expansion for Crash Lens.

## Fully Operational

| State | Data Source | Notes |
|-------|-----------|-------|
| Virginia | Virginia Roads ArcGIS API | Original state, 500K+ crashes, 95+ counties + 38 cities |
| Colorado | CDOT OnBase | Statewide coverage |
| Maryland | Socrata SODA API | Montgomery County operational |

## Pipeline Onboarded (Download + Normalize)

| State | Status |
|-------|--------|
| Delaware | Data pipeline configured |
| Alaska | Download workflow exists |
| Arkansas | Download workflow exists |
| Connecticut | Download workflow exists |
| Florida | Download workflow exists |
| Georgia | Download workflow exists |
| Hawaii | Download workflow exists |
| Idaho | Download workflow exists |
| Illinois | Download workflow exists |
| Iowa | Download workflow exists |
| Louisiana | Download workflow exists |
| Massachusetts | Download workflow exists |
| Mississippi | Download workflow exists |
| Montana | Download workflow exists |
| Nevada | Download workflow exists |
| New York | Download workflow exists |
| NYC | Separate download workflow |
| Ohio | Download workflow exists |
| Oklahoma | Download workflow exists |
| Oregon | Download workflow exists |
| Pennsylvania | Download workflow exists |
| South Carolina | Download workflow exists |
| Texas | Download workflow exists |
| Utah | Download workflow exists |
| Vermont | Download workflow exists |
| Washington | Download workflow exists |
| West Virginia | Download workflow exists |
| Wisconsin | Download workflow exists |

## Expansion Pattern

New states follow the [[state-onboarding]] process. The config-driven [[data-pipeline-architecture]] means each state needs only:
1. A download script for its specific API
2. Column mapping configuration
3. Jurisdiction hierarchy definition

## Related Pages

- [[state-onboarding]] — Onboarding process
- [[crash-lens-workflow]] — Pipeline repo
- [[data-pipeline-architecture]] — How the pipeline handles multiple states
