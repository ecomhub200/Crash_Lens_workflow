---
title: State Onboarding
type: concept
tags: [onboarding, states, expansion, configuration]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# State Onboarding

The process of **adding a new US state's crash data** to the Crash Lens platform.

## Steps to Onboard a State

1. **Identify data source** — Find the state DOT's crash data API or download portal
2. **Write download script** — Python script to fetch raw crash records
3. **Create column mapping** — `states/{state}/config.json` mapping state fields to standard schema
4. **Define hierarchy** — `states/{state}/hierarchy.json` with counties, cities, MPOs, FIPS codes
5. **Configure severity derivation** — Map state-specific severity codes to KABCO scale
6. **Create GitHub Actions workflow** — `download-{state}-crash-data.yml`
7. **Test pipeline** — Run full pipeline: download → normalize → enrich → upload
8. **Validate in frontend** — Verify data loads correctly in [[douglas-county-frontend]]

## Current State Coverage

See [[state-coverage]] for the full list. Key states:
- **Virginia** — Original, fully operational (500K+ crashes)
- **Colorado** — Fully operational, statewide
- **Maryland** — Montgomery County operational
- **Delaware** — Data pipeline onboarded
- **26+ additional states** — Framework in place, various stages of onboarding

## Config-Driven Design

The pipeline is designed so adding a state requires **only configuration, not code changes**:
- Column mapping config tells the normalizer how to translate fields
- Hierarchy config defines jurisdictions
- The enricher, splitter, and uploader work universally

## Related Pages

- [[crash-lens-workflow]] — The pipeline repo
- [[data-pipeline-architecture]] — How the pipeline processes state data
- [[state-coverage]] — Current multi-state status
- [[github-actions-ci]] — Workflows created per state
