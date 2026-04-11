---
title: "Source: Crash Lens Workflow Repo"
type: source
tags: [source, repo, pipeline, python]
created: 2026-04-05
updated: 2026-04-05
path: "raw/Crash_Lens_workflow-main (3)/"
---

# Source: Crash Lens Workflow Repository

**Raw source location**: `raw/Crash_Lens_workflow-main (3)/Crash_Lens_workflow-main/`

## What This Is

Python-based data pipeline repository for the Crash Lens platform. Contains 33 Python scripts, 40+ GitHub Actions workflows, and configuration files for 30+ US states.

## Key Takeaways Extracted

- Unified 7-stage pipeline architecture (v7) processes all states identically
- Config-driven design: adding a state requires only JSON configuration, not code
- DuckDB spatial grid engine enables memory-efficient processing of 1M+ records
- AWS SageMaker Chronos-2 used for crash forecasting
- Comprehensive documentation in `data-pipeline/` directory (8 guides)

## Wiki Pages Created From This Source

- [[crash-lens-workflow]] — Entity page for the repo
- [[data-pipeline-architecture]] — 7-stage pipeline design
- [[crash-enrichment]] — GPS-based enrichment process
- [[state-onboarding]] — How new states are added
- [[github-actions-ci]] — CI/CD automation
- [[cloudflare-r2]] — Storage layer (shared with frontend)
- [[data-sources-inventory]] — All external data sources
