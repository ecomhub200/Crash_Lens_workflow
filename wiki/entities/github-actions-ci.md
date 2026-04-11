---
title: GitHub Actions CI/CD
type: entity
tags: [ci-cd, github-actions, automation, workflows]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# GitHub Actions CI/CD

**40+ GitHub Actions workflows** automate the entire data collection and processing pipeline for [[crash-lens-workflow]].

## Workflow Types

### State-Specific Download Workflows
One workflow per state, triggered manually or on schedule:
- `download-virginia.yml`, `download-colorado.yml`, `download-delaware-crash-data.yml`
- Each calls the appropriate Python download script for that state's API
- ~30 state download workflows

### Processing Workflows
- `batch-pipeline.yml` — Unified 7-stage processing pipeline
- `batch-all-jurisdictions.yml` — Run pipeline across all jurisdictions
- `build-road-inventory.yml` — Generate road inventory database

### Data Source Workflows
- `generate-osm-cache.yml` — Build OpenStreetMap cache
- `generate-hpms-cache.yml` — Build HPMS data cache
- `generate-federal-cache.yml` — Build federal infrastructure cache
- `generate-boundaries.yml` — Generate jurisdiction boundaries
- `generate-mapillary-cache.yml` — Fetch street imagery metadata

### Infrastructure Workflows
- `create-r2-folders.yml` — Set up [[cloudflare-r2]] bucket structure
- `manual-upload-state.yml` — Manual data upload to R2

## Trigger Pattern

```
State Download → Merge/Convert → Unified Pipeline → R2 Upload
(manual/schedule)  (auto-trigger)   (7 stages)      (auto)
```

## Related Pages

- [[crash-lens-workflow]] — The repo these workflows belong to
- [[data-pipeline-architecture]] — Pipeline design details
- [[state-onboarding]] — Adding workflows for new states


## Supabase Workflows (v2.8)

| Workflow | Purpose |
|----------|---------|
| `supabase-sync.yml` | Standalone monthly sync from R2 → Supabase (manual or cron) |
| Stage 4.5 in `delaware-batch-pipeline.yml` | Inline sync after R2 upload (automatic with each pipeline run) |

### GitHub Secrets for Supabase

| Secret | Purpose |
|--------|---------|
| `SUPABASE_DB_PASSWORD` | PostgreSQL password for self-hosted Supabase |
| `SUPABASE_SSH_KEY` | SSH private key for tunnel to VPS (`srv1503081.hstgr.cloud`) |

These are in addition to the existing `CF_ACCOUNT_ID`, `CF_R2_ACCESS_KEY_ID`, `CF_R2_SECRET_ACCESS_KEY` secrets.

See [[supabase-sync-ci]] for standalone workflow details.
