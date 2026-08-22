---
title: GitHub Actions CI/CD
type: entity
tags: [ci-cd, github-actions, automation, workflows]
created: 2026-04-05
updated: 2026-08-22
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

## Scheduled Refreshes

Cron in GitHub Actions **OR**s day-of-month with day-of-week whenever both fields are
restricted. `'0 11 1-7 * 1'` therefore does **not** mean "first Monday of the month" —
it fires on days 1-7 **and** every Monday, roughly 9-10 times a month. For a true
monthly cadence restrict only one field: `'0 11 1 * *'`.

Scheduled events also carry **no `workflow_dispatch` inputs**, so every
`github.event.inputs.*` expression resolves to an empty string. Any workflow with both
triggers must supply explicit fallbacks (`${{ github.event.inputs.scope || 'statewide' }}`)
or the scheduled run silently uses the dispatch defaults — or fails input validation.

| Workflow | Cron | Cadence |
|----------|------|---------|
| `virginia-batch-download.yml` | `0 11 1 * *` | Monthly — statewide, all 133 jurisdictions, incremental; chains to `virginia-batch-pipeline.yml` |
| `download-delaware-crash-data.yml` | `0 11 1 * *` | Monthly |
| `download-colorado.yml` | `0 11 1 * *` | Monthly |
| `generate-state-dot-data-virginia.yml` | `0 6 1 4 *` | Annual (April 1 — VDOT publishes Q1 LRS in March) |
| `r2-health-check.yml` | `0 6 * * *` | Daily |

`download-virginia.yml` is **manual-only**; it handles one-off jurisdiction / region / MPO
pulls. Its monthly schedule moved to `virginia-batch-download.yml` on 2026-08-22.

Still carrying the OR'd `1-7 * N` pattern (not yet reviewed): `download-data.yml`
(also string-matches that literal cron at two `if:` conditions, so any fix must update
those too), `validate-data.yml`, `generate-osm-nationwide.yml`.

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
