---
title: Supabase Sync CI
type: concept
tags: [supabase, github-actions, automation, ci-cd]
created: 2026-04-06
updated: 2026-04-06
sources: [supabase-schema-v3]
status: active
---

# Supabase Sync CI — Automated Monthly Pipeline

GitHub Actions workflow that syncs crash data from R2 → self-hosted Supabase on a monthly schedule.

## Architecture

```
GitHub Actions (monthly cron)
  ├── SSH tunnel → VPS:5432 (PostgreSQL)
  ├── Download from R2 (CF_* secrets, already in repo)
  └── python supabase_sync.py --state <state> --from-r2
```

## GitHub Secrets Required

| Secret | Value | Status |
|--------|-------|--------|
| `CF_ACCOUNT_ID` | Cloudflare account ID | ✅ Already exists |
| `CF_R2_ACCESS_KEY_ID` | R2 access key | ✅ Already exists |
| `CF_R2_SECRET_ACCESS_KEY` | R2 secret key | ✅ Already exists |
| `SUPABASE_DB_PASSWORD` | Postgres password from VPS .env | ❌ Add this |
| `SUPABASE_SSH_KEY` | Contents of `C:\Users\murad\.ssh\supabase_tunnel` private key | ❌ Add this |

## How to Add the Secrets

1. Go to `github.com/ecomhub200/Crash_Lens_workflow/settings/secrets/actions`
2. Click "New repository secret"
3. Add `SUPABASE_DB_PASSWORD` — the Postgres password from `/root/supabase/docker/.env` on the VPS
4. Add `SUPABASE_SSH_KEY` — paste the full contents of `C:\Users\murad\.ssh\supabase_tunnel` (the private key file)

## Workflow: `.github/workflows/supabase-sync.yml`

- **Schedule**: 1st of every month at 6 AM UTC
- **Manual trigger**: Select state (de, va, co) or "all"
- **Dry-run**: Validate without inserting
- **SSH tunnel**: Forwards localhost:5432 → VPS PostgreSQL
- **Verification**: Runs row count and states table check after sync

## Usage

```bash
# Manual trigger for Delaware
gh workflow run supabase-sync.yml -f state=de

# Manual trigger for all active states
gh workflow run supabase-sync.yml -f state=all

# Dry run
gh workflow run supabase-sync.yml -f state=de -f dry_run=true
```

## Pipeline Order (full monthly cycle)

```
1st of month, 2 AM UTC:  generate-osm-cache.yml (if scheduled)
1st of month, 4 AM UTC:  download-{state}-crash-data.yml
1st of month, 5 AM UTC:  normalize + enrich + split → R2
1st of month, 6 AM UTC:  supabase-sync.yml → PostgreSQL
```

## Related Pages

- [[supabase-schema-v3]] — Database schema
- [[data-pipeline-architecture]] — Full pipeline stages
- [[delaware-pipeline]] — Reference state


## VPS Port Map (confirmed 2026-04-08)

| Port | Service | psycopg2? |
|------|---------|-----------|
| 5432 | Supavisor (connection pooler) | ❌ `Tenant or user not found` |
| 6543 | Direct PostgreSQL | ✅ Works with psycopg2 COPY |

**All SSH tunnels must use port 6543**, not 5432:
```bash
ssh -f -N -L 5432:localhost:6543 -i ~/.ssh/supabase_tunnel root@srv1503081.hstgr.cloud
```

This applies to: `delaware-batch-pipeline.yml` (Stage 4.5), `supabase-sync.yml` (standalone), and local tunnel bat file.


## UPDATED Port Map (confirmed 2026-04-08)

Previous port map was wrong. Corrected after testing all three ports:

| VPS Port | Service | psycopg2? |
|----------|---------|-----------|
| 5432 | Supavisor (pooler) | ❌ `Tenant or user not found` |
| 6543 | Supavisor (pooler) | ❌ `Tenant or user not found` |
| **5433** | **Direct PostgreSQL** | **✅ Works — 566,762 rows confirmed** |

**All SSH tunnels must use port 5433:**
```bash
ssh -f -N -L 5432:localhost:5433 -i ~/.ssh/supabase_tunnel root@srv1503081.hstgr.cloud
```

Port 5433 was added by editing `/root/supabase/docker/docker-compose.yml` — added `ports: "5433:5432"` to the `db:` service.


## CONFIRMED Port Map (2026-04-08)

Previous port map was wrong. Updated after testing all three ports:

| VPS Port | Service | psycopg2? | Tested |
|----------|---------|-----------|--------|
| 5432 | Supavisor | ❌ `Tenant or user not found` | Run #1 |
| 6543 | Supavisor (pool) | ❌ `Tenant or user not found` | Run #2 |
| 5433 | **Direct Postgres** | ✅ `566,762 rows` | Run #3 |

**Fix applied:** Added `ports: "5433:5432"` to `db:` service in `/root/supabase/docker/docker-compose.yml`.

**All SSH tunnels:** `ssh -f -N -L 5432:localhost:5433`


## Pipeline Run Results (2026-04-08)

Schema v3.1 applied successfully. `supabase_sync.py` v3.1 confirmed working with direct Postgres port 5433.

| Metric | Value |
|--------|-------|
| Total rows | 566,762 |
| crash_date_parsed | 566,759 (99.99%) |
| geom (PostGIS) | 558,771 (98.6%) |
| Baselines rows | 81 jurisdictions |
| New indexes | 4 (date, geom GiST, hotspot, intersection) |

### SSH Tunnel (FINAL — verified)

```
ssh -f -N -L 5432:localhost:5433 \
  -i ~/.ssh/supabase_tunnel \
  root@srv1503081.hstgr.cloud
```

Port 5433 = direct Postgres. Ports 5432/6543 = Supavisor (blocked for psycopg2).


## Timeout & Resume Support (2026-04-08)

### Problem
Stage 4.5 timed out at ~50min. The `UPDATE SET geom = ST_Point(x,y)` on 558K rows over SSH tunnel took ~30-40min alone. GitHub Actions step timed out client-side but the Postgres transaction committed successfully.

### Fixes Applied

**1. Timeout: 360 minutes (6 hours)**
Process job `timeout-minutes` increased to 360 (GitHub Actions maximum). Accommodates large states like Virginia (2.1M rows).

**2. Resume mode: `--resume` flag**
```bash
python supabase_sync.py --state de --input delaware.parquet.gz --resume
```
- Skips DROP+CREATE partition
- Queries existing objectids
- Inserts only missing rows
- GitHub Actions input: `resume_supabase: true`

**3. Batched geom UPDATE**
Instead of single 558K-row UPDATE, runs in 50K-row batches with progress reporting. Prevents transaction timeout over SSH tunnel.

### Resume Flow (large states)
```
Run 1: Full sync → timeout at 1.5M rows
Run 2: --resume → inserts remaining 600K rows
Run 3: --resume → 0 new → done ✅
```

### Verified Pipeline Run (Delaware)
| Metric | Value |
|--------|-------|
| Total rows | 566,762 ✅ |
| crash_date_parsed | 566,759 (99.99%) |
| geom (PostGIS) | 558,771 (98.6%) |
| pipeline_status | active |
| federal_summary | 1,726 rows |
| Stage 4.5 duration | 49m 47s (timed out client-side, committed server-side) |


## Batched Sync Architecture (2026-04-09)

### Problem
supabase_sync.py OOM on GitHub Actions (7GB RAM). Peak memory ~6.5GB from building JSONB columns for 566K × 518 column rows. Linux OOM killer sends "shutdown signal" — no error message.

### Solution: GitHub Matrix Strategy (same as Mapillary)
Split sync into matrix batch jobs. Each batch = 25K rows, own runner (1.8GB peak).

```
Plan job → Matrix batch jobs (sequential) → Finalize job
```

### New workflow: supabase-sync.yml
State-agnostic. Takes `state` input. Callable standalone or via `workflow_call` from batch pipeline.

**Plan job (10s):** Download parquet metadata from R2, count rows, output `batch_matrix`.
**Sync batches (matrix, 360min each):** Each batch downloads statewide parquet from R2, reads its row range via pyarrow slice, builds JSONB for 25K rows, COPYs to Postgres.
**Finalize (30min):** Populate geom (batched 50K), refresh matviews, update states table.

### Resume Strategy
- `resume=true`: Each batch checks existing objectids, skips already-inserted rows
- Batch 1 + resume=false: DROP+CREATE partition (full reload)
- Batch 1 + resume=true: Keep partition, insert only missing rows

### Memory per batch
```
Load parquet (pyarrow mmap):  1.5 GB (only slice used)
Slice 25K rows:               0.1 GB
Build JSONB (25K × 300 keys): 0.1 GB
sync_df:                      0.05 GB
PEAK:                        ~1.8 GB ← fits in 7GB runner
```

### Pipeline integration
```yaml
# delaware-batch-pipeline.yml
jobs:
  prepare:    ...
  process:    needs: prepare       # Stages 0-4
  supabase:   needs: process       # Stage 4.5 (workflow_call)
    uses: ./.github/workflows/supabase-sync.yml
    with:
      state: "de"
      batch_size: "25000"
  forecasts:  needs: supabase      # Stage 5
  commit:     needs: forecasts     # Stage 6
```


## First Successful Batched Sync (2026-04-09)

Delaware batched sync completed: 569,829 rows, 23 batches, zero failures.

| Metric | Value |
|--------|-------|
| Total rows | 569,829 |
| Geom | 569,829 (100%) |
| crash_date_parsed | 569,826 (99.999%) |
| Batches | 23 × 25K + 1 × 19,829 |
| Pipeline runs | 71 (all success) |
| Finalize | ~65s (geom + matviews) |
| federal_summary | 103 rows |

### Bugs fixed during deployment
1. Matrix batch ordering → partition creation moved to plan job
2. Table ownership → `ALTER TABLE crashes OWNER TO postgres` (one-time VPS fix)
3. JSONB double-quoting → `quoting=csv.QUOTE_NONE` in bulk_insert
4. NaN in JSON → rewrote `_row_to_json()` with math.isnan + string checks


## Supabase Connection Architecture (Verified 2026-04-08)

Self-hosted Supabase on VPS exposes two PostgreSQL ports:

| Port | Service | Use Case |
|------|---------|----------|
| 5432 | Supavisor (connection pooler) | Frontend PostgREST API, connection pooling |
| 5433 | Direct PostgreSQL | Pipeline bulk sync (psycopg2 COPY), admin queries |

**SSH tunnel commands:**
- Claude Desktop / local: `ssh -L 5432:localhost:5433` (already works, port 5433 direct)
- GitHub Actions pipeline: `ssh -L 5432:localhost:5433` (fixed from 5432→5432 which hit Supavisor)
- Supavisor blocks direct psycopg2 with "Tenant or user not found" — only PostgREST/pgbouncer-aware clients work through it

**GitHub Actions secrets:** `SUPABASE_DB_PASSWORD` + `SUPABASE_SSH_KEY` (both added)

### Current Data Status (2026-04-08)
- crashes_delaware: 519,829 rows (partial — needs re-sync for full 566,762)
- Intersection Name: 96.5% filled ✅
- Node: 98.6% filled ✅
- All JSONB: 100% populated ✅
- Pending: re-run pipeline with port 5433 tunnel fix


## Webhook Architecture (v2.9 — replaces SSH tunnel)

### Why
GitHub Actions SSH tunnel to VPS failed repeatedly — "Network is unreachable" from Azure IPs. The VPS is not always reachable from GitHub's IP ranges.

### How
Pipeline sends one HTTPS POST to VPS after R2 upload. VPS pulls data from R2 and syncs locally. No inbound connection to VPS needed.

```
GitHub Actions (normalize → enrich → split → R2 upload)
  → curl POST https://srv1503081.hstgr.cloud/api/sync
  → VPS webhook receives request
  → VPS: python3 supabase_sync.py --from-r2 --state de
  → VPS: localhost:5433 (no tunnel, direct Postgres)
```

### Components
- `/root/crashlens-webhook/webhook.py` — Flask app on localhost:8765
- Caddy reverse proxy: `/api/sync*` → `172.18.0.1:8765`
- systemd: `crashlens-webhook.service`
- GitHub secret: `SYNC_WEBHOOK_TOKEN`

### R2 Download Paths
- Primary: `{state_name}/_state/all_roads.parquet`
- Fallback: `{state_name}/_statewide/statewide_all_roads.parquet.gz`

### VPS Memory Constraint
8GB RAM — full sync OOM-killed. Requires batched mode (25K rows/batch). 4GB swap file at `/swapfile` as safety net.

### SSH Tunnel Batching [DEPRECATED]
The SSH tunnel approach (`supabase-sync.yml` matrix, 23 parallel jobs) is kept as fallback but replaced by webhook for automated runs. Use only if webhook is unavailable.

See [[webhook-sync]] for full management guide.
