---
type: entity
tags: [infrastructure, supabase, webhook, vps]
created: 2026-04-11
status: active
---

# Webhook Sync

VPS-hosted webhook server that triggers Supabase sync after GitHub Actions pipeline uploads to R2. Replaces SSH tunnel batching (supabase-sync.yml matrix) as of v2.9.

## Location
- Server: srv1503081.hstgr.cloud
- App dir: `/root/crashlens-webhook/`
- Repo: `/root/Crash_Lens_workflow/`
- Logs: `/root/crashlens-webhook/logs/`

## Endpoints

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| /api/sync | POST | Bearer token | Trigger sync |
| /api/sync/status | GET | None | Check status |
| /api/sync/health | GET | None | Health check |

## Flow
1. GitHub Actions: normalize → enrich → split → R2 upload
2. `curl POST https://srv1503081.hstgr.cloud/api/sync` with `{"state":"de"}`
3. VPS receives webhook → runs `supabase_sync.py --from-r2 --state de`
4. supabase_sync.py downloads from R2 → batched sync to localhost:5433 (geom + crash_date_parsed auto-set per-row by the `trg_compute_geom` BEFORE INSERT trigger)
5. Finalize: safety-net backfill (usually 0 rows), matview refresh, states upsert — ~30s total with the trigger active
6. Writes status to `/root/crashlens-webhook/last_sync_{state}.json`

## R2 Download Paths (supabase_sync.py)
- Primary: `{state_name}/_state/all_roads.parquet`
- Fallback: `{state_name}/_statewide/statewide_all_roads.parquet.gz`

## Key Details
- **Lock file:** `/tmp/crashlens_sync.lock` prevents concurrent syncs
- **Caddy route:** `/api/sync*` → `172.18.0.1:8765` (Docker bridge gateway)
- **systemd:** `crashlens-webhook.service` (gunicorn, 1 worker)
- **GitHub secret:** `SYNC_WEBHOOK_TOKEN`
- **VPS RAM:** 8GB — requires batched mode (25K rows/batch) to avoid OOM
- **Swap:** 4GB swap file at `/swapfile` as safety net

## Management Commands

```bash
# Restart
systemctl restart crashlens-webhook

# Logs
journalctl -u crashlens-webhook -f

# Sync logs
ls -t /root/crashlens-webhook/logs/ | head -5

# Manual trigger
curl -X POST http://localhost:8765/api/sync \
  -H "Authorization: Bearer $(grep SYNC_WEBHOOK_TOKEN /root/crashlens-webhook/.env | cut -d= -f2)" \
  -H "Content-Type: application/json" \
  -d '{"state": "de"}'

# Check status
curl http://localhost:8765/api/sync/status?state=de

# Update repo
cd /root/Crash_Lens_workflow && git pull
```

## Dependencies
- Python: flask, gunicorn, pandas, pyarrow, psycopg2-binary, boto3
- .env: SYNC_WEBHOOK_TOKEN, SUPABASE_DB_PASSWORD, CF_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY

## Geom/Date Trigger (2026-04-12)

`webhook/webhook.py` is a pure orchestrator — it contains zero direct SQL against the database and was **not** changed as part of the permanent geom/date fix. Only the subprocess comment at line 294 was updated to reflect that finalize now runs in ~30s instead of 30 min. The 1800s timeout is retained as a conservative ceiling.

All geom/`crash_date_parsed` handling now happens in two places:
- **VPS:** `trg_compute_geom` BEFORE INSERT trigger on the `crashes` parent table (auto-propagates to all partitions).
- **`supabase_sync.py::finalize_sync()`:** advisory-locked safety-net backfill + matview refresh + states upsert.

See [[supabase-sync-ci]] → "Geom / Date Trigger" section for the full trigger SQL, the execution flow diff, and rollback steps.

## Why Not SSH Tunnel?
The previous approach (23 parallel GitHub Actions jobs, each opening SSH tunnel to VPS) failed when GitHub's Azure IPs couldn't reach the VPS ("Network is unreachable"). The webhook approach inverts the direction — VPS makes outbound connections to R2 only, which always works.

## Related Pages
- [[supabase-sync-ci]]
- [[pipeline-architecture-v29]]
- [[supabase-schema-v3]]
