# CrashLens Sync Webhook

Small Flask/gunicorn service that lives on the VPS (`srv1503081.hstgr.cloud`)
and runs `supabase_sync.py --from-r2` in a background thread whenever GitHub
Actions POSTs to it. Replaces the matrix-batched `supabase-sync.yml` workflow
for the normal path — that workflow is kept as a manual fallback.

## Layout on the VPS

```
/root/crashlens-webhook/
├── webhook.py                  # Flask app (this dir)
├── crashlens-webhook.service   # systemd unit
├── setup.sh                    # one-shot installer
├── .env.template               # copy → .env, fill in secrets
├── .env                        # real secrets (chmod 600)
├── logs/                       # per-run sync logs
└── last_sync_<state>.json      # most recent run summary per state

/root/Crash_Lens_workflow/      # cloned by setup.sh, provides supabase_sync.py
/tmp/crashlens_sync.lock        # presence = a sync is in flight
```

## First-time install

```bash
mkdir -p /root/crashlens-webhook
scp webhook/* root@srv1503081.hstgr.cloud:/root/crashlens-webhook/
ssh root@srv1503081.hstgr.cloud
cd /root/crashlens-webhook
chmod +x setup.sh
./setup.sh
# Edit /root/crashlens-webhook/.env and fill in SUPABASE_DB_PASSWORD, CF_*
vim .env
systemctl restart crashlens-webhook
```

`setup.sh` prints the generated `SYNC_WEBHOOK_TOKEN` — add it to GitHub
secrets on `ecomhub200/Crash_Lens_workflow` with that exact name.

## Caddy reverse proxy

The public Caddy runs inside Docker. Edit the live Caddyfile (location
depends on the VPS setup — typically mounted from
`/root/supabase/docker/volumes/caddy/Caddyfile` or similar — check
`docker inspect <caddy_container> | grep -A3 Mounts`). Inside the existing
`srv1503081.hstgr.cloud` site block, add:

```
    handle /api/sync* {
        reverse_proxy host.docker.internal:8765
    }
```

If the caddy service in `docker-compose.caddy.yml` does **not** have
`extra_hosts: ["host.docker.internal:host-gateway"]`, replace
`host.docker.internal` with the Docker bridge gateway IP `172.17.0.1`.

Reload Caddy:
```bash
docker exec <caddy_container> caddy reload --config /etc/caddy/Caddyfile
```

## API

### `POST /api/sync`
Headers: `Authorization: Bearer <SYNC_WEBHOOK_TOKEN>`
Body (JSON):
```json
{ "state": "de", "batch_size": 25000 }
```
`batch_size` is optional (default 25000). `state` must match `^[a-z]{2}$`.

Responses:
- `202 {"status":"accepted","state":"de"}` — spawned background thread, sync running
- `401 {"status":"unauthorized"}` — missing/wrong token
- `400` — bad body
- `409 {"status":"busy","running_state":"..."}` — another sync is already in flight

### `GET /api/sync/status?state=de` (no auth)
Returns the contents of `last_sync_de.json`. If a sync is currently running,
the response also includes `"currently_running": {...lock contents}`.

`?state=all` returns `{"records": [...]}` with every `last_sync_*.json`.

### `GET /api/sync/health` (no auth)
`{"status":"ok"}` — for monitoring/uptime checks.

## Testing (on the VPS)

```bash
# Load token for the tests below
source /root/crashlens-webhook/.env

# Liveness
curl http://localhost:8765/api/sync/health

# Auth reject (expect 401)
curl -X POST http://localhost:8765/api/sync \
  -H "Content-Type: application/json" \
  -d '{"state":"de"}'

# Real trigger (expect 202)
curl -X POST http://localhost:8765/api/sync \
  -H "Authorization: Bearer $SYNC_WEBHOOK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"state":"de"}'

# Concurrent trigger (expect 409)
curl -X POST http://localhost:8765/api/sync \
  -H "Authorization: Bearer $SYNC_WEBHOOK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"state":"de"}'

# Status
curl "http://localhost:8765/api/sync/status?state=de"
curl "http://localhost:8765/api/sync/status?state=all"

# Follow sync log
tail -f /root/crashlens-webhook/logs/de_*.log

# Follow webhook (gunicorn) log
journalctl -u crashlens-webhook -f

# Service control
systemctl status crashlens-webhook
systemctl restart crashlens-webhook

# Public (via Caddy) — run from anywhere
curl https://srv1503081.hstgr.cloud/api/sync/health
```

## Operations

- **Stale lock after crash**: if the webhook or sync process was killed
  mid-run, the lock file may outlive it. Clear it manually:
  ```bash
  rm /tmp/crashlens_sync.lock
  ```
- **Rotate token**: generate a new one, update `.env`, `systemctl restart
  crashlens-webhook`, update the `SYNC_WEBHOOK_TOKEN` GitHub secret.
- **Update supabase_sync.py**: `cd /root/Crash_Lens_workflow && git pull`.
  The webhook picks it up on the next `/api/sync` call (no restart needed —
  it shells out to the script by path).
- **Logs fill disk**: rotate `/root/crashlens-webhook/logs/` periodically
  (no automation shipped; add a logrotate rule if runs are frequent).

## Manual fallback

If the VPS webhook is down, trigger the batched matrix workflow directly:
GitHub Actions → `.github/workflows/supabase-sync.yml` → `Run workflow`
with `state=de`.
