#!/usr/bin/env bash
# CrashLens webhook installer — run from /root/crashlens-webhook/ on the VPS.
set -euo pipefail

APP_DIR="/root/crashlens-webhook"
REPO_DIR="/root/Crash_Lens_workflow"
REPO_URL="https://github.com/ecomhub200/Crash_Lens_workflow.git"

cd "$APP_DIR"

echo "==> Creating logs directory"
mkdir -p "$APP_DIR/logs"

echo "==> Installing webhook Python dependencies"
pip install --quiet flask gunicorn

echo "==> Installing supabase_sync.py dependencies"
pip install --quiet pandas pyarrow psycopg2-binary boto3

if [ ! -d "$REPO_DIR" ]; then
    echo "==> Cloning Crash_Lens_workflow repo to $REPO_DIR"
    git clone "$REPO_URL" "$REPO_DIR"
else
    echo "==> Repo already present at $REPO_DIR (skipping clone)"
fi

TOKEN=""
if [ ! -f "$APP_DIR/.env" ]; then
    echo "==> Generating .env from template"
    TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
    cp "$APP_DIR/.env.template" "$APP_DIR/.env"
    sed -i "s|CHANGE_ME_64_CHAR_RANDOM|$TOKEN|" "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    echo "    .env created — fill in Supabase + R2 credentials before the service can run"
else
    echo "==> .env already exists (preserving it)"
fi

echo "==> Installing systemd unit"
cp "$APP_DIR/crashlens-webhook.service" /etc/systemd/system/crashlens-webhook.service
systemctl daemon-reload
systemctl enable crashlens-webhook
systemctl restart crashlens-webhook

echo ""
echo "═══════════════════════════════════════"
echo "Webhook running on localhost:8765"
if [ -n "$TOKEN" ]; then
    echo "Add this GitHub secret:"
    echo "  SYNC_WEBHOOK_TOKEN=$TOKEN"
else
    echo "Existing .env preserved — reuse the SYNC_WEBHOOK_TOKEN already stored there."
fi
echo ""
echo "Test: curl -X POST http://localhost:8765/api/sync/health"
echo "═══════════════════════════════════════"
