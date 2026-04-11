"""CrashLens Supabase sync webhook.

Flask app served by gunicorn on 127.0.0.1:8765. Caddy reverse-proxies
https://srv1503081.hstgr.cloud/api/sync* to this service.

POST /api/sync       Trigger supabase_sync.py --from-r2 in the background
GET  /api/sync/status  Read last_sync_{state}.json
GET  /api/sync/health  Liveness probe
"""

import glob
import json
import os
import re
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request

APP_DIR = Path("/root/crashlens-webhook")
LOG_DIR = APP_DIR / "logs"
LOCK_FILE = Path("/tmp/crashlens_sync.lock")
SYNC_SCRIPT = Path("/root/Crash_Lens_workflow/supabase_sync.py")
STATE_RE = re.compile(r"^[a-z]{2}$")

app = Flask(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_lock() -> dict | None:
    if not LOCK_FILE.exists():
        return None
    try:
        return json.loads(LOCK_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return {"state": "unknown"}


def _status_path(state: str) -> Path:
    return APP_DIR / f"last_sync_{state}.json"


def _run_sync(state: str, batch_size: int) -> None:
    """Background worker: run supabase_sync.py and record status."""
    started = _now()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{state}_{ts}.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    LOCK_FILE.write_text(json.dumps({
        "state": state,
        "started": started,
        "pid": os.getpid(),
    }))

    exit_code = -1
    status = "failed"
    try:
        env = os.environ.copy()
        env.update({
            "SUPABASE_DB_HOST": "localhost",
            "SUPABASE_DB_PORT": "5433",
            "SUPABASE_DB_NAME": "postgres",
            "SUPABASE_DB_USER": "postgres",
        })
        with open(log_path, "w") as log:
            log.write(f"[webhook] started={started} state={state} batch_size={batch_size}\n")
            log.flush()
            result = subprocess.run(
                [
                    "python3", str(SYNC_SCRIPT),
                    "--state", state,
                    "--from-r2",
                    "--batch-size", str(batch_size),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
            exit_code = result.returncode
            status = "success" if exit_code == 0 else "failed"
    except Exception as exc:  # noqa: BLE001
        with open(log_path, "a") as log:
            log.write(f"[webhook] exception: {exc}\n")
    finally:
        _status_path(state).write_text(json.dumps({
            "state": state,
            "status": status,
            "exit_code": exit_code,
            "started": started,
            "finished": _now(),
            "log_file": str(log_path),
        }, indent=2))
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass


@app.post("/api/sync")
def trigger_sync():
    expected = os.environ.get("SYNC_WEBHOOK_TOKEN", "")
    auth = request.headers.get("Authorization", "")
    if not expected or auth != f"Bearer {expected}":
        return jsonify({"status": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    state = str(body.get("state", "")).lower().strip()
    if not STATE_RE.match(state):
        return jsonify({"status": "bad_request", "error": "state must match ^[a-z]{2}$"}), 400
    try:
        batch_size = int(body.get("batch_size", 25000))
    except (TypeError, ValueError):
        return jsonify({"status": "bad_request", "error": "batch_size must be int"}), 400

    lock = _read_lock()
    if lock is not None:
        return jsonify({"status": "busy", "running_state": lock.get("state")}), 409

    threading.Thread(target=_run_sync, args=(state, batch_size), daemon=True).start()
    return jsonify({"status": "accepted", "state": state}), 202


@app.get("/api/sync/status")
def sync_status():
    state = request.args.get("state", "").lower().strip()
    lock = _read_lock()

    def _load(p: Path) -> dict | None:
        try:
            return json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    if state == "all":
        records = []
        for p in sorted(glob.glob(str(APP_DIR / "last_sync_*.json"))):
            data = _load(Path(p))
            if data:
                records.append(data)
        payload = {"records": records}
    elif STATE_RE.match(state):
        data = _load(_status_path(state))
        payload = data or {"state": state, "status": "unknown"}
    else:
        return jsonify({"status": "bad_request", "error": "state must match ^[a-z]{2}$ or be 'all'"}), 400

    if lock is not None:
        payload["currently_running"] = lock
    return jsonify(payload), 200


@app.get("/api/sync/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765)
