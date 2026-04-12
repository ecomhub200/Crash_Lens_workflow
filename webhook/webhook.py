"""CrashLens Supabase sync webhook.

Flask app served by gunicorn on 127.0.0.1:8765. Caddy reverse-proxies
https://srv1503081.hstgr.cloud/api/sync* to this service.

POST /api/sync         Trigger a batched supabase_sync.py run in the background
GET  /api/sync/status  Read last_sync_{state}.json
GET  /api/sync/health  Liveness probe

The background sync uses the batch matrix protocol (download → count →
truncate → loop --batch N → --finalize) to avoid the full-load OOM on the
8GB VPS. Each batch runs as its own subprocess so peak memory drops back
between batches.
"""

import glob
import hmac
import json
import math
import os
import re
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request

# Allow `from supabase_sync import STATES` from the installed repo location
WORK_DIR = "/root/Crash_Lens_workflow"
if WORK_DIR not in sys.path:
    sys.path.insert(0, WORK_DIR)
from supabase_sync import STATES  # noqa: E402

SYNC_SCRIPT = f"{WORK_DIR}/supabase_sync.py"
WEBHOOK_DIR = Path("/root/crashlens-webhook")
LOG_DIR = WEBHOOK_DIR / "logs"
LOCK_FILE = Path("/tmp/crashlens_sync.lock")
STATE_RE = re.compile(r"^[a-z]{2}$")

app = Flask(__name__)


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


def _read_lock() -> dict | None:
    if not LOCK_FILE.exists():
        return None
    try:
        return json.loads(LOCK_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return {"state": "unknown"}


def _status_path(state: str) -> Path:
    return WEBHOOK_DIR / f"last_sync_{state}.json"


def _build_env() -> dict:
    """Env for subprocesses. Inherits os.environ (systemd EnvironmentFile
    provides SUPABASE_DB_PASSWORD, CF_*, etc.) and pins the local tunnel."""
    env = {**os.environ}
    env["SUPABASE_DB_HOST"] = "localhost"
    env["SUPABASE_DB_PORT"] = "5433"
    env.setdefault("SUPABASE_DB_NAME", "postgres")
    env.setdefault("SUPABASE_DB_USER", "postgres")
    return env


def _cleanup_stale_inputs(log) -> None:
    """Remove any leftover parquet/csv from a previous crashed run so glob
    doesn't pick up stale data as the 'just-downloaded' file."""
    patterns = [
        f"{WORK_DIR}/all_roads.parquet*",
        f"{WORK_DIR}/*statewide_all_roads*",
    ]
    for pat in patterns:
        for f in glob.glob(pat):
            try:
                os.remove(f)
                log.write(f"[sync] Cleaned up stale file: {f}\n")
            except OSError:
                pass


def _find_downloaded_input() -> str | None:
    """Return path to the input file dropped by download_from_r2.

    Real filenames on disk (from supabase_sync.download_from_r2):
      - all_roads.parquet.gz        (primary R2 key)
      - <slug>_statewide_all_roads.csv  (fallback R2 key)
    """
    candidates = (
        glob.glob(f"{WORK_DIR}/all_roads.parquet*")
        + glob.glob(f"{WORK_DIR}/*statewide_all_roads*")
    )
    return candidates[0] if candidates else None


def _count_rows(input_file: str, env: dict, log) -> int | None:
    """Count rows cheaply. Try pyarrow metadata first (no full load); fall
    back to pandas for CSV or on pyarrow failure. Returns None on failure."""
    # Primary: pyarrow metadata (parquet only, ~ms)
    try:
        cr = subprocess.run(
            [
                "python3",
                "-c",
                "import sys, pyarrow.parquet as pq; "
                "print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)",
                input_file,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if cr.returncode == 0 and cr.stdout.strip():
            return int(cr.stdout.strip())
        log.write(
            f"[sync] pyarrow count rc={cr.returncode} stdout={cr.stdout!r} "
            f"stderr={cr.stderr!r}\n"
        )
    except Exception as exc:  # noqa: BLE001
        log.write(f"[sync] pyarrow count raised: {exc}\n")

    # Fallback: pandas (handles CSV + non-standard parquet names)
    is_csv = input_file.lower().endswith(".csv")
    loader = (
        "import sys, pandas as pd; df = pd.read_csv(sys.argv[1], usecols=[0]); "
        "print(len(df)); del df"
        if is_csv
        else "import sys, pandas as pd; df = pd.read_parquet(sys.argv[1]); "
        "print(len(df)); del df"
    )
    try:
        cr2 = subprocess.run(
            ["python3", "-c", loader, input_file],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if cr2.returncode == 0 and cr2.stdout.strip():
            return int(cr2.stdout.strip())
        log.write(
            f"[sync] pandas count rc={cr2.returncode} stdout={cr2.stdout!r} "
            f"stderr={cr2.stderr!r}\n"
        )
    except Exception as exc:  # noqa: BLE001
        log.write(f"[sync] pandas count raised: {exc}\n")
    return None


def _run_batched_sync(
    abbr: str, batch_size: int, log_path: Path, mode: str = "full"
) -> tuple[str, int, list[int]]:
    """Download → count → truncate → batch loop → finalize → cleanup.

    Each step is a SEPARATE subprocess to free memory between batches.
    Returns (status, total_rows, failed_batches).
    """
    env = _build_env()
    _, slug, _ = STATES[abbr]
    input_file: str | None = None

    with open(log_path, "a") as log:
        log.write(f"[sync] Mode: {mode} (Phase 1: full reload for both modes)\n")
        # ── Step 0: clean up leftover files from any previous failed run ──
        _cleanup_stale_inputs(log)

        # ── Step 1: download parquet from R2 (subprocess, never in-process) ──
        log.write("[sync] Step 1/6: Downloading from R2...\n")
        log.flush()
        dl_code = (
            "import sys; sys.path.insert(0, '" + WORK_DIR + "'); "
            "from supabase_sync import download_from_r2; "
            f"download_from_r2({slug!r}, {abbr!r})"
        )
        try:
            dl = subprocess.run(
                ["python3", "-c", dl_code],
                env=env,
                cwd=WORK_DIR,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            log.write("[sync] FATAL: R2 download timed out\n")
            return "failed", 0, []
        if dl.returncode != 0:
            log.write(f"[sync] FATAL: R2 download failed (rc={dl.returncode})\n")
            return "failed", 0, []

        input_file = _find_downloaded_input()
        if not input_file:
            log.write("[sync] FATAL: No parquet/csv file found after download\n")
            return "failed", 0, []
        log.write(f"[sync] Downloaded: {input_file}\n")
        log.flush()

        # ── Step 2: count rows (metadata only — cheap) ──
        log.write("[sync] Step 2/6: Counting rows...\n")
        log.flush()
        total_rows = _count_rows(input_file, env, log)
        if total_rows is None or total_rows <= 0:
            log.write("[sync] FATAL: could not determine row count\n")
            return "failed", 0, []

        num_batches = math.ceil(total_rows / batch_size)
        log.write(
            f"[sync] Rows: {total_rows:,} | "
            f"Batches: {num_batches} x {batch_size:,}\n"
        )
        log.flush()

        # ── Step 3: truncate partition (batch mode does NOT truncate) ──
        log.write(f"[sync] Step 3/6: Truncating crashes_{slug}...\n")
        log.flush()
        truncate_code = (
            "import os, psycopg2\n"
            "conn = psycopg2.connect(\n"
            "    host='localhost', port=5433,\n"
            "    dbname=os.environ.get('SUPABASE_DB_NAME', 'postgres'),\n"
            "    user=os.environ.get('SUPABASE_DB_USER', 'postgres'),\n"
            "    password=os.environ['SUPABASE_DB_PASSWORD'],\n"
            ")\n"
            "cur = conn.cursor()\n"
            f"cur.execute('TRUNCATE crashes_{slug}')\n"
            "conn.commit()\n"
            "conn.close()\n"
            f"print('Truncated crashes_{slug}')\n"
        )
        try:
            tr = subprocess.run(
                ["python3", "-c", truncate_code],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            log.write("[sync] FATAL: Truncate timed out\n")
            return "failed", total_rows, []
        if tr.returncode != 0:
            log.write(f"[sync] FATAL: Truncate failed (rc={tr.returncode})\n")
            return "failed", total_rows, []

        # ── Step 4: batch loop (fresh subprocess per batch, memory resets) ──
        failed_batches: list[int] = []
        for i in range(1, num_batches + 1):
            log.write(f"[sync] Step 4/6: Batch {i}/{num_batches}...\n")
            log.flush()
            try:
                br = subprocess.run(
                    [
                        "python3",
                        SYNC_SCRIPT,
                        "--state",
                        abbr,
                        "--input",
                        input_file,
                        "--batch",
                        str(i),
                        "--batch-size",
                        str(batch_size),
                        "--total-rows",
                        str(total_rows),
                    ],
                    env=env,
                    cwd=WORK_DIR,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=600,
                )
                if br.returncode != 0:
                    failed_batches.append(i)
                    log.write(
                        f"[sync] WARNING: Batch {i} failed (rc={br.returncode})\n"
                    )
            except subprocess.TimeoutExpired:
                failed_batches.append(i)
                log.write(f"[sync] WARNING: Batch {i} timed out\n")
            log.flush()

        # ── Step 5: finalize (matviews + states upsert; geom handled by trigger) ──
        log.write("[sync] Step 5/6: Finalizing (matviews, states; geom already set by trigger)...\n")
        log.flush()
        try:
            fr = subprocess.run(
                ["python3", SYNC_SCRIPT, "--state", abbr, "--finalize"],
                env=env,
                cwd=WORK_DIR,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=1800,  # conservative ceiling; trigger-driven finalize ~30s in practice
            )
            if fr.returncode != 0:
                log.write(
                    f"[sync] WARNING: Finalize exited rc={fr.returncode} "
                    "(non-fatal; partial refresh is usually recoverable)\n"
                )
        except subprocess.TimeoutExpired:
            log.write("[sync] WARNING: Finalize timed out after 30 min\n")

        # ── Step 6: cleanup input file ──
        log.write("[sync] Step 6/6: Cleanup...\n")
        log.flush()
        try:
            if input_file and os.path.exists(input_file):
                os.remove(input_file)
                log.write(f"[sync] Removed {input_file}\n")
        except OSError as exc:
            log.write(f"[sync] Cleanup warning: {exc}\n")

        status = "success" if not failed_batches else "partial"
        ok = num_batches - len(failed_batches)
        log.write(
            f"[sync] DONE: {status} | {total_rows:,} rows | "
            f"{ok}/{num_batches} batches ok\n"
        )
        return status, total_rows, failed_batches


def _sync_thread(abbr: str, batch_size: int, mode: str = "full") -> None:
    """Thread wrapper: owns the lock file and writes the status JSON.

    The lock file is ALWAYS removed in `finally` so a crash mid-sync doesn't
    permanently wedge the webhook.
    """
    started_dt = _now_dt()
    ts_str = started_dt.strftime("%Y%m%d_%H%M%S")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{abbr}_{ts_str}.log"

    with open(log_path, "w") as log:
        log.write(
            f"[webhook] started={started_dt.isoformat()} "
            f"state={abbr} batch_size={batch_size} mode={mode}\n"
        )

    status: str = "failed"
    total_rows: int = 0
    failed: list[int] = []
    try:
        LOCK_FILE.write_text(
            json.dumps(
                {
                    "state": abbr,
                    "pid": os.getpid(),
                    "started": started_dt.isoformat(),
                }
            )
        )
        status, total_rows, failed = _run_batched_sync(abbr, batch_size, log_path, mode)
    except Exception as exc:  # noqa: BLE001
        try:
            with open(log_path, "a") as log:
                log.write(f"[webhook] EXCEPTION: {exc}\n")
        except OSError:
            pass
    finally:
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    finished_dt = _now_dt()
    num_batches = math.ceil(total_rows / batch_size) if total_rows > 0 else 0
    status_data = {
        "state": abbr,
        "status": status,
        "mode": mode,
        "total_rows": total_rows,
        "num_batches": num_batches,
        "failed_batches": failed,
        "batch_size": batch_size,
        "started": started_dt.isoformat(),
        "finished": finished_dt.isoformat(),
        "duration_min": round((finished_dt - started_dt).total_seconds() / 60, 1),
        "log_file": str(log_path),
    }
    try:
        _status_path(abbr).write_text(json.dumps(status_data, indent=2))
    except OSError:
        pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False
    return True


@app.post("/api/sync")
def trigger_sync():
    expected = os.environ.get("SYNC_WEBHOOK_TOKEN", "")
    auth = request.headers.get("Authorization", "")
    if not expected or not hmac.compare_digest(auth, f"Bearer {expected}"):
        return jsonify({"status": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    state = str(body.get("state", "")).lower().strip()
    if not STATE_RE.match(state) or state not in STATES:
        return (
            jsonify(
                {
                    "status": "bad_request",
                    "error": "state must match ^[a-z]{2}$ and exist in STATES",
                }
            ),
            400,
        )
    try:
        batch_size = int(body.get("batch_size", 25000))
    except (TypeError, ValueError):
        return jsonify({"status": "bad_request", "error": "batch_size must be int"}), 400
    if not (1000 <= batch_size <= 100000):
        return (
            jsonify(
                {
                    "status": "bad_request",
                    "error": "batch_size must be between 1000 and 100000",
                }
            ),
            400,
        )

    mode = str(body.get("mode", "full")).lower().strip()
    if mode not in ("incremental", "full"):
        mode = "full"

    lock = _read_lock()
    if lock is not None:
        lock_pid = lock.get("pid")
        if isinstance(lock_pid, int) and not _pid_alive(lock_pid):
            # Stale lock from a crashed prior run — clear and proceed
            try:
                LOCK_FILE.unlink()
            except FileNotFoundError:
                pass
        else:
            return (
                jsonify({"status": "busy", "running_state": lock.get("state")}),
                409,
            )

    threading.Thread(
        target=_sync_thread, args=(state, batch_size, mode), daemon=True
    ).start()
    return jsonify({"status": "accepted", "state": state, "mode": mode}), 202


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
        for p in sorted(glob.glob(str(WEBHOOK_DIR / "last_sync_*.json"))):
            data = _load(Path(p))
            if data:
                records.append(data)
        payload = {"records": records}
    elif STATE_RE.match(state) and state in STATES:
        data = _load(_status_path(state))
        payload = data or {"state": state, "status": "unknown"}
    else:
        return (
            jsonify(
                {
                    "status": "bad_request",
                    "error": "state must match ^[a-z]{2}$ (in STATES) or be 'all'",
                }
            ),
            400,
        )

    if lock is not None:
        payload["currently_running"] = lock
    return jsonify(payload), 200


@app.get("/api/sync/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765)
