#!/usr/bin/env python3
"""
CrashLens Incremental Diff Engine — content-hash based diff for crash data.

Compares freshly normalized data against the existing R2 statewide file
using a 5-field content hash (Crash Date | Crash Military Time | x | y | Collision Type).

Usage:
  python incremental_diff.py --state de --fresh normalized.parquet.gz --output diff_result/
  python incremental_diff.py --state de --fresh normalized.parquet.gz --output diff_result/ --force-full

Modes:
  incremental — <threshold% new rows → enrich only new rows
  full        — ≥threshold% new rows (or forced/first load/code change)
  skip        — 0 new rows
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

# ─── Content hash columns (5-field, handles multi-vehicle via Collision Type) ──
HASH_COLUMNS = ["Crash Date", "Crash Military Time", "x", "y", "Collision Type"]
DEFAULT_THRESHOLD = 10


def compute_crash_hashes(df):
    """Vectorized content hash using 5 immutable source fields."""
    concat = (
        df["Crash Date"].fillna("").astype(str) + "|" +
        df["Crash Military Time"].fillna("").astype(str) + "|" +
        df["x"].fillna("").astype(str) + "|" +
        df["y"].fillna("").astype(str) + "|" +
        df["Collision Type"].fillna("").astype(str)
    )
    return concat.apply(lambda s: hashlib.md5(s.encode()).hexdigest())


def download_existing_hashes(state_name, abbr):
    """Download ONLY hash columns from existing R2 statewide (~5MB not 180MB)."""
    import boto3
    import pyarrow.parquet as pq

    acct = os.environ.get("CF_ACCOUNT_ID")
    akey = os.environ.get("CF_R2_ACCESS_KEY_ID")
    skey = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
    if not all([acct, akey, skey]):
        print("  R2 creds not available — forcing full mode")
        return None

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
        aws_access_key_id=akey,
        aws_secret_access_key=skey,
        region_name="auto",
    )

    # Try multiple known R2 paths (handles old + new upload locations)
    for key in [
        f"{state_name}/_state/all_roads.parquet",
        f"{state_name}/_state/all_roads.parquet.gz",
        f"{state_name}/_statewide/statewide_all_roads.parquet.gz",
    ]:
        try:
            local = f"/tmp/existing_{abbr}.parquet"
            print(f"  Trying R2: {key}")
            s3.download_file("crash-lens-data", key, local)

            available = pq.read_schema(local).names
            read_cols = [c for c in HASH_COLUMNS if c in available]
            if len(read_cols) < 4:
                print(f"  Only {len(read_cols)}/5 hash columns — skipping this file")
                os.remove(local)
                continue

            df = pd.read_parquet(local, columns=read_cols)
            os.remove(local)
            print(f"  Loaded {len(df):,} existing rows for comparison")
            return df
        except Exception as e:
            print(f"  {key}: {e}")
            continue

    print("  No existing statewide in R2 — first load, forcing full")
    return None


def check_pipeline_version(state_name):
    """Check if pipeline code changed since last run. Mismatch → force full."""
    import boto3

    enricher_path = Path(__file__).parent / "crash_enricher.py"
    if not enricher_path.exists():
        return True

    current_hash = hashlib.md5(enricher_path.read_bytes()).hexdigest()[:12]

    try:
        acct = os.environ.get("CF_ACCOUNT_ID")
        akey = os.environ.get("CF_R2_ACCESS_KEY_ID")
        skey = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
        if not all([acct, akey, skey]):
            return True

        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
            aws_access_key_id=akey,
            aws_secret_access_key=skey,
            region_name="auto",
        )
        obj = s3.get_object(
            Bucket="crash-lens-data",
            Key=f"{state_name}/_statewide/.pipeline_version",
        )
        stored = obj["Body"].read().decode().strip()
        if stored != current_hash:
            print(f"  Pipeline code changed ({stored[:8]}→{current_hash[:8]}) — forcing full")
            return True
        print(f"  Pipeline version unchanged ({current_hash[:8]})")
        return False
    except Exception:
        print("  No stored pipeline version — forcing full")
        return True


def save_pipeline_version(state_name):
    """Store current pipeline version hash in R2 for next run."""
    import boto3

    enricher_path = Path(__file__).parent / "crash_enricher.py"
    if not enricher_path.exists():
        return

    current_hash = hashlib.md5(enricher_path.read_bytes()).hexdigest()[:12]

    try:
        acct = os.environ.get("CF_ACCOUNT_ID")
        akey = os.environ.get("CF_R2_ACCESS_KEY_ID")
        skey = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
        if not all([acct, akey, skey]):
            return

        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
            aws_access_key_id=akey,
            aws_secret_access_key=skey,
            region_name="auto",
        )
        s3.put_object(
            Bucket="crash-lens-data",
            Key=f"{state_name}/_statewide/.pipeline_version",
            Body=current_hash.encode(),
        )
        print(f"  Saved pipeline version: {current_hash[:8]}")
    except Exception as e:
        print(f"  WARNING: Could not save pipeline version: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="CrashLens Incremental Diff — content-hash based diff engine"
    )
    parser.add_argument(
        "--state", required=True,
        help="State abbreviation (2-letter lowercase, e.g. 'de')",
    )
    parser.add_argument(
        "--fresh", required=True,
        help="Path to freshly normalized parquet file",
    )
    parser.add_argument(
        "--output", required=True,
        help="Output directory for new_rows + diff_report.json",
    )
    parser.add_argument(
        "--threshold", type=int, default=DEFAULT_THRESHOLD,
        help=f"Percent threshold for incremental vs full (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--force-full", action="store_true",
        help="Force full mode regardless of diff result",
    )
    args = parser.parse_args()

    t0 = time.time()
    abbr = args.state.lower().strip()

    # Validate state against registry
    try:
        from states_registry import STATES
    except ImportError:
        # Fallback: allow any 2-letter code
        STATES = None

    if STATES and abbr not in STATES:
        print(f"  ERROR: '{abbr}' not in STATES registry")
        sys.exit(1)

    if STATES:
        _, state_name, _ = STATES[abbr]
    else:
        state_name = abbr

    # Create output directory
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load fresh parquet (hash columns only for comparison)
    print(f"\n{'='*60}")
    print(f"  CrashLens Incremental Diff")
    print(f"  State: {abbr} ({state_name})")
    print(f"  Fresh: {args.fresh}")
    print(f"  Threshold: {args.threshold}%")
    print(f"{'='*60}\n")

    fresh_path = Path(args.fresh)
    if not fresh_path.exists():
        print(f"  ERROR: Fresh file not found: {args.fresh}")
        sys.exit(1)

    # Check which hash columns are available
    import pyarrow.parquet as pq
    available_cols = pq.read_schema(str(fresh_path)).names
    hash_cols_present = [c for c in HASH_COLUMNS if c in available_cols]
    if len(hash_cols_present) < 4:
        print(f"  Only {len(hash_cols_present)}/5 hash columns in fresh data — forcing full")
        mode = "full"
        reason = f"insufficient hash columns ({len(hash_cols_present)}/5)"
        fresh_hash_df = None
        total_fresh = len(pd.read_parquet(str(fresh_path), columns=[available_cols[0]]))
        new_count = total_fresh
        pct_new = 100.0
    else:
        fresh_hash_df = pd.read_parquet(str(fresh_path), columns=hash_cols_present)
        total_fresh = len(fresh_hash_df)
        print(f"  Fresh data: {total_fresh:,} rows")

        # Determine mode
        if args.force_full:
            mode = "full"
            reason = "forced"
            new_count = total_fresh
            pct_new = 100.0
            print(f"  Mode: FULL (--force-full)")
        elif check_pipeline_version(state_name):
            mode = "full"
            reason = "pipeline code changed"
            new_count = total_fresh
            pct_new = 100.0
            print(f"  Mode: FULL (pipeline code changed)")
        else:
            existing_df = download_existing_hashes(state_name, abbr)
            if existing_df is None:
                mode = "full"
                reason = "first load"
                new_count = total_fresh
                pct_new = 100.0
                print(f"  Mode: FULL (first load — no existing data)")
            else:
                # Compute hashes for both
                print("  Computing content hashes...")
                fresh_hashes = set(compute_crash_hashes(fresh_hash_df))
                existing_hashes = set(compute_crash_hashes(existing_df))
                del existing_df

                new_hashes = fresh_hashes - existing_hashes
                new_count = len(new_hashes)
                pct_new = (new_count / total_fresh * 100) if total_fresh > 0 else 0

                print(f"  Existing: {len(existing_hashes):,} hashes")
                print(f"  Fresh:    {len(fresh_hashes):,} hashes")
                print(f"  New:      {new_count:,} ({pct_new:.1f}%)")

                if pct_new >= args.threshold:
                    mode = "full"
                    reason = f"{pct_new:.1f}% new >= {args.threshold}% threshold"
                elif new_count == 0:
                    mode = "skip"
                    reason = "no new rows"
                else:
                    mode = "incremental"
                    reason = f"{new_count:,} new rows ({pct_new:.1f}% < {args.threshold}%)"

                print(f"  Mode: {mode.upper()} ({reason})")

    # Output new_rows file
    if mode == "incremental":
        print(f"\n  Extracting {new_count:,} new rows...")
        # Reload full fresh parquet, filter to new rows by hash
        full_fresh = pd.read_parquet(str(fresh_path))
        full_fresh["_content_hash"] = compute_crash_hashes(full_fresh)
        new_rows = full_fresh[full_fresh["_content_hash"].isin(new_hashes)].copy()
        new_rows.drop(columns=["_content_hash"], inplace=True)
        del full_fresh

        out_path = out_dir / "new_rows.parquet.gz"
        new_rows.to_parquet(str(out_path), engine="pyarrow", compression="snappy", index=False)
        print(f"  Saved: {out_path} ({len(new_rows):,} rows)")
        del new_rows

    elif mode == "full":
        out_path = out_dir / "new_rows.parquet.gz"
        shutil.copy2(str(fresh_path), str(out_path))
        print(f"  Copied full file as new_rows: {out_path}")

    # Save pipeline version (skip mode doesn't need it)
    if mode != "skip":
        save_pipeline_version(state_name)

    # Write diff report
    duration = round(time.time() - t0, 1)
    report = {
        "state": abbr,
        "state_name": state_name,
        "mode": mode,
        "reason": reason,
        "total_fresh": total_fresh,
        "new_count": new_count,
        "pct_new": round(pct_new, 2),
        "threshold": args.threshold,
        "duration_sec": duration,
    }
    report_path = out_dir / "diff_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n  Report: {report_path}")
    print(f"  Duration: {duration}s")

    # GitHub Actions output
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as f:
            f.write(f"mode={mode}\n")
            f.write(f"new_count={new_count}\n")
            f.write(f"pct_new={round(pct_new, 2)}\n")

    print(f"\n{'='*60}")
    print(f"  RESULT: {mode.upper()} — {reason}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
