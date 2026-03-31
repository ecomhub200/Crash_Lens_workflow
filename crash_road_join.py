#!/usr/bin/env python3
"""
crash_road_join.py — Spatial Join: Crash CSV + Road Inventory Parquet

Single-purpose: take a normalized crash CSV, find the nearest road
segment for each crash, attach road inventory columns, save output.

This is the ONLY slow step in the pipeline. Everything else is fast.
Separated from de_normalize.py so it can run independently, be retried,
or moved to a faster machine (VPS vs CI runner).

Usage:
python crash_road_join.py --crashes normalized.csv --state de --output joined.csv
python crash_road_join.py --crashes normalized.csv --state de --cache-dir cache

Architecture:
1. Load road inventory parquet ONCE via RoadInventorySession (~10s)
- Includes: proximity cleanup, column drops, 12-rule validator
2. Load crash CSV
3. Run Tier 1 self-enrichment (flags, severity — no spatial data needed)
4. Per-county spatial join via KDTree + perpendicular refinement
5. Derive Intersection Analysis (needs Ownership from road join)
6. Save joined CSV

Memory budget (GitHub Actions 7GB):
Road inventory session:  ~0.7 GB (stays loaded)
One county chunk:        ~0.3 GB (freed after write)
Peak:                    ~1.0 GB (no full dataset in memory)
"""

import argparse
import gc
import os
import sys
import time
from pathlib import Path

import pandas as pd

def main():
    parser = argparse.ArgumentParser(
        description="Spatial join: crash GPS → nearest road segment")
    parser.add_argument("--crashes", "-c", required=True,
                        help="Input normalized crash CSV")
    parser.add_argument("--output", "-o", default=None,
                        help="Output joined CSV (default: {input}_joined.csv)")
    parser.add_argument("--state", "-s", required=True,
                        help="State abbreviation (e.g. de)")
    parser.add_argument("--state-fips", default=None,
                        help="State FIPS code (auto-detected if not provided)")
    parser.add_argument("--state-name", default=None,
                        help="State full name (auto-detected if not provided)")
    parser.add_argument("--cache-dir", default="cache",
                        help="Cache directory with road inventory parquet")
    parser.add_argument("--max-chunk", type=int, default=75_000,
                        help="Max rows per enrichment chunk")
    args = parser.parse_args()

    abbr = args.state.lower()
    crashes_path = Path(args.crashes)
    cache_dir = args.cache_dir

    if not crashes_path.exists():
        print(f"❌ Crash file not found: {crashes_path}")
        sys.exit(1)

    output_path = args.output or str(crashes_path.with_suffix("").with_suffix(".joined.csv"))

    # ── Auto-detect state info ──
    STATE_REGISTRY = {
        "al": ("01", "Alabama"), "ak": ("02", "Alaska"), "az": ("04", "Arizona"),
        "ar": ("05", "Arkansas"), "ca": ("06", "California"), "co": ("08", "Colorado"),
        "ct": ("09", "Connecticut"), "de": ("10", "Delaware"),
        "dc": ("11", "District of Columbia"), "fl": ("12", "Florida"),
        "ga": ("13", "Georgia"), "hi": ("15", "Hawaii"), "id": ("16", "Idaho"),
        "il": ("17", "Illinois"), "in": ("18", "Indiana"), "ia": ("19", "Iowa"),
        "ks": ("20", "Kansas"), "ky": ("21", "Kentucky"), "la": ("22", "Louisiana"),
        "me": ("23", "Maine"), "md": ("24", "Maryland"), "ma": ("25", "Massachusetts"),
        "mi": ("26", "Michigan"), "mn": ("27", "Minnesota"), "ms": ("28", "Mississippi"),
        "mo": ("29", "Missouri"), "mt": ("30", "Montana"), "ne": ("31", "Nebraska"),
        "nv": ("32", "Nevada"), "nh": ("33", "New Hampshire"), "nj": ("34", "New Jersey"),
        "nm": ("35", "New Mexico"), "ny": ("36", "New York"),
        "nc": ("37", "North Carolina"), "nd": ("38", "North Dakota"),
        "oh": ("39", "Ohio"), "ok": ("40", "Oklahoma"), "or": ("41", "Oregon"),
        "pa": ("42", "Pennsylvania"), "ri": ("44", "Rhode Island"),
        "sc": ("45", "South Carolina"), "sd": ("46", "South Dakota"),
        "tn": ("47", "Tennessee"), "tx": ("48", "Texas"), "ut": ("49", "Utah"),
        "vt": ("50", "Vermont"), "va": ("51", "Virginia"), "wa": ("53", "Washington"),
        "wv": ("54", "West Virginia"), "wi": ("55", "Wisconsin"), "wy": ("56", "Wyoming"),
    }
    fips, name = STATE_REGISTRY.get(abbr, ("00", abbr.title()))
    fips = args.state_fips or fips
    name = args.state_name or name

    t0 = time.time()
    print("=" * 65)
    print(f"  CRASH ↔ ROAD INVENTORY SPATIAL JOIN")
    print(f"  State:   {name} ({abbr.upper()})")
    print(f"  Crashes: {crashes_path}")
    print(f"  Output:  {output_path}")
    print("=" * 65)

    # ── Step 1: Load road inventory ──
    print(f"\n  [1/5] Loading road inventory...")
    try:
        from road_inventory_enricher import RoadInventorySession
    except ImportError:
        print("❌ road_inventory_enricher.py not found")
        sys.exit(1)

    session = RoadInventorySession(abbr, cache_dir)
    if not session.ready:
        print("❌ Road inventory not available")
        sys.exit(1)

    # ── Step 2: Load crash data ──
    print(f"\n  [2/5] Loading crash data...")
    t_load = time.time()
    df = pd.read_csv(crashes_path, dtype=str, low_memory=False)
    n_total = len(df)
    print(f"    {n_total:,} rows × {len(df.columns)} cols ({time.time()-t_load:.1f}s)")

    # ── Step 3: Tier 1 self-enrichment (flags — no spatial data) ──
    print(f"\n  [3/5] Tier 1 self-enrichment...")
    try:
        from crash_enricher import CrashEnricher
        enricher = CrashEnricher(fips, abbr.upper(), name, cache_dir=cache_dir)
        df = enricher.enrich_tier1(df)
    except ImportError:
        print("    crash_enricher.py not found — skipping Tier 1")
        enricher = None

    # ── Step 4: Spatial join (per-county chunked) ──
    print(f"\n  [4/5] Spatial join ({n_total:,} crashes → {len(session.ri):,} road segments)...")
    counties = df["FIPS"].fillna("").str.strip() if "FIPS" in df.columns \
        else pd.Series("000", index=df.index)
    unique_fips = sorted([f for f in counties.unique() if f])

    if not unique_fips:
        unique_fips = ["000"]
        counties = pd.Series("000", index=df.index)

    header_written = False
    total_matched = 0
    all_columns = None
    t_join = time.time()
    MAX_CHUNK = args.max_chunk

    # Discover columns from first small chunk
    first_chunk = df.loc[counties == unique_fips[0]].head(100).copy()
    first_enriched = session.enrich(first_chunk)
    all_columns = list(first_enriched.columns)
    del first_chunk, first_enriched
    gc.collect()

    temp_path = output_path + ".tmp"

    for i, fip in enumerate(unique_fips):
        county_mask = counties == fip
        county_df = df.loc[county_mask].copy()

        county_name = county_df["Physical Juris Name"].iloc[0] \
            if "Physical Juris Name" in county_df.columns else fip

        # Sub-chunk large counties
        if len(county_df) <= MAX_CHUNK:
            sub_chunks = [county_df]
        else:
            n_sub = (len(county_df) + MAX_CHUNK - 1) // MAX_CHUNK
            sub_chunks = [county_df.iloc[j*MAX_CHUNK:(j+1)*MAX_CHUNK].copy()
                          for j in range(n_sub)]

        label = f"[{i+1}/{len(unique_fips)}] {county_name} ({len(county_df):,}"
        if len(sub_chunks) > 1:
            label += f", {len(sub_chunks)} chunks"
        label += ")"
        print(f"    {label}")

        for sub in sub_chunks:
            try:
                enriched = session.enrich(sub)
            except MemoryError:
                gc.collect()
                half = len(sub) // 2
                if half < 5000:
                    enriched = sub
                else:
                    print(f"      ⚠️ OOM — retrying as 2×{half:,}")
                    e1 = session.enrich(sub.iloc[:half].copy())
                    gc.collect()
                    e2 = session.enrich(sub.iloc[half:].copy())
                    gc.collect()
                    enriched = pd.concat([e1, e2], ignore_index=False)
                    del e1, e2
            except Exception as e:
                print(f"      ❌ {e}")
                enriched = sub

            total_matched += (enriched.get("ri_matched", pd.Series("")) == "Yes").sum()

            for col in all_columns:
                if col not in enriched.columns:
                    enriched[col] = ""

            enriched[all_columns].to_csv(
                temp_path, mode="a" if header_written else "w",
                header=not header_written, index=False)
            header_written = True

            del enriched, sub
            gc.collect()

        del county_df, sub_chunks
        gc.collect()

    del session
    gc.collect()

    join_time = time.time() - t_join
    match_pct = total_matched / max(n_total, 1) * 100

    # ── Step 5: Post-join (Intersection Analysis + final save) ──
    print(f"\n  [5/5] Post-join processing...")
    df = pd.read_csv(temp_path, dtype=str, low_memory=False)

    if enricher:
        df = enricher._derive_intersection_analysis(df)

    df.to_csv(output_path, index=False)

    # Cleanup temp
    try:
        os.remove(temp_path)
    except OSError:
        pass

    # ── Summary ──
    elapsed = time.time() - t0
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n{'=' * 65}")
    print(f"  DONE: Spatial Join Complete")
    print(f"  Matched: {total_matched:,}/{n_total:,} ({match_pct:.1f}%)")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Output:  {output_path} ({size_mb:.0f} MB)")
    print(f"  Join:    {join_time:.0f}s | Total: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
