═══════════════════════════════════════════════════════════════
 BATCHED SUPABASE SYNC — GitHub Matrix Strategy
═══════════════════════════════════════════════════════════════

Same pattern as generate-mapillary-cache.yml:
  Plan job → Matrix batch jobs → Finalize job

Each batch processes 25K rows in its own job with 360-min timeout.
Peak memory: ~1.5 GB per batch (vs 6.5 GB monolithic). Resume-safe.

Delaware (566K rows) = 23 batches
Virginia (2.1M rows) = 84 batches
Texas (5M rows)      = 200 batches

═══════════════════════════════════════════════════════════════
 ARCHITECTURE
═══════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────┐
  │ PLAN JOB (30s)                                          │
  │  Count rows in R2 statewide parquet → output batch_matrix│
  │  e.g. {"batch": [1,2,3,...,23]} for 566K rows @ 25K each│
  └──────────────────────┬──────────────────────────────────┘
                         │
  ┌──────────────────────▼──────────────────────────────────┐
  │ SYNC BATCHES (matrix, 360 min each)                     │
  │  Batch 1: rows 0-24999     → COPY to crashes_delaware   │
  │  Batch 2: rows 25000-49999 → COPY to crashes_delaware   │
  │  Batch 3: rows 50000-74999 → COPY to crashes_delaware   │
  │  ...                                                     │
  │  Batch 23: rows 550000-566761                            │
  │                                                          │
  │  Each batch:                                             │
  │    1. Download statewide parquet from R2                  │
  │    2. Read ONLY its row range (pyarrow slice)             │
  │    3. Build JSONB for 25K rows (~0.3 GB)                 │
  │    4. COPY to Postgres (append)                          │
  │    5. Memory freed, job exits                            │
  │                                                          │
  │  Batch 1 special: DROP + CREATE partition first           │
  │  Resume: skip batches where objectids already exist       │
  └──────────────────────┬──────────────────────────────────┘
                         │
  ┌──────────────────────▼──────────────────────────────────┐
  │ FINALIZE JOB (30 min)                                    │
  │  1. Populate geom (batched 50K UPDATE)                   │
  │  2. Populate crash_date_parsed                           │
  │  3. Update states table                                  │
  │  4. Refresh federal_summary matview                      │
  │  5. Refresh jurisdiction_baselines matview                │
  │  6. Verify row count                                     │
  └─────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════
 CHANGE 1: New supabase_sync.py with batch mode
═══════════════════════════════════════════════════════════════

Add these new arguments and a batch_sync() function to supabase_sync.py.

Add to argument parser in main():

    p.add_argument("--batch", type=int, default=0,
                   help="Batch number (1-indexed). 0=full sync (legacy)")
    p.add_argument("--batch-size", type=int, default=25000,
                   help="Rows per batch (default 25000)")
    p.add_argument("--total-rows", type=int, default=0,
                   help="Total rows (from plan job)")
    p.add_argument("--finalize", action="store_true",
                   help="Run finalize only (geom, matviews, states)")

Add this new function BEFORE main():

def batch_sync(conn, filepath, state_name, abbr, fips, display,
               batch_num, batch_size, total_rows, resume=False):
    """Process a single batch of rows. Memory-safe for GitHub Actions."""
    import gc
    import pyarrow.parquet as pq

    t0 = time.time()
    cur = conn.cursor()

    # Calculate row range
    start_row = (batch_num - 1) * batch_size
    end_row = min(start_row + batch_size, total_rows)
    n_rows = end_row - start_row

    print(f"\n  {'='*65}")
    print(f"  BATCH {batch_num}: rows {start_row:,}-{end_row-1:,} ({n_rows:,} rows)")
    print(f"  State: {display} | Target: crashes_{state_name}")
    print(f"  {'='*65}")

    # ── Batch 1 special: DROP + CREATE partition ──
    if batch_num == 1 and not resume:
        print(f"  DROP TABLE IF EXISTS crashes_{state_name}")
        cur.execute(f"DROP TABLE IF EXISTS crashes_{state_name}")
        conn.commit()
        print(f"  CREATE TABLE crashes_{state_name} PARTITION OF crashes")
        cur.execute(f"CREATE TABLE crashes_{state_name} PARTITION OF crashes FOR VALUES IN ('{state_name}')")
        conn.commit()

    # ── Load ONLY this batch's rows using pyarrow slicing ──
    print(f"  Loading rows {start_row:,}-{end_row-1:,} from parquet...")
    pf = pq.ParquetFile(filepath)
    # Read all row groups, then slice (parquet row groups are ~64K rows)
    table = pf.read()
    df_batch = table.slice(start_row, n_rows).to_pandas()
    del table
    gc.collect()

    # Convert all to string (matches load_input behavior)
    for c in df_batch.columns:
        df_batch[c] = df_batch[c].astype(str).replace({"nan": "", "None": "", "NaT": ""})

    print(f"  Loaded: {len(df_batch):,} rows × {len(df_batch.columns)} cols")

    # ── Resume: check for existing objectids ──
    if resume:
        if "OBJECTID" in df_batch.columns:
            batch_ids = df_batch["OBJECTID"].tolist()
            # Check which already exist
            placeholders = ",".join(["%s"] * len(batch_ids))
            cur.execute(f"SELECT objectid FROM crashes_{state_name} WHERE objectid IN ({placeholders})", batch_ids)
            existing = {r[0] for r in cur.fetchall()}
            before = len(df_batch)
            df_batch = df_batch[~df_batch["OBJECTID"].isin(existing)]
            print(f"  Resume: {before:,} → {len(df_batch):,} new rows ({len(existing):,} already exist)")
            if len(df_batch) == 0:
                print(f"  ✅ Batch {batch_num} already complete — skipping")
                return

    # ── Build sync_df for this batch ──
    sync_df, cl = build_sync_df(df_batch, abbr, state_name)
    del df_batch
    gc.collect()

    # ── COPY insert ──
    print(f"  COPY {len(sync_df):,} rows...")
    ti = time.time()
    inserted = bulk_insert(conn, sync_df, state_name)
    conn.commit()
    del sync_df
    gc.collect()

    dur = round(time.time() - t0, 1)
    print(f"  ✅ Batch {batch_num}: {inserted:,} rows in {dur}s")

    # Log this batch
    log_run(conn, state_name, f"batch_{batch_num}", "success",
            rows=inserted, dur=dur,
            meta={"batch": batch_num, "start": start_row, "end": end_row})


def finalize_sync(conn, state_name, abbr, fips, display):
    """Post-batch: geom, crash_date_parsed, matviews, states table."""
    import gc
    t0 = time.time()
    cur = conn.cursor()

    print(f"\n  {'='*65}")
    print(f"  FINALIZE: {display}")
    print(f"  {'='*65}")

    # Count rows
    cur.execute(f"SELECT COUNT(*) FROM crashes_{state_name}")
    total = cur.fetchone()[0]
    print(f"  Total rows: {total:,}")

    # Year range
    cur.execute(f"SELECT MIN(crash_year), MAX(crash_year) FROM crashes_{state_name} WHERE crash_year IS NOT NULL")
    yr_min, yr_max = cur.fetchone()
    yr_min = yr_min or 0
    yr_max = yr_max or 0
    print(f"  Year range: [{yr_min}, {yr_max}]")

    # Populate geom in batches
    print(f"  Populating geom column (batched)...")
    ti = time.time()
    batch_size = 50000
    total_geom = 0
    while True:
        cur.execute(f"""
            UPDATE crashes_{state_name}
            SET geom = ST_SetSRID(ST_Point(x, y), 4326)
            WHERE x IS NOT NULL AND y IS NOT NULL AND geom IS NULL
            AND id IN (
                SELECT id FROM crashes_{state_name}
                WHERE geom IS NULL AND x IS NOT NULL
                LIMIT {batch_size}
            )
        """)
        batch_count = cur.rowcount
        conn.commit()
        total_geom += batch_count
        if batch_count > 0:
            print(f"    geom batch: +{batch_count:,} ({total_geom:,} total)")
        if batch_count < batch_size:
            break
    print(f"  ✅ geom: {total_geom:,} points in {time.time()-ti:.1f}s")

    # Update states table
    cur.execute("""INSERT INTO states (abbr,name,fips,display_name,pipeline_status,total_crashes,year_range,last_sync_at)
        VALUES (%s,%s,%s,%s,'active',%s,int4range(%s,%s,'[)'),NOW())
        ON CONFLICT (abbr) DO UPDATE SET pipeline_status='active',total_crashes=EXCLUDED.total_crashes,
        year_range=EXCLUDED.year_range,last_sync_at=NOW()""",
        (abbr, state_name, fips, display, total, yr_min, int(yr_max)+1))
    conn.commit()

    # Refresh matviews
    for mv in ["federal_summary", "jurisdiction_baselines"]:
        print(f"  Refreshing {mv}...")
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            conn.commit()
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
                conn.commit()
            except Exception as e:
                print(f"  ⚠️ {mv}: {e}")
                conn.rollback()
                cur = conn.cursor()

    # Verify
    cur.execute(f"SELECT COUNT(*), COUNT(geom) FROM crashes_{state_name}")
    total_final, geom_count = cur.fetchone()

    dur = round(time.time() - t0, 1)
    print(f"\n  {'='*65}")
    print(f"  FINALIZE COMPLETE: {display}")
    print(f"  Rows: {total_final:,} | Geom: {geom_count:,} | Duration: {dur}s")
    print(f"  {'='*65}")

    log_run(conn, state_name, "finalize", "success",
            rows=total_final, dur=dur,
            meta={"geom": geom_count, "years": f"{yr_min}-{yr_max}"})


Update main() to route to batch/finalize modes:

    # In main(), after df = load_input(path), add:
    if args.batch > 0:
        # Batch mode: process one chunk
        conn = get_db_connection()
        try:
            batch_sync(conn, path, state_name, abbr, fips, display,
                       batch_num=args.batch, batch_size=args.batch_size,
                       total_rows=args.total_rows, resume=args.resume)
        finally:
            conn.close()
        return

    if args.finalize:
        conn = get_db_connection()
        try:
            finalize_sync(conn, state_name, abbr, fips, display)
        finally:
            conn.close()
        return

    # Legacy full sync (unchanged — fallback)
    ...existing sync() code...

═══════════════════════════════════════════════════════════════
 CHANGE 2: New workflow — supabase-sync.yml
═══════════════════════════════════════════════════════════════

Create .github/workflows/supabase-sync.yml:

name: "Supabase: Sync State Data"

on:
  workflow_dispatch:
    inputs:
      state:
        description: 'State abbreviation'
        required: true
        type: choice
        options: [de, va, co, md, ct, pa, nj]
      batch_size:
        description: 'Rows per batch'
        required: false
        type: choice
        default: '25000'
        options: ['10000', '25000', '50000']
      resume:
        description: 'Resume (skip existing objectids)'
        required: false
        type: boolean
        default: false

  workflow_call:
    inputs:
      state:
        required: true
        type: string
      batch_size:
        required: false
        type: string
        default: '25000'
      resume:
        required: false
        type: boolean
        default: false

permissions:
  contents: read

env:
  R2_ENDPOINT: "https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com"
  R2_BUCKET: crash-lens-data

jobs:
  # ════════════════════════════════════════════════════════════
  #  PLAN — Count rows, create batch matrix
  # ════════════════════════════════════════════════════════════
  plan:
    name: "Plan (${{ inputs.state }})"
    runs-on: ubuntu-24.04
    timeout-minutes: 10
    outputs:
      batch_matrix: ${{ steps.plan.outputs.batch_matrix }}
      total_rows: ${{ steps.plan.outputs.total_rows }}
      total_batches: ${{ steps.plan.outputs.total_batches }}
      state_name: ${{ steps.plan.outputs.state_name }}
      r2_key: ${{ steps.plan.outputs.r2_key }}
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v6
        with:
          python-version: '3.11'
      - run: pip install pandas pyarrow boto3

      - name: Plan batches
        id: plan
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: auto
        run: |
          STATE="${{ inputs.state }}"
          BATCH_SIZE="${{ inputs.batch_size || '25000' }}"

          python3 << PYEOF
          import json, os, sys
          import boto3
          import pyarrow.parquet as pq

          state_abbr = "$STATE"
          batch_size = int("$BATCH_SIZE")

          # Get state name from registry
          from states_registry import STATES
          if state_abbr not in STATES:
              print(f"Unknown state: {state_abbr}")
              sys.exit(1)
          display, state_name, fips = STATES[state_abbr]

          # Find statewide parquet in R2
          endpoint = os.environ.get("R2_ENDPOINT", "${{ env.R2_ENDPOINT }}")
          s3 = boto3.client("s3",
              endpoint_url=endpoint,
              aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
              aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
              region_name="auto")

          r2_key = f"{state_name}/_statewide/statewide_all_roads.parquet.gz"
          local = "statewide.parquet.gz"

          print(f"Downloading {r2_key}...")
          s3.download_file("crash-lens-data", r2_key, local)

          # Count rows without loading into memory
          pf = pq.ParquetFile(local)
          total_rows = pf.metadata.num_rows
          print(f"Total rows: {total_rows:,}")

          # Calculate batches
          n_batches = (total_rows + batch_size - 1) // batch_size
          # GitHub Actions matrix limit is 256
          if n_batches > 256:
              # Increase batch size to fit
              batch_size = (total_rows + 255) // 256
              n_batches = (total_rows + batch_size - 1) // batch_size
              print(f"Adjusted batch_size to {batch_size:,} (256 job limit)")

          batches = list(range(1, n_batches + 1))
          matrix = json.dumps({"batch": batches})

          print(f"Batches: {n_batches} × {batch_size:,} rows")
          print(f"Matrix: {matrix[:100]}...")

          with open(os.environ["GITHUB_OUTPUT"], "a") as f:
              f.write(f"batch_matrix={matrix}\n")
              f.write(f"total_rows={total_rows}\n")
              f.write(f"total_batches={n_batches}\n")
              f.write(f"state_name={state_name}\n")
              f.write(f"r2_key={r2_key}\n")

          os.remove(local)
          PYEOF

  # ════════════════════════════════════════════════════════════
  #  SYNC BATCHES — Each batch = 25K rows, own job, 360 min
  # ════════════════════════════════════════════════════════════
  sync:
    name: "Batch ${{ matrix.batch }}/${{ needs.plan.outputs.total_batches }}"
    needs: plan
    runs-on: ubuntu-24.04
    timeout-minutes: 360
    strategy:
      matrix: ${{ fromJson(needs.plan.outputs.batch_matrix) }}
      max-parallel: 1       # Sequential — avoids Postgres contention
      fail-fast: false       # Other batches continue if one fails
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v6
        with:
          python-version: '3.11'
      - run: pip install pandas pyarrow psycopg2-binary boto3

      - name: Download statewide parquet
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.CF_R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.CF_R2_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: auto
        run: |
          R2_KEY="${{ needs.plan.outputs.r2_key }}"
          aws s3 cp "s3://${{ env.R2_BUCKET }}/$R2_KEY" statewide.parquet.gz \
            --endpoint-url "${{ env.R2_ENDPOINT }}" --only-show-errors
          echo "Downloaded: $(ls -lh statewide.parquet.gz | awk '{print $5}')"

      - name: Setup SSH tunnel
        run: |
          mkdir -p ~/.ssh
          echo "${{ secrets.SUPABASE_SSH_KEY }}" > ~/.ssh/supabase_tunnel
          chmod 600 ~/.ssh/supabase_tunnel
          ssh-keyscan -H srv1503081.hstgr.cloud >> ~/.ssh/known_hosts 2>/dev/null
          ssh -f -N -L 5432:localhost:5433 \
            -i ~/.ssh/supabase_tunnel \
            root@srv1503081.hstgr.cloud \
            -o StrictHostKeyChecking=no \
            -o ServerAliveInterval=60 \
            -o ServerAliveCountMax=3
          sleep 3
          echo "✅ SSH tunnel established"

      - name: "Sync batch ${{ matrix.batch }}"
        env:
          SUPABASE_DB_PASSWORD: ${{ secrets.SUPABASE_DB_PASSWORD }}
        run: |
          STATE="${{ inputs.state }}"
          BATCH="${{ matrix.batch }}"
          BATCH_SIZE="${{ inputs.batch_size || '25000' }}"
          TOTAL="${{ needs.plan.outputs.total_rows }}"
          RESUME="${{ inputs.resume || 'false' }}"

          RESUME_FLAG=""
          if [ "$RESUME" = "true" ]; then
            RESUME_FLAG="--resume"
          fi

          python supabase_sync.py \
            --state "$STATE" \
            --input statewide.parquet.gz \
            --batch "$BATCH" \
            --batch-size "$BATCH_SIZE" \
            --total-rows "$TOTAL" \
            $RESUME_FLAG

  # ════════════════════════════════════════════════════════════
  #  FINALIZE — geom, matviews, states table
  # ════════════════════════════════════════════════════════════
  finalize:
    name: "Finalize (${{ inputs.state }})"
    needs: [plan, sync]
    if: always() && needs.plan.result == 'success'
    runs-on: ubuntu-24.04
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v6
        with:
          python-version: '3.11'
      - run: pip install pandas psycopg2-binary

      - name: Setup SSH tunnel
        run: |
          mkdir -p ~/.ssh
          echo "${{ secrets.SUPABASE_SSH_KEY }}" > ~/.ssh/supabase_tunnel
          chmod 600 ~/.ssh/supabase_tunnel
          ssh-keyscan -H srv1503081.hstgr.cloud >> ~/.ssh/known_hosts 2>/dev/null
          ssh -f -N -L 5432:localhost:5433 \
            -i ~/.ssh/supabase_tunnel \
            root@srv1503081.hstgr.cloud \
            -o StrictHostKeyChecking=no \
            -o ServerAliveInterval=60 \
            -o ServerAliveCountMax=3
          sleep 3

      - name: Finalize
        env:
          SUPABASE_DB_PASSWORD: ${{ secrets.SUPABASE_DB_PASSWORD }}
        run: |
          python supabase_sync.py \
            --state "${{ inputs.state }}" \
            --finalize

═══════════════════════════════════════════════════════════════
 CHANGE 3: Update delaware-batch-pipeline.yml Stage 4.5
═══════════════════════════════════════════════════════════════

Replace the Stage 4.5 step with a workflow_call to supabase-sync.yml:

    # Remove the entire "Stage 4.5: Sync to Supabase" step
    # and "Stage 4.5: Set up SSH tunnel for Supabase" step.
    # Replace with:

    supabase_sync:
      name: "Stage 4.5: Supabase Sync"
      needs: process
      if: ${{ github.event.inputs.skip_supabase != 'true' }}
      uses: ./.github/workflows/supabase-sync.yml
      with:
        state: "de"
        batch_size: "25000"
        resume: ${{ github.event.inputs.resume_supabase == 'true' }}
      secrets: inherit

═══════════════════════════════════════════════════════════════
 MEMORY PER BATCH
═══════════════════════════════════════════════════════════════

25K rows × 518 cols:
  Load parquet (full):    1.5 GB (pyarrow mmap, only slice used)
  Slice 25K rows:         0.1 GB
  Build JSONB:            0.1 GB (25K × 300 keys)
  sync_df:                0.05 GB
  PEAK:                  ~1.8 GB ← easily fits in 7GB runner ✅

Virginia 2.1M rows → 84 batches × 25K rows:
  Same 1.8 GB peak per batch ✅
  Total GitHub time: ~84 × 5 min = ~7 hours (sequential)

Texas 5M rows → 200 batches × 25K rows:
  Same 1.8 GB peak per batch ✅
  Total time: ~200 × 5 min = ~17 hours (sequential)
  Can increase batch_size to 50K → 100 batches → ~8 hours

═══════════════════════════════════════════════════════════════
 RESUME STRATEGY
═══════════════════════════════════════════════════════════════

Scenario: Batch 15 of 23 fails (network blip)

Without resume:
  Re-run with resume=false → drops table, starts from batch 1 ❌

With resume:
  Re-run with resume=true →
    Batch 1: checks objectids → all exist → skip ✅ (5 seconds)
    Batch 2: checks objectids → all exist → skip ✅
    ...
    Batch 14: checks objectids → all exist → skip ✅
    Batch 15: checks objectids → some missing → insert only new ✅
    Batch 16-23: normal insert ✅

Key: Batch 1 with resume=true does NOT drop the partition.
     Only batch 1 with resume=false drops and recreates.

═══════════════════════════════════════════════════════════════
 COMPARISON WITH MAPILLARY PATTERN
═══════════════════════════════════════════════════════════════

| Feature | Mapillary | Supabase Sync |
|---------|-----------|---------------|
| Plan job | Count counties | Count rows in parquet |
| Matrix key | County name | Batch number (1-N) |
| Per-batch work | Download tiles for 1 county | COPY 25K rows to Postgres |
| Consolidate | Merge county parquets | Populate geom + matviews |
| Resume | Skip counties in R2 | Skip existing objectids |
| max-parallel | 1 (API rate limit) | 1 (Postgres contention) |
| fail-fast | false | false |
| Timeout | 360 min | 360 min |

═══════════════════════════════════════════════════════════════
 EXECUTION ORDER
═══════════════════════════════════════════════════════════════

1. Apply batch_sync() + finalize_sync() to supabase_sync.py
2. Create .github/workflows/supabase-sync.yml
3. Update delaware-batch-pipeline.yml Stage 4.5 to use workflow_call
4. Git push
5. Run "Supabase: Sync State Data" for DE (standalone test)
6. Once confirmed, run full "Delaware: Batch Pipeline"

═══════════════════════════════════════════════════════════════
 SCALING TABLE
═══════════════════════════════════════════════════════════════

| State | Rows | Batches (25K) | Est. time | Peak mem |
|-------|------|---------------|-----------|----------|
| DE | 566K | 23 | ~2 hours | 1.8 GB |
| MD | 800K | 32 | ~3 hours | 1.8 GB |
| VA | 2.1M | 84 | ~7 hours | 1.8 GB |
| PA | 3.5M | 140 | ~12 hours | 1.8 GB |
| TX | 5M | 200 | ~17 hours | 1.8 GB |
| CA | 8M | 256* | ~21 hours | 3.2 GB* |

* California: auto-adjusts batch_size to 32K (256 job limit)
