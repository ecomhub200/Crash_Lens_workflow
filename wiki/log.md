---
title: Wiki Log
type: log
updated: 2026-04-12
---

# Crash Lens Wiki — Log

Chronological record of wiki activity.

---

## [2026-04-12] fix | Permanent geom/date fix — BEFORE INSERT trigger + finalize rewrite

Eliminates the recurring 2–4h VPS hang caused by `finalize_sync()` running one massive `UPDATE crashes_{state} SET geom = ST_Point(x,y)` on 570K+ rows in a single transaction (WAL explosion → table lock → Studio unresponsive).

**Three layers, fully state-agnostic:**

1. **VPS one-time (Step 1 — done):** Installed `BEFORE INSERT` trigger `trg_compute_geom` on the `crashes` parent partitioned table. Auto-computes `geom = ST_SetSRID(ST_Point(x, y), 4326)` and `crash_date_parsed = TO_DATE(crash_date, 'MM/DD/YYYY')` per-row during every COPY. Skips NaN/Infinity/out-of-range coords. Auto-propagates to all 51 state partitions (existing + future, including `DROP TABLE … PARTITION OF crashes` recreations). Bad dates become NULL via `EXCEPTION WHEN OTHERS`.
2. **supabase_sync.py — Step 2 (this change):**
   - Added `TRIGGER_MANAGED_COLUMNS = {"geom", "crash_date_parsed"}` (supabase_sync.py:208).
   - `bulk_insert()` now excludes trigger-managed columns from the COPY column list so empty strings never hit GEOMETRY / DATE types (supabase_sync.py:417–434).
   - `sync()` no longer runs the inline batched geom UPDATE loop — replaced with a verify-only read that flags any rows the trigger missed (supabase_sync.py:510–526).
   - `finalize_sync()` fully rewritten (supabase_sync.py:677–856):
     - Kills stuck prior-run queries scoped to `crashes_{state_name}` partition only (doesn't affect parallel finalizes for other states).
     - `pg_try_advisory_lock(42)` prevents concurrent finalize races on matview refresh.
     - Safety-net geom + date backfills use 10K `ctid`-keyed batches with commit between — can never hang, should always be 0 rows with the trigger active.
     - Matview refresh pattern (`CONCURRENTLY` with blocking fallback) preserved.
     - `states` upsert payload and `log_run()` telemetry preserved byte-for-byte.
     - Advisory lock always released in `finally`.
3. **webhook/webhook.py:** Comment-only update at line 294/304 — webhook remains a pure orchestrator and needs zero code changes. Finalize is now ~30s instead of 30min; 1800s timeout kept as conservative ceiling.

**Expected new-pipeline behavior:** Zero `UPDATE … SET geom` calls, zero table locks, zero hangs. Log should show `✅ geom: all rows already populated (trigger working)`. Rollback path: `DROP TRIGGER IF EXISTS trg_compute_geom ON crashes` — the safety-net backfill in `finalize_sync()` will then repopulate on the next run (slower but functional).

Files changed: `supabase_sync.py`, `webhook/webhook.py`, `wiki/concepts/supabase-sync-ci.md`, `wiki/entities/webhook-sync.md`, `wiki/log.md`. Files explicitly unchanged: `build_road_inventory.py`, `crash_enricher.py`, `split.py`, `TIER1_MAP`, `build_sync_df()`, `batch_sync()`.

---

## [2026-04-12] fix | State DOT authority + source tracking

Fixed FC/Ownership/SYSTEM priority so HPMS wins over StateDOT (FHWA validates federally). Added missing `resolved_fc_source`, `resolved_ownership_source`, `resolved_facility_source` columns. Added StateDOT column to source contribution matrix. Added DOT counts to Lanes/Surface/FC print statements. Swapped pipeline order (HPMS before DOT). Freed state_dot/hpms memory after enrichment.

- **road_data_authority.py**: FC/Ownership priority: OSM → StateDOT → HPMS (HPMS wins). Added resolved source columns for FC, Ownership, Facility Type.
- **build_road_inventory.py**: Pipeline order: HPMS → State DOT. Source matrix includes StateDOT column. Memory freed after enrichment.

---

## [2026-04-12] fix | Data Quality — Intersection, Curvature, Speed

Three data quality fixes for Delaware Safety Focus tab (Kent County, no_interstate, 34,678 records):

- **Intersection Type**: Replaced GPS clustering (97.7% intersection rate — wrong) with road inventory node proximity detection. Uses `intersection_degree >= 3` + 30m distance to segment endpoints from OSM road graph. Neutered `_detect_intersections_from_clusters()` in `crash_enricher.py`; added vectorized proximity check in `road_inventory_enricher.py`.
- **Curvature / Roadway Alignment**: Fixed FHWA threshold — `curve_class <= 2` (Straight + Slight) now classified as straight per FHWA HSM base condition (was `<= 1`). Added curve_class OVERWRITE in `_canonicalize_post_enrichment()`. Updated OSM fallback `derive_roadway_alignment()` threshold from 1.05 to 1.10.
- **Speed diagnostic**: Added DE-specific logging in `_derive_flags_from_circumstance()` to investigate 1.4% speed rate vs 10-15% national average. Prints top 20 contributing circumstances and speed-keyword matches.

Files changed: `crash_enricher.py`, `road_inventory_enricher.py`, `build_road_inventory.py`.

## [2026-04-12] feat | Incremental Pipeline v1

Content-hash diff engine for crash data pipeline. Reduces enrichment from ~10 min to ~5 sec on typical daily runs (~500 new crashes out of 569K).

- **New file**: `incremental_diff.py` — 5-field MD5 content hash diff (Crash Date, Military Time, x, y, Collision Type). Modes: incremental (<10% new), full (>=10% or forced), skip (0 new).
- **de_normalize.py**: Added `--keep-objectids` flag for stable OBJECTID assignment during incremental merge (new rows get max+1).
- **supabase_sync.py**: Fixed R2 download paths — added `.parquet` (no .gz) as primary, plus `_statewide/` path.
- **webhook.py**: Added `mode` parameter (backward compatible, defaults to "full"). Phase 1: both modes do TRUNCATE+COPY.
- **delaware-batch-all-jurisdictions.yml**: Added Step 7.7 incremental diff, `force_full` input, mode/new_count outputs to pipeline trigger.
- **delaware-batch-pipeline.yml**: Mode-aware Stage 0.5 enrichment — incremental path enriches only new rows, merges with existing, re-ranks with `--keep-objectids`.

Addresses 12 pre-identified bugs (unstable Document Nbr hash, OBJECTID reassignment, R2 path mismatch, first-load handling, pipeline version detection, etc.).

## [2026-04-05] init | Wiki Created
Set up LLM Wiki structure following Karpathy's pattern. Created directory layout, CLAUDE.md schema, and initial wiki pages from two raw source repos.

## [2026-04-05] ingest | Crash_Lens_workflow-main
Ingested data pipeline repository. Created pages: [[crash-lens-workflow]], [[data-pipeline-architecture]], [[crash-enrichment]], [[state-onboarding]], [[source-workflow-repo]], [[github-actions-ci]].

## [2026-04-05] ingest | Douglas_County_2-main
Ingested frontend application repository. Created pages: [[douglas-county-frontend]], [[safety-countermeasures]], [[hotspot-analysis]], [[before-after-studies]], [[ai-integration]], [[source-frontend-repo]], [[firebase-auth]], [[stripe-billing]], [[qdrant-vector-db]].

## [2026-04-05] analysis | Cross-repo synthesis
Created cross-cutting analysis pages: [[crash-lens-overview]], [[technology-stack]], [[state-coverage]], [[data-sources-inventory]], [[cloudflare-r2]], [[llm-wiki-pattern]].


## [2026-04-06] create | supabase-schema + build-road-inventory + delaware-pipeline
Created wiki pages: [[supabase-schema]] (Supabase database schema and sync spec), [[build-road-inventory]] (road inventory consolidation pipeline), [[delaware-pipeline]] (reference state entity). Created `supabase_sync.py` (Pipeline Stage 5).


## [2026-04-06] create | supabase-schema-v3
Major architecture update: 3-Tier Column Strategy for Supabase. Validated against real Delaware data (517 cols → 111 explicit + 312 road_data JSONB + 18 state_extras JSONB + 76 ranking_data JSONB). Created `001_crashlens_migration_v3.sql` and `supabase_sync.py` v3.0. Supersedes [[supabase-schema]].

## [2026-04-06] validation | supabase-schema-v3 battle-tested
Full validation of migration SQL and sync script against real Delaware parquet (566,762 rows x 517 columns). Results:
- **TIER1_MAP**: All 111 keys present in data. Zero missing, zero mismatched.
- **Classification**: 111 + 312 + 18 + 76 = 517 (0 uncategorized, 0 catch-all).
- **Bug fixed**: `crash_date DATE` → `crash_date TEXT` (data is M/D/YYYY strings).
- **Bug fixed**: `work_zone_related = 'Yes'` → `LIKE '%Yes%'` (data has "1. Yes"/"2. No").
- **Confirmed**: "DOT District" (not "VDOT District") in pipeline output.
- **Confirmed**: All integer columns safe for `fillna(0).astype(int)`.
- **Confirmed**: No tab characters in data — COPY FORMAT text is safe.
- Created `verify_supabase.sql` with 24 post-migration verification queries.
- Updated [[supabase-schema-v3]] with Claude Chat Execution Guide.

## 2026-04-06 — Supabase Migration v3 Executed

**Migration:** `001_crashlens_migration_v3.sql` (3-Tier Column Strategy)
**Target:** Self-hosted Supabase on srv1503081.hstgr.cloud

### Blocks Executed (all succeeded)

| # | Block | Status |
|---|-------|--------|
| 1 | Extensions (postgis, pg_trgm) | ✅ |
| 2 | `states` table | ✅ |
| 3 | `crashes` table (partitioned by LIST on state) | ✅ |
| 4 | Partitions: crashes_delaware, crashes_virginia, crashes_colorado | ✅ |
| 5 | 12 indexes on crashes (jurisdiction, road type, federal, spatial, upsert) | ✅ |
| 6 | `rankings` table + 2 indexes | ✅ |
| 7 | `federal_summary` materialized view + unique index | ✅ |
| 8 | Supporting tables: `hierarchies`, `pipeline_runs` | ✅ |
| 9 | `organizations` + `user_roles` tables; seeded 5 orgs (CrashLens, FHWA, DelDOT, VDOT, CDOT) | ✅ |
| 10 | Seeded Delaware into `states` (abbr=de, fips=10, status=pending) | ✅ |

### Verification Queries (1–6 from verify_supabase.sql)

**Q1 — Table existence:**
crashes, crashes_colorado, crashes_delaware, crashes_virginia, hierarchies, organizations, pipeline_runs, rankings, spatial_ref_sys, states, user_roles — **PASS** (all expected tables present; spatial_ref_sys from postgis)

**Q2 — Partition check:**
crashes_delaware, crashes_virginia, crashes_colorado — **PASS**

**Q3 — Column count on crashes:**
118 columns (111 tier-1 + state + id + 3 JSONB + created_at + updated_at) — **PASS** (comment in SQL said 117; actual is 118 due to counting all columns including `state`)

**Q4 — Index check:**
12 parent indexes + partition-propagated copies (13 per partition × 3 partitions = 39) + 3 pkeys = 52 total — **PASS**

**Q5 — States seeded:**
`de | delaware | 10 | Delaware | pending` — **PASS**

**Q6 — Organizations seeded:**
CDOT (state_dot), CrashLens (platform), DelDOT (state_dot), FHWA (federal), VDOT (state_dot) — **PASS**

### Next Step
Run `python supabase_sync.py --state de --from-r2` to load Delaware crash data, then run post-sync verification queries 7–24.


## [2026-04-06] milestone | Supabase schema deployed
Migration v3.0 executed by Claude Code against self-hosted Supabase on srv1503081.hstgr.cloud. All 51 state partitions created. Schema validated in Supabase Studio. Wiki documentation complete in [[supabase-schema-v3]]. Next: run `supabase_sync.py --state de --from-r2` to load Delaware crash data.


## [2026-04-06] audit | Supabase security & performance review
Claude Desktop ran security/performance advisors against self-hosted Supabase. Findings:
- **Data loaded**: crashes_delaware has 60K rows (non_dot_roads subset). Full 566K statewide sync pending.
- **Security**: Extensions (postgis, pg_trgm) in public schema — should move to `extensions` schema.
- **Security**: RLS disabled on all 59 tables — required before exposing API to customers (Phase 4).
- **Performance**: Missing index on `user_roles.org_id` FK.
- **Performance**: ~500 unused indexes on empty state partitions — expected, will be used as states load.
- **Action items**: Add `user_roles.org_id` index. Move extensions in Phase 4. RLS policies in Phase 4.


## [2026-04-06] create | supabase-sync-ci
Created GitHub Actions workflow `supabase-sync.yml` for automated monthly R2 → Supabase sync. SSH tunnel approach. Requires 2 new repo secrets: SUPABASE_DB_PASSWORD, SUPABASE_SSH_KEY. Created [[supabase-sync-ci]] wiki page.

## 2026-04-07 — All-State Partitions + Delaware Data Sync

### Partitions Expanded to 50 States + DC

Added 48 partitions to the existing 3 (delaware, virginia, colorado) — now all 50 states + DC have dedicated `crashes_{state_name}` partitions. Confirmed via `pg_inherits`: **51 partitions total**.

### Delaware Data Sync — 566,762 Rows Loaded

**Method:** REST API sync via PostgREST (direct psycopg2 blocked by Supavisor tenant config).
Created `rest_sync.py` — reads parquet, applies same 3-tier column classification as `supabase_sync.py`, inserts via Supabase REST API in batches of 2,000.

**Input:** `all_roads.parquet.gz` (167MB, 566,762 rows x 517 cols)

**Column Classification:**
- Tier 1 (explicit): 111 columns
- Tier 2 (road_data JSONB): 312 keys
- Tier 3 (state_extras JSONB): 18 keys (de_*)
- Ranking (ranking_data JSONB): 76 keys

**Post-Load Updates:**
- `states` table: de → active, total_crashes=566762, year_range=[2009,2026)
- `pipeline_runs`: logged as rest_sync / success / 566762 rows
- `federal_summary` materialized view: refreshed (1,726 summary rows)

### Post-Sync Verification Queries (7–24)

| # | Query | Result | Status |
|---|-------|--------|--------|
| 7 | Row count | 566,762 | PASS |
| 8 | Severity distribution | O=476,563 / A=88,425 / K=1,774 | PASS (matches expected) |
| 9 | Year range | 2009–2025 | PASS (note: data extends to 2025, not 2023 as originally expected) |
| 10 | Year distribution | 17 years, ~31K–37K/yr avg | PASS |
| 11 | States table updated | de / active / 566,762 / [2009,2026) | PASS |
| 12 | OBJECTID format | de-0552477, de-0552478, etc. | PASS |
| 13 | Coordinates | lon [-75.79, -75.05] lat [8.78, 39.84] | WARN — min_lat=8.78 is outlier (expected ~38.4) |
| 14 | DOT Districts | North=335,725 / South=133,428 / Central=97,609 | PASS |
| 15 | road_data keys | hpms_iri, hpms_aadt, etc. present | PASS (note: some keys mangled by itertuples as _NNN) |
| 16 | road_data key count | Populated across rows | PASS |
| 17 | state_extras | de_Day_Of_Week_Code, de_Lighting_Condition_Code, etc. | PASS |
| 18 | ranking_data | MPO_Rank_trend_ksi, Juris_Rank_total_epdo, etc. | PASS |
| 19 | HPMS JSONB query | hpms_iri=167.0, hpms_aadt_combination=118, etc. | PASS |
| 20 | Federal summary rows | 1,726 | PASS |
| 21 | Federal summary sample | Crash counts per year/severity with alcohol + wz flags | PASS |
| 22 | Federal summary total | 566,762 | PASS |
| 23 | Pipeline runs | delaware / rest_sync / success / 566,762 | PASS |
| 24 | Boolean flags | alcohol=21,733 / speed=6,163 / distracted=129,289 / ped=7,490 / bike=3,257 / wz=3,907 | PASS (exact match) |

### Known Issues
1. **Q13 min_lat outlier:** A few records have lat ~8.78 (likely geocoding errors in source data). Recommend filtering y < 30 as invalid for Delaware.
2. **road_data key mangling:** `itertuples()` renames some numeric-prefixed columns to `_NNN`. Cosmetic issue in JSONB catch-all only — Tier 1 columns unaffected. Fix: use `to_dict(orient='records')` in future sync runs.
3. **Supavisor connection:** Direct psycopg2 connection blocked by Supavisor "Tenant or user not found" — REST API workaround via `rest_sync.py` works but is slower (~65 rows/s vs COPY bulk).

### Next Steps
- Run `rest_sync.py` for Virginia and Colorado when data is available in R2
- Investigate Supavisor tenant config to enable direct psycopg2 COPY for faster syncs
- Clean up lat outliers in crashes_delaware (y < 30)


## [2026-04-07] decision | Supabase sync insertion point
Architecture decision: Add Supabase sync as final step in each state's EXISTING workflow, not as a separate workflow. The parquet file is already on disk after split.py — no R2 re-download needed. Same runner adds SSH tunnel + `python supabase_sync.py --state {abbr} --input output/_state/all_roads.parquet.gz`. Standalone `supabase-sync.yml` kept for manual re-syncs. Updated [[data-pipeline-architecture]] and [[supabase-sync-ci]]. Diagrams created: current pipeline (R2 terminal) vs proposed (Stage 5 added inline).


## [2026-04-07] ingest | Frontend deep-dive (Claude Code + Claude Chat)
Claude Code read the entire Douglas_County_2 frontend codebase (12MB index.html + modules). Created 4 wiki pages:
- **[[frontend-ui-structure]]**: 13 tabs, 61 Chart.js canvases, 27 data tables, 40+ dropdowns, 100+ buttons, 6 export formats (CSV/PDF/DOCX/PPTX/KML/PNG)
- **[[data-loader]]**: COL constants (56 column name mappings), R2 URL patterns, data aggregation structure, filter system, current vs future data flow
- **[[frontend-supabase-migration]]**: Module-by-module migration plan with exact SQL queries for Dashboard (8 charts), Hotspots (EPDO), Safety Focus (21 categories), CMF matching, Before/After (PostGIS), Grant Ranking, Trends. Performance comparison table. 5-phase migration sequence.
- **[[frontend-data-inventory]]**: Complete column mapping — 56 of 111 Tier 1 columns actively used by frontend. COL constant → CSV header → Postgres column → which modules use it → filter/display/aggregate usage.

Key findings:
- Frontend uses 56 of 111 Tier 1 columns (the rest are available for API/AI queries)
- Deep Dive tab uses state-specific columns from `state_extras` JSONB
- Only 3 road_data JSONB keys accessed directly by UI (hpms_aadt, hpms_iri, hpms_design_speed)
- Boolean flag convention: `isYes(val)` matches Yes/Y/1/true — Supabase WHERE uses `= 'Yes'`
- R2 base URL: `https://data.aicreatesai.com`
- All aggregation currently done client-side in `buildAggregates()` — moves to PostgreSQL GROUP BY

Updated [[wiki/index.md]] with all new pages. Claude Chat completed index update and cross-reference after Claude Code timed out on Supabase MCP query.

## [2026-04-07] update | Pipeline v2.8 — Supabase sync inline
Updated `delaware-batch-pipeline.yml` to v2.8. Added Stage 4.5 (Supabase Sync) between R2 upload and forecast generation. Uses SSH tunnel from GitHub Actions runner → VPS PostgreSQL. No R2 re-download — uses the statewide parquet already on disk. Added `skip_supabase` input toggle.


## [2026-04-07] audit | Full wiki audit and gap fix (Claude Chat)
Comprehensive audit of all wiki pages. Issues found and fixed:

| Page | Issue | Fix |
|------|-------|-----|
| `index.md` | Garbled from bad patch | Deleted and recreated clean |
| `delaware-pipeline.md` | Year range wrong, missing Supabase results | Appended verified sync results (24/24 pass) |
| `data-pipeline-architecture.md` | Missing Stage 4.5 Supabase | Appended Stage 4.5 section |
| `cloudflare-r2.md` | Missing paths, URLs, sizes | Appended full R2 inventory from screenshots |
| `douglas-county-frontend.md` | Outdated tab count, missing links | Appended deep-dive results + links to 4 new pages |
| `crash-lens-workflow.md` | Missing supabase scripts | Appended supabase_sync.py, rest_sync.py, migration SQL |
| `github-actions-ci.md` | Missing supabase workflows | Appended supabase-sync.yml + Stage 4.5 inline step |

**Wiki stats:** 24 pages total (8 entities, 15 concepts, 4 analyses, 2 sources). All cross-linked. Zero orphans.


## [2026-04-07] audit | Deep audit complete (Claude Chat)
Read all 36 wiki pages. Verified:
- **12 Claude Code pages** (dashboard-tab, safety-focus, crash-profile, epdo-scoring, grant-ranking, baselines-analysis, trends-analysis, cmf-countermeasures, state-adapter, upload-pipeline, mcp-server-tools, before-after-study) — all high quality with JS function names, column mappings, SQL queries, and backlinks
- **before-after-study.md vs before-after-studies.md** — NOT duplicates. Studies is high-level concept, Study is detailed implementation with 5 charts, 30+ CSV export columns, PostGIS SQL
- **mcp-server-tools.md** — 22 MCP tools + 6 resources documented (crash, analysis, safety, CMF, infrastructure)
- **state-adapter.md** — Colorado→Virginia normalization, 40+ collision type mappings, FIPSDatabase
- **All pages cross-linked** — zero orphans, every page has Related Pages section
- **Supabase schema fully covers frontend** — 56 of 111 Tier 1 columns used, zero gaps
- **Wiki now production-ready** — comprehensive knowledge base for any AI tool to understand CrashLens


## [2026-04-08] pipeline-test | Delaware Batch Pipeline v2.8 first run
First CI run of v2.8 pipeline with Stage 4.5 Supabase Sync.

**Results:**
- Stages 0-4: ✅ All passed (download 566,762 rows, enrich 98.6%, split 368 files, R2 upload 368/368)
- Stage 4.5: ❌ FAILED — `psycopg2.OperationalError: Tenant or user not found` (Supavisor blocks direct psycopg2)
- Enrichment: 777.9s, 558,771/566,762 matched (99%), median 3ft distance
- Split: 149.1s, 81 counties + 3 regions + 4 MPOs + 3 PDs = 368 parquet files

**Three issues identified:**
1. **Supavisor tenant error**: SSH tunnel port 5432 → Supavisor, not raw Postgres. Fix: tunnel to port 5433 (direct db container)
2. **R2 format duplicates**: Pipeline now produces `.parquet` (Snappy) but old `.parquet.gz` still in R2. Need cleanup step to remove `.parquet.gz` files.
3. **Intersection Name 0%**: build_road_inventory.py doesn't derive intersection names. Fix: add `intersection_name` column by mapping road names at intersection nodes (u_node/v_node → road name lookup). This propagates to crashes via spatial join.

**Enrichment Fill Report (from CI log):**
- Intersection Name: 0% ❌ (needs build_road_inventory fix)
- Node: 0% ❌ (needs intersection cache in pipeline)
- RTE Name: 53.5%
- Functional Class: 98.6%
- AADT: 98.6%
- Traffic Control Type: 32.9%
- Intersection Analysis: 100%


## [2026-04-08] fixes | Three pipeline fixes queued for Claude Code

### Fix 1: Supavisor Port (delaware-batch-pipeline.yml)
**Problem:** SSH tunnel forwards to port 5432 on VPS which is Supavisor (connection pooler). psycopg2 gets `FATAL: Tenant or user not found`.
**Root cause:** VPS port mapping — 5432=Supavisor, 6543=direct PostgreSQL.
**Fix:** Change tunnel line from `5432:localhost:5432` to `5432:localhost:6543`. Also update `supabase-sync.yml`.
**Files:** `.github/workflows/delaware-batch-pipeline.yml`, `.github/workflows/supabase-sync.yml`

### Fix 2: R2 Cleanup (delaware-batch-pipeline.yml)
**Problem:** Pipeline now produces `.parquet` (Snappy compressed) but old `.parquet.gz` files still in R2 causing duplicates (both visible in R2 console).
**Fix:** Add `.parquet.gz` removal to "Pre-upload: Clean stale R2 split data" step.
**Files:** `.github/workflows/delaware-batch-pipeline.yml`

### Fix 3: Intersection Name (build_road_inventory.py)
**Problem:** `Intersection Name` is 0% filled in crash data. Road inventory doesn't derive intersection names from OSM node-road mappings.
**Root cause:** Roads cache has `name` (94.6% filled) + `u_node`/`v_node`, intersections cache has 39,921 nodes — but `build_road_inventory.py` never combines them to derive "Road1 & Road2" intersection names.
**Fix:** Add ~30 lines after intersection attributes are assigned: build `node_road_names` mapping from roads cache, then set `intersection_name = "Road1 & Road2"` for each intersection segment. This propagates to crashes via `crash_enricher.py` spatial join.
**Files:** `build_road_inventory.py`
**Execution order:** Run `build_road_inventory.py --state de --upload` first, THEN re-run pipeline.

### Execution Order
```
1. Claude Code applies all 3 fixes
2. python build_road_inventory.py --state de --upload    (rebuilds with intersection_name)
3. git push all changes
4. Run "Delaware: Batch Pipeline" (skip_supabase=false, skip_forecasts=true)
5. Verify: intersection_name populated, Supabase sync succeeds on port 6543, R2 has only .parquet (no .parquet.gz)
```

### VPS Port Map (confirmed 2026-04-08)
| Port | Service | Used by |
|------|---------|---------|
| 5432 | Supavisor (pooler) | ❌ Blocks psycopg2 |
| 6543 | Direct PostgreSQL | ✅ Use this for pipeline |


## [2026-04-08] verified | Fix 3 intersection_name — 79.7% filled
Verified new `de_road_inventory.parquet.gz` output (394 cols, up from 393):
- `intersection_name`: 120,533/151,270 segments (79.7%) — exceeds 30-50% estimate
- Samples: "Pineview Road & Westway Drive", "Breasure Road & Dupont Boulevard"
- **Issue found:** Column named `intersection_name` (lowercase) but CrashLens standard is `Intersection Name` (title case). Need one rename in build_road_inventory.py: `roads.rename(columns={'intersection_name': 'Intersection Name'}, inplace=True)` before output assembly.
- After rename + pipeline rerun: crash_enricher spatial join transfers `Intersection Name` → Supabase `intersection_name` column via TIER1_MAP.


## [2026-04-08] audit | Frontend COL ↔ Supabase column cross-reference

**Result: 57/58 match, 1 mismatch found.**

| Status | Count | Details |
|--------|-------|---------|
| ✅ Match | 57 | Frontend COL → CSV header → TIER1_MAP → Postgres column all aligned |
| ⚠️ Mismatch | 1 | `COL.NODE_OFFSET = 'Node Offset'` but pipeline/TIER1_MAP uses `'Node Offset (ft)'` |
| 📋 Supabase-only | 26 | Available for API/MCP but not in frontend COL constants (AADT, lanes, EPDO, etc.) |

**The mismatch:** Frontend reads `Node Offset` (no unit), pipeline outputs `Node Offset (ft)` (with unit). Either the frontend COL constant needs `(ft)` appended, or the pipeline column name needs the suffix removed. Low priority — this column is display-only (not used for filtering or aggregation).

**Key takeaway:** The Supabase schema fully supports the frontend. When migrating from R2 CSV to Supabase PostgREST, the frontend just needs to map COL constants to Postgres column names (all lowercase, underscored). The 26 extra Supabase columns (AADT, Through_Lanes, Intersection Name, EPDO_Score, etc.) become available for new features without schema changes.


## [2026-04-08] critical | 18 missing columns in Supabase crashes table
Full cross-reference of actual Supabase schema (100 columns) vs expected migration SQL (118 columns) revealed 18 missing columns. The migration SQL defined them but they weren't created.

**Missing columns (by category):**
- **JSONB bags (3):** `road_data`, `state_extras`, `ranking_data` — CRITICAL, stores 406 non-Tier-1 columns
- **Resolved values (5):** `resolved_speed_limit`, `resolved_has_lighting`, `resolved_has_signal`, `resolved_on_bridge`, `resolved_school_zone`
- **Intersection/Ramp (4):** `is_intersection`, `intersection_degree`, `is_ramp`, `ramp_type`
- **Road geometry (4):** `curvature`, `length_ft`, `road_lon`, `road_lat`
- **Metadata (2):** `created_at`, `updated_at`

**Fix:** `002_add_missing_columns.sql` — ALTER TABLE ADD COLUMN IF NOT EXISTS for all 18 columns. Run in Supabase SQL Editor before next pipeline sync.

**Frontend columns:** All 57 frontend COL constants → Supabase columns match ✅. One naming mismatch: `Node Offset` (frontend) vs `Node Offset (ft)` (pipeline) — low priority, display-only.


## [2026-04-08] milestone | Port 5433 direct Postgres CONFIRMED
Added `ports: "5433:5432"` to db service in `/root/supabase/docker/docker-compose.yml`. Direct psycopg2 connection verified:
```
docker exec supabase-db psql -U postgres -c "SELECT COUNT(*) FROM crashes_delaware;"
→ 566762 ✅
```

**Final port map (confirmed):**
| VPS Port | Service | psycopg2? |
|----------|---------|-----------|
| 5432 | Supavisor | ❌ Tenant not found |
| 6543 | Supavisor (pool) | ❌ Tenant not found |
| 5433 | Direct Postgres (NEW) | ✅ 566,762 rows confirmed |

**All SSH tunnels:** `ssh -f -N -L 5432:localhost:5433`
Pipeline YAML change: one line, `6543` → `5433`. Push and run.


## [2026-04-08] milestone | Port 5433 direct Postgres CONFIRMED
Added `ports: "5433:5432"` to db service in `/root/supabase/docker/docker-compose.yml`. Direct psycopg2 connection verified:
```
docker exec supabase-db psql -U postgres -c "SELECT COUNT(*) FROM crashes_delaware;"
→ 566762 ✅
```
Port 5433 bypasses Supavisor completely. Pipeline YAML tunnel changing to `5432:localhost:5433`.

**Final VPS port map:**
| VPS Port | Service | psycopg2? |
|----------|---------|-----------|
| 5432 | Supavisor | ❌ Tenant not found |
| 6543 | Supavisor (pool) | ❌ Tenant not found |
| 5433 | Direct Postgres (NEW) | ✅ 566,762 rows confirmed |

**All tunnels use:** `ssh -f -N -L 5432:localhost:5433`


## [2026-04-08] milestone | Port 5433 direct Postgres CONFIRMED
Added `ports: "5433:5432"` to db service in `/root/supabase/docker/docker-compose.yml`. Direct connection verified:
```
docker exec supabase-db psql -U postgres -c "SELECT COUNT(*) FROM crashes_delaware;"
→ 566762 ✅
```

**Final port map (verified):**
| VPS Port | Service | psycopg2? |
|----------|---------|-----------|
| 5432 | Supavisor | ❌ Tenant not found |
| 6543 | Supavisor (pool) | ❌ Tenant not found |
| 5433 | Direct Postgres (NEW) | ✅ 566,762 rows confirmed |

Pipeline YAML tunnel: `ssh -f -N -L 5432:localhost:5433`. One-line fix pushed to Claude Code.


## [2026-04-08] analysis | 6 frontend-driven schema improvements identified

Cross-referenced all frontend wiki pages (safety-focus, crash-profile, grant-ranking, before-after-study, epdo-scoring, baselines-analysis, frontend-supabase-migration) against current Supabase schema. Found 6 gaps that will block frontend migration.

| # | Fix | Priority | Phase | Issue |
|---|-----|----------|-------|-------|
| 1 | `crash_date_parsed DATE` column | CRITICAL | 2 | crash_date is TEXT "M/D/YYYY" — `EXTRACT(DOW FROM crash_date::date)` fails. Dashboard DOW/month/trend charts all broken. |
| 2 | PostGIS `geom` column + GiST index | CRITICAL | 4 | Current `idx_crashes_coords` is B-tree. Before/After `ST_DWithin()` does full table scan without GiST. |
| 3 | Hotspot composite index | HIGH | 2 | `GROUP BY node, rte_name HAVING COUNT(*) >= 5` — no index exists. Most expensive frontend query. |
| 4 | EPDO weights in `states.config_json` | MEDIUM | 3 | 8 state-specific weight sets hardcoded in JS. Need in Postgres for server-side EPDO. |
| 5 | `jurisdiction_baselines` matview | MEDIUM | 3 | Pre-computed baseline rates (14 metrics per jurisdiction). Avoids 500K+ row scan per load. |
| 6 | Intersection name partial index | MEDIUM | 2 | Once populated, `WHERE intersection_name IS NOT NULL GROUP BY intersection_name` needs index. |

**Deliverables created:**
- `003_frontend_schema_improvements.sql` — Run in VPS Supabase SQL Editor (one-time)
- `claude_code_supabase_sync_update.md` — Updates `supabase_sync.py` to populate `crash_date_parsed` + `geom` during every pipeline run

**Self-hosted Supabase confirmed:** All migrations run against `srv1503081.hstgr.cloud` via SSH tunnel to port 5433 (direct Postgres). Cloud Supabase MCP in Claude.ai cannot reach the VPS database.


## [2026-04-08] milestone | Schema v3.1 — all 6 frontend fixes applied ✅

All 6 frontend-driven schema improvements confirmed working on self-hosted Supabase:

| Fix | Result | Status |
|-----|--------|--------|
| crash_date_parsed | 566,759/566,762 parsed (3 null dates) | ✅ |
| geom (PostGIS) | 558,771/566,762 points (7,991 missing coords) | ✅ |
| Hotspot index | idx_crashes_hotspot created | ✅ |
| EPDO weights | DE config_json updated | ✅ |
| jurisdiction_baselines | 81 rows materialized | ✅ |
| Intersection index | idx_crashes_intersection created | ✅ |

**Schema now at v3.1:** 120 columns (118 + crash_date_parsed + geom). All frontend migration queries (DOW/month extraction, PostGIS ST_DWithin, hotspot GROUP BY) will work.

**7,991 crashes without geom:** These have NULL x/y coordinates — likely geocoding failures in source data. Not a schema issue. Worth filtering `WHERE x IS NULL` to investigate.

**Self-hosted Supabase connectivity confirmed:**
- Port 5433: Direct Postgres ✅ (bypasses Supavisor)
- Pipeline (GitHub Actions): SSH tunnel localhost:5432 → VPS:5433 ✅
- Claude Desktop MCP: SSH tunnel localhost:5432 → VPS:5433 ✅
- Claude Chat (claude.ai): Supabase cloud MCP only — cannot reach VPS directly
- Supabase Studio: https://srv1503081.hstgr.cloud (SQL Editor for manual queries)


## [2026-04-08] decision | Auto-wiki and auto-memory rules established
After any pipeline, schema, or architecture change in chat, Claude automatically updates the relevant wiki page + log.md WITHOUT being asked. Before ending long sessions, Claude asks: "Any wiki updates needed?" Important decisions auto-saved to memory.

**Multi-interface setup confirmed:**
- Claude Code: direct filesystem + git push
- Claude Desktop: MCP (Supabase SSH tunnel, Obsidian, filesystem, GitHub)
- Claude Chat: Project Knowledge (14 files) + GitHub MCP
- All three share one repo: `ecomhub200/Crash_Lens_workflow`


## [2026-04-08] milestone | First successful end-to-end pipeline with Supabase sync
Delaware Batch Pipeline completed with Supabase sync. Stage 4.5 ran for 49m47s — the GitHub Actions step timed out client-side but the Postgres transaction committed successfully. All data verified:
- 566,762 rows in crashes_delaware ✅
- crash_date_parsed: 566,759 ✅
- geom: 558,771 ✅
- pipeline_status: active ✅
- federal_summary: 1,726 rows ✅

## [2026-04-08] decision | 6-hour timeout + resume support for supabase_sync.py
Pipeline Stage 4.5 timed out at ~50min because the geom UPDATE (558K rows over SSH tunnel) took too long. Three fixes applied:

1. **Timeout 360min** — process job increased to GitHub Actions maximum (6 hours)
2. **--resume flag** — skips DROP+CREATE, only inserts rows not already present (by objectid). Enables multi-run sync for large states like Virginia (2.1M rows)
3. **Batched geom UPDATE** — 50K rows per batch instead of single 558K-row UPDATE. Progress reporting per batch.

Resume flow for large states:
```
Run 1: Full sync → times out at 1.5M rows
Run 2: --resume → finds 1.5M existing, inserts remaining 600K
Run 3: --resume → 0 new rows → done ✅
```

GitHub Actions input: `resume_supabase: true` triggers resume mode.


## [2026-04-09] fix | 6 data quality fixes from parquet analysis

Root cause analysis of raw parquet files (`de_road_inventory.parquet.gz` + `delaware__state_non_dot_roads.parquet`) revealed why intersection_name, node, and 3 safety flags were empty/wrong.

**Claude Code prompt generated for 3 files:**

| Fix | File | Column | Before → After |
|-----|------|--------|---------------|
| 1 | build_road_inventory.py | Intersection Name | 0% → ~79% (rename lowercase to title case) |
| 2a | de_normalize.py | Unrestrained? | 0 Yes → 7,780 (map "Unbelted" → "Yes") |
| 2b | de_normalize.py | School Zone | 0 Yes → 1,096 (map "1. Yes"/"2. Yes - With School Activity" → "Yes") |
| 2c | de_normalize.py | Weather Condition | 100% bug → ~13% (normalize "1. No Adverse Condition..." → "1. Clear") |
| 3 | crash_enricher.py | Node | 0% → ~98% (derive from osm_u_node) |

**Root causes found:**
- `intersection_name` (lowercase) in road inventory doesn't overwrite empty `Intersection Name` (title case) in crash data — case mismatch
- Delaware Socrata has "Belted"/"Unbelted" not "Yes"/"No" for restraint
- Delaware weather uses "1. No Adverse Condition (Clear/Cloudy)" not "Clear"
- Delaware School Zone uses "1. Yes"/"2. Yes - With School Activity" not plain "Yes"
- Node derivable from `osm_u_node` (100% filled from road inventory spatial join)

**Not bugs (correct DE behavior):** Drowsy, Senior, Young, Hitrun, Lgtruck all "No" — Delaware source doesn't track these. RoadDeparture Type empty — not in source data.


## [2026-04-09] decision | Batched Supabase sync architecture (GitHub matrix strategy)

**Problem:** supabase_sync.py OOM on GitHub Actions (7GB RAM). Building JSONB for 566K × 518 columns = ~6.5GB peak. "Runner received shutdown signal" = Linux OOM killer.

**Solution:** Split Supabase sync into matrix batch jobs, same pattern as `generate-mapillary-cache.yml`. Each batch processes 25K rows in its own job (1.8GB peak).

### Architecture
```
delaware-batch-pipeline.yml
├── prepare job
├── process job (Stages 0-4: enrich, split, upload R2)
├── supabase_sync job (workflow_call → supabase-sync.yml)
│   ├── plan: Count rows from R2 parquet → batch_matrix
│   ├── sync: Matrix [1..N], 25K rows/batch, 360min each
│   │   ├── Batch 1: rows 0-24K (DROP+CREATE partition)
│   │   ├── Batch 2: rows 25K-49K (append)
│   │   └── Batch N: remaining rows
│   └── finalize: geom (50K batches) + matviews + states table
├── forecasts job (Stage 5)
└── commit job (Stage 6)
```

### Key design decisions
- **State-agnostic:** `supabase-sync.yml` takes `state` input, works for all 51 states
- **Dynamic batching:** Plan job counts rows at runtime, calculates batch count. Auto-adjusts batch_size if >256 batches (GitHub limit)
- **Resume-safe:** Each batch checks existing objectids, skips already-inserted rows
- **Batch 1 special:** Only batch 1 (with resume=false) does DROP+CREATE partition
- **max-parallel: 1** — sequential to avoid Postgres contention
- **fail-fast: false** — other batches continue if one fails
- **Finalize job:** Runs after all batches, populates geom (batched 50K), refreshes matviews

### Scaling
| State | Rows | Batches | Peak memory | Est. time |
|-------|------|---------|-------------|-----------|
| DE | 566K | 23 | 1.8 GB | ~2 hours |
| VA | 2.1M | 84 | 1.8 GB | ~7 hours |
| TX | 5M | 200 | 1.8 GB | ~17 hours |
| CA | 8M | 256 | 3.2 GB | ~21 hours |

### Files changed
- `supabase_sync.py` — added `batch_sync()`, `finalize_sync()`, `--batch`, `--batch-size`, `--total-rows`, `--finalize` args
- `.github/workflows/supabase-sync.yml` — NEW workflow (plan → matrix sync → finalize)
- `.github/workflows/delaware-batch-pipeline.yml` — Stage 4.5 replaced with `workflow_call`


## [2026-04-09] bugfix | Table ownership — InsufficientPrivilege on partition creation

**Bug:** `psycopg2.errors.InsufficientPrivilege: must be owner of table crashes`
**Root cause:** The `crashes` parent table and all 51 state partitions were owned by `supabase_admin` (who ran the original migration). Pipeline connects as `postgres` which can't CREATE PARTITION OF on a table it doesn't own.
**Fix:** One-time VPS command transferred ownership of all 53 tables (crashes + 51 partitions + states + pipeline_runs), both materialized views (federal_summary, jurisdiction_baselines), and granted full schema privileges to `postgres`. Verified all show `tableowner = postgres`.


## [2026-04-09] bugfix | JSONB double-quoting + NaN in JSON

**Bug 1:** `invalid input syntax for type json — Expected end of input, but found ""de_Day_Of_Week_Code""`
**Root cause:** `pandas.to_csv()` wraps JSON strings in double-quotes, escaping internal quotes as `""`. PostgreSQL COPY expects raw JSON.
**Fix:** Added `quoting=csv.QUOTE_NONE` to `bulk_insert()` to_csv call.

**Bug 2:** `invalid input syntax for type json — Token "NaN" is invalid`
**Root cause:** Parquet stores real `float('nan')` values. `_row_to_json()` checked `v != "nan"` (string) but `float('nan')` passes that check. `json.dumps()` then outputs the token `NaN` which is invalid JSON.
**Fix:** Rewrote `_row_to_json()` to handle all NaN variants: `math.isnan()` for float NaN, string checks for "nan"/"NaN"/"None"/"NaT"/"inf", and None check. Also updated `batch_sync()` string conversion to replace all NaN variants.


## [2026-04-09] milestone | Batched Supabase sync — FIRST SUCCESSFUL RUN ✅

Delaware batched sync pipeline completed with zero failures.

### Results
| Metric | Value |
|--------|-------|
| Total rows | 569,829 |
| Geom coverage | 569,829 (100%) |
| crash_date_parsed | 569,826 (99.999%) |
| Severity | O=479,166 / A=88,872 / K=1,791 |
| Year range | 2009–2025 |
| Batches | 23 × 25K + 1 × 19,829 |
| Pipeline runs | 71 total, all `success` |
| Finalize duration | ~65 seconds |
| federal_summary | 103 rows |
| Pipeline status | `active`, last sync 2026-04-09 20:27 UTC |

### Bugs fixed to reach this milestone
1. **Matrix ordering** — GitHub doesn't guarantee batch order. Moved DROP+CREATE partition to plan job.
2. **Table ownership** — `crashes` owned by `supabase_admin`, pipeline connects as `postgres`. Transferred ownership of all 53 tables.
3. **JSONB double-quoting** — `pandas.to_csv()` wraps JSON in quotes. Fixed with `quoting=csv.QUOTE_NONE`.
4. **NaN in JSON** — Parquet float NaN leaked into `json.dumps()` output. Rewrote `_row_to_json()` with `math.isnan()` + string checks.

### Architecture confirmed working
```
Pipeline → R2 upload → Plan (count rows, create partition)
  → Matrix batches (23 jobs, 25K rows each, max-parallel:1)
  → Finalize (geom, matviews, states table)
```
State-agnostic. Ready for Virginia, Colorado, and all 50 states.


## [2026-04-09] bugfix | Supabase sync gets un-enriched data (167 cols instead of 517)

**Bug:** All road inventory columns are empty in Supabase (FC, Ownership, AADT, Through_Lanes, etc. all 0%). road_data JSONB is `{}` for all 569,829 rows.
**Root cause:** The batch pipeline enriches crashes locally (Stage 0.5), but never re-uploads the enriched statewide parquet to R2 `_statewide/`. The Supabase sync job downloads the ORIGINAL un-enriched 167-column file from R2, not the enriched 517-column version.
**Fix:** Add "Stage 0.5b" step after enrichment to upload the enriched statewide parquet back to R2 before split and Supabase sync.
**Impact:** After fix, full pipeline must be re-run (not just Supabase sync) to re-enrich and re-upload.


## [2026-04-09] analysis | Full pipeline flow traced — enrichment gap identified

### Two-Workflow Architecture

**Workflow 1: "Batch All Jurisdictions" (`batch-all-jurisdictions.yml`)**
```
Download Socrata → Normalize (de_normalize.py with Phase 8 enrichment)
  → Upload CSV to R2: statewide/delaware_statewide_all_roads.csv
  → Upload CSV to R2: _state/statewide_all_roads.csv.gz
  → Trigger "Delaware: Batch Pipeline"
```
Note: This uploads CSV to `statewide/` and `_state/`, NOT parquet to `_statewide/`.

**Workflow 2: "Delaware: Batch Pipeline" (`delaware-batch-pipeline.yml`)**
```
Download from R2: _statewide/statewide_all_roads.parquet.gz (167 cols)
  → Stage 0.5: Download road inventory from R2 cache
  → Stage 0.5: crash_enricher.py enriches locally (→ 517 cols)
  → Stage 0.5b: ⚠️ MISSING — enriched parquet NOT uploaded back to _statewide/
  → Stages 1-3: Split (uses enriched local file ✅)
  → Stage 4: Upload splits to R2 (enriched ✅)
  → Stage 4.5: Supabase sync downloads from _statewide/ (gets OLD 167 cols ❌)
```

### Root Cause
The `_statewide/statewide_all_roads.parquet.gz` file was created by an earlier pipeline run WITHOUT road inventory enrichment. The batch pipeline enriches it locally but never re-uploads the enriched version. The Supabase sync (separate job) downloads the old un-enriched file from R2.

### Fix
Add "Stage 0.5b" step after enrichment to upload enriched statewide parquet back to R2 `_statewide/`. Then re-run full pipeline.


## [2026-04-09] create | Comprehensive pipeline architecture diagram (Excalidraw)

Created full 6-phase Excalidraw diagram covering every file, step, and data flow in the CrashLens pipeline. Serves as replication guide for new state onboarding.

### Diagram sections:

**Phase 0: Cache Generation (6 generators)**
| Generator | Script | Data Source | R2 Output |
|-----------|--------|-------------|-----------|
| OSM | `generate_osm_data.py` | OpenStreetMap (osmnx) | `de_roads`, `de_intersections`, `de_pois` |
| HPMS | `generate_hpms_data.py` | geo.dot.gov FeatureServer | `de_hpms` (46 cols, 75K segments) |
| State DOT | `generate_state_dot_data.py` | State shapefiles | `de_special_data` (NOT `_state_dot`) |
| Federal | `generate_federal_data.py` | NBI/FRA/NTM/Urban Institute | bridges/rail/schools/transit |
| Boundaries | `generate_boundaries.py` | Census TIGER 2020 | `de_boundaries` (Urban/Suburban/Rural) |
| Mapillary | `mapillary_county_download.py` | Mapillary API v4 | traffic-inventory per county |

**Phase 1: Build Road Inventory**
- `build_road_inventory.py --state de --upload`
- Merges ALL 6 caches into single consolidated file
- 4-tier Data Authority: Tier A (HPMS overwrite) > Tier B (State wins) > Tier C (fill) > Tier 2b (federal POI)
- Intersection Name derivation: u_node/v_node → "Road1 & Road2" (79.7% DE)
- Output: `de_road_inventory.parquet.gz` (151K segments × 394 cols, 22 MB)

**Phase 2: Download + Normalize**
- `batch-all-jurisdictions.yml` → `download_crash_data.py` (registry-driven) → `{abbr}_normalize.py`
- Normalizer: 7 phases (column rename, value transforms, FIPS/GPS, IDs, EPDO, ranking)
- Output: 167 cols (69 Golden + 4 Enrichment + 76 Ranking + 18 State Extras)
- Upload to R2 `_statewide/statewide_all_roads.parquet.gz`

**Phase 3: Batch Pipeline (Process Job)**
- `delaware-batch-pipeline.yml` → downloads statewide from R2
- Stage 0.5: Download road inventory + `crash_enricher.py` 4-tier enrichment (167 → 517 cols)
- Stage 0.5b: Re-upload enriched statewide to R2 (NEW FIX)
- Stages 1-3: `split.py` — 6 tiers × 2 road type sets (DE: 368 files)
- Stage 4: Upload splits to R2 (SNAPPY compression, NOT GZIP)

**Phase 4: Supabase Sync (Batched Matrix)**
- `supabase-sync.yml` (workflow_call from batch pipeline)
- Plan: count rows, DROP+CREATE partition, output batch_matrix
- Sync: 23 × 25K row batches, max-parallel:1, SSH tunnel to port 5433
- Finalize: geom ST_Point, crash_date_parsed, REFRESH matviews, UPDATE states
- 3-tier column strategy: 111 explicit + road_data JSONB (312) + state_extras (18) + ranking_data (76)

**New State Checklist (13 steps)**
1. `states_registry.py` (abbr, name, FIPS)
2. `states/{state}/hierarchy.json`
3-7. Run 5 cache generators
8. `build_road_inventory.py --state {abbr} --upload`
9. Create `{abbr}_normalize.py` from template
10. Add to `download-registry.json`
11-12. Run batch-all-jurisdictions + batch-pipeline
13. Supabase sync auto-runs


## [2026-04-10] milestone | Stage 0.5b fix CONFIRMED — enriched data flowing to Supabase

Batch 1 log confirms the fix works:
- Downloaded: **170M** (was 24M before fix — enriched parquet 7x larger)
- Loaded: **25,000 rows x 517 cols** (was 167 cols before fix)
- Column classification: **111 Tier1 + 312 road_data + 18 state_extras + 76 rankings = 517/517**
- road_data JSONB: **312 keys** (was 0 keys before fix)
- Batch 1: 25,000 rows in 145.9s ✅

Waiting for remaining 22 batches + finalize to complete. Full data quality audit pending.


## [2026-04-10] issue | VPS network unreachable during batched sync

Batch 13/23 failed — VPS `srv1503081.hstgr.cloud` unreachable on port 22 (all 5 SSH attempts failed). Not a pipeline bug — Hostinger VPS network issue. Some batches (1-12) may have completed before outage. Resume with `resume_supabase: true` once VPS is back online.


## [2026-04-10] audit | Full Data Quality Audit v2 + Schema Truth Document

Comprehensive 40-query audit against `crashes_delaware` (569,829 rows). Created Schema Truth Document (PDF) as canonical reference for all column names, types, fill rates, and data quality status.

### Key Findings
- **Road inventory fully populated**: road_data JSONB avg 7,072 chars, 195+ keys per row
- **3 previous fixes confirmed**: Weather (1. Clear ✅), Unrestrained (Yes/No ✅), School Zone (Yes/No ✅)
- **Road inventory match rate**: 561,791/569,829 = 98.6%
- **Geom coverage**: 569,829/569,829 = 100% (improved from 98.6%)

### 9 Data Quality Fixes Designed (claude_code_all_data_quality_fixes.md)

| # | Fix | File(s) | Before | After (est.) |
|---|-----|---------|--------|-------------|
| 1 | MPO name canonicalization | crash_enricher + de_normalize + hierarchy.json + mpo_canonical.json (NEW) | 4 variants | 3 canonical |
| 2 | RTE Name fallback | crash_enricher | 53.6% | ~80-85% |
| 3 | Place FIPS from Census | crash_enricher | 0% | ~65-75% |
| 4 | Persons Injured + Vehicle Count | de_normalize | 0% | derived from severity/collision type |
| 5 | Ped Killed/Injured | de_normalize | 0% | derived from ped flag + severity |
| 6 | Road Departure Type | crash_enricher | 0% | 100% |
| 7 | Surface Type Brick 31% bug | crash_enricher mapping | 31% Brick | ~1% (HPMS code 3 = Composite, not Brick) |
| 8 | Node Offset from ri_match_dist_ft | crash_enricher | 0% | ~98.6% |
| 9 | Relation To Roadway default | crash_enricher | 3.8% | ~100% |

### Not Fixable (source limitation)
first_harmful_event, first_harmful_event_loc, roadway_defect, traffic_control_status, b/c_people — Delaware Socrata doesn't have these fields.

### Architecture: All state-agnostic fixes go into one new method
`_canonicalize_post_enrichment()` in crash_enricher.py, called after `_derive_intersection_analysis()`. Uses lookup tables (us_mpos.json, mpo_canonical.json, us_places.json), not hardcoded state logic.

### Files Applied to raw/Crash_Lens_workflow
All 9 fixes applied via Claude Code. Changes pushed to repo.


## [2026-04-10] critical | Deep accuracy audit — 4 proposed fixes would create WRONG data

Government project accuracy review. Found 11 issues where pipeline produces or would produce misleading data.

### Fixes REMOVED (would create inaccurate data)
| Proposed Fix | Why Removed |
|-------------|-------------|
| Vehicle Count from collision type | "Rear End" could be 2-5 vehicles, not always 2 |
| Ped Killed from ped flag + K severity | Driver could have died, not the pedestrian |
| Node Offset from ri_match_dist_ft | That's LATERAL distance (3-50ft), not LONGITUDINAL distance along road |
| Safety flags default "No" for untracked | "No" when Delaware doesn't track it = lie. Changed to NULL. |

### Fixes ADDED (from accuracy audit)
| New Fix | What |
|---------|------|
| School Zone from LOCATION | Replace wrong "school bus involved" with resolved_school_zone + near_school_1500ft |
| Safety flags NULL for untracked | DE_NOT_TRACKED_FLAGS = {Drowsy, Senior, Young, Hitrun, Lgtruck} → empty not "No" |
| Persons Injured = A+B+C (exact) | Mathematically correct from existing KABCO counts, not estimated |

### Key principle established
**Rule: If we can't determine the correct value → leave NULL. "Unknown" is honest. "No" when we don't know is a lie.**

### Final 9 fixes (revised)
1. ✅ MPO name canonicalization (alias map)
2. ✅ RTE Name fallback (real matched road names)
3. ✅ Place FIPS (Census geometry lookup)
4. 🆕 School Zone from location (replacing wrong school bus source)
5. 🆕 Safety flags NULL for untracked (honest unknown)
6. ✅ Surface Type code 3 mapping bug
7. ⚠️ Road Departure Type (definitive indicators ONLY)
8. ⚠️ Persons Injured = A+B+C (mathematically exact)
9. ⚠️ Relation To Roadway (ramps only, no defaults)


## [2026-04-10] final | 10-fix Claude Code prompt — accuracy-first revision

Final version of data quality fixes. Ped Killed derivation kept (ped+K → minimum 1 ped killed, ~95% accurate per NHTSA). Documented as minimum estimate.

**10 fixes total:**
1. MPO name canonicalization (alias map)
2. RTE Name fallback (road inventory matched names)
3. Place FIPS (Census centroid lookup)
4. School Zone from LOCATION (replace wrong school bus source)
5. Safety flags NULL for untracked (DE_NOT_TRACKED_FLAGS)
6. Surface Type code 3 bug (Composite → Blacktop, not Brick)
7. Road Departure Type (definitive indicators only)
8. Persons Injured = A+B+C (mathematically exact)
9. Ped Killed/Injured (ped flag + severity, minimum estimate)
10. Relation To Roadway (ramps only, no defaults)

**Still removed (would create wrong data):**
- Vehicle Count from collision type (chain reactions = 3-5 vehicles)
- Node Offset from ri_match_dist_ft (lateral ≠ longitudinal)
- Safety flag "No" for untracked fields (changed to NULL)

**File: `claude_code_all_fixes_final.md`**


## [2026-04-08] validation | Supabase health check via Claude Desktop MCP

Ran 9 verification queries against self-hosted Supabase via Claude Desktop (supabase-self-hosted MCP).

### Results Summary

| Metric | Expected | Actual | Status |
|--------|----------|--------|--------|
| Row count | 566,762 | 519,829 | ❌ 47K gap (older rest_sync.py load) |
| Intersection Name | 0% | 96.5% | ✅ Already populated! |
| Node | 0% | 98.6% | ✅ Already populated! |
| RTE Name | 53.5% | 82.1% | ✅ Better than CI log |
| Functional Class | 98.6% | 98.6% | ✅ |
| Ownership | 98.6% | 98.6% | ✅ |
| AADT (nonzero) | — | 85.2% | ✅ |
| Through Lanes | 98.6% | 98.6% | ✅ |
| Traffic Control Type | 32.9% | 32.9% | ⚠️ Low (source limitation) |
| DOT District | 100% | 100% | ✅ |
| MPO Name | 100% | 100% | ✅ |
| Intersection Analysis | 100% | 100% | ✅ |
| Severity O | 476,563 | 436,603 | ❌ Proportional to row gap |
| Severity A | 88,425 | 81,593 | ❌ Proportional |
| Severity K | 1,774 | 1,633 | ❌ Proportional |
| B_People / C_People | 0 / 0 | 0 / 0 | ✅ Delaware limitation |
| Year range | 2009-2025 | 2009-2025 | ✅ |
| road_data JSONB | — | 100% populated | ✅ |
| state_extras JSONB | — | 100% populated | ✅ |
| ranking_data JSONB | — | 100% populated | ✅ |
| Boolean flags | — | All proportional | ✅ |
| Connection port | — | 5432 (direct Postgres) | ✅ |

### Key Findings

1. **Intersection Name is NOT missing** — 96.5% filled in current Supabase data. The 0% we saw earlier was from an older parquet file upload. The road inventory enrichment works correctly.

2. **Row gap (519,829 vs 566,762)** — Not data corruption. The Supabase data was loaded via `rest_sync.py` from an earlier/partial source file. Fix: re-run pipeline with corrected SSH tunnel port to sync full 566,762 rows.

3. **hpms_aadt key doesn't exist in road_data JSONB** — By design. AADT is a Tier 1 explicit column (`aadt TEXT`, 85.2% filled), not in JSONB. The JSONB only has supplemental HPMS fields (27 keys: hpms_future_aadt, hpms_iri, hpms_design_speed, etc.). Confirmed correct per [[supabase-schema-v3]] Tier 1 "Key Analysis (10)" group.

4. **Port 5432 = direct Postgres** — Claude Desktop's tunnel reaches raw PostgreSQL, not Supavisor. Good for performance.

5. **CI pipeline failed on Supavisor** — The GitHub Actions tunnel uses `5432:localhost:5432` which hits Supavisor. Fix: change to `5432:localhost:5433` (direct Postgres port). One-line change in `delaware-batch-pipeline.yml`.

### Remaining Fix

Single line change in `delaware-batch-pipeline.yml` Stage 4.5:
```
ssh -f -N -L 5432:localhost:5432  →  ssh -f -N -L 5432:localhost:5433
```
Then re-run pipeline to sync full 566,762 rows.


## [2026-04-11] milestone | Webhook Supabase sync deployed + architecture diagrams

### Webhook Sync (replaces SSH tunnel batching)
- **Problem:** GitHub Actions SSH tunnel to VPS failed — "Network is unreachable" from Azure IPs
- **Solution:** VPS-hosted Flask webhook at `https://srv1503081.hstgr.cloud/api/sync`
- GitHub Actions sends one POST after R2 upload (fire-and-forget, HTTP 202)
- VPS runs `supabase_sync.py --from-r2` locally (localhost:5433, no tunnel)
- Caddy reverse proxy: `/api/sync*` → `172.18.0.1:8765` (Docker bridge gateway)
- systemd service: `crashlens-webhook.service` (gunicorn, 1 worker)
- New GitHub secret: `SYNC_WEBHOOK_TOKEN`
- `supabase-sync.yml` kept as backup for manual batched runs

### R2 Path Fix
- supabase_sync.py `download_from_r2` paths corrected:
  - Primary: `{state_name}/_state/all_roads.parquet` (no .gz)
  - Fallback: `{state_name}/_statewide/statewide_all_roads.parquet.gz`

### VPS Memory Issue
- Full sync OOM-killed on 8GB VPS (JSONB build for 315 cols × 570K rows)
- Fix: 4GB swap file (`/swapfile`) + batched mode (25K rows/batch)
- webhook.py needs update to use batched mode (pending)

### Architecture Diagrams Added
- `Full_pipeline.png` — CrashLens Pipeline Architecture v2.9 (all phases 0-4)
- `For_any_state.png` — Simplified replicable architecture for any state
- `crashlens_schema_truth_document.pdf` — Canonical schema reference (111 explicit + ~195 JSONB keys)

### Setup Issues Resolved
- `pip` → `pip3` → `--break-system-packages` (Ubuntu 24.04 PEP 668)
- gunicorn bind `127.0.0.1` → `0.0.0.0` (Docker bridge can't reach localhost)
- Caddy gateway `172.17.0.1` → `172.18.0.1` (actual Docker bridge IP)
- `.env` sourcing: `source .env` fails without `export` prefix → use `export $(grep -v '^#' .env | xargs)`

### Wiki Pages Created
- [[pipeline-architecture-v29]] — Full v2.9 pipeline with diagrams and 13-step new state checklist
- [[schema-truth-document]] — Canonical schema reference entity page
- [[webhook-sync]] — Webhook infrastructure entity page



## [2026-04-11] docs | State data dictionary + webhook batched sync confirmed

- Created [[state-data-dictionary-template]] — reusable template for documenting each state's data characteristics, column mappings, value transforms, not-tracked fields, state extras, fill rates, known issues
- Created [[delaware-data-dictionary]] — reference implementation with all 18 state extras, value mappings, fill rates, known issues, Supabase sync details
- Webhook batched sync deployed and verified:
  - webhook.py updated to use batched subprocess mode (23 x 25K rows) instead of full --from-r2 (OOM on 8GB VPS)
  - Each batch runs as separate subprocess — memory freed between batches
  - Lock file in finally block, stale PID detection, pyarrow count with pandas fallback
  - GitHub Actions PR #73 merged: "Implement batched sync with memory-efficient subprocess pooling"
  - End-to-end test: POST /api/sync → HTTP 202 → batches run → finalize (geom + matviews) → success
- VPS setup: 4GB swap file at /swapfile as OOM safety net
- R2 download paths confirmed: primary=_state/all_roads.parquet, fallback=_statewide/statewide_all_roads.parquet.gz
- Wiki sync: Obsidian vault mirrored to Crash_Lens_workflow/wiki/ in GitHub repo for Claude Code web access
- CLAUDE.md updated with wiki-first + auto-wiki rules (Karpathy LLM Wiki pattern)
- Memory rule: Claude updates wiki directly via Obsidian MCP, not via Claude Code prompts

## [2026-04-11] docs | Architecture diagrams updated + state data dictionary expanded

- Expanded Phase 4 in [[pipeline-architecture-v29]] to reflect webhook sync — added Why batched rationale, Infrastructure (Flask + Caddy + systemd + lock file + logs), and Monitoring commands (status check, live logs, manual trigger)
- Expanded [[state-data-dictionary-template]] — added extra rows/sections so the template fully captures: source characteristics, column mappings with transforms, value transforms, not-tracked fields, state extras, fill rates (incl. Through Lanes, Traffic Control Type), known issues, and pipeline Special handling notes
- [[delaware-data-dictionary]] verified as reference implementation (all 18 state extras, value mappings, fill rates, known issues, Supabase sync details already complete)
