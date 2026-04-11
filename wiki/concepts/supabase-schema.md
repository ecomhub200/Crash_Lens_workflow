---
title: Supabase Schema Setup
type: concept
tags: [supabase, database, schema, migration, phase-1]
created: 2026-04-06
updated: 2026-04-06
sources: [CrashLens_Architecture_Roadmap_v2]
status: active
---

# Supabase Schema Setup

Master reference for the CrashLens Supabase database migration. Single source of truth for schema, sync logic, and execution steps.

## Connection Details

| Property | Value |
|----------|-------|
| Host | `srv1503081.hstgr.cloud` (Hostinger VPS) |
| Type | Self-hosted Supabase |
| Access | SSH tunnel (`C:\Users\murad\.ssh\supabase_tunnel`) |
| MCP | Connected via `mcp-remote` in Claude Desktop |

## Schema: 6 Tables + 1 Materialized View

| Table | Purpose | Phase |
|-------|---------|-------|
| `states` | State registry | 1 |
| `crashes` | Partitioned crash records | 1 |
| `rankings` | Pre-computed jurisdiction rankings | 1 |
| `hierarchies` | Per-state hierarchy.json | 1 |
| `pipeline_runs` | Audit log | 1 |
| `organizations` | DOT agency access | 4 |
| `user_roles` | User-org-role mapping | 4 |
| `federal_summary` | Cross-state aggregation matview | 1 |

## Pipeline Stage 5: supabase_sync.py

**Input:** `{state}_statewide_all_roads.csv` or `.parquet.gz`

**Steps:**
1. Read input → rename columns (COLUMN_MAP) → add state column
2. Log to `pipeline_runs` (status=running)
3. DROP + CREATE partition (clean reload)
4. Bulk INSERT via COPY protocol
5. Update `states` metadata
6. REFRESH MATERIALIZED VIEW federal_summary
7. Log success to `pipeline_runs`

**CLI:**
```bash
python supabase_sync.py --state de --input delaware_statewide_all_roads.csv
python supabase_sync.py --state de --from-r2
python supabase_sync.py --state de --dry-run
```

## Execution Plan

1. Create schema → Claude Chat via Supabase MCP
2. Write supabase_sync.py → Done (in repo)
3. Register Delaware → `INSERT INTO states`
4. Sync Delaware → `python supabase_sync.py --state de --from-r2`
5. Verify → `SELECT COUNT(*) FROM crashes_delaware`

## Related Pages

- [[delaware-pipeline]]
- [[data-pipeline-architecture]]
- [[crash-enrichment]]
- [[build-road-inventory]]
- [[technology-stack]]
