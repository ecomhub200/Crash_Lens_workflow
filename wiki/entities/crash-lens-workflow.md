---
title: Crash Lens Workflow Repository
type: entity
tags: [repo, pipeline, python, etl, backend]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Crash Lens Workflow

The **data pipeline repository** — handles downloading, normalizing, enriching, and uploading crash data from 30+ US state DOT sources.

## Purpose

Automates the entire ETL (Extract-Transform-Load) process for traffic crash data:
1. **Download** raw crash records from state DOT APIs
2. **Normalize** to a standardized column schema
3. **Enrich** with road network data (OSM, HPMS, federal sources)
4. **Upload** processed files to [[cloudflare-r2]] for the [[douglas-county-frontend]] to consume

## Key Scripts (33 Python modules)

| Script | Purpose |
|--------|---------|
| `download_crash_data.py` | Virginia Roads ArcGIS API downloader |
| `download_cdot_crash_data.py` | Colorado DOT OnBase downloader |
| `crash_enricher.py` | Universal GPS-based crash enrichment engine |
| `build_road_inventory.py` | Consolidates 8+ data sources into unified road DB |
| `generate_osm_data.py` | OpenStreetMap road network extraction |
| `generate_hpms_data.py` | Federal highway performance data |
| `generate_federal_data.py` | Federal infrastructure data integration |
| `boundary_resolver.py` | Jurisdiction & geographic boundary resolution |
| `geo_resolver.py` | Geographic coordinate resolution |

## Unified Pipeline Architecture (v7)

Seven-stage pipeline orchestrated via [[github-actions-ci]]:

1. **Download** — State-specific API calls
2. **Merge** — Combine multi-file downloads
3. **Convert** — Normalize to standard schema
4. **Init Cache** — Build spatial indexes (DuckDB)
5. **Split** — Partition by jurisdiction
6. **Upload** — Push to [[cloudflare-r2]]
7. **Predict** — Generate crash forecasts (SageMaker Chronos-2)

See [[data-pipeline-architecture]] for details.

## Configuration

- `config.json` (115 KB) — Master state/jurisdiction configuration
- `states/{state}/config.json` — Per-state column mappings, severity derivation
- `states/{state}/hierarchy.json` — Regions, MPOs, counties with FIPS codes

## Tech Stack

Python 3.11, pandas, geopandas, shapely, DuckDB, PyArrow, OSMnx, scipy, Playwright

## Related Pages

- [[data-pipeline-architecture]] — Detailed pipeline design
- [[crash-enrichment]] — How crash records are enriched
- [[state-onboarding]] — Adding new state data sources
- [[github-actions-ci]] — CI/CD workflows
- [[douglas-county-frontend]] — The frontend that consumes this data


## Supabase Integration Scripts (v2.8)

| Script | Purpose |
|--------|---------|
| `supabase_sync.py` | Pipeline Stage 5 — 3-tier column strategy sync to PostgreSQL |
| `rest_sync.py` | REST API fallback when psycopg2 COPY is blocked by Supavisor |
| `001_crashlens_migration_v3.sql` | Schema DDL — partitioned crashes table, indexes, matview |
| `verify_supabase.sql` | 24 post-migration verification queries |

### Pipeline Updated (v2.8)

```
Stage 1: Download from DOT API
Stage 2: Merge multi-file downloads
Stage 3: Normalize + Enrich (4-tier)
Stage 4: Split + Upload to R2 (parquet.gz)
Stage 4.5: Supabase Sync (SSH tunnel → PostgreSQL)  ← NEW
Stage 5: Predict (SageMaker Chronos-2)
Stage 6: Manifest commit
```

See [[supabase-schema-v3]], [[supabase-sync-ci]], [[frontend-supabase-migration]].
