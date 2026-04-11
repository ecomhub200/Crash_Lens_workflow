---
title: Cloudflare R2
type: entity
tags: [storage, cloudflare, infrastructure, r2]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo, source-frontend-repo]
---

# Cloudflare R2

**Object storage layer** that connects the [[crash-lens-workflow|data pipeline]] to the [[douglas-county-frontend|frontend application]].

## Role in Crash Lens

R2 is the central data exchange point:
- The **workflow repo** uploads processed crash CSVs, Parquet files, and forecast JSONs
- The **frontend repo** downloads and loads this data for analysis in the browser
- **User uploads** through the frontend are also stored in R2

## Data Flow

```
Pipeline (Python) → R2 Buckets → Frontend (Browser)
                         ↑
              User uploads via frontend
```

## Storage Organization

Data is organized by state and jurisdiction:
- `{state}/` — State-level data
- `{state}/{jurisdiction}/` — County/city-level crash files
- Formats: CSV, Parquet, JSON (forecasts), manifest files

## Integration Points

- **Pipeline**: `@aws-sdk/client-s3` compatible API (S3-compatible)
- **Frontend**: Direct browser fetches via R2 public URLs
- **GitHub Actions**: Upload action in `.github/actions/upload-r2/`

## Related Pages

- [[crash-lens-workflow]] — Produces the data
- [[douglas-county-frontend]] — Consumes the data
- [[data-pipeline-architecture]] — Pipeline stages that write to R2


## R2 Storage Details (Verified 2026-04-07)

| Property | Value |
|----------|-------|
| Bucket name | `crash-lens-data` |
| Total size | 5.36 GB |
| Public access | Enabled |
| Base URL | `https://data.aicreatesai.com` |

### Delaware R2 File Inventory (`delaware/_state/`)

| File | Size | Modified |
|------|------|----------|
| `all_roads.parquet.gz` | 174.47 MB | 2026-04-05 |
| `dot_roads.parquet.gz` | 140.21 MB | 2026-04-05 |
| `non_dot_roads.parquet.gz` | 23.96 MB | 2026-04-05 |
| `primary_roads.parquet.gz` | 8.78 MB | 2026-04-05 |

### R2 Path Structure (per state)

```
{state}/
├── _statewide/
│   └── statewide_all_roads.parquet.gz    ← normalized (pre-split)
├── _state/
│   ├── all_roads.parquet.gz              ← statewide split (SET A)
│   ├── dot_roads.parquet.gz
│   ├── primary_roads.parquet.gz
│   └── non_dot_roads.parquet.gz
├── _region/{id}/                          ← SET A per region
├── _mpo/{id}/                             ← SET B per MPO
├── _planning_district/{id}/               ← SET B per PD
├── _city/{slug}/                          ← SET B per city
├── {county_key}/                          ← SET B per county
└── cache/
    ├── {abbr}_roads.parquet.gz
    ├── {abbr}_intersections.parquet.gz
    ├── {abbr}_hpms.parquet.gz
    ├── {abbr}_pois.parquet.gz
    └── {abbr}_road_inventory.parquet.gz
```

### GitHub Secrets (CF_ prefix convention)

| Secret | Purpose |
|--------|---------|
| `CF_ACCOUNT_ID` | Cloudflare account ID |
| `CF_R2_ACCESS_KEY_ID` | R2 access key |
| `CF_R2_SECRET_ACCESS_KEY` | R2 secret key |

### Supabase Relationship

R2 remains the primary data store and CDN. Supabase PostgreSQL is the query engine. The pipeline uploads to R2 first (Stage 4), then syncs to Supabase (Stage 4.5). `supabase_sync.py --from-r2` can also pull from R2 directly. See [[frontend-supabase-migration]] for the transition plan.


## R2 Format Transition: .parquet.gz → .parquet (2026-04-08)

Pipeline split.py now outputs `.parquet` (Snappy compressed internally) instead of `.parquet.gz` (external gzip). Both formats existed temporarily in R2 causing duplicates.

**Fix:** Added `.parquet.gz` cleanup to "Pre-upload: Clean stale R2 split data" step in `delaware-batch-pipeline.yml`. The upload step also removes legacy `.parquet.gz` keys after uploading each `.parquet` file.

**Frontend impact:** The frontend data-loader must handle both `.parquet` and `.parquet.gz` extensions during transition. The `hyparquet` browser library requires Snappy compression (not GZIP) — the `.parquet` format is correct for browser parsing.
