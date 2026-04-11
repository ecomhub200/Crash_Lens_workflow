---
title: Upload Pipeline
type: concept
tags: [frontend, upload, pipeline, r2, data-flow]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Upload Pipeline

## Purpose
5-stage pipeline for ingesting crash data files (CSV, CSV.GZ, Parquet.GZ), normalizing columns, validating GPS coordinates, loading into the app, and optionally saving to R2.

## 5 Stages

### Stage 1: Detect & Convert
- Detect file format: .csv, .csv.gz, .parquet.gz
- Decompress gzip via `pako`
- Parse parquet via `hyparquet`
- `StateAdapter.detect()` identifies state format from CSV headers

### Stage 2: Validate & Normalize
- `StateAdapter.normalizeRow()` per row
- PapaParse with 5MB chunks
- Validate required columns present

### Stage 3: GPS Check
- Validate lat/lon (COL.Y and COL.X)
- Calculate % geocoded records
- Track missing GPS count

### Stage 4: Load
- Process rows into app memory
- Build aggregates (severity, collision, weather, light, route, node, etc.)
- Generate mapPoints array

### Stage 5: Save to R2
- Convert to CSV via `Papa.unparse()`
- POST to `/api/r2/upload-geocoded` with `{r2Key, csvData}`
- R2 key constructed by `buildR2DestinationPath()`

## R2 Destination Path Construction
```javascript
buildR2DestinationPath() {
  // tier = 'federal' | 'state' | 'region' | 'mpo' | 'planning_district' | 'county' | 'city'
  // Returns: {statePrefix}/{tierPrefix}/{entityId}/all_roads.csv.gz
}
```

## UI Controls
- `pipelineStateSelect` — State dropdown
- `pipelineDestTier` — Tier (federal/state/region/mpo/county/city)
- `pipelineJurisdictionSelect` — Jurisdiction (populated dynamically)
- Road type selection
- File input (drag & drop supported)

## Related Pages
- [[data-loader]] — Data loading architecture
- [[state-adapter]] — State detection/normalization
- [[frontend-supabase-migration]] — Migration plan
