# NYCDOT (New York City Department of Transportation) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | New York City |
| **Abbreviation** | NYC |
| **FIPS** | 36 |
| **DOT Name** | NYC DOT |
| **Full Name** | New York City Department of Transportation |
| **Counties** | 5 |

## Data Source

- **System:** Socrata SODA API at `data.cityofnewyork.us`
- **Portal:** `data.cityofnewyork.us`
- **API URL:** `https://data.cityofnewyork.us/resource/h9gi-nx95.json`
- **Format:** JSON/CSV (Socrata SODA API)

## API Access

### Socrata SODA API
- Pagination: `$limit`/`$offset` (max 50,000 per request)
- Filtering: SoQL `$where` clause
- No authentication required for public datasets

## Column Mapping

See `config.json` for the complete column mapping from New York City raw fields to CRASH LENS standardized format (VDOT reference).

**Key fields to map:** ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

## Severity Mapping

Check `config.json` → `derivedFields.SEVERITY` for the New York City-specific severity derivation method.

## Jurisdiction Filtering

- 5 jurisdictions defined in `source_manifest.json`
- All with FIPS codes for programmatic filtering
- State FIPS: 36

## R2 Storage

All processed CSVs must be gzip-compressed before uploading to R2:

```
crash-lens-data/nyc/{jurisdiction}/
  all_roads.csv.gz
  county_roads.csv.gz
  no_interstate.csv.gz
  forecasts_all_roads.json
  forecasts_county_roads.json
  forecasts_no_interstate.json
```

## Pipeline Stages

1. **CONVERT** — Normalize to VDOT format, map severity and values
2. **VALIDATE** — QA/QC with auto-correction
3. **GEOCODE** — Fill missing GPS coordinates
4. **SPLIT** — Filter by road type (all_roads, county_roads, no_interstate)
5. **PREDICT** — SageMaker Chronos-2 forecasting

## Key Files in This Folder

| File | Purpose |
|------|---------|
| `config.json` | Column mappings, road systems, severity mapping, EPDO weights |
| `source_manifest.json` | API endpoints, all jurisdictions with FIPS codes |
| `PIPELINE_ARCHITECTURE.md` | New York City-specific pipeline reference guide |
| `download_nyc_crash_data.py` | Production download script |

## EPDO Weights

Using FHWA 2025 (FHWA-SA-25-021): K=883, A=94, B=21, C=11, O=1

## GitHub Actions Workflow

- **File:** `.github/workflows/download-nyc-crash-data.yml`
- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select county from jurisdiction dropdown
- **R2 upload:** Gzip-compressed CSVs to `nyc/{jurisdiction}/`
