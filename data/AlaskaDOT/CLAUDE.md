# AlaskaDOT (Alaska Department of Transportation and Public Facilities) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | Alaska |
| **Abbreviation** | AK |
| **FIPS** | 02 |
| **DOT Name** | AKDOT&PF |
| **Full Name** | Alaska Department of Transportation and Public Facilities |
| **Counties** | 30 |

## Data Source

- **System:** ArcGIS GeoServices API at `data-soa-akdot.opendata.arcgis.com`
- **Portal:** `data-soa-akdot.opendata.arcgis.com`
- **API URL:** `https://data-soa-akdot.opendata.arcgis.com/datasets/crash-data/FeatureServer/0`
- **Format:** GeoJSON/CSV (ArcGIS Feature Server)

## API Access

### ArcGIS Feature Server
- Pagination: `resultOffset`/`resultRecordCount`
- Filtering: SQL-like `where` clause
- Response format: `f=json` or `f=geojson`

## Column Mapping

See `config.json` for the complete column mapping from Alaska raw fields to CRASH LENS standardized format (VDOT reference).

**Key fields to map:** ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

## Severity Mapping

Check `config.json` → `derivedFields.SEVERITY` for the Alaska-specific severity derivation method.

## Jurisdiction Filtering

- 30 jurisdictions defined in `source_manifest.json`
- All with FIPS codes for programmatic filtering
- State FIPS: 02

## R2 Storage

All processed CSVs must be gzip-compressed before uploading to R2:

```
crash-lens-data/alaska/{jurisdiction}/
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
| `PIPELINE_ARCHITECTURE.md` | Alaska-specific pipeline reference guide |
| `download_alaska_crash_data.py` | Production download script |

## EPDO Weights

Using FHWA 2025 (FHWA-SA-25-021): K=883, A=94, B=21, C=11, O=1

## GitHub Actions Workflow

- **File:** `.github/workflows/download-alaska-crash-data.yml`
- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select county from jurisdiction dropdown
- **R2 upload:** Gzip-compressed CSVs to `alaska/{jurisdiction}/`
