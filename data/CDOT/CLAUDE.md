# CDOT (Colorado Department of Transportation) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | Colorado |
| **Abbreviation** | CO |
| **FIPS** | 08 |
| **DOT Name** | CDOT |
| **Full Name** | Colorado Department of Transportation |
| **Counties** | 64 |

## Data Source

- **System:** CDOT Hyland OnBase document management portal
- **URL:** `https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx`
- **Format:** MS Excel Spreadsheet (.xlsx)
- **Refresh:** Annual (CDOT publishes each year's data as a separate Excel file)
- **Coverage:** 2021–2025 (document IDs in `source_manifest.json`)

## API Access

CDOT does **not** use a standard REST API. Data is downloaded from the Hyland OnBase document portal using document IDs. The download script (`download_cdot_crash_data.py` at project root) handles:
- OnBase document retrieval via direct URL endpoints
- Browser-like headers to avoid blocking
- Playwright/Chromium headless browsing as fallback
- Excel → CSV conversion
- County filtering from statewide data

## Column Mapping Summary

See `config.json` for the complete mapping. Key highlights:

| CRASH LENS Field | CDOT Raw Column | Notes |
|-----------------|----------------|-------|
| ID | `CUID` | Unique crash identifier |
| DATE | `Crash Date` | Date of crash |
| LAT/LON | `Latitude` / `Longitude` | GPS coordinates |
| SEVERITY | **DERIVED** | Must compute from Injury 00–04 counts |
| ROUTE | **DERIVED** | Built from System Code + Rd_Number + Location 1 |
| COLLISION | `MHE` | Most Harmful Event — maps to standard collision types |
| WEATHER | `Weather` | Weather conditions |
| LIGHT | `Daylight` | Lighting conditions |
| JURISDICTION | `County` | County name (uppercase) |

## Severity Mapping — CRITICAL

Colorado does **NOT** have a single severity column. Severity must be **derived** from injury count columns:

```
If Injury 04 > 0 → K (Fatal)
Elif Injury 03 > 0 → A (Suspected Serious Injury)
Elif Injury 02 > 0 → B (Suspected Minor Injury)
Elif Injury 01 > 0 → C (Possible Injury)
Else → O (Property Damage Only)
```

## VDOT Value Mapping — Key Differences

Colorado raw values must be mapped to VDOT vocabulary. Common pitfalls:
- `Clear` → `1. No Adverse Condition (Clear/Cloudy)`
- `Rear End` → `1. Rear End`  (must add numbered prefix)
- `Non-Intersection` in Road Description → `1. Two-Way, Not Divided` (different semantic meaning)
- `Curve Right, Downhill` → `4. Grade - Curve` (different granularity)

See `config.json` → `crashTypeMapping` for the full mapping table.

## Jurisdiction Filtering

- Filter statewide data using the `County` column (uppercase values)
- FIPS codes for all 64 counties listed in `source_manifest.json`
- Currently 6 counties fully configured in `jurisdictions.json` (Douglas, Arapahoe, Jefferson, El Paso, Denver, Adams)

## R2 Storage

All processed CSVs must be gzip-compressed before uploading to R2:

```
crash-lens-data/colorado/{jurisdiction}/
  all_roads.csv.gz
  county_roads.csv.gz
  no_interstate.csv.gz
  standardized.csv.gz
  raw/{year}.csv.gz
  forecasts_all_roads.json
  forecasts_county_roads.json
  forecasts_no_interstate.json
```

## Pipeline Stages

1. **MERGE** — Concatenate multiple year Excel files into one CSV
2. **CONVERT** — Normalize columns, derive severity, map values to VDOT format
3. **VALIDATE** — QA/QC with auto-correction
4. **GEOCODE** — Fill missing GPS using cache, node lookup, Nominatim
5. **SPLIT** — Filter by road type (all_roads, county_roads, no_interstate)
6. **PREDICT** — SageMaker Chronos-2 forecasting

## Key Files in This Folder

| File | Purpose |
|------|---------|
| `config.json` | Column mappings, road systems, derived fields, EPDO weights |
| `source_manifest.json` | CDOT OnBase document IDs, all 64 counties with FIPS |
| `jurisdictions.json` | County definitions (6 configured, expandable to 64) |
| `enhancements.json` | Deep Dive panel configuration for CO-specific columns |
| `PIPELINE_ARCHITECTURE.md` | Complete 5-stage pipeline reference guide |
| `r2-integration-plan.md` | Cloudflare R2 storage integration plan |

## EPDO Weights

Using FHWA 2025 (FHWA-SA-25-021): K=883, A=94, B=21, C=11, O=1

## Derived Boolean Flags

13 flags derived from raw CDOT columns:
PED, BIKE, MOTORCYCLE, ALCOHOL, SPEED, HITRUN, DISTRACTED, DROWSY, DRUG, YOUNG, SENIOR, UNRESTRAINED, NIGHT

## Road Systems

| Colorado Value | Classification |
|---------------|---------------|
| City Street | Local |
| County Road | Local |
| State Highway | CDOT-maintained |
| Interstate Highway | CDOT-maintained |
| Frontage Road | CDOT-maintained |

## GitHub Actions Workflow

- **File:** `.github/workflows/download-cdot-crash-data.yml`
- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select county, years, force-download options
- **R2 upload:** Raw annual CSVs uploaded to `colorado/{jurisdiction}/raw/{year}.csv`
