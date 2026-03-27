# MarylandDOT (Maryland Department of Transportation) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | Maryland |
| **Abbreviation** | MD |
| **FIPS** | 24 |
| **DOT Name** | MDOT SHA |
| **Full Name** | Maryland Department of Transportation State Highway Administration |
| **Counties** | 23 counties + Baltimore City = 24 jurisdictions |

## Data Source

- **System:** Maryland Open Data Portal (Socrata)
- **Portal:** `https://opendata.maryland.gov`
- **Dataset:** Maryland Statewide Vehicle Crashes
- **Resource ID:** `65du-s3qu`
- **API Endpoint:** `https://opendata.maryland.gov/resource/65du-s3qu.json`
- **Format:** JSON/CSV via Socrata SODA API
- **Refresh:** Periodic (approved crash reports only)
- **Coverage:** January 2015 through present
- **Data Dictionary:** ACRS Field Reference Guide + Zero Deaths MD definitions

## API Access — Socrata SODA

- **Base URL:** `https://opendata.maryland.gov/resource/65du-s3qu.json`
- **Pagination:** `$limit` (max 50,000 per request) + `$offset`
- **Filtering:** SoQL `$where` clause (e.g., `$where=county_desc='Montgomery'`)
- **Metadata:** `https://opendata.maryland.gov/api/views/65du-s3qu.json`
- **CSV Bulk:** `https://opendata.maryland.gov/api/views/65du-s3qu/rows.csv?accessType=DOWNLOAD`
- **No authentication required** for public datasets

## Column Mapping Summary

See `config.json` for the complete mapping. Key highlights:

| CRASH LENS Field | Maryland Raw Column | Notes |
|-----------------|---------------------|-------|
| ID | `report_no` | Unique crash report number |
| DATE | `acc_date` | Calendar date type |
| TIME | `acc_time` | HH:MM:SS format |
| LAT/LON | `latitude` / `longitude` | GPS coordinates |
| SEVERITY | `acrs_report_type` | 3-tier: Fatal/Injury/PDO — see mapping below |
| ROUTE | `road_name` | Road name |
| ROUTE_TYPE | `route_type_desc` | Interstate, US Route, MD Route, County, Municipality |
| COLLISION | `collision_type_desc` | Collision type description |
| WEATHER | `weather_desc` | Weather conditions |
| LIGHT | `light_desc` | Lighting conditions |
| SURFACE | `surf_cond_desc` | Surface condition |
| JURISDICTION | `county_desc` | County name or "Baltimore City" |
| JUNCTION | `junction_desc` | Junction/intersection type |
| HITRUN | `hit_run` | Hit and run flag |

## Severity Mapping — CRITICAL

Maryland uses a **3-tier crash-level classification** (not individual KABCO):

| `acrs_report_type` Value | Maps To | Notes |
|--------------------------|---------|-------|
| `Fatal Crash` | K | At least one fatality |
| `Injury Crash` | B | Conservative middle estimate (A/B/C breakdown requires Person Details dataset) |
| `Property Damage Crash` | O | No injuries |

**For true KABCO breakdown:** Join with Person Details dataset (`py4c-dicf`) by `report_no` and take max severity per crash. The Person Details dataset has `INJ_SEVER_DESC` with full K/A/B/C/O values.

## VDOT Value Mapping — Key Differences

Maryland uses descriptive text values. Key mappings to VDOT vocabulary:
- `Clear` → `1. No Adverse Condition (Clear/Cloudy)`
- `Raining` → `2. Rain`
- `Daylight` → `1. Daylight`
- `Dark-Lights On` → `2. Dark - Street Lights`
- `Dark-No Lights` → `3. Dark - No Street Lights`
- Collision types use descriptive text (Angle, Head On, Rear End, etc.) — must map to numbered VDOT format

## Jurisdiction Filtering

- Filter using `county_desc` field in SoQL: `$where=county_desc='Montgomery'`
- Alternative: `county_no` numeric code
- 23 counties + Baltimore City (independent city, FIPS 24510)
- All FIPS codes listed in `source_manifest.json`

## Related Datasets

All linked by `report_no`:
- **Vehicle Details:** `mhft-5t5y`
- **Person Details (KABCO):** `py4c-dicf`
- **Pedestrian Details:** `yhmz-gxyw`

## ACRS 2.0 Note

The ACRS system was updated January 1, 2024. Some fields were added/modified. The download script handles both pre-2024 and post-2024 schemas.

## R2 Storage

All processed CSVs must be gzip-compressed before uploading to R2:

```
crash-lens-data/maryland/{jurisdiction}/
  all_roads.csv.gz
  county_roads.csv.gz
  no_interstate.csv.gz
  forecasts_all_roads.json
  forecasts_county_roads.json
  forecasts_no_interstate.json
```

## Pipeline Stages

1. **CONVERT** — Normalize Socrata JSON/CSV to VDOT format, map 3-tier severity
2. **VALIDATE** — QA/QC with auto-correction
3. **GEOCODE** — Fill missing GPS (most MD records have coordinates)
4. **SPLIT** — Filter by road type (all_roads, county_roads, no_interstate)
5. **PREDICT** — SageMaker Chronos-2 forecasting

## Key Files in This Folder

| File | Purpose |
|------|---------|
| `config.json` | Column mappings, road systems, severity mapping, EPDO weights |
| `source_manifest.json` | Socrata resource IDs, all 24 jurisdictions with FIPS codes |
| `PIPELINE_ARCHITECTURE.md` | Maryland-specific pipeline reference guide |
| `download_maryland_crash_data.py` | Production download script (Socrata SODA API) |

## EPDO Weights

Using FHWA 2025 (FHWA-SA-25-021): K=883, A=94, B=21, C=11, O=1

## GitHub Actions Workflow

- **File:** `.github/workflows/download-maryland-crash-data.yml`
- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select county from 24 jurisdiction dropdown
- **R2 upload:** Gzip-compressed CSVs to `maryland/{jurisdiction}/`
