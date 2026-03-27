# Pennsylvania Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing Pennsylvania crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **DOT:** Pennsylvania Department of Transportation (PennDOT)
- **Portal:** `crashinfo.penndot.pa.gov`
- **API Type:** ArcGIS GeoServices
- **State FIPS:** 42
- **Jurisdictions:** 67

---

## 2. Pipeline Overview

```
Pennsylvania Data Source
      |
      v
+------------------+    +------------------+    +------------------+    +------------------+
|   DOWNLOAD       |    |   CONVERT        |    |   VALIDATE       |    |   SPLIT          |
|   (ArcGIS)     |--->|   (normalize)    |--->|   (QA/QC)        |--->|   (road type)    |
+------------------+    +------------------+    +------------------+    +------------------+
                                                                               |
                                                                      +--------+--------+
                                                                      |                 |
                                                                      v                 v
                                                             3 CSV.gz files      3 forecast JSONs
                                                                      |                 |
                                                                      v                 v
                                                             +-------------------+
                                                             |   R2 UPLOAD       |
                                                             +-------------------+
```

---

## 3. Download Stage

### API Details

| Parameter | Value |
|-----------|-------|
| Feature Server | `https://gis.penndot.pa.gov/arcgis/rest/services/CrashData/FeatureServer/0` |
| Query endpoint | `https://gis.penndot.pa.gov/arcgis/rest/services/CrashData/FeatureServer/0/query` |
| Pagination | `resultOffset` / `resultRecordCount` |
| Max per request | 2,000 features |
| Response format | `f=json` or `f=geojson` |

### Download Script

```bash
python data/PennsylvaniaDOT/download_pennsylvania_crash_data.py \
  --jurisdiction <county> \
  --years 2023 2024 \
  --gzip \
  --data-dir data/PennsylvaniaDOT
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County/jurisdiction to filter |
| `--years` | Years to download |
| `--data-dir` | Output directory (default: `data/PennsylvaniaDOT`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping

See `config.json` for the complete mapping from Pennsylvania raw fields to CRASH LENS standard (VDOT reference format).

Key fields: ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

---

## 5. R2 Storage Paths

```
crash-lens-data/pennsylvania/
  {jurisdiction}/
    all_roads.csv.gz
    county_roads.csv.gz
    no_interstate.csv.gz
    forecasts_all_roads.json
    forecasts_county_roads.json
    forecasts_no_interstate.json
```

All CSVs gzip-compressed per R2 integration plan.

---

## 6. GitHub Actions Workflow

**File:** `.github/workflows/download-pennsylvania-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select jurisdiction from dropdown
- **R2 upload:** Gzip CSVs to `pennsylvania/{jurisdiction}/`
- **Manifest:** Updates `data/r2-manifest.json`
