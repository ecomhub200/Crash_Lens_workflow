# Arkansas Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing Arkansas crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **DOT:** Arkansas Department of Transportation (ArDOT)
- **Portal:** `gis.arkansas.gov`
- **API Type:** ArcGIS GeoServices
- **State FIPS:** 05
- **Jurisdictions:** 75

---

## 2. Pipeline Overview

```
Arkansas Data Source
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
| Feature Server | `https://gis.arkansas.gov/arcgis/rest/services/ARDOT/CrashData/FeatureServer/0` |
| Query endpoint | `https://gis.arkansas.gov/arcgis/rest/services/ARDOT/CrashData/FeatureServer/0/query` |
| Pagination | `resultOffset` / `resultRecordCount` |
| Max per request | 2,000 features |
| Response format | `f=json` or `f=geojson` |

### Download Script

```bash
python data/ArkansasDOT/download_arkansas_crash_data.py \
  --jurisdiction <county> \
  --years 2023 2024 \
  --gzip \
  --data-dir data/ArkansasDOT
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County/jurisdiction to filter |
| `--years` | Years to download |
| `--data-dir` | Output directory (default: `data/ArkansasDOT`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping

See `config.json` for the complete mapping from Arkansas raw fields to CRASH LENS standard (VDOT reference format).

Key fields: ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

---

## 5. R2 Storage Paths

```
crash-lens-data/arkansas/
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

**File:** `.github/workflows/download-arkansas-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select jurisdiction from dropdown
- **R2 upload:** Gzip CSVs to `arkansas/{jurisdiction}/`
- **Manifest:** Updates `data/r2-manifest.json`
