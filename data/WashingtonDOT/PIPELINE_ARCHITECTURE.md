# Washington Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing Washington crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **DOT:** Washington State Department of Transportation (WSDOT)
- **Portal:** `geo.wa.gov`
- **API Type:** ArcGIS GeoServices
- **State FIPS:** 53
- **Jurisdictions:** 39

---

## 2. Pipeline Overview

```
Washington Data Source
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
| Feature Server | `https://geo.wa.gov/datasets/wsdot-crash-data/FeatureServer/0` |
| Query endpoint | `https://geo.wa.gov/datasets/wsdot-crash-data/FeatureServer/0/query` |
| Pagination | `resultOffset` / `resultRecordCount` |
| Max per request | 2,000 features |
| Response format | `f=json` or `f=geojson` |

### Download Script

```bash
python data/WashingtonDOT/download_washington_crash_data.py \
  --jurisdiction <county> \
  --years 2023 2024 \
  --gzip \
  --data-dir data/WashingtonDOT
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County/jurisdiction to filter |
| `--years` | Years to download |
| `--data-dir` | Output directory (default: `data/WashingtonDOT`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping

See `config.json` for the complete mapping from Washington raw fields to CRASH LENS standard (VDOT reference format).

Key fields: ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

---

## 5. R2 Storage Paths

```
crash-lens-data/washington/
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

**File:** `.github/workflows/download-washington-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select jurisdiction from dropdown
- **R2 upload:** Gzip CSVs to `washington/{jurisdiction}/`
- **Manifest:** Updates `data/r2-manifest.json`
