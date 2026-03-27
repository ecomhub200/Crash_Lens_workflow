# Hawaii Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing Hawaii crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **DOT:** Hawaii Department of Transportation (HDOT)
- **Portal:** `data.hawaii.gov`
- **API Type:** Socrata SODA
- **State FIPS:** 15
- **Jurisdictions:** 4

---

## 2. Pipeline Overview

```
Hawaii Data Source
      |
      v
+------------------+    +------------------+    +------------------+    +------------------+
|   DOWNLOAD       |    |   CONVERT        |    |   VALIDATE       |    |   SPLIT          |
|   (Socrata)     |--->|   (normalize)    |--->|   (QA/QC)        |--->|   (road type)    |
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
| Base URL | `https://data.hawaii.gov/resource/a393-uawk.json` |
| Pagination | `$limit=50000&$offset=0` |
| Filtering | SoQL `$where` clause |
| Max per request | 50,000 rows |

### Download Script

```bash
python data/HawaiiDOT/download_hawaii_crash_data.py \
  --jurisdiction <county> \
  --years 2023 2024 \
  --gzip \
  --data-dir data/HawaiiDOT
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County/jurisdiction to filter |
| `--years` | Years to download |
| `--data-dir` | Output directory (default: `data/HawaiiDOT`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping

See `config.json` for the complete mapping from Hawaii raw fields to CRASH LENS standard (VDOT reference format).

Key fields: ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

---

## 5. R2 Storage Paths

```
crash-lens-data/hawaii/
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

**File:** `.github/workflows/download-hawaii-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select jurisdiction from dropdown
- **R2 upload:** Gzip CSVs to `hawaii/{jurisdiction}/`
- **Manifest:** Updates `data/r2-manifest.json`
