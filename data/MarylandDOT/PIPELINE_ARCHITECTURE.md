# Maryland Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing Maryland ACRS crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **System:** Maryland Automated Crash Reporting System (ACRS)
- **Portal:** Maryland Open Data Portal (Socrata)
- **Dataset:** Maryland Statewide Vehicle Crashes
- **Resource ID:** `65du-s3qu`
- **API:** Socrata SODA API
- **URL:** `https://opendata.maryland.gov/resource/65du-s3qu.json`
- **Format:** JSON/CSV
- **Coverage:** January 2015 – present
- **Refresh:** Periodic (approved reports only)
- **Data Dictionary:** [Zero Deaths MD](https://zerodeathsmd.gov/resources/crashdata/crashdashboard/dashboard-data-definitions/)

---

## 2. Pipeline Overview

```
Maryland Socrata API
      |
      v
+------------------+    +------------------+    +------------------+    +------------------+    +------------------+
|   DOWNLOAD       |    |   CONVERT        |    |   VALIDATE       |    |   GEOCODE        |    |   SPLIT          |
|   (SODA API)     |--->|   (normalize)    |--->|   (QA/QC)        |--->|   (fill GPS)     |--->|   (road type)    |
+------------------+    +------------------+    +------------------+    +------------------+    +------------------+
                                                                                                       |
                                                                                              +--------+--------+
                                                                                              |                 |
                                                                                              v                 v
                                                                                    3 output CSV.gz      3 forecast JSONs
                                                                                              |                 |
                                                                                              v                 v
                                                                                    +-------------------+
                                                                                    |   R2 UPLOAD       |
                                                                                    |   (gzip CSVs)     |
                                                                                    +-------------------+
```

---

## 3. Download Stage

### API Details

| Parameter | Value |
|-----------|-------|
| Base URL | `https://opendata.maryland.gov/resource/65du-s3qu.json` |
| Pagination | `$limit=50000&$offset=0` |
| County filter | `$where=county_desc='Montgomery'` |
| Date filter | `$where=acc_date>='2023-01-01T00:00:00.000'` |
| Order | `$order=acc_date ASC` |
| Max per request | 50,000 rows |
| Rate limit | Reasonable (no strict limit for public datasets) |

### Download Script

```bash
python data/MarylandDOT/download_maryland_crash_data.py \
  --jurisdiction montgomery \
  --years 2023 2024 \
  --gzip \
  --data-dir data/MarylandDOT
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County key (e.g., `montgomery`, `baltimore_city`) |
| `--years` | Space-separated years to download |
| `--data-dir` | Output directory (default: `data/MarylandDOT`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping (Maryland → VDOT Standard)

### Key Fields

| CRASH LENS Standard | Maryland Socrata Field | Type | Notes |
|--------------------|-----------------------|------|-------|
| Document Nbr | `report_no` | text | Unique crash ID |
| DATE | `acc_date` | calendar_date | Crash date |
| TIME | `acc_time` | text | HH:MM:SS |
| Latitude | `latitude` | number | GPS lat |
| Longitude | `longitude` | number | GPS lon |
| Crash Severity | **DERIVED** | — | From `acrs_report_type` (3-tier) |
| RTE Name | `road_name` | text | Road name |
| Collision Type | `collision_type_desc` | text | Collision type |
| Weather | `weather_desc` | text | Weather conditions |
| Light Condition | `light_desc` | text | Lighting |
| Physical Juris Name | `county_desc` | text | County name |
| Intersection Type | `junction_desc` | text | Junction type |
| Hit and Run? | `hit_run` | text | Hit & run flag |

### Severity Derivation

Maryland uses 3-tier crash classification (not individual KABCO):

| `acrs_report_type` | Maps To | EPDO Weight |
|---------------------|---------|-------------|
| Fatal Crash | K | 462 |
| Injury Crash | B | 12 |
| Property Damage Crash | O | 1 |

**Note:** For true A/B/C breakdown, join with Person Details dataset (`py4c-dicf`) by `report_no`.

---

## 5. Value Mapping (Maryland → VDOT Vocabulary)

### Weather

| Maryland Value | VDOT Standard |
|---------------|---------------|
| Clear | 1. No Adverse Condition (Clear/Cloudy) |
| Cloudy | 1. No Adverse Condition (Clear/Cloudy) |
| Raining | 2. Rain |
| Foggy | 3. Fog/Smog/Smoke |
| Snow | 4. Snow |
| Sleet | 5. Sleet/Hail/Freezing Rain |
| Blowing Sand/Dirt/Snow | 7. Other |
| Severe Crosswinds | 6. Severe Crosswinds |

### Lighting

| Maryland Value | VDOT Standard |
|---------------|---------------|
| Daylight | 1. Daylight |
| Dark-Lights On | 2. Dark - Street Lights |
| Dark-No Lights | 3. Dark - No Street Lights |
| Dawn | 4. Dawn |
| Dusk | 5. Dusk |
| Dark - Unknown Lighting | 6. Dark - Unknown |

### Route Type

| Maryland Value | Road System |
|---------------|-------------|
| Interstate | State-maintained |
| US (State) | State-maintained |
| Maryland (State) | State-maintained |
| County | Local |
| Municipality | Local |
| Other Public Roadway | Local |

---

## 6. Geocoding

Most Maryland ACRS records include GPS coordinates (`latitude`, `longitude`). For records missing coordinates:
1. Node lookup from intersection (`road_name` & `cross_street_name`)
2. Nominatim geocoding fallback
3. Persistent cache at `data/MarylandDOT/.geocode_cache.json`

---

## 7. R2 Storage Paths

```
crash-lens-data/
  maryland/
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

## 8. GitHub Actions Workflow

**File:** `.github/workflows/download-maryland-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select from 24 jurisdiction dropdown
- **R2 upload:** Gzip CSVs to `maryland/{jurisdiction}/`
- **Manifest:** Updates `data/r2-manifest.json`

---

## 9. ACRS 2.0 Schema Changes (January 2024)

The ACRS system was updated January 1, 2024. Key changes:
- Some fields added/modified
- Bicycle types changed: "Bicyclist" → "Cyclist (Electric)" / "Cyclist (Non-Electric)"
- `LANE_DESC` was removed October 2022

The download script handles both schemas transparently.

---

## 10. Related Datasets

For enhanced analysis, join by `report_no`:

| Dataset | Resource ID | Use Case |
|---------|------------|----------|
| Person Details | `py4c-dicf` | True KABCO severity per person |
| Vehicle Details | `mhft-5t5y` | Vehicle type, driver actions |
| Pedestrian Details | `yhmz-gxyw` | Pedestrian/cyclist analysis |
