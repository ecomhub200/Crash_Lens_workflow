# Delaware DOT — Data Configuration & Onboarding Guide

## Purpose

This document is the **single source of truth** for Claude Code when working with Delaware crash data. It covers data configuration, download, normalization, exceptions, automation, and known limitations. Any future enhancement to Delaware's data pipeline should reference and update this document.

---

## 1. State Data Profile

| Field | Value |
|-------|-------|
| **State** | Delaware |
| **Abbreviation** | DE |
| **FIPS** | 10 |
| **DOT** | DelDOT (Delaware Department of Transportation) |
| **Counties** | 3 (Kent=001, New Castle=003, Sussex=005) |
| **Data Custodian** | Delaware Dept. of Safety & Homeland Security (DSHS) |
| **Data Portal** | https://data.delaware.gov |
| **Dataset ID** | `827n-m6xc` |
| **API URL** | `https://data.delaware.gov/resource/827n-m6xc.json` |
| **API Type** | Socrata SODA API (public, no auth required) |
| **Update Frequency** | Monthly |
| **Historical Range** | 2009–present |

---

## 2. Data Source Details

### API Behavior
- **Pagination**: `$limit`/`$offset` (max 50,000 per request)
- **Filtering**: SoQL `$where` clause (e.g., `county_desc='Sussex'`)
- **Download method**: CSV export (`rows.csv?accessType=DOWNLOAD`) — preferred because it includes ALL fields
- **CSV Export**: Returns UPPERCASE field names with spaces (e.g., `CRASH DATETIME`, `COUNTY NAME`)
- **JSON API**: Returns **abbreviated** lowercase field names (e.g., `crash_class_desc`, `county_desc`)
- **IMPORTANT**: The JSON API is **missing** `crash_datetime` and `year` fields — always use CSV export

### Field Names — Three Formats

The dataset has undergone a schema change. The normalizer supports all three field name formats:

| CSV Export (UPPERCASE) | JSON API (Abbreviated) | Original Canonical Name |
|---|---|---|
| `CRASH DATETIME` | *(missing from API)* | `crash_datetime` |
| `YEAR` | *(missing from API)* | `year` |
| `CRASH CLASSIFICATION CODE` | `crash_class` | `crash_classification_code` |
| `CRASH CLASSIFICATION DESCRIPTION` | `crash_class_desc` | `crash_classification_description` |
| `MANNER OF IMPACT DESCRIPTION` | `impact_desc` | `manner_of_impact_description` |
| `WEATHER 1 DESCRIPTION` | `weather_1_desc` | `weather_1_description` |
| `LIGHTING CONDITION DESCRIPTION` | `light_cond_desc` | `lighting_condition_description` |
| `ROAD SURFACE DESCRIPTION` | `road_surface_desc` | `road_surface_description` |
| `LATITUDE` | `latitude` | `latitude` |
| `LONGITUDE` | `longitude` | `longitude` |
| `COUNTY NAME` | `county_desc` | `county_name` |
| `COUNTY CODE` | `county` | `county_code` |
| `PEDESTRIAN INVOLVED` | `ped_involved` | `pedestrian_involved` |
| `BICYCLED INVOLVED` | `bike_involved` | `bicycled_involved` |
| `ALCOHOL INVOLVED` | `alcohol_involved` | `alcohol_involved` |
| `DRUG INVOLVED` | `drug_involved` | `drug_involved` |
| `MOTORCYCLE INVOLVED` | `mc_involved` | `motorcycle_involved` |
| `SEATBELT USED` | `seatbelt_used` | `seatbelt_used` |
| `WORK ZONE` | `work_zone` | `work_zone` |
| `PRIMARY CONTRIBUTING CIRCUMSTANCE CODE` | `pri_contrib_circum` | `primary_contributing_circumstance_code` |
| `SCHOOL BUS INVOLVED CODE` | `school_bus_involved` | `school_bus_involved_code` |
| `DAY OF WEEK DESCRIPTION` | `day_of_week_desc` | `day_of_week_description` |

---

## 3. Normalization Rules

### Normalizer Location
- **File**: `scripts/state_adapter.py` → `DelawareNormalizer` class
- **State key**: `delaware` (JSON API) or `delaware_csv` (Excel/CSV export)
- **Config**: `states/delaware/config.json`

### Severity Mapping (Critical)

Delaware provides only **3 crash-level categories** — no person-level KABCO:

| Delaware Value | KABCO | Notes |
|---------------|-------|-------|
| `Fatal Crash` / `Fatality Crash` | K | Direct mapping |
| `Personal Injury Crash` | **A/B/C (proportional split)** | See below |
| `Property Damage Crash` / `Property Damage Only` | O | Direct mapping |
| `Non-Reportable` | O | Below reporting threshold |

**Proportional A/B/C Split**: Since Delaware does not distinguish between A, B, and C injury severity, we use NHTSA national averages to distribute injury crashes:
- **A (Suspected Serious)**: 8% of injury crashes
- **B (Suspected Minor)**: 32% of injury crashes
- **C (Possible Injury)**: 60% of injury crashes

Assignment is **deterministic** via MD5 hash of the crash composite ID, so the same crash always receives the same severity across runs.

**Why not map all to A?** Mapping all injury → A inflates EPDO by 2.3x (14.4/crash vs typical 5-6/crash), making Delaware data incomparable with Virginia and other states that have proper KABCO.

### EPDO Weights

Using **FHWA-SA-25-021 (October 2025)** comprehensive crash unit costs in 2024 dollars:

| Severity | Crash Cost | EPDO Weight |
|----------|-----------|-------------|
| K (Fatal) | $15,988,000 | 883 |
| A (Suspected Serious) | $1,705,100 | 94 |
| B (Suspected Minor) | $384,000 | 21 |
| C (Possible Injury) | $204,600 | 11 |
| O (PDO) | $18,100 | 1 |

Blended EPDO weight for undifferentiated "Personal Injury Crash": `0.08×94 + 0.32×21 + 0.60×11 ≈ 21` per injury crash.

### Composite Crash ID

Delaware has no crash ID field. We generate: `DE-{YYYYMMDD}-{HHMM}-{lat6}-{lon6}`

Example: `DE-20230615-1430-391059-755409`

### DateTime Parsing

Two formats must be handled:
1. **Socrata JSON API**: ISO 8601 `2023-06-15T14:30:00.000`
2. **Excel/CSV export**: Named month `2012 Apr 29 05:32:00 PM`

Both are parsed to: `Crash Date` = `YYYY-MM-DD`, `Crash Military Time` = `HHMM`, `Crash Year` = `YYYY`

### Boolean Field Mapping

| Virginia Standard | Delaware Source | Transform |
|-------------------|---------------|-----------|
| `Pedestrian?` | `pedestrian_involved` | Y→Yes, N→No |
| `Bike?` | `bicycled_involved` | Y→Yes, N→No |
| `Alcohol?` | `alcohol_involved` | Y→Yes, N→No |
| `Drug Related?` | `drug_involved` | Y→Yes, N→No |
| `Motorcycle?` | `motorcycle_involved` | Y→Yes, N→No |
| `Unrestrained?` | `seatbelt_used` | **Inverted**: Y→No, N→Yes |
| `Work Zone Related` | `work_zone` | Y→Yes, N→No |
| `Night?` | `lighting_condition_description` | Contains "dark" → Yes |
| `Speed?` | `primary_contributing_circumstance_code` | Codes 50-53 → Yes |
| `Distracted?` | `primary_contributing_circumstance_code` | Codes 60-66 → Yes |

### Fields NOT Available (Empty String)

| Virginia Standard | Why Missing | Future Resolution |
|-------------------|-------------|-------------------|
| `RTE Name` | No road/route name in dataset | Reverse geocoding via Cloudflare Workers pipeline |
| `Node` | No intersection ID | Spatial join against road network |
| `SYSTEM` | No road system classification | Derive from reverse geocoded route |
| `Functional Class` | Not in dataset | Derive from FHWA functional class shapefile |
| `Traffic Control Type` | Not in dataset | None planned |
| `Hitrun?` | Not in dataset | None available |
| `Drowsy?` | Not in dataset | None available |
| `Young?` / `Senior?` | No age data in crash-level dataset | Would need person-level data |

---

## 4. Download Pipeline

### Workflow File
`.github/workflows/download-delaware-crash-data.yml`

### Pipeline Flow
```
1. Download from Socrata API (download_delaware_crash_data.py)
      ↓ Raw CSV with Socrata field names
2. Normalize to Virginia standard (scripts/state_adapter.py --state delaware)
      ↓ Normalized CSV with Virginia-standard column names
3. Gzip compress
      ↓ .csv.gz files
4. Upload to R2 (crash-lens-data/delaware/{jurisdiction}/)
5. Commit manifest
6. Trigger pipeline.yml (split, aggregate, forecast)
```

### Download Script
- **File**: `data/DelawareDOT/download_delaware_crash_data.py`
- **Inputs**: `--jurisdiction`, `--years`, `--data-dir`, `--gzip`, `--force`
- **Output**: `{jurisdiction}_crashes.csv` in `data/DelawareDOT/`

### Schedule
- **Cron**: 1st of every month at 11:00 UTC
- **Manual**: `workflow_dispatch` with jurisdiction dropdown

### R2 Storage Path
```
crash-lens-data/delaware/{jurisdiction}/
  {jurisdiction}_crashes.csv.gz
```

---

## 5. Known Limitations & Exceptions

### Data Quality Issues
1. **3 rows out of ~2000 have NULL `CRASH DATETIME`** — produces empty date/year/time but GPS still works
2. **`bicycled_involved`** has a typo in the source (should be "bicycle") — handled as-is
3. **`primary_contributing_circumstance_code`** is `NA` for most records — Speed/Distracted flags will be mostly "No"

### Analysis Limitations
1. **Hotspots tab** will not work (requires `RTE Name` — empty until geocoding)
2. **CMF/Countermeasures tab** partially limited (no intersection type, no traffic control)
3. **Before/After studies** limited without route-based location selection
4. **Warrants tab** requires traffic control data (not available)
5. **EPDO accuracy** — proportional A/B/C split is an approximation; individual crash severity may be incorrect but aggregate statistics are nationally representative

### Comparison Caveat
When comparing Delaware to Virginia data in the same app:
- Delaware EPDO uses proportional A/B/C split (statistically accurate in aggregate)
- Virginia EPDO uses actual person-level KABCO (accurate per crash)
- Cross-state EPDO comparisons should note this methodological difference

---

## 6. Configuration Files Reference

| File | Purpose | Location |
|------|---------|----------|
| `data/DelawareDOT/config.json` | Raw field mappings, EPDO weights, data source | Data folder |
| `states/delaware/config.json` | Pipeline config, jurisdictions, split rules | States folder |
| `states/delaware/hierarchy.json` | Regions, MPOs, county hierarchy | States folder |
| `data/DelawareDOT/source_manifest.json` | API endpoints, FIPS codes | Data folder |
| `data/DelawareDOT/download_delaware_crash_data.py` | Download script | Data folder |
| `scripts/state_adapter.py` → `DelawareNormalizer` | Normalization logic | Scripts folder |
| `.github/workflows/download-delaware-crash-data.yml` | CI/CD workflow | Workflows folder |

---

## 7. Future Enhancement Roadmap

### Priority 1: Reverse Geocoding (Route Names)
- Add a geocoding step to the validation/geocoding Cloudflare Workers pipeline
- Use lat/lon to populate `RTE Name` from Delaware road network (TIGER/Line or DelDOT road centerlines)
- This unblocks: Hotspots tab, CMF route-based analysis, Before/After studies

### Priority 2: Road System Classification
- Once route names are available, classify into Interstate/US Route/State Route/Local
- Populate `SYSTEM`, `Functional Class`, `Ownership` fields
- Enables road-type split (all_roads, county_roads, no_interstate)

### Priority 3: Node/Intersection ID
- Spatial join crash locations against intersection point dataset
- Populate `Node` field with intersection identifiers
- Enables intersection-level analysis in CMF and Warrants tabs

### Priority 4: Person-Level Data
- Investigate if DSHS provides a related person-level dataset
- If available, join to crash data for true A/B/C severity
- Would replace proportional split with actual KABCO values
