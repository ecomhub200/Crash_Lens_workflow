# 30 States Crash Data Configuration

## Overview

CRASH LENS expansion from 2 states (Virginia + Colorado) to **30 states** with per-state data pipeline folders under `data/`. Each state normalizes crash data to the Virginia VDOT reference format and stores in **Cloudflare R2** as gzip-compressed CSV.

**Total Infrastructure Created:**
- 29 new state DOT folders (+ existing CDOT)
- 5 files per state folder: `CLAUDE.md`, `config.json`, `source_manifest.json`, `PIPELINE_ARCHITECTURE.md`, `download_{state}_crash_data.py`
- 30 GitHub Actions workflow files (`.github/workflows/download-{state}-crash-data.yml`)
- R2 storage integration with gzip compression support

---

## Configuration Status Summary

### Fully Configured (12 states) — 0 _TBD_ fields

| # | State | Abbr | Folder | API Type | API Endpoint |
|---|-------|------|--------|----------|-------------|
| 1 | Colorado | CO | `CDOT/` | Socrata | `data.colorado.gov` |
| 2 | Connecticut | CT | `ConnecticutDOT/` | ArcGIS | `gis.cti.uconn.edu/arcgis/rest/services/Crash_Dashboards` |
| 3 | Delaware | DE | `DelawareDOT/` | Socrata | `data.delaware.gov/resource/827n-m6xc.json` |
| 4 | Florida | FL | `FloridaDOT/` | ArcGIS | `gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer/0` |
| 5 | Idaho | ID | `IdahoDOT/` | ArcGIS | `data-iplan.opendata.arcgis.com` |
| 6 | Illinois | IL | `IllinoisDOT/` | ArcGIS | `gis-idot.opendata.arcgis.com/datasets/crashes/FeatureServer/0` |
| 7 | Iowa | IA | `IowaDOT/` | ArcGIS | `gis.iowadot.gov/agshost/rest/services/Traffic_Safety/Crash_Data/FeatureServer/0` |
| 8 | Maryland | MD | `MarylandDOT/` | Socrata | `opendata.maryland.gov/resource/65du-s3qu.json` |
| 9 | New York | NY | `NewYorkDOT/` | Socrata | `data.ny.gov/resource/e8ky-4vqe.json` |
| 10 | NYC | NYC | `NYCDOT/` | Socrata | `data.cityofnewyork.us/resource/h9gi-nx95.json` |
| 11 | Oregon | OR | `OregonDOT/` | ArcGIS | `gis.odot.state.or.us/arcgis1006/rest/services/agol/OTSDE_Crash/MapServer/0` |
| 12 | Pennsylvania | PA | `PennsylvaniaDOT/` | ArcGIS | `gis.penndot.pa.gov/arcgis/rest/services/CrashData/FeatureServer/0` |

### Partially Configured (6 states) — Some fields need research

| # | State | Abbr | Folder | _TBD_ Fields | API Type | Notes |
|---|-------|------|--------|-------------|----------|-------|
| 13 | Massachusetts | MA | `MassachusettsDOT/` | 9 | ArcGIS | MassDOT IMPACT crash system. WKID:26986 (NAD83 StatePlane MA Mainland). |
| 14 | Nevada | NV | `NevadaDOT/` | 10 | ArcGIS | NDOT GeoHub crash data. Some fields confirmed but collision/weather/light/surface TBD. |
| 15 | Wisconsin | WI | `WisconsinDOT/` | 11 | ArcGIS | WisDOT crash data. Several fields confirmed from FeatureServer metadata. |
| 16 | Alaska | AK | `AlaskaDOT/` | 13 | ArcGIS | AKDOT test FeatureServer available. Limited public field documentation. |
| 17 | Ohio | OH | `OhioDOT/` | 14 | ArcGIS | TIMS crash data. ODOT has FeatureServer but limited public field docs. |
| 18 | Utah | UT | `UtahDOT/` | 14 | ArcGIS MapServer | UDOT Crash_Locations. Uses Numetric for querying. EPSG:26912. |

### Restricted / No Public API (12 states) — Full column mapping requires data access

| # | State | Abbr | Folder | _TBD_ Fields | API Status | Action Required |
|---|-------|------|--------|-------------|------------|----------------|
| 19 | Washington | WA | `WashingtonDOT/` | 16 | Dashboard only | Public Disclosure Request needed |
| 20 | Arkansas | AR | `ArkansasDOT/` | 17 | ArcGIS | Need to query FeatureServer metadata |
| 21 | Georgia | GA | `GeorgiaDOT/` | 17 | Login required | AASHTO Safety signin — not public |
| 22 | Louisiana | LA | `LouisianaDOT/` | 17 | No public endpoint | Contact LADOTD for access |
| 23 | Mississippi | MS | `MississippiDOT/` | 17 | No public endpoint | Contact MDOT for access |
| 24 | Oklahoma | OK | `OklahomaDOT/` | 17 | No public FeatureServer | Uses Numetric (internal) |
| 25 | South Carolina | SC | `SouthCarolinaDOT/` | 17 | Fatalities only | Full crash data via SCCATTS (not public) |
| 26 | Texas | TX | `TexasDOT/` | 17 | CRIS (login required) | Bulk download after creating account |
| 27 | Vermont | VT | `VermontDOT/` | 17 | Custom REST API | `apps.vtrans.vermont.gov/crashdata/api` |
| 28 | West Virginia | WV | `WestVirginiaDOT/` | 17 | No public endpoint | Contact DOTSupport@wv.gov |
| 29 | Hawaii | HI | `HawaiiDOT/` | 18 | Socrata | Resource exists but field names need verification |
| 30 | Montana | MT | `MontanaDOT/` | 18 | Web map only | Need to find FeatureServer URL from web map |

---

## API Types and Pagination

### Socrata SODA API (6 states)
- **Pagination:** `$limit` / `$offset`
- **Format:** JSON by default, CSV with `.csv` extension
- **States:** MD, CT, DE, NY, NYC, HI

### ArcGIS GeoServices API (21 states)
- **Pagination:** `resultOffset` / `resultRecordCount`
- **Format:** JSON (GeoJSON via `f=geojson`)
- **Max records per request:** Varies (1,000–10,000)
- **States:** IA, IL, FL, OR, PA, MA, OH, WI, AK, NV, UT, ID, WA, GA, SC, AR, MT, MS, OK, LA, WV

### Custom REST API (1 state)
- **Vermont:** Custom API at `apps.vtrans.vermont.gov/crashdata/api/Accident`

### Bulk Download Portal (1 state)
- **Texas:** CRIS at `cris.dot.state.tx.us` — requires account creation

### Existing (1 state)
- **Colorado:** Already configured via Socrata at `data.colorado.gov`

---

## Per-State File Inventory

Each state folder contains 5 standard files:

| File | Purpose |
|------|---------|
| `CLAUDE.md` | AI agent instructions for that state's pipeline |
| `config.json` | Column mappings, severity config, road systems, data source info |
| `source_manifest.json` | County FIPS codes, API endpoints, jurisdiction filters |
| `PIPELINE_ARCHITECTURE.md` | Pipeline documentation, data dictionary references |
| `download_{state}_crash_data.py` | Standalone download script with `--jurisdiction`, `--years`, `--gzip`, `--health-check` |

Each state also has a dedicated GitHub Actions workflow:
- `.github/workflows/download-{state}-crash-data.yml`
- Monthly schedule + manual `workflow_dispatch` with county dropdown
- R2 upload step with gzip compression

---

## Column Mapping Reference

All states normalize to the CRASH LENS standard field set. Fields are mapped from state-specific column names in each `config.json`.

### Standard Field Codes

| Field Code | Description | Priority |
|------------|-------------|----------|
| `ID` | Crash record identifier | Critical |
| `DATE` | Crash date | Critical |
| `TIME` | Crash time | Critical |
| `LAT` | Latitude (WGS84) | Critical |
| `LON` | Longitude (WGS84) | Critical |
| `SEVERITY` | KABCO severity level | Critical |
| `JURISDICTION` | County/jurisdiction name | Critical |
| `ROUTE` | Road/route name | High |
| `COLLISION` | Collision type | High |
| `WEATHER` | Weather conditions | Medium |
| `LIGHT` | Lighting conditions | Medium |
| `SURFACE_CONDITION` | Road surface condition | Medium |
| `JUNCTION` | Junction/intersection type | Medium |
| `HITRUN` | Hit-and-run indicator | Medium |
| `PED_COUNT` | Pedestrian count | Medium |
| `BIKE_COUNT` | Bicycle count | Medium |
| `VEHICLE_COUNT` | Vehicle count | Low |

### Fully Mapped States — Column Name Details

#### Colorado (CO) — Socrata
| Field | Column Name |
|-------|------------|
| ID | `case_id` |
| DATE | `crash_date` |
| LAT | `geo_lat` |
| LON | `geo_lon` |
| SEVERITY | `injury_sev` |

#### Maryland (MD) — Socrata
| Field | Column Name |
|-------|------------|
| ID | `report_no` |
| DATE | `crash_date_time` |
| LAT | `latitude` |
| LON | `longitude` |
| SEVERITY | `inj_sev_code` |
| ROUTE | `mainroad_name` |
| COLLISION | `collision_type_code` |
| WEATHER | `weather_code` |
| LIGHT | `light_code` |
| SURFACE | `surf_cond_code` |
| HITRUN | `hit_run_flag` |

#### Connecticut (CT) — ArcGIS via UConn
| Field | Column Name |
|-------|------------|
| ID | `CrashId` |
| DATE | `DateOfCrash` |
| LAT | `Latitude` |
| LON | `Longitude` |
| SEVERITY | `CrashSeverity` |
| ROUTE | `RoadwayName` |
| COLLISION | `MannerOfCollision` |
| WEATHER | `WeatherCondition1` |
| LIGHT | `LightCondition` |
| SURFACE | `RoadSurfaceCondition` |
| JUNCTION | `TrafficControlDevice` |
| HITRUN | `IsHitAndRun` |

#### Delaware (DE) — Socrata
| Field | Column Name |
|-------|------------|
| ID | `collision_id` |
| DATE | `collision_date` |
| LAT | `latitude` |
| LON | `longitude` |
| SEVERITY | `highest_injury_severity` |
| ROUTE | `mainroad_name` |
| COLLISION | `collision_type` |
| WEATHER | `weather_conditions` |
| LIGHT | `light_conditions` |
| SURFACE | `surface_conditions` |
| JUNCTION | `junction_type` |
| HITRUN | `hit_and_run` |

#### New York (NY) — Socrata
| Field | Column Name |
|-------|------------|
| ID | `case_individual_id` |
| DATE | `date` |
| LAT | `latitude` |
| LON | `longitude` |
| SEVERITY | `event_descriptor` |
| ROUTE | `road_name` |
| COLLISION | `collision_type_descriptor` |
| WEATHER | `weather_conditions` |
| LIGHT | `lighting_conditions` |
| SURFACE | `road_surface_conditions` |
| JUNCTION | `traffic_control_device` |

#### NYC — Socrata
| Field | Column Name |
|-------|------------|
| ID | `collision_id` |
| DATE | `crash_date` |
| LAT | `latitude` |
| LON | `longitude` |
| SEVERITY | `_DERIVED` (from injury/fatality counts) |
| ROUTE | `on_street_name` |

#### Iowa (IA) — ArcGIS
| Field | Column Name |
|-------|------------|
| ID | `CASENUMBER` |
| DATE | `CRASHDATE` |
| LAT | `CSLATDD` |
| LON | `CSLONDD` |
| SEVERITY | `INJURYSEVERITY` |
| ROUTE | `MAJORCAUSE` |
| COLLISION | `MANNEROFCRASHCOLLISION` |
| WEATHER | `WEATHER1` |
| LIGHT | `LIGHTCONDITION` |
| SURFACE | `ROADSURFACE` |
| JUNCTION | `TRAFFICCONTROLDEVICE` |
| HITRUN | `HITANDRUN` |
**Note:** XCOORD/YCOORD are EPSG:26915 (UTM Zone 15N). Use CSLATDD/CSLONDD for WGS84.

#### Illinois (IL) — ArcGIS
| Field | Column Name |
|-------|------------|
| ID | `CrashID` or `CASENUMBER` |
| DATE | `CRASHDATE` |
| LAT | `LATITUDE` |
| LON | `LONGITUDE` |
| SEVERITY | `MOSTSEVEREINJ` |
| ROUTE | `STREETNAME` |
| COLLISION | `FIRSTCRASHTYPE` |
| WEATHER | `WEATHER` |
| LIGHT | `LIGHTCONDITION` |
| SURFACE | `ROADSURFACECOND` |
| JUNCTION | `TRAFFICCONTROLDEVICE` |
| HITRUN | `HITANDRUNIND` |

#### Florida (FL) — ArcGIS
| Field | Column Name |
|-------|------------|
| ID | `CRASH_NUMBER` |
| DATE | `CRASH_DATE` |
| LAT | `OFFICER_LATITUDE` |
| LON | `OFFICER_LONGITUDE` |
| SEVERITY | Derived from `INJSEVER` |
| ROUTE | `ON_ROADWAY_NAME` |
| COLLISION | `FRST_HARM_LOC_CD` |
| WEATHER | `EVNT_WTHR_COND_CD` |
| LIGHT | `LGHT_COND_CD` |
| SURFACE | `RD_SRFC_COND_CD` |
| JUNCTION | `JCT_CD` |
| HITRUN | Vehicle-level field (Signal4 derived) |

#### Oregon (OR) — ArcGIS MapServer
| Field | Column Name |
|-------|------------|
| ID | `CRASH_ID` |
| DATE | `CRASH_DT` |
| LAT | `LAT_DD` |
| LON | `LONGTD_DD` |
| SEVERITY | `KABCO` (direct) |
| ROUTE | `RTE_NM` |
| COLLISION | `COLLIS_TYP_LONG_DESC` |
| WEATHER | `WTHR_COND_LONG_DESC` |
| LIGHT | `LGT_COND_LONG_DESC` |
| SURFACE | `RD_SURF_MED_DESC` |
| JUNCTION | `TRAF_CNTL_DEVICE_LONG_DESC` |
| HITRUN | `CRASH_HIT_RUN_FLG` |

#### Pennsylvania (PA) — ArcGIS
| Field | Column Name |
|-------|------------|
| ID | `CRN` |
| DATE | `CRASH_DATE` (text yyyymmdd) |
| LAT | `DEC_LAT` |
| LON | `DEC_LONG` |
| SEVERITY | `MAX_SEVERITY_LEVEL` |
| ROUTE | ROADWAY table join (via CRN) |
| COLLISION | `COLLISION_TYPE` |
| WEATHER | `WEATHER` |
| LIGHT | `ILLUMINATION` |
| SURFACE | `ROAD_CONDITION` |
| JUNCTION | `INTERSECT_TYPE` |
| HITRUN | FLAG table join (via CRN) |
**Note:** Multi-table database. CRASH, ROADWAY, FLAG, PERSON, VEHICLE tables joined via CRN.

#### Idaho (ID) — ArcGIS
| Field | Column Name |
|-------|------------|
| ID | `Serial_Number` |
| DATE | Derived from `Accident_Year` + `Accident_Month` |
| LAT | `Latitude` |
| LON | `Longitude` |
| SEVERITY | `Severity` (text: Fatal/Type A/Type B/Type C/PDO) |
| ROUTE | `Street1` |
| COLLISION | EVENTS table join (CIRCA database) |
| WEATHER | `Weather_Condition_1` |
| LIGHT | `Light_Condition` |
| SURFACE | `Road_Surface_Condition` |
| JUNCTION | `IntersectionRelated` |
| HITRUN | UNIT table join (CIRCA database) |

---

## Multi-Table Database States

Some states store crash data across multiple linked tables requiring joins:

| State | Primary Table | Secondary Tables | Join Key |
|-------|--------------|-----------------|----------|
| Pennsylvania | CRASH | ROADWAY, FLAG, PERSON, VEHICLE | CRN |
| Idaho | Crash record | EVENTS, UNIT | Serial_Number |
| Florida | Crashes_All | Vehicle-level data (Signal4) | CRASH_NUMBER |

---

## Severity Mapping Methods

| Method | States | Description |
|--------|--------|-------------|
| `direct_kabco` | OR | Direct KABCO letter in field |
| `direct_coded` | PA, IL, IA, CT, MD | Numeric or letter code mapping to KABCO |
| `text_to_kabco` | ID | Text severity (e.g., "Fatal" → K) |
| `direct_or_derived` | FL, MA, NV, WI, AK, OH, UT, WA | May need injury count derivation |
| `socrata_text` | DE, NY, NYC | Text field with various descriptions |
| `_TBD` | GA, LA, MS, OK, SC, TX, VT, WV, HI, MT, AR | Not yet determined |

### EPDO Weights (All States)
| Severity | Weight | Description |
|----------|--------|-------------|
| K | 462 | Fatal |
| A | 62 | Suspected Serious Injury |
| B | 12 | Suspected Minor Injury |
| C | 5 | Possible Injury |
| O | 1 | Property Damage Only |

Source: FHWA/HSM Standard

---

## R2 Storage Configuration

### Bucket Structure
```
crash-lens-data/
  {state_lowercase}/
    {jurisdiction}/
      all_roads.csv.gz
      county_roads.csv.gz
      no_interstate.csv.gz
      standardized.csv.gz
      raw/
        {year}.csv.gz
      forecasts_all_roads.json
      forecasts_county_roads.json
      forecasts_no_interstate.json
    _state/
      statewide.csv.gz
    _statewide/
      aggregates.json
    _region/
      {region_name}.json
    _mpo/
      {mpo_name}.json
```

### R2 Upload Action
- Location: `.github/actions/upload-r2/action.yml`
- Content-Encoding: `gzip` for `.csv.gz` and `.json.gz` files
- Manifest: `data/r2-manifest.json` (version 3)
- Retry logic: 3 attempts with exponential backoff

---

## GitHub Actions Workflows

Each state has a dedicated workflow at `.github/workflows/download-{state}-crash-data.yml`:

### Workflow Features
- **Schedule:** Monthly (`cron: '0 11 1 * *'`)
- **Manual trigger:** `workflow_dispatch` with county dropdown
- **Python 3.11** environment
- **Arguments:** `--jurisdiction`, `--years`, `--data-dir`, `--gzip`
- **R2 upload** step with gzip compression
- **Manifest commit** with retry logic

### Workflow Count: 30 total
```
download-alaska-crash-data.yml
download-arkansas-crash-data.yml
download-cdot-crash-data.yml (existing)
download-connecticut-crash-data.yml
download-delaware-crash-data.yml
download-florida-crash-data.yml
download-georgia-crash-data.yml
download-hawaii-crash-data.yml
download-idaho-crash-data.yml
download-illinois-crash-data.yml
download-iowa-crash-data.yml
download-louisiana-crash-data.yml
download-maryland-crash-data.yml
download-massachusetts-crash-data.yml
download-mississippi-crash-data.yml
download-montana-crash-data.yml
download-nevada-crash-data.yml
download-new-york-crash-data.yml
download-nyc-crash-data.yml
download-ohio-crash-data.yml
download-oklahoma-crash-data.yml
download-oregon-crash-data.yml
download-pennsylvania-crash-data.yml
download-south-carolina-crash-data.yml
download-texas-crash-data.yml
download-utah-crash-data.yml
download-vermont-crash-data.yml
download-washington-crash-data.yml
download-west-virginia-crash-data.yml
download-wisconsin-crash-data.yml
```

---

## Next Steps for Completion

### Priority 1: Partially Configured States (6 states)
These have public APIs but incomplete field mappings. Requires querying FeatureServer metadata (`?f=json`) to get field names.

1. **Massachusetts** (9 TBD) — Query `gis.impact.dot.state.ma.us` FeatureServer fields
2. **Nevada** (10 TBD) — Query NDOT GeoHub FeatureServer fields
3. **Wisconsin** (11 TBD) — Query WisDOT FeatureServer fields
4. **Alaska** (13 TBD) — Query AKDOT test FeatureServer fields
5. **Ohio** (14 TBD) — Query TIMS CrashData FeatureServer fields
6. **Utah** (14 TBD) — Query UDOT Crash_Locations MapServer fields

### Priority 2: Accessible But Undocumented States (3 states)
- **Arkansas** (17 TBD) — ArcGIS endpoint exists, query for field names
- **Hawaii** (18 TBD) — Socrata resource exists, query metadata
- **Montana** (18 TBD) — Web map exists, need to find FeatureServer URL

### Priority 3: Restricted Access States (9 states)
These require formal data access requests or account creation:

| State | Action Required |
|-------|----------------|
| Georgia | Request AASHTO Safety portal access |
| Louisiana | Contact LADOTD for crash data access |
| Mississippi | Contact MDOT for data |
| Oklahoma | Request Numetric access from OKDOT |
| South Carolina | Request SCCATTS data from SCDPS |
| Texas | Create CRIS account for bulk download |
| Vermont | Test custom REST API endpoints |
| Washington | Submit Public Disclosure Request to WSDOT |
| West Virginia | Contact DOTSupport@wv.gov |

### Priority 4: Validation and Testing
1. Validate all `config.json` files parse correctly
2. Validate all `source_manifest.json` files have correct FIPS codes
3. Validate all Python download scripts compile without errors
4. Validate all YAML workflow files parse correctly
5. Run `--health-check` on states with public APIs
6. Test `--gzip` output produces valid `.csv.gz` files

---

## Data Dictionary References

| State | Data Dictionary URL/Reference |
|-------|------------------------------|
| CO | CDOT Crash Data on data.colorado.gov |
| MD | Maryland Open Data Portal - crash fields |
| CT | UConn CTDOT Crash Data Dashboard |
| DE | Delaware Open Data Portal - collision fields |
| NY | NY Open Data - Motor Vehicle Crashes |
| NYC | NYC Open Data - Motor Vehicle Collisions |
| IA | Iowa DOT Traffic Safety data fields |
| IL | IDOT SR 1050 Crash Reporting Data Dictionary |
| FL | FDOT Signal4 Analytics / State Safety Office |
| OR | CDS Code Manual: oregon.gov/ODOT/Data/documents/CDS_Code_Manual.pdf |
| PA | PennDOT Data Dictionary 2025: gis.penndot.pa.gov/gishub/crashZip/Crash_Data_Dictionary_2025.pdf |
| ID | ITD Numetric Data Dictionary: support.numetric.com/en/articles/8715919-itd-data-dictionary |
| MA | MassDOT IMPACT ODP 2024 FeatureServer |
| OH | ODOT TIMS: gis.dot.state.oh.us/tims |
| UT | UDOT Numetric: support.numetric.com/en/articles/4730297-udot-crash-data-fields-by-data-table |
| WI | WisDOT crash data FeatureServer |
| NV | NDOT GeoHub crash dataset |
| AK | AKDOT CrashData_test FeatureServer |
| WA | WSDOT NHFP Crash Data Dictionary: wsdot.wa.gov/sites/default/files/2022-01/NHFP-crash-data-dictionary.pdf |
| TX | TxDOT CRIS: cris.dot.state.tx.us |
| VT | VTrans Crash Data API: apps.vtrans.vermont.gov/crashdata |
| AR | ARDOT CrashData FeatureServer |
| GA | GDOT AASHTO Safety: gdot.aashtowaresafety.com |
| HI | Hawaii DOT Highways Socrata: highways.hidot.hawaii.gov |
| LA | LADOTD Open Data: data-ladotd.opendata.arcgis.com |
| MS | MDOT Open Data: gis-mdot.opendata.arcgis.com |
| MT | MDT Open Data: gis-mdt.opendata.arcgis.com |
| OK | OKDOT Numetric: support.numetric.com/en/articles/6150044-odot-crash-query-overview |
| SC | SCDPS Fatality Count: fatality-count-scdps.hub.arcgis.com |
| WV | WVDOT: data-wvdot.opendata.arcgis.com (no crash layers public) |

---

## Coordinate System Notes

Most states provide WGS84 lat/lon directly. Exceptions:

| State | Native CRS | Fields | Conversion |
|-------|-----------|--------|------------|
| Iowa | EPSG:26915 (UTM Zone 15N) | XCOORD, YCOORD | Use CSLATDD/CSLONDD instead |
| Massachusetts | WKID:26986 (NAD83 SP MA Mainland) | Geometry X/Y | Reproject to WGS84 |
| Utah | EPSG:26912 (NAD83 UTM Zone 12N) | MapServer geometry | Reproject to WGS84 |
| Washington | NAD83 HARN SP WA South FIPS 4602 | Geometry | Request `outSR=4326` |
| Oregon | Projected | MapServer geometry | Use LAT_DD/LONGTD_DD fields |

---

*Last updated: 2026-02-15*
*Branch: claude/integrate-cdot-r2-storage-w8YO1*
*PR: #105*
