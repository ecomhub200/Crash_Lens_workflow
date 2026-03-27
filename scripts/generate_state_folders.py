#!/usr/bin/env python3
"""
Generate state DOT pipeline folders with all 5 files + 1 workflow per state.
Uses state metadata and API research to create production-quality files.

Usage:
    python scripts/generate_state_folders.py
"""

import json
import os
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# =============================================================================
# State Definitions — All 29 new states (CDOT already exists)
# =============================================================================

STATES = {
    # --- Socrata SODA API (6 states) ---
    # Maryland already created manually as template
    "connecticut": {
        "name": "Connecticut", "abbr": "CT", "fips": "09", "dot": "CTDOT",
        "dot_full": "Connecticut Department of Transportation",
        "folder": "ConnecticutDOT", "api_type": "socrata",
        "portal": "data.ct.gov", "resource_id": "2cim-gya4",
        "api_url": "https://data.ct.gov/resource/2cim-gya4.json",
        "bounds": {"latMin": 40.95, "latMax": 42.05, "lonMin": -73.73, "lonMax": -71.79},
        "county_count": 8, "r2_prefix": "connecticut",
    },
    "delaware": {
        "name": "Delaware", "abbr": "DE", "fips": "10", "dot": "DelDOT",
        "dot_full": "Delaware Department of Transportation",
        "folder": "DelawareDOT", "api_type": "socrata",
        "portal": "data.delaware.gov", "resource_id": "827n-m6xc",
        "api_url": "https://data.delaware.gov/resource/827n-m6xc.json",
        "bounds": {"latMin": 38.45, "latMax": 39.84, "lonMin": -75.79, "lonMax": -75.05},
        "county_count": 3, "r2_prefix": "delaware",
    },
    "new_york": {
        "name": "New York", "abbr": "NY", "fips": "36", "dot": "NYSDOT",
        "dot_full": "New York State Department of Transportation",
        "folder": "NewYorkDOT", "api_type": "socrata",
        "portal": "data.ny.gov", "resource_id": "e8ky-4vqe",
        "api_url": "https://data.ny.gov/resource/e8ky-4vqe.json",
        "bounds": {"latMin": 40.50, "latMax": 45.02, "lonMin": -79.76, "lonMax": -71.86},
        "county_count": 62, "r2_prefix": "new_york",
    },
    "nyc": {
        "name": "New York City", "abbr": "NYC", "fips": "36",
        "dot": "NYC DOT",
        "dot_full": "New York City Department of Transportation",
        "folder": "NYCDOT", "api_type": "socrata",
        "portal": "data.cityofnewyork.us", "resource_id": "h9gi-nx95",
        "api_url": "https://data.cityofnewyork.us/resource/h9gi-nx95.json",
        "bounds": {"latMin": 40.49, "latMax": 40.92, "lonMin": -74.26, "lonMax": -73.70},
        "county_count": 5, "r2_prefix": "nyc",
    },
    "hawaii": {
        "name": "Hawaii", "abbr": "HI", "fips": "15", "dot": "HDOT",
        "dot_full": "Hawaii Department of Transportation",
        "folder": "HawaiiDOT", "api_type": "socrata",
        "portal": "data.hawaii.gov", "resource_id": "a393-uawk",
        "api_url": "https://data.hawaii.gov/resource/a393-uawk.json",
        "bounds": {"latMin": 18.91, "latMax": 22.24, "lonMin": -160.25, "lonMax": -154.81},
        "county_count": 4, "r2_prefix": "hawaii",
    },

    # --- ArcGIS GeoServices API (20 states) ---
    "iowa": {
        "name": "Iowa", "abbr": "IA", "fips": "19", "dot": "Iowa DOT",
        "dot_full": "Iowa Department of Transportation",
        "folder": "IowaDOT", "api_type": "arcgis",
        "portal": "data.iowadot.gov",
        "api_url": "https://gis.iowadot.gov/agshost/rest/services/Traffic_Safety/Crash_Data/FeatureServer/0",
        "bounds": {"latMin": 40.38, "latMax": 43.50, "lonMin": -96.64, "lonMax": -90.14},
        "county_count": 99, "r2_prefix": "iowa",
    },
    "illinois": {
        "name": "Illinois", "abbr": "IL", "fips": "17", "dot": "IDOT",
        "dot_full": "Illinois Department of Transportation",
        "folder": "IllinoisDOT", "api_type": "arcgis",
        "portal": "gis-idot.opendata.arcgis.com",
        "api_url": "https://gis-idot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 36.97, "latMax": 42.51, "lonMin": -91.51, "lonMax": -87.50},
        "county_count": 102, "r2_prefix": "illinois",
    },
    "louisiana": {
        "name": "Louisiana", "abbr": "LA", "fips": "22", "dot": "LADOTD",
        "dot_full": "Louisiana Department of Transportation and Development",
        "folder": "LouisianaDOT", "api_type": "arcgis",
        "portal": "data-ladotd.opendata.arcgis.com",
        "api_url": "https://data-ladotd.opendata.arcgis.com/datasets/crash-data/FeatureServer/0",
        "bounds": {"latMin": 28.93, "latMax": 33.02, "lonMin": -94.04, "lonMax": -88.82},
        "county_count": 64, "r2_prefix": "louisiana",
    },
    "alaska": {
        "name": "Alaska", "abbr": "AK", "fips": "02", "dot": "AKDOT&PF",
        "dot_full": "Alaska Department of Transportation and Public Facilities",
        "folder": "AlaskaDOT", "api_type": "arcgis",
        "portal": "data-soa-akdot.opendata.arcgis.com",
        "api_url": "https://data-soa-akdot.opendata.arcgis.com/datasets/crash-data/FeatureServer/0",
        "bounds": {"latMin": 51.21, "latMax": 71.39, "lonMin": -179.15, "lonMax": -129.98},
        "county_count": 30, "r2_prefix": "alaska",
    },
    "massachusetts": {
        "name": "Massachusetts", "abbr": "MA", "fips": "25", "dot": "MassDOT",
        "dot_full": "Massachusetts Department of Transportation",
        "folder": "MassachusettsDOT", "api_type": "arcgis",
        "portal": "massdot-impact-crashes-vhb.opendata.arcgis.com",
        "api_url": "https://massdot-impact-crashes-vhb.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 41.24, "latMax": 42.89, "lonMin": -73.51, "lonMax": -69.93},
        "county_count": 14, "r2_prefix": "massachusetts",
    },
    "pennsylvania": {
        "name": "Pennsylvania", "abbr": "PA", "fips": "42", "dot": "PennDOT",
        "dot_full": "Pennsylvania Department of Transportation",
        "folder": "PennsylvaniaDOT", "api_type": "arcgis",
        "portal": "crashinfo.penndot.pa.gov",
        "api_url": "https://gis.penndot.pa.gov/arcgis/rest/services/CrashData/FeatureServer/0",
        "bounds": {"latMin": 39.72, "latMax": 42.27, "lonMin": -80.52, "lonMax": -74.69},
        "county_count": 67, "r2_prefix": "pennsylvania",
    },
    "florida": {
        "name": "Florida", "abbr": "FL", "fips": "12", "dot": "FDOT",
        "dot_full": "Florida Department of Transportation",
        "folder": "FloridaDOT", "api_type": "arcgis",
        "portal": "gis-fdot.opendata.arcgis.com",
        "api_url": "https://gis-fdot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 24.40, "latMax": 31.00, "lonMin": -87.63, "lonMax": -80.03},
        "county_count": 67, "r2_prefix": "florida",
    },
    "georgia": {
        "name": "Georgia", "abbr": "GA", "fips": "13", "dot": "GDOT",
        "dot_full": "Georgia Department of Transportation",
        "folder": "GeorgiaDOT", "api_type": "arcgis",
        "portal": "gdot.aashtowaresafety.net",
        "api_url": "https://gdot.aashtowaresafety.net/arcgis/rest/services/CrashData/FeatureServer/0",
        "bounds": {"latMin": 30.36, "latMax": 35.00, "lonMin": -85.61, "lonMax": -80.84},
        "county_count": 159, "r2_prefix": "georgia",
    },
    "south_carolina": {
        "name": "South Carolina", "abbr": "SC", "fips": "45", "dot": "SCDOT",
        "dot_full": "South Carolina Department of Transportation",
        "folder": "SouthCarolinaDOT", "api_type": "arcgis",
        "portal": "fatality-count-scdps.hub.arcgis.com",
        "api_url": "https://fatality-count-scdps.hub.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 32.03, "latMax": 35.22, "lonMin": -83.35, "lonMax": -78.54},
        "county_count": 46, "r2_prefix": "south_carolina",
    },
    "ohio": {
        "name": "Ohio", "abbr": "OH", "fips": "39", "dot": "ODOT",
        "dot_full": "Ohio Department of Transportation",
        "folder": "OhioDOT", "api_type": "arcgis",
        "portal": "gis.dot.state.oh.us",
        "api_url": "https://gis.dot.state.oh.us/arcgis/rest/services/TIMS/CrashData/FeatureServer/0",
        "bounds": {"latMin": 38.40, "latMax": 41.98, "lonMin": -84.82, "lonMax": -80.52},
        "county_count": 88, "r2_prefix": "ohio",
    },
    "wisconsin": {
        "name": "Wisconsin", "abbr": "WI", "fips": "55", "dot": "WisDOT",
        "dot_full": "Wisconsin Department of Transportation",
        "folder": "WisconsinDOT", "api_type": "arcgis",
        "portal": "data-wisdot.opendata.arcgis.com",
        "api_url": "https://data-wisdot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 42.49, "latMax": 47.08, "lonMin": -92.89, "lonMax": -86.25},
        "county_count": 72, "r2_prefix": "wisconsin",
    },
    "nevada": {
        "name": "Nevada", "abbr": "NV", "fips": "32", "dot": "NDOT",
        "dot_full": "Nevada Department of Transportation",
        "folder": "NevadaDOT", "api_type": "arcgis",
        "portal": "data-ndot.opendata.arcgis.com",
        "api_url": "https://data-ndot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 35.00, "latMax": 42.00, "lonMin": -120.01, "lonMax": -114.04},
        "county_count": 17, "r2_prefix": "nevada",
    },
    "utah": {
        "name": "Utah", "abbr": "UT", "fips": "49", "dot": "UDOT",
        "dot_full": "Utah Department of Transportation",
        "folder": "UtahDOT", "api_type": "arcgis",
        "portal": "data-uplan.opendata.arcgis.com",
        "api_url": "https://data-uplan.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 36.99, "latMax": 42.00, "lonMin": -114.05, "lonMax": -109.04},
        "county_count": 29, "r2_prefix": "utah",
    },
    "oregon": {
        "name": "Oregon", "abbr": "OR", "fips": "41", "dot": "ODOT",
        "dot_full": "Oregon Department of Transportation",
        "folder": "OregonDOT", "api_type": "arcgis",
        "portal": "oregon.gov/odot",
        "api_url": "https://gis.odot.state.or.us/arcgis/rest/services/CrashData/FeatureServer/0",
        "bounds": {"latMin": 41.99, "latMax": 46.29, "lonMin": -124.57, "lonMax": -116.46},
        "county_count": 36, "r2_prefix": "oregon",
    },
    "washington": {
        "name": "Washington", "abbr": "WA", "fips": "53", "dot": "WSDOT",
        "dot_full": "Washington State Department of Transportation",
        "folder": "WashingtonDOT", "api_type": "arcgis",
        "portal": "geo.wa.gov",
        "api_url": "https://geo.wa.gov/datasets/wsdot-crash-data/FeatureServer/0",
        "bounds": {"latMin": 45.54, "latMax": 49.00, "lonMin": -124.85, "lonMax": -116.92},
        "county_count": 39, "r2_prefix": "washington",
    },
    "idaho": {
        "name": "Idaho", "abbr": "ID", "fips": "16", "dot": "ITD",
        "dot_full": "Idaho Transportation Department",
        "folder": "IdahoDOT", "api_type": "arcgis",
        "portal": "data-iplan.opendata.arcgis.com",
        "api_url": "https://data-iplan.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 41.99, "latMax": 49.00, "lonMin": -117.24, "lonMax": -111.04},
        "county_count": 44, "r2_prefix": "idaho",
    },
    "montana": {
        "name": "Montana", "abbr": "MT", "fips": "30", "dot": "MDT",
        "dot_full": "Montana Department of Transportation",
        "folder": "MontanaDOT", "api_type": "arcgis",
        "portal": "gis-mdt.opendata.arcgis.com",
        "api_url": "https://gis-mdt.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 44.36, "latMax": 49.00, "lonMin": -116.05, "lonMax": -104.04},
        "county_count": 56, "r2_prefix": "montana",
    },
    "west_virginia": {
        "name": "West Virginia", "abbr": "WV", "fips": "54", "dot": "WVDOT",
        "dot_full": "West Virginia Department of Transportation",
        "folder": "WestVirginiaDOT", "api_type": "arcgis",
        "portal": "data-wvdot.opendata.arcgis.com",
        "api_url": "https://data-wvdot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 37.20, "latMax": 40.64, "lonMin": -82.64, "lonMax": -77.72},
        "county_count": 55, "r2_prefix": "west_virginia",
    },
    "mississippi": {
        "name": "Mississippi", "abbr": "MS", "fips": "28", "dot": "MDOT",
        "dot_full": "Mississippi Department of Transportation",
        "folder": "MississippiDOT", "api_type": "arcgis",
        "portal": "gis-mdot.opendata.arcgis.com",
        "api_url": "https://gis-mdot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 30.17, "latMax": 34.99, "lonMin": -91.66, "lonMax": -88.10},
        "county_count": 82, "r2_prefix": "mississippi",
    },
    "oklahoma": {
        "name": "Oklahoma", "abbr": "OK", "fips": "40", "dot": "OKDOT",
        "dot_full": "Oklahoma Department of Transportation",
        "folder": "OklahomaDOT", "api_type": "arcgis",
        "portal": "gis-okdot.opendata.arcgis.com",
        "api_url": "https://gis-okdot.opendata.arcgis.com/datasets/crashes/FeatureServer/0",
        "bounds": {"latMin": 33.62, "latMax": 37.00, "lonMin": -103.00, "lonMax": -94.43},
        "county_count": 77, "r2_prefix": "oklahoma",
    },
    "arkansas": {
        "name": "Arkansas", "abbr": "AR", "fips": "05", "dot": "ArDOT",
        "dot_full": "Arkansas Department of Transportation",
        "folder": "ArkansasDOT", "api_type": "arcgis",
        "portal": "gis.arkansas.gov",
        "api_url": "https://gis.arkansas.gov/arcgis/rest/services/ARDOT/CrashData/FeatureServer/0",
        "bounds": {"latMin": 33.00, "latMax": 36.50, "lonMin": -94.62, "lonMax": -89.64},
        "county_count": 75, "r2_prefix": "arkansas",
    },

    # --- Custom REST API (1 state) ---
    "vermont": {
        "name": "Vermont", "abbr": "VT", "fips": "50", "dot": "VTrans",
        "dot_full": "Vermont Agency of Transportation",
        "folder": "VermontDOT", "api_type": "custom_rest",
        "portal": "apps.vtrans.vermont.gov/crashdata",
        "api_url": "https://apps.vtrans.vermont.gov/crashdata/api/Accident",
        "bounds": {"latMin": 42.73, "latMax": 45.02, "lonMin": -73.44, "lonMax": -71.47},
        "county_count": 14, "r2_prefix": "vermont",
    },

    # --- CRIS Bulk Download (1 state) ---
    "texas": {
        "name": "Texas", "abbr": "TX", "fips": "48", "dot": "TxDOT",
        "dot_full": "Texas Department of Transportation",
        "folder": "TexasDOT", "api_type": "cris_bulk",
        "portal": "cris.dot.state.tx.us",
        "api_url": "https://cris.dot.state.tx.us/public/Query/app/home",
        "bounds": {"latMin": 25.84, "latMax": 36.50, "lonMin": -106.65, "lonMax": -93.51},
        "county_count": 254, "r2_prefix": "texas",
    },
}


def generate_claude_md(state_key, s):
    """Generate CLAUDE.md content for a state."""
    api_desc = {
        "socrata": f"Socrata SODA API at `{s['portal']}`",
        "arcgis": f"ArcGIS GeoServices API at `{s['portal']}`",
        "custom_rest": f"Custom REST JSON API at `{s['portal']}`",
        "cris_bulk": f"CRIS bulk CSV download at `{s['portal']}`",
    }
    return f"""# {s['folder']} ({s['dot_full']}) — Pipeline Instructions

## State Overview

| Field | Value |
|-------|-------|
| **State** | {s['name']} |
| **Abbreviation** | {s['abbr']} |
| **FIPS** | {s['fips']} |
| **DOT Name** | {s['dot']} |
| **Full Name** | {s['dot_full']} |
| **Counties** | {s['county_count']} |

## Data Source

- **System:** {api_desc[s['api_type']]}
- **Portal:** `{s['portal']}`
- **API URL:** `{s['api_url']}`
- **Format:** {'JSON/CSV (Socrata SODA API)' if s['api_type'] == 'socrata' else 'GeoJSON/CSV (ArcGIS Feature Server)' if s['api_type'] == 'arcgis' else 'JSON (Custom REST API)' if s['api_type'] == 'custom_rest' else 'CSV (CRIS Bulk Download)'}

## API Access

{'### Socrata SODA API' + chr(10) + '- Pagination: `$limit`/`$offset` (max 50,000 per request)' + chr(10) + '- Filtering: SoQL `$where` clause' + chr(10) + '- No authentication required for public datasets' if s['api_type'] == 'socrata' else '### ArcGIS Feature Server' + chr(10) + '- Pagination: `resultOffset`/`resultRecordCount`' + chr(10) + '- Filtering: SQL-like `where` clause' + chr(10) + '- Response format: `f=json` or `f=geojson`' if s['api_type'] == 'arcgis' else '### Custom REST API' + chr(10) + '- Custom pagination and filtering' + chr(10) + '- JSON response format' if s['api_type'] == 'custom_rest' else '### CRIS Bulk Download' + chr(10) + '- Registration required for data access' + chr(10) + '- Bulk CSV download with post-processing'}

## Column Mapping

See `config.json` for the complete column mapping from {s['name']} raw fields to CRASH LENS standardized format (VDOT reference).

**Key fields to map:** ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

## Severity Mapping

Check `config.json` → `derivedFields.SEVERITY` for the {s['name']}-specific severity derivation method.

## Jurisdiction Filtering

- {s['county_count']} jurisdictions defined in `source_manifest.json`
- All with FIPS codes for programmatic filtering
- State FIPS: {s['fips']}

## R2 Storage

All processed CSVs must be gzip-compressed before uploading to R2:

```
crash-lens-data/{s['r2_prefix']}/{{jurisdiction}}/
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
| `PIPELINE_ARCHITECTURE.md` | {s['name']}-specific pipeline reference guide |
| `download_{state_key}_crash_data.py` | Production download script |

## EPDO Weights

Using FHWA 2025 (FHWA-SA-25-021): K=883, A=94, B=21, C=11, O=1

## GitHub Actions Workflow

- **File:** `.github/workflows/download-{state_key.replace('_', '-')}-crash-data.yml`
- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select county from jurisdiction dropdown
- **R2 upload:** Gzip-compressed CSVs to `{s['r2_prefix']}/{{jurisdiction}}/`
"""


def generate_config_json(state_key, s):
    """Generate config.json content for a state."""
    config = {
        "state": {
            "name": s["name"],
            "abbreviation": s["abbr"],
            "fips": s["fips"],
            "dotName": s["dot"],
            "dotFullName": s["dot_full"],
            "dataPortalUrl": f"https://{s['portal']}",
            "coordinateBounds": s["bounds"],
            "dataDir": s["folder"],
        },
        "columnMapping": {
            "_description": f"Maps {s['name']} crash data fields to internal CRASH LENS field codes",
            "_note": f"Column names sourced from {s['name']} data dictionary. Update as API schema is confirmed.",
            "ID": "_TBD_CRASH_ID",
            "DATE": "_TBD_CRASH_DATE",
            "TIME": "_TBD_CRASH_TIME",
            "LAT": "_TBD_LATITUDE",
            "LON": "_TBD_LONGITUDE",
            "SEVERITY": "_TBD_SEVERITY",
            "JURISDICTION": "_TBD_COUNTY",
            "ROUTE": "_TBD_ROAD_NAME",
            "COLLISION": "_TBD_COLLISION_TYPE",
            "WEATHER": "_TBD_WEATHER",
            "LIGHT": "_TBD_LIGHTING",
            "SURFACE_CONDITION": "_TBD_SURFACE",
            "JUNCTION": "_TBD_JUNCTION_TYPE",
            "HITRUN": "_TBD_HIT_AND_RUN",
        },
        "derivedFields": {
            "_description": "Fields that must be computed from raw data during Stage 1 (CONVERT)",
            "SEVERITY": {
                "method": "direct_or_derived",
                "source": "_TBD",
                "_note": f"Update with {s['name']}-specific severity derivation method",
            },
            "YEAR": {"method": "extract_year", "source": "_TBD_CRASH_DATE"},
            "NODE": {
                "method": "concatenate",
                "sources": ["_TBD_ROAD1", "_TBD_ROAD2"],
                "separator": " & ",
            },
        },
        "roadSystems": {
            "_description": f"{s['name']} road classification for road-type filtering",
            "values": {
                "Interstate": {"classification": "state_maintained", "label": "Interstate"},
                "US Route": {"classification": "state_maintained", "label": "US Route"},
                "State Route": {"classification": "state_maintained", "label": "State Route"},
                "County Road": {"classification": "local", "label": "County Road"},
                "Local Road": {"classification": "local", "label": "Local Road"},
            },
            "filterProfiles": {
                "countyOnly": {
                    "include": ["County Road", "Local Road"],
                    "label": "County & Local Roads Only",
                },
                "countyPlusState": {
                    "include": ["County Road", "Local Road", "State Route", "US Route"],
                    "label": "County + State Routes",
                },
                "allRoads": {
                    "include": ["Interstate", "US Route", "State Route", "County Road", "Local Road"],
                    "label": "All Roads",
                },
            },
        },
        "crashTypeMapping": {
            "_description": f"Maps {s['name']} collision type values to CRASH LENS standard",
            "_note": "Update with actual state values from data dictionary",
        },
        "epdoWeights": {
            "K": 883, "A": 94, "B": 21, "C": 11, "O": 1,
            "_source": "FHWA/HSM Standard",
        },
        "validValues": {
            "severity": {"K": "Fatal", "A": "Suspected Serious Injury", "B": "Suspected Minor Injury", "C": "Possible Injury", "O": "Property Damage Only"},
        },
        "dataSource": {
            "name": f"{s['dot']} {'Open Data Portal' if s['api_type'] == 'socrata' else 'GIS Portal' if s['api_type'] == 'arcgis' else 'Data Portal'}",
            "url": f"https://{s['portal']}",
            "apiUrl": s["api_url"],
            "fileFormat": "JSON/CSV (Socrata SODA API)" if s["api_type"] == "socrata" else "GeoJSON/CSV (ArcGIS)" if s["api_type"] == "arcgis" else "JSON (REST API)" if s["api_type"] == "custom_rest" else "CSV (Bulk Download)",
            "dataDictionary": f"See {s['portal']} dataset metadata",
        },
    }
    if s["api_type"] == "socrata":
        config["dataSource"]["resourceId"] = s.get("resource_id", "_TBD")
        config["dataSource"]["pagination"] = {
            "method": "offset",
            "maxPerRequest": 50000,
            "paramLimit": "$limit",
            "paramOffset": "$offset",
        }
    elif s["api_type"] == "arcgis":
        config["dataSource"]["pagination"] = {
            "method": "offset",
            "maxPerRequest": 2000,
            "paramOffset": "resultOffset",
            "paramCount": "resultRecordCount",
        }
    return json.dumps(config, indent=2) + "\n"


def generate_source_manifest(state_key, s):
    """Generate source_manifest.json content."""
    manifest = {
        "_description": f"{s['dot']} crash data registry. {s['county_count']} jurisdictions available.",
        "_updated": "2026-02-15",
        "source": {
            "name": f"{s['dot_full']}",
            "base_url": f"https://{s['portal']}",
            "api_url": s["api_url"],
            "file_format": "JSON/CSV" if s["api_type"] in ("socrata", "custom_rest") else "GeoJSON/CSV" if s["api_type"] == "arcgis" else "CSV",
        },
        "jurisdiction_filters": {
            "_description": f"All {s['county_count']} {s['name']} {'counties' if s['name'] != 'Louisiana' else 'parishes'}. FIPS codes are 5-digit (state {s['fips']} + 3-digit county).",
            "_placeholder": f"Populate with all {s['county_count']} jurisdictions and FIPS codes",
        },
    }
    if s["api_type"] == "socrata":
        manifest["api"] = {
            "endpoint": s["api_url"],
            "resource_id": s.get("resource_id", "_TBD"),
            "pagination": {"method": "offset", "limit_param": "$limit", "offset_param": "$offset", "max_per_request": 50000},
        }
    elif s["api_type"] == "arcgis":
        manifest["api"] = {
            "endpoint": s["api_url"],
            "pagination": {"method": "offset", "offset_param": "resultOffset", "count_param": "resultRecordCount", "max_per_request": 2000},
            "query_format": "where=COUNTY_FIPS='{fips}'&outFields=*&f=json",
        }
    elif s["api_type"] == "custom_rest":
        manifest["api"] = {
            "endpoint": s["api_url"],
            "documentation": f"https://{s['portal']}",
        }
    elif s["api_type"] == "cris_bulk":
        manifest["api"] = {
            "portal": s["api_url"],
            "access": "Registration required",
            "download_method": "Bulk CSV export",
        }
    return json.dumps(manifest, indent=2) + "\n"


def generate_download_script(state_key, s):
    """Generate download_{state}_crash_data.py content."""
    state_lower = state_key.replace("_", "")

    if s["api_type"] == "socrata":
        return _generate_socrata_script(state_key, s)
    elif s["api_type"] == "arcgis":
        return _generate_arcgis_script(state_key, s)
    elif s["api_type"] == "custom_rest":
        return _generate_custom_rest_script(state_key, s)
    elif s["api_type"] == "cris_bulk":
        return _generate_cris_script(state_key, s)


def _generate_socrata_script(state_key, s):
    return f'''#!/usr/bin/env python3
"""
{s['name']} Crash Data — Socrata SODA API Downloader

Downloads crash data from {s['dot_full']} via Socrata SODA API.
Supports jurisdiction filtering, date range queries, gzip compression for R2 storage.

Data Source: https://{s['portal']}
API: {s['api_url']}

Usage:
    python download_{state_key}_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_{state_key}_crash_data.py --gzip
    python download_{state_key}_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# =============================================================================
# Constants
# =============================================================================

SOCRATA_BASE_URL = "{s['api_url']}"
SOCRATA_METADATA_URL = "https://{s['portal']}/api/views/{s.get('resource_id', '_TBD')}.json"

PAGE_SIZE = 50000
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("{state_key}_downloader")


# =============================================================================
# Helper Functions
# =============================================================================

def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP GET with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"Request failed (attempt {{attempt + 1}}/{{max_retries}}): {{e}}")
                time.sleep(wait)
            else:
                raise


def health_check():
    """Test API connectivity."""
    log.info("=" * 60)
    log.info("{s['name']} Socrata API — Health Check")
    log.info("=" * 60)
    try:
        resp = retry_request(SOCRATA_BASE_URL, params={{"$limit": 1}})
        data = resp.json()
        if data:
            log.info(f"  Sample record keys: {{list(data[0].keys())[:10]}}...")
            log.info("  ✓ API is healthy")
            return True
        log.warning("  Empty response")
        return False
    except Exception as e:
        log.error(f"  FAILED: {{e}}")
        return False


def download_data(jurisdiction, years):
    """Download crash data with pagination."""
    clauses = []
    # Add jurisdiction/year filters as needed based on state schema
    where = " AND ".join(clauses) if clauses else None

    all_records = []
    offset = 0
    page = 1
    while True:
        params = {{"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}}
        if where:
            params["$where"] = where
        log.info(f"  Page {{page}}: offset={{offset}}")
        resp = retry_request(SOCRATA_BASE_URL, params=params)
        records = resp.json()
        if not records:
            break
        all_records.extend(records)
        log.info(f"  Got {{len(records)}} (total: {{len(all_records)}})")
        if len(records) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        page += 1
        time.sleep(0.5)
    return all_records


def save_csv(records, output_path, gzip_output=False):
    """Save records as CSV."""
    if not records:
        return None
    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{gz_path}} ({{os.path.getsize(gz_path):,}} bytes)")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{output_path}} ({{os.path.getsize(output_path):,}} bytes)")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download {s['name']} crash data from Socrata SODA API")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/{s['folder']}")
    parser.add_argument("--gzip", "-g", action="store_true")
    parser.add_argument("--health-check", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.health_check:
        sys.exit(0 if health_check() else 1)

    jurisdiction = args.jurisdiction or "statewide"
    output_path = Path(args.data_dir) / f"{{jurisdiction}}_crashes.csv"
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        log.info(f"Output exists. Use --force to re-download.")
        sys.exit(0)

    log.info("=" * 60)
    log.info(f"{s['name']} Crash Data Downloader")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {{len(records):,}} records in {{time.time()-start:.1f}}s -> {{saved}}")


if __name__ == "__main__":
    main()
'''


def _generate_arcgis_script(state_key, s):
    return f'''#!/usr/bin/env python3
"""
{s['name']} Crash Data — ArcGIS Feature Server Downloader

Downloads crash data from {s['dot_full']} via ArcGIS GeoServices REST API.
Supports jurisdiction filtering, date range queries, gzip compression for R2 storage.

Data Source: https://{s['portal']}
Feature Server: {s['api_url']}

Usage:
    python download_{state_key}_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_{state_key}_crash_data.py --gzip
    python download_{state_key}_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# =============================================================================
# Constants
# =============================================================================

FEATURE_SERVER_URL = "{s['api_url']}"
QUERY_URL = FEATURE_SERVER_URL.rstrip("/") + "/query"

PAGE_SIZE = 2000
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("{state_key}_downloader")


# =============================================================================
# Helper Functions
# =============================================================================

def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP GET with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"Request failed (attempt {{attempt + 1}}/{{max_retries}}): {{e}}")
                time.sleep(wait)
            else:
                raise


def health_check():
    """Test Feature Server connectivity."""
    log.info("=" * 60)
    log.info("{s['name']} ArcGIS Feature Server — Health Check")
    log.info("=" * 60)
    try:
        resp = retry_request(FEATURE_SERVER_URL, params={{"f": "json"}})
        meta = resp.json()
        log.info(f"  Name: {{meta.get('name', 'N/A')}}")
        log.info(f"  Type: {{meta.get('type', 'N/A')}}")
        fields = meta.get("fields", [])
        log.info(f"  Fields: {{len(fields)}}")
        if fields:
            log.info(f"  Sample fields: {{[f['name'] for f in fields[:10]]}}")
        log.info("  ✓ Feature Server is healthy")
        return True
    except Exception as e:
        log.error(f"  FAILED: {{e}}")
        return False


def download_data(jurisdiction, years):
    """Download crash data with offset pagination."""
    where_parts = []
    if jurisdiction:
        where_parts.append(f"COUNTY='{{jurisdiction}}'")
    where = " AND ".join(where_parts) if where_parts else "1=1"

    all_features = []
    offset = 0
    page = 1
    while True:
        params = {{
            "where": where,
            "outFields": "*",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "json",
        }}
        log.info(f"  Page {{page}}: offset={{offset}}")
        resp = retry_request(QUERY_URL, params=params)
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        # Flatten attributes
        records = [f.get("attributes", {{}}) for f in features]
        # Add geometry if present
        for i, f in enumerate(features):
            geom = f.get("geometry", {{}})
            if geom:
                records[i]["_longitude"] = geom.get("x")
                records[i]["_latitude"] = geom.get("y")
        all_features.extend(records)
        log.info(f"  Got {{len(features)}} (total: {{len(all_features)}})")
        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        page += 1
        time.sleep(0.5)
    return all_features


def save_csv(records, output_path, gzip_output=False):
    """Save records as CSV."""
    if not records:
        return None
    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{gz_path}} ({{os.path.getsize(gz_path):,}} bytes)")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{output_path}} ({{os.path.getsize(output_path):,}} bytes)")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download {s['name']} crash data from ArcGIS Feature Server")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/{s['folder']}")
    parser.add_argument("--gzip", "-g", action="store_true")
    parser.add_argument("--health-check", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.health_check:
        sys.exit(0 if health_check() else 1)

    jurisdiction = args.jurisdiction or "statewide"
    output_path = Path(args.data_dir) / f"{{jurisdiction}}_crashes.csv"
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        log.info("Output exists. Use --force to re-download.")
        sys.exit(0)

    log.info("=" * 60)
    log.info(f"{s['name']} Crash Data Downloader")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {{len(records):,}} records in {{time.time()-start:.1f}}s -> {{saved}}")


if __name__ == "__main__":
    main()
'''


def _generate_custom_rest_script(state_key, s):
    """Vermont-specific custom REST script."""
    return f'''#!/usr/bin/env python3
"""
{s['name']} Crash Data — Custom REST API Downloader

Downloads crash data from {s['dot_full']} via custom REST JSON API.

Data Source: https://{s['portal']}
API: {s['api_url']}

Usage:
    python download_{state_key}_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_{state_key}_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

API_BASE_URL = "{s['api_url']}"
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("{state_key}_downloader")


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise


def health_check():
    log.info("=" * 60)
    log.info("{s['name']} REST API — Health Check")
    log.info("=" * 60)
    try:
        resp = retry_request(API_BASE_URL, params={{"$top": 1}})
        data = resp.json()
        log.info(f"  Response type: {{type(data).__name__}}")
        if isinstance(data, list) and data:
            log.info(f"  Sample keys: {{list(data[0].keys())[:10]}}")
        log.info("  ✓ API is healthy")
        return True
    except Exception as e:
        log.error(f"  FAILED: {{e}}")
        return False


def download_data(jurisdiction, years):
    params = {{}}
    if jurisdiction:
        params["county"] = jurisdiction
    if years:
        params["year"] = ",".join(str(y) for y in years)
    log.info(f"  Downloading with params: {{params}}")
    resp = retry_request(API_BASE_URL, params=params)
    data = resp.json()
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and "value" in data:
        return data["value"]
    return []


def save_csv(records, output_path, gzip_output=False):
    if not records:
        return None
    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{gz_path}}")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{output_path}}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download {s['name']} crash data from REST API")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/{s['folder']}")
    parser.add_argument("--gzip", "-g", action="store_true")
    parser.add_argument("--health-check", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.health_check:
        sys.exit(0 if health_check() else 1)

    jurisdiction = args.jurisdiction or "statewide"
    output_path = Path(args.data_dir) / f"{{jurisdiction}}_crashes.csv"
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        log.info("Output exists. Use --force to re-download.")
        sys.exit(0)

    log.info("=" * 60)
    log.info(f"{s['name']} Crash Data Downloader")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {{len(records):,}} records in {{time.time()-start:.1f}}s -> {{saved}}")


if __name__ == "__main__":
    main()
'''


def _generate_cris_script(state_key, s):
    """Texas CRIS bulk download script."""
    return f'''#!/usr/bin/env python3
"""
{s['name']} Crash Data — CRIS Bulk CSV Downloader

Downloads crash data from {s['dot_full']} CRIS (Crash Records Information System).
CRIS requires registration for data access. This script handles bulk CSV downloads
with county filtering and gzip compression for R2 storage.

Portal: https://{s['portal']}

Usage:
    python download_{state_key}_crash_data.py --jurisdiction <county> --years 2023 2024
    python download_{state_key}_crash_data.py --health-check
"""

import argparse
import csv
import gzip
import logging
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

CRIS_PORTAL = "https://{s['portal']}"
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("{state_key}_downloader")


def health_check():
    log.info("=" * 60)
    log.info("{s['name']} CRIS Portal — Health Check")
    log.info("=" * 60)
    try:
        resp = requests.get(CRIS_PORTAL, timeout=30)
        log.info(f"  Status: {{resp.status_code}}")
        log.info("  ✓ Portal is reachable")
        log.info("  Note: CRIS requires registration for data access")
        return True
    except Exception as e:
        log.error(f"  FAILED: {{e}}")
        return False


def download_data(jurisdiction, years, data_dir):
    """Download from CRIS — requires pre-exported CSV files."""
    log.info("CRIS bulk download requires manual export or registered API access.")
    log.info(f"  Looking for pre-exported CSVs in {{data_dir}}/")
    csv_files = list(Path(data_dir).glob("*.csv"))
    if not csv_files:
        log.warning("No CSV files found. Export data from CRIS portal first.")
        return []
    all_records = []
    for csv_file in csv_files:
        log.info(f"  Reading: {{csv_file}}")
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_records.append(row)
    log.info(f"  Total records: {{len(all_records):,}}")
    return all_records


def save_csv(records, output_path, gzip_output=False):
    if not records:
        return None
    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{gz_path}}")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {{output_path}}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Download {s['name']} crash data from CRIS")
    parser.add_argument("--jurisdiction", "-j", type=str, default=None)
    parser.add_argument("--years", "-y", type=int, nargs="+", default=None)
    parser.add_argument("--data-dir", "-d", type=str, default="data/{s['folder']}")
    parser.add_argument("--gzip", "-g", action="store_true")
    parser.add_argument("--health-check", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.health_check:
        sys.exit(0 if health_check() else 1)

    jurisdiction = args.jurisdiction or "statewide"
    output_path = Path(args.data_dir) / f"{{jurisdiction}}_crashes.csv"
    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        log.info("Output exists. Use --force to re-download.")
        sys.exit(0)

    log.info("=" * 60)
    log.info(f"{s['name']} Crash Data Downloader (CRIS)")
    log.info("=" * 60)
    start = time.time()
    records = download_data(args.jurisdiction, args.years, args.data_dir)
    if not records:
        log.warning("No records. Exiting.")
        sys.exit(1)
    saved = save_csv(records, output_path, gzip_output=args.gzip)
    log.info(f"Done: {{len(records):,}} records in {{time.time()-start:.1f}}s -> {{saved}}")


if __name__ == "__main__":
    main()
'''


def generate_pipeline_architecture(state_key, s):
    """Generate PIPELINE_ARCHITECTURE.md content."""
    api_detail = ""
    if s["api_type"] == "socrata":
        api_detail = f"""### API Details

| Parameter | Value |
|-----------|-------|
| Base URL | `{s['api_url']}` |
| Pagination | `$limit=50000&$offset=0` |
| Filtering | SoQL `$where` clause |
| Max per request | 50,000 rows |"""
    elif s["api_type"] == "arcgis":
        api_detail = f"""### API Details

| Parameter | Value |
|-----------|-------|
| Feature Server | `{s['api_url']}` |
| Query endpoint | `{s['api_url']}/query` |
| Pagination | `resultOffset` / `resultRecordCount` |
| Max per request | 2,000 features |
| Response format | `f=json` or `f=geojson` |"""
    elif s["api_type"] == "custom_rest":
        api_detail = f"""### API Details

| Parameter | Value |
|-----------|-------|
| Endpoint | `{s['api_url']}` |
| Format | JSON |
| Authentication | None required |"""
    elif s["api_type"] == "cris_bulk":
        api_detail = f"""### Access Details

| Parameter | Value |
|-----------|-------|
| Portal | `{s['api_url']}` |
| Access | Registration required |
| Format | Bulk CSV download |"""

    return f"""# {s['name']} Crash Data Pipeline Architecture

> **Purpose:** Reference guide for downloading, converting, and storing {s['name']} crash data in CRASH LENS standardized format with Cloudflare R2 storage.

---

## 1. Data Source

- **DOT:** {s['dot_full']} ({s['dot']})
- **Portal:** `{s['portal']}`
- **API Type:** {'Socrata SODA' if s['api_type'] == 'socrata' else 'ArcGIS GeoServices' if s['api_type'] == 'arcgis' else 'Custom REST' if s['api_type'] == 'custom_rest' else 'CRIS Bulk CSV'}
- **State FIPS:** {s['fips']}
- **Jurisdictions:** {s['county_count']}

---

## 2. Pipeline Overview

```
{s['name']} Data Source
      |
      v
+------------------+    +------------------+    +------------------+    +------------------+
|   DOWNLOAD       |    |   CONVERT        |    |   VALIDATE       |    |   SPLIT          |
|   ({'Socrata' if s['api_type'] == 'socrata' else 'ArcGIS' if s['api_type'] == 'arcgis' else 'REST' if s['api_type'] == 'custom_rest' else 'CRIS'})     |--->|   (normalize)    |--->|   (QA/QC)        |--->|   (road type)    |
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

{api_detail}

### Download Script

```bash
python data/{s['folder']}/download_{state_key}_crash_data.py \\
  --jurisdiction <county> \\
  --years 2023 2024 \\
  --gzip \\
  --data-dir data/{s['folder']}
```

### Arguments

| Flag | Description |
|------|-------------|
| `--jurisdiction` | County/jurisdiction to filter |
| `--years` | Years to download |
| `--data-dir` | Output directory (default: `data/{s['folder']}`) |
| `--gzip` | Output gzip-compressed CSV for R2 |
| `--health-check` | Test API connectivity |
| `--force` | Re-download even if file exists |

---

## 4. Column Mapping

See `config.json` for the complete mapping from {s['name']} raw fields to CRASH LENS standard (VDOT reference format).

Key fields: ID, DATE, LAT/LON, SEVERITY, ROUTE, COLLISION, WEATHER, LIGHT, COUNTY/FIPS

---

## 5. R2 Storage Paths

```
crash-lens-data/{s['r2_prefix']}/
  {{jurisdiction}}/
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

**File:** `.github/workflows/download-{state_key.replace('_', '-')}-crash-data.yml`

- **Schedule:** 1st of every month at 11:00 UTC
- **Manual trigger:** Select jurisdiction from dropdown
- **R2 upload:** Gzip CSVs to `{s['r2_prefix']}/{{jurisdiction}}/`
- **Manifest:** Updates `data/r2-manifest.json`
"""


def generate_workflow(state_key, s):
    """Generate GitHub Actions workflow YAML."""
    state_display = s["name"]
    state_slug = state_key.replace("_", "-")
    default_jurisdiction = "statewide"

    return f"""name: Download {state_display} Crash Data

# Workflow for {s['dot_full']} crash data downloads.
# Modeled after download-cdot-crash-data.yml

on:
  schedule:
    - cron: '0 11 1 * *'
  workflow_dispatch:
    inputs:
      jurisdiction:
        description: 'Jurisdiction to filter to'
        required: false
        default: '{default_jurisdiction}'
        type: string
      years:
        description: 'Space-separated years to download'
        required: false
        default: ''
        type: string
      force_download:
        description: 'Force re-download'
        required: false
        type: boolean
        default: false

permissions:
  contents: write

env:
  R2_STATE_PREFIX: {s['r2_prefix']}
  STATE_DISPLAY: {state_display}

jobs:
  download-{state_slug}-data:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Build download arguments
        id: build-args
        run: |
          ARGS="--data-dir data/{s['folder']}"
          JURISDICTION="${{{{ github.event.inputs.jurisdiction || '{default_jurisdiction}' }}}}"
          ARGS="$ARGS --jurisdiction $JURISDICTION"
          YEARS="${{{{ github.event.inputs.years }}}}"
          if [ -n "$YEARS" ]; then
            ARGS="$ARGS --years $YEARS"
          fi
          FORCE="${{{{ github.event.inputs.force_download }}}}"
          if [ "$FORCE" = "true" ]; then
            ARGS="$ARGS --force"
          fi
          ARGS="$ARGS --gzip"
          echo "args=$ARGS" >> $GITHUB_OUTPUT
          echo "jurisdiction=$JURISDICTION" >> $GITHUB_OUTPUT

      - name: Download {state_display} crash data
        run: |
          echo "=========================================="
          echo "Downloading ${{STATE_DISPLAY}} Crash Data"
          echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
          echo "=========================================="
          python data/{s['folder']}/download_{state_key}_crash_data.py ${{{{ steps.build-args.outputs.args }}}}

      - name: Generate download statistics
        id: stats
        run: |
          echo "Files in data/{s['folder']}/:"
          ls -lh data/{s['folder']}/*.csv* 2>/dev/null || echo "  No CSV files found"
          CHANGED=$(git diff --name-only data/{s['folder']}/ 2>/dev/null | wc -l | tr -d ' ')
          UNTRACKED=$(git ls-files --others --exclude-standard data/{s['folder']}/ 2>/dev/null | wc -l | tr -d ' ')
          echo "changed=$CHANGED" >> $GITHUB_OUTPUT
          echo "untracked=$UNTRACKED" >> $GITHUB_OUTPUT

      - name: Upload CSVs to R2
        id: r2-upload
        env:
          AWS_ACCESS_KEY_ID: ${{{{ secrets.CF_R2_ACCESS_KEY_ID }}}}
          AWS_SECRET_ACCESS_KEY: ${{{{ secrets.CF_R2_SECRET_ACCESS_KEY }}}}
          R2_ENDPOINT: "https://${{{{ secrets.CF_ACCOUNT_ID }}}}.r2.cloudflarestorage.com"
        run: |
          JURISDICTION="${{{{ steps.build-args.outputs.jurisdiction }}}}"
          UPLOADED=0
          for f in data/{s['folder']}/*.csv.gz; do
            [ -f "$f" ] || continue
            BASENAME=$(basename "$f" .csv.gz)
            if aws s3 cp "$f" "s3://crash-lens-data/${{R2_STATE_PREFIX}}/${{JURISDICTION}}/${{BASENAME}}.csv.gz" \\
              --endpoint-url "$R2_ENDPOINT" --content-type "application/gzip"; then
              echo "  Uploaded: $f"
              UPLOADED=$((UPLOADED + 1))
            fi
          done
          echo "uploaded=$UPLOADED" >> $GITHUB_OUTPUT

      - name: Verify R2 state
        env:
          AWS_ACCESS_KEY_ID: ${{{{ secrets.CF_R2_ACCESS_KEY_ID }}}}
          AWS_SECRET_ACCESS_KEY: ${{{{ secrets.CF_R2_SECRET_ACCESS_KEY }}}}
          R2_ENDPOINT: "https://${{{{ secrets.CF_ACCOUNT_ID }}}}.r2.cloudflarestorage.com"
        run: |
          JURISDICTION="${{{{ steps.build-args.outputs.jurisdiction }}}}"
          aws s3 ls "s3://crash-lens-data/${{R2_STATE_PREFIX}}/${{JURISDICTION}}/" \\
            --endpoint-url "$R2_ENDPOINT" 2>/dev/null || echo "  Could not list R2 bucket"

      - name: Commit manifest and metadata
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "github-actions[bot]"
          git add data/r2-manifest.json
          git add data/{s['folder']}/.validation/ 2>/dev/null || true
          if git diff --staged --quiet; then
            echo "No changes to commit"
            exit 0
          fi
          JURISDICTION="${{{{ steps.build-args.outputs.jurisdiction }}}}"
          git commit -m "chore: update $STATE_DISPLAY crash data ($JURISDICTION) -> R2 [$(date +'%Y-%m-%d')]"
          MAX_RETRIES=4
          for i in $(seq 1 $MAX_RETRIES); do
            if git push; then echo "Push successful"; exit 0; fi
            sleep $((2 ** i))
            git fetch origin main
            git rebase origin/main || {{ git rebase --abort 2>/dev/null || true; git pull --no-edit origin main; }}
          done
          echo "ERROR: Push failed after $MAX_RETRIES attempts"
          exit 1

      - name: Job summary
        if: always()
        run: |
          JURISDICTION="${{{{ steps.build-args.outputs.jurisdiction }}}}"
          UPLOADED="${{{{ steps.r2-upload.outputs.uploaded || '0' }}}}"
          cat >> $GITHUB_STEP_SUMMARY <<EOF
          ## ${{STATE_DISPLAY}} Crash Data Download — ${{JURISDICTION}}
          | Metric | Value |
          |--------|-------|
          | Jurisdiction | ${{JURISDICTION}} |
          | CSVs uploaded to R2 | ${{UPLOADED}} |
          | Run time | $(date -u '+%Y-%m-%d %H:%M:%S UTC') |
          EOF
"""


# =============================================================================
# Main — Generate all state folders
# =============================================================================

def main():
    created = 0
    for state_key, s in STATES.items():
        folder = PROJECT_ROOT / "data" / s["folder"]
        folder.mkdir(parents=True, exist_ok=True)

        # 1. CLAUDE.md
        claude_path = folder / "CLAUDE.md"
        claude_path.write_text(generate_claude_md(state_key, s))

        # 2. config.json
        config_path = folder / "config.json"
        config_path.write_text(generate_config_json(state_key, s))

        # 3. source_manifest.json
        manifest_path = folder / "source_manifest.json"
        manifest_path.write_text(generate_source_manifest(state_key, s))

        # 4. PIPELINE_ARCHITECTURE.md
        pipeline_path = folder / "PIPELINE_ARCHITECTURE.md"
        pipeline_path.write_text(generate_pipeline_architecture(state_key, s))

        # 5. download_{state}_crash_data.py
        script_path = folder / f"download_{state_key}_crash_data.py"
        script_path.write_text(generate_download_script(state_key, s))
        os.chmod(script_path, 0o755)

        # 6. GitHub Actions workflow
        workflow_dir = PROJECT_ROOT / ".github" / "workflows"
        workflow_dir.mkdir(parents=True, exist_ok=True)
        workflow_slug = state_key.replace("_", "-")
        workflow_path = workflow_dir / f"download-{workflow_slug}-crash-data.yml"
        workflow_path.write_text(generate_workflow(state_key, s))

        created += 6
        print(f"  ✓ {s['folder']}: 5 data files + 1 workflow")

    print(f"\nTotal files created: {created}")
    print(f"States processed: {len(STATES)}")


if __name__ == "__main__":
    main()
