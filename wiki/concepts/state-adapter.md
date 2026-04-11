---
title: State Adapter — Detection & Normalization
type: concept
tags: [frontend, state-adapter, normalization, colorado, virginia]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# State Adapter

## Purpose
Auto-detects which state's crash data format is loaded by fingerprinting CSV headers, then normalizes to the internal Virginia-compatible column format.

## Detection Logic
1. Check for pre-normalized data (`_co_*` columns → skip JS normalization)
2. Match required columns against STATE_SIGNATURES
3. Partial match scoring (>50% threshold)
4. Default: Virginia format (pass-through)

## State Signatures

### Colorado (CDOT)
Required: `['CUID', 'System Code', 'Injury 00', 'Injury 04']`

### Virginia (TREDS)
Required: `['Document Nbr', 'Crash Severity', 'RTE Name', 'SYSTEM']`

## Colorado → Virginia Normalization (key transforms)

| Colorado Column | Virginia Column (COL) | Transform |
|---|---|---|
| Injury 04 count | K_People / Crash Severity=K | Highest injury = severity |
| Injury 03 | A_People / Crash Severity=A | |
| Injury 02 | B_People / Crash Severity=B | |
| Injury 01 | C_People / Crash Severity=C | |
| Injury 00 | Crash Severity=O | |
| System Code | SYSTEM | City Street→Non-DOT, Interstate→Interstate, State Highway→Primary |
| Location 1 & 2 | Node | "{Road1} & {Road2}" alphabetically sorted |
| Rd_Number | RTE Name | Interstate: I-{num}, State: CO-{num} |
| Crash Type + MHE | Collision Type | 40+ collision type mappings |
| NM Type | Pedestrian? / Bike? | Pattern match for pedestrian/bicycle keywords |
| TU-1/TU-2 Alcohol Suspected | Alcohol? | 'Yes - SFST', 'Yes - BAC', etc. → 'Yes' |
| Lighting Conditions | Night? | 'Dark – Lighted', 'Dark – Unlighted' → 'Yes' |
| TU-1/TU-2 Age | Senior? / Young? | ≥65 → Senior; 16-20 → Young |

## Colorado Provenance Fields (preserved as _co_*)
_co_system_code, _co_rd_number, _co_location1, _co_location2, _co_link, _co_crash_type, _co_mhe, _co_agency, _co_city, _co_tu1_speed_limit, _co_tu1_estimated_speed, _co_tu1_driver_action, _co_tu1_human_factor

## Filter Profiles (Road Type)
| Profile | SYSTEM Values |
|---------|--------------|
| countyOnly | Non-DOT secondary |
| allRoads | Non-DOT secondary, Primary, Secondary, Interstate |
| countyPlusVDOT | Non-DOT secondary, Primary, Secondary |

## Dynamic State Selection
`setStateByFips(fips)` builds config dynamically using FIPSDatabase:
- Loads state info (name, abbr, dotName)
- Fetches counties from TIGERweb
- Builds geoConfig with bounding boxes

## Related Pages
- [[data-loader]] — Calls StateAdapter during loading
- [[upload-pipeline]] — Uses StateAdapter in upload flow
- [[frontend-data-inventory]] — Column mapping
