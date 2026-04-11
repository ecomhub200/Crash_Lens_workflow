---
type: concept
tags: [schema, data-dictionary, template, state-agnostic]
created: 2026-04-11
status: active
---

# State Data Dictionary Template

Template for documenting each state's crash data. Copy this file as `wiki/entities/{state}-data-dictionary.md` when onboarding a new state.

## State Info
| Field | Value |
|-------|-------|
| State | {Full Name} |
| Abbreviation | {xx} |
| FIPS | {##} |
| Source Portal | {URL} |
| Source Format | Socrata / ArcGIS / CSV |
| Year Range | {YYYY}–{YYYY} |
| Total Rows | {N} |
| Road Inventory Match | {N}% |

## Data Source Characteristics
| Characteristic | Value |
|----------------|-------|
| Download method | Socrata SODA2 / ArcGIS FeatureServer / CSV |
| Update frequency | Monthly / Quarterly / Annual |
| Severity system | KABCO / KAB / KA+O |
| B/C people available | Yes / No |
| Pedestrian detail | Flag only / Killed+Injured counts |
| Vehicle count available | Yes / No |
| GPS coordinates | Yes (100%) / Partial / No |
| Unique ID field | {source column name} |

## Column Mapping (source → golden 69)
| Golden Column | Source Column | Transform | Notes |
|---------------|--------------|-----------|-------|
| Document Nbr | {source} | direct | |
| Crash Year | {source} | strip commas (Socrata) | |
| Crash Date | {source} | parse to M/D/YYYY | |
| Crash Severity | {source} | map to K/A/B/C/O | |
| ... | ... | ... | |

## Value Mappings (state-specific)
| Column | Source Value | Standard Value |
|--------|-------------|----------------|
| Weather | {state value} | 1. Clear |
| Unrestrained | Unbelted | Yes |
| School Zone | 1. Yes | Yes |
| ... | ... | ... |

## Not-Tracked Fields
Fields the state does NOT collect (leave NULL, never default to "No"):
- {field1}: {reason}
- {field2}: {reason}

## State Extras ({abbr}_* columns)
| Column | Fill Rate | Description |
|--------|-----------|-------------|
| {abbr}_{field1} | {N}% | {description} |
| {abbr}_{field2} | {N}% | {description} |

## Fill Rate Summary (after enrichment)
| Column | Fill % | Notes |
|--------|--------|-------|
| Functional Class | | |
| Ownership | | |
| AADT | | |
| RTE Name | | |
| Intersection Name | | |
| Through Lanes | | |
| Traffic Control Type | | |

## Known Data Issues
| Issue | Severity | Details |
|-------|----------|---------|
| {issue1} | CRITICAL/MEDIUM/LOW | {details} |

## Pipeline Notes
- Normalizer: `{abbr}_normalize.py`
- Hierarchy: `states/{state}/hierarchy.json`
- R2 prefix: `{state_name}/`
- Partition: `crashes_{state_name}`
- Special handling: {any state-specific quirks}

## Related Pages
- [[pipeline-architecture-v29]]
- [[schema-truth-document]]
- [[delaware-data-dictionary]]
