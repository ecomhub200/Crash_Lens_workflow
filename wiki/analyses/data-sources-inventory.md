---
title: Data Sources Inventory
type: analysis
tags: [data-sources, apis, external-data]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Data Sources Inventory

All external data sources integrated into the Crash Lens platform.

## Crash Data Sources (Per State)

| Source | States | API Type |
|--------|--------|----------|
| Virginia Roads | Virginia | ArcGIS REST API |
| CDOT OnBase | Colorado | Document management system |
| Socrata SODA | Maryland, others | Open data API |
| State DOT portals | Various | Web scraping (Playwright), direct download |

## Road Network Sources

| Source | Data Provided | Script |
|--------|--------------|--------|
| OpenStreetMap (OSMnx) | Road geometry, names, classifications | `generate_osm_data.py` |
| HPMS (FHWA) | Traffic counts, functional class, pavement | `generate_hpms_data.py` |
| Census TIGERweb | Jurisdiction boundaries, FIPS codes | `boundary_resolver.py` |
| State DOT road inventories | State-specific road attributes | Various |

## Federal Data Sources

| Source | Data Provided | Script |
|--------|--------------|--------|
| FHWA infrastructure data | Bridges, tunnels, federal roads | `generate_federal_data.py` |
| Grants.gov | Federal safety funding opportunities | Grants module |

## Imagery & Visualization

| Source | Data Provided | Script |
|--------|--------------|--------|
| Mapillary | Street-level imagery metadata | `generate_mapillary_cache.py` |
| Overture Maps (PMTiles) | Base map vector tiles | Frontend integration |

## Safety Knowledge

| Source | Data Provided | Location |
|--------|--------------|----------|
| FHWA CMF Clearinghouse | Crash Modification Factors | Frontend CMF database |
| MUTCD | Signal warrant criteria | Frontend warrants module |

## How Sources Are Used

1. **Crash data** → [[data-pipeline-architecture|Pipeline]] → normalized records
2. **Road network** → [[crash-enrichment|Enricher]] → attributes added to crash records
3. **Boundaries** → Jurisdiction splitting → per-county/city datasets
4. **Safety knowledge** → [[safety-countermeasures|CMF matching]] → treatment recommendations

## Related Pages

- [[crash-enrichment]] — How road data enriches crash records
- [[data-pipeline-architecture]] — Pipeline that processes all sources
- [[state-onboarding]] — Adding new state data sources
