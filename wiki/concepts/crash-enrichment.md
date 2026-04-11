---
title: Crash Enrichment
type: concept
tags: [enrichment, gps, osm, hpms, road-data]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Crash Enrichment

**GPS-based enrichment** adds road network attributes to crash records. Implemented in `crash_enricher.py` within [[crash-lens-workflow]].

## What It Does

Each crash record has GPS coordinates. The enricher matches these to the nearest road segment and adds:
- Road classification (functional class, urban/rural)
- Speed limit, lane count, surface type
- Intersection proximity
- Road curvature and grade
- Traffic volume estimates (AADT)

## Data Sources Used

| Source | Data Provided |
|--------|--------------|
| OpenStreetMap (OSMnx) | Road network geometry, names, classifications |
| HPMS | Federal highway performance metrics, traffic counts |
| Federal infrastructure data | Bridge locations, tunnel data |
| Census TIGERweb | Jurisdiction boundaries |
| State DOT road inventories | State-specific road attributes |

## Processing Approach

1. Build spatial index of road network segments (DuckDB spatial grid)
2. For each crash, find nearest road segment within threshold
3. Transfer road attributes to crash record
4. Flag low-confidence matches for review

The enricher is **universal** — it works across all states using the same spatial matching logic, with road data sources varying by availability.

## Why It Matters

Enriched crash data enables:
- [[hotspot-analysis]] — Identify dangerous road segments, not just crash clusters
- [[safety-countermeasures]] — Match road characteristics to applicable CMFs
- [[before-after-studies]] — Compare road conditions pre/post improvement

## Related Pages

- [[data-pipeline-architecture]] — Where enrichment fits in the pipeline
- [[data-sources-inventory]] — Full list of data sources
- [[crash-lens-workflow]] — The repo containing `crash_enricher.py`
