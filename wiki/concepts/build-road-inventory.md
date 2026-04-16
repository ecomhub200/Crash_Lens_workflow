---
title: Build Road Inventory
type: concept
tags: [road-inventory, pipeline, spatial, hpms, osm, mapillary, federal-data]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo]
---

# Build Road Inventory

**`build_road_inventory.py`** consolidates all cache files into a single statewide road database by spatially joining multiple data sources to OSM road segments.

## Purpose

Creates a unified road-level dataset where every road segment has attributes from HPMS, OSM, Mapillary, federal bridge/rail/transit data, and school proximity — all linked by GPS coordinates. This is the road-side complement to the crash-side enrichment done by [[crash-enrichment|crash_enricher.py]].

## Architecture

**Base layer**: OSM road segments (mid_lat, mid_lon coordinates)

**Spatial join method**: STRtree on LineString geometry (accurate point-to-road matching), with KDTree midpoint fallback when Shapely is unavailable.

**Output**: `{state}/cache/{abbr}_road_inventory.parquet.gz`

For Delaware: 151,270 segments × 261 columns.

## Input Files (10 sources)

All read from `{state}/cache/`:

| File | Source | Generator |
|------|--------|-----------|
| `{abbr}_roads.parquet.gz` | OSM road network | `generate_osm_data.py` |
| `{abbr}_intersections.parquet.gz` | OSM intersection nodes | `generate_osm_data.py` |
| `{abbr}_hpms.parquet.gz` | FHWA road inventory (46 cols) | `generate_hpms_data.py` |
| `{abbr}_pois.parquet.gz` | OSM POIs (13 categories) | `generate_osm_data.py` |
| `{abbr}_bridges.parquet.gz` | NBI bridges | `generate_federal_data.py` |
| `{abbr}_rail_crossings.parquet.gz` | FRA rail crossings | `generate_federal_data.py` |
| `{abbr}_schools.parquet.gz` | Urban Institute schools | `generate_federal_data.py` |
| `{abbr}_transit.parquet.gz` | NTM transit stops | `generate_federal_data.py` |
| `{abbr}_mapillary.parquet.gz` | Mapillary traffic inventory | `generate_mapillary_cache.py` |
| `{abbr}_special_data.parquet.gz` | State DOT road attributes | `generate_state_dot_data.py` |

## Proximity Thresholds

| Asset | Threshold |
|-------|-----------|
| Schools | 1000 ft |
| Transit stops | 500 ft |
| Bridges | 500 ft |
| Rail crossings | 500 ft |
| POI bars | 1500 ft |
| POI colleges | 1500 ft |
| POI hospitals | 1000 ft |
| POI clinics | 1000 ft |
| POI rest areas | 1000 ft |
| POI restaurants | 500 ft |
| POI parking/fuel | 500 ft |
| POI signals/stop signs/crossings | 100 ft |
| Mapillary speed signs | 500 ft |
| Mapillary signals | 500 ft |
| Mapillary general signs | 100 ft |

**Honesty principle.** `enrich_nearest_asset` populates the `nearest_{asset}_*`
attribute columns (name, lat, lon, type, etc.) **only for roads within the
threshold**. Beyond threshold, every attribute is `""` (strings) or `0`
(numerics), and the `Near_{Asset}_{N}ft` flag is `"No"`. The `dist_ft` column
is always set regardless of threshold — it's the only attribute that carries
meaning for unmatched roads. This preserves the principle that "NULL is
honest; 'No' when we don't know is a lie."

Asset enrichment indexes assets (not roads) in an STRtree and queries from
each road's geometry — this guarantees every road segment receives a nearest-
asset distance (fill ≈ 100% of roads computed, ≈ 11% of roads with
`Near_School_1000ft = Yes` in Delaware). The pre-v2.7.2 path had inverted
direction and produced ~0.4% Yes.

## Data Authority & Resolution

The script implements a multi-source resolution system — when multiple sources provide the same attribute (e.g., speed limit from HPMS, Mapillary, and OSM), a priority hierarchy selects the best value and records the source in a `resolved_*_source` column.

**Source contribution matrix** (tracked per attribute):

| Attribute | Sources (by priority) |
|-----------|----------------------|
| Speed limit | HPMS → Mapillary → OSM |
| Lanes | HPMS → OSM |
| Surface type | HPMS → OSM |
| Signals | HPMS → Mapillary → POI |
| Street lighting | Mapillary → OSM |
| On bridge | OSM → Federal (NBI) |
| School zone | Mapillary → Federal (Urban Institute) |
| Functional class | HPMS → OSM |
| Ownership | HPMS → OSM |

## Output Column Groups

The 261 output columns are organized into groups:

- **Frontend (VDOT standard)**: Columns matching the CrashLens frontend schema
- **Base (OSM)**: Road geometry, name, highway tag, curvature
- **Geography**: County, state, urban/rural
- **Resolved (authority)**: Best-value attributes with source tracking
- **Confidence/Cross-validation**: Match quality indicators
- **Risk indicators**: Crash-relevant road features
- **Curve analysis**: Angular deflection, curvature class
- **Traffic engineering**: Speed, lanes, signals, access control
- **Intersection**: Type, control, signal warrants
- **Ramp**: On/off ramp identification
- **HPMS**: All 46 federal highway columns
- **Bridges**: NBI bridge attributes
- **Rail crossings**: FRA crossing data
- **Schools**: Urban Institute school proximity
- **Transit**: NTM transit stop data
- **POI categories**: 13 OSM POI proximity flags
- **Mapillary**: Traffic sign/infrastructure detection

## Postprocessing

`road_inventory_postprocess.py` runs after build to fix three data quality issues:
- FC 1-6 AADT must be 100% filled (no zeros on classified roads)
- Interstate speed minimum 45 mph enforced
- Through_Lanes 100% filled for classified roads

## Split.py Compatibility

The output includes a validation check ensuring all `split.py` road type filters will produce data:
- `dot_roads`: Ownership = "1. State Hwy Agency"
- `county_roads`: Ownership = "2. County Hwy Agency"
- `city_roads`: Ownership = "3. City or Town Hwy Agency"
- `primary_roads`: FC starts with "1-" or "2-"
- `no_interstate`: FC does NOT start with "1-" or "2-"

## Usage

```bash
python build_road_inventory.py --state de
python build_road_inventory.py --state de --upload    # → R2
python build_road_inventory.py --state va --cache-dir cache --local-only
```

## Related Pages

- [[crash-enrichment]] — Crash-side enrichment using similar data sources
- [[data-pipeline-architecture]] — Where road inventory fits in the pipeline
- [[delaware-pipeline]] — Reference implementation results
- [[data-sources-inventory]] — All data sources integrated


## Intersection Name Derivation (Fix 3 — 2026-04-08)

**Problem:** `Intersection Name` was 0% filled in crash data. The road inventory had `is_intersection` and `intersection_degree` but no derived intersection names.

**Solution:** After loading roads cache (has `name`, `u_node`, `v_node`) and intersections cache (has `node_id`, `degree`):

1. Build `node_road_names` mapping: for each road segment, add its `name` to sets for both `u_node` and `v_node`
2. For each road segment with `is_intersection=Yes`, look up the node with the most connecting roads
3. Sort road names alphabetically → `intersection_name = "Road1 & Road2"`

**Expected fill rate:** ~30-50% of segments (only intersection segments with 2+ named connecting roads).

**Propagation:** When `crash_enricher.py` does the spatial join against road_inventory, `intersection_name` transfers to crashes automatically. No extra cache downloads needed in the batch pipeline.

**Data flow:**
```
OSM roads (name, u_node, v_node)
  + OSM intersections (node_id, degree)
  → build_road_inventory.py derives intersection_name
  → road_inventory.parquet.gz uploaded to R2
  → crash_enricher.py spatial join transfers to crashes
  → split.py outputs include intersection_name
  → supabase_sync.py pushes to crashes.intersection_name
```
