---
type: concept
tags: [pipeline, architecture, diagram]
created: 2026-04-11
status: active
---

# Pipeline Architecture v2.9

![[Full_pipeline.png]]

Simplified (state-agnostic):
![[For_any_state.png]]

## Phases

### Phase 0: Cache Generation (run once per state, monthly refresh)
6 generators, all output to R2 `{state_prefix}/cache/`:
- `generate_osm_data.py` → roads, intersections, POIs (13 categories)
- `generate_hpms_data.py` → HPMS federal road data (46 cols, 75K segments for DE)
- `generate_state_dot_data.py` → State DOT shapefiles (speed, lanes, surface)
- `generate_federal_data.py` → Bridges (NBI), rail crossings (FRA), schools, transit
- `generate_boundaries.py` → Census TIGER 2020 (Urban/Suburban/Rural)
- `mapillary_county_download.py` → Traffic signs/signals (Mapillary API v4)

### Phase 1: Build Road Inventory
`build_road_inventory.py` merges all caches into single spatial join.
4-tier authority: HPMS > State > OSM > Federal.
Output: `{abbr}_road_inventory.parquet.gz` (~22MB for DE, 151K segments × 394 cols)

## Intersection Classification (MIRE Element 121)

The pipeline uses two metrics for intersection classification on road segments:

- **`intersection_degree`**: Directed MultiDiGraph degree from osmnx. **Each two-way road contributes 2 directed edges** per node, so a 3-road T-intersection has degree 6 (not 3). Present in all historical caches.
- **`streets_per_node`** (Phase 2, added 2026-04-12): Undirected physical street count from `osmnx.stats.streets_per_node(G)`. **FHWA MIRE-correct** — one physical road counts as 1 regardless of directionality. Present only in caches regenerated on or after 2026-04-12.

### `Intersection Type` mapping

Downstream classifiers prefer `streets_per_node` when present; otherwise fall back to directed-degree thresholds.

```
streets_per_node (preferred)       | directed degree (fallback)
  spn >= 5  → "5. Five-Point, or More"    deg >= 10
  spn == 4  → "4. Four Approaches"        deg >= 8  (and < 10)
  spn == 3  → "3. Three Approaches"       deg >= 6  (and < 8)
  spn <= 2  → "1. Not at Intersection"    deg <  6
```

Degree 3–5 represents ramp merges / continuations / bends — not real intersections.

### Classification locations

Four places in the codebase assign `Intersection Type`. All use the same mapping above:

1. **`road_data_authority.merge_frontend_columns()`** (`road_data_authority.py:1325`) — first-pass on road segments during `build_road_inventory.py`. Overridden by step 2.
2. **`road_inventory_postprocess.fix_intersection_type()`** (`road_inventory_postprocess.py:384`, FIX 4) — final authoritative value for road segments. `DEGREE_TO_INTERSECTION` constant at `road_inventory_postprocess.py:82`. `DEGREE_FIVE_PLUS_THRESHOLD = 10`.
3. **`RoadInventorySession.enrich()`** (`road_inventory_enricher.py:271`) — per-crash assignment. Requires matched segment's node within 30m (squared distance ≤ 900).
4. **`crash_enricher.py` fallback default** (line 989) — any remaining empty values → "1. Not at Intersection".

`crash_enricher.py` prints an `Intersection Type distribution` table after enrichment for diagnostic monitoring. Expected for Delaware: ~35% "Not at Intersection" under Phase 1, ~45–50% under Phase 2.

### `is_intersection` flag

The `is_intersection == "Yes"` flag in `build_road_inventory.enrich_intersections()` is **kept permissive** (any graph junction node with endpoint in `int_set`). It is intentionally NOT tightened to `deg >= 6` because downstream consumers rely on the permissive semantics:
- `crash_enricher.py:966` (Node derivation)
- `build_road_inventory.py:1269` (intersection-name lookup)
- `road_inventory_validator.py:349`
- `road_inventory_postprocess.fix_intersection_type()` (gate)
- Supabase `is_intersection` column

The `Intersection Type` classification is the single source of truth for whether a segment/crash is at a real intersection.

### Phase 2: Download + Normalize (batch-all-jurisdictions.yml)
State-specific normalizer (`{abbr}_normalize.py`):
- Phase 1: Column rename to 69 golden
- Phase 2: Value transforms (must match VDOT frontend standard)
- Phase 3: FIPS, GPS, EPDO, Rankings
Output: `_statewide/statewide_all_roads.parquet.gz` (167 cols, no road inventory)

### Phase 3: Batch Pipeline (delaware-batch-pipeline.yml)
- Stage 0.5: Download road inventory from R2 cache
- Stage 0.5: `crash_enricher.py` 4-tier enrichment (167 → 517 cols)
- Stage 0.5b: Re-upload enriched to R2 `_statewide/`
- Stages 1-3: `split.py` (6 tiers × 2 road type sets = 368 files for DE)
- Stage 4: Upload splits to R2 (SNAPPY compression, NOT GZIP)
- Stage 4.5: Trigger Supabase sync via VPS webhook (NEW in v2.9)

### Phase 4: Supabase Sync (VPS webhook — replaces SSH tunnel batching)
- GitHub Actions POSTs to `https://srv1503081.hstgr.cloud/api/sync`
- VPS webhook downloads enriched parquet from R2
- Batched sync: 23 batches × 25K rows (avoids OOM on 8GB VPS)
- Finalize: geom, crash_date_parsed, matviews, states table
- 3-tier column strategy: 111 explicit + road_data JSONB + state_extras JSONB + ranking_data JSONB

**Why batched:** VPS has 8GB RAM. Full sync loads 570K × 521 cols at once → OOM killed. Each batch subprocess loads 25K rows (~1.8GB peak), exits, memory freed before next batch.

**Infrastructure:**
- `webhook.py` (Flask) on `localhost:8765`
- Caddy reverse proxy: `/api/sync*` → `172.18.0.1:8765`
- systemd: `crashlens-webhook.service`
- GitHub secret: `SYNC_WEBHOOK_TOKEN`
- Lock file: `/tmp/crashlens_sync.lock` (prevents concurrent syncs)
- Logs: `/root/crashlens-webhook/logs/{state}_{timestamp}.log`
- Status: `GET /api/sync/status?state=de`

**Monitoring:**
```bash
# Check if sync is running
curl https://srv1503081.hstgr.cloud/api/sync/status?state=de

# Watch live logs
tail -f /root/crashlens-webhook/logs/de_*.log

# Manual trigger
curl -X POST https://srv1503081.hstgr.cloud/api/sync \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"state": "de"}'
```

## New State Checklist (13 steps)
1. Add to `states_registry.py` (abbr, name, FIPS)
2. Create `states/{state}/hierarchy.json`
3. Run `generate_osm_data.py --state {abbr}`
4. Run `generate_hpms_data.py --state {abbr}`
5. Run `generate_state_dot_data.py --state {abbr}`
6. Run `generate_federal_data.py --state {abbr}`
7. Run `generate_boundaries.py --state {abbr}`
8. Run `build_road_inventory.py --state {abbr} --upload`
9. Create `{abbr}_normalize.py` from template
10. Add to `download-registry.json`
11. Run `batch-all-jurisdictions.yml`
12. Run `batch-pipeline.yml`
13. Supabase sync auto-runs (partition auto-created)

## Related Pages
- [[supabase-sync-ci]]
- [[webhook-sync]]
- [[data-pipeline-architecture]]
- [[schema-truth-document]]
