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
