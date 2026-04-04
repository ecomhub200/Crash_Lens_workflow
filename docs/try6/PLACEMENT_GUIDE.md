## CrashLens Pipeline Update — File Placement Guide

Place these 6 files from `docs/try6/` into the repo at the exact paths below.

### File Placement

```
docs/try6/de_normalize.py                      → states/delaware/de_normalize.py
docs/try6/geo_resolver.py                      → geo_resolver.py  (repo root)
docs/try6/crash_enricher.py                    → crash_enricher.py  (repo root)
docs/try6/split.py                             → split.py  (repo root)
docs/try6/delaware-batch-all-jurisdictions.yml → .github/workflows/delaware-batch-all-jurisdictions.yml
docs/try6/delaware-batch-pipeline.yml          → .github/workflows/delaware-batch-pipeline.yml
```

### I/O Format Rule

- **Write**: always parquet.gz
- **Read**: auto-detect from extension (parquet.gz or CSV — never fails)
- **R2 storage**: always parquet.gz
- **YAMLs**: prefer parquet.gz, auto-fallback to CSV

### Bugs Fixed (8 total)

| # | File | Bug | Impact |
|---|------|-----|--------|
| 1 | de_normalize.py | --skip-enrichment skipped ALL enrichment | Flags empty |
| 2 | de_normalize.py | Crash Year .0 decimal | "2012.0" in output |
| 3 | de_normalize.py | merge() float reversion | K_People ".0" |
| 4 | geo_resolver.py | _load_hierarchy() never read tprs | MPO Name always empty |
| 5 | geo_resolver.py | resolve_mpo() no hierarchy fallback | BTS names didn't match |
| 6 | YAML #2 | STATE[:2] for abbreviation | Breaks new_hampshire → "ne" |
| 7 | de_normalize.py | Duplicate columns (Roadway Alignment/Description) | **parquet write crashes** |
| 8 | crash_enricher.py | road_inventory_enricher import not in try/except | **Pipeline crashes** |

### Test Results (3,546 Delaware crashes)

| Check | Result |
|-------|--------|
| Stage 2: CSV → parquet.gz | ✅ 3,546 × 115 cols |
| Stage 3: parquet.gz → parquet.gz | ✅ 3,546 × 115 cols |
| Stage 4: parquet.gz → 52 splits | ✅ 52 parquet.gz files |
| Duplicate columns | ✅ NONE |
| Crash Year .0 | ✅ 0 |
| K_People .0 | ✅ 0 |
| Tier 1 flags | ✅ 100% filled |
| K→K_People | ✅ 17/17 |
| FC values | ✅ 0 invalid |
| MPO Names | ✅ 3 correct |
| Edge: parquet→parquet rerank | ✅ |
| Edge: CSV→parquet enricher | ✅ |
| Edge: CSV→split | ✅ |

### Suggested Commit Message

```
feat(pipeline): parquet.gz native I/O, enrichment stage, 8 bug fixes

- All stages: auto-detect input (parquet.gz or CSV), write parquet.gz
- YAML #1: normalize-only, direct parquet.gz output + upload
- YAML #2: added Stage 0.5 enrichment (crash_enricher + road_inventory_enricher)
- de_normalize.py: 4 fixes (Tier1 skip, Year .0, float cleanup, dup cols)
- geo_resolver.py: 2 fixes (tprs loading, BTS MPO normalization)
- crash_enricher.py: CLI parquet I/O + ImportError resilience
- split.py: auto-detect parquet/CSV input
- State resolution via states_registry.py (all 51 states)
```
