# CrashLens Pipeline Update — Claude Code Instructions

## Read this file first, then place all 6 files at the exact paths below.

### File Placement (6 files)

```
deploy/de_normalize.py                      → states/delaware/de_normalize.py
deploy/geo_resolver.py                      → geo_resolver.py  (repo root)
deploy/crash_enricher.py                    → crash_enricher.py  (repo root)
deploy/split.py                             → scripts/split.py  ← CRITICAL: scripts/ not root
deploy/delaware-batch-all-jurisdictions.yml → .github/workflows/delaware-batch-all-jurisdictions.yml
deploy/delaware-batch-pipeline.yml          → .github/workflows/delaware-batch-pipeline.yml
```

### Important Notes

1. `split.py` goes to `scripts/split.py` (NOT repo root) — the YAML calls `scripts/split.py`
2. `ranking_engine.py` is NOT needed — ranking is now embedded inside `de_normalize.py`
3. If a `ranking_engine.py` exists at repo root, leave it (won't be imported)

### Commit Message

```
feat(pipeline): 76 ranking columns (embedded), parquet.gz native I/O, 10 bug fixes

Ranking v2 embedded in de_normalize.py (no external ranking_engine dependency):
- 24 count columns: where do crashes happen?
- 24 trend columns: 3-year % change (getting worse?)
- 16 proportion columns: night/impaired/speed/distracted %
- 8 severity columns: EPDO per crash, fatals per 1K crashes
- 4 composite columns: weighted safety score for investment decisions

Pipeline improvements:
- All I/O is parquet.gz native (read auto-detect, write always parquet.gz)
- smart_fill toggle in both YAMLs with passthrough
- states_registry.py for state resolution (not hardcoded STATE[:2])
- scripts/split.py updated with parquet auto-detect

10 bug fixes:
1. --skip-enrichment skipped ALL enrichment (now runs Tier 1)
2. Crash Year .0 decimal (2012.0 → 2012)
3. K_People/A_People float reversion after merge
4. geo_resolver _load_hierarchy() never read tprs
5. geo_resolver resolve_mpo() no hierarchy fallback
6. YAML #2 STATE[:2] → states_registry.py
7. Duplicate columns (Roadway Alignment) crashed parquet write
8. crash_enricher road_inventory_enricher import resilience
9. ranking_engine import before sys.path configured (eliminated)
10. scripts/split.py not updated (old version without parquet auto-detect)
```

### Verification After Push

Run **Delaware: Batch All Jurisdictions** workflow (YAML #1).
- Expected: 566,762 rows → 167 columns (69+4+2+76+18)
- The ranking line should show: `76 ranking columns in Xs`
- Then YAML #2 triggers automatically with enrichment + split
