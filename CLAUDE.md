# CrashLens Workflow — Claude Code Instructions

## Wiki

This repo contains a `wiki/` folder following the [Karpathy LLM Wiki pattern](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f). It is the single source of truth for CrashLens architecture, schema, pipeline design, and infrastructure.

### Wiki-First Rule
Before answering ANY question about CrashLens architecture, database schema, pipeline design, column names, port numbers, R2 paths, or infrastructure — **read the relevant wiki page first**. Never rely on memory or training data alone.

Key files to check:
- `wiki/log.md` — Most recent changes (read FIRST for current state)
- `wiki/concepts/supabase-schema-v3.md` — Database schema, column types, TIER1_MAP
- `wiki/concepts/supabase-sync-ci.md` — Sync architecture (webhook + SSH tunnel)
- `wiki/concepts/pipeline-architecture-v29.md` — Full pipeline phases 0-4
- `wiki/entities/delaware-pipeline.md` — Reference state implementation
- `wiki/entities/webhook-sync.md` — VPS webhook infrastructure

### Auto-Wiki Rule
When making code changes, pipeline modifications, or architecture decisions:
1. Update the relevant wiki page(s)
2. Add an entry to `wiki/log.md` with date and summary
3. Do NOT skip this — the wiki must stay current

### Wiki Structure (Karpathy pattern)
- **concepts/** — How things work (architecture, patterns, schemas)
- **entities/** — Specific things (states, tools, infrastructure)
- **analyses/** — Cross-cutting investigations
- **sources/** — External source documentation
- **log.md** — Chronological record of all changes

### Wiki Sync
This wiki/ is mirrored from an Obsidian vault. After updating wiki/ here, the Obsidian vault needs to be synced (GitHub Desktop pull). Both locations should stay consistent.
