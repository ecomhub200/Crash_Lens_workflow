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

### Wiki Log Conflict-Avoidance Rule (IMPORTANT)
`wiki/log.md` conflicts on nearly every PR because multiple branches append entries to the same top-of-file region in parallel. To minimize merge conflicts:

1. **ALWAYS create a NEW log entry with a fresh heading.** Do this even if the user asks you to update, amend, or reuse a specific existing log entry / date / "log number". Never edit or rewrite an existing entry — it may have been modified on `main` since you branched, which would force a 3-way conflict.
2. **Use a unique heading per entry.** Format: `## [YYYY-MM-DD] <type> | <short title — unique to this change>`. If an entry for today's date already exists on your branch (or on `main` as of your last fetch), pick a title that doesn't collide with any existing heading on that date — e.g. add the feature name or PR slug so two entries on the same day can't share a heading line.
3. **Insert new entries at the top of the log** (below the frontmatter and the first `---`), matching the existing append-at-top convention.
4. **If a conflict does happen on merge**, resolve by **stacking both sides' entries in reverse-chronological order** — never discard an entry from either side. Example fix from PR #84: both `main` and the feature branch added new entries at the top; the resolution kept both, newest-date first. See the merge commit `8668608` for the pattern.
5. **Update the frontmatter `updated:` date** to the newest entry's date whenever you add an entry. If this line conflicts during merge, always take the later of the two dates.

Rationale: this rule trades a tiny amount of edit flexibility (you never rewrite history) for a large reduction in merge-conflict frequency, since purely-additive, uniquely-keyed entries are the easiest case for git to auto-merge — and when they do conflict, the resolution is mechanical (stack both).

### Wiki Structure (Karpathy pattern)
- **concepts/** — How things work (architecture, patterns, schemas)
- **entities/** — Specific things (states, tools, infrastructure)
- **analyses/** — Cross-cutting investigations
- **sources/** — External source documentation
- **log.md** — Chronological record of all changes

### Wiki Sync
This wiki/ is mirrored from an Obsidian vault. After updating wiki/ here, the Obsidian vault needs to be synced (GitHub Desktop pull). Both locations should stay consistent.
