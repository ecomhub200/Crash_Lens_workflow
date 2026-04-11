---
title: LLM Wiki Pattern
type: concept
tags: [llm-wiki, knowledge-management, karpathy, obsidian, methodology]
created: 2026-04-05
updated: 2026-04-05
sources: []
---

# LLM Wiki Pattern

A knowledge management approach by **Andrej Karpathy** where LLMs incrementally build and maintain a persistent, interlinked wiki from raw sources.

Source: [LLM Wiki gist](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f)

## Core Idea

Instead of RAG (re-deriving knowledge on every query), the LLM **compiles knowledge once** into structured wiki pages and keeps them current. The wiki is a persistent, compounding artifact.

## Three Layers

1. **Raw sources** — Immutable documents (articles, code, PDFs). LLM reads, never modifies.
2. **The wiki** — LLM-generated markdown pages. Summaries, entities, concepts, cross-references.
3. **The schema** — Configuration telling the LLM how the wiki is structured (e.g., `CLAUDE.md`).

## Three Operations

1. **Ingest** — Process new sources → create/update wiki pages → update index & log
2. **Query** — Search wiki → synthesize answer → optionally file answer as new page
3. **Lint** — Health-check for contradictions, orphans, stale claims, missing links

## Key Files

- `index.md` — Content catalog of all pages (LLM reads this first for navigation)
- `log.md` — Chronological append-only activity record

## Why It Works

> "The tedious part of maintaining a knowledge base is not the reading or the thinking — it's the bookkeeping."

LLMs handle the maintenance burden (cross-references, summaries, consistency) that makes humans abandon wikis. The human curates sources and directs analysis.

## This Vault

This Obsidian vault follows this pattern. See the root `CLAUDE.md` for the schema.

## Related Pages

- [[crash-lens-overview]] — The project this wiki documents
