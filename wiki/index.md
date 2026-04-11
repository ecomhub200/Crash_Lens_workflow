---
title: Wiki Index
type: index
updated: 2026-04-07
---

# Crash Lens Wiki — Index

Master catalog of all 36 pages in this knowledge base.

---

## Overview
| Page | Summary |
|------|---------|
| [[crash-lens-overview]] | High-level overview of the Crash Lens platform |

## Entities (8 pages)
| Page | Summary |
|------|---------|
| [[crash-lens-workflow]] | Data pipeline repo — Python ETL, 8-stage pipeline, Supabase sync |
| [[douglas-county-frontend]] | Frontend web app — 13 tabs, 61 charts, 27 tables, 6 export formats |
| [[cloudflare-r2]] | Object storage — 5.36 GB bucket, base URL data.aicreatesai.com |
| [[github-actions-ci]] | CI/CD — 40+ workflows including Supabase sync (Stage 4.5) |
| [[firebase-auth]] | Authentication & user management (migrating to Supabase Auth) |
| [[stripe-billing]] | Payment processing and subscription plans |
| [[qdrant-vector-db]] | Vector database for AI-powered search |
| [[delaware-pipeline]] | Reference state — 566,762 crashes, 517 columns, Supabase verified (24/24) |

## Concepts — Pipeline & Backend (11 pages)
| Page | Summary |
|------|---------|
| [[data-pipeline-architecture]] | 8-stage pipeline: download → normalize → enrich → split → R2 → Supabase → predict → manifest |
| [[crash-enrichment]] | GPS-based 4-tier enrichment: Self → HPMS → OSM → POI |
| [[state-onboarding]] | Pattern for adding new state crash data sources |
| [[build-road-inventory]] | Road inventory consolidation — 10 sources → 261 columns |
| [[supabase-schema-v3]] | **Active** — 3-Tier Column Strategy (111 explicit + JSONB), validated |
| [[supabase-schema]] | Original v1 spec (superseded by v3) |
| [[supabase-sync-ci]] | GitHub Actions workflow for R2 → Supabase sync |
| [[llm-wiki-pattern]] | The LLM Wiki knowledge management pattern (Karpathy) |
| [[state-adapter]] | State detection and column normalization (state_adapter.js) |
| [[upload-pipeline]] | Browser upload pipeline and R2 path construction |
| [[mcp-server-tools]] | MCP server tools for Claude Code/Desktop integration |

## Concepts — Frontend Features (12 pages)
| Page | Summary |
|------|---------|
| [[frontend-ui-structure]] | Full UI inventory: 13 tabs, 61 charts, 27 tables |
| [[data-loader]] | Data loader: COL constants, R2 paths, aggregation, filter system |
| [[frontend-supabase-migration]] | **Critical** — Module-by-module migration with SQL queries |
| [[dashboard-tab]] | Dashboard tab: 12 charts, severity/collision/weather/trend analysis |
| [[safety-focus]] | Safety Focus: 21 crash categories (ped, bike, alcohol, speed, etc.) |
| [[hotspot-analysis]] | Hot Spots: EPDO-weighted location ranking |
| [[cmf-countermeasures]] | CMF/Countermeasures: 808 evidence-based treatments |
| [[safety-countermeasures]] | CMF matching methodology |
| [[before-after-studies]] | Before/After study methodology |
| [[crash-profile]] | Crash profile generation for locations |
| [[baselines-analysis]] | Baseline crash rate calculations |
| [[epdo-scoring]] | EPDO severity weighting methodology |
| [[grant-ranking]] | Grant eligibility scoring and ORI calculations |
| [[trends-analysis]] | Year-over-year trend analysis |
| [[ai-integration]] | Claude AI assistant for natural language crash analysis |

## Sources (2 pages)
| Page | Summary |
|------|---------|
| [[source-workflow-repo]] | Crash_Lens_workflow-main — data pipeline codebase |
| [[source-frontend-repo]] | Douglas_County_2-main — frontend application codebase |

## Analyses (4 pages)
| Page | Summary |
|------|---------|
| [[technology-stack]] | Full technology inventory across both repos |
| [[state-coverage]] | Multi-state expansion status (30+ states) |
| [[data-sources-inventory]] | All external data sources integrated |
| [[frontend-data-inventory]] | **Critical** — Column mapping: 56 of 111 Tier 1 used by frontend |
