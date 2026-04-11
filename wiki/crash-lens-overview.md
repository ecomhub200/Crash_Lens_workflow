---
title: Crash Lens Overview
type: overview
tags: [crash-lens, platform, safety]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo, source-frontend-repo]
---

# Crash Lens

Crash Lens is a **browser-based crash analysis and safety management platform** for transportation agencies. It enables traffic engineers and safety analysts to analyze millions of traffic crash records, identify high-risk locations, and generate data-driven safety improvement recommendations.

## What It Does

- Ingests crash data from **30+ US state DOT APIs**
- Normalizes, enriches, and stores crash records in a unified format
- Provides interactive maps, charts, and analysis tools in the browser
- Matches crash patterns to **500+ evidence-based safety countermeasures**
- Generates professional reports (PDF, DOCX) for HSIP funding applications
- Includes an AI assistant (Claude) for natural language crash analysis

## Key Metrics

- 500,000+ crashes analyzed statewide (Virginia alone)
- 95+ counties + 38 cities in Virginia
- 500+ evidence-based safety countermeasures in the CMF database
- $10M+ in HSIP funding applications supported
- Multi-state coverage expanding to 30+ states

## System Architecture

```
┌─────────────────────────────────────────┐
│  Crash_Lens_Workflow (Backend)          │
│  Python data pipeline                   │
│  - Download from state DOT APIs         │
│  - Normalize & enrich crash records     │
│  - Upload to Cloudflare R2              │
│  - GitHub Actions automation            │
└────────────────┬────────────────────────┘
                 │ CSV + Parquet files via R2
                 ▼
┌─────────────────────────────────────────┐
│  Douglas_County (Frontend)              │
│  Browser-based web application          │
│  - 12+ analysis tabs                    │
│  - Interactive maps (Leaflet)           │
│  - Report generation (PDF/DOCX)         │
│  - AI assistant (Claude)               │
│  - User auth (Firebase) + billing       │
└─────────────────────────────────────────┘
```

## Two Repositories

| Repo | Role | Language | See |
|------|------|----------|-----|
| [[crash-lens-workflow]] | Data pipeline & ETL | Python 3.11 | [[data-pipeline-architecture]] |
| [[douglas-county-frontend]] | Web app & UI | JavaScript/Node.js | [[safety-countermeasures]] |

## Related Pages

- [[technology-stack]] — Full tech inventory
- [[state-coverage]] — Multi-state expansion status
- [[data-sources-inventory]] — All integrated data sources
- [[cloudflare-r2]] — Storage layer connecting both repos
