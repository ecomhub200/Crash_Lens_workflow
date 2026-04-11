---
title: Douglas County Frontend
type: entity
tags: [repo, frontend, javascript, web-app, ui]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Douglas County Frontend

The **web application repository** — provides the browser-based interface for crash analysis, visualization, reporting, and AI-assisted safety analysis.

## Purpose

Serves as the user-facing layer of [[crash-lens-overview|Crash Lens]]. Traffic engineers and safety analysts use it to:
- Visualize crash data on interactive maps
- Perform statistical safety analysis
- Match crash patterns to evidence-based countermeasures
- Generate professional reports for HSIP funding
- Run before/after studies on safety improvements

## 12+ Analysis Tabs

| Tab | Function |
|-----|----------|
| Dashboard | County-wide crash overview & trends |
| Analysis | Statistical summaries & filtering |
| Map | Interactive crash location visualization (Leaflet) |
| Hotspots | High-risk location identification |
| CMF/Countermeasures | Evidence-based safety treatment matching |
| Warrants | Signal warrant analysis (MUTCD standards) |
| Grants | Federal funding application recommendations |
| Before/After | Safety improvement evaluation studies |
| Safety Focus | Pedestrian/bicycle/speeding-specific analysis |
| Asset Deficiency | Road inventory deficiency analysis |
| Traffic Inventory | Road segment database browser |
| AI Assistant | Claude-powered natural language analysis |

## Architecture

- **Frontend**: Single-page app (SPA), modular JavaScript in `app/modules/`
- **Backend**: Node.js proxy server (`server/qdrant-proxy.js`)
- **Auth**: [[firebase-auth]] (Google OAuth + Email/Password)
- **Payments**: [[stripe-billing]] (Trial, Individual, Team, Agency plans)
- **Search**: [[qdrant-vector-db]] for AI-powered semantic search
- **Deployment**: Docker (node:18-alpine), Nginx + Node.js, Coolify platform

## State Management

Global state objects: `crashState`, `cmfState`, `warrantsState`, `grantState`, `baState`, `aiState`, `selectionState`

## Visualization Libraries

- **Chart.js** — Statistical charts and graphs
- **Leaflet.js** — Interactive mapping (+ MarkerCluster, Heatmap, VectorGrid)
- **PMTiles** — Overture Maps cloud-native vector tiles
- **Turf.js** — Client-side geospatial operations

## Report Generation

- **jsPDF** — PDF export
- **DOCX.js** — Word document generation

## Related Pages

- [[crash-lens-workflow]] — The data pipeline that feeds this app
- [[cloudflare-r2]] — Storage layer for crash data
- [[safety-countermeasures]] — CMF matching system
- [[hotspot-analysis]] — Risk identification methodology
- [[ai-integration]] — Claude AI assistant details


## Frontend Deep-Dive Results (2026-04-07)

Full codebase analysis by Claude Code revealed:

### Scale
- **13 main tabs** + 4 additional (CMF, Warrants, AI, Grants)
- **61 Chart.js canvases** across all tabs
- **27 data tables**
- **40+ dropdown/select controls**
- **100+ action buttons**
- **6 export formats**: CSV, PDF, DOCX, PPTX, KML, PNG

### Key Architecture Findings
- **Monolithic SPA**: `app/index.html` is 12MB with all tabs, charts, and logic inline
- **COL constants**: 56 column name mappings in data-loader.js (CSV header → JS variable)
- **R2 base URL**: `https://data.aicreatesai.com`
- **All aggregation is client-side**: `buildAggregates()` processes 500K+ rows in browser
- **State adapter**: `states/state_adapter.js` (46K) handles column normalization per state

### Frontend → Supabase Migration
Complete migration plan documented with exact SQL queries for every module. Key performance gains:
- Initial load: 5-50MB CSV download → ~1KB aggregate query
- Hotspot ranking: Sort 10K locations in JS → `ORDER BY ... LIMIT` with index
- Spatial filter: Haversine on every mapPoint → PostGIS `ST_DWithin`

### Detailed Wiki Pages
- [[frontend-ui-structure]] — Full tab/chart/table inventory
- [[data-loader]] — COL constants, R2 paths, aggregation structure
- [[frontend-supabase-migration]] — Module-by-module SQL migration plan
- [[frontend-data-inventory]] — Complete column mapping (56 of 111 Tier 1 used)
