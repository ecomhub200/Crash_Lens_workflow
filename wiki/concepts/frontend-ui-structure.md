---
title: Frontend UI Structure
type: concept
tags: [frontend, ui, tabs, charts, controls]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# Frontend UI Structure

## Application Entry Point
`app/index.html` (12MB) — monolithic single-page application with all tabs, charts, and logic inline.

## Main Tabs (13)

| Tab ID | Name | Charts | Tables |
|--------|------|--------|--------|
| `tab-upload` | Upload Data | 0 | 0 |
| `tab-dashboard` | Dashboard | 12 | 4 |
| `tab-map` | Map | 0 | 0 (Leaflet) |
| `tab-hotspots` | Hot Spots | 0 | 2 |
| `tab-crashtree` | Crash Tree | 0 | 1 |
| `tab-analysis` | Analysis | 0 | varies |
| `tab-fatalspeeding` | Fatal & Speeding | 8 | 4 |
| `tab-intersection` | Intersections | 4 | 1 |
| `tab-pedestrian` | Ped/Bike | 13 | 3 |
| `tab-deepdive` | Deep Dive | 20 | 4 |
| `tab-prediction` | Crash Prediction | 0 | 4 |
| `tab-safety` | Safety Focus | 0 | 1 |
| `tab-reports` | Reports | 0 | 0 |

### Additional Tabs
- `tab-cmf` — Countermeasures (subtabs: main, asset-deficiency)
- `tab-warrants` — Warrant Analyzer
- `tab-ai` — MUTCD AI Assistant
- `tab-grants` — Funding Opportunities

## Total UI Elements
- **61 Chart.js canvases**
- **27 data tables**
- **40+ dropdown/select controls**
- **100+ action buttons**
- **1 Leaflet map** with heatmap, markers, drawing tools

## Tier/Jurisdiction Selection
8 geographic tiers: federal, state, region, MPO, planning_district, county, city, town.
Dynamic dropdowns populated based on tier + state selection.

## Export Capabilities
Every analysis tab supports multiple export formats:
- **CSV** — Raw data tables
- **PDF** — Formatted reports with charts (jsPDF + AutoTable)
- **DOCX** — Word documents (docx library)
- **PPTX** — Presentations (PptxGenJS)
- **KML** — GIS-compatible geographic data
- **PNG** — Chart images (html2canvas)

## External Libraries
PapaParse (CSV), Chart.js (charts), D3.js (viz), Leaflet (maps), Turf.js (geospatial), jsPDF (PDF), XLSX (Excel), Firebase (auth), pako (gzip), marked (Markdown)

## Related Pages
- [[data-loader]] — Data loading architecture
- [[dashboard-tab]] — Dashboard details
- [[frontend-supabase-migration]] — Migration plan
