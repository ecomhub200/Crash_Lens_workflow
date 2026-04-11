---
title: Technology Stack
type: analysis
tags: [technology, stack, tools, libraries]
created: 2026-04-05
updated: 2026-04-05
sources: [source-workflow-repo, source-frontend-repo]
---

# Technology Stack

Full technology inventory across the Crash Lens platform.

## Data Pipeline ([[crash-lens-workflow]])

| Category | Technology |
|----------|-----------|
| Language | Python 3.11 |
| Data Processing | pandas, geopandas, PyArrow, DuckDB |
| Geospatial | shapely, OSMnx, scipy |
| Browser Automation | Playwright |
| Forecasting | AWS SageMaker Chronos-2 |
| CI/CD | GitHub Actions (40+ workflows) |
| Storage | Cloudflare R2 (S3-compatible) |

## Frontend Application ([[douglas-county-frontend]])

| Category | Technology |
|----------|-----------|
| Frontend | HTML5, CSS3, JavaScript ES6+ |
| Backend | Node.js |
| Maps | Leaflet.js (+ MarkerCluster, Heatmap, VectorGrid) |
| Charts | Chart.js |
| Geospatial (client) | Turf.js, PMTiles |
| Reports | jsPDF, DOCX.js |
| Data Parsing | PapaParse, SheetJS |
| Auth | Firebase (Google OAuth + Email) |
| Payments | Stripe |
| AI | Claude API, Qdrant vector DB |
| Email | Brevo |
| Deployment | Docker, Nginx, Supervisord, Coolify |

## Infrastructure

| Service | Purpose |
|---------|---------|
| [[cloudflare-r2]] | Object storage for crash data |
| [[firebase-auth]] | User authentication |
| [[stripe-billing]] | Subscription management |
| [[qdrant-vector-db]] | Semantic search embeddings |
| GitHub Actions | Pipeline automation |
| AWS SageMaker | Crash forecasting |
| Coolify | Application deployment |

## Related Pages

- [[crash-lens-overview]] — Platform overview
- [[crash-lens-workflow]] — Pipeline repo
- [[douglas-county-frontend]] — Frontend repo
