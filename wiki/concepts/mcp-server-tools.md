---
title: MCP Server — 22 Tools
type: concept
tags: [frontend, mcp, tools, api, analysis]
created: 2026-04-07
updated: 2026-04-07
sources: [source-frontend-repo]
---

# MCP Server — 22 Analysis Tools

## Overview
The CrashLens MCP server exposes 22 tools and 6 resources via Model Context Protocol. It runs in Node.js, loads crash CSV data, and provides structured analysis to AI assistants.

## Tool Registry

### Crash Tools (5)
| Tool | Purpose | Key Inputs |
|------|---------|-----------|
| `query_crashes` | Filter & query individual crash records | route, node, severity[], date range, factors[] |
| `get_crash_statistics` | Aggregate statistics for location | route, node, date range |
| `calculate_epdo` | EPDO from severity counts | K, A, B, C, O, state_fips |
| `analyze_hotspots` | Identify crash concentration locations | type (route/intersection), min_crashes, sort_by |
| `build_crash_profile` | Detailed crash profile for location | route, node, date range |

### Analysis Tools (4)
| Tool | Purpose | Key Inputs |
|------|---------|-----------|
| `calculate_baselines` | County-wide baseline crash rates | (none — uses full dataset) |
| `analyze_over_representation` | ORI + significance for location | route, node |
| `analyze_crash_trends` | Temporal trend analysis | route, node, date range |
| `compare_locations` | Side-by-side location comparison | location_a, location_b |

### Safety Tools (3)
| Tool | Purpose | Key Inputs |
|------|---------|-----------|
| `analyze_safety_category` | Analyze one of 21 safety categories | category enum, route, date range |
| `get_safety_overview` | All 21 categories ranked | date range, sort_by |
| `run_before_after_study` | Treatment effectiveness evaluation | route, node, treatment_date, method |

### CMF Tools (3)
| Tool | Purpose | Key Inputs |
|------|---------|-----------|
| `search_cmf_database` | Search 808 countermeasures | crash_types[], category, min_rating |
| `recommend_countermeasures` | Auto-recommend for location | route, node |
| `calculate_combined_cmf` | Combined effect of multiple CMFs | cmf_values[] |

### Infrastructure Tools (7)
| Tool | Purpose | Key Inputs |
|------|---------|-----------|
| `evaluate_signal_warrant` | MUTCD signal warrant criteria | major/minor volumes, lanes |
| `score_grant_eligibility` | Grant program scoring | route, node, scoring_profile |
| `get_forecasts` | Crash forecasts for jurisdiction | state, jurisdiction, road_type |
| `search_grants` | Search available grant programs | program, keyword, status |
| `get_jurisdiction_info` | Jurisdiction metadata | state_fips, state_name |
| `list_locations` | Discover routes/intersections | type, min_crashes, search |
| `get_data_quality` | Assess data completeness | route, node |

## Resources (6)
1. `crashlens://data/summary` — Data summary
2. `crashlens://config/epdo-weights` — EPDO weights
3. `crashlens://config/states` — Available states
4. `crashlens://data/cmf-summary` — CMF database summary
5. `crashlens://config/safety-categories` — 21 safety categories
6. `crashlens://config/cmf-categories` — CMF category list

## Environment Variables
- `CRASHLENS_STATE` — State name (standalone mode)
- `CRASHLENS_JURISDICTION` — Jurisdiction (standalone mode)
- `CRASHLENS_ROAD_TYPE` — all_roads / county_roads / no_interstate
- `CRASHLENS_API_KEY` — Required for R2 data download

## Related Pages
- [[data-loader]] — Data loading used by all tools
- [[crash-profile]] — Profile analysis tools
- [[safety-focus]] — Safety category analysis
- [[cmf-countermeasures]] — CMF search/recommend
