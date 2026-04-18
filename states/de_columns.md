# Delaware Column Registry (`de_columns.md`)

Per-state column list for Delaware. Governed by the **Per-State Columns Rule** in the root `CLAUDE.md` — every state lives in `states/{abbr}_columns.md`. The repo-root `COLUMNS.md` aggregates shared columns (Golden Schema, HPMS, Mapillary, OSM, etc.) and points here for state-specific `dot_*` columns.

## State DOT Raw — Delaware (50)
_Source: DelDOT shapefile, `states/delaware/de_state_dot.py` → `generate_state_dot_data.py` → `build_road_inventory.py`_

Global column numbers (429–478) are shared with the canonical Delaware parquet run (121,733 rows × 532 cols, 2026-04-15). Fill-% is measured against that run.

| # | Column | Type | Fill % |
|---|---|---|---|
| 429 | `dot_raw_OBJECTID` | str | 100% |
| 430 | `dot_road_name` | str | 25% |
| 431 | `dot_road_number` | str | 25% |
| 432 | `dot_road_type` | str | 25% |
| 433 | `dot_route_number` | str | 1% |
| 434 | `dot_route_type` | str | 0% |
| 435 | `dot_county_code` | str | 25% |
| 436 | `dot_district_id` | str | 25% |
| 437 | `dot_municipality_code` | str | 25% |
| 438 | `dot_fc_code` | str | 25% |
| 439 | `dot_hpms_fc` | str | 0% |
| 440 | `dot_system_class` | str | 0% |
| 441 | `dot_ownership_code` | str | 0% |
| 442 | `dot_lanes` | str | 100% |
| 443 | `dot_surface_type_code` | str | 1% |
| 444 | `dot_surface_width_ft` | str | 100% |
| 445 | `dot_roadway_width_ft` | str | 100% |
| 446 | `dot_median_code` | str | 1% |
| 447 | `dot_median_width_ft` | str | 76% |
| 448 | `dot_lshldr_code` | str | 25% |
| 449 | `dot_lshldr_width_ft` | str | 100% |
| 450 | `dot_rshldr_code` | str | 25% |
| 451 | `dot_rshldr_width_ft` | str | 100% |
| 452 | `dot_lguardrail` | str | 0% |
| 453 | `dot_rguardrail` | str | 0% |
| 454 | `dot_lcurb` | str | 20% |
| 455 | `dot_rcurb` | str | 21% |
| 456 | `dot_lsidewalk` | str | 16% |
| 457 | `dot_rsidewalk` | str | 17% |
| 458 | `dot_traffic_dir` | str | 25% |
| 459 | `dot_area_type_code` | str | 25% |
| 460 | `dot_beg_mp` | str | 100% |
| 461 | `dot_end_mp` | str | 100% |
| 462 | `dot_maint_area` | str | 25% |
| 463 | `dot_hundreds` | str | 0% |
| 464 | `dot_parking` | str | 20% |
| 465 | `dot_snow_class` | str | 1% |
| 466 | `dot_bike_path` | str | 0% |
| 467 | `dot_raw_INVNTRY_DIR_CODE` | str | 25% |
| 468 | `dot_accept_year` | str | 75% |
| 469 | `dot_survey_date` | str | 100% |
| 470 | `dot_row_width_ft` | str | 0% |
| 471 | `dot_maint_rsp_code` | str | 25% |
| 472 | `dot_lane_width` | str | 100% |
| 473 | `dot_control_code` | str | 0% |
| 474 | `dot_gutter_code` | str | 0% |
| 475 | `dot_nhs_code` | str | 1% |
| 476 | `dot_surface_condition` | str | 1% |
| 477 | `dot_construction_type` | str | 0% |
| 478 | `dot_hsip_code` | str | 25% |

## State Extras — `de_*` (19)
_Source: `states/delaware/de_normalize.py` — columns 153–171 in the global registry._

See `COLUMNS.md` section "State Extras (de_*)" for the canonical list; those columns live in the shared registry because they're referenced from Tier-3 Supabase (`state_extras` JSONB) and cross-cut multiple pipeline stages.
