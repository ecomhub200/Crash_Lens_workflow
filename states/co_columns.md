# Colorado Column Registry (`co_columns.md`)

Per-state column list for Colorado. Governed by the **Per-State Columns Rule** in the root `CLAUDE.md` — every state lives in `states/{abbr}_columns.md`. The repo-root `COLUMNS.md` aggregates shared columns (Golden Schema, HPMS, Mapillary, OSM, etc.) and points here for state-specific `dot_*` columns.

## State DOT Raw — Colorado (25)
_Source: CDOT CPLAN FeatureServer Layer 7 (Highways), `states/colorado/co_state_dot.py` → `generate_state_dot_data.py` → `build_road_inventory.py`_

Local numbering (`CO-1..CO-25`). These columns are not yet in the canonical Delaware parquet, so fill-% is TBD until a Colorado run is completed.

| # | Column | Type | Fill % | Notes |
|---|---|---|---|---|
| CO-1  | `dot_fc_text`          | str | TBD | CDOT FUNCCLASS (text, e.g. "Rural Minor Arterial") |
| CO-2  | `dot_route_sign`       | str | TBD | I / US / SH |
| CO-3  | `dot_nhs`              | str | TBD | National Highway System designation |
| CO-4  | `dot_access_control`   | str | TBD | Access control category |
| CO-5  | `dot_description`      | str | TBD | Road description / alias |
| CO-6  | `dot_county_name`      | str | TBD | County name (text) |
| CO-7  | `dot_county_fips`      | str | TBD | County FIPS (3 digits) |
| CO-8  | `dot_city`             | str | TBD | City name |
| CO-9  | `dot_city_fips`        | str | TBD | City FIPS |
| CO-10 | `dot_region_code`      | str | TBD | CDOT engineering region 1-5 |
| CO-11 | `dot_tpr_id`           | str | TBD | Transportation Planning Region |
| CO-12 | `dot_terrain`          | str | TBD | Flat / Rolling / Mountainous |
| CO-13 | `dot_surface_type`     | str | TBD | Text: Asphalt / Concrete / Composite / Gravel / Dirt |
| CO-14 | `dot_is_divided`       | str | TBD | Y/N divided-highway flag |
| CO-15 | `dot_median_type`      | str | TBD | Median type (text) |
| CO-16 | `dot_median_width`     | str | TBD | Median width (ft) |
| CO-17 | `dot_shoulder_type`    | str | TBD | Outside shoulder type |
| CO-18 | `dot_shoulder_width`   | str | TBD | Outside shoulder width (ft) |
| CO-19 | `dot_aadt`             | str | TBD | Annual average daily traffic |
| CO-20 | `dot_aadt_single`      | str | TBD | Single-unit truck AADT |
| CO-21 | `dot_aadt_combo`       | str | TBD | Combination truck AADT |
| CO-22 | `dot_speed_limit`      | str | TBD | Posted speed limit (mph) — actual, not estimated |
| CO-23 | `dot_vmt`              | str | TBD | Vehicle miles traveled |
| CO-24 | `dot_vc_ratio`         | str | TBD | Volume/capacity ratio |
| CO-25 | `dot_seg_length`       | str | TBD | Segment length (miles) |

## Shared with Delaware

Columns the Colorado normalizer writes that also appear in Delaware's `dot_*` section (`states/de_columns.md`) — same name, same type, written by both states:

`dot_road_name`, `dot_route_number`, `dot_lanes`, `dot_lane_width`, `dot_beg_mp`, `dot_end_mp`.

Written by `normalize()` on every state row (not state-specific): `dot_source`, `dot_source_url`.
