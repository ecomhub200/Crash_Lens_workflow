# CrashLens Column Registry — Delaware v2.0

Generated from Supabase `crashes_delaware` (569,829 rows)
Date: 2026-04-19
Pipeline: v2.7.2 | Last sync: 2026-04-18 00:34 UTC

## Supabase Schema Summary

- **Tier 1 (explicit columns):** 118
- **road_data JSONB keys:** 268
- **state_extras JSONB keys:** 15
- **ranking_data JSONB keys:** 76
- **Total unique fields:** 477

## Column Name Rules

1. **Tier 1 columns:** snake_case in Postgres — `crash_severity`, `max_speed_diff`
2. **Enrichment/resolved:** snake_case — `resolved_speed_limit`, `ri_matched`
3. **HPMS:** `hpms_` prefix — `hpms_speed_limit`, `hpms_aadt`
4. **State DOT raw:** `dot_` prefix — `dot_road_name`, `dot_lanes`
5. **State DOT resolved:** `sdot_` prefix — `sdot_Speed_Limit_Est`, `sdot_Through_Lanes`
6. **Mapillary:** `map_` prefix — `map_signal_present`, `map_speed_limit_value`
7. **Rankings:** `{Tier}_Rank_{metric}` — `Juris_Rank_total_crash`
8. **State extras:** `{abbr}_` prefix — `de_Day_Of_Week_Code`
9. **POI proximity:** `near_poi_{type}_{radius}` — `near_poi_signal_100ft`
10. **Federal proximity:** `near_{asset}_{radius}` — `near_bridge_500ft`

## CRITICAL: Cross-Reference Map

These columns reference the SAME data across pipeline stages.
If you rename one, you MUST update the other references.

| Road Inventory Column | Code Reference | Used In |
|---|---|---|
| `sdot_Speed_Limit_Est` | road_data_authority.py resolve_speed_limit() | Tier A speed |
| `sdot_Through_Lanes` | road_data_authority.py resolve_lanes() | Tier A lanes |
| `sdot_Roadway Surface Type` | road_data_authority.py resolve_surface() | Tier A surface |
| `sdot_RTE Name` | road_data_authority.py merge_frontend_columns() | Tier A route name |
| `sdot_Functional Class` | road_data_authority.py merge_frontend_columns() | Tier A FC |
| `sdot_Ownership` | road_data_authority.py merge_frontend_columns() | Tier A ownership |
| `map_signal_present` | road_data_authority.py resolve_signals() | Tier C signal |
| `map_speed_limit_value` | road_data_authority.py resolve_speed_limit() | Tier C speed |
| `hpms_speed_limit` | road_data_authority.py resolve_speed_limit() | Tier B speed |
| `maxspeed` | road_data_authority.py resolve_speed_limit() | Tier D speed (OSM) |
| `resolved_speed_limit` | crash_enricher.py → max_speed_diff | Frontend speed |
| `intersection_name` | crash_enricher.py → rte_name fallback | Frontend route |

---

## Tier 1 — Explicit Columns (118)

### Identity (6)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 1 | `id` | bigint | 569,829 | 100% |
| 2 | `state` | text | 569,829 | 100% |
| 3 | `objectid` | text | 569,829 | 100% |
| 4 | `document_nbr` | text | 569,829 | 100% |
| 5 | `crash_year` | integer | 569,826 | 100% |
| 6 | `crash_date` | text | 569,826 | 100% |
| 7 | `crash_military_time` | text | 569,829 | 100% |

### Severity & Counts (8)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 8 | `crash_severity` | text | 569,829 | 100% |
| 9 | `k_people` | integer | 569,829 | 100% |
| 10 | `a_people` | integer | 569,829 | 100% |
| 11 | `b_people` | integer | 569,829 | 100% |
| 12 | `c_people` | integer | 569,829 | 100% |
| 13 | `persons_injured` | integer | 569,829 | 100% |
| 14 | `pedestrians_killed` | integer | 569,829 | 100% |
| 15 | `pedestrians_injured` | integer | 569,829 | 100% |
| 16 | `vehicle_count` | integer | 569,829 | 100% |

### Crash Characteristics (7)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 17 | `collision_type` | text | 569,829 | 100% |
| 18 | `weather_condition` | text | 569,829 | 100% |
| 19 | `light_condition` | text | 569,829 | 100% |
| 20 | `roadway_surface_cond` | text | 569,829 | 100% |
| 21 | `relation_to_roadway` | text | 20,275 | 3.6% |
| 22 | `roadway_alignment` | text | 560,234 | 98.3% |
| 23 | `roadway_surface_type` | text | 560,234 | 98.3% |
| 24 | `roadway_defect` | text | 0 | 0% |

### Road Description (4)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 25 | `roadway_description` | text | 560,234 | 98.3% |
| 26 | `intersection_type` | text | 569,829 | 100% |
| 27 | `traffic_control_type` | text | 168,600 | 29.6% |
| 28 | `traffic_control_status` | text | 0 | 0% |

### Work/School Zone (4)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 29 | `work_zone_related` | text | 569,829 | 100% |
| 30 | `work_zone_location` | text | 3,878 | 0.7% |
| 31 | `work_zone_type` | text | 3,880 | 0.7% |
| 32 | `school_zone` | text | 569,829 | 100% |

### Harmful Events (2)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 33 | `first_harmful_event` | text | 0 | 0% |
| 34 | `first_harmful_event_loc` | text | 0 | 0% |

### Safety Flags (13)

| # | Column (pg) | Type | Filled | Fill % | Note |
|---|---|---|---|---|---|
| 35 | `alcohol` | text | 569,829 | 100% | Yes/No |
| 36 | `animal_related` | text | 569,829 | 100% | Yes/No |
| 37 | `unrestrained` | text | 569,829 | 100% | Yes/No |
| 38 | `bike` | text | 569,829 | 100% | Yes/No |
| 39 | `distracted` | text | 569,829 | 100% | Yes/No |
| 40 | `drowsy` | text | 0 | 0% | v2.7.1: not tracked by DE |
| 41 | `drug_related` | text | 569,829 | 100% | Yes/No |
| 42 | `guardrail_related` | text | 369,941 | 64.9% | Yes/No |
| 43 | `hitrun` | text | 0 | 0% | v2.7.1: not tracked by DE |
| 44 | `lgtruck` | text | 0 | 0% | v2.7.1: not tracked by DE |
| 45 | `motorcycle` | text | 569,829 | 100% | Yes/No |
| 46 | `pedestrian` | text | 569,829 | 100% | Yes/No |
| 47 | `speed` | text | 569,829 | 100% | Yes/No |

### Analysis Fields (8)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 48 | `max_speed_diff` | text | 536,025 | 94.1% |
| 49 | `road_departure_type` | text | 0 | 0% |
| 50 | `intersection_analysis` | text | 569,829 | 100% |
| 51 | `senior` | text | 0 | 0% |
| 52 | `young` | text | 0 | 0% |
| 53 | `mainline` | text | 569,829 | 100% |
| 54 | `night` | text | 569,829 | 100% |

### Geography & Jurisdiction (11)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 55 | `dot_district` | text | 569,829 | 100% |
| 56 | `juris_code` | text | 569,829 | 100% |
| 57 | `physical_juris_name` | text | 569,829 | 100% |
| 58 | `functional_class` | text | 560,234 | 98.3% |
| 59 | `facility_type` | text | 560,234 | 98.3% |
| 60 | `area_type` | text | 569,829 | 100% |
| 61 | `system` | text | 560,234 | 98.3% |
| 62 | `vsp` | text | 0 | 0% |
| 63 | `ownership` | text | 560,234 | 98.3% |
| 64 | `planning_district` | text | 569,829 | 100% |
| 65 | `mpo_name` | text | 569,829 | 100% |

### Route/Node/Coordinates (8)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 66 | `rte_name` | text | 529,106 | 92.9% |
| 67 | `rns_mp` | text | 346,290 | 60.8% |
| 68 | `node` | text | 560,234 | 98.3% |
| 69 | `node_offset_ft` | text | 0 | 0% |
| 70 | `x` | double precision | 569,829 | 100% |
| 71 | `y` | double precision | 569,829 | 100% |
| 72 | `road_lon` | text | 560,234 | 98.3% |
| 73 | `road_lat` | text | 560,234 | 98.3% |

### Enrichment: FIPS & Scoring (4)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 74 | `fips` | text | 569,829 | 100% |
| 75 | `place_fips` | text | 266,112 | 46.7% |
| 76 | `epdo_score` | text | 569,829 | 100% |
| 77 | `intersection_name` | text | 515,474 | 90.5% |

### Road Inventory Attributes (11)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 78 | `through_lanes` | text | 560,234 | 98.3% |
| 79 | `aadt` | text | 560,234 | 98.3% |
| 80 | `aadt_source` | text | 476,409 | 83.6% |
| 81 | `lane_width_ft` | text | 360,346 | 63.2% |
| 82 | `median_width_ft` | text | 198,012 | 34.7% |
| 83 | `shoulder_width_ft` | text | 290,503 | 51.0% |
| 84 | `has_sidewalk` | text | 360,346 | 63.2% |
| 85 | `has_bike_lane` | text | 360,346 | 63.2% |
| 86 | `urban_area_name` | text | 462,148 | 81.1% |
| 87 | `urban_area_geoid` | text | 462,148 | 81.1% |
| 88 | `length_ft` | text | 560,234 | 98.3% |

### Resolved Attributes (10)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 89 | `resolved_speed_limit` | text | 560,234 | 98.3% |
| 90 | `resolved_has_lighting` | text | 560,234 | 98.3% |
| 91 | `resolved_has_signal` | text | 560,234 | 98.3% |
| 92 | `resolved_on_bridge` | text | 560,234 | 98.3% |
| 93 | `resolved_school_zone` | text | 560,234 | 98.3% |
| 94 | `is_intersection` | text | 560,234 | 98.3% |
| 95 | `intersection_degree` | text | 560,234 | 98.3% |
| 96 | `is_ramp` | text | 560,234 | 98.3% |
| 97 | `ramp_type` | text | 20,275 | 3.6% |
| 98 | `curvature` | text | 560,234 | 98.3% |

### POI Proximity Flags (15)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 99 | `near_poi_bar_1500ft` | text | 560,234 | 98.3% |
| 100 | `near_poi_clinic_1500ft` | text | 0 | 0% |
| 101 | `near_poi_college_1500ft` | text | 560,234 | 98.3% |
| 102 | `near_poi_crossing_100ft` | text | 560,234 | 98.3% |
| 103 | `near_poi_fuel_500ft` | text | 560,234 | 98.3% |
| 104 | `near_poi_hospital_2000ft` | text | 0 | 0% |
| 105 | `near_poi_parking_500ft` | text | 560,234 | 98.3% |
| 106 | `near_poi_rest_area_1000ft` | text | 560,234 | 98.3% |
| 107 | `near_poi_restaurant_500ft` | text | 560,234 | 98.3% |
| 108 | `near_poi_signal_100ft` | text | 560,234 | 98.3% |
| 109 | `near_poi_stop_sign_100ft` | text | 560,234 | 98.3% |
| 110 | `near_bridge_500ft` | text | 560,234 | 98.3% |
| 111 | `near_rail_xing_500ft` | text | 560,234 | 98.3% |
| 112 | `near_school_1500ft` | text | 0 | 0% |
| 113 | `near_transit_500ft` | text | 560,234 | 98.3% |

### JSONB & System Columns (6)

| # | Column (pg) | Type | Filled | Fill % |
|---|---|---|---|---|
| 114 | `road_data` | jsonb | 569,829 | 100% |
| 115 | `state_extras` | jsonb | 569,829 | 100% |
| 116 | `ranking_data` | jsonb | 569,829 | 100% |
| 117 | `crash_date_parsed` | date | 569,826 | 100% |
| 118 | `geom` | geometry | 569,829 | 100% |

---

## road_data JSONB — Enrichment & Road Inventory (268 keys)

### ARNOLD / Route ID (3)

| # | Key |
|---|---|
| 1 | `ARNOLD_Begin_MP` |
| 2 | `ARNOLD_End_MP` |
| 3 | `ARNOLD_Route_ID` |

### Confidence Scores (6)

| # | Key |
|---|---|
| 4 | `conf_bridge` |
| 5 | `conf_crosswalk` |
| 6 | `conf_school_zone` |
| 7 | `conf_signal` |
| 8 | `conf_speed_limit` |
| 9 | `conf_stop_sign` |

### Pavement / Geometry (2)

| # | Key |
|---|---|
| 10 | `Cracking_Pct` |
| 11 | `length_m` |

### Curvature Analysis (8)

| # | Key |
|---|---|
| 12 | `curve_advisory_speed` |
| 13 | `curve_class` |
| 14 | `curve_class_label` |
| 15 | `curve_has_warning_sign` |
| 16 | `curve_is_curve` |
| 17 | `curve_risk_score` |
| 18 | `curve_speed_differential` |

### State DOT Raw — dot_ (33)

| # | Key |
|---|---|
| 19 | `dot_area_type_code` |
| 20 | `dot_beg_mp` |
| 21 | `dot_bike_path` |
| 22 | `dot_county_code` |
| 23 | `dot_district_id` |
| 24 | `dot_end_mp` |
| 25 | `dot_fc_code` |
| 26 | `dot_hsip_code` |
| 27 | `dot_lane_width` |
| 28 | `dot_lanes` |
| 29 | `dot_lshldr_code` |
| 30 | `dot_lshldr_width_ft` |
| 31 | `dot_maint_area` |
| 32 | `dot_maint_rsp_code` |
| 33 | `dot_median_code` |
| 34 | `dot_median_width_ft` |
| 35 | `dot_municipality_code` |
| 36 | `dot_nhs_code` |
| 37 | `dot_parking` |
| 38 | `dot_raw_INVNTRY_DIR_CODE` |
| 39 | `dot_raw_OBJECTID` |
| 40 | `dot_rcurb` |
| 41 | `dot_road_name` |
| 42 | `dot_road_number` |
| 43 | `dot_road_type` |
| 44 | `dot_roadway_width_ft` |
| 45 | `dot_route_number` |
| 46 | `dot_rshldr_code` |
| 47 | `dot_rshldr_width_ft` |
| 48 | `dot_snow_class` |
| 49 | `dot_surface_condition` |
| 50 | `dot_surface_type_code` |
| 51 | `dot_surface_width_ft` |
| 52 | `dot_survey_date` |
| 53 | `dot_traffic_dir` |

### HPMS Federal — hpms_ (38)

| # | Key |
|---|---|
| 54 | `hpms_aadt_combination` |
| 55 | `hpms_aadt_single_unit` |
| 56 | `hpms_access_control` |
| 57 | `hpms_begin_point` |
| 58 | `hpms_capacity` |
| 59 | `hpms_climate_zone` |
| 60 | `hpms_cracking_percent` |
| 61 | `hpms_design_speed` |
| 62 | `hpms_dir_factor` |
| 63 | `hpms_directional_through_lanes` |
| 64 | `hpms_end_point` |
| 65 | `hpms_faulting` |
| 66 | `hpms_future_aadt` |
| 67 | `hpms_hov_type` |
| 68 | `hpms_iri` |
| 69 | `hpms_k_factor` |
| 70 | `hpms_lane_width` |
| 71 | `hpms_length_mi` |
| 72 | `hpms_median_type` |
| 73 | `hpms_median_width` |
| 74 | `hpms_nhs` |
| 75 | `hpms_num_signalized_int` |
| 76 | `hpms_num_signals` |
| 77 | `hpms_num_stop_int` |
| 78 | `hpms_pct_green_time` |
| 79 | `hpms_pct_peak_combination` |
| 80 | `hpms_pct_peak_single` |
| 81 | `hpms_peak_lanes` |
| 82 | `hpms_peak_parking` |
| 83 | `hpms_psr` |
| 84 | `hpms_route_id` |
| 85 | `hpms_route_number` |
| 86 | `hpms_route_signing` |
| 87 | `hpms_rutting` |
| 88 | `hpms_section_length` |
| 89 | `hpms_shoulder_type_l` |
| 90 | `hpms_shoulder_type_r` |
| 91 | `hpms_shoulder_width_l` |
| 92 | `hpms_shoulder_width_r` |
| 93 | `hpms_signal_type` |
| 94 | `hpms_state_code` |
| 95 | `hpms_structure_type` |
| 96 | `hpms_terrain_type` |
| 97 | `hpms_toll_charged` |
| 98 | `hpms_turn_lanes_l` |
| 99 | `hpms_turn_lanes_r` |
| 100 | `hpms_widening_feasibility` |
| 101 | `hpms_year_last_construction` |
| 102 | `hpms_year_record` |

### Mapillary Sign Detection — map_ (42)

| # | Key |
|---|---|
| 103 | `map_bollard` |
| 104 | `map_crosswalk_count` |
| 105 | `map_curve_warning` |
| 106 | `map_do_not_enter` |
| 107 | `map_fire_hydrant_count` |
| 108 | `map_guard_rail` |
| 109 | `map_keep_right` |
| 110 | `map_keep_right_count` |
| 111 | `map_no_left_turn` |
| 112 | `map_no_left_turn_count` |
| 113 | `map_no_parking` |
| 114 | `map_no_parking_count` |
| 115 | `map_no_right_turn` |
| 116 | `map_no_right_turn_count` |
| 117 | `map_no_u_turn` |
| 118 | `map_no_u_turn_count` |
| 119 | `map_one_way` |
| 120 | `map_one_way_count` |
| 121 | `map_ped_crossing_warning` |
| 122 | `map_rr_crossing_warning` |
| 123 | `map_rr_crossing_warning_count` |
| 124 | `map_school_zone` |
| 125 | `map_school_zone_count` |
| 126 | `map_signal_ahead` |
| 127 | `map_signal_count_500ft` |
| 128 | `map_signal_present` |
| 129 | `map_speed_10_count` |
| 130 | `map_speed_15_count` |
| 131 | `map_speed_20_count` |
| 132 | `map_speed_25_count` |
| 133 | `map_speed_30_count` |
| 134 | `map_speed_35_count` |
| 135 | `map_speed_40_count` |
| 136 | `map_speed_45_count` |
| 137 | `map_speed_50_count` |
| 138 | `map_speed_55_count` |
| 139 | `map_speed_5_count` |
| 140 | `map_speed_60_count` |
| 141 | `map_speed_65_count` |
| 142 | `map_speed_70_count` |
| 143 | `map_speed_limit_dist_ft` |
| 144 | `map_speed_limit_value` |
| 145 | `map_speed_sign_count_500ft` |
| 146 | `map_stop_ahead` |
| 147 | `map_stop_ahead_count` |
| 148 | `map_stop_line_count` |
| 149 | `map_stop_sign` |
| 150 | `map_stop_sign_count` |
| 151 | `map_street_light_count` |
| 152 | `map_total_features_100ft` |
| 153 | `map_turn_warning` |
| 154 | `map_turn_warning_count` |
| 155 | `map_winding_road` |
| 156 | `map_winding_road_count` |
| 157 | `map_yield_sign` |
| 158 | `map_yield_sign_count` |

### Nearest Feature — nearest_ (32)

| # | Key |
|---|---|
| 159 | `nearest_bridge_adt` |
| 160 | `nearest_bridge_dist_ft` |
| 161 | `nearest_bridge_lat` |
| 162 | `nearest_bridge_lon` |
| 163 | `nearest_bridge_width_m` |
| 164 | `nearest_bridge_year_built` |
| 165 | `nearest_node_id` |
| 166 | `nearest_poi_bar_dist_ft` |
| 167 | `nearest_poi_bar_lat` |
| 168 | `nearest_poi_bar_lon` |
| 169 | `nearest_poi_bar_name` |
| 170 | `nearest_poi_clinic_dist_ft` |
| 171 | `nearest_poi_college_dist_ft` |
| 172 | `nearest_poi_crossing_dist_ft` |
| 173 | `nearest_poi_crossing_lat` |
| 174 | `nearest_poi_crossing_lon` |
| 175 | `nearest_poi_fuel_dist_ft` |
| 176 | `nearest_poi_fuel_lat` |
| 177 | `nearest_poi_fuel_lon` |
| 178 | `nearest_poi_hospital_dist_ft` |
| 179 | `nearest_poi_parking_dist_ft` |
| 180 | `nearest_poi_parking_lat` |
| 181 | `nearest_poi_parking_lon` |
| 182 | `nearest_poi_rest_area_dist_ft` |
| 183 | `nearest_poi_restaurant_dist_ft` |
| 184 | `nearest_poi_restaurant_lat` |
| 185 | `nearest_poi_restaurant_lon` |
| 186 | `nearest_poi_signal_dist_ft` |
| 187 | `nearest_poi_signal_lat` |
| 188 | `nearest_poi_signal_lon` |
| 189 | `nearest_poi_stop_sign_dist_ft` |
| 190 | `nearest_rail_xing_dist_ft` |
| 191 | `nearest_rail_xing_lat` |
| 192 | `nearest_rail_xing_lon` |
| 193 | `nearest_school_dist_ft` |
| 194 | `nearest_school_enrollment` |
| 195 | `nearest_school_lat` |
| 196 | `nearest_school_leaid` |
| 197 | `nearest_school_level` |
| 198 | `nearest_school_lon` |
| 199 | `nearest_school_name` |
| 200 | `nearest_school_ncessch` |
| 201 | `nearest_school_type` |
| 202 | `nearest_transit_dist_ft` |
| 203 | `nearest_transit_lat` |
| 204 | `nearest_transit_lon` |
| 205 | `nearest_transit_stop_id` |
| 206 | `nearest_transit_stop_name` |
| 207 | `nearest_transit_wheelchair` |

### Legacy Near_ Proximity (3)

| # | Key |
|---|---|
| 208 | `Near_PoiClinic_1000ft` |
| 209 | `Near_PoiHospital_1000ft` |
| 210 | `Near_School_1000ft` |

### Node Matching (5)

| # | Key |
|---|---|
| 211 | `node_distance_ft` |
| 212 | `node_distance_m` |
| 213 | `node_intersection_type` |
| 214 | `node_streets_per_node` |
| 215 | `osm_u_node` |
| 216 | `osm_v_node` |

### POI Counts (12)

| # | Key |
|---|---|
| 217 | `bridge_count_500ft` |
| 218 | `poi_bar_count_1500ft` |
| 219 | `poi_clinic_count_1000ft` |
| 220 | `poi_college_count_1500ft` |
| 221 | `poi_crossing_count_100ft` |
| 222 | `poi_fuel_count_500ft` |
| 223 | `poi_hospital_count_1000ft` |
| 224 | `poi_parking_count_500ft` |
| 225 | `poi_rest_area_count_1000ft` |
| 226 | `poi_restaurant_count_500ft` |
| 227 | `poi_signal_count_100ft` |
| 228 | `poi_stop_sign_count_100ft` |
| 229 | `rail_xing_count_500ft` |
| 230 | `school_count_1000ft` |
| 231 | `transit_count_500ft` |

### Resolved Sources & Values (18)

| # | Key |
|---|---|
| 232 | `Peak_Lanes` |
| 233 | `resolved_facility_source` |
| 234 | `resolved_fc_source` |
| 235 | `resolved_lanes` |
| 236 | `resolved_lanes_source` |
| 237 | `resolved_mpo` |
| 238 | `resolved_ownership_source` |
| 239 | `resolved_place_fips` |
| 240 | `resolved_place_name` |
| 241 | `resolved_speed_source` |
| 242 | `resolved_surface_source` |
| 243 | `resolved_surface_type` |
| 244 | `Structure_Type` |
| 245 | `streets_per_node` |

### Road Inventory Match (5)

| # | Key |
|---|---|
| 246 | `ri_confidence` |
| 247 | `ri_match_dist_ft` |
| 248 | `ri_matched` |
| 249 | `ri_match_method` |
| 250 | `ri_segment_id` |

### Risk Indicators (7)

| # | Key |
|---|---|
| 251 | `risk_bridge_condition` |
| 252 | `risk_departure_curve` |
| 253 | `risk_departure_score` |
| 254 | `risk_school_exposure` |
| 255 | `risk_speed_transition` |
| 256 | `risk_speed_transition_diff` |
| 257 | `risk_unsignalized_xwalk` |

### State DOT Resolved — sdot_ (24)

| # | Key |
|---|---|
| 258 | `sdot_Area Type` |
| 259 | `sdot_DOT District` |
| 260 | `sdot_Facility Type` |
| 261 | `sdot_Functional Class` |
| 262 | `sdot_Guardrail Related?` |
| 263 | `sdot_Has_Bike_Lane` |
| 264 | `sdot_Has_Sidewalk` |
| 265 | `sdot_Is_NHS` |
| 266 | `sdot_Lane_Width_ft` |
| 267 | `sdot_match_dist_ft` |
| 268 | `sdot_matched` |
| 269 | `sdot_Median_Width_ft` |
| 270 | `sdot_Ownership` |
| 271 | `sdot_Physical Juris Name` |
| 272 | `sdot_RNS MP` |
| 273 | `sdot_Roadway Description` |
| 274 | `sdot_Roadway Surface Type` |
| 275 | `sdot_RTE Name` |
| 276 | `sdot_Segment_Length_mi` |
| 277 | `sdot_Shoulder_Width_ft` |
| 278 | `sdot_Speed_Limit_Est` |
| 279 | `sdot_Surface_Condition` |
| 280 | `sdot_SYSTEM` |
| 281 | `sdot_Through_Lanes` |

### Traffic Engineering (8)

| # | Key |
|---|---|
| 282 | `te_is_hov` |
| 283 | `te_is_toll` |
| 284 | `te_lane_utilization` |
| 285 | `te_on_structure` |
| 286 | `te_pavement_condition` |
| 287 | `te_peak_hour_volume` |
| 288 | `te_truck_pct` |

### Cross-Validation (4)

| # | Key |
|---|---|
| 289 | `xval_crosswalk_sources` |
| 290 | `xval_school_sources` |
| 291 | `xval_signal_sources` |
| 292 | `xval_stop_sign_sources` |

---

## state_extras JSONB — Delaware Extras (15 keys)

| # | Key |
|---|---|
| 1 | `de_Bicycle_Helmet_Used` |
| 2 | `de_Collision_On_Private_Property` |
| 3 | `de_Crash_Classification_Code` |
| 4 | `de_Day_Of_Week_Code` |
| 5 | `de_Day_Of_Week_Description` |
| 6 | `de_Lighting_Condition_Code` |
| 7 | `de_Manner_Of_Impact_Code` |
| 8 | `de_Motorcycle_Helmet_Used` |
| 9 | `de_Primary_Contributing_Circumstance_Code` |
| 10 | `de_Primary_Contributing_Circumstance_Description` |
| 11 | `de_Road_Surface_Code` |
| 12 | `de_School_Bus_Involved_Code` |
| 13 | `de_School_Bus_Involved_Description` |
| 14 | `de_Weather_1_Code` |
| 15 | `de_Workers_Present` |

---

## ranking_data JSONB — Rankings (76 keys)

4 tiers × 19 metrics = 76 ranking keys.
Tiers: `Juris_Rank_`, `District_Rank_`, `MPO_Rank_`, `PlanningDistrict_Rank_`

| # | Metric Suffix | Juris | District | MPO | PlanDistrict |
|---|---|---|---|---|---|
| 1 | `total_crash` | ✓ | ✓ | ✓ | ✓ |
| 2 | `total_ped_crash` | ✓ | ✓ | ✓ | ✓ |
| 3 | `total_bike_crash` | ✓ | ✓ | ✓ | ✓ |
| 4 | `total_fatal` | ✓ | ✓ | ✓ | ✓ |
| 5 | `total_fatal_serious_injury` | ✓ | ✓ | ✓ | ✓ |
| 6 | `total_epdo` | ✓ | ✓ | ✓ | ✓ |
| 7 | `trend_total_crash` | ✓ | ✓ | ✓ | ✓ |
| 8 | `trend_fatal` | ✓ | ✓ | ✓ | ✓ |
| 9 | `trend_ksi` | ✓ | ✓ | ✓ | ✓ |
| 10 | `trend_epdo` | ✓ | ✓ | ✓ | ✓ |
| 11 | `trend_ped_crash` | ✓ | ✓ | ✓ | ✓ |
| 12 | `trend_bike_crash` | ✓ | ✓ | ✓ | ✓ |
| 13 | `pct_night_fatal` | ✓ | ✓ | ✓ | ✓ |
| 14 | `pct_impaired_crash` | ✓ | ✓ | ✓ | ✓ |
| 15 | `pct_distracted_crash` | ✓ | ✓ | ✓ | ✓ |
| 16 | `pct_speed_crash` | ✓ | ✓ | ✓ | ✓ |
| 17 | `severity_index` | ✓ | ✓ | ✓ | ✓ |
| 18 | `fatality_rate` | ✓ | ✓ | ✓ | ✓ |
| 19 | `safety_score` | ✓ | ✓ | ✓ | ✓ |

---

## Speed Source Distribution (v2.7.2)

| Source | Count | Pct |
|--------|-------|-----|
| StateDOT | 360,346 | 67.2% |
| Statutory | 77,305 | 14.4% |
| Mapillary | 47,540 | 8.9% |
| OSM | 41,568 | 7.8% |
| HPMS | 9,266 | 1.7% |
| No speed | 33,804 | 5.9% |

---

## v2.7.x Fix Status

| Fix | Version | Status |
|-----|---------|--------|
| Intersection (63.6% at-int) | v2.7.0 | ✅ Confirmed |
| Curvature (23.5% curves) | v2.7.0 | ✅ Confirmed |
| School Zone (51K federal) | v2.7.1 | ✅ Confirmed |
| Not-tracked flags (all NULL) | v2.7.1 | ✅ Confirmed |
| Phantom MSD (0 zeros) | v2.7.1 | ✅ Confirmed |
| Statutory speed fallback | v2.7.2 | ✅ Confirmed |
| Geom trigger (auto on INSERT) | infra | ✅ Active on all 52 tables |
