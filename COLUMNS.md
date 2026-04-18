# CrashLens Column Registry v1.0

Generated from Delaware non_dot_roads (121,733 rows × 532 cols)
Date: 2026-04-15

## Purpose

Single source of truth for ALL column names in the CrashLens pipeline.
Prevents column name mismatch bugs (e.g., sdot_Max Speed Diff vs sdot_Speed_Limit_Est).
Every pipeline script MUST reference this file, not hardcode column names.

## Column Name Rules

1. Golden Schema (1-69): Title Case with spaces — `Crash Severity`, `Max Speed Diff`
2. Enrichment/resolved: snake_case — `resolved_speed_limit`, `ri_matched`
3. HPMS: `hpms_` prefix — `hpms_speed_limit`, `hpms_aadt`
4. State DOT raw: `dot_` prefix — `dot_road_name`, `dot_lanes`
5. State DOT resolved: `sdot_` prefix — `sdot_Speed_Limit_Est`, `sdot_Through_Lanes`
6. Mapillary: `map_` prefix — `map_signal_present`, `map_speed_limit_value`
7. Rankings: `{Tier}_Rank_{metric}` — `Juris_Rank_total_crash`
8. State extras: `{abbr}_` prefix — `de_Day_Of_Week_Code`
9. POI proximity: `Near_Poi{Type}_{radius}` — `Near_PoiSignal_100ft`
10. Federal proximity: `Near_{Asset}_{radius}` — `Near_Bridge_500ft`

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
| `resolved_speed_limit` | crash_enricher.py → Max Speed Diff | Frontend speed |
| `Intersection Name` | crash_enricher.py → RTE Name fallback | Frontend route |

---

### Golden Schema — Identity (5)
_Core crash identification fields_

| # | Column | Type | Fill % |
|---|---|---|---|
| 1 | `OBJECTID` | str | 100% |
| 2 | `Document Nbr` | str | 100% |
| 3 | `Crash Year` | str | 100% |
| 4 | `Crash Date` | str | 100% |
| 5 | `Crash Military Time` | str | 100% |

### Golden Schema — Severity (7)
_Injury counts and severity_

| # | Column | Type | Fill % |
|---|---|---|---|
| 6 | `Crash Severity` | str | 100% |
| 7 | `K_People` | str | 0% |
| 8 | `A_People` | str | 12% |
| 9 | `B_People` | str | 0% |
| 10 | `C_People` | str | 0% |
| 11 | `Persons Injured` | str | 12% |
| 12 | `Pedestrians Killed` | str | 0% |
| 13 | `Pedestrians Injured` | str | 1% |
| 14 | `Vehicle Count` | str | 0% |

### Golden Schema — Crash Characteristics (8)
_Collision and conditions_

| # | Column | Type | Fill % |
|---|---|---|---|
| 15 | `Collision Type` | str | 100% |
| 16 | `Weather Condition` | str | 100% |
| 17 | `Light Condition` | str | 100% |
| 18 | `Roadway Surface Condition` | str | 100% |
| 19 | `Relation To Roadway` | str | 1% |
| 20 | `Roadway Alignment` | str | 100% |
| 21 | `Roadway Surface Type` | str | 100% |
| 22 | `Roadway Defect` | str | 0% |

### Golden Schema — Road Description (3)
_Road type and intersection_

| # | Column | Type | Fill % |
|---|---|---|---|
| 23 | `Roadway Description` | str | 100% |
| 24 | `Intersection Type` | str | 100% |
| 25 | `Traffic Control Type` | str | 11% |
| 26 | `Traffic Control Status` | str | 0% |

### Golden Schema — Work/School Zone (4)
_Zone flags_

| # | Column | Type | Fill % |
|---|---|---|---|
| 27 | `Work Zone Related` | str | 100% |
| 28 | `Work Zone Location` | str | 0% |
| 29 | `Work Zone Type` | str | 0% |
| 30 | `School Zone` | str | 100% |

### Golden Schema — Harmful Events (2)
_First harmful event_

| # | Column | Type | Fill % |
|---|---|---|---|
| 31 | `First Harmful Event` | str | 0% |
| 32 | `First Harmful Event Loc` | str | 0% |

### Golden Schema — Safety Flags (13)
_Boolean Yes/No/empty crash flags_

| # | Column | Type | Fill % |
|---|---|---|---|
| 33 | `Alcohol?` | str | 100% |
| 34 | `Animal Related?` | str | 100% |
| 35 | `Unrestrained?` | str | 100% |
| 36 | `Bike?` | str | 100% |
| 37 | `Distracted?` | str | 100% |
| 38 | `Drowsy?` | str | 0% |
| 39 | `Drug Related?` | str | 100% |
| 40 | `Guardrail Related?` | str | 25% |
| 41 | `Hitrun?` | str | 0% |
| 42 | `Lgtruck?` | str | 0% |
| 43 | `Motorcycle?` | str | 100% |
| 44 | `Pedestrian?` | str | 100% |
| 45 | `Speed?` | str | 100% |

### Golden Schema — Analysis Fields (6)
_Derived analysis columns_

| # | Column | Type | Fill % |
|---|---|---|---|
| 46 | `Max Speed Diff` | str | 100% |
| 47 | `RoadDeparture Type` | str | 0% |
| 48 | `Intersection Analysis` | str | 100% |
| 49 | `Senior?` | str | 0% |
| 50 | `Young?` | str | 0% |
| 51 | `Mainline?` | str | 100% |
| 52 | `Night?` | str | 100% |

### Golden Schema — Geography (11)
_Location and jurisdiction_

| # | Column | Type | Fill % |
|---|---|---|---|
| 53 | `DOT District` | str | 100% |
| 54 | `Juris Code` | str | 100% |
| 55 | `Physical Juris Name` | str | 100% |
| 56 | `Functional Class` | str | 100% |
| 57 | `Facility Type` | str | 100% |
| 58 | `Area Type` | str | 100% |
| 59 | `SYSTEM` | str | 100% |
| 60 | `VSP` | str | 0% |
| 61 | `Ownership` | str | 100% |
| 62 | `Planning District` | str | 100% |
| 63 | `MPO Name` | str | 100% |

### Golden Schema — Route/Node (5)
_Route and node location_

| # | Column | Type | Fill % |
|---|---|---|---|
| 64 | `RTE Name` | str | 25% |
| 65 | `RNS MP` | str | 22% |
| 66 | `Node` | str | 100% |
| 67 | `Node Offset (ft)` | str | 0% |
| 68 | `x` | str | 100% |
| 69 | `y` | str | 100% |

### Enrichment Core (7)
_Pipeline-added identity/scoring_

| # | Column | Type | Fill % |
|---|---|---|---|
| 70 | `FIPS` | str | 100% |
| 71 | `Place FIPS` | str | 67% |
| 72 | `EPDO_Score` | str | 100% |
| 73 | `Intersection Name` | str | 99% |
| 74 | `resolved_place_fips` | str | 67% |
| 75 | `resolved_place_name` | str | 67% |
| 76 | `resolved_mpo` | str | 80% |

### Resolved Attributes (16)
_Multi-source resolved values (road_data_authority.py)_

| # | Column | Type | Fill % |
|---|---|---|---|
| 74 | `resolved_place_fips` | str | 67% |
| 75 | `resolved_place_name` | str | 67% |
| 76 | `resolved_mpo` | str | 80% |
| 179 | `resolved_bridge_source` | str | 2% |
| 180 | `resolved_facility_source` | str | 9% |
| 181 | `resolved_fc_source` | str | 100% |
| 182 | `resolved_has_lighting` | str | 100% |
| 183 | `resolved_has_signal` | str | 100% |
| 184 | `resolved_lanes` | str | 100% |
| 185 | `resolved_lanes_source` | str | 36% |
| 186 | `resolved_lighting_source` | str | 11% |
| 187 | `resolved_on_bridge` | str | 100% |
| 188 | `resolved_ownership_source` | str | 100% |
| 189 | `resolved_school_source` | str | 20% |
| 190 | `resolved_school_zone` | str | 100% |
| 191 | `resolved_signal_source` | str | 8% |
| 192 | `resolved_speed_limit` | str | 100% |
| 193 | `resolved_speed_source` | str | 36% |
| 194 | `resolved_surface_source` | str | 44% |
| 195 | `resolved_surface_type` | str | 44% |

### Confidence Scores (6)
_Cross-source validation confidence 0-100_

| # | Column | Type | Fill % |
|---|---|---|---|
| 196 | `conf_bridge` | str | 100% |
| 197 | `conf_crosswalk` | str | 100% |
| 198 | `conf_school_zone` | str | 100% |
| 199 | `conf_signal` | str | 100% |
| 200 | `conf_speed_limit` | str | 100% |
| 201 | `conf_stop_sign` | str | 100% |

### Cross-Validation Counts (4)
_Number of sources confirming attribute_

| # | Column | Type | Fill % |
|---|---|---|---|
| 202 | `xval_crosswalk_sources` | str | 100% |
| 203 | `xval_school_sources` | str | 100% |
| 204 | `xval_signal_sources` | str | 100% |
| 205 | `xval_stop_sign_sources` | str | 100% |

### Risk Indicators (7)
_Derived safety risk scores_

| # | Column | Type | Fill % |
|---|---|---|---|
| 206 | `risk_bridge_condition` | str | 100% |
| 207 | `risk_departure_curve` | str | 100% |
| 208 | `risk_departure_score` | str | 100% |
| 209 | `risk_school_exposure` | str | 100% |
| 210 | `risk_speed_transition` | str | 100% |
| 211 | `risk_speed_transition_diff` | str | 100% |
| 212 | `risk_unsignalized_xwalk` | str | 100% |

### Curve Analysis (8)
_Curvature classification and risk_

| # | Column | Type | Fill % |
|---|---|---|---|
| 213 | `curve_advisory_speed` | str | 100% |
| 214 | `curve_class` | str | 100% |
| 215 | `curve_class_label` | str | 100% |
| 216 | `curve_has_warning_sign` | str | 100% |
| 217 | `curve_is_curve` | str | 100% |
| 218 | `curve_risk_score` | str | 100% |
| 219 | `curve_speed_differential` | str | 100% |
| 220 | `curve_warning_sign_type` | str | 0% |

### Traffic Engineering (9)
_HPMS-derived engineering metrics_

| # | Column | Type | Fill % |
|---|---|---|---|
| 221 | `te_is_hov` | str | 100% |
| 222 | `te_is_toll` | str | 100% |
| 223 | `te_lane_utilization` | str | 100% |
| 224 | `te_level_of_service` | str | 0% |
| 225 | `te_on_structure` | str | 100% |
| 226 | `te_pavement_condition` | str | 0% |
| 227 | `te_peak_hour_volume` | str | 100% |
| 228 | `te_truck_pct` | str | 100% |

### Geometry & Matching (11)
_Road inventory match and geometry_

| # | Column | Type | Fill % |
|---|---|---|---|
| 172 | `Through_Lanes` | str | 100% |
| 173 | `AADT` | str | 100% |
| 174 | `AADT_source` | str | 44% |
| 175 | `geometry_coords` | str | 0% |
| 176 | `length_m` | str | 100% |
| 177 | `divider` | str | 0% |
| 178 | `curvature` | str | 100% |
| 229 | `is_intersection` | str | 100% |
| 230 | `intersection_degree` | str | 100% |
| 231 | `is_ramp` | str | 100% |
| 232 | `ramp_type` | str | 1% |

### HPMS Federal Road Inventory (54)
_Source: geo.dot.gov FeatureServer, 2024 endpoints, generate_hpms_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 233 | `hpms_aadt_combination` | str | 100% |
| 234 | `hpms_aadt_single_unit` | str | 100% |
| 235 | `hpms_access_control` | str | 100% |
| 236 | `hpms_begin_point` | str | 100% |
| 237 | `hpms_capacity` | str | 100% |
| 238 | `hpms_climate_zone` | str | 100% |
| 239 | `hpms_county_code` | str | 0% |
| 240 | `hpms_cracking_percent` | str | 100% |
| 241 | `hpms_curve_class` | str | 0% |
| 242 | `hpms_design_speed` | str | 100% |
| 243 | `hpms_dir_factor` | str | 100% |
| 244 | `hpms_directional_through_lanes` | str | 100% |
| 245 | `hpms_end_point` | str | 100% |
| 246 | `hpms_faulting` | str | 100% |
| 247 | `hpms_future_aadt` | str | 100% |
| 248 | `hpms_grade_class` | str | 0% |
| 249 | `hpms_hov_type` | str | 100% |
| 250 | `hpms_iri` | str | 100% |
| 251 | `hpms_k_factor` | str | 100% |
| 252 | `hpms_lane_width` | str | 100% |
| 253 | `hpms_length_mi` | str | 100% |
| 254 | `hpms_median_type` | str | 100% |
| 255 | `hpms_median_width` | str | 100% |
| 256 | `hpms_nhs` | str | 100% |
| 257 | `hpms_num_signalized_int` | str | 100% |
| 258 | `hpms_num_signals` | str | 100% |
| 259 | `hpms_num_stop_int` | str | 100% |
| 260 | `hpms_pct_green_time` | str | 100% |
| 261 | `hpms_pct_peak_combination` | str | 100% |
| 262 | `hpms_pct_peak_single` | str | 100% |
| 263 | `hpms_peak_lanes` | str | 100% |
| 264 | `hpms_peak_parking` | str | 100% |
| 265 | `hpms_psr` | str | 100% |
| 266 | `hpms_route_id` | str | 9% |
| 267 | `hpms_route_name` | str | 0% |
| 268 | `hpms_route_number` | str | 100% |
| 269 | `hpms_route_signing` | str | 100% |
| 270 | `hpms_rutting` | str | 100% |
| 271 | `hpms_section_length` | str | 100% |
| 272 | `hpms_shoulder_type_l` | str | 100% |
| 273 | `hpms_shoulder_type_r` | str | 100% |
| 274 | `hpms_shoulder_width_l` | str | 100% |
| 275 | `hpms_shoulder_width_r` | str | 100% |
| 276 | `hpms_signal_type` | str | 100% |
| 277 | `hpms_state_code` | str | 100% |
| 278 | `hpms_structure_type` | str | 100% |
| 279 | `hpms_terrain_type` | str | 100% |
| 280 | `hpms_toll_charged` | str | 100% |
| 281 | `hpms_turn_lanes_l` | str | 100% |
| 282 | `hpms_turn_lanes_r` | str | 100% |
| 283 | `hpms_urban_code` | str | 0% |
| 284 | `hpms_widening_feasibility` | str | 100% |
| 285 | `hpms_year_last_construction` | str | 100% |
| 286 | `hpms_year_record` | str | 100% |

### Federal Assets — Bridges (11)
_Source: NBI/BTS, generate_federal_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 287 | `bridge_count_500ft` | str | 100% |
| 288 | `nearest_bridge_adt` | str | 100% |
| 289 | `nearest_bridge_condition` | str | 0% |
| 290 | `nearest_bridge_dist_ft` | str | 100% |
| 291 | `nearest_bridge_facility` | str | 0% |
| 292 | `nearest_bridge_feature` | str | 0% |
| 293 | `nearest_bridge_lanes` | str | 0% |
| 294 | `nearest_bridge_status` | str | 0% |
| 295 | `nearest_bridge_structure_id` | str | 0% |
| 296 | `nearest_bridge_width_m` | str | 100% |
| 297 | `nearest_bridge_year_built` | str | 100% |

### Federal Assets — Rail Crossings (8)
_Source: FRA/BTS, generate_federal_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 298 | `nearest_rail_xing_dist_ft` | str | 100% |
| 299 | `nearest_rail_xing_id` | str | 0% |
| 300 | `nearest_rail_xing_railroad` | str | 0% |
| 301 | `nearest_rail_xing_street` | str | 0% |
| 302 | `nearest_rail_xing_trains_per_day` | str | 0% |
| 303 | `nearest_rail_xing_warning_device` | str | 0% |
| 304 | `nearest_rail_xing_warning_level` | str | 0% |
| 305 | `rail_xing_count_500ft` | str | 100% |

### Federal Assets — Schools (6)
_Source: Urban Institute API, generate_federal_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 306 | `nearest_school_dist_ft` | str | 100% |
| 307 | `nearest_school_enrollment` | str | 100% |
| 308 | `nearest_school_level` | str | 100% |
| 309 | `nearest_school_name` | str | 0% |
| 310 | `nearest_school_type` | str | 100% |
| 311 | `school_count_1500ft` | str | 100% |

### Federal Assets — Transit (5)
_Source: NTM/BTS, generate_federal_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 312 | `nearest_transit_dist_ft` | str | 100% |
| 313 | `nearest_transit_stop_id` | str | 1% |
| 314 | `nearest_transit_stop_name` | str | 1% |
| 315 | `nearest_transit_wheelchair` | str | 1% |
| 316 | `transit_count_500ft` | str | 100% |

### Proximity Flags (15)
_Federal + POI proximity flags from build_road_inventory.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 317 | `Near_PoiBar_1500ft` | str | 100% |
| 318 | `Near_PoiClinic_1000ft` | str | 100% |
| 319 | `Near_PoiCollege_1500ft` | str | 100% |
| 320 | `Near_PoiCrossing_100ft` | str | 100% |
| 321 | `Near_PoiFuel_500ft` | str | 100% |
| 322 | `Near_PoiHospital_1000ft` | str | 100% |
| 323 | `Near_PoiParking_500ft` | str | 100% |
| 324 | `Near_PoiRestArea_1000ft` | str | 100% |
| 325 | `Near_PoiRestaurant_500ft` | str | 100% |
| 326 | `Near_PoiSignal_100ft` | str | 100% |
| 327 | `Near_PoiStopSign_100ft` | str | 100% |
| 510 | `Near_Bridge_500ft` | str | 100% |
| 511 | `Near_RailXing_500ft` | str | 100% |
| 512 | `Near_School_1000ft` | str | 100% |
| 513 | `Near_Transit_500ft` | str | 100% |

### POI Details (41)
_Nearest POI details from OSM, build_road_inventory.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 328 | `nearest_poi_bar_dist_ft` | str | 100% |
| 329 | `nearest_poi_bar_name` | str | 0% |
| 330 | `nearest_poi_bar_subcategory` | str | 0% |
| 331 | `nearest_poi_clinic_dist_ft` | str | 100% |
| 332 | `nearest_poi_clinic_name` | str | 0% |
| 333 | `nearest_poi_college_dist_ft` | str | 100% |
| 334 | `nearest_poi_college_name` | str | 0% |
| 335 | `nearest_poi_college_subcategory` | str | 0% |
| 336 | `nearest_poi_crossing_dist_ft` | str | 100% |
| 337 | `nearest_poi_crossing_lat` | str | 100% |
| 338 | `nearest_poi_crossing_lon` | str | 100% |
| 339 | `nearest_poi_crossing_name` | str | 0% |
| 340 | `nearest_poi_fuel_dist_ft` | str | 100% |
| 341 | `nearest_poi_fuel_name` | str | 0% |
| 342 | `nearest_poi_hospital_dist_ft` | str | 100% |
| 343 | `nearest_poi_hospital_name` | str | 0% |
| 344 | `nearest_poi_parking_dist_ft` | str | 100% |
| 345 | `nearest_poi_parking_lat` | str | 100% |
| 346 | `nearest_poi_parking_lon` | str | 100% |
| 347 | `nearest_poi_parking_name` | str | 1% |
| 348 | `nearest_poi_rest_area_dist_ft` | str | 100% |
| 349 | `nearest_poi_rest_area_name` | str | 0% |
| 350 | `nearest_poi_rest_area_subcategory` | str | 0% |
| 351 | `nearest_poi_restaurant_dist_ft` | str | 100% |
| 352 | `nearest_poi_restaurant_name` | str | 3% |
| 353 | `nearest_poi_restaurant_subcategory` | str | 3% |
| 354 | `nearest_poi_signal_dist_ft` | str | 100% |
| 355 | `nearest_poi_signal_name` | str | 0% |
| 356 | `nearest_poi_stop_sign_dist_ft` | str | 100% |
| 357 | `nearest_poi_stop_sign_name` | str | 0% |
| 358 | `poi_bar_count_1500ft` | str | 100% |
| 359 | `poi_clinic_count_1500ft` | str | 100% |
| 360 | `poi_college_count_1500ft` | str | 100% |
| 361 | `poi_crossing_count_100ft` | str | 100% |
| 362 | `poi_fuel_count_500ft` | str | 100% |
| 363 | `poi_hospital_count_2000ft` | str | 100% |
| 364 | `poi_parking_count_500ft` | str | 100% |
| 365 | `poi_rest_area_count_1000ft` | str | 100% |
| 366 | `poi_restaurant_count_500ft` | str | 100% |
| 367 | `poi_signal_count_100ft` | str | 100% |
| 368 | `poi_stop_sign_count_100ft` | str | 100% |

### Mapillary Street-Level (57)
_Source: Mapillary API, mapillary_county_download.py → build_road_inventory.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 369 | `map_do_not_enter` | str | 100% |
| 370 | `map_keep_right` | str | 100% |
| 371 | `map_keep_right_count` | str | 100% |
| 372 | `map_no_left_turn` | str | 100% |
| 373 | `map_no_left_turn_count` | str | 100% |
| 374 | `map_no_parking` | str | 100% |
| 375 | `map_no_parking_count` | str | 100% |
| 376 | `map_no_right_turn` | str | 100% |
| 377 | `map_no_right_turn_count` | str | 100% |
| 378 | `map_no_u_turn` | str | 100% |
| 379 | `map_no_u_turn_count` | str | 100% |
| 380 | `map_one_way` | str | 100% |
| 381 | `map_one_way_count` | str | 100% |
| 382 | `map_stop_sign` | str | 100% |
| 383 | `map_stop_sign_count` | str | 100% |
| 384 | `map_yield_sign` | str | 100% |
| 385 | `map_yield_sign_count` | str | 100% |
| 386 | `map_curve_warning` | str | 100% |
| 387 | `map_ped_crossing_warning` | str | 100% |
| 388 | `map_rr_crossing_warning` | str | 100% |
| 389 | `map_rr_crossing_warning_count` | str | 100% |
| 390 | `map_signal_ahead` | str | 100% |
| 391 | `map_stop_ahead` | str | 100% |
| 392 | `map_stop_ahead_count` | str | 100% |
| 393 | `map_turn_warning` | str | 100% |
| 394 | `map_turn_warning_count` | str | 100% |
| 395 | `map_winding_road` | str | 100% |
| 396 | `map_winding_road_count` | str | 100% |
| 397 | `map_school_zone` | str | 100% |
| 398 | `map_school_zone_count` | str | 100% |
| 399 | `map_speed_10_count` | str | 100% |
| 400 | `map_speed_15_count` | str | 100% |
| 401 | `map_speed_20_count` | str | 100% |
| 402 | `map_speed_25_count` | str | 100% |
| 403 | `map_speed_30_count` | str | 100% |
| 404 | `map_speed_35_count` | str | 100% |
| 405 | `map_speed_40_count` | str | 100% |
| 406 | `map_speed_45_count` | str | 100% |
| 407 | `map_speed_50_count` | str | 100% |
| 408 | `map_speed_55_count` | str | 100% |
| 409 | `map_speed_5_count` | str | 100% |
| 410 | `map_speed_60_count` | str | 100% |
| 411 | `map_speed_65_count` | str | 100% |
| 412 | `map_speed_70_count` | str | 100% |
| 413 | `map_speed_limit_dist_ft` | str | 23% |
| 414 | `map_speed_limit_value` | str | 23% |
| 415 | `map_speed_sign_count_500ft` | str | 100% |
| 416 | `map_signal_count_500ft` | str | 100% |
| 417 | `map_signal_heads` | str | 7% |
| 418 | `map_signal_present` | str | 100% |
| 419 | `map_bollard` | str | 100% |
| 420 | `map_crosswalk_count` | str | 100% |
| 421 | `map_fire_hydrant_count` | str | 100% |
| 422 | `map_guard_rail` | str | 100% |
| 423 | `map_stop_line_count` | str | 100% |
| 424 | `map_street_light_count` | str | 100% |
| 425 | `map_total_features_100ft` | str | 100% |

### OSM Graph Nodes (3)
_Source: osmnx, generate_osm_data.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 426 | `osm_u_node` | str | 100% |
| 427 | `osm_v_node` | str | 100% |
| 428 | `streets_per_node` | str | 100% |

### State DOT Raw — Delaware (50)
_Source: DelDOT shapefile, generate_state_dot_data.py → build_road_inventory.py_

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

### State DOT Raw — Colorado (25)
_Source: CDOT CPLAN FeatureServer Layer 7 (Highways), `states/colorado/co_state_dot.py` → `generate_state_dot_data.py`_
_Local numbering — fill-% TBD (not yet in the canonical Delaware parquet)._

| # | Column | Type | Fill % |
|---|---|---|---|
| CO-1  | `dot_fc_text`          | str | TBD |
| CO-2  | `dot_route_sign`       | str | TBD |
| CO-3  | `dot_nhs`              | str | TBD |
| CO-4  | `dot_access_control`   | str | TBD |
| CO-5  | `dot_description`      | str | TBD |
| CO-6  | `dot_county_name`      | str | TBD |
| CO-7  | `dot_county_fips`      | str | TBD |
| CO-8  | `dot_city`             | str | TBD |
| CO-9  | `dot_city_fips`        | str | TBD |
| CO-10 | `dot_region_code`      | str | TBD |
| CO-11 | `dot_tpr_id`           | str | TBD |
| CO-12 | `dot_terrain`          | str | TBD |
| CO-13 | `dot_surface_type`     | str | TBD |
| CO-14 | `dot_is_divided`       | str | TBD |
| CO-15 | `dot_median_type`      | str | TBD |
| CO-16 | `dot_median_width`     | str | TBD |
| CO-17 | `dot_shoulder_type`    | str | TBD |
| CO-18 | `dot_shoulder_width`   | str | TBD |
| CO-19 | `dot_aadt`             | str | TBD |
| CO-20 | `dot_aadt_single`      | str | TBD |
| CO-21 | `dot_aadt_combo`       | str | TBD |
| CO-22 | `dot_speed_limit`      | str | TBD |
| CO-23 | `dot_vmt`              | str | TBD |
| CO-24 | `dot_vc_ratio`         | str | TBD |
| CO-25 | `dot_seg_length`       | str | TBD |

Shared with Delaware (already in the `dot_*` section above, same column name, reused by Colorado):
`dot_road_name`, `dot_route_number`, `dot_lanes`, `dot_lane_width`, `dot_beg_mp`, `dot_end_mp`.

Also written by `normalize()` on every state row (not state-specific): `dot_source`, `dot_source_url`.

### State DOT Resolved — sdot_ (24)
_Source: generate_state_dot_data.py → build_road_inventory.py enrich_state_dot()_
_⚠️ These are the ACTUAL column names. Code must match EXACTLY._

| # | Column | Type | Fill % | Used By |
|---|---|---|---|---|
| 479 | `sdot_Functional Class` | str | 25% | merge_frontend_columns() Tier A |
| 480 | `sdot_SYSTEM` | str | 25% |  |
| 481 | `sdot_Ownership` | str | 25% | merge_frontend_columns() Tier A |
| 482 | `sdot_Facility Type` | str | 25% |  |
| 483 | `sdot_Roadway Surface Type` | str | 25% | resolve_surface() Tier A |
| 484 | `sdot_Area Type` | str | 25% |  |
| 485 | `sdot_DOT District` | str | 25% |  |
| 486 | `sdot_Physical Juris Name` | str | 25% |  |
| 487 | `sdot_Through_Lanes` | str | 25% | resolve_lanes() Tier A |
| 488 | `sdot_RTE Name` | str | 1% | merge_frontend_columns() Tier A |
| 489 | `sdot_RNS MP` | str | 100% |  |
| 490 | `sdot_Segment_Length_mi` | str | 100% |  |
| 491 | `sdot_Lane_Width_ft` | str | 100% |  |
| 492 | `sdot_Median_Width_ft` | str | 100% |  |
| 493 | `sdot_Shoulder_Width_ft` | str | 100% |  |
| 494 | `sdot_Has_Sidewalk` | str | 25% |  |
| 495 | `sdot_Guardrail Related?` | str | 25% |  |
| 496 | `sdot_Has_Bike_Lane` | str | 25% |  |
| 497 | `sdot_Roadway Description` | str | 25% |  |
| 498 | `sdot_Is_NHS` | str | 25% |  |
| 499 | `sdot_Surface_Condition` | str | 1% |  |
| 500 | `sdot_Speed_Limit_Est` | str | 25% | resolve_speed_limit() Tier A |
| 501 | `sdot_match_dist_ft` | str | 100% |  |
| 502 | `sdot_matched` | str | 100% |  |

### Rankings (76)
_Computed by crash_enricher.py ranking module_

| # | Column | Type |
|---|---|---|
| 77 | `Juris_Rank_total_crash` | str |
| 78 | `District_Rank_total_crash` | str |
| 79 | `MPO_Rank_total_crash` | str |
| 80 | `PlanningDistrict_Rank_total_crash` | str |
| 81 | `Juris_Rank_total_ped_crash` | str |
| 82 | `District_Rank_total_ped_crash` | str |
| 83 | `MPO_Rank_total_ped_crash` | str |
| 84 | `PlanningDistrict_Rank_total_ped_crash` | str |
| 85 | `Juris_Rank_total_bike_crash` | str |
| 86 | `District_Rank_total_bike_crash` | str |
| 87 | `MPO_Rank_total_bike_crash` | str |
| 88 | `PlanningDistrict_Rank_total_bike_crash` | str |
| 89 | `Juris_Rank_total_fatal` | str |
| 90 | `District_Rank_total_fatal` | str |
| 91 | `MPO_Rank_total_fatal` | str |
| 92 | `PlanningDistrict_Rank_total_fatal` | str |
| 93 | `Juris_Rank_total_fatal_serious_injury` | str |
| 94 | `District_Rank_total_fatal_serious_injury` | str |
| 95 | `MPO_Rank_total_fatal_serious_injury` | str |
| 96 | `PlanningDistrict_Rank_total_fatal_serious_injury` | str |
| 97 | `Juris_Rank_total_epdo` | str |
| 98 | `District_Rank_total_epdo` | str |
| 99 | `MPO_Rank_total_epdo` | str |
| 100 | `PlanningDistrict_Rank_total_epdo` | str |
| 101 | `Juris_Rank_trend_total_crash` | str |
| 102 | `District_Rank_trend_total_crash` | str |
| 103 | `MPO_Rank_trend_total_crash` | str |
| 104 | `PlanningDistrict_Rank_trend_total_crash` | str |
| 105 | `Juris_Rank_trend_fatal` | str |
| 106 | `District_Rank_trend_fatal` | str |
| 107 | `MPO_Rank_trend_fatal` | str |
| 108 | `PlanningDistrict_Rank_trend_fatal` | str |
| 109 | `Juris_Rank_trend_ksi` | str |
| 110 | `District_Rank_trend_ksi` | str |
| 111 | `MPO_Rank_trend_ksi` | str |
| 112 | `PlanningDistrict_Rank_trend_ksi` | str |
| 113 | `Juris_Rank_trend_epdo` | str |
| 114 | `District_Rank_trend_epdo` | str |
| 115 | `MPO_Rank_trend_epdo` | str |
| 116 | `PlanningDistrict_Rank_trend_epdo` | str |
| 117 | `Juris_Rank_trend_ped_crash` | str |
| 118 | `District_Rank_trend_ped_crash` | str |
| 119 | `MPO_Rank_trend_ped_crash` | str |
| 120 | `PlanningDistrict_Rank_trend_ped_crash` | str |
| 121 | `Juris_Rank_trend_bike_crash` | str |
| 122 | `District_Rank_trend_bike_crash` | str |
| 123 | `MPO_Rank_trend_bike_crash` | str |
| 124 | `PlanningDistrict_Rank_trend_bike_crash` | str |
| 125 | `Juris_Rank_pct_night_fatal` | str |
| 126 | `District_Rank_pct_night_fatal` | str |
| 127 | `MPO_Rank_pct_night_fatal` | str |
| 128 | `PlanningDistrict_Rank_pct_night_fatal` | str |
| 129 | `Juris_Rank_pct_impaired_crash` | str |
| 130 | `District_Rank_pct_impaired_crash` | str |
| 131 | `MPO_Rank_pct_impaired_crash` | str |
| 132 | `PlanningDistrict_Rank_pct_impaired_crash` | str |
| 133 | `Juris_Rank_pct_distracted_crash` | str |
| 134 | `District_Rank_pct_distracted_crash` | str |
| 135 | `MPO_Rank_pct_distracted_crash` | str |
| 136 | `PlanningDistrict_Rank_pct_distracted_crash` | str |
| 137 | `Juris_Rank_pct_speed_crash` | str |
| 138 | `District_Rank_pct_speed_crash` | str |
| 139 | `MPO_Rank_pct_speed_crash` | str |
| 140 | `PlanningDistrict_Rank_pct_speed_crash` | str |
| 141 | `Juris_Rank_severity_index` | str |
| 142 | `District_Rank_severity_index` | str |
| 143 | `MPO_Rank_severity_index` | str |
| 144 | `PlanningDistrict_Rank_severity_index` | str |
| 145 | `Juris_Rank_fatality_rate` | str |
| 146 | `District_Rank_fatality_rate` | str |
| 147 | `MPO_Rank_fatality_rate` | str |
| 148 | `PlanningDistrict_Rank_fatality_rate` | str |
| 149 | `Juris_Rank_safety_score` | str |
| 150 | `District_Rank_safety_score` | str |
| 151 | `MPO_Rank_safety_score` | str |
| 152 | `PlanningDistrict_Rank_safety_score` | str |

### State Extras — Delaware (19)
_Source columns preserved from de_normalize.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 153 | `de_Day_Of_Week_Code` | str | 100% |
| 154 | `de_Day_Of_Week_Description` | str | 100% |
| 155 | `de_Crash_Classification_Code` | str | 100% |
| 156 | `de_Collision_On_Private_Property` | str | 100% |
| 157 | `de_Manner_Of_Impact_Code` | str | 95% |
| 158 | `de_Road_Surface_Code` | str | 95% |
| 159 | `de_Lighting_Condition_Code` | str | 95% |
| 160 | `de_Weather_1_Code` | str | 95% |
| 161 | `de_Weather_2_Code` | str | 4% |
| 162 | `de_Weather_2_Description` | str | 4% |
| 163 | `de_Motorcycle_Helmet_Used` | str | 100% |
| 164 | `de_Bicycle_Helmet_Used` | str | 100% |
| 165 | `de_Primary_Contributing_Circumstance_Code` | str | 98% |
| 166 | `de_Primary_Contributing_Circumstance_Description` | str | 98% |
| 167 | `de_School_Bus_Involved_Code` | str | 100% |
| 168 | `de_School_Bus_Involved_Description` | str | 100% |
| 169 | `de_Work_Zone_Location_Code` | str | 0% |
| 170 | `de_Work_Zone_Type_Code` | str | 0% |
| 171 | `de_Workers_Present` | str | 100% |

### Frontend Merged (16)
_Merged from multiple sources in merge_frontend_columns()_

| # | Column | Type | Fill % |
|---|---|---|---|
| 503 | `Lane_Width_ft` | str | 25% |
| 504 | `Median_Width_ft` | str | 1% |
| 505 | `Shoulder_Width_ft` | str | 9% |
| 506 | `Has_Sidewalk` | str | 25% |
| 507 | `Has_Bike_Lane` | str | 25% |
| 508 | `Urban_Area_Name` | str | 86% |
| 509 | `Urban_Area_GEOID` | str | 86% |
| 517 | `Peak_Lanes` | str | 100% |
| 518 | `Structure_Type` | str | 100% |
| 519 | `Cracking_Pct` | str | 100% |
| 520 | `ARNOLD_Route_ID` | str | 9% |
| 521 | `ARNOLD_Begin_MP` | str | 100% |
| 522 | `ARNOLD_End_MP` | str | 100% |
| 514 | `road_lon` | str | 100% |
| 515 | `road_lat` | str | 100% |
| 516 | `length_ft` | str | 100% |

### Road & Node Matching (10)
_Spatial match results from crash_enricher.py_

| # | Column | Type | Fill % |
|---|---|---|---|
| 523 | `ri_matched` | str | 100% |
| 524 | `ri_match_dist_ft` | str | 100% |
| 525 | `ri_confidence` | str | 100% |
| 526 | `ri_segment_id` | str | 100% |
| 527 | `ri_match_method` | str | 100% |
| 528 | `node_intersection_type` | str | 100% |
| 529 | `nearest_node_id` | str | 100% |
| 530 | `node_distance_m` | str | 100% |
| 531 | `node_distance_ft` | str | 100% |
| 532 | `node_streets_per_node` | str | 100% |
