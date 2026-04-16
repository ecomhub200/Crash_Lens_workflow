-- Migration 003: Safety ranking matviews for Delaware
-- (schools, hospitals, transit, rail crossings).
--
-- All matviews query JSONB (most attribute keys live in road_data), but the
-- flag column is Tier 1 (near_school_1000ft, etc.). Each matview has a
-- UNIQUE INDEX so REFRESH MATERIALIZED VIEW CONCURRENTLY works.

-- ═══ 1. Schools — 236 rows ═══
CREATE MATERIALIZED VIEW IF NOT EXISTS schools_safety_delaware AS
WITH crash_schools AS (
    SELECT
        road_data->>'nearest_school_ncessch'                         AS ncessch,
        road_data->>'nearest_school_name'                            AS school_name,
        road_data->>'nearest_school_leaid'                           AS leaid,
        NULLIF(road_data->>'nearest_school_enrollment','')::numeric  AS enrollment,
        road_data->>'nearest_school_level'                           AS school_level,
        road_data->>'nearest_school_type'                            AS school_type,
        NULLIF(road_data->>'nearest_school_lat','')::numeric         AS school_lat,
        NULLIF(road_data->>'nearest_school_lon','')::numeric         AS school_lon,
        NULLIF(road_data->>'nearest_school_dist_ft','')::numeric     AS dist_ft,
        crash_severity, pedestrian, bike, night, speed, near_school_1000ft
    FROM crashes
    WHERE state = 'delaware'
      AND COALESCE(road_data->>'nearest_school_ncessch', '') <> ''
),
agg AS (
    SELECT
        ncessch,
        MAX(school_name)  AS school_name,
        MAX(leaid)        AS leaid,
        MAX(enrollment)   AS enrollment,
        MAX(school_level) AS school_level,
        MAX(school_type)  AS school_type,
        MAX(school_lat)   AS school_lat,
        MAX(school_lon)   AS school_lon,
        COUNT(*)                                                              AS total_crashes_1000ft,
        COUNT(*) FILTER (WHERE crash_severity IN ('K','A'))                   AS ksi_1000ft,
        COUNT(*) FILTER (WHERE crash_severity = 'K')                          AS fatal_1000ft,
        COUNT(*) FILTER (WHERE pedestrian = 'Yes')                            AS ped_1000ft,
        COUNT(*) FILTER (WHERE bike = 'Yes')                                  AS bike_1000ft,
        COUNT(*) FILTER (WHERE night = 'Yes')                                 AS night_1000ft,
        COUNT(*) FILTER (WHERE speed = 'Yes')                                 AS speed_1000ft
    FROM crash_schools
    GROUP BY ncessch
),
normed AS (
    SELECT *,
        COALESCE(ksi_1000ft::numeric       / NULLIF(MAX(ksi_1000ft)       OVER (), 0), 0) AS n_ksi,
        COALESCE((ped_1000ft+bike_1000ft)  / NULLIF(MAX(ped_1000ft+bike_1000ft) OVER (), 0), 0) AS n_vru,
        COALESCE(night_1000ft::numeric     / NULLIF(MAX(night_1000ft)     OVER (), 0), 0) AS n_night,
        COALESCE(speed_1000ft::numeric     / NULLIF(MAX(speed_1000ft)     OVER (), 0), 0) AS n_speed,
        COALESCE(enrollment                / NULLIF(MAX(enrollment)       OVER (), 0), 0) AS n_enroll
    FROM agg
)
SELECT
    ncessch, school_name, leaid, enrollment, school_level, school_type, school_lat, school_lon,
    total_crashes_1000ft, ksi_1000ft, fatal_1000ft, ped_1000ft, bike_1000ft, night_1000ft, speed_1000ft,
    ROUND((0.40*n_ksi + 0.25*n_vru + 0.15*n_night + 0.10*n_speed + 0.10*n_enroll) * 100, 2) AS safety_score,
    RANK() OVER (ORDER BY (0.40*n_ksi + 0.25*n_vru + 0.15*n_night + 0.10*n_speed + 0.10*n_enroll) DESC) AS safety_rank
FROM normed;

CREATE UNIQUE INDEX IF NOT EXISTS idx_schools_safety_delaware ON schools_safety_delaware(ncessch);

-- ═══ 2. Hospitals — ~20 rows ═══
CREATE MATERIALIZED VIEW IF NOT EXISTS hospitals_safety_delaware AS
WITH crash_hospitals AS (
    SELECT
        road_data->>'nearest_poi_hospital_name'                       AS hospital_name,
        NULLIF(road_data->>'nearest_poi_hospital_dist_ft','')::numeric AS dist_ft,
        crash_severity, pedestrian, bike, night, near_poi_hospital_1000ft
    FROM crashes
    WHERE state='delaware'
      AND COALESCE(road_data->>'nearest_poi_hospital_name','') <> ''
      AND near_poi_hospital_1000ft = 'Yes'
),
agg AS (
    SELECT hospital_name,
        COUNT(*)                                            AS total_crashes_1000ft,
        COUNT(*) FILTER (WHERE crash_severity IN ('K','A')) AS ksi_1000ft,
        COUNT(*) FILTER (WHERE pedestrian = 'Yes')          AS ped_1000ft,
        COUNT(*) FILTER (WHERE bike = 'Yes')                AS bike_1000ft,
        COUNT(*) FILTER (WHERE night = 'Yes')               AS night_1000ft
    FROM crash_hospitals GROUP BY hospital_name
)
SELECT *, RANK() OVER (ORDER BY ksi_1000ft DESC, total_crashes_1000ft DESC) AS safety_rank
FROM agg;
CREATE UNIQUE INDEX IF NOT EXISTS idx_hospitals_safety_delaware ON hospitals_safety_delaware(hospital_name);

-- ═══ 3. Transit — ~2,000 stops ═══
CREATE MATERIALIZED VIEW IF NOT EXISTS transit_safety_delaware AS
WITH crash_transit AS (
    SELECT
        road_data->>'nearest_transit_stop_id'                    AS stop_id,
        road_data->>'nearest_transit_stop_name'                  AS stop_name,
        road_data->>'nearest_transit_wheelchair'                 AS wheelchair,
        NULLIF(road_data->>'nearest_transit_dist_ft','')::numeric AS dist_ft,
        crash_severity, pedestrian, bike, night, near_transit_500ft
    FROM crashes
    WHERE state='delaware'
      AND COALESCE(road_data->>'nearest_transit_stop_id','') <> ''
      AND near_transit_500ft = 'Yes'
),
agg AS (
    SELECT stop_id, MAX(stop_name) AS stop_name, MAX(wheelchair) AS wheelchair,
        COUNT(*)                                            AS total_crashes_500ft,
        COUNT(*) FILTER (WHERE crash_severity IN ('K','A')) AS ksi_500ft,
        COUNT(*) FILTER (WHERE pedestrian = 'Yes')          AS ped_500ft,
        COUNT(*) FILTER (WHERE bike = 'Yes')                AS bike_500ft,
        COUNT(*) FILTER (WHERE night = 'Yes')               AS night_500ft
    FROM crash_transit GROUP BY stop_id
)
SELECT *, RANK() OVER (ORDER BY ped_500ft DESC, ksi_500ft DESC) AS safety_rank
FROM agg;
CREATE UNIQUE INDEX IF NOT EXISTS idx_transit_safety_delaware ON transit_safety_delaware(stop_id);

-- ═══ 4. Rail crossings — ~1,330 rows + priority flag ═══
CREATE MATERIALIZED VIEW IF NOT EXISTS rail_xings_safety_delaware AS
WITH crash_rail AS (
    SELECT
        road_data->>'nearest_rail_xing_id'                        AS crossing_id,
        road_data->>'nearest_rail_xing_street'                    AS street,
        road_data->>'nearest_rail_xing_railroad'                  AS railroad,
        road_data->>'nearest_rail_xing_warning_device'            AS warning_device,
        road_data->>'nearest_rail_xing_warning_level'             AS warning_level,
        NULLIF(road_data->>'nearest_rail_xing_trains_per_day','')::numeric AS trains_per_day,
        NULLIF(road_data->>'nearest_rail_xing_dist_ft','')::numeric        AS dist_ft,
        crash_severity, near_rail_xing_500ft
    FROM crashes
    WHERE state='delaware'
      AND COALESCE(road_data->>'nearest_rail_xing_id','') <> ''
      AND near_rail_xing_500ft = 'Yes'
),
agg AS (
    SELECT crossing_id,
        MAX(street) AS street, MAX(railroad) AS railroad,
        MAX(warning_device) AS warning_device, MAX(warning_level) AS warning_level,
        MAX(trains_per_day) AS trains_per_day,
        COUNT(*)                                            AS total_crashes_500ft,
        COUNT(*) FILTER (WHERE crash_severity IN ('K','A')) AS ksi_500ft,
        COUNT(*) FILTER (WHERE crash_severity = 'K')        AS fatal_500ft
    FROM crash_rail GROUP BY crossing_id
)
SELECT *,
    (warning_device = 'Unknown' AND total_crashes_500ft > 0) AS priority_flag,
    RANK() OVER (ORDER BY ksi_500ft DESC, total_crashes_500ft DESC) AS safety_rank
FROM agg;
CREATE UNIQUE INDEX IF NOT EXISTS idx_rail_xings_safety_delaware ON rail_xings_safety_delaware(crossing_id);
