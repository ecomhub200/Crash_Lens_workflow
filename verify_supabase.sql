-- ═══════════════════════════════════════════════════════════════
--  CrashLens Supabase Migration — Verification Queries
--  Run these after migration to confirm everything is correct.
-- ═══════════════════════════════════════════════════════════════


-- ─── PRE-SYNC: Schema Verification ─────────────────────────────

-- 1. Table existence
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
-- Expected: crashes, crashes_colorado, crashes_delaware, crashes_virginia,
--           hierarchies, organizations, pipeline_runs, rankings, states, user_roles

-- 2. Partition check
SELECT inhrelid::regclass AS partition FROM pg_inherits WHERE inhparent = 'crashes'::regclass;
-- Expected: crashes_delaware, crashes_virginia, crashes_colorado

-- 3. Column count on crashes table
SELECT COUNT(*) AS column_count FROM information_schema.columns WHERE table_name = 'crashes';
-- Expected: 117 (111 tier1 + state + 3 JSONB + id + created_at + updated_at)

-- 4. Index check
SELECT indexname FROM pg_indexes WHERE tablename LIKE 'crashes%' ORDER BY indexname;
-- Expected: idx_crashes_coords, idx_crashes_county, idx_crashes_dot_district,
--           idx_crashes_fc, idx_crashes_federal_fc, idx_crashes_mpo,
--           idx_crashes_objectid, idx_crashes_ownership, idx_crashes_planning_dist,
--           idx_crashes_severity, idx_crashes_state_year, idx_crashes_year_severity,
--           plus partition-specific copies

-- 5. States table seeded
SELECT * FROM states;
-- Expected: de | delaware | 10 | Delaware | pending

-- 6. Organizations seeded
SELECT name, org_type FROM organizations ORDER BY name;
-- Expected: CDOT, CrashLens, DelDOT, FHWA, VDOT


-- ─── POST-SYNC: Data Verification ──────────────────────────────

-- 7. Row count
SELECT COUNT(*) AS total_rows FROM crashes_delaware;
-- Expected: 566,762

-- 8. Severity distribution
SELECT crash_severity, COUNT(*) AS cnt
FROM crashes_delaware
GROUP BY crash_severity
ORDER BY cnt DESC;
-- Expected: O=476563, A=88425, K=1774

-- 9. Year range
SELECT MIN(crash_year) AS min_year, MAX(crash_year) AS max_year FROM crashes_delaware;
-- Expected: 2009, 2023

-- 10. Year distribution
SELECT crash_year, COUNT(*) AS cnt
FROM crashes_delaware
WHERE crash_year IS NOT NULL
GROUP BY crash_year
ORDER BY crash_year;
-- Expected: 15 years (2009-2023), ~37K rows per year average

-- 11. States table updated
SELECT abbr, name, pipeline_status, total_crashes, year_range, last_sync_at
FROM states WHERE abbr = 'de';
-- Expected: de | delaware | active | 566762 | [2009,2024) | <recent timestamp>

-- 12. OBJECTID format check
SELECT objectid FROM crashes_delaware LIMIT 5;
-- Expected: de-0000001, de-0000002, etc.

-- 13. Coordinates check (Delaware bounding box: lat ~38.4-39.8, lon ~-75.8 to -75.0)
SELECT
    MIN(x) AS min_lon, MAX(x) AS max_lon,
    MIN(y) AS min_lat, MAX(y) AS max_lat
FROM crashes_delaware
WHERE x IS NOT NULL AND y IS NOT NULL;
-- Expected: lon in [-75.8, -75.0], lat in [38.4, 39.9]

-- 14. DOT District values
SELECT dot_district, COUNT(*) AS cnt
FROM crashes_delaware
WHERE dot_district IS NOT NULL
GROUP BY dot_district
ORDER BY cnt DESC;
-- Expected: Central District, North District, South District (3 districts)


-- ─── POST-SYNC: JSONB Verification ─────────────────────────────

-- 15. road_data key count (sample)
SELECT objectid,
       jsonb_object_keys(road_data) AS rd_key
FROM crashes_delaware
WHERE road_data IS NOT NULL AND road_data != '{}'::jsonb
LIMIT 10;
-- Expected: keys like hpms_*, map_*, dot_*, nearest_*, etc.

-- 16. road_data key count per row
SELECT
    MIN(jsonb_array_length(jsonb_agg(k))) AS min_keys,
    MAX(jsonb_array_length(jsonb_agg(k))) AS max_keys
FROM (
    SELECT objectid, jsonb_object_keys(road_data) AS k
    FROM crashes_delaware
    WHERE road_data != '{}'::jsonb
    LIMIT 1000
) sub
GROUP BY objectid
LIMIT 5;

-- 17. state_extras check (should have de_* keys)
SELECT objectid, state_extras
FROM crashes_delaware
WHERE state_extras IS NOT NULL AND state_extras != '{}'::jsonb
LIMIT 3;
-- Expected: keys like de_Day_Of_Week_Code, de_Lighting_Condition_Code, etc.

-- 18. ranking_data check
SELECT objectid,
       jsonb_object_keys(ranking_data) AS rk_key
FROM crashes_delaware
WHERE ranking_data IS NOT NULL AND ranking_data != '{}'::jsonb
LIMIT 10;
-- Expected: keys like District_Rank_total_crash, Juris_Rank_total_epdo, etc.

-- 19. Specific JSONB query test — HPMS data
SELECT objectid,
       road_data->>'hpms_aadt_combination' AS hpms_aadt,
       road_data->>'hpms_design_speed' AS hpms_speed,
       road_data->>'hpms_iri' AS hpms_iri
FROM crashes_delaware
WHERE road_data->>'hpms_aadt_combination' IS NOT NULL
LIMIT 5;


-- ─── POST-SYNC: Federal Summary ────────────────────────────────

-- 20. Federal summary existence
SELECT COUNT(*) AS summary_rows FROM federal_summary;
-- Expected: > 0 (varies by grouping)

-- 21. Federal summary sample
SELECT crash_year, crash_severity, state,
       total_crashes, total_k, total_a, alcohol_crashes, wz_crashes
FROM federal_summary
WHERE state = 'delaware'
ORDER BY crash_year DESC, crash_severity
LIMIT 10;
-- Expected: crash counts per year/severity, wz_crashes > 0 (uses LIKE '%Yes%')

-- 22. Federal summary totals check
SELECT SUM(total_crashes) AS total FROM federal_summary WHERE state = 'delaware';
-- Expected: 566,762 (or close — some rows have NULL crash_year)


-- ─── POST-SYNC: Pipeline Runs ──────────────────────────────────

-- 23. Pipeline run log
SELECT id, state, stage, status, rows_processed, duration_sec, created_at
FROM pipeline_runs
ORDER BY created_at DESC
LIMIT 5;
-- Expected: Most recent = delaware | sync | success | 566762


-- ─── POST-SYNC: Boolean Flag Validation ────────────────────────

-- 24. Boolean flag values (spot check)
SELECT
    COUNT(*) FILTER (WHERE alcohol = 'Yes') AS alcohol_yes,
    COUNT(*) FILTER (WHERE speed = 'Yes') AS speed_yes,
    COUNT(*) FILTER (WHERE distracted = 'Yes') AS distracted_yes,
    COUNT(*) FILTER (WHERE pedestrian = 'Yes') AS ped_yes,
    COUNT(*) FILTER (WHERE bike = 'Yes') AS bike_yes,
    COUNT(*) FILTER (WHERE work_zone_related LIKE '%Yes%') AS wz_yes
FROM crashes_delaware;
-- Expected: alcohol=21733, speed=6163, distracted=129289, ped=7490, bike=3257, wz=3907
