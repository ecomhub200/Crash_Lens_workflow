-- Migration 002: Rename 3 Tier 1 columns to match new thresholds.
-- Schools 1500→1000, Hospital 2000→1000, Clinic 1500→1000.
-- ALL OTHER near_* columns unchanged.
--
-- RENAME preserves existing data. Data semantics become correct on next
-- full pipeline cycle (build_road_inventory → crash_enricher → supabase_sync).
--
-- Run on parent crashes table — partitions inherit automatically.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='crashes' AND column_name='near_school_1500ft') THEN
        ALTER TABLE crashes RENAME COLUMN near_school_1500ft TO near_school_1000ft;
        RAISE NOTICE 'Renamed near_school_1500ft → near_school_1000ft';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='crashes' AND column_name='near_poi_hospital_2000ft') THEN
        ALTER TABLE crashes RENAME COLUMN near_poi_hospital_2000ft TO near_poi_hospital_1000ft;
        RAISE NOTICE 'Renamed near_poi_hospital_2000ft → near_poi_hospital_1000ft';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='crashes' AND column_name='near_poi_clinic_1500ft') THEN
        ALTER TABLE crashes RENAME COLUMN near_poi_clinic_1500ft TO near_poi_clinic_1000ft;
        RAISE NOTICE 'Renamed near_poi_clinic_1500ft → near_poi_clinic_1000ft';
    END IF;
END $$;

-- Verify all 3 renames succeeded
SELECT column_name FROM information_schema.columns
WHERE table_name='crashes'
  AND column_name IN ('near_school_1000ft', 'near_poi_hospital_1000ft', 'near_poi_clinic_1000ft')
ORDER BY column_name;
-- Expected: 3 rows
