-- ═══════════════════════════════════════════════════════════════════
--  CrashLens Supabase Migration — v3.0 (3-Tier Column Strategy)
-- ═══════════════════════════════════════════════════════════════════
--  Tier 1: 111 explicit columns (queryable, indexable)
--  Tier 2: road_data JSONB (312 road inventory columns)
--  Tier 3: state_extras JSONB (de_*, va_*, etc. — varies per state)
--  Bonus:  ranking_data JSONB (76 per-crash ranking columns)
--
--  Run this against self-hosted Supabase on srv1503081.hstgr.cloud
--  Execute blocks in order. Idempotent (IF NOT EXISTS / OR REPLACE).
-- ═══════════════════════════════════════════════════════════════════


-- ─── EXTENSIONS ──────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- ─── 1. STATES REGISTRY ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS states (
    abbr            TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    fips            TEXT NOT NULL,
    display_name    TEXT NOT NULL,
    pipeline_status TEXT DEFAULT 'pending',
    total_crashes   INTEGER DEFAULT 0,
    year_range      INT4RANGE,
    last_sync_at    TIMESTAMPTZ,
    config_json     JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ─── 2. CRASHES (PARTITIONED) ───────────────────────────────────
--  111 explicit columns + 3 JSONB bags + metadata
--  Partitioned by LIST on state column.
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crashes (
    id                      BIGSERIAL,
    state                   TEXT NOT NULL,

    -- ── Golden 69 ────────────────────────────────────────────
    objectid                TEXT NOT NULL,
    document_nbr            TEXT,
    crash_year              INTEGER,
    crash_date              TEXT,
    crash_military_time     TEXT,
    crash_severity          TEXT,
    k_people                INTEGER DEFAULT 0,
    a_people                INTEGER DEFAULT 0,
    b_people                INTEGER DEFAULT 0,
    c_people                INTEGER DEFAULT 0,
    persons_injured         INTEGER DEFAULT 0,
    pedestrians_killed      INTEGER DEFAULT 0,
    pedestrians_injured     INTEGER DEFAULT 0,
    vehicle_count           INTEGER,
    collision_type          TEXT,
    weather_condition       TEXT,
    light_condition         TEXT,
    roadway_surface_cond    TEXT,
    relation_to_roadway     TEXT,
    roadway_alignment       TEXT,
    roadway_surface_type    TEXT,
    roadway_defect          TEXT,
    roadway_description     TEXT,
    intersection_type       TEXT,
    traffic_control_type    TEXT,
    traffic_control_status  TEXT,
    work_zone_related       TEXT,
    work_zone_location      TEXT,
    work_zone_type          TEXT,
    school_zone             TEXT,
    first_harmful_event     TEXT,
    first_harmful_event_loc TEXT,
    alcohol                 TEXT,
    animal_related          TEXT,
    unrestrained            TEXT,
    bike                    TEXT,
    distracted              TEXT,
    drowsy                  TEXT,
    drug_related            TEXT,
    guardrail_related       TEXT,
    hitrun                  TEXT,
    lgtruck                 TEXT,
    motorcycle              TEXT,
    pedestrian              TEXT,
    speed                   TEXT,
    max_speed_diff          TEXT,
    road_departure_type     TEXT,
    intersection_analysis   TEXT,
    senior                  TEXT,
    young                   TEXT,
    mainline                TEXT,
    night                   TEXT,
    dot_district            TEXT,
    juris_code              TEXT,
    physical_juris_name     TEXT,
    functional_class        TEXT,
    facility_type           TEXT,
    area_type               TEXT,
    system                  TEXT,
    vsp                     TEXT,
    ownership               TEXT,
    planning_district       TEXT,
    mpo_name                TEXT,
    rte_name                TEXT,
    rns_mp                  TEXT,
    node                    TEXT,
    node_offset_ft          TEXT,
    x                       DOUBLE PRECISION,
    y                       DOUBLE PRECISION,

    -- ── Extra Enrichment (4) ─────────────────────────────────
    fips                    TEXT,
    place_fips              TEXT,
    epdo_score              TEXT,
    intersection_name       TEXT,

    -- ── Key Analysis (10) ────────────────────────────────────
    through_lanes           TEXT,
    aadt                    TEXT,
    aadt_source             TEXT,
    lane_width_ft           TEXT,
    median_width_ft         TEXT,
    shoulder_width_ft       TEXT,
    has_sidewalk            TEXT,
    has_bike_lane           TEXT,
    urban_area_name         TEXT,
    urban_area_geoid        TEXT,

    -- ── Proximity Flags — POI (11) ───────────────────────────
    near_poi_bar_1500ft     TEXT,
    near_poi_clinic_1500ft  TEXT,
    near_poi_college_1500ft TEXT,
    near_poi_crossing_100ft TEXT,
    near_poi_fuel_500ft     TEXT,
    near_poi_hospital_2000ft TEXT,
    near_poi_parking_500ft  TEXT,
    near_poi_rest_area_1000ft TEXT,
    near_poi_restaurant_500ft TEXT,
    near_poi_signal_100ft   TEXT,
    near_poi_stop_sign_100ft TEXT,

    -- ── Proximity Flags — Federal Assets (4) ─────────────────
    near_bridge_500ft       TEXT,
    near_rail_xing_500ft    TEXT,
    near_school_1500ft      TEXT,
    near_transit_500ft      TEXT,

    -- ── Resolved Values (5) ──────────────────────────────────
    resolved_speed_limit    TEXT,
    resolved_has_lighting   TEXT,
    resolved_has_signal     TEXT,
    resolved_on_bridge      TEXT,
    resolved_school_zone    TEXT,

    -- ── Intersection & Ramp (4) ──────────────────────────────
    is_intersection         TEXT,
    intersection_degree     TEXT,
    is_ramp                 TEXT,
    ramp_type               TEXT,

    -- ── Road Geometry (4) ────────────────────────────────────
    curvature               TEXT,
    length_ft               TEXT,
    road_lon                TEXT,
    road_lat                TEXT,

    -- ── JSONB BAGS ───────────────────────────────────────────
    road_data               JSONB,          -- 312 cols: hpms_*, bridge_*, poi_*, map_*, etc.
    state_extras            JSONB,          -- de_*, va_*, co_* (varies per state)
    ranking_data            JSONB,          -- 76 ranking columns (4 scopes × 19 metrics)

    -- ── Metadata ─────────────────────────────────────────────
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (id, state)
) PARTITION BY LIST (state);


-- ─── 3. PARTITIONS (Phase 1 states) ─────────────────────────────
CREATE TABLE IF NOT EXISTS crashes_delaware  PARTITION OF crashes FOR VALUES IN ('delaware');
CREATE TABLE IF NOT EXISTS crashes_virginia  PARTITION OF crashes FOR VALUES IN ('virginia');
CREATE TABLE IF NOT EXISTS crashes_colorado  PARTITION OF crashes FOR VALUES IN ('colorado');


-- ─── 4. INDEXES ─────────────────────────────────────────────────
-- Jurisdiction lookups (most common frontend queries)
CREATE INDEX IF NOT EXISTS idx_crashes_state_year     ON crashes(state, crash_year);
CREATE INDEX IF NOT EXISTS idx_crashes_county         ON crashes(state, physical_juris_name);
CREATE INDEX IF NOT EXISTS idx_crashes_mpo            ON crashes(state, mpo_name);
CREATE INDEX IF NOT EXISTS idx_crashes_dot_district   ON crashes(state, dot_district);
CREATE INDEX IF NOT EXISTS idx_crashes_planning_dist  ON crashes(state, planning_district);
CREATE INDEX IF NOT EXISTS idx_crashes_severity       ON crashes(state, crash_severity);

-- Road type filters (SET A and SET B)
CREATE INDEX IF NOT EXISTS idx_crashes_ownership      ON crashes(state, ownership);
CREATE INDEX IF NOT EXISTS idx_crashes_fc             ON crashes(state, functional_class);

-- Federal view (cross-state)
CREATE INDEX IF NOT EXISTS idx_crashes_year_severity  ON crashes(crash_year, crash_severity);
CREATE INDEX IF NOT EXISTS idx_crashes_federal_fc     ON crashes(functional_class, crash_severity);

-- Spatial
CREATE INDEX IF NOT EXISTS idx_crashes_coords         ON crashes(state, x, y);

-- Upsert key
CREATE UNIQUE INDEX IF NOT EXISTS idx_crashes_objectid ON crashes(state, objectid);


-- ─── 5. RANKINGS (aggregate per jurisdiction) ───────────────────
CREATE TABLE IF NOT EXISTS rankings (
    id              BIGSERIAL PRIMARY KEY,
    state           TEXT NOT NULL,
    tier            TEXT NOT NULL,
    jurisdiction    TEXT NOT NULL,
    road_type       TEXT NOT NULL,
    crash_year      INTEGER,
    total_crashes   INTEGER,
    fatal_crashes   INTEGER,
    injury_crashes  INTEGER,
    pdo_crashes     INTEGER,
    epdo_score      REAL,
    k_people        INTEGER,
    a_people        INTEGER,
    b_people        INTEGER,
    c_people        INTEGER,
    pedestrian_fatal  INTEGER,
    pedestrian_injury INTEGER,
    bike_fatal      INTEGER,
    bike_injury     INTEGER,
    data_json       JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(state, tier, jurisdiction, road_type, crash_year)
);

CREATE INDEX IF NOT EXISTS idx_rankings_lookup  ON rankings(state, tier, jurisdiction, road_type);
CREATE INDEX IF NOT EXISTS idx_rankings_federal ON rankings(tier, crash_year) WHERE state = '_federal';


-- ─── 6. FEDERAL SUMMARY (materialized view) ─────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS federal_summary AS
SELECT
    crash_year, crash_severity, state,
    functional_class, ownership, area_type,
    COUNT(*)                                           AS total_crashes,
    SUM(k_people)                                      AS total_k,
    SUM(a_people)                                      AS total_a,
    SUM(b_people)                                      AS total_b,
    SUM(c_people)                                      AS total_c,
    SUM(persons_injured)                               AS total_injured,
    SUM(pedestrians_killed)                            AS total_ped_k,
    SUM(pedestrians_injured)                           AS total_ped_inj,
    COUNT(*) FILTER (WHERE alcohol = 'Yes')            AS alcohol_crashes,
    COUNT(*) FILTER (WHERE speed = 'Yes')              AS speed_crashes,
    COUNT(*) FILTER (WHERE distracted = 'Yes')         AS distracted_crashes,
    COUNT(*) FILTER (WHERE pedestrian = 'Yes')         AS ped_crashes,
    COUNT(*) FILTER (WHERE bike = 'Yes')               AS bike_crashes,
    COUNT(*) FILTER (WHERE work_zone_related LIKE '%Yes%')  AS wz_crashes
FROM crashes
GROUP BY crash_year, crash_severity, state, functional_class, ownership, area_type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_federal_summary
    ON federal_summary(crash_year, crash_severity, state, functional_class, ownership, area_type);


-- ─── 7. SUPPORTING TABLES ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS hierarchies (
    state           TEXT PRIMARY KEY,
    hierarchy_json  JSONB NOT NULL,
    regions         JSONB,
    mpos            JSONB,
    counties        JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    state           TEXT NOT NULL,
    stage           TEXT NOT NULL,
    status          TEXT NOT NULL,
    rows_processed  INTEGER,
    duration_sec    REAL,
    error_message   TEXT,
    metadata_json   JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_state ON pipeline_runs(state, created_at DESC);


-- ─── 8. ORGANIZATIONS & USERS (Phase 4 — create now, use later) ─
CREATE TABLE IF NOT EXISTS organizations (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    org_type    TEXT NOT NULL,
    states      TEXT[] NOT NULL,
    config_json JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id     UUID,
    org_id      BIGINT REFERENCES organizations(id),
    role        TEXT NOT NULL,
    permissions TEXT[] DEFAULT '{}',
    PRIMARY KEY (user_id, org_id)
);

INSERT INTO organizations (name, org_type, states) VALUES
    ('CrashLens',  'platform',  ARRAY['*']),
    ('FHWA',       'federal',   ARRAY['*']),
    ('DelDOT',     'state_dot', ARRAY['delaware']),
    ('VDOT',       'state_dot', ARRAY['virginia']),
    ('CDOT',       'state_dot', ARRAY['colorado'])
ON CONFLICT (name) DO NOTHING;


-- ─── 9. SEED DELAWARE ───────────────────────────────────────────
INSERT INTO states (abbr, name, fips, display_name, pipeline_status)
VALUES ('de', 'delaware', '10', 'Delaware', 'pending')
ON CONFLICT (abbr) DO NOTHING;


-- ═══════════════════════════════════════════════════════════════════
--  MIGRATION COMPLETE
--  Next: python supabase_sync.py --state de --from-r2
-- ═══════════════════════════════════════════════════════════════════
