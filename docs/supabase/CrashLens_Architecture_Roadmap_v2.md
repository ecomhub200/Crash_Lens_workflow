# CrashLens Architecture Roadmap v2
## Multi-State, Federal-to-City Platform

---

## DASHBOARD HIERARCHY (7 TIERS)

```
┌─────────────────────────────────────────────────────────────┐
│                                                              │
│   FEDERAL          All 50 states consolidated               │
│      │              National crash trends, cross-state       │
│      │              comparisons, FHWA-level analysis         │
│      │                                                       │
│      ▼                                                       │
│   STATE            Single state (e.g., Delaware)            │
│      │              Statewide totals, DOT-level overview     │
│      │                                                       │
│      ├──▶ REGION         DOT Districts within a state       │
│      │                                                       │
│      ├──▶ PLANNING       Planning Districts                 │
│      │    DISTRICT                                           │
│      │                                                       │
│      ├──▶ MPO            Metropolitan Planning Orgs         │
│      │                                                       │
│      ├──▶ COUNTY         Individual counties                │
│      │                                                       │
│      └──▶ CITY/TOWN      Cities & towns within counties     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Road Type Sets Per Tier

| Tier | Road Types | Filter Logic |
|------|-----------|--------------|
| **Federal** | all_roads, interstate, nhs, non_nhs | FC 1 = Interstate, NHS flag, etc. |
| **State** | all_roads, dot_roads, primary_roads, non_dot_roads | SET A |
| **Region** | all_roads, dot_roads, primary_roads, non_dot_roads | SET A |
| **Planning District** | all_roads, county_roads, city_roads, no_interstate | SET B |
| **MPO** | all_roads, county_roads, city_roads, no_interstate | SET B |
| **County** | all_roads, county_roads, city_roads, no_interstate | SET B |
| **City/Town** | all_roads, county_roads, city_roads, no_interstate | SET B |

### Federal View — Cross-State Queries

With Supabase, the federal view is just a query with no state filter:

```sql
-- Federal: all states, all years
SELECT state, crash_year, crash_severity, COUNT(*) as total
FROM crashes
GROUP BY state, crash_year, crash_severity;

-- Federal: compare K-fatality rate by state
SELECT state, 
       COUNT(*) FILTER (WHERE crash_severity = 'K') as fatals,
       COUNT(*) as total,
       ROUND(COUNT(*) FILTER (WHERE crash_severity = 'K')::numeric / COUNT(*)::numeric * 100, 2) as fatal_pct
FROM crashes
GROUP BY state
ORDER BY fatal_pct DESC;

-- Federal: interstate crashes across all states
SELECT state, COUNT(*) 
FROM crashes 
WHERE functional_class LIKE '1-%'
GROUP BY state;
```

No consolidation needed — PostgreSQL scans all partitions automatically when no `state` filter is applied. Each state's data stays isolated in its partition, but the federal view sees everything through one query.

---

## CURRENT STATE (v2.6.5)

```
┌──────────────────────────────────────────────────────────────┐
│  Pipeline: Socrata → normalize → enrich → split → R2         │
│  Frontend: Fetches parquet.gz files from Cloudflare R2        │
│  Auth: Firebase                                               │
│  Dashboard: State + County tiers only                         │
│  States: Delaware (reference), Virginia, Colorado (partial)   │
└──────────────────────────────────────────────────────────────┘
```

---

## PHASE 1: MULTI-STATE DATABASE (DO NOW)

### Schema — Partitioned by State

```sql
-- ═══════════════════════════════════════════════════════════
--  EXTENSIONS
-- ═══════════════════════════════════════════════════════════
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;    -- fuzzy text search

-- ═══════════════════════════════════════════════════════════
--  STATES REGISTRY
-- ═══════════════════════════════════════════════════════════
CREATE TABLE states (
    abbr            TEXT PRIMARY KEY,        -- "de", "va", "co"
    name            TEXT NOT NULL,           -- "delaware", "virginia"
    fips            TEXT NOT NULL,           -- "10", "51", "08"
    display_name    TEXT NOT NULL,           -- "Delaware", "Virginia"
    pipeline_status TEXT DEFAULT 'pending',  -- pending/active/error
    total_crashes   INTEGER DEFAULT 0,
    year_range      INT4RANGE,              -- [2012,2024)
    last_sync_at    TIMESTAMPTZ,
    config_json     JSONB,                  -- state-specific settings
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════
--  CRASHES — PARTITIONED BY STATE
-- ═══════════════════════════════════════════════════════════
--  One logical table, physically separated per state.
--  Federal view: query without state filter → scans all partitions.
--  State view: query with state filter → scans only that partition.
--  Adding a state: one CREATE TABLE command.
--  Reloading a state: DROP + CREATE partition → zero impact on others.
-- ═══════════════════════════════════════════════════════════

CREATE TABLE crashes (
    id                      BIGSERIAL,
    state                   TEXT NOT NULL,

    -- ── Golden 69 Columns ──────────────────────────────────
    objectid                TEXT NOT NULL,           -- "de-0000001"
    document_nbr            TEXT,
    crash_year              INTEGER,
    crash_date              DATE,
    crash_military_time     INTEGER,
    crash_severity          TEXT,                    -- K, A, B, C, O
    k_people                INTEGER DEFAULT 0,
    a_people                INTEGER DEFAULT 0,
    b_people                INTEGER DEFAULT 0,
    c_people                INTEGER DEFAULT 0,
    persons_injured         INTEGER DEFAULT 0,
    pedestrians_killed      INTEGER DEFAULT 0,
    pedestrians_injured     INTEGER DEFAULT 0,
    vehicle_count           INTEGER,

    -- Conditions
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

    -- Work zone
    work_zone_related       TEXT,
    work_zone_location      TEXT,
    work_zone_type          TEXT,
    school_zone             TEXT,

    -- Harmful events
    first_harmful_event     TEXT,
    first_harmful_event_loc TEXT,

    -- Boolean flags (Yes/No text — matches frontend)
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

    -- Numeric
    max_speed_diff          REAL,

    -- Analysis flags
    road_departure_type     TEXT,
    intersection_analysis   TEXT,
    senior                  TEXT,
    young                   TEXT,
    mainline                TEXT,
    night                   TEXT,

    -- Jurisdiction
    dot_district            TEXT,
    juris_code              INTEGER,
    physical_juris_name     TEXT,
    functional_class        TEXT,
    facility_type           TEXT,
    area_type               TEXT,
    system                  TEXT,
    vsp                     INTEGER,
    ownership               TEXT,
    planning_district       TEXT,
    mpo_name                TEXT,
    rte_name                TEXT,
    rns_mp                  REAL,
    node                    REAL,
    node_offset_ft          REAL,

    -- Coordinates
    x                       DOUBLE PRECISION,       -- longitude
    y                       DOUBLE PRECISION,       -- latitude

    -- ── Enrichment 4 ──────────────────────────────────────
    enrichment_score        TEXT,
    enrichment_source       TEXT,
    enrichment_fc_source    TEXT,
    enrichment_own_source   TEXT,

    -- ── Analysis 18 ───────────────────────────────────────
    aadt                    INTEGER,
    through_lanes           INTEGER,
    access_control          TEXT,
    lane_width_ft           REAL,
    median_width_ft         REAL,
    shoulder_width_ft       REAL,
    aadt_trucks             INTEGER,
    design_speed_mph        INTEGER,
    has_street_lighting     TEXT,
    has_sidewalk            TEXT,
    has_bike_lane           TEXT,
    on_bridge               TEXT,
    near_bar_1500ft         TEXT,
    near_school_1000ft      TEXT,
    near_crossing_100ft     TEXT,
    near_parking_150ft      TEXT,
    near_rail_xing_150ft    TEXT,
    nearest_hospital_mi     REAL,

    -- ── Metadata ──────────────────────────────────────────
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (id, state)
) PARTITION BY LIST (state);


-- ═══════════════════════════════════════════════════════════
--  STATE PARTITIONS
-- ═══════════════════════════════════════════════════════════
--  Each state's data is physically isolated.
--  DROP TABLE crashes_delaware → instant delete, no impact on others.
--  Pipeline reload: DROP + CREATE + bulk INSERT.
-- ═══════════════════════════════════════════════════════════

-- Phase 1 states (active)
CREATE TABLE crashes_delaware    PARTITION OF crashes FOR VALUES IN ('delaware');
CREATE TABLE crashes_virginia    PARTITION OF crashes FOR VALUES IN ('virginia');
CREATE TABLE crashes_colorado    PARTITION OF crashes FOR VALUES IN ('colorado');

-- Phase 2 states (add as pipeline expands)
-- CREATE TABLE crashes_maryland    PARTITION OF crashes FOR VALUES IN ('maryland');
-- CREATE TABLE crashes_ohio        PARTITION OF crashes FOR VALUES IN ('ohio');
-- CREATE TABLE crashes_texas       PARTITION OF crashes FOR VALUES IN ('texas');
-- ... one line per state, add when ready


-- ═══════════════════════════════════════════════════════════
--  INDEXES
-- ═══════════════════════════════════════════════════════════
--  PostgreSQL automatically creates per-partition indexes.
--  Federal queries scan all partitions but use local indexes.
-- ═══════════════════════════════════════════════════════════

-- Jurisdiction lookups (most common frontend queries)
CREATE INDEX idx_crashes_state_year     ON crashes(state, crash_year);
CREATE INDEX idx_crashes_county         ON crashes(state, physical_juris_name);
CREATE INDEX idx_crashes_mpo            ON crashes(state, mpo_name);
CREATE INDEX idx_crashes_dot_district   ON crashes(state, dot_district);
CREATE INDEX idx_crashes_planning_dist  ON crashes(state, planning_district);
CREATE INDEX idx_crashes_severity       ON crashes(state, crash_severity);

-- Road type filters (used by SET A and SET B)
CREATE INDEX idx_crashes_ownership      ON crashes(state, ownership);
CREATE INDEX idx_crashes_fc             ON crashes(state, functional_class);

-- Federal view (cross-state aggregation)
CREATE INDEX idx_crashes_year_severity  ON crashes(crash_year, crash_severity);
CREATE INDEX idx_crashes_federal_fc     ON crashes(functional_class, crash_severity);

-- Spatial (for map views)
CREATE INDEX idx_crashes_coords         ON crashes(state, x, y);

-- Unique constraint for upsert (pipeline reload)
CREATE UNIQUE INDEX idx_crashes_objectid ON crashes(state, objectid);


-- ═══════════════════════════════════════════════════════════
--  RANKINGS (PRE-COMPUTED)
-- ═══════════════════════════════════════════════════════════
--  Pipeline pre-computes rankings per jurisdiction per road type.
--  Frontend reads these directly — no client-side computation.
--  Federal rankings computed across all states.
-- ═══════════════════════════════════════════════════════════

CREATE TABLE rankings (
    id              BIGSERIAL PRIMARY KEY,
    state           TEXT NOT NULL,           -- "delaware" or "_federal" for national
    tier            TEXT NOT NULL,           -- "federal","state","region","planning_district","mpo","county","city"
    jurisdiction    TEXT NOT NULL,           -- slug: "new_castle", "wilmapco", "_all" for state/federal
    road_type       TEXT NOT NULL,           -- "all_roads", "dot_roads", etc.
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
    pedestrian_fatal INTEGER,
    pedestrian_injury INTEGER,
    bike_fatal      INTEGER,
    bike_injury     INTEGER,
    data_json       JSONB,                  -- full ranking detail (24 ranking columns)
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(state, tier, jurisdiction, road_type, crash_year)
);

CREATE INDEX idx_rankings_lookup ON rankings(state, tier, jurisdiction, road_type);
CREATE INDEX idx_rankings_federal ON rankings(tier, crash_year) WHERE state = '_federal';


-- ═══════════════════════════════════════════════════════════
--  FEDERAL SUMMARY (MATERIALIZED VIEW)
-- ═══════════════════════════════════════════════════════════
--  Pre-computed national summary refreshed after each state sync.
--  Fast reads for federal dashboard without scanning all partitions.
-- ═══════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW federal_summary AS
SELECT
    crash_year,
    crash_severity,
    state,
    functional_class,
    ownership,
    area_type,
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
    COUNT(*) FILTER (WHERE work_zone_related = 'Yes')  AS wz_crashes
FROM crashes
GROUP BY crash_year, crash_severity, state, functional_class, ownership, area_type;

CREATE UNIQUE INDEX idx_federal_summary
    ON federal_summary(crash_year, crash_severity, state, functional_class, ownership, area_type);

-- Refresh after each state sync:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY federal_summary;


-- ═══════════════════════════════════════════════════════════
--  HIERARCHIES (per state)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE hierarchies (
    state           TEXT PRIMARY KEY,
    hierarchy_json  JSONB NOT NULL,         -- full hierarchy.json content
    regions         JSONB,                  -- extracted region list
    mpos            JSONB,                  -- extracted MPO list
    counties        JSONB,                  -- extracted county list
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════
--  ORGANIZATIONS & USERS (FUTURE — Phase 4)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE organizations (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,    -- "DelDOT", "VDOT", "CDOT", "CrashLens", "FHWA"
    org_type        TEXT NOT NULL,           -- "state_dot", "mpo", "federal", "platform"
    states          TEXT[] NOT NULL,         -- ['delaware'] or ['*'] for federal/platform
    config_json     JSONB,                  -- org-specific settings
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_roles (
    user_id         UUID,                   -- REFERENCES auth.users(id) when auth migrated
    org_id          BIGINT REFERENCES organizations(id),
    role            TEXT NOT NULL,           -- 'admin', 'analyst', 'viewer'
    permissions     TEXT[] DEFAULT '{}',     -- ['read', 'write', 'export', 'manage_users']
    PRIMARY KEY (user_id, org_id)
);

-- Pre-seed organizations
INSERT INTO organizations (name, org_type, states) VALUES
    ('CrashLens',  'platform',  ARRAY['*']),
    ('FHWA',       'federal',   ARRAY['*']),
    ('DelDOT',     'state_dot', ARRAY['delaware']),
    ('VDOT',       'state_dot', ARRAY['virginia']),
    ('CDOT',       'state_dot', ARRAY['colorado']);


-- ═══════════════════════════════════════════════════════════
--  PIPELINE RUNS (audit log)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    state           TEXT NOT NULL,
    stage           TEXT NOT NULL,           -- "download","normalize","enrich","split","sync"
    status          TEXT NOT NULL,           -- "running","success","failed"
    rows_processed  INTEGER,
    duration_sec    REAL,
    error_message   TEXT,
    metadata_json   JSONB,                  -- CI run URL, commit SHA, etc.
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pipeline_state ON pipeline_runs(state, created_at DESC);


-- ═══════════════════════════════════════════════════════════
--  ROW-LEVEL SECURITY (enabled in Phase 4)
-- ═══════════════════════════════════════════════════════════
--  Uncomment when auth migration is complete.
-- ═══════════════════════════════════════════════════════════

-- ALTER TABLE crashes ENABLE ROW LEVEL SECURITY;
--
-- -- Public read for anon (frontend with anon key)
-- CREATE POLICY "anon_read_all" ON crashes
--     FOR SELECT TO anon
--     USING (true);
--
-- -- Org-scoped access for authenticated users
-- CREATE POLICY "org_state_access" ON crashes
--     FOR SELECT TO authenticated
--     USING (
--         state = ANY(
--             SELECT unnest(o.states)
--             FROM user_roles ur
--             JOIN organizations o ON o.id = ur.org_id
--             WHERE ur.user_id = auth.uid()
--         )
--         OR EXISTS (
--             SELECT 1 FROM user_roles ur
--             JOIN organizations o ON o.id = ur.org_id
--             WHERE ur.user_id = auth.uid() AND '*' = ANY(o.states)
--         )
--     );
--
-- -- Admin write access
-- CREATE POLICY "admin_write" ON crashes
--     FOR ALL TO authenticated
--     USING (
--         EXISTS (
--             SELECT 1 FROM user_roles ur
--             WHERE ur.user_id = auth.uid() AND ur.role = 'admin'
--         )
--     );
```

### How Each Dashboard Tier Queries Supabase

```
┌──────────────────────────────────────────────────────────────────┐
│  FRONTEND QUERY MAP                                               │
│                                                                    │
│  FEDERAL VIEW (no state filter — scans all partitions)            │
│  ─────────────────────────────────────────────────────            │
│  SELECT * FROM federal_summary                                    │
│  WHERE crash_year BETWEEN 2018 AND 2024;                          │
│  → Pre-aggregated, instant response                               │
│                                                                    │
│  STATE VIEW                                                        │
│  ──────────                                                        │
│  SELECT * FROM crashes WHERE state = 'delaware';                  │
│  → Scans only crashes_delaware partition                          │
│                                                                    │
│  REGION VIEW (DOT District)                                        │
│  ──────────────────────────                                        │
│  SELECT * FROM crashes                                             │
│  WHERE state = 'virginia' AND dot_district = 'Salem';             │
│                                                                    │
│  PLANNING DISTRICT VIEW                                            │
│  ──────────────────────                                            │
│  SELECT * FROM crashes                                             │
│  WHERE state = 'virginia' AND planning_district = 'Roanoke Valley';│
│                                                                    │
│  MPO VIEW                                                          │
│  ────────                                                          │
│  SELECT * FROM crashes                                             │
│  WHERE state = 'delaware' AND mpo_name = 'WILMAPCO';             │
│                                                                    │
│  COUNTY VIEW                                                       │
│  ───────────                                                       │
│  SELECT * FROM crashes                                             │
│  WHERE state = 'delaware'                                         │
│    AND physical_juris_name = '003. New Castle';                   │
│                                                                    │
│  CITY/TOWN VIEW                                                    │
│  ──────────────                                                    │
│  SELECT * FROM crashes                                             │
│  WHERE state = 'delaware'                                         │
│    AND physical_juris_name LIKE '%City of Wilmington%';           │
│                                                                    │
│  ROAD TYPE FILTER (applied to any tier)                            │
│  ──────────────────────────────────────                            │
│  SET A: AND ownership = '1. State Hwy Agency'        -- dot_roads │
│  SET A: AND functional_class LIKE '1-%'              -- primary   │
│  SET B: AND ownership = '2. County Hwy Agency'       -- county   │
│  SET B: AND functional_class NOT LIKE '1-%'          -- no_intst │
│         AND functional_class NOT LIKE '2-%'                       │
└──────────────────────────────────────────────────────────────────┘
```

---

## DATA ISOLATION GUARANTEES

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL Partition Architecture                            │
│                                                               │
│  crashes (logical table)                                      │
│  ├── crashes_delaware    566K rows    (FIPS 10, 3 counties)  │
│  ├── crashes_virginia    2.1M rows   (FIPS 51, 133 counties) │
│  ├── crashes_colorado    800K rows   (FIPS 08, 64 counties)  │
│  ├── crashes_maryland    (future)                             │
│  ├── crashes_ohio        (future)                             │
│  ├── ...                                                      │
│  └── crashes_wyoming     (future)                             │
│                                                               │
│  ISOLATION RULES:                                             │
│  ✅ Delaware query NEVER touches Virginia partition            │
│  ✅ DROP crashes_ohio has ZERO impact on other states          │
│  ✅ Pipeline reload = DROP partition + CREATE + INSERT         │
│  ✅ Each partition has its own indexes                         │
│  ✅ VACUUM/maintenance per partition                           │
│  ✅ Federal view reads all partitions via one query            │
│  ✅ RLS can restrict users to specific state partitions        │
│                                                               │
│  NO DATA POLLUTION:                                           │
│  ✗ State A's pipeline cannot write to State B's partition     │
│  ✗ Bulk delete one state never risks touching another          │
│  ✗ Schema changes apply to all partitions uniformly           │
└─────────────────────────────────────────────────────────────┘
```

---

## ADDING A NEW STATE (3-step process)

```bash
# Step 1: Create partition (Supabase MCP or SQL)
CREATE TABLE crashes_ohio PARTITION OF crashes FOR VALUES IN ('ohio');

# Step 2: Register state
INSERT INTO states (abbr, name, fips, display_name)
VALUES ('oh', 'ohio', '39', 'Ohio');

# Step 3: Run pipeline
python supabase_sync.py --state ohio --input ohio_statewide.parquet.gz
# → Bulk inserts into crashes_ohio partition
# → Updates states table (total_crashes, year_range, last_sync_at)
# → Inserts rankings into rankings table
# → Refreshes federal_summary materialized view
```

That's it. Frontend automatically sees Ohio in the state picker and federal view.

---

## PIPELINE STAGES (UPDATED)

```
Stage 1: Socrata/DOT download
Stage 2: {state}_normalize.py (Tier 1 self-enrichment)
Stage 3: crash_enricher.py (Tier 3 HPMS → Tier 2 OSM → Tier 2b POI)
Stage 4: split.py → R2 upload (parquet.gz — KEPT as fallback)
Stage 5: supabase_sync.py → Supabase PostgreSQL ← NEW
           a) DROP + CREATE partition (clean reload)
           b) Bulk INSERT via COPY (fastest for 500K+ rows)
           c) Update states table metadata
           d) Upsert rankings table
           e) REFRESH MATERIALIZED VIEW CONCURRENTLY federal_summary
           f) Log to pipeline_runs
```

---

## PHASE TIMELINE

```
PHASE 1 — Database + Sync (NOW)                    ██████░░░░  2 weeks
  ✦ Create schema on self-hosted Supabase
  ✦ Build supabase_sync.py
  ✦ Sync Delaware as first state
  ✦ Verify via Supabase Studio

PHASE 2 — Frontend Dual-Source                      ░░░░░░░░░░  3 weeks
  ✦ Add Supabase client to frontend
  ✦ Implement all 7 tiers (federal → city)
  ✦ Supabase primary, R2 fallback
  ✦ Road type filtering via SQL WHERE clauses

PHASE 3 — Multi-State Expansion                     ░░░░░░░░░░  Ongoing
  ✦ Virginia sync (2.1M rows)
  ✦ Colorado sync
  ✦ Maryland, Ohio, etc.
  ✦ Federal dashboard goes live when 3+ states loaded

PHASE 4 — Auth Migration                            ░░░░░░░░░░  Future
  ✦ Firebase → Supabase Auth
  ✦ Organizations + user_roles tables
  ✦ Enable RLS policies
  ✦ State DOT agency accounts (DelDOT, VDOT, etc.)

PHASE 5 — Full Platform                             ░░░░░░░░░░  Vision
  ✦ Supabase Realtime (live crash data updates)
  ✦ Supabase Storage (replace R2 or keep as CDN)
  ✦ API keys for external consumers (MPOs, researchers)
  ✦ Scheduled pipeline via Supabase Edge Functions
```

---

## FULL ARCHITECTURE (VISION)

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  ┌───────────────────┐     ┌────────────────────────────────────┐  │
│  │  GitHub Actions    │     │  Hostinger VPS                     │  │
│  │  (Pipeline CI)     │     │                                    │  │
│  │                    │     │  ┌──────────────────────────────┐  │  │
│  │  50 state jobs     │     │  │  Supabase (self-hosted)      │  │  │
│  │  ├─ download      ─┼─────┼─▶│                              │  │  │
│  │  ├─ normalize      │     │  │  PostgreSQL                  │  │  │
│  │  ├─ enrich         │     │  │  ├─ crashes (partitioned)    │  │  │
│  │  ├─ split → R2     │     │  │  │  ├─ crashes_delaware     │  │  │
│  │  └─ sync → Supa   ─┼─────┼─▶│  │  ├─ crashes_virginia    │  │  │
│  │                    │     │  │  │  ├─ crashes_colorado     │  │  │
│  └────────────────────┘     │  │  │  └─ ... (50 states)      │  │  │
│                             │  │  ├─ rankings                 │  │  │
│  ┌───────────────────┐     │  │  ├─ federal_summary (matview)│  │  │
│  │  Cloudflare R2     │     │  │  ├─ hierarchies              │  │  │
│  │  (FALLBACK + CDN)  │     │  │  └─ pipeline_runs            │  │  │
│  │                    │     │  │                              │  │  │
│  │  parquet.gz files  │     │  │  Auth (Phase 4)              │  │  │
│  │  per jurisdiction  │     │  │  ├─ users                    │  │  │
│  │  per road type     │     │  │  ├─ organizations            │  │  │
│  └────────┬───────────┘     │  │  └─ user_roles + RLS        │  │  │
│           │                 │  │                              │  │  │
│           │  fallback       │  │  PostgREST API               │  │  │
│           │                 │  │  Realtime WebSocket           │  │  │
│           │                 │  └──────────────┬───────────────┘  │  │
│           │                 │                 │                   │  │
│           │                 └─────────────────┼───────────────────┘  │
│           │                                   │                      │
│           │              ┌────────────────────▼──────────────────┐  │
│           └──── fallback─▶│  Frontend (Cloudflare Workers)       │  │
│                          │                                       │  │
│                          │  7 Dashboard Tiers:                   │  │
│                          │  Federal → State → Region → PD →     │  │
│                          │  MPO → County → City/Town             │  │
│                          │                                       │  │
│                          │  Primary: Supabase PostgREST API     │  │
│                          │  Fallback: R2 parquet.gz files       │  │
│                          │  Auth: Supabase Auth (Phase 4)       │  │
│                          │  Maps: Mapbox/Leaflet                │  │
│                          └───────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Development Tools                                             │  │
│  │                                                                 │  │
│  │  Claude Desktop ──── MCP ────▶ Supabase (query, manage)       │  │
│  │  Claude Code    ──── MCP ────▶ Supabase + GitHub              │  │
│  │  Claude.ai      ──── Project Knowledge ──▶ Architecture       │  │
│  │  VPS Screen     ──── SSH ────▶ Long-running jobs (OSM, etc.)  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

---

## ESTIMATED SCALE (50 states)

| Metric | Per State (avg) | 50 States | Notes |
|--------|----------------|-----------|-------|
| Crash rows | ~500K | ~25M | PostgreSQL handles 100M+ easily |
| Disk (DB) | ~200MB | ~10GB | VPS has 80GB free |
| Disk (R2) | ~500MB | ~25GB | R2 free tier = 10GB, then $0.015/GB |
| Partitions | 1 | 50 | Each isolated, indexed independently |
| Rankings rows | ~2K | ~100K | Tiny table, fast lookups |
| Federal summary | — | ~50K rows | Materialized view, refreshed on sync |

---

## COST

| Component | Cost |
|-----------|------|
| Hostinger VPS (already paying) | $0 additional |
| Supabase (self-hosted on VPS) | $0 |
| Cloudflare R2 (fallback) | ~$0 (free tier covers most) |
| GitHub Actions (pipeline) | $0 (free tier) |
| Firebase → Supabase Auth | $0 (removes Firebase cost) |
| **Total additional cost** | **$0** |
