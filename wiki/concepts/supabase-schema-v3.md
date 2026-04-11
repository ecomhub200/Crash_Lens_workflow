---
title: Supabase Schema v3
type: concept
tags: [supabase, database, schema, migration, phase-1, 3-tier]
created: 2026-04-06
updated: 2026-04-06
sources: [CrashLens_Architecture_Roadmap_v2, delaware_parquet_validation]
status: validated
---

# Supabase Schema v3 --- 3-Tier Column Strategy

Production schema for CrashLens Supabase migration. **Validated against real Delaware pipeline output** (566,762 rows x 517 columns, `delaware__state_all_roads.parquet.gz`).

## 3-Tier Architecture

| Tier | Storage | Columns | Purpose |
|------|---------|---------|---------|
| Tier 1 | 111 explicit Postgres columns | Golden 69 + enrichment + analysis + proximity | Queryable, indexable, WHERE clauses |
| Tier 2 | `road_data` JSONB | 312 road inventory keys | hpms_*, map_*, bridge_*, poi_*, dot_*, etc. |
| Tier 3 | `state_extras` JSONB | varies (18 for DE) | {abbr}_* columns, state-specific |
| Rankings | `ranking_data` JSONB | 76 keys | 4 scopes x 19 metrics |

## Why 3 Tiers

- 517 columns is too many for explicit Postgres columns
- Tier 1 covers everything the frontend queries/filters on
- Tier 2 is display-only detail data (shown when clicking a crash)
- Tier 3 is state-specific --- auto-detected by {abbr}_ prefix
- NULL for missing data (not 0 --- 0 means "zero AADT" vs "unknown")

## State-Agnostic Design

- State extras auto-detected: any column starting with `{abbr}_` goes to `state_extras` JSONB
- Delaware has 18 state extras (de_Day_Of_Week_Code, de_Day_Of_Week_Description, etc.)
- Virginia will have va_* columns, Colorado will have co_* columns
- Road data prefixes are universal (hpms_*, map_*, dot_*, sdot_*, etc.)
- Unknown columns fall into road_data JSONB as safe catch-all
- Zero uncategorized columns for Delaware (all 517 classified)

## Validated Column Classification (Delaware --- 566,762 rows)

```
Tier 1 (explicit):      111  (all present in data)
Tier 2 (road_data):     312  (prefixes: dot_(50), hpms_(54), map_/osm_(59), nearest_*(69), conf_(6), xval_(4), risk_(7), curve_(8), te_(8), sdot_(24), ri_(5), + exact matches)
Tier 3 (state_extras):   18  (de_* columns)
Rankings:                 76  (4 scopes x 19 metrics: Juris_, District_, MPO_, PlanningDistrict_)
Total:                   517 of 517 (0 uncategorized, 0 catch-all)
```

## Validated Data Characteristics

### Data Types
- **All columns stored as string/object** in the pipeline parquet output
- Postgres types: most columns TEXT, some INTEGER (for SUM in matview), DOUBLE PRECISION for coordinates
- `crash_date` stored as TEXT (format: "M/D/YYYY" e.g. "4/29/2012")

### Severity Distribution
- O (Property Damage Only): 476,563 (84.1%)
- A (Incapacitating Injury): 88,425 (15.6%)
- K (Fatal): 1,774 (0.3%)
- Note: No B or C severity levels in Delaware data

### Boolean Flag Values
- Most flags use "Yes"/"No": Alcohol?, Speed?, Distracted?, Pedestrian?, Bike?
- **Exception**: `Work Zone Related` uses "1. Yes"/"2. No" (handled with LIKE '%Yes%' in matview)

### Integer Columns (needed for federal_summary SUM)
- K_People: 1,774 non-empty (matches K severity count)
- A_People: 88,425 non-empty (matches A severity count)
- B_People, C_People, Persons Injured, Pedestrians Killed, Pedestrians Injured, Vehicle Count: all empty/nan for Delaware
- Sync script handles with `fillna(0).astype(int)`

### Year Range
- 2009--2023 (15 years)
- 3 rows with empty Crash Year (handled as NULL)

### Key Column: DOT District (not VDOT District)
- The normalized pipeline output uses "DOT District" (state-agnostic)
- Early CSV versions used "VDOT District" (Virginia-specific)
- TIER1_MAP correctly maps "DOT District" -> "dot_district"

## Bugs Found and Fixed

### BUG 1: crash_date DATE type (FIXED)
- **Problem**: SQL had `crash_date DATE` but data is "M/D/YYYY" strings
- **Fix**: Changed to `crash_date TEXT` in migration SQL
- **Why**: TEXT matches pipeline philosophy; DATE parsing depends on datestyle setting

### BUG 2: Work Zone Related matview filter (FIXED)
- **Problem**: `WHERE work_zone_related = 'Yes'` but Delaware data has "1. Yes"
- **Fix**: Changed to `WHERE work_zone_related LIKE '%Yes%'` in federal_summary
- **Why**: Different states may use different boolean encodings

## Complete TIER1_MAP (111 columns, validated)

### Golden 69 (core crash attributes)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| OBJECTID | objectid | TEXT NOT NULL |
| Document Nbr | document_nbr | TEXT |
| Crash Year | crash_year | INTEGER |
| Crash Date | crash_date | TEXT |
| Crash Military Time | crash_military_time | TEXT |
| Crash Severity | crash_severity | TEXT |
| K_People | k_people | INTEGER DEFAULT 0 |
| A_People | a_people | INTEGER DEFAULT 0 |
| B_People | b_people | INTEGER DEFAULT 0 |
| C_People | c_people | INTEGER DEFAULT 0 |
| Persons Injured | persons_injured | INTEGER DEFAULT 0 |
| Pedestrians Killed | pedestrians_killed | INTEGER DEFAULT 0 |
| Pedestrians Injured | pedestrians_injured | INTEGER DEFAULT 0 |
| Vehicle Count | vehicle_count | INTEGER |
| Collision Type | collision_type | TEXT |
| Weather Condition | weather_condition | TEXT |
| Light Condition | light_condition | TEXT |
| Roadway Surface Condition | roadway_surface_cond | TEXT |
| Relation To Roadway | relation_to_roadway | TEXT |
| Roadway Alignment | roadway_alignment | TEXT |
| Roadway Surface Type | roadway_surface_type | TEXT |
| Roadway Defect | roadway_defect | TEXT |
| Roadway Description | roadway_description | TEXT |
| Intersection Type | intersection_type | TEXT |
| Traffic Control Type | traffic_control_type | TEXT |
| Traffic Control Status | traffic_control_status | TEXT |
| Work Zone Related | work_zone_related | TEXT |
| Work Zone Location | work_zone_location | TEXT |
| Work Zone Type | work_zone_type | TEXT |
| School Zone | school_zone | TEXT |
| First Harmful Event | first_harmful_event | TEXT |
| First Harmful Event Loc | first_harmful_event_loc | TEXT |
| Alcohol? | alcohol | TEXT |
| Animal Related? | animal_related | TEXT |
| Unrestrained? | unrestrained | TEXT |
| Bike? | bike | TEXT |
| Distracted? | distracted | TEXT |
| Drowsy? | drowsy | TEXT |
| Drug Related? | drug_related | TEXT |
| Guardrail Related? | guardrail_related | TEXT |
| Hitrun? | hitrun | TEXT |
| Lgtruck? | lgtruck | TEXT |
| Motorcycle? | motorcycle | TEXT |
| Pedestrian? | pedestrian | TEXT |
| Speed? | speed | TEXT |
| Max Speed Diff | max_speed_diff | TEXT |
| RoadDeparture Type | road_departure_type | TEXT |
| Intersection Analysis | intersection_analysis | TEXT |
| Senior? | senior | TEXT |
| Young? | young | TEXT |
| Mainline? | mainline | TEXT |
| Night? | night | TEXT |
| DOT District | dot_district | TEXT |
| Juris Code | juris_code | TEXT |
| Physical Juris Name | physical_juris_name | TEXT |
| Functional Class | functional_class | TEXT |
| Facility Type | facility_type | TEXT |
| Area Type | area_type | TEXT |
| SYSTEM | system | TEXT |
| VSP | vsp | TEXT |
| Ownership | ownership | TEXT |
| Planning District | planning_district | TEXT |
| MPO Name | mpo_name | TEXT |
| RTE Name | rte_name | TEXT |
| RNS MP | rns_mp | TEXT |
| Node | node | TEXT |
| Node Offset (ft) | node_offset_ft | TEXT |
| x | x | DOUBLE PRECISION |
| y | y | DOUBLE PRECISION |

### Extra Enrichment (4)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| FIPS | fips | TEXT |
| Place FIPS | place_fips | TEXT |
| EPDO_Score | epdo_score | TEXT |
| Intersection Name | intersection_name | TEXT |

### Key Analysis (10)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| Through_Lanes | through_lanes | TEXT |
| AADT | aadt | TEXT |
| AADT_source | aadt_source | TEXT |
| Lane_Width_ft | lane_width_ft | TEXT |
| Median_Width_ft | median_width_ft | TEXT |
| Shoulder_Width_ft | shoulder_width_ft | TEXT |
| Has_Sidewalk | has_sidewalk | TEXT |
| Has_Bike_Lane | has_bike_lane | TEXT |
| Urban_Area_Name | urban_area_name | TEXT |
| Urban_Area_GEOID | urban_area_geoid | TEXT |

### POI Proximity Flags (11)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| Near_PoiBar_1500ft | near_poi_bar_1500ft | TEXT |
| Near_PoiClinic_1500ft | near_poi_clinic_1500ft | TEXT |
| Near_PoiCollege_1500ft | near_poi_college_1500ft | TEXT |
| Near_PoiCrossing_100ft | near_poi_crossing_100ft | TEXT |
| Near_PoiFuel_500ft | near_poi_fuel_500ft | TEXT |
| Near_PoiHospital_2000ft | near_poi_hospital_2000ft | TEXT |
| Near_PoiParking_500ft | near_poi_parking_500ft | TEXT |
| Near_PoiRestArea_1000ft | near_poi_rest_area_1000ft | TEXT |
| Near_PoiRestaurant_500ft | near_poi_restaurant_500ft | TEXT |
| Near_PoiSignal_100ft | near_poi_signal_100ft | TEXT |
| Near_PoiStopSign_100ft | near_poi_stop_sign_100ft | TEXT |

### Federal Asset Proximity (4)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| Near_Bridge_500ft | near_bridge_500ft | TEXT |
| Near_RailXing_500ft | near_rail_xing_500ft | TEXT |
| Near_School_1500ft | near_school_1500ft | TEXT |
| Near_Transit_500ft | near_transit_500ft | TEXT |

### Resolved Values (5)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| resolved_speed_limit | resolved_speed_limit | TEXT |
| resolved_has_lighting | resolved_has_lighting | TEXT |
| resolved_has_signal | resolved_has_signal | TEXT |
| resolved_on_bridge | resolved_on_bridge | TEXT |
| resolved_school_zone | resolved_school_zone | TEXT |

### Intersection and Ramp (4)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| is_intersection | is_intersection | TEXT |
| intersection_degree | intersection_degree | TEXT |
| is_ramp | is_ramp | TEXT |
| ramp_type | ramp_type | TEXT |

### Road Geometry (4)
| CSV Column | Postgres Column | Type |
|-----------|----------------|------|
| curvature | curvature | TEXT |
| length_ft | length_ft | TEXT |
| road_lon | road_lon | TEXT |
| road_lat | road_lat | TEXT |

## Road Data JSONB Prefixes (Tier 2)

The following prefixes route columns to `road_data` JSONB:

```
resolved_*, conf_*, xval_*, risk_*, curve_*, te_*,
hpms_*, nearest_bridge*, bridge_count*, nearest_rail*, rail_xing*,
nearest_school*, school_count*, nearest_transit*, transit_count*,
nearest_poi*, poi_*, map_*, osm_*, dot_*, sdot_*, ri_*,
geometry_coords, length_m, divider
```

Plus exact matches: `Peak_Lanes, Structure_Type, Cracking_Pct, ARNOLD_Route_ID, ARNOLD_Begin_MP, ARNOLD_End_MP`

**Note**: The 5 `resolved_*` columns in Tier 1 are excluded from road_data because the classify_columns() function checks Tier 1 membership first.

## Ranking Data (76 columns)

4 scopes x 19 metrics:
- **Scopes**: Juris_, District_, MPO_, PlanningDistrict_
- **Metrics**: total_crash, total_ped_crash, total_bike_crash, total_fatal, total_fatal_serious_injury, total_epdo, trend_total_crash, trend_fatal, trend_ksi, trend_epdo, trend_ped_crash, trend_bike_crash, pct_night_fatal, pct_impaired_crash, pct_distracted_crash, pct_speed_crash, severity_index, fatality_rate, safety_score

Pattern: `{Scope}_Rank_{metric}` (e.g., `District_Rank_total_crash`)

## Delaware State Extras (18 columns)

```
de_Day_Of_Week_Code, de_Day_Of_Week_Description,
de_Crash_Classification_Code, de_Collision_On_Private_Property,
de_Manner_Of_Impact_Code, de_Road_Surface_Code,
de_Lighting_Condition_Code, de_Weather_1_Code,
de_Weather_2_Code, de_Weather_2_Description,
de_Motorcycle_Helmet_Used, de_Bicycle_Helmet_Used,
de_Primary_Contributing_Circumstance_Code,
de_Primary_Contributing_Circumstance_Description,
de_School_Bus_Involved_Code, de_Work_Zone_Location_Code,
de_Work_Zone_Type_Code, de_Workers_Present
```

---

## Claude Chat Execution Guide

This section contains exact SQL for Claude Chat (claude.ai) to execute via Supabase MCP. Execute each block in order.

### Pre-requisites
- SSH tunnel to srv1503081.hstgr.cloud is active
- Supabase MCP is connected in Claude Desktop
- `supabase_sync.py` is in the Crash_Lens_workflow repo root

### Step 1: Create Extensions

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

### Step 2: Create States Table

```sql
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
```

### Step 3: Create Crashes Table (partitioned, 111 Tier 1 columns + 3 JSONB)

```sql
CREATE TABLE IF NOT EXISTS crashes (
    id                      BIGSERIAL,
    state                   TEXT NOT NULL,

    -- Golden 69
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

    -- Extra Enrichment (4)
    fips                    TEXT,
    place_fips              TEXT,
    epdo_score              TEXT,
    intersection_name       TEXT,

    -- Key Analysis (10)
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

    -- POI Proximity Flags (11)
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

    -- Federal Asset Proximity (4)
    near_bridge_500ft       TEXT,
    near_rail_xing_500ft    TEXT,
    near_school_1500ft      TEXT,
    near_transit_500ft      TEXT,

    -- Resolved Values (5)
    resolved_speed_limit    TEXT,
    resolved_has_lighting   TEXT,
    resolved_has_signal     TEXT,
    resolved_on_bridge      TEXT,
    resolved_school_zone    TEXT,

    -- Intersection and Ramp (4)
    is_intersection         TEXT,
    intersection_degree     TEXT,
    is_ramp                 TEXT,
    ramp_type               TEXT,

    -- Road Geometry (4)
    curvature               TEXT,
    length_ft               TEXT,
    road_lon                TEXT,
    road_lat                TEXT,

    -- JSONB Bags
    road_data               JSONB,
    state_extras            JSONB,
    ranking_data            JSONB,

    -- Metadata
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (id, state)
) PARTITION BY LIST (state);
```

### Step 4: Create Partitions

```sql
CREATE TABLE IF NOT EXISTS crashes_delaware  PARTITION OF crashes FOR VALUES IN ('delaware');
CREATE TABLE IF NOT EXISTS crashes_virginia  PARTITION OF crashes FOR VALUES IN ('virginia');
CREATE TABLE IF NOT EXISTS crashes_colorado  PARTITION OF crashes FOR VALUES IN ('colorado');
```

### Step 5: Create Indexes

```sql
-- Jurisdiction lookups
CREATE INDEX IF NOT EXISTS idx_crashes_state_year     ON crashes(state, crash_year);
CREATE INDEX IF NOT EXISTS idx_crashes_county         ON crashes(state, physical_juris_name);
CREATE INDEX IF NOT EXISTS idx_crashes_mpo            ON crashes(state, mpo_name);
CREATE INDEX IF NOT EXISTS idx_crashes_dot_district   ON crashes(state, dot_district);
CREATE INDEX IF NOT EXISTS idx_crashes_planning_dist  ON crashes(state, planning_district);
CREATE INDEX IF NOT EXISTS idx_crashes_severity       ON crashes(state, crash_severity);

-- Road type filters
CREATE INDEX IF NOT EXISTS idx_crashes_ownership      ON crashes(state, ownership);
CREATE INDEX IF NOT EXISTS idx_crashes_fc             ON crashes(state, functional_class);

-- Federal view
CREATE INDEX IF NOT EXISTS idx_crashes_year_severity  ON crashes(crash_year, crash_severity);
CREATE INDEX IF NOT EXISTS idx_crashes_federal_fc     ON crashes(functional_class, crash_severity);

-- Spatial
CREATE INDEX IF NOT EXISTS idx_crashes_coords         ON crashes(state, x, y);

-- Upsert key
CREATE UNIQUE INDEX IF NOT EXISTS idx_crashes_objectid ON crashes(state, objectid);
```

### Step 6: Create Rankings Table

```sql
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
```

### Step 7: Create Federal Summary Materialized View

```sql
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
```

### Step 8: Create Supporting Tables

```sql
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
```

### Step 9: Create Organizations and User Roles

```sql
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
```

### Step 10: Seed Delaware

```sql
INSERT INTO states (abbr, name, fips, display_name, pipeline_status)
VALUES ('de', 'delaware', '10', 'Delaware', 'pending')
ON CONFLICT (abbr) DO NOTHING;
```

### Step 11: Run Sync

```bash
python supabase_sync.py --state de --from-r2
```

### Step 12: Verify (run queries from verify_supabase.sql)

Expected results after sync:
- `crashes_delaware` row count: 566,762
- Year range: 2009--2023
- Severity: O=476,563, A=88,425, K=1,774
- states table: de | delaware | 10 | Delaware | active
- road_data JSONB keys: ~312 per row (varies, empty values stripped)
- state_extras JSONB keys: up to 18 per row
- ranking_data JSONB keys: up to 76 per row

## Files

- `001_crashlens_migration_v3.sql` --- Full schema DDL (validated, bugs fixed)
- `supabase_sync.py` --- Pipeline Stage 5 v3.0 (state-agnostic, validated)
- `verify_supabase.sql` --- Post-migration verification queries

## Related Pages

- [[supabase-schema]] --- Original v1 spec (superseded)
- [[delaware-pipeline]] --- Reference state
- [[data-pipeline-architecture]] --- Pipeline stages
- [[build-road-inventory]] --- Road inventory consolidation
- [[crash-enrichment]] --- 4-tier enrichment process


## Schema v3.1 — Frontend Migration Columns (2026-04-08)

### Applied and Verified

| Addition | Type | Result |
|----------|------|--------|
| `crash_date_parsed` | DATE | 566,759/566,762 parsed |
| `geom` | GEOMETRY(Point, 4326) | 558,771/566,762 points |
| `idx_crashes_date` | B-tree (state, crash_date_parsed) | Created |
| `idx_crashes_geom` | GiST (geom) | Created |
| `idx_crashes_hotspot` | B-tree (state, node, rte_name) | Created |
| `idx_crashes_intersection` | Partial B-tree | Created |
| `jurisdiction_baselines` | Materialized view | 81 rows |
| EPDO weights | states.config_json | DE set |

**Total columns:** 118 → 120.

### supabase_sync.py v3.1 Changes

- `build_sync_df()` parses `crash_date` → `crash_date_parsed` DATE
- `sync()` runs `UPDATE SET geom = ST_SetSRID(ST_Point(x,y), 4326)` after COPY
- `sync()` refreshes `jurisdiction_baselines` matview
- `get_db_connection()` adds `options='-c search_path=public'` + connection test

### Self-Hosted Supabase Access Methods

| Method | Port | Works? |
|--------|------|--------|
| Direct Postgres (VPS) | 5433 | ✅ psycopg2 COPY |
| Supavisor (VPS) | 5432, 6543 | ❌ Tenant not found |
| SSH tunnel (GitHub Actions) | localhost:5432 → VPS:5433 | ✅ |
| SSH tunnel (Claude Desktop) | localhost:5432 → VPS:5433 | ✅ |
| Supabase Studio | https://srv1503081.hstgr.cloud | ✅ SQL Editor |
| Cloud Supabase MCP (claude.ai) | N/A | ❌ Different service |


## Schema v3.1 — Frontend Migration Columns (2026-04-08)

New columns: `crash_date_parsed DATE`, `geom GEOMETRY(Point, 4326)`.
New indexes: `idx_crashes_date`, `idx_crashes_geom` (GiST), `idx_crashes_hotspot`, `idx_crashes_intersection`.
New matview: `jurisdiction_baselines` (81 rows for Delaware).
EPDO weights in `states.config_json`. Total columns: 118 → 120.

**Verified results:**
- crash_date_parsed: 566,759/566,762 (3 null dates)
- geom: 558,771/566,762 (7,991 missing coords — geocoding failures in source)
- jurisdiction_baselines: 81 rows
- supabase_sync.py v3.1: populates crash_date_parsed + geom during every pipeline run

**Self-hosted access (verified port map):**
| Port | Service | psycopg2? |
|------|---------|-----------|
| 5432 | Supavisor | ❌ |
| 6543 | Supavisor | ❌ |
| 5433 | Direct Postgres | ✅ |


### First Successful Pipeline Run (2026-04-08)
Delaware Batch Pipeline completed end-to-end with Supabase sync via port 5433 (direct Postgres). Stage 4.5 took 49m47s — client-side timeout but server committed. Resume support added for large states (--resume flag, batched geom UPDATE, 360min timeout).


## AADT Storage Clarification (2026-04-08)

AADT is a **Tier 1 explicit column** (`aadt TEXT`, 85.2% filled), NOT in `road_data` JSONB. The JSONB `road_data` contains 27 supplemental `hpms_*` keys (future_aadt, iri, design_speed, lane_width, median_type, nhs, etc.) but not current AADT — it was promoted to Tier 1 because the frontend queries and filters on it directly. There is no `hpms_aadt` key in JSONB. This is correct by design.
