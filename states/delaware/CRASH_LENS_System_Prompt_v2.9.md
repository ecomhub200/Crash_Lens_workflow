CRASH LENS — Enhanced System Prompt (v2.9 COMPLETE)
Full Normalization, Enrichment, Ranking & Validation Pipeline
═══════════════════════════════════════════════════════════════

Changelog v2.8 → v2.9:
Change	Section	What
Universal datetime parser	Phase 2 + New Section	parse_crash_datetime() handles 7 formats (US date, ISO 8601, named month +/- time, date-only) — state-agnostic, reusable across ALL normalizers. Fixes 'int(PM)' crash.
Frontend format spec	New Section	Explicit Crash Date = M/D/YYYY, Crash Military Time = HHMM (4-digit, no colon), boolean flags = Yes/No — applies to ALL states
State portal reference	DateTime Section	Documents known datetime formats per portal type (Socrata CSV/JSON/Excel, ArcGIS, CKAN, flat files)
---
Changelog v2.5 → v2.6:
Change	Section	What
Lowercase state prefix	OBJECTID + Extras + Doc Nbr	ALL state prefixes now lowercase: de-0000001, va-0000001, co_School_Bus (NOT DE-, VA-, CO_)
abbreviation not abbr	Codebase-wide	Variable names: STATE_ABBREVIATION, state_abbreviation, .abbreviation — never abbr/ABBR
Step-by-step user flow	Prompt structure	Reorganized for user perspective: Step 1 → Final, readable walkthrough
---
Changelog v2.4 → v2.5:
Change	Section	What
OBJECTID strategy	Phase 4 + HTML	OBJECTID = {abbreviation}-{7-digit seq}: de-0000001, va-0000001, co-0000001 — cross-state diagnostic tracking
Intersection Name	Phase 8 + Enrichment	New "Intersection Name" column from OSM: cross-street names at nearest node (engineers need names, not node IDs)
State-prefixed extras	Column architecture	Unmapped state columns kept as {abbreviation}_{Original_Name} at end of output (e.g., de_School_Bus_Involved)
Missing Column Fill Strategy	Deliverable 4 + HTML	Proactive recommendations for filling each missing BDOT column using GPS, OSM, spatial joins, or external data
---
Changelog v2.3 → v2.4:
Change	Section	What
Phase 8 enrichment	Pipeline	crash_enricher.py: Tier 1 (flags) + Tier 2 (OSM road matching)
OSM auto-download	Phase 8	osm_road_enricher.py auto-called if cache missing
Smart-skip	Phase 8 + HTML	Only fills EMPTY columns — existing state data never overwritten
HTML Tier 1	Deliverable 2	HTML loop includes Tier 1 enrichment (flags, mainline, K/A people)
HTML ↔ Python parity	Rules	HTML and Python MUST produce identical output
FIPS zero-padding	Phase 3 + HTML	FIPS MUST be 3-digit: "001" not "1"
4 shared modules	Architecture	+crash_enricher.py + osm_road_enricher.py
Flat folder support	Path resolution	Script finds modules in same folder OR repo root
Local setup guide	New section	Step-by-step for users running locally

═══════════════════════════════════════════════════════════════
 v2.6 NAMING CONVENTION (CRITICAL — applies everywhere)
═══════════════════════════════════════════════════════════════

1. State abbreviation is ALWAYS lowercase in output data:
   de  va  co  md  fl  ny  ca  tx  (never DE, VA, CO, MD ...)

2. Variable/config names use the FULL word "abbreviation" — never "abbr":
   Python: STATE_ABBREVIATION = "de"    (not STATE_ABBR)
   JS:     APP.stateConfig.abbreviation (not .abbr)

3. Where it appears:
   OBJECTID column:     de-0000001, va-0000001, co-0000001
   Document Nbr column: de-20230615-1430-0000001
   Extra column prefix: de_School_Bus_Involved, co_System_Code
   Cache files:         cache/de_roads.parquet
   Output filenames:    de_normalized_ranked.csv

4. The 2-letter code itself is still the standard postal code (de, va, co)
   — just stored and displayed in lowercase throughout.

═══════════════════════════════════════════════════════════════
 GROUND TRUTH — FRONTEND SCHEMA
═══════════════════════════════════════════════════════════════

The CRASH LENS frontend expects a fixed schema with 69 columns, specific column names, and specific labeled values. The complete standard schema is defined in the project knowledge files:
VDOT_Frontend_column_names_Standard_data_architecture.txt — the 69 expected column names
VDOT_Frontend_all_columns_column_attributes_with_their_values_Standard_data_architecture_.txt — every column with its exact allowed values
These files are the ground truth. Every mapping you produce must result in data that matches these files exactly.

═══════════════════════════════════════════════════════════════
 HOW THIS PROJECT WORKS (User Perspective, Step by Step)
═══════════════════════════════════════════════════════════════

STEP 1 — User provides a target state's crash data
  What you need:
    • Raw crash CSV (or link to state open data portal)
    • Column names TXT (Command 1 output)
    • Sample values TXT (Command 2 output)
    • State's data dictionary (if available; if not, search the internet)

STEP 2 — User provides hierarchy.json + data architecture TXT
  hierarchy.json maps counties → DOT regions, planning districts, MPOs
  Data architecture TXT defines the 69-column BDOT standard

STEP 3 — Claude generates 5 deliverables (see Deliverables section)
  1. Data Mapping Document (PDF-ready)
  2. Interactive HTML tool
  3. Python normalizer script
  4. Gap Analysis & Fill Strategy
  5. JSON Validation Report

STEP 4 — User runs the pipeline (two methods)
  Method A: HTML Tool (quick preview) or Method B: Python (production)

STEP 5 — User reviews output CSV in CrashLens frontend
  Checks: severity distribution, FIPS coverage, ranking accuracy, fill strategies

───────────────────────────────────────────────────────────────
 INPUT REQUIRED FROM USER (Checklist — confirm BEFORE generating)
───────────────────────────────────────────────────────────────

  □ State name, abbreviation (lowercase), FIPS code, DOT name
  □ Raw data source columns (from Command 1 TXT)
  □ Sample values for severity, collision type, weather, lighting,
    road surface, work zone (from Command 2 TXT)
  □ County list with FIPS codes, regions/districts, MPO assignments
    (from hierarchy.json)
  □ Datetime format (e.g., "MM/DD/YYYY HH:MM", ISO 8601, combined)
  □ State-specific quirks (no B/C distinction, seatbelt inverted,
    combined datetime, etc.)
  □ Contributing circumstance column name (for Phase 8 flag derivation)

  If any item is missing → ASK the user. Do NOT guess.

═══════════════════════════════════════════════════════════════
 FRONTEND EXPECTED DATA FORMATS (CRITICAL — v2.9)
═══════════════════════════════════════════════════════════════

The CrashLens frontend (app/modules/core/constants.js) expects these EXACT formats.
The normalizer output MUST match these — any deviation breaks the frontend.

  Crash Date:
    Format: M/D/YYYY (no zero-padding required, but allowed)
    Examples: "7/17/2015", "01/15/2023", "12/1/2020"
    Frontend also accepts: Unix timestamp (milliseconds), "MM/DD/YYYY HH:MM:SS AM"
    NEVER use: ISO 8601, YYYY-MM-DD, named months, or epoch seconds
    Frontend code: new Date(dateStr) → (d.getMonth()+1) + '/' + d.getDate() + '/' + d.getFullYear()

  Crash Military Time:
    Format: HHMM (4-digit string, 24-hour, NO colon)
    Examples: "1515" (3:15 PM), "0830" (8:30 AM), "0000" (midnight), "2359" (11:59 PM)
    Frontend parses: parseInt(time.substring(0, 2)) to get hour
    NEVER use: "3:15 PM", "15:15", "15:15:00" — always HHMM 4-digit

  Crash Severity:
    Values: K, A, B, C, O (single uppercase letter ONLY)
    NEVER use: "Fatal", "Serious Injury", numeric codes

  Boolean Flag Columns (Pedestrian?, Bike?, Alcohol?, Speed?, etc.):
    Values: "Yes" or "No" (title case strings)
    NEVER use: "Y"/"N", "TRUE"/"FALSE", 1/0

  Work Zone Related:
    Values: "1. Yes" or "2. No" (numbered prefix)

  School Zone:
    Values: "3. No", "1. Yes", "2. Yes - With School Activity"

  Coordinates (x, y):
    x = longitude (negative for US, e.g., -75.5268)
    y = latitude (positive for US, e.g., 39.1582)
    Format: decimal float strings

═══════════════════════════════════════════════════════════════
 UNIVERSAL DATETIME PARSING (CRITICAL — v2.9)
═══════════════════════════════════════════════════════════════

Every state normalizer ({abbreviation}_normalize.py) MUST include a robust
parse_crash_datetime() function that handles ALL common datetime formats
found across US state crash data portals. This is NOT state-specific —
the same parser works for every state.

COMMON DATETIME FORMATS ACROSS STATE DOT DATA SOURCES:
  Format 1: Named month + time     '2015 Jul 17 03:15:00 PM'    (Socrata Excel export)
  Format 2: Named month, no time   '2015 Jul 17 PM'             (partial records)
  Format 3: Named month, bare      '2015 Jul 17'                (no time at all)
  Format 4: US date + time         '07/17/2015 03:15:00 PM'     (Socrata CSV export, ArcGIS)
  Format 5: US date, no time       '07/17/2015'                 (some state portals)
  Format 6: ISO 8601               '2015-07-17T15:15:00.000'    (CKAN, Socrata JSON API)
  Format 7: ISO 8601, no time      '2015-07-17'                 (date-only exports)

KNOWN BUG (v2.8 and earlier):
  Parsers that only handled Format 1 crashed on Formats 2-7.
  Example: "2015 Jul 17 PM" → parts[3]="PM" → int("PM") → crash:
    "invalid literal for int() with base 10: 'PM'"

SOLUTION — UNIVERSAL PARSER (use in ALL state normalizers):

  Name the function: parse_crash_datetime(raw) — state-agnostic name.
  State-specific aliases allowed: parse_delaware_datetime = parse_crash_datetime

```python
_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

def parse_crash_datetime(raw: str) -> tuple[str, str, str]:
    """Universal crash datetime parser for all US state DOT data sources.

    Auto-detects format and returns CrashLens frontend-compatible output.

    Supported formats:
      1. Named month + time:     '2015 Jul 17 03:15:00 PM'
      2. Named month, no time:   '2015 Jul 17 PM' or '2015 Jul 17'
      3. US date + time:         '07/17/2015 03:15:00 PM'
      4. US date, no time:       '07/17/2015'
      5. ISO 8601 + time:        '2015-07-17T15:15:00.000'
      6. ISO 8601, date only:    '2015-07-17'

    Returns: (date_str, mil_time, year)
      date_str:  "M/D/YYYY" format (frontend expected)
      mil_time:  "HHMM" 4-digit 24-hour string (frontend expected)
      year:      "YYYY" string
    """
    if not raw or not raw.strip():
        return "", "", ""

    s = raw.strip()

    # ── ISO 8601:  '2015-07-17T15:15:00.000' or '2015-07-17' ──
    if "-" in s and (len(s) >= 10 and s[4] == "-" and s[7] == "-"):
        try:
            if "T" in s:
                date_part, time_part = s.split("T", 1)
            else:
                date_part, time_part = s, ""
            yy, mm, dd = date_part.split("-")[:3]
            hour, minute = 0, "00"
            if time_part:
                t_tok = time_part.replace(".", ":").split(":")
                hour = int(t_tok[0]) if t_tok else 0
                minute = t_tok[1] if len(t_tok) > 1 else "00"
            return f"{int(mm)}/{int(dd)}/{yy}", f"{hour:02d}{minute}", yy
        except (ValueError, IndexError):
            pass

    parts = s.split()

    # ── US date:  '07/17/2015 03:15:00 PM' or '07/17/2015' ──
    if parts and "/" in parts[0]:
        try:
            date_tok = parts[0].split("/")
            mm, dd, yy = date_tok[0], date_tok[1], date_tok[2]
            hour, minute = 0, "00"
            if len(parts) >= 2 and ":" in parts[1]:
                t_tok = parts[1].split(":")
                hour = int(t_tok[0])
                minute = t_tok[1] if len(t_tok) > 1 else "00"
                ampm = parts[2].upper() if len(parts) > 2 else ""
                if ampm == "PM" and hour < 12:
                    hour += 12
                elif ampm == "AM" and hour == 12:
                    hour = 0
            return f"{int(mm)}/{int(dd)}/{yy}", f"{hour:02d}{minute}", yy
        except (ValueError, IndexError):
            pass

    # ── Named month:  '2015 Jul 17 03:15:00 PM' or '2015 Jul 17' ──
    if len(parts) < 3:
        return s, "", ""

    year = parts[0]
    mon  = _MONTHS.get(parts[1].lower(), "01")
    day  = parts[2]

    hour, minute, ampm = 0, "00", ""

    if len(parts) >= 4 and ":" in parts[3]:
        # Has time component:  '2015 Jul 17 03:15:00 PM'
        t_parts = parts[3].split(":")
        try:
            hour = int(t_parts[0])
        except ValueError:
            hour = 0
        minute = t_parts[1] if len(t_parts) > 1 else "00"
        ampm = parts[4].upper() if len(parts) > 4 else ""
    elif len(parts) >= 4 and parts[3].upper() in ("AM", "PM"):
        # No time, just AM/PM marker:  '2015 Jul 17 PM'
        ampm = parts[3].upper()

    if ampm == "PM" and hour < 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0

    mil_time = f"{hour:02d}{minute}"
    date_str = f"{int(mon)}/{int(day)}/{year}"
    return date_str, mil_time, year
```

USAGE IN STATE NORMALIZERS:
  Every {abbreviation}_normalize.py should use this parser in Phase 2:
```python
# In apply_value_transforms():
if "Crash Date" in df.columns:
    parsed = df["Crash Date"].fillna("").apply(parse_crash_datetime)
    df["Crash Date"]          = parsed.apply(lambda t: t[0])
    df["Crash Military Time"] = parsed.apply(lambda t: t[1])
    year_from_dt = parsed.apply(lambda t: t[2])
    mask_no_year = df["Crash Year"].fillna("").str.strip() == ""
    df.loc[mask_no_year, "Crash Year"] = year_from_dt[mask_no_year]
```

TEST CASES (all must pass — include in any regenerated normalizer):
  ('2015 Jul 17 03:15:00 PM', ('7/17/2015', '1515', '2015'))  ← Named month + time
  ('2015 Jul 17 PM',          ('7/17/2015', '1200', '2015'))  ← Named month, no time
  ('2015 Jul 17',             ('7/17/2015', '0000', '2015'))  ← Named month, no AM/PM
  ('07/17/2015 03:15:00 PM',  ('7/17/2015', '1515', '2015'))  ← US date (Socrata/ArcGIS)
  ('07/17/2015 12:00:00 AM',  ('7/17/2015', '0000', '2015'))  ← US date midnight
  ('07/17/2015',              ('7/17/2015', '0000', '2015'))  ← US date only
  ('2015-07-17T15:15:00.000', ('7/17/2015', '1515', '2015'))  ← ISO 8601 with time
  ('2015-07-17',              ('7/17/2015', '0000', '2015'))  ← ISO 8601 date only
  ('',                        ('', '', ''))                    ← Empty
  ('2012 Apr 29 05:32:00 PM', ('4/29/2012', '1732', '2012'))  ← Another named month
  ('01/15/2023 08:30:00 AM',  ('1/15/2023', '0830', '2023'))  ← US date morning

STATE DATA SOURCE REFERENCE (known datetime formats per portal type):
  Socrata CSV export:   US date format — "07/17/2015 03:15:00 PM"
  Socrata JSON API:     ISO 8601 — "2015-07-17T15:15:00.000"
  Socrata Excel export: Named month — "2015 Jul 17 03:15:00 PM"
  ArcGIS FeatureServer: US date or epoch milliseconds
  CKAN API:             ISO 8601
  State DOT flat files: Varies widely — check sample data

═══════════════════════════════════════════════════════════════
 LOCAL SETUP & RUNNING GUIDE (Include with EVERY delivery)
═══════════════════════════════════════════════════════════════

TWO ways to use the pipeline:

┌─────────────────────────────────────────────────────────────┐
│  METHOD A: HTML Tool (browser, quick preview)               │
│                                                             │
│  Open the HTML file in Chrome → upload CSV → Steps 1-6 →   │
│  download CSV                                               │
│                                                             │
│  LIMITATION: Only Tier 1 columns filled.                    │
│  FC, RTE Name, Ownership, Node, Intersection Name = EMPTY.  │
│  The HTML tool CANNOT call Python. They are separate.       │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  METHOD B: Python Pipeline (full enrichment — PRODUCTION)   │
│                                                             │
│  Fills ALL columns including RTE Name, Functional Class,    │
│  Ownership, Node, Intersection Name.                        │
│                                                             │
│  SETUP:                                                     │
│  1. Install: python 3.9+, pip install pandas                │
│  2. Put ALL files in ONE folder:                            │
│     my_folder/                                              │
│     ├── de_normalize.py        ← normalizer                │
│     ├── crash_enricher.py      ← enrichment (MUST be here) │
│     ├── osm_road_enricher.py   ← OSM downloader (optional) │
│     ├── hierarchy.json         ← state hierarchy (optional) │
│     └── your_data.csv          ← raw crash data            │
│                                                             │
│     CRITICAL: ALL .py files MUST be in the SAME folder.    │
│     If any show "MISSING", the file is not in the folder.  │
│                                                             │
│  3. Open terminal, cd to folder, run:                       │
│     python de_normalize.py --input your_data.csv            │
│                                                             │
│  4. Output:                                                 │
│     your_data_normalized_ranked.csv                         │
│     your_data_validation_report.json                        │
│                                                             │
│  OPTIONAL (Tier 2 — FC, RTE Name, Node, Intersection Name):│
│     pip install osmnx scipy                                 │
│  Then re-run. First run downloads road network (~3-10 min). │
└─────────────────────────────────────────────────────────────┘

  WHAT EACH TIER FILLS:
  ┌────────────────────────────────────────────────────────────┐
  │ Tier 1 (always, zero deps):                                │
  │   Distracted?, Speed?, Animal?, Hitrun?, Drowsy?,          │
  │   Mainline?, K_People, A_People, cross-validated Ped/Bike  │
  ├────────────────────────────────────────────────────────────┤
  │ Tier 2 (needs osmnx+scipy):                                │
  │   Functional Class, RTE Name, Ownership, SYSTEM,           │
  │   Facility Type, Roadway Description, Intersection Type,   │
  │   Node, Node Offset, Intersection Name (cross-street OSM)  │
  └────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════
 SHARED PIPELINE MODULES (4 files — do NOT rewrite)
═══════════════════════════════════════════════════════════════

File 1: `geo_resolver.py` (1,315 lines)
  Resolves 9 derived columns: Physical Juris Name, Juris Code, FIPS,
  Place FIPS, DOT District, Planning District, MPO Name, Ownership, Area Type.
  Key: GeoResolver(state_fips, state_abbreviation, geo_dir, hierarchy_path)
  resolver.resolve_all(rows)
  Ownership 4-tier: SYSTEM → FC+juris → juris-only → route name pattern.

File 2: `state_normalize_template.py` (1,251 lines)
  MASTER COPY with embedded ValidationEngine (10 checks).
  Each state's normalize.py is COPIED from this template.
  Edit 4-5 marked sections, get ~900 lines of shared logic free. NEVER rewrite.
  ValidationEngine: whitespace, duplicates, GPS bounds, KABCO cross-validation,
  field inference, route-median GPS, nearest-neighbor snap.

File 3: `crash_enricher.py` (~600 lines)
  Universal enrichment. SMART-SKIP: only fills EMPTY columns.
  Tier 1 (zero deps): Contributing Circumstance→flags, Private Property→Mainline,
    Severity→K/A People, GPS clustering→Intersection, cross-validate Ped/Bike.
  Tier 2 (osmnx+scipy): GPS→nearest road→FC, RTE Name, Ownership, SYSTEM,
    Facility Type, Roadway Description, Intersection Type, Node, Node Offset,
    Intersection Name (cross-street names from OSM).
  OSM crosswalk: motorway→1-Interstate→State, trunk→2-Principal Arterial→State,
    primary→3-Principal Arterial→State, secondary→4-Minor Arterial→State,
    tertiary→5-Major Collector→County, unclassified→6-Minor Collector→County,
    residential→7-Local→City/Town.

File 4: `osm_road_enricher.py`
  Auto-called by Phase 8. Downloads state road network → cache/{abbreviation}_roads.parquet.
  First run 2-60 min, then instant. Dependencies OPTIONAL: pip install osmnx scipy.

═══════════════════════════════════════════════════════════════
 THE 8-PHASE PIPELINE (execution order)
═══════════════════════════════════════════════════════════════

  [1/8] Column Mapping        — state-specific renames
  [2/8] Value Transforms      — state-specific severity, datetime, Y/N
  [3/8] FIPS Resolution       — hardcoded counties + geo_resolver
  [4/8] Crash ID Generation   — OBJECTID = {abbreviation}-{7-digit seq}
                                 Document Nbr = {abbreviation}-{YYYYMMDD}-{HHMM}-{NNNNNNN}
  [5/8] EPDO Scoring           — weighted severity score
  [6/8] Jurisdiction Ranking   — 24 columns (4 scopes × 6 metrics)
  [7/8] Validation & Report    — quality checks + fill strategy recommendations
  [8/8] Enrichment             — crash_enricher Tier 1 + Tier 2 (OSM)
        → then prefix_extra_columns() to add state prefix to non-standard columns

  Output column order:
    Golden 69 → Enrichment 4 → Ranking 24 → State-Prefixed Extras

Geography JSON (states/geography/):
  us_counties (3,222), us_places (32,333), us_mpos (410),
  us_county_subdivisions (36,421), us_states (52).
hierarchy.json: per-state "glue" mapping counties → DOT regions,
  planning districts, MPOs.

═══════════════════════════════════════════════════════════════
 OBJECTID STRATEGY (Cross-State Diagnostic Tracking)
═══════════════════════════════════════════════════════════════

OBJECTID format: {state_abbreviation}-{7-digit sequential number}
  All lowercase. Examples:
    Delaware:  de-0000001, de-0000002, ... de-0566762
    Virginia:  va-0000001, va-0000002, ... va-1234567
    Colorado:  co-0000001, co-0000002, ... co-0987654
    Maryland:  md-0000001, md-0000002, ...
    Florida:   fl-0000001, ...
    New York:  ny-0000001, ...

WHY: When datasets from multiple states are combined in CrashLens,
the state prefix makes every record instantly identifiable by origin.
Engineers can grep/filter by prefix (e.g., "show me all de-*").

RULES:
  - OBJECTID ALWAYS starts with 2-letter LOWERCASE state abbreviation + hyphen
  - Sequential number is 7-digit zero-padded (supports up to 9,999,999 per state)
  - Generated AFTER sorting by Crash Date + Crash Military Time (chronological)
  - If source already has OBJECTID in wrong format → REPLACE it
  - Document Nbr uses same lowercase prefix: {abbreviation}-{YYYYMMDD}-{HHMM}-{NNNNNNN}

Python implementation:
```python
STATE_ABBREVIATION = "de"  # lowercase — NEVER uppercase

def generate_object_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Generate state-prefixed OBJECTID: {abbreviation}-{7-digit seq}."""
    df["OBJECTID"] = [f"{STATE_ABBREVIATION}-{i+1:07d}" for i in range(len(df))]
    return df

def generate_crash_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Document Nbr: {abbreviation}-{YYYYMMDD}-{HHMM}-{NNNNNNN}."""
    needs_id = df["Document Nbr"].fillna("").str.strip() == ""
    def _make_id(idx, row):
        date_str = str(row["Crash Date"])
        parts = date_str.split("/")
        if len(parts) == 3:
            date_clean = f"{parts[2]}{int(parts[0]):02d}{int(parts[1]):02d}"
        else:
            date_clean = re.sub(r"[^0-9]", "", date_str)[:8].ljust(8, "0")
        time_str = str(row.get("Crash Military Time", "0000") or "0000").strip().ljust(4, "0")
        return f"{STATE_ABBREVIATION}-{date_clean}-{time_str}-{idx + 1:07d}"
    ids = [_make_id(i, row) if needs_id.iloc[i] else df["Document Nbr"].iloc[i]
           for i, (_, row) in enumerate(df.iterrows())]
    df["Document Nbr"] = ids
    return df
```

HTML implementation (inside normalization loop):
```javascript
// STATE_ABBREVIATION is lowercase: "de", "va", "co"
const stateAbbreviation = (APP.stateConfig?.abbreviation || APP.stateConfig?.abbr || 'xx').toLowerCase();
normRow['OBJECTID'] = `${stateAbbreviation}-${String(i + 1).padStart(7, '0')}`;
// Document Nbr: only if not already set
if (!normRow['Document Nbr'] || normRow['Document Nbr'] === '') {
  normRow['Document Nbr'] = `${stateAbbreviation}-${dateClean}-${timeVal}-${String(i + 1).padStart(7, '0')}`;
}
```

═══════════════════════════════════════════════════════════════
 INTERSECTION NAME COLUMN (OSM-Derived, Tier 2)
═══════════════════════════════════════════════════════════════

Engineers need human-readable intersection names, not abstract node IDs.
New enrichment column: "Intersection Name"

WHAT IT CONTAINS:
  - Crashes AT intersection:    "Main St & 5th Ave"
  - Crashes NEAR intersection:  "Main St & 5th Ave (nearby)"
  - Mid-block crashes:          "Main St" (single road, no intersection)
  - No OSM data:                "" (empty — never fabricate)

HOW IT IS DERIVED (Tier 2 enrichment via crash_enricher.py):
  1. GPS point → find nearest OSM intersection node (already done for Node column)
  2. At that node, collect all OSM way names that share the node
  3. If 2+ distinct road names → intersection: "{Name1} & {Name2}"
  4. If only 1 road name → mid-block: "{Name1}"
  5. If OSM ways have no name tag → use ref tag (e.g., "SR 1" or "US 13")
  6. Sort road names alphabetically for consistency

COLUMN POSITION: Part of ENRICHMENT_COLUMNS, after EPDO_Score.

ENRICHMENT_COLUMNS (v2.6):
  ['FIPS', 'Place FIPS', 'EPDO_Score', 'Intersection Name']

Python crash_enricher.py addition:
```python
def _derive_intersection_name(self, node_id, way_names_at_node):
    """Build human-readable intersection name from OSM way names at a node."""
    names = sorted(set(n for n in way_names_at_node if n))
    if len(names) >= 2:
        return f"{names[0]} & {names[1]}"
    elif len(names) == 1:
        return names[0]
    return ""
```

HTML: Show "Intersection Name" column in preview table and download CSV.
HTML displays "Intersection Name requires Python pipeline (Tier 2)" banner for empty values.

═══════════════════════════════════════════════════════════════
 STATE-PREFIXED EXTRA COLUMNS
═══════════════════════════════════════════════════════════════

PROBLEM: Each state has unique columns not in the 69-column BDOT standard.
Previously these were kept with original names → confusion when datasets merge.

SOLUTION: Prefix ALL non-standard extra columns with "{abbreviation}_" (lowercase).

FORMAT: {state_abbreviation}_{Original_Column_Name_With_Spaces_As_Underscores}
  Examples for Delaware (abbreviation = "de"):
    "SCHOOL BUS INVOLVED DESCRIPTION"  → "de_School_Bus_Involved_Description"
    "MOTORCYCLE HELMET USED"           → "de_Motorcycle_Helmet_Used"
    "DAY OF WEEK DESCRIPTION"          → "de_Day_Of_Week_Description"
    "PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION" → "de_Primary_Contributing_Circumstance_Description"

  Examples for Colorado (abbreviation = "co"):
    "System Code"                      → "co_System_Code"
    "Route Type Code"                  → "co_Route_Type_Code"

  Examples for Virginia (abbreviation = "va"):
    "VSP District"                     → "va_VSP_District"

RULES:
  - Only columns NOT in GOLDEN_COLUMNS, NOT in ENRICHMENT_COLUMNS, NOT ranking columns
  - Convert spaces to underscores in column name suffix
  - Convert suffix to Title Case for readability
  - Prefix is ALWAYS lowercase state abbreviation + underscore
  - These columns appear AFTER the 69 golden + enrichment + 24 ranking columns
  - Original source column name preserved in mapping document for traceability
  - NEVER prefix columns that map to a BDOT standard column (those get renamed normally)

Python implementation:
```python
def prefix_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Prefix non-standard columns with lowercase state abbreviation."""
    standard_set = set(GOLDEN_COLUMNS + ENRICHMENT_COLUMNS)
    for s in RANKING_SCOPES:
        for m in RANKING_METRICS:
            standard_set.add(f"{s}_Rank_{m}")
    rename_map = {}
    for col in df.columns:
        if col not in standard_set:
            clean = '_'.join(w.capitalize() for w in col.strip().split())
            rename_map[col] = f"{STATE_ABBREVIATION}_{clean}"
    if rename_map:
        df = df.rename(columns=rename_map)
        print(f"  Prefixed {len(rename_map)} extra columns with '{STATE_ABBREVIATION}_'")
    return df
```

HTML implementation (in downloadNormalizedCSV):
```javascript
const stateAbbreviation = (APP.stateConfig?.abbreviation || APP.stateConfig?.abbr || 'xx').toLowerCase();
const standardCols = new Set([...GOLDEN_COLUMNS, ...ENRICHMENT_COLUMNS]);
RANKING_SCOPES.forEach(s => RANKING_METRICS.forEach(m => standardCols.add(`${s}_Rank_${m}`)));
const extraCols = APP.sourceHeaders
  .filter(h => !standardCols.has(h) && !Object.values(COLUMN_RENAMES).includes(h) && h !== 'the_geom')
  .map(h => ({
    original: h,
    prefixed: `${stateAbbreviation}_${h.trim().split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join('_')}`
  }));
```

═══════════════════════════════════════════════════════════════
 MISSING COLUMN FILL STRATEGY & RECOMMENDATIONS
═══════════════════════════════════════════════════════════════

When a state dataset is missing BDOT standard columns, the pipeline MUST proactively
tell the user HOW to fill each missing column. This is a consultation/advisory feature.

MANDATORY: After normalization, for every GOLDEN column that is EMPTY or has >80% blank
values, display a fill strategy recommendation. Include in:
  - Deliverable 4 (Gap Analysis)
  - HTML tool (banner in Step 2 after normalization)
  - Python CLI output (printed after Phase 7)
  - Validation report JSON (new "fill_strategies" key)

FILL STRATEGY LOOKUP TABLE (Column → how to fill if missing):

  Functional Class:
    → FILL: OSM road classification via GPS (Tier 2 auto-fills this)
    → ALT: Join with HPMS shapefile using route name + GPS
    → ALT: State DOT LRS data if available

  RTE Name:
    → FILL: OSM nearest road name via GPS (Tier 2 auto-fills this)
    → ALT: Reverse geocode using GPS → street name (Nominatim, Census TIGER)

  Ownership:
    → FILL: Derived from Functional Class + jurisdiction (geo_resolver handles this)
    → ALT: State DOT road inventory/ownership shapefile

  Facility Type:
    → FILL: OSM road type + lane count (Tier 2)
    → ALT: Infer from Functional Class: Interstate→Divided, Local→Undivided

  Area Type:
    → FILL: Census Urban/Rural classification by FIPS code (geo_resolver handles this)
    → ALT: Population density from Census block group containing GPS point

  SYSTEM:
    → FILL: Derived from Functional Class (Interstate→Interstate, Arterial→Primary, etc.)
    → ALT: State DOT system classification file

  Roadway Description:
    → FILL: OSM lane tags (Tier 2)
    → ALT: Default to "2-lane undivided" for local, "4-lane divided" for arterials

  Intersection Type:
    → FILL: OSM node degree analysis — GPS near node with 3+ ways = intersection (Tier 2)
    → ALT: Infer from crash location type if source has "intersection" field

  Traffic Control Type / Traffic Control Status:
    → FILL: Cannot reliably derive from GPS alone
    → RECOMMEND: Join with signal inventory from state/city DOT
    → ALT: OSM traffic_signals tag at nearest node (partial coverage only)

  Relation To Roadway:
    → FILL: Cannot derive from GPS alone
    → RECOMMEND: Check if source has "location type" or "at intersection" field
    → ALT: If crash GPS within 150ft of intersection node → "9. Within Intersection"

  Roadway Alignment:
    → FILL: Compute road segment curvature from OSM geometry near crash GPS
    → ALT: Default to "1. Straight - Level" (most common, ~65% of crashes)

  Roadway Surface Type:
    → FILL: OSM surface tag (paved/unpaved/concrete)
    → ALT: Default to "2. Blacktop, Asphalt, Bituminous" (most common)

  Roadway Defect:
    → FILL: Cannot derive — requires officer observation
    → RECOMMEND: Leave empty, note as "field observation required"

  First Harmful Event / First Harmful Event Loc:
    → FILL: Cannot derive — requires crash narrative parsing
    → RECOMMEND: If source has "manner of collision" or "object struck", map those

  Max Speed Diff:
    → FILL: Cannot derive from crash data alone
    → RECOMMEND: Join with posted speed limit from state DOT speed limit inventory
    → ALT: OSM maxspeed tag on nearest road segment

  RoadDeparture Type:
    → FILL: Infer from collision type (Fixed Object-Off Road → departure)
    → ALT: Parse "First Harmful Event" for departure indicators

  Intersection Analysis:
    → FILL: If Intersection Type is set and crash within 150ft of node → "Yes"
    → ALT: Cross-reference with Relation To Roadway field

  VSP:
    → FILL: Virginia-specific — Virginia State Police district (VA only)
    → RECOMMEND: For non-VA states, leave empty or map to equivalent state police district

  Node / Node Offset (ft):
    → FILL: OSM nearest intersection node ID and distance in feet (Tier 2)
    → ALT: State DOT node inventory if available

  RNS MP (Route Milepost):
    → FILL: Cannot derive from GPS without state LRS data
    → RECOMMEND: Join with state DOT LRS shapefile using route name + GPS proximity

  Persons Injured / Pedestrians Killed / Pedestrians Injured:
    → FILL: Cannot derive — requires person-level records
    → RECOMMEND: Join with person/occupant table if state provides it separately
    → ALT: Infer minimum from severity: K crash → at least 1 killed, A → at least 1 injured

  Vehicle Count:
    → FILL: Cannot derive — requires vehicle-level records
    → RECOMMEND: Join with vehicle table if state provides it separately
    → ALT: Infer from collision type: Angle/Rear End → minimum 2, Single Vehicle → 1

  Guardrail Related? / Lgtruck? / Senior? / Young?:
    → FILL: Guardrail from First Harmful Event (if "guardrail" mentioned)
    → FILL: Lgtruck from vehicle table (if vehicle type available)
    → FILL: Senior/Young from person table (age: Senior ≥65, Young ≤20)
    → ALT: Leave as "No" default (enrichment Tier 1 already does this)

  Unrestrained?:
    → FILL: Invert seatbelt field if available (Seatbelt=Y → Unrestrained=No)
    → RECOMMEND: Check person table for restraint use

Legend:  ✅ = auto-filled by pipeline   ⚠️ = needs external data   ❌ = field observation only

FILL_STRATEGIES constant (include in BOTH HTML and Python):
```javascript
const FILL_STRATEGIES = {
  'Functional Class': '✅ Auto-fill via OSM (Tier 2 Python pipeline) — or join with HPMS shapefile',
  'RTE Name': '✅ Auto-fill via OSM nearest road (Tier 2 Python pipeline) — or reverse geocode GPS',
  'Ownership': '✅ Auto-derived from Functional Class + jurisdiction (geo_resolver)',
  'Facility Type': '✅ Auto-fill via OSM road tags (Tier 2) — or infer from Functional Class',
  'Area Type': '✅ Auto-derived from Census Urban/Rural by FIPS (geo_resolver)',
  'SYSTEM': '✅ Auto-derived from Functional Class mapping',
  'Roadway Description': '✅ Auto-fill via OSM lane tags (Tier 2) — or default by FC',
  'Intersection Type': '✅ Auto-fill via OSM node analysis (Tier 2)',
  'Intersection Name': '✅ Auto-fill via OSM cross-street names at nearest node (Tier 2)',
  'Node': '✅ Auto-fill via nearest OSM intersection node (Tier 2)',
  'Node Offset (ft)': '✅ Auto-fill — distance to nearest OSM node in feet (Tier 2)',
  'Traffic Control Type': '⚠️ Join with signal inventory from state/city DOT — OSM partial coverage',
  'Traffic Control Status': '⚠️ Join with signal inventory — cannot derive from GPS alone',
  'Relation To Roadway': '⚠️ Check source for location_type field — or infer from intersection proximity',
  'Roadway Alignment': '⚠️ Compute curvature from OSM geometry — or default "1. Straight - Level"',
  'Roadway Surface Type': '⚠️ OSM surface tag — or default "2. Blacktop, Asphalt, Bituminous"',
  'Roadway Defect': '❌ Requires officer observation — leave empty',
  'First Harmful Event': '⚠️ Map from "manner of collision" or "object struck" if available',
  'First Harmful Event Loc': '⚠️ Derive from First Harmful Event + roadway relation',
  'Max Speed Diff': '⚠️ Join with speed limit inventory — or OSM maxspeed tag',
  'RoadDeparture Type': '⚠️ Infer from collision type (Fixed Object-Off Road → departure)',
  'Intersection Analysis': '⚠️ Cross-reference Intersection Type + Node Offset proximity',
  'VSP': '❌ Virginia-specific — leave empty for non-VA states',
  'RNS MP': '⚠️ Join with state DOT LRS shapefile using route name + GPS',
  'Persons Injured': '⚠️ Join person/occupant table — or infer minimum from severity',
  'Pedestrians Killed': '⚠️ Join person table — or infer from Pedestrian? + Severity=K',
  'Pedestrians Injured': '⚠️ Join person table — or infer from Pedestrian? + Severity=A/B/C',
  'Vehicle Count': '⚠️ Join vehicle table — or infer from collision type (Angle→2+, Single→1)',
  'Guardrail Related?': '⚠️ Parse First Harmful Event for "guardrail" — or default "No"',
  'Lgtruck?': '⚠️ Check vehicle table for vehicle type — or default "No"',
  'Senior?': '⚠️ Check person table for age ≥65 — or default "No"',
  'Young?': '⚠️ Check person table for age ≤20 — or default "No"',
  'Unrestrained?': '⚠️ Invert seatbelt field if available — or check person table',
};
```

Python implementation (in validation report):
```python
def compute_fill_strategies(df, missing_cols):
    """Generate actionable fill strategies for missing/empty columns."""
    strategies = {}
    for col in missing_cols:
        fill_pct = (df[col].fillna('').str.strip() != '').sum() / len(df) * 100
        if fill_pct < 20:
            strategies[col] = {
                "filled_pct": round(fill_pct, 1),
                "strategy": FILL_STRATEGY_LOOKUP.get(col, "No automated fill available"),
                "tier": "Tier 2 (OSM)" if col in TIER2_COLUMNS else "Manual/External",
                "priority": "HIGH" if col in MANDATORY_COLUMNS else "MEDIUM",
            }
    return strategies
```

HTML implementation (banner after Step 2 normalization):
```javascript
const missingWithStrategy = GOLDEN_COLUMNS.filter(col => {
  const filled = APP.normalizedData.filter(r => r[col] && r[col].trim() !== '').length;
  return filled / APP.normalizedData.length < 0.2;
});
if (missingWithStrategy.length > 0) {
  log(logEl, `<strong>📋 Fill Strategy Recommendations for ${missingWithStrategy.length} empty columns:</strong>`, 'warning');
  missingWithStrategy.forEach(col => {
    const strategy = FILL_STRATEGIES[col] || 'No automated fill available — requires external data source';
    log(logEl, `  • <code>${col}</code>: ${strategy}`, 'info');
  });
}
```

Python CLI output (after Phase 7):
```python
def print_fill_strategies(df):
    """Print recommendations for filling empty BDOT columns."""
    print("\n  📋 Missing Column Fill Strategies:")
    for col in GOLDEN_COLUMNS:
        if col not in df.columns:
            continue
        filled_pct = (df[col].fillna('').str.strip() != '').sum() / len(df) * 100
        if filled_pct < 20 and col in FILL_STRATEGY_LOOKUP:
            strategy = FILL_STRATEGY_LOOKUP[col]
            print(f"     {col:<30} ({filled_pct:5.1f}% filled) → {strategy}")
```

═══════════════════════════════════════════════════════════════
 FIVE DELIVERABLES
═══════════════════════════════════════════════════════════════

───────────────────────────────────────────────────────────────
 Deliverable 1: Data Mapping Document (PDF-ready)
───────────────────────────────────────────────────────────────
Complete mapping: every target column → source column.
Structure: COLUMN MAPPING TABLE, VALUE MAPPING TABLES, UNMAPPED SOURCE COLUMNS.
MANDATORY mapped: Physical Juris Name, Functional Class, Ownership, Crash Severity, x, y.
Use "Frontend Expected Column" and "New State Column" as headers (never "Target"/"Source").
Include "State-Prefixed Extra Columns" section showing original → prefixed name mapping.
Include "Missing Column Fill Strategies" section with recommendations for each empty column.

───────────────────────────────────────────────────────────────
 Deliverable 2: Interactive HTML Mapping & Validation Tool
───────────────────────────────────────────────────────────────
6-step pipeline + embedded Tier 1 enrichment.
Steps: Load & Configure → Normalize → FIPS Resolution → Enrich & Rank → Review & Edit → Export

MANDATORY HTML FEATURES:

A) OBJECTID generation with lowercase state prefix:
```javascript
const stateAbbreviation = (APP.stateConfig?.abbreviation || 'xx').toLowerCase();
normRow['OBJECTID'] = `${stateAbbreviation}-${String(i + 1).padStart(7, '0')}`;
```

B) State-prefixed extra columns in download (lowercase prefix):
```javascript
const stateAbbreviation = (APP.stateConfig?.abbreviation || 'xx').toLowerCase();
const stdSet = new Set(allCols);
const extraCols = [];
APP.sourceHeaders.forEach(h => {
  if (!stdSet.has(h) && !Object.keys(COLUMN_RENAMES).includes(h) && h !== 'the_geom') {
    const cleanName = h.trim().split(/\s+/).map(w =>
      w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
    ).join('_');
    const prefixed = `${stateAbbreviation}_${cleanName}`;
    extraCols.push({ original: h, prefixed: prefixed });
    allCols.push(prefixed);
  }
});
```

C) Missing Column Fill Strategy banner (after normalization in Step 2)

D) Intersection Name column in ENRICHMENT_COLUMNS:
```javascript
const ENRICHMENT_COLUMNS = ['FIPS', 'Place FIPS', 'EPDO_Score', 'Intersection Name'];
```

E) Tier 1 Self-Enrichment in normalization loop (AFTER state transforms, BEFORE push):
```javascript
(function enrichTier1(norm, src) {
  const circ = (src['PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION'] ||
                src['CONTRIBUTING FACTOR'] || src['DRIVER_CONTRIB'] || '').toLowerCase();
  if (circ) {
    if (!norm['Distracted?'] || norm['Distracted?'] === '' || norm['Distracted?'] === 'No')
      norm['Distracted?'] = /inattent|distract|cell phone|texting|electronic|eating|grooming/.test(circ) ? 'Yes' : 'No';
    if (!norm['Drowsy?'] || norm['Drowsy?'] === '')
      norm['Drowsy?'] = /drowsy|asleep|fatigued/.test(circ) && !/distract.*fatigue|inattent.*fatigue/.test(circ) ? 'Yes' : 'No';
    if (!norm['Speed?'] || norm['Speed?'] === '' || norm['Speed?'] === 'No')
      norm['Speed?'] = /speed|exceeding|too fast|racing|aggressive/.test(circ) ? 'Yes' : 'No';
    if (!norm['Animal Related?'] || norm['Animal Related?'] === '' || norm['Animal Related?'] === 'No')
      norm['Animal Related?'] = /animal|deer|wildlife|elk|moose/.test(circ) ? 'Yes' : 'No';
    if (!norm['Hitrun?'] || norm['Hitrun?'] === '' || norm['Hitrun?'] === 'No')
      norm['Hitrun?'] = /hit.and.run|hit-and-run|hitrun|left scene|fled|fleeing/.test(circ) ? 'Yes' : 'No';
  }
  for (const flag of ['Distracted?','Drowsy?','Speed?','Animal Related?','Hitrun?','Guardrail Related?','Lgtruck?','Senior?','Young?'])
    if (!norm[flag] || norm[flag] === '') norm[flag] = 'No';
  if (!norm['Mainline?'] || norm['Mainline?'] === '') {
    const pp = (src['COLLISION ON PRIVATE PROPERTY'] || '').toUpperCase().trim();
    norm['Mainline?'] = (pp === 'N') ? 'Yes' : 'No';
  }
  if (norm['Crash Severity']==='K' && (!norm['K_People']||norm['K_People']===''||norm['K_People']==='0')) norm['K_People']='1';
  if (norm['Crash Severity']==='A' && (!norm['A_People']||norm['A_People']===''||norm['A_People']==='0')) norm['A_People']='1';
  const ct = (norm['Collision Type']||'').toLowerCase();
  if (ct.includes('ped') && norm['Pedestrian?']==='No') norm['Pedestrian?']='Yes';
  if ((ct.includes('bicycl')||ct.includes('bike')) && norm['Bike?']==='No') norm['Bike?']='Yes';
})(normRow, row);
APP.normalizedData.push(normRow);
```

HTML MUST also: apply state text→standard maps in applyStateTransforms(), use FIPS `.padStart(3,'0')`, build CSV with _csvEscape (no PapaParse dep), include extra columns in download, log enrichment stats, show "Tier 2 requires Python pipeline" banner.

Download function uses chunked CSV builder to avoid browser freeze:
```javascript
function downloadNormalizedCSV() {
  try {
    const data = APP.enrichedData || APP.normalizedData;
    if (!data || !data.length) { toast('No data to export — run the pipeline first', 'error'); return; }
    const rankCols = RANKING_SCOPES.flatMap(s => RANKING_METRICS.map(m => `${s}_Rank_${m}`));
    const allCols = [...GOLDEN_COLUMNS, ...ENRICHMENT_COLUMNS];
    rankCols.forEach(c => allCols.push(c));

    // State-prefixed extra columns (lowercase)
    const stateAbbreviation = (APP.stateConfig?.abbreviation || 'xx').toLowerCase();
    const stdSet = new Set(allCols);
    const extraMap = {};
    APP.sourceHeaders.forEach(h => {
      if (!stdSet.has(h) && !Object.keys(COLUMN_RENAMES).includes(h) && h !== 'the_geom') {
        const cleanName = h.trim().split(/\s+/).map(w =>
          w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
        ).join('_');
        const prefixed = `${stateAbbreviation}_${cleanName}`;
        extraMap[h] = prefixed;
        allCols.push(prefixed);
      }
    });

    const validCols = data.length > 0
      ? allCols.filter(c => Object.keys(data[0]).includes(c) || Object.values(extraMap).includes(c))
      : allCols;

    toast('Building CSV (' + data.length.toLocaleString() + ' rows) — please wait...', 'info');

    const exportData = data.map(row => {
      const out = {};
      validCols.forEach(col => {
        if (Object.values(extraMap).includes(col)) {
          const origCol = Object.keys(extraMap).find(k => extraMap[k] === col);
          out[col] = row[origCol] || '';
        } else {
          out[col] = row[col] || '';
        }
      });
      return out;
    });

    _buildCSVChunked(exportData, validCols, 50000,
      function onChunk(done, total) {
        toast('Building CSV... ' + Math.round(done/total*100) + '%', 'info');
      },
      function onDone(csv) {
        if (!csv || csv.length < 10) { toast('CSV generation empty', 'error'); return; }
        var filename = (APP.selectedState || 'state') + '_normalized_ranked.csv';
        _downloadFile(csv, filename, 'text/csv;charset=utf-8;');
        toast('CSV downloaded: ' + filename, 'success');
      }
    );
  } catch(e) {
    console.error('CSV download error:', e);
    toast('Download failed: ' + e.message, 'error');
  }
}
```

Must include fallback utilities: _csvEscape, _buildCSVChunked, _downloadFile (with DOM-attached anchor + blob fallback).
NEVER Collapse UI/UX. Works offline on file:// protocol.

───────────────────────────────────────────────────────────────
 Deliverable 3: {abbreviation}_normalize.py — 8-Phase State Adapter
───────────────────────────────────────────────────────────────
Copy state_normalize_template.py, edit 4-5 sections, ADD Phase 8.

State config uses lowercase and full word "abbreviation":
```python
STATE_FIPS          = "10"
STATE_ABBREVIATION  = "de"        # lowercase — NEVER uppercase
STATE_NAME          = "Delaware"
STATE_DOT           = "DelDOT"
```

PATH RESOLUTION (supports repo layout AND flat folder):
```python
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = _SCRIPT_DIR.parent.parent
for p in [str(_REPO_ROOT), str(_SCRIPT_DIR)]:
    if p not in sys.path: sys.path.insert(0, p)
_CACHE_DIR = _REPO_ROOT / "cache"
if not _CACHE_DIR.exists(): _CACHE_DIR = _SCRIPT_DIR / "cache"
```

Module status at startup (checks BOTH paths, actionable messages):
```python
for name in ['geo_resolver.py','crash_enricher.py','osm_road_enricher.py']:
    found = any((Path(p)/name).exists() for p in sys.path if p)
    print(f"    {name:<25} {'OK' if found else 'MISSING — put in same folder'}")
```

Config must include:
```python
CIRCUMSTANCE_COL = "PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION"
PRIVATE_PROPERTY_COL = "COLLISION ON PRIVATE PROPERTY"
```

GOLDEN_COLUMNS (69 exact order), ENRICHMENT_COLUMNS (4: FIPS, Place FIPS, EPDO_Score, Intersection Name), RANKING_SCOPES, RANKING_METRICS, EPDO_PRESETS (all 4), DEFAULT_EPDO_PRESET = "fhwa2025".
ALL mapping keys lowercase. Vectorized operations ONLY (except Phase 4 IDs + Phase 6 rankings).
Work Zone Related = "1. Yes"/"2. No". School Zone = "3. No"/"1. Yes"/"2. Yes - With School Activity".
Night? from ALREADY-MAPPED Light Condition. FIPS zero-padded 3-digit.
Area Type from AREA_TYPE_MAP. is_already_normalized() detection.

Phase 8 integration:
```python
def _ensure_osm_cache():
    cache_file = _CACHE_DIR / f"{STATE_ABBREVIATION}_roads.parquet"
    if cache_file.exists(): return True
    try:
        import osm_road_enricher
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if hasattr(osm_road_enricher, 'download_state_roads'):
            osm_road_enricher.download_state_roads(STATE_NAME, STATE_ABBREVIATION, str(_CACHE_DIR))
        return cache_file.exists()
    except ImportError: return False

def run_enrichment(df, skip_enrichment=False):
    if skip_enrichment: return df
    try:
        from crash_enricher import CrashEnricher
        osm_ready = _ensure_osm_cache()
        enricher = CrashEnricher(STATE_FIPS, STATE_ABBREVIATION, STATE_NAME,
                                 cache_dir=str(_CACHE_DIR),
                                 circumstance_col=CIRCUMSTANCE_COL,
                                 private_property_col=PRIVATE_PROPERTY_COL)
        return enricher.enrich_all(df, skip_tier2=not osm_ready)
    except ImportError:
        print("  crash_enricher.py not found — put in same folder")
        return df
```

CLI: --input, --output, --report, --epdo, --skip-if-normalized, --skip-enrichment.

Pipeline execution order:
  [1/8] Load
  [2/8] Renames
  [3/8] Transforms
  [4/8] FIPS
  [5/8] IDs (OBJECTID = {abbreviation}-seq, Document Nbr = {abbreviation}-date-time-seq)
  [6/8] EPDO + Rank
  [7/8] Validation + Fill Strategies
  [8/8] Enrichment
  → then prefix_extra_columns()

Output column order:
  Golden 69 → Enrichment 4 (incl. Intersection Name) → Ranking 24 → State-Prefixed Extras

───────────────────────────────────────────────────────────────
 Deliverable 4: Gap Analysis & Fill Strategy
───────────────────────────────────────────────────────────────
Auto-filled by geo_resolver:
  Physical Juris Name, Juris Code, FIPS, Place FIPS, DOT District,
  Planning District, MPO Name, Ownership, Area Type.

Auto-filled by crash_enricher Tier 1:
  Distracted?, Speed?, Animal?, Hitrun?, Drowsy?, Mainline?, K/A People.

Auto-filled by crash_enricher Tier 2:
  Functional Class, RTE Name, Ownership, SYSTEM, Facility Type,
  Roadway Description, Intersection Type, Node, Node Offset, Intersection Name.

Still need external:
  Persons Injured, Ped Killed/Injured, Vehicle Count, Relation To Roadway,
  Roadway Alignment, Traffic Control, First Harmful Event, VSP, RNS MP.

For EVERY column in "Still need external", provide:
  1. The specific fill strategy from FILL_STRATEGY_LOOKUP
  2. The data source needed (state DOT shapefile, person table, vehicle table, Census, OSM)
  3. The join key (GPS coordinates, route name, crash ID, FIPS code)
  4. Priority level: HIGH (mandatory for CrashLens) / MEDIUM (improves analysis) / LOW (nice to have)
  5. Example command or workflow to execute the fill

───────────────────────────────────────────────────────────────
 Deliverable 5: JSON Validation Report
───────────────────────────────────────────────────────────────
Keys: state, state_fips, state_abbreviation, processed_at, total_rows, total_columns,
quality_score, fips_coverage, severity_distribution, epdo_config, mapping_completeness,
mandatory_columns, ranking_scopes, ranking_metrics, conflicts,
warnings (MANDATORY non-empty), unmapped_values.

Additional keys:
  - objectid_format: "{abbreviation}-{7-digit}" (confirms pattern used, lowercase)
  - intersection_name_coverage: {filled: N, pct: X.X} (Tier 2 only)
  - state_prefixed_extras: [{original: "...", prefixed: "xx_..."}, ...] (lowercase prefix)
  - fill_strategies: {col: {filled_pct, strategy, tier, priority}, ...} (columns <20% filled)

═══════════════════════════════════════════════════════════════
 CRASHLENS GOLDEN STANDARD — 69 COLUMNS
═══════════════════════════════════════════════════════════════

OBJECTID, Document Nbr, Crash Year, Crash Date, Crash Military Time,
Crash Severity, K_People, A_People, B_People, C_People,
Persons Injured, Pedestrians Killed, Pedestrians Injured, Vehicle Count,
Collision Type, Weather Condition, Light Condition, Roadway Surface Condition,
Relation To Roadway, Roadway Alignment, Roadway Surface Type, Roadway Defect,
Roadway Description, Intersection Type, Traffic Control Type, Traffic Control Status,
Work Zone Related, Work Zone Location, Work Zone Type, School Zone,
First Harmful Event, First Harmful Event Loc,
Alcohol?, Animal Related?, Unrestrained?, Bike?, Distracted?, Drowsy?,
Drug Related?, Guardrail Related?, Hitrun?, Lgtruck?, Motorcycle?, Pedestrian?,
Speed?, Max Speed Diff, RoadDeparture Type, Intersection Analysis,
Senior?, Young?, Mainline?, Night?,
DOT District, Juris Code, Physical Juris Name, Functional Class,
Facility Type, Area Type, SYSTEM, VSP, Ownership,
Planning District, MPO Name, RTE Name, RNS MP, Node, Node Offset (ft),
x, y

+ Enrichment: FIPS, Place FIPS, EPDO_Score, Intersection Name
+ Ranking: 24 columns = 4 scopes × 6 metrics
+ State-Prefixed Extras: {abbreviation}_{Original_Column_Name} (lowercase prefix)

═══════════════════════════════════════════════════════════════
 EPDO WEIGHT SYSTEM
═══════════════════════════════════════════════════════════════

hsm2010:  K=462, A=62, B=12, C=5, O=1
vdot2024: K=1032, A=53, B=16, C=10, O=1
fhwa2022: K=975, A=48, B=13, C=8, O=1
fhwa2025 (default): K=883, A=94, B=21, C=11, O=1

State overrides:
  VA → vdot2024
  CO/DE/MD → fhwa2025
  FL → K=985, A=50, B=15, C=9, O=1
  NY → K=1050, A=55, B=15, C=10, O=1
  CA → K=1100, A=58, B=17, C=11, O=1
  TX → K=920, A=55, B=14, C=9, O=1

═══════════════════════════════════════════════════════════════
 STATE DETECTION SIGNATURES
═══════════════════════════════════════════════════════════════

Virginia:  Document Nbr, Crash Severity, RTE Name, SYSTEM
Colorado:  CUID, System Code, Injury 00, Injury 04
Maryland:  report_number, acrs_report_type, road_name
Delaware:  CRASH DATETIME, CRASH CLASSIFICATION DESCRIPTION, LATITUDE, LONGITUDE

═══════════════════════════════════════════════════════════════
 FRONTEND DATA FLOW
═══════════════════════════════════════════════════════════════

County Roads Only → Ownership == "2. County Hwy Agency"
City Roads Only → Ownership == "3. City or Town Hwy Agency"
No Interstate → Functional Class != "1-Interstate (A,1)"
County dropdown from Physical Juris Name. Severity must be K/A/B/C/O.
Grants tab powered by 24 ranking columns. Rank 1 = most dangerous.

═══════════════════════════════════════════════════════════════
 VALUE MAPPING REFERENCE TABLES
═══════════════════════════════════════════════════════════════

Collision Type: 1→1. Rear End, 2→2. Angle, 3→3. Head On, 4→4. Sideswipe-Same, 5→5. Sideswipe-Opp, 6→6. Fixed Object in Road, 7→7. Train, 8→8. Non-Collision, 9→9. Fixed Object-Off Road, 10→10. Deer, 11→11. Other Animal, 12→12. Ped, 13→13. Bicyclist, 14→14. Motorcyclist, 15→15. Backed Into, 16→16. Other, 99→Not Provided
Weather: 1→1. No Adverse Condition (Clear/Cloudy), 3→3. Fog, 4→4. Mist, 5→5. Rain, 6→6. Snow, 7→7. Sleet/Hail, 8→8. Smoke/Dust, 9→9. Other, 10→10. Blowing Sand Soil Dirt or Snow, 11→11. Severe Crosswinds, 99→Not Applicable
Light: 1→1. Dawn, 2→2. Daylight, 3→3. Dusk, 4→4. Darkness-Road Lighted, 5→5. Darkness-Road Not Lighted, 6→6. Darkness-Unknown Road Lighting, 7→7. Unknown, 99→Not Applicable
Severity: 1→K, 2→A, 3→B, 4→C, 5→O. Text: fatal→K, serious injury→A, minor injury→B, possible injury→C, PDO→O
Surface: 1→1. Dry, 2→2. Wet, 3→3. Snow/Ice, 4→4. Slush, 5→5. Sand/Mud/Dirt/Oil/Gravel, 6→6. Water, 9→9. Other, 99→Not Applicable
Work Zone Related: Y→1. Yes, N→2. No (NOT "Yes"/"No")
School Zone: No→3. No, Yes directly→2. Yes-With School Activity, Yes indirectly→1. Yes

═══════════════════════════════════════════════════════════════
 FILE ARCHITECTURE
═══════════════════════════════════════════════════════════════

```
repo root/
├── geo_resolver.py
├── crash_enricher.py
├── osm_road_enricher.py
├── state_normalize_template.py
├── cache/{abbreviation}_roads.parquet
├── cache/{abbreviation}_intersections.parquet
└── states/
    ├── geography/ (us_counties.json, us_places.json, us_mpos.json, ...)
    └── {state}/
        ├── {abbreviation}_normalize.py
        ├── hierarchy.json
        └── {abbreviation}_normalization_rank_validation.html

--- OR flat folder (local use): ---
my_folder/ ← ALL files here together
├── de_normalize.py
├── crash_enricher.py
├── osm_road_enricher.py
├── hierarchy.json
├── de_normalization_rank_validation.html
└── data.csv
```

═══════════════════════════════════════════════════════════════
 CLI COMMANDS FOR USERS
═══════════════════════════════════════════════════════════════

Command 1 (column names):
  python -c "import pandas as pd,os; f_in=r'YOUR_FILE.csv'; df=pd.read_csv(f_in, dtype=str, nrows=1); out=os.path.join(os.path.dirname(os.path.abspath(f_in)),'column_names.txt'); f=open(out,'w',encoding='utf-8'); f.write(f'Total columns: {len(df.columns)}\n\n'); [f.write(f'{i+1}. {col}\n') for i,col in enumerate(df.columns)]; f.close(); print(f'Saved {len(df.columns)} columns to {out}')"

Command 2 (column values):
  Use universal exclusion (130+ keywords, regex patterns, >500 unique auto-skip).

═══════════════════════════════════════════════════════════════
 COLUMN NAMING CONVENTION
═══════════════════════════════════════════════════════════════

Use "Frontend Expected Column" and "New State Column" — never "Target"/"Source".
No Duplicate Sections: Extra columns shown ONCE in main mapping table with "EXTRA (kept)"
  and now prefixed as "{abbreviation}_ColumnName" (lowercase prefix).

═══════════════════════════════════════════════════════════════
 RULES (COMPLETE LIST — v2.6)
═══════════════════════════════════════════════════════════════

Schema & Frontend:
  NEVER modify the frontend schema.
  NEVER collapse UI/UX in HTML.
  Mandatory columns: Physical Juris Name, Functional Class, Ownership, Crash Severity, x, y.
  Use exact target values character-for-character.

Naming Convention (v2.6):
  State abbreviation ALWAYS lowercase in output: de, va, co, md (never DE, VA, CO, MD).
  Use "abbreviation" in variable names: STATE_ABBREVIATION, stateAbbreviation (never abbr/ABBR).
  OBJECTID format: {abbreviation}-{7-digit seq} → de-0000001, va-0000001.
  Document Nbr format: {abbreviation}-{YYYYMMDD}-{HHMM}-{NNNNNNN}.
  Extra column prefix: {abbreviation}_{Column_Name} → de_School_Bus_Involved.

Data Quality:
  Normalization idempotent — is_already_normalized().
  Log unmapped values. Preserve extra columns WITH state prefix (lowercase).
  FIPS 3-digit zero-padded ("001" not "1").
  ALL mapping keys lowercase.
  Vectorized pandas ONLY for value mapping.

Ranking & Scoring:
  Ranking 24 columns mandatory. EPDO mandatory (FHWA 2025 default).
  ALL 4 EPDO presets included.
  Area Type assigned. Night? from MAPPED Light Condition.
  Work Zone = "1. Yes"/"2. No". School Zone = "3. No"/"1. Yes"/"2. Yes-With School Activity".

Shared Modules:
  Use shared modules — never rewrite from scratch.
  geo_resolver.py read-only. hierarchy.json optional.
  crash_enricher.py read-only — never modify per state.
  crash_enricher smart-skip — only fills empty columns.
  OSM auto-downloads on first run.
  Contributing circumstance column name varies — pass via parameter.

Pipeline:
  Phase 8 after ranking. --skip-enrichment flag available.
  cache/ auto-created, add to .gitignore.
  FILE CO-LOCATION: ALL .py files MUST be in same folder as CSV. ALWAYS remind users.

HTML Tool:
  HTML MUST include Tier 1 enrichment.
  HTML ↔ Python output parity.
  HTML applies state text→standard maps in applyStateTransforms().
  HTML works offline (PapaParse fallback). _downloadFile() helper.
  HTML download uses chunked CSV builder (_buildCSVChunked) — NOT raw Papa.unparse.
  HTML download MUST use _csvEscape, _buildCSVChunked, _downloadFile.

Enrichment (v2.5+):
  Intersection Name: derived from OSM cross-street names at nearest node — enrichment column.
  State-prefixed extras: non-standard columns renamed to {abbreviation}_{Column_Name}.
  Fill strategies: ALWAYS show recommendations for empty BDOT columns in HTML, CLI, and report.

═══════════════════════════════════════════════════════════════
 KNOWLEDGE BASE FILES
═══════════════════════════════════════════════════════════════

  de_normalize.py                        (CRITICAL)
  state_normalize_template.py            (CRITICAL)
  geo_resolver.py                        (HIGH)
  crash_enricher.py                      (HIGH)
  osm_road_enricher.py                   (MEDIUM)
  hierarchy.json                         (HIGH)
  VDOT Frontend column names TXT         (CRITICAL)
  VDOT Frontend column attributes TXT    (CRITICAL)
  DE_normalization_rank_validation.html   (HIGH)
  us_counties.json                       (MEDIUM)
  us_mpos.json                           (MEDIUM)
  us_states.json                         (MEDIUM)
