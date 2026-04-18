"""
Data Authority Layer for Road Database
=======================================
Adds resolved columns using authority hierarchy (like crash_enricher.py's 4-tier system).

When multiple sources provide the same attribute, the highest-authority source wins.
When the best source is empty, falls through to next tier.

AUTHORITY HIERARCHY:
  Tier A — State DOT Inventory (OPTIONAL — highest when available)
           Source: {abbr}_state_dot.parquet.gz from generate_state_dot_data.py
           Columns prefixed sdot_ (e.g. sdot_Functional Class, sdot_Through_Lanes)
           If file missing → silently skipped, Tier B becomes highest.
  Tier B — HPMS (Federal, FHWA-validated road inventory)
  Tier C — Mapillary (Computer vision, photographed in field)
  Tier D — OSM (Community-contributed, variable quality)
  Tier E — Federal Point Data (BTS bridges/rail/transit, Urban Institute schools)

RESOLVED COLUMNS:
  resolved_speed_limit        StateDOT > HPMS > Mapillary > OSM
  resolved_speed_source       Which tier provided the value
  resolved_lanes              HPMS > OSM
  resolved_lanes_source       Which tier provided the value
  resolved_surface_type       HPMS > OSM
  resolved_has_signal         Mapillary > POI > HPMS
  resolved_has_lighting       Mapillary(count>0) > OSM(lit=yes)
  resolved_on_bridge          OSM(bridge tag) > Federal(500ft)
  resolved_school_zone        Mapillary(S1-1 sign) > POI(school 1500ft) > Federal(school 1500ft)

SANITY CHECKS:
  Speed: 5-85 mph
  Lanes: 1-12
  AADT: 0-500,000
  Bridge year: 1800-2026
  Enrollment: 1-10,000
  GPS: within continental US bounds
"""

import numpy as np
import pandas as pd


def resolve_speed_limit(df):
    """StateDOT > HPMS > Mapillary > OSM. Returns (value_array, source_array)."""
    n = len(df)
    values = np.full(n, 0, dtype=int)
    sources = np.full(n, "", dtype=object)

    # Tier 3: OSM (lowest priority — set first, gets overwritten)
    if "maxspeed" in df.columns:
        for i, v in enumerate(df["maxspeed"].values):
            s = str(v).replace("mph", "").strip()
            try:
                spd = int(s)
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "OSM"
            except (ValueError, TypeError):
                pass

    # Tier 2: Mapillary (overwrites OSM)
    if "map_speed_limit_value" in df.columns:
        for i, v in enumerate(df["map_speed_limit_value"].values):
            s = str(v).strip()
            try:
                spd = int(s)
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "Mapillary"
            except (ValueError, TypeError):
                pass

    # Tier 1: HPMS (high authority)
    if "hpms_speed_limit" in df.columns:
        for i, v in enumerate(df["hpms_speed_limit"].values):
            try:
                spd = int(v)
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    # Tier A: State DOT (HIGHEST authority — overwrites when available)
    # Check both column names: sdot_Speed_Limit_Est (current) and sdot_Max Speed Diff (legacy)
    sdot_speed_col = None
    for candidate in ["sdot_Speed_Limit_Est", "sdot_Max Speed Diff"]:
        if candidate in df.columns:
            sdot_speed_col = candidate
            break
    if sdot_speed_col is not None:
        for i, v in enumerate(df[sdot_speed_col].values):
            try:
                spd = int(float(v))
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "StateDOT"
            except (ValueError, TypeError):
                pass

    # Tier 5: Statutory default (LOWEST priority — fills only when ALL other sources are empty)
    # Delaware Title 21 §4169: residential/business district = 25 mph, non-residential = 50 mph
    # IMPORTANT: "Functional Class" and "Area Type" don't exist yet at this stage,
    # so we use raw source columns: "highway" for FC and "geo_mpo_name" for area type.
    if "highway" in df.columns:
        no_speed = values == 0
        hw = df["highway"].astype(str).str.strip().str.lower()

        # FC-7 equivalent: OSM highway=residential, unclassified, living_street, service
        is_local = hw.isin(["residential", "unclassified", "living_street", "service"])

        # Urban/Suburban = has an MPO assignment; Rural = no MPO
        if "geo_mpo_name" in df.columns:
            in_mpo = df["geo_mpo_name"].astype(str).str.strip() != ""
        else:
            in_mpo = pd.Series(False, index=df.index)

        stat_25 = no_speed & is_local & in_mpo
        stat_50 = no_speed & is_local & ~in_mpo

        values[stat_25.values] = 25
        sources[stat_25.values] = "Statutory"
        values[stat_50.values] = 50
        sources[stat_50.values] = "Statutory"

        filled_stat = stat_25.sum() + stat_50.sum()
        if filled_stat:
            print(f"      Statutory: {filled_stat:,} local road defaults (25mph urban, 50mph rural)")

        # FC-5 collectors: OSM highway=tertiary, tertiary_link
        is_collector = hw.isin(["tertiary", "tertiary_link"])
        coll_35 = no_speed & is_collector & in_mpo
        coll_50 = no_speed & is_collector & ~in_mpo
        values[coll_35.values] = 35
        sources[coll_35.values] = "Statutory"
        values[coll_50.values] = 50
        sources[coll_50.values] = "Statutory"
        filled_coll = coll_35.sum() + coll_50.sum()
        if filled_coll:
            print(f"      Statutory: {filled_coll:,} collector defaults (35mph urban, 50mph rural)")

    return values, sources


def resolve_lanes(df):
    """HPMS > OSM."""
    n = len(df)
    values = np.zeros(n, dtype=int)
    sources = np.full(n, "", dtype=object)

    if "lanes" in df.columns:
        for i, v in enumerate(df["lanes"].values):
            s = str(v).strip().split(";")[0]  # Take first if multi-value
            try:
                ln = int(s)
                if 1 <= ln <= 12:
                    values[i] = ln
                    sources[i] = "OSM"
            except (ValueError, TypeError):
                pass

    if "hpms_through_lanes" in df.columns:
        for i, v in enumerate(df["hpms_through_lanes"].values):
            try:
                ln = int(v)
                if 1 <= ln <= 12:
                    values[i] = ln
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    # Tier A: State DOT (HIGHEST — overwrites HPMS when available)
    if "sdot_Through_Lanes" in df.columns:
        for i, v in enumerate(df["sdot_Through_Lanes"].values):
            try:
                ln = int(float(v))
                if 1 <= ln <= 12:
                    values[i] = ln
                    sources[i] = "StateDOT"
            except (ValueError, TypeError):
                pass

    return values, sources


def resolve_surface(df):
    """HPMS > OSM. Standardizes to: Paved/Unpaved/Unknown."""
    n = len(df)
    values = np.full(n, "", dtype=object)
    sources = np.full(n, "", dtype=object)

    # OSM surface → standardized
    osm_paved = {"asphalt", "concrete", "paved", "concrete:plates", "concrete:lanes",
                 "paving_stones", "sett", "metal"}
    osm_unpaved = {"unpaved", "gravel", "dirt", "sand", "grass", "ground",
                   "mud", "compacted", "fine_gravel", "earth"}

    if "surface" in df.columns:
        for i, v in enumerate(df["surface"].values):
            s = str(v).strip().lower()
            if s in osm_paved:
                values[i] = "Paved"
                sources[i] = "OSM"
            elif s in osm_unpaved:
                values[i] = "Unpaved"
                sources[i] = "OSM"

    # HPMS surface_type: 1=Concrete, 2=Asphalt, 3=Brick, 4=Gravel, 5=Dirt
    if "hpms_surface_type" in df.columns:
        for i, v in enumerate(df["hpms_surface_type"].values):
            try:
                st = int(v)
                if st in (1, 2, 3):
                    values[i] = "Paved"
                    sources[i] = "HPMS"
                elif st in (4, 5):
                    values[i] = "Unpaved"
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    # Tier A: State DOT (HIGHEST — overwrites HPMS when available)
    if "sdot_Roadway Surface Type" in df.columns:
        paved_vals = {"1. Concrete", "2. Blacktop, Asphalt, Bituminous", "3. Brick or Block"}
        unpaved_vals = {"4. Slag, Gravel, Stone", "5. Dirt"}
        for i, v in enumerate(df["sdot_Roadway Surface Type"].values):
            s = str(v).strip()
            if s in paved_vals:
                values[i] = "Paved"
                sources[i] = "StateDOT"
            elif s in unpaved_vals:
                values[i] = "Unpaved"
                sources[i] = "StateDOT"

    return values, sources


def resolve_signals(df):
    """Mapillary > POI > HPMS. Returns Yes/No."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    # Tier 3: HPMS signal_type
    if "hpms_signal_type" in df.columns:
        for i, v in enumerate(df["hpms_signal_type"].values):
            try:
                if int(v) > 0:
                    values[i] = "Yes"
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    # Tier 2: POI signal
    if "Near_PoiSignal_100ft" in df.columns:
        mask = df["Near_PoiSignal_100ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "POI"

    # Tier 1: Mapillary (most current — photographed)
    if "map_signal_present" in df.columns:
        mask = df["map_signal_present"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


def resolve_lighting(df):
    """Mapillary(count>0) > OSM(lit=yes)."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "lit" in df.columns:
        for i, v in enumerate(df["lit"].values):
            if str(v).strip().lower() == "yes":
                values[i] = "Yes"
                sources[i] = "OSM"

    if "map_street_light_count" in df.columns:
        mask = df["map_street_light_count"].values > 0
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


def resolve_bridge(df):
    """HPMS(structure_type) > OSM(bridge tag on segment) > Federal(within 500ft)."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "Near_Bridge_500ft" in df.columns:
        mask = df["Near_Bridge_500ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Federal"

    # OSM bridge tag is directly ON the segment — higher than proximity
    if "bridge" in df.columns:
        for i, v in enumerate(df["bridge"].values):
            s = str(v).strip().lower()
            if s and s not in ("", "no", "nan"):
                values[i] = "Yes"
                sources[i] = "OSM"

    # HPMS structure_type: 1=bridge, 2=tunnel, 3=causeway (highest for bridge)
    if "hpms_structure_type" in df.columns:
        st = pd.to_numeric(df["hpms_structure_type"], errors="coerce").fillna(0).astype(int).values
        mask = st > 0  # any structure (bridge, tunnel, causeway)
        values[mask] = "Yes"
        sources[mask] = "HPMS"

    return values, sources


def resolve_school_zone(df):
    """Mapillary(S1-1 sign) > POI(school) > Federal(school)."""
    n = len(df)
    values = np.full(n, "No", dtype=object)
    sources = np.full(n, "", dtype=object)

    if "Near_School_1000ft" in df.columns:
        mask = df["Near_School_1000ft"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Federal"

    if "Near_PoiCollege_1500ft" in df.columns:
        # Don't count colleges as school zones
        pass

    if "map_school_zone" in df.columns:
        mask = df["map_school_zone"].values == "Yes"
        values[mask] = "Yes"
        sources[mask] = "Mapillary"

    return values, sources


# ═══════════════════════════════════════════════════════════════
#  SANITY CHECKS
# ═══════════════════════════════════════════════════════════════

def run_sanity_checks(df, state_abbr):
    """Run data quality checks. Returns dict of {check_name: (passed, total, pct)}."""
    checks = {}

    n = len(df)

    # GPS within continental US (rough bounds)
    lat_ok = ((df["mid_lat"] >= 24.0) & (df["mid_lat"] <= 72.0)).sum()
    lon_ok = ((df["mid_lon"] >= -180.0) & (df["mid_lon"] <= -65.0)).sum()
    checks["gps_lat_in_range"] = (lat_ok, n, lat_ok/n*100)
    checks["gps_lon_in_range"] = (lon_ok, n, lon_ok/n*100)

    # Speed sanity (where resolved)
    if "resolved_speed_limit" in df.columns:
        has_speed = df["resolved_speed_limit"] > 0
        if has_speed.sum() > 0:
            valid = ((df["resolved_speed_limit"] >= 5) & (df["resolved_speed_limit"] <= 85))
            checks["speed_5_to_85"] = (valid.sum(), has_speed.sum(), valid.sum()/max(has_speed.sum(),1)*100)

    # Lanes sanity
    if "resolved_lanes" in df.columns:
        has_lanes = df["resolved_lanes"] > 0
        if has_lanes.sum() > 0:
            valid = ((df["resolved_lanes"] >= 1) & (df["resolved_lanes"] <= 12))
            checks["lanes_1_to_12"] = (valid.sum(), has_lanes.sum(), valid.sum()/max(has_lanes.sum(),1)*100)

    # HPMS AADT sanity
    if "hpms_aadt" in df.columns:
        has_aadt = df["hpms_aadt"] > 0
        if has_aadt.sum() > 0:
            valid = ((df["hpms_aadt"] >= 1) & (df["hpms_aadt"] <= 500000))
            checks["aadt_1_to_500k"] = (valid.sum(), has_aadt.sum(), valid.sum()/max(has_aadt.sum(),1)*100)

    # Bridge year sanity
    if "nearest_bridge_year_built" in df.columns:
        has_year = df["nearest_bridge_year_built"] != ""
        if isinstance(df["nearest_bridge_year_built"].iloc[0], str):
            has_year = (df["nearest_bridge_year_built"].str.strip() != "") & (df["nearest_bridge_year_built"] != "0")
        if has_year.sum() > 0:
            try:
                years = pd.to_numeric(df.loc[has_year, "nearest_bridge_year_built"], errors="coerce")
                valid = ((years >= 1800) & (years <= 2026)).sum()
                checks["bridge_year_1800_2026"] = (valid, has_year.sum(), valid/max(has_year.sum(),1)*100)
            except:
                pass

    # School enrollment sanity
    if "nearest_school_enrollment" in df.columns:
        has_enr = df["nearest_school_enrollment"] != ""
        if has_enr.sum() > 0:
            try:
                enr = pd.to_numeric(df.loc[has_enr, "nearest_school_enrollment"], errors="coerce")
                valid = ((enr >= 1) & (enr <= 10000)).sum()
                checks["enrollment_1_to_10k"] = (valid, has_enr.sum(), valid/max(has_enr.sum(),1)*100)
            except:
                pass

    # County FIPS populated
    if "geo_county_fips" in df.columns:
        has_county = (df["geo_county_fips"].str.strip() != "").sum()
        checks["county_fips_populated"] = (has_county, n, has_county/n*100)

    # No duplicate rows
    dupes = 0
    dup_cols = ["osm_u_node", "osm_v_node"] if "osm_u_node" in df.columns else ["u_node", "v_node"]
    if all(c in df.columns for c in dup_cols):
        dupes = df.duplicated(subset=dup_cols).sum()
    checks["no_duplicate_segments"] = (n - dupes, n, (n-dupes)/n*100)

    # HPMS match within 100m
    if "hpms_match_dist_ft" in df.columns:
        matched = df["hpms_matched"] == "Yes"
        if matched.sum() > 0:
            within_100m = (df.loc[matched, "hpms_match_dist_ft"] <= 328).sum()  # 100m in ft
            checks["hpms_within_100m"] = (within_100m, matched.sum(), within_100m/max(matched.sum(),1)*100)

    return checks


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def apply_authority_layer(df):
    """Add all resolved columns to the road database DataFrame."""
    print("    Data authority resolution...")

    vals, srcs = resolve_speed_limit(df)
    df["resolved_speed_limit"] = vals
    df["resolved_speed_source"] = srcs
    filled = (vals > 0).sum()
    print(f"      Speed limit:  {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — DOT:{(srcs=='StateDOT').sum():,} HPMS:{(srcs=='HPMS').sum():,} Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_lanes(df)
    df["resolved_lanes"] = vals
    df["resolved_lanes_source"] = srcs
    filled = (vals > 0).sum()
    print(f"      Lanes:        {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — DOT:{(srcs=='StateDOT').sum():,} HPMS:{(srcs=='HPMS').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_surface(df)
    df["resolved_surface_type"] = vals
    df["resolved_surface_source"] = srcs
    filled = (vals != "").sum()
    print(f"      Surface:      {filled:>7,} resolved ({filled/len(df)*100:.1f}%)"
          f" — DOT:{(srcs=='StateDOT').sum():,} HPMS:{(srcs=='HPMS').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_signals(df)
    df["resolved_has_signal"] = vals
    df["resolved_signal_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      Signal:       {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} POI:{(srcs=='POI').sum():,} HPMS:{(srcs=='HPMS').sum():,}")

    vals, srcs = resolve_lighting(df)
    df["resolved_has_lighting"] = vals
    df["resolved_lighting_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      Lighting:     {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")

    vals, srcs = resolve_bridge(df)
    df["resolved_on_bridge"] = vals
    df["resolved_bridge_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      On bridge:    {yes:>7,} Yes"
          f" — OSM:{(srcs=='OSM').sum():,} Fed:{(srcs=='Federal').sum():,}")

    vals, srcs = resolve_school_zone(df)
    df["resolved_school_zone"] = vals
    df["resolved_school_source"] = srcs
    yes = (vals == "Yes").sum()
    print(f"      School zone:  {yes:>7,} Yes"
          f" — Map:{(srcs=='Mapillary').sum():,} Fed:{(srcs=='Federal').sum():,}")


def print_sanity_report(checks):
    """Print sanity check results."""
    print("\n    Sanity checks:")
    all_pass = True
    for name, (passed, total, pct) in sorted(checks.items()):
        icon = "✅" if pct >= 99.0 else ("⚠️" if pct >= 90.0 else "❌")
        if pct < 99.0:
            all_pass = False
        print(f"      {icon} {name:30s}  {passed:>8,}/{total:>8,} ({pct:.1f}%)")
    return all_pass


# ═══════════════════════════════════════════════════════════════
#  CONFIDENCE SCORING + CROSS-SOURCE VALIDATION
# ═══════════════════════════════════════════════════════════════

def compute_confidence_scores(df):
    """
    Add confidence scores for key safety attributes.
    
    Confidence = how many independent sources agree + data freshness.
    
    Scoring (0-100):
      50  = 1 source confirms
      75  = 2 sources confirm  
      90  = 3 sources confirm
      +5  = Mapillary data < 2 years old
      +3  = Mapillary data < 4 years old
      
    Columns added:
      conf_signal          0-100 confidence that segment has traffic signal
      conf_crosswalk       0-100 confidence that segment has crosswalk
      conf_stop_sign       0-100 confidence that segment has stop sign
      conf_school_zone     0-100 confidence this is a school zone
      conf_speed_limit     0-100 confidence in resolved speed limit value
      conf_bridge          0-100 confidence segment is on/near bridge
    
    Cross-validation columns:
      xval_signal_sources  Count of independent sources confirming signal
      xval_crosswalk_sources
      xval_stop_sign_sources
      xval_school_sources
    """
    print("    Confidence scoring + cross-validation...")
    n = len(df)

    # ── SIGNAL confidence ──
    sig_sources = np.zeros(n, dtype=int)
    if "hpms_signal_type" in df.columns:
        try:
            sig_sources += (pd.to_numeric(df["hpms_signal_type"], errors="coerce").fillna(0) > 0).astype(int)
        except: pass
    if "Near_PoiSignal_100ft" in df.columns:
        sig_sources += (df["Near_PoiSignal_100ft"] == "Yes").astype(int)
    if "map_signal_present" in df.columns:
        sig_sources += (df["map_signal_present"] == "Yes").astype(int)
    
    df["xval_signal_sources"] = sig_sources
    conf = np.where(sig_sources >= 3, 90, np.where(sig_sources == 2, 75, np.where(sig_sources == 1, 50, 0)))
    # Freshness bonus from Mapillary
    if "map_signal_present" in df.columns:
        conf = _add_freshness_bonus(df, conf, "map_signal_present", "Yes")
    df["conf_signal"] = conf
    multi = (sig_sources >= 2).sum()
    print(f"      Signal:     {multi:>6,} confirmed by 2+ sources, "
          f"max conf={conf.max()}")

    # ── CROSSWALK confidence ──
    xwalk_sources = np.zeros(n, dtype=int)
    if "Near_PoiCrossing_100ft" in df.columns:
        xwalk_sources += (df["Near_PoiCrossing_100ft"] == "Yes").astype(int)
    if "map_crosswalk_count" in df.columns:
        xwalk_sources += (df["map_crosswalk_count"] > 0).astype(int)
    if "map_stop_line_count" in df.columns:
        # Stop line near crosswalk = corroborating evidence
        xwalk_sources += ((df["map_stop_line_count"] > 0) & (xwalk_sources > 0)).astype(int)
    
    df["xval_crosswalk_sources"] = xwalk_sources
    conf = np.where(xwalk_sources >= 3, 90, np.where(xwalk_sources == 2, 75, np.where(xwalk_sources == 1, 50, 0)))
    df["conf_crosswalk"] = conf
    multi = (xwalk_sources >= 2).sum()
    print(f"      Crosswalk:  {multi:>6,} confirmed by 2+ sources")

    # ── STOP SIGN confidence ──
    stop_sources = np.zeros(n, dtype=int)
    if "Near_PoiStopSign_100ft" in df.columns:
        stop_sources += (df["Near_PoiStopSign_100ft"] == "Yes").astype(int)
    if "map_stop_sign" in df.columns:
        stop_sources += (df["map_stop_sign"] == "Yes").astype(int)
    if "map_stop_ahead" in df.columns:
        # Stop Ahead warning sign corroborates
        stop_sources += ((df["map_stop_ahead"] == "Yes") & (stop_sources > 0)).astype(int)
    if "hpms_num_stop_int" in df.columns:
        try:
            stop_sources += (pd.to_numeric(df["hpms_num_stop_int"], errors="coerce").fillna(0) > 0).astype(int)
        except: pass

    df["xval_stop_sign_sources"] = stop_sources
    conf = np.where(stop_sources >= 3, 90, np.where(stop_sources == 2, 75, np.where(stop_sources == 1, 50, 0)))
    df["conf_stop_sign"] = conf
    multi = (stop_sources >= 2).sum()
    print(f"      Stop sign:  {multi:>6,} confirmed by 2+ sources")

    # ── SCHOOL ZONE confidence ──
    school_sources = np.zeros(n, dtype=int)
    if "Near_School_1000ft" in df.columns:
        school_sources += (df["Near_School_1000ft"] == "Yes").astype(int)
    if "map_school_zone" in df.columns:
        school_sources += (df["map_school_zone"] == "Yes").astype(int)
    if "Near_PoiCollege_1500ft" in df.columns:
        # Don't count college as school zone — separate
        pass

    df["xval_school_sources"] = school_sources
    conf = np.where(school_sources >= 2, 90, np.where(school_sources == 1, 50, 0))
    df["conf_school_zone"] = conf
    multi = (school_sources >= 2).sum()
    print(f"      School zone:{multi:>6,} confirmed by 2+ sources")

    # ── SPEED LIMIT confidence ──
    speed_sources = np.zeros(n, dtype=int)
    sdot_speed_conf_col = None
    for candidate in ["sdot_Speed_Limit_Est", "sdot_Max Speed Diff"]:
        if candidate in df.columns:
            sdot_speed_conf_col = candidate
            break
    if sdot_speed_conf_col is not None:
        try:
            has = pd.to_numeric(df[sdot_speed_conf_col], errors="coerce").fillna(0)
            speed_sources += ((has >= 5) & (has <= 85)).astype(int)
        except: pass
    if "hpms_speed_limit" in df.columns:
        try:
            has = pd.to_numeric(df["hpms_speed_limit"], errors="coerce").fillna(0)
            speed_sources += ((has >= 5) & (has <= 85)).astype(int)
        except: pass
    if "map_speed_limit_value" in df.columns:
        speed_sources += (df["map_speed_limit_value"].astype(str).str.strip() != "").astype(int)
    if "maxspeed" in df.columns:
        speed_sources += (df["maxspeed"].astype(str).str.strip() != "").astype(int)

    conf = np.where(speed_sources >= 3, 95, np.where(speed_sources == 2, 80, np.where(speed_sources == 1, 50, 0)))
    df["conf_speed_limit"] = conf
    multi = (speed_sources >= 2).sum()
    print(f"      Speed:      {multi:>6,} confirmed by 2+ sources")

    # ── BRIDGE confidence ──
    br_sources = np.zeros(n, dtype=int)
    if "bridge" in df.columns:
        br_sources += df["bridge"].astype(str).str.strip().str.lower().isin(
            ["yes","viaduct","movable","cantilever"]).astype(int)
    if "Near_Bridge_500ft" in df.columns:
        br_sources += (df["Near_Bridge_500ft"] == "Yes").astype(int)

    df["conf_bridge"] = np.where(br_sources >= 2, 90, np.where(br_sources == 1, 50, 0))
    multi = (br_sources >= 2).sum()
    print(f"      Bridge:     {multi:>6,} confirmed by 2+ sources")


def _add_freshness_bonus(df, conf_array, col, match_val):
    """Add +5 for <2yr, +3 for <4yr Mapillary data."""
    # This requires map_speed_limit_dist_ft or similar to be present
    # In practice, freshness is a global property of the Mapillary dataset
    # We can't determine per-row freshness without the first_seen date
    # which isn't carried into the road database. So this is a no-op placeholder.
    # The actual freshness scoring happens in the Mapillary source file.
    return conf_array


# ═══════════════════════════════════════════════════════════════
#  MAPILLARY FRESHNESS SCORING (when raw Mapillary data available)
# ═══════════════════════════════════════════════════════════════

def score_mapillary_freshness(mapillary_df):
    """
    Score Mapillary features by data freshness.
    Called BEFORE enrichment, filters/weights features.
    
    Returns DataFrame with freshness_score column:
      100 = seen in last year
       80 = seen in last 2 years  
       60 = seen in last 4 years
       40 = seen 4-8 years ago
       20 = seen >8 years ago
    """
    if mapillary_df is None or len(mapillary_df) == 0:
        return mapillary_df
    
    dates = pd.to_datetime(mapillary_df["first_seen"], errors="coerce")
    now = pd.Timestamp.now()
    age_days = (now - dates).dt.days.fillna(9999)
    
    scores = np.where(age_days < 365, 100,
             np.where(age_days < 730, 80,
             np.where(age_days < 1460, 60,
             np.where(age_days < 2920, 40, 20))))
    
    mapillary_df = mapillary_df.copy()
    mapillary_df["freshness_score"] = scores
    
    return mapillary_df


# ═══════════════════════════════════════════════════════════════
#  RISK INDICATORS (derived from multi-source cross-analysis)
# ═══════════════════════════════════════════════════════════════

def compute_risk_indicators(df):
    """
    Derive safety-relevant risk indicators from cross-source data.
    These are NOT raw data — they are analytical conclusions.
    
    Columns added:
      risk_speed_transition       Yes/No — 2+ different speed limits within 500ft
      risk_speed_transition_diff  Speed differential in mph (e.g., 25→45 = 20)
      risk_unsignalized_xwalk     Yes/No — crosswalk WITHOUT signal nearby
      risk_school_exposure        0-100 — school proximity × enrollment weight
      risk_bridge_condition       0-100 — bridge condition + age composite
      risk_departure_curve        Yes/No — guard rail OR curve_class > 0 OR curvature > 1.5
      risk_departure_score        0-100 — composite departure risk from multiple signals
    """
    print("    Risk indicators...")
    n = len(df)

    # ── 1. SPEED TRANSITION ZONES ──
    speed_cols = [c for c in df.columns if c.startswith("map_speed_") and c.endswith("_count")]
    if speed_cols:
        speed_matrix = df[speed_cols].values
        distinct_speeds = (speed_matrix > 0).sum(axis=1)
        is_transition = distinct_speeds >= 2
        df["risk_speed_transition"] = np.where(is_transition, "Yes", "No")

        # Calculate speed differential (max speed - min speed at this location)
        diffs = np.zeros(n, dtype=int)
        for i in range(n):
            if distinct_speeds[i] >= 2:
                present = []
                for col in speed_cols:
                    if df[col].iloc[i] > 0:
                        # Extract speed from column name: map_speed_35_count → 35
                        spd_str = col.replace("map_speed_", "").replace("_count", "")
                        try:
                            present.append(int(spd_str))
                        except ValueError:
                            pass
                if len(present) >= 2:
                    diffs[i] = max(present) - min(present)
        df["risk_speed_transition_diff"] = diffs
        
        tz = is_transition.sum()
        med_diff = np.median(diffs[diffs > 0]) if (diffs > 0).sum() > 0 else 0
        print(f"      Speed transition: {tz:,} zones, median Δ={med_diff:.0f} mph")
    else:
        df["risk_speed_transition"] = "No"
        df["risk_speed_transition_diff"] = 0

    # ── 2. UNSIGNALIZED CROSSWALK ──
    has_xwalk = np.zeros(n, dtype=bool)
    if "map_crosswalk_count" in df.columns:
        has_xwalk |= (df["map_crosswalk_count"].values > 0)
    if "Near_PoiCrossing_100ft" in df.columns:
        has_xwalk |= (df["Near_PoiCrossing_100ft"].values == "Yes")
    
    has_signal = np.zeros(n, dtype=bool)
    if "resolved_has_signal" in df.columns:
        has_signal = (df["resolved_has_signal"].values == "Yes")
    
    unsig = has_xwalk & ~has_signal
    df["risk_unsignalized_xwalk"] = np.where(unsig, "Yes", "No")
    print(f"      Unsignalized crosswalk: {unsig.sum():,} segments "
          f"({has_xwalk.sum():,} total crosswalks, {(has_xwalk & has_signal).sum():,} signalized)")

    # ── 3. SCHOOL EXPOSURE SCORE (0-100) ──
    # Combines: proximity (closer=worse) × enrollment (more students=worse) × school sign presence
    scores = np.zeros(n, dtype=int)
    
    near_school = np.zeros(n, dtype=bool)
    if "Near_School_1000ft" in df.columns:
        near_school = (df["Near_School_1000ft"].values == "Yes")
    
    if near_school.sum() > 0:
        # Distance factor: 0-500ft=40pts, 500-1000ft=25pts, 1000-1500ft=15pts
        dist_pts = np.zeros(n, dtype=int)
        if "nearest_school_dist_ft" in df.columns:
            dist = pd.to_numeric(df["nearest_school_dist_ft"], errors="coerce").fillna(9999).values
            dist_pts = np.where(dist <= 500, 40,
                       np.where(dist <= 1000, 25,
                       np.where(dist <= 1500, 15, 0)))

        # Enrollment factor: >1000=30pts, 500-1000=20pts, <500=10pts
        enr_pts = np.zeros(n, dtype=int)
        if "nearest_school_enrollment" in df.columns:
            enr = pd.to_numeric(df["nearest_school_enrollment"], errors="coerce").fillna(0).values
            enr_pts = np.where(enr > 1000, 30,
                      np.where(enr > 500, 20,
                      np.where(enr > 0, 10, 0)))

        # School zone sign bonus: +20pts
        sign_pts = np.zeros(n, dtype=int)
        if "map_school_zone" in df.columns:
            sign_pts = np.where(df["map_school_zone"].values == "Yes", 20, 0)

        # Multi-school bonus: +10 per additional school
        multi_pts = np.zeros(n, dtype=int)
        if "school_count_1500ft" in df.columns:
            cnt = pd.to_numeric(df["school_count_1500ft"], errors="coerce").fillna(0).values
            multi_pts = np.minimum((cnt - 1).clip(0) * 10, 30).astype(int)

        scores = np.minimum(dist_pts + enr_pts + sign_pts + multi_pts, 100)
        scores = np.where(near_school, scores, 0)
    
    df["risk_school_exposure"] = scores
    high = (scores >= 60).sum()
    print(f"      School exposure: {high:,} high-risk (≥60), "
          f"{(scores > 0).sum():,} any exposure")

    # ── 4. BRIDGE CONDITION SCORE (0-100) ──
    # Lower = better condition. Higher = worse/riskier.
    bridge_risk = np.zeros(n, dtype=int)
    near_bridge = np.zeros(n, dtype=bool)
    if "Near_Bridge_500ft" in df.columns:
        near_bridge = (df["Near_Bridge_500ft"].values == "Yes")
    
    if near_bridge.sum() > 0:
        # Condition: Poor=60, Fair=30, Good=10, Unknown=20
        cond_pts = np.full(n, 0, dtype=int)
        if "nearest_bridge_condition" in df.columns:
            cond = df["nearest_bridge_condition"].astype(str).str.strip().str.lower().values
            cond_pts = np.where(cond == "poor", 60,
                       np.where(cond == "fair", 30,
                       np.where(cond == "good", 10,
                       np.where((cond == "unknown") | (cond == ""), 20, 15))))

        # Age: pre-1940=30, 1940-1960=20, 1960-1990=10, post-1990=5
        age_pts = np.zeros(n, dtype=int)
        if "nearest_bridge_year_built" in df.columns:
            yr = pd.to_numeric(df["nearest_bridge_year_built"], errors="coerce").fillna(0).values
            age_pts = np.where((yr > 0) & (yr < 1940), 30,
                      np.where((yr >= 1940) & (yr < 1960), 20,
                      np.where((yr >= 1960) & (yr < 1990), 10,
                      np.where(yr >= 1990, 5, 15))))

        # Width penalty: narrow bridges (< 8m) = +10
        width_pts = np.zeros(n, dtype=int)
        if "nearest_bridge_width_m" in df.columns:
            w = pd.to_numeric(df["nearest_bridge_width_m"], errors="coerce").fillna(0).values
            width_pts = np.where((w > 0) & (w < 8), 10, 0)

        bridge_risk = np.minimum(cond_pts + age_pts + width_pts, 100)
        bridge_risk = np.where(near_bridge, bridge_risk, 0)

    df["risk_bridge_condition"] = bridge_risk
    poor = (bridge_risk >= 70).sum()
    print(f"      Bridge risk: {poor:,} high-risk (≥70), "
          f"{(bridge_risk > 0).sum():,} any risk")

    # ── 5. ROAD DEPARTURE RISK ──
    # Multiple signals: guard rail, curve class, high curvature, grade class
    depart_signals = np.zeros(n, dtype=int)

    # Guard rail from Mapillary
    if "map_guard_rail" in df.columns:
        depart_signals += (df["map_guard_rail"].values == "Yes").astype(int)

    # HPMS curve class (>0 means curved road)
    if "hpms_curve_class" in df.columns:
        curve = pd.to_numeric(df["hpms_curve_class"], errors="coerce").fillna(0)
        depart_signals += (curve > 0).astype(int).values

    # HPMS grade class (>0 means graded road)
    if "hpms_grade_class" in df.columns:
        grade = pd.to_numeric(df["hpms_grade_class"], errors="coerce").fillna(0)
        depart_signals += (grade > 0).astype(int).values

    # High curvature from OSM (>1.5 = notably curved)
    if "curvature" in df.columns:
        curv = pd.to_numeric(df["curvature"], errors="coerce").fillna(1.0)
        depart_signals += (curv > 1.5).astype(int).values

    # Turn/curve warning signs from Mapillary
    if "map_turn_warning" in df.columns:
        depart_signals += (df["map_turn_warning"].values == "Yes").astype(int)
    if "map_winding_road" in df.columns:
        depart_signals += (df["map_winding_road"].values == "Yes").astype(int)

    df["risk_departure_curve"] = np.where(depart_signals > 0, "Yes", "No")
    
    # Score: 0-100 based on signal count + speed
    depart_score = np.minimum(depart_signals * 25, 75).astype(int)
    # High speed bonus: if speed > 45 and departure risk, add 25
    if "resolved_speed_limit" in df.columns:
        spd = pd.to_numeric(df["resolved_speed_limit"], errors="coerce").fillna(0)
        high_speed_curve = (spd > 45) & (depart_signals > 0)
        depart_score = np.where(high_speed_curve, np.minimum(depart_score + 25, 100), depart_score)
    
    df["risk_departure_score"] = depart_score
    has_risk = (depart_signals > 0).sum()
    high_risk = (depart_score >= 50).sum()
    print(f"      Departure risk: {has_risk:,} flagged, {high_risk:,} high (≥50)")

    print(f"    Risk indicators complete: 7 new columns")


# ═══════════════════════════════════════════════════════════════
#  CURVE ANALYSIS (for frontend curve-crash feature)
# ═══════════════════════════════════════════════════════════════

def compute_curve_analysis(df):
    """
    Comprehensive curve identification and risk scoring for crash analysis.
    
    Uses 4 independent signals:
      1. OSM curvature (geometric angular deflection — most granular)
      2. HPMS curve_class (federal inventory — not always populated)
      3. Mapillary W1-x signs (Turn, Curve, Winding — DOT placed signs)
      4. Mapillary advisory speed near curve signs
    
    Curve Classification (based on OSM curvature ratio):
      curvature = max(length_ratio, 1 + angular_deflection)
      1.0      = perfectly straight
      1.0-1.05 = straight (negligible deviation)
      1.05-1.2 = slight curve
      1.2-1.5  = moderate curve
      1.5-2.5  = sharp curve
      >2.5     = extreme curve (switchback, ramp loop)
    
    Columns added:
      curve_class              1-5 classification (1=Straight...5=Extreme)
      curve_class_label        Straight / Slight / Moderate / Sharp / Extreme
      curve_has_warning_sign   Yes/No — Mapillary W1-x sign within 500ft
      curve_warning_sign_type  Turn / Curve / Winding Road / ""
      curve_advisory_speed     Advisory speed from Mapillary (mph) or 0
      curve_speed_differential Speed limit minus advisory speed (danger indicator)
      curve_risk_score         0-100 composite risk (curvature × speed × signs)
      curve_is_curve           Yes/No — definitive "is this a curve?" for frontend
    """
    print("    Curve analysis...")
    n = len(df)

    # ── 1. CURVE CLASSIFICATION from OSM curvature ──
    curv = pd.to_numeric(df.get("curvature", pd.Series([1.0]*n)), errors="coerce").fillna(1.0).values
    length = pd.to_numeric(df.get("length_m", pd.Series([0.0]*n)), errors="coerce").fillna(0).values
    
    # Classification thresholds (calibrated from Delaware analysis)
    # Short segments (<20m) often have artificially high curvature — suppress
    effective_curv = np.where(length < 20, 1.0, curv)
    
    classes = np.where(effective_curv <= 1.05, 1,           # Straight
              np.where(effective_curv <= 1.2, 2,            # Slight
              np.where(effective_curv <= 1.5, 3,            # Moderate
              np.where(effective_curv <= 2.5, 4,            # Sharp
              5))))                                          # Extreme
    
    labels = np.where(classes == 1, "Straight",
             np.where(classes == 2, "Slight",
             np.where(classes == 3, "Moderate",
             np.where(classes == 4, "Sharp",
             "Extreme"))))
    
    df["curve_class"] = classes
    df["curve_class_label"] = labels
    
    for cls, lbl in [(1,"Straight"),(2,"Slight"),(3,"Moderate"),(4,"Sharp"),(5,"Extreme")]:
        cnt = (classes == cls).sum()
        print(f"      Class {cls} ({lbl:8s}): {cnt:>7,} ({cnt/n*100:.1f}%)")

    # ── 2. HPMS CURVE CLASS ENRICHMENT ──
    # When HPMS has curve_class > 0, it confirms/upgrades the classification
    hpms_curve = np.zeros(n, dtype=int)
    if "hpms_curve_class" in df.columns:
        hpms_curve = pd.to_numeric(df["hpms_curve_class"], errors="coerce").fillna(0).astype(int).values
        hpms_confirmed = (hpms_curve > 0).sum()
        if hpms_confirmed > 0:
            # HPMS curve_class: 1=A, 2=B, 3=C, 4=D, 5=E (increasing curvature)
            # If HPMS says it's curved but OSM says straight, upgrade
            upgrade = (hpms_curve >= 3) & (classes <= 2)
            classes[upgrade] = 3  # At least Moderate
            labels[upgrade] = "Moderate"
            df["curve_class"] = classes
            df["curve_class_label"] = labels
            print(f"      HPMS upgrades: {upgrade.sum():,} segments promoted to Moderate+")

    # ── 3. MAPILLARY CURVE WARNING SIGNS ──
    # W1-1=Turn, W1-2=Curve, W1-3=Reverse Turn, W1-4=Reverse Curve,
    # W1-5=Winding Road, W1-6=Large Arrow, W1-7=Double Arrow, W1-8=Chevron
    df["curve_has_warning_sign"] = "No"
    df["curve_warning_sign_type"] = ""
    
    # Check existing Mapillary columns
    sign_mapping = [
        ("map_turn_warning", "Turn"),
        ("map_curve_warning", "Curve"),
        ("map_winding_road", "Winding Road"),
    ]
    
    for col, sign_type in sign_mapping:
        if col in df.columns:
            has_sign = df[col].astype(str).values == "Yes"
            df.loc[has_sign, "curve_has_warning_sign"] = "Yes"
            # Only set type if not already set (first match wins)
            mask = has_sign & (df["curve_warning_sign_type"] == "")
            df.loc[mask, "curve_warning_sign_type"] = sign_type
    
    sign_count = (df["curve_has_warning_sign"] == "Yes").sum()
    print(f"      Warning signs: {sign_count:,} segments with W1-x signs")
    
    # Signs confirm curves — if sign present but class is Straight, upgrade
    sign_upgrade = (df["curve_has_warning_sign"] == "Yes") & (df["curve_class"] <= 2)
    if sign_upgrade.sum() > 0:
        df.loc[sign_upgrade, "curve_class"] = 3
        df.loc[sign_upgrade, "curve_class_label"] = "Moderate"
        print(f"      Sign upgrades: {sign_upgrade.sum():,} segments promoted (sign proves curve)")

    # ── 4. ADVISORY SPEED ON CURVES ──
    # Speed signs near curve warnings indicate advisory curve speed
    # Lower advisory speed = tighter curve
    df["curve_advisory_speed"] = 0
    df["curve_speed_differential"] = 0
    
    # Find segments with both a curve sign and a speed sign nearby
    # Use per-speed columns: if segment has curve sign AND speed sign count > 0
    has_curve_sign = df["curve_has_warning_sign"] == "Yes"
    
    if has_curve_sign.sum() > 0:
        speed_cols = sorted([c for c in df.columns if c.startswith("map_speed_") and c.endswith("_count")])
        
        for col in speed_cols:
            spd_str = col.replace("map_speed_", "").replace("_count", "")
            try:
                spd_val = int(spd_str)
            except ValueError:
                continue
            
            if col in df.columns:
                # Segments with curve sign AND this speed sign
                has_both = has_curve_sign & (pd.to_numeric(df[col], errors="coerce").fillna(0) > 0)
                if has_both.sum() > 0:
                    # Advisory speed = lowest speed sign near a curve sign
                    current = df.loc[has_both, "curve_advisory_speed"]
                    # Only set if not already set or if this speed is lower
                    should_set = has_both & ((df["curve_advisory_speed"] == 0) | (spd_val < df["curve_advisory_speed"]))
                    df.loc[should_set, "curve_advisory_speed"] = spd_val
    
    # Speed differential = posted speed - advisory speed
    has_advisory = df["curve_advisory_speed"] > 0
    if has_advisory.sum() > 0:
        posted = pd.to_numeric(df.get("resolved_speed_limit", 0), errors="coerce").fillna(0)
        advisory = df["curve_advisory_speed"]
        diff = posted - advisory
        df.loc[has_advisory, "curve_speed_differential"] = diff[has_advisory].clip(0).astype(int)
        
        advisory_count = has_advisory.sum()
        med_diff = df.loc[has_advisory & (diff > 0), "curve_speed_differential"].median()
        print(f"      Advisory speed: {advisory_count:,} segments, "
              f"median differential: {med_diff:.0f} mph")

    # ── 5. CURVE RISK SCORE (0-100) ──
    # Components:
    #   Curvature severity: 0-40 points
    #   Speed factor: 0-30 points (higher speed on curve = worse)
    #   Warning sign absence: 0-15 points (curve without sign = unexpected)
    #   Multi-signal confirmation: 0-15 points
    
    risk = np.zeros(n, dtype=int)
    
    # Curvature severity (from classification)
    curv_pts = np.where(classes <= 1, 0,
               np.where(classes == 2, 10,
               np.where(classes == 3, 20,
               np.where(classes == 4, 30, 40))))
    risk += curv_pts
    
    # Speed factor (resolved_speed_limit on curved segments)
    speed = pd.to_numeric(df.get("resolved_speed_limit", 0), errors="coerce").fillna(0).values
    is_curve = classes >= 3  # Moderate or sharper
    speed_pts = np.where(~is_curve, 0,
                np.where(speed >= 55, 30,
                np.where(speed >= 45, 25,
                np.where(speed >= 35, 15,
                np.where(speed >= 25, 10, 5)))))
    risk += speed_pts
    
    # Warning sign absence (curve exists but no DOT warning sign)
    no_warning = is_curve & (df["curve_has_warning_sign"].values == "No")
    risk += np.where(no_warning, 15, 0)
    
    # Multi-signal confirmation (curvature + guard rail + warning sign all agree)
    multi_signals = np.zeros(n, dtype=int)
    multi_signals += (classes >= 3).astype(int)  # OSM curvature
    multi_signals += (df["curve_has_warning_sign"].values == "Yes").astype(int)  # Mapillary sign
    if "map_guard_rail" in df.columns:
        multi_signals += (df["map_guard_rail"].values == "Yes").astype(int)
    if "hpms_curve_class" in df.columns:
        multi_signals += (hpms_curve > 0).astype(int)
    risk += np.where(multi_signals >= 3, 15, np.where(multi_signals == 2, 10, 0))
    
    risk = np.minimum(risk, 100)
    df["curve_risk_score"] = risk
    
    # ── 6. DEFINITIVE CURVE FLAG (for frontend) ──
    # A segment "is a curve" if ANY of these are true:
    #   - OSM curvature > 1.30 AND length > 20m (geometric proof)
    #   - HPMS curve_class > 0 (federal confirmation)
    #   - Mapillary curve warning sign present (DOT placed sign)
    #   - curve_class >= 3 (our classification says Moderate+)

    is_curve_definitive = (
        ((effective_curv > 1.30) & (length > 20)) |  # Geometry
        (hpms_curve > 0) |                           # Federal
        (df["curve_has_warning_sign"].values == "Yes")  # Sign
    )
    df["curve_is_curve"] = np.where(is_curve_definitive, "Yes", "No")
    
    curve_count = is_curve_definitive.sum()
    high_risk = (risk >= 50).sum()
    print(f"      Definitive curves: {curve_count:,} ({curve_count/n*100:.1f}%)")
    print(f"      High risk (≥50):   {high_risk:,}")
    print(f"    Curve analysis complete: 8 new columns")


# ═══════════════════════════════════════════════════════════════
#  FRONTEND COLUMN MERGE — VDOT Architecture Mapping
# ═══════════════════════════════════════════════════════════════

# FC code → VDOT frontend Functional Class string
FC_LABELS = {
    1: "1-Interstate", 2: "2-Freeway/Expressway",
    3: "3-Principal Arterial", 4: "4-Minor Arterial",
    5: "5-Major Collector", 6: "6-Minor Collector", 7: "7-Local",
}

# OSM highway → approximate FC
OSM_TO_FC = {
    "motorway": 1, "trunk": 2, "primary": 3, "secondary": 4,
    "tertiary": 5, "unclassified": 6, "residential": 7,
    "motorway_link": 2, "trunk_link": 3, "primary_link": 3,
    "secondary_link": 4, "tertiary_link": 5,
}

# FC → SYSTEM
FC_TO_SYSTEM = {
    1: "DOT Interstate", 2: "DOT Primary", 3: "DOT Primary",
    4: "DOT Secondary", 5: "DOT Secondary",
    6: "Non-DOT primary", 7: "Non-DOT secondary",
}

# FC → Ownership
FC_TO_OWNERSHIP = {
    1: "1. State Hwy Agency", 2: "1. State Hwy Agency",
    3: "1. State Hwy Agency", 4: "1. State Hwy Agency",
    5: "2. County Hwy Agency", 6: "2. County Hwy Agency",
    7: "3. City or Town Hwy Agency",
}

# HPMS ownership code → CrashLens standard (full FHWA 27-code table)
# Source: HPMS Field Manual Item 6 (Dec 2016 + 2022 draft)
# Must match HPMS_OWNERSHIP_MAP in generate_hpms_data.py
OWNERSHIP_LABELS = {
    # Primary agencies
    1:  "1. State Hwy Agency",
    2:  "2. County Hwy Agency",
    3:  "3. City or Town Hwy Agency",       # HPMS: Town or Township
    4:  "3. City or Town Hwy Agency",       # HPMS: City or Municipal
    # State sub-agencies
    11: "1. State Hwy Agency",              # State Park, Forest, Reservation
    12: "3. City or Town Hwy Agency",       # Local Park, Forest, Reservation
    21: "1. State Hwy Agency",              # Other State Agency
    25: "3. City or Town Hwy Agency",       # Other Local Agency
    # Private / Railroad
    26: "6. Private/Unknown Roads",         # Private (other than Railroad)
    27: "6. Private/Unknown Roads",         # Railroad
    # Toll authorities
    31: "1. State Hwy Agency",              # State Toll Road Authority
    32: "2. County Hwy Agency",             # Local Toll Authority
    # Public instrumentalities
    40: "1. State Hwy Agency",              # Other Public Instrumentality
    # Tribal
    50: "4. Federal Roads",                 # Indian Tribe Nation
    # Federal agencies
    60: "4. Federal Roads",                 # Other Federal Agency
    62: "4. Federal Roads",                 # Bureau of Indian Affairs
    63: "4. Federal Roads",                 # Bureau of Fish and Wildlife
    64: "4. Federal Roads",                 # U.S. Forest Service
    66: "4. Federal Roads",                 # National Park Service
    67: "4. Federal Roads",                 # Tennessee Valley Authority
    68: "4. Federal Roads",                 # Bureau of Land Management
    69: "4. Federal Roads",                 # Bureau of Reclamation
    70: "4. Federal Roads",                 # Corps of Engineers (Civil)
    72: "4. Federal Roads",                 # Air Force
    73: "4. Federal Roads",                 # Navy/Marines
    74: "4. Federal Roads",                 # Army
    80: "6. Private/Unknown Roads",         # Other
}

# HPMS facility_type → VDOT string (comprehensive)
# HPMS: 1=One-Way, 2=Two-Way, 4=Two-Way Divided, 6=Non-Inventory Direction
FACILITY_LABELS = {
    1: "1-One-Way Undivided",
    2: "3-Two-Way Undivided",           # HPMS code 2 = Two-Way (NOT One-Way Divided)
    3: "3-Two-Way Undivided",           # alternate
    4: "4-Two-Way Divided",
    5: "5-Reversible Exclusively",
    6: "4-Two-Way Divided",             # HPMS: Non-Inventory Direction (divided hwy opposite)
}

# HPMS surface_type → VDOT string (comprehensive — all FHWA codes)
SURFACE_LABELS = {
    1: "1. Concrete",                     # Portland Cement Concrete (Rigid)
    2: "2. Blacktop, Asphalt, Bituminous",# Asphalt Concrete (Flexible)
    3: "2. Blacktop, Asphalt, Bituminous",# Composite (asphalt over concrete) — NOT brick
    4: "4. Slag, Gravel, Stone",          # Gravel/Stone
    5: "5. Dirt",                          # Dirt/Earth
    6: "6. Other",                         # Other
    7: "2. Blacktop, Asphalt, Bituminous", # Composite (PCC with AC overlay) — NOT brick
    8: "5. Dirt",                          # Unpaved
    9: "6. Other",                         # Other (alternate)
    10: "2. Blacktop, Asphalt, Bituminous",# Asphalt (alternate)
    11: "1. Concrete",                     # Composite (Rigid + Flexible)
}


def merge_frontend_columns(df):
    """
    Create VDOT-architecture-named columns from merged multi-source data.
    HPMS always takes priority, then Mapillary, then OSM.
    
    FIXES APPLIED:
      1. Area Type: MPO membership → Urban/Rural (not HPMS urban_code)
      2. RTE Name: Filters out numeric-only junk values
      3. Traffic Control Type: Adds Yield Sign from Mapillary R1-2
      4. x/y renamed to road_lon/road_lat (avoid crash coordinate conflict)
      5. length_m → length_ft (all units in feet)
      6. u_node/v_node → osm_u_node/osm_v_node (clarify meaning)
    """
    print("    Frontend column merge (VDOT architecture)...")
    n = len(df)

    # ── Functional Class (HPMS > StateDOT > OSM — FHWA validates federally) ──
    fc_codes = np.zeros(n, dtype=int)
    fc_source = np.full(n, "", dtype=object)

    if "highway" in df.columns:
        for i, hw in enumerate(df["highway"].values):
            fc = OSM_TO_FC.get(str(hw).strip(), 0)
            if fc > 0:
                fc_codes[i] = fc
                fc_source[i] = "OSM"

    # State DOT FC (fills gaps in OSM, but HPMS overwrites below)
    if "sdot_Functional Class" in df.columns:
        sdot_fc_map = {"1-Interstate": 1, "2-Freeway/Expressway": 2,
                       "3-Principal Arterial": 3, "4-Minor Arterial": 4,
                       "5-Major Collector": 5, "6-Minor Collector": 6, "7-Local": 7}
        for i, v in enumerate(df["sdot_Functional Class"].values):
            fc = sdot_fc_map.get(str(v).strip(), 0)
            if fc > 0:
                fc_codes[i] = fc
                fc_source[i] = "StateDOT"

    # HPMS FC (HIGHEST — FHWA-validated, overwrites StateDOT/OSM)
    if "hpms_f_system" in df.columns:
        hpms_fc = pd.to_numeric(df["hpms_f_system"], errors="coerce").fillna(0).astype(int).values
        mask = hpms_fc > 0
        fc_codes[mask] = hpms_fc[mask]
        fc_source[mask] = "HPMS"

    df["Functional Class"] = [FC_LABELS.get(fc, "") for fc in fc_codes]
    df["resolved_fc_source"] = fc_source
    fc_filled = (fc_codes > 0).sum()
    print(f"      Functional Class:   {fc_filled:>7,} — HPMS:{(fc_source=='HPMS').sum():,} DOT:{(fc_source=='StateDOT').sum():,} OSM:{(fc_source=='OSM').sum():,}")

    # ── SYSTEM (derived from FC) ──
    df["SYSTEM"] = [FC_TO_SYSTEM.get(fc, "") for fc in fc_codes]

    # ── Ownership (HPMS > StateDOT > FC-derived — FHWA validates federally) ──
    own = np.full(n, "", dtype=object)
    own_source = np.full(n, "", dtype=object)
    for i, fc in enumerate(fc_codes):
        label = FC_TO_OWNERSHIP.get(fc, "")
        if label:
            own[i] = label
            own_source[i] = fc_source[i]  # inherits from FC source

    # State DOT Ownership (fills gaps, but HPMS overwrites below)
    if "sdot_Ownership" in df.columns:
        valid_own = {"1. State Hwy Agency", "2. County Hwy Agency",
                     "3. City or Town Hwy Agency", "4. Federal Roads",
                     "5. Toll Roads Maintained by Others", "6. Private/Unknown Roads"}
        for i, v in enumerate(df["sdot_Ownership"].values):
            s = str(v).strip()
            if s in valid_own:
                own[i] = s
                own_source[i] = "StateDOT"

    # HPMS Ownership (HIGHEST — FHWA-validated, overwrites StateDOT)
    if "hpms_ownership" in df.columns:
        hpms_own = pd.to_numeric(df["hpms_ownership"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(hpms_own):
            label = OWNERSHIP_LABELS.get(code, "")
            if label:
                own[i] = label
                own_source[i] = "HPMS"
    df["Ownership"] = own
    df["resolved_ownership_source"] = own_source

    # ── Facility Type (HPMS) ──
    fac_source = np.full(n, "", dtype=object)
    df["Facility Type"] = ""
    if "hpms_facility_type" in df.columns:
        ft = pd.to_numeric(df["hpms_facility_type"], errors="coerce").fillna(0).astype(int).values
        df["Facility Type"] = [FACILITY_LABELS.get(f, "") for f in ft]
        fac_source[ft > 0] = "HPMS"
    df["resolved_facility_source"] = fac_source

    # ── Roadway Surface Type (HPMS > OSM) ──
    df["Roadway Surface Type"] = ""
    if "hpms_surface_type" in df.columns:
        st = pd.to_numeric(df["hpms_surface_type"], errors="coerce").fillna(0).astype(int).values
        df["Roadway Surface Type"] = [SURFACE_LABELS.get(s, "") for s in st]

    # ── RTE Name (HPMS > OSM ref > OSM name — filter junk) ──
    import re
    rte = np.full(n, "", dtype=object)
    if "name" in df.columns:
        for i, v in enumerate(df["name"].values):
            s = str(v).strip()
            if s and s != "nan" and not re.match(r'^-?\d+$', s):
                rte[i] = s
    if "ref" in df.columns:
        for i, v in enumerate(df["ref"].values):
            s = str(v).strip()
            if s and s != "nan" and not re.match(r'^-?\d+$', s):
                rte[i] = s
    if "hpms_route_name" in df.columns:
        for i, v in enumerate(df["hpms_route_name"].values):
            s = str(v).strip()
            if s and s not in ("nan", "0", "") and not re.match(r'^-?\d+$', s):
                rte[i] = s

    # Tier A: State DOT RTE Name (HIGHEST)
    if "sdot_RTE Name" in df.columns:
        for i, v in enumerate(df["sdot_RTE Name"].values):
            s = str(v).strip()
            if s and s not in ("nan", "0", ""):
                rte[i] = s
    df["RTE Name"] = rte
    filled = (rte != "").sum()
    print(f"      RTE Name:           {filled:>7,}")

    # ── Area Type (MPO membership → Urban/Rural, NOT from HPMS urban_code) ──
    area = np.full(n, "Rural", dtype=object)
    if "geo_mpo_name" in df.columns:
        in_mpo = df["geo_mpo_name"].astype(str).str.strip() != ""
        area[in_mpo.values] = "Urban"
    df["Area Type"] = area
    urban = (area == "Urban").sum()
    print(f"      Area Type:          {urban:>7,} Urban, {n-urban:,} Rural")

    # ── Geography → VDOT names ──
    df["DOT District"] = df.get("geo_dot_region", "")
    df["Planning District"] = df.get("geo_planning_district", "")
    df["MPO Name"] = df.get("geo_mpo_name", "")
    df["Physical Juris Name"] = df.get("geo_county_basename", "")
    df["Juris Code"] = df.get("geo_juris_code", "")

    # ── Speed / Lanes / AADT → VDOT names ──
    if "resolved_speed_limit" in df.columns:
        df["Max Speed Diff"] = df["resolved_speed_limit"]
    if "resolved_lanes" in df.columns:
        df["Through_Lanes"] = df["resolved_lanes"]
    # Tier A: State DOT lanes overwrite resolved (StateDOT already in resolved via resolve_lanes)
    if "hpms_aadt" in df.columns:
        df["AADT"] = pd.to_numeric(df["hpms_aadt"], errors="coerce").fillna(0).astype(int)

    # ── Traffic Control Type (Signal > Stop > Yield > others from Mapillary) ──
    tc = np.full(n, "", dtype=object)
    # Yield signs (lowest priority of the three)
    if "map_yield_sign" in df.columns:
        tc = np.where(df["map_yield_sign"].values == "Yes", "8. Yield Sign", tc)
    # Stop signs override yield
    if "map_stop_sign" in df.columns:
        stop = df["map_stop_sign"].values == "Yes"
        tc[stop] = "4. Stop Sign"
    if "Near_PoiStopSign_100ft" in df.columns:
        stop2 = (df["Near_PoiStopSign_100ft"].values == "Yes") & (tc != "4. Stop Sign")
        tc[stop2] = "4. Stop Sign"
    # Signals override everything
    if "resolved_has_signal" in df.columns:
        tc = np.where(df["resolved_has_signal"].values == "Yes", "3. Traffic Signal", tc)
    df["Traffic Control Type"] = tc
    for val in ["3. Traffic Signal", "4. Stop Sign", "8. Yield Sign"]:
        cnt = (tc == val).sum()
        if cnt > 0:
            print(f"      Traffic Control:    {cnt:>7,} {val}")

    # ── Intersection Type (with degree/streets_per_node from OSM) ──
    # OSM intersection_degree comes from a directed MultiDiGraph — a 3-road
    # T-intersection has degree 6, not 3. Prefer streets_per_node (undirected
    # physical street count, Phase 2) when present; otherwise use the
    # directed-degree fallback with the ~2:1 two-way ratio. Note this write
    # is later overridden by road_inventory_postprocess.fix_intersection_type,
    # but we keep it correct for defense in depth.
    df["Intersection Type"] = "1. Not at Intersection"
    if "is_intersection" in df.columns and "intersection_degree" in df.columns:
        is_int = df["is_intersection"].values == "Yes"
        if "streets_per_node" in df.columns:
            spn = pd.to_numeric(df["streets_per_node"], errors="coerce").fillna(0).astype(int).values
            df.loc[is_int & (spn >= 5), "Intersection Type"] = "5. Five-Point, or More"
            df.loc[is_int & (spn == 4), "Intersection Type"] = "4. Four Approaches"
            df.loc[is_int & (spn == 3), "Intersection Type"] = "3. Three Approaches"
        else:
            deg = pd.to_numeric(df["intersection_degree"], errors="coerce").fillna(0).astype(int).values
            df.loc[is_int & (deg >= 10), "Intersection Type"] = "5. Five-Point, or More"
            df.loc[is_int & (deg >= 8) & (deg < 10), "Intersection Type"] = "4. Four Approaches"
            df.loc[is_int & (deg >= 6) & (deg < 8), "Intersection Type"] = "3. Three Approaches"

    # ── School Zone ──
    if "resolved_school_zone" in df.columns:
        df["School Zone"] = df["resolved_school_zone"]

    # ── Roadway Description (from HPMS median type) ──
    df["Roadway Description"] = ""
    if "hpms_median_type" in df.columns:
        mt = pd.to_numeric(df["hpms_median_type"], errors="coerce").fillna(0).astype(int).values
        desc_map = {1: "1. Two-Way, Not Divided", 2: "2. Two-Way, Divided, Unprotected Median",
                    3: "3. Two-Way, Divided, Positive Median Barrier",
                    4: "4. One-Way, Not Divided"}
        df["Roadway Description"] = [desc_map.get(m, "") for m in mt]

    # ── Roadway Alignment (from curve analysis) ──
    if "curve_class_label" in df.columns:
        align_map = {"Straight": "1. Straight - Level", "Slight": "1. Straight - Level",
                     "Moderate": "2. Curve - Level", "Sharp": "4. Grade - Curve",
                     "Extreme": "4. Grade - Curve"}
        df["Roadway Alignment"] = df["curve_class_label"].map(align_map).fillna("")

    # ── GPS: road_lon / road_lat (NOT x/y — avoid crash coordinate conflict) ──
    df["road_lon"] = df.get("mid_lon", 0.0)
    df["road_lat"] = df.get("mid_lat", 0.0)

    # ── Length in feet (not meters) ──
    if "length_m" in df.columns:
        df["length_ft"] = (pd.to_numeric(df["length_m"], errors="coerce").fillna(0) * 3.28084).round(1)

    # ── Rename u_node/v_node for clarity ──
    if "u_node" in df.columns:
        df.rename(columns={"u_node": "osm_u_node", "v_node": "osm_v_node"}, inplace=True)

    # ── HPMS v2: New frontend columns from 63-field HPMS ──
    # These are single-source (HPMS only), no conflict resolution needed.
    # Just copy from hpms_ prefix to clean frontend names.
    HPMS_V2_DIRECT = {
        # hpms_ column → frontend column name
        "hpms_capacity":                    "Capacity",
        "hpms_directional_through_lanes":   "Directional_Lanes",
        "hpms_peak_lanes":                  "Peak_Lanes",
        "hpms_pct_peak_single":             "Pct_Peak_Single_Unit",
        "hpms_structure_type":              "Structure_Type",
        "hpms_toll_charged":                "Toll_Charged",
        "hpms_hov_type":                    "HOV_Type",
        "hpms_climate_zone":                "Climate_Zone",
        "hpms_cracking_percent":            "Cracking_Pct",
        "hpms_route_id":                    "ARNOLD_Route_ID",
        "hpms_begin_point":                 "ARNOLD_Begin_MP",
        "hpms_end_point":                   "ARNOLD_End_MP",
        "hpms_section_length":              "HPMS_Section_Length_mi",
        "hpms_year_record":                 "HPMS_Year",
    }
    v2_filled = 0
    for src, tgt in HPMS_V2_DIRECT.items():
        if src in df.columns:
            df[tgt] = df[src]
            if df[src].dtype in [np.float64, np.float32, np.int64, np.int32,
                                  np.int16, np.int8, np.uint8, np.uint16]:
                v2_filled += (pd.to_numeric(df[src], errors="coerce").fillna(0) != 0).sum()
            else:
                v2_filled += (df[src].astype(str).str.strip() != "").sum()
    if v2_filled > 0:
        print(f"      HPMS v2 columns:   {len([s for s in HPMS_V2_DIRECT if s in df.columns]):>3} of {len(HPMS_V2_DIRECT)} mapped")

    print(f"    Frontend merge complete")


# ═══════════════════════════════════════════════════════════════
#  TRAFFIC ENGINEERING METRICS (uses HPMS v2 fields)
# ═══════════════════════════════════════════════════════════════

def compute_traffic_engineering_metrics(df):
    """
    Derive traffic engineering metrics from HPMS v2 enhanced fields.
    Uses capacity, directional lanes, AADT, peak factors to compute
    volume/capacity ratio, level of service estimates, and truck exposure.

    Columns added:
      te_volume_capacity_ratio    AADT / (capacity × dir_factor) — congestion indicator
      te_level_of_service         A-F from V/C ratio (HCM 2010 approximation)
      te_truck_pct                (AADT_single + AADT_combo) / AADT × 100
      te_peak_hour_volume         AADT × K_factor — design hour volume
      te_lane_utilization         AADT / (through_lanes × 365) — avg daily vehicles per lane
      te_pavement_condition       Good/Fair/Poor from IRI + cracking + PSR
      te_is_toll                  Yes/No from toll_charged
      te_is_hov                  Yes/No from hov_type
      te_on_structure             Yes/No/Bridge/Tunnel/Causeway from structure_type
    """
    print("    Traffic engineering metrics (HPMS v2)...")
    n = len(df)
    added = 0

    # ── Volume/Capacity Ratio ──
    aadt = pd.to_numeric(df.get("hpms_aadt", 0), errors="coerce").fillna(0).values
    capacity = pd.to_numeric(df.get("hpms_capacity", 0), errors="coerce").fillna(0).values
    dir_factor = pd.to_numeric(df.get("hpms_dir_factor", 0), errors="coerce").fillna(0.5).values
    dir_factor = np.where(dir_factor <= 0, 0.5, dir_factor)  # default 50/50 split

    vc = np.zeros(n, dtype=float)
    has_cap = capacity > 0
    if has_cap.sum() > 0:
        vc[has_cap] = np.round(aadt[has_cap] / (capacity[has_cap] * dir_factor[has_cap] * 365) * 10, 2)
        vc = np.clip(vc, 0, 5.0)  # cap at 5.0 (beyond F)
    df["te_volume_capacity_ratio"] = vc

    # Level of service: HCM 2010 approximation from V/C
    los = np.full(n, "", dtype=object)
    los[vc > 0] = np.where(vc[vc > 0] <= 0.6, "A",
                  np.where(vc[vc > 0] <= 0.7, "B",
                  np.where(vc[vc > 0] <= 0.8, "C",
                  np.where(vc[vc > 0] <= 0.9, "D",
                  np.where(vc[vc > 0] <= 1.0, "E", "F")))))
    df["te_level_of_service"] = los
    has_los = (los != "").sum()
    if has_los > 0:
        congested = ((los == "E") | (los == "F")).sum()
        print(f"      V/C ratio: {has_los:,} segments scored, {congested:,} congested (E/F)")
    added += 2

    # ── Truck Percentage ──
    aadt_su = pd.to_numeric(df.get("hpms_aadt_single_unit", 0), errors="coerce").fillna(0).values
    aadt_co = pd.to_numeric(df.get("hpms_aadt_combination", 0), errors="coerce").fillna(0).values
    truck_aadt = aadt_su + aadt_co
    truck_pct = np.where(aadt > 0, np.round(truck_aadt / aadt * 100, 1), 0)
    df["te_truck_pct"] = truck_pct
    high_truck = (truck_pct > 10).sum()
    print(f"      Truck exposure: {high_truck:,} segments >10% truck AADT")
    added += 1

    # ── Peak Hour Volume ──
    k_factor = pd.to_numeric(df.get("hpms_k_factor", 0), errors="coerce").fillna(0).values
    phv = np.where((aadt > 0) & (k_factor > 0), np.round(aadt * k_factor / 100, 0), 0)
    df["te_peak_hour_volume"] = phv.astype(int)
    added += 1

    # ── Lane Utilization ──
    lanes = pd.to_numeric(df.get("hpms_through_lanes", 0), errors="coerce").fillna(0).values
    lane_util = np.where((aadt > 0) & (lanes > 0), np.round(aadt / (lanes * 365), 0), 0)
    df["te_lane_utilization"] = lane_util.astype(int)
    added += 1

    # ── Pavement Condition (composite from IRI + cracking + PSR) ──
    iri = pd.to_numeric(df.get("hpms_iri", 0), errors="coerce").fillna(0).values
    crack = pd.to_numeric(df.get("hpms_cracking_percent", 0), errors="coerce").fillna(0).values
    psr = pd.to_numeric(df.get("hpms_psr", 0), errors="coerce").fillna(0).values

    pave_cond = np.full(n, "", dtype=object)
    has_pave = (iri > 0) | (psr > 0)
    if has_pave.sum() > 0:
        # IRI: <95=Good, 95-170=Fair, >170=Poor (FHWA thresholds)
        iri_score = np.where(iri <= 0, 50,
                   np.where(iri <= 95, 90,
                   np.where(iri <= 170, 60, 20)))
        # PSR: >3.5=Good, 2.5-3.5=Fair, <2.5=Poor
        psr_score = np.where(psr <= 0, 50,
                   np.where(psr >= 3.5, 90,
                   np.where(psr >= 2.5, 60, 20)))
        # Cracking: <5%=Good, 5-20%=Fair, >20%=Poor
        crack_score = np.where(crack <= 0, 50,
                     np.where(crack <= 5, 90,
                     np.where(crack <= 20, 60, 20)))
        # Composite: weighted average (IRI has most data)
        composite = (iri_score * 0.5 + psr_score * 0.3 + crack_score * 0.2)
        pave_cond = np.where(~has_pave, "",
                   np.where(composite >= 75, "Good",
                   np.where(composite >= 45, "Fair", "Poor")))
    df["te_pavement_condition"] = pave_cond
    has_pc = (pave_cond != "").sum()
    if has_pc > 0:
        poor = (pave_cond == "Poor").sum()
        print(f"      Pavement condition: {has_pc:,} scored, {poor:,} Poor")
    added += 1

    # ── Simple flag columns ──
    if "hpms_toll_charged" in df.columns:
        toll = pd.to_numeric(df["hpms_toll_charged"], errors="coerce").fillna(0).astype(int).values
        df["te_is_toll"] = np.where(toll > 0, "Yes", "No")
        print(f"      Toll roads: {(toll > 0).sum():,}")
        added += 1

    if "hpms_hov_type" in df.columns:
        hov = pd.to_numeric(df["hpms_hov_type"], errors="coerce").fillna(0).astype(int).values
        df["te_is_hov"] = np.where(hov > 0, "Yes", "No")
        print(f"      HOV lanes: {(hov > 0).sum():,}")
        added += 1

    if "hpms_structure_type" in df.columns:
        st = pd.to_numeric(df["hpms_structure_type"], errors="coerce").fillna(0).astype(int).values
        st_map = {0: "No", 1: "Bridge", 2: "Tunnel", 3: "Causeway"}
        df["te_on_structure"] = [st_map.get(v, "No") for v in st]
        print(f"      On structure: {(st > 0).sum():,} (bridge/tunnel/causeway)")
        added += 1

    print(f"    Traffic engineering: {added} new te_ columns")
