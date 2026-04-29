#!/usr/bin/env python3
"""
in_state_dot.py — Indiana DOT (INDOT) Road Inventory Downloader & Normalizer
===============================================================================
Called by generate_state_dot_data.py (root).

Data Sources:
  PRIMARY — Functional Classification (Layer 22):
    https://gis.indot.in.gov/ro/rest/services/
      RAH_GIO_Collaboration/LRSE_Functional_Class/FeatureServer/22
    ~140,000 segments, FUNCTIONAL_CLASS coded SmallInteger 1-7
    MaxRecordCount: 2000, Spatial Reference: 26916 (NAD83 UTM 16N)

  SECONDARY — Speed Limits (Layer 11):
    https://gis.indot.in.gov/ro/rest/services/
      RAH_GIO_Collaboration/LRSE_Speed_Limits/FeatureServer/11
    ~140,000 segments, joined by ROUTE_ID

ENDPOINT STABILITY: gis.indot.in.gov stable since ~2015. Service Item
ID: 2e4d483f0ad14dfdafe318a44850ae82. Server can be slow (10-30s responses).

RECORD_STATUS coded values: 0=WIP, 1=Proposed, 2=Withdrawn, +5 more.
Safety: if filtering removes >90% of rows, skip filter entirely.

Usage:
  python generate_state_dot_data.py --state in
  python generate_state_dot_data.py --state in --upload
"""

import traceback

# ═══════════════════════════════════════════════════════════════
#  ENDPOINT CONFIGURATION
# ═══════════════════════════════════════════════════════════════

STATE_ABBR = "in"
STATE_NAME = "Indiana"
STATE_FIPS = "18"
STATE_DOT = "INDOT"

ENDPOINT_URL = (
    "https://gis.indot.in.gov/ro/rest/services/"
    "RAH_GIO_Collaboration/LRSE_Functional_Class/FeatureServer/22"
)

SPEED_LIMIT_URL = (
    "https://gis.indot.in.gov/ro/rest/services/"
    "RAH_GIO_Collaboration/LRSE_Speed_Limits/FeatureServer/11"
)

MAX_RECORD_COUNT = 2000
OUT_SR = 4326
GEOMETRY_TYPE = "polyline"
OUT_FIELDS = None


# ═══════════════════════════════════════════════════════════════
#  FIELD MAPPING
# ═══════════════════════════════════════════════════════════════

FIELD_MAP = {
    "FUNCTIONAL_CLASS":              "dot_fc_code",
    "ROUTE_ID":                      "dot_route_id",
    "EVENT_ID":                      "dot_event_id",
    "FROM_MEASURE":                  "dot_beg_mp",
    "TO_MEASURE":                    "dot_end_mp",
    "RECORD_STATUS":                 "dot_record_status",
    "LOCERROR":                      "dot_loc_error",
    "FROM_DATE":                     "dot_from_date",
    "TO_DATE":                       "dot_to_date",
    "DATE_ATTR_EFFECTIVE":           "dot_attr_effective",
    "DATE_CREATED":                  "dot_date_created",
    "DATE_EDITED":                   "dot_date_edited",
    "CREATED_BY":                    "dot_created_by",
    "EDITED_BY":                     "dot_edited_by",
}


# ═══════════════════════════════════════════════════════════════
#  VALUE TRANSFORMS
# ═══════════════════════════════════════════════════════════════

# FUNCTIONAL_CLASS is SmallInteger. ArcGIS may return int or str.
FC_CODE_MAP = {
    # Integer keys (native SmallInteger from ArcGIS)
    1: "1-Interstate", 2: "2-Freeway/Expressway",
    3: "3-Principal Arterial", 4: "4-Minor Arterial",
    5: "5-Major Collector", 6: "6-Minor Collector", 7: "7-Local",
    # String keys (after astype(str))
    "1": "1-Interstate", "2": "2-Freeway/Expressway",
    "3": "3-Principal Arterial", "4": "4-Minor Arterial",
    "5": "5-Major Collector", "6": "6-Minor Collector", "7": "7-Local",
    # Float keys (some ArcGIS JSON returns floats: 7.0)
    "1.0": "1-Interstate", "2.0": "2-Freeway/Expressway",
    "3.0": "3-Principal Arterial", "4.0": "4-Minor Arterial",
    "5.0": "5-Major Collector", "6.0": "6-Minor Collector", "7.0": "7-Local",
}

FC_TO_SYSTEM = {
    "1-Interstate":          "DOT Interstate",
    "2-Freeway/Expressway":  "DOT Primary",
    "3-Principal Arterial":  "DOT Primary",
    "4-Minor Arterial":      "DOT Secondary",
    "5-Major Collector":     "DOT Secondary",
    "6-Minor Collector":     "Non-DOT primary",
    "7-Local":               "Non-DOT secondary",
}

ROUTE_PREFIX_OWNERSHIP = {
    "I": "1. State Hwy Agency",
    "U": "1. State Hwy Agency",
    "S": "1. State Hwy Agency",
    "L": "2. County Hwy Agency",
    "C": "2. County Hwy Agency",
    "M": "3. City or Town Hwy Agency",
    "T": "3. City or Town Hwy Agency",
}


# ═══════════════════════════════════════════════════════════════
#  SECONDARY DOWNLOAD HELPER
# ═══════════════════════════════════════════════════════════════

def _download_secondary_attributes(url, label, timeout=180, max_records=2000):
    """Download attributes (no geometry). Returns list of dicts or [] on failure."""
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CrashLens/1.0",
        "Accept": "application/json",
    })
    query_url = f"{url}/query"

    # Count
    try:
        resp = session.get(query_url, params={
            "where": "1=1", "returnCountOnly": "true", "f": "json"
        }, timeout=timeout)
        data = resp.json()
        if "error" in data:
            print(f"  [{label}] Server error: {data['error'].get('message', 'unknown')}")
            return []
        total = data.get("count", 0)
        if total == 0:
            print(f"  [{label}] No features — skipping")
            return []
        print(f"  [{label}] {total:,} features to download")
    except Exception as e:
        print(f"  [{label}] Count failed: {e} — skipping")
        return []

    # Paginated download
    all_attrs = []
    offset = 0
    batch_num = 0
    consecutive_failures = 0
    while offset < total:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultOffset": str(offset),
            "resultRecordCount": str(max_records),
            "f": "json",
        }
        try:
            resp = session.get(query_url, params=params, timeout=timeout)
            data = resp.json()
            if "error" in data:
                print(f"    [{label}] Server error at offset {offset}: {data['error'].get('message', '')}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    print(f"    [{label}] 3 consecutive failures, stopping")
                    break
                continue
            features = data.get("features", [])
            if not features:
                break
            for f in features:
                all_attrs.append(f.get("attributes", {}))
            offset += len(features)
            batch_num += 1
            consecutive_failures = 0
            if batch_num % 10 == 0:
                print(f"    [{label}] {len(all_attrs):,}/{total:,}...")
        except Exception as e:
            print(f"    [{label}] Batch at offset {offset} failed: {e}")
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"    [{label}] 3 consecutive failures, stopping with {len(all_attrs):,} records")
                break
            continue

    print(f"  [{label}] Downloaded {len(all_attrs):,} records")
    return all_attrs


# ═══════════════════════════════════════════════════════════════
#  NORMALIZER
# ═══════════════════════════════════════════════════════════════

def normalize(df):
    """Normalize INDOT FC data → CrashLens columns."""
    import numpy as np
    import pandas as pd
    import re

    n = len(df)
    print(f"  Normalizing {n:,} Indiana road segments...")

    # ════════════════════════════════════════════════════
    #  FILTER: Remove non-active records
    #  SAFETY: Virginia lesson — if filter removes >90%, skip it
    # ════════════════════════════════════════════════════

    status = df.get("dot_record_status", pd.Series("", index=df.index))
    status_str = status.fillna("").astype(str).str.strip()

    # Remove trailing ".0" from float-stringified ints (e.g. "3.0" → "3")
    status_str = status_str.str.replace(r'\.0$', '', regex=True)

    # Debug: show ALL status values
    status_counts = status_str.value_counts()
    print(f"    Record Status distribution:")
    for val, cnt in status_counts.items():
        print(f"      '{val}': {cnt:,}")

    # Only filter explicitly known-bad statuses
    # INDOT coded: 0=Work In Progress, 1=Proposed, 2=Withdrawn
    bad_statuses = {"0", "1", "2"}
    inactive_mask = status_str.isin(bad_statuses)
    would_remove = inactive_mask.sum()

    if would_remove > 0:
        keep_count = n - would_remove
        keep_pct = (keep_count / n * 100) if n > 0 else 0

        if keep_pct < 10:
            # SAFETY: filtering would kill >90% of rows — skip filter
            # This is the Virginia Bug 1 pattern
            print(f"    ⚠️  SAFETY: filtering would remove {would_remove:,}/{n:,} "
                  f"({100-keep_pct:.1f}%) — SKIPPING filter to avoid data loss")
        else:
            print(f"    Filtered: {would_remove:,} inactive records (WIP/Proposed/Withdrawn)")
            df = df[~inactive_mask].reset_index(drop=True)
            n = len(df)

    print(f"    Remaining: {n:,} segments")

    if n == 0:
        print("    ⚠️  No segments remaining — returning empty DataFrame")
        return df

    # ════════════════════════════════════════════════════
    #  1. FUNCTIONAL CLASS
    # ════════════════════════════════════════════════════

    fc_raw = df.get("dot_fc_code", pd.Series("", index=df.index))
    fc_str = fc_raw.fillna("").astype(str).str.strip()
    # Remove ".0" from float representation (ArcGIS JSON sometimes returns 7.0)
    fc_str = fc_str.str.replace(r'\.0$', '', regex=True)

    df["Functional Class"] = fc_str.map(FC_CODE_MAP).fillna("")

    # Fallback: try integer mapping
    empty_fc = df["Functional Class"] == ""
    if empty_fc.any():
        fc_int = pd.to_numeric(fc_raw[empty_fc], errors="coerce")
        fc_from_int = fc_int.map(FC_CODE_MAP).fillna("")
        df.loc[empty_fc & (fc_from_int != ""), "Functional Class"] = (
            fc_from_int[empty_fc & (fc_from_int != "")]
        )

    # Fallback: extract leading digit
    empty_fc = df["Functional Class"] == ""
    if empty_fc.any():
        leading = fc_str[empty_fc].str.extract(r'^(\d)', expand=False).fillna("")
        fc_from_lead = leading.map(FC_CODE_MAP).fillna("")
        df.loc[empty_fc & (fc_from_lead != ""), "Functional Class"] = (
            fc_from_lead[empty_fc & (fc_from_lead != "")]
        )

    fc_fill = (df["Functional Class"] != "").sum()
    pct = (fc_fill / n * 100) if n > 0 else 0
    print(f"    Functional Class: {fc_fill:,}/{n:,} ({pct:.1f}%)")

    # Show FC distribution
    fc_dist = df["Functional Class"].value_counts()
    print(f"    FC distribution:")
    for val, cnt in fc_dist.head(10).items():
        print(f"      {val}: {cnt:,}")

    # ════════════════════════════════════════════════════
    #  2. SYSTEM
    # ════════════════════════════════════════════════════

    df["SYSTEM"] = df["Functional Class"].map(FC_TO_SYSTEM).fillna("")

    # ════════════════════════════════════════════════════
    #  3. RTE NAME + OWNERSHIP (from ROUTE_ID)
    # ════════════════════════════════════════════════════

    route_id = df.get("dot_route_id", pd.Series("", index=df.index))
    route_id = route_id.fillna("").astype(str).str.strip()

    # Show ROUTE_ID samples for debugging
    rid_samples = route_id[route_id != ""].head(10).tolist()
    print(f"    ROUTE_ID samples: {rid_samples[:5]}")

    def _parse_route_id(rid):
        """
        Parse INDOT ROUTE_ID. Handles multiple formats:
          "I0065-0100" → ("I", 65)
          "U0031"      → ("U", 31)
          "S037"       → ("S", 37)
          "I-65"       → ("I", 65)
          "US31"       → ("U", 31)
          "SR37"       → ("S", 37)
        """
        if not rid:
            return "", 0
        rid = rid.strip()

        # Format 1: Letter + 4 digits (+ optional suffix): "I0065-0100"
        m = re.match(r'^([A-Z])(\d{3,5})', rid)
        if m:
            return m.group(1), int(m.group(2))

        # Format 2: "I-65", "US-31", "SR-37"
        m = re.match(r'^(I|US|SR|IN)-?\s*(\d+)', rid, re.IGNORECASE)
        if m:
            prefix_map = {"I": "I", "US": "U", "SR": "S", "IN": "S"}
            prefix = prefix_map.get(m.group(1).upper(), m.group(1)[0].upper())
            return prefix, int(m.group(2))

        # Format 3: just a letter prefix
        if rid and rid[0].isalpha():
            return rid[0].upper(), 0

        return "", 0

    rte_names = []
    ownerships = []

    for rid in route_id:
        prefix, number = _parse_route_id(rid)

        if prefix == "I" and number > 0:
            rte_names.append(f"I {number}")
        elif prefix == "U" and number > 0:
            rte_names.append(f"US {number}")
        elif prefix == "S" and number > 0:
            rte_names.append(f"SR {number}")
        elif prefix and number > 0:
            rte_names.append(f"{prefix} {number}")
        else:
            rte_names.append("")

        ownerships.append(ROUTE_PREFIX_OWNERSHIP.get(prefix, ""))

    df["RTE Name"] = rte_names
    df["Ownership"] = ownerships

    # Ownership fallback: FC-based
    empty_own = df["Ownership"] == ""
    if empty_own.any():
        fc_own = {
            "1-Interstate": "1. State Hwy Agency",
            "2-Freeway/Expressway": "1. State Hwy Agency",
            "3-Principal Arterial": "1. State Hwy Agency",
            "4-Minor Arterial": "1. State Hwy Agency",
            "5-Major Collector": "2. County Hwy Agency",
            "6-Minor Collector": "2. County Hwy Agency",
            "7-Local": "3. City or Town Hwy Agency",
        }
        df.loc[empty_own, "Ownership"] = (
            df.loc[empty_own, "Functional Class"].map(fc_own).fillna("")
        )

    print(f"    RTE Name: {(df['RTE Name'] != '').sum():,}/{n:,}")
    print(f"    Ownership: {(df['Ownership'] != '').sum():,}/{n:,}")

    # ════════════════════════════════════════════════════
    #  4. SPEED LIMIT (from Speed Limits layer + fallback)
    # ════════════════════════════════════════════════════

    df["Speed_Limit_Est"] = ""
    try:
        spd_attrs = _download_secondary_attributes(
            SPEED_LIMIT_URL, "SpeedLimits", timeout=180, max_records=2000
        )
        if spd_attrs:
            spd_df = pd.DataFrame(spd_attrs)
            cols = list(spd_df.columns)
            print(f"    SpeedLimits columns: {cols[:10]}...")

            # Find speed column — try many candidates
            spd_col = None
            for c in ["SPEED_LIMIT", "SPEED_LIM", "MAX_SPEED", "POSTED_SPEED",
                       "SPEED_LIMITS", "SPD_LMT", "SPEED", "POSTED_SPEED_LIMIT"]:
                if c in cols:
                    spd_col = c
                    break
            # Fallback: any column with SPEED in the name
            if not spd_col:
                speed_cols = [c for c in cols if "SPEED" in c.upper()]
                if speed_cols:
                    spd_col = speed_cols[0]
                    print(f"    SpeedLimits: using auto-detected column '{spd_col}'")

            if spd_col and "ROUTE_ID" in cols:
                spd_df["_speed"] = pd.to_numeric(spd_df[spd_col], errors="coerce")
                valid_speeds = spd_df["_speed"].dropna()
                if len(valid_speeds) > 0:
                    print(f"    SpeedLimits: {len(valid_speeds):,} valid speeds, "
                          f"range {valid_speeds.min():.0f}-{valid_speeds.max():.0f} mph")

                # Build route → median speed lookup
                speed_lookup = (spd_df[spd_df["_speed"] > 0]
                               .groupby("ROUTE_ID")["_speed"]
                               .median().to_dict())

                if "dot_route_id" in df.columns and speed_lookup:
                    spd_mapped = df["dot_route_id"].map(speed_lookup)
                    valid_spd = spd_mapped.notna() & (spd_mapped > 0)
                    df.loc[valid_spd, "Speed_Limit_Est"] = (
                        spd_mapped[valid_spd].round().astype(int).astype(str)
                    )
                    spd_count = valid_spd.sum()
                    pct = (spd_count / n * 100) if n > 0 else 0
                    print(f"    Speed from INDOT: {spd_count:,}/{n:,} ({pct:.1f}%)")
            elif spd_col:
                print(f"    SpeedLimits: found '{spd_col}' but no ROUTE_ID for joining")
            else:
                print(f"    SpeedLimits: no speed column found in {cols[:5]}...")

    except Exception as e:
        print(f"    SpeedLimits download failed (non-fatal): {e}")
        traceback.print_exc()

    # Speed fallback: estimate from FC (Indiana Code IC 9-21-5)
    empty_spd = df["Speed_Limit_Est"] == ""
    if empty_spd.any():
        fc = df["Functional Class"]
        fc_speed = {
            "1-Interstate": "70", "2-Freeway/Expressway": "55",
            "3-Principal Arterial": "45", "4-Minor Arterial": "40",
            "5-Major Collector": "35", "6-Minor Collector": "30", "7-Local": "25",
        }
        fc_spd = df.loc[empty_spd, "Functional Class"].map(fc_speed).fillna("")
        df.loc[empty_spd, "Speed_Limit_Est"] = fc_spd

    spd_fill = (df["Speed_Limit_Est"] != "").sum()
    pct = (spd_fill / n * 100) if n > 0 else 0
    print(f"    Speed_Limit_Est final: {spd_fill:,}/{n:,} ({pct:.1f}%)")

    # ════════════════════════════════════════════════════
    #  5. FACILITY TYPE
    # ════════════════════════════════════════════════════

    fc = df["Functional Class"]
    is_major = fc.isin(["1-Interstate", "2-Freeway/Expressway"])
    df["Facility Type"] = np.where(is_major, "4-Two-Way Divided", "3-Two-Way Undivided")

    # ════════════════════════════════════════════════════
    #  6. AREA TYPE (estimated)
    # ════════════════════════════════════════════════════

    df["Area Type"] = np.where(
        fc.isin(["1-Interstate", "2-Freeway/Expressway", "3-Principal Arterial"]),
        "Suburban", "Rural"
    )

    # ════════════════════════════════════════════════════
    #  7-9. DOT District / Surface / Lanes (defaults)
    # ════════════════════════════════════════════════════

    df["DOT District"] = ""
    df["Roadway Surface Type"] = np.where(
        is_major, "1. Concrete", "2. Blacktop, Asphalt, Bituminous"
    )
    fc_lanes = {
        "1-Interstate": "4", "2-Freeway/Expressway": "4",
        "3-Principal Arterial": "2", "4-Minor Arterial": "2",
        "5-Major Collector": "2", "6-Minor Collector": "2", "7-Local": "2",
    }
    df["Through_Lanes"] = df["Functional Class"].map(fc_lanes).fillna("")

    # ════════════════════════════════════════════════════
    #  10. MILEPOINTS + SEGMENT LENGTH
    # ════════════════════════════════════════════════════

    beg = pd.to_numeric(df.get("dot_beg_mp", 0), errors="coerce").fillna(0)
    end = pd.to_numeric(df.get("dot_end_mp", 0), errors="coerce").fillna(0)
    df["RNS MP"] = np.where(beg > 0, beg, 0)
    seg_len = (end - beg).clip(lower=0)
    df["Segment_Length_mi"] = np.where(seg_len > 0, np.round(seg_len, 3), 0)

    # ════════════════════════════════════════════════════
    #  11-12. ROADWAY DESCRIPTION + COUNTY
    # ════════════════════════════════════════════════════

    desc_map = {
        "3-Two-Way Undivided": "1. Two-Way, Not Divided",
        "4-Two-Way Divided": "2. Two-Way, Divided, Unprotected Median",
    }
    df["Roadway Description"] = df["Facility Type"].map(desc_map).fillna(
        "1. Two-Way, Not Divided"
    )
    df["Physical Juris Name"] = ""

    # ════════════════════════════════════════════════════
    #  SOURCE TRACKING
    # ════════════════════════════════════════════════════

    df["dot_source"] = "INDOT LRS Functional Classification"
    df["dot_source_url"] = ENDPOINT_URL

    # ════════════════════════════════════════════════════
    #  SUMMARY
    # ════════════════════════════════════════════════════

    print(f"\n  Normalization complete — {n:,} segments:")
    for col in ["Functional Class", "Ownership", "Area Type", "DOT District",
                "RTE Name", "Speed_Limit_Est", "Through_Lanes", "Facility Type",
                "Roadway Surface Type", "Physical Juris Name"]:
        if col in df.columns:
            pop = (df[col].fillna("").astype(str).str.strip() != "").sum()
            pct = (pop / n * 100) if n > 0 else 0
            print(f"    {col:25s}: {pop:>6,}/{n:,} ({pct:5.1f}%)")

    return df


if __name__ == "__main__":
    print(f"Indiana DOT Road Inventory Config (INDOT)")
    print(f"  Primary:   {ENDPOINT_URL}")
    print(f"  Secondary: {SPEED_LIMIT_URL}")
    print(f"  Fields:    {len(FIELD_MAP)} mapped")
    print(f"  FC:        SmallInteger coded 1-7")
    print(f"  Run via:   python generate_state_dot_data.py --state in")
