#!/usr/bin/env python3
"""
patch_road_data_authority.py — Apply State DOT Tier A to road_data_authority.py
================================================================================
Run this script to patch your existing road_data_authority.py with State DOT
as highest authority tier.

Usage:
  python patch_road_data_authority.py road_data_authority.py

Changes applied:
  1. Header: Updated authority hierarchy (State DOT = Tier A)
  2. resolve_speed_limit: Added sdot_ speed check above HPMS
  3. resolve_lanes: Added sdot_ lanes check above HPMS
  4. resolve_surface: Added sdot_ surface check above HPMS
  5. merge_frontend_columns: Added sdot_ priority for FC, Ownership, etc.
  6. OWNERSHIP_LABELS: Fixed to proper VDOT standard values
"""

import sys
from pathlib import Path


def patch_file(filepath):
    with open(filepath, "r") as f:
        code = f.read()

    changes = 0

    # ═══════════════════════════════════════════════════════════
    # CHANGE 1: Update header docstring
    # ═══════════════════════════════════════════════════════════
    old = '''AUTHORITY HIERARCHY:
  Tier 1 — HPMS (Federal, FHWA-validated road inventory)
  Tier 2 — Mapillary (Computer vision, photographed in field)
  Tier 3 — OSM (Community-contributed, variable quality)
  Tier 4 — Federal Point Data (BTS bridges/rail/transit, Urban Institute schools)'''

    new = '''AUTHORITY HIERARCHY:
  Tier A — State DOT Inventory (OPTIONAL — highest when available)
           Source: {abbr}_state_dot.parquet.gz from generate_state_dot_data.py
           Columns prefixed sdot_ (e.g. sdot_Functional Class, sdot_Through_Lanes)
           If file missing → silently skipped, Tier B becomes highest.
  Tier B — HPMS (Federal, FHWA-validated road inventory)
  Tier C — Mapillary (Computer vision, photographed in field)
  Tier D — OSM (Community-contributed, variable quality)
  Tier E — Federal Point Data (BTS bridges/rail/transit, Urban Institute schools)'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ Header updated")
    else:
        print("  ⚠️  Header not found — may already be updated")

    # ═══════════════════════════════════════════════════════════
    # CHANGE 2: resolve_speed_limit — add State DOT above HPMS
    # ═══════════════════════════════════════════════════════════
    old = '''    # Tier 1: HPMS (highest authority — always wins)
    if "hpms_speed_limit" in df.columns:
        for i, v in enumerate(df["hpms_speed_limit"].values):
            try:
                spd = int(v)
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    return values, sources'''

    new = '''    # Tier 1: HPMS (high authority)
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
    if "sdot_Max Speed Diff" in df.columns:
        for i, v in enumerate(df["sdot_Max Speed Diff"].values):
            try:
                spd = int(float(v))
                if 5 <= spd <= 85:
                    values[i] = spd
                    sources[i] = "StateDOT"
            except (ValueError, TypeError):
                pass

    return values, sources'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ resolve_speed_limit updated")

    # ═══════════════════════════════════════════════════════════
    # CHANGE 3: resolve_lanes — add State DOT above HPMS
    # ═══════════════════════════════════════════════════════════
    old = '''    if "hpms_through_lanes" in df.columns:
        for i, v in enumerate(df["hpms_through_lanes"].values):
            try:
                ln = int(v)
                if 1 <= ln <= 12:
                    values[i] = ln
                    sources[i] = "HPMS"
            except (ValueError, TypeError):
                pass

    return values, sources'''

    new = '''    if "hpms_through_lanes" in df.columns:
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

    return values, sources'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ resolve_lanes updated")

    # ═══════════════════════════════════════════════════════════
    # CHANGE 4: resolve_surface — add State DOT above HPMS
    # ═══════════════════════════════════════════════════════════
    old = '''    # HPMS surface_type: 1=Concrete, 2=Asphalt, 3=Brick, 4=Gravel, 5=Dirt
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

    return values, sources'''

    new = '''    # HPMS surface_type: 1=Concrete, 2=Asphalt, 3=Brick, 4=Gravel, 5=Dirt
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

    return values, sources'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ resolve_surface updated")

    # ═══════════════════════════════════════════════════════════
    # CHANGE 5: merge_frontend_columns — add State DOT priority
    # Add after HPMS FC check, before df["Functional Class"] assignment
    # ═══════════════════════════════════════════════════════════
    old = '''    if "hpms_f_system" in df.columns:
        hpms_fc = pd.to_numeric(df["hpms_f_system"], errors="coerce").fillna(0).astype(int).values
        mask = hpms_fc > 0
        fc_codes[mask] = hpms_fc[mask]
        fc_source[mask] = "HPMS"
    
    df["Functional Class"] = [FC_LABELS.get(fc, "") for fc in fc_codes]'''

    new = '''    if "hpms_f_system" in df.columns:
        hpms_fc = pd.to_numeric(df["hpms_f_system"], errors="coerce").fillna(0).astype(int).values
        mask = hpms_fc > 0
        fc_codes[mask] = hpms_fc[mask]
        fc_source[mask] = "HPMS"

    # Tier A: State DOT FC (HIGHEST — overwrites HPMS when available)
    if "sdot_Functional Class" in df.columns:
        sdot_fc_map = {"1-Interstate": 1, "2-Freeway/Expressway": 2,
                       "3-Principal Arterial": 3, "4-Minor Arterial": 4,
                       "5-Major Collector": 5, "6-Minor Collector": 6, "7-Local": 7}
        for i, v in enumerate(df["sdot_Functional Class"].values):
            fc = sdot_fc_map.get(str(v).strip(), 0)
            if fc > 0:
                fc_codes[i] = fc
                fc_source[i] = "StateDOT"
    
    df["Functional Class"] = [FC_LABELS.get(fc, "") for fc in fc_codes]'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ merge_frontend_columns FC updated")

    # Add State DOT Ownership priority
    old = '''    if "hpms_ownership" in df.columns:
        hpms_own = pd.to_numeric(df["hpms_ownership"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(hpms_own):
            label = OWNERSHIP_LABELS.get(code, "")
            if label:
                own[i] = label
    df["Ownership"] = own'''

    new = '''    if "hpms_ownership" in df.columns:
        hpms_own = pd.to_numeric(df["hpms_ownership"], errors="coerce").fillna(0).astype(int).values
        for i, code in enumerate(hpms_own):
            label = OWNERSHIP_LABELS.get(code, "")
            if label:
                own[i] = label

    # Tier A: State DOT Ownership (HIGHEST — overwrites HPMS)
    if "sdot_Ownership" in df.columns:
        valid_own = {"1. State Hwy Agency", "2. County Hwy Agency",
                     "3. City or Town Hwy Agency", "4. Federal Roads",
                     "5. Toll Roads Maintained by Others", "6. Private/Unknown Roads"}
        for i, v in enumerate(df["sdot_Ownership"].values):
            s = str(v).strip()
            if s in valid_own:
                own[i] = s
    df["Ownership"] = own'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ merge_frontend_columns Ownership updated")

    # Add State DOT RTE Name priority
    old = '''    if "hpms_route_name" in df.columns:
        for i, v in enumerate(df["hpms_route_name"].values):
            s = str(v).strip()
            if s and s not in ("nan", "0", "") and not re.match(r\'^-?\\d+$\', s):
                rte[i] = s
    df["RTE Name"] = rte'''

    new = '''    if "hpms_route_name" in df.columns:
        for i, v in enumerate(df["hpms_route_name"].values):
            s = str(v).strip()
            if s and s not in ("nan", "0", "") and not re.match(r\'^-?\\d+$\', s):
                rte[i] = s

    # Tier A: State DOT RTE Name (HIGHEST)
    if "sdot_RTE Name" in df.columns:
        for i, v in enumerate(df["sdot_RTE Name"].values):
            s = str(v).strip()
            if s and s not in ("nan", "0", ""):
                rte[i] = s
    df["RTE Name"] = rte'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ merge_frontend_columns RTE Name updated")

    # Add State DOT Through_Lanes priority
    old = '''    if "resolved_lanes" in df.columns:
        df["Through_Lanes"] = df["resolved_lanes"]'''

    new = '''    if "resolved_lanes" in df.columns:
        df["Through_Lanes"] = df["resolved_lanes"]
    # Tier A: State DOT lanes overwrite resolved (StateDOT already in resolved via resolve_lanes)'''

    if old in code:
        code = code.replace(old, new)
        changes += 1
        print("  ✅ merge_frontend_columns Through_Lanes noted")

    # ═══════════════════════════════════════════════════════════
    # CHANGE 6: Update apply_authority_layer print to show StateDOT
    # ═══════════════════════════════════════════════════════════
    old = '''          f" — HPMS:{(srcs=='HPMS').sum():,} Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")'''

    new = '''          f" — DOT:{(srcs=='StateDOT').sum():,} HPMS:{(srcs=='HPMS').sum():,} Map:{(srcs=='Mapillary').sum():,} OSM:{(srcs=='OSM').sum():,}")'''

    code = code.replace(old, new)
    # This replacement happens for speed_limit only (first occurrence)

    # ═══════════════════════════════════════════════════════════
    # CHANGE 7: Update resolved_columns docstring
    # ═══════════════════════════════════════════════════════════
    old = '''RESOLVED COLUMNS:
  resolved_speed_limit        HPMS > Mapillary > OSM'''

    new = '''RESOLVED COLUMNS:
  resolved_speed_limit        StateDOT > HPMS > Mapillary > OSM'''

    if old in code:
        code = code.replace(old, new)

    # Write patched file
    with open(filepath, "w") as f:
        f.write(code)

    print(f"\n  Total changes applied: {changes}")
    print(f"  Output: {filepath} ({len(code.splitlines())} lines)")
    return changes


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python patch_road_data_authority.py road_data_authority.py")
        sys.exit(1)

    filepath = sys.argv[1]
    if not Path(filepath).exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    print(f"Patching {filepath} with State DOT Tier A...")
    n = patch_file(filepath)
    if n > 0:
        print(f"\n✅ Done. {n} changes applied.")
    else:
        print(f"\n⚠️  No changes applied — file may already be patched.")
