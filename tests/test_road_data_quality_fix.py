"""
Bug test for road-data-quality fixes (2026-04-17).

Covers two independent fixes:
  1. road_data_authority.resolve_speed_limit — FC-5 collector statutory
     defaults (tertiary/tertiary_link → 35 mph in-MPO, 50 mph rural).
     Prior to the fix, ~20.6k FC-5 Major Collector segments had
     resolved_speed_limit=0 and empty source.
  2. geo_resolver.MPO_ALIASES + resolve_row — normalizes the two
     variants of Dover/Kent County MPO so R2 folder slugs are unique.

Run:   pytest tests/test_road_data_quality_fix.py -v
"""
import numpy as np
import pandas as pd
import pytest

from road_data_authority import resolve_speed_limit
from geo_resolver import MPO_ALIASES


# ═══════════════════════════════════════════════════════════
#  FIX 1 — FC-5 statutory speed defaults
# ═══════════════════════════════════════════════════════════

def _mk(highway, mpo=""):
    """Build a 1-row df with the given OSM highway tag and MPO membership."""
    return pd.DataFrame({
        "highway":       [highway],
        "geo_mpo_name":  [mpo],
    })


def test_tertiary_in_mpo_defaults_to_35():
    v, s = resolve_speed_limit(_mk("tertiary", mpo="Dover / Kent County MPO"))
    assert v[0] == 35
    assert s[0] == "Statutory"


def test_tertiary_link_in_mpo_defaults_to_35():
    v, s = resolve_speed_limit(_mk("tertiary_link", mpo="WILMAPCO"))
    assert v[0] == 35
    assert s[0] == "Statutory"


def test_tertiary_rural_defaults_to_50():
    v, s = resolve_speed_limit(_mk("tertiary", mpo=""))
    assert v[0] == 50
    assert s[0] == "Statutory"


def test_tertiary_link_rural_defaults_to_50():
    v, s = resolve_speed_limit(_mk("tertiary_link", mpo=""))
    assert v[0] == 50
    assert s[0] == "Statutory"


# ── FC-7 locals: existing block must still work ───────────────────────────

@pytest.mark.parametrize("hw", ["residential", "unclassified", "living_street", "service"])
def test_fc7_locals_urban_still_25(hw):
    v, s = resolve_speed_limit(_mk(hw, mpo="WILMAPCO"))
    assert v[0] == 25, f"{hw} in-MPO should remain 25 (FC-7 statutory)"
    assert s[0] == "Statutory"


@pytest.mark.parametrize("hw", ["residential", "unclassified", "living_street", "service"])
def test_fc7_locals_rural_still_50(hw):
    v, s = resolve_speed_limit(_mk(hw, mpo=""))
    assert v[0] == 50, f"{hw} rural should remain 50 (FC-7 statutory)"
    assert s[0] == "Statutory"


# ── Other highway types are NOT touched by either statutory block ────────

@pytest.mark.parametrize("hw", ["motorway", "trunk", "primary", "secondary",
                                 "motorway_link", "primary_link", "secondary_link"])
def test_non_collector_non_local_not_filled(hw):
    """FC-1..FC-4 should be left at 0 when no other source provides speed."""
    v, s = resolve_speed_limit(_mk(hw, mpo="WILMAPCO"))
    assert v[0] == 0, f"{hw} should not get a statutory default"
    assert s[0] == ""


# ── Don't overwrite higher-authority sources ─────────────────────────────

def test_osm_maxspeed_preserved_on_tertiary():
    df = pd.DataFrame({
        "highway":      ["tertiary"],
        "maxspeed":     ["45"],
        "geo_mpo_name": [""],
    })
    v, s = resolve_speed_limit(df)
    assert v[0] == 45, "OSM maxspeed must beat Tier 5 Statutory"
    assert s[0] == "OSM"


def test_hpms_speed_preserved_on_tertiary():
    df = pd.DataFrame({
        "highway":          ["tertiary"],
        "hpms_speed_limit": [55],
        "geo_mpo_name":     [""],
    })
    v, s = resolve_speed_limit(df)
    assert v[0] == 55
    assert s[0] == "HPMS"


# ── Vectorized over a mixed batch (shape of the real fix) ────────────────

def test_batch_mixed_fc5_fc7_and_arterial():
    df = pd.DataFrame({
        "highway": [
            "tertiary",       # FC-5, in MPO   → 35 Statutory
            "tertiary_link",  # FC-5, rural    → 50 Statutory
            "residential",    # FC-7, in MPO   → 25 Statutory
            "unclassified",   # FC-7, rural    → 50 Statutory
            "secondary",      # FC-4, in MPO   → 0 (untouched)
            "motorway",       # FC-1, rural    → 0 (untouched)
        ],
        "geo_mpo_name": [
            "Dover / Kent County MPO",
            "",
            "WILMAPCO",
            "",
            "WILMAPCO",
            "",
        ],
    })
    v, s = resolve_speed_limit(df)
    assert list(v) == [35, 50, 25, 50, 0, 0]
    assert list(s) == ["Statutory", "Statutory", "Statutory", "Statutory", "", ""]


def test_statutory_does_not_re_fill_existing_values():
    """no_speed mask must exclude rows already set by higher tiers."""
    df = pd.DataFrame({
        "highway":      ["tertiary", "tertiary"],
        "maxspeed":     ["25", ""],          # row 0 has OSM, row 1 doesn't
        "geo_mpo_name": ["",         ""],    # both rural
    })
    v, s = resolve_speed_limit(df)
    assert (v[0], s[0]) == (25, "OSM")       # kept
    assert (v[1], s[1]) == (50, "Statutory") # filled by new block


# ═══════════════════════════════════════════════════════════
#  FIX 2 — MPO name alias normalization
# ═══════════════════════════════════════════════════════════

def test_mpo_aliases_contains_dover_kent_variant():
    assert "Dover/Kent County MPO" in MPO_ALIASES, (
        "MPO_ALIASES must map the hierarchy-tprs variant"
    )
    assert MPO_ALIASES["Dover/Kent County MPO"] == "Dover / Kent County MPO", (
        "Canonical form is the spaced variant (23,664 rows vs 2)"
    )


def test_mpo_alias_lookup_is_idempotent_on_canonical_form():
    """Canonical value must pass through unchanged."""
    canon = "Dover / Kent County MPO"
    assert MPO_ALIASES.get(canon, canon) == canon


def test_unknown_mpo_name_passes_through_unchanged():
    for name in ["WILMAPCO", "Sussex County", "", "Some Future MPO"]:
        assert MPO_ALIASES.get(name, name) == name, (
            f"Non-aliased MPO name {name!r} must pass through unchanged"
        )


def test_mpo_alias_direction_matches_majority_form():
    """Sanity: the RHS (canonical) is the spaced form, LHS is the compact form.

    This direction is deliberate — the spaced form came from us_mpos.json
    centroid match (23,664 rows), the compact form came from hierarchy.json
    tprs (2 rows). Flipping this would re-break the split.py R2 folder
    slugs that already use the spaced form.
    """
    assert "/" in "Dover/Kent County MPO"
    assert " / " in "Dover / Kent County MPO"
    # compact (no spaces around slash) → spaced (with spaces)
    for compact, spaced in MPO_ALIASES.items():
        if "Dover" in compact:
            assert "/" in compact and " / " not in compact
            assert " / " in spaced
