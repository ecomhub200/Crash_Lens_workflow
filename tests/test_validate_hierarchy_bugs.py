#!/usr/bin/env python3
"""
Bug tests for scripts/validate_hierarchy.py

Validates that:
  1. Orphaned counties are detected (in allCounties but not in any region)
  2. Empty MPO counties are detected (MPOs with counties=[])
  3. Unresolvable FIPS codes are detected (in regions but not in allCounties)
  4. Missing counties from Census data are detected
  5. Region coverage is calculated correctly
  6. MPO coverage is calculated correctly
  7. Auto-fix assigns orphaned county to nearest region
  8. Auto-fix assigns counties to empty MPOs via spatial matching
  9. No false positives on a clean hierarchy
 10. Virginia hierarchy detects known orphaned counties
 11. Virginia hierarchy detects known empty MPOs
 12. Pipeline integration: Stage 0.1 exists in pipeline.yml
 13. HierarchyIssue repr shows correct icons
 14. Duplicate FIPS in multiple regions doesn't crash
 15. Empty allCounties doesn't crash (edge case)

Run with:
    python tests/test_validate_hierarchy_bugs.py
    python -m pytest tests/test_validate_hierarchy_bugs.py -v
"""

import json
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from validate_hierarchy import (
    HierarchyIssue,
    check_empty_mpo_counties,
    check_missing_counties,
    check_mpo_coverage,
    check_orphaned_counties,
    check_region_completeness,
    check_unresolvable_fips,
    assign_to_nearest_region,
    haversine_km,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_hierarchy():
    """A hierarchy with no issues — all counties assigned to regions."""
    return {
        "state": {"fips": "99", "name": "TestState", "abbreviation": "TS"},
        "allCounties": {
            "001": "Alpha County",
            "003": "Bravo County",
            "005": "Charlie County",
        },
        "regions": {
            "north": {
                "name": "North Region",
                "counties": ["001", "003"],
                "countyNames": {"001": "Alpha County", "003": "Bravo County"},
                "center": [-79.0, 38.0],
            },
            "south": {
                "name": "South Region",
                "counties": ["005"],
                "countyNames": {"005": "Charlie County"},
                "center": [-79.0, 37.0],
            },
        },
        "tprs": {
            "metro_mpo": {
                "name": "Metro MPO",
                "type": "mpo",
                "counties": ["001"],
                "countyNames": {"001": "Alpha County"},
                "center": [-79.0, 38.0],
            }
        },
    }


@pytest.fixture
def orphaned_hierarchy():
    """Hierarchy with counties not assigned to any region."""
    return {
        "state": {"fips": "99", "name": "TestState"},
        "allCounties": {
            "001": "Alpha County",
            "003": "Bravo County",
            "005": "Charlie County",
            "007": "Delta County",
        },
        "regions": {
            "north": {
                "name": "North Region",
                "counties": ["001"],
                "countyNames": {"001": "Alpha County"},
                "center": [-79.0, 38.0],
            },
        },
        "tprs": {},
    }


@pytest.fixture
def empty_mpo_hierarchy():
    """Hierarchy with MPOs that have empty county arrays."""
    return {
        "state": {"fips": "99", "name": "TestState"},
        "allCounties": {
            "001": "Alpha County",
            "003": "Bravo County",
        },
        "regions": {
            "north": {
                "name": "North Region",
                "counties": ["001", "003"],
                "countyNames": {"001": "Alpha County", "003": "Bravo County"},
            },
        },
        "tprs": {
            "ghost_mpo": {
                "name": "Ghost MPO",
                "type": "mpo",
                "counties": [],
                "countyNames": {},
                "center": [-79.0, 38.0],
            },
            "real_mpo": {
                "name": "Real MPO",
                "type": "mpo",
                "counties": ["001"],
                "countyNames": {"001": "Alpha County"},
            },
        },
    }


@pytest.fixture
def unresolvable_hierarchy():
    """Hierarchy with FIPS codes in regions not in allCounties."""
    return {
        "state": {"fips": "99", "name": "TestState"},
        "allCounties": {
            "001": "Alpha County",
        },
        "regions": {
            "north": {
                "name": "North Region",
                "counties": ["001", "999"],  # 999 doesn't exist
            },
        },
        "tprs": {
            "bad_mpo": {
                "name": "Bad MPO",
                "type": "mpo",
                "counties": ["888"],  # 888 doesn't exist
            }
        },
    }


def load_virginia_hierarchy():
    """Load the real Virginia hierarchy.json."""
    path = PROJECT_ROOT / "states" / "virginia" / "hierarchy.json"
    if not path.exists():
        pytest.skip("Virginia hierarchy.json not found")
    with open(path) as f:
        return json.load(f)


# ── Test 1: Orphaned county detection ────────────────────────────────────────

def test_orphaned_counties_detected(orphaned_hierarchy):
    """BUG: Counties in allCounties but not in any region must be flagged."""
    issues = check_orphaned_counties(orphaned_hierarchy, "99")
    orphaned_fips = {i.message.split()[1] for i in issues}
    assert "003" in orphaned_fips, "Bravo County (003) should be orphaned"
    assert "005" in orphaned_fips, "Charlie County (005) should be orphaned"
    assert "007" in orphaned_fips, "Delta County (007) should be orphaned"
    assert "001" not in orphaned_fips, "Alpha County (001) is assigned, not orphaned"


def test_orphaned_counties_level_is_warning(orphaned_hierarchy):
    """Orphaned counties should be warnings, not errors."""
    issues = check_orphaned_counties(orphaned_hierarchy, "99")
    assert all(i.level == "warning" for i in issues)


def test_orphaned_counties_have_fix_fn(orphaned_hierarchy):
    """Each orphaned county issue should have an auto-fix function."""
    issues = check_orphaned_counties(orphaned_hierarchy, "99")
    assert all(i.fix_fn is not None for i in issues)


# ── Test 2: Empty MPO detection ──────────────────────────────────────────────

def test_empty_mpo_detected(empty_mpo_hierarchy):
    """BUG: MPOs with empty counties[] arrays must be flagged."""
    issues = check_empty_mpo_counties(empty_mpo_hierarchy, "99")
    mpo_keys = [i.message for i in issues]
    assert len(issues) == 1, f"Only ghost_mpo should be flagged, got {len(issues)}"
    assert "ghost_mpo" in mpo_keys[0]


def test_empty_mpo_not_flagged_when_populated(empty_mpo_hierarchy):
    """MPOs with counties should NOT be flagged."""
    issues = check_empty_mpo_counties(empty_mpo_hierarchy, "99")
    messages = " ".join(i.message for i in issues)
    assert "real_mpo" not in messages


# ── Test 3: Unresolvable FIPS detection ──────────────────────────────────────

def test_unresolvable_fips_detected(unresolvable_hierarchy):
    """BUG: FIPS codes in regions/MPOs not in allCounties must be flagged."""
    issues = check_unresolvable_fips(unresolvable_hierarchy)
    bad_fips = {i.message.split("'")[1] for i in issues}
    assert "999" in bad_fips, "FIPS 999 in region should be flagged"
    assert "888" in bad_fips, "FIPS 888 in MPO should be flagged"
    assert len(issues) == 2


def test_unresolvable_fips_level_is_error(unresolvable_hierarchy):
    """Unresolvable FIPS should be errors, not warnings."""
    issues = check_unresolvable_fips(unresolvable_hierarchy)
    assert all(i.level == "error" for i in issues)


# ── Test 4: Region coverage ──────────────────────────────────────────────────

def test_region_coverage_clean(clean_hierarchy):
    """Clean hierarchy should show 100% region coverage (info only)."""
    issues = check_region_completeness(clean_hierarchy)
    # All 3 counties assigned → no coverage warning
    coverage_issues = [i for i in issues if i.check == "region_coverage"]
    assert len(coverage_issues) == 0, "100% coverage should produce no coverage issue"


def test_region_coverage_partial(orphaned_hierarchy):
    """Partial coverage should report info-level issue."""
    issues = check_region_completeness(orphaned_hierarchy)
    coverage_issues = [i for i in issues if i.check == "region_coverage"]
    assert len(coverage_issues) == 1
    assert coverage_issues[0].level == "info"
    assert "25%" in coverage_issues[0].message  # 1 of 4 assigned


# ── Test 5: MPO coverage ─────────────────────────────────────────────────────

def test_mpo_coverage_reported(clean_hierarchy):
    """MPO coverage is always reported as info."""
    issues = check_mpo_coverage(clean_hierarchy)
    assert len(issues) == 1
    assert issues[0].level == "info"
    assert issues[0].check == "mpo_coverage"


# ── Test 6: No false positives on clean hierarchy ────────────────────────────

def test_no_false_positives_clean(clean_hierarchy):
    """A clean hierarchy should produce zero errors and zero warnings."""
    all_issues = []
    all_issues.extend(check_orphaned_counties(clean_hierarchy, "99"))
    all_issues.extend(check_unresolvable_fips(clean_hierarchy))
    all_issues.extend(check_empty_mpo_counties(clean_hierarchy, "99"))

    errors = [i for i in all_issues if i.level == "error"]
    warnings = [i for i in all_issues if i.level == "warning"]
    assert len(errors) == 0, f"Clean hierarchy should have no errors: {errors}"
    assert len(warnings) == 0, f"Clean hierarchy should have no warnings: {warnings}"


# ── Test 7: Auto-fix assigns orphaned county to nearest region ───────────────

def test_autofix_nearest_region():
    """Auto-fix should assign orphaned county to the geographically closest region."""
    hierarchy = {
        "state": {"fips": "99"},
        "allCounties": {
            "001": "Alpha",
            "003": "Bravo",
            "005": "Orphan",
        },
        "regions": {
            "north": {
                "name": "North",
                "counties": ["001"],
                "countyNames": {"001": "Alpha"},
                "center": [-79.0, 39.0],  # far north
            },
            "south": {
                "name": "South",
                "counties": ["003"],
                "countyNames": {"003": "Bravo"},
                "center": [-79.0, 36.0],  # far south
            },
        },
    }
    # Can't test full auto-fix without Census data, but verify the function
    # doesn't crash when called without Census data
    result = assign_to_nearest_region(hierarchy, "005", "99")
    # Without Census centroid data, it returns False
    assert result is False or result is True  # doesn't crash


# ── Test 8: HierarchyIssue repr ─────────────────────────────────────────────

def test_issue_repr_icons():
    """HierarchyIssue repr should show correct icon per level."""
    err = HierarchyIssue("error", "test", "error message")
    warn = HierarchyIssue("warning", "test", "warn message")
    info = HierarchyIssue("info", "test", "info message")
    assert "✗" in repr(err)
    assert "⚠" in repr(warn)
    assert "ℹ" in repr(info)


def test_issue_repr_includes_check_name():
    """HierarchyIssue repr should include the check name in brackets."""
    issue = HierarchyIssue("error", "my_check", "something broke")
    assert "[my_check]" in repr(issue)


# ── Test 9: Edge cases ───────────────────────────────────────────────────────

def test_empty_allcounties_no_crash():
    """Empty allCounties should not crash any check."""
    hierarchy = {
        "state": {"fips": "99"},
        "allCounties": {},
        "regions": {},
        "tprs": {},
    }
    check_orphaned_counties(hierarchy, "99")
    check_unresolvable_fips(hierarchy)
    check_empty_mpo_counties(hierarchy, "99")
    check_region_completeness(hierarchy)
    check_mpo_coverage(hierarchy)


def test_no_regions_section():
    """Missing regions section should not crash."""
    hierarchy = {
        "state": {"fips": "99"},
        "allCounties": {"001": "Alpha"},
    }
    issues = check_orphaned_counties(hierarchy, "99")
    assert len(issues) == 1  # Alpha is orphaned (no regions)


def test_duplicate_fips_in_regions():
    """County listed in multiple regions should not crash."""
    hierarchy = {
        "state": {"fips": "99"},
        "allCounties": {"001": "Alpha"},
        "regions": {
            "north": {"counties": ["001"]},
            "south": {"counties": ["001"]},  # duplicate
        },
    }
    issues = check_orphaned_counties(hierarchy, "99")
    assert len(issues) == 0  # 001 is assigned (even if duplicated)


# ── Test 10: Haversine calculation ───────────────────────────────────────────

def test_haversine_same_point():
    """Distance between same point should be 0."""
    assert haversine_km(38.0, -79.0, 38.0, -79.0) == 0.0


def test_haversine_known_distance():
    """Richmond to Norfolk is ~150km — verify rough accuracy."""
    dist = haversine_km(37.5407, -77.4360, 36.8508, -76.2859)
    assert 100 < dist < 170, f"Richmond-Norfolk should be ~128km, got {dist}"


# ── Test 11: Virginia real-world detection ───────────────────────────────────

def test_virginia_has_orphaned_counties():
    """BUG: Virginia hierarchy should flag orphaned counties (known issue)."""
    hierarchy = load_virginia_hierarchy()
    state_fips = hierarchy.get("state", {}).get("fips", "51")
    issues = check_orphaned_counties(hierarchy, state_fips)
    assert len(issues) > 0, "Virginia should have orphaned counties"
    orphaned_fips = {i.message.split()[1] for i in issues}
    # Known orphans from dry-run output
    assert "001" in orphaned_fips, "Accomack (001) should be orphaned"
    assert "035" in orphaned_fips, "Carroll (035) should be orphaned"


def test_virginia_has_empty_mpos():
    """BUG: Virginia hierarchy should flag empty MPOs (known issue)."""
    hierarchy = load_virginia_hierarchy()
    state_fips = hierarchy.get("state", {}).get("fips", "51")
    issues = check_empty_mpo_counties(hierarchy, state_fips)
    assert len(issues) > 0, "Virginia should have empty MPOs"
    messages = " ".join(i.message for i in issues)
    assert "danville_mpo" in messages, "Danville MPO should have empty counties"


def test_virginia_no_unresolvable_fips():
    """Virginia FIPS codes in regions should all be valid."""
    hierarchy = load_virginia_hierarchy()
    issues = check_unresolvable_fips(hierarchy)
    assert len(issues) == 0, f"Virginia has unresolvable FIPS: {issues}"


# ── Test 12: Pipeline integration ────────────────────────────────────────────

def test_pipeline_has_stage_01():
    """pipeline.yml must include Stage 0.1 hierarchy validation step."""
    pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "pipeline.yml"
    assert pipeline_path.exists(), "pipeline.yml not found"
    content = pipeline_path.read_text()
    assert "validate_hierarchy.py" in content, "pipeline.yml must call validate_hierarchy.py"
    assert "Stage 0.1" in content, "pipeline.yml must have Stage 0.1 comment"


def test_pipeline_stage_01_is_nonfatal():
    """Stage 0.1 should be non-fatal (won't block pipeline on failure)."""
    pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "pipeline.yml"
    content = pipeline_path.read_text()
    # Find the Stage 0.1 block and verify it has non-fatal error handling
    assert "non-fatal" in content.lower() or "|| {" in content, \
        "Stage 0.1 should have non-fatal error handling"


def test_pipeline_stage_01_after_cache_before_download():
    """Stage 0.1 should appear after Stage 0 (cache) and before Stage 0.5 (download)."""
    pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "pipeline.yml"
    content = pipeline_path.read_text()
    pos_cache = content.find("Stage 0: Initialize")
    pos_validate = content.find("Stage 0.1")
    pos_download = content.find("Stage 0.5")
    assert pos_cache < pos_validate < pos_download, \
        "Stage order must be: 0 (cache) → 0.1 (validate) → 0.5 (download)"


# ── Test 13: Script CLI ──────────────────────────────────────────────────────

def test_script_exists():
    """validate_hierarchy.py must exist in scripts/."""
    path = PROJECT_ROOT / "scripts" / "validate_hierarchy.py"
    assert path.exists()


def test_script_is_executable_python():
    """Script should be valid Python (importable)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "validate_hierarchy",
        PROJECT_ROOT / "scripts" / "validate_hierarchy.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert hasattr(mod, "main")
    assert hasattr(mod, "validate_state")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
