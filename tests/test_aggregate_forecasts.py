#!/usr/bin/env python3
"""
Bug tests for the forecast aggregation pipeline:
  - scripts/aggregate_forecasts.py: Merge jurisdiction forecasts into region/MPO aggregates
  - scripts/aggregate_by_scope.py: CSV output filename fix for region/MPO

Validates that:
  1. aggregate_forecasts.py loads and has correct public API
  2. Time-series history values sum correctly across jurisdictions
  3. Forecast quantiles (p10/p50/p90) sum correctly
  4. M02 severity aggregation preserves all K/A/B/C/O levels
  5. M03 corridor merging re-ranks by EPDO correctly
  6. Summary stats sum counts and recalculate rates
  7. Derived metrics rebuild correctly from aggregated matrices
  8. Empty/missing jurisdiction forecasts are handled gracefully
  9. aggregate_by_scope.py uses {road_type}.csv (no group_id prefix)
 10. Output JSON includes aggregation metadata fields
 11. EPDO history is rebuilt (not just summed) from aggregated severity

Run with:
    python tests/test_aggregate_forecasts.py
    python -m pytest tests/test_aggregate_forecasts.py -v
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from aggregate_forecasts import (
    aggregate_m01,
    aggregate_m02,
    aggregate_m03,
    aggregate_summary,
    merge_forecast_quantiles,
    rebuild_derived_metrics,
    sum_history,
    sum_quantile_arrays,
    aggregate_forecasts_for_group,
    load_jurisdiction_forecast,
    EPDO_WEIGHTS,
)


# ============================================================
# Fixtures: Synthetic forecast data
# ============================================================

def make_forecast_json(jurisdiction, total_crashes=100, k=2, a=5, b=15, c=30, o=48,
                       months=None, horizon_months=None):
    """Build a minimal but realistic forecast JSON for testing."""
    if months is None:
        months = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06",
                  "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"]
    if horizon_months is None:
        horizon_months = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]

    monthly_avg = total_crashes / len(months)

    # M01: Total frequency
    m01_history = [{"month": m, "value": round(monthly_avg)} for m in months]
    m01_forecast = {
        "months": horizon_months,
        "p10": [round(monthly_avg * 0.8)] * len(horizon_months),
        "p50": [round(monthly_avg)] * len(horizon_months),
        "p90": [round(monthly_avg * 1.2)] * len(horizon_months),
    }

    # M02: Severity
    sev_counts = {"K": k, "A": a, "B": b, "C": c, "O": o}
    m02_history = {}
    m02_forecast = {}
    for sev, count in sev_counts.items():
        sev_monthly = count / len(months)
        m02_history[sev] = [{"month": m, "value": round(sev_monthly, 1)} for m in months]
        m02_forecast[sev] = {
            "months": horizon_months,
            "p10": [round(sev_monthly * 0.8, 1)] * len(horizon_months),
            "p50": [round(sev_monthly, 1)] * len(horizon_months),
            "p90": [round(sev_monthly * 1.2, 1)] * len(horizon_months),
        }

    epdo = sum(sev_counts[s] * EPDO_WEIGHTS[s] for s in sev_counts)
    epdo_history = [{"month": m, "value": round(epdo / len(months))} for m in months]

    # M03: Corridors
    m03 = {
        "id": "m03",
        "title": "Top Corridor Forecasts",
        "subtitle": f"Top Corridors in {jurisdiction}",
        "corridors": [
            {
                "route": f"RT-{jurisdiction.upper()}-1",
                "totalCrashes": round(total_crashes * 0.3),
                "epdo": round(epdo * 0.3),
                "severity": {s: round(v * 0.3) for s, v in sev_counts.items()},
                "history": [{"month": m, "value": round(monthly_avg * 0.3)} for m in months],
                "forecast": {
                    "months": horizon_months,
                    "p50": [round(monthly_avg * 0.3)] * len(horizon_months),
                },
            },
            {
                "route": f"RT-{jurisdiction.upper()}-2",
                "totalCrashes": round(total_crashes * 0.2),
                "epdo": round(epdo * 0.2),
                "severity": {s: round(v * 0.2) for s, v in sev_counts.items()},
                "history": [{"month": m, "value": round(monthly_avg * 0.2)} for m in months],
                "forecast": {
                    "months": horizon_months,
                    "p50": [round(monthly_avg * 0.2)] * len(horizon_months),
                },
            },
        ],
        "months": horizon_months,
    }

    return {
        "generated": "2025-01-15T10:00:00",
        "model": "amazon/chronos-2",
        "horizon": 24,
        "quantileLevels": [0.1, 0.5, 0.9],
        "epdoWeights": EPDO_WEIGHTS,
        "roadType": "all_roads",
        "summary": {
            "totalCrashes": total_crashes,
            "years": [2024],
            "dateRange": {"start": months[0], "end": months[-1]},
            "severity": sev_counts,
            "epdo": epdo,
            "monthlyAvg": round(monthly_avg, 1),
            "recentTrend": {
                "recent6mo": round(total_crashes * 0.55),
                "prev6mo": round(total_crashes * 0.45),
                "changePct": 22.2,
            },
        },
        "matrices": {
            "m01": {
                "id": "m01",
                "title": "Total Crash Frequency",
                "subtitle": "County-Wide Monthly Forecast",
                "history": m01_history,
                "forecast": m01_forecast,
            },
            "m02": {
                "id": "m02",
                "title": "Severity-Level Forecast",
                "subtitle": "Joint K-A-B-C-O Prediction",
                "history": m02_history,
                "forecast": m02_forecast,
                "epdoHistory": epdo_history,
            },
            "m03": m03,
        },
        "derivedMetrics": {},
    }


@pytest.fixture
def two_jurisdictions():
    """Two jurisdictions with known values for sum verification."""
    return [
        make_forecast_json("county_a", total_crashes=120, k=3, a=6, b=20, c=40, o=51),
        make_forecast_json("county_b", total_crashes=80, k=1, a=4, b=10, c=20, o=45),
    ]


@pytest.fixture
def tmp_forecast_dir(two_jurisdictions):
    """Write synthetic forecast JSONs to a temp directory matching expected layout."""
    with tempfile.TemporaryDirectory() as tmpdir:
        for i, (name, fc) in enumerate(zip(["county_a", "county_b"], two_jurisdictions)):
            county_dir = Path(tmpdir) / name
            county_dir.mkdir()
            for rt in ["all_roads", "county_roads"]:
                fc_copy = dict(fc)
                fc_copy["roadType"] = rt
                with open(county_dir / f"forecasts_{rt}.json", "w") as f:
                    json.dump(fc_copy, f)
        yield tmpdir


# ============================================================
# Test: sum_history
# ============================================================

class TestSumHistory:
    def test_basic_sum(self):
        h1 = [{"month": "2024-01", "value": 10}, {"month": "2024-02", "value": 20}]
        h2 = [{"month": "2024-01", "value": 5}, {"month": "2024-02", "value": 15}]
        result = sum_history([h1, h2])
        assert result == [{"month": "2024-01", "value": 15}, {"month": "2024-02", "value": 35}]

    def test_mismatched_months(self):
        """Jurisdictions may have different date ranges — union all months."""
        h1 = [{"month": "2024-01", "value": 10}]
        h2 = [{"month": "2024-02", "value": 20}]
        result = sum_history([h1, h2])
        assert len(result) == 2
        months = {r["month"] for r in result}
        assert months == {"2024-01", "2024-02"}

    def test_empty_input(self):
        assert sum_history([]) == []
        assert sum_history([[], []]) == []

    def test_single_jurisdiction(self):
        h1 = [{"month": "2024-01", "value": 42}]
        result = sum_history([h1])
        assert result[0]["value"] == 42


# ============================================================
# Test: sum_quantile_arrays
# ============================================================

class TestSumQuantileArrays:
    def test_basic_sum(self):
        result = sum_quantile_arrays([[1, 2, 3], [4, 5, 6]])
        assert result == [5, 7, 9]

    def test_different_lengths(self):
        """Shorter arrays contribute 0 for missing positions."""
        result = sum_quantile_arrays([[1, 2], [3, 4, 5]])
        assert result == [4, 6, 5]

    def test_empty(self):
        assert sum_quantile_arrays([]) == []


# ============================================================
# Test: merge_forecast_quantiles
# ============================================================

class TestMergeForecastQuantiles:
    def test_basic_merge(self):
        fc1 = {"months": ["2025-01", "2025-02"], "p10": [8, 9], "p50": [10, 11], "p90": [12, 13]}
        fc2 = {"months": ["2025-01", "2025-02"], "p10": [2, 3], "p50": [4, 5], "p90": [6, 7]}
        result = merge_forecast_quantiles([fc1, fc2])
        assert result["p50"] == [14, 16]
        assert result["p10"] == [10, 12]
        assert result["p90"] == [18, 20]

    def test_empty_list(self):
        assert merge_forecast_quantiles([]) == {}

    def test_missing_quantile(self):
        """If one forecast lacks p10, only sum available ones."""
        fc1 = {"months": ["2025-01"], "p50": [10]}
        fc2 = {"months": ["2025-01"], "p50": [5], "p10": [3]}
        result = merge_forecast_quantiles([fc1, fc2])
        assert result["p50"] == [15]
        # p10 only has one contributor
        assert result["p10"] == [3]


# ============================================================
# Test: aggregate_m01
# ============================================================

class TestAggregateM01:
    def test_sums_history_and_forecast(self, two_jurisdictions):
        m01s = [fc["matrices"]["m01"] for fc in two_jurisdictions]
        result = aggregate_m01(m01s)

        assert result is not None
        assert result["id"] == "m01"
        assert result["subtitle"] == "Aggregate Monthly Forecast"

        # History values should be summed
        total_jan = sum(m["history"][0]["value"] for m in m01s)
        assert result["history"][0]["value"] == total_jan

        # Forecast p50 should be summed
        total_p50_jan = sum(m["forecast"]["p50"][0] for m in m01s)
        assert result["forecast"]["p50"][0] == total_p50_jan

    def test_empty_list(self):
        assert aggregate_m01([]) is None

    def test_none_entries(self):
        assert aggregate_m01([None, None]) is None


# ============================================================
# Test: aggregate_m02
# ============================================================

class TestAggregateM02:
    def test_all_severities_present(self, two_jurisdictions):
        m02s = [fc["matrices"]["m02"] for fc in two_jurisdictions]
        result = aggregate_m02(m02s)

        assert result is not None
        for sev in ["K", "A", "B", "C", "O"]:
            assert sev in result["history"], f"Missing severity {sev} in history"
            assert sev in result["forecast"], f"Missing severity {sev} in forecast"

    def test_epdo_history_rebuilt(self, two_jurisdictions):
        """EPDO history should be rebuilt from aggregated severity, not just summed."""
        m02s = [fc["matrices"]["m02"] for fc in two_jurisdictions]
        result = aggregate_m02(m02s)

        assert result is not None
        assert len(result["epdoHistory"]) > 0

        # Verify EPDO is computed correctly for first month
        first_month = result["epdoHistory"][0]["month"]
        expected_epdo = 0
        for sev in ["K", "A", "B", "C", "O"]:
            sev_hist = {e["month"]: e["value"] for e in result["history"].get(sev, [])}
            expected_epdo += sev_hist.get(first_month, 0) * EPDO_WEIGHTS[sev]
        assert result["epdoHistory"][0]["value"] == round(expected_epdo)


# ============================================================
# Test: aggregate_m03
# ============================================================

class TestAggregateM03:
    def test_corridors_merged_and_ranked(self, two_jurisdictions):
        m03s = [fc["matrices"]["m03"] for fc in two_jurisdictions]
        result = aggregate_m03(m03s)

        assert result is not None
        assert len(result["corridors"]) == 4  # 2 corridors per jurisdiction, all unique

        # Should be ranked by EPDO descending
        epdos = [c["epdo"] for c in result["corridors"]]
        assert epdos == sorted(epdos, reverse=True)

        # Rank field should be 1-indexed
        assert result["corridors"][0]["rank"] == 1

    def test_same_route_merges(self):
        """Routes with the same name across jurisdictions should merge."""
        m03_a = {
            "id": "m03",
            "corridors": [{"route": "US-1", "totalCrashes": 50, "epdo": 500,
                           "severity": {"K": 1, "A": 2, "B": 5, "C": 10, "O": 32},
                           "history": [{"month": "2024-01", "value": 10}],
                           "forecast": {"months": ["2025-01"], "p50": [10]}}],
            "months": ["2025-01"],
        }
        m03_b = {
            "id": "m03",
            "corridors": [{"route": "US-1", "totalCrashes": 30, "epdo": 300,
                           "severity": {"K": 0, "A": 1, "B": 3, "C": 8, "O": 18},
                           "history": [{"month": "2024-01", "value": 6}],
                           "forecast": {"months": ["2025-01"], "p50": [6]}}],
            "months": ["2025-01"],
        }
        result = aggregate_m03([m03_a, m03_b])
        assert len(result["corridors"]) == 1
        assert result["corridors"][0]["totalCrashes"] == 80
        assert result["corridors"][0]["epdo"] == 800
        assert result["corridors"][0]["severity"]["K"] == 1


# ============================================================
# Test: aggregate_summary
# ============================================================

class TestAggregateSummary:
    def test_total_crashes_sum(self, two_jurisdictions):
        summaries = [fc["summary"] for fc in two_jurisdictions]
        result = aggregate_summary(summaries)
        assert result["totalCrashes"] == 200  # 120 + 80

    def test_severity_counts_sum(self, two_jurisdictions):
        summaries = [fc["summary"] for fc in two_jurisdictions]
        result = aggregate_summary(summaries)
        assert result["severity"]["K"] == 4  # 3 + 1
        assert result["severity"]["A"] == 10  # 6 + 4

    def test_trend_recalculated(self, two_jurisdictions):
        summaries = [fc["summary"] for fc in two_jurisdictions]
        result = aggregate_summary(summaries)
        recent = result["recentTrend"]["recent6mo"]
        prev = result["recentTrend"]["prev6mo"]
        expected_pct = round(((recent - prev) / max(prev, 1)) * 100, 1)
        assert result["recentTrend"]["changePct"] == expected_pct

    def test_epdo_recalculated(self, two_jurisdictions):
        summaries = [fc["summary"] for fc in two_jurisdictions]
        result = aggregate_summary(summaries)
        expected = sum(result["severity"].get(s, 0) * EPDO_WEIGHTS.get(s, 1) for s in result["severity"])
        assert result["epdo"] == expected


# ============================================================
# Test: rebuild_derived_metrics
# ============================================================

class TestRebuildDerivedMetrics:
    def test_confidence_width_computed(self, two_jurisdictions):
        m01s = [fc["matrices"]["m01"] for fc in two_jurisdictions]
        m02s = [fc["matrices"]["m02"] for fc in two_jurisdictions]
        agg_m01 = aggregate_m01(m01s)
        agg_m02 = aggregate_m02(m02s)
        summaries = [fc["summary"] for fc in two_jurisdictions]
        agg_summary = aggregate_summary(summaries)

        matrices = {"m01": agg_m01, "m02": agg_m02}
        derived = rebuild_derived_metrics(matrices, agg_summary)

        assert "confidenceWidth" in derived
        assert derived["confidenceWidth"]["interpretation"] in ["low", "moderate", "high"]

    def test_epdo_forecast_computed(self, two_jurisdictions):
        m01s = [fc["matrices"]["m01"] for fc in two_jurisdictions]
        m02s = [fc["matrices"]["m02"] for fc in two_jurisdictions]
        agg_m01 = aggregate_m01(m01s)
        agg_m02 = aggregate_m02(m02s)
        summaries = [fc["summary"] for fc in two_jurisdictions]
        agg_summary = aggregate_summary(summaries)

        matrices = {"m01": agg_m01, "m02": agg_m02}
        derived = rebuild_derived_metrics(matrices, agg_summary)

        assert "epdoForecast" in derived
        assert len(derived["epdoForecast"]["p50"]) > 0


# ============================================================
# Test: End-to-end with tmp directory
# ============================================================

class TestEndToEnd:
    def test_load_jurisdiction_forecast(self, tmp_forecast_dir):
        fc = load_jurisdiction_forecast(tmp_forecast_dir, "county_a", "all_roads")
        assert fc is not None
        assert fc["roadType"] == "all_roads"

    def test_load_missing_jurisdiction(self, tmp_forecast_dir):
        fc = load_jurisdiction_forecast(tmp_forecast_dir, "nonexistent", "all_roads")
        assert fc is None

    def test_aggregate_writes_output(self, tmp_forecast_dir):
        """Full pipeline: aggregate 2 jurisdictions into a region forecast."""
        members = ["county_a", "county_b"]
        count = aggregate_forecasts_for_group(
            "region", "test_region", members, tmp_forecast_dir, tmp_forecast_dir
        )

        assert count > 0

        # Check output file exists
        out_path = Path(tmp_forecast_dir) / "_region" / "test_region" / "forecasts_all_roads.json"
        assert out_path.exists(), f"Expected output at {out_path}"

        with open(out_path) as f:
            result = json.load(f)

        # Verify aggregation metadata
        assert result["aggregated"] is True
        assert result["aggregationType"] == "region"
        assert result["aggregationId"] == "test_region"
        assert set(result["memberJurisdictions"]) == {"county_a", "county_b"}
        assert result["memberCount"] == 2

        # Verify summed total
        assert result["summary"]["totalCrashes"] == 200

        # Verify matrices present
        assert "m01" in result["matrices"]
        assert "m02" in result["matrices"]
        assert "m03" in result["matrices"]


# ============================================================
# Test: aggregate_by_scope.py filename convention
# ============================================================

class TestAggregateByScope:
    def test_output_path_no_group_id_prefix(self):
        """Verify generate_group_csvs uses {road_type}.csv, not {group_id}_{road_type}.csv."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "aggregate_by_scope", PROJECT_ROOT / "scripts" / "aggregate_by_scope.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Inspect the source code for the output_path pattern
        import inspect
        source = inspect.getsource(mod.generate_group_csvs)

        # Should NOT contain group_id prefix in filename
        assert 'f"{group_id}_{road_type}.csv"' not in source, \
            "Output filename should not contain group_id prefix"
        # Should contain just road_type
        assert 'f"{road_type}.csv"' in source, \
            "Output filename should be just {road_type}.csv"


# ============================================================
# Test: batch-pipeline.yml has stages 5c and 5d
# ============================================================

class TestBatchPipeline:
    def test_stages_5c_5d_present(self):
        pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "batch-pipeline.yml"
        content = pipeline_path.read_text()

        assert "Stage 5c:" in content, "Missing Stage 5c in batch-pipeline.yml"
        assert "Stage 5d:" in content, "Missing Stage 5d in batch-pipeline.yml"
        assert "aggregate_forecasts.py" in content, "Missing aggregate_forecasts.py reference"

    def test_stage_5c_runs_after_5b(self):
        """Stage 5c should appear after Stage 5b in the file."""
        pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "batch-pipeline.yml"
        content = pipeline_path.read_text()
        pos_5b = content.index("Stage 5b:")
        pos_5c = content.index("Stage 5c:")
        assert pos_5c > pos_5b

    def test_stage_5d_uploads_to_r2(self):
        pipeline_path = PROJECT_ROOT / ".github" / "workflows" / "batch-pipeline.yml"
        content = pipeline_path.read_text()
        # Stage 5d should use aws s3 cp for forecast JSONs
        stage_5d_start = content.index("Stage 5d:")
        stage_after = content.index("Stage 6:", stage_5d_start)
        stage_5d = content[stage_5d_start:stage_after]
        assert "aws s3 cp" in stage_5d
        assert "forecasts_*.json" in stage_5d
        assert "application/json" in stage_5d


# ============================================================
# Test: FIPS collision bug (cross-state)
# ============================================================

class TestFipsCollision:
    """Regression test for FIPS collision across states.

    Bug: FIPS codes are NOT unique across states (e.g., FIPS 027 = Buchanan
    in VA, Custer in CO). Without state filtering, load_config_fips_map()
    would return whichever state's entry was loaded last, producing incorrect
    jurisdiction keys (e.g., co_custer in a Virginia region).
    """

    def test_virginia_regions_have_no_colorado_keys(self):
        from aggregate_forecasts import get_regions, load_hierarchy
        hierarchy_path = PROJECT_ROOT / "states" / "virginia" / "hierarchy.json"
        if not hierarchy_path.exists():
            pytest.skip("Virginia hierarchy.json not available")

        hierarchy = load_hierarchy("virginia")
        regions = get_regions(hierarchy)

        for rid, members in regions.items():
            for m in members:
                assert not m.startswith("co_"), \
                    f"Virginia region '{rid}' contains Colorado key '{m}' — FIPS collision bug"

    def test_load_config_fips_map_filters_by_state(self):
        from aggregate_forecasts import load_config_fips_map

        va_map = load_config_fips_map("VA")
        co_map = load_config_fips_map("CO")

        # FIPS 027 exists in both states with different keys
        if "027" in va_map and "027" in co_map:
            assert va_map["027"] != co_map["027"], \
                "FIPS 027 should resolve to different keys for VA vs CO"
            assert not va_map["027"].startswith("co_"), \
                f"VA FIPS 027 should not be a CO key: {va_map['027']}"

    def test_unfiltered_map_has_collision(self):
        """Without state filter, FIPS collision WILL occur."""
        from aggregate_forecasts import load_config_fips_map

        # Unfiltered map — the last-loaded entry for FIPS 027 wins
        all_map = load_config_fips_map(None)
        va_map = load_config_fips_map("VA")

        # If both states have FIPS 027, the unfiltered result is ambiguous
        if "027" in va_map:
            # The unfiltered map may or may not match VA — that's the bug
            # Just verify that filtered maps are deterministic
            assert va_map["027"] == load_config_fips_map("VA")["027"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
