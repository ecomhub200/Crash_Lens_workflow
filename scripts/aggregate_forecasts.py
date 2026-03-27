#!/usr/bin/env python3
"""
Aggregate jurisdiction-level forecast JSONs into region/MPO forecasts.

Instead of re-running SageMaker (expensive), this script mathematically merges
existing per-jurisdiction forecast JSONs into region and MPO aggregate forecasts.

Aggregation logic:
  - Time-series values (history, forecast p10/p50/p90): SUM across jurisdictions
  - Percentages/rates: WEIGHTED AVERAGE by total crashes
  - Corridors (M03): MERGE + RE-RANK by EPDO across all member jurisdictions
  - Derived metrics: RE-COMPUTE from aggregated matrices
  - Summary stats: SUM counts, recalculate rates

Usage:
    # All regions + MPOs for a state
    python scripts/aggregate_forecasts.py --state virginia --data-dir data

    # Single region
    python scripts/aggregate_forecasts.py --state virginia --scope region --selection richmond --data-dir data

    # Single MPO
    python scripts/aggregate_forecasts.py --state colorado --scope mpo --selection drcog --data-dir data/CDOT
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger('aggregate_forecasts')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent

ROAD_TYPES = ['county_roads', 'city_roads', 'no_interstate', 'all_roads']

# FHWA 2025 EPDO weights (must match generate_forecast.py)
EPDO_WEIGHTS = {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}


def load_hierarchy(state):
    path = PROJECT_ROOT / 'states' / state / 'hierarchy.json'
    if not path.exists():
        logger.error(f"No hierarchy.json for '{state}' at {path}")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def load_config_fips_map(state_abbrev=None):
    """Build FIPS → config.json jurisdiction key mapping, filtered by state.

    Args:
        state_abbrev: State abbreviation (e.g., 'VA', 'CO') to filter
            jurisdictions. Prevents FIPS collisions across states.
    """
    config_path = PROJECT_ROOT / 'config.json'
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        config = json.load(f)
    fips_to_key = {}
    for jid, jinfo in config.get('jurisdictions', {}).items():
        if isinstance(jinfo, dict) and 'fips' in jinfo:
            if state_abbrev and jinfo.get('state', '').upper() != state_abbrev.upper():
                continue
            fips_to_key[jinfo['fips']] = jid
    return fips_to_key


def build_fips_to_key_map(hierarchy):
    state_abbrev = hierarchy.get('state', {}).get('abbreviation')
    config_fips_map = load_config_fips_map(state_abbrev)
    all_counties = hierarchy.get('allCounties', {})
    fips_to_key = {}
    for fips, name in all_counties.items():
        if fips in config_fips_map:
            fips_to_key[fips] = config_fips_map[fips]
        elif isinstance(name, str):
            key = name.lower().strip().replace("'", "").replace(".", "")
            import re
            key = re.sub(r'[^a-z0-9]+', '_', key).strip('_')
            fips_to_key[fips] = key
        elif isinstance(name, dict):
            display = name.get('name', name.get('displayName', fips))
            key = display.lower().strip().replace("'", "").replace(".", "")
            import re
            key = re.sub(r'[^a-z0-9]+', '_', key).strip('_')
            fips_to_key[fips] = key
    return fips_to_key


def get_regions(hierarchy):
    fips_to_key = build_fips_to_key_map(hierarchy)
    regions = hierarchy.get('regions', {})
    result = {}
    for rid, rdata in regions.items():
        fips_codes = rdata.get('counties', [])
        result[rid] = [fips_to_key[c] for c in fips_codes if c in fips_to_key]
    return result


def get_mpos(hierarchy):
    fips_to_key = build_fips_to_key_map(hierarchy)
    tprs = hierarchy.get('tprs', {})
    mpos = hierarchy.get('mpos', {})
    result = {}
    for key, val in tprs.items():
        if val.get('type') == 'mpo':
            fips_codes = val.get('counties', [])
            result[key] = [fips_to_key[c] for c in fips_codes if c in fips_to_key]
    for key, val in mpos.items():
        fips_codes = val.get('counties', [])
        result[key] = [fips_to_key[c] for c in fips_codes if c in fips_to_key]
    return result


def load_jurisdiction_forecast(data_dir, jurisdiction, road_type):
    """Load a jurisdiction forecast JSON, trying multiple path patterns."""
    candidates = [
        Path(data_dir) / jurisdiction / f"forecasts_{road_type}.json",
        Path(data_dir) / f"forecasts_{road_type}.json",
    ]
    for path in candidates:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    return None


def sum_history(histories):
    """Sum history arrays from multiple forecasts.

    Each history is a list of {month, value} dicts. Aligns by month and sums.
    """
    if not histories:
        return []

    by_month = {}
    month_order = []
    for hist in histories:
        if not hist:
            continue
        for entry in hist:
            m = entry["month"]
            if m not in by_month:
                by_month[m] = 0
                month_order.append(m)
            by_month[m] += entry["value"]

    month_order = sorted(set(month_order))
    return [{"month": m, "value": by_month.get(m, 0)} for m in month_order]


def sum_quantile_arrays(arrays):
    """Sum lists of numbers element-wise. All arrays must have same length."""
    if not arrays:
        return []
    max_len = max(len(a) for a in arrays)
    result = [0] * max_len
    for arr in arrays:
        for i, v in enumerate(arr):
            result[i] += v
    return [round(v, 2) for v in result]


def merge_forecast_quantiles(forecasts_list):
    """Merge forecast dicts with p10/p50/p90/months keys by summing."""
    if not forecasts_list:
        return {}

    # Get months from first non-empty forecast
    months = None
    for fc in forecasts_list:
        if fc and fc.get('months'):
            months = fc['months']
            break
    if not months:
        return {}

    result = {"months": months}
    for q in ['p10', 'p50', 'p90']:
        arrays = [fc.get(q, []) for fc in forecasts_list if fc and fc.get(q)]
        if arrays:
            result[q] = sum_quantile_arrays(arrays)
    return result


def aggregate_m01(m01_list):
    """Aggregate M01 (Total Crash Frequency) by summing time series."""
    valid = [m for m in m01_list if m]
    if not valid:
        return None

    histories = [m.get("history", []) for m in valid]
    forecasts = [m.get("forecast", {}) for m in valid]

    return {
        "id": "m01",
        "title": "Total Crash Frequency",
        "subtitle": "Aggregate Monthly Forecast",
        "history": sum_history(histories),
        "forecast": merge_forecast_quantiles(forecasts),
    }


def aggregate_m02(m02_list):
    """Aggregate M02 (Severity-Level) by summing each severity time series."""
    valid = [m for m in m02_list if m]
    if not valid:
        return None

    severities = ["K", "A", "B", "C", "O"]

    # Aggregate history per severity
    agg_history = {}
    for sev in severities:
        sev_histories = [m.get("history", {}).get(sev, []) for m in valid]
        agg_history[sev] = sum_history(sev_histories)

    # Aggregate forecast per severity
    agg_forecast = {}
    for sev in severities:
        sev_forecasts = [m.get("forecast", {}).get(sev, {}) for m in valid]
        merged = merge_forecast_quantiles(sev_forecasts)
        if merged:
            agg_forecast[sev] = merged

    # Rebuild EPDO history from aggregated severity history
    all_months = sorted(set(e["month"] for sev in severities for e in agg_history.get(sev, [])))
    epdo_history = []
    for month in all_months:
        month_sev = {}
        for sev in severities:
            sev_by_month = {e["month"]: e["value"] for e in agg_history.get(sev, [])}
            month_sev[sev] = sev_by_month.get(month, 0)
        epdo = sum(month_sev.get(s, 0) * EPDO_WEIGHTS.get(s, 1) for s in severities)
        epdo_history.append({"month": month, "value": round(epdo)})

    return {
        "id": "m02",
        "title": "Severity-Level Forecast",
        "subtitle": "Aggregate K-A-B-C-O Prediction",
        "history": agg_history,
        "forecast": agg_forecast,
        "epdoHistory": epdo_history,
    }


def aggregate_m03(m03_list):
    """Aggregate M03 (Corridors) by merging and re-ranking by EPDO."""
    valid = [m for m in m03_list if m]
    if not valid:
        return None

    # Collect all corridors across jurisdictions, merge by route name
    corridor_map = {}
    for m in valid:
        for corr in m.get("corridors", []):
            route = corr.get("route", "")
            if route not in corridor_map:
                corridor_map[route] = {
                    "route": route,
                    "totalCrashes": 0,
                    "epdo": 0,
                    "severity": {"K": 0, "A": 0, "B": 0, "C": 0, "O": 0},
                    "history": [],
                    "forecast": {},
                }
            entry = corridor_map[route]
            entry["totalCrashes"] += corr.get("totalCrashes", 0)
            entry["epdo"] += corr.get("epdo", 0)
            for sev in ["K", "A", "B", "C", "O"]:
                entry["severity"][sev] += corr.get("severity", {}).get(sev, 0)

            # Sum history and forecast for same route
            if corr.get("history"):
                entry["history"] = sum_history([entry["history"], corr["history"]])
            if corr.get("forecast"):
                existing = entry["forecast"]
                new_fc = corr["forecast"]
                if existing and existing.get("p50"):
                    entry["forecast"] = merge_forecast_quantiles([existing, new_fc])
                else:
                    entry["forecast"] = new_fc

    # Rank by EPDO, take top 10
    ranked = sorted(corridor_map.values(), key=lambda x: x["epdo"], reverse=True)[:10]
    for i, corr in enumerate(ranked):
        corr["rank"] = i + 1

    # Get months from any forecast
    months = None
    for m in valid:
        if m.get("months"):
            months = m["months"]
            break

    return {
        "id": "m03",
        "title": "Top Corridor Forecasts",
        "subtitle": "Aggregate Top 10 by EPDO",
        "corridors": ranked,
        "months": months,
    }


def aggregate_m04(m04_list):
    """Aggregate M04 (Collision Types) by summing time series per type."""
    valid = [m for m in m04_list if m]
    if not valid:
        return None

    # Collect all crash types
    all_types = set()
    for m in valid:
        all_types.update(m.get("forecast", {}).keys())
    all_types.discard("months")

    # Aggregate history per type
    agg_history = {}
    for ct in all_types:
        histories = [m.get("history", {}).get(ct, []) for m in valid]
        agg_history[ct] = sum_history(histories)

    # Aggregate forecast per type
    agg_forecast = {}
    months = None
    for ct in all_types:
        forecasts = [m.get("forecast", {}).get(ct, {}) for m in valid]
        merged = merge_forecast_quantiles(forecasts)
        if merged:
            agg_forecast[ct] = merged
            if not months and merged.get("months"):
                months = merged["months"]

    return {
        "id": "m04",
        "title": "Crash Type Forecasts",
        "subtitle": "Aggregate by Collision Type",
        "history": agg_history,
        "forecast": agg_forecast,
        "months": months,
    }


def aggregate_m05(m05_list):
    """Aggregate M05 (Contributing Factors) by summing time series per factor."""
    valid = [m for m in m05_list if m]
    if not valid:
        return None

    all_factors = set()
    for m in valid:
        all_factors.update(m.get("forecast", {}).keys())
    all_factors.discard("months")

    agg_history = {}
    for f in all_factors:
        histories = [m.get("history", {}).get(f, []) for m in valid]
        agg_history[f] = sum_history(histories)

    agg_forecast = {}
    months = None
    for f in all_factors:
        forecasts = [m.get("forecast", {}).get(f, {}) for m in valid]
        merged = merge_forecast_quantiles(forecasts)
        if merged:
            agg_forecast[f] = merged
            if not months and merged.get("months"):
                months = merged["months"]

    return {
        "id": "m05",
        "title": "Contributing Factor Forecasts",
        "subtitle": "Aggregate by Factor",
        "history": agg_history,
        "forecast": agg_forecast,
        "months": months,
    }


def aggregate_m06(m06_list):
    """Aggregate M06 (Intersection vs Segment) by summing."""
    valid = [m for m in m06_list if m]
    if not valid:
        return None

    all_locs = set()
    for m in valid:
        all_locs.update(m.get("forecast", {}).keys())
    all_locs.discard("months")

    agg_history = {}
    for loc in all_locs:
        histories = [m.get("history", {}).get(loc, []) for m in valid]
        agg_history[loc] = sum_history(histories)

    agg_forecast = {}
    months = None
    for loc in all_locs:
        forecasts = [m.get("forecast", {}).get(loc, {}) for m in valid]
        merged = merge_forecast_quantiles(forecasts)
        if merged:
            agg_forecast[loc] = merged
            if not months and merged.get("months"):
                months = merged["months"]

    return {
        "id": "m06",
        "title": "Location Type Forecasts",
        "subtitle": "Aggregate Intersection vs Segment",
        "history": agg_history,
        "forecast": agg_forecast,
        "months": months,
    }


def aggregate_summary(summaries):
    """Aggregate summary stats by summing counts and recalculating rates."""
    if not summaries:
        return {}

    total = sum(s.get("totalCrashes", 0) for s in summaries)
    all_years = set()
    for s in summaries:
        all_years.update(s.get("years", []))

    # Sum severity counts
    severity = {"K": 0, "A": 0, "B": 0, "C": 0, "O": 0}
    for s in summaries:
        for sev, count in s.get("severity", {}).items():
            severity[sev] = severity.get(sev, 0) + count

    epdo = sum(severity.get(s, 0) * EPDO_WEIGHTS.get(s, 1) for s in severity)

    # Date range (earliest start, latest end)
    starts = [s.get("dateRange", {}).get("start", "") for s in summaries if s.get("dateRange", {}).get("start")]
    ends = [s.get("dateRange", {}).get("end", "") for s in summaries if s.get("dateRange", {}).get("end")]

    # Recent trend: sum raw counts, recalculate pct
    recent_6mo = sum(s.get("recentTrend", {}).get("recent6mo", 0) for s in summaries)
    prev_6mo = sum(s.get("recentTrend", {}).get("prev6mo", 0) for s in summaries)
    trend_pct = round(((recent_6mo - prev_6mo) / max(prev_6mo, 1)) * 100, 1)

    # Monthly average: sum of individual monthly averages
    monthly_avg = round(sum(s.get("monthlyAvg", 0) for s in summaries), 1)

    return {
        "totalCrashes": total,
        "years": sorted(all_years),
        "dateRange": {
            "start": min(starts) if starts else "",
            "end": max(ends) if ends else "",
        },
        "severity": severity,
        "epdo": epdo,
        "monthlyAvg": monthly_avg,
        "recentTrend": {
            "recent6mo": recent_6mo,
            "prev6mo": prev_6mo,
            "changePct": trend_pct,
        },
    }


def rebuild_derived_metrics(matrices, summary):
    """Rebuild key derived metrics from aggregated matrices.

    This is a lightweight version of generate_forecast.py's build_derived_metrics().
    We recompute the most important derived metrics from the aggregated data.
    """
    derived = {}

    # 1. Confidence Width (from M01)
    m01 = matrices.get("m01", {})
    fc01 = m01.get("forecast", {})
    if fc01.get("p90") and fc01.get("p10"):
        widths = [round(p90 - p10, 1) for p90, p10 in zip(fc01["p90"], fc01["p10"])]
        avg_width = round(sum(widths) / max(len(widths), 1), 1)
        p50_avg = sum(fc01.get("p50", [0])) / max(len(fc01.get("p50", [1])), 1)
        cv = round(avg_width / max(p50_avg, 1) * 100, 1)
        derived["confidenceWidth"] = {
            "monthlyWidths": widths,
            "months": fc01.get("months", []),
            "avgWidth": avg_width,
            "coefficientOfVariation": cv,
            "interpretation": "low" if cv < 30 else "moderate" if cv < 60 else "high",
        }

    # 2. EPDO Forecast (from M02)
    m02 = matrices.get("m02", {})
    fc02 = m02.get("forecast", {})
    if fc02.get("K") and fc02["K"].get("p50"):
        months_list = fc02["K"].get("months", [])
        epdo_forecast = []
        epdo_p10 = []
        epdo_p90 = []
        for i in range(len(months_list)):
            epdo_val = ep10 = ep90 = 0
            for sev, weight in EPDO_WEIGHTS.items():
                sev_fc = fc02.get(sev, {})
                p50_vals = sev_fc.get("p50", [])
                p10_vals = sev_fc.get("p10", [])
                p90_vals = sev_fc.get("p90", [])
                epdo_val += (p50_vals[i] if i < len(p50_vals) else 0) * weight
                ep10 += (p10_vals[i] if i < len(p10_vals) else 0) * weight
                ep90 += (p90_vals[i] if i < len(p90_vals) else 0) * weight
            epdo_forecast.append(round(epdo_val))
            epdo_p10.append(round(ep10))
            epdo_p90.append(round(ep90))

        hist_epdo = m02.get("epdoHistory", [])
        hist_6mo = sum(h["value"] for h in hist_epdo[-6:]) if hist_epdo else 0
        pred_6mo = sum(epdo_forecast[:6])
        epdo_change = round(((pred_6mo - hist_6mo) / max(hist_6mo, 1)) * 100, 1) if hist_6mo else 0

        derived["epdoForecast"] = {
            "months": months_list,
            "p50": epdo_forecast,
            "p10": epdo_p10,
            "p90": epdo_p90,
            "predicted6moTotal": round(pred_6mo),
            "historical6moTotal": round(hist_6mo),
            "changePct": epdo_change,
        }

    # 3. KA Forecast
    if fc02.get("K") and fc02.get("A"):
        months_list = fc02["K"].get("months", [])
        ka_forecast = []
        total_forecast = fc01.get("p50", [])
        for i in range(len(months_list)):
            k_val = fc02["K"]["p50"][i] if i < len(fc02["K"].get("p50", [])) else 0
            a_val = fc02["A"]["p50"][i] if i < len(fc02["A"].get("p50", [])) else 0
            ka_forecast.append(round(k_val + a_val, 1))

        ka_rates = []
        for i, ka in enumerate(ka_forecast):
            total = total_forecast[i] if i < len(total_forecast) else 1
            ka_rates.append(round(ka / max(total, 1) * 100, 1))

        hist_k = m02.get("history", {}).get("K", [])
        hist_a = m02.get("history", {}).get("A", [])
        hist_ka_6mo = (sum(h["value"] for h in hist_k[-6:]) +
                       sum(h["value"] for h in hist_a[-6:])) if hist_k else 0
        pred_ka_6mo = sum(ka_forecast[:6])

        derived["kaForecast"] = {
            "months": months_list,
            "predicted": ka_forecast,
            "kaRatePct": ka_rates,
            "predicted6mo": round(pred_ka_6mo, 1),
            "historical6mo": round(hist_ka_6mo, 1),
            "trend": "worsening" if pred_ka_6mo > hist_ka_6mo else "improving",
        }

    return derived


def aggregate_forecasts_for_group(group_type, group_id, member_counties, data_dir, output_dir):
    """Aggregate forecast JSONs for a region or MPO group.

    For each road type, loads member jurisdiction forecasts, merges them
    mathematically, and writes the aggregate forecast JSON.
    """
    logger.info(f"  Aggregating forecasts for {group_type}/{group_id} ({len(member_counties)} counties)")
    generated = 0

    for road_type in ROAD_TYPES:
        # Load all member jurisdiction forecasts for this road type
        forecasts = []
        found_jurisdictions = []
        for county in member_counties:
            fc = load_jurisdiction_forecast(data_dir, county, road_type)
            if fc:
                forecasts.append(fc)
                found_jurisdictions.append(county)

        if not forecasts:
            logger.debug(f"    No forecasts found for {road_type}")
            continue

        logger.info(f"    {road_type}: merging {len(forecasts)}/{len(member_counties)} jurisdiction forecasts")

        # Extract matrices from each forecast
        m01_list = [fc.get("matrices", {}).get("m01") for fc in forecasts]
        m02_list = [fc.get("matrices", {}).get("m02") for fc in forecasts]
        m03_list = [fc.get("matrices", {}).get("m03") for fc in forecasts]
        m04_list = [fc.get("matrices", {}).get("m04") for fc in forecasts]
        m05_list = [fc.get("matrices", {}).get("m05") for fc in forecasts]
        m06_list = [fc.get("matrices", {}).get("m06") for fc in forecasts]

        # Aggregate each matrix
        matrices = {}
        agg_m01 = aggregate_m01(m01_list)
        if agg_m01:
            matrices["m01"] = agg_m01
        agg_m02 = aggregate_m02(m02_list)
        if agg_m02:
            matrices["m02"] = agg_m02
        agg_m03 = aggregate_m03(m03_list)
        if agg_m03:
            matrices["m03"] = agg_m03
        agg_m04 = aggregate_m04(m04_list)
        if agg_m04:
            matrices["m04"] = agg_m04
        agg_m05 = aggregate_m05(m05_list)
        if agg_m05:
            matrices["m05"] = agg_m05
        agg_m06 = aggregate_m06(m06_list)
        if agg_m06:
            matrices["m06"] = agg_m06

        # Aggregate summary stats
        summaries = [fc.get("summary", {}) for fc in forecasts]
        agg_summary = aggregate_summary(summaries)

        # Rebuild derived metrics from aggregated data
        derived = rebuild_derived_metrics(matrices, agg_summary)

        # Use metadata from first forecast
        first = forecasts[0]

        output = {
            "generated": first.get("generated", ""),
            "model": first.get("model", "amazon/chronos-2"),
            "aggregated": True,
            "aggregationType": group_type,
            "aggregationId": group_id,
            "memberJurisdictions": found_jurisdictions,
            "memberCount": len(found_jurisdictions),
            "horizon": first.get("horizon", 24),
            "quantileLevels": first.get("quantileLevels", []),
            "epdoWeights": EPDO_WEIGHTS,
            "roadType": road_type,
            "summary": agg_summary,
            "matrices": matrices,
            "derivedMetrics": derived,
        }

        # Write output — use {road_type}.json naming to match R2 convention
        output_path = Path(output_dir) / f"_{group_type}" / group_id / f"forecasts_{road_type}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2, default=str)

        size_kb = os.path.getsize(output_path) / 1024
        logger.info(f"    Written: {output_path.name} ({size_kb:.1f} KB)")
        generated += 1

    return generated


def main():
    parser = argparse.ArgumentParser(description='Aggregate jurisdiction forecast JSONs into region/MPO forecasts')
    parser.add_argument('--state', required=True, help='State key (e.g., virginia, colorado)')
    parser.add_argument('--scope', choices=['region', 'mpo', 'statewide'], default='statewide',
                        help='Aggregation scope (default: statewide = all regions + all MPOs)')
    parser.add_argument('--selection', default='', help='Region/MPO ID (for scope=region/mpo)')
    parser.add_argument('--data-dir', required=True, help='Directory containing jurisdiction forecast JSONs')
    parser.add_argument('--output-dir', default='', help='Output directory (defaults to data-dir)')
    args = parser.parse_args()

    output_dir = args.output_dir or args.data_dir

    hierarchy = load_hierarchy(args.state)
    total_generated = 0

    logger.info(f"[{args.state}] Aggregating forecasts (scope={args.scope})")

    if args.scope == 'region':
        if not args.selection:
            logger.error("--selection required for scope=region")
            return 1
        regions = get_regions(hierarchy)
        if args.selection not in regions:
            logger.error(f"Region '{args.selection}' not found. Available: {', '.join(regions.keys())}")
            return 1
        total_generated += aggregate_forecasts_for_group(
            'region', args.selection, regions[args.selection], args.data_dir, output_dir)

    elif args.scope == 'mpo':
        if not args.selection:
            logger.error("--selection required for scope=mpo")
            return 1
        mpos = get_mpos(hierarchy)
        if args.selection not in mpos:
            logger.error(f"MPO '{args.selection}' not found. Available: {', '.join(mpos.keys())}")
            return 1
        total_generated += aggregate_forecasts_for_group(
            'mpo', args.selection, mpos[args.selection], args.data_dir, output_dir)

    elif args.scope == 'statewide':
        regions = get_regions(hierarchy)
        mpos = get_mpos(hierarchy)

        logger.info(f"  Processing {len(regions)} regions + {len(mpos)} MPOs")

        for rid, members in regions.items():
            total_generated += aggregate_forecasts_for_group(
                'region', rid, members, args.data_dir, output_dir)

        for mid, members in mpos.items():
            total_generated += aggregate_forecasts_for_group(
                'mpo', mid, members, args.data_dir, output_dir)

    logger.info("=" * 60)
    logger.info(f"[{args.state}] FORECAST AGGREGATION COMPLETE: {total_generated} files generated")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
