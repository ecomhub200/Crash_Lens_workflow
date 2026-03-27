#!/usr/bin/env python3
"""
Prediction Forecast Accuracy & Integrity Test
===============================================
Validates forecast JSON files against their source crash CSVs.

Tests:
  - Structural integrity (all required keys present)
  - Data consistency (summary matches source CSV)
  - Mathematical accuracy (EPDO, severity sums, quantile ordering)
  - Cross-file consistency (county_roads < all_roads)
  - Derived metrics validity (10 analytics computed correctly)
  - Temporal integrity (history dates match CSV, forecast extends correctly)

Run:
    python3 tests/test_prediction_accuracy.py
    python3 tests/test_prediction_accuracy.py --data-dir data/CDOT --jurisdiction douglas
"""

import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'CDOT')
JURISDICTION = 'douglas'

EPDO_WEIGHTS = {'K': 462, 'A': 62, 'B': 12, 'C': 5, 'O': 1}

ROAD_TYPES = {
    'county_roads': {
        'forecast_file': 'forecasts_county_roads.json',
        'csv_file': '{jurisdiction}_county_roads.csv',
    },
    'city_roads': {
        'forecast_file': 'forecasts_city_roads.json',
        'csv_file': '{jurisdiction}_city_roads.csv',
    },
    'no_interstate': {
        'forecast_file': 'forecasts_no_interstate.json',
        'csv_file': '{jurisdiction}_no_interstate.csv',
    },
    'all_roads': {
        'forecast_file': 'forecasts_all_roads.json',
        'csv_file': '{jurisdiction}_all_roads.csv',
    },
}

# All 6 matrices that should be present
REQUIRED_MATRICES = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06']

# All 10 derived metrics
REQUIRED_DERIVED_METRICS = [
    'confidenceWidth', 'epdoForecast', 'kaForecast', 'severityShift',
    'corridorRankMovement', 'crashTypeMomentum', 'factorTrends',
    'seasonalRiskCalendar', 'compositeRiskScore', 'locationTypeSplit',
]

# Quantile keys expected in forecast objects
QUANTILE_KEYS = ['p10', 'p25', 'p50', 'p75', 'p90']

# Valid severity values
VALID_SEVERITIES = {'K', 'A', 'B', 'C', 'O'}

# ─── Test Tracking ───────────────────────────────────────────────────────────

class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.results = []

    def record(self, level, test_name, message, detail=''):
        status = 'PASS' if level == 'pass' else 'FAIL' if level in ('critical', 'high') else 'WARN'
        self.results.append({
            'level': level,
            'status': status,
            'test': test_name,
            'message': message,
            'detail': detail,
        })
        if status == 'PASS':
            self.passed += 1
        elif status == 'FAIL':
            self.failed += 1
        else:
            self.warnings += 1

        icon = '  PASS' if status == 'PASS' else '  FAIL' if status == 'FAIL' else '  WARN'
        print(f'{icon}  {test_name}: {message}')
        if detail:
            print(f'         {detail}')

    def summary(self):
        total = self.passed + self.failed + self.warnings
        print(f'\n{"="*70}')
        print(f'  PREDICTION ACCURACY TEST RESULTS')
        print(f'{"="*70}')
        print(f'  Total: {total}  |  Passed: {self.passed}  |  Failed: {self.failed}  |  Warnings: {self.warnings}')
        if self.failed > 0:
            print(f'\n  FAILURES:')
            for r in self.results:
                if r['status'] == 'FAIL':
                    print(f'    [{r["level"].upper()}] {r["test"]}: {r["message"]}')
                    if r['detail']:
                        print(f'           {r["detail"]}')
        print(f'{"="*70}')
        return self.failed == 0


# ─── Data Loading ─────────────────────────────────────────────────────────────

def load_forecast(path):
    """Load a forecast JSON file."""
    with open(path) as f:
        return json.load(f)


def load_csv_stats(path):
    """Load a crash CSV and compute basic stats for comparison."""
    with open(path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    severity_counts = Counter()
    date_months = set()
    route_counts = Counter()

    for row in rows:
        sev = row.get('Crash Severity', '').strip()
        if sev in VALID_SEVERITIES:
            severity_counts[sev] += 1

        date_str = row.get('Crash Date', '').strip()
        if date_str:
            try:
                for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%m/%d/%y'):
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        date_months.add(f'{dt.year}-{dt.month:02d}')
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        rte = row.get('RTE Name', '').strip()
        if rte:
            route_counts[rte] += 1

    epdo = sum(severity_counts.get(s, 0) * w for s, w in EPDO_WEIGHTS.items())

    return {
        'total': total,
        'severity': dict(severity_counts),
        'severity_sum': sum(severity_counts.values()),
        'epdo': epdo,
        'months': sorted(date_months),
        'route_counts': route_counts,
    }


# ─── Test Functions ───────────────────────────────────────────────────────────

def test_file_existence(results, data_dir, jurisdiction):
    """Test 1: All forecast and source CSV files exist."""
    for road_type, config in ROAD_TYPES.items():
        forecast_path = os.path.join(data_dir, config['forecast_file'])
        csv_path = os.path.join(data_dir, config['csv_file'].format(jurisdiction=jurisdiction))

        if os.path.exists(forecast_path):
            results.record('pass', f'file_exists_{road_type}_json',
                           f'{config["forecast_file"]} exists')
        else:
            results.record('critical', f'file_exists_{road_type}_json',
                           f'{config["forecast_file"]} NOT FOUND',
                           f'Expected at: {forecast_path}')

        if os.path.exists(csv_path):
            results.record('pass', f'file_exists_{road_type}_csv',
                           f'Source CSV exists for {road_type}')
        else:
            results.record('high', f'file_exists_{road_type}_csv',
                           f'Source CSV NOT FOUND for {road_type}',
                           f'Expected at: {csv_path}')


def test_structural_integrity(results, forecast, road_type):
    """Test 2: Forecast JSON has all required top-level keys and structure."""
    # Top-level keys
    required_top = ['generated', 'model', 'horizon', 'quantileLevels',
                    'epdoWeights', 'roadType', 'summary', 'matrices', 'derivedMetrics']
    for key in required_top:
        if key in forecast:
            results.record('pass', f'struct_{road_type}_key_{key}',
                           f'Top-level key "{key}" present')
        else:
            results.record('critical', f'struct_{road_type}_key_{key}',
                           f'Missing top-level key: "{key}"')

    # roadType matches
    if forecast.get('roadType') == road_type:
        results.record('pass', f'struct_{road_type}_roadtype_match',
                       f'roadType field matches: {road_type}')
    else:
        results.record('high', f'struct_{road_type}_roadtype_match',
                       f'roadType mismatch',
                       f'Expected "{road_type}", got "{forecast.get("roadType")}"')

    # All 6 matrices present
    matrices = forecast.get('matrices', {})
    for matrix_id in REQUIRED_MATRICES:
        if matrix_id in matrices:
            results.record('pass', f'struct_{road_type}_matrix_{matrix_id}',
                           f'Matrix {matrix_id} present')
        else:
            results.record('high', f'struct_{road_type}_matrix_{matrix_id}',
                           f'Missing matrix: {matrix_id}')

    # All 10 derived metrics present
    derived = forecast.get('derivedMetrics', {})
    for metric_name in REQUIRED_DERIVED_METRICS:
        if metric_name in derived:
            results.record('pass', f'struct_{road_type}_derived_{metric_name}',
                           f'Derived metric "{metric_name}" present')
        else:
            results.record('high', f'struct_{road_type}_derived_{metric_name}',
                           f'Missing derived metric: {metric_name}')


def test_summary_vs_csv(results, forecast, csv_stats, road_type):
    """Test 3: Forecast summary statistics match the source CSV."""
    summary = forecast.get('summary', {})

    # Total crashes
    fc_total = summary.get('totalCrashes', 0)
    csv_total = csv_stats['total']
    if fc_total == csv_total:
        results.record('pass', f'summary_{road_type}_total_crashes',
                       f'totalCrashes matches CSV: {fc_total:,}')
    else:
        results.record('critical', f'summary_{road_type}_total_crashes',
                       f'totalCrashes MISMATCH',
                       f'Forecast: {fc_total:,}, CSV: {csv_total:,} (diff: {fc_total - csv_total:+,})')

    # Severity sum equals totalCrashes
    fc_severity = summary.get('severity', {})
    fc_sev_sum = sum(fc_severity.values())
    if fc_sev_sum == fc_total:
        results.record('pass', f'summary_{road_type}_severity_sum',
                       f'Severity sum equals totalCrashes: {fc_sev_sum:,}')
    else:
        results.record('critical', f'summary_{road_type}_severity_sum',
                       f'Severity sum != totalCrashes',
                       f'Sum(K+A+B+C+O)={fc_sev_sum:,} vs totalCrashes={fc_total:,}')

    # Individual severity counts match CSV
    csv_severity = csv_stats['severity']
    for sev in VALID_SEVERITIES:
        fc_count = fc_severity.get(sev, 0)
        csv_count = csv_severity.get(sev, 0)
        if fc_count == csv_count:
            results.record('pass', f'summary_{road_type}_severity_{sev}',
                           f'{sev} count matches: {fc_count:,}')
        else:
            results.record('high', f'summary_{road_type}_severity_{sev}',
                           f'{sev} count MISMATCH',
                           f'Forecast: {fc_count:,}, CSV: {csv_count:,}')

    # EPDO calculation consistency
    fc_epdo = summary.get('epdo', 0)
    expected_epdo = sum(fc_severity.get(s, 0) * w for s, w in EPDO_WEIGHTS.items())
    if fc_epdo == expected_epdo:
        results.record('pass', f'summary_{road_type}_epdo',
                       f'EPDO calculation correct: {fc_epdo:,}')
    else:
        results.record('high', f'summary_{road_type}_epdo',
                       f'EPDO calculation MISMATCH',
                       f'Stored: {fc_epdo:,}, Computed: {expected_epdo:,}')

    # EPDO matches CSV-computed value
    csv_epdo = csv_stats['epdo']
    if fc_epdo == csv_epdo:
        results.record('pass', f'summary_{road_type}_epdo_vs_csv',
                       f'EPDO matches CSV-computed value: {fc_epdo:,}')
    else:
        results.record('high', f'summary_{road_type}_epdo_vs_csv',
                       f'EPDO mismatch with CSV',
                       f'Forecast: {fc_epdo:,}, CSV-computed: {csv_epdo:,}')

    # Date range present and reasonable
    date_range = summary.get('dateRange', {})
    if date_range.get('start') and date_range.get('end'):
        results.record('pass', f'summary_{road_type}_date_range',
                       f'Date range: {date_range["start"]} to {date_range["end"]}')
    else:
        results.record('high', f'summary_{road_type}_date_range',
                       f'Missing or empty date range')

    # Monthly average sanity check
    monthly_avg = summary.get('monthlyAvg', 0)
    if monthly_avg > 0:
        results.record('pass', f'summary_{road_type}_monthly_avg',
                       f'Monthly average: {monthly_avg}')
    else:
        results.record('high', f'summary_{road_type}_monthly_avg',
                       f'Monthly average is zero or missing')


def test_matrix_m01(results, forecast, road_type):
    """Test 4: Matrix M01 (Total Crash Frequency) integrity."""
    m01 = forecast.get('matrices', {}).get('m01', {})
    if not m01:
        results.record('critical', f'm01_{road_type}_exists', 'M01 matrix missing')
        return

    # History present and non-empty
    history = m01.get('history', [])
    if len(history) > 0:
        results.record('pass', f'm01_{road_type}_history',
                       f'M01 history has {len(history)} months')
    else:
        results.record('critical', f'm01_{road_type}_history', 'M01 history empty')
        return

    # History values are non-negative
    neg_vals = [h for h in history if h.get('value', 0) < 0]
    if not neg_vals:
        results.record('pass', f'm01_{road_type}_history_nonneg',
                       'All M01 history values non-negative')
    else:
        results.record('high', f'm01_{road_type}_history_nonneg',
                       f'{len(neg_vals)} negative history values in M01')

    # History sum should match totalCrashes
    history_sum = sum(h.get('value', 0) for h in history)
    fc_total = forecast.get('summary', {}).get('totalCrashes', 0)
    if history_sum == fc_total:
        results.record('pass', f'm01_{road_type}_history_sum',
                       f'M01 history sum matches totalCrashes: {history_sum:,}')
    else:
        results.record('critical', f'm01_{road_type}_history_sum',
                       f'M01 history sum != totalCrashes',
                       f'History sum: {history_sum:,}, totalCrashes: {fc_total:,}')

    # Forecast present
    fc = m01.get('forecast', {})
    test_quantile_forecast(results, fc, f'm01_{road_type}', forecast.get('horizon', 12))


def test_matrix_m02(results, forecast, road_type):
    """Test 5: Matrix M02 (Severity-Level Multivariate) integrity."""
    m02 = forecast.get('matrices', {}).get('m02', {})
    if not m02:
        results.record('critical', f'm02_{road_type}_exists', 'M02 matrix missing')
        return

    # All 5 severity levels present in history
    history = m02.get('history', {})
    for sev in VALID_SEVERITIES:
        if sev in history and len(history[sev]) > 0:
            results.record('pass', f'm02_{road_type}_history_{sev}',
                           f'M02 history has {sev}: {len(history[sev])} months')
        else:
            results.record('high', f'm02_{road_type}_history_{sev}',
                           f'M02 history missing severity {sev}')

    # Severity history sums should match summary.severity
    fc_severity = forecast.get('summary', {}).get('severity', {})
    for sev in VALID_SEVERITIES:
        if sev in history:
            hist_sum = sum(h.get('value', 0) for h in history[sev])
            expected = fc_severity.get(sev, 0)
            if hist_sum == expected:
                results.record('pass', f'm02_{road_type}_hist_sum_{sev}',
                               f'M02 {sev} history sum matches: {hist_sum:,}')
            else:
                results.record('high', f'm02_{road_type}_hist_sum_{sev}',
                               f'M02 {sev} history sum mismatch',
                               f'Sum: {hist_sum:,}, Expected: {expected:,}')

    # All 5 severity levels present in forecast
    fc02 = m02.get('forecast', {})
    for sev in VALID_SEVERITIES:
        if sev in fc02:
            test_quantile_forecast(results, fc02[sev], f'm02_{road_type}_{sev}',
                                   forecast.get('horizon', 12))
        else:
            results.record('high', f'm02_{road_type}_fc_{sev}',
                           f'M02 forecast missing severity {sev}')

    # EPDO history present
    epdo_hist = m02.get('epdoHistory', [])
    if len(epdo_hist) > 0:
        results.record('pass', f'm02_{road_type}_epdo_history',
                       f'M02 EPDO history: {len(epdo_hist)} months')
    else:
        results.record('high', f'm02_{road_type}_epdo_history',
                       'M02 EPDO history missing or empty')


def test_matrix_m03(results, forecast, csv_stats, road_type):
    """Test 6: Matrix M03 (Corridor Cross-Learning) integrity."""
    m03 = forecast.get('matrices', {}).get('m03', {})
    if not m03:
        results.record('high', f'm03_{road_type}_exists', 'M03 matrix missing')
        return

    corridors = m03.get('corridors', {})
    if not corridors:
        results.record('high', f'm03_{road_type}_corridors', 'M03 has no corridors')
        return

    results.record('pass', f'm03_{road_type}_corridor_count',
                   f'M03 has {len(corridors)} corridors')

    # Verify corridor names exist in source CSV routes
    csv_routes = set(csv_stats['route_counts'].keys())
    csv_routes_upper = {r.upper() for r in csv_routes}
    for corridor_name in corridors:
        # Partial match (corridor names are often substrings of full RTE Names)
        found = any(corridor_name.upper() in rte for rte in csv_routes_upper)
        if found:
            results.record('pass', f'm03_{road_type}_corridor_{corridor_name}',
                           f'Corridor "{corridor_name}" found in CSV routes')
        else:
            results.record('warn', f'm03_{road_type}_corridor_{corridor_name}',
                           f'Corridor "{corridor_name}" not matched in CSV routes')

    # Each corridor has history, forecast, stats
    for name, cdata in corridors.items():
        if 'history' in cdata and 'forecast' in cdata and 'stats' in cdata:
            results.record('pass', f'm03_{road_type}_structure_{name}',
                           f'Corridor "{name}" has history + forecast + stats')
        else:
            missing = [k for k in ('history', 'forecast', 'stats') if k not in cdata]
            results.record('high', f'm03_{road_type}_structure_{name}',
                           f'Corridor "{name}" missing: {missing}')


def test_matrix_m04(results, forecast, road_type):
    """Test 7: Matrix M04 (Crash Type Distribution) integrity."""
    m04 = forecast.get('matrices', {}).get('m04', {})
    if not m04:
        results.record('high', f'm04_{road_type}_exists', 'M04 matrix missing')
        return

    types = m04.get('types', {})
    if not types:
        results.record('high', f'm04_{road_type}_types', 'M04 has no crash types')
        return

    results.record('pass', f'm04_{road_type}_type_count',
                   f'M04 has {len(types)} crash types: {list(types.keys())}')

    # Each type has history, forecast, total
    for type_name, tdata in types.items():
        if 'history' in tdata and 'forecast' in tdata and 'total' in tdata:
            results.record('pass', f'm04_{road_type}_struct_{type_name}',
                           f'Crash type "{type_name}" structure valid (total={tdata["total"]})')
        else:
            results.record('high', f'm04_{road_type}_struct_{type_name}',
                           f'Crash type "{type_name}" missing required fields')


def test_matrix_m05(results, forecast, road_type):
    """Test 8: Matrix M05 (Contributing Factor Trends) integrity."""
    m05 = forecast.get('matrices', {}).get('m05', {})
    if not m05:
        results.record('high', f'm05_{road_type}_exists', 'M05 matrix missing')
        return

    factors = m05.get('factors', {})
    expected_factors = {'speed', 'alcohol', 'pedestrian', 'bicycle', 'night'}

    for factor_name in expected_factors:
        if factor_name in factors:
            fdata = factors[factor_name]
            if 'total' in fdata and 'history' in fdata and 'forecast' in fdata:
                results.record('pass', f'm05_{road_type}_factor_{factor_name}',
                               f'Factor "{factor_name}" valid (total={fdata["total"]})')
            else:
                results.record('high', f'm05_{road_type}_factor_{factor_name}',
                               f'Factor "{factor_name}" missing required fields')
        else:
            results.record('high', f'm05_{road_type}_factor_{factor_name}',
                           f'Factor "{factor_name}" missing from M05')


def test_matrix_m06(results, forecast, road_type):
    """Test 9: Matrix M06 (Intersection vs Segment) integrity."""
    m06 = forecast.get('matrices', {}).get('m06', {})
    if not m06:
        results.record('high', f'm06_{road_type}_exists', 'M06 matrix missing')
        return

    loc_types = m06.get('locationTypes', {})
    if not loc_types:
        results.record('high', f'm06_{road_type}_types', 'M06 has no location types')
        return

    results.record('pass', f'm06_{road_type}_type_count',
                   f'M06 has {len(loc_types)} location types: {list(loc_types.keys())}')

    # segment should be present
    if 'segment' in loc_types:
        results.record('pass', f'm06_{road_type}_has_segment', 'M06 has "segment" type')
    else:
        results.record('high', f'm06_{road_type}_has_segment', 'M06 missing "segment" type')


def test_quantile_forecast(results, fc, prefix, expected_horizon):
    """Test quantile ordering and structure for a forecast object."""
    if not fc:
        results.record('high', f'{prefix}_forecast_empty', 'Forecast object empty')
        return

    months = fc.get('months', [])
    if len(months) != expected_horizon:
        results.record('high', f'{prefix}_horizon',
                       f'Forecast horizon mismatch',
                       f'Expected {expected_horizon} months, got {len(months)}')
    else:
        results.record('pass', f'{prefix}_horizon',
                       f'Forecast horizon correct: {len(months)} months')

    # Check quantile keys exist
    has_quantiles = all(key in fc for key in QUANTILE_KEYS)
    if has_quantiles:
        results.record('pass', f'{prefix}_quantile_keys',
                       'All 5 quantile keys present (p10-p90)')
    else:
        missing = [k for k in QUANTILE_KEYS if k not in fc]
        results.record('high', f'{prefix}_quantile_keys',
                       f'Missing quantile keys: {missing}')
        return

    # Quantile ordering: p10 <= p25 <= p50 <= p75 <= p90
    violations = 0
    for i in range(len(months)):
        vals = {}
        for qk in QUANTILE_KEYS:
            q_vals = fc.get(qk, [])
            vals[qk] = q_vals[i] if i < len(q_vals) else None

        if all(v is not None for v in vals.values()):
            if not (vals['p10'] <= vals['p25'] <= vals['p50'] <= vals['p75'] <= vals['p90']):
                violations += 1

    if violations == 0:
        results.record('pass', f'{prefix}_quantile_order',
                       f'Quantile ordering valid (p10<=p25<=p50<=p75<=p90) across {len(months)} months')
    else:
        results.record('high', f'{prefix}_quantile_order',
                       f'Quantile ordering VIOLATED in {violations}/{len(months)} months')

    # All forecast values non-negative
    neg_count = 0
    for qk in QUANTILE_KEYS:
        for v in fc.get(qk, []):
            if v < 0:
                neg_count += 1

    if neg_count == 0:
        results.record('pass', f'{prefix}_nonneg',
                       'All forecast values non-negative')
    else:
        results.record('high', f'{prefix}_nonneg',
                       f'{neg_count} negative forecast values found')


def test_derived_metrics(results, forecast, road_type):
    """Test 10: Derived metrics validity."""
    derived = forecast.get('derivedMetrics', {})

    # Confidence width
    cw = derived.get('confidenceWidth', {})
    if cw:
        interp = cw.get('interpretation', '')
        if interp in ('low', 'moderate', 'high'):
            results.record('pass', f'derived_{road_type}_confidence_interp',
                           f'Confidence interpretation valid: {interp}')
        else:
            results.record('high', f'derived_{road_type}_confidence_interp',
                           f'Invalid confidence interpretation: {interp}')

        cv = cw.get('coefficientOfVariation', -1)
        if cv >= 0:
            results.record('pass', f'derived_{road_type}_confidence_cv',
                           f'CV: {cv}%')
        else:
            results.record('high', f'derived_{road_type}_confidence_cv',
                           f'Negative CV: {cv}')

    # Composite risk score
    crs = derived.get('compositeRiskScore', {})
    if crs:
        level = crs.get('level', '')
        if level in ('low', 'moderate', 'elevated', 'critical'):
            results.record('pass', f'derived_{road_type}_risk_level',
                           f'Risk level valid: {level}')
        else:
            results.record('high', f'derived_{road_type}_risk_level',
                           f'Invalid risk level: {level}')

        # Verify composite score calculation
        components = crs.get('components', {})
        weights = crs.get('weights', {})
        if components and weights:
            expected_score = (
                components.get('epdoChange', 0) * weights.get('epdoChange', 0.5) +
                components.get('totalTrend', 0) * weights.get('totalTrend', 0.3) +
                components.get('severityShift', 0) * 10 * weights.get('severityShift', 0.2)
            )
            actual_score = crs.get('score', 0)
            if abs(actual_score - round(expected_score, 1)) < 0.2:
                results.record('pass', f'derived_{road_type}_risk_calc',
                               f'Composite risk score calculation verified: {actual_score}')
            else:
                results.record('high', f'derived_{road_type}_risk_calc',
                               f'Composite risk score mismatch',
                               f'Stored: {actual_score}, Expected: {round(expected_score, 1)}')

    # Severity shift interpretation
    ss = derived.get('severityShift', {})
    if ss:
        interp = ss.get('interpretation', '')
        if interp in ('improving', 'stable', 'worsening'):
            results.record('pass', f'derived_{road_type}_severity_shift_interp',
                           f'Severity shift interpretation: {interp}')
        else:
            results.record('high', f'derived_{road_type}_severity_shift_interp',
                           f'Invalid severity shift interpretation: {interp}')

        # Verify predicted distribution sums to reasonable total
        pred_dist = ss.get('predictedDistribution', {})
        if pred_dist:
            pred_total = sum(pred_dist.values())
            if pred_total > 0:
                results.record('pass', f'derived_{road_type}_severity_pred_dist',
                               f'Predicted severity distribution total: {pred_total}')
            else:
                results.record('high', f'derived_{road_type}_severity_pred_dist',
                               'Predicted severity distribution sums to zero')

    # KA forecast trend
    ka = derived.get('kaForecast', {})
    if ka:
        trend = ka.get('trend', '')
        if trend in ('improving', 'worsening'):
            results.record('pass', f'derived_{road_type}_ka_trend',
                           f'KA trend: {trend}')
        else:
            results.record('high', f'derived_{road_type}_ka_trend',
                           f'Invalid KA trend: {trend}')

    # Location type split percentages sum to ~100%
    lts = derived.get('locationTypeSplit', {})
    if lts:
        int_pct = lts.get('intersectionSharePct', 0)
        seg_pct = lts.get('segmentSharePct', 0)
        total_pct = int_pct + seg_pct
        if 99.0 <= total_pct <= 101.0:
            results.record('pass', f'derived_{road_type}_loc_split_pct',
                           f'Location type split: intersection {int_pct}% + segment {seg_pct}% = {total_pct}%')
        else:
            results.record('high', f'derived_{road_type}_loc_split_pct',
                           f'Location type percentages sum to {total_pct}% (expected ~100%)')

    # Corridor rank movement
    crm = derived.get('corridorRankMovement', {})
    if crm:
        rankings = crm.get('rankings', [])
        if rankings:
            # All rankings should have required fields
            required_fields = {'name', 'historical6moAvg', 'predicted6moAvg',
                               'changePct', 'predictedRank', 'historicalRank', 'direction'}
            for r in rankings:
                missing = required_fields - set(r.keys())
                if missing:
                    results.record('high', f'derived_{road_type}_crm_fields_{r.get("name", "?")}',
                                   f'Corridor ranking missing fields: {missing}')
                    break
            else:
                results.record('pass', f'derived_{road_type}_crm_fields',
                               f'All {len(rankings)} corridor rankings have required fields')

            # Direction values should be valid
            valid_directions = {'rising', 'falling', 'stable'}
            bad_dirs = [r for r in rankings if r.get('direction') not in valid_directions]
            if not bad_dirs:
                results.record('pass', f'derived_{road_type}_crm_directions',
                               'All corridor directions valid')
            else:
                results.record('high', f'derived_{road_type}_crm_directions',
                               f'{len(bad_dirs)} corridors have invalid direction')

    # Seasonal risk calendar
    src = derived.get('seasonalRiskCalendar', {})
    if src:
        months = src.get('months', [])
        horizon = forecast.get('horizon', 12)
        if len(months) == horizon:
            results.record('pass', f'derived_{road_type}_seasonal_months',
                           f'Seasonal calendar has {len(months)} months')
        else:
            results.record('high', f'derived_{road_type}_seasonal_months',
                           f'Seasonal calendar has {len(months)} months (expected {horizon})')

        # All months have riskRank
        ranked = [m for m in months if 'riskRank' in m]
        if len(ranked) == len(months):
            results.record('pass', f'derived_{road_type}_seasonal_ranked',
                           'All seasonal months have risk ranks')
        else:
            results.record('high', f'derived_{road_type}_seasonal_ranked',
                           f'Only {len(ranked)}/{len(months)} months have risk ranks')


def test_temporal_integrity(results, forecast, csv_stats, road_type):
    """Test 11: History dates match CSV, forecast extends correctly."""
    m01 = forecast.get('matrices', {}).get('m01', {})
    if not m01:
        return

    history = m01.get('history', [])
    fc = m01.get('forecast', {})

    if not history:
        results.record('high', f'temporal_{road_type}_no_history', 'No history data')
        return

    # History months should be contiguous
    hist_months = [h['month'] for h in history]
    first_month = hist_months[0]
    last_month = hist_months[-1]

    # Verify the CSV date range matches
    csv_months = csv_stats['months']
    if csv_months:
        csv_first = csv_months[0]
        csv_last = csv_months[-1]
        if first_month == csv_first:
            results.record('pass', f'temporal_{road_type}_start_match',
                           f'History start matches CSV: {first_month}')
        else:
            results.record('high', f'temporal_{road_type}_start_match',
                           f'History start mismatch',
                           f'History: {first_month}, CSV: {csv_first}')

        if last_month == csv_last:
            results.record('pass', f'temporal_{road_type}_end_match',
                           f'History end matches CSV: {last_month}')
        else:
            results.record('high', f'temporal_{road_type}_end_match',
                           f'History end mismatch',
                           f'History: {last_month}, CSV: {csv_last}')

    # Forecast months should start right after last history month
    fc_months = fc.get('months', [])
    if fc_months and last_month:
        # Parse last history month and first forecast month
        last_year, last_mo = map(int, last_month.split('-'))
        fc_year, fc_mo = map(int, fc_months[0].split('-'))

        expected_mo = last_mo + 1
        expected_year = last_year
        if expected_mo > 12:
            expected_mo = 1
            expected_year += 1

        expected_first_fc = f'{expected_year}-{expected_mo:02d}'
        if fc_months[0] == expected_first_fc:
            results.record('pass', f'temporal_{road_type}_fc_continuity',
                           f'Forecast starts right after history: {fc_months[0]}')
        else:
            results.record('high', f'temporal_{road_type}_fc_continuity',
                           f'Forecast start gap/overlap',
                           f'Last history: {last_month}, First forecast: {fc_months[0]}, Expected: {expected_first_fc}')


def test_cross_file_consistency(results, forecasts):
    """Test 12: Cross-file consistency (county_roads < no_interstate < all_roads)."""
    if 'county_roads' in forecasts and 'all_roads' in forecasts:
        cr_total = forecasts['county_roads'].get('summary', {}).get('totalCrashes', 0)
        ar_total = forecasts['all_roads'].get('summary', {}).get('totalCrashes', 0)
        if cr_total < ar_total:
            results.record('pass', 'cross_county_lt_all',
                           f'county_roads ({cr_total:,}) < all_roads ({ar_total:,})')
        elif cr_total == ar_total:
            results.record('warn', 'cross_county_lt_all',
                           f'county_roads ({cr_total:,}) == all_roads ({ar_total:,}) — expected county < all')
        else:
            results.record('critical', 'cross_county_lt_all',
                           f'county_roads ({cr_total:,}) > all_roads ({ar_total:,}) — INVALID',
                           'County roads should be a subset of all roads')

    if 'no_interstate' in forecasts and 'all_roads' in forecasts:
        ni_total = forecasts['no_interstate'].get('summary', {}).get('totalCrashes', 0)
        ar_total = forecasts['all_roads'].get('summary', {}).get('totalCrashes', 0)
        if ni_total <= ar_total:
            results.record('pass', 'cross_nointerstate_le_all',
                           f'no_interstate ({ni_total:,}) <= all_roads ({ar_total:,})')
        else:
            results.record('critical', 'cross_nointerstate_le_all',
                           f'no_interstate ({ni_total:,}) > all_roads ({ar_total:,}) — INVALID')

    if 'county_roads' in forecasts and 'no_interstate' in forecasts:
        cr_total = forecasts['county_roads'].get('summary', {}).get('totalCrashes', 0)
        ni_total = forecasts['no_interstate'].get('summary', {}).get('totalCrashes', 0)
        if cr_total <= ni_total:
            results.record('pass', 'cross_county_le_nointerstate',
                           f'county_roads ({cr_total:,}) <= no_interstate ({ni_total:,})')
        else:
            results.record('critical', 'cross_county_le_nointerstate',
                           f'county_roads ({cr_total:,}) > no_interstate ({ni_total:,}) — INVALID')

    # Horizon should be consistent across all files
    horizons = {}
    for rt, fc in forecasts.items():
        horizons[rt] = fc.get('horizon', 0)
    unique_horizons = set(horizons.values())
    if len(unique_horizons) == 1:
        results.record('pass', 'cross_horizon_consistent',
                       f'Consistent horizon across all files: {unique_horizons.pop()} months')
    else:
        results.record('high', 'cross_horizon_consistent',
                       f'Inconsistent horizons: {horizons}')

    # Model should be consistent across all files
    models = {}
    for rt, fc in forecasts.items():
        models[rt] = fc.get('model', '')
    unique_models = set(models.values())
    if len(unique_models) == 1:
        results.record('pass', 'cross_model_consistent',
                       f'Consistent model: {unique_models.pop()}')
    else:
        results.record('high', 'cross_model_consistent',
                       f'Inconsistent models: {models}')


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Prediction Forecast Accuracy Test')
    parser.add_argument('--data-dir', default=DATA_DIR, help='Data directory')
    parser.add_argument('--jurisdiction', default=JURISDICTION, help='Jurisdiction name')
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    jurisdiction = args.jurisdiction

    print(f'{"="*70}')
    print(f'  PREDICTION FORECAST ACCURACY & INTEGRITY TEST')
    print(f'  Data dir: {data_dir}')
    print(f'  Jurisdiction: {jurisdiction}')
    print(f'{"="*70}\n')

    results = TestResults()

    # ── Test 1: File existence ────────────────────────────────────────────
    print('--- Test 1: File Existence ---')
    test_file_existence(results, data_dir, jurisdiction)

    # ── Load data ─────────────────────────────────────────────────────────
    forecasts = {}
    csv_stats_map = {}

    for road_type, config in ROAD_TYPES.items():
        forecast_path = os.path.join(data_dir, config['forecast_file'])
        csv_path = os.path.join(data_dir, config['csv_file'].format(jurisdiction=jurisdiction))

        if os.path.exists(forecast_path):
            forecasts[road_type] = load_forecast(forecast_path)
        if os.path.exists(csv_path):
            csv_stats_map[road_type] = load_csv_stats(csv_path)

    if not forecasts:
        print('\nERROR: No forecast files found. Cannot run remaining tests.')
        results.summary()
        return 1

    # ── Per-road-type tests ───────────────────────────────────────────────
    for road_type, forecast in forecasts.items():
        print(f'\n--- Tests for {road_type} ---')

        # Test 2: Structural integrity
        print(f'\n  [Structural Integrity]')
        test_structural_integrity(results, forecast, road_type)

        # Test 3: Summary vs CSV
        if road_type in csv_stats_map:
            print(f'\n  [Summary vs CSV]')
            test_summary_vs_csv(results, forecast, csv_stats_map[road_type], road_type)

        # Test 4: M01
        print(f'\n  [Matrix M01 - Total Crash Frequency]')
        test_matrix_m01(results, forecast, road_type)

        # Test 5: M02
        print(f'\n  [Matrix M02 - Severity-Level]')
        test_matrix_m02(results, forecast, road_type)

        # Test 6: M03
        if road_type in csv_stats_map:
            print(f'\n  [Matrix M03 - Corridors]')
            test_matrix_m03(results, forecast, csv_stats_map[road_type], road_type)

        # Test 7: M04
        print(f'\n  [Matrix M04 - Crash Types]')
        test_matrix_m04(results, forecast, road_type)

        # Test 8: M05
        print(f'\n  [Matrix M05 - Contributing Factors]')
        test_matrix_m05(results, forecast, road_type)

        # Test 9: M06
        print(f'\n  [Matrix M06 - Location Types]')
        test_matrix_m06(results, forecast, road_type)

        # Test 10: Derived metrics
        print(f'\n  [Derived Metrics]')
        test_derived_metrics(results, forecast, road_type)

        # Test 11: Temporal integrity
        if road_type in csv_stats_map:
            print(f'\n  [Temporal Integrity]')
            test_temporal_integrity(results, forecast, csv_stats_map[road_type], road_type)

    # ── Test 12: Cross-file consistency ───────────────────────────────────
    if len(forecasts) > 1:
        print(f'\n--- Cross-File Consistency ---')
        test_cross_file_consistency(results, forecasts)

    # ── Summary ───────────────────────────────────────────────────────────
    success = results.summary()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
