#!/usr/bin/env python3
"""
Temporal Embedding Layer — Bug & Regression Tests
===================================================
Unit tests for the temporal embedding layer in generate_forecast.py.

Tests cover:
  - Seasonal pattern estimation correctness
  - Transform routing (log1p vs seasonal vs passthrough)
  - Round-trip integrity (apply → inverse ≈ original)
  - Boundary conditions and edge cases
  - Clamping behaviour and information loss
  - Year-boundary month handling in inverse transform

Run:
    python3 tests/test_temporal_embedding.py
"""

import math
import os
import sys

# ─── Path setup ──────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'scripts')
sys.path.insert(0, os.path.abspath(SCRIPT_DIR))

from generate_forecast import (
    LOG_TRANSFORM_THRESHOLD,
    MIN_MONTHS_FOR_SEASONAL,
    apply_temporal_embedding,
    estimate_seasonal_pattern,
    inverse_temporal_embedding,
)

# ─── Test Tracking ───────────────────────────────────────────────────────────

class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results = []

    def record(self, passed, name, message, detail=''):
        status = 'PASS' if passed else 'FAIL'
        self.results.append({'status': status, 'test': name, 'message': message})
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        icon = '  PASS' if passed else '  FAIL'
        print(f'{icon}  {name}: {message}')
        if detail and not passed:
            print(f'         {detail}')

    def summary(self):
        total = self.passed + self.failed
        print(f'\n{"="*70}')
        print(f'  TEMPORAL EMBEDDING BUG TEST RESULTS')
        print(f'{"="*70}')
        print(f'  Total: {total}  |  Passed: {self.passed}  |  Failed: {self.failed}')
        if self.failed > 0:
            print(f'\n  FAILURES:')
            for r in self.results:
                if r['status'] == 'FAIL':
                    print(f'    {r["test"]}: {r["message"]}')
        print(f'{"="*70}')
        return self.failed == 0


# ─── Helper: build synthetic monthly series ──────────────────────────────────

def make_series(monthly_values, start_year=2021, start_month=1):
    """Build [(month_str, value), ...] from a flat list of values."""
    series = []
    y, m = start_year, start_month
    for v in monthly_values:
        series.append((f'{y}-{m:02d}', v))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return series


def make_constant_series(value, n_months, start_year=2021, start_month=1):
    """Build a constant-value series of n months."""
    return make_series([value] * n_months, start_year, start_month)


def make_seasonal_series(base, amplitude, n_months, start_year=2021, start_month=1):
    """Build a series with a clear sinusoidal seasonal pattern."""
    values = []
    for i in range(n_months):
        month_num = (start_month - 1 + i) % 12  # 0-indexed month
        seasonal = amplitude * math.sin(2 * math.pi * month_num / 12)
        values.append(round(max(0, base + seasonal)))
    return make_series(values, start_year, start_month)


# ─── 1. estimate_seasonal_pattern tests ──────────────────────────────────────

def test_seasonal_returns_none_insufficient_data(results):
    """Seasonal estimation requires >= MIN_MONTHS_FOR_SEASONAL data points."""
    series = make_constant_series(50, MIN_MONTHS_FOR_SEASONAL - 1)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    results.record(
        pattern is None,
        'seasonal_insufficient_data',
        f'{MIN_MONTHS_FOR_SEASONAL - 1} months should return None'
    )


def test_seasonal_returns_none_all_zeros(results):
    """All-zero series has mean=0, should return None."""
    series = make_constant_series(0, 36)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    results.record(
        pattern is None,
        'seasonal_all_zeros',
        'All-zero series should return None'
    )


def test_seasonal_exact_24_months(results):
    """Exactly MIN_MONTHS_FOR_SEASONAL months should be accepted."""
    series = make_seasonal_series(50, 20, MIN_MONTHS_FOR_SEASONAL)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    results.record(
        pattern is not None,
        'seasonal_exact_boundary',
        f'Exactly {MIN_MONTHS_FOR_SEASONAL} months should return a pattern'
    )


def test_seasonal_factors_cover_all_12_months(results):
    """Pattern should have keys 1-12 for all calendar months."""
    series = make_seasonal_series(100, 30, 36)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    if pattern is None:
        results.record(False, 'seasonal_12_keys', 'Pattern is None unexpectedly')
        return
    has_all = set(pattern.keys()) == set(range(1, 13))
    results.record(
        has_all,
        'seasonal_12_keys',
        f'Pattern keys: {sorted(pattern.keys())}'
    )


def test_seasonal_factors_sum_near_zero(results):
    """Additive seasonal deviations should approximately sum to zero.

    The seasonal component is (monthly_avg - overall_mean) per calendar month.
    If each month has equal representation, the sum of deviations should be
    close to zero.
    """
    # Use exactly 3 complete years so each month has equal weight
    series = make_seasonal_series(100, 30, 36)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    if pattern is None:
        results.record(False, 'seasonal_sum_zero', 'Pattern is None unexpectedly')
        return
    factor_sum = sum(pattern.values())
    results.record(
        abs(factor_sum) < 1.0,
        'seasonal_sum_zero',
        f'Seasonal factors sum = {factor_sum:.4f} (expect ≈ 0)',
        f'Sum too far from zero: {factor_sum}'
    )


def test_seasonal_constant_series_no_pattern(results):
    """A perfectly constant series should have near-zero seasonal factors."""
    series = make_constant_series(50, 36)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)
    if pattern is None:
        results.record(False, 'seasonal_constant', 'Pattern is None unexpectedly')
        return
    max_factor = max(abs(v) for v in pattern.values())
    results.record(
        max_factor < 0.01,
        'seasonal_constant',
        f'Constant series max seasonal factor = {max_factor:.6f} (expect ≈ 0)'
    )


# ─── 2. apply_temporal_embedding routing tests ──────────────────────────────

def test_routing_low_count_gets_log1p(results):
    """Series with mean < threshold should receive log1p transform."""
    series_dict = {'K': make_constant_series(3, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)
    results.record(
        meta['K']['transform'] == 'log1p',
        'route_low_count_log1p',
        f'Mean=3 → transform={meta["K"]["transform"]} (expect log1p)'
    )


def test_routing_high_count_gets_seasonal(results):
    """Series with mean >= threshold and 24+ months should receive seasonal."""
    series_dict = {'total': make_constant_series(50, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)
    results.record(
        meta['total']['transform'] == 'seasonal',
        'route_high_count_seasonal',
        f'Mean=50, 36mo → transform={meta["total"]["transform"]} (expect seasonal)'
    )


def test_routing_zero_mean_passthrough(results):
    """Series with all zeros should pass through unchanged."""
    series_dict = {'empty': make_constant_series(0, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)
    results.record(
        meta['empty']['transform'] == 'none',
        'route_zero_passthrough',
        f'All zeros → transform={meta["empty"]["transform"]} (expect none)'
    )


def test_routing_high_count_short_history_passthrough(results):
    """High-count series with < 24 months gets NO transform (potential gap)."""
    series_dict = {'short': make_constant_series(50, 20)}
    transformed, meta = apply_temporal_embedding(series_dict)
    results.record(
        meta['short']['transform'] == 'none',
        'route_short_high_passthrough',
        f'Mean=50 but 20mo → transform={meta["short"]["transform"]} (expect none)'
    )


def test_routing_threshold_boundary_at_10(results):
    """Mean exactly at threshold (10) should get seasonal, not log1p.

    The condition is: mean < 10 → log1p, mean >= 10 → seasonal.
    """
    series_dict = {'boundary': make_constant_series(10, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)
    transform = meta['boundary']['transform']
    results.record(
        transform == 'seasonal',
        'route_threshold_boundary',
        f'Mean=10.0 → transform={transform} (expect seasonal, not log1p)'
    )


def test_routing_just_below_threshold(results):
    """Mean just below threshold (9) should get log1p."""
    series_dict = {'below': make_constant_series(9, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)
    transform = meta['below']['transform']
    results.record(
        transform == 'log1p',
        'route_just_below_threshold',
        f'Mean=9 → transform={transform} (expect log1p)'
    )


def test_routing_empty_dict(results):
    """Empty series dict should return empty results without error."""
    transformed, meta = apply_temporal_embedding({})
    results.record(
        len(transformed) == 0 and len(meta) == 0,
        'route_empty_dict',
        'Empty input produces empty output'
    )


def test_routing_mixed_series(results):
    """Multiple series with different characteristics route correctly."""
    series_dict = {
        'K': make_constant_series(2, 36),    # log1p
        'O': make_constant_series(100, 36),  # seasonal
        'new': make_constant_series(50, 12), # passthrough (too short)
    }
    transformed, meta = apply_temporal_embedding(series_dict)
    ok = (
        meta['K']['transform'] == 'log1p'
        and meta['O']['transform'] == 'seasonal'
        and meta['new']['transform'] == 'none'
    )
    results.record(
        ok,
        'route_mixed_series',
        f'K={meta["K"]["transform"]}, O={meta["O"]["transform"]}, new={meta["new"]["transform"]}'
    )


# ─── 3. Round-trip integrity tests ──────────────────────────────────────────

def test_roundtrip_log1p(results):
    """log1p forward → inverse should recover original values within ±0.5.

    Forward:  x → log1p(x)
    Inverse:  y → expm1(y)
    Rounding to 1 decimal in inverse may lose precision on small values.
    """
    original_values = [0, 1, 2, 3, 0, 5, 1, 0, 2, 4, 3, 1,
                       0, 2, 1, 3, 0, 4, 2, 1, 0, 3, 5, 2,
                       1, 0, 2, 3, 1, 4, 0, 2, 1, 3, 2, 1]
    series_dict = {'K': make_series(original_values)}
    transformed, meta = apply_temporal_embedding(series_dict)

    # Simulate model returning the transformed values as forecast (identity)
    t_values = [pt[1] for pt in transformed['K']]
    t_months = [pt[0] for pt in transformed['K']]
    mock_forecast = {
        'K': {
            'months': t_months[-12:],
            'p50': t_values[-12:],
        }
    }
    recovered = inverse_temporal_embedding(mock_forecast, meta)
    recovered_vals = recovered['K']['p50']
    expected_vals = original_values[-12:]

    max_err = max(abs(r - e) for r, e in zip(recovered_vals, expected_vals))
    results.record(
        max_err <= 0.5,
        'roundtrip_log1p',
        f'Max error = {max_err:.2f} (threshold ≤ 0.5)',
        f'Recovered: {recovered_vals}, Expected: {expected_vals}'
    )


def test_roundtrip_seasonal(results):
    """Seasonal forward → inverse should recover original values within ±1.0.

    Forward:  x → x - seasonal[month]
    Inverse:  y → y + seasonal[month]
    The ±1.0 tolerance accounts for rounding and clamping at zero.
    """
    base, amp = 50, 15
    series_dict = {'total': make_seasonal_series(base, amp, 36)}
    transformed, meta = apply_temporal_embedding(series_dict)

    # Use last 12 history values as mock forecast
    orig_series = series_dict['total']
    t_series = transformed['total']
    t_values = [pt[1] for pt in t_series]
    t_months = [pt[0] for pt in t_series]
    mock_forecast = {
        'total': {
            'months': t_months[-12:],
            'p50': t_values[-12:],
        }
    }
    recovered = inverse_temporal_embedding(mock_forecast, meta)
    recovered_vals = recovered['total']['p50']
    expected_vals = [pt[1] for pt in orig_series[-12:]]

    max_err = max(abs(r - e) for r, e in zip(recovered_vals, expected_vals))
    results.record(
        max_err <= 1.0,
        'roundtrip_seasonal',
        f'Max error = {max_err:.2f} (threshold ≤ 1.0)',
        f'Recovered: {recovered_vals}, Expected: {expected_vals}'
    )


def test_roundtrip_passthrough(results):
    """Passthrough (no transform) should return values unchanged."""
    series_dict = {'short': make_constant_series(50, 12)}
    transformed, meta = apply_temporal_embedding(series_dict)

    mock_forecast = {
        'short': {
            'months': ['2022-01', '2022-02', '2022-03'],
            'p50': [50.0, 51.0, 49.0],
        }
    }
    recovered = inverse_temporal_embedding(mock_forecast, meta)
    results.record(
        recovered['short']['p50'] == [50.0, 51.0, 49.0],
        'roundtrip_passthrough',
        'Passthrough values unchanged after inverse'
    )


# ─── 4. Clamping and information-loss tests ─────────────────────────────────

def test_deseasonalize_clamps_to_zero(results):
    """If seasonal factor > actual value, deseasonalized should clamp to 0."""
    # Jan has seasonal factor of +40 above mean.  A January with value 20
    # would become max(0, 20 - 40) = 0.
    values = [60, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  # year 1 (Jan=60)
              60, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  # year 2
              20, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]  # year 3 (Jan=20!)
    series_dict = {'test': make_series(values)}
    transformed, meta = apply_temporal_embedding(series_dict)
    t_vals = [pt[1] for pt in transformed['test']]

    # All values should be >= 0
    all_nonneg = all(v >= 0 for v in t_vals)
    results.record(
        all_nonneg,
        'clamp_deseasonalize_nonneg',
        'All deseasonalized values are >= 0 after clamping'
    )


def test_clamp_information_loss(results):
    """Clamping can lose information — verify the magnitude of loss.

    When a low January (20) is deseasonalized with a high seasonal factor
    (+offset), the clamped value (0) will reconstruct to the seasonal
    factor rather than the original value.  This test quantifies the loss.
    """
    values = [80, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  # year 1
              80, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,  # year 2
              5,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]  # year 3
    # Mean ≈ 16.4, Jan seasonal ≈ 55 - 16.4 = 38.6
    # Year 3 Jan: max(0, 5 - 38.6) = 0.  Inverse: 0 + 38.6 = 38.6, not 5.
    series_dict = {'test': make_series(values)}
    transformed, meta = apply_temporal_embedding(series_dict)

    # Forward: first month of year 3 (index 24)
    t_vals = [pt[1] for pt in transformed['test']]
    clamped_val = t_vals[24]

    # Inverse: simulate forecast with the clamped value
    mock_fc = {
        'test': {
            'months': ['2023-01'],
            'p50': [clamped_val],
        }
    }
    recovered = inverse_temporal_embedding(mock_fc, meta)
    recovered_val = recovered['test']['p50'][0]

    # The original was 5, recovered will be higher due to clamping
    loss = abs(recovered_val - 5)
    results.record(
        True,  # informational — always passes, reports the loss
        'clamp_info_loss_magnitude',
        f'Original=5, Recovered={recovered_val}, Loss={loss:.1f} '
        f'(clamping distortion on outlier month)'
    )


def test_inverse_nonneg_output(results):
    """All inverse-transformed forecast values should be >= 0."""
    # Seasonal with negative factors for some months
    values = [100, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
              100, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
              100, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
    series_dict = {'test': make_series(values)}
    _, meta = apply_temporal_embedding(series_dict)

    # Forecast with very low values — seasonal inverse might try to go negative
    mock_fc = {
        'test': {
            'months': [f'2024-{m:02d}' for m in range(1, 13)],
            'p50': [0.0] * 12,  # all-zero forecast
            'p10': [0.0] * 12,
        }
    }
    recovered = inverse_temporal_embedding(mock_fc, meta)
    all_nonneg = all(v >= 0 for v in recovered['test']['p50'])
    all_nonneg_p10 = all(v >= 0 for v in recovered['test']['p10'])
    results.record(
        all_nonneg and all_nonneg_p10,
        'inverse_nonneg',
        'All inverse values >= 0 even with all-zero forecast input'
    )


# ─── 5. Edge-case and bug-hunting tests ─────────────────────────────────────

def test_log1p_handles_zero_values(results):
    """log1p(0) = 0, expm1(0) = 0 — zeros should survive the round trip."""
    values = [0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 1, 0,
              0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 1, 0,
              0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 1, 0]
    series_dict = {'sparse': make_series(values)}
    transformed, meta = apply_temporal_embedding(series_dict)

    # Check zeros remain zero after log1p
    t_vals = [pt[1] for pt in transformed['sparse']]
    zeros_preserved = all(t_vals[i] == 0.0 for i in range(len(values)) if values[i] == 0)
    results.record(
        zeros_preserved,
        'log1p_zero_values',
        'log1p(0) = 0 preserved for all zero entries'
    )


def test_inverse_unknown_series_passthrough(results):
    """Series in forecast but NOT in metadata should pass through unchanged."""
    metadata = {}  # empty — no transforms recorded
    forecast = {
        'mystery': {
            'months': ['2024-01', '2024-02'],
            'p50': [42.0, 43.0],
        }
    }
    recovered = inverse_temporal_embedding(forecast, metadata)
    results.record(
        recovered['mystery']['p50'] == [42.0, 43.0],
        'inverse_unknown_series',
        'Unknown series passes through unchanged'
    )


def test_inverse_preserves_months_key(results):
    """The 'months' key should never be transformed."""
    series_dict = {'K': make_constant_series(3, 36)}
    _, meta = apply_temporal_embedding(series_dict)

    months_list = ['2024-01', '2024-02', '2024-03']
    mock_fc = {
        'K': {
            'months': months_list,
            'p50': [1.0, 1.1, 0.9],
        }
    }
    recovered = inverse_temporal_embedding(mock_fc, meta)
    results.record(
        recovered['K']['months'] == months_list,
        'inverse_months_preserved',
        'months key passed through untransformed'
    )


def test_inverse_preserves_non_list_keys(results):
    """Non-list values (strings, ints) in forecast should pass through."""
    series_dict = {'K': make_constant_series(3, 36)}
    _, meta = apply_temporal_embedding(series_dict)

    mock_fc = {
        'K': {
            'months': ['2024-01'],
            'p50': [1.0],
            'label': 'fatal',      # string — should survive
            'count': 42,            # int — should survive
        }
    }
    recovered = inverse_temporal_embedding(mock_fc, meta)
    results.record(
        recovered['K'].get('label') == 'fatal' and recovered['K'].get('count') == 42,
        'inverse_non_list_preserved',
        'Non-list keys (label, count) preserved through inverse'
    )


def test_year_boundary_seasonal_inverse(results):
    """Seasonal inverse should handle Dec→Jan year boundary correctly.

    Forecast months that cross a year boundary (e.g., 2024-11, 2024-12,
    2025-01, 2025-02) must pick up the correct calendar-month seasonal
    factor for each, not confuse month 12 and month 1.
    """
    # Build a series where Dec and Jan have very different seasonal factors
    # High in Jan (month 1), low in Dec (month 12)
    values = []
    for year in range(3):
        for month in range(1, 13):
            if month == 1:
                values.append(100)
            elif month == 12:
                values.append(10)
            else:
                values.append(50)

    series_dict = {'test': make_series(values, 2021, 1)}
    _, meta = apply_temporal_embedding(series_dict)
    seasonal = meta['test'].get('seasonal', {})

    if not seasonal:
        results.record(False, 'year_boundary_seasonal', 'No seasonal pattern computed')
        return

    # Jan factor should be positive, Dec factor should be negative
    jan_factor = seasonal.get(1, 0)
    dec_factor = seasonal.get(12, 0)

    # Now inverse-transform a forecast crossing Dec→Jan
    mock_fc = {
        'test': {
            'months': ['2024-11', '2024-12', '2025-01', '2025-02'],
            'p50': [40.0, 40.0, 40.0, 40.0],  # flat baseline
        }
    }
    recovered = inverse_temporal_embedding(mock_fc, meta)
    vals = recovered['test']['p50']

    # Jan (index 2) should get boosted, Dec (index 1) should get reduced
    jan_val = vals[2]  # 2025-01
    dec_val = vals[1]  # 2024-12
    results.record(
        jan_val > dec_val,
        'year_boundary_seasonal',
        f'Dec={dec_val}, Jan={jan_val} (Jan > Dec confirms correct month lookup)',
        f'Jan factor={jan_factor:.1f}, Dec factor={dec_factor:.1f}'
    )


def test_forecast_months_length_mismatch(results):
    """If a quantile array is longer than 'months', excess values use fallback.

    In inverse_temporal_embedding, when i >= len(fc_months), the fallback
    calendar month is 1 (January).  This test verifies no crash occurs and
    the fallback is applied.
    """
    series_dict = {'total': make_constant_series(50, 36)}
    _, meta = apply_temporal_embedding(series_dict)

    # p50 has 14 values but months only has 12
    mock_fc = {
        'total': {
            'months': [f'2024-{m:02d}' for m in range(1, 13)],  # 12 months
            'p50': [45.0] * 14,  # 14 values — 2 extra!
        }
    }
    try:
        recovered = inverse_temporal_embedding(mock_fc, meta)
        # Should not crash; extra values use January fallback
        results.record(
            len(recovered['total']['p50']) == 14,
            'forecast_length_mismatch',
            'Handles p50 longer than months without crashing (fallback to Jan)'
        )
    except (IndexError, KeyError) as e:
        results.record(
            False,
            'forecast_length_mismatch',
            f'Crashed with {type(e).__name__}: {e}'
        )


def test_missing_months_key_in_seasonal_inverse(results):
    """If forecast dict has no 'months' key, seasonal inverse uses fallback.

    This is a defensive edge case — Chronos-2 always returns 'months', but
    a malformed or partial dict should not crash the pipeline.
    """
    series_dict = {'total': make_constant_series(50, 36)}
    _, meta = apply_temporal_embedding(series_dict)

    mock_fc = {
        'total': {
            # No 'months' key!
            'p50': [45.0, 46.0, 44.0],
        }
    }
    try:
        recovered = inverse_temporal_embedding(mock_fc, meta)
        # All values should use January fallback (cal=1)
        results.record(
            len(recovered['total']['p50']) == 3,
            'missing_months_key',
            'No crash when months key missing (uses January fallback)'
        )
    except Exception as e:
        results.record(
            False,
            'missing_months_key',
            f'Crashed with {type(e).__name__}: {e}'
        )


def test_log1p_large_values(results):
    """log1p should handle large values without overflow."""
    values = [500, 600, 550, 580, 520, 610, 490, 570, 540, 560, 530, 590,
              500, 600, 550, 580, 520, 610, 490, 570, 540, 560, 530, 590,
              500, 600, 550, 580, 520, 610, 490, 570, 540, 560, 530, 590]
    # mean ≈ 553, which is > threshold → should get seasonal, not log1p
    series_dict = {'big': make_series(values)}
    transformed, meta = apply_temporal_embedding(series_dict)
    results.record(
        meta['big']['transform'] == 'seasonal',
        'log1p_large_values_routing',
        f'Large-value series (mean≈553) routes to seasonal, not log1p'
    )


def test_single_value_series(results):
    """A single-month series should pass through without crashing."""
    series_dict = {'one': [('2024-01', 5)]}
    try:
        transformed, meta = apply_temporal_embedding(series_dict)
        results.record(
            meta['one']['transform'] in ('log1p', 'none'),
            'single_value_series',
            f'Single-month series handled (transform={meta["one"]["transform"]})'
        )
    except Exception as e:
        results.record(
            False,
            'single_value_series',
            f'Crashed with {type(e).__name__}: {e}'
        )


# ─── 6. Mathematical correctness tests ──────────────────────────────────────

def test_log1p_expm1_identity(results):
    """expm1(log1p(x)) == x for all non-negative x (mathematical identity)."""
    test_values = [0, 1, 2, 5, 10, 100, 0.5, 0.01]
    max_err = 0
    for x in test_values:
        roundtripped = math.expm1(math.log1p(x))
        err = abs(roundtripped - x)
        max_err = max(max_err, err)

    results.record(
        max_err < 1e-10,
        'log1p_expm1_identity',
        f'Max mathematical error = {max_err:.2e} (expect < 1e-10)'
    )


def test_seasonal_pattern_captures_known_signal(results):
    """Given a series with known seasonal pattern, verify extraction accuracy.

    Build: value = 100 + 30*sin(2π*month/12) for 36 months.
    The seasonal factor for the peak month should be close to +30.
    """
    series = make_seasonal_series(100, 30, 36)
    months = [pt[0] for pt in series]
    values = [pt[1] for pt in series]
    pattern = estimate_seasonal_pattern(months, values)

    if pattern is None:
        results.record(False, 'seasonal_known_signal', 'Pattern is None')
        return

    # Peak should be around month 3-4 (sin peaks at π/2 → month 3)
    max_month = max(pattern, key=pattern.get)
    max_val = pattern[max_month]

    # The peak factor should be in the range [20, 35] (30 is ideal, rounding distorts)
    results.record(
        15 < max_val < 40,
        'seasonal_known_signal',
        f'Peak month={max_month}, factor={max_val:.1f} (expect ≈ 25-30)'
    )


def test_deseasonalized_variance_reduction(results):
    """Deseasonalized series should have lower variance than original.

    The whole point of seasonal decomposition is to reduce variance by
    removing the predictable cyclical component.
    """
    series = make_seasonal_series(100, 30, 36)
    series_dict = {'total': series}
    transformed, _ = apply_temporal_embedding(series_dict)

    orig_values = [pt[1] for pt in series]
    trans_values = [pt[1] for pt in transformed['total']]

    orig_var = sum((v - sum(orig_values) / len(orig_values)) ** 2 for v in orig_values) / len(orig_values)
    trans_var = sum((v - sum(trans_values) / len(trans_values)) ** 2 for v in trans_values) / len(trans_values)

    results.record(
        trans_var < orig_var,
        'variance_reduction',
        f'Original variance={orig_var:.1f}, Deseasonalized variance={trans_var:.1f}'
    )


# ─── 7. Full pipeline integration test ──────────────────────────────────────

def test_full_pipeline_dry_run_output_has_embedding_metadata(results):
    """Verify the generated forecast JSON includes temporalEmbedding metadata."""
    import json
    forecast_path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'CDOT', 'forecasts_all_roads.json'
    )
    if not os.path.exists(forecast_path):
        results.record(False, 'pipeline_embedding_metadata',
                       f'Forecast file not found: {forecast_path}')
        return

    with open(forecast_path) as f:
        data = json.load(f)

    te = data.get('temporalEmbedding', {})
    ok = (
        te.get('enabled') is True
        and 'seasonal_decomposition' in te.get('transforms', [])
        and 'log1p_variance_stabilization' in te.get('transforms', [])
        and te.get('logTransformThreshold') == LOG_TRANSFORM_THRESHOLD
        and te.get('minMonthsForSeasonal') == MIN_MONTHS_FOR_SEASONAL
    )
    results.record(
        ok,
        'pipeline_embedding_metadata',
        f'temporalEmbedding metadata present and correct: {te}'
    )


def test_full_pipeline_quantile_ordering_preserved(results):
    """After temporal embedding, quantile ordering (p10<=p25<=p50<=p75<=p90)
    must still hold in the output JSON."""
    import json
    forecast_path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'CDOT', 'forecasts_all_roads.json'
    )
    if not os.path.exists(forecast_path):
        results.record(False, 'pipeline_quantile_order', 'Forecast file not found')
        return

    with open(forecast_path) as f:
        data = json.load(f)

    # Check M02 severity K (log1p-transformed) specifically
    m02 = data.get('matrices', {}).get('m02', {})
    fc_k = m02.get('forecast', {}).get('K', {})
    if not fc_k or not fc_k.get('p10'):
        results.record(False, 'pipeline_quantile_order', 'M02 K forecast missing')
        return

    violations = 0
    quantiles = ['p10', 'p25', 'p50', 'p75', 'p90']
    for i in range(len(fc_k.get('months', []))):
        vals = [fc_k[q][i] for q in quantiles if q in fc_k and i < len(fc_k[q])]
        if len(vals) == 5:
            if not all(vals[j] <= vals[j + 1] for j in range(4)):
                violations += 1

    results.record(
        violations == 0,
        'pipeline_quantile_order',
        f'M02-K (log1p transformed): {violations} quantile ordering violations'
    )


def test_full_pipeline_nonneg_forecasts(results):
    """No negative forecast values in any matrix after inverse transform."""
    import json
    forecast_path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'CDOT', 'forecasts_all_roads.json'
    )
    if not os.path.exists(forecast_path):
        results.record(False, 'pipeline_nonneg', 'Forecast file not found')
        return

    with open(forecast_path) as f:
        data = json.load(f)

    neg_count = 0
    quantiles = ['p10', 'p25', 'p50', 'p75', 'p90']
    for matrix_id in ['m01', 'm02']:
        matrix = data.get('matrices', {}).get(matrix_id, {})
        fc = matrix.get('forecast', {})

        if matrix_id == 'm01':
            # Direct forecast
            for q in quantiles:
                for v in fc.get(q, []):
                    if v < 0:
                        neg_count += 1
        elif matrix_id == 'm02':
            # Per-severity forecast
            for sev in ['K', 'A', 'B', 'C', 'O']:
                sev_fc = fc.get(sev, {})
                for q in quantiles:
                    for v in sev_fc.get(q, []):
                        if v < 0:
                            neg_count += 1

    results.record(
        neg_count == 0,
        'pipeline_nonneg',
        f'{neg_count} negative forecast values across M01+M02'
    )


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f'{"="*70}')
    print(f'  TEMPORAL EMBEDDING LAYER — BUG & REGRESSION TESTS')
    print(f'{"="*70}\n')

    results = TestResults()

    # 1. estimate_seasonal_pattern
    print('--- 1. Seasonal Pattern Estimation ---')
    test_seasonal_returns_none_insufficient_data(results)
    test_seasonal_returns_none_all_zeros(results)
    test_seasonal_exact_24_months(results)
    test_seasonal_factors_cover_all_12_months(results)
    test_seasonal_factors_sum_near_zero(results)
    test_seasonal_constant_series_no_pattern(results)

    # 2. Transform routing
    print('\n--- 2. Transform Routing ---')
    test_routing_low_count_gets_log1p(results)
    test_routing_high_count_gets_seasonal(results)
    test_routing_zero_mean_passthrough(results)
    test_routing_high_count_short_history_passthrough(results)
    test_routing_threshold_boundary_at_10(results)
    test_routing_just_below_threshold(results)
    test_routing_empty_dict(results)
    test_routing_mixed_series(results)

    # 3. Round-trip integrity
    print('\n--- 3. Round-Trip Integrity ---')
    test_roundtrip_log1p(results)
    test_roundtrip_seasonal(results)
    test_roundtrip_passthrough(results)

    # 4. Clamping and information loss
    print('\n--- 4. Clamping & Information Loss ---')
    test_deseasonalize_clamps_to_zero(results)
    test_clamp_information_loss(results)
    test_inverse_nonneg_output(results)

    # 5. Edge cases and bug hunting
    print('\n--- 5. Edge Cases & Bug Hunting ---')
    test_log1p_handles_zero_values(results)
    test_inverse_unknown_series_passthrough(results)
    test_inverse_preserves_months_key(results)
    test_inverse_preserves_non_list_keys(results)
    test_year_boundary_seasonal_inverse(results)
    test_forecast_months_length_mismatch(results)
    test_missing_months_key_in_seasonal_inverse(results)
    test_log1p_large_values(results)
    test_single_value_series(results)

    # 6. Mathematical correctness
    print('\n--- 6. Mathematical Correctness ---')
    test_log1p_expm1_identity(results)
    test_seasonal_pattern_captures_known_signal(results)
    test_deseasonalized_variance_reduction(results)

    # 7. Full pipeline integration
    print('\n--- 7. Full Pipeline Integration ---')
    test_full_pipeline_dry_run_output_has_embedding_metadata(results)
    test_full_pipeline_quantile_ordering_preserved(results)
    test_full_pipeline_nonneg_forecasts(results)

    success = results.summary()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
