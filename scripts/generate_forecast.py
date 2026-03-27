#!/usr/bin/env python3
"""
Generate crash forecasts using Chronos-2 on Amazon SageMaker.

Jurisdiction-agnostic forecast generator for CRASH LENS. Reads crash data
from local files or R2 CDN, aggregates into monthly time series, sends to
the Chronos-2 SageMaker endpoint, and writes forecast JSONs for the Crash
Prediction tab.

Each DOT's prediction data is stored alongside its source data (e.g., data/VDOT/).
EPDO weights are loaded from state-specific config at states/{state}/config.json.

Temporal Embedding Layer
~~~~~~~~~~~~~~~~~~~~~~~~
Before inference, a temporal embedding layer transforms each series to
improve Chronos-2 prediction accuracy:

  - **Seasonal decomposition** (additive) for high-count series (≥ 10/month)
    with 2+ years of history.  Removes the 12-month cyclical pattern so
    Chronos-2 can focus on trend and irregular components.
  - **Log1p variance stabilization** for low-count series (< 10/month, e.g.
    K and A severity crashes).  Compresses the range of sparse counts so the
    model treats small differences proportionally.

Both transforms are automatically reversed after prediction; the output JSON
structure is unchanged.

Prediction Matrices
~~~~~~~~~~~~~~~~~~~
  M01: Total Crash Frequency (county-wide)
  M02: Severity-Level Multivariate (K/A/B/C/O)
  M03: Corridor Cross-Learning (top 10 routes)
  M04: Crash Type Distribution (rear-end, angle, etc.)
  M05: Contributing Factor Trends (speed, alcohol, ped, bike)
  M06: Intersection vs Segment

Usage:
    python scripts/generate_forecast.py --state virginia --all-road-types --jurisdiction henrico --data-dir data/VDOT
    python scripts/generate_forecast.py --state colorado --all-road-types --jurisdiction douglas --data-dir data/CDOT --source r2
    python scripts/generate_forecast.py --state virginia --data data/VDOT/henrico_all_roads.csv --output data/VDOT/forecasts.json

Environment Variables:
    AWS_ACCESS_KEY_ID       - IAM user access key
    AWS_SECRET_ACCESS_KEY   - IAM user secret key
    AWS_REGION              - AWS region (default: us-east-1)
"""

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas")
    sys.exit(1)

# ============================================================
# Configuration
# ============================================================

ENDPOINT_NAME = "crashlens-chronos2-endpoint"
QUANTILE_LEVELS = [0.1, 0.25, 0.5, 0.75, 0.9]
DEFAULT_HORIZON = 12  # months

# EPDO weights — loaded from state config, falls back to HSM standard
def load_epdo_weights(config_path=None):
    """Load EPDO weights from state config JSON file."""
    default_weights = {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}
    if config_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, "data", "CDOT", "config.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        weights = config.get("epdoWeights", default_weights)
        for key in ["K", "A", "B", "C", "O"]:
            if key not in weights:
                weights[key] = default_weights[key]
        print(f"[Config] EPDO weights loaded from {config_path}: {weights}")
        return weights
    except Exception as e:
        print(f"[Config] Using default EPDO weights: {e}")
        return default_weights

EPDO_WEIGHTS = {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}  # Default (FHWA 2025); overridden in main() from state config


def download_from_r2(state, jurisdiction, data_dir):
    """Download validated road-type CSVs from R2 CDN into data_dir.

    R2 objects are stored with Content-Encoding: gzip for storage savings.
    This function handles transparent decompression for gzipped responses
    (urllib does not auto-decompress Content-Encoding: gzip like browsers do).
    """
    import gzip as gz
    import io
    import urllib.request
    CDN_BASE = "https://data.aicreatesai.com"
    os.makedirs(data_dir, exist_ok=True)

    for rt in ["county_roads", "city_roads", "no_interstate", "all_roads"]:
        url = f"{CDN_BASE}/{state}/{jurisdiction}/{rt}.csv"
        local_path = os.path.join(data_dir, f"{jurisdiction}_{rt}.csv")
        print(f"  Downloading {url} → {local_path}")
        try:
            req = urllib.request.Request(url)
            req.add_header("Accept-Encoding", "gzip, identity")
            with urllib.request.urlopen(req) as resp:
                encoding = resp.headers.get("Content-Encoding", "")
                raw = resp.read()
                if encoding == "gzip":
                    raw = gz.decompress(raw)
                with open(local_path, "wb") as f:
                    f.write(raw)
            size = os.path.getsize(local_path)
            print(f"    OK ({size:,} bytes, encoding={encoding or 'identity'})")
        except Exception as e:
            print(f"    SKIP: {e}")


def check_sagemaker_endpoint(session, endpoint_name="crashlens-chronos2-endpoint"):
    """Verify SageMaker endpoint is available before running forecasts."""
    sm = session.client("sagemaker")
    try:
        resp = sm.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        if status != "InService":
            print(f"ERROR: SageMaker endpoint '{endpoint_name}' is {status}, not InService")
            sys.exit(1)
        print(f"[SageMaker] Endpoint '{endpoint_name}' is InService")
    except Exception as e:
        print(f"ERROR: Cannot reach SageMaker endpoint: {e}")
        sys.exit(1)


# Will be populated dynamically from data via auto_detect_top_corridors()
TOP_CORRIDORS = []


def auto_detect_top_corridors(df, top_n=10):
    """Auto-detect top corridors by crash volume from the data.

    Finds the top N routes by crash count, filtering out empty/unknown values.
    Works for any jurisdiction/state — no hardcoded route names required.

    Args:
        df: DataFrame with crash records
        top_n: Number of top corridors to return (default: 10)

    Returns:
        list of route name strings, sorted by crash volume descending
    """
    if "RTE Name" in df.columns:
        route_col = "RTE Name"
    elif "RTE_NAME" in df.columns:
        route_col = "RTE_NAME"
    else:
        print("  [AutoDetect] No route column found. Returning empty corridor list.")
        return []

    # Count crashes per route, filter out empty/unknown
    route_counts = df[route_col].value_counts()
    route_counts = route_counts[
        ~route_counts.index.isin(["", "Unknown", "UNKNOWN", "N/A", "NA", "None"])
    ]
    route_counts = route_counts[route_counts.index.notna()]

    if route_counts.empty:
        print("  [AutoDetect] No valid routes found. Returning empty corridor list.")
        return []

    top_routes = route_counts.head(top_n).index.tolist()
    print(f"  [AutoDetect] Top {len(top_routes)} corridors by crash volume:")
    for i, route in enumerate(top_routes, 1):
        print(f"    {i:2d}. {route} ({route_counts[route]:,} crashes)")

    return top_routes

# Crash type mapping — supports both Colorado-numbered and Virginia label formats
CRASH_TYPE_MAP = {
    # Colorado (numbered prefix) formats
    "1. Rear End": "rear_end",
    "2. Broadside": "angle",
    "2. Angle": "angle",
    "9. Fixed Object - Off Road": "fixed_object",
    "9. Fixed Object": "fixed_object",
    "4. Sideswipe - Same Direction": "sideswipe_same",
    "4. Sideswipe Same": "sideswipe_same",
    "10. Deer/Animal": "animal",
    "3. Head On": "head_on",
    # Virginia (plain label) formats — from TREDS Collision Type field
    "Rear End": "rear_end",
    "Angle": "angle",
    "Fixed Object - Off Road": "fixed_object",
    "Fixed Object in Road": "fixed_object",
    "Sideswipe - Same Direction": "sideswipe_same",
    "Sideswipe - Opposite Direction": "sideswipe_opposite",
    "Head On": "head_on",
    "Pedestrian": "pedestrian",
    "Bicyclist": "bicycle",
    "Other Animal": "animal",
    "Deer": "animal",
    "Non-Collision": "non_collision",
    "Backed Into": "backed_into",
    "Other": "other",
}

# Intersection type mapping — supports both Colorado-numbered and Virginia label formats
INTERSECTION_TYPE_MAP = {
    # Colorado (numbered prefix) formats
    "1. Not at Intersection": "segment",
    "4. Four Approaches": "four_leg",
    "2. Two Approaches": "three_leg",
    "5. Roundabout": "roundabout",
    # Virginia (plain label) formats — from TREDS Intersection Type / Roadway Description
    "Non-Intersection": "segment",
    "Intersection": "four_leg",
    "Driveway": "segment",
    "Ramp": "segment",
    "Roundabout": "roundabout",
    "Railroad Crossing": "segment",
}


def load_crash_data(csv_path):
    """Load and parse crash CSV data."""
    print(f"Loading crash data from {csv_path}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  Loaded {len(df):,} crash records")

    # Parse dates
    df["date"] = pd.to_datetime(df["Crash Date"], format="mixed", errors="coerce")
    df = df.dropna(subset=["date"])

    # Ensure severity column
    if "Crash Severity" in df.columns:
        df["severity"] = df["Crash Severity"].str.strip()
    else:
        df["severity"] = "O"

    # Extract month period
    df["month"] = df["date"].dt.to_period("M")

    print(f"  Date range: {df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
    print(f"  Months: {df['month'].nunique()}")
    return df


def build_monthly_series(df, group_col=None, value_col=None, filter_func=None,
                         full_month_range=None):
    """Build monthly time series from crash data.

    Args:
        df: DataFrame with crash records
        group_col: Column to group by (creates multiple series)
        value_col: Column to count distinct values of (default: count rows)
        filter_func: Optional function to filter df before aggregation
        full_month_range: Optional sorted list of all Period months from the
            parent dataset.  When provided, ensures zero-count months are
            included in the output series (critical for sparse series like
            bike and pedestrian crashes).

    Returns:
        dict: {series_id: [(month_str, count), ...]}
    """
    if filter_func:
        df = filter_func(df)

    if group_col:
        groups = df.groupby([group_col, "month"]).size().reset_index(name="count")
        # Ensure all months present for each group
        all_months = full_month_range if full_month_range is not None else sorted(df["month"].unique())
        result = {}
        for group_name in groups[group_col].unique():
            group_data = groups[groups[group_col] == group_name]
            month_counts = dict(zip(group_data["month"], group_data["count"]))
            series = [(str(m), month_counts.get(m, 0)) for m in all_months]
            result[str(group_name)] = series
        return result
    else:
        monthly = df.groupby("month").size().reset_index(name="count")
        all_months = full_month_range if full_month_range is not None else sorted(df["month"].unique())
        month_counts = dict(zip(monthly["month"], monthly["count"]))
        series = [(str(m), month_counts.get(m, 0)) for m in all_months]
        return {"total": series}


def calc_epdo(severity_counts):
    """Calculate EPDO score from severity counts."""
    return sum(
        severity_counts.get(s, 0) * w for s, w in EPDO_WEIGHTS.items()
    )


def invoke_endpoint(session, series_dict, horizon):
    """Send time series to SageMaker endpoint and get forecasts.

    Args:
        session: boto3 Session
        series_dict: {series_id: [(month, count), ...]}
        horizon: prediction length in months

    Returns:
        dict: {series_id: {quantile: [values], months: [month_strs]}}
    """
    runtime = session.client("sagemaker-runtime")

    # Build payload
    series_ids = list(series_dict.keys())
    inputs = []
    for sid in series_ids:
        target = [point[1] for point in series_dict[sid]]
        inputs.append({"target": target})

    payload = {
        "inputs": inputs,
        "parameters": {
            "prediction_length": horizon,
            "quantile_levels": QUANTILE_LEVELS,
        },
    }

    print(f"  Sending {len(inputs)} series to endpoint (horizon={horizon})...")
    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="application/json",
        Body=json.dumps(payload),
    )

    result = json.loads(response["Body"].read().decode("utf-8"))
    predictions = result.get("predictions", {})

    # Parse response — generate future month labels
    last_month_str = series_dict[series_ids[0]][-1][0]  # e.g., "2025-08"
    last_date = pd.Period(last_month_str, freq="M")
    future_months = [str(last_date + i + 1) for i in range(horizon)]

    forecasts = {}
    for idx, sid in enumerate(series_ids):
        forecast = {"months": future_months}
        for q_key, q_values in predictions.items():
            if q_key == "mean":
                forecast["mean"] = q_values[idx] if idx < len(q_values) else []
            else:
                forecast[f"p{int(float(q_key)*100)}"] = (
                    q_values[idx] if idx < len(q_values) else []
                )
        forecasts[sid] = forecast

    return forecasts


def generate_synthetic_forecast(series_dict, horizon):
    """Generate synthetic forecasts using Holt-Winters exponential smoothing.

    Produces statistically grounded forecasts with proper prediction
    intervals based on historical residual variance, regression-to-mean
    dampening for low-count series, and widening CIs over the horizon.
    """
    import random
    random.seed(42)

    forecasts = {}
    for sid, series in series_dict.items():
        values = [point[1] for point in series]
        if not values:
            continue

        n = len(values)
        mean_val = sum(values) / n if n else 0
        long_run_mean = mean_val

        # --- Holt-Winters double exponential smoothing ---
        # Additive trend + additive seasonality (period=12)
        period = 12

        # Calculate monthly seasonal indices (additive)
        seasonal_indices = [0.0] * period
        if n >= 2 * period:
            monthly_buckets = defaultdict(list)
            for month_str, count in series:
                m = int(month_str.split("-")[1]) - 1  # 0-indexed
                monthly_buckets[m].append(count)
            for m in range(period):
                if monthly_buckets[m]:
                    seasonal_indices[m] = sum(monthly_buckets[m]) / len(monthly_buckets[m]) - mean_val

        # Initialize level and trend from last 12 months
        if n >= period:
            # Deseasonalize for initialization
            deseas = []
            for month_str, v in series:
                m = int(month_str.split("-")[1]) - 1
                deseas.append(v - seasonal_indices[m])
            level = sum(deseas[-period:]) / period
            if n >= 2 * period:
                prev_level = sum(deseas[-2 * period:-period]) / period
                trend = (level - prev_level) / period
            else:
                trend = 0
        else:
            level = mean_val
            trend = 0

        # Smoothing parameters
        alpha = 0.3  # level
        beta = 0.1   # trend
        gamma = 0.15  # seasonal

        # Run Holt-Winters on historical data to get fitted values and residuals
        fitted = []
        hw_level = level if n < period else sum(values[:period]) / period
        hw_trend = trend
        hw_seasonal = list(seasonal_indices)

        # Initialize from first period
        if n >= period:
            hw_level = sum(values[:period]) / period
            if n >= 2 * period:
                hw_trend = sum(values[period:2*period]) / period - hw_level
                hw_trend /= period
            for i in range(period):
                hw_seasonal[i] = values[i] - hw_level

        # Forward pass
        for i, (month_str, v) in enumerate(series):
            m = int(month_str.split("-")[1]) - 1
            if i < period:
                fitted.append(hw_level + hw_seasonal[m])
                continue
            forecast_val = hw_level + hw_trend + hw_seasonal[m]
            fitted.append(forecast_val)
            # Update
            new_level = alpha * (v - hw_seasonal[m]) + (1 - alpha) * (hw_level + hw_trend)
            new_trend = beta * (new_level - hw_level) + (1 - beta) * hw_trend
            hw_seasonal[m] = gamma * (v - new_level) + (1 - gamma) * hw_seasonal[m]
            hw_level = new_level
            hw_trend = new_trend

        # Calculate residual standard error (for prediction intervals)
        if len(fitted) > period:
            residuals = [values[i] - fitted[i] for i in range(period, n)]
            residual_var = sum(r ** 2 for r in residuals) / max(len(residuals) - 1, 1)
            residual_se = residual_var ** 0.5
        else:
            residual_se = (sum((v - mean_val) ** 2 for v in values) / max(n - 1, 1)) ** 0.5

        # Regression-to-mean dampening for low-count series
        # Shrink recent trend toward long-run mean based on data availability
        shrinkage = min(1.0, n / 36.0)  # Full weight only with 3+ years of data
        dampened_level = shrinkage * hw_level + (1 - shrinkage) * long_run_mean
        dampened_trend = shrinkage * hw_trend  # Trend shrinks toward zero

        # Generate future months
        last_month_str = series[-1][0]
        last_date = pd.Period(last_month_str, freq="M")
        future_months = [str(last_date + i + 1) for i in range(horizon)]

        # Generate quantile forecasts with widening intervals
        p50 = []
        p10 = []
        p25 = []
        p75 = []
        p90 = []
        means = []

        for i, fm in enumerate(future_months):
            month_num = int(fm.split("-")[1]) - 1
            # Point forecast: dampened level + dampened trend + seasonal
            base = dampened_level + dampened_trend * (i + 1) + hw_seasonal[month_num]
            base = max(0, base)
            p50.append(round(base, 1))

            # Prediction interval widens with sqrt(h) — standard for additive models
            h_factor = (1 + i) ** 0.5
            se_h = residual_se * h_factor

            # Quantile offsets (normal approximation)
            p10.append(round(max(0, base - 1.282 * se_h), 1))
            p25.append(round(max(0, base - 0.674 * se_h), 1))
            p75.append(round(base + 0.674 * se_h, 1))
            p90.append(round(base + 1.282 * se_h, 1))
            means.append(round(base + random.gauss(0, se_h * 0.02), 1))

        forecast = {
            "months": future_months,
            "p10": p10,
            "p25": p25,
            "p50": p50,
            "p75": p75,
            "p90": p90,
            "mean": means,
        }
        forecasts[sid] = forecast

    return forecasts


# ============================================================
# Temporal Embedding Layer
# ============================================================
# Pre-model transformation layer to improve Chronos-2 forecast accuracy.
#
# Two transforms are applied based on series characteristics:
#   1. Seasonal decomposition (additive) for high-count series with 2+ years
#      of history.  Removes known 12-month cyclical patterns so Chronos-2
#      can focus on trend and irregular components.
#   2. Log1p variance stabilization for low-count series (mean < 10/month).
#      Compresses the range of sparse counts (e.g., fatal crashes: 0,3,0,1)
#      so the model treats small differences proportionally.
#
# Both transforms are automatically reversed after prediction, producing
# forecasts in the original count scale.  The output JSON is unchanged.

LOG_TRANSFORM_THRESHOLD = 10   # series with mean below this get log1p
MIN_MONTHS_FOR_SEASONAL = 24   # need 2+ full years for seasonal estimation


def estimate_seasonal_pattern(month_strings, values):
    """Estimate additive seasonal factors by calendar month.

    For each calendar month (Jan=1 … Dec=12), computes the average
    deviation from the overall series mean.

    Args:
        month_strings: list of "YYYY-MM" strings
        values: list of numeric values (same length)

    Returns:
        dict {int: float} mapping calendar month (1-12) to seasonal
        deviation, or None if insufficient data or zero-mean series.
    """
    if len(values) < MIN_MONTHS_FOR_SEASONAL:
        return None

    overall_mean = sum(values) / len(values)
    if overall_mean == 0:
        return None

    monthly_sums = defaultdict(float)
    monthly_counts = defaultdict(int)
    for month_str, value in zip(month_strings, values):
        cal_month = int(month_str.split("-")[1])
        monthly_sums[cal_month] += value
        monthly_counts[cal_month] += 1

    seasonal = {}
    for m in range(1, 13):
        if monthly_counts[m] > 0:
            seasonal[m] = monthly_sums[m] / monthly_counts[m] - overall_mean
        else:
            seasonal[m] = 0.0

    return seasonal


def apply_temporal_embedding(series_dict):
    """Apply temporal transforms to series before Chronos-2 inference.

    Low-count series (mean < LOG_TRANSFORM_THRESHOLD) receive a log1p
    transform.  Higher-count series with enough history receive additive
    seasonal decomposition.  Series that qualify for neither are passed
    through unchanged.

    Args:
        series_dict: {series_id: [(month_str, count), ...]}

    Returns:
        (transformed_dict, metadata_dict)
        - transformed_dict: same structure with transformed values
        - metadata_dict: per-series info needed for inverse_temporal_embedding
    """
    transformed = {}
    metadata = {}
    log_ids = []
    seasonal_ids = []

    for sid, series in series_dict.items():
        values = [pt[1] for pt in series]
        months = [pt[0] for pt in series]
        n = len(values)
        mean_val = sum(values) / n if n > 0 else 0
        meta = {"transform": "none"}

        if 0 < mean_val < LOG_TRANSFORM_THRESHOLD:
            # Low-count series: log1p variance stabilization
            tv = [round(math.log1p(v), 6) for v in values]
            transformed[sid] = list(zip(months, tv))
            meta["transform"] = "log1p"
            log_ids.append(sid)

        elif n >= MIN_MONTHS_FOR_SEASONAL and mean_val >= LOG_TRANSFORM_THRESHOLD:
            seasonal = estimate_seasonal_pattern(months, values)
            if seasonal is not None:
                dv = []
                for month_str, v in zip(months, values):
                    cal = int(month_str.split("-")[1])
                    dv.append(max(0.0, round(v - seasonal.get(cal, 0.0), 6)))
                transformed[sid] = list(zip(months, dv))
                meta["transform"] = "seasonal"
                meta["seasonal"] = seasonal
                seasonal_ids.append(sid)
            else:
                transformed[sid] = series
        else:
            transformed[sid] = series

        metadata[sid] = meta

    if log_ids:
        print(f"    Temporal embedding: log1p → {log_ids}")
    if seasonal_ids:
        print(f"    Temporal embedding: seasonal decomposition → {seasonal_ids}")

    return transformed, metadata


def inverse_temporal_embedding(forecasts, metadata):
    """Reverse temporal transforms on Chronos-2 output.

    Args:
        forecasts: {series_id: {"months": [...], "p10": [...], ...}}
        metadata:  per-series transform info from apply_temporal_embedding

    Returns:
        dict with same structure, values restored to original count scale.
    """
    result = {}

    for sid, fc in forecasts.items():
        meta = metadata.get(sid, {"transform": "none"})
        transform = meta["transform"]

        if transform == "none":
            result[sid] = fc
            continue

        inv = {}
        for key, vals in fc.items():
            if key == "months" or not isinstance(vals, list):
                inv[key] = vals
                continue

            if transform == "log1p":
                inv[key] = [round(max(0.0, math.expm1(v)), 1) for v in vals]

            elif transform == "seasonal":
                seasonal = meta["seasonal"]
                fc_months = fc.get("months", [])
                adjusted = []
                for i, v in enumerate(vals):
                    cal = int(fc_months[i].split("-")[1]) if i < len(fc_months) else 1
                    adjusted.append(round(max(0.0, v + seasonal.get(cal, 0.0)), 1))
                inv[key] = adjusted

        result[sid] = inv

    return result


def build_matrix_01(df, horizon, call_endpoint):
    """Matrix 01: Total Crash Frequency Forecast (county-wide monthly)."""
    print("\n[M01] Total Crash Frequency Forecast...")
    series = build_monthly_series(df)
    forecasts = call_endpoint(series, horizon)

    # Add history
    history = [{"month": m, "value": v} for m, v in series["total"]]

    return {
        "id": "m01",
        "title": "Total Crash Frequency",
        "subtitle": "County-Wide Monthly Forecast",
        "history": history,
        "forecast": forecasts.get("total", {}),
    }


def build_matrix_02(df, horizon, call_endpoint):
    """Matrix 02: Severity-Level Multivariate Forecast (K/A/B/C/O)."""
    print("\n[M02] Severity-Level Multivariate Forecast...")
    series = build_monthly_series(df, group_col="severity")

    # Ensure all severity levels present
    for sev in ["K", "A", "B", "C", "O"]:
        if sev not in series:
            # Build empty series
            all_months = sorted(df["month"].unique())
            series[sev] = [(str(m), 0) for m in all_months]

    forecasts = call_endpoint(series, horizon)

    # Build history per severity
    history = {}
    for sev in ["K", "A", "B", "C", "O"]:
        if sev in series:
            history[sev] = [{"month": m, "value": v} for m, v in series[sev]]

    # Calculate historical EPDO per month
    all_months = sorted(df["month"].unique())
    epdo_history = []
    for month in all_months:
        month_sev = {}
        for sev in ["K", "A", "B", "C", "O"]:
            month_data = dict(series.get(sev, []))
            month_sev[sev] = month_data.get(month, 0)
        epdo_history.append({
            "month": str(month),
            "value": calc_epdo(month_sev),
        })

    return {
        "id": "m02",
        "title": "Severity-Level Forecast",
        "subtitle": "Joint K-A-B-C-O Prediction",
        "history": history,
        "forecast": forecasts,
        "epdoHistory": epdo_history,
    }


def build_matrix_03(df, horizon, call_endpoint):
    """Matrix 03: Corridor-Level Cross-Learning Forecast."""
    print("\n[M03] Corridor Cross-Learning Forecast...")

    # Normalize route names for matching
    df_corr = df.copy()
    if "RTE Name" in df_corr.columns:
        route_col = "RTE Name"
    elif "RTE_NAME" in df_corr.columns:
        route_col = "RTE_NAME"
    else:
        print("  WARNING: No route column found. Skipping corridor forecast.")
        return None

    # Auto-detect top corridors from data if not already populated
    corridors_to_use = TOP_CORRIDORS if TOP_CORRIDORS else auto_detect_top_corridors(df)

    # Match top corridors (case-insensitive partial match)
    corridor_map = {}
    for _, row in df_corr.iterrows():
        rte = str(row.get(route_col, "")).strip().upper()
        for corr in corridors_to_use:
            if corr.upper() in rte or rte in corr.upper():
                corridor_map[row.name] = corr
                break

    df_corr["corridor"] = df_corr.index.map(corridor_map)
    df_corr = df_corr.dropna(subset=["corridor"])

    if df_corr.empty:
        print("  WARNING: No corridor matches found. Skipping.")
        return None

    series = build_monthly_series(df_corr, group_col="corridor")
    forecasts = call_endpoint(series, horizon)

    # Build history + stats per corridor
    corridors = {}
    for corr_name, corr_series in series.items():
        total = sum(v for _, v in corr_series)
        monthly_avg = total / max(len(corr_series), 1)
        # Get severity breakdown
        corr_df = df_corr[df_corr["corridor"] == corr_name]
        sev_counts = corr_df["severity"].value_counts().to_dict()
        epdo = calc_epdo(sev_counts)

        corridors[corr_name] = {
            "history": [{"month": m, "value": v} for m, v in corr_series],
            "forecast": forecasts.get(corr_name, {}),
            "stats": {
                "total": int(total),
                "monthlyAvg": round(monthly_avg, 1),
                "epdo": int(epdo),
                "severity": {k: int(v) for k, v in sev_counts.items()},
            },
        }

    return {
        "id": "m03",
        "title": "Corridor Forecast",
        "subtitle": "Cross-Learning Top Routes",
        "corridors": corridors,
    }


def build_matrix_04(df, horizon, call_endpoint):
    """Matrix 04: Crash Type Distribution Forecast."""
    print("\n[M04] Crash Type Distribution Forecast...")

    if "Collision Type" not in df.columns:
        print("  WARNING: No Collision Type column. Skipping.")
        return None

    df_typed = df.copy()
    df_typed["crash_type"] = df_typed["Collision Type"].map(CRASH_TYPE_MAP)
    df_typed = df_typed.dropna(subset=["crash_type"])

    series = build_monthly_series(df_typed, group_col="crash_type")
    forecasts = call_endpoint(series, horizon)

    # Build stats per type
    types = {}
    for type_name, type_series in series.items():
        total = sum(v for _, v in type_series)
        types[type_name] = {
            "history": [{"month": m, "value": v} for m, v in type_series],
            "forecast": forecasts.get(type_name, {}),
            "total": int(total),
        }

    return {
        "id": "m04",
        "title": "Crash Type Forecast",
        "subtitle": "By Collision Type",
        "types": types,
    }


def build_matrix_05(df, horizon, call_endpoint):
    """Matrix 05: Contributing Factor Trends Forecast."""
    print("\n[M05] Contributing Factor Trends Forecast...")

    factors = {
        "speed": "Speed?",
        "alcohol": "Alcohol?",
        "pedestrian": "Pedestrian?",
        "bicycle": "Bike?",
        "night": "Night?",
    }

    # Get full month range from the parent dataset so that sparse factors
    # (bike, pedestrian) include zero-count months in their time series.
    full_month_range = sorted(df["month"].unique())

    all_series = {}
    factor_stats = {}
    for factor_name, col_name in factors.items():
        if col_name not in df.columns:
            continue
        factor_df = df[df[col_name].str.strip().str.upper().isin(["YES", "Y", "1", "TRUE"])]
        series = build_monthly_series(factor_df, full_month_range=full_month_range)
        all_series[factor_name] = series["total"]
        factor_stats[factor_name] = {
            "total": int(len(factor_df)),
            "history": [{"month": m, "value": v} for m, v in series["total"]],
        }

    if not all_series:
        print("  WARNING: No contributing factor columns found. Skipping.")
        return None

    forecasts = call_endpoint(all_series, horizon)

    for factor_name in factor_stats:
        factor_stats[factor_name]["forecast"] = forecasts.get(factor_name, {})

    return {
        "id": "m05",
        "title": "Contributing Factor Trends",
        "subtitle": "Speed / Alcohol / Ped / Bike / Night",
        "factors": factor_stats,
    }


def build_matrix_06(df, horizon, call_endpoint):
    """Matrix 06: Intersection vs Segment Forecast."""
    print("\n[M06] Intersection vs Segment Forecast...")

    if "Intersection Type" not in df.columns:
        print("  WARNING: No Intersection Type column. Skipping.")
        return None

    df_loc = df.copy()
    df_loc["loc_type"] = df_loc["Intersection Type"].map(INTERSECTION_TYPE_MAP)
    df_loc = df_loc.dropna(subset=["loc_type"])

    series = build_monthly_series(df_loc, group_col="loc_type")
    forecasts = call_endpoint(series, horizon)

    loc_types = {}
    for loc_name, loc_series in series.items():
        total = sum(v for _, v in loc_series)
        loc_types[loc_name] = {
            "history": [{"month": m, "value": v} for m, v in loc_series],
            "forecast": forecasts.get(loc_name, {}),
            "total": int(total),
        }

    return {
        "id": "m06",
        "title": "Intersection vs Segment",
        "subtitle": "By Location Type",
        "locationTypes": loc_types,
    }


def build_summary_stats(df, matrices):
    """Build summary statistics for the dashboard KPI cards."""
    total = len(df)
    years = sorted(df["Crash Year"].unique()) if "Crash Year" in df.columns else []
    sev = df["severity"].value_counts().to_dict()

    # Recent trend (last 6 months vs previous 6 months)
    months = sorted(df["month"].unique())
    if len(months) >= 12:
        recent_6 = months[-6:]
        prev_6 = months[-12:-6]
        recent_count = len(df[df["month"].isin(recent_6)])
        prev_count = len(df[df["month"].isin(prev_6)])
        trend_pct = ((recent_count - prev_count) / max(prev_count, 1)) * 100
    else:
        recent_count = total
        prev_count = total
        trend_pct = 0

    return {
        "totalCrashes": int(total),
        "years": [int(y) for y in years],
        "dateRange": {
            "start": str(months[0]) if months else "",
            "end": str(months[-1]) if months else "",
        },
        "severity": {k: int(v) for k, v in sev.items()},
        "epdo": int(calc_epdo(sev)),
        "monthlyAvg": round(total / max(len(months), 1), 1),
        "recentTrend": {
            "recent6mo": int(recent_count),
            "prev6mo": int(prev_count),
            "changePct": round(trend_pct, 1),
        },
    }


def build_crash_pattern_matrix(df):
    """Build historical severity x crash type cross-tabulation.

    Computes the proportion of each crash type within each severity level,
    enabling safety engineers to understand what crash patterns drive
    fatal and serious injury outcomes.

    Returns:
        dict with 'historicalMatrix', 'severityTotals', 'typeTotals',
        and 'epdoByType' for the crash pattern heatmap.
    """
    print("\n[Derived] Building crash pattern matrix (Severity x Crash Type)...")

    if "Collision Type" not in df.columns or "severity" not in df.columns:
        print("  WARNING: Missing Collision Type or severity column. Skipping.")
        return None

    df_typed = df.copy()
    df_typed["crash_type"] = df_typed["Collision Type"].map(CRASH_TYPE_MAP)
    df_typed = df_typed.dropna(subset=["crash_type"])

    severities = ["K", "A", "B", "C", "O"]
    crash_types = sorted(df_typed["crash_type"].unique())

    # Build the cross-tabulation: count of crashes per (severity, type)
    cross_tab = df_typed.groupby(["severity", "crash_type"]).size().reset_index(name="count")
    ct_dict = {}
    for _, row in cross_tab.iterrows():
        sev = row["severity"]
        ct = row["crash_type"]
        ct_dict[(sev, ct)] = int(row["count"])

    # Build matrix with counts and percentages
    matrix = {}
    severity_totals = {}
    type_totals = {ct: 0 for ct in crash_types}

    for sev in severities:
        sev_total = sum(ct_dict.get((sev, ct), 0) for ct in crash_types)
        severity_totals[sev] = sev_total
        row = {}
        for ct in crash_types:
            count = ct_dict.get((sev, ct), 0)
            type_totals[ct] += count
            pct = round(count / max(sev_total, 1) * 100, 1)
            row[ct] = {"count": count, "pct": pct}
        matrix[sev] = row

    # EPDO-weighted crash counts by type (which types cause highest severity cost)
    epdo_by_type = {}
    for ct in crash_types:
        epdo = sum(ct_dict.get((sev, ct), 0) * EPDO_WEIGHTS.get(sev, 1) for sev in severities)
        epdo_by_type[ct] = epdo

    # KA rate by type (% of each type that is K or A)
    ka_rate_by_type = {}
    for ct in crash_types:
        ct_total = type_totals[ct]
        ka_count = ct_dict.get(("K", ct), 0) + ct_dict.get(("A", ct), 0)
        ka_rate_by_type[ct] = round(ka_count / max(ct_total, 1) * 100, 1)

    print(f"  Matrix: {len(severities)} severities x {len(crash_types)} types")
    print(f"  Total typed crashes: {sum(type_totals.values()):,}")
    for ct in sorted(ka_rate_by_type, key=ka_rate_by_type.get, reverse=True):
        print(f"    {ct}: KA rate = {ka_rate_by_type[ct]}%, EPDO = {epdo_by_type[ct]:,}")

    return {
        "historicalMatrix": matrix,
        "severityTotals": severity_totals,
        "typeTotals": type_totals,
        "crashTypes": crash_types,
        "epdoByType": epdo_by_type,
        "kaRateByType": ka_rate_by_type,
    }


def build_derived_metrics(matrices, summary, horizon, crash_pattern=None):
    """Build derived analytics from the 6 prediction matrices.

    These metrics provide deeper insights beyond raw forecasts:
    - Uncertainty/confidence analysis
    - EPDO-weighted risk forecasts
    - Severity shift detection
    - Corridor rank movement predictions
    - Crash type momentum (which types are growing fastest)
    - Seasonal risk calendar
    - Composite risk scoring
    - Crash pattern matrix (severity x crash type)
    """
    derived = {}

    # ---- 0. Crash Pattern Matrix (severity x crash type) ----
    if crash_pattern:
        derived["crashPatternMatrix"] = crash_pattern

    # ---- 1. Prediction Confidence Width (from M01) ----
    m01 = matrices.get("m01", {})
    fc01 = m01.get("forecast", {})
    if fc01.get("p90") and fc01.get("p10"):
        widths = [round(p90 - p10, 1) for p90, p10 in zip(fc01["p90"], fc01["p10"])]
        avg_width = round(sum(widths) / max(len(widths), 1), 1)
        p50_avg = sum(fc01.get("p50", [0])) / max(len(fc01.get("p50", [1])), 1)
        # Coefficient of variation of the CI — lower = more confident
        cv = round(avg_width / max(p50_avg, 1) * 100, 1)
        derived["confidenceWidth"] = {
            "monthlyWidths": widths,
            "months": fc01.get("months", []),
            "avgWidth": avg_width,
            "coefficientOfVariation": cv,
            "interpretation": "low" if cv < 30 else "moderate" if cv < 60 else "high",
        }

    # ---- 2. EPDO Forecast (from M02 severity forecasts) ----
    m02 = matrices.get("m02", {})
    fc02 = m02.get("forecast", {})
    if fc02.get("K") and fc02["K"].get("p50"):
        months_list = fc02["K"].get("months", [])
        epdo_forecast = []
        epdo_p10 = []
        epdo_p90 = []
        for i in range(len(months_list)):
            epdo_val = 0
            ep10 = 0
            ep90 = 0
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

        # Compare to historical EPDO
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

    # ---- 3. Predicted KA Rate (fatal + serious injury forecast) ----
    if fc02.get("K") and fc02.get("A"):
        months_list = fc02["K"].get("months", [])
        ka_forecast = []
        total_forecast = fc01.get("p50", [])
        for i in range(len(months_list)):
            k_val = fc02["K"]["p50"][i] if i < len(fc02["K"].get("p50", [])) else 0
            a_val = fc02["A"]["p50"][i] if i < len(fc02["A"].get("p50", [])) else 0
            ka_forecast.append(round(k_val + a_val, 1))

        # KA rate as percentage of total
        ka_rates = []
        for i, ka in enumerate(ka_forecast):
            total = total_forecast[i] if i < len(total_forecast) else 1
            ka_rates.append(round(ka / max(total, 1) * 100, 1))

        # Historical KA rate for comparison
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

    # ---- 4. Severity Shift Index (is severity mix changing?) ----
    if fc02.get("K") and fc02.get("O"):
        # Compare predicted severity distribution vs historical
        hist_sev = summary.get("severity", {})
        hist_total = sum(hist_sev.values())
        hist_ka_share = round((hist_sev.get("K", 0) + hist_sev.get("A", 0)) / max(hist_total, 1) * 100, 2)

        pred_sev = {}
        for sev in ["K", "A", "B", "C", "O"]:
            sev_fc = fc02.get(sev, {})
            p50 = sev_fc.get("p50", [])
            pred_sev[sev] = round(sum(p50[:6]))
        pred_total = sum(pred_sev.values())
        pred_ka_share = round((pred_sev.get("K", 0) + pred_sev.get("A", 0)) / max(pred_total, 1) * 100, 2)

        shift = round(pred_ka_share - hist_ka_share, 2)
        derived["severityShift"] = {
            "historicalKAShare": hist_ka_share,
            "predictedKAShare": pred_ka_share,
            "shiftPct": shift,
            "interpretation": "worsening" if shift > 0.5 else "stable" if shift > -0.5 else "improving",
            "predictedDistribution": pred_sev,
        }

    # ---- 5. Corridor Risk Ranking Movement (from M03) ----
    m03 = matrices.get("m03", {})
    corridors = m03.get("corridors", {})
    if corridors:
        corridor_rankings = []
        for name, cdata in corridors.items():
            stats = cdata.get("stats", {})
            fc = cdata.get("forecast", {})
            hist = cdata.get("history", [])

            # Historical monthly average (last 6 months)
            hist_6mo_avg = sum(h["value"] for h in hist[-6:]) / 6 if len(hist) >= 6 else 0
            # Predicted monthly average (next 6 months)
            p50 = fc.get("p50", [])
            pred_6mo_avg = sum(p50[:6]) / min(6, len(p50)) if p50 else 0

            # Trend direction
            change_pct = round(((pred_6mo_avg - hist_6mo_avg) / max(hist_6mo_avg, 1)) * 100, 1)

            # Confidence width (uncertainty) for this corridor
            p90 = fc.get("p90", [])
            p10 = fc.get("p10", [])
            avg_ci = round(sum(a - b for a, b in zip(p90[:6], p10[:6])) / 6, 1) if p90 and p10 else 0

            corridor_rankings.append({
                "name": name,
                "historical6moAvg": round(hist_6mo_avg, 1),
                "predicted6moAvg": round(pred_6mo_avg, 1),
                "changePct": change_pct,
                "epdo": stats.get("epdo", 0),
                "confidenceWidth": avg_ci,
                "direction": "rising" if change_pct > 5 else "falling" if change_pct < -5 else "stable",
            })

        # Sort by predicted average (highest risk first)
        corridor_rankings.sort(key=lambda x: x["predicted6moAvg"], reverse=True)
        # Add rank and rank change
        hist_sorted = sorted(corridor_rankings, key=lambda x: x["historical6moAvg"], reverse=True)
        hist_rank_map = {c["name"]: i + 1 for i, c in enumerate(hist_sorted)}
        for i, c in enumerate(corridor_rankings):
            c["predictedRank"] = i + 1
            c["historicalRank"] = hist_rank_map.get(c["name"], 0)
            c["rankChange"] = c["historicalRank"] - c["predictedRank"]  # positive = moved up in risk

        # Identify emerging risk corridors (biggest upward movers)
        emerging = [c for c in corridor_rankings if c["changePct"] > 5]
        emerging.sort(key=lambda x: x["changePct"], reverse=True)

        derived["corridorRankMovement"] = {
            "rankings": corridor_rankings,
            "emergingRisk": [c["name"] for c in emerging[:3]],
            "improvingMost": sorted(
                [c for c in corridor_rankings if c["changePct"] < -5],
                key=lambda x: x["changePct"]
            )[:3] if corridor_rankings else [],
        }

    # ---- 6. Crash Type Momentum (from M04) ----
    m04 = matrices.get("m04", {})
    crash_types = m04.get("types", {})
    if crash_types:
        type_momentum = []
        for type_name, tdata in crash_types.items():
            hist = tdata.get("history", [])
            fc = tdata.get("forecast", {})
            p50 = fc.get("p50", [])

            hist_6mo = sum(h["value"] for h in hist[-6:]) if len(hist) >= 6 else 0
            pred_6mo = sum(p50[:6]) if p50 else 0
            change_pct = round(((pred_6mo - hist_6mo) / max(hist_6mo, 1)) * 100, 1) if hist_6mo else 0

            # Calculate trend slope (linear regression of predicted values)
            if len(p50) >= 3:
                n = len(p50)
                x_mean = (n - 1) / 2
                y_mean = sum(p50) / n
                slope = sum((i - x_mean) * (p50[i] - y_mean) for i in range(n)) / max(
                    sum((i - x_mean) ** 2 for i in range(n)), 1
                )
            else:
                slope = 0

            type_momentum.append({
                "type": type_name,
                "historical6mo": round(hist_6mo),
                "predicted6mo": round(pred_6mo),
                "changePct": change_pct,
                "trendSlope": round(slope, 2),
                "total": tdata.get("total", 0),
                "momentum": "accelerating" if change_pct > 10 else "decelerating" if change_pct < -10 else "steady",
            })

        type_momentum.sort(key=lambda x: x["changePct"], reverse=True)
        derived["crashTypeMomentum"] = {
            "types": type_momentum,
            "fastestGrowing": type_momentum[0]["type"] if type_momentum else None,
            "fastestDeclining": type_momentum[-1]["type"] if type_momentum else None,
        }

    # ---- 7. Contributing Factor Trend Slopes (from M05) ----
    m05 = matrices.get("m05", {})
    factors = m05.get("factors", {})
    if factors:
        factor_trends = []
        for factor_name, fdata in factors.items():
            hist = fdata.get("history", [])
            fc = fdata.get("forecast", {})
            p50 = fc.get("p50", [])

            hist_6mo = sum(h["value"] for h in hist[-6:]) if len(hist) >= 6 else 0
            pred_6mo = sum(p50[:6]) if p50 else 0
            change_pct = round(((pred_6mo - hist_6mo) / max(hist_6mo, 1)) * 100, 1) if hist_6mo else 0

            # As percentage of total predicted crashes
            total_pred = sum(fc01.get("p50", [0])[:6]) if fc01.get("p50") else 1
            involvement_rate = round(pred_6mo / max(total_pred, 1) * 100, 1)

            factor_trends.append({
                "factor": factor_name,
                "historical6mo": round(hist_6mo),
                "predicted6mo": round(pred_6mo),
                "changePct": change_pct,
                "involvementRatePct": involvement_rate,
                "total": fdata.get("total", 0),
            })

        factor_trends.sort(key=lambda x: x["changePct"], reverse=True)
        derived["factorTrends"] = {
            "factors": factor_trends,
            "steepestRising": factor_trends[0]["factor"] if factor_trends and factor_trends[0]["changePct"] > 0 else None,
        }

    # ---- 8. Seasonal Risk Calendar (from M01 + M02) ----
    if fc01.get("p50") and fc01.get("months"):
        monthly_risk = []
        for i, month_str in enumerate(fc01["months"]):
            month_num = int(month_str.split("-")[1])
            total_pred = fc01["p50"][i] if i < len(fc01["p50"]) else 0

            # Get KA prediction for this month
            ka_pred = 0
            for sev in ["K", "A"]:
                sev_fc = fc02.get(sev, {})
                p50 = sev_fc.get("p50", [])
                ka_pred += p50[i] if i < len(p50) else 0

            # EPDO for this month
            epdo_val = 0
            for sev, weight in EPDO_WEIGHTS.items():
                sev_fc = fc02.get(sev, {})
                p50 = sev_fc.get("p50", [])
                epdo_val += (p50[i] if i < len(p50) else 0) * weight

            monthly_risk.append({
                "month": month_str,
                "monthNum": month_num,
                "totalPredicted": round(total_pred, 1),
                "kaPredicted": round(ka_pred, 1),
                "epdoPredicted": round(epdo_val),
            })

        # Rank months by risk (EPDO)
        sorted_by_risk = sorted(monthly_risk, key=lambda x: x["epdoPredicted"], reverse=True)
        for rank, m in enumerate(sorted_by_risk):
            m["riskRank"] = rank + 1

        derived["seasonalRiskCalendar"] = {
            "months": monthly_risk,
            "highestRiskMonth": sorted_by_risk[0]["month"] if sorted_by_risk else None,
            "lowestRiskMonth": sorted_by_risk[-1]["month"] if sorted_by_risk else None,
        }

    # ---- 9. Composite Risk Score (blended index) ----
    # Combines total crash trend, severity trend, and EPDO trend into one score
    total_trend = summary.get("recentTrend", {}).get("changePct", 0)
    severity_shift = derived.get("severityShift", {}).get("shiftPct", 0)
    epdo_change = derived.get("epdoForecast", {}).get("changePct", 0)

    # Weighted composite: EPDO matters most (50%), total trend (30%), severity shift (20%)
    composite = round(epdo_change * 0.5 + total_trend * 0.3 + severity_shift * 10 * 0.2, 1)
    if composite > 10:
        risk_level = "critical"
    elif composite > 5:
        risk_level = "elevated"
    elif composite > -5:
        risk_level = "moderate"
    else:
        risk_level = "low"

    derived["compositeRiskScore"] = {
        "score": composite,
        "level": risk_level,
        "components": {
            "totalTrend": round(total_trend, 1),
            "severityShift": round(severity_shift, 2),
            "epdoChange": round(epdo_change, 1),
        },
        "weights": {"epdoChange": 0.5, "totalTrend": 0.3, "severityShift": 0.2},
    }

    # ---- 10. Location Type Split Forecast (from M06) ----
    m06 = matrices.get("m06", {})
    loc_types = m06.get("locationTypes", {})
    if loc_types:
        intersection_pred = 0
        segment_pred = 0
        for lt_name, lt_data in loc_types.items():
            p50 = lt_data.get("forecast", {}).get("p50", [])
            pred_6mo = sum(p50[:6]) if p50 else 0
            if lt_name in ("four_leg", "three_leg", "roundabout"):
                intersection_pred += pred_6mo
            else:
                segment_pred += pred_6mo

        total_loc = intersection_pred + segment_pred
        derived["locationTypeSplit"] = {
            "intersectionPredicted6mo": round(intersection_pred),
            "segmentPredicted6mo": round(segment_pred),
            "intersectionSharePct": round(intersection_pred / max(total_loc, 1) * 100, 1),
            "segmentSharePct": round(segment_pred / max(total_loc, 1) * 100, 1),
        }

    return derived


BACKTEST_HOLDOUT = 6  # months to hold out for backtesting


def _calc_mape(actual, predicted):
    """Calculate Mean Absolute Percentage Error, skipping zeros."""
    pairs = [(a, p) for a, p in zip(actual, predicted) if a > 0]
    if not pairs:
        return None
    return round(sum(abs(a - p) / a for a, p in pairs) / len(pairs) * 100, 1)


def _calc_mae(actual, predicted):
    """Calculate Mean Absolute Error."""
    if not actual:
        return None
    return round(sum(abs(a - p) for a, p in zip(actual, predicted)) / len(actual), 1)


def _calc_rmse(actual, predicted):
    """Calculate Root Mean Squared Error."""
    if not actual:
        return None
    mse = sum((a - p) ** 2 for a, p in zip(actual, predicted)) / len(actual)
    return round(mse ** 0.5, 1)


def _calc_directional_accuracy(actual, predicted):
    """Calculate % of months where trend direction matches."""
    if len(actual) < 2:
        return None
    correct = 0
    total = len(actual) - 1
    for i in range(1, len(actual)):
        actual_dir = actual[i] - actual[i - 1]
        pred_dir = predicted[i] - predicted[i - 1]
        if (actual_dir >= 0 and pred_dir >= 0) or (actual_dir < 0 and pred_dir < 0):
            correct += 1
    return round(correct / total, 2) if total > 0 else None


def _assign_grade(mape):
    """Assign accuracy grade based on MAPE."""
    if mape is None:
        return "N/A"
    if mape < 10:
        return "A"
    if mape < 20:
        return "B"
    if mape < 30:
        return "C"
    return "D"


def backtest_forecast(df, call_endpoint, horizon=BACKTEST_HOLDOUT):
    """Run hold-out backtesting on M01, M02, and M03 matrices.

    Holds out the last `horizon` months of data, generates forecasts
    using only the training portion, then compares with actual values.

    Args:
        df: Full crash DataFrame
        call_endpoint: Endpoint function (real or synthetic, with temporal embedding)
        horizon: Number of months to hold out (default: 6)

    Returns:
        dict with backtesting results including MAPE, MAE, RMSE, grades,
        and actual vs predicted arrays for frontend overlay charts.
    """
    print(f"\n{'='*60}")
    print(f"  BACKTESTING: Holding out last {horizon} months")
    print(f"{'='*60}")

    all_months = sorted(df["month"].unique())
    if len(all_months) < horizon + 12:
        print("  WARNING: Not enough history for backtesting. Skipping.")
        return None

    cutoff_months = all_months[-horizon:]
    train_months = all_months[:-horizon]
    df_train = df[df["month"].isin(train_months)]
    df_test = df[df["month"].isin(cutoff_months)]

    print(f"  Training: {len(train_months)} months ({train_months[0]} to {train_months[-1]})")
    print(f"  Test: {len(cutoff_months)} months ({cutoff_months[0]} to {cutoff_months[-1]})")

    results = {"holdoutMonths": horizon}

    # --- M01: Total crash frequency ---
    print("\n  [Backtest M01] Total crashes...")
    train_series = build_monthly_series(df_train)
    bt_forecasts = call_endpoint(train_series, horizon)
    bt_fc = bt_forecasts.get("total", {})
    predicted_m01 = bt_fc.get("p50", [])

    # Actual test values
    test_monthly = df_test.groupby("month").size()
    actual_m01 = [int(test_monthly.get(m, 0)) for m in cutoff_months]

    mape = _calc_mape(actual_m01, predicted_m01)
    mae = _calc_mae(actual_m01, predicted_m01)
    rmse = _calc_rmse(actual_m01, predicted_m01)
    da = _calc_directional_accuracy(actual_m01, predicted_m01)
    grade = _assign_grade(mape)

    results["m01"] = {
        "mape": mape, "mae": mae, "rmse": rmse,
        "directionalAccuracy": da, "grade": grade,
    }
    print(f"    MAPE: {mape}%, MAE: {mae}, Grade: {grade}")

    # Store actual vs predicted for overlay chart
    results["actualVsPredicted"] = {
        "m01": {
            "months": [str(m) for m in cutoff_months],
            "actual": actual_m01,
            "predicted": [round(v, 1) for v in predicted_m01[:len(actual_m01)]],
        }
    }

    # --- M02: Severity-level backtesting ---
    print("\n  [Backtest M02] Severity levels...")
    train_sev_series = build_monthly_series(df_train, group_col="severity")
    for sev in ["K", "A", "B", "C", "O"]:
        if sev not in train_sev_series:
            train_sev_series[sev] = [(str(m), 0) for m in train_months]

    bt_sev_forecasts = call_endpoint(train_sev_series, horizon)

    m02_results = {}
    for sev in ["K", "A", "B", "C", "O"]:
        sev_predicted = bt_sev_forecasts.get(sev, {}).get("p50", [])
        sev_test = df_test[df_test["severity"] == sev].groupby("month").size()
        sev_actual = [int(sev_test.get(m, 0)) for m in cutoff_months]
        sev_mape = _calc_mape(sev_actual, sev_predicted)
        sev_grade = _assign_grade(sev_mape)
        m02_results[sev] = {"mape": sev_mape, "grade": sev_grade}
        print(f"    {sev}: MAPE={sev_mape}%, Grade={sev_grade}")

    results["m02"] = m02_results

    # --- M03: Corridor backtesting ---
    print("\n  [Backtest M03] Corridors...")
    if TOP_CORRIDORS:
        route_col = "RTE Name" if "RTE Name" in df_train.columns else (
            "RTE_NAME" if "RTE_NAME" in df_train.columns else None)
        if route_col:
            df_train_corr = df_train.copy()
            corridor_map = {}
            for _, row in df_train_corr.iterrows():
                rte = str(row.get(route_col, "")).strip().upper()
                for corr in TOP_CORRIDORS:
                    if corr.upper() in rte or rte in corr.upper():
                        corridor_map[row.name] = corr
                        break
            df_train_corr["corridor"] = df_train_corr.index.map(corridor_map)
            df_train_corr = df_train_corr.dropna(subset=["corridor"])

            df_test_corr = df_test.copy()
            corridor_map_test = {}
            for _, row in df_test_corr.iterrows():
                rte = str(row.get(route_col, "")).strip().upper()
                for corr in TOP_CORRIDORS:
                    if corr.upper() in rte or rte in corr.upper():
                        corridor_map_test[row.name] = corr
                        break
            df_test_corr["corridor"] = df_test_corr.index.map(corridor_map_test)
            df_test_corr = df_test_corr.dropna(subset=["corridor"])

            if not df_train_corr.empty:
                train_corr_series = build_monthly_series(df_train_corr, group_col="corridor")
                bt_corr_forecasts = call_endpoint(train_corr_series, horizon)

                m03_results = {}
                for corr in TOP_CORRIDORS:
                    corr_predicted = bt_corr_forecasts.get(corr, {}).get("p50", [])
                    corr_test = df_test_corr[df_test_corr["corridor"] == corr].groupby("month").size()
                    corr_actual = [int(corr_test.get(m, 0)) for m in cutoff_months]
                    corr_mape = _calc_mape(corr_actual, corr_predicted)
                    corr_grade = _assign_grade(corr_mape)
                    m03_results[corr] = {"mape": corr_mape, "grade": corr_grade}
                    print(f"    {corr}: MAPE={corr_mape}%, Grade={corr_grade}")

                results["m03"] = m03_results

    # Overall grade (weighted: M01 50%, M02 avg 30%, M03 avg 20%)
    grades_to_score = {"A": 1, "B": 2, "C": 3, "D": 4, "N/A": 3}
    m01_score = grades_to_score.get(results["m01"]["grade"], 3)

    m02_scores = [grades_to_score.get(v["grade"], 3) for v in m02_results.values() if v["grade"] != "N/A"]
    m02_avg = sum(m02_scores) / max(len(m02_scores), 1) if m02_scores else 3

    m03_results_dict = results.get("m03", {})
    m03_scores = [grades_to_score.get(v["grade"], 3) for v in m03_results_dict.values() if v["grade"] != "N/A"]
    m03_avg = sum(m03_scores) / max(len(m03_scores), 1) if m03_scores else 3

    overall_score = m01_score * 0.5 + m02_avg * 0.3 + m03_avg * 0.2
    if overall_score < 1.5:
        results["overallGrade"] = "A"
    elif overall_score < 2.5:
        results["overallGrade"] = "B"
    elif overall_score < 3.5:
        results["overallGrade"] = "C"
    else:
        results["overallGrade"] = "D"

    print(f"\n  Overall Backtest Grade: {results['overallGrade']} (score: {overall_score:.1f})")
    return results


def generate_single_forecast(csv_path, output_path, horizon, road_type_label=None,
                             mode="sagemaker"):
    """Generate forecast for a single crash data file.

    Args:
        csv_path: Path to crash CSV
        output_path: Path for output JSON
        horizon: Forecast horizon in months
        road_type_label: Road type label for output metadata
        mode: 'sagemaker' (real Chronos-2 endpoint) or 'synthetic' (Holt-Winters fallback)
    """
    global TOP_CORRIDORS
    df = load_crash_data(csv_path)

    # Auto-detect top corridors from this jurisdiction's data
    TOP_CORRIDORS = auto_detect_top_corridors(df)

    if mode == "synthetic":
        # Use Holt-Winters synthetic forecasts (no AWS/SageMaker needed)
        print("[Mode] Using synthetic Holt-Winters forecasts (no SageMaker)")
        raw_endpoint = lambda series, h: generate_synthetic_forecast(series, h)
    else:
        # Use real SageMaker Chronos-2 endpoint
        try:
            import boto3
        except ImportError:
            print("ERROR: boto3 is required. Install with: pip install boto3")
            sys.exit(1)

        session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        check_sagemaker_endpoint(session)
        raw_endpoint = lambda series, h: invoke_endpoint(session, series, h)

    # Wrap with temporal embedding layer: seasonal decomposition for
    # high-count series, log1p for low-count series (K, A severity).
    def call_endpoint(series, h):
        transformed, meta = apply_temporal_embedding(series)
        forecasts = raw_endpoint(transformed, h)
        return inverse_temporal_embedding(forecasts, meta)

    # Build all 6 matrices
    matrices = {}

    m01 = build_matrix_01(df, horizon, call_endpoint)
    matrices["m01"] = m01

    m02 = build_matrix_02(df, horizon, call_endpoint)
    matrices["m02"] = m02

    m03 = build_matrix_03(df, horizon, call_endpoint)
    if m03:
        matrices["m03"] = m03

    m04 = build_matrix_04(df, horizon, call_endpoint)
    if m04:
        matrices["m04"] = m04

    m05 = build_matrix_05(df, horizon, call_endpoint)
    if m05:
        matrices["m05"] = m05

    m06 = build_matrix_06(df, horizon, call_endpoint)
    if m06:
        matrices["m06"] = m06

    # Build summary
    summary = build_summary_stats(df, matrices)

    # Build crash pattern matrix from raw data (severity x crash type)
    crash_pattern = build_crash_pattern_matrix(df)

    # Build derived analytics from the matrices
    print("\n[Derived] Building derived prediction metrics...")
    derived = build_derived_metrics(matrices, summary, horizon, crash_pattern=crash_pattern)
    print(f"  Derived metrics: {list(derived.keys())}")

    # Run backtesting (hold out last 6 months, compare predicted vs actual)
    backtesting = backtest_forecast(df, call_endpoint, horizon=BACKTEST_HOLDOUT)

    # Assemble final output
    model_name = "holt-winters-synthetic" if mode == "synthetic" else "amazon/chronos-2"
    output = {
        "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "model": model_name,
        "horizon": horizon,
        "quantileLevels": QUANTILE_LEVELS,
        "epdoWeights": EPDO_WEIGHTS,
        "roadType": road_type_label or "all_roads",
        "temporalEmbedding": {
            "enabled": True,
            "transforms": [
                "seasonal_decomposition",
                "log1p_variance_stabilization",
            ],
            "logTransformThreshold": LOG_TRANSFORM_THRESHOLD,
            "minMonthsForSeasonal": MIN_MONTHS_FOR_SEASONAL,
        },
        "summary": summary,
        "matrices": matrices,
        "derivedMetrics": derived,
    }

    if backtesting:
        output["backtesting"] = backtesting

    # Write output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nForecast written to {output_path}")
    print(f"  Road type: {road_type_label or 'all_roads'}")
    print(f"  Matrices generated: {list(matrices.keys())}")
    print(f"  Horizon: {horizon} months")
    file_size = os.path.getsize(output_path)
    print(f"  File size: {file_size:,} bytes")


# Road type configurations matching CRASH LENS UI filter options
ROAD_TYPE_CONFIGS = {
    "county_roads": {
        "label": "County Roads Only",
        "suffix": "county_roads",
        "output": "forecasts_county_roads.json",
    },
    "city_roads": {
        "label": "City Roads Only",
        "suffix": "city_roads",
        "output": "forecasts_city_roads.json",
    },
    "no_interstate": {
        "label": "All Roads (No Interstate)",
        "suffix": "no_interstate",
        "output": "forecasts_no_interstate.json",
    },
    "all_roads": {
        "label": "All Roads (Including Interstates)",
        "suffix": "all_roads",
        "output": "forecasts_all_roads.json",
    },
}


def main():
    parser = argparse.ArgumentParser(description="Generate crash forecasts with Chronos-2")
    parser.add_argument("--state", required=True,
                        help="State name (e.g., virginia, colorado). Used for loading state-specific config.")
    parser.add_argument("--data", default="data/crash_data.csv", help="Path to crash CSV")
    parser.add_argument("--output", default="data/forecasts.json", help="Output JSON path")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="Forecast horizon (months)")
    parser.add_argument("--source", choices=["local", "r2"], default="local",
                        help="Data source: 'local' reads from data-dir, 'r2' downloads from R2 CDN first")
    parser.add_argument("--all-road-types", action="store_true",
                        help="Generate forecasts for all 3 road type datasets (county_roads, no_interstate, all_roads)")
    parser.add_argument("--jurisdiction", default=None,
                        help="Jurisdiction name prefix for data files (required with --all-road-types)")
    parser.add_argument("--data-dir", default="data",
                        help="Directory containing road-type CSV files (default: data)")
    parser.add_argument("--mode", choices=["sagemaker", "synthetic"], default="sagemaker",
                        help="Forecast engine: 'sagemaker' uses Chronos-2 endpoint, "
                             "'synthetic' uses Holt-Winters (no AWS needed). Default: sagemaker")
    args = parser.parse_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Load state-specific EPDO weights
    global EPDO_WEIGHTS
    config_path = os.path.join(project_root, "states", args.state, "config.json")
    EPDO_WEIGHTS = load_epdo_weights(config_path)

    if args.all_road_types:
        if not args.jurisdiction:
            print("ERROR: --jurisdiction is required when using --all-road-types")
            sys.exit(1)

        # Generate forecasts for all road type datasets
        data_dir = os.path.join(project_root, args.data_dir) if not os.path.isabs(args.data_dir) else args.data_dir

        # If source is R2, download validated CSVs from CDN first
        if args.source == "r2":
            print(f"\n[R2] Downloading validated CSVs for {args.state}/{args.jurisdiction}...")
            download_from_r2(args.state, args.jurisdiction, data_dir)

        # Output forecast JSONs to the same directory as the input data
        output_dir = data_dir
        generated = 0

        for road_type, config in ROAD_TYPE_CONFIGS.items():
            csv_file = os.path.join(data_dir, f"{args.jurisdiction}_{config['suffix']}.csv")
            out_file = os.path.join(output_dir, config["output"])

            if not os.path.exists(csv_file):
                print(f"\n[SKIP] {csv_file} not found — skipping {road_type}")
                continue

            print(f"\n{'='*60}")
            print(f"  Generating forecast: {config['label']}")
            print(f"  Data: {csv_file}")
            print(f"  Output: {out_file}")
            print(f"{'='*60}")

            generate_single_forecast(csv_file, out_file, args.horizon, road_type,
                                     mode=args.mode)
            generated += 1

        print(f"\n{'='*60}")
        print(f"  Generated {generated} forecast files for {generated} road types")
        print(f"{'='*60}")
    else:
        # Single file mode (backward compatible)
        csv_path = os.path.join(project_root, args.data) if not os.path.isabs(args.data) else args.data
        output_path = os.path.join(project_root, args.output) if not os.path.isabs(args.output) else args.output

        # Detect road type from filename
        road_type = "all_roads"
        for rt, config in ROAD_TYPE_CONFIGS.items():
            if config["suffix"] in os.path.basename(csv_path):
                road_type = rt
                break

        generate_single_forecast(csv_path, output_path, args.horizon, road_type,
                                 mode=args.mode)

    print("\nDone!")


if __name__ == "__main__":
    main()
