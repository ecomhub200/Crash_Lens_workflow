# Claude Code Prompt: Enhance Stage 5 Forecast in Existing CrashLens Pipeline

## Objective

Enhance the **existing** `pipeline.yml` ("Pipeline: Process Crash Data") forecast step (Stage 5) and the `scripts/generate_forecast.py` script so that:

1. Forecasts use **real Chronos-2 via AWS SageMaker** (never dry-run/synthetic)
2. Forecast input CSVs are sourced **from R2 storage** (validated data), not raw local files
3. The pipeline is **fully jurisdiction-agnostic** — works for any state/jurisdiction the user selects
4. **EPDO weights** load from the correct state config (not hardcoded to Colorado)
5. Forecast output JSONs are stored with **consistent naming** so monthly automation overwrites old files
6. The **R2 → Frontend** connection works for the Crash Prediction tab

**IMPORTANT: Do NOT create a new architecture or new workflow files. Follow the existing `pipeline.yml` structure and enhance Stage 5 within it.**

---

## Current Architecture (DO NOT CHANGE overall structure)

The existing pipeline in `.github/workflows/pipeline.yml` has these stages:

```
Stage 0: Init Cache (split jurisdictions)
Stage 1: Split Road Type → 3 CSVs per jurisdiction
Stage 2: Generate aggregate CSVs
Stage 3/4: Upload CSVs to R2
Stage 5: Generate forecasts        ← ENHANCE THIS
Stage 5b: Upload forecast JSONs    ← VERIFY this works correctly
Stage 6: Commit manifest
```

The pipeline already receives `state`, `scope`, `selection`, `data_dir`, `r2_prefix`, and `jurisdictions_json` from the `prepare` job.

---

## File 1: `scripts/generate_forecast.py` — Changes Required

### Current CLI Arguments (lines 1667-1677):
```python
parser.add_argument("--data", default="data/CDOT/douglas_all_roads.csv")
parser.add_argument("--output", default="data/CDOT/forecasts.json")
parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--all-road-types", action="store_true")
parser.add_argument("--jurisdiction", default="douglas")
parser.add_argument("--data-dir", default="data/CDOT")
```

### Changes to make:

#### A. Add `--state` argument
```python
parser.add_argument("--state", required=True,
                    help="State name (e.g., virginia, colorado). Used for loading state-specific config.")
```
This is needed so the script can load the correct EPDO weights from `states/{state}/config.json`.

#### B. Add `--source` argument (r2 or local)
```python
parser.add_argument("--source", choices=["local", "r2"], default="local",
                    help="Data source: 'local' reads from data-dir, 'r2' downloads from R2 CDN first")
```
When `--source r2` is specified, the script should download the 3 road-type CSVs from R2 into `data-dir` before processing. This ensures the forecast always uses the latest validated data.

#### C. Remove `--dry-run` flag entirely
Delete the `--dry-run` argument and all references to it. The pipeline should always use real SageMaker. Specifically:
- Remove `args.dry_run` references
- Remove the `if dry_run:` synthetic generator branch in `generate_single_forecast()` (line 1546)
- The model tag should always be `"amazon/chronos-2"` (line 1611)

#### D. Fix `load_epdo_weights()` to use state config
Current code (line 74) hardcodes `data/CDOT/config.json`. Change to accept `--state` and load from:
```python
config_path = os.path.join(project_root, "states", state, "config.json")
```
The function already handles fallback to defaults if the file is missing, which is good.

Also: the `EPDO_WEIGHTS = load_epdo_weights()` at module level (line 88) should be moved into `main()` so it uses the `--state` argument:
```python
global EPDO_WEIGHTS
EPDO_WEIGHTS = load_epdo_weights(
    os.path.join(project_root, "states", args.state, "config.json")
)
```

#### E. Add R2 download function
Add a function to download CSVs from R2 CDN before processing:
```python
def download_from_r2(state, jurisdiction, data_dir):
    """Download validated road-type CSVs from R2 CDN into data_dir."""
    import urllib.request
    CDN_BASE = "https://data.aicreatesai.com"
    os.makedirs(data_dir, exist_ok=True)

    for rt in ["county_roads", "no_interstate", "all_roads"]:
        url = f"{CDN_BASE}/{state}/{jurisdiction}/{rt}.csv"
        local_path = os.path.join(data_dir, f"{jurisdiction}_{rt}.csv")
        print(f"  Downloading {url} → {local_path}")
        try:
            urllib.request.urlretrieve(url, local_path)
            size = os.path.getsize(local_path)
            print(f"    OK ({size:,} bytes)")
        except Exception as e:
            print(f"    SKIP: {e}")
```

#### F. Add SageMaker pre-check
Before generating forecasts, verify the SageMaker endpoint exists and is InService:
```python
def check_sagemaker_endpoint(session, endpoint_name="crashlens-chronos2-endpoint"):
    """Verify SageMaker endpoint is available before running forecasts."""
    sm = session.client("sagemaker")
    try:
        resp = sm.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        if status != "InService":
            print(f"ERROR: SageMaker endpoint '{endpoint_name}' is {status}, not InService")
            sys.exit(1)
        print(f"[SageMaker] Endpoint '{endpoint_name}' is InService ✓")
    except Exception as e:
        print(f"ERROR: Cannot reach SageMaker endpoint: {e}")
        sys.exit(1)
```

#### G. Update `main()` flow
The updated `main()` should:
1. Parse args (with new `--state`, `--source`, no `--dry-run`)
2. Load state-specific EPDO weights using `--state`
3. If `--source r2`: download CSVs from R2 CDN into `--data-dir`
4. Create boto3 session and check SageMaker endpoint
5. Proceed with existing `--all-road-types` loop (unchanged logic)

#### H. Remove Colorado-specific defaults
- Change `--data` default to just `"data/crash_data.csv"` (or make it required when not using `--all-road-types`)
- Change `--jurisdiction` default from `"douglas"` to required when using `--all-road-types`
- Change `--data-dir` default from `"data/CDOT"` to just `"data"`
- Remove `DEFAULT_CORRIDORS` Colorado-specific list (line 91-93). The `auto_detect_top_corridors()` function already handles this dynamically.

---

## File 2: `.github/workflows/pipeline.yml` — Stage 5 Enhancement

### Current Stage 5 command (lines 362-364):
```python
cmd = ['python', 'scripts/generate_forecast.py',
       '--all-road-types', '--jurisdiction', j, '--data-dir', data_dir]
```

### Change to:
```python
STATE = '${{ needs.prepare.outputs.state }}'
# ...
cmd = ['python', 'scripts/generate_forecast.py',
       '--all-road-types',
       '--jurisdiction', j,
       '--data-dir', data_dir,
       '--state', STATE,
       '--source', 'r2']
```

This is the **only change needed in pipeline.yml Stage 5** — pass `--state` and `--source r2` to the existing command. The rest of the stage logic (iterating over jurisdictions, subprocess calls, error handling) stays the same.

### Stage 5b verification (lines 373-431):
Stage 5b already correctly:
- Iterates over all 3 road types (`county_roads`, `no_interstate`, `all_roads`)
- Checks for files at both `{data_dir}/forecasts_{rt}.json` and `{data_dir}/{jurisdiction}/forecasts_{rt}.json`
- Uploads to R2 at `{r2_prefix}/{jurisdiction}/forecasts_{rt}.json`

**Verify** that `generate_forecast.py` outputs forecast JSONs to paths that Stage 5b can find. Currently the script writes to `{data_dir}/forecasts_{rt}.json` (line 1694: `out_file = os.path.join(output_dir, config["output"])`), and Stage 5b checks that path first. This should work as-is.

---

## File 3: Frontend Verification (no code changes expected)

The frontend already handles forecast loading correctly:
- `initPredictionTab()` constructs: `{r2Prefix}/{jurisdiction}/forecasts_{suffix}.json`
- It fetches from CDN: `https://data.aicreatesai.com/{state}/{jurisdiction}/forecasts_{suffix}.json`
- The 3 road type suffixes match: `county_roads`, `no_interstate`, `all_roads`

**Just verify** that after running the pipeline for any jurisdiction (e.g., henrico), the Crash Prediction tab loads the forecast JSON correctly. If there are issues, they would be in the `appConfig.states` registry in `app/index.html` which may need new states added as they come online.

---

## Summary of Changes

| File | What Changes | What Stays Same |
|------|-------------|-----------------|
| `scripts/generate_forecast.py` | Add `--state`, `--source r2`, remove `--dry-run`, fix EPDO loading, add R2 download, add SageMaker pre-check | All 6 matrix builders, temporal embedding, backtest, invoke_endpoint, auto_detect_top_corridors |
| `pipeline.yml` Stage 5 | Pass `--state` and `--source r2` to command | Jurisdiction iteration, subprocess calls, error handling |
| `pipeline.yml` Stage 5b | No changes | Upload logic, path checking |
| `pipeline.yml` all other stages | No changes | Everything else |
| Frontend | No changes expected | Forecast loading logic |

---

## Key Constants & Config

- **SageMaker endpoint**: `crashlens-chronos2-endpoint`
- **SageMaker instance**: `ml.m5.xlarge`
- **R2 bucket**: `crash-lens-data`
- **R2 CDN**: `https://data.aicreatesai.com`
- **State configs**: `states/{state}/config.json` (contains EPDO weights, column mappings)
- **Road types**: `county_roads`, `no_interstate`, `all_roads`
- **Forecast output naming**: `forecasts_{road_type}.json` (same name each month → overwrites)
- **AWS creds**: Already in GitHub Secrets as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- **R2 creds**: Already in GitHub Secrets as `CF_R2_ACCESS_KEY_ID`, `CF_R2_SECRET_ACCESS_KEY`, `CF_ACCOUNT_ID`

---

## State Config Example (`states/virginia/config.json`)

The EPDO weights vary by state. Virginia example:
```json
{
  "epdoWeights": {"K": 1032, "A": 53, "B": 12, "C": 5, "O": 1}
}
```

Colorado example:
```json
{
  "epdoWeights": {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}
}
```

If `states/{state}/config.json` doesn't exist for a new state, fall back to HSM defaults: `{"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}`.

---

## Testing Checklist

After implementing changes:

1. **Run for Henrico, Virginia**:
   ```bash
   python scripts/generate_forecast.py \
     --all-road-types --jurisdiction henrico \
     --data-dir data/VDOT --state virginia --source r2
   ```
   Verify:
   - CSVs downloaded from `https://data.aicreatesai.com/virginia/henrico/*.csv`
   - EPDO weights loaded from `states/virginia/config.json`
   - SageMaker endpoint called (not synthetic)
   - 3 forecast JSONs created with `"model": "amazon/chronos-2"`

2. **Run for Douglas, Colorado**:
   ```bash
   python scripts/generate_forecast.py \
     --all-road-types --jurisdiction douglas \
     --data-dir data/CDOT --state colorado --source r2
   ```
   Verify same as above with Colorado EPDO weights.

3. **Verify forecast JSON structure** — each file should contain:
   - `"model": "amazon/chronos-2"` (NOT `"synthetic-demo"`)
   - `"epdoWeights"` matching the state config
   - `"matrices"` with keys: m01, m02, m03, m04, m05, m06
   - `"derivedMetrics"` and `"backtesting"` sections

4. **Verify R2 upload path** — Stage 5b should upload to:
   - `virginia/henrico/forecasts_all_roads.json`
   - `virginia/henrico/forecasts_county_roads.json`
   - `virginia/henrico/forecasts_no_interstate.json`

5. **Verify frontend loads** — Navigate to Crash Prediction tab, select Henrico → each road type should load real forecasts.

---

## What NOT to Change

- Do NOT modify the overall pipeline.yml structure (stages 0-4, 6)
- Do NOT create new workflow files
- Do NOT change the download workflows (download-virginia.yml, download-colorado.yml)
- Do NOT change the frontend forecast loading logic
- Do NOT change the 6 matrix builder functions (M01-M06)
- Do NOT change the temporal embedding layer
- Do NOT change the invoke_endpoint() function (it already uses boto3 correctly)
- Do NOT change the R2 upload logic in Stage 5b (it already works correctly)
