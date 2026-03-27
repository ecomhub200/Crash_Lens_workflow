# Revised Delaware Pipeline Integration Prompt

```
I have a Delaware crash data normalizer at `states/delaware/de_normalize.py`.
I need you to verify and fix its integration into the existing CrashLens pipeline architecture.

Here is what the script does:
- Input: raw DelDOT CSV from data.delaware.gov (Socrata dataset 827n-m6xc)
- Output: normalized CSV matching the CrashLens 69-column standard schema (+ extra passthrough columns)
- CLI: `python de_normalize.py input.csv output.csv` (positional args, no flags)
- The script is idempotent — re-running on already-normalized data is safe

The following files ALREADY EXIST in the repo:

| File | Status |
|------|--------|
| `states/delaware/de_normalize.py` | EXISTS — 368-line normalizer |
| `states/delaware/config.json` | EXISTS — column mappings, EPDO weights, jurisdictions, data source |
| `states/delaware/hierarchy.json` | EXISTS — counties, regions, MPOs |
| `states/delaware/pipeline.md` | EXISTS — full pipeline documentation |
| `.github/workflows/delaware-batch-all-jurisdictions.yml` | EXISTS — download + normalize + R2 upload workflow |
| `.github/workflows/delaware-batch-pipeline.yml` | EXISTS — processing pipeline (Stages 0-6) with normalization validation |
| `states/download-registry.json` | EXISTS — Delaware entry already present (tier 3) |

All workflows live at the REPO ROOT `.github/workflows/` — NOT under `states/delaware/.github/`.

Do the following:

1. **Audit `delaware-batch-all-jurisdictions.yml`** — Verify the normalization step correctly calls:
   ```
   python states/delaware/de_normalize.py <raw_csv> <normalized_csv>
   ```
   (NOT `--input`/`--output` flags — the script uses positional args).
   Check that the normalized CSV overwrites/replaces the raw CSV before R2 upload.
   Verify it triggers `delaware-batch-pipeline.yml` (not `batch-pipeline.yml`).

2. **Audit `delaware-batch-pipeline.yml`** — Verify:
   - Stage 0.25 (HTML normalization tool via Playwright) is correctly wired
   - Stage 0.5 (normalization validation) runs BEFORE Stage 1 (Split by Jurisdiction)
   - The R2 download path uses `delaware/_state/statewide_all_roads.csv.gz`
   - All stage inputs/outputs chain correctly

3. **Audit `states/download-registry.json`** Delaware entry — Currently it has:
   ```json
   {
     "needsStandardization": true,
     "needsMerge": true
   }
   ```
   Since `de_normalize.py` handles standardization, should these be `false`?
   Or does the Delaware-specific workflow bypass them anyway?
   Also check if `pipelineWorkflow` should be set to `"delaware-batch-pipeline.yml"`.

4. **Audit `states/delaware/config.json`** — Verify:
   - The `columnMapping` matches what `de_normalize.py` actually produces
   - The `epdoWeights` match FHWA-SA-25-021 values used in the HTML tool
   - The 3 jurisdiction entries (Kent, New Castle, Sussex) have correct FIPS codes

5. **Check for consistency issues** between:
   - The severity mapping in `de_normalize.py` (Personal Injury → B)
   - The severity mapping in `config.json` (Personal Injury → A)
   - The severity mapping documented in `pipeline.md`
   These MUST be consistent — pick one and fix the others.

6. **Verify the `batch-all-jurisdictions.yml` (generic) dropdown** includes
   `delaware` in its state options list (it should already be there).

After auditing, show me:
- A summary of every inconsistency found
- Every file you modified and what you changed
- The exact normalization step from `delaware-batch-all-jurisdictions.yml` so I can verify it
- Whether the download-registry.json entry needs updating and why
- The severity mapping resolution (which value did you pick and why)
```
