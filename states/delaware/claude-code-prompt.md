# Claude Code Prompt: Delaware Crash Data Normalization

Copy and paste this entire prompt into Claude Code to trigger the Delaware normalization pipeline automatically.

---

## Prompt

```
Run the Delaware crash data normalization pipeline. Follow these steps exactly:

1. DOWNLOAD: Fetch the latest Delaware crash data from the Socrata API:
   - Dataset: https://data.delaware.gov/resource/827n-m6xc.json
   - Use offset-based pagination with $limit=50000 per request
   - Save raw CSV to data/DelawareDOT/delaware_raw.csv
   - Expected: ~566,000 records with 40 columns

2. NORMALIZE: Run the normalizer script:
   ```bash
   python states/delaware/de_normalize.py data/DelawareDOT/delaware_raw.csv data/DelawareDOT/delaware_statewide_all_roads.csv
   ```

3. VALIDATE: Check the output meets CrashLens standards:
   - Verify all 69 standard columns exist
   - Confirm Crash Severity values are KABCO only (K/A/B/C/O)
   - Confirm crash IDs follow format DE-YYYY-NNNNNN
   - Confirm Crash Date is MM/DD/YYYY format
   - Confirm Crash Military Time is HHMM 24-hour format
   - Confirm Physical Juris Name matches "NNN. County Name" pattern
   - Report severity distribution (expect ~0.3% K, ~30% B, ~70% O)
   - Report coordinate coverage percentage

4. GEOGRAPHY CHECK: Verify jurisdiction assignment:
   - All rows should have Physical Juris Name (not "Unknown")
   - Expected counties: 001. Kent County, 003. New Castle County, 005. Sussex County
   - Report distribution across counties

5. REPORT: Print a summary table showing:
   - Total records in/out
   - Column count (should be 69+)
   - Severity breakdown (K/A/B/C/O counts)
   - County breakdown
   - Coordinate coverage %
   - Any unmapped values found

If any validation check fails at >5% error rate, stop and report the issue.
Do NOT upload to R2 — that's handled by the GitHub Actions workflow.
```

## Alternative: One-Line Trigger

For quick re-normalization of an existing raw CSV:

```bash
python states/delaware/de_normalize.py data/DelawareDOT/Public_Crash_Data_*.csv data/DelawareDOT/delaware_statewide_all_roads.csv
```

## Triggering via GitHub Actions

For the full automated pipeline (download + normalize + split + upload + forecast):

1. Go to repository **Actions** tab
2. Select **"Delaware: Batch All Jurisdictions"**
3. Click **"Run workflow"**
4. Set scope to `statewide`, leave other defaults
5. Click **"Run workflow"** button

The workflow will automatically:
- Download from data.delaware.gov
- Run de_normalize.py
- Validate output
- Upload to R2
- Trigger the Delaware batch pipeline
- Split by jurisdiction (3 counties)
- Generate forecasts
- Update manifest
