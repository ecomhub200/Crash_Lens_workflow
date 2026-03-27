#!/usr/bin/env python3
"""
Bug tests for the batch workflow architecture:
  - batch-all-jurisdictions.yml: Download + R2 upload + trigger batch-pipeline.yml
  - batch-pipeline.yml: Download from R2 + process (same stages as pipeline.yml)

Validates that:
  1. Both files are valid YAML with correct GitHub Actions structure
  2. batch-all-jurisdictions.yml has exactly 2 jobs: download + trigger-pipeline
  3. Triggers batch-pipeline.yml (NOT pipeline.yml) with R2 key
  4. Preserves all 51 state options
  5. Has no leftover references to removed processing stages
  6. Mirrors download-virginia.yml trigger pattern
  7. Has correct permissions (contents:write, actions:write)
  8. Has correct job dependency chain
  9. Passes skip_forecasts and dry_run through to batch pipeline
 10. Has no references to removed inputs
 11. Uses R2 upload instead of git commit for statewide data
 12. batch-pipeline.yml downloads from R2 and has all processing stages
 13. pipeline.yml is untouched (still works for single-jurisdiction)

Run with:
    python tests/test_batch_workflow_bugs.py
    python -m pytest tests/test_batch_workflow_bugs.py -v
"""

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.resolve()


def load_workflow(path):
    """Load a GitHub Actions YAML file, handling the 'on' -> True key issue."""
    content = path.read_text()
    parsed = yaml.safe_load(content)
    # PyYAML parses bare 'on' as boolean True
    if True in parsed and 'on' not in parsed:
        parsed['on'] = parsed.pop(True)
    return parsed, content


WORKFLOWS_DIR = PROJECT_ROOT / '.github' / 'workflows'

BATCH_WORKFLOW = WORKFLOWS_DIR / 'batch-all-jurisdictions.yml'
BATCH_PIPELINE = WORKFLOWS_DIR / 'batch-pipeline.yml'
DOWNLOAD_VA_WORKFLOW = WORKFLOWS_DIR / 'download-virginia.yml'
PIPELINE_WORKFLOW = WORKFLOWS_DIR / 'pipeline.yml'


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def batch_yaml():
    """Load and parse batch-all-jurisdictions.yml."""
    assert BATCH_WORKFLOW.exists(), f'Missing: {BATCH_WORKFLOW}'
    return load_workflow(BATCH_WORKFLOW)


@pytest.fixture(scope='module')
def batch_pipeline_yaml():
    """Load and parse batch-pipeline.yml."""
    assert BATCH_PIPELINE.exists(), f'Missing: {BATCH_PIPELINE}'
    return load_workflow(BATCH_PIPELINE)


@pytest.fixture(scope='module')
def download_va_yaml():
    """Load and parse download-virginia.yml."""
    assert DOWNLOAD_VA_WORKFLOW.exists(), f'Missing: {DOWNLOAD_VA_WORKFLOW}'
    parsed, _ = load_workflow(DOWNLOAD_VA_WORKFLOW)
    return parsed


@pytest.fixture(scope='module')
def pipeline_yaml():
    """Load and parse pipeline.yml."""
    assert PIPELINE_WORKFLOW.exists(), f'Missing: {PIPELINE_WORKFLOW}'
    parsed, _ = load_workflow(PIPELINE_WORKFLOW)
    return parsed


# ---------------------------------------------------------------------------
# Test 1: Valid YAML structure
# ---------------------------------------------------------------------------

class TestYAMLStructure:
    def test_valid_yaml(self, batch_yaml):
        """Workflow file must be parseable YAML."""
        parsed, _ = batch_yaml
        assert parsed is not None

    def test_has_name(self, batch_yaml):
        parsed, _ = batch_yaml
        assert 'name' in parsed
        assert 'Batch All Jurisdictions' in parsed['name']

    def test_has_workflow_dispatch(self, batch_yaml):
        parsed, _ = batch_yaml
        assert 'workflow_dispatch' in parsed['on']

    def test_has_permissions(self, batch_yaml):
        parsed, _ = batch_yaml
        perms = parsed.get('permissions', {})
        assert perms.get('contents') == 'write', 'Needs contents:write'
        assert perms.get('actions') == 'write', 'Needs actions:write to trigger batch-pipeline.yml'


# ---------------------------------------------------------------------------
# Test 2: Exactly 2 jobs (download + trigger-pipeline)
# ---------------------------------------------------------------------------

class TestJobStructure:
    def test_has_exactly_two_jobs(self, batch_yaml):
        parsed, _ = batch_yaml
        jobs = parsed.get('jobs', {})
        assert len(jobs) == 2, f'Expected 2 jobs, got {len(jobs)}: {list(jobs.keys())}'

    def test_has_download_job(self, batch_yaml):
        parsed, _ = batch_yaml
        assert 'download' in parsed['jobs']

    def test_has_trigger_pipeline_job(self, batch_yaml):
        parsed, _ = batch_yaml
        assert 'trigger-pipeline' in parsed['jobs']

    def test_trigger_depends_on_download(self, batch_yaml):
        parsed, _ = batch_yaml
        trigger = parsed['jobs']['trigger-pipeline']
        needs = trigger.get('needs', '')
        if isinstance(needs, list):
            assert 'download' in needs
        else:
            assert needs == 'download'

    def test_trigger_has_skip_condition(self, batch_yaml):
        """trigger-pipeline should be skippable via skip_pipeline input."""
        parsed, _ = batch_yaml
        trigger = parsed['jobs']['trigger-pipeline']
        condition = trigger.get('if', '')
        assert 'skip_pipeline' in condition

    def test_download_job_has_outputs(self, batch_yaml):
        parsed, _ = batch_yaml
        download = parsed['jobs']['download']
        outputs = download.get('outputs', {})
        assert 'state' in outputs, 'Download job must output state'
        assert 'csv_path' in outputs, 'Download job must output csv_path'
        assert 'r2_data_key' in outputs, 'Download job must output r2_data_key'


# ---------------------------------------------------------------------------
# Test 3: Pipeline trigger passes correct inputs
# ---------------------------------------------------------------------------

class TestPipelineTrigger:
    def test_triggers_batch_pipeline_yml(self, batch_yaml):
        """Must reference batch-pipeline.yml (NOT pipeline.yml) in the trigger step."""
        _, raw = batch_yaml
        assert 'batch-pipeline.yml' in raw

    def test_passes_scope_statewide(self, batch_yaml):
        """Must pass scope: 'statewide' to batch pipeline."""
        _, raw = batch_yaml
        assert "scope: 'statewide'" in raw or 'scope: statewide' in raw

    def test_passes_state_input(self, batch_yaml):
        """Must forward the selected state to batch pipeline."""
        _, raw = batch_yaml
        assert 'state: state' in raw or "state:" in raw

    def test_passes_r2_data_key(self, batch_yaml):
        """Must pass R2 key (not csv_path) to batch pipeline."""
        _, raw = batch_yaml
        assert 'r2_data_key' in raw

    def test_passes_skip_forecasts(self, batch_yaml):
        """Must forward skip_forecasts to batch pipeline."""
        _, raw = batch_yaml
        assert 'skip_forecasts' in raw

    def test_passes_dry_run(self, batch_yaml):
        """Must forward dry_run to batch pipeline."""
        _, raw = batch_yaml
        assert 'dry_run' in raw

    def test_uses_create_workflow_dispatch(self, batch_yaml):
        """Must use createWorkflowDispatch API (same as download-virginia.yml)."""
        _, raw = batch_yaml
        assert 'createWorkflowDispatch' in raw

    def test_selection_is_empty_for_statewide(self, batch_yaml):
        """For statewide scope, selection should be empty string."""
        _, raw = batch_yaml
        assert "selection: ''" in raw or "selection: \"\"" in raw


# ---------------------------------------------------------------------------
# Test 4: State options preserved
# ---------------------------------------------------------------------------

class TestStateOptions:
    EXPECTED_STATES = [
        'virginia', 'colorado', 'alabama', 'alaska', 'arizona', 'arkansas',
        'california', 'connecticut', 'delaware', 'district_of_columbia',
        'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana',
        'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
        'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri',
        'montana', 'nebraska', 'nevada', 'new_hampshire', 'new_jersey',
        'new_mexico', 'new_york', 'north_carolina', 'north_dakota', 'ohio',
        'oklahoma', 'oregon', 'pennsylvania', 'rhode_island', 'south_carolina',
        'south_dakota', 'tennessee', 'texas', 'utah', 'vermont',
        'washington_state', 'west_virginia', 'wisconsin', 'wyoming',
    ]

    def test_has_all_51_state_options(self, batch_yaml):
        parsed, _ = batch_yaml
        state_input = parsed['on']['workflow_dispatch']['inputs']['state']
        options = state_input.get('options', [])
        assert len(options) == 51, f'Expected 51 states (50 + DC), got {len(options)}'

    @pytest.mark.parametrize('state', EXPECTED_STATES)
    def test_state_present(self, batch_yaml, state):
        parsed, _ = batch_yaml
        options = parsed['on']['workflow_dispatch']['inputs']['state']['options']
        assert state in options, f'{state} missing from state options'


# ---------------------------------------------------------------------------
# Test 5: No leftover references to removed processing stages
# ---------------------------------------------------------------------------

class TestNoLeftoverStages:
    """The batch download workflow must NOT contain processing logic."""

    def test_no_split_jurisdictions_call(self, batch_yaml):
        _, raw = batch_yaml
        assert 'split_jurisdictions.py' not in raw, \
            'split_jurisdictions.py should be in batch-pipeline.yml, not batch'

    def test_no_split_road_type_call(self, batch_yaml):
        _, raw = batch_yaml
        assert 'split_road_type.py' not in raw

    def test_no_generate_forecast(self, batch_yaml):
        _, raw = batch_yaml
        assert 'generate_forecast.py' not in raw

    def test_no_generate_aggregates(self, batch_yaml):
        _, raw = batch_yaml
        assert 'generate_aggregates.py' not in raw

    def test_no_aggregate_by_scope(self, batch_yaml):
        _, raw = batch_yaml
        assert 'aggregate_by_scope.py' not in raw


# ---------------------------------------------------------------------------
# Test 6: Mirrors download-virginia.yml trigger pattern
# ---------------------------------------------------------------------------

class TestMirrorsDownloadVirginia:
    def test_both_have_trigger_pipeline_job(self, batch_yaml, download_va_yaml):
        parsed, _ = batch_yaml
        assert 'trigger-pipeline' in parsed['jobs']
        assert 'trigger-pipeline' in download_va_yaml['jobs']

    def test_both_use_actions_github_script(self, batch_yaml, download_va_yaml):
        parsed, _ = batch_yaml
        batch_trigger = parsed['jobs']['trigger-pipeline']
        va_trigger = download_va_yaml['jobs']['trigger-pipeline']

        batch_uses = [s.get('uses', '') for s in batch_trigger.get('steps', [])]
        va_uses = [s.get('uses', '') for s in va_trigger.get('steps', [])]

        assert any('github-script' in u for u in batch_uses)
        assert any('github-script' in u for u in va_uses)

    def test_both_have_skip_pipeline_input(self, batch_yaml, download_va_yaml):
        parsed, _ = batch_yaml
        batch_inputs = parsed['on']['workflow_dispatch']['inputs']
        va_inputs = download_va_yaml['on']['workflow_dispatch']['inputs']
        assert 'skip_pipeline' in batch_inputs
        assert 'skip_pipeline' in va_inputs

    def test_both_have_actions_write_permission(self, batch_yaml, download_va_yaml):
        parsed, _ = batch_yaml
        assert parsed.get('permissions', {}).get('actions') == 'write'
        assert download_va_yaml.get('permissions', {}).get('actions') == 'write'


# ---------------------------------------------------------------------------
# Test 7: R2 upload replaces git commit
# ---------------------------------------------------------------------------

class TestR2Upload:
    """batch-all-jurisdictions.yml should upload to R2 instead of git commit."""

    def test_has_r2_upload_step(self, batch_yaml):
        _, raw = batch_yaml
        assert 'Upload statewide CSV to R2' in raw

    def test_uses_aws_s3_cp(self, batch_yaml):
        _, raw = batch_yaml
        assert 'aws s3 cp' in raw

    def test_references_r2_bucket(self, batch_yaml):
        _, raw = batch_yaml
        assert 'crash-lens-data' in raw

    def test_references_r2_endpoint(self, batch_yaml):
        _, raw = batch_yaml
        assert 'r2.cloudflarestorage.com' in raw

    def test_has_r2_upload_retry(self, batch_yaml):
        """R2 upload should have retry logic."""
        _, raw = batch_yaml
        assert 'for i in' in raw
        assert 'attempt' in raw.lower() or 'retry' in raw.lower()

    def test_has_r2_upload_verification(self, batch_yaml):
        """Should verify R2 upload succeeded."""
        _, raw = batch_yaml
        assert 'head-object' in raw or 'verification' in raw.lower()

    def test_no_git_commit_of_csv(self, batch_yaml):
        """Should NOT git commit large CSVs anymore (comments are OK)."""
        _, raw = batch_yaml
        # Check non-comment lines only
        code_lines = [line for line in raw.splitlines()
                      if line.strip() and not line.strip().startswith('#')]
        code_text = '\n'.join(code_lines)
        assert 'git commit' not in code_text, \
            'Statewide CSVs should go to R2, not git'

    def test_no_git_push(self, batch_yaml):
        """Should NOT git push (no data committed)."""
        _, raw = batch_yaml
        assert 'git push' not in raw

    def test_outputs_r2_key(self, batch_yaml):
        parsed, _ = batch_yaml
        download = parsed['jobs']['download']
        outputs = download.get('outputs', {})
        assert 'r2_data_key' in outputs


# ---------------------------------------------------------------------------
# Test 8: batch-pipeline.yml structure
# ---------------------------------------------------------------------------

class TestBatchPipelineStructure:
    """batch-pipeline.yml should download from R2 and process identically to pipeline.yml."""

    def test_exists(self):
        assert BATCH_PIPELINE.exists(), f'Missing: {BATCH_PIPELINE}'

    def test_valid_yaml(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        assert parsed is not None

    def test_has_name(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        assert 'name' in parsed

    def test_accepts_r2_data_key_input(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'r2_data_key' in inputs, 'Must accept r2_data_key input'

    def test_accepts_state_input(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'state' in inputs

    def test_accepts_skip_forecasts(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'skip_forecasts' in inputs

    def test_accepts_dry_run(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'dry_run' in inputs

    def test_has_prepare_job(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        assert 'prepare' in parsed['jobs']

    def test_has_process_job(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        assert 'process' in parsed['jobs']

    def test_downloads_from_r2(self, batch_pipeline_yaml):
        """Must download statewide CSV from R2."""
        _, raw = batch_pipeline_yaml
        assert 'Download statewide CSV from R2' in raw
        assert 'aws s3 cp' in raw

    def test_has_all_processing_stages(self, batch_pipeline_yaml):
        """Must have same processing stages as pipeline.yml."""
        _, raw = batch_pipeline_yaml
        assert 'split_jurisdictions.py' in raw, 'Missing Stage 1: Split by jurisdiction'
        assert 'split_road_type.py' in raw, 'Missing Stage 2: Split by road type'
        assert 'aggregate_by_scope.py' in raw, 'Missing Stage 3: Aggregate'
        assert 'Stage 4: Upload to R2' in raw, 'Missing Stage 4: Upload'
        assert 'generate_forecast.py' in raw, 'Missing Stage 5: Forecasts'

    def test_has_r2_upload_stage(self, batch_pipeline_yaml):
        _, raw = batch_pipeline_yaml
        assert 'crash-lens-data' in raw

    def test_process_depends_on_prepare(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        process = parsed['jobs']['process']
        needs = process.get('needs', '')
        if isinstance(needs, list):
            assert 'prepare' in needs
        else:
            assert needs == 'prepare'


# ---------------------------------------------------------------------------
# Test 9: pipeline.yml untouched (single-jurisdiction still works)
# ---------------------------------------------------------------------------

class TestPipelineUntouched:
    """pipeline.yml must still work for single-jurisdiction workflows."""

    def test_pipeline_accepts_data_source(self, pipeline_yaml):
        """pipeline.yml still accepts data_source (file path from git)."""
        inputs = pipeline_yaml['on']['workflow_dispatch']['inputs']
        assert 'data_source' in inputs

    def test_pipeline_accepts_state(self, pipeline_yaml):
        inputs = pipeline_yaml['on']['workflow_dispatch']['inputs']
        assert 'state' in inputs

    def test_pipeline_accepts_scope(self, pipeline_yaml):
        inputs = pipeline_yaml['on']['workflow_dispatch']['inputs']
        assert 'scope' in inputs

    def test_pipeline_does_not_reference_r2_data_key(self, pipeline_yaml):
        """pipeline.yml should NOT have r2_data_key — it uses git-committed CSVs."""
        inputs = pipeline_yaml['on']['workflow_dispatch']['inputs']
        assert 'r2_data_key' not in inputs, \
            'pipeline.yml should use data_source, not r2_data_key'


# ---------------------------------------------------------------------------
# Test 10: No removed inputs remain
# ---------------------------------------------------------------------------

class TestNoRemovedInputs:
    """Inputs that belonged to the monolithic batch workflow should be gone."""

    def test_no_batch_size_input(self, batch_yaml):
        parsed, _ = batch_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'batch_size' not in inputs, 'batch_size is no longer needed'

    def test_no_jurisdictions_dropdown(self, batch_yaml):
        """The massive 2,857-jurisdiction dropdown should be removed."""
        parsed, _ = batch_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'jurisdictions' not in inputs

    def test_no_skip_validation_input(self, batch_yaml):
        parsed, _ = batch_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'skip_validation' not in inputs

    def test_no_skip_geocode_input(self, batch_yaml):
        parsed, _ = batch_yaml
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert 'skip_geocode' not in inputs


# ---------------------------------------------------------------------------
# Test 11: Download job handles all 3 state types
# ---------------------------------------------------------------------------

class TestDownloadStages:
    def test_handles_virginia(self, batch_yaml):
        _, raw = batch_yaml
        assert 'download_crash_data.py' in raw

    def test_handles_colorado(self, batch_yaml):
        _, raw = batch_yaml
        assert 'download_cdot_crash_data.py' in raw

    def test_handles_generic_states(self, batch_yaml):
        """Generic state handler should find state-specific download scripts."""
        _, raw = batch_yaml
        assert 'download_*_crash_data.py' in raw or 'download_' in raw

    def test_has_merge_stage(self, batch_yaml):
        _, raw = batch_yaml
        assert 'Merge' in raw or 'merge' in raw

    def test_has_convert_stage(self, batch_yaml):
        _, raw = batch_yaml
        assert 'Convert' in raw or 'convert' in raw or 'standardized' in raw

    def test_has_timeout(self, batch_yaml):
        parsed, _ = batch_yaml
        download = parsed['jobs']['download']
        assert download.get('timeout-minutes', 0) > 0


# ---------------------------------------------------------------------------
# Test 12: Workflow size sanity check
# ---------------------------------------------------------------------------

class TestWorkflowSize:
    def test_batch_under_600_lines(self, batch_yaml):
        """Batch workflow should be under 600 lines."""
        _, raw = batch_yaml
        lines = raw.count('\n') + 1
        assert lines < 600, f'Workflow is {lines} lines — should be < 600'

    def test_batch_under_25kb(self, batch_yaml):
        """File size sanity check."""
        size = BATCH_WORKFLOW.stat().st_size
        assert size < 25_000, f'Workflow is {size} bytes — should be < 25KB'

    def test_batch_pipeline_under_600_lines(self, batch_pipeline_yaml):
        _, raw = batch_pipeline_yaml
        lines = raw.count('\n') + 1
        assert lines < 600, f'batch-pipeline.yml is {lines} lines'


# ---------------------------------------------------------------------------
# CLI runner (for running without pytest)
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v', '--tb=short']))
