#!/usr/bin/env python3
"""
Bug tests for the manual-upload-state.yml workflow.

Validates that:
  1. Valid YAML with correct GitHub Actions structure
  2. Default R2 key uses statewide_all_roads.csv.gz (NOT manual_upload.csv)
  3. Has exactly 2 jobs: normalize + trigger-pipeline
  4. Handles .csv.gz (gzipped) input with decompression
  5. Has all required workflow_dispatch inputs
  6. Has correct permissions (contents:write, actions:write)
  7. Normalize step calls state_adapter.py
  8. R2 re-upload step uploads normalized CSV
  9. Pipeline trigger dispatches correct workflow
 10. No leftover references to manual_upload.csv in default logic
 11. State options match download-registry.json states
 12. Gzipped copy uploaded to _state/ path

Run with:
    python tests/test_manual_upload_workflow_bugs.py
    python -m pytest tests/test_manual_upload_workflow_bugs.py -v
"""

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
WORKFLOWS_DIR = PROJECT_ROOT / '.github' / 'workflows'
MANUAL_UPLOAD = WORKFLOWS_DIR / 'manual-upload-state.yml'


def load_workflow(path):
    """Load a GitHub Actions YAML file, handling the 'on' -> True key issue."""
    content = path.read_text()
    parsed = yaml.safe_load(content)
    if True in parsed and 'on' not in parsed:
        parsed['on'] = parsed.pop(True)
    return parsed, content


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def workflow():
    """Load and parse manual-upload-state.yml."""
    assert MANUAL_UPLOAD.exists(), f'Missing: {MANUAL_UPLOAD}'
    return load_workflow(MANUAL_UPLOAD)


# ---------------------------------------------------------------------------
# Test 1: Valid YAML structure
# ---------------------------------------------------------------------------

class TestYAMLStructure:
    def test_valid_yaml(self, workflow):
        parsed, _ = workflow
        assert parsed is not None

    def test_has_name(self, workflow):
        parsed, _ = workflow
        assert 'name' in parsed
        assert 'Manual Upload' in parsed['name']

    def test_has_workflow_dispatch(self, workflow):
        parsed, _ = workflow
        assert 'workflow_dispatch' in parsed['on']

    def test_has_permissions(self, workflow):
        parsed, _ = workflow
        perms = parsed.get('permissions', {})
        assert perms.get('contents') == 'write', 'Needs contents:write'
        assert perms.get('actions') == 'write', 'Needs actions:write to trigger pipeline'


# ---------------------------------------------------------------------------
# Test 2: Default R2 key uses statewide_all_roads.csv.gz (NOT manual_upload.csv)
# ---------------------------------------------------------------------------

class TestDefaultR2Key:
    """The critical bug fix: default key must match existing R2 naming convention."""

    def test_default_key_is_statewide_all_roads_gz(self, workflow):
        """Default R2 key in bash logic must be statewide_all_roads.csv.gz."""
        _, raw = workflow
        assert 'statewide_all_roads.csv.gz' in raw, \
            'Default R2 key must use statewide_all_roads.csv.gz'

    def test_no_manual_upload_csv_in_default(self, workflow):
        """manual_upload.csv must NOT appear as the default key."""
        _, raw = workflow
        # Check the bash default assignment line specifically
        lines = raw.splitlines()
        for line in lines:
            stripped = line.strip()
            # Skip comments
            if stripped.startswith('#'):
                continue
            # The default assignment line
            if 'R2_RAW_KEY=' in stripped and 'manual_upload' in stripped:
                pytest.fail(
                    f'Found manual_upload.csv in default key assignment: {stripped}'
                )

    def test_description_mentions_statewide(self, workflow):
        """The r2_raw_key input description should reference statewide_all_roads."""
        parsed, _ = workflow
        inputs = parsed['on']['workflow_dispatch']['inputs']
        r2_key_desc = inputs['r2_raw_key']['description']
        assert 'statewide_all_roads' in r2_key_desc, \
            f'Description should mention statewide_all_roads, got: {r2_key_desc}'

    def test_description_does_not_mention_manual_upload(self, workflow):
        """The r2_raw_key input description should NOT reference manual_upload."""
        parsed, _ = workflow
        inputs = parsed['on']['workflow_dispatch']['inputs']
        r2_key_desc = inputs['r2_raw_key']['description']
        assert 'manual_upload' not in r2_key_desc, \
            f'Description should not mention manual_upload, got: {r2_key_desc}'


# ---------------------------------------------------------------------------
# Test 3: Job structure (normalize + trigger-pipeline)
# ---------------------------------------------------------------------------

class TestJobStructure:
    def test_has_exactly_two_jobs(self, workflow):
        parsed, _ = workflow
        jobs = parsed.get('jobs', {})
        assert len(jobs) == 2, f'Expected 2 jobs, got {len(jobs)}: {list(jobs.keys())}'

    def test_has_normalize_job(self, workflow):
        parsed, _ = workflow
        assert 'normalize' in parsed['jobs']

    def test_has_trigger_pipeline_job(self, workflow):
        parsed, _ = workflow
        assert 'trigger-pipeline' in parsed['jobs']

    def test_trigger_depends_on_normalize(self, workflow):
        parsed, _ = workflow
        trigger = parsed['jobs']['trigger-pipeline']
        needs = trigger.get('needs', '')
        if isinstance(needs, list):
            assert 'normalize' in needs
        else:
            assert needs == 'normalize'

    def test_trigger_has_skip_condition(self, workflow):
        """trigger-pipeline should be skippable via skip_pipeline input."""
        parsed, _ = workflow
        trigger = parsed['jobs']['trigger-pipeline']
        condition = trigger.get('if', '')
        assert 'skip_pipeline' in condition

    def test_normalize_has_outputs(self, workflow):
        parsed, _ = workflow
        normalize = parsed['jobs']['normalize']
        outputs = normalize.get('outputs', {})
        assert 'state' in outputs, 'Normalize job must output state'
        assert 'r2_data_key' in outputs, 'Normalize job must output r2_data_key'


# ---------------------------------------------------------------------------
# Test 4: Gzip handling
# ---------------------------------------------------------------------------

class TestGzipHandling:
    """Workflow must handle .csv.gz input since R2 stores gzipped files."""

    def test_has_gzip_detection(self, workflow):
        _, raw = workflow
        assert '.gz' in raw, 'Must detect .gz extension'

    def test_has_gunzip_step(self, workflow):
        """Must decompress gzipped files after download."""
        _, raw = workflow
        assert 'gunzip' in raw, 'Must gunzip downloaded .csv.gz files'

    def test_uploads_gzipped_copy(self, workflow):
        """Must upload gzipped copy back to _state/ path."""
        _, raw = workflow
        assert 'gzip -k' in raw or 'gzip' in raw, 'Must create gzipped copy for upload'

    def test_gzipped_upload_key_is_correct(self, workflow):
        """Gzipped copy should go to {prefix}/_state/statewide_all_roads.csv.gz."""
        _, raw = workflow
        assert 'statewide_all_roads.csv.gz' in raw


# ---------------------------------------------------------------------------
# Test 5: Required workflow_dispatch inputs
# ---------------------------------------------------------------------------

class TestWorkflowInputs:
    REQUIRED_INPUTS = ['state', 'r2_raw_key', 'scope', 'selection',
                       'skip_pipeline', 'skip_forecasts', 'dry_run']

    @pytest.mark.parametrize('input_name', REQUIRED_INPUTS)
    def test_has_input(self, workflow, input_name):
        parsed, _ = workflow
        inputs = parsed['on']['workflow_dispatch']['inputs']
        assert input_name in inputs, f'Missing input: {input_name}'

    def test_state_is_choice(self, workflow):
        parsed, _ = workflow
        state_input = parsed['on']['workflow_dispatch']['inputs']['state']
        assert state_input.get('type') == 'choice'

    def test_state_is_required(self, workflow):
        parsed, _ = workflow
        state_input = parsed['on']['workflow_dispatch']['inputs']['state']
        assert state_input.get('required') is True

    def test_r2_raw_key_is_optional(self, workflow):
        parsed, _ = workflow
        r2_input = parsed['on']['workflow_dispatch']['inputs']['r2_raw_key']
        assert r2_input.get('required') is False or r2_input.get('required') is None

    def test_scope_default_is_statewide(self, workflow):
        parsed, _ = workflow
        scope_input = parsed['on']['workflow_dispatch']['inputs']['scope']
        assert scope_input.get('default') == 'statewide'

    def test_scope_options(self, workflow):
        parsed, _ = workflow
        scope_input = parsed['on']['workflow_dispatch']['inputs']['scope']
        options = scope_input.get('options', [])
        assert 'statewide' in options
        assert 'jurisdiction' in options


# ---------------------------------------------------------------------------
# Test 6: Normalize step
# ---------------------------------------------------------------------------

class TestNormalizeStep:
    def test_calls_state_adapter(self, workflow):
        """Must use state_adapter.py for normalization."""
        _, raw = workflow
        assert 'state_adapter.py' in raw

    def test_has_fallback_normalizer(self, workflow):
        """Should fall back to process_crash_data.py if state_adapter fails."""
        _, raw = workflow
        assert 'process_crash_data.py' in raw

    def test_validates_normalized_output(self, workflow):
        """Must validate normalized CSV has required columns."""
        _, raw = workflow
        assert 'Document Nbr' in raw
        assert 'Crash Severity' in raw
        assert 'Crash Date' in raw

    def test_outputs_severity_distribution(self, workflow):
        """Should log severity distribution for verification."""
        _, raw = workflow
        assert 'severity' in raw.lower()

    def test_uses_download_registry(self, workflow):
        """Must resolve state config from download-registry.json."""
        _, raw = workflow
        assert 'download-registry.json' in raw


# ---------------------------------------------------------------------------
# Test 7: R2 upload of normalized data
# ---------------------------------------------------------------------------

class TestR2Upload:
    def test_uploads_normalized_csv(self, workflow):
        _, raw = workflow
        assert 'Upload normalized CSV to R2' in raw

    def test_uses_aws_s3_cp(self, workflow):
        _, raw = workflow
        assert 'aws s3 cp' in raw

    def test_has_upload_retry(self, workflow):
        _, raw = workflow
        assert 'for i in' in raw

    def test_has_upload_verification(self, workflow):
        _, raw = workflow
        assert 'head-object' in raw

    def test_upload_key_follows_convention(self, workflow):
        """Normalized CSV must go to {prefix}/statewide/{state}_statewide_all_roads.csv."""
        _, raw = workflow
        assert 'statewide_all_roads.csv' in raw


# ---------------------------------------------------------------------------
# Test 8: Pipeline trigger
# ---------------------------------------------------------------------------

class TestPipelineTrigger:
    def test_triggers_batch_pipeline_for_statewide(self, workflow):
        _, raw = workflow
        assert 'batch-pipeline.yml' in raw

    def test_triggers_pipeline_for_jurisdiction(self, workflow):
        _, raw = workflow
        assert 'pipeline.yml' in raw

    def test_uses_create_workflow_dispatch(self, workflow):
        _, raw = workflow
        assert 'createWorkflowDispatch' in raw

    def test_passes_r2_data_key(self, workflow):
        _, raw = workflow
        assert 'r2_data_key' in raw or 'r2DataKey' in raw

    def test_passes_skip_forecasts(self, workflow):
        _, raw = workflow
        assert 'skip_forecasts' in raw or 'skipForecasts' in raw


# ---------------------------------------------------------------------------
# Test 9: No leftover manual_upload references in operational code
# ---------------------------------------------------------------------------

class TestNoManualUploadLeftovers:
    """Ensure manual_upload.csv is fully replaced in all non-comment code."""

    def test_no_manual_upload_in_bash_defaults(self, workflow):
        """The default key assignment must not use manual_upload."""
        _, raw = workflow
        lines = raw.splitlines()
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            if 'R2_RAW_KEY=' in stripped and 'manual_upload' in stripped:
                pytest.fail(f'manual_upload found in default: {stripped}')

    def test_no_manual_upload_in_description(self, workflow):
        """Input descriptions should not reference manual_upload.csv."""
        parsed, _ = workflow
        inputs = parsed['on']['workflow_dispatch']['inputs']
        for name, cfg in inputs.items():
            desc = cfg.get('description', '')
            assert 'manual_upload.csv' not in desc, \
                f'Input {name} description still references manual_upload.csv'


# ---------------------------------------------------------------------------
# Test 10: Workflow size sanity
# ---------------------------------------------------------------------------

class TestWorkflowSize:
    def test_under_700_lines(self, workflow):
        _, raw = workflow
        lines = raw.count('\n') + 1
        assert lines < 700, f'Workflow is {lines} lines — should be < 700'

    def test_under_30kb(self, workflow):
        size = MANUAL_UPLOAD.stat().st_size
        assert size < 30_000, f'Workflow is {size} bytes — should be < 30KB'


# ---------------------------------------------------------------------------
# Test 11: Download step has retry logic
# ---------------------------------------------------------------------------

class TestDownloadResilience:
    def test_has_download_retry(self, workflow):
        """R2 download must have retry with exponential backoff."""
        _, raw = workflow
        assert 'for i in 1 2 3 4' in raw

    def test_validates_downloaded_file(self, workflow):
        """Must check downloaded file is non-empty."""
        _, raw = workflow
        assert '! -s' in raw or 'empty' in raw.lower()


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v', '--tb=short']))
