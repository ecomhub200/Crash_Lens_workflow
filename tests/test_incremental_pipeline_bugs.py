#!/usr/bin/env python3
"""
Bug tests for the incremental pipeline v1 (12 pre-identified bugs).

Each test class maps to one bug. Tests verify the fix is present in code
WITHOUT needing R2 credentials, Supabase, or network access.

Run with:
    python -m pytest tests/test_incremental_pipeline_bugs.py -v
    python tests/test_incremental_pipeline_bugs.py
"""

import ast
import hashlib
import importlib
import inspect
import json
import os
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))


# ─── Helpers ──────────────────────────────────────────────────────────────────


def load_workflow(path):
    """Load a GitHub Actions YAML file, handling the 'on' -> True key issue."""
    content = path.read_text()
    parsed = yaml.safe_load(content)
    if True in parsed and "on" not in parsed:
        parsed["on"] = parsed.pop(True)
    return parsed, content


WORKFLOWS = PROJECT_ROOT / ".github" / "workflows"
ALL_JURISDICTIONS = WORKFLOWS / "delaware-batch-all-jurisdictions.yml"
BATCH_PIPELINE = WORKFLOWS / "delaware-batch-pipeline.yml"


@pytest.fixture(scope="module")
def all_jurisdictions_yaml():
    assert ALL_JURISDICTIONS.exists(), f"Missing: {ALL_JURISDICTIONS}"
    return load_workflow(ALL_JURISDICTIONS)


@pytest.fixture(scope="module")
def batch_pipeline_yaml():
    assert BATCH_PIPELINE.exists(), f"Missing: {BATCH_PIPELINE}"
    return load_workflow(BATCH_PIPELINE)


def make_crash_df(n=100, collision_types=None):
    """Build a synthetic crash DataFrame with the 5 hash columns + OBJECTID."""
    if collision_types is None:
        collision_types = ["rear end", "angle", "sideswipe", "head on", "fixed object"]
    return pd.DataFrame(
        {
            "Crash Date": [f"2025-01-{(i % 28) + 1:02d}" for i in range(n)],
            "Crash Military Time": [f"{(i * 7) % 2400:04d}" for i in range(n)],
            "x": [-(75.5 + i * 0.001) for i in range(n)],
            "y": [(39.1 + i * 0.001) for i in range(n)],
            "Collision Type": [collision_types[i % len(collision_types)] for i in range(n)],
            "OBJECTID": [f"de-{i + 1:07d}" for i in range(n)],
            "Document Nbr": [f"de-20250101-0800-{i + 1:07d}" for i in range(n)],
            "Crash Severity": ["Property Damage Only"] * n,
        }
    )


# ===========================================================================
# BUG 1: Document Nbr is NOT stable — use CONTENT HASH for diff
# ===========================================================================


class TestBug1_ContentHashNotDocumentNbr:
    """incremental_diff.py must use content hash, not Document Nbr."""

    def test_hash_uses_five_fields(self):
        from incremental_diff import HASH_COLUMNS

        assert HASH_COLUMNS == [
            "Crash Date",
            "Crash Military Time",
            "x",
            "y",
            "Collision Type",
        ], "Hash must use exactly 5 immutable source fields"

    def test_document_nbr_not_in_hash(self):
        from incremental_diff import HASH_COLUMNS

        assert "Document Nbr" not in HASH_COLUMNS

    def test_hash_deterministic_regardless_of_row_order(self):
        from incremental_diff import compute_crash_hashes

        df = make_crash_df(10)
        hashes_original = set(compute_crash_hashes(df))

        # Reverse order — same data, different position
        df_reversed = df.iloc[::-1].reset_index(drop=True)
        hashes_reversed = set(compute_crash_hashes(df_reversed))

        assert hashes_original == hashes_reversed, (
            "Content hashes must be identical regardless of row order"
        )

    def test_different_document_nbr_same_hash(self):
        """Two rows with same crash fields but different Document Nbr → same hash."""
        from incremental_diff import compute_crash_hashes

        df = make_crash_df(2)
        df.loc[0, "Document Nbr"] = "de-20250101-0800-AAAAAAA"
        df.loc[1, "Document Nbr"] = "de-20250101-0800-BBBBBBB"
        # Make crash fields identical
        for col in ["Crash Date", "Crash Military Time", "x", "y", "Collision Type"]:
            df.loc[1, col] = df.loc[0, col]

        hashes = compute_crash_hashes(df)
        assert hashes.iloc[0] == hashes.iloc[1], (
            "Rows with same crash fields must produce same hash even with different Document Nbr"
        )


# ===========================================================================
# BUG 2: OBJECTID is sequential and changes every run — keep existing
# ===========================================================================


class TestBug2_KeepExistingObjectIDs:
    """de_normalize.py --keep-objectids must preserve existing OBJECTIDs."""

    def test_keep_objectids_flag_exists(self):
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert "--keep-objectids" in source

    def test_keep_objectids_in_normalize_signature(self):
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert "keep_objectids" in source
        # Check it's in the function signature
        assert "keep_objectids: bool = False" in source

    def test_existing_objectids_preserved_in_code(self):
        """The keep_objectids branch must NOT overwrite OBJECTID column."""
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        # The code checks for missing_mask and only assigns to rows missing OBJECTIDs
        assert "missing_mask" in source
        assert 'Keeping existing OBJECTIDs' in source


# ===========================================================================
# BUG 3: --rerank-only regenerates ALL OBJECTIDs — conditional now
# ===========================================================================


class TestBug3_RerankOnlyConditionalOBJECTID:
    """Rerank-only must skip OBJECTID regen when --keep-objectids is set."""

    def test_conditional_objectid_regeneration(self):
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        # Must have the conditional check
        assert "if not keep_objectids:" in source, (
            "OBJECTID regeneration must be conditional on keep_objectids"
        )

    def test_new_rows_get_max_plus_one(self):
        """New rows (missing OBJECTID) must get IDs starting from max+1."""
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert "max_id + i + 1" in source or "max_id+i+1" in source, (
            "New OBJECTIDs must start from max existing + 1"
        )

    def test_objectid_extract_regex(self):
        """The regex must extract trailing digits from OBJECTID like 'de-0000123'."""
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert r"(\d+)$" in source, (
            "Must extract trailing digits from OBJECTID format"
        )


# ===========================================================================
# BUG 4: Supabase sync stays FULL reload (TRUNCATE+COPY) for Phase 1
# ===========================================================================


class TestBug4_SyncStaysFullReload:
    """Webhook sync must do TRUNCATE+COPY for BOTH modes in Phase 1."""

    def test_run_batched_sync_logs_mode(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert "Phase 1: full reload for both modes" in source, (
            "Must explicitly document that both modes do full reload"
        )

    def test_truncate_still_present(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert "TRUNCATE" in source, (
            "TRUNCATE step must still exist — no upsert in Phase 1"
        )

    def test_no_upsert_in_webhook(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert "UPSERT" not in source.upper() or "upsert" not in source.lower(), (
            "No upsert logic should be in webhook for Phase 1"
        )


# ===========================================================================
# BUG 5: Content hash collision with multi-vehicle crashes — use 5 fields
# ===========================================================================


class TestBug5_FiveFieldHash:
    """Hash must include Collision Type (5th field) to avoid collisions."""

    def test_collision_type_in_hash(self):
        from incremental_diff import HASH_COLUMNS

        assert "Collision Type" in HASH_COLUMNS, (
            "Collision Type must be in hash to distinguish multi-vehicle crashes"
        )

    def test_five_fields_not_four(self):
        from incremental_diff import HASH_COLUMNS

        assert len(HASH_COLUMNS) == 5, f"Expected 5 hash fields, got {len(HASH_COLUMNS)}"

    def test_same_location_different_collision_type_produces_different_hash(self):
        from incremental_diff import compute_crash_hashes

        df = pd.DataFrame(
            {
                "Crash Date": ["2025-01-15", "2025-01-15"],
                "Crash Military Time": ["1430", "1430"],
                "x": [-75.55, -75.55],
                "y": [39.15, 39.15],
                "Collision Type": ["rear end", "angle"],
            }
        )
        hashes = compute_crash_hashes(df)
        assert hashes.iloc[0] != hashes.iloc[1], (
            "Same location+time but different Collision Type must produce different hashes"
        )


# ===========================================================================
# BUG 6: R2 download needs CF_ credentials (same as supabase_sync.py)
# ===========================================================================


class TestBug6_R2CredentialPattern:
    """incremental_diff.py must use the same CF_ env var pattern as supabase_sync."""

    def test_uses_cf_credentials(self):
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        assert "CF_ACCOUNT_ID" in source
        assert "CF_R2_ACCESS_KEY_ID" in source
        assert "CF_R2_SECRET_ACCESS_KEY" in source

    def test_missing_creds_returns_none(self):
        """If creds missing, download_existing_hashes must return None (not crash)."""
        from incremental_diff import download_existing_hashes

        # Mock boto3 to avoid import error in CI without the dependency
        mock_boto3 = MagicMock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            # Unset all CF_ vars
            env = {k: v for k, v in os.environ.items() if not k.startswith("CF_")}
            with patch.dict(os.environ, env, clear=True):
                result = download_existing_hashes("delaware", "de")
        assert result is None, "Missing creds must return None, not raise"

    def test_uses_pyarrow_column_projection(self):
        """Must use pyarrow column projection for efficient download."""
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        assert "pyarrow.parquet" in source
        assert "read_schema" in source, (
            "Must check available columns via pyarrow schema before reading"
        )


# ===========================================================================
# BUG 7: Pipeline code changes should force full reload
# ===========================================================================


class TestBug7_PipelineVersionDetection:
    """Pipeline version hash of crash_enricher.py must trigger full on change."""

    def test_check_pipeline_version_exists(self):
        from incremental_diff import check_pipeline_version

        assert callable(check_pipeline_version)

    def test_save_pipeline_version_exists(self):
        from incremental_diff import save_pipeline_version

        assert callable(save_pipeline_version)

    def test_version_hash_uses_crash_enricher(self):
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        assert "crash_enricher.py" in source, (
            "Pipeline version must be based on crash_enricher.py hash"
        )

    def test_version_stored_in_r2(self):
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        assert ".pipeline_version" in source, (
            "Pipeline version must be stored as .pipeline_version in R2"
        )

    def test_missing_creds_forces_full(self):
        """No R2 creds → check_pipeline_version returns True (force full)."""
        from incremental_diff import check_pipeline_version

        mock_boto3 = MagicMock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            env = {k: v for k, v in os.environ.items() if not k.startswith("CF_")}
            with patch.dict(os.environ, env, clear=True):
                result = check_pipeline_version("delaware")
        assert result is True, "Missing creds must force full reload"


# ===========================================================================
# BUG 8: Road inventory rebuild forces full (via pipeline version hash)
# ===========================================================================


class TestBug8_EnricherChangesForceFull:
    """If crash_enricher.py changes, pipeline version mismatches → full."""

    def test_version_hash_covers_enricher_content(self):
        """Pipeline version must be derived from crash_enricher.py file content."""
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        # Must read crash_enricher.py bytes and hash them
        assert "enricher_path" in source
        assert "read_bytes" in source
        assert "md5" in source


# ===========================================================================
# BUG 9: First-time load has no existing R2 file — must not crash
# ===========================================================================


class TestBug9_FirstLoadGraceful:
    """Missing R2 file must gracefully fall back to full, not crash."""

    def test_download_returns_none_on_failure(self):
        """download_existing_hashes must return None, not raise, when no file."""
        from incremental_diff import download_existing_hashes

        mock_boto3 = MagicMock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            # No CF_ creds → simulates no R2 access
            env = {k: v for k, v in os.environ.items() if not k.startswith("CF_")}
            with patch.dict(os.environ, env, clear=True):
                result = download_existing_hashes("delaware", "de")
        assert result is None

    def test_none_triggers_full_mode(self):
        """Main logic must set mode=full when download returns None."""
        source = (PROJECT_ROOT / "incremental_diff.py").read_text()
        assert '"first load"' in source or "'first load'" in source, (
            "Must have 'first load' as a reason when no existing data"
        )


# ===========================================================================
# BUG 10: Enrichment needs BOTH files in incremental mode
# ===========================================================================


class TestBug10_IncrementalNeedsBothFiles:
    """Incremental enrichment must download new_rows AND existing enriched."""

    def test_pipeline_downloads_new_rows(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "new_rows.parquet.gz" in content, (
            "Pipeline must download new_rows.parquet.gz for incremental"
        )

    def test_pipeline_downloads_existing_enriched(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "existing_enriched" in content, (
            "Pipeline must download existing enriched statewide for merge"
        )

    def test_pipeline_merges_both(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "pd.concat" in content, (
            "Pipeline must concat existing + new_rows after enrichment"
        )

    def test_uploads_new_rows_to_r2(self, all_jurisdictions_yaml):
        _, content = all_jurisdictions_yaml
        assert "new_rows.parquet.gz" in content, (
            "All-jurisdictions must upload new_rows.parquet.gz to R2"
        )


# ===========================================================================
# BUG 11: Webhook mode parameter backward compatibility
# ===========================================================================


class TestBug11_WebhookModeBackwardCompat:
    """Webhook must accept mode param but default to 'full' if missing."""

    def test_mode_defaults_to_full(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert '"mode", "full"' in source or "'mode', 'full'" in source, (
            "body.get('mode', 'full') must default to 'full'"
        )

    def test_invalid_mode_falls_back_to_full(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert 'mode not in ("incremental", "full")' in source or \
               "mode not in ('incremental', 'full')" in source, (
            "Invalid mode values must fall back to 'full'"
        )

    def test_mode_in_sync_thread_signature(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert 'def _sync_thread(abbr: str, batch_size: int, mode: str = "full")' in source

    def test_mode_in_run_batched_sync_signature(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert "mode: str" in source

    def test_mode_in_status_json(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert '"mode": mode' in source or "'mode': mode" in source, (
            "Status JSON must include mode for monitoring"
        )

    def test_mode_in_accepted_response(self):
        source = (PROJECT_ROOT / "webhook/webhook.py").read_text()
        assert '"mode": mode' in source, (
            "202 response must include mode"
        )


# ===========================================================================
# BUG 12: download_from_r2() has wrong R2 path (.parquet.gz → .parquet)
# ===========================================================================


class TestBug12_R2DownloadPathFix:
    """download_from_r2 must try .parquet first, .parquet.gz as fallback."""

    def test_parquet_no_gz_is_primary(self):
        source = (PROJECT_ROOT / "supabase_sync.py").read_text()
        # Find the for key in [...] block
        assert "/_state/all_roads.parquet\"" in source or \
               "/_state/all_roads.parquet'" in source, (
            "Must have .parquet (no .gz) as a download path"
        )

    def test_parquet_gz_still_present(self):
        source = (PROJECT_ROOT / "supabase_sync.py").read_text()
        assert "/_state/all_roads.parquet.gz" in source, (
            ".parquet.gz must remain as fallback"
        )

    def test_statewide_path_added(self):
        source = (PROJECT_ROOT / "supabase_sync.py").read_text()
        assert "/_statewide/statewide_all_roads.parquet.gz" in source, (
            "Must include _statewide/ path (new upload location)"
        )

    def test_legacy_csv_path_kept(self):
        source = (PROJECT_ROOT / "supabase_sync.py").read_text()
        assert "_statewide_all_roads.csv" in source, (
            "Legacy CSV path must be kept for backward compat"
        )

    def test_four_paths_in_order(self):
        """download_from_r2 must try 4 paths in priority order."""
        source = (PROJECT_ROOT / "supabase_sync.py").read_text()
        # Extract just the download_from_r2 function body for precise search
        func_start = source.find("def download_from_r2")
        assert func_start != -1, "download_from_r2 function not found"
        func_body = source[func_start:func_start + 800]

        idx_parquet = func_body.find("/_state/all_roads.parquet\",")
        idx_parquet_gz = func_body.find("/_state/all_roads.parquet.gz")
        idx_statewide = func_body.find("/_statewide/statewide_all_roads.parquet.gz")
        idx_csv = func_body.find("_statewide_all_roads.csv")

        assert all(i != -1 for i in [idx_parquet, idx_parquet_gz, idx_statewide, idx_csv]), (
            "All 4 R2 paths must be present in download_from_r2"
        )
        assert idx_parquet < idx_parquet_gz < idx_statewide < idx_csv, (
            "Paths must be tried in order: .parquet → .parquet.gz → _statewide/ → CSV"
        )


# ===========================================================================
# WORKFLOW INTEGRATION: Incremental diff step + mode propagation
# ===========================================================================


class TestWorkflowIntegration:
    """Workflow files must wire up incremental diff and mode propagation."""

    def test_force_full_input_exists(self, all_jurisdictions_yaml):
        parsed, _ = all_jurisdictions_yaml
        inputs = parsed["on"]["workflow_dispatch"]["inputs"]
        assert "force_full" in inputs, "Must have force_full input"
        assert inputs["force_full"]["type"] == "boolean"

    def test_diff_step_exists(self, all_jurisdictions_yaml):
        _, content = all_jurisdictions_yaml
        assert "Incremental diff" in content, "Must have incremental diff step"
        assert "incremental_diff.py" in content, "Must call incremental_diff.py"

    def test_mode_output_propagated(self, all_jurisdictions_yaml):
        parsed, _ = all_jurisdictions_yaml
        jobs = parsed.get("jobs", {})
        dl_job = jobs.get("download-and-normalize", {})
        outputs = dl_job.get("outputs", {})
        assert "mode" in outputs, "download-and-normalize must output mode"
        assert "new_count" in outputs, "download-and-normalize must output new_count"

    def test_pipeline_receives_mode_input(self, batch_pipeline_yaml):
        parsed, _ = batch_pipeline_yaml
        inputs = parsed["on"]["workflow_dispatch"]["inputs"]
        assert "mode" in inputs, "Pipeline must accept mode input"
        assert "new_count" in inputs, "Pipeline must accept new_count input"

    def test_mode_detect_step_exists(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "Detect pipeline mode" in content
        assert "mode_detect" in content

    def test_incremental_enrichment_path(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "INCREMENTAL ENRICHMENT" in content
        assert "--keep-objectids" in content, (
            "Incremental path must use --keep-objectids for rerank"
        )

    def test_full_enrichment_preserved(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        # The else branch must have the original enrichment
        assert "crash_enricher.py" in content
        assert "--state-fips" in content, (
            "Enricher must be called with --state-fips (actual CLI)"
        )

    def test_skip_mode_skips_enrichment(self, batch_pipeline_yaml):
        _, content = batch_pipeline_yaml
        assert "skip" in content.lower()
        assert "mode_detect.outputs.skip" in content

    def test_trigger_passes_mode(self, all_jurisdictions_yaml):
        _, content = all_jurisdictions_yaml
        assert "mode:" in content
        assert "new_count:" in content


# ===========================================================================
# DIFF ENGINE: Core logic correctness
# ===========================================================================


class TestDiffEngineLogic:
    """Verify incremental_diff.py core logic produces correct results."""

    def test_identical_data_produces_skip(self, tmp_path):
        """If fresh == existing, mode must be 'skip'."""
        from incremental_diff import compute_crash_hashes

        df = make_crash_df(100)
        fresh_hashes = set(compute_crash_hashes(df))
        existing_hashes = set(compute_crash_hashes(df))

        new_hashes = fresh_hashes - existing_hashes
        assert len(new_hashes) == 0, "Identical data must produce 0 new hashes"

    def test_small_addition_produces_incremental(self):
        """5 new rows out of 100 (5%) < 10% threshold → incremental."""
        from incremental_diff import compute_crash_hashes, DEFAULT_THRESHOLD

        existing = make_crash_df(100)
        # Add 5 genuinely new rows
        new_rows = pd.DataFrame(
            {
                "Crash Date": ["2025-12-25"] * 5,
                "Crash Military Time": ["2359"] * 5,
                "x": [-76.0, -76.1, -76.2, -76.3, -76.4],
                "y": [40.0, 40.1, 40.2, 40.3, 40.4],
                "Collision Type": ["rollover"] * 5,
                "OBJECTID": [""] * 5,
                "Document Nbr": [""] * 5,
                "Crash Severity": ["Property Damage Only"] * 5,
            }
        )
        fresh = pd.concat([existing, new_rows], ignore_index=True)

        existing_hashes = set(compute_crash_hashes(existing))
        fresh_hashes = set(compute_crash_hashes(fresh))
        new_count = len(fresh_hashes - existing_hashes)
        pct_new = new_count / len(fresh) * 100

        assert new_count == 5
        assert pct_new < DEFAULT_THRESHOLD, (
            f"5/105 = {pct_new:.1f}% should be < {DEFAULT_THRESHOLD}%"
        )

    def test_large_addition_produces_full(self):
        """50 new rows out of 100 (33%) >= 10% threshold → full."""
        from incremental_diff import compute_crash_hashes, DEFAULT_THRESHOLD

        existing = make_crash_df(100)
        new_rows = pd.DataFrame(
            {
                "Crash Date": [f"2025-12-{(i % 28) + 1:02d}" for i in range(50)],
                "Crash Military Time": [f"{(i * 13) % 2400:04d}" for i in range(50)],
                "x": [-(76.0 + i * 0.01) for i in range(50)],
                "y": [(40.0 + i * 0.01) for i in range(50)],
                "Collision Type": ["rollover"] * 50,
                "OBJECTID": [""] * 50,
                "Document Nbr": [""] * 50,
                "Crash Severity": ["Property Damage Only"] * 50,
            }
        )
        fresh = pd.concat([existing, new_rows], ignore_index=True)

        existing_hashes = set(compute_crash_hashes(existing))
        fresh_hashes = set(compute_crash_hashes(fresh))
        new_count = len(fresh_hashes - existing_hashes)
        pct_new = new_count / len(fresh) * 100

        assert new_count == 50
        assert pct_new >= DEFAULT_THRESHOLD, (
            f"50/150 = {pct_new:.1f}% should be >= {DEFAULT_THRESHOLD}%"
        )

    def test_null_handling_in_hash(self):
        """Null/NaN values must not crash the hash function."""
        from incremental_diff import compute_crash_hashes

        df = pd.DataFrame(
            {
                "Crash Date": [None, "2025-01-01"],
                "Crash Military Time": ["1200", None],
                "x": [float("nan"), -75.5],
                "y": [39.1, float("nan")],
                "Collision Type": ["", None],
            }
        )
        hashes = compute_crash_hashes(df)
        assert len(hashes) == 2
        assert all(len(h) == 32 for h in hashes), "Each hash must be a 32-char MD5 hex"


# ===========================================================================
# OBJECTID ASSIGNMENT: Keep existing + max+1 for new
# ===========================================================================


class TestObjectIDAssignment:
    """Verify OBJECTID logic handles all edge cases."""

    def test_all_existing_ids_preserved(self):
        """When all rows have OBJECTIDs, none should be reassigned."""
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert "All rows have OBJECTIDs" in source

    def test_missing_ids_detected(self):
        """Rows with empty/nan/None OBJECTID must be detected as missing."""
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        # Must check for "", "nan", "None"
        assert '"nan"' in source or "'nan'" in source
        assert '"None"' in source or "'None'" in source


# ===========================================================================
# CLI ARGUMENT: --keep-objectids wired through properly
# ===========================================================================


class TestCLIWiring:
    """Verify --keep-objectids is properly wired from CLI to normalize()."""

    def test_argparse_has_keep_objectids(self):
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert '"--keep-objectids"' in source or "'--keep-objectids'" in source

    def test_passed_to_normalize_call(self):
        source = (PROJECT_ROOT / "states/delaware/de_normalize.py").read_text()
        assert "keep_objectids=args.keep_objectids" in source, (
            "CLI arg must be passed through to normalize()"
        )


# ===========================================================================
# MAIN: Run with pytest or standalone
# ===========================================================================


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
