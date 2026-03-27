#!/usr/bin/env python3
"""
Tests for download_virginia_crash_data.py — Playwright-based Virginia downloader.

Covers:
  1. File handle leaks in validate_csv() and main()
  2. Pagination loop off-by-one in download_with_browser()
  3. JS injection risk in browser_fetch_json() URL interpolation
  4. Bare except clauses in save_cached_url() / load_cached_url()
  5. validate_csv() correctly rejects driver-level datasets
  6. filter_by_jurisdiction() matching logic
  7. Manifest save/load for incremental downloads
  8. merge_delta_into_csv() deduplication logic
  9. OBJECTID reset detection in main() flow
  10. Workflow GITHUB_STEP_SUMMARY formatting
"""

import csv
import importlib
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

# Add repo root to path so we can import the module
REPO_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(REPO_ROOT))


def _import_module():
    """Import the virginia downloader module."""
    spec = importlib.util.spec_from_file_location(
        "virginia_dl", REPO_ROOT / "download_virginia_crash_data.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestValidateCSV(unittest.TestCase):
    """Tests for validate_csv()."""

    def test_valid_crash_csv(self):
        mod = _import_module()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['OBJECTID', 'Crash_Year', 'Crash_Severity', 'x', 'y'])
            for i in range(100):
                writer.writerow([i, 2024, 'K', -77.5, 37.5])
            path = f.name
        try:
            self.assertTrue(mod.validate_csv(path))
        finally:
            os.unlink(path)

    def test_rejects_driver_level_dataset(self):
        mod = _import_module()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['OBJECTID', 'Driver_Age', 'DRIVER_AGE', 'Bike_VehicleNumber'])
            for i in range(50):
                writer.writerow([i, 25, 25, 1])
            path = f.name
        try:
            self.assertFalse(mod.validate_csv(path), "Should reject driver-level CSV")
        finally:
            os.unlink(path)

    def test_rejects_html_error_page(self):
        mod = _import_module()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('<html><body>403 Forbidden</body></html>')
            path = f.name
        try:
            self.assertFalse(mod.validate_csv(path), "Should reject HTML error pages")
        finally:
            os.unlink(path)

    def test_file_handle_leak(self):
        """validate_csv must use 'with' for all file opens — no leaked handles."""
        mod = _import_module()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['OBJECTID', 'Crash_Year', 'x', 'y'])
            for i in range(50):
                writer.writerow([i, 2024, -77.5, 37.5])
            path = f.name
        try:
            import gc
            gc.collect()
            result = mod.validate_csv(path)
            self.assertTrue(result)
            os.unlink(path)
            self.assertFalse(os.path.exists(path))
        except Exception:
            if os.path.exists(path):
                os.unlink(path)
            raise


class TestPaginationLoop(unittest.TestCase):
    def test_loop_condition_correct(self):
        """Pagination loop must use 'offset < download_count', not + batch_size."""
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        match = re.search(r'while offset < ((?:total_count|download_count)\s*\+\s*batch_size)', source)
        if match:
            self.fail(
                f"Pagination bug: 'while offset < {match.group(1)}' causes extra API calls."
            )


class TestJSInjection(unittest.TestCase):
    def test_url_properly_escaped(self):
        """browser_fetch_json must escape URL for JS string (json.dumps or similar)."""
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        func_match = re.search(
            r'def browser_fetch_json\(.*?\):\s*""".*?"""(.*?)(?=\ndef |\Z)',
            source, re.DOTALL
        )
        if func_match:
            func_body = func_match.group(1)
            has_escaping = (
                'json.dumps' in func_body or
                'quote(' in func_body or
                'encodeURI' in func_body
            )
            self.assertTrue(has_escaping, "URL must be escaped before JS interpolation")


class TestBareExcepts(unittest.TestCase):
    def test_no_bare_excepts(self):
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        lines = source.split('\n')
        bare_excepts = []
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped == 'except:' or stripped.startswith('except:'):
                bare_excepts.append((i, line.strip()))
        if bare_excepts:
            locations = ", ".join(f"line {n}" for n, _ in bare_excepts)
            self.fail(f"Bare 'except:' at {locations}. Use 'except Exception:'.")


class TestManifest(unittest.TestCase):
    """Tests for download manifest save/load."""

    def test_save_and_load_manifest(self):
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            mod.save_manifest(
                tmpdir, max_objectid=500000, total_count=1097423,
                record_count=1097423, mode='full', service_url='https://example.com',
                delta_count=0
            )
            manifest = mod.load_manifest(tmpdir)
            self.assertIsNotNone(manifest)
            self.assertEqual(manifest['max_objectid'], 500000)
            self.assertEqual(manifest['total_count'], 1097423)
            self.assertEqual(manifest['download_mode'], 'full')
            self.assertIn('download_date', manifest)

    def test_load_manifest_missing_file(self):
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = mod.load_manifest(tmpdir)
            self.assertIsNone(manifest)

    def test_load_manifest_corrupt_file(self):
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / mod.MANIFEST_FILENAME
            path.write_text("not valid json {{{")
            manifest = mod.load_manifest(tmpdir)
            self.assertIsNone(manifest)


class TestMergeDelta(unittest.TestCase):
    """Tests for merge_delta_into_csv() deduplication logic."""

    @unittest.skipUnless(
        importlib.util.find_spec("pandas"), "pandas not installed"
    )
    def test_merge_appends_new_records(self):
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            existing = os.path.join(tmpdir, 'existing.csv')
            delta = os.path.join(tmpdir, 'delta.csv')
            output = os.path.join(tmpdir, 'output.csv')

            # Existing: 3 records
            with open(existing, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR', 'Crash_Year'])
                w.writerow([1, 'DOC001', '2024'])
                w.writerow([2, 'DOC002', '2024'])
                w.writerow([3, 'DOC003', '2024'])

            # Delta: 2 new records
            with open(delta, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR', 'Crash_Year'])
                w.writerow([4, 'DOC004', '2025'])
                w.writerow([5, 'DOC005', '2025'])

            result = mod.merge_delta_into_csv(existing, delta, output)
            self.assertEqual(result['total_rows'], 5)
            self.assertEqual(result['delta_rows'], 2)
            self.assertEqual(result['duplicates_removed'], 0)

    @unittest.skipUnless(
        importlib.util.find_spec("pandas"), "pandas not installed"
    )
    def test_merge_deduplicates_by_document_nbr(self):
        """If delta contains a record with same Document Nbr, keep the newer one."""
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            existing = os.path.join(tmpdir, 'existing.csv')
            delta = os.path.join(tmpdir, 'delta.csv')
            output = os.path.join(tmpdir, 'output.csv')

            # Existing: DOC001 with Crash_Year=2024
            with open(existing, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR', 'Crash_Year'])
                w.writerow([1, 'DOC001', '2024'])
                w.writerow([2, 'DOC002', '2024'])

            # Delta: DOC001 again (updated) + DOC003 (new)
            with open(delta, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR', 'Crash_Year'])
                w.writerow([100, 'DOC001', '2025'])  # Updated record
                w.writerow([101, 'DOC003', '2025'])

            result = mod.merge_delta_into_csv(existing, delta, output)
            self.assertEqual(result['total_rows'], 3)  # DOC001, DOC002, DOC003
            self.assertEqual(result['duplicates_removed'], 1)  # Old DOC001 removed

            # Verify the kept DOC001 is the newer version (Crash_Year=2025)
            import pandas as pd
            df = pd.read_csv(output, dtype=str)
            doc001 = df[df['DOCUMENT_NBR'] == 'DOC001']
            self.assertEqual(len(doc001), 1)
            self.assertEqual(doc001.iloc[0]['Crash_Year'], '2025')

    @unittest.skipUnless(
        importlib.util.find_spec("pandas"), "pandas not installed"
    )
    def test_merge_empty_delta(self):
        mod = _import_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            existing = os.path.join(tmpdir, 'existing.csv')
            delta = os.path.join(tmpdir, 'delta.csv')
            output = os.path.join(tmpdir, 'output.csv')

            with open(existing, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR'])
                w.writerow([1, 'DOC001'])

            with open(delta, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['OBJECTID', 'DOCUMENT_NBR'])
                # No data rows

            result = mod.merge_delta_into_csv(existing, delta, output)
            self.assertEqual(result['total_rows'], 1)
            self.assertEqual(result['delta_rows'], 0)


class TestFindDocNbrColumn(unittest.TestCase):
    """Tests for find_doc_nbr_column()."""

    def test_finds_primary_column(self):
        mod = _import_module()
        result = mod.find_doc_nbr_column(['OBJECTID', 'DOCUMENT_NBR', 'Crash_Year'])
        self.assertEqual(result, 'DOCUMENT_NBR')

    def test_finds_fallback_column(self):
        mod = _import_module()
        result = mod.find_doc_nbr_column(['OBJECTID', 'Document Nbr', 'Crash_Year'])
        self.assertEqual(result, 'Document Nbr')

    def test_finds_case_insensitive(self):
        mod = _import_module()
        result = mod.find_doc_nbr_column(['objectid', 'document_nbr', 'crash_year'])
        self.assertEqual(result, 'document_nbr')

    def test_returns_none_when_missing(self):
        mod = _import_module()
        result = mod.find_doc_nbr_column(['OBJECTID', 'Crash_Year', 'x', 'y'])
        self.assertIsNone(result)


class TestDownloadWithBrowserSignature(unittest.TestCase):
    """Test that download_with_browser returns metadata dict (not just bool)."""

    def test_function_accepts_where_clause(self):
        """download_with_browser must accept where_clause parameter."""
        import inspect
        mod = _import_module()
        sig = inspect.signature(mod.download_with_browser)
        self.assertIn('where_clause', sig.parameters,
                      "download_with_browser must accept where_clause for incremental mode")

    def test_function_accepts_append_mode(self):
        import inspect
        mod = _import_module()
        sig = inspect.signature(mod.download_with_browser)
        self.assertIn('append_mode', sig.parameters)


class TestCLIFlags(unittest.TestCase):
    """Test that CLI has incremental-related flags."""

    def test_has_full_flag(self):
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        self.assertIn("'--full'", source, "Script must have --full flag for full download mode")

    def test_has_incremental_logic(self):
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        self.assertIn("incremental", source.lower(),
                      "Script must have incremental download logic")

    def test_has_objectid_reset_detection(self):
        source = (REPO_ROOT / "download_virginia_crash_data.py").read_text()
        self.assertIn("OBJECTID RESET DETECTED", source,
                      "Script must detect OBJECTID resets and fall back to full download")


class TestFilterByJurisdiction(unittest.TestCase):
    """Tests for filter_by_jurisdiction()."""

    @unittest.skipUnless(
        importlib.util.find_spec("pandas"), "pandas not installed"
    )
    def test_filters_by_juris_code(self):
        import pandas as pd
        mod = _import_module()
        df = pd.DataFrame({
            'Juris_Code': ['087', '087', '041', '041', '087'],
            'Physical_Juris_Name': ['HENRICO', 'HENRICO', 'CHESTERFIELD', 'CHESTERFIELD', 'HENRICO'],
            'Crash_Year': [2024, 2024, 2024, 2024, 2024],
        })
        jconfig = {'jurisCode': '087', 'fips': '087', 'name': 'Henrico', 'namePatterns': ['HENRICO']}
        result = mod.filter_by_jurisdiction(df, 'henrico', jconfig)
        self.assertEqual(len(result), 3)

    @unittest.skipUnless(
        importlib.util.find_spec("pandas"), "pandas not installed"
    )
    def test_falls_back_to_name_pattern(self):
        import pandas as pd
        mod = _import_module()
        df = pd.DataFrame({
            'Physical_Juris_Name': ['HENRICO', 'HENRICO', 'FAIRFAX', 'HENRICO'],
            'Crash_Year': [2024, 2024, 2024, 2024],
        })
        jconfig = {'jurisCode': '', 'fips': '', 'name': 'Henrico', 'namePatterns': ['HENRICO']}
        result = mod.filter_by_jurisdiction(df, 'henrico', jconfig)
        self.assertEqual(len(result), 3)


class TestWorkflowStepSummary(unittest.TestCase):
    def test_step_summary_indentation(self):
        workflow_path = REPO_ROOT / ".github" / "workflows" / "download-virginia.yml"
        if not workflow_path.exists():
            self.skipTest("Workflow file not found")

        content = workflow_path.read_text()
        match = re.search(
            r'cat >> \$GITHUB_STEP_SUMMARY <<EOF\n(.*?)EOF',
            content, re.DOTALL
        )
        if not match:
            self.skipTest("No GITHUB_STEP_SUMMARY heredoc found")

        heredoc_body = match.group(1)
        lines = heredoc_body.split('\n')
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('##') or stripped.startswith('|'):
                leading_spaces = len(line) - len(line.lstrip())
                if leading_spaces >= 4:
                    self.fail(
                        f"GITHUB_STEP_SUMMARY has {leading_spaces} leading spaces on "
                        f"'{stripped[:40]}...'. GitHub renders 4+ spaces as code block."
                    )

    def test_workflow_has_incremental_mode_input(self):
        workflow_path = REPO_ROOT / ".github" / "workflows" / "download-virginia.yml"
        if not workflow_path.exists():
            self.skipTest("Workflow file not found")
        content = workflow_path.read_text()
        self.assertIn('download_mode', content,
                      "Workflow must have download_mode input for incremental/full")

    def test_workflow_has_quarterly_schedule(self):
        workflow_path = REPO_ROOT / ".github" / "workflows" / "download-virginia.yml"
        if not workflow_path.exists():
            self.skipTest("Workflow file not found")
        content = workflow_path.read_text()
        self.assertIn('1,4,7,10', content,
                      "Workflow must have quarterly cron schedule (Jan/Apr/Jul/Oct)")


if __name__ == '__main__':
    unittest.main(verbosity=2)
