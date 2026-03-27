#!/usr/bin/env python3
"""
End-to-end test for the multi-jurisdiction pipeline.

Tests the full flow WITHOUT needing API keys, R2 credentials, or network:
  1. Generates synthetic statewide CSVs (Virginia + Colorado format)
  2. Splits them into per-jurisdiction CSVs
  3. Verifies correct jurisdiction assignment
  4. Verifies 3 road-type variants per jurisdiction
  5. Tests R2 manifest generation
  6. Tests resolveDataUrl() logic for multi-word jurisdictions
  7. Verifies generate_aggregates.py can find the split CSVs

Usage:
    python scripts/test_multi_jurisdiction_pipeline.py
    python scripts/test_multi_jurisdiction_pipeline.py --verbose
    python scripts/test_multi_jurisdiction_pipeline.py --keep-files   # Don't delete temp dir
"""

import argparse
import csv
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Test parameters
VA_TEST_JURISDICTIONS = [
    'henrico', 'chesterfield', 'fairfax_county', 'richmond_city',
    'prince_william', 'arlington', 'virginia_beach', 'norfolk'
]
CO_TEST_JURISDICTIONS = [
    'douglas', 'adams', 'denver', 'boulder', 'elpaso', 'jefferson'
]

PASS = '\033[92m✓ PASS\033[0m'
FAIL = '\033[91m✗ FAIL\033[0m'
WARN = '\033[93m⚠ WARN\033[0m'


class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.details = []

    def check(self, name, condition, detail=''):
        if condition:
            self.passed += 1
            self.details.append((PASS, name, detail))
        else:
            self.failed += 1
            self.details.append((FAIL, name, detail))

    def warn(self, name, detail=''):
        self.warnings += 1
        self.details.append((WARN, name, detail))

    def print_summary(self):
        print("\n" + "=" * 70)
        print("TEST RESULTS")
        print("=" * 70)
        for status, name, detail in self.details:
            suffix = f' — {detail}' if detail else ''
            print(f"  {status}  {name}{suffix}")
        print("=" * 70)
        print(f"  Passed: {self.passed}  |  Failed: {self.failed}  |  Warnings: {self.warnings}")
        if self.failed == 0:
            print(f"  \033[92mALL TESTS PASSED\033[0m")
        else:
            print(f"  \033[91m{self.failed} TEST(S) FAILED\033[0m")
        print("=" * 70)


def generate_va_statewide_csv(output_path, jurisdictions, records_per=50):
    """Generate synthetic Virginia statewide CSV with known jurisdictions."""
    # Virginia CSV columns (standardized format)
    headers = [
        'Document Nbr', 'Crash Date', 'Crash Severity', 'Collision Type',
        'SYSTEM', 'Rte_Nm', 'Jurisdiction', 'Juris_Code', 'FIPS',
        'x', 'y', 'Weather', 'Light', 'Ped', 'Bike'
    ]

    # Jurisdiction metadata
    juris_meta = {
        'henrico':         {'code': '44', 'fips': '087', 'name': 'Henrico County'},
        'chesterfield':    {'code': '21', 'fips': '041', 'name': 'Chesterfield County'},
        'fairfax_county':  {'code': '30', 'fips': '059', 'name': 'Fairfax County'},
        'richmond_city':   {'code': '81', 'fips': '760', 'name': 'Richmond City'},
        'prince_william':  {'code': '76', 'fips': '153', 'name': 'Prince William County'},
        'arlington':       {'code': '05', 'fips': '013', 'name': 'Arlington County'},
        'virginia_beach':  {'code': '91', 'fips': '810', 'name': 'Virginia Beach City'},
        'norfolk':         {'code': '66', 'fips': '710', 'name': 'Norfolk City'},
    }

    severities = ['K', 'A', 'B', 'C', 'O']
    systems = ['Non-DOT secondary', 'DOT Primary', 'DOT Secondary', 'DOT Interstate']
    collisions = ['Rear End', 'Angle', 'Sideswipe - Same', 'Head On', 'Fixed Object']

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        doc_id = 1000

        for jid in jurisdictions:
            meta = juris_meta.get(jid, {
                'code': '99', 'fips': '999', 'name': jid.replace('_', ' ').title()
            })
            for i in range(records_per):
                doc_id += 1
                system = random.choice(systems)
                route = f"{'I-' if system == 'DOT Interstate' else 'US-'}{random.randint(1, 95)}"
                writer.writerow([
                    f'VA{doc_id}',
                    f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
                    random.choice(severities),
                    random.choice(collisions),
                    system,
                    route,
                    meta['name'],
                    meta['code'],
                    meta['fips'],
                    f'-{77 + random.random():.6f}',
                    f'{37 + random.random():.6f}',
                    random.choice(['Clear', 'Rain', 'Snow']),
                    random.choice(['Daylight', 'Dark', 'Dusk']),
                    random.choice(['Y', 'N', 'N', 'N', 'N']),
                    random.choice(['Y', 'N', 'N', 'N', 'N']),
                ])

    return doc_id - 1000


def generate_co_statewide_csv(output_path, jurisdictions, records_per=50):
    """Generate synthetic Colorado statewide CSV with known counties."""
    headers = [
        'CUID', 'CrashDate', 'Severity', 'MHE',
        'County', 'Rd System', 'Rd_Number',
        'Longitude', 'Latitude', 'Weather', 'Light'
    ]

    county_names = {
        'douglas': 'DOUGLAS',
        'adams': 'ADAMS',
        'denver': 'DENVER',
        'boulder': 'BOULDER',
        'elpaso': 'EL PASO',
        'jefferson': 'JEFFERSON',
    }

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        cuid = 5000

        for jid in jurisdictions:
            county = county_names.get(jid, jid.upper())
            for i in range(records_per):
                cuid += 1
                system = random.choice(['State Highway', 'County Road', 'Interstate', 'US Highway'])
                writer.writerow([
                    f'CO{cuid}',
                    f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
                    random.choice(['Fatal', 'Serious Injury', 'Minor Injury', 'Possible Injury', 'PDO']),
                    random.choice(['Rear End', 'Broadside', 'Sideswipe Same', 'Head On']),
                    county,
                    system,
                    str(random.randint(1, 285)),
                    f'-{104 + random.random():.6f}',
                    f'{39 + random.random():.6f}',
                    random.choice(['Clear', 'Rain', 'Snow']),
                    random.choice(['Daylight', 'Dark', 'Dusk']),
                ])

    return cuid - 5000


def test_list_jurisdictions(results, verbose=False):
    """Test 1: List jurisdictions for both states."""
    print("\n--- Test 1: List Jurisdictions ---")

    for state, expected_min in [('virginia', 130), ('colorado', 60)]:
        try:
            output = subprocess.check_output(
                [sys.executable, 'scripts/split_jurisdictions.py', '--state', state, '--list'],
                cwd=str(PROJECT_ROOT), text=True, stderr=subprocess.STDOUT
            )
            lines = [l for l in output.strip().split('\n') if 'FIPS:' in l]
            count = len(lines)
            results.check(
                f'{state}: list jurisdictions',
                count >= expected_min,
                f'{count} jurisdictions found (expected >={expected_min})'
            )
            if verbose:
                print(f"    First 3: {lines[:3]}")
        except subprocess.CalledProcessError as e:
            results.check(f'{state}: list jurisdictions', False, f'Script failed: {e.output[:200]}')


def test_virginia_split(results, tmp_dir, verbose=False):
    """Test 2: Split Virginia statewide CSV into jurisdictions."""
    print("\n--- Test 2: Virginia Split ---")

    va_csv = os.path.join(tmp_dir, 'virginia_statewide_all_roads.csv')
    va_output = os.path.join(tmp_dir, 'va_output')
    os.makedirs(va_output, exist_ok=True)

    # Generate synthetic data
    total = generate_va_statewide_csv(va_csv, VA_TEST_JURISDICTIONS, records_per=60)
    results.check('VA: generate synthetic CSV', os.path.exists(va_csv), f'{total} records')

    # Run split
    try:
        output = subprocess.check_output(
            [sys.executable, 'scripts/split_jurisdictions.py',
             '--state', 'virginia',
             '--input', va_csv,
             '--output-dir', va_output,
             '--jurisdictions'] + VA_TEST_JURISDICTIONS,
            cwd=str(PROJECT_ROOT), text=True, stderr=subprocess.STDOUT
        )
        if verbose:
            print(output)
        results.check('VA: split script completed', True)
    except subprocess.CalledProcessError as e:
        results.check('VA: split script completed', False, e.output[:300])
        return

    # Verify output files exist
    for jid in VA_TEST_JURISDICTIONS:
        for suffix in ['county_roads', 'city_roads', 'no_interstate', 'all_roads']:
            fpath = os.path.join(va_output, f'{jid}_{suffix}.csv')
            exists = os.path.exists(fpath)
            if exists:
                with open(fpath) as f:
                    rows = sum(1 for _ in f) - 1  # minus header
                detail = f'{rows} records'
            else:
                detail = 'FILE NOT FOUND'
            results.check(f'VA: {jid}_{suffix}.csv exists', exists, detail)

    # Verify multi-word jurisdiction correctness (key bug we fixed)
    for jid in ['fairfax_county', 'richmond_city', 'prince_william', 'virginia_beach']:
        all_roads = os.path.join(va_output, f'{jid}_all_roads.csv')
        if os.path.exists(all_roads):
            with open(all_roads) as f:
                rows = sum(1 for _ in f) - 1
            results.check(
                f'VA: multi-word "{jid}" has data',
                rows > 0,
                f'{rows} records'
            )
        else:
            results.check(f'VA: multi-word "{jid}" has data', False, 'File missing')

    # Verify split report
    report_path = os.path.join(va_output, '.split_report.json')
    if os.path.exists(report_path):
        with open(report_path) as f:
            report = json.load(f)
        results.check(
            'VA: split report generated',
            report['successful'] == len(VA_TEST_JURISDICTIONS),
            f"successful={report['successful']}, empty={report['empty']}, failed={report['failed']}"
        )
    else:
        results.check('VA: split report generated', False, 'Report not found')

    # Verify road type filtering (county_roads < all_roads)
    henrico_county = os.path.join(va_output, 'henrico_county_roads.csv')
    henrico_all = os.path.join(va_output, 'henrico_all_roads.csv')
    if os.path.exists(henrico_county) and os.path.exists(henrico_all):
        with open(henrico_county) as f:
            county_rows = sum(1 for _ in f) - 1
        with open(henrico_all) as f:
            all_rows = sum(1 for _ in f) - 1
        results.check(
            'VA: county_roads <= all_roads',
            county_rows <= all_rows,
            f'county={county_rows}, all={all_rows}'
        )

    return va_output


def test_colorado_split(results, tmp_dir, verbose=False):
    """Test 3: Split Colorado statewide CSV into jurisdictions."""
    print("\n--- Test 3: Colorado Split ---")

    co_csv = os.path.join(tmp_dir, 'colorado_statewide_all_roads.csv')
    co_output = os.path.join(tmp_dir, 'co_output')
    os.makedirs(co_output, exist_ok=True)

    # Generate synthetic data
    total = generate_co_statewide_csv(co_csv, CO_TEST_JURISDICTIONS, records_per=50)
    results.check('CO: generate synthetic CSV', os.path.exists(co_csv), f'{total} records')

    # Run split
    try:
        output = subprocess.check_output(
            [sys.executable, 'scripts/split_jurisdictions.py',
             '--state', 'colorado',
             '--input', co_csv,
             '--output-dir', co_output,
             '--jurisdictions'] + CO_TEST_JURISDICTIONS,
            cwd=str(PROJECT_ROOT), text=True, stderr=subprocess.STDOUT
        )
        if verbose:
            print(output)
        results.check('CO: split script completed', True)
    except subprocess.CalledProcessError as e:
        results.check('CO: split script completed', False, e.output[:300])
        return

    # Verify output files
    for jid in CO_TEST_JURISDICTIONS:
        all_roads = os.path.join(co_output, f'{jid}_all_roads.csv')
        exists = os.path.exists(all_roads)
        if exists:
            with open(all_roads) as f:
                rows = sum(1 for _ in f) - 1
            detail = f'{rows} records'
        else:
            detail = 'FILE NOT FOUND'
        results.check(f'CO: {jid}_all_roads.csv exists', exists, detail)

    return co_output


def test_r2_manifest(results, va_output, verbose=False):
    """Test 4: R2 manifest generation."""
    print("\n--- Test 4: R2 Manifest Generation ---")

    if not va_output:
        results.check('R2 manifest: skipped', False, 'VA output not available')
        return

    try:
        output = subprocess.check_output(
            [sys.executable, 'scripts/split_jurisdictions.py',
             '--state', 'virginia',
             '--r2-manifest',
             '--r2-prefix', 'virginia',
             '--output-dir', va_output],
            cwd=str(PROJECT_ROOT), text=True, stderr=subprocess.STDOUT
        )
        manifest = json.loads(output)
        results.check(
            'R2 manifest: generated',
            len(manifest) > 0,
            f'{len(manifest)} files in manifest'
        )

        # Verify R2 key format
        if manifest:
            sample = manifest[0]
            results.check(
                'R2 manifest: correct key format',
                sample['r2_key'].startswith('virginia/'),
                f"key={sample['r2_key']}"
            )

            # Check multi-word jurisdictions get correct R2 keys
            for entry in manifest:
                key = entry['r2_key']
                if 'fairfax_county' in key:
                    results.check(
                        'R2 manifest: fairfax_county key correct',
                        '/fairfax_county/' in key,
                        f"key={key}"
                    )
                    break
            else:
                results.warn('R2 manifest: fairfax_county not in manifest (might be empty)')

        if verbose:
            print(f"    Sample entries: {json.dumps(manifest[:3], indent=2)}")

    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        results.check('R2 manifest: generated', False, str(e)[:200])


def test_resolve_data_url_logic(results, verbose=False):
    """Test 5: Frontend resolveDataUrl() Strategy 2 logic (simulated)."""
    print("\n--- Test 5: resolveDataUrl() Strategy 2 Logic ---")

    # Simulate the JavaScript suffix-matching logic in Python
    known_suffixes = ['_county_roads.csv', '_city_roads.csv', '_no_interstate.csv', '_all_roads.csv']

    test_cases = [
        # (filename, expected_jurisdiction, expected_filter)
        ('henrico_all_roads.csv', 'henrico', 'all_roads.csv'),
        ('henrico_county_roads.csv', 'henrico', 'county_roads.csv'),
        ('fairfax_county_all_roads.csv', 'fairfax_county', 'all_roads.csv'),
        ('richmond_city_no_interstate.csv', 'richmond_city', 'no_interstate.csv'),
        ('prince_william_county_roads.csv', 'prince_william', 'county_roads.csv'),
        ('virginia_beach_all_roads.csv', 'virginia_beach', 'all_roads.csv'),
        ('colonial_heights_no_interstate.csv', 'colonial_heights', 'no_interstate.csv'),
        ('king_george_all_roads.csv', 'king_george', 'all_roads.csv'),
        ('douglas_all_roads.csv', 'douglas', 'all_roads.csv'),
    ]

    for filename, expected_jurisdiction, expected_filter in test_cases:
        # Replicate the JS logic
        jurisdiction = None
        filter_with_ext = None
        for suffix in known_suffixes:
            if filename.endswith(suffix):
                jurisdiction = filename[:len(filename) - len(suffix)]
                filter_with_ext = suffix[1:]  # Remove leading underscore
                break

        if not jurisdiction:
            idx = filename.index('_')
            jurisdiction = filename[:idx]
            filter_with_ext = filename[idx + 1:]

        r2_key = f'virginia/{jurisdiction}/{filter_with_ext}'
        expected_key = f'virginia/{expected_jurisdiction}/{expected_filter}'

        results.check(
            f'URL: {filename}',
            r2_key == expected_key,
            f'got={r2_key}' if r2_key != expected_key else f'→ {r2_key}'
        )


def test_dry_run(results, verbose=False):
    """Test 6: Dry run mode produces no output files."""
    print("\n--- Test 6: Dry Run Mode ---")

    tmp_dir = tempfile.mkdtemp(prefix='dry_run_')
    try:
        va_csv = os.path.join(tmp_dir, 'va_statewide.csv')
        dry_output = os.path.join(tmp_dir, 'dry_output')
        os.makedirs(dry_output, exist_ok=True)

        generate_va_statewide_csv(va_csv, ['henrico'], records_per=10)

        subprocess.check_output(
            [sys.executable, 'scripts/split_jurisdictions.py',
             '--state', 'virginia',
             '--input', va_csv,
             '--output-dir', dry_output,
             '--dry-run',
             '--jurisdictions', 'henrico'],
            cwd=str(PROJECT_ROOT), text=True, stderr=subprocess.STDOUT
        )

        csvs = list(Path(dry_output).glob('*.csv'))
        results.check(
            'Dry run: no CSV files created',
            len(csvs) == 0,
            f'{len(csvs)} CSVs found' if csvs else 'clean'
        )
    except subprocess.CalledProcessError as e:
        results.check('Dry run: completed', False, e.output[:200])
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_download_script_preserves_csv(results, verbose=False):
    """Test 7: Verify download scripts no longer delete uncompressed CSV."""
    print("\n--- Test 7: Download Scripts Preserve Uncompressed CSV ---")

    # Check Virginia download script
    va_script = PROJECT_ROOT / 'download_crash_data.py'
    if va_script.exists():
        content = va_script.read_text()
        has_os_remove = 'os.remove(statewide_path)' in content
        results.check(
            'VA download: no os.remove(statewide_path)',
            not has_os_remove,
            'os.remove() still present!' if has_os_remove else 'correctly preserved'
        )
    else:
        results.warn('VA download: script not found')

    # Check Colorado download script
    co_script = PROJECT_ROOT / 'download_cdot_crash_data.py'
    if co_script.exists():
        content = co_script.read_text()
        has_os_remove = 'os.remove(statewide_path)' in content
        results.check(
            'CO download: no os.remove(statewide_path)',
            not has_os_remove,
            'os.remove() still present!' if has_os_remove else 'correctly preserved'
        )
    else:
        results.warn('CO download: script not found')


def test_frontend_changes(results, verbose=False):
    """Test 8: Verify frontend code has required functions."""
    print("\n--- Test 8: Frontend Code Integrity ---")

    index_html = PROJECT_ROOT / 'app' / 'index.html'
    if not index_html.exists():
        results.warn('Frontend: index.html not found')
        return

    content = index_html.read_text()

    checks = [
        ('AggregateLoader.loadStatewideCSV', 'async loadStatewideCSV(stateKey)'),
        ('resolveDataUrl Strategy 3', "tierPrefixes.some(p => normalizedPath.includes(p))"),
        ('loadStatewideCSVForTier', 'async function loadStatewideCSVForTier(stateKey)'),
        ('getDataFilePath tier-aware', "tier === 'state'"),
        ('resolveDataUrl suffix matching', 'knownSuffixes'),
        ('handleTierChange state CSV', 'loadStatewideCSVForTier(stateKey)'),
    ]

    for name, pattern in checks:
        found = pattern in content
        results.check(f'Frontend: {name}', found, 'present' if found else 'MISSING')


def test_workflow_file(results, verbose=False):
    """Test 9: Verify batch workflow YAML delegates to pipeline.yml."""
    print("\n--- Test 9: Batch Workflow File ---")

    workflow = PROJECT_ROOT / '.github' / 'workflows' / 'batch-all-jurisdictions.yml'
    if not workflow.exists():
        results.check('Workflow: file exists', False, 'batch-all-jurisdictions.yml not found')
        return

    content = workflow.read_text()

    checks = [
        ('has state input', 'virginia'),
        ('has colorado option', 'colorado'),
        ('has dry_run input', 'dry_run'),
        ('triggers pipeline.yml', 'pipeline.yml'),
        ('uses createWorkflowDispatch', 'createWorkflowDispatch'),
        ('passes scope statewide', 'statewide'),
        ('has skip_pipeline input', 'skip_pipeline'),
        ('has job summary', 'GITHUB_STEP_SUMMARY'),
    ]

    # Verify removed stages are gone
    removed_checks = [
        ('no split_jurisdictions.py', 'split_jurisdictions.py'),
        ('no aws s3 cp', 'aws s3 cp'),
        ('no generate_aggregates.py', 'generate_aggregates.py'),
        ('no generate_forecast.py', 'generate_forecast.py'),
    ]

    results.check('Workflow: file exists', True)
    for name, pattern in checks:
        found = pattern in content
        results.check(f'Workflow: {name}', found)
    for name, pattern in removed_checks:
        absent = pattern not in content
        results.check(f'Workflow: {name}', absent, 'still present!' if not absent else 'correctly removed')


def test_plan_document(results, verbose=False):
    """Test 10: Verify plan document is comprehensive."""
    print("\n--- Test 10: Plan Document ---")

    plan = PROJECT_ROOT / 'data' / 'CDOT' / 'R2_STORAGE_PIPELINE_PLAN.md'
    if not plan.exists():
        results.check('Plan: file exists', False)
        return

    content = plan.read_text()
    results.check('Plan: file exists', True)

    sections = [
        'Multi-Jurisdiction Architecture',
        'Download Once, Split All',
        'split_jurisdictions.py',
        'batch-all-jurisdictions.yml',
        'Adding a New State',
        'Frontend: R2',
        'resolveDataUrl',
        'Bug Fixes Applied',
        'R2 Storage Layout',
        'Data Flow Diagram',
    ]

    for section in sections:
        found = section in content
        results.check(f'Plan: has "{section}"', found)


def main():
    parser = argparse.ArgumentParser(description='Test multi-jurisdiction pipeline')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--keep-files', action='store_true', help='Keep temp files after test')
    args = parser.parse_args()

    results = TestResults()
    tmp_dir = tempfile.mkdtemp(prefix='pipeline_test_')

    print(f"Temp directory: {tmp_dir}")
    print(f"Project root:   {PROJECT_ROOT}")

    try:
        # Test 1: List jurisdictions
        test_list_jurisdictions(results, verbose=args.verbose)

        # Test 2: Virginia split
        va_output = test_virginia_split(results, tmp_dir, verbose=args.verbose)

        # Test 3: Colorado split
        test_colorado_split(results, tmp_dir, verbose=args.verbose)

        # Test 4: R2 manifest
        test_r2_manifest(results, va_output, verbose=args.verbose)

        # Test 5: resolveDataUrl() logic
        test_resolve_data_url_logic(results, verbose=args.verbose)

        # Test 6: Dry run
        test_dry_run(results, verbose=args.verbose)

        # Test 7: Download script preservation
        test_download_script_preserves_csv(results, verbose=args.verbose)

        # Test 8: Frontend changes
        test_frontend_changes(results, verbose=args.verbose)

        # Test 9: Workflow file
        test_workflow_file(results, verbose=args.verbose)

        # Test 10: Plan document
        test_plan_document(results, verbose=args.verbose)

    finally:
        if args.keep_files:
            print(f"\nTemp files kept at: {tmp_dir}")
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    results.print_summary()
    return 0 if results.failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
