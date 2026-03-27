#!/usr/bin/env python3
"""
Headless Validator Runner — Triggers crash-data-validator-v13.html via Playwright.

Runs the same validation engine used in the Upload tab, but headlessly in CI/CD.
Reads CSVs from R2 CDN, validates, auto-corrects, and saves corrected files locally.
The calling pipeline step then re-uploads corrected files to R2.

The validator HTML is NOT modified — this script drives it via:
  1. Injecting jurisdiction config via page.evaluate() (same as postMessage API)
  2. Calling selectFile(roadType) which triggers runAutonomousPipeline()
  3. Waiting for pipeline completion (Step 5 reached)
  4. Extracting corrected CSV + validation report from page memory

Usage:
    # Validate a single jurisdiction, all road types
    python scripts/run-validator-headless.py \\
        --state virginia --jurisdiction henrico --data-dir data

    # Validate multiple jurisdictions
    python scripts/run-validator-headless.py \\
        --state virginia --jurisdictions henrico fairfax --data-dir data

    # Auto-detect jurisdictions from split output files
    python scripts/run-validator-headless.py \\
        --state virginia --auto --data-dir data

    # Specific road types only
    python scripts/run-validator-headless.py \\
        --state virginia --jurisdiction henrico --data-dir data \\
        --road-types county_roads city_roads

    # Skip auto-correct (validation report only)
    python scripts/run-validator-headless.py \\
        --state virginia --auto --data-dir data --validate-only
"""

import argparse
import http.server
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

logger = logging.getLogger('validator-headless')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent
VALIDATOR_HTML = PROJECT_ROOT / 'scripts' / 'crash-data-validator-v13.html'

# Default road types to validate (all 4 split outputs)
DEFAULT_ROAD_TYPES = ['all_roads', 'county_roads', 'city_roads', 'no_interstate']

# Timeout per road type file (seconds) — large files may take a while
DEFAULT_TIMEOUT = 300  # 5 minutes per file


def check_playwright():
    """Verify Playwright is installed."""
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True
    except ImportError:
        logger.error("Playwright not installed.")
        logger.error("Run: pip install playwright && playwright install chromium --with-deps")
        return False


def load_state_config(state):
    """Load state config for jurisdiction bounds and split config."""
    config_path = PROJECT_ROOT / 'states' / state / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def load_app_config():
    """Load main app config.json for jurisdiction definitions."""
    config_path = PROJECT_ROOT / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def get_jurisdiction_info(state, jurisdiction_id, app_config, state_config):
    """Build jurisdiction info dict for the validator.

    Returns dict with: state, county, label, bounds (or None if not found).
    """
    # Try app config jurisdictions first
    jurisdictions = app_config.get('jurisdictions', {})
    jconfig = jurisdictions.get(jurisdiction_id, {})

    # Try state config jurisdictions
    if not jconfig:
        jconfig = state_config.get('jurisdictions', {}).get(jurisdiction_id, {})

    county = jconfig.get('name', jurisdiction_id).lower().replace(' county', '').replace(' ', '_')
    label = jconfig.get('name', jurisdiction_id.replace('_', ' ').title())

    bbox = jconfig.get('bbox')
    bounds = None
    if bbox and len(bbox) == 4:
        bounds = {
            'minLon': bbox[0],
            'minLat': bbox[1],
            'maxLon': bbox[2],
            'maxLat': bbox[3],
        }

    return {
        'state': state,
        'county': county,
        'label': label,
        'bounds': bounds,
    }


def discover_jurisdictions(data_dir):
    """Auto-detect jurisdictions from *_all_roads.csv files."""
    data_path = Path(data_dir)
    jurisdictions = []
    for f in sorted(data_path.glob('*_all_roads.csv')):
        j = f.stem.replace('_all_roads', '')
        jurisdictions.append(j)
    return jurisdictions


def discover_road_types(data_dir, jurisdiction):
    """Discover which road type CSVs exist for a jurisdiction."""
    data_path = Path(data_dir)
    found = []
    for rt in DEFAULT_ROAD_TYPES:
        csv_path = data_path / f"{jurisdiction}_{rt}.csv"
        if csv_path.exists():
            found.append(rt)
    return found


def start_local_server(root_dir, port=0):
    """Start a local HTTP server to serve the validator HTML.

    Returns (server, port, thread).
    Using port=0 lets the OS pick an available port.
    """

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        """HTTP handler that serves from root_dir and suppresses log noise."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(root_dir), **kwargs)

        def log_message(self, format, *args):
            pass  # Suppress HTTP logs

    server = http.server.HTTPServer(('127.0.0.1', port), QuietHandler)
    actual_port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    return server, actual_port, thread


def validate_jurisdiction_road_type(page, base_url, state, jurisdiction_id, road_type,
                                    jinfo, data_dir, validate_only, timeout):
    """Run validation + auto-correct for one jurisdiction × road type.

    Returns dict with validation results, or None on failure.
    """
    logger.info(f"  [{jurisdiction_id}] {road_type} — starting")

    # Navigate to validator with fresh state
    page.goto(f"{base_url}/scripts/crash-data-validator-v13.html", wait_until='domcontentloaded')
    page.wait_for_timeout(500)  # Let DOM initialize

    # Inject jurisdiction config directly (bypassing postMessage/dropdown)
    county = jinfo['county']
    bounds_json = json.dumps(jinfo['bounds']) if jinfo['bounds'] else 'null'

    page.evaluate(f"""() => {{
        // Set jurisdiction fields
        document.getElementById('jurisdState').value = '{state}';
        document.getElementById('jurisdCounty').value = '{county}';

        // Set R2 path
        APP.config.r2Path = '/{state}/{county}/';
        document.getElementById('r2Path').value = APP.config.r2Path;

        // Set bounds
        const bounds = {bounds_json};
        if (bounds) {{
            APP.config.bounds = bounds;
        }}

        // Disable R2 push (we extract data and upload via CLI instead)
        APP.config.r2ServerConfigured = false;

        // Update header badge
        const badge = document.getElementById('envBadge');
        if (badge) badge.textContent = '{jinfo["label"]}';
    }}""")

    # If validate-only mode, uncheck all auto-correct checkboxes
    if validate_only:
        page.evaluate("""() => {
            ['acBounds', 'acGeocode', 'acSeverity', 'acDates', 'acTrim', 'acFields'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.checked = false;
            });
        }""")

    # Disable Nominatim geocoding in pipeline (rate-limited, slow, unnecessary in batch)
    page.evaluate("""() => {
        const el = document.getElementById('vGeocode');
        if (el) el.checked = false;
        const acGeo = document.getElementById('acGeocode');
        if (acGeo) acGeo.checked = false;
    }""")

    # Trigger the autonomous pipeline for this road type
    # selectFile() → runAutonomousPipeline() → Load → Validate → AutoCorrect → (skip Push)
    page.evaluate(f"selectFile('{road_type}')")

    # Wait for pipeline completion: Step 5 (Export) becomes active
    try:
        page.wait_for_function(
            """() => {
                const section5 = document.getElementById('section-5');
                return section5 && section5.classList.contains('active');
            }""",
            timeout=timeout * 1000
        )
    except Exception as e:
        # Check if pipeline errored or just timed out on file load (file might not exist in R2)
        error_check = page.evaluate("""() => {
            const activeFile = APP.activeFile;
            const hasData = APP.files[activeFile] && APP.files[activeFile].rows &&
                            APP.files[activeFile].rows.length > 0;
            return { activeFile, hasData, issueCount: APP.issues ? APP.issues.length : 0 };
        }""")

        if not error_check.get('hasData'):
            logger.warning(f"  [{jurisdiction_id}] {road_type} — file not found in R2 or empty, skipping")
            return None
        else:
            logger.warning(f"  [{jurisdiction_id}] {road_type} — pipeline timed out after {timeout}s: {e}")
            # Continue to extract whatever we have

    # Extract results from page
    results = page.evaluate("""() => {
        const activeFile = APP.activeFile;
        const data = APP.files[activeFile];
        if (!data || !data.rows) return null;

        const actionable = APP.issues.filter(i => i.severity !== 'info');
        const fixed = actionable.filter(i => i.fixed).length;
        const remaining = actionable.filter(i => !i.fixed).length;

        return {
            roadType: activeFile,
            totalRecords: data.rows.length,
            totalIssues: APP.issues.length,
            actionableIssues: actionable.length,
            autoFixed: fixed,
            remaining: remaining,
            fixRate: actionable.length > 0
                ? ((fixed / actionable.length) * 100).toFixed(1) + '%'
                : '100%',
            issuesByType: APP.issues.reduce((acc, i) => {
                acc[i.type] = (acc[i.type] || 0) + 1;
                return acc;
            }, {}),
            correctionsCount: Object.keys(APP.corrections || {}).length,
        };
    }""")

    if not results:
        logger.warning(f"  [{jurisdiction_id}] {road_type} — no results extracted")
        return None

    logger.info(
        f"  [{jurisdiction_id}] {road_type} — "
        f"records={results['totalRecords']:,}, "
        f"issues={results['actionableIssues']}, "
        f"fixed={results['autoFixed']}, "
        f"remaining={results['remaining']}, "
        f"fixRate={results['fixRate']}"
    )

    # Extract corrected CSV (only if auto-correct ran and made changes)
    if not validate_only and results['autoFixed'] > 0:
        csv_data = page.evaluate("""() => {
            const data = APP.files[APP.activeFile];
            if (!data) return null;
            return Papa.unparse({
                fields: data.headers,
                data: data.rows.map(row => data.headers.map(h => row[h] || ''))
            });
        }""")

        if csv_data:
            out_path = Path(data_dir) / f"{jurisdiction_id}_{road_type}.csv"
            with open(out_path, 'w', encoding='utf-8', newline='') as f:
                f.write(csv_data)
            logger.info(f"  [{jurisdiction_id}] {road_type} — saved corrected CSV → {out_path}")
            results['correctedCsvSaved'] = str(out_path)

    # Extract and save validation report
    report = page.evaluate("""() => {
        const activeFile = APP.activeFile;
        const data = APP.files[activeFile];
        if (!data) return null;

        const actionable = APP.issues.filter(i => i.severity !== 'info');
        const fixed = actionable.filter(i => i.fixed).length;
        const remaining = actionable.filter(i => !i.fixed).length;

        return {
            timestamp: new Date().toISOString(),
            trigger: 'pipeline',
            file: data.filename,
            totalRecords: data.rows.length,
            issuesFound: actionable.length,
            autoFixed: fixed,
            remaining: remaining,
            fixRate: actionable.length > 0
                ? ((fixed / actionable.length) * 100).toFixed(1) + '%'
                : '100%',
            issuesByType: APP.issues.reduce((acc, i) => {
                acc[i.type] = (acc[i.type] || 0) + 1;
                return acc;
            }, {}),
            unfixedSample: actionable.filter(i => !i.fixed).slice(0, 20).map(i => ({
                doc: i.docNbr, type: i.type, field: i.field, message: i.message
            })),
        };
    }""")

    if report:
        report_path = Path(data_dir) / f"validation_report_{jurisdiction_id}_{road_type}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        results['reportSaved'] = str(report_path)

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Run crash-data-validator-v13.html headlessly via Playwright')
    parser.add_argument('--state', required=True, help='State key (e.g., virginia, colorado)')
    parser.add_argument('--jurisdiction', help='Single jurisdiction to validate')
    parser.add_argument('--jurisdictions', nargs='+', help='Multiple jurisdictions to validate')
    parser.add_argument('--auto', action='store_true',
                        help='Auto-detect jurisdictions from *_all_roads.csv files in data-dir')
    parser.add_argument('--data-dir', required=True,
                        help='Directory containing split jurisdiction CSVs')
    parser.add_argument('--road-types', nargs='+', default=None,
                        help=f'Road types to validate (default: {DEFAULT_ROAD_TYPES})')
    parser.add_argument('--validate-only', action='store_true',
                        help='Run validation checks only (skip auto-correct)')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                        help=f'Timeout per file in seconds (default: {DEFAULT_TIMEOUT})')
    parser.add_argument('--summary-file', default=None,
                        help='Path to save summary JSON (default: {data-dir}/validation_summary.json)')
    args = parser.parse_args()

    if not check_playwright():
        return 1

    if not VALIDATOR_HTML.exists():
        logger.error(f"Validator HTML not found: {VALIDATOR_HTML}")
        return 1

    # Determine jurisdictions
    jurisdictions = []
    if args.jurisdiction:
        jurisdictions = [args.jurisdiction]
    elif args.jurisdictions:
        jurisdictions = args.jurisdictions
    elif args.auto:
        jurisdictions = discover_jurisdictions(args.data_dir)
        if not jurisdictions:
            logger.error(f"No *_all_roads.csv files found in {args.data_dir}")
            return 1
        logger.info(f"Auto-detected {len(jurisdictions)} jurisdictions")
    else:
        logger.error("Specify --jurisdiction, --jurisdictions, or --auto")
        return 1

    # Load configs
    state = args.state
    state_config = load_state_config(state)
    app_config = load_app_config()

    # Check if city_roads split is configured for this state
    split_config = state_config.get('roadSystems', {}).get('splitConfig', {})
    has_city_roads = 'cityRoads' in split_config

    # Determine road types to validate
    road_types = args.road_types or DEFAULT_ROAD_TYPES
    if not has_city_roads and 'city_roads' in road_types:
        road_types = [rt for rt in road_types if rt != 'city_roads']
        logger.info(f"[{state}] No cityRoads in splitConfig — skipping city_roads validation")

    logger.info(f"[{state}] Headless validation")
    logger.info(f"[{state}] Jurisdictions: {len(jurisdictions)}")
    logger.info(f"[{state}] Road types: {road_types}")
    logger.info(f"[{state}] Mode: {'validate-only' if args.validate_only else 'validate + auto-correct'}")

    # Start local HTTP server
    server, port, _ = start_local_server(PROJECT_ROOT)
    base_url = f"http://127.0.0.1:{port}"
    logger.info(f"Local server started on port {port}")

    # Run Playwright
    from playwright.sync_api import sync_playwright

    summary = {
        'state': state,
        'trigger': 'pipeline',
        'startTime': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'mode': 'validate-only' if args.validate_only else 'validate-and-correct',
        'jurisdictions': {},
        'totals': {
            'jurisdictions': 0,
            'filesValidated': 0,
            'totalRecords': 0,
            'totalIssues': 0,
            'totalFixed': 0,
            'totalRemaining': 0,
            'correctedFilesSaved': 0,
        }
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-web-security', '--no-sandbox']
        )
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            # Bypass CORS for R2 CDN reads
            bypass_csp=True,
        )
        page = context.new_page()

        # Suppress console noise from the validator
        page.on('console', lambda msg: None)

        for jurisdiction_id in jurisdictions:
            jinfo = get_jurisdiction_info(state, jurisdiction_id, app_config, state_config)
            logger.info(f"[{jurisdiction_id}] Processing ({jinfo['label']})")

            # Determine which road types actually exist for this jurisdiction
            available_road_types = discover_road_types(args.data_dir, jurisdiction_id)
            active_road_types = [rt for rt in road_types if rt in available_road_types]

            if not active_road_types:
                logger.warning(f"[{jurisdiction_id}] No matching road type CSVs found locally, "
                               f"will attempt R2 fetch for: {road_types}")
                active_road_types = road_types

            jurisdiction_results = {}

            for road_type in active_road_types:
                try:
                    result = validate_jurisdiction_road_type(
                        page=page,
                        base_url=base_url,
                        state=state,
                        jurisdiction_id=jurisdiction_id,
                        road_type=road_type,
                        jinfo=jinfo,
                        data_dir=args.data_dir,
                        validate_only=args.validate_only,
                        timeout=args.timeout,
                    )
                    if result:
                        jurisdiction_results[road_type] = result
                        summary['totals']['filesValidated'] += 1
                        summary['totals']['totalRecords'] += result.get('totalRecords', 0)
                        summary['totals']['totalIssues'] += result.get('actionableIssues', 0)
                        summary['totals']['totalFixed'] += result.get('autoFixed', 0)
                        summary['totals']['totalRemaining'] += result.get('remaining', 0)
                        if result.get('correctedCsvSaved'):
                            summary['totals']['correctedFilesSaved'] += 1
                except Exception as e:
                    logger.error(f"  [{jurisdiction_id}] {road_type} — error: {e}")
                    jurisdiction_results[road_type] = {'error': str(e)}

            if jurisdiction_results:
                summary['jurisdictions'][jurisdiction_id] = jurisdiction_results
                summary['totals']['jurisdictions'] += 1

        browser.close()

    # Shutdown server
    server.shutdown()

    summary['endTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    # Save summary
    summary_path = args.summary_file or str(Path(args.data_dir) / 'validation_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    # Print summary
    t = summary['totals']
    logger.info("=" * 60)
    logger.info(f"[{state}] HEADLESS VALIDATION COMPLETE")
    logger.info(f"[{state}]   Jurisdictions:     {t['jurisdictions']}")
    logger.info(f"[{state}]   Files validated:    {t['filesValidated']}")
    logger.info(f"[{state}]   Total records:      {t['totalRecords']:,}")
    logger.info(f"[{state}]   Issues found:       {t['totalIssues']:,}")
    logger.info(f"[{state}]   Auto-fixed:         {t['totalFixed']:,}")
    logger.info(f"[{state}]   Remaining:          {t['totalRemaining']:,}")
    logger.info(f"[{state}]   Corrected CSVs:     {t['correctedFilesSaved']}")
    logger.info(f"[{state}]   Summary:            {summary_path}")
    logger.info("=" * 60)

    # GitHub Actions annotation for issues
    if t['totalRemaining'] > 0:
        print(f"::warning::Validation found {t['totalRemaining']} unfixed issues "
              f"across {t['jurisdictions']} jurisdictions — see validation reports")

    return 0


if __name__ == '__main__':
    sys.exit(main())
