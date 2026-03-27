#!/usr/bin/env python3
"""
CRASH LENS — Virginia Crash Data Downloader (v6)

Downloads CrashData_Basic from virginiaroads.org via ArcGIS FeatureServer.
ALL API calls are routed through Playwright's browser context to bypass
VDOT's bot detection (Python requests gets connection-reset).

Strategy:
  1. Open browser, navigate to Hub page (establishes session/cookies)
  2. Discover live FeatureServer URL from network traffic
  3. Paginate ALL records via fetch() inside browser context
  4. Stream results to CSV on disk

No row limit. Works at any dataset size. Future-proof.

Usage:
    python download_virginia_crash_data.py --data-dir data --jurisdiction henrico --force
    python download_virginia_crash_data.py --data-dir data --force --batch-size 10000
    python download_virginia_crash_data.py --data-dir data --headful --force
"""

import argparse
import csv
import gzip
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('virginia-download')

SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIG_PATH = SCRIPT_DIR / 'config.json'

HUB_URL = 'https://www.virginiaroads.org/datasets/VDOT::full-crash-1/explore?layer=0'

# Known FeatureServer URLs (discovered 2026-03-15)
KNOWN_FEATURE_SERVERS = [
    'https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/Full_Crash/FeatureServer/0',
    'https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/CrashData_Basic/FeatureServer/0',
    'https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/CrashData_Basic_Updated/FeatureServer/0',
]

CACHE_PATH = SCRIPT_DIR / '.virginia_featureserver_cache.json'


def load_cached_url():
    if CACHE_PATH.exists():
        try:
            data = json.loads(CACHE_PATH.read_text())
            url = data.get('url', '')
            if url:
                logger.info(f"  Cached: {url}")
                return url
        except:
            pass
    return None


def save_cached_url(url):
    try:
        CACHE_PATH.write_text(json.dumps({
            'url': url, 'discovered': datetime.now().isoformat(),
        }))
    except:
        pass


# ============================================================================
# Browser-context API helper
# ============================================================================

def browser_fetch_json(page, url, retries=5):
    """
    Make a fetch() call FROM the browser context. This bypasses bot detection
    because the request has the browser's TLS fingerprint, cookies, and headers.
    Returns parsed JSON dict, or None on failure.
    """
    for attempt in range(retries):
        try:
            result = page.evaluate(f"""
                async () => {{
                    try {{
                        const resp = await fetch("{url}");
                        if (!resp.ok) return {{error: 'HTTP ' + resp.status}};
                        const text = await resp.text();
                        return JSON.parse(text);
                    }} catch(e) {{
                        return {{error: e.message}};
                    }}
                }}
            """)

            if result and 'error' not in result:
                return result

            err = result.get('error', 'unknown') if result else 'null response'
            if attempt < retries - 1:
                wait = min(15, 2 ** attempt)
                logger.info(f"    fetch error: {err}, retry {attempt+1} in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"    fetch failed after {retries} attempts: {err}")
                return None

        except Exception as e:
            if attempt < retries - 1:
                wait = min(15, 2 ** attempt)
                logger.info(f"    exception: {e}, retry {attempt+1} in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"    exception after {retries} attempts: {e}")
                return None

    return None


# ============================================================================
# Main download flow (all inside one browser session)
# ============================================================================

def download_with_browser(output_path, headless=True, batch_size=5000):
    """
    Single browser session that:
      1. Loads Hub page (establishes session)
      2. Discovers FeatureServer URL
      3. Paginates all records via browser fetch()
      4. Streams to CSV
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )
            page = context.new_page()

            # ============================================================
            # Step 1: Load Hub page (establishes cookies/session)
            # ============================================================
            logger.info("Step 1: Loading Hub page to establish session...")
            
            # Capture FeatureServer URLs from network traffic
            discovered_fs_urls = []
            def on_request(request):
                url = request.url
                if 'FeatureServer' in url:
                    discovered_fs_urls.append(url)
                if 'replicafilescache' in url:
                    discovered_fs_urls.append(url)
            page.on('request', on_request)

            page.goto(HUB_URL, wait_until='networkidle', timeout=60000)
            page.wait_for_timeout(3000)
            logger.info(f"  Page loaded: {page.title()}")

            # ============================================================
            # Step 2: Find FeatureServer URL
            # ============================================================
            logger.info("Step 2: Finding FeatureServer URL...")

            service_url = None

            # Try cached URL first (via browser fetch to verify)
            cached = load_cached_url()
            if cached:
                count_url = f'{cached}/query?where=1%3D1&returnCountOnly=true&f=json'
                data = browser_fetch_json(page, count_url, retries=2)
                if data and 'count' in data and data['count'] > 100000:
                    service_url = cached
                    total_count = data['count']
                    logger.info(f"  Cached URL works: {total_count:,} records")

            # Discover from network traffic
            if not service_url:
                # Click Download → CSV to trigger network requests that reveal the URL
                logger.info("  Triggering download to discover URL...")
                page.evaluate("""
                    () => {
                        const walker = document.createTreeWalker(
                            document.body, NodeFilter.SHOW_ELEMENT, null
                        );
                        let node;
                        while (node = walker.nextNode()) {
                            const text = node.textContent.trim();
                            const tag = node.tagName.toLowerCase();
                            if (text === 'Download' && !text.includes('Recent') &&
                                (tag === 'a' || tag === 'button' || tag === 'calcite-action' || tag === 'calcite-button')) {
                                const rect = node.getBoundingClientRect();
                                if (rect.x < 600 && rect.y > 200) {
                                    node.click();
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                """)
                page.wait_for_timeout(3000)

                # Click first download button (CSV)
                try:
                    btns = page.locator('a:has-text("Download"), button:has-text("Download"), calcite-button:has-text("Download")').all()
                    for btn in btns:
                        try:
                            if not btn.is_visible(timeout=1000):
                                continue
                            text = btn.inner_text().strip()
                            bbox = btn.bounding_box()
                            if 'Recent' in text or (bbox and bbox['y'] < 200):
                                continue
                            btn.click()
                            page.wait_for_timeout(5000)
                            break
                        except:
                            continue
                except:
                    pass

                # Extract FeatureServer URL from captured traffic
                for url in discovered_fs_urls:
                    match = re.search(
                        r'(https://services\.arcgis\.com/[^/]+/arcgis/rest/services/[^/]+/FeatureServer)',
                        url
                    )
                    if match:
                        candidate = match.group(1) + '/0'
                        count_url = f'{candidate}/query?where=1%3D1&returnCountOnly=true&f=json'
                        data = browser_fetch_json(page, count_url, retries=3)
                        if data and 'count' in data and data['count'] > 100000:
                            service_url = candidate
                            total_count = data['count']
                            save_cached_url(service_url)
                            logger.info(f"  Discovered: {service_url} ({total_count:,} records)")
                            break

                    # Also check replicafilescache pattern
                    match = re.search(r'rest/services/([^/]+)/FeatureServer', url)
                    if match:
                        svc_name = match.group(1)
                        candidate = f'https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/{svc_name}/FeatureServer/0'
                        if candidate != service_url:
                            count_url = f'{candidate}/query?where=1%3D1&returnCountOnly=true&f=json'
                            data = browser_fetch_json(page, count_url, retries=3)
                            if data and 'count' in data and data['count'] > 100000:
                                service_url = candidate
                                total_count = data['count']
                                save_cached_url(service_url)
                                logger.info(f"  Discovered: {service_url} ({total_count:,} records)")
                                break

            # Try known URLs as fallback
            if not service_url:
                logger.info("  Trying known FeatureServer URLs via browser fetch...")
                for url in KNOWN_FEATURE_SERVERS:
                    svc_name = url.split('/')[-3]
                    count_url = f'{url}/query?where=1%3D1&returnCountOnly=true&f=json'
                    data = browser_fetch_json(page, count_url, retries=3)
                    if data and 'count' in data:
                        count = data['count']
                        logger.info(f"  {svc_name}: {count:,} records")
                        if count > 100000:
                            service_url = url
                            total_count = count
                            save_cached_url(service_url)
                            break
                    else:
                        logger.info(f"  {svc_name}: failed")

            if not service_url:
                logger.error("  No working FeatureServer found")
                browser.close()
                return False

            logger.info(f"  Using: {service_url}")
            logger.info(f"  Total records: {total_count:,}")

            # ============================================================
            # Step 3: Get field schema
            # ============================================================
            logger.info("Step 3: Fetching field schema...")
            schema_url = f'{service_url}?f=json'
            schema = browser_fetch_json(page, schema_url)
            
            if not schema or 'fields' not in schema:
                logger.error("  Could not get field schema")
                browser.close()
                return False

            fields = schema['fields']
            has_geometry = 'Point' in schema.get('geometryType', '')
            fieldnames = [f['name'] for f in fields]
            if has_geometry:
                fieldnames.extend(['x', 'y'])

            logger.info(f"  {len(fields)} fields, geometry={has_geometry}")
            logger.info(f"  Fields: {', '.join(fieldnames[:10])}...")

            # ============================================================
            # Step 4: Paginate all records via browser fetch()
            # ============================================================
            logger.info(f"Step 4: Downloading {total_count:,} records ({batch_size}/batch)...")

            temp_path = output_path + '.partial'
            offset = 0
            total_written = 0
            start_time = time.time()
            consecutive_errors = 0
            max_consecutive_errors = 10

            query_base = f'{service_url}/query'

            with open(temp_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()

                while offset < total_count + batch_size:
                    # Progress logging
                    pct = min(100, int(offset / max(total_count, 1) * 100))
                    elapsed = time.time() - start_time
                    rate = total_written / max(elapsed, 0.1)
                    eta = (total_count - total_written) / max(rate, 0.1)

                    if offset % (batch_size * 5) == 0 or offset == 0:
                        logger.info(
                            f"  {total_written:,} / {total_count:,} ({pct}%) "
                            f"| {rate:.0f} rec/s | ETA {eta/60:.1f} min"
                        )

                    # Build query URL
                    params = (
                        f'?where=1%3D1'
                        f'&outFields=*'
                        f'&resultOffset={offset}'
                        f'&resultRecordCount={batch_size}'
                        f'&orderByFields=OBJECTID+ASC'
                        f'&f=json'
                    )
                    if has_geometry:
                        params += '&outSR=4326'
                    
                    fetch_url = query_base + params

                    # Fetch via browser
                    data = browser_fetch_json(page, fetch_url, retries=5)

                    if data is None:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"  {max_consecutive_errors} consecutive errors, stopping")
                            break
                        # Try refreshing the page
                        logger.info("  Refreshing page to reset session...")
                        page.goto(HUB_URL, wait_until='networkidle', timeout=60000)
                        page.wait_for_timeout(3000)
                        continue

                    if 'error' in data:
                        err = data['error']
                        logger.warning(f"  API error at offset {offset}: {err}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            break
                        time.sleep(2)
                        continue

                    features = data.get('features', [])
                    if not features:
                        break

                    consecutive_errors = 0

                    # Write batch to CSV
                    for feat in features:
                        row = feat.get('attributes', {})
                        geom = feat.get('geometry')
                        if geom and has_geometry:
                            row['x'] = geom.get('x', '')
                            row['y'] = geom.get('y', '')
                        writer.writerow(row)
                        total_written += 1

                    offset += len(features)
                    if len(features) < batch_size:
                        break

            browser.close()

            elapsed = time.time() - start_time
            logger.info(f"  Downloaded {total_written:,} records in {elapsed/60:.1f} min")
            logger.info(f"  Average: {total_written/max(elapsed,1):.0f} rec/s")

            if total_written == 0:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return False

            if total_written < total_count * 0.95:
                logger.warning(
                    f"  Got {total_written:,} of {total_count:,} "
                    f"({total_written/total_count*100:.1f}%)"
                )

            # Replace old file with new one (handles Windows file locks)
            replaced = False
            if os.path.exists(output_path):
                for attempt in range(5):
                    try:
                        os.remove(output_path)
                        replaced = True
                        break
                    except PermissionError:
                        if attempt < 4:
                            logger.info(f"  File locked, retrying in {attempt+1}s (close Excel?)...")
                            time.sleep(attempt + 1)
                        else:
                            # Last resort: save with timestamp suffix
                            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                            alt_path = output_path.replace('.csv', f'_{ts}.csv')
                            shutil.move(temp_path, alt_path)
                            size = os.path.getsize(alt_path)
                            logger.warning(f"  Old file locked — saved as: {alt_path} ({size/(1024*1024):.1f} MB)")
                            logger.info(f"  Close Excel, delete the old file, and rename this one.")
                            return True

            shutil.move(temp_path, output_path)
            size = os.path.getsize(output_path)
            logger.info(f"  Saved: {output_path} ({size/(1024*1024):.1f} MB)")
            return True

    except Exception as e:
        logger.error(f"  Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# CSV validation
# ============================================================================

def validate_csv(path):
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            head = f.read(500)
        if '<html' in head.lower() or 'Cannot fetch' in head:
            logger.error("  Error page, not CSV")
            return False
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)
    except Exception as e:
        logger.error(f"  CSV error: {e}")
        return False

    headers_set = set(h.strip() for h in headers)
    if {'Driver_Age', 'DRIVER_AGE', 'Bike_VehicleNumber'} & headers_set:
        logger.error("  Wrong dataset (driver-level)")
        return False

    rows = sum(1 for _ in open(path, encoding='utf-8-sig')) - 1
    logger.info(f"  CSV: {len(headers)} columns, {rows:,} records")
    logger.info(f"  Columns: {headers[:10]}...")
    return True


# ============================================================================
# Jurisdiction filtering
# ============================================================================

def load_jurisdiction_config(jurisdiction):
    if not CONFIG_PATH.exists():
        return {'name': jurisdiction.replace('_', ' ').title()}
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    jurisdictions = config.get('jurisdictions', {})
    if jurisdiction in jurisdictions:
        return jurisdictions[jurisdiction]
    for key, val in jurisdictions.items():
        if key.lower() == jurisdiction.lower():
            return val
    return {'name': jurisdiction.replace('_', ' ').title()}


def filter_by_jurisdiction(df, jurisdiction, jconfig):
    import pandas as pd

    juris_code = str(jconfig.get('jurisCode', ''))
    fips = str(jconfig.get('fips', ''))
    name_patterns = jconfig.get('namePatterns', [])
    display_name = jconfig.get('name', jurisdiction.replace('_', ' ').title())

    logger.info(f"Filtering to {display_name} (code={juris_code}, fips={fips})")
    mask = pd.Series([False] * len(df), index=df.index)
    cols_lower = {c.lower().strip(): c for c in df.columns}

    if juris_code:
        for cl, ca in cols_lower.items():
            if 'juris' in cl and ('code' in cl or 'name' in cl):
                col_val = df[ca].astype(str).str.strip()
                mask |= col_val == juris_code
                if juris_code.isdigit():
                    mask |= col_val == str(int(juris_code))
                    mask |= col_val == juris_code.zfill(3)
                if mask.sum() > 0:
                    break

    if fips and mask.sum() == 0:
        for cl, ca in cols_lower.items():
            if 'fips' in cl:
                mask |= df[ca].astype(str).str.strip() == fips
                break

    if name_patterns and mask.sum() == 0:
        for cl, ca in cols_lower.items():
            if ('juris' in cl and 'name' in cl) or 'physical' in cl:
                for pat in name_patterns:
                    try:
                        mask |= df[ca].astype(str).str.contains(pat, case=False, na=False, regex=True)
                    except re.error:
                        mask |= df[ca].astype(str).str.lower() == pat.lower()
                break

    if mask.sum() == 0:
        for cl, ca in cols_lower.items():
            if any(kw in cl for kw in ['juris', 'county', 'city', 'physical']):
                mask |= df[ca].astype(str).str.upper().str.contains(display_name.upper(), na=False)
                if mask.sum() > 0:
                    break

    filtered = df[mask].copy()
    logger.info(f"  {len(df):,} -> {len(filtered):,} records for {display_name}")

    if len(filtered) == 0:
        for cl, ca in cols_lower.items():
            if any(kw in cl for kw in ['juris', 'physical', 'county']):
                sample = df[ca].dropna().unique()[:10].tolist()
                logger.warning(f"  Sample {ca}: {sample}")

    return filtered


def split_road_types(df, jurisdiction, data_dir):
    cols_lower = {c.lower().strip(): c for c in df.columns}
    sys_col = next((cols_lower[cl] for cl in cols_lower if cl in ('system', 'sys_id', 'road_system', 'route_syst')), None)
    own_col = next((cols_lower[cl] for cl in cols_lower if 'ownership' in cl), None)

    if sys_col:
        logger.info(f"  System '{sys_col}': {df[sys_col].value_counts().head(8).to_dict()}")
        county_vals = {'Non-DOT secondary', 'Non-DOT', 'SECONDARY', 'Secondary'}
        county = df[df[sys_col].astype(str).str.strip().isin(county_vals)]
        if len(county) > 0:
            county.to_csv(str(data_dir / f'{jurisdiction}_county_roads.csv'), index=False)
            logger.info(f"  county_roads: {len(county):,}")

        interstate_vals = {'Interstate', 'INTERSTATE', 'IS'}
        no_int = df[~df[sys_col].astype(str).str.strip().isin(interstate_vals)]
        if len(no_int) < len(df):
            no_int.to_csv(str(data_dir / f'{jurisdiction}_no_interstate.csv'), index=False)
            logger.info(f"  no_interstate: {len(no_int):,}")

    if own_col:
        logger.info(f"  Ownership '{own_col}': {df[own_col].value_counts().head(8).to_dict()}")
        city = df[df[own_col].astype(str).str.upper().apply(lambda v: 'CITY' in v or 'TOWN' in v)]
        if len(city) > 0:
            city.to_csv(str(data_dir / f'{jurisdiction}_city_roads.csv'), index=False)
            logger.info(f"  city_roads: {len(city):,}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Download Virginia crash data')
    parser.add_argument('--data-dir', default='data')
    parser.add_argument('--jurisdiction', default='statewide')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--gzip', action='store_true')
    parser.add_argument('--headful', action='store_true', help='Show browser window')
    parser.add_argument('--batch-size', type=int, default=5000, help='Records per request')
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    jurisdiction = args.jurisdiction.lower().strip()
    headless = not args.headful

    logger.info("=" * 60)
    logger.info("  CRASH LENS — Virginia Crash Data Download (v6)")
    logger.info(f"  Method: Browser-routed FeatureServer pagination")
    logger.info(f"  Output: {data_dir}")
    logger.info(f"  Jurisdiction: {jurisdiction}")
    logger.info(f"  Batch size: {args.batch_size:,}")
    logger.info(f"  Started: {datetime.now()}")
    logger.info("=" * 60)

    output_path = str(data_dir / 'virginia_statewide_all_roads.csv')

    if not args.force and os.path.exists(output_path):
        size = os.path.getsize(output_path)
        if size > 10_000_000:
            rows = sum(1 for _ in open(output_path, encoding='utf-8-sig')) - 1
            logger.info(f"Existing: {output_path} ({rows:,} records). Use --force.")
            csv_path = output_path
        else:
            csv_path = None
    else:
        csv_path = None

    if csv_path is None:
        success = download_with_browser(
            output_path, headless=headless, batch_size=args.batch_size
        )
        if not success:
            logger.error("Download failed")
            sys.exit(1)
        if not validate_csv(output_path):
            logger.error("CSV validation failed")
            sys.exit(1)
        csv_path = output_path

    if args.gzip:
        gz = csv_path + '.gz'
        with open(csv_path, 'rb') as fi, gzip.open(gz, 'wb') as fo:
            shutil.copyfileobj(fi, fo)
        logger.info(f"Compressed: {os.path.getsize(gz)/(1024*1024):.1f} MB")

    if jurisdiction and jurisdiction != 'statewide':
        import pandas as pd
        jconfig = load_jurisdiction_config(jurisdiction)
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
        logger.info(f"Loaded {len(df):,} statewide ({len(df.columns)} columns)")

        filtered = filter_by_jurisdiction(df, jurisdiction, jconfig)
        if len(filtered) == 0:
            logger.error(f"No records for '{jurisdiction}'")
            sys.exit(1)

        all_path = str(data_dir / f'{jurisdiction}_all_roads.csv')
        filtered.to_csv(all_path, index=False)
        logger.info(f"Saved: {all_path} ({len(filtered):,} records)")
        split_road_types(filtered, jurisdiction, data_dir)

    logger.info("")
    logger.info("=" * 60)
    logger.info("  DONE")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
