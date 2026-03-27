#!/usr/bin/env python3
"""
CRASH LENS — Delaware Crash Data Downloader (v2)
=================================================

Two-strategy downloader for Delaware crash data:

  Strategy 1 (Primary):  Socrata CSV export from data.delaware.gov
    - Fast, supports server-side SoQL filtering ($where)
    - Includes CRASH DATETIME and YEAR in CSV export
    - May fail with 403 / rate-limit / timeout on large downloads

  Strategy 2 (Fallback): Playwright browser-routed FeatureServer pagination
    from de-firstmap-delaware.hub.arcgis.com (ArcGIS Hub / FirstMap)
    - 566,762 records (2009 — present)
    - No row limit (bypasses ArcGIS 1M-row CSV export cap)
    - Bypasses bot detection via browser TLS fingerprint
    - Discovers FeatureServer URL automatically, caches for reuse

Data Sources:
  Socrata:   https://data.delaware.gov/Transportation/Public-Crash-Data/827n-m6xc
  ArcGIS Hub: https://de-firstmap-delaware.hub.arcgis.com/datasets/
              delaware::delaware-public-crash-data-2-0/explore

Usage:
    python download_delaware_crash_data.py --jurisdiction sussex
    python download_delaware_crash_data.py --jurisdiction new_castle --years 2023 2024
    python download_delaware_crash_data.py --jurisdiction statewide --force
    python download_delaware_crash_data.py --strategy playwright --headful
    python download_delaware_crash_data.py --strategy socrata --gzip
    python download_delaware_crash_data.py --health-check
    python download_delaware_crash_data.py --discover-only
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("delaware_downloader")

# =============================================================================
# Constants — Socrata (Strategy 1)
# =============================================================================

# CSV export has ALL fields including CRASH DATETIME and YEAR
SOCRATA_CSV_EXPORT_URL = "https://data.delaware.gov/api/views/827n-m6xc/rows.csv?accessType=DOWNLOAD"
# JSON API (used only for health check — missing datetime fields)
SOCRATA_JSON_URL = "https://data.delaware.gov/resource/827n-m6xc.json"
# SODA CSV endpoint — supports SoQL $where filtering (server-side)
SOCRATA_SODA_CSV_URL = "https://data.delaware.gov/resource/827n-m6xc.csv"

# =============================================================================
# Constants — ArcGIS Hub / FirstMap (Strategy 2)
# =============================================================================

HUB_URL = (
    "https://de-firstmap-delaware.hub.arcgis.com/datasets/"
    "delaware::delaware-public-crash-data-2-0/explore"
    "?location=39.143300%2C-75.422000%2C8"
)

# Known FeatureServer URLs — Delaware self-hosts on enterprise.firstmap.delaware.gov.
# Discovered via ArcGIS item API: the item points to a MapServer; we try both
# MapServer and FeatureServer variants. The test server (firstmaptest) has the
# service but it's not started, so we prefer the production server.
KNOWN_FEATURE_SERVERS = [
    "https://enterprise.firstmap.delaware.gov/arcgis/rest/services/Transportation/DE_Public_Crash_Data/FeatureServer/0",
    "https://enterprise.firstmap.delaware.gov/arcgis/rest/services/Transportation/DE_Public_Crash_Data/MapServer/0",
    "https://apps.firstmap.delaware.gov/apps/rest/services/DelDot/DE_ODP_CRASH_DATA/FeatureServer/0",
    "https://enterprise.firstmaptest.delaware.gov/arcgis/rest/services/Transportation/DE_Public_Crash_Data/FeatureServer/0",
]

# Minimum record count to consider a FeatureServer valid
# (Delaware has ~566K records — anything below 50K is wrong)
MIN_RECORD_COUNT = 50000

# ArcGIS Online item ID for Delaware Public Crash Data 2.0
# Used to query the sharing REST API for the actual service URL
ARCGIS_ITEM_ID = "f73de2b024974bcda0c94f66d1c03f83"

CACHE_PATH = Path(".delaware_featureserver_cache.json")

# =============================================================================
# Constants — Shared
# =============================================================================

MAX_RETRIES = 4
RETRY_BACKOFF = [2, 4, 8, 16]

# Delaware jurisdictions: slug → county info
JURISDICTIONS = {
    "kent":       {"county_name": "Kent",       "fips": "10001"},
    "new_castle": {"county_name": "New Castle",  "fips": "10003"},
    "sussex":     {"county_name": "Sussex",      "fips": "10005"},
}

VALID_JURISDICTIONS = list(JURISDICTIONS.keys()) + ["statewide"]

# Column name candidates — Socrata CSV export uses UPPERCASE;
# ArcGIS Hub FeatureServer may use different casing
COUNTY_COLUMN_CANDIDATES = [
    "COUNTY NAME", "COUNTY_NAME", "county_name", "County_Name",
    "COUNTY DESC", "COUNTY_DESC", "county_desc", "County_Desc",
    "COUNTY", "county", "County",
]

YEAR_COLUMN_CANDIDATES = [
    "YEAR", "year", "Year",
    "CRASH YEAR", "crash_year", "Crash_Year",
    "CRASH_YEAR",
]


# =============================================================================
# Utility Functions
# =============================================================================

def retry_request(url, params=None, max_retries=MAX_RETRIES, stream=False):
    """HTTP GET with exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=300, stream=stream)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"  Attempt {attempt + 1}/{max_retries} failed: {e}")
                time.sleep(wait)
            else:
                raise


def find_column(headers, candidates):
    """Find the first matching column name from a list of candidates."""
    header_set = set(headers)
    for c in candidates:
        if c in header_set:
            return c
    # Try case-insensitive
    header_lower = {h.lower().strip(): h for h in headers}
    for c in candidates:
        if c.lower().strip() in header_lower:
            return header_lower[c.lower().strip()]
    return None


def save_csv(records, fieldnames, output_path, gzip_output=False):
    """Save records to CSV (optionally gzipped)."""
    if not records:
        return None
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if gzip_output:
        gz_path = str(output_path) + ".gz"
        with gzip.open(gz_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {gz_path} ({os.path.getsize(gz_path):,} bytes)")
        return gz_path
    else:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"  Saved: {output_path} ({os.path.getsize(output_path):,} bytes)")
        return str(output_path)


def filter_records(records, headers, jurisdiction_key, years):
    """Client-side filtering by jurisdiction and/or year."""
    if not jurisdiction_key and not years:
        return records

    county_col = find_column(headers, COUNTY_COLUMN_CANDIDATES)
    year_col = find_column(headers, YEAR_COLUMN_CANDIDATES)

    filtered = records
    before = len(filtered)

    # Filter by jurisdiction
    if jurisdiction_key and jurisdiction_key in JURISDICTIONS:
        target = JURISDICTIONS[jurisdiction_key]["county_name"]
        if county_col:
            filtered = [
                r for r in filtered
                if r.get(county_col, "").strip().lower() == target.lower()
            ]
            log.info(f"  Filter {county_col}='{target}': {before:,} → {len(filtered):,}")
        else:
            log.warning(f"  County column not found! Tried: {COUNTY_COLUMN_CANDIDATES}")
            log.warning(f"  Available: {headers[:15]}...")

    # Filter by year
    if years and year_col:
        before = len(filtered)
        year_strs = {str(y) for y in years}
        filtered = [r for r in filtered if r.get(year_col, "").strip() in year_strs]
        log.info(f"  Filter {year_col} in {sorted(years)}: {before:,} → {len(filtered):,}")
    elif years:
        log.warning(f"  Year column not found! Tried: {YEAR_COLUMN_CANDIDATES}")

    return filtered


# =============================================================================
# Health Check
# =============================================================================

def health_check():
    """Test both Socrata API and ArcGIS Hub connectivity."""
    log.info("=" * 62)
    log.info("  Delaware Crash Data — Health Check")
    log.info("=" * 62)

    # --- Socrata ---
    log.info("")
    log.info("Strategy 1: Socrata (data.delaware.gov)")
    try:
        resp = retry_request(SOCRATA_JSON_URL, params={"$limit": 1})
        data = resp.json()
        if data:
            log.info(f"  ✓ JSON API healthy — fields: {list(data[0].keys())[:8]}...")
        resp2 = retry_request(SOCRATA_CSV_EXPORT_URL, stream=True)
        first_line = next(resp2.iter_lines(decode_unicode=True))
        resp2.close()
        cols = first_line.split(",")[:8]
        log.info(f"  ✓ CSV export healthy — columns: {cols}...")
        socrata_ok = True
    except Exception as e:
        log.error(f"  ✗ Socrata FAILED: {e}")
        socrata_ok = False

    # --- ArcGIS Hub ---
    log.info("")
    log.info("Strategy 2: ArcGIS Hub (FirstMap)")
    hub_ok = False
    try:
        resp = requests.get(HUB_URL, timeout=30)
        if resp.status_code == 200:
            log.info(f"  ✓ Hub page reachable (HTTP {resp.status_code})")
        else:
            log.warning(f"  ⚠ Hub page returned HTTP {resp.status_code}")

        # Try known FeatureServer URLs via direct requests
        for url in KNOWN_FEATURE_SERVERS:
            svc_name = url.split("/")[-3]
            try:
                count_url = f"{url}/query?where=1%3D1&returnCountOnly=true&f=json"
                resp = requests.get(count_url, timeout=15)
                data = resp.json()
                if "count" in data:
                    log.info(f"  ✓ {svc_name}: {data['count']:,} records (direct)")
                    hub_ok = True
                    break
                else:
                    log.info(f"  ✗ {svc_name}: no count in response")
            except requests.exceptions.ConnectionError:
                log.info(f"  ✗ {svc_name}: connection reset (bot detection — Playwright needed)")
            except Exception as e:
                log.info(f"  ✗ {svc_name}: {e}")

        if not hub_ok:
            log.info("  ℹ Direct API blocked — Playwright browser routing will be required")
            hub_ok = True  # Hub page loaded, just need Playwright for API

    except Exception as e:
        log.error(f"  ✗ ArcGIS Hub FAILED: {e}")

    log.info("")
    log.info(f"  Socrata:    {'✓ OK' if socrata_ok else '✗ FAILED'}")
    log.info(f"  ArcGIS Hub: {'✓ OK' if hub_ok else '✗ FAILED'}")
    return socrata_ok or hub_ok


# =============================================================================
# Strategy 1: Socrata CSV Export (Primary)
# =============================================================================

def download_via_socrata(jurisdiction_key=None, years=None):
    """
    Download from Socrata CSV export endpoint.
    Uses SODA CSV with server-side $where filtering when possible.
    Falls back to full CSV export for statewide (includes CRASH DATETIME).
    Returns (records, fieldnames) or raises on failure.
    """
    # Build SoQL $where for server-side filtering
    where_clauses = []
    if jurisdiction_key and jurisdiction_key in JURISDICTIONS:
        county = JURISDICTIONS[jurisdiction_key]["county_name"]
        where_clauses.append(f"county_desc='{county}'")
    if years:
        year_list = ",".join(str(y) for y in years)
        where_clauses.append(f"year in({year_list})")

    if where_clauses:
        where = " AND ".join(where_clauses)
        params = {"$where": where, "$limit": 500000}
        log.info(f"  SODA CSV filter: {where}")

        resp = retry_request(SOCRATA_SODA_CSV_URL, params=params)
        raw_text = resp.text
        log.info(f"  Downloaded {len(raw_text):,} bytes")

        reader = csv.DictReader(io.StringIO(raw_text))
        records = list(reader)
        fieldnames = reader.fieldnames or []
        log.info(f"  Parsed {len(records):,} records × {len(fieldnames)} columns")

        # SODA may drop crash_datetime — check and fall through if needed
        has_datetime = any(
            "datetime" in f.lower() for f in fieldnames
        )
        if records and has_datetime:
            return records, fieldnames
        elif records and not has_datetime:
            log.warning("  SODA CSV missing crash_datetime — falling back to full export")
        # Fall through to full export

    # Full CSV export (no server-side filtering, but has all columns)
    log.info("  Downloading full Socrata CSV export...")
    resp = retry_request(SOCRATA_CSV_EXPORT_URL)
    content_length = resp.headers.get("Content-Length")
    if content_length:
        log.info(f"  Content-Length: {int(content_length):,} bytes")

    raw_text = resp.text
    log.info(f"  Downloaded {len(raw_text):,} bytes")

    reader = csv.DictReader(io.StringIO(raw_text))
    records = list(reader)
    fieldnames = reader.fieldnames or []
    log.info(f"  Parsed {len(records):,} records × {len(fieldnames)} columns")

    return records, fieldnames


# =============================================================================
# Strategy 2: SODA2 Paginated Batch Download (Fallback #1)
# =============================================================================

def download_via_soda2_paginated(jurisdiction_key=None, years=None, batch_size=50000):
    """
    Download crash data using SODA2 $limit/$offset pagination.

    Delaware's SODA2 API has a default limit of 1,000 rows, but you can
    override it with $limit up to 50,000 per request. This function
    paginates through the entire dataset in batches.

    Supports server-side $where filtering for jurisdiction and year.
    Uses $order=:id to ensure stable pagination ordering.

    Returns (records, fieldnames) or raises on failure.
    """
    log.info(f"  SODA2 paginated download (batch_size={batch_size:,})...")

    # Build SoQL $where clause
    where_clauses = []
    if jurisdiction_key and jurisdiction_key in JURISDICTIONS:
        county = JURISDICTIONS[jurisdiction_key]["county_name"]
        where_clauses.append(f"county_desc='{county}'")
    if years:
        year_list = ",".join(str(y) for y in years)
        where_clauses.append(f"year in({year_list})")

    where = " AND ".join(where_clauses) if where_clauses else "1=1"
    log.info(f"  Filter: {where}")

    all_records = []
    fieldnames = None
    offset = 0
    start_time = time.time()
    consecutive_errors = 0

    while True:
        params = {
            "$where": where,
            "$limit": batch_size,
            "$offset": offset,
            "$order": ":id",
        }

        pct_label = f"{offset:,}+" if not all_records else f"{len(all_records):,}"
        elapsed = time.time() - start_time
        rate = len(all_records) / max(elapsed, 0.1)
        log.info(
            f"  Batch at offset {offset:,} | {pct_label} total"
            f" | {rate:,.0f} rec/s"
        )

        try:
            resp = retry_request(SOCRATA_SODA_CSV_URL, params=params)
        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors >= 5:
                log.error(f"  5 consecutive failures — aborting at {len(all_records):,} records")
                break
            log.warning(f"  Request failed: {e}, retrying...")
            time.sleep(2 ** consecutive_errors)
            continue

        raw_text = resp.text
        if not raw_text.strip():
            log.info(f"  Empty response at offset {offset:,} — done")
            break

        reader = csv.DictReader(io.StringIO(raw_text))
        batch = list(reader)

        if not batch:
            log.info(f"  Zero records at offset {offset:,} — done")
            break

        if fieldnames is None:
            fieldnames = reader.fieldnames or []
            log.info(f"  Columns ({len(fieldnames)}): {fieldnames[:8]}...")

        all_records.extend(batch)
        consecutive_errors = 0
        offset += len(batch)

        # If we got fewer than batch_size, we've reached the end
        if len(batch) < batch_size:
            log.info(f"  Last batch: {len(batch):,} records (< {batch_size:,}) — done")
            break

    elapsed = time.time() - start_time
    log.info(
        f"  SODA2 pagination complete: {len(all_records):,} records"
        f" in {elapsed:.1f}s ({len(all_records)/max(elapsed,1):,.0f} rec/s)"
    )

    if not all_records:
        raise RuntimeError("SODA2 paginated download returned 0 records")

    return all_records, fieldnames or []


# =============================================================================
# Strategy 3: Playwright Browser-Routed FeatureServer Pagination (Fallback #2)
# =============================================================================

def load_cached_url():
    """Load cached FeatureServer URL from disk."""
    if CACHE_PATH.exists():
        try:
            data = json.loads(CACHE_PATH.read_text())
            url = data.get("url", "")
            if url:
                log.info(f"  Cache: {url}")
                return url
        except Exception:
            pass
    return None


def save_cached_url(url):
    """Cache discovered FeatureServer URL."""
    try:
        CACHE_PATH.write_text(json.dumps({
            "url": url,
            "discovered": datetime.now().isoformat(),
            "state": "delaware",
        }, indent=2))
    except Exception:
        pass


def browser_fetch_json(page, url, retries=5, timeout_ms=30000):
    """
    Execute fetch() inside the browser context.
    Inherits browser TLS fingerprint, cookies, and headers →
    bypasses bot detection that blocks Python requests.
    """
    for attempt in range(retries):
        try:
            result = page.evaluate(f"""
                async () => {{
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(), {timeout_ms});
                    try {{
                        const resp = await fetch("{url}", {{ signal: controller.signal }});
                        clearTimeout(timer);
                        if (!resp.ok) return {{ error: 'HTTP ' + resp.status }};
                        const text = await resp.text();
                        try {{
                            return JSON.parse(text);
                        }} catch(pe) {{
                            return {{ error: 'JSON parse: ' + pe.message, raw: text.substring(0, 200) }};
                        }}
                    }} catch(e) {{
                        clearTimeout(timer);
                        return {{ error: e.name === 'AbortError' ? 'Timeout' : e.message }};
                    }}
                }}
            """)

            if result and "error" not in result:
                return result

            err = result.get("error", "unknown") if result else "null"
            if attempt < retries - 1:
                wait = min(20, 2 ** attempt)
                log.debug(f"    Retry {attempt + 1}/{retries} in {wait}s ({err})")
                time.sleep(wait)
            else:
                log.error(f"    Failed after {retries} attempts: {err}")
                return None

        except Exception as e:
            if attempt < retries - 1:
                wait = min(20, 2 ** attempt)
                log.debug(f"    Exception retry {attempt + 1}: {e}")
                time.sleep(wait)
            else:
                log.error(f"    Exception after {retries} attempts: {e}")
                return None
    return None


def discover_featureserver(page):
    """
    Multi-strategy FeatureServer URL discovery for Delaware:
      1. Cached URL (verified via browser fetch count query)
      2. Network interception from Hub download flow
      3. Known URLs from KNOWN_FEATURE_SERVERS fallback list
    """

    service_url = None
    total_count = 0

    # --- Strategy A: Cached URL ---
    cached = load_cached_url()
    if cached:
        count_url = f"{cached}/query?where=1%3D1&returnCountOnly=true&f=json"
        data = browser_fetch_json(page, count_url, retries=2)
        if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
            log.info(f"  ✓ Cache valid: {data['count']:,} records")
            return cached, data["count"]
        else:
            log.info("  ✗ Cache stale, re-discovering...")

    # --- Strategy B: Network traffic interception ---
    discovered_urls = []

    def on_request(request):
        url = request.url
        # Catch any FeatureServer, MapServer, or replica traffic
        if any(kw in url for kw in [
            "FeatureServer", "MapServer", "replicafilescache",
            "DE_ODP_CRASH", "CRASH_DATA", "crash_data",
            "firstmaptest.delaware.gov", "firstmap.delaware.gov",
        ]):
            discovered_urls.append(url)

    page.on("request", on_request)

    log.info("  Triggering download flow to capture FeatureServer URL...")

    # Click the sidebar Download icon (text-walker pattern for Shadow DOM)
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
                    (tag === 'a' || tag === 'button' ||
                     tag === 'calcite-action' || tag === 'calcite-button')) {
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

    # Click the CSV download button to trigger FeatureServer network traffic
    try:
        btns = page.locator(
            'a:has-text("Download"), button:has-text("Download"), '
            'calcite-button:has-text("Download")'
        ).all()
        for btn in btns:
            try:
                if not btn.is_visible(timeout=1000):
                    continue
                text = btn.inner_text().strip()
                bbox = btn.bounding_box()
                if "Recent" in text or (bbox and bbox["y"] < 200):
                    continue
                btn.click()
                page.wait_for_timeout(5000)
                break
            except Exception:
                continue
    except Exception:
        pass

    # Extract FeatureServer URL from captured network traffic
    # Delaware self-hosts (firstmaptest.delaware.gov), so we match ANY domain
    if discovered_urls:
        log.info(f"  Captured {len(discovered_urls)} FeatureServer URL(s) from traffic")
        for u in discovered_urls[:5]:
            log.info(f"    → {u[:120]}...")

    for url in discovered_urls:
        # Generic: match any FeatureServer URL ending in /FeatureServer or /FeatureServer/N
        match = re.search(
            r"(https?://[^/]+(?:/[^?#]+)?/FeatureServer)(?:/(\d+))?",
            url,
        )
        if match:
            base = match.group(1)
            layer = match.group(2) or "0"
            candidate = f"{base}/{layer}"
            # Skip metadata/info/replica URLs — we want the query endpoint
            if any(skip in url for skip in ["replicafilescache", "/info/", "/metadata"]):
                continue
            count_url = f"{candidate}/query?where=1%3D1&returnCountOnly=true&f=json"
            data = browser_fetch_json(page, count_url, retries=3)
            if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
                save_cached_url(candidate)
                log.info(f"  ✓ Discovered: {candidate} ({data['count']:,} records)")
                return candidate, data["count"]
            elif data and "count" in data:
                log.info(f"    {candidate}: only {data['count']:,} records (need {MIN_RECORD_COUNT:,}+)")

    # Check replicafilescache URLs for service name hints
    for url in discovered_urls:
        match = re.search(r"rest/services/([^/]+/[^/]+)/FeatureServer", url)
        if not match:
            match = re.search(r"rest/services/([^/]+)/FeatureServer", url)
        if match:
            svc_path = match.group(1)
            # Reconstruct on known Delaware server bases
            for base in [
                "https://apps.firstmaptest.delaware.gov/apps/rest/services",
                "https://enterprise.firstmaptest.delaware.gov/arcgis/rest/services",
            ]:
                candidate = f"{base}/{svc_path}/FeatureServer/0"
                count_url = f"{candidate}/query?where=1%3D1&returnCountOnly=true&f=json"
                data = browser_fetch_json(page, count_url, retries=2)
                if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
                    save_cached_url(candidate)
                    log.info(f"  ✓ Discovered: {candidate} ({data['count']:,} records)")
                    return candidate, data["count"]

    page.remove_listener("request", on_request)

    # --- Strategy B2: Query ArcGIS item metadata API from browser ---
    # The Hub page references an ArcGIS Online item. The item metadata
    # contains the actual FeatureServer URL in the 'url' field.
    log.info("  Querying ArcGIS item metadata for service URL...")
    item_url = page.evaluate("""
        () => {
            // Extract item ID from the page URL or DOM
            // Hub URL pattern: /datasets/{org}::{slug}/explore
            // Item ID may be in data attributes or meta tags
            const url = window.location.href;

            // Try to find item ID in meta tags
            const meta = document.querySelector('meta[name="item-id"], meta[property="dc:identifier"]');
            if (meta) return meta.content;

            // Try to find item ID in script data / JSON-LD
            const scripts = document.querySelectorAll('script[type="application/json"], script[type="application/ld+json"]');
            for (const s of scripts) {
                try {
                    const d = JSON.parse(s.textContent);
                    if (d.id && d.id.length === 32) return d.id;
                    if (d.itemId && d.itemId.length === 32) return d.itemId;
                } catch(e) {}
            }

            // Try __NEXT_DATA__ (common in Hub v3 pages)
            const nd = document.querySelector('#__NEXT_DATA__');
            if (nd) {
                try {
                    const d = JSON.parse(nd.textContent);
                    // Walk the tree looking for item ID or service URL
                    const json = JSON.stringify(d);
                    const fsMatch = json.match(/https?:[^"]+FeatureServer[^"]*/);
                    if (fsMatch) return 'URL:' + fsMatch[0].replace(/\\\\/g, '');
                    const idMatch = json.match(/"id":"([a-f0-9]{32})"/);
                    if (idMatch) return idMatch[1];
                } catch(e) {}
            }

            // Try hub-dataset components
            const hubDs = document.querySelector('[data-item-id]');
            if (hubDs) return hubDs.getAttribute('data-item-id');

            // Last resort: extract from page URL if it contains the item hash
            const hashMatch = url.match(/([a-f0-9]{32})/);
            if (hashMatch) return hashMatch[1];

            return null;
        }
    """)

    if item_url and item_url.startswith('URL:'):
        # Direct FeatureServer URL found in __NEXT_DATA__
        raw_url = item_url[4:]
        log.info(f"  Found FeatureServer URL in page data: {raw_url[:120]}")
        match = re.search(r"(https?://[^\"'\\s]+/FeatureServer)(?:/(\d+))?", raw_url)
        if match:
            candidate = f"{match.group(1)}/{match.group(2) or '0'}"
            count_url = f"{candidate}/query?where=1%3D1&returnCountOnly=true&f=json"
            data = browser_fetch_json(page, count_url, retries=3)
            if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
                save_cached_url(candidate)
                log.info(f"  ✓ Discovered: {candidate} ({data['count']:,} records)")
                return candidate, data["count"]

    # Resolve item ID: prefer DOM-extracted, fall back to known constant
    item_id = None
    if item_url and not item_url.startswith("URL:") and len(item_url) == 32:
        item_id = item_url
        log.info(f"  Extracted item ID from DOM: {item_id}")
    else:
        item_id = ARCGIS_ITEM_ID
        log.info(f"  Using known item ID: {item_id}")

    # Query the ArcGIS sharing REST API for the actual service URL
    log.info(f"  Querying ArcGIS item API...")
    item_meta = browser_fetch_json(
        page,
        f"https://www.arcgis.com/sharing/rest/content/items/{item_id}?f=json",
        retries=3,
    )
    if item_meta:
        svc_url = item_meta.get("url", "")
        item_type = item_meta.get("type", "unknown")
        org_id = item_meta.get("orgId", "")
        svc_name = item_meta.get("name", "")
        log.info(f"  Item type: {item_type}, orgId: {org_id}, name: {svc_name}")
        if svc_url:
            log.info(f"  Service URL: {svc_url}")

        # Try the service URL and variants (FeatureServer, MapServer→FeatureServer)
        url_candidates = []
        if svc_url:
            url_candidates.append(svc_url)
            if "MapServer" in svc_url:
                url_candidates.append(svc_url.replace("MapServer", "FeatureServer"))

        # Also try constructing from orgId + name on ArcGIS Online
        if org_id:
            names_to_try = []
            if svc_name:
                names_to_try.append(svc_name.replace(" ", "_"))
                if svc_name != svc_name.replace(" ", "_"):
                    names_to_try.append(svc_name)
            for svc_num in ["", "1", "2", "3"]:
                for name in names_to_try:
                    url_candidates.append(
                        f"https://services{svc_num}.arcgis.com/{org_id}"
                        f"/arcgis/rest/services/{name}/FeatureServer/0"
                    )

        # Test each candidate
        tested = set()
        for candidate_url in url_candidates:
            test_url = candidate_url.rstrip("/")
            if not re.search(r"/\d+$", test_url):
                test_url += "/0"
            if test_url in tested:
                continue
            tested.add(test_url)

            count_url = f"{test_url}/query?where=1%3D1&returnCountOnly=true&f=json"
            data = browser_fetch_json(page, count_url, retries=2)
            if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
                save_cached_url(test_url)
                log.info(f"  ✓ Discovered via item API: {test_url} ({data['count']:,} records)")
                return test_url, data["count"]
            elif data and "count" in data:
                log.info(f"    {test_url}: {data['count']:,} records (too few)")
            elif data and "error" in data:
                err = data.get("error", data.get("message", "unknown"))
                log.info(f"    {test_url}: error — {err}")
    else:
        log.info("  Item API returned no data")

    # --- Strategy B3: Scrape page HTML for any service URLs ---
    log.info("  Scraping full page HTML for service references...")
    page_urls = page.evaluate("""
        () => {
            const urls = new Set();
            const html = document.documentElement.innerHTML;
            // Match any rest/services/.../FeatureServer or MapServer URL
            const re = /https?:\\/\\/[^"'\\s<>]+(?:Feature|Map)Server[^"'\\s<>]*/g;
            let m;
            while ((m = re.exec(html)) !== null) {
                urls.add(m[0].replace(/&amp;/g, '&').replace(/\\\\\\//g, '/').replace(/\\\\/g, ''));
            }
            return [...urls];
        }
    """)
    if page_urls:
        log.info(f"  Found {len(page_urls)} service URL(s) in HTML:")
        for pu in page_urls[:5]:
            log.info(f"    → {pu[:120]}")
        for pu in page_urls:
            match = re.search(r"(https?://[^\"'\\s<>]+/FeatureServer)(?:/(\d+))?", pu)
            if match:
                candidate = f"{match.group(1)}/{match.group(2) or '0'}"
                count_url = f"{candidate}/query?where=1%3D1&returnCountOnly=true&f=json"
                data = browser_fetch_json(page, count_url, retries=3)
                if data and "count" in data and data["count"] >= MIN_RECORD_COUNT:
                    save_cached_url(candidate)
                    log.info(f"  ✓ Discovered from HTML: {candidate} ({data['count']:,} records)")
                    return candidate, data["count"]
    else:
        log.info("  No service URLs found in HTML")

    # --- Strategy C: Known URL fallback ---
    if KNOWN_FEATURE_SERVERS:
        log.info(f"  Trying {len(KNOWN_FEATURE_SERVERS)} known FeatureServer URL(s)...")
        for url in KNOWN_FEATURE_SERVERS:
            svc_name = url.split("/")[-3]
            count_url = f"{url}/query?where=1%3D1&returnCountOnly=true&f=json"
            data = browser_fetch_json(page, count_url, retries=3)
            if data and "count" in data:
                count = data["count"]
                log.info(f"    {svc_name}: {count:,} records")
                if count >= MIN_RECORD_COUNT:
                    save_cached_url(url)
                    return url, count
            else:
                log.info(f"    {svc_name}: unreachable")

    return None, 0


def download_via_playwright(output_path, headless=True, batch_size=5000):
    """
    Full Playwright browser-session download (Virginia pattern):
      1. Launch Chromium → load Hub page (establish session/cookies)
      2. Discover FeatureServer URL via network interception
      3. Fetch field schema via browser fetch()
      4. Paginate ALL records via browser fetch() (5K/batch)
      5. Stream results to CSV on disk
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright not installed. Run:")
        log.error("  pip install playwright && playwright install chromium")
        return False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            page = context.new_page()

            # ==============================================================
            # Step 1: Load Hub page → establish session
            # ==============================================================
            log.info("Step 1: Loading Delaware FirstMap Hub page...")
            try:
                page.goto(HUB_URL, wait_until="networkidle", timeout=60000)
            except Exception:
                log.warning("  networkidle timeout, trying domcontentloaded...")
                try:
                    page.goto(HUB_URL, wait_until="domcontentloaded", timeout=30000)
                except Exception as e:
                    log.error(f"  Could not load Hub page: {e}")
                    browser.close()
                    return False

            page.wait_for_timeout(3000)
            log.info(f"  Page loaded: {page.title()}")

            # ==============================================================
            # Step 2: Discover FeatureServer URL
            # ==============================================================
            log.info("Step 2: Discovering FeatureServer URL...")
            service_url, total_count = discover_featureserver(page)

            if not service_url:
                log.error("  No working FeatureServer found for Delaware")
                browser.close()
                return False

            log.info(f"  Service URL: {service_url}")
            log.info(f"  Total records: {total_count:,}")

            # ==============================================================
            # Step 3: Fetch field schema
            # ==============================================================
            log.info("Step 3: Fetching field schema...")
            schema_url = f"{service_url}?f=json"
            schema = browser_fetch_json(page, schema_url)

            if not schema or "fields" not in schema:
                log.error("  Could not get field schema")
                browser.close()
                return False

            fields = schema["fields"]
            has_geometry = "Point" in schema.get("geometryType", "")
            fieldnames = [f["name"] for f in fields]
            if has_geometry:
                fieldnames.extend(["x", "y"])

            log.info(f"  {len(fields)} fields | geometry={has_geometry}")
            log.info(f"  Fields: {', '.join(fieldnames[:10])}...")

            # ==============================================================
            # Step 4: Paginate ALL records via browser fetch()
            # ==============================================================
            log.info(f"Step 4: Downloading {total_count:,} records ({batch_size}/batch)...")

            temp_path = output_path + ".partial"
            offset = 0
            total_written = 0
            start_time = time.time()
            consecutive_errors = 0
            max_consecutive_errors = 10
            query_base = f"{service_url}/query"

            with open(temp_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()

                while offset < total_count + batch_size:
                    # Progress
                    pct = min(100, int(offset / max(total_count, 1) * 100))
                    elapsed = time.time() - start_time
                    rate = total_written / max(elapsed, 0.1)
                    eta_min = (total_count - total_written) / max(rate, 0.1) / 60

                    if offset % (batch_size * 5) == 0 or offset == 0:
                        log.info(
                            f"  {total_written:>10,} / {total_count:,} ({pct:>3}%)"
                            f" | {rate:,.0f} rec/s | ETA {eta_min:.1f} min"
                        )

                    # Build paginated query URL
                    params = (
                        f"?where=1%3D1"
                        f"&outFields=*"
                        f"&resultOffset={offset}"
                        f"&resultRecordCount={batch_size}"
                        f"&orderByFields=OBJECTID+ASC"
                        f"&f=json"
                    )
                    if has_geometry:
                        params += "&outSR=4326"

                    fetch_url = query_base + params
                    data = browser_fetch_json(page, fetch_url, retries=5)

                    if data is None:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            log.error(
                                f"  {max_consecutive_errors} consecutive errors — aborting"
                            )
                            break
                        # Refresh page to re-establish session
                        log.info("  Refreshing page to reset session...")
                        try:
                            page.goto(HUB_URL, wait_until="networkidle", timeout=60000)
                        except Exception:
                            page.goto(HUB_URL, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(3000)
                        continue

                    if "error" in data:
                        log.warning(f"  API error at offset {offset}: {data['error']}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            break
                        time.sleep(2)
                        continue

                    features = data.get("features", [])
                    if not features:
                        break

                    consecutive_errors = 0

                    # Stream batch to CSV
                    for feat in features:
                        row = feat.get("attributes", {})
                        geom = feat.get("geometry")
                        if geom and has_geometry:
                            row["x"] = geom.get("x", "")
                            row["y"] = geom.get("y", "")
                        writer.writerow(row)
                        total_written += 1

                    offset += len(features)
                    if len(features) < batch_size:
                        break

            browser.close()

            elapsed = time.time() - start_time
            log.info(f"  Downloaded {total_written:,} records in {elapsed/60:.1f} min")
            log.info(f"  Throughput: {total_written/max(elapsed, 1):,.0f} rec/s")

            if total_written == 0:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return False

            if total_written < total_count * 0.95:
                log.warning(
                    f"  Partial download: {total_written:,}/{total_count:,}"
                    f" ({total_written / total_count * 100:.1f}%)"
                )

            # Atomic file replacement (Windows file-lock safe)
            if os.path.exists(output_path):
                for attempt in range(5):
                    try:
                        os.remove(output_path)
                        break
                    except PermissionError:
                        if attempt < 4:
                            log.info(f"  File locked, retry in {attempt + 1}s...")
                            time.sleep(attempt + 1)
                        else:
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            alt = output_path.replace(".csv", f"_{ts}.csv")
                            shutil.move(temp_path, alt)
                            log.warning(f"  Old file locked — saved as: {alt}")
                            return True

            shutil.move(temp_path, output_path)
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log.info(f"  Saved: {output_path} ({size_mb:.1f} MB)")
            return True

    except Exception as e:
        log.error(f"  Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# CSV Validation
# =============================================================================

def validate_csv(path):
    """Validate downloaded CSV is crash data, not an error page."""
    try:
        if not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8-sig") as f:
            head = f.read(500)
        if "<html" in head.lower() or "Cannot fetch" in head:
            log.error("  Downloaded an error page, not crash data CSV")
            return False
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader)
        rows = sum(1 for _ in open(path, encoding="utf-8-sig")) - 1
        log.info(f"  ✓ Validated: {len(headers)} columns × {rows:,} rows")
        return True
    except Exception as e:
        log.error(f"  Validation error: {e}")
        return False


# =============================================================================
# Main — Orchestration: Socrata Primary → Playwright Fallback
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="CRASH LENS — Delaware Crash Data Downloader (Socrata + Playwright)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategies:
  auto       Try Socrata export → SODA2 paginated → Playwright (default)
  socrata    Socrata full CSV export only
  paginated  SODA2 paginated batch download ($limit/$offset)
  playwright ArcGIS Hub FeatureServer via Playwright only

Examples:
  %(prog)s --jurisdiction sussex
  %(prog)s --jurisdiction new_castle --years 2023 2024
  %(prog)s --jurisdiction statewide --force
  %(prog)s --strategy paginated --force
  %(prog)s --strategy playwright --headful
  %(prog)s --discover-only
  %(prog)s --health-check
        """,
    )
    parser.add_argument(
        "--jurisdiction", "-j", type=str, default=None,
        choices=VALID_JURISDICTIONS,
        help="County filter (default: statewide)",
    )
    parser.add_argument(
        "--years", "-y", type=int, nargs="+", default=None,
        help="Year(s) to download (e.g., --years 2023 2024)",
    )
    parser.add_argument(
        "--data-dir", "-d", type=str, default="data/DelawareDOT",
        help="Output directory (default: data/DelawareDOT)",
    )
    parser.add_argument(
        "--strategy", "-s", type=str, default="auto",
        choices=["auto", "socrata", "paginated", "playwright"],
        help="Download strategy (default: auto)",
    )
    parser.add_argument(
        "--gzip", "-g", action="store_true",
        help="Output gzip-compressed CSV for R2 storage",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download even if output exists",
    )
    parser.add_argument(
        "--headful", action="store_true",
        help="Show browser window (Playwright only)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50000,
        help="Records per batch (SODA2: 50000, Playwright: 5000)",
    )
    parser.add_argument(
        "--health-check", action="store_true",
        help="Test API connectivity and exit",
    )
    parser.add_argument(
        "--discover-only", action="store_true",
        help="Find FeatureServer URL and exit (Playwright)",
    )
    args = parser.parse_args()

    # --- Health check ---
    if args.health_check:
        sys.exit(0 if health_check() else 1)

    # --- Discover-only mode ---
    if args.discover_only:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log.error("Playwright not installed. Run:")
            log.error("  pip install playwright && playwright install chromium")
            sys.exit(1)

        log.info("Discover-only mode: finding FeatureServer URL...")
        headless = not args.headful
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            page = context.new_page()
            try:
                page.goto(HUB_URL, wait_until="networkidle", timeout=60000)
            except Exception:
                page.goto(HUB_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            url, count = discover_featureserver(page)
            browser.close()

            if url:
                log.info(f"  FeatureServer: {url}")
                log.info(f"  Records: {count:,}")
            else:
                log.error("  No FeatureServer found")
                sys.exit(1)
        return

    # --- Build output path ---
    jurisdiction_key = args.jurisdiction
    if jurisdiction_key == "statewide":
        jurisdiction_key = None

    jurisdiction_label = args.jurisdiction or "statewide"
    year_suffix = ""
    if args.years:
        if len(args.years) == 1:
            year_suffix = f"_{args.years[0]}"
        else:
            year_suffix = f"_{min(args.years)}-{max(args.years)}"

    output_filename = f"{jurisdiction_label}{year_suffix}_crashes.csv"
    output_path = Path(args.data_dir) / output_filename

    gz_path = Path(str(output_path) + ".gz")
    if not args.force and (output_path.exists() or gz_path.exists()):
        existing = gz_path if gz_path.exists() else output_path
        log.info(f"Output already exists: {existing}")
        log.info("Use --force to re-download")
        sys.exit(0)

    # --- Banner ---
    print()
    print("=" * 62)
    print("  CRASH LENS — Delaware Crash Data Download (v2)")
    print(f"  Strategy: {args.strategy}")
    print(f"  Output: {args.data_dir}")
    print(f"  Jurisdiction: {jurisdiction_label}")
    if args.years:
        print(f"  Years: {', '.join(str(y) for y in args.years)}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 62)
    print()

    start = time.time()
    records = None
    fieldnames = None
    playwright_csv_path = None
    strategy_used = None

    # ── Strategy dispatch ──

    if args.strategy in ("auto", "socrata"):
        log.info("Strategy 1: Socrata CSV export...")
        try:
            records, fieldnames = download_via_socrata(jurisdiction_key, args.years)
            if records and len(records) > 0:
                strategy_used = "socrata"
                log.info(f"  ✓ Socrata returned {len(records):,} records")
            else:
                log.warning("  Socrata returned 0 records")
                records = None
        except Exception as e:
            log.warning(f"  Socrata failed: {e}")
            records = None

    if records is None and args.strategy in ("auto", "paginated"):
        log.info("")
        log.info("Strategy 2: SODA2 paginated batch download...")
        try:
            records, fieldnames = download_via_soda2_paginated(
                jurisdiction_key, args.years, batch_size=args.batch_size,
            )
            if records and len(records) > 0:
                strategy_used = "soda2_paginated"
                log.info(f"  ✓ SODA2 paginated returned {len(records):,} records")
            else:
                log.warning("  SODA2 paginated returned 0 records")
                records = None
        except Exception as e:
            log.warning(f"  SODA2 paginated failed: {e}")
            records = None

    if records is None and args.strategy in ("auto", "playwright"):
        log.info("")
        log.info("Strategy 3: Playwright browser-routed FeatureServer...")
        headless = not args.headful
        statewide_path = str(Path(args.data_dir) / "statewide_all_crashes.csv")
        success = download_via_playwright(
            statewide_path,
            headless=headless,
            batch_size=min(args.batch_size, 10000),  # FeatureServer max ~10K
        )
        if success and os.path.exists(statewide_path):
            if not validate_csv(statewide_path):
                log.error("Playwright CSV validation failed")
                sys.exit(1)
            strategy_used = "playwright"
            playwright_csv_path = statewide_path
            # Load into memory for filtering
            with open(statewide_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                records = list(reader)
                fieldnames = reader.fieldnames
            log.info(f"  ✓ Playwright downloaded {len(records):,} records")
        else:
            log.error("Playwright download failed")
            sys.exit(1)

    if records is None:
        log.error("All download strategies failed")
        sys.exit(1)

    # ── Client-side filtering (safety net for full-export / statewide paths) ──
    if jurisdiction_key and strategy_used in ("playwright", "socrata"):
        records = filter_records(records, fieldnames, jurisdiction_key, args.years)

    if not records:
        log.warning("No records after filtering")
        sys.exit(1)

    # ── Save output ──
    saved = save_csv(records, fieldnames, output_path, gzip_output=args.gzip)

    elapsed = time.time() - start
    print()
    print("=" * 62)
    print("  DONE ✓")
    print(f"  Strategy: {strategy_used}")
    print(f"  Records: {len(records):,}")
    print(f"  Output: {saved}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print("=" * 62)
    print()


if __name__ == "__main__":
    main()
