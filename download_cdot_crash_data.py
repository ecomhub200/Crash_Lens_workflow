#!/usr/bin/env python3
"""
Download statewide crash data from CDOT's Hyland OnBase document management system,
filter to a specific Colorado county, and save as CSV.

The statewide Excel files are hosted at:
    https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx?clienttype=html&docid=<DOCID>

Doc IDs for each year are stored in data/CDOT/source_manifest.json. This file must be
updated manually when CDOT publishes new annual data (they assign arbitrary doc IDs).

Usage:
    python download_cdot_crash_data.py                          # Download all years for default county
    python download_cdot_crash_data.py --latest                 # Download most recent year only
    python download_cdot_crash_data.py --years 2023 2024        # Download specific years
    python download_cdot_crash_data.py --jurisdiction elpaso    # Download for El Paso County
    python download_cdot_crash_data.py --list                   # Show available years and counties
    python download_cdot_crash_data.py --no-dict                # Skip data dictionary downloads
    python download_cdot_crash_data.py --statewide              # Keep statewide file (don't filter)
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory
SCRIPT_DIR = Path(__file__).parent.resolve()

# Manifest path
MANIFEST_FILE = SCRIPT_DIR / 'data' / 'CDOT' / 'source_manifest.json'

# Default output directory
DEFAULT_DATA_DIR = SCRIPT_DIR / 'data' / 'CDOT'

# Retry settings (matches existing downloaders)
MAX_RETRIES = 4
RETRY_BACKOFF_FACTOR = 2  # 2s, 4s, 8s, 16s

# OnBase base URL
ONBASE_BASE_URL = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx'

# OnBase direct retrieval endpoints (bypass HTML viewer)
ONBASE_GETDOC_URLS = [
    'https://oitco.hylandcloud.com/CDOTRMPop/docpop/GetDoc.aspx',
    'https://oitco.hylandcloud.com/CDOTRMPop/docpop/PdfPop.aspx',
    'https://oitco.hylandcloud.com/CDOTRMPop/api/document',
]

# Realistic browser headers to avoid being blocked
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Minimum file size to consider a download valid (100 KB)
MIN_VALID_FILE_SIZE = 100 * 1024

# Excel file magic bytes
XLSX_MAGIC = b'PK'  # ZIP-based format (xlsx)
XLS_MAGIC = b'\xd0\xcf'  # OLE2 format (xls)


def create_session_with_retries():
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(BROWSER_HEADERS)
    return session


def bootstrap_session(session):
    """
    Visit the OnBase DocPop landing page to establish session cookies.
    Many document management systems require a valid session before serving
    individual documents. Without this step, direct document requests
    return 403 Forbidden.
    """
    landing_url = ONBASE_BASE_URL
    logger.info("Bootstrapping OnBase session (visiting DocPop landing page)...")
    try:
        resp = session.get(landing_url, timeout=60, allow_redirects=True)
        # Accept any 2xx/3xx — we just need the cookies, not the content
        logger.info(f"  Session bootstrap: HTTP {resp.status_code}, "
                     f"cookies={list(session.cookies.keys())}")
        # Follow any meta-refresh or JS redirects embedded in the HTML
        if resp.status_code == 200:
            html = resp.text[:4000].lower()
            meta_match = re.search(
                r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\'][\d;]*\s*url=([^"\'>\s]+)',
                html
            )
            if meta_match:
                redirect_url = meta_match.group(1)
                if not redirect_url.startswith('http'):
                    from urllib.parse import urljoin
                    redirect_url = urljoin(landing_url, redirect_url)
                logger.info(f"  Following meta-refresh to {redirect_url[:80]}...")
                session.get(redirect_url, timeout=60, allow_redirects=True)
        return True
    except Exception as e:
        logger.warning(f"  Session bootstrap failed (non-fatal): {e}")
        return False


def make_request_with_retry(session, url, params=None, timeout=120, max_manual_retries=4,
                            stream=False):
    """
    Make HTTP request with manual retry logic for network errors.
    Uses exponential backoff: 2s, 4s, 8s, 16s
    """
    last_exception = None

    for attempt in range(max_manual_retries):
        try:
            response = session.get(url, params=params, timeout=timeout,
                                   allow_redirects=True, stream=stream)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout as e:
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"Request timeout (attempt {attempt + 1}/{max_manual_retries}). "
                           f"Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"Connection error (attempt {attempt + 1}/{max_manual_retries}). "
                           f"Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                raise
            last_exception = e
            wait_time = RETRY_BACKOFF_FACTOR ** (attempt + 1)
            logger.warning(f"HTTP error (attempt {attempt + 1}/{max_manual_retries}). "
                           f"Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise last_exception or Exception("Request failed after all retries")


def load_manifest(manifest_path=None):
    """Load the source manifest with doc IDs and configuration."""
    path = Path(manifest_path) if manifest_path else MANIFEST_FILE
    if not path.exists():
        logger.error(f"Manifest file not found: {path}")
        logger.info("Expected location: data/CDOT/source_manifest.json")
        sys.exit(1)

    with open(path, 'r') as f:
        manifest = json.load(f)

    year_count = len(manifest.get('files', {}))
    county_count = len(manifest.get('jurisdiction_filters', {}))
    logger.info(f"Loaded manifest: {year_count} years, {county_count} jurisdictions")
    return manifest


def detect_file_type(content, content_type='', content_disposition=''):
    """
    Detect if downloaded content is actually an Excel file (not HTML error page).
    Returns (is_valid, extension, reason).
    """
    # Check magic bytes
    if content[:2] == XLSX_MAGIC:
        return True, '.xlsx', 'XLSX (ZIP) magic bytes detected'
    if content[:2] == XLS_MAGIC:
        return True, '.xls', 'XLS (OLE2) magic bytes detected'

    # Check if it's HTML (error page or login redirect)
    if content[:500].strip().lower().startswith((b'<!doctype', b'<html', b'<?xml')):
        return False, '.html', 'Response is HTML (likely error page or login redirect)'

    # Check Content-Type header
    ct = content_type.lower()
    if 'spreadsheet' in ct or 'excel' in ct or 'octet-stream' in ct:
        return True, '.xlsx', f'Content-Type suggests binary: {content_type}'

    # Check Content-Disposition for filename
    if content_disposition:
        match = re.search(r'filename[*]?=["\']?([^"\';\s]+)', content_disposition)
        if match:
            filename = match.group(1)
            if filename.endswith('.xlsx'):
                return True, '.xlsx', f'Filename from header: {filename}'
            if filename.endswith('.xls'):
                return True, '.xls', f'Filename from header: {filename}'

    # Default: check file size (Excel files are typically > 100KB)
    if len(content) > MIN_VALID_FILE_SIZE:
        return True, '.xlsx', f'Large binary file ({len(content):,} bytes), assuming xlsx'

    return False, '.unknown', f'Cannot determine file type ({len(content):,} bytes)'


def extract_download_url_from_html(html_content, base_url):
    """
    If OnBase returns an HTML viewer page instead of the file directly,
    parse it to find the actual document download URL.
    """
    html_str = (html_content.decode('utf-8', errors='ignore')
                if isinstance(html_content, bytes) else html_content)

    # Look for common OnBase download URL patterns
    patterns = [
        r'href=["\']([^"\']*PdfPop\.aspx[^"\']*)["\']',
        r'href=["\']([^"\']*GetDoc[^"\']*)["\']',
        r'href=["\']([^"\']*Download[^"\']*)["\']',
        r'src=["\']([^"\']*\.xlsx?[^"\']*)["\']',
        r'window\.location\s*=\s*["\']([^"\']+)["\']',
        r'window\.open\(["\']([^"\']+)["\']',
        # OnBase native-document retrieval patterns (viewer pages)
        r'href=["\']([^"\']*RetrieveNativeDocument[^"\']*)["\']',
        r'href=["\']([^"\']*GetNativeDoc[^"\']*)["\']',
        r'href=["\']([^"\']*SendToApplication[^"\']*)["\']',
        r'href=["\']([^"\']*RenderDocument[^"\']*)["\']',
        r'href=["\']([^"\']*docid=\d+[^"\']*\.(?:xlsx?|csv)[^"\']*)["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, html_str, re.IGNORECASE)
        if match:
            url = match.group(1)
            # Handle relative URLs
            if url.startswith('/'):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
            elif not url.startswith('http'):
                url = base_url.rsplit('/', 1)[0] + '/' + url
            return url

    return None


def extract_obtoken_url(html_content, base_url):
    """
    Extract the ViewDocumentEx.aspx?OBToken=... URL from an OnBase DocPop HTML
    response.

    When docpop.aspx processes a document request the server generates a
    session-scoped OBToken GUID and embeds it in the frameset, iframe src, or
    inline JavaScript.  ViewDocumentEx.aspx requires this token (along with the
    matching ASP.NET_SessionId cookie) to serve the actual file content.

    Returns the absolute URL string, or None if no OBToken was found.
    """
    html_str = (html_content.decode('utf-8', errors='ignore')
                if isinstance(html_content, bytes) else html_content)

    from urllib.parse import urljoin

    # Patterns ordered from most specific (frame/iframe src) to broadest
    # (standalone GUID).  We stop at the first match.
    patterns = [
        # <frame src="ViewDocumentEx.aspx?OBToken=..."> or <iframe src="...">
        r'(?:frame|iframe)[^>]+src=["\']([^"\']*ViewDocumentEx[^"\']*OBToken=[^"\']+)["\']',
        # Any src= or href= containing OBToken
        r'(?:src|href)\s*=\s*["\']([^"\']*OBToken=[^"\']+)["\']',
        # JS: .src = "...OBToken=..."  or  location = "...OBToken=..."
        r'(?:\.src|location)\s*=\s*["\']([^"\']*OBToken=[^"\']+)["\']',
        # JS: window.open("...OBToken=...")
        r'window\.open\(\s*["\']([^"\']*OBToken=[^"\']+)["\']',
        # Quoted string with full ViewDocumentEx URL (catches JS variable assignments)
        r'["\']([^"\']*ViewDocumentEx\.aspx\?[^"\']*OBToken=[^"\']+)["\']',
        # Bare OBToken GUID anywhere in the page — we'll construct the URL
        r'OBToken=([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
    ]

    for pattern in patterns:
        match = re.search(pattern, html_str, re.IGNORECASE)
        if match:
            url = match.group(1)
            # Last pattern captures only the bare GUID — build the URL
            if re.fullmatch(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}', url):
                url = f'ViewDocumentEx.aspx?OBToken={url}'
            # Resolve relative URLs against the base (docpop.aspx response URL)
            if not url.startswith('http'):
                url = urljoin(base_url, url)
            return url

    return None


def _extract_virtual_root(html_str):
    """
    Extract the __VirtualRoot variable from OnBase page JavaScript.

    OnBase pages set ``var __VirtualRoot="https://…/cdotrmpop"`` which is
    the application root (one directory above /docpop/).  ViewDocumentEx.aspx
    lives directly under this root, not under /docpop/.
    """
    match = re.search(r'__VirtualRoot\s*=\s*["\']([^"\']+)["\']', html_str)
    return match.group(1).rstrip('/') if match else None


def _extract_guids_from_html(html_str):
    """
    Find all GUID-formatted strings in the HTML page.

    The OBToken is a GUID embedded in inline JavaScript (e.g. as a variable
    value) but not necessarily in a URL string.  This function extracts all
    candidate GUIDs so we can try each one as a potential OBToken.
    """
    guid_pattern = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
    return list(dict.fromkeys(re.findall(guid_pattern, html_str)))  # unique, preserve order


def _extract_onbase_keys(html_str):
    """
    Extract underscore-separated hex strings from OnBase HTML.

    The ``k`` parameter in ViewDocumentEx URLs uses the format
    ``xxxxxxxx_xxxx_xxxx_xxxx_`` (hex with underscores).  This is a
    validation key generated server-side.
    """
    pattern = r'[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_'
    return list(dict.fromkeys(re.findall(pattern, html_str)))


def build_obtoken_candidates(html_content, base_url, docid):
    """
    Build candidate ViewDocumentEx URLs when :func:`extract_obtoken_url`
    cannot find a direct ``OBToken=…`` pattern.

    OnBase DocPop renders an ASP.NET frameset where the iframe src starts as
    ``blank.aspx`` and is then set by JavaScript to a ViewDocumentEx URL.
    Because `requests` does not execute JavaScript, we must extract the
    OBToken GUID, dochandle, and k values from the raw HTML and reconstruct
    the URL ourselves.

    Returns a list of candidate URLs to try, ordered by likelihood.
    """
    html_str = (html_content.decode('utf-8', errors='ignore')
                if isinstance(html_content, bytes) else html_content)

    # --- Determine the application root ---
    virtual_root = _extract_virtual_root(html_str)
    if not virtual_root:
        # Derive from base_url: …/CDOTRMPop/docpop/docpop.aspx → …/CDOTRMPop
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        path = parsed.path
        docpop_idx = path.lower().find('/docpop/')
        if docpop_idx >= 0:
            path = path[:docpop_idx]
        virtual_root = f"{parsed.scheme}://{parsed.netloc}{path}"

    # --- Extract GUID candidates ---
    guids = _extract_guids_from_html(html_str)
    if not guids:
        return []

    # --- Extract underscore-hex keys (potential k parameter) ---
    keys = _extract_onbase_keys(html_str)

    logger.info(f"  [GUID scan] Found {len(guids)} GUIDs, {len(keys)} k-keys "
                f"in {len(html_str):,}-char HTML")
    for g in guids[:6]:
        logger.info(f"    GUID: {g}")
    for k in keys[:3]:
        logger.info(f"    k-key: {k}")

    candidates = []
    for guid in guids:
        # --- RetrieveNativeDocument first (returns binary directly) ---
        native_base = (f"{virtual_root}/RetrieveNativeDocument.ashx"
                       f"?OBToken={guid}&dochandle={docid}")
        if keys:
            candidates.append(f"{native_base}&k={keys[0]}")
        candidates.append(native_base)

        # --- ViewDocumentEx (may return HTML viewer or binary) ---
        view_base = (f"{virtual_root}/ViewDocumentEx.aspx"
                     f"?OBToken={guid}&dochandle={docid}")
        if keys:
            candidates.append(f"{view_base}&k={keys[0]}")
        candidates.append(view_base)

    return candidates


def extract_viewer_binary_urls(html_content, base_url):
    """
    Parse a ViewDocumentEx HTML viewer page for URLs that serve the
    actual binary document content.

    OnBase renders an HTML viewer (using Infragistics controls) that makes
    AJAX calls to endpoints like ``Retrieve.ashx``, ``RenderPage.aspx``,
    ``GetDocumentContent.ashx`` etc. to load document pages/content.

    The viewer HTML also contains ``RetrieveNativeDocument.ashx`` URLs with
    query parameters like ``docid=`` or ``documentId=`` that return the
    original file directly.  These are the highest-value targets.
    """
    html_str = (html_content.decode('utf-8', errors='ignore')
                if isinstance(html_content, bytes) else html_content)

    from urllib.parse import urljoin, urlparse, parse_qs

    # Look for binary-serving endpoint patterns in the viewer HTML
    patterns = [
        # --- High-value: full URLs with query parameters (ashx + aspx) ---
        # Matches quoted strings containing .ashx?... or .aspx?... with
        # document-retrieval function names.  The query string often
        # carries docid / documentId / dochandle which makes the URL
        # self-contained.
        r'["\']([^"\']*(?:RetrieveNativeDocument|GetNativeDocument|'
        r'GetRawContent|GetDocument|RetrieveDocument|GetContent|'
        r'DocumentContent|GetDocumentContent|RenderDocument|'
        r'SendToApplication|FetchDocument|ImageViewer|'
        r'RetrieveNativeDoc|NativeDocRetrieval|'
        r'GetOriginalDocument|DownloadNativeFile)'
        r'\.(?:ashx|aspx)[^"\']*)["\']',

        # --- .ashx handlers (common for binary content in ASP.NET) ---
        r'["\']([^"\']*(?:Retrieve|GetDoc|DocumentContent|RenderPage|'
        r'FetchDocument|PageContent|SendTo|GetNativeDoc)[^"\']*\.ashx[^"\']*)["\']',
        # .aspx pages that serve content
        r'["\']([^"\']*(?:Retrieve|GetDoc|FetchDocument|RenderPage|'
        r'DocumentContent|GetNativeDocument|SendToApplication)[^"\']*\.aspx[^"\']*)["\']',

        # --- JS string assignments containing .ashx/.aspx paths ---
        # OnBase viewer JS may assign these as string literals used in
        # AJAX requests, e.g.  var url = "/CDOTRMPop/Retrieve.ashx?…"
        r'=\s*["\']([^"\']*\.ashx\?[^"\']*(?:doc|content|render)[^"\']*)["\']',

        # --- Generic download/content URLs ---
        r'["\']([^"\']*(?:download|retrieve|getdoc|fetchdoc|sendto|'
        r'nativedoc|docstream)[^"\']*)["\']',
    ]

    urls = []
    for pattern in patterns:
        for match in re.finditer(pattern, html_str, re.IGNORECASE):
            url = match.group(1)
            # Skip JavaScript function calls, CSS, and images
            if any(s in url.lower() for s in (
                'javascript:', 'function', '.css', '.gif', '.png',
                '.jpg', '.ico', '.svg', 'void(0)',
            )):
                continue
            # Skip very short matches that are likely just function names
            if len(url) < 8:
                continue
            if not url.startswith('http'):
                url = urljoin(base_url, url)
            if url not in urls:
                urls.append(url)

    # --- Construct RetrieveNativeDocument URL from ViewDocumentEx params ---
    # If we're on a ViewDocumentEx page, the OBToken and dochandle from the
    # URL can be used to build a direct RetrieveNativeDocument request.
    try:
        parsed = urlparse(base_url)
        qs = parse_qs(parsed.query)
        obtoken = qs.get('OBToken', [None])[0]
        dochandle = qs.get('dochandle', [None])[0]
        if obtoken and dochandle:
            # Build the retrieval URL at the same application root
            app_root = parsed.path
            docpop_idx = app_root.lower().find('/docpop/')
            viewdoc_idx = app_root.lower().find('/viewdocumentex')
            if docpop_idx >= 0:
                app_root = app_root[:docpop_idx]
            elif viewdoc_idx >= 0:
                app_root = app_root[:viewdoc_idx]
            root = f"{parsed.scheme}://{parsed.netloc}{app_root}"
            for endpoint in [
                f"{root}/RetrieveNativeDocument.ashx?OBToken={obtoken}&dochandle={dochandle}",
                f"{root}/docpop/RetrieveNativeDocument.ashx?OBToken={obtoken}&dochandle={dochandle}",
                f"{root}/GetRawContent.ashx?OBToken={obtoken}&dochandle={dochandle}",
            ]:
                if endpoint not in urls:
                    urls.insert(0, endpoint)  # highest priority
    except Exception:
        pass

    return urls


# ═══════════════════════════════════════════════════════════════════
# AUTO-DISCOVERY
# ═══════════════════════════════════════════════════════════════════

def discover_crash_listings(headless=True):
    """
    Browse OnBase DocPop to find crash listing documents.

    Scans the portal page for links containing docid parameters and
    table rows mentioning "crash listing", extracting doc IDs and year
    labels.  Returns a list of ``{doc_id, title, year}`` dicts.
    """
    if not _playwright_available():
        logger.warning("  Auto-discovery requires Playwright. Skipping.")
        return []

    from playwright.sync_api import sync_playwright

    logger.info("Auto-discovery: Browsing OnBase for crash listings...")
    found = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        try:
            page.goto(ONBASE_BASE_URL, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            for frame in page.frames:
                try:
                    links = frame.evaluate("""() => {
                        const results = [];
                        document.querySelectorAll('a[href*="docid"], a[href*="docId"]').forEach(a => {
                            const match = (a.href || '').match(/docid=(\\d+)/i);
                            if (match) results.push({
                                doc_id: match[1], title: a.textContent.trim()
                            });
                        });
                        document.querySelectorAll('tr').forEach(tr => {
                            const text = tr.textContent || '';
                            if (text.toLowerCase().includes('crash listing')) {
                                const idMatch = text.match(/(\\d{7,9})/);
                                if (idMatch) results.push({
                                    doc_id: idMatch[1], title: text.trim().substring(0, 200)
                                });
                            }
                        });
                        return results;
                    }""")
                    found.extend([l for l in links if l.get("doc_id")])
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"  Discovery failed: {e}")
        finally:
            browser.close()

    seen = set()
    unique = []
    for item in found:
        did = item.get("doc_id")
        if did and did not in seen:
            seen.add(did)
            year_match = re.search(r'20\d{2}', item.get("title", ""))
            item["year"] = year_match.group(0) if year_match else None
            unique.append(item)
            logger.info(f"  Found: docid={did} year={item['year']} "
                        f"title={item.get('title', '')[:80]}")
    return unique


def check_for_new_year(manifest, manifest_path):
    """
    Check if CDOT published new year data via auto-discovery.

    Compares discovered crash listings against known years in the manifest.
    If a new year is found, it's added as ``preliminary`` and the previous
    year is marked ``final`` (since CDOT only publishes the next year when
    the current one is finalized).

    Returns True if manifest was updated.
    """
    files = manifest.get("files", {})
    known_years = set(files.keys())
    logger.info(f"Checking for new data (known years: {', '.join(sorted(known_years))})")

    discovered = discover_crash_listings()
    updated = False
    for item in discovered:
        year = item.get("year")
        doc_id = item.get("doc_id")
        if year and doc_id and year not in known_years:
            logger.info(f"  New year discovered: {year} (docid={doc_id})")
            files[year] = {
                "docid": int(doc_id),
                "status": "preliminary",
                "note": f"Auto-discovered {datetime.now().isoformat()}",
            }
            # Finalize previous year — a newer one exists
            prev = str(int(year) - 1)
            if prev in files and files[prev].get("status") == "preliminary":
                files[prev]["status"] = "final"
                logger.info(f"  {prev} -> finalized (newer year {year} exists)")
            updated = True

    if updated:
        manifest["files"] = files
        manifest["_updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"  Manifest updated: {manifest_path}")
    else:
        logger.info("  No new years found via auto-discovery.")

    return updated


def _playwright_available():
    """Check if Playwright is installed and has browsers."""
    try:
        from playwright.sync_api import sync_playwright
        # Verify chromium binary is actually installed (not just the Python package)
        with sync_playwright() as p:
            browser_path = p.chromium.executable_path
            if browser_path and os.path.isfile(browser_path):
                return True
            logger.warning(f"  Playwright package installed but chromium binary not found at: {browser_path}")
            return False
    except ImportError:
        return False
    except Exception as e:
        logger.warning(f"  Playwright availability check failed: {e}")
        return False


def download_with_playwright_event(docid, label='document', timeout_sec=90):
    """
    Strategy 0 (primary): Playwright two-phase session + download event.

    OnBase's OleHandler.ashx fires a native browser download when the page
    is loaded with a valid session.  Instead of trying to extract OBTokens
    or parse viewer HTML, we let the browser handle it:

      1. Navigate to the base DocPop URL to establish an ASP.NET session
      2. Navigate to the docid URL — OleHandler.ashx triggers a download
      3. Capture the download via Playwright's download event handler

    This is the proven approach from the tested working script.

    Returns (content_bytes, extension) or raises on failure.
    """
    from playwright.sync_api import sync_playwright
    import tempfile

    url = f"{ONBASE_BASE_URL}?clienttype=html&docid={docid}"
    logger.info(f"  Strategy 0 (Playwright event): launching browser for docid={docid}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=ProtocolHandlerPermissionPrompt",
                "--disable-external-intent-requests",
            ],
        )
        try:
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

            captured = {}

            # Use a temporary file to save the download
            tmp_dir = tempfile.mkdtemp(prefix='onbase_dl_')
            output_path = os.path.join(tmp_dir, f'doc_{docid}')

            def on_download(download):
                logger.info(f"  Download event fired: {download.suggested_filename}")
                try:
                    download.save_as(output_path)
                    captured["ok"] = True
                    captured["filename"] = download.suggested_filename
                except Exception as e:
                    logger.error(f"  Download save failed: {e}")
                    captured["error"] = str(e)

            page.on("download", on_download)
            page.on("dialog", lambda d: d.accept())

            # Phase 1: Establish session by visiting the base URL
            logger.info(f"  Phase 1: establishing session at base URL...")
            page.goto(ONBASE_BASE_URL, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(1000)

            # Phase 2: Navigate to the document — OleHandler.ashx fires download
            logger.info(f"  Phase 2: navigating to docid={docid}...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Poll for download completion
            for _ in range(timeout_sec):
                if captured.get("ok"):
                    break
                page.wait_for_timeout(1000)

            browser.close()

            if captured.get("ok") and os.path.isfile(output_path):
                with open(output_path, 'rb') as f:
                    content = f.read()

                # Clean up temp file
                try:
                    os.remove(output_path)
                    os.rmdir(tmp_dir)
                except OSError:
                    pass

                if len(content) > 500:
                    is_valid, ext, reason = detect_file_type(content)
                    if is_valid:
                        logger.info(f"  Strategy 0 success: {len(content):,} bytes "
                                    f"({captured.get('filename', 'unknown')}, {reason})")
                        return content, ext
                    else:
                        logger.info(f"  Strategy 0: downloaded {len(content):,} bytes "
                                    f"but not valid: {reason}")
                        raise Exception(f"Downloaded file is not valid Excel: {reason}")
                else:
                    raise Exception(f"Downloaded file too small: {len(content)} bytes")

            error = captured.get("error", "no download event received")
            raise Exception(f"Strategy 0 failed: {error}")

        except Exception:
            try:
                browser.close()
            except Exception:
                pass
            raise


def download_with_playwright(docid, label='document', timeout_ms=60000):
    """
    Fallback: use a headless Chromium browser to download the file.
    Playwright handles JavaScript rendering, session cookies, and redirects
    that requests cannot follow.

    OnBase uses an ASP.NET frameset where the DocSelectPage iframe starts at
    blank.aspx and gets populated asynchronously by JavaScript.  We must:
      1. Intercept network responses carrying Excel/binary content
      2. Wait for the iframe to navigate away from blank.aspx
      3. Look for download triggers inside the iframe (not just the main page)

    Returns (content_bytes, extension) or raises on failure.
    """
    from playwright.sync_api import sync_playwright

    url = f"{ONBASE_BASE_URL}?clienttype=html&docid={docid}"
    logger.info(f"  Playwright fallback: launching headless browser...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # --- Network response interceptor ---
        # OnBase may serve the Excel file as a sub-request while the
        # DocSelectPage iframe loads.  Capture it before it disappears.
        captured_excel = {}

        def _on_response(response):
            if captured_excel:
                return  # already captured
            try:
                ct = (response.headers.get('content-type') or '').lower()
                cd = response.headers.get('content-disposition') or ''
                resp_url = response.url.lower()

                is_binary = ('octet-stream' in ct or 'spreadsheet' in ct or
                             'excel' in ct or 'ms-excel' in ct)
                has_file_url = any(k in resp_url for k in [
                    '.xlsx', '.xls', 'getdoc', 'retrievedoc',
                    'fetchdocument', 'renderpage',
                    # Additional OnBase stream endpoints
                    'getdocument', 'retrievedocument', 'documentstream',
                    'docstream', 'viewdoc', 'binarydata', 'getfile',
                ])
                has_file_disp = '.xls' in cd.lower()

                if is_binary or has_file_url or has_file_disp:
                    body = response.body()
                    if len(body) > 1000:  # skip tiny responses
                        is_valid, ext, reason = detect_file_type(body, ct, cd)
                        if is_valid:
                            captured_excel['data'] = body
                            captured_excel['ext'] = ext
                            captured_excel['reason'] = f'network intercept: {reason}'
                            logger.info(f"  [Intercept] Captured Excel from {response.url[:100]}")
            except Exception:
                pass

        page.on('response', _on_response)

        # Visit the DocPop landing page first to establish session cookies,
        # then navigate to the document.
        try:
            logger.info(f"  Playwright: visiting DocPop landing page for session...")
            page.goto(ONBASE_BASE_URL, wait_until='networkidle', timeout=30000)
            logger.info(f"  Playwright: session established, page title: {page.title()}")
        except Exception as e:
            logger.warning(f"  Playwright: landing page visit failed (continuing): {e}")

        downloaded_path = None

        try:
            # Navigate to the document page
            page.goto(url, wait_until='networkidle', timeout=timeout_ms)
            logger.info(f"  Page loaded: {page.title()}")

            # Check if network interceptor already captured the file
            if captured_excel:
                browser.close()
                logger.info(f"  Playwright success ({captured_excel['reason']})")
                return captured_excel['data'], captured_excel['ext']

            # --- Wait for DocSelectPage iframe to navigate ---
            # The iframe starts at blank.aspx and JS populates it.
            logger.info("  Waiting for OnBase DocSelectPage iframe to load...")
            doc_frame = None
            for _ in range(40):  # up to ~40 seconds
                for frame in page.frames:
                    furl = frame.url.lower()
                    if (frame != page.main_frame
                            and 'blank.aspx' not in furl
                            and 'unloadhandler' not in furl
                            and furl not in ('', 'about:blank')):
                        doc_frame = frame
                        break
                if doc_frame or captured_excel:
                    break
                page.wait_for_timeout(1000)

            if captured_excel:
                browser.close()
                logger.info(f"  Playwright success ({captured_excel['reason']})")
                return captured_excel['data'], captured_excel['ext']

            # --- Prepare list of frames to search for download triggers ---
            all_frames = [page] + ([doc_frame] if doc_frame else [])
            if doc_frame:
                logger.info(f"  DocSelectPage iframe navigated to: {doc_frame.url[:150]}")
                try:
                    doc_frame.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    pass
                if captured_excel:
                    browser.close()
                    logger.info(f"  Playwright success ({captured_excel['reason']})")
                    return captured_excel['data'], captured_excel['ext']
            else:
                logger.info("  DocSelectPage iframe did not navigate away from blank.aspx")

            # --- Click download triggers in all frames ---
            download_selectors = [
                'a[href*="GetDoc"]', 'a[href*="getdoc"]',
                'a[href*="Download"]', 'a[href*="download"]',
                'a[href*="Retrieve"]', 'a[href*="retrieve"]',
                'a[href*="PdfPop"]', 'a[href*="pdfpop"]',
                'a[href*=".xlsx"]', 'a[href*=".xls"]',
                '[id*="lnkRetrieve"]', '[id*="btnDownload"]',
                '[id*="btnRetrieve"]', '[id*="SendTo"]', '[id*="sendTo"]',
                'input[type="button"][value*="Download"]',
                'input[type="button"][value*="Retrieve"]',
                'input[type="submit"][value*="Download"]',
                'input[type="submit"][value*="Retrieve"]',
                '[title*="Send to"]', '[title*="Retrieve"]',
                '[title*="Download"]', '[title*="Save"]',
                'text=Download', 'text=Retrieve',
                'text=Open', 'text=Save',
                'text=Send to', 'text=Send To',
                # OnBase-specific button IDs
                '#SaveButton', '#btnSave', '#btnExport',
                # Title-based toolbar actions
                '[title="Save As"]', '[title="Export"]', '[title="Print"]',
                '[title*="export" i]',
                # Infragistics toolbar controls (OnBase UI framework)
                '.ig_Item', '.ig_ToolbarItem',
                '[class*="toolbar"] button', '[class*="toolbar"] a',
                # ARIA roles
                '[role="button"]', '[role="menuitem"]',
            ]

            for target_frame in all_frames:
                frame_label = 'main' if target_frame == page else 'iframe'
                for selector in download_selectors:
                    try:
                        el = target_frame.query_selector(selector)
                        if el and el.is_visible():
                            logger.info(f"  Clicking [{frame_label}]: {selector}")
                            try:
                                with page.expect_download(timeout=15000) as dl_info:
                                    el.click()
                                download = dl_info.value
                                downloaded_path = download.path()
                                logger.info(f"  Download captured: {download.suggested_filename}")
                                break
                            except Exception:
                                if captured_excel:
                                    browser.close()
                                    logger.info(f"  Playwright success ({captured_excel['reason']})")
                                    return captured_excel['data'], captured_excel['ext']
                    except Exception:
                        continue
                if downloaded_path:
                    break

            # --- Try fetching iframe URL directly (it may serve the file) ---
            viewer_html_for_extraction = None
            viewer_base_url = None
            if downloaded_path is None and doc_frame and doc_frame.url:
                frame_url = doc_frame.url
                if 'blank.aspx' not in frame_url.lower():
                    logger.info(f"  Trying direct fetch of iframe URL: {frame_url[:120]}")
                    try:
                        resp = page.request.get(frame_url)
                        body = resp.body()
                        is_valid, ext, reason = detect_file_type(body)
                        if is_valid:
                            browser.close()
                            logger.info(f"  Playwright success via iframe URL: {reason}")
                            return body, ext
                        # Save the viewer HTML for binary URL extraction below
                        if len(body) > 1000:
                            viewer_html_for_extraction = body
                            viewer_base_url = frame_url
                            logger.info(f"  Iframe returned HTML viewer ({len(body):,} bytes), "
                                        f"will extract binary URLs")
                    except Exception as e:
                        logger.info(f"  Direct iframe URL fetch failed: {e}")

            # --- Extract binary URLs from the viewer HTML page ---
            # When the iframe URL returns a ViewDocumentEx HTML viewer (not
            # the binary file), parse it for RetrieveNativeDocument.ashx and
            # other binary-serving endpoints, then fetch them using
            # Playwright's authenticated session (which has the correct
            # ASP.NET_SessionId cookie).
            if downloaded_path is None and viewer_html_for_extraction:
                binary_urls = extract_viewer_binary_urls(
                    viewer_html_for_extraction, viewer_base_url)
                if binary_urls:
                    logger.info(f"  Playwright: found {len(binary_urls)} binary "
                                f"endpoint(s) in viewer HTML")
                for bi, burl in enumerate(binary_urls[:8]):
                    logger.info(f"  Playwright binary [{bi+1}]: {burl[:140]}")
                    try:
                        resp = page.request.get(burl)
                        body = resp.body()
                        ct = (resp.headers.get('content-type') or '').lower()
                        cd = resp.headers.get('content-disposition') or ''
                        is_valid, ext, reason = detect_file_type(body, ct, cd)
                        logger.info(f"    Result: {reason} ({len(body):,} bytes)")
                        if is_valid:
                            browser.close()
                            logger.info(f"  Playwright success via viewer binary "
                                        f"endpoint: {reason}")
                            return body, ext
                    except Exception as e:
                        logger.info(f"    Failed: {e}")

            # --- JavaScript-based download trigger across all frames ---
            if downloaded_path is None:
                for target_frame in all_frames:
                    try:
                        with page.expect_download(timeout=10000) as dl_info:
                            target_frame.evaluate("""() => {
                                const links = document.querySelectorAll('a');
                                for (const a of links) {
                                    const href = (a.href || '').toLowerCase();
                                    if (href.includes('getdoc') || href.includes('download') ||
                                        href.includes('.xls') || href.includes('pdfpop') ||
                                        href.includes('retrieve')) {
                                        a.click();
                                        return true;
                                    }
                                }
                                const btn = document.querySelector(
                                    '[id*="download"], [id*="Download"], ' +
                                    '[id*="Retrieve"], [id*="retrieve"]'
                                );
                                if (btn) { btn.click(); return true; }
                                return false;
                            }""")
                        download = dl_info.value
                        downloaded_path = download.path()
                        logger.info(f"  JS-triggered download: {download.suggested_filename}")
                        break
                    except Exception:
                        continue

            # --- Keyboard shortcut (Ctrl+S) to trigger viewer save ---
            if downloaded_path is None and not captured_excel:
                for target_frame in all_frames:
                    try:
                        logger.info("  Trying Ctrl+S keyboard shortcut...")
                        target_frame.locator('body').first.focus()
                        with page.expect_download(timeout=8000) as dl_info:
                            page.keyboard.press('Control+s')
                        download = dl_info.value
                        downloaded_path = download.path()
                        logger.info(f"  Ctrl+S download: {download.suggested_filename}")
                        break
                    except Exception:
                        if captured_excel:
                            break
                        continue

            # --- Scan JS and DOM for hidden document URLs ---
            if downloaded_path is None and not captured_excel:
                logger.info("  Scanning page scripts and DOM for document URLs...")
                for target_frame in all_frames:
                    try:
                        discovered_urls = target_frame.evaluate("""() => {
                            const urls = new Set();
                            const scripts = document.querySelectorAll('script');
                            const urlPatterns = [
                                /['"](https?:\\/\\/[^'"]*(?:GetDoc|Download|Retrieve|Stream|Export|document)[^'"]*)['"]/gi,
                                /documentUrl\\s*[:=]\\s*['"]([^'"]+)['"]/gi,
                                /downloadUrl\\s*[:=]\\s*['"]([^'"]+)['"]/gi,
                                /fileUrl\\s*[:=]\\s*['"]([^'"]+)['"]/gi,
                                /blobUrl\\s*[:=]\\s*['"]([^'"]+)['"]/gi
                            ];
                            for (const script of scripts) {
                                const text = script.textContent || '';
                                for (const pattern of urlPatterns) {
                                    pattern.lastIndex = 0;
                                    let match;
                                    while ((match = pattern.exec(text)) !== null) {
                                        urls.add(match[1]);
                                    }
                                }
                            }
                            for (const tag of ['object', 'embed', 'iframe']) {
                                for (const el of document.querySelectorAll(tag)) {
                                    const src = el.getAttribute('data') || el.getAttribute('src') || '';
                                    if (src && (src.startsWith('blob:') ||
                                        /GetDoc|Download|Retrieve|Stream|document/i.test(src))) {
                                        urls.add(src);
                                    }
                                }
                            }
                            return Array.from(urls).slice(0, 10);
                        }""")
                    except Exception:
                        discovered_urls = []
                        continue

                    if discovered_urls:
                        logger.info(f"  Found {len(discovered_urls)} candidate URL(s) in page scripts")

                    for di, disc_url in enumerate(discovered_urls):
                        if disc_url.startswith('blob:'):
                            logger.info(f"    [{di+1}] Skipping blob URL: {disc_url[:80]}")
                            continue
                        logger.info(f"    [{di+1}] Trying: {disc_url[:120]}")
                        try:
                            resp = page.request.get(disc_url)
                            body = resp.body()
                            ct = (resp.headers.get('content-type') or '').lower()
                            cd = resp.headers.get('content-disposition') or ''
                            is_valid, ext, reason = detect_file_type(body, ct, cd)
                            if is_valid:
                                browser.close()
                                logger.info(f"  Playwright success via JS-discovered URL: {reason}")
                                return body, ext
                            else:
                                logger.info(f"    Not a valid file: {reason}")
                        except Exception as e:
                            logger.info(f"    Fetch failed: {e}")
                        if captured_excel:
                            break
                    if captured_excel:
                        break

            # --- Final check on network interceptor ---
            if captured_excel:
                browser.close()
                logger.info(f"  Playwright success ({captured_excel['reason']})")
                return captured_excel['data'], captured_excel['ext']

            # --- Try RetrieveNativeDocument using OBToken from iframe URL ---
            # The iframe URL contains the real OBToken that was generated
            # server-side after JS execution.  Use it to build direct
            # retrieval URLs and fetch with Playwright's authenticated
            # session cookies.
            if downloaded_path is None and doc_frame and doc_frame.url:
                from urllib.parse import urlparse, parse_qs
                try:
                    parsed_frame = urlparse(doc_frame.url)
                    qs = parse_qs(parsed_frame.query)
                    real_obtoken = qs.get('OBToken', [None])[0]
                    real_dochandle = qs.get('dochandle', [None])[0]
                    real_k = qs.get('k', [None])[0]

                    if real_obtoken:
                        logger.info(f"  Playwright: extracted real OBToken "
                                    f"from iframe: {real_obtoken}")
                        # Build application root from frame URL
                        fpath = parsed_frame.path
                        for marker in ('/ViewDocumentEx', '/docpop/', '/DocPop/'):
                            idx = fpath.lower().find(marker.lower())
                            if idx >= 0:
                                fpath = fpath[:idx]
                                break
                        app_root = (f"{parsed_frame.scheme}://"
                                    f"{parsed_frame.netloc}{fpath}")

                        native_urls = []
                        dh = real_dochandle or str(docid)
                        for ep in [
                            f"{app_root}/RetrieveNativeDocument.ashx"
                            f"?OBToken={real_obtoken}&dochandle={dh}",
                            f"{app_root}/docpop/RetrieveNativeDocument.ashx"
                            f"?OBToken={real_obtoken}&dochandle={dh}",
                            f"{app_root}/GetRawContent.ashx"
                            f"?OBToken={real_obtoken}&dochandle={dh}",
                            f"{app_root}/GetDocument.ashx"
                            f"?OBToken={real_obtoken}&dochandle={dh}",
                        ]:
                            if real_k:
                                native_urls.append(f"{ep}&k={real_k}")
                            native_urls.append(ep)

                        for ni, nurl in enumerate(native_urls):
                            logger.info(f"  Playwright native [{ni+1}]: "
                                        f"{nurl[:140]}")
                            try:
                                resp = page.request.get(nurl)
                                body = resp.body()
                                ct = (resp.headers.get('content-type')
                                      or '').lower()
                                cd = (resp.headers.get('content-disposition')
                                      or '')
                                is_valid, ext, reason = detect_file_type(
                                    body, ct, cd)
                                logger.info(f"    Result: {reason} "
                                            f"({len(body):,} bytes)")
                                if is_valid:
                                    browser.close()
                                    logger.info(
                                        f"  Playwright success via "
                                        f"RetrieveNativeDocument: {reason}")
                                    return body, ext
                            except Exception as e:
                                logger.info(f"    Failed: {e}")
                except Exception as e:
                    logger.info(f"  Playwright native doc extraction "
                                f"failed: {e}")

            # --- Final check on network interceptor (again) ---
            if captured_excel:
                browser.close()
                logger.info(f"  Playwright success ({captured_excel['reason']})")
                return captured_excel['data'], captured_excel['ext']

            # --- Diagnostic dump (only on failure) ---
            if downloaded_path is None:
                try:
                    page_html = page.content()
                    logger.info(f"  [Playwright diag] Page URL: {page.url}")
                    logger.info(f"  [Playwright diag] Page title: {page.title()}")
                    logger.info(f"  [Playwright diag] HTML length: {len(page_html):,} chars")
                    all_links = page.evaluate("""() => {
                        return Array.from(document.querySelectorAll('a')).slice(0, 20).map(a => ({
                            text: (a.textContent || '').trim().substring(0, 60),
                            href: (a.href || '').substring(0, 120),
                            visible: a.offsetParent !== null
                        }));
                    }""")
                    logger.info(f"  [Playwright diag] Links found: {len(all_links)}")
                    for lnk in all_links:
                        logger.info(f"    <a href='{lnk['href']}' visible={lnk['visible']}>{lnk['text']}</a>")
                    all_iframes = page.evaluate("""() => {
                        return Array.from(document.querySelectorAll('iframe')).map(f => ({
                            src: (f.src || '').substring(0, 150),
                            id: f.id || '',
                            name: f.name || ''
                        }));
                    }""")
                    logger.info(f"  [Playwright diag] Iframes: {len(all_iframes)}")
                    for ifr in all_iframes:
                        logger.info(f"    <iframe src='{ifr['src']}' id='{ifr['id']}' name='{ifr['name']}'/>")
                    # Dump iframe content too
                    for frame in page.frames:
                        if frame != page.main_frame:
                            try:
                                fc = frame.content()
                                logger.info(f"  [Playwright diag] Frame {frame.url[:80]} HTML ({len(fc)} chars):\n{fc[:1000]}")
                            except Exception:
                                pass
                    # Dump toolbar / Infragistics control structure
                    try:
                        toolbar_info = page.evaluate("""() => {
                            const selectors = [
                                '[class*="toolbar" i]', '[id*="toolbar" i]',
                                '.ig_Control', '[class*="ig_"]'
                            ];
                            const results = [];
                            for (const sel of selectors) {
                                try {
                                    const els = document.querySelectorAll(sel);
                                    for (const el of Array.from(els).slice(0, 5)) {
                                        results.push({
                                            tag: el.tagName,
                                            id: el.id || '',
                                            className: (el.className || '').toString().substring(0, 80),
                                            childCount: el.children.length,
                                            innerText: (el.innerText || '').substring(0, 60)
                                        });
                                    }
                                } catch(e) {}
                            }
                            return results;
                        }""")
                        if toolbar_info:
                            logger.info(f"  [Playwright diag] Toolbar/Infragistics controls: {len(toolbar_info)}")
                            for ti in toolbar_info:
                                logger.info(f"    <{ti['tag']} id='{ti['id']}' class='{ti['className']}' "
                                           f"children={ti['childCount']}>{ti['innerText']}</{ti['tag']}>")
                    except Exception:
                        pass
                    logger.info(f"  [Playwright diag] Main HTML preview:\n{page_html[:1500]}")
                except Exception as diag_err:
                    logger.warning(f"  [Playwright diag] Failed to dump page: {diag_err}")

            # Read the downloaded file
            if downloaded_path:
                with open(downloaded_path, 'rb') as f:
                    content = f.read()

                is_valid, ext, reason = detect_file_type(content)
                browser.close()

                if is_valid:
                    logger.info(f"  Playwright success: {len(content):,} bytes ({reason})")
                    return content, ext
                else:
                    raise Exception(f"Playwright downloaded file but it's not Excel: {reason}")

            browser.close()
            raise Exception("Playwright could not trigger a file download from OnBase")

        except Exception as e:
            browser.close()
            raise Exception(f"Playwright fallback failed: {e}")


def download_onbase_document(session, docid, label='document'):
    """
    Download a document from Hyland OnBase. Tries multiple strategies:
      0.  Playwright two-phase session + download event (primary, proven)
      1.  Direct requests with redirect following
      1b. Re-bootstrap session and retry (if strategy 1 got 403)
      1.5 Extract OBToken from HTML (A: explicit pattern, B: GUID scan
          from script blocks) → fetch ViewDocumentEx → parse viewer HTML
          for binary download endpoints
      1c. Direct document retrieval endpoints (GetDoc, PdfPop, /api)
      2.  Parse HTML response for real download URL
      3.  clienttype=activex parameter
      4.  Playwright headless browser (complex fallback)

    Returns (content_bytes, extension) or raises on failure.
    """
    url = ONBASE_BASE_URL
    params = {'clienttype': 'html', 'docid': str(docid)}

    logger.info(f"Downloading {label} (docid={docid})...")

    # --- Strategy 0: Playwright two-phase session + download event ---
    # This is the proven primary approach.  OnBase's OleHandler.ashx fires
    # a native browser download when the page loads with a valid session.
    # We try this FIRST because it works reliably where HTTP strategies fail.
    if _playwright_available():
        try:
            return download_with_playwright_event(docid, label=label)
        except Exception as e:
            logger.warning(f"  Strategy 0 failed: {e}")
            logger.info("  Falling back to HTTP-based strategies...")
    else:
        logger.info("  Playwright not available, using HTTP-based strategies")

    html_content = None  # Saved for strategy 2 if strategy 1 returns HTML
    response_url = None

    # --- Strategy 1: direct request with redirects ---
    try:
        response = make_request_with_retry(session, url, params=params, timeout=180)
        content = response.content
        content_type = response.headers.get('Content-Type', '')
        content_disposition = response.headers.get('Content-Disposition', '')

        is_valid, ext, reason = detect_file_type(content, content_type, content_disposition)
        logger.info(f"  Strategy 1 (requests): {reason}")
        logger.info(f"    Content-Type: {content_type}")
        logger.info(f"    Content-Disposition: {content_disposition}")
        logger.info(f"    Response URL: {response.url[:120]}")
        logger.info(f"    Response size: {len(content):,} bytes")

        if is_valid:
            logger.info(f"  Downloaded {len(content):,} bytes")
            return content, ext

        # Not a valid file — save HTML for strategy 2
        html_content = content
        response_url = response.url
        # Log first 500 chars of HTML for debugging
        if content[:500].strip().lower().startswith((b'<!doctype', b'<html', b'<?xml')):
            logger.debug(f"  HTML preview: {content[:500].decode('utf-8', errors='replace')}")

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 'unknown'
        logger.warning(f"  Strategy 1 failed: HTTP {status}")

        # --- Strategy 1b: re-bootstrap session and retry on 403 ---
        if e.response is not None and e.response.status_code == 403:
            logger.info("  Got 403 — re-bootstrapping session and retrying...")
            bootstrap_session(session)
            try:
                response = make_request_with_retry(session, url, params=params, timeout=180)
                content = response.content
                content_type = response.headers.get('Content-Type', '')
                content_disposition = response.headers.get('Content-Disposition', '')

                is_valid, ext, reason = detect_file_type(content, content_type, content_disposition)
                logger.info(f"  Strategy 1b (post-bootstrap): {reason}")

                if is_valid:
                    logger.info(f"  Downloaded {len(content):,} bytes")
                    return content, ext

                html_content = content
                response_url = response.url
            except Exception as e2:
                logger.warning(f"  Strategy 1b also failed: {e2}")
    except Exception as e:
        logger.warning(f"  Strategy 1 failed: {e}")

    # --- Strategy 1.5: extract OBToken and fetch ViewDocumentEx directly ---
    # OnBase embeds a session-scoped OBToken in the DocPop HTML (in the
    # frameset, iframe src, or inline JavaScript).  ViewDocumentEx.aspx
    # needs this token + the ASP.NET_SessionId cookie to serve content.
    #
    # Phase A: look for an explicit OBToken=GUID pattern in the HTML.
    # Phase B: if not found, extract bare GUIDs from <script> blocks and
    #          try each one as a candidate OBToken (the iframe src starts
    #          as blank.aspx and JS sets it dynamically — the GUID is in
    #          the JS vars, not in a URL string).
    if html_content is not None:
        # --- Phase A: direct OBToken pattern ---
        obtoken_url = extract_obtoken_url(html_content, response_url or url)
        candidate_urls = [obtoken_url] if obtoken_url else []

        # --- Phase B: broader GUID extraction from scripts ---
        if not candidate_urls:
            candidate_urls = build_obtoken_candidates(
                html_content, response_url or url, docid)

        if candidate_urls:
            logger.info(f"  Strategy 1.5: trying {len(candidate_urls)} "
                        f"candidate URL(s)...")
        else:
            logger.info("  Strategy 1.5: no OBToken or GUID candidates "
                        "found in HTML response")

        for ci, candidate_url in enumerate(candidate_urls):
            logger.info(f"  Strategy 1.5 [{ci+1}/{len(candidate_urls)}]: "
                        f"{candidate_url[:150]}")
            try:
                response = make_request_with_retry(
                    session, candidate_url, timeout=60,
                    max_manual_retries=1)
                content = response.content
                content_type = response.headers.get('Content-Type', '')
                content_disposition = response.headers.get(
                    'Content-Disposition', '')

                is_valid, ext, reason = detect_file_type(
                    content, content_type, content_disposition)
                logger.info(f"    Result: {reason}")
                logger.info(f"    Content-Type: {content_type}")
                logger.info(f"    Size: {len(content):,} bytes")

                if is_valid:
                    logger.info(f"  Downloaded {len(content):,} bytes "
                                f"via OBToken candidate {ci+1}")
                    return content, ext

                # ViewDocumentEx returned HTML (viewer) — dig into it for
                # the actual binary download URL.
                if len(content) > 50:
                    # Try standard download link patterns first
                    viewer_link = extract_download_url_from_html(
                        content, response.url)
                    # Then try OnBase-specific binary endpoints
                    binary_urls = extract_viewer_binary_urls(
                        content, response.url)

                    follow_urls = []
                    if viewer_link:
                        follow_urls.append(viewer_link)
                    follow_urls.extend(
                        u for u in binary_urls if u != viewer_link)

                    if follow_urls:
                        logger.info(f"    Viewer HTML returned. Found "
                                    f"{len(follow_urls)} sub-link(s)")

                    for fi, follow_url in enumerate(follow_urls[:10]):
                        logger.info(f"    Following sub-link {fi+1}: "
                                    f"{follow_url[:120]}")
                        try:
                            resp2 = make_request_with_retry(
                                session, follow_url, timeout=60,
                                max_manual_retries=1)
                            c2 = resp2.content
                            ct2 = resp2.headers.get('Content-Type', '')
                            cd2 = resp2.headers.get(
                                'Content-Disposition', '')
                            ok2, ext2, r2 = detect_file_type(c2, ct2, cd2)
                            logger.info(f"      Sub-link result: {r2}")
                            if ok2:
                                logger.info(
                                    f"  Downloaded {len(c2):,} bytes "
                                    f"via viewer sub-link")
                                return c2, ext2
                        except Exception as e2:
                            logger.info(f"      Sub-link failed: {e2}")
            except requests.exceptions.HTTPError as e:
                status = (e.response.status_code
                          if e.response is not None else '?')
                logger.info(f"    HTTP {status} — skipping candidate")
            except Exception as e:
                logger.info(f"    Failed: {e}")

    # --- Strategy 1c: try direct document retrieval endpoints ---
    for i, getdoc_url in enumerate(ONBASE_GETDOC_URLS):
        try:
            logger.info(f"  Strategy 1c-{i+1}: trying direct endpoint {getdoc_url}...")
            getdoc_params = {'docid': str(docid)}
            response = make_request_with_retry(session, getdoc_url, params=getdoc_params,
                                                timeout=180, max_manual_retries=2)
            content = response.content
            content_type = response.headers.get('Content-Type', '')
            content_disposition = response.headers.get('Content-Disposition', '')

            is_valid, ext, reason = detect_file_type(content, content_type, content_disposition)
            logger.info(f"  Strategy 1c-{i+1}: {reason}")

            if is_valid:
                logger.info(f"  Downloaded {len(content):,} bytes via direct endpoint")
                return content, ext
        except Exception as e:
            logger.info(f"  Strategy 1c-{i+1}: {e}")

    # --- Strategy 2: parse HTML for download URL ---
    if html_content is not None:
        logger.info("  Got HTML response. Parsing for download link...")
        try:
            download_url = extract_download_url_from_html(html_content, response_url or url)
            if download_url:
                logger.info(f"  Found download link: {download_url[:100]}...")
                response = make_request_with_retry(session, download_url, timeout=180)
                content = response.content
                content_type = response.headers.get('Content-Type', '')
                content_disposition = response.headers.get('Content-Disposition', '')

                is_valid, ext, reason = detect_file_type(content, content_type, content_disposition)
                logger.info(f"  Strategy 2 (HTML parse): {reason}")

                if is_valid:
                    logger.info(f"  Downloaded {len(content):,} bytes")
                    return content, ext
            else:
                logger.info("  Strategy 2: no download link found in HTML")
        except Exception as e:
            logger.warning(f"  Strategy 2 failed: {e}")

    # --- Strategy 3: alternative client types ---
    for ctype in ['activex', 'browser']:
        logger.info(f"  Trying clienttype={ctype}...")
        params_alt = {'clienttype': ctype, 'docid': str(docid)}
        try:
            response = make_request_with_retry(session, url, params=params_alt, timeout=180,
                                               max_manual_retries=2)
            content = response.content
            content_type = response.headers.get('Content-Type', '')
            content_disposition = response.headers.get('Content-Disposition', '')

            is_valid, ext, reason = detect_file_type(content, content_type, content_disposition)
            logger.info(f"  Strategy 3 ({ctype}): {reason}")

            if is_valid:
                logger.info(f"  Downloaded {len(content):,} bytes")
                return content, ext
        except Exception as e:
            logger.warning(f"  Strategy 3 ({ctype}) failed: {e}")

    # --- Strategy 4: Playwright headless browser ---
    playwright_status = 'not_installed'
    if _playwright_available():
        playwright_status = 'available'
        logger.info("  Falling back to Playwright headless browser...")
        try:
            return download_with_playwright(docid, label=label)
        except Exception as e:
            playwright_status = f'failed: {e}'
            logger.error(f"  Playwright fallback failed: {e}")
    else:
        logger.warning("  Playwright not installed. To enable browser fallback:")
        logger.warning("    pip install playwright && playwright install chromium")

    strategies_tried = "requests + direct-endpoint + HTML-parse + activex + browser"
    if playwright_status == 'not_installed':
        strategies_tried += " (Playwright NOT available — install it for headless browser fallback)"
    else:
        strategies_tried += f" + Playwright ({playwright_status})"

    raise Exception(
        f"Failed to download {label} (docid={docid}). "
        f"All strategies failed: {strategies_tried}"
    )


def excel_to_dataframe(content_bytes, ext):
    """Read Excel bytes into a pandas DataFrame."""
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas is required for Excel-to-CSV conversion. Install with: pip install pandas")
        sys.exit(1)

    try:
        import openpyxl  # noqa: F401 - needed by pandas for xlsx
    except ImportError:
        logger.error("openpyxl is required for reading .xlsx files. Install with: pip install openpyxl")
        sys.exit(1)

    buf = io.BytesIO(content_bytes)

    if ext == '.xls':
        # Older Excel format
        try:
            df = pd.read_excel(buf, engine='xlrd')
        except ImportError:
            logger.error("xlrd is required for reading .xls files. Install with: pip install xlrd")
            sys.exit(1)
    else:
        df = pd.read_excel(buf, engine='openpyxl')

    logger.info(f"  Parsed Excel: {len(df):,} rows x {len(df.columns)} columns")
    return df


def filter_to_jurisdiction(df, jurisdiction_config, jurisdiction_key):
    """
    Filter statewide DataFrame to a specific county.
    Uses the County column as primary filter.
    """
    county_name = jurisdiction_config['county']
    display_name = jurisdiction_config.get('display_name', county_name)

    original_count = len(df)

    # Find the County column (case-insensitive)
    county_col = None
    for col in df.columns:
        if col.strip().lower() == 'county':
            county_col = col
            break

    if county_col is None:
        logger.error(f"County column not found in data. Available columns: {list(df.columns)[:20]}")
        return df

    # Filter by county name (case-insensitive)
    mask = df[county_col].astype(str).str.strip().str.upper() == county_name.upper()
    df_filtered = df[mask].copy().reset_index(drop=True)

    logger.info(f"  Filtered: {original_count:,} statewide → {len(df_filtered):,} {display_name} records")

    if len(df_filtered) == 0:
        # Log unique county values for debugging
        unique_counties = sorted(df[county_col].dropna().astype(str).str.strip().unique())
        logger.warning(f"  No records matched! Unique counties in data ({len(unique_counties)} total):")
        for c in unique_counties[:15]:
            logger.warning(f"    {c}")
        if len(unique_counties) > 15:
            logger.warning(f"    ... and {len(unique_counties) - 15} more")

    return df_filtered


def _get_output_filename(year, jurisdiction_key, manifest):
    """Build the output CSV filename for a given year and jurisdiction."""
    jurisdiction_filters = manifest.get('jurisdiction_filters', {})
    if jurisdiction_key and jurisdiction_key in jurisdiction_filters:
        display_name = jurisdiction_filters[jurisdiction_key].get('display_name', jurisdiction_key)
        return f"{year} {display_name.lower().replace(' county', '').strip()}.csv"
    return f"cdot_crash_statewide_{year}.csv"


# The unique crash identifier column used for deduplication
CUID_COLUMN = 'CUID'


def merge_with_existing(new_df, existing_path):
    """
    Merge new downloaded data with an existing CSV file using CUID as the
    deduplication key. Preserves all existing records and appends only
    genuinely new ones.

    Returns (merged_df, stats_dict) where stats_dict contains:
      - existing_count: rows in the existing file
      - new_download_count: rows in the fresh download
      - new_records: count of records added (CUID not in existing)
      - updated_records: count of records with same CUID but changed data
      - merged_count: total rows in the merged result
    """
    import pandas as pd

    existing_df = pd.read_csv(existing_path)
    stats = {
        'existing_count': len(existing_df),
        'new_download_count': len(new_df),
        'new_records': 0,
        'updated_records': 0,
        'merged_count': 0,
    }

    # Find the CUID column (case-insensitive) in both DataFrames
    cuid_col_existing = None
    for col in existing_df.columns:
        if col.strip().upper() == CUID_COLUMN:
            cuid_col_existing = col
            break

    cuid_col_new = None
    for col in new_df.columns:
        if col.strip().upper() == CUID_COLUMN:
            cuid_col_new = col
            break

    if cuid_col_existing is None or cuid_col_new is None:
        # No CUID column — can't deduplicate, fall back to full replace
        logger.warning(f"  CUID column not found. Falling back to full replacement.")
        logger.warning(f"    Existing columns: {list(existing_df.columns)[:10]}")
        logger.warning(f"    New columns: {list(new_df.columns)[:10]}")
        stats['merged_count'] = len(new_df)
        return new_df, stats

    existing_cuids = set(existing_df[cuid_col_existing].dropna().astype(str))
    new_cuids = set(new_df[cuid_col_new].dropna().astype(str))

    # New records: CUIDs in download that aren't in existing
    added_cuids = new_cuids - existing_cuids
    stats['new_records'] = len(added_cuids)

    # Check for updated records (same CUID, different data)
    common_cuids = existing_cuids & new_cuids
    if common_cuids:
        # Compare row counts — if CDOT revised a record, the new version wins
        # but we log it for audit
        existing_common = existing_df[existing_df[cuid_col_existing].astype(str).isin(common_cuids)]
        new_common = new_df[new_df[cuid_col_new].astype(str).isin(common_cuids)]

        # Quick check: row count difference in common CUIDs
        if len(existing_common) != len(new_common):
            stats['updated_records'] = abs(len(new_common) - len(existing_common))

    if stats['new_records'] == 0:
        logger.info(f"  No new records to merge (all {len(common_cuids)} CUIDs already exist)")
        stats['merged_count'] = len(existing_df)
        return existing_df, stats

    # Append only the new rows
    new_rows = new_df[new_df[cuid_col_new].astype(str).isin(added_cuids)].copy()
    merged_df = pd.concat([existing_df, new_rows], ignore_index=True)
    stats['merged_count'] = len(merged_df)

    logger.info(f"  Merge: {stats['existing_count']:,} existing + {stats['new_records']:,} new "
                f"= {stats['merged_count']:,} total")

    return merged_df, stats


def process_year(session, year, year_info, manifest, data_dir, jurisdiction_key,
                 force=False):
    """
    Download a single year's statewide file, filter to county, save as CSV.

    Merge strategy:
      - "final" years: skip download if CSV already exists (use --force to override)
      - "preliminary" years: download and merge with existing using CUID dedup
      - missing files: download fresh regardless of status

    Returns (year, success_bool, output_path_or_error).
    """
    docid = year_info['docid']
    status = year_info.get('status', 'unknown')
    label = f"CDOT Crash Listing {year}"
    if status == 'preliminary':
        label += " (preliminary)"

    # Build output filename
    filename = _get_output_filename(year, jurisdiction_key, manifest)
    output_path = data_dir / filename

    # --- Skip-if-final: don't re-download finalized years that already exist ---
    if output_path.exists() and status == 'final' and not force:
        import pandas as pd
        existing_count = len(pd.read_csv(output_path))
        logger.info(f"  {year}: SKIPPED — final data already exists "
                    f"({output_path.name}, {existing_count:,} records). Use --force to re-download.")
        return year, True, str(output_path)

    try:
        # Download the Excel file
        content, ext = download_onbase_document(session, docid, label=label)

        # Parse Excel to DataFrame
        df = excel_to_dataframe(content, ext)

        if df.empty:
            return year, False, "Downloaded file contains no data rows"

        # Log column summary
        logger.info(f"  Columns: {list(df.columns)[:10]}...")

        # Filter to jurisdiction
        jurisdiction_filters = manifest.get('jurisdiction_filters', {})
        if jurisdiction_key and jurisdiction_key in jurisdiction_filters:
            jur_config = jurisdiction_filters[jurisdiction_key]
            df = filter_to_jurisdiction(df, jur_config, jurisdiction_key)
            display_name = jur_config.get('display_name', jurisdiction_key)

            if df.empty:
                return year, False, f"No records for {display_name} in {year} data"
        else:
            pass  # statewide — no filter

        # --- Merge logic: merge with existing data if file exists ---
        if output_path.exists() and status == 'preliminary':
            logger.info(f"  Merging with existing {output_path.name}...")
            df, merge_stats = merge_with_existing(df, output_path)
            logger.info(f"  Merge result: +{merge_stats['new_records']} new records")
        elif output_path.exists() and force:
            logger.info(f"  Force mode: replacing existing {output_path.name}")

        # Save as CSV
        df.to_csv(output_path, index=False)
        logger.info(f"  Saved: {output_path} ({len(df):,} records)")

        return year, True, str(output_path)

    except Exception as e:
        logger.error(f"  Failed to process {year}: {e}")
        return year, False, str(e)


def download_data_dictionary(session, dict_info, data_dir):
    """Download a data dictionary file."""
    docid = dict_info['docid']
    desc = dict_info.get('description', f'docid={docid}')

    try:
        content, ext = download_onbase_document(session, docid, label=desc)
        output_path = data_dir / f"cdot_data_dictionary{ext}"
        with open(output_path, 'wb') as f:
            f.write(content)
        logger.info(f"  Saved data dictionary: {output_path}")
        return True
    except Exception as e:
        logger.warning(f"  Failed to download data dictionary: {e}")
        return False


def list_available(manifest):
    """List all available years and jurisdictions from the manifest."""
    print("\n" + "=" * 60)
    print("CDOT CRASH DATA - AVAILABLE DOWNLOADS")
    print("=" * 60)

    files = manifest.get('files', {})
    print(f"\nYEARS ({len(files)}):")
    print("-" * 40)
    for year in sorted(files.keys(), reverse=True):
        info = files[year]
        status_tag = f" [{info['status']}]" if info.get('status') != 'final' else ''
        print(f"  {year}  docid={info['docid']}{status_tag}")

    jurisdictions = {k: v for k, v in manifest.get('jurisdiction_filters', {}).items()
                     if not k.startswith('_') and isinstance(v, dict)}
    print(f"\nJURISDICTIONS ({len(jurisdictions)}):")
    print("-" * 40)
    for key, jur in sorted(jurisdictions.items()):
        fips = f" (FIPS: {jur['fips']})" if jur.get('fips') else ''
        print(f"  {key:<15} {jur['display_name']}{fips}")

    dicts = manifest.get('data_dictionaries', {})
    print(f"\nDATA DICTIONARIES ({len(dicts)}):")
    print("-" * 40)
    for range_key, info in sorted(dicts.items()):
        print(f"  {range_key}  docid={info['docid']}  {info.get('description', '')}")

    print("=" * 60)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Download CDOT statewide crash data, filter to county, and save as CSV.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_cdot_crash_data.py                          # All years, default county (douglas)
  python download_cdot_crash_data.py --latest                 # Most recent year only
  python download_cdot_crash_data.py --years 2023 2024        # Specific years
  python download_cdot_crash_data.py --jurisdiction elpaso    # El Paso County
  python download_cdot_crash_data.py --list                   # Show available years/counties
  python download_cdot_crash_data.py --statewide              # Keep statewide (no county filter)
  python download_cdot_crash_data.py --no-dict                # Skip data dictionary
  python download_cdot_crash_data.py --force --years 2024      # Re-download even if 2024 exists
        """
    )

    parser.add_argument(
        '--years', '-y',
        nargs='+',
        type=str,
        help='Specific years to download (e.g., 2023 2024)'
    )

    parser.add_argument(
        '--latest',
        action='store_true',
        help='Download only the most recent year'
    )

    parser.add_argument(
        '--jurisdiction', '-j',
        type=str,
        default='douglas',
        help='County to filter to (default: douglas). See --list for options.'
    )

    parser.add_argument(
        '--statewide',
        action='store_true',
        help='Save statewide data without county filtering'
    )

    parser.add_argument(
        '--save-statewide-gzip',
        action='store_true',
        help='After downloading, merge all year CSVs into a single gzipped statewide file'
    )

    parser.add_argument(
        '--data-dir', '-d',
        type=str,
        default=None,
        help=f'Output directory (default: data/CDOT)'
    )

    parser.add_argument(
        '--no-dict',
        action='store_true',
        help='Skip downloading data dictionaries'
    )

    parser.add_argument(
        '--manifest', '-m',
        type=str,
        default=None,
        help='Path to source manifest JSON (default: data/CDOT/source_manifest.json)'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List available years and jurisdictions, then exit'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download even for finalized years that already exist locally'
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()

    # Load manifest
    manifest = load_manifest(args.manifest)

    # Handle --list
    if args.list:
        list_available(manifest)
        return 0

    # Determine output directory
    data_dir = Path(args.data_dir) if args.data_dir else DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    # Determine which years to download
    all_files = manifest.get('files', {})

    if args.latest:
        latest_year = max(all_files.keys())
        years_to_download = {latest_year: all_files[latest_year]}
    elif args.years:
        years_to_download = {}
        for y in args.years:
            if y in all_files:
                years_to_download[y] = all_files[y]
            else:
                logger.warning(f"Year {y} not found in manifest. Available: {sorted(all_files.keys())}")
        if not years_to_download:
            logger.error("No valid years specified.")
            return 1
    else:
        years_to_download = all_files

    # Determine jurisdiction
    jurisdiction_key = None if args.statewide else args.jurisdiction
    jurisdiction_filters = manifest.get('jurisdiction_filters', {})

    if jurisdiction_key and jurisdiction_key not in jurisdiction_filters:
        logger.error(f"Unknown jurisdiction: {jurisdiction_key}")
        logger.info(f"Available: {', '.join(sorted(jurisdiction_filters.keys()))}")
        return 1

    # Print summary
    logger.info("=" * 60)
    logger.info(f"CDOT Crash Data Downloader")
    logger.info(f"Started at: {datetime.now()}")
    logger.info(f"Years: {', '.join(sorted(years_to_download.keys()))}")
    if jurisdiction_key:
        display = jurisdiction_filters[jurisdiction_key]['display_name']
        logger.info(f"Jurisdiction: {display}")
    else:
        logger.info(f"Jurisdiction: Statewide (no filter)")
    logger.info(f"Output: {data_dir}")
    logger.info("=" * 60)

    # Create session (reuse across all downloads for connection pooling)
    session = create_session_with_retries()

    # Bootstrap session — visit the DocPop landing page to establish cookies.
    # OnBase returns 403 for direct document requests without a valid session.
    bootstrap_session(session)

    # Download data dictionaries first (unless skipped)
    if not args.no_dict:
        logger.info("--- Downloading data dictionaries ---")
        for range_key, dict_info in manifest.get('data_dictionaries', {}).items():
            download_data_dictionary(session, dict_info, data_dir)

    # Download and process each year
    results = []
    logger.info(f"--- Downloading {len(years_to_download)} year(s) ---")

    for year in sorted(years_to_download.keys(), reverse=True):
        year_info = years_to_download[year]
        year_result = process_year(session, year, year_info, manifest, data_dir, jurisdiction_key,
                                   force=args.force)
        results.append(year_result)

    # Stage 1.5: Assemble statewide gzip from all downloaded year CSVs
    successes = [(y, p) for y, ok, p in results if ok]
    failures = [(y, e) for y, ok, e in results if not ok]

    if args.save_statewide_gzip and successes:
        try:
            logger.info("")
            logger.info("=" * 60)
            logger.info("STAGE 1.5: Assembling statewide gzip from per-year CSVs")
            logger.info("=" * 60)

            import gzip
            import shutil
            import subprocess as sp

            year_dfs = []
            for year, path in successes:
                if os.path.isfile(path):
                    logger.info(f"  Reading {year}: {path}")
                    year_df = pd.read_csv(path, dtype=str)
                    year_dfs.append(year_df)

            if year_dfs:
                combined = pd.concat(year_dfs, ignore_index=True)
                total_records = len(combined)

                # Deduplicate by CUID if column exists
                cuid_col = None
                for col in combined.columns:
                    if col.upper() == 'CUID':
                        cuid_col = col
                        break
                if cuid_col:
                    before_dedup = len(combined)
                    combined = combined.drop_duplicates(subset=[cuid_col], keep='first')
                    deduped = before_dedup - len(combined)
                    if deduped > 0:
                        logger.info(f"  Deduplicated: removed {deduped:,} duplicate CUIDs")

                # Save raw merged CSV (temporary — will be converted)
                raw_statewide_path = os.path.join(str(data_dir), "colorado_statewide_raw.csv")
                combined.to_csv(raw_statewide_path, index=False)
                logger.info(f"  Raw merged CSV: {raw_statewide_path} ({len(combined):,} records)")

                # Run through convert + validate pipeline (same as county data)
                # This normalizes CDOT columns to Virginia-compatible format and validates
                logger.info("  Running convert + validate pipeline on statewide data...")
                scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
                pipeline_cmd = [
                    sys.executable, os.path.join(scripts_dir, 'process_crash_data.py'),
                    '-i', raw_statewide_path,
                    '-s', 'colorado',
                    '-j', 'statewide',
                    '-o', str(data_dir),
                    '--skip-split',      # Don't split into road types — keep all roads
                    '-f', '-v'
                ]
                logger.info(f"  Pipeline cmd: {' '.join(pipeline_cmd)}")

                result = sp.run(pipeline_cmd, capture_output=True, text=True, timeout=1800)
                if result.returncode == 0:
                    logger.info("  Convert + validate pipeline completed successfully")
                    if result.stdout:
                        for line in result.stdout.strip().split('\n')[-10:]:
                            logger.info(f"    {line}")
                else:
                    logger.warning(f"  Pipeline returned code {result.returncode}")
                    if result.stderr:
                        for line in result.stderr.strip().split('\n')[-5:]:
                            logger.warning(f"    {line}")
                    logger.warning("  Falling back to raw merged CSV (without conversion)")

                # Find the standardized output (process_crash_data.py creates statewide_standardized.csv)
                standardized_path = os.path.join(str(data_dir), "statewide_standardized.csv")
                if os.path.isfile(standardized_path):
                    source_path = standardized_path
                    logger.info(f"  Using standardized output: {standardized_path}")
                else:
                    source_path = raw_statewide_path
                    logger.info(f"  Using raw merged CSV (no standardized output found)")

                # Rename to final statewide name and gzip
                statewide_path = os.path.join(str(data_dir), "colorado_statewide_all_roads.csv")
                os.rename(source_path, statewide_path)

                gz_path = f"{statewide_path}.gz"
                with open(statewide_path, 'rb') as f_in:
                    with gzip.open(gz_path, 'wb', compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out)
                uncompressed_size = os.path.getsize(statewide_path)
                compressed_size = os.path.getsize(gz_path)
                # Keep uncompressed CSV alongside gzip for batch splitting workflows
                # (split_jurisdictions.py needs the uncompressed CSV to split into
                # per-jurisdiction files). The batch workflow cleans up after splitting.
                ratio = uncompressed_size / compressed_size if compressed_size > 0 else 0
                logger.info(f"  Statewide gzip: {gz_path} ({compressed_size:,} bytes, {ratio:.1f}x compression)")
                logger.info(f"  Statewide CSV kept: {statewide_path} ({uncompressed_size:,} bytes)")

                # Cleanup temporary raw file (only if different from processed statewide CSV)
                if os.path.isfile(raw_statewide_path) and raw_statewide_path != source_path and raw_statewide_path != statewide_path:
                    os.remove(raw_statewide_path)
            else:
                logger.warning("  No year CSVs available for statewide assembly")

        except Exception as e:
            logger.warning(f"  Statewide gzip assembly failed (non-fatal): {e}")

    # Print summary

    logger.info("")
    logger.info("=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)

    if successes:
        logger.info(f"Succeeded: {len(successes)}")
        for year, path in successes:
            logger.info(f"  {year}: {path}")

    if failures:
        logger.warning(f"Failed: {len(failures)}")
        for year, error in failures:
            logger.warning(f"  {year}: {error}")

    logger.info("=" * 60)

    if failures and not successes:
        # Check if all failures are preliminary years — treat as non-fatal warning
        # so scheduled --latest runs don't fail the entire pipeline when OnBase
        # is temporarily unreachable or the preliminary data isn't ready yet.
        all_files = manifest.get('files', {})
        all_preliminary = all(
            all_files.get(str(y), {}).get('status') == 'preliminary'
            for y, _ in failures
        )

        if all_preliminary:
            logger.warning("All failed downloads were preliminary data — treating as non-fatal.")
            logger.warning("Finalized data is served from R2. Will retry next scheduled run.")
            return 0
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
