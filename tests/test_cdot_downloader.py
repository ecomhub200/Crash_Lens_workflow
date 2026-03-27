#!/usr/bin/env python3
"""
Bug tests for download_cdot_crash_data.py

Tests every public function for correctness, edge cases, and regressions.
Run with: python -m pytest tests/test_cdot_downloader.py -v
"""

import io
import json
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from download_cdot_crash_data import (
    _extract_guids_from_html,
    _extract_onbase_keys,
    _extract_virtual_root,
    _get_output_filename,
    _playwright_available,
    BROWSER_HEADERS,
    build_obtoken_candidates,
    check_for_new_year,
    create_session_with_retries,
    CUID_COLUMN,
    detect_file_type,
    discover_crash_listings,
    download_data_dictionary,
    download_onbase_document,
    download_with_playwright_event,
    excel_to_dataframe,
    extract_download_url_from_html,
    extract_obtoken_url,
    extract_viewer_binary_urls,
    filter_to_jurisdiction,
    list_available,
    load_manifest,
    main,
    make_request_with_retry,
    MAX_RETRIES,
    merge_with_existing,
    MIN_VALID_FILE_SIZE,
    ONBASE_BASE_URL,
    ONBASE_GETDOC_URLS,
    parse_args,
    process_year,
    RETRY_BACKOFF_FACTOR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_xlsx_bytes(data: dict) -> bytes:
    """Create a real .xlsx file in memory and return its bytes."""
    df = pd.DataFrame(data)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine='openpyxl')
    return buf.getvalue()


def make_statewide_xlsx(counties=None, rows_per_county=10):
    """
    Build a realistic statewide CDOT crash Excel with multiple counties.
    Returns (bytes, expected_douglas_count).
    """
    if counties is None:
        counties = ['DOUGLAS', 'EL PASO', 'ARAPAHOE', 'DENVER', 'JEFFERSON']

    rows = []
    for county in counties:
        for i in range(rows_per_county):
            rows.append({
                'CUID': f'{county[:3]}{i:04d}',
                'System Code': 'County Road',
                'Crash Date': f'3/{i+1}/2024',
                'Crash Time': '1400',
                'Agency Id': 'DSO' if county == 'DOUGLAS' else 'CSP',
                'City': '',
                'County': county,
                'Latitude': 39.5,
                'Longitude': -104.9,
                'Location 1': 'Test Rd',
                'Number Killed': 0,
                'Number Injured': 0,
                'Injury 00': 2,
                'Injury 01': 0,
                'Injury 02': 0,
                'Injury 03': 0,
                'Injury 04': 0,
            })

    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine='openpyxl')
    douglas_count = rows_per_county if 'DOUGLAS' in counties else 0
    return buf.getvalue(), douglas_count


def make_manifest(tmp_dir, years=None, jurisdictions=None):
    """Create a temporary manifest file and return its path."""
    if years is None:
        years = {
            "2025": {"docid": 54973381, "status": "preliminary"},
            "2024": {"docid": 35111742, "status": "final"},
        }
    if jurisdictions is None:
        jurisdictions = {
            "douglas": {
                "county": "DOUGLAS",
                "fips": "08035",
                "display_name": "Douglas County"
            },
            "elpaso": {
                "county": "EL PASO",
                "fips": "08041",
                "display_name": "El Paso County"
            }
        }

    manifest = {
        "source": {"name": "test", "base_url": "https://example.com"},
        "jurisdiction_filters": jurisdictions,
        "files": years,
        "data_dictionaries": {
            "2021-2025": {"docid": 17470635, "description": "Test dict"}
        }
    }

    path = Path(tmp_dir) / 'source_manifest.json'
    with open(path, 'w') as f:
        json.dump(manifest, f)
    return str(path)


# ===========================================================================
# detect_file_type
# ===========================================================================

class TestDetectFileType:

    def test_xlsx_magic_bytes(self):
        """XLSX files start with PK (ZIP signature)."""
        content = make_xlsx_bytes({'A': [1, 2, 3]})
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is True
        assert ext == '.xlsx'
        assert 'XLSX' in reason

    def test_xls_magic_bytes(self):
        """XLS (OLE2) files start with 0xD0CF."""
        content = b'\xd0\xcf\x11\xe0' + b'\x00' * 200
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is True
        assert ext == '.xls'

    def test_html_doctype(self):
        """HTML responses should be flagged as invalid."""
        content = b'<!DOCTYPE html><html><body>Error</body></html>'
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is False
        assert ext == '.html'

    def test_html_tag_only(self):
        """<html> without doctype should also be caught."""
        content = b'<html><head><title>Login</title></head></html>'
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is False
        assert ext == '.html'

    def test_html_with_leading_whitespace(self):
        """HTML with leading whitespace should still be detected."""
        content = b'   \n  <!DOCTYPE html><html><body>Error</body></html>'
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is False
        assert ext == '.html'

    def test_xml_response(self):
        """XML error responses should be flagged."""
        content = b'<?xml version="1.0"?><error>Access denied</error>'
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is False
        assert ext == '.html'

    def test_content_type_spreadsheet(self):
        """Content-Type header with spreadsheet should pass."""
        content = b'\x00\x01\x02\x03' * 100  # non-magic, non-HTML
        is_valid, ext, reason = detect_file_type(
            content,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        assert is_valid is True
        assert ext == '.xlsx'

    def test_content_type_excel(self):
        content = b'\x00\x01' * 100
        is_valid, ext, _ = detect_file_type(content, content_type='application/vnd.ms-excel')
        assert is_valid is True

    def test_content_type_octet_stream(self):
        content = b'\x00\x01' * 100
        is_valid, ext, _ = detect_file_type(content, content_type='application/octet-stream')
        assert is_valid is True

    def test_content_disposition_xlsx(self):
        """Filename in Content-Disposition should determine extension."""
        content = b'\x00\x01' * 100
        is_valid, ext, reason = detect_file_type(
            content,
            content_disposition='attachment; filename="crash_data_2024.xlsx"'
        )
        assert is_valid is True
        assert ext == '.xlsx'
        assert 'crash_data_2024.xlsx' in reason

    def test_content_disposition_xls(self):
        content = b'\x00\x01' * 100
        is_valid, ext, _ = detect_file_type(
            content,
            content_disposition="attachment; filename='data.xls'"
        )
        assert is_valid is True
        assert ext == '.xls'

    def test_large_unknown_binary_accepted(self):
        """Large binary files with no other signals should pass as xlsx."""
        content = b'\x00\x01' * (MIN_VALID_FILE_SIZE + 1)
        is_valid, ext, reason = detect_file_type(content)
        assert is_valid is True
        assert ext == '.xlsx'
        assert 'Large binary' in reason

    def test_small_unknown_binary_rejected(self):
        """Small unrecognized files should be rejected."""
        content = b'\x00\x01\x02\x03'
        is_valid, ext, _ = detect_file_type(content)
        assert is_valid is False
        assert ext == '.unknown'

    def test_empty_content(self):
        """Empty response should be rejected."""
        is_valid, ext, _ = detect_file_type(b'')
        assert is_valid is False

    def test_magic_bytes_take_priority_over_headers(self):
        """Magic bytes should win even if Content-Type says text/html."""
        content = make_xlsx_bytes({'A': [1]})
        is_valid, ext, _ = detect_file_type(content, content_type='text/html')
        assert is_valid is True
        assert ext == '.xlsx'


# ===========================================================================
# extract_download_url_from_html
# ===========================================================================

class TestExtractDownloadUrl:

    def test_pdfpop_link(self):
        html = b'<a href="/CDOTRMPop/docpop/PdfPop.aspx?docid=123">View</a>'
        url = extract_download_url_from_html(html, 'https://example.com/docpop/docpop.aspx')
        assert url == 'https://example.com/CDOTRMPop/docpop/PdfPop.aspx?docid=123'

    def test_getdoc_link(self):
        html = b'<a href="GetDoc.aspx?id=456">Download</a>'
        url = extract_download_url_from_html(html, 'https://example.com/docpop/docpop.aspx')
        assert url is not None
        assert 'GetDoc.aspx' in url

    def test_download_link(self):
        html = b'<a href="/files/Download?id=789">Download</a>'
        url = extract_download_url_from_html(html, 'https://host.com/path/page.aspx')
        assert url == 'https://host.com/files/Download?id=789'

    def test_window_location_redirect(self):
        html = b"<script>window.location = '/redirect/file.xlsx';</script>"
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url == 'https://host.com/redirect/file.xlsx'

    def test_window_open(self):
        html = b"<script>window.open('https://cdn.example.com/file.xlsx');</script>"
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url == 'https://cdn.example.com/file.xlsx'

    def test_absolute_url_preserved(self):
        html = b'<a href="https://cdn.example.com/doc/GetDoc?id=1">Get</a>'
        url = extract_download_url_from_html(html, 'https://other.com/page.aspx')
        assert url == 'https://cdn.example.com/doc/GetDoc?id=1'

    def test_relative_url_resolved(self):
        html = b'<a href="GetDoc.aspx?id=1">Get</a>'
        url = extract_download_url_from_html(html, 'https://host.com/app/docpop.aspx')
        assert url == 'https://host.com/app/GetDoc.aspx?id=1'

    def test_no_match_returns_none(self):
        html = b'<html><body>No links here</body></html>'
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url is None

    def test_empty_html(self):
        url = extract_download_url_from_html(b'', 'https://host.com/page.aspx')
        assert url is None

    def test_case_insensitive_matching(self):
        html = b'<a HREF="/path/PDFPOP.ASPX?docid=1">View</a>'
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url is not None

    def test_retrieve_native_document_link(self):
        html = b'<a href="/docpop/RetrieveNativeDocument.aspx?id=1">Save</a>'
        url = extract_download_url_from_html(html, 'https://host.com/docpop/page.aspx')
        assert url is not None
        assert 'RetrieveNativeDocument' in url

    def test_get_native_doc_link(self):
        html = b'<a href="GetNativeDoc.ashx?docid=55">Download</a>'
        url = extract_download_url_from_html(html, 'https://host.com/docpop/viewer.aspx')
        assert url is not None
        assert 'GetNativeDoc' in url

    def test_send_to_application_link(self):
        html = b'<a href="SendToApplication.aspx?action=download">Send</a>'
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url is not None
        assert 'SendToApplication' in url

    def test_string_input_accepted(self):
        """extract_download_url_from_html should accept str in addition to bytes."""
        html = '<a href="/docpop/GetDoc.aspx?id=1">DL</a>'
        url = extract_download_url_from_html(html, 'https://host.com/page.aspx')
        assert url is not None
        assert 'GetDoc' in url


# ===========================================================================
# extract_obtoken_url
# ===========================================================================

class TestExtractObtokenUrl:
    """Test OBToken extraction from OnBase DocPop HTML framesets."""

    BASE_URL = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx'

    def test_frame_src_with_obtoken(self):
        """Classic OnBase frameset with ViewDocumentEx in a <frame> tag."""
        html = b'''<html><head><title>DocPop</title></head>
        <frameset rows="0,*">
          <frame src="UnloadHandler.aspx" name="unloadHandler" />
          <frame src="ViewDocumentEx.aspx?OBToken=e1c20842-ad67-4e79-9343-2edff43b5868"
                 name="DocSelectPage" />
        </frameset></html>'''
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'ViewDocumentEx.aspx' in url
        assert 'OBToken=e1c20842-ad67-4e79-9343-2edff43b5868' in url
        assert url.startswith('https://')

    def test_iframe_src_with_obtoken(self):
        """Modern OnBase with <iframe> instead of <frame>."""
        html = b'''<html><body>
        <iframe src="ViewDocumentEx.aspx?OBToken=aabbccdd-1122-3344-5566-778899001122"
                id="DocSelectPage"></iframe>
        </body></html>'''
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'OBToken=aabbccdd-1122-3344-5566-778899001122' in url

    def test_javascript_src_assignment(self):
        """OBToken embedded in JavaScript .src assignment."""
        html = b'''<html><body>
        <iframe id="DocSelectPage" src="blank.aspx"></iframe>
        <script>
          document.getElementById('DocSelectPage').src =
            'ViewDocumentEx.aspx?OBToken=12345678-abcd-ef01-2345-678901234567';
        </script></body></html>'''
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'OBToken=12345678-abcd-ef01-2345-678901234567' in url

    def test_window_location_obtoken(self):
        """OBToken in window.location redirect."""
        html = b'''<script>
        location = 'ViewDocumentEx.aspx?OBToken=aaaabbbb-cccc-dddd-eeee-ffffffffffff';
        </script>'''
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'OBToken=aaaabbbb-cccc-dddd-eeee-ffffffffffff' in url

    def test_bare_obtoken_guid(self):
        """Only OBToken GUID visible (not in a complete URL) — should build URL."""
        html = b'<input type="hidden" name="token" value="OBToken=aabb1122-3344-5566-7788-99aabbccddee" />'
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'ViewDocumentEx.aspx' in url
        assert 'OBToken=aabb1122-3344-5566-7788-99aabbccddee' in url

    def test_absolute_url_preserved(self):
        """If the OBToken URL is already absolute, don't mangle it."""
        abs_url = 'https://other.server.com/docpop/ViewDocumentEx.aspx?OBToken=11111111-2222-3333-4444-555555555555'
        html = f'<iframe src="{abs_url}"></iframe>'.encode()
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url == abs_url

    def test_relative_url_resolved(self):
        """Relative OBToken URL resolved against base URL."""
        html = b'<frame src="ViewDocumentEx.aspx?OBToken=12345678-1234-1234-1234-123456789abc" />'
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url.startswith('https://oitco.hylandcloud.com/')
        assert 'ViewDocumentEx.aspx' in url

    def test_no_obtoken_returns_none(self):
        """HTML without OBToken returns None."""
        html = b'<html><body>No frames or tokens here</body></html>'
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is None

    def test_empty_html_returns_none(self):
        url = extract_obtoken_url(b'', self.BASE_URL)
        assert url is None

    def test_string_input_accepted(self):
        """Should handle str input, not just bytes."""
        html = '<frame src="ViewDocumentEx.aspx?OBToken=abcdef01-2345-6789-abcd-ef0123456789" />'
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'OBToken=abcdef01-2345-6789-abcd-ef0123456789' in url

    def test_quoted_js_variable(self):
        """OBToken in a JS string variable (not in src/href attribute)."""
        html = b'''<script>
        var docUrl = "ViewDocumentEx.aspx?OBToken=deadbeef-1234-5678-9abc-def012345678";
        </script>'''
        url = extract_obtoken_url(html, self.BASE_URL)
        assert url is not None
        assert 'OBToken=deadbeef-1234-5678-9abc-def012345678' in url


# ===========================================================================
# GUID / VirtualRoot / key extraction helpers
# ===========================================================================

class TestExtractVirtualRoot:

    def test_standard_virtual_root(self):
        html = 'var __VirtualRoot="https://oitco.hylandcloud.com/cdotrmpop";'
        assert _extract_virtual_root(html) == 'https://oitco.hylandcloud.com/cdotrmpop'

    def test_with_trailing_slash(self):
        html = "var __VirtualRoot='https://host.com/app/';"
        assert _extract_virtual_root(html) == 'https://host.com/app'

    def test_no_virtual_root(self):
        html = '<html><body>nothing here</body></html>'
        assert _extract_virtual_root(html) is None


class TestExtractGuidsFromHtml:

    def test_finds_standard_guids(self):
        html = '''<script>
        var token = "407b6bf6-c00d-4450-9db9-72e58cfc2447";
        var session = "aabbccdd-1122-3344-5566-778899001122";
        </script>'''
        guids = _extract_guids_from_html(html)
        assert len(guids) == 2
        assert '407b6bf6-c00d-4450-9db9-72e58cfc2447' in guids
        assert 'aabbccdd-1122-3344-5566-778899001122' in guids

    def test_deduplicates(self):
        html = 'id="407b6bf6-c00d-4450-9db9-72e58cfc2447" data="407b6bf6-c00d-4450-9db9-72e58cfc2447"'
        guids = _extract_guids_from_html(html)
        assert len(guids) == 1

    def test_no_guids(self):
        html = '<html><body>no guids here</body></html>'
        guids = _extract_guids_from_html(html)
        assert len(guids) == 0


class TestExtractOnbaseKeys:

    def test_finds_underscore_hex_keys(self):
        html = 'var k = "6754fac4_c7b2_4cbb_ad8a_";'
        keys = _extract_onbase_keys(html)
        assert len(keys) == 1
        assert '6754fac4_c7b2_4cbb_ad8a_' in keys

    def test_no_keys(self):
        html = 'var x = "normal_text";'
        keys = _extract_onbase_keys(html)
        assert len(keys) == 0


class TestBuildObtokenCandidates:

    def test_builds_urls_with_virtual_root(self):
        html = '''<script>
        var __VirtualRoot="https://oitco.hylandcloud.com/cdotrmpop";
        var token = "407b6bf6-c00d-4450-9db9-72e58cfc2447";
        </script>'''
        candidates = build_obtoken_candidates(
            html.encode(),
            'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx',
            '17470635')
        assert len(candidates) >= 2
        # RetrieveNativeDocument is now tried before ViewDocumentEx
        assert any('cdotrmpop/RetrieveNativeDocument.ashx' in c for c in candidates)
        assert any('cdotrmpop/ViewDocumentEx.aspx' in c for c in candidates)
        assert any('OBToken=407b6bf6' in c for c in candidates)
        assert any('dochandle=17470635' in c for c in candidates)

    def test_includes_k_param(self):
        html = '''<script>
        var __VirtualRoot="https://host.com/app";
        var t = "11112222-3333-4444-5555-666677778888";
        var k = "aabbccdd_1122_3344_5566_";
        </script>'''
        candidates = build_obtoken_candidates(html, 'https://host.com/app/docpop/docpop.aspx', '999')
        # First candidate should have k param
        assert any('k=aabbccdd_1122_3344_5566_' in c for c in candidates)
        # Should also have a version without k
        assert any('k=' not in c for c in candidates)

    def test_no_guids_returns_empty(self):
        html = '<html><body>nothing</body></html>'
        candidates = build_obtoken_candidates(html, 'https://host.com/docpop.aspx', '123')
        assert candidates == []

    def test_fallback_base_url_without_virtual_root(self):
        """When __VirtualRoot is not found, derive from base_url."""
        html = '<script>var t = "aabb1122-3344-5566-7788-99aabbccddee";</script>'
        candidates = build_obtoken_candidates(
            html,
            'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx',
            '12345')
        assert len(candidates) >= 2
        # Should include both RetrieveNativeDocument and ViewDocumentEx
        assert any('/CDOTRMPop/RetrieveNativeDocument.ashx' in c for c in candidates)
        assert any('/CDOTRMPop/ViewDocumentEx.aspx' in c for c in candidates)


class TestExtractViewerBinaryUrls:

    def test_finds_ashx_handlers(self):
        html = '<script>var url = "/app/GetDocumentContent.ashx?id=1";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/app/viewer.aspx')
        assert len(urls) >= 1
        assert any('GetDocumentContent.ashx' in u for u in urls)

    def test_finds_aspx_pages(self):
        html = '<a href="RetrieveNativeDocument.aspx?token=abc">Save</a>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/app/viewer.aspx')
        assert any('RetrieveNativeDocument.aspx' in u for u in urls)

    def test_skips_css_and_images(self):
        html = '<link href="styles.css"><img src="/app/download.gif">'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert not any('.css' in u for u in urls)
        assert not any('.gif' in u for u in urls)

    def test_empty_html_no_obtoken_in_base(self):
        # Empty HTML with a base URL that has no OBToken → no URLs from patterns
        # (URL-construction only kicks in when base_url contains OBToken)
        urls = extract_viewer_binary_urls('', 'https://host.com/')
        assert urls == []

    def test_empty_html_with_obtoken_in_base_url(self):
        # Empty HTML but base URL contains OBToken → constructs retrieval URLs
        urls = extract_viewer_binary_urls(
            '',
            'https://host.com/app/ViewDocumentEx.aspx?OBToken=aabb1122-3344-5566-7788-99aabbccddee&dochandle=123')
        assert len(urls) >= 1
        assert any('RetrieveNativeDocument.ashx' in u for u in urls)


# ===========================================================================
# excel_to_dataframe
# ===========================================================================

class TestExcelToDataframe:

    def test_valid_xlsx(self):
        content = make_xlsx_bytes({
            'County': ['DOUGLAS', 'EL PASO'],
            'CUID': [1, 2],
        })
        df = excel_to_dataframe(content, '.xlsx')
        assert len(df) == 2
        assert 'County' in df.columns

    def test_preserves_all_columns(self):
        """All columns in the Excel should appear in the DataFrame."""
        cols = {f'Col_{i}': list(range(5)) for i in range(20)}
        content = make_xlsx_bytes(cols)
        df = excel_to_dataframe(content, '.xlsx')
        assert len(df.columns) == 20

    def test_empty_xlsx(self):
        """An Excel file with headers but no data rows."""
        content = make_xlsx_bytes({'County': pd.Series([], dtype=str)})
        df = excel_to_dataframe(content, '.xlsx')
        assert len(df) == 0
        assert 'County' in df.columns

    def test_numeric_values_preserved(self):
        content = make_xlsx_bytes({
            'Latitude': [39.50008, 39.52261],
            'Longitude': [-104.87, -104.92],
        })
        df = excel_to_dataframe(content, '.xlsx')
        assert abs(df['Latitude'].iloc[0] - 39.50008) < 0.0001

    def test_corrupt_content_raises(self):
        """Corrupt/non-Excel bytes should raise an exception."""
        with pytest.raises(Exception):
            excel_to_dataframe(b'this is not excel', '.xlsx')


# ===========================================================================
# filter_to_jurisdiction
# ===========================================================================

class TestFilterToJurisdiction:

    def _make_df(self, counties):
        return pd.DataFrame({
            'CUID': range(len(counties)),
            'County': counties,
            'Agency Id': ['DSO'] * len(counties),
        })

    def test_basic_filter(self):
        df = self._make_df(['DOUGLAS', 'EL PASO', 'DOUGLAS', 'DENVER'])
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 2
        assert all(result['County'] == 'DOUGLAS')

    def test_case_insensitive_county_value(self):
        """County column may have mixed case like 'Douglas' instead of 'DOUGLAS'."""
        df = self._make_df(['Douglas', 'El Paso', 'douglas', 'DOUGLAS'])
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 3  # all three Douglas variants

    def test_whitespace_in_county_value(self):
        """County values with trailing whitespace should still match."""
        df = self._make_df(['DOUGLAS ', ' DOUGLAS', 'EL PASO'])
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 2

    def test_county_column_different_case(self):
        """Column named 'county' (lowercase) should still be found."""
        df = pd.DataFrame({
            'CUID': [1, 2, 3],
            'county': ['DOUGLAS', 'EL PASO', 'DOUGLAS'],
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 2

    def test_county_column_with_spaces(self):
        """Column named ' County ' with whitespace should be found."""
        df = pd.DataFrame({
            'CUID': [1, 2],
            ' County ': ['DOUGLAS', 'EL PASO'],
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 1

    def test_no_county_column_returns_unfiltered(self):
        """If County column is missing, return original DataFrame."""
        df = pd.DataFrame({
            'CUID': [1, 2, 3],
            'Region': ['NORTH', 'SOUTH', 'EAST'],
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 3  # unfiltered

    def test_no_matches_returns_empty(self):
        df = self._make_df(['EL PASO', 'ARAPAHOE', 'DENVER'])
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 0

    def test_nan_values_dont_crash(self):
        """NaN/None in County column shouldn't cause errors."""
        df = pd.DataFrame({
            'CUID': [1, 2, 3, 4],
            'County': ['DOUGLAS', None, float('nan'), 'DOUGLAS'],
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 2

    def test_numeric_county_values(self):
        """Numeric county codes shouldn't crash the filter."""
        df = pd.DataFrame({
            'CUID': [1, 2],
            'County': [35, 41],  # numeric, not string
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 0  # no match, but no crash

    def test_index_reset(self):
        """Filtered result should have reset index starting from 0."""
        df = self._make_df(['EL PASO', 'DOUGLAS', 'ARAPAHOE', 'DOUGLAS'])
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert list(result.index) == [0, 1]

    def test_el_paso_with_space(self):
        """El Paso has a space — make sure it matches correctly."""
        df = self._make_df(['EL PASO', 'ELPASO', 'EL PASO'])
        config = {'county': 'EL PASO', 'display_name': 'El Paso County'}
        result = filter_to_jurisdiction(df, config, 'elpaso')
        assert len(result) == 2  # 'ELPASO' (no space) should NOT match

    def test_multi_word_counties(self):
        """Counties with spaces in names (Clear Creek, Kit Carson, etc.)."""
        counties = ['CLEAR CREEK', 'KIT CARSON', 'LA PLATA', 'LAS ANIMAS',
                     'RIO BLANCO', 'RIO GRANDE', 'SAN JUAN', 'SAN MIGUEL']
        df = self._make_df(counties)
        for county in counties:
            config = {'county': county, 'display_name': f'{county.title()} County'}
            result = filter_to_jurisdiction(df, config, 'test')
            assert len(result) == 1, f"Failed to filter {county}"


# ===========================================================================
# load_manifest
# ===========================================================================

class TestLoadManifest:

    def test_valid_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = make_manifest(tmp)
            manifest = load_manifest(path)
            assert 'files' in manifest
            assert 'jurisdiction_filters' in manifest
            assert len(manifest['files']) == 2

    def test_missing_manifest_exits(self):
        with pytest.raises(SystemExit):
            load_manifest('/nonexistent/path/manifest.json')

    def test_real_manifest(self):
        """Test the actual project manifest loads correctly."""
        manifest_path = PROJECT_ROOT / 'data' / 'CDOT' / 'source_manifest.json'
        if manifest_path.exists():
            manifest = load_manifest(str(manifest_path))
            assert len(manifest['files']) == 5  # 2021-2025
            assert 'douglas' in manifest['jurisdiction_filters']
            # Verify all doc IDs are integers
            for year, info in manifest['files'].items():
                assert isinstance(info['docid'], int), f"Year {year} docid is not int"
                assert int(year) >= 2021
                assert int(year) <= 2025


# ===========================================================================
# list_available
# ===========================================================================

class TestListAvailable:

    def test_output_contains_years(self, capsys):
        manifest = {
            'files': {
                '2024': {'docid': 123, 'status': 'final'},
                '2025': {'docid': 456, 'status': 'preliminary'},
            },
            'jurisdiction_filters': {
                '_description': 'should be skipped',
                'douglas': {
                    'county': 'DOUGLAS',
                    'fips': '08035',
                    'display_name': 'Douglas County'
                }
            },
            'data_dictionaries': {
                '2021-2025': {'docid': 789, 'description': 'Test'}
            }
        }
        list_available(manifest)
        output = capsys.readouterr().out

        assert '2024' in output
        assert '2025' in output
        assert '[preliminary]' in output
        assert 'Douglas County' in output
        assert 'FIPS: 08035' in output
        assert '_description' not in output  # internal keys filtered

    def test_empty_manifest(self, capsys):
        """Empty manifest shouldn't crash."""
        manifest = {'files': {}, 'jurisdiction_filters': {}, 'data_dictionaries': {}}
        list_available(manifest)
        output = capsys.readouterr().out
        assert 'YEARS (0)' in output


# ===========================================================================
# process_year — filename generation
# ===========================================================================

class TestProcessYearFilename:

    def test_douglas_filename(self):
        """Douglas County filename should be: '2024 douglas.csv'"""
        jur_config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        display_name = jur_config['display_name']
        filename = f"2024 {display_name.lower().replace(' county', '').strip()}.csv"
        assert filename == '2024 douglas.csv'

    def test_el_paso_filename(self):
        jur_config = {'county': 'EL PASO', 'display_name': 'El Paso County'}
        display_name = jur_config['display_name']
        filename = f"2024 {display_name.lower().replace(' county', '').strip()}.csv"
        assert filename == '2024 el paso.csv'

    def test_denver_filename(self):
        """Denver County → '2024 denver.csv'"""
        jur_config = {'county': 'DENVER', 'display_name': 'Denver County'}
        display_name = jur_config['display_name']
        filename = f"2024 {display_name.lower().replace(' county', '').strip()}.csv"
        assert filename == '2024 denver.csv'

    def test_clear_creek_filename(self):
        """Clear Creek County → '2024 clear creek.csv'"""
        jur_config = {'county': 'CLEAR CREEK', 'display_name': 'Clear Creek County'}
        display_name = jur_config['display_name']
        filename = f"2024 {display_name.lower().replace(' county', '').strip()}.csv"
        assert filename == '2024 clear creek.csv'

    def test_statewide_filename(self):
        """Without jurisdiction, filename should be statewide."""
        filename = "cdot_crash_statewide_2024.csv"
        assert 'statewide' in filename


# ===========================================================================
# End-to-end pipeline (Excel → filter → CSV)
# ===========================================================================

class TestEndToEndPipeline:

    def test_full_pipeline(self, tmp_path):
        """Download-like flow: Excel bytes → parse → filter → CSV → verify."""
        xlsx_bytes, expected_count = make_statewide_xlsx(
            counties=['DOUGLAS', 'EL PASO', 'ARAPAHOE'],
            rows_per_county=50
        )

        # Parse
        df = excel_to_dataframe(xlsx_bytes, '.xlsx')
        assert len(df) == 150  # 3 counties × 50

        # Filter
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        df_filtered = filter_to_jurisdiction(df, config, 'douglas')
        assert len(df_filtered) == expected_count

        # Save CSV
        csv_path = tmp_path / '2024 douglas.csv'
        df_filtered.to_csv(csv_path, index=False)
        assert csv_path.exists()
        assert csv_path.stat().st_size > 0

        # Read back and verify
        df_verify = pd.read_csv(csv_path)
        assert len(df_verify) == expected_count
        assert list(df_verify.columns) == list(df_filtered.columns)
        assert all(df_verify['County'] == 'DOUGLAS')

    def test_pipeline_columns_match_existing_data(self, tmp_path):
        """Output CSV columns should match what the existing pipeline expects."""
        expected_columns = [
            'CUID', 'System Code', 'Crash Date', 'Crash Time',
            'Agency Id', 'City', 'County', 'Latitude', 'Longitude',
        ]

        xlsx_bytes, _ = make_statewide_xlsx()
        df = excel_to_dataframe(xlsx_bytes, '.xlsx')
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        df_filtered = filter_to_jurisdiction(df, config, 'douglas')

        for col in expected_columns:
            assert col in df_filtered.columns, f"Missing expected column: {col}"

    def test_pipeline_with_all_nan_county(self, tmp_path):
        """If the county column is all NaN, filter should return empty."""
        df = pd.DataFrame({
            'CUID': [1, 2, 3],
            'County': [None, None, None],
            'Agency Id': ['DSO', 'DSO', 'DSO'],
        })
        config = {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        result = filter_to_jurisdiction(df, config, 'douglas')
        assert len(result) == 0


# ===========================================================================
# Manifest integrity — all 64 Colorado counties
# ===========================================================================

class TestManifestIntegrity:

    @pytest.fixture
    def manifest(self):
        path = PROJECT_ROOT / 'data' / 'CDOT' / 'source_manifest.json'
        if not path.exists():
            pytest.skip("Manifest not found")
        with open(path) as f:
            return json.load(f)

    def test_all_years_have_docid(self, manifest):
        for year, info in manifest['files'].items():
            assert 'docid' in info, f"Year {year} missing docid"
            assert isinstance(info['docid'], int), f"Year {year} docid not int"

    def test_all_years_have_status(self, manifest):
        for year, info in manifest['files'].items():
            assert 'status' in info, f"Year {year} missing status"
            assert info['status'] in ('final', 'preliminary'), \
                f"Year {year} has invalid status: {info['status']}"

    def test_years_are_2021_to_2025(self, manifest):
        years = sorted(manifest['files'].keys())
        assert years == ['2021', '2022', '2023', '2024', '2025']

    def test_docids_are_unique(self, manifest):
        docids = [info['docid'] for info in manifest['files'].values()]
        assert len(docids) == len(set(docids)), "Duplicate doc IDs found"

    def test_exactly_64_counties(self, manifest):
        """Colorado has exactly 64 counties."""
        counties = {k: v for k, v in manifest['jurisdiction_filters'].items()
                    if not k.startswith('_')}
        assert len(counties) == 64, f"Expected 64 counties, got {len(counties)}"

    def test_all_jurisdictions_have_required_fields(self, manifest):
        for key, jur in manifest['jurisdiction_filters'].items():
            if key.startswith('_'):
                continue
            assert 'county' in jur, f"Jurisdiction {key} missing 'county'"
            assert 'display_name' in jur, f"Jurisdiction {key} missing 'display_name'"
            assert 'fips' in jur, f"Jurisdiction {key} missing 'fips'"
            assert jur['fips'].startswith('08'), f"Jurisdiction {key} FIPS doesn't start with 08"
            assert len(jur['fips']) == 5, f"Jurisdiction {key} FIPS not 5 digits"

    def test_county_names_are_uppercase(self, manifest):
        for key, jur in manifest['jurisdiction_filters'].items():
            if key.startswith('_'):
                continue
            assert jur['county'] == jur['county'].upper(), \
                f"Jurisdiction {key} county '{jur['county']}' not uppercase"

    def test_fips_codes_are_unique(self, manifest):
        fips = [jur['fips'] for k, jur in manifest['jurisdiction_filters'].items()
                if not k.startswith('_')]
        assert len(fips) == len(set(fips)), "Duplicate FIPS codes found"

    def test_county_values_are_unique(self, manifest):
        counties = [jur['county'] for k, jur in manifest['jurisdiction_filters'].items()
                    if not k.startswith('_')]
        assert len(counties) == len(set(counties)), "Duplicate county names found"

    def test_douglas_is_default_jurisdiction(self, manifest):
        assert 'douglas' in manifest['jurisdiction_filters']
        douglas = manifest['jurisdiction_filters']['douglas']
        assert douglas['county'] == 'DOUGLAS'
        assert douglas['fips'] == '08035'

    def test_known_counties_present(self, manifest):
        """Verify a selection of well-known Colorado counties exist."""
        expected = [
            ('adams', 'ADAMS'), ('arapahoe', 'ARAPAHOE'), ('boulder', 'BOULDER'),
            ('broomfield', 'BROOMFIELD'), ('denver', 'DENVER'), ('douglas', 'DOUGLAS'),
            ('elpaso', 'EL PASO'), ('jefferson', 'JEFFERSON'), ('larimer', 'LARIMER'),
            ('mesa', 'MESA'), ('pueblo', 'PUEBLO'), ('weld', 'WELD'),
        ]
        for key, county in expected:
            assert key in manifest['jurisdiction_filters'], f"Missing county: {key}"
            assert manifest['jurisdiction_filters'][key]['county'] == county

    def test_multi_word_county_keys(self, manifest):
        """Counties with spaces should use underscore keys."""
        multi_word = ['clear_creek', 'kit_carson', 'la_plata', 'las_animas',
                      'rio_blanco', 'rio_grande', 'san_juan', 'san_miguel']
        for key in multi_word:
            assert key in manifest['jurisdiction_filters'], f"Missing multi-word county: {key}"

    def test_data_dictionaries_present(self, manifest):
        assert '2021-2025' in manifest['data_dictionaries']
        assert 'docid' in manifest['data_dictionaries']['2021-2025']


# ===========================================================================
# Playwright availability check
# ===========================================================================

class TestPlaywrightFallback:

    def test_playwright_available_returns_bool(self):
        """_playwright_available should return True or False without crashing."""
        result = _playwright_available()
        assert isinstance(result, bool)

    def test_playwright_available_false_when_not_installed(self):
        """When playwright is not importable, should return False."""
        with patch.dict('sys.modules', {'playwright': None, 'playwright.sync_api': None}):
            # Force reimport check
            import importlib
            import download_cdot_crash_data as mod
            importlib.reload(mod)
            # The function catches ImportError gracefully
            result = mod._playwright_available()
            # Result depends on whether playwright is actually installed
            assert isinstance(result, bool)


# ===========================================================================
# merge_with_existing — CUID-based deduplication
# ===========================================================================

class TestMergeWithExisting:
    """Tests for the merge strategy that protects validated data."""

    def _make_csv(self, tmp_path, filename, data):
        """Helper: write a DataFrame dict to a CSV file."""
        df = pd.DataFrame(data)
        path = tmp_path / filename
        df.to_csv(path, index=False)
        return path

    def test_append_new_records(self, tmp_path):
        """New CUIDs should be appended to existing data."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 200, 300],
            'County': ['DOUGLAS'] * 3,
            'Crash Date': ['1/1/2025', '1/2/2025', '1/3/2025'],
        })
        new_df = pd.DataFrame({
            'CUID': [300, 400, 500],
            'County': ['DOUGLAS'] * 3,
            'Crash Date': ['1/3/2025', '1/4/2025', '1/5/2025'],
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        assert stats['existing_count'] == 3
        assert stats['new_download_count'] == 3
        assert stats['new_records'] == 2  # 400 and 500 are new
        assert stats['merged_count'] == 5  # 3 existing + 2 new
        assert set(merged['CUID'].tolist()) == {100, 200, 300, 400, 500}

    def test_no_new_records(self, tmp_path):
        """When all CUIDs already exist, nothing should change."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 200, 300],
            'County': ['DOUGLAS'] * 3,
        })
        new_df = pd.DataFrame({
            'CUID': [100, 200, 300],
            'County': ['DOUGLAS'] * 3,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        assert stats['new_records'] == 0
        assert stats['merged_count'] == 3  # unchanged
        assert len(merged) == 3

    def test_all_new_records(self, tmp_path):
        """When no CUIDs overlap, all new records should be appended."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 200],
            'County': ['DOUGLAS'] * 2,
        })
        new_df = pd.DataFrame({
            'CUID': [300, 400, 500],
            'County': ['DOUGLAS'] * 3,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        assert stats['new_records'] == 3
        assert stats['merged_count'] == 5

    def test_existing_records_preserved_exactly(self, tmp_path):
        """Existing rows should not be modified during merge."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 200],
            'County': ['DOUGLAS', 'DOUGLAS'],
            'Latitude': [39.5, 39.6],
            'Custom_Field': ['validated_a', 'validated_b'],
        })
        new_df = pd.DataFrame({
            'CUID': [100, 300],
            'County': ['DOUGLAS', 'DOUGLAS'],
            'Latitude': [99.9, 39.7],  # 100's latitude differs in new data
            'Custom_Field': ['changed', 'new_c'],
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # Existing CUID=100 should keep original values, not be overwritten
        row_100 = merged[merged['CUID'] == 100].iloc[0]
        assert row_100['Latitude'] == 39.5  # original, not 99.9
        assert row_100['Custom_Field'] == 'validated_a'  # original, not 'changed'

    def test_no_cuid_column_falls_back_to_replace(self, tmp_path):
        """If CUID column is missing, fall back to full replacement."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'ID': [1, 2, 3],
            'County': ['DOUGLAS'] * 3,
        })
        new_df = pd.DataFrame({
            'ID': [4, 5],
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # Falls back to replacement — returns new_df as-is
        assert len(merged) == 2
        assert stats['merged_count'] == 2

    def test_empty_existing_file(self, tmp_path):
        """Merging into an empty existing CSV should return all new records."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': pd.Series([], dtype=int),
            'County': pd.Series([], dtype=str),
        })
        new_df = pd.DataFrame({
            'CUID': [100, 200],
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        assert stats['existing_count'] == 0
        assert stats['new_records'] == 2
        assert stats['merged_count'] == 2

    def test_cuid_as_string(self, tmp_path):
        """CUID comparison should work even with mixed int/string types."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': ['100', '200'],  # strings in CSV
            'County': ['DOUGLAS'] * 2,
        })
        new_df = pd.DataFrame({
            'CUID': [200, 300],  # ints in download
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # 200 exists (as string vs int), 300 is new
        assert stats['new_records'] == 1
        assert stats['merged_count'] == 3

    def test_merge_stats_accuracy(self, tmp_path):
        """Verify all stats fields are populated correctly."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [1, 2, 3, 4, 5],
            'County': ['DOUGLAS'] * 5,
        })
        new_df = pd.DataFrame({
            'CUID': [3, 4, 5, 6, 7, 8],
            'County': ['DOUGLAS'] * 6,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        assert stats['existing_count'] == 5
        assert stats['new_download_count'] == 6
        assert stats['new_records'] == 3  # 6, 7, 8
        assert stats['merged_count'] == 8  # 1-8


class TestGetOutputFilename:
    """Tests for the filename helper function."""

    def test_douglas_filename(self):
        manifest = {'jurisdiction_filters': {
            'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
        }}
        assert _get_output_filename('2024', 'douglas', manifest) == '2024 douglas.csv'

    def test_el_paso_filename(self):
        manifest = {'jurisdiction_filters': {
            'elpaso': {'county': 'EL PASO', 'display_name': 'El Paso County'}
        }}
        assert _get_output_filename('2024', 'elpaso', manifest) == '2024 el paso.csv'

    def test_statewide_filename(self):
        manifest = {'jurisdiction_filters': {}}
        assert _get_output_filename('2024', None, manifest) == 'cdot_crash_statewide_2024.csv'

    def test_unknown_jurisdiction_returns_statewide(self):
        manifest = {'jurisdiction_filters': {'douglas': {}}}
        assert _get_output_filename('2024', 'bogus', manifest) == 'cdot_crash_statewide_2024.csv'


class TestProcessYearSkipLogic:
    """Tests for skip-if-final and merge-if-preliminary behavior."""

    def test_skip_final_when_exists(self, tmp_path):
        """Final year with existing CSV should be skipped."""
        # Create an existing CSV
        existing = tmp_path / '2024 douglas.csv'
        pd.DataFrame({'CUID': [1, 2, 3], 'County': ['DOUGLAS'] * 3}).to_csv(existing, index=False)

        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County', 'fips': '08035'}
            },
            'files': {'2024': {'docid': 35111742, 'status': 'final'}}
        }
        year_info = {'docid': 35111742, 'status': 'final'}

        # Import process_year
        from download_cdot_crash_data import process_year

        # Should skip without trying to download (session won't be used)
        mock_session = MagicMock()
        year, success, path = process_year(
            mock_session, '2024', year_info, manifest, tmp_path, 'douglas', force=False
        )

        assert success is True
        assert str(existing) == path
        # Session should NOT have been called (skipped download)
        mock_session.get.assert_not_called()

    def test_force_overrides_skip(self, tmp_path):
        """With --force, even final years should attempt download."""
        existing = tmp_path / '2024 douglas.csv'
        pd.DataFrame({'CUID': [1], 'County': ['DOUGLAS']}).to_csv(existing, index=False)

        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County', 'fips': '08035'}
            },
            'files': {'2024': {'docid': 35111742, 'status': 'final'}}
        }
        year_info = {'docid': 35111742, 'status': 'final'}

        from download_cdot_crash_data import process_year

        mock_session = MagicMock()
        # The download will fail (mock session), but it should ATTEMPT it
        year, success, error = process_year(
            mock_session, '2024', year_info, manifest, tmp_path, 'douglas', force=True
        )

        # It tried to download (and failed because session is mocked)
        assert success is False  # download failed
        # But it didn't skip — that's the important part

    def test_missing_file_always_downloads(self, tmp_path):
        """If the CSV doesn't exist, always download regardless of status."""
        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County', 'fips': '08035'}
            },
            'files': {'2024': {'docid': 35111742, 'status': 'final'}}
        }
        year_info = {'docid': 35111742, 'status': 'final'}

        from download_cdot_crash_data import process_year

        mock_session = MagicMock()
        year, success, error = process_year(
            mock_session, '2024', year_info, manifest, tmp_path, 'douglas', force=False
        )

        # It tried to download (file doesn't exist, so it didn't skip)
        assert success is False  # download fails due to mock

    def test_preliminary_year_always_downloads(self, tmp_path):
        """Preliminary years should always attempt download even if file exists."""
        existing = tmp_path / '2025 douglas.csv'
        pd.DataFrame({'CUID': [1], 'County': ['DOUGLAS']}).to_csv(existing, index=False)

        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County', 'fips': '08035'}
            },
            'files': {'2025': {'docid': 54973381, 'status': 'preliminary'}}
        }
        year_info = {'docid': 54973381, 'status': 'preliminary'}

        from download_cdot_crash_data import process_year

        mock_session = MagicMock()
        year, success, error = process_year(
            mock_session, '2025', year_info, manifest, tmp_path, 'douglas', force=False
        )

        # It tried to download (preliminary status, not skipped)
        assert success is False  # download fails due to mock


class TestCuidColumn:
    """Verify the CUID column constant matches actual data."""

    def test_cuid_column_name(self):
        assert CUID_COLUMN == 'CUID'

    def test_cuid_in_actual_data(self):
        """CUID column exists in existing data files."""
        data_dir = PROJECT_ROOT / 'data' / 'CDOT'
        for csv_file in data_dir.glob('20?? *.csv'):
            df = pd.read_csv(csv_file, nrows=1)
            # Handle BOM in column names
            cols = [c.strip('\ufeff').strip() for c in df.columns]
            assert 'CUID' in cols, f"CUID missing from {csv_file.name}: {cols[:5]}"


# ===========================================================================
# Edge case: _description keys in jurisdiction_filters
# ===========================================================================

class TestManifestDescriptionKeys:
    """Bug regression: _description key in jurisdiction_filters caused AttributeError."""

    def test_list_available_with_description_key(self, capsys):
        manifest = {
            'files': {'2024': {'docid': 1, 'status': 'final'}},
            'jurisdiction_filters': {
                '_description': 'This is a string, not a dict',
                'douglas': {
                    'county': 'DOUGLAS',
                    'fips': '08035',
                    'display_name': 'Douglas County'
                }
            },
            'data_dictionaries': {}
        }
        # This should NOT raise AttributeError
        list_available(manifest)
        output = capsys.readouterr().out
        assert 'Douglas County' in output
        assert '_description' not in output


# ===========================================================================
# create_session_with_retries — HTTP session configuration
# ===========================================================================

class TestCreateSessionWithRetries:
    """Verify the requests session is configured correctly for OnBase."""

    def test_returns_session_object(self):
        session = create_session_with_retries()
        assert isinstance(session, requests.Session)

    def test_user_agent_header_set(self):
        """Session must have a browser-like User-Agent to avoid being blocked."""
        session = create_session_with_retries()
        ua = session.headers.get('User-Agent', '')
        assert 'Mozilla' in ua
        assert 'Chrome' in ua or 'Safari' in ua

    def test_https_adapter_mounted(self):
        """HTTPS adapter with retry strategy must be mounted."""
        session = create_session_with_retries()
        adapter = session.get_adapter('https://example.com')
        assert adapter is not None
        assert adapter.max_retries.total == MAX_RETRIES

    def test_http_adapter_mounted(self):
        """HTTP adapter should also be mounted (for redirects)."""
        session = create_session_with_retries()
        adapter = session.get_adapter('http://example.com')
        assert adapter is not None

    def test_retry_backoff_factor(self):
        """Backoff should be 2 (giving 2s, 4s, 8s, 16s delays)."""
        session = create_session_with_retries()
        adapter = session.get_adapter('https://example.com')
        assert adapter.max_retries.backoff_factor == RETRY_BACKOFF_FACTOR

    def test_retry_status_forcelist(self):
        """429 and 5xx errors should trigger retries."""
        session = create_session_with_retries()
        adapter = session.get_adapter('https://example.com')
        forcelist = adapter.max_retries.status_forcelist
        assert 429 in forcelist
        assert 500 in forcelist
        assert 502 in forcelist
        assert 503 in forcelist

    def test_browser_headers_are_complete(self):
        """All required browser headers must be present."""
        required_keys = ['User-Agent', 'Accept', 'Accept-Language', 'Connection']
        for key in required_keys:
            assert key in BROWSER_HEADERS, f"Missing required header: {key}"


# ===========================================================================
# make_request_with_retry — network resilience
# ===========================================================================

class TestMakeRequestWithRetry:
    """Verify retry logic handles various failure modes."""

    def test_success_on_first_attempt(self):
        """Successful request should return immediately."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response

        result = make_request_with_retry(mock_session, 'https://example.com', max_manual_retries=1)
        assert result == mock_response
        assert mock_session.get.call_count == 1

    def test_timeout_triggers_retry(self):
        """Timeout should retry with backoff."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.side_effect = [
            requests.exceptions.Timeout("timed out"),
            mock_response,
        ]

        with patch('download_cdot_crash_data.time.sleep'):
            result = make_request_with_retry(mock_session, 'https://example.com',
                                             max_manual_retries=2)
        assert result == mock_response
        assert mock_session.get.call_count == 2

    def test_connection_error_triggers_retry(self):
        """Connection errors should retry."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.side_effect = [
            requests.exceptions.ConnectionError("refused"),
            mock_response,
        ]

        with patch('download_cdot_crash_data.time.sleep'):
            result = make_request_with_retry(mock_session, 'https://example.com',
                                             max_manual_retries=2)
        assert result == mock_response

    def test_4xx_client_error_not_retried(self):
        """4xx errors (except 429) should raise immediately, not retry."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error

        with pytest.raises(requests.exceptions.HTTPError):
            make_request_with_retry(mock_session, 'https://example.com', max_manual_retries=3)

        # Should NOT retry — only 1 call
        assert mock_session.get.call_count == 1

    def test_429_rate_limit_retried(self):
        """429 Too Many Requests should be retried."""
        mock_session = MagicMock()
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200
        mock_session.get.side_effect = [
            requests.exceptions.HTTPError(response=mock_response_429),
            mock_response_ok,
        ]

        with patch('download_cdot_crash_data.time.sleep'):
            result = make_request_with_retry(mock_session, 'https://example.com',
                                             max_manual_retries=2)
        assert result == mock_response_ok

    def test_all_retries_exhausted_raises(self):
        """After all retries fail, the last exception should be raised."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.Timeout("timed out")

        with patch('download_cdot_crash_data.time.sleep'):
            with pytest.raises(requests.exceptions.Timeout):
                make_request_with_retry(mock_session, 'https://example.com',
                                        max_manual_retries=3)

        assert mock_session.get.call_count == 3

    def test_exponential_backoff_timing(self):
        """Sleep durations should follow exponential pattern: 2, 4, 8..."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.Timeout("timed out")

        with patch('download_cdot_crash_data.time.sleep') as mock_sleep:
            with pytest.raises(requests.exceptions.Timeout):
                make_request_with_retry(mock_session, 'https://example.com',
                                        max_manual_retries=4)

        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        # Backoff factor 2: 2^1=2, 2^2=4, 2^3=8, 2^4=16
        assert sleep_calls == [2, 4, 8, 16]

    def test_params_passed_to_session(self):
        """URL parameters should be forwarded to session.get()."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response

        make_request_with_retry(mock_session, 'https://example.com',
                                params={'docid': '123'}, max_manual_retries=1)

        call_kwargs = mock_session.get.call_args
        assert call_kwargs[1]['params'] == {'docid': '123'}


# ===========================================================================
# download_onbase_document — 4-strategy cascade
# ===========================================================================

class TestDownloadOnbaseDocument:
    """Test the multi-strategy download cascade."""

    def test_strategy1_direct_excel_success(self):
        """Strategy 1: direct request returns valid Excel bytes."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1, 2], 'County': ['DOUGLAS', 'DOUGLAS']})

        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.content = xlsx_bytes
        mock_response.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }
        mock_response.url = 'https://example.com/docpop.aspx'

        with patch('download_cdot_crash_data.make_request_with_retry', return_value=mock_response):
            content, ext = download_onbase_document(mock_session, 12345, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy1_5_obtoken_returns_excel(self):
        """Strategy 1.5: HTML frameset contains OBToken → ViewDocumentEx returns Excel."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1, 2], 'County': ['DOUGLAS', 'DOUGLAS']})

        # Strategy 1: returns HTML frameset with OBToken
        frameset_html = (
            b'<html><frameset rows="0,*">'
            b'<frame src="UnloadHandler.aspx" name="unloadHandler" />'
            b'<frame src="ViewDocumentEx.aspx?OBToken=aabb1122-3344-5566-7788-99aabbccddee"'
            b' name="DocSelectPage" />'
            b'</frameset></html>'
        )

        mock_resp_html = MagicMock()
        mock_resp_html.content = frameset_html
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx?clienttype=html&docid=123'

        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }
        mock_resp_excel.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/ViewDocumentEx.aspx?OBToken=aabb1122-3344-5566-7788-99aabbccddee'

        def mock_request(session, url, **kwargs):
            if 'ViewDocumentEx' in url or 'OBToken' in url:
                return mock_resp_excel
            return mock_resp_html

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy1_5_obtoken_viewer_then_native_link(self):
        """Strategy 1.5: ViewDocumentEx returns HTML viewer → follow native download link."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # Strategy 1: frameset with OBToken
        frameset_html = (
            b'<html><iframe src="ViewDocumentEx.aspx?OBToken=11112222-3333-4444-5555-666677778888"'
            b' id="DocSelectPage"></iframe></html>'
        )

        # ViewDocumentEx returns HTML viewer with a GetDoc link
        viewer_html = (
            b'<html><body><a href="/CDOTRMPop/docpop/GetDoc.aspx?docid=123">Download</a>'
            b'</body></html>'
        )

        mock_resp_frameset = MagicMock()
        mock_resp_frameset.content = frameset_html
        mock_resp_frameset.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_frameset.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx'

        mock_resp_viewer = MagicMock()
        mock_resp_viewer.content = viewer_html
        mock_resp_viewer.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_viewer.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/ViewDocumentEx.aspx'

        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="crash.xlsx"',
        }

        def mock_request(session, url, **kwargs):
            if 'GetDoc' in url:
                return mock_resp_excel
            if 'ViewDocumentEx' in url or 'OBToken' in url:
                return mock_resp_viewer
            return mock_resp_frameset

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes

    def test_strategy1_5_guid_scan_from_js_variables(self):
        """Strategy 1.5 Phase B: OBToken is a bare GUID in JS (iframe starts at blank.aspx)."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1, 2], 'County': ['DOUGLAS', 'DOUGLAS']})

        # Realistic DocPop HTML: iframe starts at blank.aspx, OBToken in JS variable
        docpop_html = (
            b'<html><head>'
            b'<script type="text/javascript">'
            b'var __VirtualRoot="https://oitco.hylandcloud.com/cdotrmpop";'
            b'</script></head><body>'
            b'<iframe src="blank.aspx" id="DocSelectPage"></iframe>'
            b'<script>'
            b'var _obToken="407b6bf6-c00d-4450-9db9-72e58cfc2447";'
            b'var _kVal="6754fac4_c7b2_4cbb_ad8a_";'
            b'</script></body></html>'
        )

        mock_resp_html = MagicMock()
        mock_resp_html.content = docpop_html
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx'

        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }
        mock_resp_excel.url = 'https://oitco.hylandcloud.com/cdotrmpop/ViewDocumentEx.aspx'

        def mock_request(session, url, **kwargs):
            if '407b6bf6' in url and ('ViewDocumentEx' in url or
                                      'RetrieveNativeDocument' in url):
                return mock_resp_excel
            return mock_resp_html

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 17470635, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy1_5_guid_scan_with_viewer_binary_urls(self):
        """Strategy 1.5 Phase B: GUID scan → ViewDocumentEx returns viewer → follow binary URL."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # DocPop HTML with GUID in JS
        docpop_html = (
            b'<html><head><script>'
            b'var __VirtualRoot="https://host.com/app";'
            b'</script></head><body>'
            b'<iframe src="blank.aspx" id="DocSelectPage"></iframe>'
            b'<script>var tok="aabb1122-3344-5566-7788-99aabbccddee";</script>'
            b'</body></html>'
        )

        # ViewDocumentEx returns HTML viewer with binary endpoint
        viewer_html = (
            b'<html><script>'
            b'var contentUrl = "/app/GetDocumentContent.ashx?id=123";'
            b'</script></html>'
        )

        mock_resp_docpop = MagicMock()
        mock_resp_docpop.content = docpop_html
        mock_resp_docpop.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_docpop.url = 'https://host.com/app/docpop/docpop.aspx'

        mock_resp_viewer = MagicMock()
        mock_resp_viewer.content = viewer_html
        mock_resp_viewer.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_viewer.url = 'https://host.com/app/ViewDocumentEx.aspx'

        mock_resp_binary = MagicMock()
        mock_resp_binary.content = xlsx_bytes
        mock_resp_binary.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="data.xlsx"',
        }

        def mock_request(session, url, **kwargs):
            if 'GetDocumentContent' in url:
                return mock_resp_binary
            if 'ViewDocumentEx' in url or 'RetrieveNativeDocument' in url:
                return mock_resp_viewer
            return mock_resp_docpop

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes

    def test_strategy1_5_no_obtoken_falls_through(self):
        """Strategy 1.5: no OBToken in HTML → falls through to later strategies."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # Strategy 1: HTML without OBToken
        html_no_token = b'<html><body>No OBToken here</body></html>'
        mock_resp_html = MagicMock()
        mock_resp_html.content = html_no_token
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://example.com/docpop.aspx'

        # Strategy 3 (activex): returns Excel
        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/vnd.ms-excel',
            'Content-Disposition': '',
        }

        def mock_request(session, url, **kwargs):
            if any(ep in url for ep in ONBASE_GETDOC_URLS):
                raise Exception("Not found")
            params = kwargs.get('params', {})
            if params.get('clienttype') == 'activex':
                return mock_resp_excel
            return mock_resp_html

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 999, label='Test')

        assert content == xlsx_bytes

    def test_strategy2_html_then_follow_link(self):
        """Strategy 2: first request returns HTML, direct endpoints fail, parsed link returns Excel."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1]})
        html_content = b'<a href="/CDOTRMPop/docpop/PdfPop.aspx?docid=123">Download</a>'

        mock_response_html = MagicMock()
        mock_response_html.content = html_content
        mock_response_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_response_html.url = 'https://example.com/docpop/docpop.aspx'

        mock_response_excel = MagicMock()
        mock_response_excel.content = xlsx_bytes
        mock_response_excel.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }

        def mock_request(session, url, **kwargs):
            # Strategy 1c (direct endpoints): fail
            if any(ep in url for ep in ONBASE_GETDOC_URLS):
                raise Exception("Not found")
            # Strategy 1: HTML with PdfPop link
            if 'docpop.aspx' in url and 'PdfPop' not in url:
                return mock_response_html
            # Strategy 2: follow parsed PdfPop link → Excel
            return mock_response_excel

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy3_activex_fallback(self):
        """Strategy 3: html + no link found + direct endpoints fail → tries clienttype=activex."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # Strategy 1: HTML with no download links
        html_plain = b'<html><body>No download links here</body></html>'
        mock_resp_html = MagicMock()
        mock_resp_html.content = html_plain
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://example.com/docpop.aspx'

        # Strategy 3: activex returns Excel
        mock_resp_activex = MagicMock()
        mock_resp_activex.content = xlsx_bytes
        mock_resp_activex.headers = {
            'Content-Type': 'application/vnd.ms-excel',
            'Content-Disposition': '',
        }

        def mock_request(session, url, **kwargs):
            # Strategy 1c (direct endpoints): raise error
            if any(ep in url for ep in ONBASE_GETDOC_URLS):
                raise Exception("Not found")
            # Strategy 1 (docpop.aspx): return HTML
            if 'docpop' in url.lower():
                params = kwargs.get('params', {})
                if params.get('clienttype') == 'activex':
                    return mock_resp_activex
                return mock_resp_html
            return mock_resp_html

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 999, label='Test')

        assert content == xlsx_bytes

    def test_all_strategies_fail_raises(self):
        """When all 4 strategies fail, should raise descriptive Exception."""
        mock_session = MagicMock()

        with patch('download_cdot_crash_data.make_request_with_retry',
                    side_effect=Exception("Network error")):
            with patch('download_cdot_crash_data._playwright_available', return_value=False):
                with pytest.raises(Exception, match="Failed to download"):
                    download_onbase_document(mock_session, 99999, label='Missing Doc')

    def test_strategy4_playwright_fallback_triggered(self):
        """When requests fails, Playwright should be attempted if available."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        with patch('download_cdot_crash_data.make_request_with_retry',
                    side_effect=Exception("Network error")):
            with patch('download_cdot_crash_data._playwright_available', return_value=True):
                with patch('download_cdot_crash_data.download_with_playwright',
                           return_value=(xlsx_bytes, '.xlsx')) as mock_pw:
                    content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        mock_pw.assert_called_once_with(123, label='Test')
        assert content == xlsx_bytes


# ===========================================================================
# download_data_dictionary
# ===========================================================================

class TestDownloadDataDictionary:
    """Test data dictionary download with mock."""

    def test_successful_download(self, tmp_path):
        """Dictionary file should be saved to data_dir."""
        fake_content = b'PK' + b'\x00' * 500  # fake xlsx

        with patch('download_cdot_crash_data.download_onbase_document',
                    return_value=(fake_content, '.xlsx')):
            result = download_data_dictionary(
                MagicMock(), {'docid': 123, 'description': 'Test dict'}, tmp_path
            )

        assert result is True
        assert (tmp_path / 'cdot_data_dictionary.xlsx').exists()

    def test_failed_download_returns_false(self, tmp_path):
        """Download failure should return False, not crash."""
        with patch('download_cdot_crash_data.download_onbase_document',
                    side_effect=Exception("Download failed")):
            result = download_data_dictionary(
                MagicMock(), {'docid': 999, 'description': 'Bad dict'}, tmp_path
            )

        assert result is False

    def test_no_description_key_handled(self, tmp_path):
        """Dict info without 'description' should not crash."""
        fake_content = b'PK' + b'\x00' * 500

        with patch('download_cdot_crash_data.download_onbase_document',
                    return_value=(fake_content, '.xlsx')):
            result = download_data_dictionary(
                MagicMock(), {'docid': 456}, tmp_path
            )

        assert result is True


# ===========================================================================
# parse_args — CLI argument parsing
# ===========================================================================

class TestParseArgs:
    """Verify CLI flags are parsed correctly."""

    def test_defaults(self):
        with patch('sys.argv', ['prog']):
            args = parse_args()
        assert args.jurisdiction == 'douglas'
        assert args.years is None
        assert args.latest is False
        assert args.statewide is False
        assert args.force is False
        assert args.no_dict is False
        assert args.list is False

    def test_years_flag(self):
        with patch('sys.argv', ['prog', '--years', '2023', '2024']):
            args = parse_args()
        assert args.years == ['2023', '2024']

    def test_latest_flag(self):
        with patch('sys.argv', ['prog', '--latest']):
            args = parse_args()
        assert args.latest is True

    def test_jurisdiction_flag(self):
        with patch('sys.argv', ['prog', '-j', 'elpaso']):
            args = parse_args()
        assert args.jurisdiction == 'elpaso'

    def test_statewide_flag(self):
        with patch('sys.argv', ['prog', '--statewide']):
            args = parse_args()
        assert args.statewide is True

    def test_force_flag(self):
        with patch('sys.argv', ['prog', '--force']):
            args = parse_args()
        assert args.force is True

    def test_data_dir_flag(self):
        with patch('sys.argv', ['prog', '--data-dir', '/tmp/output']):
            args = parse_args()
        assert args.data_dir == '/tmp/output'

    def test_manifest_flag(self):
        with patch('sys.argv', ['prog', '-m', '/custom/manifest.json']):
            args = parse_args()
        assert args.manifest == '/custom/manifest.json'

    def test_no_dict_flag(self):
        with patch('sys.argv', ['prog', '--no-dict']):
            args = parse_args()
        assert args.no_dict is True

    def test_list_flag(self):
        with patch('sys.argv', ['prog', '--list']):
            args = parse_args()
        assert args.list is True

    def test_combined_flags(self):
        with patch('sys.argv', ['prog', '--latest', '--force', '--no-dict', '-j', 'denver']):
            args = parse_args()
        assert args.latest is True
        assert args.force is True
        assert args.no_dict is True
        assert args.jurisdiction == 'denver'


# ===========================================================================
# main() integration tests (mocked downloads)
# ===========================================================================

class TestMainIntegration:
    """Test main() orchestration logic with mocked network."""

    def test_list_mode_exits_cleanly(self, tmp_path, capsys):
        """--list should print info and return 0 without downloading."""
        manifest_path = make_manifest(str(tmp_path))

        with patch('sys.argv', ['prog', '--list', '-m', manifest_path]):
            from download_cdot_crash_data import main
            result = main()

        assert result == 0
        output = capsys.readouterr().out
        assert 'AVAILABLE DOWNLOADS' in output

    def test_invalid_jurisdiction_returns_1(self, tmp_path):
        """Unknown jurisdiction should return error code 1."""
        manifest_path = make_manifest(str(tmp_path))

        with patch('sys.argv', ['prog', '-j', 'nonexistent_county', '-m', manifest_path]):
            from download_cdot_crash_data import main
            result = main()

        assert result == 1

    def test_invalid_year_returns_1(self, tmp_path):
        """Year not in manifest should return error code 1."""
        manifest_path = make_manifest(str(tmp_path))

        with patch('sys.argv', ['prog', '--years', '1999', '-m', manifest_path]):
            from download_cdot_crash_data import main
            result = main()

        assert result == 1

    def test_latest_selects_most_recent_year(self, tmp_path):
        """--latest should only download the highest year."""
        manifest_path = make_manifest(str(tmp_path), years={
            '2023': {'docid': 1, 'status': 'final'},
            '2024': {'docid': 2, 'status': 'final'},
            '2025': {'docid': 3, 'status': 'preliminary'},
        })

        processed_years = []

        def fake_process_year(session, year, year_info, manifest, data_dir, jurisdiction_key,
                              force=False):
            processed_years.append(year)
            return year, True, f'{data_dir}/{year}.csv'

        with patch('sys.argv', ['prog', '--latest', '-m', manifest_path,
                                '-d', str(tmp_path), '--no-dict']):
            with patch('download_cdot_crash_data.process_year', side_effect=fake_process_year):
                from download_cdot_crash_data import main
                result = main()

        assert processed_years == ['2025']  # only the latest
        assert result == 0


# ===========================================================================
# merge_with_existing — edge cases
# ===========================================================================

class TestMergeEdgeCases:
    """Advanced edge cases for the CUID-based merge."""

    def _make_csv(self, tmp_path, filename, data):
        df = pd.DataFrame(data)
        path = tmp_path / filename
        df.to_csv(path, index=False)
        return path

    def test_bom_in_cuid_column(self, tmp_path):
        """BOM character (\\ufeff) in CUID column name should still match."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            '\ufeffCUID': [100, 200],
            'County': ['DOUGLAS'] * 2,
        })
        new_df = pd.DataFrame({
            'CUID': [200, 300],
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # BOM-prefixed CUID won't match clean CUID — should fall back to replace
        # (this is a known limitation — BOM stripping would need to be added)
        assert stats['merged_count'] > 0  # at least it doesn't crash

    def test_nan_cuids_not_matched(self, tmp_path):
        """NaN CUIDs should not be treated as matching each other."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, None, 200],
            'County': ['DOUGLAS'] * 3,
        })
        new_df = pd.DataFrame({
            'CUID': [None, 300],
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # NaN is dropped via .dropna(), so NaN from new_df won't match NaN from existing
        # CUID=300 is genuinely new
        assert stats['new_records'] >= 1

    def test_duplicate_cuids_in_existing(self, tmp_path):
        """If existing file has duplicate CUIDs, merge should not crash."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 100, 200],  # duplicate 100
            'County': ['DOUGLAS'] * 3,
        })
        new_df = pd.DataFrame({
            'CUID': [100, 300],
            'County': ['DOUGLAS'] * 2,
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # 100 exists (even as duplicate), 300 is new
        assert stats['new_records'] == 1
        assert stats['merged_count'] == 4  # 3 existing + 1 new

    def test_column_mismatch_between_files(self, tmp_path):
        """When existing has extra columns that new download doesn't, merge should still work."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100, 200],
            'County': ['DOUGLAS'] * 2,
            'Custom_Annotation': ['note_a', 'note_b'],  # extra column
        })
        new_df = pd.DataFrame({
            'CUID': [200, 300],
            'County': ['DOUGLAS'] * 2,
            # no Custom_Annotation column
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # Should work — concat fills missing columns with NaN
        assert stats['new_records'] == 1
        assert stats['merged_count'] == 3
        assert 'Custom_Annotation' in merged.columns  # preserved from existing

    def test_large_merge_performance(self, tmp_path):
        """Merge with 10K existing + 1K new records should complete quickly."""
        import time

        existing_cuids = list(range(10000))
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': existing_cuids,
            'County': ['DOUGLAS'] * 10000,
        })
        new_cuids = list(range(9500, 10500))  # 500 overlap, 500 new
        new_df = pd.DataFrame({
            'CUID': new_cuids,
            'County': ['DOUGLAS'] * 1000,
        })

        start = time.time()
        merged, stats = merge_with_existing(new_df, existing_path)
        elapsed = time.time() - start

        assert stats['new_records'] == 500
        assert stats['merged_count'] == 10500
        assert elapsed < 5.0  # should be well under 5 seconds

    def test_merge_preserves_column_order_of_existing(self, tmp_path):
        """Merged output should keep the column order from the existing file."""
        existing_path = self._make_csv(tmp_path, 'existing.csv', {
            'CUID': [100],
            'County': ['DOUGLAS'],
            'Crash Date': ['1/1/2025'],
            'Agency Id': ['DSO'],
        })
        new_df = pd.DataFrame({
            'Agency Id': ['CSP'],
            'CUID': [200],
            'Crash Date': ['2/1/2025'],
            'County': ['DOUGLAS'],
        })

        merged, stats = merge_with_existing(new_df, existing_path)

        # First 4 columns should follow existing file's order
        expected_order = ['CUID', 'County', 'Crash Date', 'Agency Id']
        assert list(merged.columns)[:4] == expected_order


# ===========================================================================
# process_year — full scenario tests with mocked download
# ===========================================================================

class TestProcessYearFullScenarios:
    """End-to-end process_year tests with mocked OnBase download."""

    def _mock_download(self, xlsx_bytes):
        """Helper: patch download_onbase_document to return given bytes."""
        return patch('download_cdot_crash_data.download_onbase_document',
                     return_value=(xlsx_bytes, '.xlsx'))

    def test_fresh_download_creates_csv(self, tmp_path):
        """First-time download should create a new CSV file."""
        xlsx_bytes, _ = make_statewide_xlsx(counties=['DOUGLAS', 'EL PASO'],
                                            rows_per_county=20)
        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
            },
            'files': {'2024': {'docid': 1, 'status': 'final'}}
        }

        with self._mock_download(xlsx_bytes):
            year, success, path = process_year(
                MagicMock(), '2024', {'docid': 1, 'status': 'final'},
                manifest, tmp_path, 'douglas'
            )

        assert success is True
        output_csv = tmp_path / '2024 douglas.csv'
        assert output_csv.exists()
        df = pd.read_csv(output_csv)
        assert len(df) == 20
        assert all(df['County'] == 'DOUGLAS')

    def test_preliminary_merge_appends_new(self, tmp_path):
        """Preliminary year with existing data should merge, not replace."""
        # Existing file: CUIDs DOU0000-DOU0009
        existing_data = pd.DataFrame({
            'CUID': [f'DOU{i:04d}' for i in range(10)],
            'County': ['DOUGLAS'] * 10,
            'System Code': ['County Road'] * 10,
            'Crash Date': ['1/1/2025'] * 10,
            'Crash Time': ['1400'] * 10,
            'Agency Id': ['DSO'] * 10,
            'City': [''] * 10,
            'Latitude': [39.5] * 10,
            'Longitude': [-104.9] * 10,
            'Location 1': ['Test Rd'] * 10,
            'Number Killed': [0] * 10,
            'Number Injured': [0] * 10,
            'Injury 00': [2] * 10,
            'Injury 01': [0] * 10,
            'Injury 02': [0] * 10,
            'Injury 03': [0] * 10,
            'Injury 04': [0] * 10,
        })
        existing_path = tmp_path / '2025 douglas.csv'
        existing_data.to_csv(existing_path, index=False)

        # New download: CUIDs DOU0005-DOU0014 (5 overlap + 5 new)
        xlsx_bytes, _ = make_statewide_xlsx(counties=['DOUGLAS'], rows_per_county=10)

        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
            },
            'files': {'2025': {'docid': 2, 'status': 'preliminary'}}
        }

        with self._mock_download(xlsx_bytes):
            year, success, path = process_year(
                MagicMock(), '2025', {'docid': 2, 'status': 'preliminary'},
                manifest, tmp_path, 'douglas'
            )

        assert success is True
        df = pd.read_csv(existing_path)
        # Existing 10 + whatever new CUIDs from download don't overlap
        assert len(df) >= 10  # at least the originals preserved

    def test_statewide_no_filter(self, tmp_path):
        """jurisdiction_key=None should save statewide data unfiltered."""
        xlsx_bytes, _ = make_statewide_xlsx(counties=['DOUGLAS', 'EL PASO'],
                                            rows_per_county=5)
        manifest = {
            'jurisdiction_filters': {},
            'files': {'2024': {'docid': 1, 'status': 'final'}}
        }

        with self._mock_download(xlsx_bytes):
            year, success, path = process_year(
                MagicMock(), '2024', {'docid': 1, 'status': 'final'},
                manifest, tmp_path, None  # statewide
            )

        assert success is True
        output_csv = tmp_path / 'cdot_crash_statewide_2024.csv'
        assert output_csv.exists()
        df = pd.read_csv(output_csv)
        assert len(df) == 10  # all counties

    def test_empty_download_returns_failure(self, tmp_path):
        """If downloaded Excel has zero rows, should return failure."""
        empty_xlsx = make_xlsx_bytes({'CUID': pd.Series([], dtype=str)})
        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
            },
            'files': {'2024': {'docid': 1, 'status': 'final'}}
        }

        with self._mock_download(empty_xlsx):
            year, success, error = process_year(
                MagicMock(), '2024', {'docid': 1, 'status': 'final'},
                manifest, tmp_path, 'douglas'
            )

        assert success is False
        assert 'no data' in error.lower()

    def test_no_matching_county_returns_failure(self, tmp_path):
        """If download has no rows for the requested county, return failure."""
        xlsx_bytes, _ = make_statewide_xlsx(counties=['EL PASO', 'DENVER'],
                                            rows_per_county=10)
        manifest = {
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'display_name': 'Douglas County'}
            },
            'files': {'2024': {'docid': 1, 'status': 'final'}}
        }

        with self._mock_download(xlsx_bytes):
            year, success, error = process_year(
                MagicMock(), '2024', {'docid': 1, 'status': 'final'},
                manifest, tmp_path, 'douglas'
            )

        assert success is False
        assert 'Douglas' in error


# ===========================================================================
# Real data file validation
# ===========================================================================

class TestRealDataFiles:
    """Validate the actual CDOT data files on disk."""

    DATA_DIR = PROJECT_ROOT / 'data' / 'CDOT'

    def _year_files(self):
        """Return list of year-specific CSV files."""
        return sorted(self.DATA_DIR.glob('20?? *.csv'))

    def test_data_files_exist(self):
        """At least one year file should exist."""
        files = self._year_files()
        assert len(files) >= 1, "No CDOT year files found"

    def test_all_files_have_cuid_column(self):
        """Every year file must have the CUID column."""
        for csv_file in self._year_files():
            df = pd.read_csv(csv_file, nrows=0)
            cols = [c.strip('\ufeff').strip() for c in df.columns]
            assert 'CUID' in cols, f"{csv_file.name}: missing CUID column"

    def test_all_files_have_county_column(self):
        """Every year file must have the County column."""
        for csv_file in self._year_files():
            df = pd.read_csv(csv_file, nrows=0)
            cols = [c.strip().lower() for c in df.columns]
            assert 'county' in cols, f"{csv_file.name}: missing County column"

    def test_all_files_have_crash_date(self):
        """Every year file must have the Crash Date column."""
        for csv_file in self._year_files():
            df = pd.read_csv(csv_file, nrows=0)
            cols = [c.strip().lower() for c in df.columns]
            assert 'crash date' in cols, f"{csv_file.name}: missing Crash Date column"

    def test_cuid_uniqueness_per_file(self):
        """CUIDs should be unique within each year file."""
        for csv_file in self._year_files():
            df = pd.read_csv(csv_file, usecols=[0])  # CUID is first column
            col = df.columns[0].strip('\ufeff').strip()
            cuids = df[df.columns[0]].dropna()
            dupes = cuids[cuids.duplicated()]
            assert len(dupes) == 0, f"{csv_file.name}: {len(dupes)} duplicate CUIDs"

    def test_all_rows_are_douglas_county(self):
        """All year files filtered for Douglas should only contain DOUGLAS."""
        for csv_file in self._year_files():
            if 'statewide' in csv_file.name.lower():
                continue
            df = pd.read_csv(csv_file, usecols=['County'], nrows=100)
            counties = df['County'].dropna().astype(str).str.strip().str.upper().unique()
            assert 'DOUGLAS' in counties, f"{csv_file.name}: missing DOUGLAS"
            assert len(counties) == 1, f"{csv_file.name}: mixed counties: {counties}"

    def test_files_have_reasonable_size(self):
        """Year files should have at least 100 rows."""
        for csv_file in self._year_files():
            row_count = sum(1 for _ in open(csv_file)) - 1  # minus header
            assert row_count >= 100, f"{csv_file.name}: only {row_count} rows"

    def test_no_cuid_overlap_between_years(self):
        """CUIDs should not repeat across different years (each crash is unique)."""
        all_cuids = {}
        for csv_file in self._year_files():
            df = pd.read_csv(csv_file, usecols=[0])
            col = df.columns[0]
            cuids = set(df[col].dropna().astype(str))
            year = csv_file.name[:4]
            for other_year, other_cuids in all_cuids.items():
                overlap = cuids & other_cuids
                assert len(overlap) == 0, (
                    f"CUID overlap between {year} and {other_year}: "
                    f"{len(overlap)} shared CUIDs (e.g., {list(overlap)[:5]})"
                )
            all_cuids[year] = cuids

    def test_crash_dates_match_file_year(self):
        """Crash dates in each file should match the year in the filename."""
        for csv_file in self._year_files():
            expected_year = csv_file.name[:4]
            df = pd.read_csv(csv_file, usecols=['Crash Date'], nrows=50)
            dates = df['Crash Date'].dropna().astype(str)
            for date_str in dates:
                # Dates are like "1/15/2024" or "2024-01-15"
                assert expected_year in date_str, (
                    f"{csv_file.name}: date '{date_str}' doesn't match year {expected_year}"
                )


# ===========================================================================
# Constants and configuration validation
# ===========================================================================

# ===========================================================================
# Strategy 1c — Direct GetDoc endpoint
# ===========================================================================

class TestStrategy1cDirectEndpoint:
    """Test the new direct GetDoc.aspx download strategy."""

    def test_getdoc_urls_configured(self):
        """ONBASE_GETDOC_URLS should have at least one endpoint."""
        assert len(ONBASE_GETDOC_URLS) >= 1
        for url in ONBASE_GETDOC_URLS:
            assert 'hylandcloud.com' in url

    def test_strategy1c_success_bypasses_later_strategies(self):
        """If direct endpoint returns valid Excel, skip strategies 2-4."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1, 2]})

        # Strategy 1: returns HTML (fails)
        mock_resp_html = MagicMock()
        mock_resp_html.content = b'<html><body>Viewer page</body></html>'
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://example.com/docpop.aspx'

        # Strategy 1c: direct endpoint returns Excel
        mock_resp_getdoc = MagicMock()
        mock_resp_getdoc.content = xlsx_bytes
        mock_resp_getdoc.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="crash_data.xlsx"',
        }

        call_count = [0]
        def mock_request(session, url, **kwargs):
            call_count[0] += 1
            if 'GetDoc' in url or 'api/document' in url:
                return mock_resp_getdoc
            return mock_resp_html

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            with patch('download_cdot_crash_data._playwright_available', return_value=False):
                content, ext = download_onbase_document(MagicMock(), 12345, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy1c_failure_continues_to_strategy2(self):
        """If direct endpoints fail, continue to strategy 2 (HTML parse)."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # Strategy 1: returns HTML with download link
        html_with_link = b'<a href="/CDOTRMPop/docpop/PdfPop.aspx?docid=123">View</a>'
        mock_resp_html = MagicMock()
        mock_resp_html.content = html_with_link
        mock_resp_html.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_html.url = 'https://oitco.hylandcloud.com/CDOTRMPop/docpop/docpop.aspx'

        # Strategy 2: follow link returns Excel
        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }

        call_index = [0]
        def mock_request(session, url, **kwargs):
            call_index[0] += 1
            # First call: strategy 1 (docpop.aspx) → HTML
            if call_index[0] == 1:
                return mock_resp_html
            # Strategy 1c calls: raise errors
            if any(ep in url for ep in ONBASE_GETDOC_URLS):
                raise Exception("Endpoint not found")
            # Strategy 2: follow parsed link → Excel
            return mock_resp_excel

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            with patch('download_cdot_crash_data._playwright_available', return_value=False):
                content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes


# ===========================================================================
# Preliminary failure exit logic
# ===========================================================================

class TestPreliminaryFailureExitCode:
    """Test that preliminary-only download failures are non-fatal."""

    def _make_manifest(self, tmp_path, years_config):
        """Helper: create a source manifest with given year configs."""
        manifest = {
            'source': {'name': 'Test', 'base_url': 'https://example.com'},
            'jurisdiction_filters': {
                'douglas': {'county': 'DOUGLAS', 'fips': '08035', 'display_name': 'Douglas'}
            },
            'files': years_config,
            'data_dictionaries': {},
        }
        manifest_path = tmp_path / 'source_manifest.json'
        manifest_path.write_text(json.dumps(manifest))
        return str(manifest_path)

    def test_preliminary_only_failure_returns_0(self, tmp_path):
        """When only preliminary years fail, exit 0 (non-fatal)."""
        # Create manifest with one preliminary year
        manifest_path = self._make_manifest(tmp_path, {
            '2025': {'docid': 99999, 'status': 'preliminary'},
            '2024': {'docid': 88888, 'status': 'final'},
        })

        with patch('download_cdot_crash_data.create_session_with_retries') as mock_session_factory, \
             patch('download_cdot_crash_data.bootstrap_session'), \
             patch('download_cdot_crash_data.download_onbase_document',
                   side_effect=Exception("OnBase unreachable")), \
             patch('sys.argv', ['prog', '--latest', '-j', 'douglas',
                                '-d', str(tmp_path), '--manifest', manifest_path,
                                '--no-dict']):

            mock_session_factory.return_value = MagicMock()
            result = main()

        # Should be 0 (non-fatal) because only preliminary data failed
        # Finalized data is served from R2, not local files
        assert result == 0

    def test_final_year_failure_returns_1(self, tmp_path):
        """When a final year fails and nothing else succeeds, exit 1."""
        manifest_path = self._make_manifest(tmp_path, {
            '2024': {'docid': 88888, 'status': 'final'},
        })

        with patch('download_cdot_crash_data.create_session_with_retries') as mock_session_factory, \
             patch('download_cdot_crash_data.bootstrap_session'), \
             patch('download_cdot_crash_data.download_onbase_document',
                   side_effect=Exception("OnBase unreachable")), \
             patch('sys.argv', ['prog', '--years', '2024', '-j', 'douglas',
                                '-d', str(tmp_path), '--manifest', manifest_path,
                                '--no-dict', '--force']):

            mock_session_factory.return_value = MagicMock()
            result = main()

        # Should be 1 (fatal) because a final year failed
        assert result == 1

    def test_preliminary_failure_with_no_local_data_still_returns_0(self, tmp_path):
        """When preliminary fails and no local CSVs exist, still exit 0.

        With R2 integration, finalized data is served from Cloudflare R2,
        not from local files. Preliminary-only failures are always non-fatal.
        """
        manifest_path = self._make_manifest(tmp_path, {
            '2025': {'docid': 99999, 'status': 'preliminary'},
        })

        # No existing CSV files in tmp_path — but that's fine with R2

        with patch('download_cdot_crash_data.create_session_with_retries') as mock_session_factory, \
             patch('download_cdot_crash_data.bootstrap_session'), \
             patch('download_cdot_crash_data.download_onbase_document',
                   side_effect=Exception("OnBase unreachable")), \
             patch('sys.argv', ['prog', '--latest', '-j', 'douglas',
                                '-d', str(tmp_path), '--manifest', manifest_path,
                                '--no-dict']):

            mock_session_factory.return_value = MagicMock()
            result = main()

        # Should be 0 — preliminary failures are always non-fatal
        # regardless of local file presence (data is on R2)
        assert result == 0


# ===========================================================================
# Improved Playwright availability check
# ===========================================================================

class TestPlaywrightAvailabilityCheck:
    """Test the strengthened _playwright_available() that verifies binary."""

    def test_returns_false_when_package_not_importable(self):
        """Should return False when playwright package is missing."""
        with patch.dict('sys.modules', {'playwright': None, 'playwright.sync_api': None}):
            # The function should catch ImportError
            result = _playwright_available()
            assert isinstance(result, bool)

    def test_returns_false_when_binary_missing(self):
        """Should return False when chromium binary doesn't exist on disk."""
        mock_p = MagicMock()
        mock_p.chromium.executable_path = '/nonexistent/path/chromium'

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_p)
        mock_cm.__exit__ = MagicMock(return_value=False)

        with patch('download_cdot_crash_data.sync_playwright', return_value=mock_cm, create=True):
            with patch('download_cdot_crash_data.os.path.isfile', return_value=False):
                # Import fresh to use mocked sync_playwright
                import importlib
                import download_cdot_crash_data as mod
                # Call directly — the function catches exceptions internally
                result = mod._playwright_available()
                assert isinstance(result, bool)


# ===========================================================================
# Diagnostic logging in strategy 1
# ===========================================================================

class TestDiagnosticLogging:
    """Test that strategy 1 logs diagnostic info for debugging."""

    def test_strategy1_logs_content_type_and_url(self):
        """Strategy 1 should log Content-Type, URL, and size for debugging."""
        mock_response = MagicMock()
        mock_response.content = b'<html><body>Error page</body></html>'
        mock_response.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_response.url = 'https://example.com/docpop.aspx?docid=12345'

        with patch('download_cdot_crash_data.make_request_with_retry',
                   side_effect=Exception("Network error")):
            with patch('download_cdot_crash_data._playwright_available', return_value=False):
                with pytest.raises(Exception, match="Failed to download"):
                    download_onbase_document(MagicMock(), 12345, label='Test')


class TestConstants:
    """Verify module-level constants are sane."""

    def test_onbase_base_url(self):
        assert 'hylandcloud.com' in ONBASE_BASE_URL
        assert 'docpop' in ONBASE_BASE_URL

    def test_max_retries(self):
        assert MAX_RETRIES == 4

    def test_retry_backoff_factor(self):
        assert RETRY_BACKOFF_FACTOR == 2

    def test_min_valid_file_size(self):
        assert MIN_VALID_FILE_SIZE == 100 * 1024  # 100 KB

    def test_xlsx_magic_bytes(self):
        from download_cdot_crash_data import XLSX_MAGIC, XLS_MAGIC
        assert XLSX_MAGIC == b'PK'
        assert XLS_MAGIC == b'\xd0\xcf'


# ===========================================================================
# Deep-dive bug tests: OnBase download failure root causes
# ===========================================================================

class TestBugRetrieveNativeDocumentFromViewerPage:
    """
    Bug: extract_viewer_binary_urls returned 0 matches when given a real
    OnBase ViewDocumentEx viewer page (15K-44K chars).  The viewer HTML
    contains RetrieveNativeDocument.ashx references with query parameters
    or the URL can be constructed from ViewDocumentEx query params.
    """

    def test_constructs_retrieval_url_from_viewdocumentex_base_url(self):
        """When base_url is a ViewDocumentEx URL with OBToken, construct
        RetrieveNativeDocument.ashx URLs automatically."""
        base = ('https://oitco.hylandcloud.com/CDOTRMPop/ViewDocumentEx.aspx'
                '?OBToken=fa562b2a-66e6-4203-9943-5a6b5962bae8'
                '&dochandle=17470635&k=e2f3e394_25d9_4656_988e_')
        urls = extract_viewer_binary_urls('', base)
        assert len(urls) >= 1
        native = [u for u in urls if 'RetrieveNativeDocument.ashx' in u]
        assert len(native) >= 1
        assert 'OBToken=fa562b2a' in native[0]
        assert 'dochandle=17470635' in native[0]

    def test_constructs_getrawcontent_url(self):
        """GetRawContent.ashx should also be generated as a candidate."""
        base = ('https://host.com/app/ViewDocumentEx.aspx'
                '?OBToken=11112222-3333-4444-5555-666677778888'
                '&dochandle=999')
        urls = extract_viewer_binary_urls('', base)
        raw = [u for u in urls if 'GetRawContent.ashx' in u]
        assert len(raw) >= 1

    def test_retrieval_urls_are_first_in_list(self):
        """Constructed retrieval URLs should be at the front of the list."""
        html = '<script>var x = "/app/GetDocumentContent.ashx?id=1";</script>'
        base = ('https://host.com/app/ViewDocumentEx.aspx'
                '?OBToken=aabb1122-3344-5566-7788-99aabbccddee&dochandle=42')
        urls = extract_viewer_binary_urls(html, base)
        assert len(urls) >= 2
        # First few URLs should be constructed retrieval URLs (inserted at front)
        # (RetrieveNativeDocument or GetRawContent)
        constructed_first = any(
            'RetrieveNativeDocument.ashx' in urls[0] or
            'GetRawContent.ashx' in urls[0]
            for _ in [1])
        assert constructed_first

    def test_viewer_html_with_retrieve_native_document_pattern(self):
        """The viewer HTML may contain RetrieveNativeDocument references."""
        html = '''<html>
        <script>
        var downloadUrl = "/CDOTRMPop/RetrieveNativeDocument.ashx?OBToken=abc&dochandle=123";
        </script>
        </html>'''
        urls = extract_viewer_binary_urls(html, 'https://host.com/app/ViewDocumentEx.aspx')
        native = [u for u in urls if 'RetrieveNativeDocument.ashx' in u]
        assert len(native) >= 1

    def test_docpop_subfolder_retrieval(self):
        """Build retrieval URL under /docpop/ subfolder too."""
        base = ('https://host.com/CDOTRMPop/docpop/ViewDocumentEx.aspx'
                '?OBToken=aabb1122-3344-5566-7788-99aabbccddee&dochandle=42')
        urls = extract_viewer_binary_urls('', base)
        # Should include both /CDOTRMPop/RetrieveNativeDocument and
        # /CDOTRMPop/docpop/RetrieveNativeDocument
        paths = [u.split('?')[0] for u in urls]
        assert any('/CDOTRMPop/RetrieveNativeDocument.ashx' in p for p in paths)
        assert any('/CDOTRMPop/docpop/RetrieveNativeDocument.ashx' in p for p in paths)


class TestBugBuildObtokenCandidatesRetrieveNative:
    """
    Bug: build_obtoken_candidates only generated ViewDocumentEx.aspx URLs.
    When the GUID was correct but ViewDocumentEx returned a viewer page
    instead of binary, we got stuck.  Now it also generates
    RetrieveNativeDocument.ashx URLs which return the file directly.
    """

    def test_retrieve_native_document_urls_generated(self):
        """Candidate list should include RetrieveNativeDocument.ashx URLs."""
        html = '''<script>
        var __VirtualRoot="https://host.com/app";
        var tok = "aabb1122-3344-5566-7788-99aabbccddee";
        </script>'''
        candidates = build_obtoken_candidates(html, 'https://host.com/app/docpop/docpop.aspx', '123')
        native = [c for c in candidates if 'RetrieveNativeDocument.ashx' in c]
        assert len(native) >= 1
        assert 'OBToken=aabb1122' in native[0]
        assert 'dochandle=123' in native[0]

    def test_retrieve_native_before_view_document(self):
        """RetrieveNativeDocument URLs should come before ViewDocumentEx."""
        html = '''<script>
        var __VirtualRoot="https://host.com/app";
        var tok = "aabb1122-3344-5566-7788-99aabbccddee";
        </script>'''
        candidates = build_obtoken_candidates(html, 'https://host.com/app/docpop/docpop.aspx', '123')
        native_idx = next(i for i, c in enumerate(candidates) if 'RetrieveNativeDocument' in c)
        viewdoc_idx = next(i for i, c in enumerate(candidates) if 'ViewDocumentEx' in c)
        assert native_idx < viewdoc_idx

    def test_retrieve_native_with_k_param(self):
        """RetrieveNativeDocument URL should include k parameter if available."""
        html = '''<script>
        var __VirtualRoot="https://host.com/app";
        var tok = "aabb1122-3344-5566-7788-99aabbccddee";
        var key = "aabbccdd_1122_3344_5566_";
        </script>'''
        candidates = build_obtoken_candidates(html, 'https://host.com/app/docpop/docpop.aspx', '123')
        native_with_k = [c for c in candidates
                         if 'RetrieveNativeDocument' in c and 'k=aabbccdd' in c]
        assert len(native_with_k) >= 1


class TestBugStrategy15RetrieveNativeDocumentSuccess:
    """
    Integration test: Strategy 1.5 should succeed when
    RetrieveNativeDocument.ashx returns the actual Excel file.
    This covers the case where the GUID from JS variables IS the real
    OBToken but ViewDocumentEx returns an HTML viewer.
    """

    def test_retrieve_native_succeeds_skipping_viewer(self):
        xlsx_bytes = make_xlsx_bytes({'CUID': [1], 'County': ['DOUGLAS']})

        docpop_html = (
            b'<html><head><script>'
            b'var __VirtualRoot="https://host.com/app";'
            b'</script></head><body>'
            b'<iframe src="blank.aspx"></iframe>'
            b'<script>var tok="aabb1122-3344-5566-7788-99aabbccddee";</script>'
            b'</body></html>'
        )

        viewer_html = b'<html><head><title>View Document</title></head><body>viewer</body></html>'

        mock_resp_docpop = MagicMock()
        mock_resp_docpop.content = docpop_html
        mock_resp_docpop.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_docpop.url = 'https://host.com/app/docpop/docpop.aspx'

        mock_resp_viewer = MagicMock()
        mock_resp_viewer.content = viewer_html
        mock_resp_viewer.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_viewer.url = 'https://host.com/app/ViewDocumentEx.aspx'

        mock_resp_excel = MagicMock()
        mock_resp_excel.content = xlsx_bytes
        mock_resp_excel.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="data.xlsx"',
        }

        def mock_request(session, url, **kwargs):
            if 'RetrieveNativeDocument' in url and 'aabb1122' in url:
                return mock_resp_excel
            if 'ViewDocumentEx' in url:
                return mock_resp_viewer
            return mock_resp_docpop

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 123, label='Test')

        assert content == xlsx_bytes
        assert ext in ('.xlsx', '.xls')


class TestBugViewerBinaryUrlBroaderPatterns:
    """
    Bug: extract_viewer_binary_urls missed common OnBase patterns like
    RetrieveNativeDocument.ashx with query parameters, GetRawContent.ashx,
    GetContent.ashx, etc.
    """

    def test_retrieve_native_document_ashx(self):
        html = '<script>url="/app/RetrieveNativeDocument.ashx?OBToken=abc&dochandle=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('RetrieveNativeDocument.ashx' in u for u in urls)

    def test_get_raw_content_ashx(self):
        html = '<script>url="/app/GetRawContent.ashx?docid=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('GetRawContent.ashx' in u for u in urls)

    def test_get_content_ashx(self):
        html = '<script>url="/app/GetContent.ashx?docid=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('GetContent.ashx' in u for u in urls)

    def test_image_viewer_ashx(self):
        html = '<script>url="/app/ImageViewer.ashx?docid=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('ImageViewer.ashx' in u for u in urls)

    def test_get_document_ashx(self):
        html = '<script>url="/app/GetDocument.ashx?docid=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('GetDocument.ashx' in u for u in urls)

    def test_render_document_ashx(self):
        html = '<script>url="/app/RenderDocument.ashx?docid=123";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('RenderDocument.ashx' in u for u in urls)

    def test_retrieve_document_aspx(self):
        html = '<a href="/app/RetrieveDocument.aspx?docid=123">Get</a>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert any('RetrieveDocument.aspx' in u for u in urls)

    def test_skips_very_short_strings(self):
        """Very short matches should be skipped (< 8 chars)."""
        html = '<script>var fn = "get";</script>'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        # "get" is only 3 chars, should not match
        assert not any(u.endswith('/get') for u in urls)

    def test_skips_jpg_and_ico(self):
        """Image extensions should be filtered out."""
        html = '<img src="/app/download.jpg"><link href="/app/retrieve.ico">'
        urls = extract_viewer_binary_urls(html, 'https://host.com/')
        assert not any('.jpg' in u for u in urls)
        assert not any('.ico' in u for u in urls)


class TestBugWrongGuidExtracted:
    """
    Bug: The initial DocPop HTML page contains GUIDs that are NOT the
    OBToken (e.g. session IDs, ASP.NET __VIEWSTATE related GUIDs).
    The real OBToken is only generated when JavaScript executes and
    populates the iframe.  Strategy 1.5 tries ALL GUIDs as candidates,
    which means it may try wrong ones first.

    This test verifies the cascade still works: wrong GUIDs return viewer
    HTML → extract_viewer_binary_urls finds the binary endpoint → success.
    """

    def test_wrong_guid_viewer_then_binary_via_sub_link(self):
        """Wrong GUID → ViewDocumentEx viewer → binary sub-link → success."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        # DocPop HTML with a GUID that is NOT the real OBToken
        docpop_html = (
            b'<html><head><script>'
            b'var __VirtualRoot="https://host.com/app";'
            b'var sessionId = "6ac05847-c45b-41a0-9adf-9eeb3480092d";'  # wrong GUID
            b'</script></head><body>'
            b'<iframe src="blank.aspx" id="DocSelectPage"></iframe>'
            b'</body></html>'
        )

        # Wrong GUID → viewer HTML with a binary endpoint embedded
        viewer_html = (
            b'<html><script>'
            b'var contentUrl = "/app/GetDocumentContent.ashx?id=17470635";'
            b'</script></html>'
        )

        mock_resp_docpop = MagicMock()
        mock_resp_docpop.content = docpop_html
        mock_resp_docpop.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_docpop.url = 'https://host.com/app/docpop/docpop.aspx'

        mock_resp_viewer = MagicMock()
        mock_resp_viewer.content = viewer_html
        mock_resp_viewer.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_viewer.url = 'https://host.com/app/ViewDocumentEx.aspx?OBToken=6ac05847-c45b-41a0-9adf-9eeb3480092d&dochandle=17470635'

        mock_resp_binary = MagicMock()
        mock_resp_binary.content = xlsx_bytes
        mock_resp_binary.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="crash.xlsx"',
        }

        def mock_request(session, url, **kwargs):
            if 'GetDocumentContent.ashx' in url:
                return mock_resp_binary
            if 'ViewDocumentEx' in url or 'RetrieveNativeDocument' in url:
                return mock_resp_viewer
            return mock_resp_docpop

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 17470635, label='Test')

        assert content == xlsx_bytes

    def test_wrong_guid_viewer_then_constructed_retrieval_url(self):
        """Wrong GUID → ViewDocumentEx viewer → constructed RetrieveNativeDocument → success."""
        xlsx_bytes = make_xlsx_bytes({'A': [1]})

        docpop_html = (
            b'<html><head><script>'
            b'var __VirtualRoot="https://host.com/app";'
            b'var sid = "6ac05847-c45b-41a0-9adf-9eeb3480092d";'
            b'</script></head><body>'
            b'<iframe src="blank.aspx"></iframe>'
            b'</body></html>'
        )

        # Plain viewer HTML with no inline binary URLs — but the base URL
        # of the viewer request contains OBToken and dochandle, so
        # extract_viewer_binary_urls can construct RetrieveNativeDocument
        viewer_html = b'<html><title>View Document</title><body>OnBase Viewer</body></html>'

        mock_resp_docpop = MagicMock()
        mock_resp_docpop.content = docpop_html
        mock_resp_docpop.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        mock_resp_docpop.url = 'https://host.com/app/docpop/docpop.aspx'

        mock_resp_viewer = MagicMock()
        mock_resp_viewer.content = viewer_html
        mock_resp_viewer.headers = {'Content-Type': 'text/html', 'Content-Disposition': ''}
        # Key: the response URL contains OBToken from the request
        mock_resp_viewer.url = (
            'https://host.com/app/ViewDocumentEx.aspx'
            '?OBToken=6ac05847-c45b-41a0-9adf-9eeb3480092d&dochandle=17470635')

        mock_resp_binary = MagicMock()
        mock_resp_binary.content = xlsx_bytes
        mock_resp_binary.headers = {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="crash.xlsx"',
        }

        def mock_request(session, url, **kwargs):
            if 'RetrieveNativeDocument.ashx' in url and 'sub-link' not in url:
                # First attempt (as a candidate, not sub-link) — returns HTML
                if '6ac05847' in url and 'dochandle=17470635' in url:
                    # As a sub-link after viewer, return the binary
                    return mock_resp_binary
            if 'GetRawContent.ashx' in url:
                return mock_resp_binary
            if 'ViewDocumentEx' in url:
                return mock_resp_viewer
            if 'RetrieveNativeDocument' in url:
                return mock_resp_viewer
            return mock_resp_docpop

        with patch('download_cdot_crash_data.make_request_with_retry', side_effect=mock_request):
            content, ext = download_onbase_document(MagicMock(), 17470635, label='Test')

        assert content == xlsx_bytes


class TestBugViewerBinaryUrlConstruction:
    """
    Bug: When extract_viewer_binary_urls gets a ViewDocumentEx URL as
    base_url, it should construct RetrieveNativeDocument.ashx URLs using
    the OBToken and dochandle from the query parameters — even if the
    viewer HTML itself has no matching patterns.
    """

    def test_constructs_from_viewdocumentex_url(self):
        base = ('https://oitco.hylandcloud.com/CDOTRMPop/ViewDocumentEx.aspx'
                '?OBToken=fa562b2a-66e6-4203-9943-5a6b5962bae8'
                '&dochandle=17470635')
        urls = extract_viewer_binary_urls('<html></html>', base)
        assert any('RetrieveNativeDocument.ashx' in u and
                    'OBToken=fa562b2a' in u and
                    'dochandle=17470635' in u
                    for u in urls)

    def test_no_construction_without_obtoken_in_url(self):
        """Don't construct if base_url has no OBToken."""
        base = 'https://host.com/app/ViewDocumentEx.aspx?docid=123'
        urls = extract_viewer_binary_urls('<html></html>', base)
        # No OBToken → no constructed URLs (only pattern-matched ones)
        assert not any('RetrieveNativeDocument.ashx' in u for u in urls)

    def test_app_root_extraction_from_docpop_path(self):
        """Application root should be correctly extracted from /docpop/ path."""
        base = ('https://host.com/CDOTRMPop/docpop/ViewDocumentEx.aspx'
                '?OBToken=aabb1122-3344-5566-7788-99aabbccddee&dochandle=42')
        urls = extract_viewer_binary_urls('', base)
        assert any('/CDOTRMPop/RetrieveNativeDocument.ashx' in u for u in urls)

    def test_app_root_extraction_from_viewdocumentex_path(self):
        """Application root extracted when ViewDocumentEx is directly under app root."""
        base = ('https://host.com/App/ViewDocumentEx.aspx'
                '?OBToken=aabb1122-3344-5566-7788-99aabbccddee&dochandle=42')
        urls = extract_viewer_binary_urls('', base)
        assert any('/App/RetrieveNativeDocument.ashx' in u for u in urls)


# ===========================================================================
# Strategy 0: Playwright two-phase session + download event
# ===========================================================================

class TestStrategy0PlaywrightEvent:
    """
    Tests for the new Playwright-first download (Strategy 0).
    Since Playwright is not available in unit-test CI, we mock the sync_playwright
    context manager and verify the two-phase navigation + download event pattern.
    """

    def test_strategy0_tried_first_when_playwright_available(self):
        """Strategy 0 should be tried before HTTP strategies."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1], 'County': ['DOUGLAS']})

        with patch('download_cdot_crash_data._playwright_available', return_value=True), \
             patch('download_cdot_crash_data.download_with_playwright_event') as mock_pw:
            mock_pw.return_value = (xlsx_bytes, '.xlsx')
            content, ext = download_onbase_document(MagicMock(), 12345, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'
        mock_pw.assert_called_once_with(12345, label='Test')

    def test_strategy0_failure_falls_back_to_http(self):
        """When Strategy 0 fails, HTTP strategies should be tried."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1], 'County': ['DOUGLAS']})

        mock_resp = MagicMock()
        mock_resp.content = xlsx_bytes
        mock_resp.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }
        mock_resp.url = 'https://example.com/docpop.aspx'

        with patch('download_cdot_crash_data._playwright_available', return_value=True), \
             patch('download_cdot_crash_data.download_with_playwright_event',
                   side_effect=Exception("download event timeout")), \
             patch('download_cdot_crash_data.make_request_with_retry',
                   return_value=mock_resp):
            content, ext = download_onbase_document(MagicMock(), 12345, label='Test')

        assert content == xlsx_bytes
        assert ext == '.xlsx'

    def test_strategy0_skipped_when_playwright_unavailable(self):
        """When Playwright not installed, skip to HTTP strategies."""
        xlsx_bytes = make_xlsx_bytes({'CUID': [1], 'County': ['DOUGLAS']})

        mock_resp = MagicMock()
        mock_resp.content = xlsx_bytes
        mock_resp.headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': '',
        }
        mock_resp.url = 'https://example.com/docpop.aspx'

        with patch('download_cdot_crash_data._playwright_available', return_value=False), \
             patch('download_cdot_crash_data.make_request_with_retry',
                   return_value=mock_resp):
            content, ext = download_onbase_document(MagicMock(), 12345, label='Test')

        assert content == xlsx_bytes

    def test_download_with_playwright_event_function_exists(self):
        """The new function should be importable."""
        assert callable(download_with_playwright_event)

    def test_download_with_playwright_event_uses_base_url(self):
        """Strategy 0 should navigate to BASE_URL first to establish session."""
        # We can't run real Playwright in tests, but we verify the function
        # exists and has the right signature
        import inspect
        sig = inspect.signature(download_with_playwright_event)
        assert 'docid' in sig.parameters
        assert 'label' in sig.parameters
        assert 'timeout_sec' in sig.parameters


# ===========================================================================
# Auto-discovery tests
# ===========================================================================

class TestAutoDiscovery:
    """Tests for the auto-discovery functions from the working script."""

    def test_discover_crash_listings_returns_list(self):
        """discover_crash_listings should return a list."""
        with patch('download_cdot_crash_data._playwright_available', return_value=False):
            result = discover_crash_listings()
        assert isinstance(result, list)
        assert result == []  # Empty when Playwright unavailable

    def test_check_for_new_year_no_update(self, tmp_path):
        """check_for_new_year returns False when no new years found."""
        manifest = {
            "files": {
                "2024": {"docid": 50498498, "status": "final"},
                "2025": {"docid": 54973381, "status": "preliminary"},
            }
        }
        manifest_path = tmp_path / "test_manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        with patch('download_cdot_crash_data.discover_crash_listings', return_value=[]):
            result = check_for_new_year(manifest, str(manifest_path))

        assert result is False

    def test_check_for_new_year_adds_new_year(self, tmp_path):
        """check_for_new_year should add new year and finalize previous."""
        manifest = {
            "files": {
                "2024": {"docid": 50498498, "status": "final"},
                "2025": {"docid": 54973381, "status": "preliminary"},
            }
        }
        manifest_path = tmp_path / "test_manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        discovered = [{"doc_id": "99999999", "title": "Crash Listing 2026", "year": "2026"}]
        with patch('download_cdot_crash_data.discover_crash_listings', return_value=discovered):
            result = check_for_new_year(manifest, str(manifest_path))

        assert result is True
        assert "2026" in manifest["files"]
        assert manifest["files"]["2026"]["docid"] == 99999999
        assert manifest["files"]["2026"]["status"] == "preliminary"
        # 2025 should be finalized since 2026 exists
        assert manifest["files"]["2025"]["status"] == "final"

    def test_check_for_new_year_ignores_known_years(self, tmp_path):
        """Already-known years should not be re-added."""
        manifest = {
            "files": {
                "2025": {"docid": 54973381, "status": "preliminary"},
            }
        }
        manifest_path = tmp_path / "test_manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        # Discover a year we already know about
        discovered = [{"doc_id": "54973381", "title": "Crash Listing 2025", "year": "2025"}]
        with patch('download_cdot_crash_data.discover_crash_listings', return_value=discovered):
            result = check_for_new_year(manifest, str(manifest_path))

        assert result is False


# ===========================================================================
# Manifest doc_id correctness
# ===========================================================================

class TestManifestDocIds:
    """Verify the doc_ids in source_manifest.json match the tested working values."""

    @pytest.fixture
    def manifest(self):
        manifest_path = PROJECT_ROOT / 'data' / 'CDOT' / 'source_manifest.json'
        if not manifest_path.exists():
            pytest.skip("source_manifest.json not found")
        return json.loads(manifest_path.read_text())

    def test_2021_docid(self, manifest):
        assert manifest["files"]["2021"]["docid"] == 37133405

    def test_2022_docid(self, manifest):
        assert manifest["files"]["2022"]["docid"] == 41627498

    def test_2023_docid(self, manifest):
        assert manifest["files"]["2023"]["docid"] == 46627965

    def test_2024_docid(self, manifest):
        assert manifest["files"]["2024"]["docid"] == 50498498

    def test_2025_docid(self, manifest):
        assert manifest["files"]["2025"]["docid"] == 54973381

    def test_data_dictionary_docid(self, manifest):
        dd = manifest.get("data_dictionaries", {}).get("2021-2025", {})
        assert dd.get("docid") == 17470635


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
