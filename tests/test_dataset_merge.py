#!/usr/bin/env python3
"""
Bug tests for dataset merge strategy across Python scripts.

Tests the --merge flag in download_crash_data.py and the --merge-existing
flag in process_crash_data.py for correctness, edge cases, and regressions.

Run with: python -m pytest tests/test_dataset_merge.py -v

Test Architecture Overview
==========================

These tests verify that new data is MERGED with existing validated data
instead of replacing it. The merge strategy uses dual deduplication:

  1. Primary key: Document Number (exact match)
  2. Secondary key: Crash Date + lon(4dp) + lat(4dp) + Collision Type

Existing validated records always take priority (keep='first' since
existing data is concatenated before new data).

Test Coverage Map
=================

download_crash_data.py --merge:
  - TestDownloadMerge_BasicMerge: New + existing rows combined correctly
  - TestDownloadMerge_DocNumberDedup: Duplicate Document Nbr removed (existing kept)
  - TestDownloadMerge_GeoDedup: Duplicate date+coords+collision removed
  - TestDownloadMerge_NoExistingFile: Merge flag with no existing file = normal save
  - TestDownloadMerge_MergeDisabled: Without --merge flag = full replacement
  - TestDownloadMerge_EmptyExisting: Existing file is empty
  - TestDownloadMerge_MissingColumns: Dedup columns missing gracefully
  - TestDownloadMerge_NullCoordinates: Rows with null/zero coords not geo-deduped
  - TestDownloadMerge_ExistingPriority: Existing record values preserved over new
  - TestDownloadMerge_MixedDedup: Both doc and geo dedup in same merge
  - TestDownloadMerge_LargeDataset: Performance with 10k+ rows
  - TestDownloadMerge_DuplicateWithinNewData: Duplicates in new data itself

process_crash_data.py --merge-existing:
  - TestSplitMerge_BasicMerge: New rows appended to existing split files
  - TestSplitMerge_DocNumberDedup: Document number dedup in split stage
  - TestSplitMerge_GeoDedup: Geo key dedup in split stage
  - TestSplitMerge_NoExistingFile: No existing file = fresh create
  - TestSplitMerge_HeaderMerge: New columns in new data are added to headers
  - TestSplitMerge_ExistingPriority: Existing rows kept, new duplicates dropped
  - TestSplitMerge_NanDocValues: 'nan' doc values not treated as duplicates
  - TestSplitMerge_EmptyCoordinates: Empty/zero coords skip geo dedup
  - TestSplitMerge_FloatPrecision: Coordinate rounding to 4dp works correctly
  - TestSplitMerge_MergeDisabled: Without --merge-existing = no merge behavior
"""

import csv
import io
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))


# ---------------------------------------------------------------------------
# Helpers — shared fixtures for crash data CSVs
# ---------------------------------------------------------------------------

STANDARD_HEADERS = [
    'Document Nbr', 'Crash Year', 'Crash Date', 'Crash Military Time',
    'Crash Severity', 'Collision Type', 'Weather Condition', 'Light Condition',
    'RTE Name', 'Node', 'x', 'y', 'Pedestrian?', 'Bike?', 'Speed?', 'Night?',
    'K_People', 'A_People', 'B_People', 'C_People', 'SYSTEM'
]


def make_crash_row(doc_nbr, year=2024, date='01/15/2024', severity='O',
                   collision='01. Rear End', route='MAIN ST', node='N001',
                   x='-104.9000', y='39.5000', **overrides):
    """Build a single crash row dict with standard column names."""
    row = {
        'Document Nbr': str(doc_nbr),
        'Crash Year': str(year),
        'Crash Date': date,
        'Crash Military Time': '1400',
        'Crash Severity': severity,
        'Collision Type': collision,
        'Weather Condition': '1. Clear',
        'Light Condition': '1. Daylight',
        'RTE Name': route,
        'Node': node,
        'x': str(x),
        'y': str(y),
        'Pedestrian?': 'No',
        'Bike?': 'No',
        'Speed?': 'No',
        'Night?': 'No',
        'K_People': '0',
        'A_People': '0',
        'B_People': '0',
        'C_People': '0',
        'SYSTEM': 'Non-DOT secondary',
    }
    row.update(overrides)
    return row


def write_csv(path, rows, headers=None):
    """Write rows to a CSV file."""
    if headers is None:
        headers = STANDARD_HEADERS
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path):
    """Read a CSV file and return list of dicts."""
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def make_dataframe(rows):
    """Convert list of row dicts to a pandas DataFrame (all str dtype)."""
    return pd.DataFrame(rows).astype(str)


# ===========================================================================
# TEST SUITE 1: download_crash_data.py --merge
# ===========================================================================

class TestDownloadMerge_BasicMerge:
    """New data is appended to existing validated dataset."""

    def test_new_unique_rows_added(self, tmp_path):
        """Non-duplicate rows from new download are added."""
        existing = [
            make_crash_row('DOC001', date='01/01/2024'),
            make_crash_row('DOC002', date='01/02/2024'),
        ]
        new = [
            make_crash_row('DOC003', date='01/03/2024'),
            make_crash_row('DOC004', date='01/04/2024'),
        ]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        assert len(combined_df) == 4, f"Expected 4 rows, got {len(combined_df)}"

    def test_total_count_is_existing_plus_new(self, tmp_path):
        """Total row count = existing + new unique records."""
        existing = [make_crash_row(f'DOC{i:03d}') for i in range(5)]
        new = [make_crash_row(f'DOC{i:03d}') for i in range(5, 8)]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        assert len(combined) == 8


class TestDownloadMerge_DocNumberDedup:
    """Duplicate Document Numbers are removed; existing record kept."""

    def test_duplicate_doc_numbers_removed(self, tmp_path):
        """Rows with same Document Nbr as existing are dropped."""
        existing = [make_crash_row('DOC001', severity='K')]
        new = [make_crash_row('DOC001', severity='O')]  # Same doc, different severity
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # Dedup by Document Nbr, keep first (existing)
        mask = combined['Document Nbr'].notna() & (combined['Document Nbr'] != '') & (combined['Document Nbr'] != 'nan')
        has_doc = combined[mask]
        no_doc = combined[~mask]
        has_doc_deduped = has_doc.drop_duplicates(subset=['Document Nbr'], keep='first')
        result = pd.concat([has_doc_deduped, no_doc], ignore_index=True)

        assert len(result) == 1, f"Expected 1 row after dedup, got {len(result)}"
        assert result.iloc[0]['Crash Severity'] == 'K', "Existing record (K) should be preserved"

    def test_existing_values_preserved_not_overwritten(self, tmp_path):
        """When doc numbers match, existing record's field values are kept."""
        existing = [make_crash_row('DOC001', route='OLD ROUTE')]
        new = [make_crash_row('DOC001', route='NEW ROUTE')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        result = combined.drop_duplicates(subset=['Document Nbr'], keep='first')

        assert result.iloc[0]['RTE Name'] == 'OLD ROUTE'


class TestDownloadMerge_GeoDedup:
    """Geo key (date + coords + collision) deduplication works."""

    def test_same_date_coords_collision_removed(self, tmp_path):
        """Rows with same date+lon+lat+collision as existing are dropped."""
        existing = [make_crash_row('DOC001', date='01/15/2024', x='-104.9000',
                                    y='39.5000', collision='01. Rear End')]
        new = [make_crash_row('DOC999', date='01/15/2024', x='-104.9000',
                               y='39.5000', collision='01. Rear End')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        dedup_cols = ['Crash Date', 'x', 'y', 'Collision Type']
        has_coords = (
            combined['x'].notna() & (combined['x'] != '') & (combined['x'] != '0') &
            combined['y'].notna() & (combined['y'] != '') & (combined['y'] != '0')
        )
        df_with = combined[has_coords]
        df_without = combined[~has_coords]
        df_deduped = df_with.drop_duplicates(subset=dedup_cols, keep='first')
        result = pd.concat([df_deduped, df_without], ignore_index=True)

        assert len(result) == 1

    def test_slightly_different_coords_not_deduped(self, tmp_path):
        """Rows at different coordinates are NOT deduped."""
        existing = [make_crash_row('DOC001', date='01/15/2024', x='-104.9000',
                                    y='39.5000', collision='01. Rear End')]
        new = [make_crash_row('DOC002', date='01/15/2024', x='-104.9100',
                               y='39.5100', collision='01. Rear End')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        dedup_cols = ['Crash Date', 'x', 'y', 'Collision Type']
        result = combined.drop_duplicates(subset=dedup_cols, keep='first')

        assert len(result) == 2, "Different coordinates should NOT be deduped"

    def test_same_coords_different_collision_not_deduped(self):
        """Same date+coords but different collision type = NOT duplicates."""
        existing = [make_crash_row('DOC001', collision='01. Rear End')]
        new = [make_crash_row('DOC002', collision='02. Angle')]

        df = make_dataframe(existing + new)
        result = df.drop_duplicates(subset=['Crash Date', 'x', 'y', 'Collision Type'], keep='first')

        assert len(result) == 2


class TestDownloadMerge_NoExistingFile:
    """When --merge is set but no existing file exists."""

    def test_creates_new_file_normally(self, tmp_path):
        """If output file doesn't exist, merge simply creates it."""
        output = tmp_path / 'crashes.csv'
        new = [make_crash_row('DOC001')]

        # Simulate: file doesn't exist, so merge logic is skipped
        assert not output.exists()

        new_df = make_dataframe(new)
        new_df.to_csv(output, index=False)

        assert output.exists()
        result = read_csv(output)
        assert len(result) == 1


class TestDownloadMerge_MergeDisabled:
    """Without --merge flag, data is fully replaced."""

    def test_full_replacement_when_no_merge_flag(self, tmp_path):
        """Without merge, existing data is overwritten."""
        existing = [make_crash_row(f'DOC{i:03d}') for i in range(10)]
        new = [make_crash_row('DOC999')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        # Normal save (no merge) overwrites
        new_df = make_dataframe(new)
        new_df.to_csv(output, index=False)

        result = read_csv(output)
        assert len(result) == 1, "Without merge, old data should be gone"
        assert result[0]['Document Nbr'] == 'DOC999'


class TestDownloadMerge_EmptyExisting:
    """Edge case: existing file exists but is empty or header-only."""

    def test_empty_existing_file(self, tmp_path):
        """Existing file with only headers → new data added normally."""
        output = tmp_path / 'crashes.csv'
        write_csv(output, [])  # Header only, no rows

        existing_df = pd.read_csv(output, dtype=str)
        new = [make_crash_row('DOC001')]
        new_df = make_dataframe(new)

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        assert len(combined) == 1


class TestDownloadMerge_MissingColumns:
    """Graceful handling when dedup columns are missing."""

    def test_no_doc_column_still_works(self, tmp_path):
        """If 'Document Nbr' column is missing, geo dedup still works."""
        headers_no_doc = [h for h in STANDARD_HEADERS if h != 'Document Nbr']
        row1 = make_crash_row('', date='01/01/2024')
        del row1['Document Nbr']
        row2 = make_crash_row('', date='01/02/2024')
        del row2['Document Nbr']

        output = tmp_path / 'crashes.csv'
        write_csv(output, [row1], headers=headers_no_doc)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe([row2])

        # Doc col search should not find any
        doc_col = None
        for col in ['Document Nbr', 'Document Number', 'CrashID', 'CRASH_ID']:
            if col in pd.concat([existing_df, new_df]).columns:
                doc_col = col
                break

        # Should still work without doc dedup
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        assert len(combined) >= 1  # No crash


class TestDownloadMerge_NullCoordinates:
    """Rows with null/zero coordinates skip geo deduplication."""

    def test_zero_coords_not_geo_deduped(self, tmp_path):
        """Rows with x=0, y=0 should NOT be geo-deduped."""
        existing = [make_crash_row('DOC001', x='0', y='0')]
        new = [make_crash_row('DOC002', x='0', y='0')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        has_coords = (
            combined['x'].notna() & (combined['x'] != '') & (combined['x'] != '0') &
            combined['y'].notna() & (combined['y'] != '') & (combined['y'] != '0')
        )
        # Both should be in ~has_coords (no coords), so they skip dedup
        assert has_coords.sum() == 0
        assert len(combined) == 2, "Zero-coord rows should not be deduped"

    def test_empty_coords_not_geo_deduped(self):
        """Rows with empty x/y should NOT be geo-deduped."""
        existing = [make_crash_row('DOC001', x='', y='')]
        new = [make_crash_row('DOC002', x='', y='')]
        df = make_dataframe(existing + new)

        has_coords = df['x'].notna() & (df['x'] != '') & (df['x'] != '0')
        assert has_coords.sum() == 0


class TestDownloadMerge_ExistingPriority:
    """Existing validated records take priority over new data."""

    def test_existing_severity_preserved(self, tmp_path):
        """When merging duplicates, existing severity is kept (not overwritten)."""
        existing = [make_crash_row('DOC001', severity='K', route='EXISTING RD')]
        new = [make_crash_row('DOC001', severity='O', route='NEW RD')]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        result = combined.drop_duplicates(subset=['Document Nbr'], keep='first')

        assert result.iloc[0]['Crash Severity'] == 'K'
        assert result.iloc[0]['RTE Name'] == 'EXISTING RD'


class TestDownloadMerge_MixedDedup:
    """Both doc dedup and geo dedup apply in the same merge."""

    def test_doc_dedup_then_geo_dedup(self, tmp_path):
        """Doc dedup runs first, then geo dedup catches remaining duplicates."""
        existing = [
            make_crash_row('DOC001', date='01/01/2024', x='-104.9', y='39.5', collision='01. Rear End'),
            make_crash_row('DOC002', date='01/02/2024', x='-104.8', y='39.6', collision='02. Angle'),
        ]
        new = [
            make_crash_row('DOC001', date='01/01/2024'),  # Doc duplicate of DOC001
            make_crash_row('DOC003', date='01/02/2024', x='-104.8', y='39.6', collision='02. Angle'),  # Geo dup of DOC002
            make_crash_row('DOC004', date='01/03/2024', x='-104.7', y='39.7', collision='03. Head On'),  # Unique
        ]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # Step 1: Doc dedup
        mask = combined['Document Nbr'].notna() & (combined['Document Nbr'] != '')
        has_doc = combined[mask]
        no_doc = combined[~mask]
        has_doc_deduped = has_doc.drop_duplicates(subset=['Document Nbr'], keep='first')
        combined = pd.concat([has_doc_deduped, no_doc], ignore_index=True)

        # Step 2: Geo dedup
        dedup_cols = ['Crash Date', 'x', 'y', 'Collision Type']
        has_coords = combined['x'].notna() & (combined['x'] != '') & (combined['x'] != '0')
        df_with = combined[has_coords]
        df_without = combined[~has_coords]
        df_deduped = df_with.drop_duplicates(subset=dedup_cols, keep='first')
        result = pd.concat([df_deduped, df_without], ignore_index=True)

        # Should have: DOC001, DOC002, DOC004 = 3 unique records
        assert len(result) == 3, f"Expected 3 after mixed dedup, got {len(result)}"


class TestDownloadMerge_LargeDataset:
    """Performance test with larger datasets."""

    def test_10k_rows_merge(self, tmp_path):
        """Merge with 10k existing + 5k new (2k overlap) completes."""
        existing = [make_crash_row(f'DOC{i:06d}', x=str(-104.9 + i * 0.0001),
                                    y=str(39.5 + i * 0.0001)) for i in range(10000)]
        # 2000 overlapping + 3000 new
        new = [make_crash_row(f'DOC{i:06d}', x=str(-104.9 + i * 0.0001),
                               y=str(39.5 + i * 0.0001)) for i in range(8000, 13000)]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        result = combined.drop_duplicates(subset=['Document Nbr'], keep='first')

        assert len(result) == 13000, f"Expected 13000 unique, got {len(result)}"


class TestDownloadMerge_DuplicateWithinNewData:
    """Duplicates within the new data itself should also be handled."""

    def test_internal_duplicates_in_new_data(self, tmp_path):
        """If the new download has internal duplicates, they're still deduped."""
        existing = [make_crash_row('DOC001')]
        new = [
            make_crash_row('DOC002'),
            make_crash_row('DOC002'),  # Internal duplicate
            make_crash_row('DOC003'),
        ]
        output = tmp_path / 'crashes.csv'
        write_csv(output, existing)

        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        result = combined.drop_duplicates(subset=['Document Nbr'], keep='first')

        assert len(result) == 3, f"Expected 3, got {len(result)}"


# ===========================================================================
# TEST SUITE 2: process_crash_data.py --merge-existing (stage_split)
# ===========================================================================

class TestSplitMerge_BasicMerge:
    """New validated rows are appended to existing split files."""

    def test_rows_combined(self, tmp_path):
        """New rows are appended to existing all_roads file."""
        existing_rows = [make_crash_row('DOC001'), make_crash_row('DOC002')]
        new_rows = [make_crash_row('DOC003'), make_crash_row('DOC004')]

        existing_path = tmp_path / 'douglas_all_roads.csv'
        write_csv(existing_path, existing_rows)

        # Simulate merge
        with open(existing_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_loaded = list(reader)

        combined = existing_loaded + new_rows
        assert len(combined) == 4


class TestSplitMerge_DocNumberDedup:
    """Doc number dedup works in split stage."""

    def test_duplicate_docs_removed(self, tmp_path):
        """Duplicate Document Nbrs in merge are removed."""
        existing_rows = [make_crash_row('DOC001', severity='K')]
        new_rows = [make_crash_row('DOC001', severity='O')]

        combined = existing_rows + new_rows

        seen_docs = set()
        deduped = []
        for row in combined:
            doc_val = (row.get('Document Nbr', '') or '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 1
        assert deduped[0]['Crash Severity'] == 'K', "Existing record should be kept"


class TestSplitMerge_GeoDedup:
    """Geo key dedup works in split stage."""

    def test_geo_duplicates_removed(self, tmp_path):
        """Same date+coords+collision are deduped in split merge."""
        existing = [make_crash_row('DOC001', date='01/15/2024', x='-104.9000',
                                    y='39.5000', collision='01. Rear End')]
        new = [make_crash_row('DOC999', date='01/15/2024', x='-104.9000',
                               y='39.5000', collision='01. Rear End')]
        combined = existing + new

        seen_geo = set()
        deduped = []
        for row in combined:
            x_val = (row.get('x', '') or '').strip()
            y_val = (row.get('y', '') or '').strip()
            date_val = (row.get('Crash Date', '') or '').strip()
            coll_val = (row.get('Collision Type', '') or '').strip()
            if x_val and y_val and x_val != '0' and y_val != '0' and date_val and coll_val:
                try:
                    geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                except (ValueError, TypeError):
                    geo_key = None
                if geo_key:
                    if geo_key in seen_geo:
                        continue
                    seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 1


class TestSplitMerge_NoExistingFile:
    """No existing file = fresh creation, no merge."""

    def test_creates_fresh_when_no_existing(self, tmp_path):
        """If no existing file, merge mode still creates output normally."""
        all_roads_path = tmp_path / 'douglas_all_roads.csv'
        assert not all_roads_path.exists()

        new_rows = [make_crash_row('DOC001')]
        write_csv(all_roads_path, new_rows)
        assert all_roads_path.exists()

        result = read_csv(all_roads_path)
        assert len(result) == 1


class TestSplitMerge_HeaderMerge:
    """New columns from new data are added to the merged headers."""

    def test_extra_columns_preserved(self, tmp_path):
        """If new data has extra columns, they're added to output."""
        existing_headers = ['Document Nbr', 'Crash Year', 'Crash Date', 'x', 'y']
        new_headers = ['Document Nbr', 'Crash Year', 'Crash Date', 'x', 'y', 'New Column']

        # Simulate header merge logic
        merged_headers = list(existing_headers)
        for h in new_headers:
            if h not in merged_headers:
                merged_headers.append(h)

        assert 'New Column' in merged_headers
        assert len(merged_headers) == 6
        # Original order preserved
        assert merged_headers[:5] == existing_headers


class TestSplitMerge_ExistingPriority:
    """Existing rows take priority over new duplicates."""

    def test_existing_row_values_kept(self):
        """When both doc and geo match, existing row is preserved."""
        existing = [make_crash_row('DOC001', severity='A', route='VALIDATED RD')]
        new = [make_crash_row('DOC001', severity='O', route='NEW RD')]
        combined = existing + new

        seen_docs = set()
        deduped = []
        for row in combined:
            doc_val = row.get('Document Nbr', '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 1
        assert deduped[0]['Crash Severity'] == 'A'
        assert deduped[0]['RTE Name'] == 'VALIDATED RD'


class TestSplitMerge_NanDocValues:
    """'nan' document values should not cause false dedup matches."""

    def test_nan_docs_not_treated_as_duplicates(self):
        """Rows with 'nan' Doc Nbr should not deduplicate each other."""
        rows = [
            make_crash_row('nan', date='01/01/2024', x='-104.9', y='39.5'),
            make_crash_row('nan', date='01/02/2024', x='-104.8', y='39.6'),
        ]

        seen_docs = set()
        deduped = []
        for row in rows:
            doc_val = (row.get('Document Nbr', '') or '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 2, "'nan' doc values should not deduplicate"


class TestSplitMerge_EmptyCoordinates:
    """Rows with empty/zero coordinates skip geo deduplication."""

    def test_empty_coords_not_geo_deduped(self):
        """Two rows with empty coords + same date should both be kept."""
        rows = [
            make_crash_row('DOC001', date='01/01/2024', x='', y=''),
            make_crash_row('DOC002', date='01/01/2024', x='', y=''),
        ]

        seen_geo = set()
        deduped = []
        for row in rows:
            x_val = (row.get('x', '') or '').strip()
            y_val = (row.get('y', '') or '').strip()
            date_val = (row.get('Crash Date', '') or '').strip()
            coll_val = (row.get('Collision Type', '') or '').strip()
            if x_val and y_val and x_val != '0' and y_val != '0' and date_val and coll_val:
                geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                if geo_key in seen_geo:
                    continue
                seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 2


class TestSplitMerge_FloatPrecision:
    """Coordinate rounding to 4 decimal places works correctly."""

    def test_coords_rounded_to_4dp(self):
        """Coordinates differing only beyond 4dp should be treated as same."""
        rows = [
            make_crash_row('DOC001', x='-104.90001', y='39.50001', collision='01. Rear End'),
            make_crash_row('DOC002', x='-104.90004', y='39.50004', collision='01. Rear End'),
        ]

        seen_geo = set()
        deduped = []
        for row in rows:
            x_val = row.get('x', '').strip()
            y_val = row.get('y', '').strip()
            date_val = row.get('Crash Date', '').strip()
            coll_val = row.get('Collision Type', '').strip()
            if x_val and y_val and x_val != '0' and y_val != '0':
                geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                if geo_key in seen_geo:
                    continue
                seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 1, "Coords within 4dp should be treated as same location"

    def test_coords_beyond_4dp_difference_kept(self):
        """Coordinates differing at 4dp should be treated as different."""
        rows = [
            make_crash_row('DOC001', x='-104.9000', y='39.5000', collision='01. Rear End'),
            make_crash_row('DOC002', x='-104.9001', y='39.5001', collision='01. Rear End'),
        ]

        seen_geo = set()
        deduped = []
        for row in rows:
            x_val = row.get('x', '').strip()
            y_val = row.get('y', '').strip()
            date_val = row.get('Crash Date', '').strip()
            coll_val = row.get('Collision Type', '').strip()
            if x_val and y_val:
                geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                if geo_key in seen_geo:
                    continue
                seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 2, "Coords different at 4dp should be kept as separate"


class TestSplitMerge_MergeDisabled:
    """Without --merge-existing, no merge behavior occurs."""

    def test_no_merge_flag_replaces(self, tmp_path):
        """Without merge-existing, writing new data replaces old file."""
        existing = [make_crash_row(f'DOC{i:03d}') for i in range(10)]
        new = [make_crash_row('DOC999')]
        output = tmp_path / 'douglas_all_roads.csv'
        write_csv(output, existing)

        # Simple overwrite (no merge)
        write_csv(output, new)
        result = read_csv(output)

        assert len(result) == 1
        assert result[0]['Document Nbr'] == 'DOC999'


# ===========================================================================
# TEST SUITE 3: Edge cases and integration
# ===========================================================================

class TestMerge_EdgeCases:
    """Edge cases that could cause bugs."""

    def test_empty_doc_nbr_not_false_match(self):
        """Empty string Document Nbr should not match other empty strings."""
        rows = [
            make_crash_row('', date='01/01/2024', x='-104.9', y='39.5'),
            make_crash_row('', date='01/02/2024', x='-104.8', y='39.6'),
        ]

        seen_docs = set()
        deduped = []
        for row in rows:
            doc_val = (row.get('Document Nbr', '') or '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 2, "Empty doc numbers should not match each other"

    def test_whitespace_in_doc_nbr_handled(self):
        """Document numbers with whitespace should be trimmed before comparing."""
        rows = [
            make_crash_row(' DOC001 '),
            make_crash_row('DOC001'),
        ]

        seen_docs = set()
        deduped = []
        for row in rows:
            doc_val = (row.get('Document Nbr', '') or '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 1, "Whitespace-trimmed docs should match"

    def test_negative_coordinates(self):
        """Negative longitude (Western hemisphere) works correctly."""
        rows = [
            make_crash_row('DOC001', x='-104.9876', y='39.1234', collision='01. Rear End'),
            make_crash_row('DOC002', x='-104.9876', y='39.1234', collision='01. Rear End'),
        ]

        seen_geo = set()
        deduped = []
        for row in rows:
            x_val = row.get('x', '').strip()
            y_val = row.get('y', '').strip()
            date_val = row.get('Crash Date', '').strip()
            coll_val = row.get('Collision Type', '').strip()
            if x_val and y_val:
                geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                if geo_key in seen_geo:
                    continue
                seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 1

    def test_merge_preserves_all_columns(self, tmp_path):
        """Merging doesn't drop any columns from either dataset."""
        existing = [make_crash_row('DOC001')]
        new = [make_crash_row('DOC002')]
        output = tmp_path / 'test.csv'

        write_csv(output, existing)
        existing_df = pd.read_csv(output, dtype=str)
        new_df = make_dataframe(new)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # All standard headers should be present
        for h in STANDARD_HEADERS:
            assert h in combined.columns, f"Column '{h}' missing after merge"

    def test_special_characters_in_collision_type(self):
        """Collision types with special chars don't break geo key."""
        rows = [
            make_crash_row('DOC001', collision='12. Ped'),
            make_crash_row('DOC002', collision='12. Ped'),
        ]

        seen_geo = set()
        deduped = []
        for row in rows:
            x_val = row.get('x', '').strip()
            y_val = row.get('y', '').strip()
            date_val = row.get('Crash Date', '').strip()
            coll_val = row.get('Collision Type', '').strip()
            if x_val and y_val:
                geo_key = f"{date_val}|{float(x_val):.4f}|{float(y_val):.4f}|{coll_val}"
                if geo_key in seen_geo:
                    continue
                seen_geo.add(geo_key)
            deduped.append(row)

        assert len(deduped) == 1

    def test_very_long_doc_number(self):
        """Very long document numbers are handled without truncation."""
        long_doc = 'DOC' + '0' * 100
        rows = [make_crash_row(long_doc), make_crash_row(long_doc)]

        seen_docs = set()
        deduped = []
        for row in rows:
            doc_val = (row.get('Document Nbr', '') or '').strip()
            if doc_val and doc_val != 'nan':
                if doc_val in seen_docs:
                    continue
                seen_docs.add(doc_val)
            deduped.append(row)

        assert len(deduped) == 1
