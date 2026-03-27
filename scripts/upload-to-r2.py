#!/usr/bin/env python3
"""
Upload existing local CSV files to Cloudflare R2.

Use this script to seed the R2 bucket with data that already exists locally
(e.g., after initial setup, or when the OnBase download workflow can't reach
CDOT from GitHub Actions IPs).

Prerequisites:
  1. R2 API Token created in Cloudflare dashboard
  2. pip install boto3

Usage:
  # Interactive (prompts for credentials):
  python scripts/upload-to-r2.py

  # With environment variables:
  CF_ACCOUNT_ID=xxx CF_R2_ACCESS_KEY_ID=xxx CF_R2_SECRET_ACCESS_KEY=xxx \
    python scripts/upload-to-r2.py

  # With R2 public URL (updates manifest):
  R2_PUBLIC_URL=https://pub-xxx.r2.dev python scripts/upload-to-r2.py

  # Dry run (show what would be uploaded):
  python scripts/upload-to-r2.py --dry-run

  # Upload specific jurisdiction:
  python scripts/upload-to-r2.py --jurisdiction douglas

  # Upload for a different state:
  python scripts/upload-to-r2.py --state colorado --jurisdiction douglas
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
MANIFEST_PATH = PROJECT_ROOT / 'data' / 'r2-manifest.json'
BUCKET_NAME = 'crash-lens-data'


def get_credentials():
    """Get R2 credentials from environment or prompt."""
    account_id = os.environ.get('CF_ACCOUNT_ID', '')
    access_key = os.environ.get('CF_R2_ACCESS_KEY_ID', '')
    secret_key = os.environ.get('CF_R2_SECRET_ACCESS_KEY', '')
    public_url = os.environ.get('R2_PUBLIC_URL', '')

    if not account_id:
        account_id = input('Cloudflare Account ID: ').strip()
    if not access_key:
        access_key = input('R2 Access Key ID: ').strip()
    if not secret_key:
        import getpass
        secret_key = getpass.getpass('R2 Secret Access Key: ').strip()
    if not public_url:
        public_url = input('R2 Public URL (e.g. https://pub-xxx.r2.dev): ').strip()

    if not all([account_id, access_key, secret_key]):
        print('ERROR: All credentials are required.')
        sys.exit(1)

    return account_id, access_key, secret_key, public_url


def build_file_map(state, jurisdiction):
    """Build the mapping of local files -> R2 keys based on state/jurisdiction."""
    files = []
    state_lower = state.lower()

    if state_lower == 'colorado':
        data_dir = PROJECT_ROOT / 'data' / 'CDOT'
        r2_prefix = f'colorado/{jurisdiction}'

        # Processed pipeline outputs
        for suffix in ['standardized', 'all_roads', 'county_roads', 'city_roads', 'no_interstate']:
            local = data_dir / f'{jurisdiction}_{suffix}.csv'
            if local.exists():
                files.append({
                    'local_path': str(local),
                    'r2_key': f'{r2_prefix}/{suffix}.csv',
                    'category': 'processed'
                })

        # crashes.csv (copy of county_roads)
        crashes = data_dir / 'crashes.csv'
        if crashes.exists():
            files.append({
                'local_path': str(crashes),
                'r2_key': f'{r2_prefix}/crashes.csv',
                'category': 'processed'
            })

        # Raw annual CSVs (e.g., "2021 douglas.csv", "2022 douglas county.csv")
        for csv_file in sorted(data_dir.glob('20*.csv')):
            name = csv_file.name
            # Extract year from filename
            year = name[:4]
            if year.isdigit():
                files.append({
                    'local_path': str(csv_file),
                    'r2_key': f'{r2_prefix}/raw/{year}.csv',
                    'category': 'raw'
                })

    elif state_lower == 'virginia':
        data_dir = PROJECT_ROOT / 'data'
        r2_prefix = f'virginia/{jurisdiction}'

        for suffix in ['all_roads', 'county_roads', 'city_roads', 'no_interstate']:
            local = data_dir / f'{jurisdiction}_{suffix}.csv'
            if local.exists():
                files.append({
                    'local_path': str(local),
                    'r2_key': f'{r2_prefix}/{suffix}.csv',
                    'category': 'processed'
                })

    else:
        print(f'WARNING: Unknown state "{state}". Looking for CSVs in data/{state}/')
        data_dir = PROJECT_ROOT / 'data' / state
        r2_prefix = f'{state_lower}/{jurisdiction}'
        if data_dir.exists():
            for csv_file in sorted(data_dir.glob('*.csv')):
                files.append({
                    'local_path': str(csv_file),
                    'r2_key': f'{r2_prefix}/{csv_file.name}',
                    'category': 'unknown'
                })

    return files


def upload_files(files, account_id, access_key, secret_key, public_url, dry_run=False):
    """Upload files to R2 and update the manifest."""
    try:
        import boto3
    except ImportError:
        print('ERROR: boto3 is required. Install with: pip install boto3')
        sys.exit(1)

    endpoint_url = f'https://{account_id}.r2.cloudflarestorage.com'

    if dry_run:
        print(f'\n--- DRY RUN (endpoint: {endpoint_url}) ---\n')
        total_size = 0
        for f in files:
            size = os.path.getsize(f['local_path'])
            total_size += size
            print(f'  [{f["category"]:>9}] {f["local_path"]}\n'
                  f'          -> {f["r2_key"]} ({size:,} bytes)')
        print(f'\nTotal: {len(files)} files, {total_size:,} bytes ({total_size/1024/1024:.1f} MB)')
        return

    # Create S3 client for R2
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'
    )

    # Load or create manifest
    try:
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        manifest = {'version': 1, 'r2BaseUrl': '', 'updated': '', 'files': {}, 'localPathMapping': {}}

    if public_url:
        manifest['r2BaseUrl'] = public_url.rstrip('/')
    manifest['updated'] = datetime.now(timezone.utc).isoformat()

    uploaded = 0
    failed = 0

    print(f'\nUploading {len(files)} files to R2 bucket "{BUCKET_NAME}"...\n')

    for entry in files:
        local_path = entry['local_path']
        r2_key = entry['r2_key']
        size = os.path.getsize(local_path)

        content_type = 'text/csv' if local_path.endswith('.csv') else 'application/octet-stream'

        try:
            s3.upload_file(
                local_path,
                BUCKET_NAME,
                r2_key,
                ExtraArgs={'ContentType': content_type}
            )

            # Compute MD5
            with open(local_path, 'rb') as fh:
                md5 = hashlib.md5(fh.read()).hexdigest()

            # Update manifest
            manifest['files'][r2_key] = {
                'size': size,
                'md5': md5,
                'uploaded': datetime.now(timezone.utc).isoformat()
            }
            manifest['localPathMapping'][local_path.replace(str(PROJECT_ROOT) + '/', '')] = r2_key

            uploaded += 1
            print(f'  OK  {r2_key} ({size:,} bytes)')

        except Exception as e:
            failed += 1
            print(f'  FAIL {r2_key}: {e}')

    # Write manifest
    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)
        f.write('\n')

    print(f'\nDone: {uploaded} uploaded, {failed} failed')
    print(f'Manifest updated: {MANIFEST_PATH}')

    if uploaded > 0 and manifest.get('r2BaseUrl'):
        print(f'\nTest URL: {manifest["r2BaseUrl"]}/{list(manifest["files"].keys())[0]}')

    return uploaded, failed


def main():
    parser = argparse.ArgumentParser(
        description='Upload local crash data CSV files to Cloudflare R2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--state', default='colorado', help='State name (default: colorado)')
    parser.add_argument('--jurisdiction', default='douglas', help='County/jurisdiction (default: douglas)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be uploaded without uploading')
    parser.add_argument('--processed-only', action='store_true', help='Upload only processed files (skip raw annual CSVs)')
    args = parser.parse_args()

    print(f'CRASH LENS - R2 Upload Tool')
    print(f'State: {args.state}, Jurisdiction: {args.jurisdiction}')
    print(f'Project root: {PROJECT_ROOT}')

    # Build file list
    files = build_file_map(args.state, args.jurisdiction)

    if args.processed_only:
        files = [f for f in files if f['category'] == 'processed']

    if not files:
        print(f'\nNo CSV files found for {args.state}/{args.jurisdiction}')
        print('Check that processed data exists in the expected location.')
        sys.exit(1)

    print(f'\nFound {len(files)} files to upload:')
    for f in files:
        size = os.path.getsize(f['local_path'])
        print(f'  [{f["category"]:>9}] {Path(f["local_path"]).name} ({size:,} bytes) -> {f["r2_key"]}')

    if args.dry_run:
        upload_files(files, '', '', '', '', dry_run=True)
        return

    # Get credentials
    account_id, access_key, secret_key, public_url = get_credentials()

    # Upload
    uploaded, failed = upload_files(files, account_id, access_key, secret_key, public_url)

    if failed > 0:
        sys.exit(1)

    print('\nNext steps:')
    print('  1. git add data/r2-manifest.json')
    print('  2. git commit -m "chore: seed R2 manifest with initial data"')
    print('  3. git push')
    print('  4. Open the app and check browser DevTools for [R2] log messages')


if __name__ == '__main__':
    main()
