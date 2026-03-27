#!/usr/bin/env python3
"""
Initialize the state-isolated cache directory structure.

Creates the .cache/ hierarchy for a given state (or all active states),
including the global _cache_registry.json and per-state cache_manifest.json.

The cache is fully state-isolated — running Colorado never touches Virginia's
cache, and vice versa.

Directory structure:
    .cache/
      _cache_registry.json          <- Global registry of all state caches
      {state}/
        cache_manifest.json         <- State cache metadata + update schedule
        validation/
          validated_hashes.json     <- Row-level validation cache
          validation_rules_hash.txt <- Config hash for invalidation
          last_run.json             <- Last validation run stats
        geocode/
          geocode_cache.json        <- Location -> coordinates cache
          geocoded_records.json     <- Document Nbr -> location_key mapping
          cache_stats.json          <- Geocoding hit rate stats

Usage:
    python scripts/init_cache.py --state virginia
    python scripts/init_cache.py --state colorado
    python scripts/init_cache.py --all          # Initialize all active states
    python scripts/init_cache.py --state virginia --reset  # Clear and reinitialize
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger('init_cache')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).parent.parent
CACHE_ROOT = PROJECT_ROOT / '.cache'

# States with active pipelines
ACTIVE_STATES = ['virginia', 'colorado', 'maryland']


def load_state_cache_config(state):
    """Load cache_config from the state's config.json."""
    config_path = PROJECT_ROOT / 'states' / state / 'config.json'
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        return config.get('cache_config', {})
    return {}


def create_cache_manifest(state, cache_config):
    """Create a cache_manifest.json for a state."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        'state': state,
        'version': 1,
        'created_at': now,
        'last_updated': now,
        'update_frequency': cache_config.get('update_frequency', 'unknown'),
        'typical_new_records_per_update': cache_config.get('typical_new_records_per_update', 0),
        'stale_threshold_days': cache_config.get('stale_threshold_days', 30),
        'geocode_ttl_days': cache_config.get('geocode_ttl_days', 365),
        'max_cache_size_mb': cache_config.get('max_cache_size_mb', 200),
        'validation': {
            'last_run': None,
            'total_validated': 0,
            'cached_records': 0,
            'cache_hit_rate': 0.0
        },
        'geocode': {
            'last_run': None,
            'total_locations': 0,
            'api_calls_total': 0,
            'cache_hit_rate': 0.0
        }
    }


def create_cache_registry(states_initialized):
    """Create or update the global _cache_registry.json."""
    now = datetime.now(timezone.utc).isoformat()
    registry = {
        'version': 1,
        'created_at': now,
        'last_updated': now,
        'description': 'Global registry of all state-isolated caches',
        'states': {}
    }

    for state in states_initialized:
        cache_config = load_state_cache_config(state)
        state_cache_dir = CACHE_ROOT / state
        registry['states'][state] = {
            'cache_dir': str(state_cache_dir.relative_to(PROJECT_ROOT)),
            'update_frequency': cache_config.get('update_frequency', 'unknown'),
            'initialized_at': now,
            'subdirectories': ['validation', 'geocode']
        }

    return registry


def init_state_cache(state, reset=False):
    """Initialize cache directory structure for a single state."""
    state_dir = CACHE_ROOT / state
    validation_dir = state_dir / 'validation'
    geocode_dir = state_dir / 'geocode'

    if reset and state_dir.exists():
        import shutil
        shutil.rmtree(state_dir)
        logger.info(f"[{state}] Cache reset (cleared)")

    # Create directories
    validation_dir.mkdir(parents=True, exist_ok=True)
    geocode_dir.mkdir(parents=True, exist_ok=True)

    # Create cache_manifest.json if it doesn't exist
    manifest_path = state_dir / 'cache_manifest.json'
    if not manifest_path.exists():
        cache_config = load_state_cache_config(state)
        manifest = create_cache_manifest(state, cache_config)
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"[{state}] Created cache_manifest.json")

    # Create empty validation cache files if they don't exist
    for fname in ['validated_hashes.json', 'last_run.json']:
        fpath = validation_dir / fname
        if not fpath.exists():
            with open(fpath, 'w') as f:
                json.dump({}, f)

    # Create empty geocode cache files if they don't exist
    for fname in ['geocode_cache.json', 'geocoded_records.json', 'cache_stats.json']:
        fpath = geocode_dir / fname
        if not fpath.exists():
            with open(fpath, 'w') as f:
                json.dump({}, f)

    logger.info(f"[{state}] Cache initialized at {state_dir}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Initialize state-isolated cache structure')
    parser.add_argument('--state', help='State to initialize (e.g., virginia, colorado)')
    parser.add_argument('--all', action='store_true', help='Initialize all active states')
    parser.add_argument('--reset', action='store_true', help='Clear and reinitialize cache')
    parser.add_argument('--list', action='store_true', help='List active states and cache status')
    args = parser.parse_args()

    if args.list:
        print(f"\nActive states: {', '.join(ACTIVE_STATES)}")
        print(f"Cache root: {CACHE_ROOT}")
        for state in ACTIVE_STATES:
            state_dir = CACHE_ROOT / state
            exists = state_dir.exists()
            manifest = state_dir / 'cache_manifest.json'
            has_manifest = manifest.exists()
            print(f"  {state}: {'initialized' if exists else 'not initialized'}"
                  f" (manifest: {'yes' if has_manifest else 'no'})")
        return 0

    if not args.state and not args.all:
        parser.error("--state or --all required")

    states_to_init = ACTIVE_STATES if args.all else [args.state]

    for state in states_to_init:
        # Verify state has a hierarchy.json
        hierarchy = PROJECT_ROOT / 'states' / state / 'hierarchy.json'
        if not hierarchy.exists():
            logger.warning(f"[{state}] No hierarchy.json found — skipping")
            continue
        init_state_cache(state, reset=args.reset)

    # Create/update global registry
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    registry = create_cache_registry(states_to_init)
    registry_path = CACHE_ROOT / '_cache_registry.json'
    with open(registry_path, 'w') as f:
        json.dump(registry, f, indent=2)
    logger.info(f"Global cache registry updated: {registry_path}")

    logger.info("=" * 60)
    logger.info("CACHE INITIALIZATION COMPLETE")
    logger.info(f"  States: {', '.join(states_to_init)}")
    logger.info(f"  Root:   {CACHE_ROOT}")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
