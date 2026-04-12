#!/usr/bin/env python3
"""
One-time cleanup: remove BTS MPO stubs with empty counties from hierarchy files.

Scans every states/*/hierarchy.json and removes TPR entries where:
  Rule A: counties is empty AND source contains "BTS"
  Rule B: counties is empty AND name fuzzy-matches an existing TPR with counties

Usage:
    python scripts/cleanup_bts_stubs.py --all               # Dry-run preview
    python scripts/cleanup_bts_stubs.py --all --write        # Apply changes
    python scripts/cleanup_bts_stubs.py --state delaware     # Single state
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def _name_to_slug(name):
    """Convert an entity name to an R2-compatible slug.

    CRITICAL: Must match the slug logic in generate_all_hierarchies.py,
    split.py, create_r2_folders.py, and validate_hierarchy.py.
    """
    slug = name.lower()
    slug = slug.replace("'", "").replace(".", "")
    slug = re.sub(r'[^a-z0-9]+', '_', slug)
    slug = re.sub(r'_+', '_', slug)
    slug = slug.strip('_')
    return slug


def fuzzy_name_match(name_a, name_b):
    """Check if two MPO names are fuzzy duplicates (substring match on lowered names)."""
    a = name_a.lower().strip()
    b = name_b.lower().strip()
    if not a or not b:
        return False
    return a in b or b in a


def cleanup_state(hierarchy, state_name, write_mode):
    """Remove BTS stubs with empty counties from a single hierarchy.

    Returns (modified_hierarchy, removed_count) or (None, 0) if no changes.
    """
    tprs = hierarchy.get("tprs", {})
    if not tprs:
        return None, 0

    # Build set of TPR names that have counties assigned
    tprs_with_counties = {}
    for key, entry in tprs.items():
        if entry.get("counties"):
            tprs_with_counties[key] = entry.get("name", key)

    to_remove = []

    for key, entry in tprs.items():
        counties = entry.get("counties", [])
        if counties:
            continue  # Has counties — never remove

        name = entry.get("name", key)
        source = entry.get("source", "")
        reason = None

        # Rule A: empty counties AND source contains "BTS"
        if "bts" in source.lower():
            reason = f"BTS source stub (source={source!r})"

        # Rule B: empty counties AND name fuzzy-matches an existing TPR with counties
        if not reason:
            for existing_key, existing_name in tprs_with_counties.items():
                if existing_key != key and fuzzy_name_match(name, existing_name):
                    reason = f"fuzzy duplicate of '{existing_name}' ({existing_key})"
                    break

        if reason:
            to_remove.append((key, name, reason))

    if not to_remove:
        return None, 0

    for key, name, reason in to_remove:
        del tprs[key]
        print(f"    - Removed '{name}' ({key}): {reason}")

    return hierarchy, len(to_remove)


def main():
    parser = argparse.ArgumentParser(description="Remove BTS MPO stubs with empty counties")
    parser.add_argument("--all", action="store_true", help="Process all states")
    parser.add_argument("--state", type=str, help="Process a single state (directory name)")
    parser.add_argument("--write", action="store_true", help="Write changes (default is dry-run)")
    args = parser.parse_args()

    if not args.all and not args.state:
        parser.error("Specify --all or --state <name>")

    states_dir = ROOT / "states"
    if args.state:
        paths = [states_dir / args.state / "hierarchy.json"]
    else:
        paths = sorted(states_dir.glob("*/hierarchy.json"))

    if not paths:
        print("No hierarchy.json files found.")
        sys.exit(2)

    mode = "WRITE" if args.write else "DRY-RUN"
    print(f"=== BTS Stub Cleanup ({mode}) ===\n")

    total_removed = 0
    total_states_modified = 0
    summary = []

    for path in paths:
        state_name = path.parent.name
        try:
            with open(path) as f:
                hierarchy = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [ERROR] {state_name}: {e}")
            continue

        tpr_count_before = len(hierarchy.get("tprs", {}))
        result, removed = cleanup_state(hierarchy, state_name, args.write)

        if removed > 0:
            tpr_count_after = tpr_count_before - removed
            total_removed += removed
            total_states_modified += 1
            summary.append((state_name, removed, tpr_count_after))

            print(f"  [{state_name}] Removed {removed}, remaining: {tpr_count_after} TPRs")

            if args.write and result is not None:
                with open(path, 'w') as f:
                    json.dump(result, f, indent=2)
                    f.write('\n')
                print(f"    ✓ Written: {path}")
        else:
            # Only show states with TPRs in verbose output
            if tpr_count_before > 0:
                pass  # Clean state, no output needed

    print(f"\n{'='*60}")
    print(f"SUMMARY: {total_removed} stubs removed across {total_states_modified} states")
    if summary:
        print(f"\n  {'State':<25} {'Removed':>8} {'Remaining':>10}")
        print(f"  {'-'*25} {'-'*8} {'-'*10}")
        for state, removed, remaining in summary:
            print(f"  {state:<25} {removed:>8} {remaining:>10}")

    if not args.write and total_removed > 0:
        print(f"\n  Run with --write to apply changes.")


if __name__ == '__main__':
    main()
