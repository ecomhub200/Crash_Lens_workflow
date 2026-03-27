#!/usr/bin/env python3
"""
Merge bounding box data into config.json for all Virginia jurisdictions.

This script reads pre-computed bounding boxes from virginia_jurisdiction_bboxes.json
and merges them into config.json.

Usage:
    python scripts/merge_bboxes_to_config.py

    # Preview without saving:
    python scripts/merge_bboxes_to_config.py --dry-run
"""

import json
import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Merge bounding boxes into config.json")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Load bbox data
    bbox_path = script_dir / "virginia_jurisdiction_bboxes.json"
    if not bbox_path.exists():
        print(f"✗ Bbox data not found: {bbox_path}")
        return 1

    with open(bbox_path, "r") as f:
        bbox_data = json.load(f)

    bbox_jurisdictions = bbox_data.get("jurisdictions", {})
    print(f"Loaded {len(bbox_jurisdictions)} jurisdiction bounding boxes")

    # Load config.json
    config_path = project_root / "config.json"
    if not config_path.exists():
        print(f"✗ Config not found: {config_path}")
        return 1

    with open(config_path, "r") as f:
        config = json.load(f)

    jurisdictions = config.get("jurisdictions", {})
    print(f"Config has {len(jurisdictions)} jurisdictions")
    print("-" * 50)

    # Merge bbox data
    matched = 0
    missing = 0
    already_has = 0

    for key, jur_config in jurisdictions.items():
        if "bbox" in jur_config:
            already_has += 1
            continue

        if key in bbox_jurisdictions:
            bbox = bbox_jurisdictions[key].get("bbox")
            if bbox:
                jur_config["bbox"] = bbox
                matched += 1
                print(f"✓ {jur_config.get('name', key)}: {bbox}")
        else:
            missing += 1
            print(f"⚠ No bbox data for: {key}")

    print("-" * 50)
    print(f"Results: {matched} added, {already_has} already had bbox, {missing} missing")

    # Save or preview
    if args.dry_run:
        print("\n[DRY RUN] Would save to config.json")
        # Show sample
        sample_key = "henrico"
        if sample_key in jurisdictions:
            print(f"\nSample entry ({sample_key}):")
            print(json.dumps(jurisdictions[sample_key], indent=2))
    else:
        print(f"\nSaving to: {config_path}")
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        print(f"✓ Config updated with {matched} bounding boxes")

    return 0


if __name__ == "__main__":
    exit(main())
