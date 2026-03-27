"""
EPDO Config Loader — Reference Implementation
===============================================

Loads EPDO (Equivalent Property Damage Only) weights from state config files
instead of hardcoding them in Python scripts.

Use this in:
  - scripts/generate_forecast.py (line 69)
  - send_notifications.py (line 123)

EPDO weights are derived from crash cost ratios:
  Weight = CrashCost(severity) / CrashCost(PDO)

Different states/agencies have different crash costs, so weights vary:
  FHWA 2025 (default): K=883, A=94, B=21, C=11, O=1  ($16.0M K / $18.1K O)
  VDOT 2024:           K=1032, A=53, B=16, C=10, O=1
  FHWA 2022:           K=975, A=48, B=13, C=8, O=1
  HSM Standard (2010): K=462, A=62, B=12, C=5, O=1
"""

import json
import os


# Default EPDO weights (FHWA-SA-25-021, Oct 2025, in 2024 dollars)
DEFAULT_EPDO_WEIGHTS = {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1}


def load_epdo_weights(config_path=None):
    """
    Load EPDO weights from a state config JSON file.

    The config file should have an 'epdoWeights' key with K/A/B/C/O values:
    {
        "epdoWeights": {
            "K": 883,
            "A": 94,
            "B": 21,
            "C": 11,
            "O": 1
        }
    }

    Args:
        config_path: Path to state config JSON file.
                     If None, auto-detects from project structure.

    Returns:
        dict: EPDO weights with keys K, A, B, C, O.
              Falls back to HSM standard if config not found.
    """
    if config_path is None:
        # Auto-detect config path relative to project root
        # Works for both scripts/ and root-level scripts
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Try common locations
        candidates = [
            os.path.join(script_dir, "..", "config.json"),           # data/CDOT/epdo/../config.json
            os.path.join(script_dir, "..", "..", "CDOT", "config.json"),  # data/CDOT/config.json
            os.path.join(script_dir, "..", "..", "..", "data", "CDOT", "config.json"),  # project root
        ]

        for candidate in candidates:
            normalized = os.path.normpath(candidate)
            if os.path.exists(normalized):
                config_path = normalized
                break

    if config_path is None:
        print(f"[EPDO Config] No config file found, using HSM standard weights")
        return dict(DEFAULT_EPDO_WEIGHTS)

    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        weights = config.get("epdoWeights", None)
        if weights is None:
            print(f"[EPDO Config] No 'epdoWeights' in {config_path}, using defaults")
            return dict(DEFAULT_EPDO_WEIGHTS)

        # Validate all required keys are present
        result = {}
        for key in ["K", "A", "B", "C", "O"]:
            if key not in weights:
                print(f"[EPDO Config] Warning: missing '{key}' in epdoWeights, using default")
                result[key] = DEFAULT_EPDO_WEIGHTS[key]
            else:
                result[key] = int(weights[key])

        print(f"[EPDO Config] Loaded weights from {config_path}: {result}")
        return result

    except FileNotFoundError:
        print(f"[EPDO Config] Config not found: {config_path}, using defaults")
        return dict(DEFAULT_EPDO_WEIGHTS)
    except json.JSONDecodeError as e:
        print(f"[EPDO Config] Invalid JSON in {config_path}: {e}, using defaults")
        return dict(DEFAULT_EPDO_WEIGHTS)
    except Exception as e:
        print(f"[EPDO Config] Error loading {config_path}: {e}, using defaults")
        return dict(DEFAULT_EPDO_WEIGHTS)


def calc_epdo(severity_counts, weights=None):
    """
    Calculate EPDO score from severity counts.

    Args:
        severity_counts: dict with keys K, A, B, C, O (counts)
        weights: dict with EPDO weights. If None, uses module-level EPDO_WEIGHTS.

    Returns:
        int: EPDO score
    """
    if weights is None:
        weights = EPDO_WEIGHTS
    return sum(severity_counts.get(s, 0) * weights.get(s, 0) for s in ["K", "A", "B", "C", "O"])


# Module-level weights loaded on import
EPDO_WEIGHTS = load_epdo_weights()


# ============================================================
# USAGE EXAMPLES
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="EPDO Weight Configuration Tool")
    parser.add_argument("--config", default=None, help="Path to state config JSON file")
    parser.add_argument("--show", action="store_true", help="Show current weights and exit")
    args = parser.parse_args()

    if args.config:
        weights = load_epdo_weights(args.config)
    else:
        weights = EPDO_WEIGHTS

    print(f"\nActive EPDO Weights:")
    print(f"  K (Fatal):    {weights['K']}")
    print(f"  A (Serious):  {weights['A']}")
    print(f"  B (Minor):    {weights['B']}")
    print(f"  C (Possible): {weights['C']}")
    print(f"  O (PDO):      {weights['O']}")

    # Example calculation
    example = {"K": 2, "A": 5, "B": 10, "C": 15, "O": 100}
    epdo = calc_epdo(example, weights)
    print(f"\nExample: {example}")
    print(f"  EPDO Score: {epdo}")
    print(f"  Formula: ({example['K']}*{weights['K']}) + ({example['A']}*{weights['A']}) + "
          f"({example['B']}*{weights['B']}) + ({example['C']}*{weights['C']}) + "
          f"({example['O']}*{weights['O']})")

    # Compare presets
    presets = {
        "FHWA 2025": {"K": 883, "A": 94, "B": 21, "C": 11, "O": 1},
        "VDOT 2024": {"K": 1032, "A": 53, "B": 16, "C": 10, "O": 1},
        "FHWA 2022": {"K": 975, "A": 48, "B": 13, "C": 8, "O": 1},
        "HSM 2010":  {"K": 462, "A": 62, "B": 12, "C": 5, "O": 1},
    }
    print(f"\nComparison across presets for same crash data:")
    for name, preset_weights in presets.items():
        score = calc_epdo(example, preset_weights)
        print(f"  {name}: EPDO = {score}")
