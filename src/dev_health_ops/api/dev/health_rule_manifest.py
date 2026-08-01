"""Generate or verify the checked-in ``health_rule_manifest.v1`` artifact (CHAOS-3302).

Mirrors ``dev_health_ops.api.dev.export_contracts_v2``'s write/check CLI
pattern: the manifest is a deterministic function of
``health_rule_registry.HEALTH_RULE_REGISTRY``, checked in under
``contracts/ask-dev/health-rules/manifest.json``, and drift between the
two is a hard CI failure (``test_health_rule_manifest.py``). This is the
"Generated manifest, code, fixtures, docs, and persisted rule fingerprints
are in parity" exit criterion, applied to the manifest half of that
parity chain.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .health_rule_registry import HEALTH_RULE_REGISTRY

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = (
    REPOSITORY_ROOT / "contracts" / "ask-dev" / "health-rules" / "manifest.json"
)


def render_manifest() -> str:
    return json.dumps(HEALTH_RULE_REGISTRY.manifest(), indent=2, sort_keys=True) + "\n"


def write_manifest() -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(render_manifest(), encoding="utf-8")


def check_manifest() -> None:
    expected = render_manifest()
    if not MANIFEST_PATH.exists():
        raise RuntimeError(f"health rule manifest missing at {MANIFEST_PATH}")
    actual = MANIFEST_PATH.read_text(encoding="utf-8")
    if actual != expected:
        raise RuntimeError(
            "health rule manifest drifted from the registry; regenerate with "
            "`python -m dev_health_ops.api.dev.health_rule_manifest write`"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("write", "check"))
    args = parser.parse_args()
    if args.mode == "write":
        write_manifest()
        print(f"wrote {MANIFEST_PATH}")
    else:
        check_manifest()
        print(f"verified {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
