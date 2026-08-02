"""Manifest/registry parity (CHAOS-3302 exit criterion): the checked-in

``contracts/ask-dev/health-rules/manifest.json`` must be an exact function
of ``HEALTH_RULE_REGISTRY``. Drift is a hard failure -- regenerate with
``python -m dev_health_ops.api.dev.health_rule_manifest write``, never
hand-edit the artifact.
"""

from __future__ import annotations

import json

import pytest

from dev_health_ops.api.dev.health_rule_manifest import check_manifest, render_manifest
from dev_health_ops.api.dev.health_rule_registry import HEALTH_RULE_REGISTRY


def test_manifest_matches_registry() -> None:
    check_manifest()


def test_manifest_covers_every_registered_rule() -> None:
    manifest = json.loads(render_manifest())
    manifest_ids = {entry["rule_id"] for entry in manifest["rules"]}
    assert manifest_ids == set(HEALTH_RULE_REGISTRY)


def test_manifest_drift_is_detected() -> None:
    """Kill site: a manifest that silently drops a rule must fail check_manifest."""

    from dev_health_ops.api.dev import health_rule_manifest as manifest_module

    original = manifest_module.render_manifest

    def _dropped_rule_manifest() -> str:
        payload = json.loads(original())
        payload["rules"].pop()
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    manifest_module.render_manifest = _dropped_rule_manifest
    try:
        with pytest.raises(RuntimeError):
            manifest_module.check_manifest()
    finally:
        manifest_module.render_manifest = original
