"""CHAOS-4184: the tier-floor pin of the sustained-rate artifact-skip alert.

Every other guard of this alert moved to Go (internal/cigates/chartgates/alerts_test.go,
skipAlert*). This one test imports the Python TIER_LIMITS_DEFAULTS, so it stays until that source
goes (CHAOS-3165 line).

`TIER_LIMITS_DEFAULTS` in `src/dev_health_ops/models/licensing.py` sets `min_sync_interval_hours`
(the FASTEST allowed custom schedule, i.e. the shortest legitimate gap between two consecutive runs)
at 24h for Community and 6h for Team. A window narrower than the slowest of those floors goes quiet
between two perfectly healthy runs, so the alert can never fire at all for that tier.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "alerts" / "rules.yml"

ALERT_NAME = "ProviderArtifactSkipsSustained"
GROUP_NAME = "provider_artifact_integrity"


def _group_rules(group_name: str) -> list[dict[str, Any]]:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    groups = document["groups"]
    group = next(candidate for candidate in groups if candidate["name"] == group_name)
    return group["rules"]


def _skip_alert() -> dict[str, Any]:
    rules = _group_rules(GROUP_NAME)
    return next(rule for rule in rules if rule["alert"] == ALERT_NAME)


def test_provider_artifact_skip_alert_window_covers_every_tier_sync_floor() -> None:
    """The window must STRICTLY exceed the slowest tier sync floor, and the tier set is pinned.

    Hardened per codex round 4: a tier missing `min_sync_interval_hours` fails loudly, and the
    comparison is strict because the rule comment claims the window PADS the floor.
    """
    from dev_health_ops.models.licensing import TIER_LIMITS_DEFAULTS

    expected_tiers = {"community", "team", "enterprise"}
    actual_tiers = {tier.value for tier in TIER_LIMITS_DEFAULTS}
    assert actual_tiers == expected_tiers, (
        f"TIER_LIMITS_DEFAULTS tier set changed: expected {expected_tiers}, "
        f"got {actual_tiers} -- update this test deliberately if a tier was "
        "added or removed, don't just widen the set"
    )

    floor_hours: dict[str, float] = {}
    for tier, limits in TIER_LIMITS_DEFAULTS.items():
        value = cast(dict[str, Any], limits).get("min_sync_interval_hours")
        assert value is not None, (
            f"{tier.value} tier has no min_sync_interval_hours in "
            "TIER_LIMITS_DEFAULTS -- this alert's window can no longer be "
            "verified against that tier's real sync floor"
        )
        floor_hours[tier.value] = float(value)
    slowest_floor_hours = max(floor_hours.values())

    alert = _skip_alert()
    window_match = re.search(
        r"increase\(\s*dev_health_provider_artifact_skipped_total\[(\d+)h\]\)",
        str(alert["expr"]),
    )
    assert window_match, "expected an increase(...[<N>h]) lookback window"
    window_hours = int(window_match.group(1))

    assert window_hours > slowest_floor_hours, (
        f"lookback window ({window_hours}h) does not exceed the slowest "
        f"documented tier sync floor ({slowest_floor_hours}h) -- two "
        "consecutive healthy runs on that tier's fastest allowed schedule "
        "would never both land inside one window with any jitter margin, "
        "so the alert could go permanently silent for it"
    )
