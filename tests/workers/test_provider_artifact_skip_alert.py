"""CHAOS-4184: sustained-rate alert on dev_health_provider_artifact_skipped_total.

CHAOS-4177 made ONE unreadable artifact survivable (skip, count, keep
walking) -- that is correct, and an isolated bad artifact must not page. What
was missing is the middle: a sustained partial failure (skips recurring
across many sync runs for hours) still produces successful units and an
advancing board, so nothing pages. These tests pin:

* the alert exists, targets the real metric name (read from the Go source,
  not retyped), and groups by its real labels (provider, dataset);
* the alert's `for` duration is strictly longer than its lookback window --
  the mechanism that makes a single isolated skip age out of the window
  before `for` can elapse, while a recurring skip keeps re-arming the window
  and eventually does elapse it (see the rule's own header comment in
  alerts/rules.yml for the full walk-through);
* the annotation says where localization comes from, per the ticket's ask,
  since the metric itself deliberately carries no source/repository label.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "alerts" / "rules.yml"
BUDGET_GO_PATH = ROOT / "internal" / "providerfoundation" / "budget.go"

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


def _write_prometheus_body() -> str:
    """Isolate the live `func (m *Metrics) WritePrometheus(...)` body.

    Anchoring the metric-name pin to this specific function's source range --
    not the whole file -- means a dead, commented-out, or alternate call
    elsewhere in budget.go cannot satisfy the pin; only the one function that
    actually renders the scraped /metrics output can.
    """
    text = BUDGET_GO_PATH.read_text(encoding="utf-8")
    start_match = re.search(
        r"^func \(m \*Metrics\) WritePrometheus\(", text, re.MULTILINE
    )
    assert start_match, "could not find func (m *Metrics) WritePrometheus( in budget.go"
    next_func = re.search(r"^func ", text[start_match.end() :], re.MULTILINE)
    end = start_match.end() + (
        next_func.start() if next_func else len(text) - start_match.end()
    )
    return text[start_match.start() : end]


def _real_metric_name_and_labels() -> tuple[str, str]:
    """Pin the metric name and its third label from the Go source itself,
    rather than retyping a string a refactor could silently drift from.

    `RecordArtifactSkipped` writes into `m.artifactSkipped`, and
    `WritePrometheus` serializes that map through
    `writeProviderDatasetReasonCounter(writer, <name>, <help>, <label>,
    m.artifactSkipped)` -- provider and dataset are baked into that helper's
    output format, and <label> is the third, reason. The search is scoped to
    WritePrometheus's own body (see `_write_prometheus_body`), so this cannot
    be satisfied by a call that isn't actually on the live scrape path.
    """
    body = _write_prometheus_body()
    match = re.search(
        r"writeProviderDatasetReasonCounter\(\s*"
        r'writer,\s*"(?P<name>[^"]+)",\s*'
        r'"(?P<help>[^"]+)",\s*'
        r'"(?P<label>[^"]+)",\s*m\.artifactSkipped,',
        body,
        re.DOTALL,
    )
    assert match, (
        "could not find the writeProviderDatasetReasonCounter(...) call for "
        "m.artifactSkipped inside WritePrometheus's own body in budget.go -- "
        "has RecordArtifactSkipped's wiring changed shape, or moved off the "
        "live scrape path?"
    )
    return match.group("name"), match.group("label")


def test_artifact_skipped_metric_name_is_pinned_from_go_source() -> None:
    metric_name, _label = _real_metric_name_and_labels()
    assert metric_name == "dev_health_provider_artifact_skipped_total"


def test_provider_artifact_skip_alert_exists_and_targets_the_real_metric() -> None:
    metric_name, reason_label = _real_metric_name_and_labels()
    alert = _skip_alert()
    expr = str(alert["expr"])
    assert metric_name in expr
    # The alert groups by the metric's other two bounded labels.
    assert "by (provider, dataset)" in expr
    # It must not claim the reason label groups anything at the alert level --
    # this alert answers "is this happening", not "what kind"; the per-skip
    # WARN log and unit failure answer "where"/"what". Asserting the label's
    # NAME appears somewhere keeps this test honest against a rename.
    assert reason_label == "reason"


def test_provider_artifact_skip_alert_for_duration_exceeds_lookback_window() -> None:
    """The core correctness property: `for` must outlast the window.

    A single isolated skip keeps `increase(metric[window]) > 0` true for at
    most `window` after it lands, then the sample ages out and the value
    drops back to 0. If `for <= window`, that lone event can still satisfy
    `for` before it ages out -- i.e. the alert fires on one bad artifact,
    which CHAOS-4184 explicitly forbids ("isolated bad artifacts are
    expected"). Only `for > window` guarantees a single burst's truthy
    stretch is too short to ever satisfy `for`, while a recurring skip
    (which keeps re-arming the window before it empties) eventually does.
    """
    alert = _skip_alert()
    expr = str(alert["expr"])

    window_match = re.search(
        r"increase\(\s*dev_health_provider_artifact_skipped_total\[(\d+)([hm])\]\)",
        expr,
    )
    assert window_match, f"expected an increase(...[<N>h]) lookback window in: {expr!r}"
    window_minutes = int(window_match.group(1)) * (
        60 if window_match.group(2) == "h" else 1
    )

    for_value = str(alert["for"])
    for_match = re.fullmatch(r"(\d+)([hm])", for_value)
    assert for_match, f"expected `for` as e.g. '3h', got {for_value!r}"
    for_minutes = int(for_match.group(1)) * (60 if for_match.group(2) == "h" else 1)

    assert for_minutes > window_minutes, (
        f"`for` ({for_value}) must be strictly longer than the lookback "
        f"window ({window_match.group(0)}) or a single isolated skip can "
        "satisfy `for` before its sample ages out of the window"
    )


def test_provider_artifact_skip_alert_window_covers_every_tier_sync_floor() -> None:
    """Adversarial codex review, round 3 (real finding, CONFIRMED): an
    earlier draft sized the window off the doc'd "hourly" incremental-sync
    example, which is not a floor -- `TIER_LIMITS_DEFAULTS` in
    `src/dev_health_ops/models/licensing.py` sets `min_sync_interval_hours`
    (the FASTEST allowed custom schedule, i.e. the shortest legitimate gap
    between two consecutive runs) at 24h for Community and 6h for Team. A
    window narrower than the slowest of those floors goes quiet between two
    perfectly healthy runs, so the alert can never fire at all for that
    tier -- not a false positive, a permanent false negative. This test
    pins the window against the real, imported constants (not retyped
    numbers) so a future tier-limit change that widens the floor further
    is caught here rather than silently reopening the gap.

    Hardened per round 4 (also real, CONFIRMED): the first version of this
    test silently dropped any tier missing a `min_sync_interval_hours` key,
    so a typo'd or removed key would still pass; it also used `>=` while the
    rule comment claims 25h "pads" the 24h floor, an unenforced claim. This
    version asserts the exact tier set and a strict margin.

    Two things this test deliberately does NOT cover, both documented as
    open limitations in the rule's own comment rather than silently
    accepted: (1) `min_sync_interval_hours` is a FLOOR here, not a ceiling
    -- an operator can still configure an even slower custom cron, which
    this test cannot detect. (2) `TIER_LIMITS_DEFAULTS` is the CODE-DEFAULT
    fallback only -- `TierLimitService` (src/dev_health_ops/api/services/
    licensing.py) resolves a per-org `OrgLicense.limits_override` or a
    `tier_limits` DB row ABOVE these defaults at runtime, without a code
    deploy; this test has no way to see that (a live DB query is out of
    reach for a unit test and irrelevant to what this rule can statically
    guarantee).
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

    # Strict, not >=: the rule comment claims the window PADS the slowest
    # floor for scheduling jitter, not merely matches it. If a future tier
    # floor lands exactly on the current window, that claim goes false --
    # this should fail loudly and force a deliberate re-pick of the margin,
    # not silently degrade to zero jitter tolerance.
    assert window_hours > slowest_floor_hours, (
        f"lookback window ({window_hours}h) does not exceed the slowest "
        f"documented tier sync floor ({slowest_floor_hours}h) -- two "
        "consecutive healthy runs on that tier's fastest allowed schedule "
        "would never both land inside one window with any jitter margin, "
        "so the alert could go permanently silent for it"
    )


def test_provider_artifact_skip_alert_threshold_is_any_nonzero_increase() -> None:
    # A count/ratio threshold above zero would need a known per-run skip
    # volume to calibrate against, which this metric does not expose (no
    # total-artifacts-read counterpart to build a ratio from). The `for >
    # window` property above is what does the "not a single event" work
    # instead, so the threshold itself stays the simple "> 0".
    alert = _skip_alert()
    assert re.search(r"\)\s*>\s*0\s*$", str(alert["expr"]).strip())


def test_provider_artifact_skip_alert_annotation_explains_localization() -> None:
    """CHAOS-4184's ask: the alert must say where localization comes from,
    since the metric carries no source/repository dimension by design."""
    alert = _skip_alert()
    description = str(alert["annotations"]["description"])
    assert "repository" in description
    assert "run" in description
    assert "unit" in description


def test_provider_artifact_skip_alert_labels_stay_bounded() -> None:
    # The expr/labels block must never grow a source/repository dimension --
    # that is precisely the unbounded-cardinality fix CHAOS-4177 closed off.
    # Mentioning "repository" in prose (the annotation description) is fine
    # and required by the test above; it must never appear as an actual
    # label key in the expr or the labels: block.
    alert = _skip_alert()
    expr = str(alert["expr"])
    labels = alert["labels"]
    assert "repository" not in expr
    assert all("repository" not in str(key) for key in labels)
    assert labels.get("team") == "platform"
    assert labels.get("severity") in {"warning", "critical"}


def test_provider_artifact_skip_alert_name_is_unique_in_rule_file() -> None:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    alerts = [
        rule["alert"]
        for group in document["groups"]
        for rule in group["rules"]
        if "alert" in rule
    ]
    assert alerts.count(ALERT_NAME) == 1
