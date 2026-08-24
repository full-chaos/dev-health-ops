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
from typing import Any

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


def _real_metric_name_and_labels() -> tuple[str, str]:
    """Pin the metric name and its third label from the Go source itself,
    rather than retyping a string a refactor could silently drift from.

    `RecordArtifactSkipped` writes into `m.artifactSkipped`, and
    `WritePrometheus` serializes that map through
    `writeProviderDatasetReasonCounter(writer, <name>, <help>, <label>,
    m.artifactSkipped)` -- provider and dataset are baked into that helper's
    output format, and <label> is the third, reason.
    """
    text = BUDGET_GO_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"writeProviderDatasetReasonCounter\(\s*"
        r'writer,\s*"(?P<name>[^"]+)",\s*'
        r'"(?P<help>[^"]+)",\s*'
        r'"(?P<label>[^"]+)",\s*m\.artifactSkipped,',
        text,
        re.DOTALL,
    )
    assert match, (
        "could not find the writeProviderDatasetReasonCounter(...) call for "
        "m.artifactSkipped in budget.go -- has RecordArtifactSkipped's "
        "wiring changed shape?"
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
    assert "all_artifacts_unreadable" in description


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
