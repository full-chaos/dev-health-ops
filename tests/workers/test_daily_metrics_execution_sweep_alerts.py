"""CHAOS-5049: the dead-claim sweep's alert rules must exist and stay correct.

Why these are worth a test rather than trusting the yaml: the sweep is fail-open
by design — it can never fail the dispatch job it rides on — so these rules are
the ONLY way a broken sweep becomes visible. A rules file that silently loses
them turns the mechanism back into the silent one it was built to replace.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "alerts" / "rules.yml"
GROUP_NAME = "daily_metrics_execution_sweep"
COUNTER = "dev_health_daily_metrics_executions_swept_ambiguous_total"


def _sweep_rules() -> list[dict[str, Any]]:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    group = next(
        candidate for candidate in document["groups"] if candidate["name"] == GROUP_NAME
    )
    assert group["interval"] == "60s"
    return group["rules"]


def test_sweep_alerts_are_exactly_the_two_intended() -> None:
    alerts = {str(rule["alert"]): str(rule["expr"]) for rule in _sweep_rules()}
    assert set(alerts) == {
        "DailyMetricsExecutionSweepFailing",
        "DailyMetricsDeadClaimExecutionsAccumulating",
    }
    for expr in alerts.values():
        assert COUNTER in expr


def test_the_failure_alert_watches_the_failed_outcome() -> None:
    """The load-bearing one.

    Without an alert on outcome="failed", a sweep erroring on every pass is
    indistinguishable from a healthy one with nothing to do — both leave
    outcome="swept" at zero. This asserts the alert reads the failure label
    specifically, not merely the counter.
    """
    alerts = {str(rule["alert"]): rule for rule in _sweep_rules()}
    rule = alerts["DailyMetricsExecutionSweepFailing"]
    assert 'outcome="failed"' in str(rule["expr"])
    # for < the rate() window, so one increment can sustain a positive rate long
    # enough to fire; the sweep runs per dispatch, not continuously.
    assert rule["for"] == "5m"
    assert "[15m]" in str(rule["expr"])


def test_the_accumulation_alert_is_a_threshold_not_a_tripwire() -> None:
    """A single stranded execution is normal attrition, not a page.

    The sweep exists so those clear themselves. Alerting at > 0 would page on
    the mechanism working; the signal is a RATE of stranding.
    """
    alerts = {str(rule["alert"]): rule for rule in _sweep_rules()}
    expr = str(alerts["DailyMetricsDeadClaimExecutionsAccumulating"]["expr"])
    assert 'outcome="swept"' in expr
    assert "> 10" in expr, "a > 0 threshold would page on normal attrition"


def test_skipped_claim_active_is_not_alerted() -> None:
    """Refusing a live claim is correct behaviour, never a paging condition."""
    for rule in _sweep_rules():
        assert "skipped_claim_active" not in str(rule["expr"])


def test_every_sweep_alert_is_routable() -> None:
    """A rule with no severity or team cannot be routed anywhere."""
    for rule in _sweep_rules():
        labels = rule["labels"]
        assert labels["severity"] in {"warning", "critical"}
        assert labels["team"] == "platform"
        assert str(rule["annotations"]["summary"]).strip()
        assert str(rule["annotations"]["description"]).strip()
