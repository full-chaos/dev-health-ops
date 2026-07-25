from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "alerts" / "rules.yml"


def _go_worker_rules() -> list[dict[str, object]]:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    groups = document["groups"]
    group = next(candidate for candidate in groups if candidate["name"] == "go_workers")
    assert group["interval"] == "30s"
    return group["rules"]


def test_go_worker_alerts_cover_phase_one_runtime_signals() -> None:
    rules = _go_worker_rules()
    alerts = {str(rule["alert"]): str(rule["expr"]) for rule in rules}

    expected_metrics = {
        "GoWorkerOldestAvailableJobHigh": "worker_job_oldest_age_seconds",
        "GoWorkerExecutionSaturated": "worker_execution_saturation_ratio",
        "GoWorkerAttemptFailureRateHigh": "worker_job_attempts_total",
        "GoWorkerDomainStateMismatch": "worker_domain_state_mismatch_total",
        "GoWorkerStreamLagHigh": "worker_stream_lag",
        "GoWorkerStreamPendingTooOld": "worker_stream_oldest_pending_seconds",
        "GoWorkerQueueControlPoolPressure": "worker_database_pool_saturation_ratio",
        "GoWorkerTelemetryTargetDown": "dev-health-go-",
    }

    assert set(alerts) == set(expected_metrics)
    for alert, metric in expected_metrics.items():
        assert metric in alerts[alert]


def test_go_worker_alerts_keep_labels_low_cardinality_and_payload_free() -> None:
    rules = _go_worker_rules()
    serialized = yaml.safe_dump(rules, sort_keys=True)

    prohibited = (
        "organization_id",
        "repository",
        "job_id",
        "correlation_id",
        "encoded_args",
        "payload",
        "error_text",
    )
    for label in prohibited:
        assert label not in serialized


def test_alert_names_are_unique_across_rule_file() -> None:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    alerts = [
        rule["alert"]
        for group in document["groups"]
        for rule in group["rules"]
        if "alert" in rule
    ]

    assert len(alerts) == len(set(alerts))
