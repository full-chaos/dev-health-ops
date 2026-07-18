from __future__ import annotations

from dev_health_ops.sync.datasets import supported_datasets


def test_pagerduty_exposes_the_operational_rest_dataset_set() -> None:
    # Given: the PagerDuty provider's sync dataset registry entry.

    # When: its supported datasets are resolved.
    dataset_keys = {spec.dataset_key for spec in supported_datasets("pagerduty")}

    # Then: every REST collection has an independently schedulable dataset.
    assert dataset_keys == {
        "services",
        "business-services",
        "escalation-policies",
        "schedules",
        "on-calls",
        "users",
        "teams",
        "incidents",
        "incident-alerts",
        "incident-log-entries",
        "incident-notes",
    }
