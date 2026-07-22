from __future__ import annotations

from dev_health_ops.providers.pagerduty.oauth import required_read_scopes
from dev_health_ops.sync.datasets import supported_datasets, supported_legacy_targets


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


def test_pagerduty_registry_dataset_keys_normalize_to_oauth_families() -> None:
    # Given: every PagerDuty dataset exposed by the sync registry.
    registry_dataset_keys = {
        spec.dataset_key for spec in supported_datasets("pagerduty")
    }

    # When: the OAuth read scopes are derived from those hyphenated keys.
    scopes = required_read_scopes(registry_dataset_keys)

    # Then: eight endpoint families collapse to PagerDuty's seven read scopes.
    assert scopes == {
        "incidents.read",
        "services.read",
        "escalation_policies.read",
        "schedules.read",
        "oncalls.read",
        "users.read",
        "teams.read",
    }


def test_operational_target_is_pagerduty_specific() -> None:
    # Given: providers that historically schedule native incident ingestion.

    # When: their legacy targets are listed beside PagerDuty's REST target.
    github_targets = supported_legacy_targets("github")
    gitlab_targets = supported_legacy_targets("gitlab")
    pagerduty_targets = supported_legacy_targets("pagerduty")

    # Then: PagerDuty alone expands its all-or-nothing operational collection.
    assert "incidents" in github_targets
    assert "incidents" in gitlab_targets
    assert "operational" not in github_targets
    assert "operational" not in gitlab_targets
    assert pagerduty_targets == ["operational"]
