from typing import Any

import pytest

from dev_health_ops.api.admin.routers.sync import (
    PROVIDER_SYNC_TARGETS,
    _planner_dataset_keys,
    _planner_dataset_options,
)


def test_provider_sync_targets_include_feature_flag_sources():
    assert "feature-flags" in PROVIDER_SYNC_TARGETS["gitlab"]
    assert PROVIDER_SYNC_TARGETS["launchdarkly"] == ["feature-flags"]


def test_pagerduty_operational_target_expands_to_all_operational_datasets() -> None:
    # Given: the backend-owned PagerDuty operational sync target.

    # When: its planner datasets are resolved.
    dataset_keys = _planner_dataset_keys("pagerduty", ["operational"])

    # Then: parent incidents and every child/reference collection are scheduled.
    assert PROVIDER_SYNC_TARGETS["pagerduty"] == ["operational"]
    assert dataset_keys == [
        "incidents",
        "services",
        "business-services",
        "escalation-policies",
        "schedules",
        "on-calls",
        "users",
        "teams",
        "incident-alerts",
        "incident-log-entries",
        "incident-notes",
    ]


def test_planner_dataset_options_forwards_pagerduty_service_mappings() -> None:
    mappings = {
        "admin": {"svc-1": [{"provider": "github", "full_name": "full-chaos/api"}]},
        "compass": {"svc-2": [{"provider": "gitlab", "full_name": "full-chaos/w"}]},
    }
    parent_options = {"service_repository_mappings": mappings}

    options = _planner_dataset_options(
        "pagerduty", "services", ["services"], parent_options
    )

    assert options["legacy_targets"] == ["services"]
    assert options["service_repository_mappings"] == mappings


def test_planner_dataset_options_scopes_mappings_to_pagerduty_services() -> None:
    parent_options: dict[str, Any] = {
        "service_repository_mappings": {"admin": {"svc-1": []}},
    }

    # Wrong dataset, wrong provider, and missing config all omit the mappings.
    assert "service_repository_mappings" not in _planner_dataset_options(
        "pagerduty", "incidents", ["incidents"], parent_options
    )
    assert "service_repository_mappings" not in _planner_dataset_options(
        "github", "services", ["services"], parent_options
    )
    assert "service_repository_mappings" not in _planner_dataset_options(
        "pagerduty", "services", ["services"], {}
    )


def test_pagerduty_malformed_legacy_target_is_flagged_before_planning() -> None:
    # Given: a pre-existing PagerDuty config that omits the operational target.

    # When: planner dataset expansion handles the malformed target.
    with pytest.raises(ValueError, match="PagerDuty sync target must be operational"):
        _planner_dataset_keys("pagerduty", ["incidents"])

    # Then: no zero-unit run can silently claim a successful PagerDuty sync.
