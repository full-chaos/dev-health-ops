from __future__ import annotations

import pytest

from dev_health_ops.workers.provider_unit_route import (
    ProviderUnitRouteError,
    ProviderUnitRouteSwitches,
)


def test_route_switch_is_exact_and_independent() -> None:
    switches = ProviderUnitRouteSwitches.from_environment(
        {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": "true"}
    )
    assert switches.routes_to_river("launchdarkly", "feature-flags")
    assert not switches.routes_to_river("launchdarkly", "work-items")
    assert not switches.routes_to_river("gitlab", "feature-flags")


@pytest.mark.parametrize(
    "name",
    (
        "WORKER_LINEAR_WORK_ITEMS_ENABLED",
        "WORKER_JIRA_WORK_ITEMS_ENABLED",
        "WORKER_JIRA_INCIDENTS_ENABLED",
    ),
)
def test_incomplete_routes_fail_closed(name: str) -> None:
    with pytest.raises(ProviderUnitRouteError, match="incomplete"):
        ProviderUnitRouteSwitches.from_environment({name: "true"})


def test_invalid_switch_fails_closed_without_echoing_value() -> None:
    value = "secret-looking-invalid-value"
    with pytest.raises(ProviderUnitRouteError) as raised:
        ProviderUnitRouteSwitches.from_environment(
            {"WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED": value}
        )
    assert value not in str(raised.value)
