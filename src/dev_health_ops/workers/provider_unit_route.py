"""Fail-closed transport gate for complete provider sync-unit routes."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_FALSE = frozenset({"", "0", "false", "no", "off"})
_TRUE = frozenset({"1", "true", "yes", "on"})


class ProviderUnitRouteError(ValueError):
    """Value-free rejection of an invalid or incomplete route switch."""


def _flag(environment: Mapping[str, str], name: str) -> bool:
    value = environment.get(name, "").strip().lower()
    if value in _FALSE:
        return False
    if value in _TRUE:
        return True
    raise ProviderUnitRouteError("provider unit route switch is invalid")


@dataclass(frozen=True, slots=True)
class ProviderUnitRouteSwitches:
    linear_work_items: bool = False
    jira_work_items: bool = False
    jira_incidents: bool = False
    launchdarkly_feature_flags: bool = False

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> ProviderUnitRouteSwitches:
        source = os.environ if environment is None else environment
        switches = cls(
            linear_work_items=_flag(source, "WORKER_LINEAR_WORK_ITEMS_ENABLED"),
            jira_work_items=_flag(source, "WORKER_JIRA_WORK_ITEMS_ENABLED"),
            jira_incidents=_flag(source, "WORKER_JIRA_INCIDENTS_ENABLED"),
            launchdarkly_feature_flags=_flag(
                source, "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED"
            ),
        )
        switches.require_complete_routes()
        return switches

    def require_complete_routes(self) -> None:
        if self.linear_work_items or self.jira_work_items or self.jira_incidents:
            raise ProviderUnitRouteError("enabled provider unit route is incomplete")

    def routes_to_river(self, provider: str, dataset: str) -> bool:
        self.require_complete_routes()
        return self.launchdarkly_feature_flags and self.is_canary_scope(
            provider, dataset
        )

    @staticmethod
    def is_canary_scope(provider: str, dataset: str) -> bool:
        """Return whether a unit is covered by the checked-in canary scope."""

        return (
            provider.strip().lower() == "launchdarkly"
            and dataset.strip().lower() == "feature-flags"
        )
