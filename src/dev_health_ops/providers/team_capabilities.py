from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TeamProviderCapability:
    provider: str
    supports_org_drift_discovery: bool
    unsupported_reason: str | None = None


ORG_TEAM_DRIFT_CAPABILITIES: tuple[TeamProviderCapability, ...] = (
    TeamProviderCapability("github", True),
    TeamProviderCapability("gitlab", True),
    TeamProviderCapability("jira", True),
    TeamProviderCapability("linear", True),
    TeamProviderCapability("ms-teams", True),
)


def team_provider_capabilities() -> tuple[TeamProviderCapability, ...]:
    return ORG_TEAM_DRIFT_CAPABILITIES


def org_drift_capable_providers() -> tuple[str, ...]:
    return tuple(
        capability.provider
        for capability in ORG_TEAM_DRIFT_CAPABILITIES
        if capability.supports_org_drift_discovery
    )
