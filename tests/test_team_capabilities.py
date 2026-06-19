from __future__ import annotations

import pytest

from dev_health_ops.providers.team_capabilities import (
    org_drift_capable_providers,
    team_provider_capabilities,
)


def test_org_team_drift_capability_registry_enumerates_supported_providers():
    capabilities = {item.provider: item for item in team_provider_capabilities()}
    assert tuple(capabilities) == ("github", "gitlab", "jira", "linear", "ms-teams")

    for provider in ("github", "gitlab", "jira", "linear", "ms-teams"):
        capability = capabilities[provider]
        if not capability.supports_org_drift_discovery:
            pytest.xfail(capability.unsupported_reason or f"{provider} unsupported")
        assert provider in org_drift_capable_providers()
