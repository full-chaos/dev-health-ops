from __future__ import annotations

import pytest

from dev_health_ops.providers.team_capabilities import (
    all_auto_import_capabilities,
    auto_import_capabilities,
    org_drift_capable_providers,
    team_provider_capabilities,
    unsupported_auto_import_categories,
)


def test_org_team_drift_capability_registry_enumerates_supported_providers():
    capabilities = {item.provider: item for item in team_provider_capabilities()}
    assert tuple(capabilities) == ("github", "gitlab", "jira", "linear", "ms-teams")

    for provider in ("github", "gitlab", "jira", "linear", "ms-teams"):
        capability = capabilities[provider]
        if not capability.supports_org_drift_discovery:
            pytest.xfail(capability.unsupported_reason or f"{provider} unsupported")
        assert provider in org_drift_capable_providers()


# ---------------------------------------------------------------------------
# CHAOS-4323: per-category auto-import capability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ["gitlab", "jira", "linear"])
def test_auto_import_capabilities_supports_all_three_for_full_providers(provider):
    capability = auto_import_capabilities(provider)
    assert capability.teams is True
    assert capability.projects is True
    assert capability.members is True
    assert dict(capability.reasons) == {}


def test_auto_import_capabilities_github_has_no_projects_support():
    capability = auto_import_capabilities("GitHub")  # case-insensitive
    assert capability.teams is True
    assert capability.projects is False
    assert capability.members is True
    assert capability.reasons["projects"] == (
        "GitHub attributes ownership via repos, not projects."
    )
    assert capability.supports("teams") is True
    assert capability.supports("projects") is False


def test_auto_import_capabilities_unrecognized_provider_supports_nothing():
    capability = auto_import_capabilities("ms-teams")
    assert capability.teams is False
    assert capability.projects is False
    assert capability.members is False
    assert set(capability.reasons) == {"teams", "projects", "members"}


def test_all_auto_import_capabilities_exposes_every_populator_backed_provider():
    capabilities = all_auto_import_capabilities()
    assert set(capabilities) == {"github", "gitlab", "jira", "linear"}
    assert capabilities["github"].projects is False


@pytest.mark.parametrize(
    ("provider", "sync_options", "expected"),
    [
        ("linear", {"auto_import_teams": True}, {}),
        ("github", {"auto_import_teams": True}, {}),
        (
            "github",
            {"auto_import_projects": True},
            {"projects": "GitHub attributes ownership via repos, not projects."},
        ),
        (
            "github",
            {
                "auto_import_teams": True,
                "auto_import_projects": True,
                "auto_import_members": True,
            },
            {"projects": "GitHub attributes ownership via repos, not projects."},
        ),
        # A category not requested at all is never flagged, even if unsupported.
        ("github", {}, {}),
        (
            "unknown-provider",
            {"auto_import_teams": True},
            {"teams": "provider does not support team/project/member auto-import"},
        ),
    ],
)
def test_unsupported_auto_import_categories(provider, sync_options, expected):
    assert unsupported_auto_import_categories(provider, sync_options) == expected
