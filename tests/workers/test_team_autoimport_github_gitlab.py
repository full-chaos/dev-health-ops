from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveryResult,
)
from dev_health_ops.workers import team_autoimport_github, team_autoimport_gitlab


@dataclass
class RecordingSink:
    teams: list[dict[str, Any]] = field(default_factory=list)
    repo_ownership: list[Any] = field(default_factory=list)
    project_ownership: list[Any] = field(default_factory=list)
    memberships: list[Any] = field(default_factory=list)
    manual_repo_ownership: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {
                "org_id": "org-1",
                "provider": "github",
                "team_id": "gh:manual",
                "repo_full_name": "full-chaos/manual",
                "source": "manual",
            }
        ]
    )

    async def insert_teams(self, rows: list[dict[str, Any]]) -> None:
        self.teams.extend(rows)

    def write_team_repo_ownership(self, rows: list[Any]) -> None:
        self.repo_ownership.extend(rows)

    def write_team_project_ownership(self, rows: list[Any]) -> None:
        self.project_ownership.extend(rows)

    def write_team_memberships(self, rows: list[Any]) -> None:
        self.memberships.extend(rows)


def test_github_org_import_writes_provider_access_repo_grants_and_nested_specificity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RecordingSink()

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="github",
                provider_team_id="platform",
                name="Platform",
                associations={
                    "repo_patterns": ["full-chaos/dev-health"],
                    "provider_org": org_name,
                },
            ),
            DiscoveredTeam(
                provider_type="github",
                provider_team_id="platform-api",
                name="Platform API",
                associations={
                    "repo_patterns": ["full-chaos/dev-health"],
                    "provider_org": org_name,
                    "parent_team_id": "platform",
                },
            ),
        ]

    async def discover_members_github(
        self, token: str, org_name: str, team_slug: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="github",
                provider_identity=f"{team_slug}-lead",
                display_name=f"{team_slug} lead",
                email=f"{team_slug}@example.com",
            )
        ]

    monkeypatch.setattr(
        team_autoimport_github.TeamDiscoveryService,
        "discover_github",
        discover_github,
    )
    monkeypatch.setattr(
        team_autoimport_github.TeamMembershipService,
        "discover_members_github",
        discover_members_github,
    )
    monkeypatch.setattr(
        team_autoimport_github, "ClickHouseMetricsSink", lambda dsn: sink
    )

    summary = team_autoimport_github.populate(
        org_id="org-1",
        credentials={"token": "secret", "org": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "sync_options": {"auto_import_teams": True},
        },
    )

    assert summary["teams_imported"] == 2
    assert summary["team_repo_ownership_imported"] == 2
    assert summary["team_memberships_imported"] == 2
    assert {row["id"] for row in sink.teams} == {"gh:platform", "gh:platform-api"}
    child_team = next(row for row in sink.teams if row["id"] == "gh:platform-api")
    assert child_team["parent_team_id"] == "gh:platform"
    assert {row.source for row in sink.repo_ownership} == {"provider_access"}
    parent_row = next(
        row for row in sink.repo_ownership if row.team_id == "gh:platform"
    )
    child_row = next(
        row for row in sink.repo_ownership if row.team_id == "gh:platform-api"
    )
    assert child_row.repo_full_name == parent_row.repo_full_name
    assert child_row.specificity > parent_row.specificity


def test_gitlab_group_import_writes_provider_access_project_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RecordingSink()

    async def discover_gitlab(
        self, token: str, group_path: str, url: str
    ) -> GitLabDiscoveryResult:
        return GitLabDiscoveryResult(
            teams=[
                DiscoveredTeam(
                    provider_type="gitlab",
                    provider_team_id="full-chaos",
                    name="Full Chaos",
                    associations={
                        "repo_patterns": ["full-chaos/platform"],
                        "provider_org": group_path,
                    },
                ),
                DiscoveredTeam(
                    provider_type="gitlab",
                    provider_team_id="full-chaos/dev-health",
                    name="Dev Health",
                    associations={
                        "repo_patterns": ["full-chaos/dev-health/api"],
                        "provider_org": group_path,
                    },
                ),
            ]
        )

    async def discover_members_gitlab(
        self, token: str, group_path: str, url: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="gitlab",
                provider_identity=group_path.replace("/", "-"),
                display_name=group_path,
            )
        ]

    monkeypatch.setattr(
        team_autoimport_gitlab.TeamDiscoveryService,
        "discover_gitlab",
        discover_gitlab,
    )
    monkeypatch.setattr(
        team_autoimport_gitlab.TeamMembershipService,
        "discover_members_gitlab",
        discover_members_gitlab,
    )
    monkeypatch.setattr(
        team_autoimport_gitlab, "ClickHouseMetricsSink", lambda dsn: sink
    )

    summary = team_autoimport_gitlab.populate(
        org_id="org-1",
        credentials={"token": "secret", "group_path": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "sync_options": {"auto_import_teams": True},
        },
    )

    assert summary["teams_imported"] == 2
    assert summary["projects_imported"] == 2
    assert summary["team_project_ownership_imported"] == 2
    assert {row["id"] for row in sink.teams} == {
        "gl:full-chaos",
        "gl:full-chaos/dev-health",
    }
    subgroup = next(
        row for row in sink.teams if row["id"] == "gl:full-chaos/dev-health"
    )
    assert subgroup["parent_team_id"] == "gl:full-chaos"
    assert {row.source for row in sink.project_ownership} == {"provider_access"}
    assert {row.project_key for row in sink.project_ownership} == {
        "full-chaos/platform",
        "full-chaos/dev-health/api",
    }


def test_github_personal_account_or_unsupported_response_skips_without_touching_manual_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RecordingSink()
    manual_before = list(sink.manual_repo_ownership)

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        raise RuntimeError("404 Not Found")

    def fail_if_sink_is_created(dsn: str) -> RecordingSink:
        raise AssertionError(
            "no-op personal account import must not write to ClickHouse"
        )

    monkeypatch.setattr(
        team_autoimport_github.TeamDiscoveryService,
        "discover_github",
        discover_github,
    )
    monkeypatch.setattr(
        team_autoimport_github, "ClickHouseMetricsSink", fail_if_sink_is_created
    )

    summary = team_autoimport_github.populate(
        org_id="org-1",
        credentials={"token": "secret", "org": "personal-user"},
        scope={"sync_options": {"auto_import_teams": True}},
    )

    assert summary["status"] == "skipped"
    assert summary["reason"] == "provider_discovery_skipped"
    assert summary["team_repo_ownership_imported"] == 0
    assert summary["team_memberships_imported"] == 0
    assert sink.manual_repo_ownership == manual_before
