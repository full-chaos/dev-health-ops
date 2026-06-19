from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.workers import team_autoimport, team_autoimport_jira


@dataclass
class FakeDimensionSink:
    projects: dict[tuple[str, str, str], ProjectRecord]
    members: dict[tuple[str, str], MemberRecord]
    memberships: dict[tuple[str, str, str, str, str], TeamMembershipRecord]
    ownership: dict[tuple[str, str, str, str, str], TeamProjectOwnershipRecord]
    teams: dict[tuple[str, str], dict[str, Any]]
    jira_legacy_links: list[dict[str, Any]]
    closed: bool = False

    def write_projects(self, rows: list[ProjectRecord]) -> None:
        for row in rows:
            self.projects[(row.org_id, row.provider, row.id)] = row

    def write_members(self, rows: list[MemberRecord]) -> None:
        for row in rows:
            self.members[(row.org_id, row.member_id)] = row

    def write_team_memberships(self, rows: list[TeamMembershipRecord]) -> None:
        for row in rows:
            self.memberships[
                (row.org_id, row.provider, row.team_id, row.member_id, row.source)
            ] = row

    def write_team_project_ownership(
        self, rows: list[TeamProjectOwnershipRecord]
    ) -> None:
        for row in rows:
            self.ownership[
                (row.org_id, row.provider, row.project_id, row.team_id, row.source)
            ] = row

    async def insert_teams(self, teams: list[dict[str, Any]]) -> None:
        for team in teams:
            self.teams[(str(team["org_id"]), str(team["id"]))] = team

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.jira_legacy_links
            if row.get("org_id") == parameters.get("org_id")
        ]

    def close(self) -> None:
        self.closed = True


def _fake_sink(
    *, jira_legacy_links: list[dict[str, Any]] | None = None
) -> FakeDimensionSink:
    return FakeDimensionSink(
        projects={},
        members={},
        memberships={},
        ownership={},
        teams={},
        jira_legacy_links=list(jira_legacy_links or []),
    )


class CapturingClickHouseSink(FakeDimensionSink):
    instances: list[CapturingClickHouseSink] = []

    def __init__(self, *, dsn: str) -> None:
        super().__init__(
            projects={},
            members={},
            memberships={},
            ownership={},
            teams={},
            jira_legacy_links=[],
        )
        self.dsn = dsn
        self.instances.append(self)


def test_jira_populate_writes_native_and_jira_legacy_ownership_without_touching_links(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="jira",
                provider_team_id="OPS",
                name="Ops Project",
                associations={"project_keys": ["OPS"]},
            )
        ]

    async def discover_members_jira_bulk(
        self: object,
        *,
        email: str,
        api_token: str,
        url: str,
        project_keys: list[str],
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="jira",
                provider_identity="account-1",
                display_name="Ops Lead",
                email="ops@example.com",
                role="lead",
            )
        ]

    monkeypatch.setattr(
        team_autoimport_jira.TeamDiscoveryService,
        "discover_jira",
        discover_jira,
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    legacy_links = [
        {
            "org_id": "org-1",
            "project_key": "OPS",
            "ops_team_id": "ops-team-legacy",
            "project_name": "Ops Project",
            "ops_team_name": "Ops Legacy",
        }
    ]
    sink = _fake_sink(jira_legacy_links=legacy_links)

    summary = team_autoimport_jira.populate(
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={"mode": "sync_config"},
        sink=sink,
    )

    assert summary["team_project_ownership_imported"] == 2
    assert summary["jira_legacy_project_ownership_imported"] == 1
    assert legacy_links == sink.jira_legacy_links
    assert (
        "org-1",
        "jira",
        "org-1:jira:OPS",
        "OPS",
        "native",
    ) in sink.ownership
    assert (
        "org-1",
        "jira",
        "org-1:jira:OPS",
        "ops-team-legacy",
        "jira_legacy",
    ) in sink.ownership
    assert ("org-1", "jira:account-1") in sink.members


def test_chaos_2547_2544_jira_autoimport_uses_analytics_db_url_with_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="jira",
                provider_team_id="OPS",
                name="Ops Project",
                associations={"project_keys": ["OPS"]},
            )
        ]

    async def discover_members_jira_bulk(
        self: object,
        *,
        email: str,
        api_token: str,
        url: str,
        project_keys: list[str],
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="jira",
                provider_identity="account-1",
                display_name="Ops Lead",
                email="ops@example.com",
                role="lead",
            )
        ]

    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    CapturingClickHouseSink.instances = []
    monkeypatch.setattr(
        team_autoimport_jira.TeamDiscoveryService,
        "discover_jira",
        discover_jira,
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    monkeypatch.setattr(
        team_autoimport_jira,
        "ClickHouseMetricsSink",
        CapturingClickHouseSink,
    )

    summary = team_autoimport.run_team_autoimport(
        provider="jira",
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={"mode": "sync_config"},
        analytics_db_url="clickhouse://jira-config-dsn",
    )

    assert summary["status"] == "success"
    assert summary["projects_imported"] == 1
    assert summary["members_imported"] == 1
    assert summary["team_memberships_imported"] == 1
    assert summary["team_project_ownership_imported"] == 1
    assert len(CapturingClickHouseSink.instances) == 1
    sink = CapturingClickHouseSink.instances[0]
    assert sink.dsn == "clickhouse://jira-config-dsn"
    assert sink.closed is True
    assert ("org-1", "jira", "org-1:jira:OPS") in sink.projects
    assert ("org-1", "jira:account-1") in sink.members
    assert (
        "org-1",
        "jira",
        "OPS",
        "jira:account-1",
        "native",
    ) in sink.memberships


def test_jira_populate_preserves_manual_project_ownership_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="jira",
                provider_team_id="OPS",
                name="Ops Project",
                associations={"project_keys": ["OPS"]},
            )
        ]

    async def discover_members_jira_bulk(
        self: object,
        *,
        email: str,
        api_token: str,
        url: str,
        project_keys: list[str],
    ) -> list[DiscoveredMember]:
        return []

    monkeypatch.setattr(
        team_autoimport_jira.TeamDiscoveryService,
        "discover_jira",
        discover_jira,
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    sink = _fake_sink()
    manual = TeamProjectOwnershipRecord(
        org_id="org-1",
        provider="jira",
        team_id="manual-team",
        project_id="org-1:jira:OPS",
        project_key="OPS",
        source="manual",
        is_primary=1,
        specificity=100,
        priority=0,
        valid_from=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    sink.write_team_project_ownership([manual])

    team_autoimport_jira.populate(
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={"mode": "sync_config"},
        sink=sink,
    )

    assert (
        "org-1",
        "jira",
        "org-1:jira:OPS",
        "manual-team",
        "manual",
    ) in sink.ownership
    assert (
        "org-1",
        "jira",
        "org-1:jira:OPS",
        "OPS",
        "native",
    ) in sink.ownership
