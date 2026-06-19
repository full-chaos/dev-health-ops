from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.workers import team_autoimport, team_autoimport_linear


@dataclass
class FakeDimensionSink:
    projects: dict[tuple[str, str, str], ProjectRecord]
    members: dict[tuple[str, str], MemberRecord]
    memberships: dict[tuple[str, str, str, str, str], TeamMembershipRecord]
    ownership: dict[tuple[str, str, str, str, str], TeamProjectOwnershipRecord]
    teams: dict[tuple[str, str], dict[str, Any]]
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

    def close(self) -> None:
        self.closed = True


def _fake_sink() -> FakeDimensionSink:
    return FakeDimensionSink(
        projects={},
        members={},
        memberships={},
        ownership={},
        teams={},
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
        )
        self.dsn = dsn
        self.instances.append(self)


def test_linear_populate_writes_projects_memberships_and_project_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_linear(self: object, api_key: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="linear",
                provider_team_id="ENG",
                name="Engineering",
                associations={"project_keys": ["ENG"]},
            )
        ]

    async def discover_members_linear(
        self: object, api_key: str, team_key: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="linear",
                provider_identity="dev@example.com",
                display_name="Dev User",
                email="dev@example.com",
            )
        ]

    monkeypatch.setattr(
        team_autoimport_linear.TeamDiscoveryService,
        "discover_linear",
        discover_linear,
    )
    monkeypatch.setattr(
        team_autoimport_linear.TeamMembershipService,
        "discover_members_linear",
        discover_members_linear,
    )
    sink = _fake_sink()

    summary = team_autoimport_linear.populate(
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        sink=sink,
    )

    assert summary["projects_imported"] == 1
    assert summary["members_imported"] == 1
    assert summary["team_memberships_imported"] == 1
    assert summary["team_project_ownership_imported"] == 1
    assert ("org-1", "linear", "org-1:linear:ENG") in sink.projects
    assert ("org-1", "linear:dev@example.com") in sink.members
    assert (
        "org-1",
        "linear",
        "ENG",
        "linear:dev@example.com",
        "native",
    ) in sink.memberships
    assert (
        "org-1",
        "linear",
        "org-1:linear:ENG",
        "ENG",
        "native",
    ) in sink.ownership
    assert sink.teams[("org-1", "ENG")]["native_team_key"] == "ENG"


def test_chaos_2547_2544_autoimport_uses_analytics_db_url_with_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_linear(self: object, api_key: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="linear",
                provider_team_id="ENG",
                name="Engineering",
                associations={"project_keys": ["ENG"]},
            )
        ]

    async def discover_members_linear(
        self: object, api_key: str, team_key: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="linear",
                provider_identity="dev@example.com",
                display_name="Dev User",
                email="dev@example.com",
            )
        ]

    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    CapturingClickHouseSink.instances = []
    monkeypatch.setattr(
        team_autoimport_linear.TeamDiscoveryService,
        "discover_linear",
        discover_linear,
    )
    monkeypatch.setattr(
        team_autoimport_linear.TeamMembershipService,
        "discover_members_linear",
        discover_members_linear,
    )
    monkeypatch.setattr(
        team_autoimport_linear,
        "ClickHouseMetricsSink",
        CapturingClickHouseSink,
    )

    summary = team_autoimport.run_team_autoimport(
        provider="linear",
        org_id="org-1",
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config"},
        analytics_db_url="clickhouse://config-dsn",
    )

    assert summary["status"] == "success"
    assert summary["projects_imported"] == 1
    assert summary["members_imported"] == 1
    assert summary["team_memberships_imported"] == 1
    assert summary["team_project_ownership_imported"] == 1
    assert len(CapturingClickHouseSink.instances) == 1
    sink = CapturingClickHouseSink.instances[0]
    assert sink.dsn == "clickhouse://config-dsn"
    assert sink.closed is True
    assert ("org-1", "linear", "org-1:linear:ENG") in sink.projects
    assert ("org-1", "linear:dev@example.com") in sink.members
    assert (
        "org-1",
        "linear",
        "ENG",
        "linear:dev@example.com",
        "native",
    ) in sink.memberships


def test_linear_populate_rerun_keeps_stable_logical_dimension_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover_linear(self: object, api_key: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="linear",
                provider_team_id="ENG",
                name="Engineering",
                associations={"project_keys": ["ENG"]},
            )
        ]

    async def discover_members_linear(
        self: object, api_key: str, team_key: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="linear",
                provider_identity="dev@example.com",
                display_name="Dev User",
                email="dev@example.com",
            )
        ]

    monkeypatch.setattr(
        team_autoimport_linear.TeamDiscoveryService,
        "discover_linear",
        discover_linear,
    )
    monkeypatch.setattr(
        team_autoimport_linear.TeamMembershipService,
        "discover_members_linear",
        discover_members_linear,
    )
    sink = _fake_sink()

    for _ in range(2):
        team_autoimport_linear.populate(
            org_id="org-1",
            credentials={"api_key": "lin-key"},
            scope={"mode": "sync_config"},
            sink=sink,
        )

    assert len(sink.projects) == 1
    assert len(sink.members) == 1
    assert len(sink.memberships) == 1
    assert len(sink.ownership) == 1
    assert len(sink.teams) == 1
