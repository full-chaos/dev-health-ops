from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest
import requests

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.providers.identity import IdentityResolver
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
    # CHAOS-4323 round-3/narrow-round-4 follow-up: set to force query_dicts
    # to raise, simulating a ClickHouse read failure during roster
    # preservation (_existing_team_members).
    query_dicts_raises: bool = False

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
        if self.query_dicts_raises:
            raise RuntimeError("simulated ClickHouse read failure")
        return [
            row
            for row in self.jira_legacy_links
            if row.get("org_id") == parameters.get("org_id")
        ]

    def close(self) -> None:
        self.closed = True


def _fake_sink(
    *,
    jira_legacy_links: list[dict[str, Any]] | None = None,
    query_dicts_raises: bool = False,
) -> FakeDimensionSink:
    return FakeDimensionSink(
        projects={},
        members={},
        memberships={},
        ownership={},
        teams={},
        jira_legacy_links=list(jira_legacy_links or []),
        query_dicts_raises=query_dicts_raises,
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
    resolver = IdentityResolver(
        alias_to_canonical={
            "jira:accountid:account-1": "canonical-jira-id@example.com",
            "ops@example.com": "canonical-jira-email@example.com",
        }
    )

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
    monkeypatch.setattr(
        team_autoimport_jira, "load_identity_resolver", lambda: resolver
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
    membership = sink.memberships[("org-1", "jira", "OPS", "jira:account-1", "native")]
    assert membership.raw_provider_user_id == "canonical-jira-id@example.com"
    assert membership.raw_email == "ops@example.com"
    assert membership.identity_facets == [
        "canonical-jira-id@example.com",
        "jira:accountid:account-1",
        "canonical-jira-email@example.com",
        "ops@example.com",
    ]
    assert sink.teams[("org-1", "OPS")]["members"] == [
        "canonical-jira-id@example.com",
        "jira:accountid:account-1",
        "canonical-jira-email@example.com",
        "ops@example.com",
    ]
    # CHAOS-2609 (CS-COV) item 6: assert the emitted native ProjectRecord fields.
    project = sink.projects[("org-1", "jira", "org-1:jira:OPS")]
    assert project.project_key == "OPS"
    assert project.name == "Ops Project"
    assert project.org_id == "org-1"
    assert project.provider == "jira"


def test_jira_org_import_fails_closed_when_roster_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4323 round 3 + narrow-round-4 follow-up (codex adversarial-
    review): the fail-closed roster-preservation path, verified for Jira
    specifically (not just GitHub/GitLab) -- the counter must fire with
    provider="jira", and the team-dimension write must be skipped."""

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
        raise AssertionError(
            "member discovery must not run when the members category is off"
        )

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
    recorded: list[str] = []
    monkeypatch.setattr(
        team_autoimport_jira,
        "record_team_autoimport_roster_preservation_failed",
        lambda *, provider: recorded.append(provider),
    )
    sink = _fake_sink(query_dicts_raises=True)

    summary = team_autoimport_jira.populate(
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={
            "mode": "sync_config",
            "import_categories": {
                "teams": True,
                "projects": False,
                "members": False,
            },
        },
        sink=sink,
    )

    assert summary["teams_imported"] == 0
    assert summary["roster_preservation_failed"] is True
    assert sink.teams == {}
    assert recorded == ["jira"]


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


def test_jira_discovery_failure_skips_internally_without_clobbering_manual_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-2609 (CS-COV) item 3: a jira team-discovery failure (e.g. HTTP 403)
    is caught INSIDE populate (matching github/gitlab) and returns a skipped
    summary with reason ``provider_discovery_skipped`` — nothing is written, so a
    pre-existing manual ownership row is left intact."""

    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        raise RuntimeError("403")

    monkeypatch.setattr(
        team_autoimport_jira.TeamDiscoveryService,
        "discover_jira",
        discover_jira,
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

    assert summary["status"] == "skipped"
    assert summary["reason"] == "provider_discovery_skipped"
    assert summary["team_project_ownership_imported"] == 0
    # The manual ownership row is untouched and no native rows were written.
    assert (
        "org-1",
        "jira",
        "org-1:jira:OPS",
        "manual-team",
        "manual",
    ) in sink.ownership
    assert len(sink.ownership) == 1
    assert sink.teams == {}


def test_jira_members_dedupe_to_one_row_per_member_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-2609 (CS-COV) item 4: the same account_id surfaced for multiple
    projects de-dupes to a single member/membership row with a stable
    ``jira:<account_id>`` id, and an email-less member is still imported."""

    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="jira",
                provider_team_id="OPS",
                name="Ops Project",
                associations={"project_keys": ["PROJ1", "PROJ2"]},
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
        # Same account_id seen across two projects, plus an email-less member.
        return [
            DiscoveredMember(
                provider_type="jira",
                provider_identity="acc-1",
                display_name="Shared Member",
                email="shared@example.com",
            ),
            DiscoveredMember(
                provider_type="jira",
                provider_identity="acc-1",
                display_name="Shared Member",
                email="shared@example.com",
            ),
            DiscoveredMember(
                provider_type="jira",
                provider_identity="acc-2",
                display_name="No Email Member",
                email=None,
            ),
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
    sink = _fake_sink()

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

    assert summary["members_imported"] == 2
    assert summary["team_memberships_imported"] == 2
    assert set(sink.members) == {("org-1", "jira:acc-1"), ("org-1", "jira:acc-2")}
    assert sink.members[("org-1", "jira:acc-2")].email is None
    assert ("org-1", "jira", "OPS", "jira:acc-1", "native") in sink.memberships
    assert ("org-1", "jira", "OPS", "jira:acc-2", "native") in sink.memberships
    # CHAOS-2609: the canonical-ladder facet carries the resolver-consumed
    # identity jira:accountid:<account_id> (member_id PK keeps the jira:<id> form).
    assert (
        sink.memberships[
            ("org-1", "jira", "OPS", "jira:acc-2", "native")
        ].raw_provider_user_id
        == "jira:accountid:acc-2"
    )


class _FakeSprintJiraClient:
    """Stands in for JiraClient: board 81 answers the real-world 400 (no
    sprint support -- e.g. a kanban-only board), board 82 returns a sprint
    normally. CHAOS-4357 requires this NOT to abort the whole populate,
    strict_reference_discovery included."""

    def __init__(self, *, auth: object, org_id: str) -> None:
        self.org_id = org_id
        self.closed = False

    def iter_boards(self, *, project_key: str):
        yield {"id": 81}
        yield {"id": 82}

    def iter_board_sprints(self, *, board_id: int):
        if board_id == 81:
            response = requests.Response()
            response.status_code = 400
            # CHAOS-4357 round 2 (codex P1): a bare 400 with no body is no
            # longer skippable -- only Jira's documented errorMessages
            # envelope for this endpoint is.
            response._content = json.dumps(
                {"errorMessages": ["The board does not support sprints"]}
            ).encode()
            raise requests.HTTPError(
                "400 Client Error: Bad Request for url: "
                ".../rest/agile/1.0/board/81/sprint",
                response=response,
            )
        yield {"id": 501, "name": "Sprint 1", "state": "active"}

    def close(self) -> None:
        self.closed = True


def test_jira_populate_skips_one_boards_sprint_400_under_strict_reference_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4357: a single board's 400 on GET .../board/{id}/sprint (typical
    for a board without sprint support) must be skipped, not raised -- even
    under strict_reference_discovery=True, where the prior behavior let one
    bad board fail the ENTIRE org's reference discovery (and, by cascading
    through run_reference_discovery_populate_strict, block that run's own
    dispatch forever)."""

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
        team_autoimport_jira.TeamDiscoveryService, "discover_jira", discover_jira
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    monkeypatch.setattr(team_autoimport_jira, "JiraClient", _FakeSprintJiraClient)

    sink = _fake_sink()

    summary = team_autoimport_jira.populate(
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={"mode": "sync_reference_discovery", "strict_reference_discovery": True},
        sink=sink,
    )

    # The whole populate must succeed (no raise), the healthy board's team
    # data AND sprint must both still land, and the failing board is simply
    # absent from the output rather than poisoning the run.
    assert summary["teams_imported"] == 1
    assert summary["reference_team_keys"] == ["OPS"]
    assert summary["sprints_imported"] == 1
    assert summary["reference_sprint_ids"] == ["501"]


def test_jira_strict_reference_discovery_still_resolves_sprints_when_all_categories_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4437 (codex review, P1): an org with every CHAOS-4323 category
    disabled must NOT skip sprint/cycle discovery under strict reference
    discovery -- sprints are unconditional reference data (see
    team_autoimport_categories.py's module docstring), needed to resolve
    dispatch-blocking sprint keys regardless of the org's write-side
    selection. Before this fix, the "no_categories_selected" early return
    fired unconditionally and skipped sprint discovery too, even in strict
    mode -- reachable only once run_team_autoimport_strict started threading
    real selections (this same PR)."""

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
        team_autoimport_jira.TeamDiscoveryService, "discover_jira", discover_jira
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    monkeypatch.setattr(team_autoimport_jira, "JiraClient", _FakeSprintJiraClient)

    sink = _fake_sink()

    summary = team_autoimport_jira.populate(
        org_id="org-1",
        credentials={
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        scope={
            "mode": "sync_reference_discovery",
            "strict_reference_discovery": True,
            "import_categories": {
                "teams": False,
                "projects": False,
                "members": False,
            },
        },
        sink=sink,
    )

    # Sprint discovery still ran and is still claimed for readback.
    assert summary["sprints_imported"] == 1
    assert summary["reference_sprint_ids"] == ["501"]
    # But nothing writable was written -- the org's selection is honoured.
    assert summary["teams_imported"] == 0
    assert summary["reference_team_keys"] == []
    assert summary["members_imported"] == 0
    assert sink.teams == {}
    assert sink.memberships == {}


class _FakeForbiddenSprintJiraClient:
    """Board 81 answers 403 (a revoked/insufficiently-scoped credential),
    NOT the documented "no sprint support" 400. CHAOS-4357 round 2 (codex
    P1): this must propagate, never be silently treated as a per-board
    skip -- a 403 means the whole reference set may be wrong/incomplete,
    which strict reference discovery must not report as a success."""

    def __init__(self, *, auth: object, org_id: str) -> None:
        self.org_id = org_id
        self.closed = False

    def iter_boards(self, *, project_key: str):
        yield {"id": 81}

    def iter_board_sprints(self, *, board_id: int):
        response = requests.Response()
        response.status_code = 403
        raise requests.HTTPError(
            "403 Client Error: Forbidden for url: .../rest/agile/1.0/board/81/sprint",
            response=response,
        )
        yield  # pragma: no cover - generator shape only

    def close(self) -> None:
        self.closed = True


def test_jira_populate_reraises_a_403_sprint_listing_failure_under_strict_reference_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4357 round 2 (codex P1): only the documented Jira 400 ("board
    does not support sprints") is a per-board skip. A 403 -- revoked or
    insufficiently-scoped credentials -- is NOT that case and must still
    fail strict reference discovery instead of being silently absorbed as
    if the board simply had no sprints."""

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
        team_autoimport_jira.TeamDiscoveryService, "discover_jira", discover_jira
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    monkeypatch.setattr(
        team_autoimport_jira, "JiraClient", _FakeForbiddenSprintJiraClient
    )

    sink = _fake_sink()

    with pytest.raises(requests.HTTPError):
        team_autoimport_jira.populate(
            org_id="org-1",
            credentials={
                "email": "jira@example.com",
                "api_token": "jira-token",
                "base_url": "https://jira.example.com",
            },
            scope={
                "mode": "sync_reference_discovery",
                "strict_reference_discovery": True,
            },
            sink=sink,
        )


class _FakeBoardListing400JiraClient:
    """iter_boards itself answers 400 (e.g. a malformed projectKeyOrId) --
    the same errorMessages envelope Jira uses for a benign "no sprints"
    response, but board LISTING has no such benign shape. CHAOS-4357 round
    2 (codex P1): this must propagate, never be treated as a per-project
    skip."""

    def __init__(self, *, auth: object, org_id: str) -> None:
        self.org_id = org_id
        self.closed = False

    def iter_boards(self, *, project_key: str):
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps(
            {"errorMessages": [f"The project key or id '{project_key}' does not exist"]}
        ).encode()
        raise requests.HTTPError(
            "400 Client Error: Bad Request for url: .../rest/agile/1.0/board",
            response=response,
        )
        yield  # pragma: no cover - generator shape only

    def iter_board_sprints(self, *, board_id: int):
        yield {"id": 501, "name": "Sprint 1", "state": "active"}  # pragma: no cover

    def close(self) -> None:
        self.closed = True


def test_jira_populate_reraises_a_board_listing_400_under_strict_reference_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4357 round 2 (codex P1): board listing has no documented benign
    400 shape -- Jira's generic errorMessages envelope covers both "no
    boards" and "malformed input" indistinguishably, so isolating it per
    project risks silently omitting a real project's reference data while
    strict discovery still reports success. A 400 here must propagate."""

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
        team_autoimport_jira.TeamDiscoveryService, "discover_jira", discover_jira
    )
    monkeypatch.setattr(
        team_autoimport_jira.TeamMembershipService,
        "discover_members_jira_bulk",
        discover_members_jira_bulk,
    )
    monkeypatch.setattr(
        team_autoimport_jira, "JiraClient", _FakeBoardListing400JiraClient
    )

    sink = _fake_sink()

    with pytest.raises(requests.HTTPError):
        team_autoimport_jira.populate(
            org_id="org-1",
            credentials={
                "email": "jira@example.com",
                "api_token": "jira-token",
                "base_url": "https://jira.example.com",
            },
            scope={
                "mode": "sync_reference_discovery",
                "strict_reference_discovery": True,
            },
            sink=sink,
        )
