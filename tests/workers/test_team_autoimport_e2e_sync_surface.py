from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveryResult,
)
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import (
    TeamAttributionCandidate,
    TeamAttributionContext,
    TeamAttributionSource,
    resolve_team_attribution,
)
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
    TeamRepoOwnershipRecord,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import JobRun, ScheduledJob, SyncConfiguration
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemProvider,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.teams import TeamResolver, _build_member_to_team
from dev_health_ops.workers import (
    sync_runtime,
    team_autoimport_github,
    team_autoimport_gitlab,
    team_autoimport_jira,
    team_autoimport_linear,
)
from tests._helpers import tables_of

ORG_ID = "org-chaos-2547"
DAY = date(2026, 6, 19)
NOW = datetime(2026, 6, 19, 12, tzinfo=timezone.utc)


@dataclass
class RecordingClickHouseSink:
    teams: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    projects: dict[tuple[str, str, str], ProjectRecord] = field(default_factory=dict)
    members: dict[tuple[str, str], MemberRecord] = field(default_factory=dict)
    memberships: dict[tuple[str, str, str, str, str], TeamMembershipRecord] = field(
        default_factory=dict
    )
    project_ownership: dict[
        tuple[str, str, str, str, str], TeamProjectOwnershipRecord
    ] = field(default_factory=dict)
    repo_ownership: dict[tuple[str, str, str, str], TeamRepoOwnershipRecord] = field(
        default_factory=dict
    )
    work_items: list[dict[str, Any]] = field(default_factory=list)
    closed: bool = False

    async def insert_teams(self, teams: list[dict[str, Any]]) -> None:
        for team in teams:
            self.teams[(str(team["org_id"]), str(team["id"]))] = dict(team)

    def write_projects(self, rows: Sequence[ProjectRecord]) -> None:
        for row in rows:
            self.projects[(row.org_id, row.provider, row.id)] = row

    def write_members(self, rows: Sequence[MemberRecord]) -> None:
        for row in rows:
            self.members[(row.org_id, row.member_id)] = row

    def write_team_memberships(self, rows: Sequence[TeamMembershipRecord]) -> None:
        for row in rows:
            self.memberships[
                (row.org_id, row.provider, row.team_id, row.member_id, row.source)
            ] = row

    def write_team_project_ownership(
        self, rows: Sequence[TeamProjectOwnershipRecord]
    ) -> None:
        for row in rows:
            self.project_ownership[
                (row.org_id, row.provider, row.project_id, row.team_id, row.source)
            ] = row

    def write_team_repo_ownership(
        self, rows: Sequence[TeamRepoOwnershipRecord]
    ) -> None:
        for row in rows:
            self.repo_ownership[
                (row.org_id, row.provider, row.repo_full_name, row.team_id)
            ] = row

    def write_work_items(self, rows: Sequence[dict[str, Any]]) -> None:
        self.work_items.extend(dict(row) for row in rows)

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return []

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def sync_session_factory(tmp_path: Any) -> Iterator[Callable[[], Any]]:
    db_path = tmp_path / "chaos-2547-sync.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(
        engine,
        tables=tables_of(SyncConfiguration, ScheduledJob, JobRun),
    )
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def session_scope() -> Iterator[Session]:
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    try:
        yield session_scope
    finally:
        engine.dispose()


def _seed_sync_config(
    session_factory: Callable[[], Any], *, provider: str, sync_options: dict[str, Any]
) -> str:
    with session_factory() as session:
        config = SyncConfiguration(
            name=f"chaos-2547-{provider}",
            provider=provider,
            org_id=ORG_ID,
            sync_targets=["work-items"],
            sync_options=sync_options,
        )
        session.add(config)
        session.flush()
        return str(config.id)


def _patch_runtime_surface(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: Callable[[], Any],
    sink: RecordingClickHouseSink,
) -> None:
    import dev_health_ops.db as db
    import dev_health_ops.metrics.job_work_items as job_work_items

    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://test")
    monkeypatch.setattr(db, "get_postgres_session_sync", session_factory)
    monkeypatch.setattr(sync_runtime, "_get_db_url", lambda: "clickhouse://test")
    monkeypatch.setattr(sync_runtime, "_dispatch_post_sync_tasks", lambda **_: None)

    def fake_run_work_items_sync_job(**kwargs: Any) -> dict[str, Any]:
        sink.write_work_items(
            [
                {
                    "provider": kwargs["provider"],
                    "org_id": kwargs["org_id"],
                    "repo_name": kwargs.get("repo_name"),
                    "search_pattern": kwargs.get("search_pattern"),
                }
            ]
        )
        return {"work_items_synced": 1}

    monkeypatch.setattr(
        job_work_items, "run_work_items_sync_job", fake_run_work_items_sync_job
    )


def _patch_clickhouse_sink(
    monkeypatch: pytest.MonkeyPatch, sink: RecordingClickHouseSink
) -> None:
    def sink_factory(dsn: str) -> RecordingClickHouseSink:
        return sink

    monkeypatch.setattr(team_autoimport_linear, "ClickHouseMetricsSink", sink_factory)
    monkeypatch.setattr(team_autoimport_jira, "ClickHouseMetricsSink", sink_factory)
    monkeypatch.setattr(team_autoimport_github, "ClickHouseMetricsSink", sink_factory)
    monkeypatch.setattr(team_autoimport_gitlab, "ClickHouseMetricsSink", sink_factory)


def _patch_provider_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
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
                provider_identity="linear-eng@example.com",
                display_name="Linear Engineer",
                email="linear-eng@example.com",
            )
        ]

    async def discover_jira(
        self: object, email: str, api_token: str, url: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="jira",
                provider_team_id="OPS",
                name="Operations",
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
                provider_identity="jira-ops-account",
                display_name="Jira Ops",
                email="jira-ops@example.com",
                role="lead",
            )
        ]

    async def discover_github(
        self: object, token: str, org_name: str
    ) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="github",
                provider_team_id="platform",
                name="Platform",
                associations={"repo_patterns": ["full-chaos/dev-health"]},
            )
        ]

    async def discover_members_github(
        self: object, token: str, org_name: str, team_slug: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="github",
                provider_identity="platform-lead",
                display_name="Platform Lead",
                email="platform@example.com",
            )
        ]

    async def discover_gitlab(
        self: object, token: str, group_path: str, url: str
    ) -> GitLabDiscoveryResult:
        return GitLabDiscoveryResult(
            teams=[
                DiscoveredTeam(
                    provider_type="gitlab",
                    provider_team_id="full-chaos/dev-health",
                    name="Dev Health",
                    associations={"repo_patterns": ["full-chaos/dev-health/api"]},
                )
            ]
        )

    async def discover_members_gitlab(
        self: object, token: str, group_path: str, url: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="gitlab",
                provider_identity="dev-health-maintainer",
                display_name="Dev Health Maintainer",
                email="gitlab@example.com",
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
        team_autoimport_gitlab.TeamDiscoveryService,
        "discover_gitlab",
        discover_gitlab,
    )
    monkeypatch.setattr(
        team_autoimport_gitlab.TeamMembershipService,
        "discover_members_gitlab",
        discover_members_gitlab,
    )


def _credentials(provider: str) -> dict[str, str]:
    return {
        "linear": {"api_key": "linear-key"},
        "jira": {
            "email": "jira@example.com",
            "api_token": "jira-token",
            "base_url": "https://jira.example.com",
        },
        "github": {"token": "github-token", "org": "full-chaos"},
        "gitlab": {"token": "gitlab-token", "group_path": "full-chaos"},
    }[provider]


def _sync_options(provider: str) -> dict[str, Any]:
    provider_options: dict[str, dict[str, Any]] = {
        "linear": {},
        "jira": {"project_keys": ["OPS"]},
        "github": {"owner": "full-chaos"},
        "gitlab": {"group": "full-chaos"},
    }
    return {"auto_import_teams": True, **provider_options[provider]}


def _run_sync_surface(
    *,
    provider: str,
    monkeypatch: pytest.MonkeyPatch,
    session_factory: Callable[[], Any],
    sink: RecordingClickHouseSink,
) -> dict[str, Any]:
    _patch_runtime_surface(monkeypatch, session_factory, sink)
    _patch_clickhouse_sink(monkeypatch, sink)
    _patch_provider_stubs(monkeypatch)
    monkeypatch.setattr(
        sync_runtime,
        "_resolve_env_credentials",
        lambda provider_name: _credentials(provider_name),
    )
    config_id = _seed_sync_config(
        session_factory, provider=provider, sync_options=_sync_options(provider)
    )
    return getattr(sync_runtime.run_sync_config, "run")(config_id, ORG_ID, "manual")


def _candidate(
    *, source: str, team_id: str, team_name: str, evidence: str, row: Any
) -> TeamAttributionCandidate:
    return TeamAttributionCandidate(
        source=cast(TeamAttributionSource, source),
        team_id=team_id,
        team_name=team_name,
        confidence="high" if int(row.is_primary) else "medium",
        evidence=evidence,
        is_primary=int(row.is_primary),
        specificity=int(row.specificity),
        priority=int(row.priority),
        updated_at=row.updated_at,
    )


def _team_name(sink: RecordingClickHouseSink, org_id: str, team_id: str) -> str:
    team = sink.teams.get((org_id, team_id))
    if team is None:
        return team_id
    return str(team.get("name") or team_id)


def _attribution_context_from_sink(
    sink: RecordingClickHouseSink,
) -> TeamAttributionContext:
    context = TeamAttributionContext()
    for project_row in sink.project_ownership.values():
        candidate = _candidate(
            source="project_ownership",
            team_id=project_row.team_id,
            team_name=_team_name(sink, project_row.org_id, project_row.team_id),
            evidence=f"project_ownership={project_row.project_id}",
            row=project_row,
        )
        context.project_by_id.setdefault(
            (project_row.provider, project_row.project_id), []
        ).append(candidate)
        if project_row.project_key:
            context.project_by_key.setdefault(
                (project_row.provider, project_row.project_key), []
            ).append(candidate)

    for repo_row in sink.repo_ownership.values():
        candidate = _candidate(
            source="repo_ownership",
            team_id=repo_row.team_id,
            team_name=_team_name(sink, repo_row.org_id, repo_row.team_id),
            evidence=f"repo_ownership={repo_row.repo_full_name}",
            row=repo_row,
        )
        context.repo_by_name.setdefault(
            (repo_row.provider, repo_row.repo_full_name), []
        ).append(candidate)

    for membership_row in sink.memberships.values():
        candidate = _candidate(
            source="assignee_membership",
            team_id=membership_row.team_id,
            team_name=_team_name(sink, membership_row.org_id, membership_row.team_id),
            evidence=f"assignee_membership={membership_row.member_id}",
            row=membership_row,
        )
        for identity in (
            membership_row.member_id,
            membership_row.raw_provider_user_id,
            membership_row.raw_email,
        ):
            key = " ".join(str(identity or "").strip().lower().split())
            if key:
                context.member_by_identity.setdefault(
                    (membership_row.provider, key), []
                ).append(candidate)
    return context


def _work_item_for_provider(provider: str) -> WorkItem:
    provider_fields: dict[str, dict[str, Any]] = {
        "linear": {
            "work_item_id": "linear:ENG-1",
            "project_key": "ENG",
            "project_id": "Engineering Project",
        },
        "jira": {
            "work_item_id": "jira:OPS-1",
            "project_key": "OPS",
            "project_id": "Operations Project",
        },
        "github": {
            "work_item_id": "gh:full-chaos/dev-health#1",
            "project_key": None,
            "project_id": "full-chaos/dev-health",
        },
        "gitlab": {
            "work_item_id": "gitlab:full-chaos/dev-health/api#1",
            "project_key": "full-chaos/dev-health/api",
            "project_id": "full-chaos/dev-health/api",
        },
    }
    fields = provider_fields[provider]
    return WorkItem(
        provider=cast(WorkItemProvider, provider),
        title="Auto-import attribution probe",
        type="task",
        status="done",
        status_raw="Done",
        assignees=[],
        reporter=None,
        created_at=NOW - timedelta(days=1),
        updated_at=NOW,
        started_at=NOW - timedelta(hours=2),
        completed_at=NOW,
        closed_at=NOW,
        labels=[],
        **fields,
    )


def _assert_context_can_resolve_provider(
    provider: str, context: TeamAttributionContext
) -> None:
    if provider == "linear":
        assert ("linear", "ENG") in context.project_by_key
    elif provider == "jira":
        assert ("jira", "OPS") in context.project_by_key
    elif provider == "github":
        assert ("github", "full-chaos/dev-health") in context.repo_by_name
    else:
        assert ("gitlab", "full-chaos/dev-health/api") in context.project_by_key


def _transition_for(item: WorkItem) -> WorkItemStatusTransition:
    return WorkItemStatusTransition(
        work_item_id=item.work_item_id,
        provider=item.provider,
        occurred_at=NOW - timedelta(hours=1),
        from_status_raw="To Do",
        to_status_raw="Done",
        from_status="todo",
        to_status="done",
        actor=None,
    )


@pytest.mark.parametrize("provider", ["linear", "jira", "github", "gitlab"])
def test_chaos_2401_2466_sync_surface_autoimport_populates_clickhouse_dims_and_named_attribution(
    provider: str,
    monkeypatch: pytest.MonkeyPatch,
    sync_session_factory: Callable[[], Any],
) -> None:
    sink = RecordingClickHouseSink()

    result = _run_sync_surface(
        provider=provider,
        monkeypatch=monkeypatch,
        session_factory=sync_session_factory,
        sink=sink,
    )

    assert result["status"] == "success"
    if provider != "jira":
        assert result["result"]["work_items_synced"] is True
    else:
        assert result["result"]["backfill_days"] == 1
    assert result["result"]["team_autoimport"]["status"] == "success"
    assert sink.work_items, "sync surface must write work_items before attribution QA"
    assert sink.teams, "auto-import must populate ClickHouse teams"
    assert sink.memberships, "auto-import must populate ClickHouse team_memberships"

    if provider in {"linear", "jira"}:
        assert sink.projects, "Linear/Jira auto-import must populate projects"
        assert sink.members, "Linear/Jira auto-import must populate members"
        assert sink.project_ownership, (
            "Linear/Jira auto-import must populate team_project_ownership"
        )
    elif provider == "github":
        assert sink.repo_ownership, (
            "GitHub auto-import must populate team_repo_ownership"
        )
    else:
        assert sink.project_ownership, (
            "GitLab auto-import must populate team_project_ownership"
        )

    attribution_context = _attribution_context_from_sink(sink)
    _assert_context_can_resolve_provider(provider, attribution_context)

    item = _work_item_for_provider(provider)
    rows = compute_work_item_state_durations_daily(
        day=DAY,
        work_items=[item],
        transitions=[_transition_for(item)],
        computed_at=NOW,
        attribution_context=attribution_context,
    )

    assert rows, "downstream attribution-backed metrics should produce rows"
    assert {row.team_id for row in rows} != {"unassigned"}
    assert all(row.team_id != "unassigned" for row in rows)
    assert all(row.team_name for row in rows)


def test_chaos_2401_2466_regression_work_item_sync_cannot_skip_autoimport_ownership_tables(
    monkeypatch: pytest.MonkeyPatch,
    sync_session_factory: Callable[[], Any],
) -> None:
    sink = RecordingClickHouseSink()

    result = _run_sync_surface(
        provider="linear",
        monkeypatch=monkeypatch,
        session_factory=sync_session_factory,
        sink=sink,
    )

    assert result["status"] == "success"
    assert sink.work_items, "regression setup must prove the provider sync ran"
    assert sink.project_ownership, (
        "CHAOS-2401/2466 regression: a sync with auto_import_teams=true must not "
        "write work_items while leaving ownership dimensions empty"
    )
    assert sink.memberships, (
        "CHAOS-2401/2466 regression: auto-import dispatch must write "
        "team_memberships for attribution"
    )


# CHAOS-2609: a no-email assignee carries the resolver-consumed identity — the
# string IdentityResolver.resolve returns UNDER THE ORG'S ALIAS MAP. With no
# alias that is the provider-qualified id (github:<login> / gitlab:<username> /
# jira:accountid:<id>); with an alias mapping that id to a canonical email, it is
# that email. Auto-import must store whatever the resolver produces, so the
# match holds either way. Each entry: the resolve() kwargs for a no-email
# assignee, the no-alias identity, the alias key + canonical the alias maps it
# to, and the team auto-import created for that member in _patch_provider_stubs.
_NO_EMAIL_ASSIGNEE: dict[str, dict[str, Any]] = {
    "github": {
        "resolve_kwargs": {"provider": "github", "username": "platform-lead"},
        "no_alias_identity": "github:platform-lead",
        "alias_key": "github:platform-lead",
        "canonical": "gh-lead@example.com",
        # The member's email from _patch_provider_stubs.discover_members_github.
        "member_email": "platform@example.com",
        "team_id": "gh:platform",
    },
    "gitlab": {
        "resolve_kwargs": {"provider": "gitlab", "username": "dev-health-maintainer"},
        "no_alias_identity": "gitlab:dev-health-maintainer",
        "alias_key": "gitlab:dev-health-maintainer",
        "canonical": "gl-lead@example.com",
        "member_email": "gitlab@example.com",
        "team_id": "gl:full-chaos/dev-health",
    },
    "jira": {
        "resolve_kwargs": {"provider": "jira", "account_id": "jira-ops-account"},
        "no_alias_identity": "jira:accountid:jira-ops-account",
        "alias_key": "jira:accountid:jira-ops-account",
        "canonical": "jira-lead@example.com",
        "member_email": "jira-ops@example.com",
        "team_id": "OPS",
    },
}


def _write_identity_mapping(path: Any, *, aliased: bool) -> None:
    """Write the global identity_mapping.yaml that BOTH the auto-import path and
    the assignee path read via load_identity_resolver() (IDENTITY_MAPPING_PATH)."""
    if not aliased:
        path.write_text("identities: []\n")
        return
    lines = ["identities:"]
    for spec in _NO_EMAIL_ASSIGNEE.values():
        lines.append(f"  - canonical: {spec['canonical']}")
        lines.append("    aliases:")
        lines.append(f"      - {spec['alias_key']}")
    path.write_text("\n".join(lines) + "\n")


def _membership_probe_work_item(provider: str, assignee: str) -> WorkItem:
    """A work item whose ONLY attribution signal is its assignee — project_key,
    project_id and repo deliberately match no ownership row, so the resolution
    winner can only be assignee_membership."""
    return WorkItem(
        work_item_id=f"{provider}:no-email-assignee-probe#1",
        provider=cast(WorkItemProvider, provider),
        title="No-email assignee attribution probe",
        type="task",
        status="done",
        status_raw="Done",
        assignees=[assignee],
        reporter=None,
        created_at=NOW - timedelta(days=1),
        updated_at=NOW,
        started_at=NOW - timedelta(hours=2),
        completed_at=NOW,
        closed_at=NOW,
        labels=[],
        project_key=None,
        project_id="zzz-unowned-scope-no-ownership-match",
    )


# scenario → (alias file aliased?, builds an email-bearing assignee?)
#   no_alias : no alias map, no-email assignee → resolves to provider-qualified id
#   aliased  : alias map present, no-email assignee → resolves to canonical email
#   email    : no alias map, assignee carries the member's EMAIL → resolves to that
#              email (regression for the email+provider-id member: the ladder
#              matched via raw_email but the roster used to MISS — CHAOS-2609 r4)
_E2E_SCENARIOS = ["no_alias", "aliased", "email"]


@pytest.mark.parametrize("scenario", _E2E_SCENARIOS, ids=_E2E_SCENARIOS)
@pytest.mark.parametrize("provider", ["github", "gitlab", "jira"])
def test_chaos_2609_no_email_assignee_resolves_to_autoimported_team_via_both_paths(
    provider: str,
    scenario: str,
    monkeypatch: pytest.MonkeyPatch,
    sync_session_factory: Callable[[], Any],
    tmp_path: Any,
) -> None:
    # The org's alias map drives BOTH sides: auto-import resolves members through
    # it, and the assignee path resolves the assignee through it. Point both at
    # the SAME file via IDENTITY_MAPPING_PATH so the aliased case is faithful.
    mapping_path = tmp_path / "identity_mapping.yaml"
    _write_identity_mapping(mapping_path, aliased=(scenario == "aliased"))
    monkeypatch.setenv("IDENTITY_MAPPING_PATH", str(mapping_path))

    sink = RecordingClickHouseSink()
    result = _run_sync_surface(
        provider=provider,
        monkeypatch=monkeypatch,
        session_factory=sync_session_factory,
        sink=sink,
    )
    assert result["status"] == "success"
    assert sink.memberships, "auto-import must populate team_memberships"

    spec = _NO_EMAIL_ASSIGNEE[provider]
    if scenario == "email":
        # An assignee that carries the member's email — resolves BY email.
        resolve_kwargs = {**spec["resolve_kwargs"], "email": spec["member_email"]}
        expected_identity = spec["member_email"]
    elif scenario == "aliased":
        resolve_kwargs = spec["resolve_kwargs"]
        expected_identity = spec["canonical"]
    else:
        resolve_kwargs = spec["resolve_kwargs"]
        expected_identity = spec["no_alias_identity"]

    # 1. The REAL resolver (same alias map auto-import used) turns the assignee
    #    into the identity it resolves to: provider-qualified id (no_alias),
    #    canonical email (aliased), or the member's email (email scenario).
    assignee = load_identity_resolver().resolve(**resolve_kwargs)
    assert assignee == expected_identity

    item = _membership_probe_work_item(provider, assignee)

    # 2. Canonical ladder: member_by_identity built EXACTLY as production builds
    #    it (member_id / raw_provider_user_id / raw_email) resolves the assignee
    #    to the auto-imported team via assignee_membership.
    context = _attribution_context_from_sink(sink)
    ladder_team_id, ladder_team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    assert ladder_team_id == spec["team_id"], (
        f"{provider} ({scenario}): assignee {assignee!r} must resolve to the "
        f"auto-imported team via the canonical ladder"
    )
    assert ladder_team_name
    assert any(c.source == "assignee_membership" for c in candidates)

    # 3. Secondary TeamResolver: the teams.members roster resolves the SAME
    #    assignee to the SAME team (the path the cosmetic roster fix missed, and
    #    the path the email-bearing member used to miss before r4).
    team_resolver = TeamResolver(
        member_to_team=_build_member_to_team(list(sink.teams.values()))
    )
    roster_team_id, _, _ = resolve_team_attribution(
        item,
        team_resolver=team_resolver,
        project_key_resolver=None,
        attribution_context=None,
    )
    assert roster_team_id == spec["team_id"], (
        f"{provider} ({scenario}): teams.members roster must resolve assignee "
        f"{assignee!r} to the auto-imported team via TeamResolver"
    )
