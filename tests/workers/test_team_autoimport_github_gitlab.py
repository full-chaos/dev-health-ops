from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveryResult,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.workers import team_autoimport_github, team_autoimport_gitlab


@dataclass
class _FakeGitLabProject:
    """Stand-in for a python-gitlab ``Project`` RESTObject: attribute access only."""

    name: str
    archived: bool = False
    web_url: str = ""


def _fake_gitlab_work_client_cls(
    projects: dict[str, _FakeGitLabProject],
    *,
    fail_paths: frozenset[str] = frozenset(),
) -> type:
    """A ``GitLabWorkClient`` stand-in keyed by the SAME path used to fetch it.

    Never makes a network call -- unit tests must not depend on GitLab being
    reachable. ``fail_paths`` models a project whose metadata fetch fails
    (e.g. a 404 for a project deleted since discovery ran) without touching
    every other discovered project.
    """

    class _FakeGitLabWorkClient:
        def __init__(self, *, auth: Any, org_id: str | None = None) -> None:
            self.auth = auth
            self.org_id = org_id

        def get_project(self, project_id_or_path: str) -> _FakeGitLabProject:
            if project_id_or_path in fail_paths:
                raise RuntimeError(
                    f"simulated GitLab fetch failure: {project_id_or_path}"
                )
            return projects[project_id_or_path]

    return _FakeGitLabWorkClient


@dataclass
class RecordingSink:
    teams: list[dict[str, Any]] = field(default_factory=list)
    repo_ownership: list[Any] = field(default_factory=list)
    project_ownership: list[Any] = field(default_factory=list)
    memberships: list[Any] = field(default_factory=list)
    projects: list[Any] = field(default_factory=list)
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

    def write_projects(self, rows: list[Any]) -> None:
        self.projects.extend(rows)


def test_github_org_import_writes_provider_access_repo_grants_and_nested_specificity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RecordingSink()
    resolver = IdentityResolver(
        alias_to_canonical={
            "github:platform-lead": "canonical-platform-id@example.com",
            "platform@example.com": "canonical-platform-email@example.com",
            "github:platform-api-lead": "canonical-platform-api-id@example.com",
            "platform-api@example.com": "canonical-platform-api-email@example.com",
        }
    )

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
    monkeypatch.setattr(
        team_autoimport_github, "load_identity_resolver", lambda: resolver
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
    # CHAOS-2609 (CS-COV): github teams carry a non-empty members roster whose
    # entries are EVERY identity an assignee could resolve to — the
    # resolver-consumed github:<login> (no-email assignee) AND the member's email
    # (email-bearing assignee) — so the secondary TeamResolver matches both.
    rosters = {row["id"]: row["members"] for row in sink.teams}
    assert rosters["gh:platform"] == [
        "canonical-platform-id@example.com",
        "github:platform-lead",
        "canonical-platform-email@example.com",
        "platform@example.com",
    ]
    assert rosters["gh:platform-api"] == [
        "canonical-platform-api-id@example.com",
        "github:platform-api-lead",
        "canonical-platform-api-email@example.com",
        "platform-api@example.com",
    ]
    # The single canonical-ladder facet (raw_provider_user_id) carries the
    # no-email identity; raw_email carries the email; member_id (PK) keeps gh:.
    by_member = {row.member_id: row for row in sink.memberships}
    assert (
        by_member["gh:platform-lead"].raw_provider_user_id
        == "canonical-platform-id@example.com"
    )
    assert by_member["gh:platform-lead"].raw_email == "platform@example.com"
    assert by_member["gh:platform-lead"].identity_facets == [
        "canonical-platform-id@example.com",
        "github:platform-lead",
        "canonical-platform-email@example.com",
        "platform@example.com",
    ]
    assert by_member["gh:platform-api-lead"].identity_facets == [
        "canonical-platform-api-id@example.com",
        "github:platform-api-lead",
        "canonical-platform-api-email@example.com",
        "platform-api@example.com",
    ]


def test_github_strict_reference_discovery_uses_app_installation_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.connectors.utils import github_app

    token_provider_args: list[dict[str, str]] = []
    discovery_calls: list[tuple[str, str]] = []

    class FakeGitHubAppTokenProvider:
        def __init__(
            self,
            *,
            app_id: str,
            private_key: str,
            installation_id: str,
            api_base_url: str,
        ) -> None:
            token_provider_args.append(
                {
                    "app_id": app_id,
                    "private_key": private_key,
                    "installation_id": installation_id,
                    "api_base_url": api_base_url,
                }
            )

        def get_token(self) -> str:
            return "installation-token"

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        discovery_calls.append((token, org_name))
        return []

    monkeypatch.setattr(
        github_app, "GitHubAppTokenProvider", FakeGitHubAppTokenProvider
    )
    monkeypatch.setattr(
        team_autoimport_github.TeamDiscoveryService,
        "discover_github",
        discover_github,
    )

    summary = team_autoimport_github.populate(
        org_id="org-1",
        credentials={
            "app_id": "123",
            "private_key": "private-key",
            "installation_id": "456",
        },
        scope={
            "strict_reference_discovery": True,
            "sync_options": {"owner": "full-chaos"},
        },
    )

    assert summary["status"] == "skipped"
    assert summary["reason"] == "no_provider_teams"
    assert token_provider_args == [
        {
            "app_id": "123",
            "private_key": "private-key",
            "installation_id": "456",
            "api_base_url": "https://api.github.com",
        }
    ]
    assert discovery_calls == [("installation-token", "full-chaos")]


def test_github_strict_reference_discovery_with_app_auth_still_fails_provider_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.connectors.utils import github_app

    class FakeGitHubAppTokenProvider:
        def __init__(self, **_: str) -> None:
            return None

        def get_token(self) -> str:
            return "installation-token"

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        raise RuntimeError("github discovery unavailable")

    monkeypatch.setattr(
        github_app, "GitHubAppTokenProvider", FakeGitHubAppTokenProvider
    )
    monkeypatch.setattr(
        team_autoimport_github.TeamDiscoveryService,
        "discover_github",
        discover_github,
    )

    with pytest.raises(RuntimeError, match="github discovery unavailable"):
        team_autoimport_github.populate(
            org_id="org-1",
            credentials={
                "app_id": "123",
                "private_key": "private-key",
                "installation_id": "456",
            },
            scope={
                "strict_reference_discovery": True,
                "sync_options": {"owner": "full-chaos"},
            },
        )


def test_gitlab_group_import_writes_provider_access_project_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RecordingSink()
    resolver = IdentityResolver(
        alias_to_canonical={
            "gitlab:full-chaos": "canonical-gitlab-root@example.com",
            "gitlab:full-chaos-dev-health": "canonical-gitlab-dev-health@example.com",
        }
    )

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
    monkeypatch.setattr(
        team_autoimport_gitlab, "load_identity_resolver", lambda: resolver
    )
    monkeypatch.setattr(
        team_autoimport_gitlab,
        "GitLabWorkClient",
        _fake_gitlab_work_client_cls(
            {
                "full-chaos/platform": _FakeGitLabProject(
                    name="Platform", web_url="https://gitlab.com/full-chaos/platform"
                ),
                "full-chaos/dev-health/api": _FakeGitLabProject(
                    name="API", web_url="https://gitlab.com/full-chaos/dev-health/api"
                ),
            }
        ),
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
    # CHAOS-3380: the projects catalog rows, distinct from "projects_imported"
    # above (which counts team-ownership project PATHS, not catalog rows).
    assert summary["native_projects_imported"] == 2
    assert summary["native_projects_complete"] is True
    assert {row.id for row in sink.projects} == {
        "full-chaos/platform",
        "full-chaos/dev-health/api",
    }
    assert {row.provider for row in sink.projects} == {"gitlab"}
    # project_key is ALWAYS None: GitLab work items never set project_key
    # (providers/gitlab/normalize.py), so the id is the only identity arm the
    # native_status_change join (CHAOS-3374) can ever match a GitLab project
    # through.
    assert {row.project_key for row in sink.projects} == {None}
    assert {row.is_active for row in sink.projects} == {1}
    platform_row = next(row for row in sink.projects if row.id == "full-chaos/platform")
    assert platform_row.name == "Platform"
    assert platform_row.url == "https://gitlab.com/full-chaos/platform"
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
    # CHAOS-2609 (CS-COV) item 1: gitlab members are normalized AND asserted.
    assert summary["team_memberships_imported"] == 2
    assert summary["members_imported"] == 2
    assert {row.member_id for row in sink.memberships} == {
        "gl:full-chaos",
        "gl:full-chaos-dev-health",
    }
    assert {row.source for row in sink.memberships} == {"provider_access"}
    assert {row.priority for row in sink.memberships} == {
        team_autoimport_gitlab.PROVIDER_ACCESS_PRIORITY
    }
    # CHAOS-2609 (CS-COV): gitlab teams carry a non-empty members roster whose
    # entries are the RESOLVER-CONSUMED identity (gitlab:<username>), and the
    # canonical-ladder facet (raw_provider_user_id) carries the same identity.
    rosters = {row["id"]: row["members"] for row in sink.teams}
    assert rosters["gl:full-chaos"] == [
        "canonical-gitlab-root@example.com",
        "gitlab:full-chaos",
    ]
    assert rosters["gl:full-chaos/dev-health"] == [
        "canonical-gitlab-dev-health@example.com",
        "gitlab:full-chaos-dev-health",
    ]
    by_member = {row.member_id: row for row in sink.memberships}
    assert (
        by_member["gl:full-chaos"].raw_provider_user_id
        == "canonical-gitlab-root@example.com"
    )
    assert by_member["gl:full-chaos"].identity_facets == [
        "canonical-gitlab-root@example.com",
        "gitlab:full-chaos",
    ]
    assert by_member["gl:full-chaos-dev-health"].identity_facets == [
        "canonical-gitlab-dev-health@example.com",
        "gitlab:full-chaos-dev-health",
    ]
    # CHAOS-2609 (CS-COV) item 7: a nested subgroup's ownership is more specific
    # than its parent group's, so it wins on specificity tie-breaks.
    parent_proj = next(
        row for row in sink.project_ownership if row.team_id == "gl:full-chaos"
    )
    subgroup_proj = next(
        row
        for row in sink.project_ownership
        if row.team_id == "gl:full-chaos/dev-health"
    )
    assert subgroup_proj.specificity > parent_proj.specificity


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


def _patch_single_group_gitlab_discovery(
    monkeypatch: pytest.MonkeyPatch, *, repo_patterns: list[str]
) -> RecordingSink:
    """Common CHAOS-3380 fixture: one GitLab group owning ``repo_patterns``."""

    sink = RecordingSink()
    resolver = IdentityResolver(alias_to_canonical={})

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
                        "repo_patterns": repo_patterns,
                        "provider_org": group_path,
                    },
                ),
            ]
        )

    async def discover_members_gitlab(
        self, token: str, group_path: str, url: str
    ) -> list[DiscoveredMember]:
        return []

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
    monkeypatch.setattr(
        team_autoimport_gitlab, "load_identity_resolver", lambda: resolver
    )
    return sink


def test_gitlab_archived_project_is_written_as_retired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3380: mirrors #1450's Linear archivedAt/trashed handling.

    ``archived`` is the only retirement signal GitLab's project resource
    exposes; a project retired this way must land ``is_active=0`` so
    ``scope_catalog`` (which filters ``is_active = 1``) stops serving it as an
    Ask Dev subject -- the same contract Linear's archived/trashed projects
    get, deliberately WITHOUT the absence-based retirement CHAOS-3372 leaves
    open (a project simply not re-discovered this run keeps its last-written
    state rather than being tombstoned).
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch, repo_patterns=["full-chaos/legacy"]
    )
    monkeypatch.setattr(
        team_autoimport_gitlab,
        "GitLabWorkClient",
        _fake_gitlab_work_client_cls(
            {"full-chaos/legacy": _FakeGitLabProject(name="Legacy", archived=True)}
        ),
    )

    summary = team_autoimport_gitlab.populate(
        org_id="org-1",
        credentials={"token": "secret", "group_path": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "sync_options": {"auto_import_teams": True},
        },
    )

    assert summary["native_projects_imported"] == 1
    assert sink.projects[0].id == "full-chaos/legacy"
    assert sink.projects[0].is_active == 0


def test_gitlab_project_metadata_fetch_failure_skips_only_that_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single project's metadata fetch failing must not discard the rest.

    Mirrors Linear's ``native_projects_complete`` contract: a partial catalog
    is reported INCOMPLETE rather than silently passing as a full one, but
    team ownership (independent of the catalog fetch) is unaffected.
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/ok", "full-chaos/gone"],
    )
    monkeypatch.setattr(
        team_autoimport_gitlab,
        "GitLabWorkClient",
        _fake_gitlab_work_client_cls(
            {"full-chaos/ok": _FakeGitLabProject(name="OK")},
            fail_paths=frozenset({"full-chaos/gone"}),
        ),
    )

    summary = team_autoimport_gitlab.populate(
        org_id="org-1",
        credentials={"token": "secret", "group_path": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "sync_options": {"auto_import_teams": True},
        },
    )

    assert summary["native_projects_imported"] == 1
    assert summary["native_projects_complete"] is False
    assert {row.id for row in sink.projects} == {"full-chaos/ok"}
    # Team ownership is a separate fetch and must survive the partial catalog.
    assert summary["team_project_ownership_imported"] == 2


def test_gitlab_no_discovered_projects_skips_the_metadata_fetch_entirely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No repo_patterns discovered -> no GitLab API call is even attempted.

    ``GitLabWorkClient`` is patched to raise on construction, so this fails
    loudly if the "nothing to fetch" guard is ever removed.
    """

    sink = _patch_single_group_gitlab_discovery(monkeypatch, repo_patterns=[])

    def _explode(*, auth: Any, org_id: str | None = None) -> Any:
        raise AssertionError(
            "no project paths were discovered; GitLabWorkClient must not be constructed"
        )

    monkeypatch.setattr(team_autoimport_gitlab, "GitLabWorkClient", _explode)

    summary = team_autoimport_gitlab.populate(
        org_id="org-1",
        credentials={"token": "secret", "group_path": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "sync_options": {"auto_import_teams": True},
        },
    )

    assert summary["native_projects_imported"] == 0
    assert summary["native_projects_complete"] is True
    assert sink.projects == []
