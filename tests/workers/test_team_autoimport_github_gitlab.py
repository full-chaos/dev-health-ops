from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveredProject,
    GitLabDiscoveryResult,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.workers import team_autoimport_github, team_autoimport_gitlab


def _discovered_project(
    id: str,
    path: str,
    *,
    name: str | None = None,
    archived: bool = False,
    web_url: str = "",
) -> GitLabDiscoveredProject:
    """A ``GitLabDiscoveredProject`` exactly as ``discover_gitlab``'s flat,
    recursive listing call produces it (CHAOS-3380 round 2): ``id`` is
    GitLab's own immutable numeric project id, ``path`` is the CURRENT
    ``path_with_namespace``.
    """

    return GitLabDiscoveredProject(
        id=id,
        path_with_namespace=path,
        name=name or path,
        archived=archived,
        web_url=web_url,
    )


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
    # CHAOS-4323: canned "currently persisted" team rows, keyed by id, for
    # exercising the members-off roster-preservation read (_existing_team_members).
    existing_team_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    query_dicts_calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    # CHAOS-4323 round 2: set to force query_dicts to raise, simulating a
    # ClickHouse read failure during roster preservation.
    query_dicts_raises: bool = False

    async def insert_teams(self, rows: list[dict[str, Any]]) -> None:
        self.teams.extend(rows)

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.query_dicts_calls.append((query, parameters))
        if self.query_dicts_raises:
            raise RuntimeError("simulated ClickHouse read failure")
        team_ids = parameters.get("team_ids") or []
        return [
            {"id": team_id, "members": self.existing_team_rows[team_id]["members"]}
            for team_id in team_ids
            if team_id in self.existing_team_rows
        ]

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


def test_github_org_import_honours_members_only_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4323: with only auto_import_members selected, GitHub writes
    memberships but neither team rows (auto_import_teams off) nor
    team_repo_ownership (auto_import_projects off, GitHub's project-ownership
    analog) -- proves the per-category write gate, not just the flag read."""

    sink = RecordingSink()
    resolver = IdentityResolver(alias_to_canonical={})

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
            "sync_options": {"auto_import_members": True},
            # The actual production seam (run_team_autoimport) threads the
            # resolved per-category selection under this key -- calling
            # populate() directly with only "sync_options" set would NOT gate
            # anything, since providers never re-derive categories from
            # sync_options themselves (single source of truth in
            # team_autoimport.run_team_autoimport).
            "import_categories": {
                "teams": False,
                "projects": False,
                "members": True,
            },
        },
    )

    assert summary["teams_imported"] == 0
    assert summary["team_repo_ownership_imported"] == 0
    assert summary["team_memberships_imported"] == 1
    # Neither the team dimension nor repo-ownership sinks were called.
    assert sink.teams == []
    assert sink.repo_ownership == []
    assert len(sink.memberships) == 1
    assert sink.memberships[0].member_id == "gh:platform-lead"


def test_github_org_import_preserves_existing_roster_when_members_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4323 (codex adversarial-review, HIGH): a teams-only run (members
    off) must NOT overwrite the team row's "members" field with an empty
    list -- that would erase a roster a previous run (or an admin) wrote.
    It must read back and carry forward whatever is currently persisted."""

    sink = RecordingSink()
    sink.existing_team_rows["gh:platform"] = {
        "members": ["preexisting-lead@example.com"]
    }
    resolver = IdentityResolver(alias_to_canonical={})

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
        ]

    async def discover_members_github(
        self, token: str, org_name: str, team_slug: str
    ) -> list[DiscoveredMember]:
        raise AssertionError(
            "member discovery must not run when the members category is off"
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
            "import_categories": {
                "teams": True,
                "projects": False,
                "members": False,
            },
        },
    )

    assert summary["teams_imported"] == 1
    assert len(sink.teams) == 1
    # Preserved, not erased -- this is the whole point of the fix.
    assert sink.teams[0]["members"] == ["preexisting-lead@example.com"]
    assert sink.query_dicts_calls, "roster-preservation read must have run"


def test_github_org_import_fails_closed_when_roster_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4323 round 2 (codex adversarial-review, HIGH): if the roster-
    preservation read itself fails (ClickHouse unavailable, degraded, or a
    schema mismatch), the fix must NOT fall through to writing an empty
    "members" list -- that reproduces the exact data loss the fix exists to
    prevent, only now triggered by infra flakiness instead of the members
    flag. The team-dimension write must be skipped entirely instead."""

    sink = RecordingSink(query_dicts_raises=True)
    resolver = IdentityResolver(alias_to_canonical={})

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
        ]

    async def discover_members_github(
        self, token: str, org_name: str, team_slug: str
    ) -> list[DiscoveredMember]:
        raise AssertionError(
            "member discovery must not run when the members category is off"
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
            "import_categories": {
                "teams": True,
                "projects": False,
                "members": False,
            },
        },
    )

    assert summary["teams_imported"] == 0
    assert summary["roster_preservation_failed"] is True
    # The write never happened -- the whole point of failing closed.
    assert sink.teams == []


def test_github_org_import_skips_populate_entirely_when_no_category_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discover_calls: list[str] = []

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        discover_calls.append(org_name)
        return []

    monkeypatch.setattr(
        team_autoimport_github.TeamDiscoveryService,
        "discover_github",
        discover_github,
    )

    summary = team_autoimport_github.populate(
        org_id="org-1",
        credentials={"token": "secret", "org": "full-chaos"},
        scope={
            "analytics_db": "clickhouse://test",
            "import_categories": {
                "teams": False,
                "projects": False,
                "members": False,
            },
        },
    )

    assert summary["status"] == "skipped"
    assert summary["reason"] == "no_categories_selected"
    # Never even reached discovery -- no network call attempted.
    assert discover_calls == []


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
            ],
            projects=[
                _discovered_project(
                    "101",
                    "full-chaos/platform",
                    name="Platform",
                    web_url="https://gitlab.com/full-chaos/platform",
                ),
                _discovered_project(
                    "102",
                    "full-chaos/dev-health/api",
                    name="API",
                    web_url="https://gitlab.com/full-chaos/dev-health/api",
                ),
            ],
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
    assert summary["native_projects_filtered_by_selection"] == 0
    # CHAOS-3380 round 2 (Codex HIGH): the catalog id is GitLab's IMMUTABLE
    # numeric project id, prefixed like Jira's -- never the mutable path.
    assert {row.id for row in sink.projects} == {
        "org-1:gitlab:101",
        "org-1:gitlab:102",
    }
    assert {row.provider for row in sink.projects} == {"gitlab"}
    # project_key carries the CURRENT path -- the identity join
    # (native_status_change._project_identity_match, CHAOS-3374) resolves a
    # GitLab project through THIS arm, matching providers/gitlab/normalize.py
    # now writing the SAME path onto work_items.project_key.
    assert {row.project_key for row in sink.projects} == {
        "full-chaos/platform",
        "full-chaos/dev-health/api",
    }
    assert {row.is_active for row in sink.projects} == {1}
    platform_row = next(row for row in sink.projects if row.id == "org-1:gitlab:101")
    assert platform_row.project_key == "full-chaos/platform"
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
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_patterns: list[str],
    projects: list[GitLabDiscoveredProject] | None = None,
    truncated: bool = False,
    warnings: list[str] | None = None,
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
            ],
            projects=projects or [],
            truncated=truncated,
            warnings=warnings or [],
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


def _populate_gitlab(**scope_overrides: Any) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "analytics_db": "clickhouse://test",
        "sync_options": {"auto_import_teams": True},
    }
    scope.update(scope_overrides)
    return team_autoimport_gitlab.populate(
        org_id="org-1",
        credentials={"token": "secret", "group_path": "full-chaos"},
        scope=scope,
    )


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
        monkeypatch,
        repo_patterns=["full-chaos/legacy"],
        projects=[_discovered_project("201", "full-chaos/legacy", archived=True)],
    )

    summary = _populate_gitlab()

    assert summary["native_projects_imported"] == 1
    assert sink.projects[0].id == "org-1:gitlab:201"
    assert sink.projects[0].project_key == "full-chaos/legacy"
    assert sink.projects[0].is_active == 0


def test_gitlab_project_rename_updates_the_same_catalog_row_by_immutable_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3380 round 2 (Codex HIGH): rename/transfer, made explicit.

    The SAME numeric project id, discovered under a NEW path on a later run
    (a rename or a group transfer), must mint a catalog row with the SAME
    ``id`` -- so the ReplacingMergeTree row this becomes in production is a
    new VERSION of the same subject, not a second, unrelated one -- with
    ``project_key`` updated to the new path. Work items ingested under the
    OLD path are NOT reattached by this (their own ``project_key`` was baked
    in at ingestion time, and GitLab's own work_item_id is path-derived,
    pre-existing and out of this ticket's scope) -- they simply stop
    matching until the provider sync re-ingests them under the new path.
    That is the fail-safe direction: orphaned, never silently merged into an
    unrelated project's history.
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/old-name"],
        projects=[_discovered_project("301", "full-chaos/old-name")],
    )
    before = _populate_gitlab()
    assert sink.projects[-1].id == "org-1:gitlab:301"
    assert sink.projects[-1].project_key == "full-chaos/old-name"

    sink2 = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/new-name"],
        projects=[_discovered_project("301", "full-chaos/new-name")],
    )
    after = _populate_gitlab()

    assert before["native_projects_imported"] == 1
    assert after["native_projects_imported"] == 1
    # SAME catalog id across the rename.
    assert sink2.projects[-1].id == "org-1:gitlab:301"
    # project_key tracks the CURRENT path.
    assert sink2.projects[-1].project_key == "full-chaos/new-name"


def test_gitlab_source_selection_filters_unselected_projects_from_the_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3380 round 2 (Codex MEDIUM): only ENABLED sources get cataloged.

    ``source_external_ids`` (populated by the reference-discovery scope,
    ``workers/reference_discovery.py``) carries the enabled
    ``IntegrationSource.external_id`` values -- GitLab's numeric project id.
    A project the credential can see but this sync run did not select must
    never become a resolvable Ask Dev subject.
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/selected", "full-chaos/unselected"],
        projects=[
            _discovered_project("401", "full-chaos/selected"),
            _discovered_project("402", "full-chaos/unselected"),
        ],
    )

    summary = _populate_gitlab(source_external_ids=["401"])

    assert summary["native_projects_imported"] == 1
    assert summary["native_projects_filtered_by_selection"] == 1
    assert {row.id for row in sink.projects} == {"org-1:gitlab:401"}
    assert "full-chaos/unselected" not in {row.project_key for row in sink.projects}


def test_gitlab_source_selection_empty_list_catalogs_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit EMPTY enabled-sources list means "select none", not
    "no restriction" -- distinguished from the key being absent entirely
    (see the next test)."""

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/a"],
        projects=[_discovered_project("501", "full-chaos/a")],
    )

    summary = _populate_gitlab(source_external_ids=[])

    assert summary["native_projects_imported"] == 0
    assert sink.projects == []


def test_gitlab_source_selection_absent_catalogs_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documents the residual gap explicitly rather than leaving it silent:
    the common POST-SYNC trigger (workers/team_autoimport.py.
    run_post_sync_team_autoimport) does not populate ``source_external_ids``
    in scope at all today, so this filter is a no-op on that path -- every
    discovered project is cataloged, exactly as before this change. Closing
    it there needs a change to that shared dispatcher (all 4 providers),
    out of this ticket's scope.
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/a", "full-chaos/b"],
        projects=[
            _discovered_project("601", "full-chaos/a"),
            _discovered_project("602", "full-chaos/b"),
        ],
    )

    summary = _populate_gitlab()  # no source_external_ids key at all

    assert summary["native_projects_imported"] == 2
    assert summary["native_projects_filtered_by_selection"] == 0
    assert len(sink.projects) == 2


def test_gitlab_selected_source_missing_from_discovery_marks_the_catalog_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3380 round 3 (Codex MEDIUM -- stale selected-source set reads as
    complete): the inverse gap from the filtering tests above. A selected id
    that discovery never returned at all (deleted, access revoked, or simply
    never reached before a pagination bound) must not vanish from the catalog
    with ``native_projects_complete`` staying ``True`` -- the ORIGINAL bug
    this fix closes: "first import creates nothing; re-import leaves a stale
    row; the run still reports complete."
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/present"],
        projects=[_discovered_project("701", "full-chaos/present")],
    )

    # "702" is selected but discovery never returned it.
    summary = _populate_gitlab(source_external_ids=["701", "702"])

    assert summary["native_projects_imported"] == 1
    assert {row.id for row in sink.projects} == {"org-1:gitlab:701"}
    assert summary["native_projects_missing_selected_source_ids"] == ["702"]
    assert summary["native_projects_complete"] is False, (
        "a selected-but-undiscovered source must mark the catalog incomplete"
    )


def test_gitlab_selected_source_present_in_discovery_stays_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The positive case beside the one above: every selected id IS
    discovered -> no missing ids, catalog stays complete."""

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/present"],
        projects=[_discovered_project("801", "full-chaos/present")],
    )

    summary = _populate_gitlab(source_external_ids=["801"])

    assert summary["native_projects_imported"] == 1
    assert {row.id for row in sink.projects} == {"org-1:gitlab:801"}
    assert summary["native_projects_missing_selected_source_ids"] == []
    assert summary["native_projects_complete"] is True


def test_gitlab_selection_explicit_empty_has_no_missing_selected_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit empty selection (nothing enabled) trivially has no
    MISSING selected source either -- vacuously complete, distinguished from
    the stale-source case, which has at least one selected id to go missing."""

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/a"],
        projects=[_discovered_project("901", "full-chaos/a")],
    )

    summary = _populate_gitlab(source_external_ids=[])

    assert sink.projects == []
    assert summary["native_projects_missing_selected_source_ids"] == []
    assert summary["native_projects_complete"] is True


def test_gitlab_selection_absent_key_has_no_missing_selected_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ``source_external_ids`` key at all (the common post-sync path) ->
    nothing is "selected" in the first place, so nothing can be missing."""

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/a"],
        projects=[_discovered_project("902", "full-chaos/a")],
    )

    summary = _populate_gitlab()

    assert len(sink.projects) == 1
    assert summary["native_projects_missing_selected_source_ids"] == []
    assert summary["native_projects_complete"] is True


def test_gitlab_truncated_discovery_marks_the_catalog_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3380 round 2 (Codex HIGH): a truncated discovery walk (either
    the teams walk or the flat projects listing hitting its pagination
    bound) must never be recorded as a complete catalog.
    """

    sink = _patch_single_group_gitlab_discovery(
        monkeypatch,
        repo_patterns=["full-chaos/a"],
        projects=[_discovered_project("701", "full-chaos/a")],
        truncated=True,
        warnings=["GitLab team discovery truncated projects (...) at 5000 results"],
    )

    summary = _populate_gitlab()

    assert summary["native_projects_complete"] is False
    # The rows discovery DID return are still cataloged -- truncation means
    # "possibly incomplete", not "discard everything we got".
    assert summary["native_projects_imported"] == 1
    assert sink.projects[0].project_key == "full-chaos/a"


def test_gitlab_no_discovered_projects_writes_no_catalog_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No projects in the discovery result -> nothing written, no network
    call attempted (the catalog rows are mapped straight off ``result.
    projects``, never fetched separately per project)."""

    sink = _patch_single_group_gitlab_discovery(monkeypatch, repo_patterns=[])

    summary = _populate_gitlab()

    assert summary["native_projects_imported"] == 0
    assert summary["native_projects_complete"] is True
    assert sink.projects == []
