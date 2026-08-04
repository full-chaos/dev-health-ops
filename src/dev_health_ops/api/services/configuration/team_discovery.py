"""Team discovery service.

Pulls team-like units (GitHub teams, GitLab subgroups, Linear teams, Jira
projects) from external providers. Discovered teams are imported into the
ClickHouse-native ``teams`` catalog by ``ClickHouseTeamAdminService`` (the
admin import endpoint); ClickHouse is the team system of record (CHAOS-2600).
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import requests
from sqlalchemy.ext.asyncio import AsyncSession

from ._helpers import _get_discovered_team_cls

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import DiscoveredTeam

logger = logging.getLogger(__name__)

# Upper bounds for GitLab discovery pagination. ``get_all=True`` walks every
# page, which on very large self-hosted instances can mean thousands of API
# calls; cap the walk and log when results are truncated.
MAX_GITLAB_DISCOVERY_SUBGROUPS = 500
MAX_GITLAB_DISCOVERY_PROJECTS = 500
#: CHAOS-3380 (Codex HIGH round 2): the project catalog walk below is a
#: SEPARATE, flatter enumeration from the per-group ``MAX_GITLAB_DISCOVERY_
#: PROJECTS`` walk above (which the ``teams``/``repo_patterns`` walk still
#: uses, unchanged) -- it lists every project under the ENTIRE root group
#: tree, including grandchild subgroups, in one paginated call
#: (``include_subgroups=True``), so it needs its own, larger bound rather
#: than reusing the per-group one.
MAX_GITLAB_DISCOVERY_ALL_PROJECTS = 5000


def _bounded_list(
    iterable: Any,
    limit: int,
    *,
    what: str,
    scope: str,
    warnings: list[str] | None = None,
) -> list[Any]:
    """Materialize at most ``limit`` items, logging when results are truncated.

    When ``warnings`` is provided, a human-readable truncation message is
    appended to it so callers can surface partial results instead of silently
    presenting them as complete.
    """
    items = list(itertools.islice(iter(iterable), limit + 1))
    if len(items) > limit:
        message = (
            f"GitLab team discovery truncated {what} for '{scope}' at "
            f"{limit} results; the import may be incomplete."
        )
        logger.warning(message)
        if warnings is not None:
            warnings.append(message)
        return items[:limit]
    return items


@dataclass(frozen=True)
class GitLabDiscoveredProject:
    """One GitLab project as returned by a group's project LISTING call.

    CHAOS-3380: sourced entirely from the listing response (``GET /groups/
    :id/projects``, which GitLab's API documents as returning the same rich
    project representation the single-project ``GET /projects/:id`` does --
    ``id``, ``path_with_namespace``, ``name``, ``archived``, and ``web_url``
    are all present on it), never from a per-project follow-up GET -- see
    ``discover_gitlab``'s ``include_subgroups=True`` listing call below.

    ``id`` is GitLab's own IMMUTABLE numeric project id (as a string) --
    ``sync/discovery.py``'s ``_map_gitlab_tuple`` already documents this as
    "the canonical GitLab identifier" and keys ``IntegrationSource.
    external_id`` on it. ``path_with_namespace`` is the CURRENT, MUTABLE
    slug -- renaming or transferring a project changes it without changing
    ``id``.
    """

    id: str
    path_with_namespace: str
    name: str
    archived: bool
    web_url: str


@dataclass
class GitLabDiscoveryResult:
    """Outcome of a GitLab team discovery walk.

    ``truncated`` is True when any subgroup/project listing hit the discovery
    pagination bound (either the per-group ``teams`` walk or the flat
    ``projects`` walk below), meaning ``teams`` (or their ``repo_patterns``)
    or ``projects`` are a partial view; ``warnings`` carries the
    human-readable details.
    """

    teams: list[DiscoveredTeam]
    #: CHAOS-3380. Additive: existing callers (``api/admin/routers/teams.py``,
    #: ``workers/team_drift_sync.py``) read only ``teams``/``truncated``/
    #: ``warnings`` and are unaffected by this new field.
    projects: list[GitLabDiscoveredProject] = field(default_factory=list)
    truncated: bool = False
    warnings: list[str] = field(default_factory=list)


class TeamDiscoveryService:
    """Service for discovering teams from external providers."""

    def __init__(self, session: AsyncSession | None, org_id: str):
        # ``session`` is optional and unused by the discover_* methods: they
        # perform external network I/O only and never touch the DB, so callers
        # can pass ``None`` and avoid holding a connection idle-in-transaction.
        # Persisting discovered teams is the ClickHouse admin import endpoint's
        # job (ClickHouse is the team system of record, CHAOS-2600).
        self.session = session
        self.org_id = org_id

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        """Discover teams from GitHub organization."""

        def _discover() -> list[DiscoveredTeam]:
            from github import Auth, Github

            DiscoveredTeam = _get_discovered_team_cls()
            auth = Auth.Token(token)
            gh = Github(auth=auth, per_page=100)
            try:
                org = gh.get_organization(org_name)
                teams: list[Any] = []
                for gh_team in org.get_teams():
                    repos = [f"{org_name}/{repo.name}" for repo in gh_team.get_repos()]
                    teams.append(
                        DiscoveredTeam(
                            provider_type="github",
                            provider_team_id=gh_team.slug,
                            name=gh_team.name,
                            description=gh_team.description,
                            member_count=getattr(gh_team, "members_count", None),
                            associations={
                                "repo_patterns": repos,
                                "provider_org": org_name,
                            },
                        )
                    )
                return teams
            finally:
                gh.close()

        return await asyncio.to_thread(_discover)

    async def discover_linear(self, api_key: str) -> list[DiscoveredTeam]:
        """Discover teams from Linear workspace."""

        def _discover() -> list[DiscoveredTeam]:
            from dev_health_ops.providers.linear.client import LinearAuth, LinearClient

            DiscoveredTeam = _get_discovered_team_cls()
            with LinearClient(
                auth=LinearAuth(api_key=api_key), org_id=self.org_id
            ) as client:
                teams: list[Any] = []
                for team in client.iter_teams():
                    teams.append(
                        DiscoveredTeam(
                            provider_type="linear",
                            provider_team_id=team["key"],
                            name=team["name"],
                            description=team.get("description"),
                            # Linear work items normalize with
                            # project_key = team key, so the team key is the
                            # attribution association (mirrors Jira's
                            # project_keys / GitHub-GitLab's repo_patterns).
                            associations={
                                "project_keys": [team["key"]],
                                "provider_org": "linear",
                            },
                        )
                    )
                return teams

        return await asyncio.to_thread(_discover)

    async def discover_gitlab(
        self,
        token: str,
        group_path: str,
        url: str = "https://gitlab.com",
    ) -> GitLabDiscoveryResult:
        """Discover groups/subgroups (as teams) and projects from GitLab.

        Returns a :class:`GitLabDiscoveryResult`; ``truncated`` is set when
        any walk below hit its discovery pagination bound, so callers can
        tell a partial import apart from a complete one.

        Two SEPARATE walks, deliberately not unified (CHAOS-3380, Codex HIGH
        round 2):

        * ``teams`` -- root + DIRECT subgroups only (unchanged from before
          CHAOS-3380), each carrying its OWN ``projects.list()`` page as
          ``repo_patterns``. Existing behavior for existing callers
          (``api/admin/routers/teams.py``, ``workers/team_drift_sync.py``,
          and this worker's OWN ``_project_ownership_rows``) is preserved
          byte-for-byte -- this walk still misses grandchild subgroups, a
          PRE-EXISTING limitation this change does not touch or expand the
          blast radius of.
        * ``projects`` -- the full project catalog CHAOS-3380 needs, listed
          in ONE recursive, paginated call via ``include_subgroups=True``
          (a real, documented ``GroupProjectManager`` list filter --
          ``gitlab/v4/objects/projects.py``'s ``GroupProjectManager.
          _list_filters`` includes it) against the ROOT group only. This
          covers grandchildren and below with no manual subgroup recursion,
          and reads project metadata (``id``, ``path_with_namespace``,
          ``name``, ``archived``, ``web_url``) straight off the listing
          response -- no per-project follow-up GET, so no N+1 fan-out
          regardless of how many projects a large install has.
        """

        def _discover() -> GitLabDiscoveryResult:
            import gitlab as gl_lib

            DiscoveredTeam = _get_discovered_team_cls()
            warnings: list[str] = []
            gl = gl_lib.Gitlab(url=url, private_token=token)
            root_group = gl.groups.get(group_path)
            groups = [root_group]
            subgroups = _bounded_list(
                root_group.subgroups.list(per_page=100, iterator=True),
                MAX_GITLAB_DISCOVERY_SUBGROUPS,
                what="subgroups",
                scope=group_path,
                warnings=warnings,
            )
            for subgroup in subgroups:
                groups.append(gl.groups.get(subgroup.id))

            teams: list[Any] = []
            for group in groups:
                projects = _bounded_list(
                    group.projects.list(per_page=100, iterator=True),
                    MAX_GITLAB_DISCOVERY_PROJECTS,
                    what="projects",
                    scope=group.full_path,
                    warnings=warnings,
                )
                repo_patterns = [p.path_with_namespace for p in projects]
                teams.append(
                    DiscoveredTeam(
                        provider_type="gitlab",
                        provider_team_id=group.full_path,
                        name=group.name,
                        description=group.description,
                        associations={
                            "repo_patterns": repo_patterns,
                            "provider_org": root_group.full_path,
                        },
                    )
                )

            all_projects_raw = _bounded_list(
                root_group.projects.list(
                    include_subgroups=True, per_page=100, iterator=True
                ),
                MAX_GITLAB_DISCOVERY_ALL_PROJECTS,
                what="projects (including all subgroups)",
                scope=group_path,
                warnings=warnings,
            )
            projects = [
                GitLabDiscoveredProject(
                    id=str(project_id),
                    path_with_namespace=str(path),
                    name=str(getattr(raw, "name", "") or path),
                    archived=bool(getattr(raw, "archived", False)),
                    web_url=str(getattr(raw, "web_url", "") or ""),
                )
                for raw in all_projects_raw
                if (project_id := getattr(raw, "id", None)) is not None
                and (path := (getattr(raw, "path_with_namespace", "") or "")).strip()
            ]

            return GitLabDiscoveryResult(
                teams=teams,
                projects=projects,
                truncated=bool(warnings),
                warnings=warnings,
            )

        return await asyncio.to_thread(_discover)

    async def discover_jira(
        self,
        email: str,
        api_token: str,
        url: str,
    ) -> list[DiscoveredTeam]:
        """Discover projects from Jira (as team units)."""

        def _discover() -> list[DiscoveredTeam]:
            DiscoveredTeam = _get_discovered_team_cls()
            response = requests.get(
                f"{url.rstrip('/')}/rest/api/3/project/search",
                auth=(email, api_token),
                params={"maxResults": 100},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()

            teams: list[Any] = []
            for project in payload.get("values", []):
                project_key = project.get("key")
                project_name = project.get("name") or project_key
                if not project_key or not project_name:
                    continue
                teams.append(
                    DiscoveredTeam(
                        provider_type="jira",
                        provider_team_id=project_key,
                        name=project_name,
                        description=project.get("description"),
                        associations={
                            "project_keys": [project_key],
                            "provider_org": url,
                        },
                    )
                )

            return teams

        return await asyncio.to_thread(_discover)

    async def discover_ms_teams(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> list[DiscoveredTeam]:
        """Discover teams from Microsoft Teams (Microsoft Graph API)."""

        async def _discover() -> list[DiscoveredTeam]:
            from dev_health_ops.connectors.teams import TeamsConnector

            DiscoveredTeam = _get_discovered_team_cls()
            connector = TeamsConnector(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
            try:
                ms_teams = await connector.list_teams()
                teams: list[Any] = []
                for t in ms_teams:
                    teams.append(
                        DiscoveredTeam(
                            provider_type="ms-teams",
                            provider_team_id=t.id,
                            name=t.display_name,
                            description=t.description,
                            associations={
                                "provider_org": tenant_id,
                            },
                        )
                    )
                return teams
            finally:
                await connector.close()

        return await _discover()
