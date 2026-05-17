"""Team discovery service.

Pulls team-like units (GitHub teams, GitLab subgroups, Linear teams, Jira
projects) from external providers and imports them into ``TeamMapping``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import requests
from sqlalchemy.ext.asyncio import AsyncSession

from ._helpers import _get_discovered_team_cls
from .team_mapping import TeamMappingService

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import DiscoveredTeam


class TeamDiscoveryService:
    """Service for discovering teams from external providers."""

    def __init__(self, session: AsyncSession, org_id: str):
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
            with LinearClient(auth=LinearAuth(api_key=api_key)) as client:
                teams: list[Any] = []
                for team in client.iter_teams():
                    teams.append(
                        DiscoveredTeam(
                            provider_type="linear",
                            provider_team_id=team["key"],
                            name=team["name"],
                            description=team.get("description"),
                            associations={"provider_org": "linear"},
                        )
                    )
                return teams

        return await asyncio.to_thread(_discover)

    async def discover_gitlab(
        self,
        token: str,
        group_path: str,
        url: str = "https://gitlab.com",
    ) -> list[DiscoveredTeam]:
        """Discover groups/subgroups from GitLab."""

        def _discover() -> list[DiscoveredTeam]:
            import gitlab as gl_lib

            DiscoveredTeam = _get_discovered_team_cls()
            gl = gl_lib.Gitlab(url=url, private_token=token)
            root_group = gl.groups.get(group_path)
            groups = [root_group]
            for subgroup in root_group.subgroups.list(per_page=100, get_all=True):
                groups.append(gl.groups.get(subgroup.id))

            teams: list[Any] = []
            for group in groups:
                projects = group.projects.list(per_page=100, get_all=True)
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

            return teams

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

    async def import_teams(
        self,
        teams: list[DiscoveredTeam],
        on_conflict: str = "skip",
    ) -> dict[str, Any]:
        """Import discovered teams into TeamMapping."""
        team_mapping_svc = TeamMappingService(self.session, self.org_id)
        imported = 0
        skipped = 0
        merged = 0
        details: list[dict[str, Any]] = []

        for team in teams:
            if team.provider_type == "github":
                team_id = f"gh:{team.provider_team_id}"
            elif team.provider_type == "gitlab":
                team_id = f"gl:{team.provider_team_id}"
            else:
                team_id = team.provider_team_id

            existing = await team_mapping_svc.get(team_id)
            if existing is not None and on_conflict == "skip":
                skipped += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "skipped",
                    }
                )
                continue

            associations = team.associations or {}
            provider_linkage = {
                "provider_type": team.provider_type,
                "provider_team_id": team.provider_team_id,
                "provider_org": associations.get("provider_org"),
                "last_discovered_at": datetime.now(timezone.utc).isoformat(),
                "sync_source": "imported",
            }

            extra_data = dict(existing.extra_data or {}) if existing else {}
            extra_data.update(
                {k: v for k, v in provider_linkage.items() if v is not None}
            )

            await team_mapping_svc.create_or_update(
                team_id=team_id,
                name=team.name,
                description=team.description,
                repo_patterns=associations.get("repo_patterns", []),
                project_keys=associations.get("project_keys", []),
                extra_data=extra_data,
            )

            if existing is None:
                imported += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "imported",
                    }
                )
            else:
                merged += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "merged",
                    }
                )

        return {
            "imported": imported,
            "skipped": skipped,
            "merged": merged,
            "details": details,
        }
