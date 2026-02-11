from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import yaml

from dev_health_ops.db import resolve_sink_uri

DEFAULT_TEAM_MAPPING_PATH = Path("src/dev_health_ops/config/team_mapping.yaml")


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


# Canonical sentinel for "no team" - used in PKs and aggregation keys
UNASSIGNED_TEAM_ID = "unassigned"
UNASSIGNED_TEAM_NAME = "Unassigned"


def normalize_team_id(team_id: Optional[str]) -> str:
    """Normalize team_id: None/empty -> 'unassigned'. Single source of truth for PK safety."""
    if not team_id or not team_id.strip():
        return UNASSIGNED_TEAM_ID
    return team_id.strip()


def normalize_team_name(team_name: Optional[str]) -> str:
    """Normalize team_name: None/empty -> 'Unassigned'."""
    if not team_name or not team_name.strip():
        return UNASSIGNED_TEAM_NAME
    return team_name.strip()


def _parse_project_types(value: Optional[str]) -> List[str]:
    raw = value or ""
    items = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return items or ["SERVICE_DESK"]


@dataclass(frozen=True)
class TeamResolver:
    member_to_team: Mapping[
        str, Tuple[str, str]
    ]  # member_identity -> (team_id, team_name)

    def resolve(self, identity: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not identity:
            return None, None
        key = _norm_key(identity)
        team = self.member_to_team.get(key)
        if not team:
            return None, None
        return team[0], team[1]


@dataclass(frozen=True)
class ProjectKeyTeamResolver:
    project_key_to_team: Mapping[str, Tuple[str, str]]

    def resolve(
        self, work_scope_id: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        if not work_scope_id:
            return None, None
        return self.project_key_to_team.get(work_scope_id.strip(), (None, None))


@dataclass(frozen=True)
class RepoPatternTeamResolver:
    _exact: Mapping[str, Tuple[str, str]]
    _prefixes: Sequence[Tuple[str, str, str]]

    def resolve(self, repo_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not repo_name:
            return None, None
        key = repo_name.strip().lower()
        if key in self._exact:
            return self._exact[key]
        for prefix, tid, tname in self._prefixes:
            if key.startswith(prefix):
                return tid, tname
        return None, None


def _build_member_to_team(teams_data: List) -> Dict[str, Tuple[str, str]]:
    """Shared helper to build identity map from a list of team-like objects or dicts."""
    member_to_team: Dict[str, Tuple[str, str]] = {}
    for team in teams_data:
        # Handle both objects (models) and dicts (from YAML)
        team_id = str(
            getattr(team, "id", None)
            or (team.get("id") if isinstance(team, dict) else None)
            or (team.get("team_id") if isinstance(team, dict) else None)
            or ""
        ).strip()
        team_name = str(
            getattr(team, "name", None)
            or (team.get("name") if isinstance(team, dict) else None)
            or (team.get("team_name") if isinstance(team, dict) else team_id)
        ).strip()

        if not team_id:
            continue

        members_raw = (
            getattr(team, "members", None)
            or (team.get("members") if isinstance(team, dict) else None)
            or []
        )
        members_list: List[Any] = list(members_raw)
        for member in members_list:
            key = _norm_key(str(member))
            if not key:
                continue
            member_to_team[key] = (team_id, team_name)
    return member_to_team


def load_team_resolver(path: Optional[Path] = None) -> TeamResolver:
    raw_path = os.getenv("TEAM_MAPPING_PATH")
    if raw_path:
        path = Path(raw_path)
    path = path or DEFAULT_TEAM_MAPPING_PATH

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except FileNotFoundError:
        payload = {}

    teams_list = payload.get("teams") or []
    member_to_team = _build_member_to_team(teams_list)
    return TeamResolver(member_to_team=member_to_team)


async def load_team_resolver_from_store(store: Any) -> TeamResolver:
    """Load team mappings from the database store."""
    try:
        teams = await store.get_all_teams()
        member_to_team = _build_member_to_team(teams)
        return TeamResolver(member_to_team=member_to_team)
    except Exception as e:
        import logging

        logging.warning(f"Failed to load teams from store: {e}")
        return TeamResolver(member_to_team={})


def build_project_key_resolver(teams_data: List) -> ProjectKeyTeamResolver:
    mapping: Dict[str, Tuple[str, str]] = {}
    for team in teams_data:
        team_id = str(
            team.get("id") if isinstance(team, dict) else getattr(team, "id", "")
        ).strip()
        team_name = str(
            team.get("name")
            if isinstance(team, dict)
            else getattr(team, "name", team_id)
        ).strip()
        project_keys = (
            team.get("project_keys")
            if isinstance(team, dict)
            else getattr(team, "project_keys", [])
        )
        if not team_id or not project_keys:
            continue
        for pk in project_keys:
            key = str(pk).strip()
            if key and key not in mapping:
                mapping[key] = (team_id, team_name)
    return ProjectKeyTeamResolver(project_key_to_team=mapping)


async def load_project_key_resolver_from_store(store: Any) -> ProjectKeyTeamResolver:
    try:
        teams = await store.get_all_teams()
        return build_project_key_resolver(teams)
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            "Failed to load project key resolver: %s", e
        )
        return ProjectKeyTeamResolver(project_key_to_team={})


def build_repo_pattern_resolver(teams_data: List) -> RepoPatternTeamResolver:
    exact: Dict[str, Tuple[str, str]] = {}
    prefixes: List[Tuple[str, str, str]] = []
    for team in teams_data:
        team_id = str(
            team.get("id") if isinstance(team, dict) else getattr(team, "id", "")
        ).strip()
        team_name = str(
            team.get("name")
            if isinstance(team, dict)
            else getattr(team, "name", team_id)
        ).strip()
        repo_patterns_raw = (
            team.get("repo_patterns")
            if isinstance(team, dict)
            else getattr(team, "repo_patterns", [])
        )
        if not team_id or not repo_patterns_raw:
            continue
        for pattern in repo_patterns_raw:
            p = str(pattern).strip().lower()
            if not p:
                continue
            if "*" in p:
                prefix = p.rstrip("*").rstrip("/")
                if prefix:
                    prefixes.append((prefix, team_id, team_name))
            else:
                exact[p] = (team_id, team_name)
    prefixes.sort(key=lambda x: -len(x[0]))
    return RepoPatternTeamResolver(_exact=exact, _prefixes=tuple(prefixes))


def sync_teams(ns: argparse.Namespace) -> int:
    """
    Sync teams from various providers (config, Jira, synthetic) to the database.
    """
    import asyncio
    import logging
    from dev_health_ops.models.teams import Team
    from dev_health_ops.storage import resolve_db_type, run_with_store

    provider = (ns.provider or "config").lower()
    teams_data: List[Team] = []
    ops_links: List[JiraProjectOpsTeamLink] = []

    if provider == "config":
        import yaml

        path = Path(ns.path) if ns.path else DEFAULT_TEAM_MAPPING_PATH
        if not path.exists():
            logging.error(f"Teams config file not found at {path}")
            return 1

        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
        except Exception as e:
            logging.error(f"Failed to parse teams config: {e}")
            return 1

        for entry in payload.get("teams") or []:
            team_id = str(entry.get("team_id") or "").strip()
            team_name = str(entry.get("team_name") or team_id).strip()
            description = entry.get("description")
            members = entry.get("members") or []
            if team_id:
                teams_data.append(
                    Team(
                        id=team_id,
                        name=team_name,
                        description=str(description) if description else None,
                        members=[str(m) for m in members],
                    )
                )

    elif provider == "jira":
        from dev_health_ops.providers.jira.client import JiraClient

        try:
            client = JiraClient.from_env()
        except ValueError as e:
            logging.error(f"Jira configuration error: {e}")
            return 1

        try:
            logging.info("Fetching projects from Jira...")
            projects = client.get_all_projects()
            for p in projects:
                # Use project Key as ID (stable), Name as Name
                key = p.get("key")
                name = p.get("name")
                desc = p.get("description")
                lead = p.get("lead", {})

                members = []
                if lead and lead.get("accountId"):
                    members.append(lead.get("accountId"))

                if key and name:
                    teams_data.append(
                        Team(
                            id=key,
                            name=name,
                            description=str(desc) if desc else f"Jira Project {key}",
                            members=members,
                        )
                    )
            logging.info(f"Fetched {len(teams_data)} projects from Jira.")
        except Exception as e:
            logging.error(f"Failed to fetch Jira projects: {e}")
            return 1
        finally:
            client.close()

    elif provider == "jira-ops":
        from atlassian.graph.api.jira_projects import (
            iter_projects_with_opsgenie_linkable_teams,
        )

        from dev_health_ops.models.teams import JiraProjectOpsTeamLink
        from dev_health_ops.providers.jira.atlassian_compat import (
            build_atlassian_graphql_client,
            get_atlassian_cloud_id,
        )

        cloud_id = get_atlassian_cloud_id()
        if not cloud_id:
            logging.error("ATLASSIAN_CLOUD_ID is required for jira-ops provider")
            return 1

        project_types = _parse_project_types(os.getenv("JIRA_OPS_PROJECT_TYPES"))
        ops_team_cache: Dict[str, Team] = {}

        client = build_atlassian_graphql_client()
        try:
            for project in iter_projects_with_opsgenie_linkable_teams(
                client,
                cloud_id=cloud_id,
                project_types=project_types,
            ):
                for team in project.opsgenie_teams:
                    team_id = f"ops:{team.id}"
                    if team_id not in ops_team_cache:
                        ops_team_cache[team_id] = Team(
                            id=team_id,
                            name=team.name,
                            description=f"Atlassian Ops team linked to Jira {project.project.key}",
                            members=[],
                        )
                    ops_links.append(
                        JiraProjectOpsTeamLink(
                            project_key=project.project.key,
                            project_name=project.project.name,
                            ops_team_id=team.id,
                            ops_team_name=team.name,
                        )
                    )

            teams_data.extend(list(ops_team_cache.values()))
            logging.info(
                "Fetched %d Jira project ops team links (teams=%d)",
                len(ops_links),
                len(teams_data),
            )
        except Exception as e:
            logging.error(f"Failed to fetch Jira ops teams: {e}")
            return 1
        finally:
            client.close()

    elif provider == "synthetic":
        from dev_health_ops.fixtures.generator import SyntheticDataGenerator

        generator = SyntheticDataGenerator()
        teams_data = generator.generate_teams(count=8)
        logging.info(f"Generated {len(teams_data)} synthetic teams.")

    elif provider == "github":
        from github import Auth, Github

        token = getattr(ns, "auth", None) or os.getenv("GITHUB_TOKEN") or ""
        owner = getattr(ns, "owner", None)
        if not owner:
            logging.error("--owner is required for github provider (org name).")
            return 1
        if not token:
            logging.error(
                "GitHub token required. Use --auth or set GITHUB_TOKEN env var."
            )
            return 1

        try:
            auth = Auth.Token(token)
            gh = Github(auth=auth, per_page=100)
            logging.info(f"Fetching teams from GitHub org '{owner}'...")
            org = gh.get_organization(owner)
            for gh_team in org.get_teams():
                members = [m.login for m in gh_team.get_members()]
                teams_data.append(
                    Team(
                        id=f"gh:{gh_team.slug}",
                        name=gh_team.name,
                        description=gh_team.description
                        or f"GitHub team {gh_team.slug}",
                        members=members,
                    )
                )
            gh.close()
            logging.info(f"Fetched {len(teams_data)} teams from GitHub.")
        except Exception as e:
            logging.error(f"Failed to fetch GitHub teams: {e}")
            return 1

    elif provider == "gitlab":
        import gitlab as gl_lib

        token = getattr(ns, "auth", None) or os.getenv("GITLAB_TOKEN") or ""
        owner = getattr(ns, "owner", None)
        url = os.getenv("GITLAB_URL", "https://gitlab.com")
        if not owner:
            logging.error("--owner is required for gitlab provider (group path).")
            return 1
        if not token:
            logging.error(
                "GitLab token required. Use --auth or set GITLAB_TOKEN env var."
            )
            return 1

        try:
            gl = gl_lib.Gitlab(url=url, private_token=token)
            logging.info(f"Fetching teams from GitLab group '{owner}'...")
            group = gl.groups.get(owner)
            members_list = group.members.list(per_page=100, get_all=True)
            teams_data.append(
                Team(
                    id=f"gl:{group.path}",
                    name=group.name,
                    description=group.description or f"GitLab group {group.full_path}",
                    members=[m.username for m in members_list],
                )
            )
            # Also fetch subgroups as separate teams
            for subgroup in group.subgroups.list(per_page=100, get_all=True):
                full_sg = gl.groups.get(subgroup.id)
                sg_members = full_sg.members.list(per_page=100, get_all=True)
                teams_data.append(
                    Team(
                        id=f"gl:{full_sg.path}",
                        name=full_sg.name,
                        description=full_sg.description
                        or f"GitLab group {full_sg.full_path}",
                        members=[m.username for m in sg_members],
                    )
                )
            logging.info(f"Fetched {len(teams_data)} teams from GitLab.")
        except Exception as e:
            logging.error(f"Failed to fetch GitLab teams: {e}")
            return 1

    elif provider == "ms-teams":
        from dev_health_ops.connectors.teams import TeamsConnector

        try:
            connector = TeamsConnector.from_env()
        except ValueError as e:
            logging.error(f"Microsoft Teams configuration error: {e}")
            return 1

        async def fetch_teams():
            teams = await connector.list_teams_with_details(
                include_channels=True,
                include_members=True,
            )
            await connector.close()
            return teams

        try:
            logging.info("Fetching teams from Microsoft Teams...")
            ms_teams = asyncio.run(fetch_teams())
            for t in ms_teams:
                member_ids = [m.id for m in t.members]
                teams_data.append(
                    Team(
                        id=f"ms-teams:{t.id}",
                        name=t.display_name,
                        description=t.description
                        or f"Microsoft Teams team: {t.display_name}",
                        members=member_ids,
                    )
                )
            logging.info(f"Fetched {len(teams_data)} teams from Microsoft Teams.")
        except Exception as e:
            logging.error(f"Failed to fetch Microsoft Teams: {e}")
            return 1

    else:
        logging.error(f"Unknown provider: {provider}")
        return 1

    if not teams_data:
        logging.warning("No teams found/generated.")
        return 0

    db_uri = resolve_sink_uri(ns)
    db_type = resolve_db_type(db_uri, ns.db_type)

    async def _handler(store):
        # Ensure table exists (for SQL stores)
        if hasattr(store, "ensure_tables"):
            await store.ensure_tables()
        await store.insert_teams(teams_data)
        if ops_links and hasattr(store, "insert_jira_project_ops_team_links"):
            await store.insert_jira_project_ops_team_links(ops_links)
            logging.info(
                "Synced %d jira project ops team links to DB.",
                len(ops_links),
            )
        logging.info(f"Synced {len(teams_data)} teams to DB.")

    asyncio.run(run_with_store(db_uri, db_type, _handler))

    _bridge_teams_to_postgres(teams_data, ns)
    return 0


def _bridge_teams_to_postgres(teams_data: List, ns: argparse.Namespace) -> None:
    """Upsert Team records into PostgreSQL TeamMapping table for multi-tenant bridge."""
    import logging

    from sqlalchemy import select

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import TeamMapping

    org_id = getattr(ns, "org", "default")

    try:
        with get_postgres_session_sync() as session:
            for team in teams_data:
                team_id = str(getattr(team, "id", "")).strip()
                if not team_id:
                    continue

                existing = session.execute(
                    select(TeamMapping).where(
                        TeamMapping.org_id == org_id,
                        TeamMapping.team_id == team_id,
                    )
                ).scalar_one_or_none()

                if existing:
                    existing.name = getattr(team, "name", team_id)
                    existing.description = getattr(team, "description", None)
                    existing.is_active = True
                else:
                    session.add(
                        TeamMapping(
                            team_id=team_id,
                            name=getattr(team, "name", team_id),
                            org_id=org_id,
                            description=getattr(team, "description", None),
                            is_active=True,
                        )
                    )
            logging.info(
                "Bridged %d teams to PostgreSQL TeamMapping (org=%s).",
                len(teams_data),
                org_id,
            )
    except Exception as e:
        logging.warning("Failed to bridge teams to PostgreSQL (non-fatal): %s", e)


def register_commands(sync_subparsers: argparse._SubParsersAction) -> None:
    teams = sync_subparsers.add_parser(
        "teams",
        help="Sync teams from dev_health_ops.config/teams.yaml, Jira, or Synthetic.",
    )
    teams.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    teams.add_argument(
        "--provider",
        choices=[
            "config",
            "jira",
            "jira-ops",
            "synthetic",
            "ms-teams",
            "github",
            "gitlab",
        ],
        default="config",
        help="Source of team data (default: config).",
    )
    teams.add_argument(
        "--path", help="Path to teams.yaml config (used if provider=config)."
    )
    teams.add_argument(
        "--owner",
        help="GitHub org or GitLab group path (required for github/gitlab providers).",
    )
    teams.add_argument(
        "--auth",
        help="Provider token override (GitHub/GitLab). Falls back to env vars.",
    )
    teams.set_defaults(func=sync_teams)
