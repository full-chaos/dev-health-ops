from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml

DEFAULT_TEAM_MAPPING_PATH = Path("src/dev_health_ops/config/team_mapping.yaml")


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


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
        # Use 8 teams as requested for better visualization
        teams_data = generator.generate_teams(count=8)
        logging.info(f"Generated {len(teams_data)} synthetic teams.")

    else:
        logging.error(f"Unknown provider: {provider}")
        return 1

    if not teams_data:
        logging.warning("No teams found/generated.")
        return 0

    db_type = resolve_db_type(ns.db, ns.db_type)

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

    asyncio.run(run_with_store(ns.db, db_type, _handler))
    return 0


def register_commands(sync_subparsers: argparse._SubParsersAction) -> None:
    teams = sync_subparsers.add_parser(
        "teams",
        help="Sync teams from dev_health_ops.config/teams.yaml, Jira, or Synthetic.",
    )
    teams.add_argument("--db", required=True, help="Database connection string.")
    teams.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    teams.add_argument(
        "--provider",
        choices=["config", "jira", "jira-ops", "synthetic"],
        default="config",
        help="Source of team data (default: config).",
    )
    teams.add_argument(
        "--path", help="Path to teams.yaml config (used if provider=config)."
    )
    teams.set_defaults(func=sync_teams)
