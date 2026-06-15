from __future__ import annotations

import argparse
import importlib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select

from dev_health_ops.db import (
    get_postgres_session_sync_for_uri,
    resolve_db_uri,
    resolve_sink_uri,
)
from dev_health_ops.models.settings import IdentityMapping, TeamMapping
from dev_health_ops.models.teams import Team
from dev_health_ops.storage import detect_db_type
from dev_health_ops.utils.cli import add_sink_arg, validate_sink

DEFAULT_TEAM_MAPPING_PATH = Path("src/dev_health_ops/config/team_mapping.yaml")


def _yaml_safe_load(stream: Any) -> Any:
    yaml_module = importlib.import_module("yaml")
    safe_load = getattr(yaml_module, "safe_load")
    return safe_load(stream)


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


# Canonical sentinel for "no team" - used in PKs and aggregation keys
UNASSIGNED_TEAM_ID = "unassigned"
UNASSIGNED_TEAM_NAME = "Unassigned"


def normalize_team_id(team_id: str | None) -> str:
    """Normalize team_id: None/empty -> 'unassigned'. Single source of truth for PK safety."""
    if not team_id or not team_id.strip():
        return UNASSIGNED_TEAM_ID
    return team_id.strip()


def normalize_team_name(team_name: str | None) -> str:
    """Normalize team_name: None/empty -> 'Unassigned'."""
    if not team_name or not team_name.strip():
        return UNASSIGNED_TEAM_NAME
    return team_name.strip()


def _parse_project_types(value: str | None) -> list[str]:
    raw = value or ""
    items = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return items or ["SERVICE_DESK"]


@dataclass(frozen=True)
class TeamResolver:
    member_to_team: Mapping[
        str, tuple[str, str]
    ]  # member_identity -> (team_id, team_name)

    def resolve(self, identity: str | None) -> tuple[str | None, str | None]:
        if not identity:
            return None, None
        key = _norm_key(identity)
        team = self.member_to_team.get(key)
        if not team:
            return None, None
        return team[0], team[1]


@dataclass(frozen=True)
class ProjectKeyTeamResolver:
    """Resolves a team from a provider attribution key.

    "Project key" here is the provider's team-attribution key, not
    necessarily a project: Jira → project key, Linear → TEAM key (Linear
    projects are a separate concept carried in ``WorkItem.project_id``).
    Callers should try ``work_scope_id`` first and fall back to
    ``project_key`` — for Linear issues inside a project the two differ.
    Membership-based ``TeamResolver`` remains the next fallback.
    """

    project_key_to_team: Mapping[str, tuple[str, str]]

    def resolve(self, work_scope_id: str | None) -> tuple[str | None, str | None]:
        if not work_scope_id:
            return None, None
        return self.project_key_to_team.get(work_scope_id.strip(), (None, None))


@dataclass(frozen=True)
class RepoPatternTeamResolver:
    _exact: Mapping[str, tuple[str, str]]
    _prefixes: Sequence[tuple[str, str, str]]

    def resolve(self, repo_name: str | None) -> tuple[str | None, str | None]:
        if not repo_name:
            return None, None
        key = repo_name.strip().lower()
        if key in self._exact:
            return self._exact[key]
        for prefix, tid, tname in self._prefixes:
            if key.startswith(prefix):
                return tid, tname
        return None, None


@dataclass(frozen=True)
class LinkedIssueTeamResolver:
    """Inherit team attribution from a linked work item.

    This is the final fallback after scope-key, project-key and membership
    resolution. A work item that itself resolves to no team — e.g. a
    GitHub/GitLab PR whose repo maps to no team and whose author is not a
    team member — borrows the team of an issue it links to via
    ``work_item_dependencies``.

    The mechanism is provider-agnostic: the donor issue may live in a
    different provider than the borrowing PR (a GitHub PR inheriting from a
    Linear or Jira issue it closes). That is exactly the cross-provider
    recovery the team-exchange chord and allocation-coverage views need,
    since PRs otherwise always land as ``unassigned`` and never share a team
    dimension with the issue trackers.

    Instances are cheap dict lookups; the edge walking happens once at build
    time. Build with
    :func:`dev_health_ops.metrics.compute_work_items.build_linked_issue_team_resolver`.
    """

    # source work_item_id -> (team_id, team_name) inherited from a linked,
    # already-team-attributed target.
    _inherited: Mapping[str, tuple[str, str]]

    def resolve(self, work_item_id: str | None) -> tuple[str | None, str | None]:
        if not work_item_id:
            return None, None
        return self._inherited.get(work_item_id, (None, None))


def _build_member_to_team(teams_data: list) -> dict[str, tuple[str, str]]:
    """Shared helper to build identity map from a list of team-like objects or dicts."""
    member_to_team: dict[str, tuple[str, str]] = {}
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
        members_list: list[Any] = list(members_raw)
        for member in members_list:
            key = _norm_key(str(member))
            if not key:
                continue
            member_to_team[key] = (team_id, team_name)
    return member_to_team


def load_team_resolver(path: Path | None = None) -> TeamResolver:
    raw_path = os.getenv("TEAM_MAPPING_PATH")
    if raw_path:
        path = Path(raw_path)
    path = path or DEFAULT_TEAM_MAPPING_PATH

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = _yaml_safe_load(handle) or {}
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


def build_project_key_resolver(teams_data: list) -> ProjectKeyTeamResolver:
    mapping: dict[str, tuple[str, str]] = {}
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


def build_repo_pattern_resolver(teams_data: list) -> RepoPatternTeamResolver:
    exact: dict[str, tuple[str, str]] = {}
    prefixes: list[tuple[str, str, str]] = []
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

    from dev_health_ops.storage import run_with_store

    provider = (ns.provider or "config").lower()
    teams_data: list[Team] = []
    ops_links: list[JiraProjectOpsTeamLink] = []

    if provider == "config":
        path = Path(ns.path) if ns.path else DEFAULT_TEAM_MAPPING_PATH
        if not path.exists():
            logging.error(f"Teams config file not found at {path}")
            return 1

        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = _yaml_safe_load(handle) or {}
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
                setattr(
                    teams_data[-1],
                    "project_keys",
                    [str(k) for k in entry.get("project_keys", []) if k],
                )
                setattr(
                    teams_data[-1],
                    "repo_patterns",
                    [str(p) for p in entry.get("repo_patterns", []) if p],
                )
                setattr(teams_data[-1], "members_complete", True)

    elif provider == "jira":
        from dev_health_ops.providers.jira.client import JiraClient

        try:
            jira_client = JiraClient.from_env()
        except ValueError as e:
            logging.error(f"Jira configuration error: {e}")
            return 1

        try:
            logging.info("Fetching projects from Jira...")
            projects = jira_client.get_all_projects()
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
                    setattr(teams_data[-1], "members_complete", False)
            logging.info(f"Fetched {len(teams_data)} projects from Jira.")
        except Exception as e:
            logging.error(f"Failed to fetch Jira projects: {e}")
            return 1
        finally:
            jira_client.close()

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
        ops_team_cache: dict[str, Team] = {}

        atlassian_client = build_atlassian_graphql_client()
        try:
            for project in iter_projects_with_opsgenie_linkable_teams(
                atlassian_client,
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
                        setattr(ops_team_cache[team_id], "members_complete", False)
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
            atlassian_client.close()

    elif provider == "synthetic":
        from dev_health_ops.fixtures.generator import SyntheticDataGenerator

        generator = SyntheticDataGenerator()
        teams_data = generator.generate_teams(count=8)
        for team in teams_data:
            setattr(team, "members_complete", True)
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
                setattr(teams_data[-1], "members_complete", True)
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
            setattr(teams_data[-1], "members_complete", True)
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
                setattr(teams_data[-1], "members_complete", True)
            logging.info(f"Fetched {len(teams_data)} teams from GitLab.")
        except Exception as e:
            logging.error(f"Failed to fetch GitLab teams: {e}")
            return 1

    elif provider == "linear":
        from dev_health_ops.providers.linear.client import LinearClient

        try:
            linear_client = LinearClient.from_env()
        except ValueError as e:
            logging.error(f"Linear configuration error: {e}")
            return 1
        try:
            logging.info("Fetching teams from Linear...")
            for t in linear_client.iter_teams():
                if t.get("archivedAt"):
                    continue
                team_key = t.get("key")
                if not team_key:
                    continue
                team_id = f"linear:{team_key}"
                name = str(t.get("name") or team_key)
                description = t.get("description")
                description_text = str(description) if description else None
                members_nodes = (t.get("members", {}) or {}).get("nodes", [])
                members_complete = True
                if t.get("members", {}).get("pageInfo", {}).get("hasNextPage"):
                    try:
                        full_members = linear_client.get_team_members(
                            str(t.get("id") or "")
                        )
                    except Exception:
                        full_members = members_nodes
                        members_complete = False
                    members_source = full_members
                else:
                    members_source = members_nodes
                members = [
                    (m.get("email") or m.get("name", "")) for m in members_source
                ]
                members = [m for m in members if m]
                team = Team(
                    id=team_id,
                    name=name,
                    description=description_text,
                    members=members,
                )
                setattr(team, "members_complete", members_complete)
                teams_data.append(team)
            logging.info(f"Fetched {len(teams_data)} teams from Linear.")
        except Exception as e:
            logging.error(f"Failed to fetch Linear teams: {e}")
            return 1
        finally:
            if hasattr(linear_client, "close"):
                try:
                    linear_client.close()
                except Exception:  # noqa: BLE001 — best-effort close, ignore errors
                    pass
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
                setattr(teams_data[-1], "members_complete", True)
            logging.info(f"Fetched {len(teams_data)} teams from Microsoft Teams.")
        except Exception as e:
            logging.error(f"Failed to fetch Microsoft Teams: {e}")
            return 1

    else:
        logging.error(f"Unknown provider: {provider}")
        return 1

    if not teams_data:
        message = "No teams found/generated."
        if getattr(ns, "allow_empty", False):
            logging.warning(message)
            return 0
        logging.error(
            "%s Pass --allow-empty to exit successfully on an empty sync.", message
        )
        return 1

    org_id = getattr(ns, "org", None)

    if org_id is not None:
        # Org-scoped path: provider -> Postgres TeamMapping -> bridge_teams_to_clickhouse.
        # Never write ClickHouse directly; the bridge reads from Postgres so the
        # semantic layer is always the source of truth for org-scoped teams.
        validate_sink(ns)
        db_uri = resolve_sink_uri(ns)
        postgres_db_uri = resolve_db_uri(ns)
        postgres_bridge_count: int = 0
        try:
            result = _bridge_teams_to_postgres(teams_data, ns)
            postgres_bridge_count = result if result is not None else 0
        except Exception as e:  # noqa: BLE001 - bridge failures must affect exit code
            logging.error("Failed to bridge teams to PostgreSQL: %s", e)
            postgres_bridge_count = 0

        if postgres_bridge_count <= 0:
            message = (
                "No teams were persisted to PostgreSQL TeamMapping "
                f"(org={org_id}, count={postgres_bridge_count})."
            )
            if getattr(ns, "allow_empty", False):
                logging.warning(message)
                return 0
            logging.error(
                "%s Pass --allow-empty to exit successfully on an empty sync.", message
            )
            return 1

        # Bridge Postgres TeamMapping -> ClickHouse.
        try:
            from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

            bridge_teams_to_clickhouse(
                org_id=org_id,
                db_url=db_uri,
                postgres_db_url=postgres_db_uri,
            )
            logging.info(
                "Bridged %d teams from PostgreSQL to ClickHouse (org=%s).",
                postgres_bridge_count,
                org_id,
            )
            if ops_links:
                import asyncio

                from dev_health_ops.storage.clickhouse import ClickHouseStore

                async def _insert_ops_links() -> None:
                    async with ClickHouseStore(db_uri) as store:
                        store.org_id = str(org_id)
                        await store.insert_jira_project_ops_team_links(ops_links)

                asyncio.run(_insert_ops_links())
                logging.info(
                    "Synced %d jira project ops team links to ClickHouse (org=%s).",
                    len(ops_links),
                    org_id,
                )
        except Exception as e:  # noqa: BLE001 - bridge failures must affect exit code
            logging.error("bridge_teams_to_clickhouse failed (org=%s): %s", org_id, e)
            return 1

        return 0

    # No-org path: write directly to ClickHouse (unchanged).
    validate_sink(ns)
    db_uri = resolve_sink_uri(ns)
    db_type = detect_db_type(db_uri)

    async def _handler(store) -> int:
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
        persisted_count = await _count_persisted_teams(store, teams_data)
        logging.info("Verified %d teams persisted to primary store.", persisted_count)
        return persisted_count

    persisted_count = asyncio.run(
        run_with_store(db_uri, db_type, _handler, org_id=None)
    )

    if persisted_count <= 0:
        message = "No teams were persisted to ClickHouse."
        if getattr(ns, "allow_empty", False):
            logging.warning(message)
            return 0
        logging.error(
            "%s Pass --allow-empty to exit successfully on an empty sync.", message
        )
        return 1

    return 0


async def _count_persisted_teams(store: Any, teams_data: list) -> int:
    expected_ids = {
        str(getattr(team, "id", "") or "").strip() for team in teams_data
    } - {""}
    if not expected_ids or not hasattr(store, "get_all_teams"):
        return 0

    org_id = getattr(store, "org_id", None)
    persisted = await store.get_all_teams()
    persisted_ids = {
        str(getattr(team, "id", "") or "").strip()
        for team in persisted
        # When the store is org-scoped, only count rows for that org so a
        # stale/global row from another tenant cannot mask a failed org write.
        if not org_id or str(getattr(team, "org_id", "") or "") == str(org_id)
    } - {""}
    return len(expected_ids & persisted_ids)


def _team_string_list(team: Any, field: str) -> list[str]:
    value = team.get(field) if isinstance(team, dict) else getattr(team, field, [])
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _provider_team_id(team_id: str, provider: str) -> str:
    if provider == "github" and team_id.startswith("gh:"):
        return team_id.removeprefix("gh:")
    if provider == "gitlab" and team_id.startswith("gl:"):
        return team_id.removeprefix("gl:")
    if provider == "linear" and team_id.startswith("linear:"):
        return team_id.removeprefix("linear:")
    if provider == "ms-teams" and team_id.startswith("ms-teams:"):
        return team_id.removeprefix("ms-teams:")
    return team_id


def _project_keys_for_team(team_id: str, provider: str, team: Any) -> list[str]:
    configured = _team_string_list(team, "project_keys")
    if configured:
        return configured
    if provider == "jira":
        return [team_id]
    if provider == "linear" and team_id.startswith("linear:"):
        return [team_id.removeprefix("linear:")]
    return []


def _team_members(team: Any) -> list[str]:
    return [
        str(member).strip()
        for member in (getattr(team, "members", None) or [])
        if str(member).strip()
    ]


def _team_members_complete(team: Any) -> bool:
    return bool(getattr(team, "members_complete", False))


def _identity_member_values(identity: Any) -> set[str]:
    values: set[str] = set()
    email = getattr(identity, "email", None)
    if email:
        values.add(str(email))
    canonical_id = getattr(identity, "canonical_id", None)
    if canonical_id:
        values.add(str(canonical_id))
    for provider_values in (
        getattr(identity, "provider_identities", None) or {}
    ).values():
        if isinstance(provider_values, list):
            values.update(str(value) for value in provider_values if value)
        elif provider_values:
            values.add(str(provider_values))
    return values


def _reconcile_team_member_identities(
    session: Any,
    *,
    org_id: str,
    team_id: str,
    provider: str,
    members: list[str],
    members_complete: bool,
) -> None:
    desired_members = set(members)
    if members_complete:
        existing_identities = (
            session.execute(
                select(IdentityMapping).where(
                    IdentityMapping.org_id == org_id,
                    IdentityMapping.is_active.is_(True),
                )
            )
            .scalars()
            .all()
        )
        for identity in existing_identities:
            provider_identities = dict(
                getattr(identity, "provider_identities", {}) or {}
            )
            if provider not in provider_identities:
                continue
            team_ids = list(getattr(identity, "team_ids", []) or [])
            if team_id not in team_ids:
                continue
            if _identity_member_values(identity) & desired_members:
                continue
            team_ids = [
                existing_team_id
                for existing_team_id in team_ids
                if existing_team_id != team_id
            ]
            setattr(identity, "team_ids", team_ids)

    for member in members:
        existing = session.execute(
            select(IdentityMapping).filter_by(org_id=org_id, canonical_id=member)
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                IdentityMapping(
                    org_id=org_id,
                    canonical_id=member,
                    email=member if "@" in member else None,
                    display_name=None if "@" in member else member,
                    provider_identities={provider: [member]},
                    team_ids=[team_id],
                    is_active=True,
                )
            )
            continue

        provider_identities = dict(getattr(existing, "provider_identities", {}) or {})
        provider_members = list(provider_identities.get(provider) or [])
        if member not in provider_members:
            provider_members.append(member)
        provider_identities[provider] = provider_members

        team_ids = list(getattr(existing, "team_ids", []) or [])
        if team_id not in team_ids:
            team_ids.append(team_id)

        setattr(existing, "provider_identities", provider_identities)
        setattr(existing, "team_ids", team_ids)
        setattr(existing, "is_active", True)


def _bridge_teams_to_postgres(teams_data: list, ns: argparse.Namespace) -> int | None:
    """Upsert provider-discovered teams into the org-scoped TeamMapping layer."""
    import logging

    org_id = getattr(ns, "org", None)
    if org_id is None:
        return None
    provider = str(getattr(ns, "provider", "config") or "config").lower()

    expected_ids = {
        str(getattr(team, "id", "") or "").strip() for team in teams_data
    } - {""}
    if not expected_ids:
        return 0

    try:
        with get_postgres_session_sync_for_uri(resolve_db_uri(ns)) as session:
            for team in teams_data:
                team_id = str(getattr(team, "id", "")).strip()
                if not team_id:
                    continue

                existing = session.execute(
                    select(TeamMapping).filter_by(org_id=org_id, team_id=team_id)
                ).scalar_one_or_none()

                team_name = str(getattr(team, "name", team_id) or team_id)
                team_description = getattr(team, "description", None)
                repo_patterns = _team_string_list(team, "repo_patterns")
                project_keys = _project_keys_for_team(team_id, provider, team)
                members = _team_members(team)
                members_complete = _team_members_complete(team)
                extra_data = dict(getattr(existing, "extra_data", {}) or {})
                extra_data.update(
                    {
                        "provider_type": provider,
                        "provider_team_id": _provider_team_id(team_id, provider),
                        "last_discovered_at": datetime.now(timezone.utc).isoformat(),
                        "sync_source": "cli-sync-teams",
                    }
                )

                if existing:
                    managed_fields = set(getattr(existing, "managed_fields", []) or [])
                    if getattr(existing, "sync_policy", 1) == 0:
                        if "name" in managed_fields:
                            setattr(existing, "name", team_name)
                        if (
                            "description" in managed_fields
                            and team_description is not None
                        ):
                            setattr(existing, "description", team_description)
                        if "repo_patterns" in managed_fields:
                            setattr(existing, "repo_patterns", repo_patterns)
                        if "project_keys" in managed_fields:
                            setattr(existing, "project_keys", project_keys)
                    setattr(existing, "is_active", True)
                    setattr(existing, "extra_data", extra_data)
                    setattr(existing, "last_drift_sync_at", datetime.now(timezone.utc))
                else:
                    session.add(
                        TeamMapping(
                            team_id=team_id,
                            name=team_name,
                            org_id=org_id,
                            description=team_description,
                            repo_patterns=repo_patterns,
                            project_keys=project_keys,
                            extra_data=extra_data,
                            is_active=True,
                        )
                    )
                _reconcile_team_member_identities(
                    session,
                    org_id=org_id,
                    team_id=team_id,
                    provider=provider,
                    members=members,
                    members_complete=members_complete,
                )
            session.flush()
            persisted_ids = set(
                session.execute(
                    select(TeamMapping.team_id).where(
                        TeamMapping.org_id == org_id,
                        TeamMapping.team_id.in_(expected_ids),
                    )
                ).scalars()
            )
            logging.info(
                "Bridged %d teams to PostgreSQL TeamMapping (org=%s).",
                len(persisted_ids),
                org_id,
            )
            return len(persisted_ids)
    except Exception as e:
        logging.error("Failed to bridge teams to PostgreSQL: %s", e)
        return 0


def register_commands(sync_subparsers: argparse._SubParsersAction) -> None:
    teams = sync_subparsers.add_parser(
        "teams",
        help="Sync teams from config, Jira, Jira Ops, GitHub, GitLab, Linear, MS Teams, or Synthetic.",
    )
    add_sink_arg(teams)
    teams.add_argument(
        "--provider",
        choices=[
            "config",
            "jira",
            "jira-ops",
            "synthetic",
            "linear",
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
    teams.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit successfully when no teams are found/generated (default: exit 1).",
    )
    teams.set_defaults(func=sync_teams)
