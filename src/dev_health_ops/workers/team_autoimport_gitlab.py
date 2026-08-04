from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.api.services.configuration.clickhouse_identity_drift import (
    split_memberships_for_review,
)
from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    project_provider_team_rows,
    project_team_rows_with_store,
)
from dev_health_ops.api.services.configuration.team_discovery import (
    TeamDiscoveryService,
)
from dev_health_ops.api.services.configuration.team_membership import (
    TeamMembershipService,
)
from dev_health_ops.metrics.schemas import (
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.gitlab.client import GitLabAuth, GitLabWorkClient
from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.team_capabilities import team_provider_capabilities
from dev_health_ops.storage.clickhouse import ClickHouseStore

logger = logging.getLogger(__name__)

PROVIDER = "gitlab"
PROVIDER_ACCESS_PRIORITY = 300
BASE_SPECIFICITY = 100
CHILD_SPECIFICITY_STEP = 10
DEFAULT_GITLAB_URL = "https://gitlab.com"
_REAL_CLICKHOUSE_SINK_TYPE = ClickHouseMetricsSink


def populate(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    return asyncio.run(
        _populate_async(
            org_id=org_id,
            credentials=credentials,
            scope=scope,
            team_store=kwargs.get("team_store"),
        )
    )


async def _populate_async(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    team_store: Any | None = None,
) -> dict[str, Any]:
    strict = bool(scope.get("strict_reference_discovery"))
    if not _provider_capable():
        if strict:
            raise ValueError(
                "GitLab is not import-capable for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="provider_not_import_capable")

    token = _first_string(credentials, "token", "access_token", "private_token")
    group_path = _gitlab_group(credentials=credentials, scope=scope)
    url = (
        _first_string(credentials, "url", "base_url", "gitlab_url")
        or DEFAULT_GITLAB_URL
    )
    if not token or not group_path:
        if strict:
            raise ValueError(
                "missing GitLab credentials or group for strict reference discovery"
            )
        return _zero_summary(
            org_id=org_id, reason="missing_gitlab_credentials_or_group"
        )

    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    try:
        result = await discovery.discover_gitlab(
            token=token,
            group_path=group_path,
            url=url,
        )
    except Exception as exc:
        if strict:
            raise
        logger.info(
            "Skipping GitLab team auto-import for org_id=%s group=%s: discovery failed: %s",
            org_id,
            group_path,
            exc,
        )
        return _zero_summary(org_id=org_id, reason="provider_discovery_skipped")

    teams = result.teams
    if not teams:
        return _zero_summary(org_id=org_id, reason="no_provider_teams")

    now = datetime.now(timezone.utc)
    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()
    team_rows = _team_rows(org_id=org_id, teams=teams, now=now)
    project_rows = _project_ownership_rows(org_id=org_id, teams=teams, now=now)
    # CHAOS-3380: import GitLab PROJECTS as their own catalog rows -- mirrors
    # Linear's CHAOS-3365 native project import -- so a GitLab project name
    # resolves as an Ask Dev subject. Kept independent of the ownership rows
    # above: a metadata-fetch failure for one project must not discard team
    # ownership, and vice versa.
    native_project_rows: list[ProjectRecord] = []
    native_projects_complete = True
    project_paths = _unique_project_paths(teams)
    if project_paths:
        try:
            native_project_rows, native_projects_complete = await asyncio.to_thread(
                _gitlab_project_records,
                org_id=org_id,
                token=token,
                url=url,
                project_paths=project_paths,
                now=now,
                strict=strict,
            )
        except Exception as exc:
            if strict:
                raise
            logger.warning(
                "GitLab project catalog import failed for org_id=%s group=%s: %s",
                org_id,
                group_path,
                exc,
                exc_info=True,
            )
            native_project_rows = []
            native_projects_complete = False
    membership_rows, _, observed_team_ids = await _membership_rows(
        org_id=org_id,
        token=token,
        url=url,
        teams=teams,
        now=now,
        resolver=resolver,
        strict=strict,
    )
    sink = _sink(scope)
    membership_rows = await _split_memberships_for_review(
        org_id=org_id,
        rows=membership_rows,
        observed_team_ids=tuple(observed_team_ids),
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    member_roster = _roster_from_memberships(membership_rows)
    for team_row in team_rows:
        team_row["members"] = member_roster.get(str(team_row["id"]), [])

    await _project_team_rows(
        org_id=org_id,
        team_rows=team_rows,
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    sink.write_team_project_ownership(project_rows)
    sink.write_team_memberships(membership_rows)
    catalog_projects = _dedupe_projects(native_project_rows)
    if hasattr(sink, "write_projects"):
        sink.write_projects(catalog_projects)

    summary: dict[str, Any] = {
        "teams_imported": len(team_rows),
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [],
        "projects_imported": len({row.project_id for row in project_rows}),
        # The Ask Dev subject catalog rows written via write_projects(), distinct
        # from "projects_imported" above (team-ownership project paths).
        "native_projects_imported": len(catalog_projects),
        # False when GitLab project metadata fetch failed for at least one
        # discovered project path -- so a partial catalog is never recorded as
        # a full one (mirrors Linear's native_projects_complete).
        "native_projects_complete": native_projects_complete,
        "members_imported": len({row.member_id for row in membership_rows}),
        "team_memberships_imported": len(membership_rows),
        "team_project_ownership_imported": len(project_rows),
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }
    if result.truncated:
        summary["warnings"] = list(result.warnings)
    return summary


def _provider_capable() -> bool:
    return any(
        capability.provider == PROVIDER and capability.supports_org_drift_discovery
        for capability in team_provider_capabilities()
    )


def _zero_summary(*, org_id: str, reason: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "provider": PROVIDER,
        "org_id": org_id,
        "reason": reason,
        "teams_imported": 0,
        "projects_imported": 0,
        "members_imported": 0,
        "team_memberships_imported": 0,
        "team_project_ownership_imported": 0,
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }


def _gitlab_group(
    *, credentials: Mapping[str, Any], scope: Mapping[str, Any]
) -> str | None:
    sync_options = _mapping(scope.get("sync_options"))
    return _first_string(
        credentials,
        "group_path",
        "group",
        "owner",
    ) or _first_string(sync_options, "group_path", "group", "owner")


def _team_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        provider_team_id = str(getattr(team, "provider_team_id"))
        team_id = _team_id(provider_team_id)
        rows.append(
            {
                "id": team_id,
                "name": str(getattr(team, "name", team_id)),
                "description": getattr(team, "description", None),
                "members": [],
                "project_keys": _strings(associations.get("repo_patterns")),
                "repo_patterns": [],
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": PROVIDER,
                "native_team_key": provider_team_id,
                "parent_team_id": _parent_team_id(provider_team_id, associations),
            }
        )
    return rows


def _project_ownership_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[TeamProjectOwnershipRecord]:
    parent_by_team = _parent_by_team(teams)
    rows: list[TeamProjectOwnershipRecord] = []
    seen: set[tuple[str, str]] = set()
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        provider_team_id = str(getattr(team, "provider_team_id"))
        team_id = _team_id(provider_team_id)
        specificity = BASE_SPECIFICITY + (
            _depth(team_id, parent_by_team) * CHILD_SPECIFICITY_STEP
        )
        for project_path in _strings(associations.get("repo_patterns")):
            key = (team_id, project_path)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                TeamProjectOwnershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    project_id=project_path,
                    project_key=project_path,
                    source="provider_access",
                    is_primary=0,
                    specificity=specificity,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows


def _unique_project_paths(teams: Iterable[Any]) -> list[str]:
    """The distinct GitLab project full paths discovered across every team.

    Each ``DiscoveredTeam`` from ``discover_gitlab`` carries its OWN group's
    projects in ``associations["repo_patterns"]`` (``team_discovery.
    discover_gitlab``: ``repo_patterns = [p.path_with_namespace for p in
    projects]``) -- the same walk that already backs
    ``_project_ownership_rows``. Reusing it here (rather than re-walking
    groups/subgroups a second time) means the project catalog covers exactly
    the projects team ownership already resolved, order-preserving so the
    metadata fetch below is deterministic across runs.
    """

    seen: set[str] = set()
    ordered: list[str] = []
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        for path in _strings(associations.get("repo_patterns")):
            if path not in seen:
                seen.add(path)
                ordered.append(path)
    return ordered


def _gitlab_project_records(
    *,
    org_id: str,
    token: str,
    url: str,
    project_paths: Iterable[str],
    now: datetime,
    strict: bool,
) -> tuple[list[ProjectRecord], bool]:
    """Fetch GitLab project metadata for the Ask Dev subject catalog.

    ``id`` is the RAW ``path_with_namespace`` requested -- deliberately the
    SAME string ``providers/gitlab/normalize.gitlab_issue_to_work_item`` and
    ``gitlab_mr_to_work_item`` write onto ``work_items.project_id``
    (normalize.py: "For work tracking metrics, treat the GitLab project path
    as the 'project' scope.") -- so a scope committed against one of these
    rows selects work items with no query change, exactly like Linear's
    raw-id catalog rows (``_linear_project_records``). The id is the path we
    ASKED for, not ``project.path_with_namespace`` off the response: a
    renamed project can still resolve its old path via GitLab's redirect,
    which would otherwise mint a catalog row under a path no work item was
    ever attributed to.

    ``project_key`` is always ``None``: GitLab work items never set
    ``project_key`` (normalize.py sets it to ``None`` for issues, merge
    requests, and epics alike) -- so the identity join
    (``native_status_change._project_identity_match``, CHAOS-3374) can only
    ever resolve a GitLab project through the raw ``project_id`` arm, never
    through the ``project_key`` arm.

    Only what GitLab's project resource genuinely exposes is mapped:
    ``archived`` retires the row (mirrors Linear's archived/trashed --
    CHAOS-3372's absence-based retirement remains explicitly out of scope,
    so a project that stops being discovered keeps its last-seen state
    rather than being tombstoned), and ``web_url`` is the URL. GitLab
    projects carry no lifecycle vocabulary beyond archived and no target
    date, so ``state``/``target_date`` are left at their empty defaults
    rather than inventing values.
    """

    client = GitLabWorkClient(auth=GitLabAuth(token=token, base_url=url), org_id=org_id)
    rows: list[ProjectRecord] = []
    complete = True
    for path in project_paths:
        try:
            project = client.get_project(path)
        except Exception as exc:
            complete = False
            if strict:
                raise
            logger.info(
                "Skipping GitLab project catalog row for org_id=%s path=%s: %s",
                org_id,
                path,
                exc,
            )
            continue
        archived = bool(getattr(project, "archived", False))
        rows.append(
            ProjectRecord(
                id=path,
                org_id=org_id,
                provider=PROVIDER,
                project_key=None,
                name=str(getattr(project, "name", None) or path),
                is_active=0 if archived else 1,
                updated_at=now,
                last_synced=now,
                url=str(getattr(project, "web_url", "") or ""),
            )
        )
    return rows, complete


def _dedupe_projects(rows: list[ProjectRecord]) -> list[ProjectRecord]:
    return list({(row.org_id, row.provider, row.id): row for row in rows}.values())


async def _membership_rows(
    *,
    org_id: str,
    token: str,
    url: str,
    teams: Iterable[Any],
    now: datetime,
    resolver: IdentityResolver,
    strict: bool,
) -> tuple[list[TeamMembershipRecord], dict[str, list[str]], set[tuple[str, str]]]:
    service = TeamMembershipService(session=cast(Any, None), org_id=org_id)
    rows: list[TeamMembershipRecord] = []
    roster: dict[str, list[str]] = {}
    observed_team_ids: set[tuple[str, str]] = set()
    seen: set[tuple[str, str]] = set()
    for team in teams:
        group_path = str(getattr(team, "provider_team_id"))
        team_id = _team_id(group_path)
        try:
            members = await service.discover_members_gitlab(
                token=token,
                group_path=group_path,
                url=url,
            )
        except Exception as exc:
            if strict:
                raise
            logger.info(
                "Skipping GitLab membership import for org_id=%s group=%s: %s",
                org_id,
                group_path,
                exc,
            )
            continue
        observed_team_ids.add((PROVIDER, team_id))
        for member in members:
            raw_identity = str(getattr(member, "provider_identity", "")).strip()
            if not raw_identity:
                continue
            member_id = f"gl:{raw_identity}"
            key = (team_id, member_id)
            if key in seen:
                continue
            seen.add(key)
            # Resolve the member through the SAME org alias map an assignee uses:
            # facets[0] is the alias-resolved identity (canonical email when the
            # gitlab:<username> is aliased, else gitlab:<username>) — the facet a
            # no-email assignee resolves to. It goes into raw_provider_user_id
            # (member_id is the PK and keeps the bare username), identity_facets,
            # AND the teams.members roster so BOTH attribution paths match
            # aliased and non-aliased members (CHAOS-2609, CHAOS-2625).
            facets = resolver.membership_facets(
                provider=PROVIDER,
                username=raw_identity,
                email=getattr(member, "email", None),
            ) or [raw_identity]
            roster_for_team = roster.setdefault(team_id, [])
            for facet in facets:
                if facet not in roster_for_team:
                    roster_for_team.append(facet)
            rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=getattr(member, "email", None),
                    identity_facets=facets,
                    source="provider_access",
                    is_primary=0,
                    specificity=BASE_SPECIFICITY,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows, roster, observed_team_ids


def _sink(scope: Mapping[str, Any]) -> ClickHouseMetricsSink:
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("CLICKHOUSE_URI is required for GitLab team auto-import")
    return ClickHouseMetricsSink(dsn=dsn)


def _roster_from_memberships(
    rows: Iterable[TeamMembershipRecord],
) -> dict[str, list[str]]:
    roster: dict[str, list[str]] = {}
    for row in rows:
        roster_for_team = roster.setdefault(str(row.team_id), [])
        values = tuple(row.identity_facets) or tuple(
            value for value in (row.raw_provider_user_id, row.raw_email) if value
        )
        for value in values:
            if value and value not in roster_for_team:
                roster_for_team.append(str(value))
    return roster


async def _project_team_rows(
    *,
    org_id: str,
    team_rows: list[dict[str, Any]],
    sink: ClickHouseMetricsSink,
    team_store: Any | None,
    discovered_at: datetime,
) -> None:
    if team_store is not None:
        await project_team_rows_with_store(
            store=team_store,
            org_id=org_id,
            provider="gitlab",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        await project_provider_team_rows(
            dsn=sink.dsn,
            org_id=org_id,
            provider="gitlab",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    await sink.insert_teams(team_rows)


async def _split_memberships_for_review(
    *,
    org_id: str,
    rows: list[TeamMembershipRecord],
    observed_team_ids: Sequence[tuple[str, str]],
    sink: Any,
    team_store: Any | None,
    discovered_at: datetime,
) -> list[TeamMembershipRecord]:
    if team_store is not None:
        return await split_memberships_for_review(
            store=team_store,
            org_id=org_id,
            rows=rows,
            observed_team_ids=observed_team_ids,
            discovered_at=discovered_at,
        )
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        async with ClickHouseStore(sink.dsn) as store:
            store.org_id = org_id
            return await split_memberships_for_review(
                store=store,
                org_id=org_id,
                rows=rows,
                observed_team_ids=observed_team_ids,
                discovered_at=discovered_at,
            )
    return rows


def _team_id(provider_team_id: str) -> str:
    return f"gl:{provider_team_id.removeprefix('gl:')}"


def _parent_team_id(
    provider_team_id: str, associations: Mapping[str, Any]
) -> str | None:
    explicit_parent = _first_string(
        associations, "parent_team_id", "parent_provider_team_id"
    )
    if explicit_parent:
        return _team_id(explicit_parent)
    if "/" not in provider_team_id:
        return None
    return _team_id(provider_team_id.rsplit("/", 1)[0])


def _parent_by_team(teams: Iterable[Any]) -> dict[str, str]:
    parents: dict[str, str] = {}
    team_ids = {_team_id(str(getattr(team, "provider_team_id"))) for team in teams}
    for team in teams:
        provider_team_id = str(getattr(team, "provider_team_id"))
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(provider_team_id)
        parent = _parent_team_id(provider_team_id, associations)
        if parent and parent in team_ids:
            parents[team_id] = parent
    return parents


def _depth(team_id: str, parent_by_team: Mapping[str, str]) -> int:
    depth = 0
    current = team_id
    visited: set[str] = set()
    while current in parent_by_team and current not in visited:
        visited.add(current)
        current = parent_by_team[current]
        depth += 1
    return depth


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if not isinstance(value, Iterable):
        return []
    return [str(item) for item in value if str(item).strip()]


def _first_string(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
