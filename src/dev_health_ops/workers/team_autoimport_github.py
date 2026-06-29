from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any, cast

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
from dev_health_ops.metrics.schemas import TeamMembershipRecord, TeamRepoOwnershipRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.team_capabilities import team_provider_capabilities

logger = logging.getLogger(__name__)

PROVIDER = "github"
PROVIDER_ACCESS_PRIORITY = 300
BASE_SPECIFICITY = 100
CHILD_SPECIFICITY_STEP = 10
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
                "GitHub is not import-capable for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="provider_not_import_capable")

    token = _first_string(credentials, "token", "access_token", "github_token")
    org_name = _github_org(credentials=credentials, scope=scope)
    if not token or not org_name:
        if strict:
            raise ValueError(
                "missing GitHub credentials or org for strict reference discovery"
            )
        return _zero_summary(org_id=org_id, reason="missing_github_credentials_or_org")

    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    try:
        teams = await discovery.discover_github(token=token, org_name=org_name)
    except Exception as exc:
        if strict:
            raise
        logger.info(
            "Skipping GitHub team auto-import for org_id=%s org=%s: discovery failed: %s",
            org_id,
            org_name,
            exc,
        )
        return _zero_summary(org_id=org_id, reason="provider_discovery_skipped")

    if not teams:
        return _zero_summary(org_id=org_id, reason="no_provider_teams")

    now = datetime.now(timezone.utc)
    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()
    team_rows = _team_rows(org_id=org_id, teams=teams, now=now)
    repo_rows = _repo_ownership_rows(org_id=org_id, teams=teams, now=now)
    membership_rows, member_roster = await _membership_rows(
        org_id=org_id,
        token=token,
        org_name=org_name,
        teams=teams,
        now=now,
        resolver=resolver,
        strict=strict,
    )
    for team_row in team_rows:
        team_row["members"] = member_roster.get(str(team_row["id"]), [])

    sink = _sink(scope)
    await _project_team_rows(
        org_id=org_id,
        team_rows=team_rows,
        sink=sink,
        team_store=team_store,
        discovered_at=now,
    )
    sink.write_team_repo_ownership(repo_rows)
    sink.write_team_memberships(membership_rows)

    return {
        "teams_imported": len(team_rows),
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [],
        "projects_imported": 0,
        "members_imported": len({row.member_id for row in membership_rows}),
        "team_memberships_imported": len(membership_rows),
        "team_project_ownership_imported": 0,
        "team_repo_ownership_imported": len(repo_rows),
        "work_item_team_attributions_imported": 0,
    }


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


def _github_org(
    *, credentials: Mapping[str, Any], scope: Mapping[str, Any]
) -> str | None:
    sync_options = _mapping(scope.get("sync_options"))
    return _first_string(
        credentials,
        "org",
        "organization",
        "org_name",
        "owner",
    ) or _first_string(sync_options, "org", "organization", "org_name", "owner")


def _team_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(str(getattr(team, "provider_team_id")))
        rows.append(
            {
                "id": team_id,
                "name": str(getattr(team, "name", team_id)),
                "description": getattr(team, "description", None),
                "members": [],
                "project_keys": [],
                "repo_patterns": _strings(associations.get("repo_patterns")),
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": PROVIDER,
                "native_team_key": str(getattr(team, "provider_team_id")),
                "parent_team_id": _parent_team_id(associations),
            }
        )
    return rows


def _repo_ownership_rows(
    *, org_id: str, teams: Iterable[Any], now: datetime
) -> list[TeamRepoOwnershipRecord]:
    parent_by_team = _parent_by_team(teams)
    rows: list[TeamRepoOwnershipRecord] = []
    seen: set[tuple[str, str]] = set()
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        team_id = _team_id(str(getattr(team, "provider_team_id")))
        specificity = BASE_SPECIFICITY + (
            _depth(team_id, parent_by_team) * CHILD_SPECIFICITY_STEP
        )
        for repo_full_name in _strings(associations.get("repo_patterns")):
            key = (team_id, repo_full_name)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                TeamRepoOwnershipRecord(
                    org_id=org_id,
                    provider=PROVIDER,
                    team_id=team_id,
                    repo_full_name=repo_full_name,
                    match_type="exact",
                    source="provider_access",
                    is_primary=0,
                    specificity=specificity,
                    priority=PROVIDER_ACCESS_PRIORITY,
                    valid_from=now,
                    updated_at=now,
                )
            )
    return rows


async def _membership_rows(
    *,
    org_id: str,
    token: str,
    org_name: str,
    teams: Iterable[Any],
    now: datetime,
    resolver: IdentityResolver,
    strict: bool,
) -> tuple[list[TeamMembershipRecord], dict[str, list[str]]]:
    service = TeamMembershipService(session=cast(Any, None), org_id=org_id)
    rows: list[TeamMembershipRecord] = []
    roster: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for team in teams:
        team_slug = str(getattr(team, "provider_team_id"))
        team_id = _team_id(team_slug)
        try:
            members = await service.discover_members_github(
                token=token,
                org_name=org_name,
                team_slug=team_slug,
            )
        except Exception as exc:
            if strict:
                raise
            logger.info(
                "Skipping GitHub membership import for org_id=%s team=%s: %s",
                org_id,
                team_slug,
                exc,
            )
            continue
        for member in members:
            raw_identity = str(getattr(member, "provider_identity", "")).strip()
            if not raw_identity:
                continue
            member_id = f"gh:{raw_identity}"
            key = (team_id, member_id)
            if key in seen:
                continue
            seen.add(key)
            # Resolve the member through the SAME org alias map an assignee uses:
            # facets[0] is the alias-resolved identity (canonical email when the
            # github:<login> is aliased, else github:<login>) — the facet a
            # no-email assignee resolves to. It goes into raw_provider_user_id
            # (member_id is the PK and keeps the bare login), identity_facets,
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
    return rows, roster


def _sink(scope: Mapping[str, Any]) -> ClickHouseMetricsSink:
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("CLICKHOUSE_URI is required for GitHub team auto-import")
    return ClickHouseMetricsSink(dsn=dsn)


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
            provider="github",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    if isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
        await project_provider_team_rows(
            dsn=sink.dsn,
            org_id=org_id,
            provider="github",
            team_rows=team_rows,
            team_writer=sink.insert_teams,
            discovered_at=discovered_at,
        )
        return
    await sink.insert_teams(team_rows)


def _team_id(provider_team_id: str) -> str:
    return f"gh:{provider_team_id.removeprefix('gh:')}"


def _parent_team_id(associations: Mapping[str, Any]) -> str | None:
    parent = _first_string(associations, "parent_team_id", "parent_provider_team_id")
    if parent is None:
        return None
    return _team_id(parent)


def _parent_by_team(teams: Iterable[Any]) -> dict[str, str]:
    parents: dict[str, str] = {}
    for team in teams:
        associations = _mapping(getattr(team, "associations", None))
        parent = _parent_team_id(associations)
        if parent:
            parents[_team_id(str(getattr(team, "provider_team_id")))] = parent
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
