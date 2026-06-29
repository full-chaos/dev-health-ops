from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Coroutine, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from threading import Thread
from typing import Any, Protocol, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

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
from dev_health_ops.credentials.resolver import linear_credentials_from_mapping
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.work_items import Sprint
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.linear.client import LinearAuth, LinearClient
from dev_health_ops.providers.linear.normalize import linear_cycle_to_sprint

_T = TypeVar("_T")
_REAL_CLICKHOUSE_SINK_TYPE = ClickHouseMetricsSink


class _DimensionSink(Protocol):
    def write_projects(self, rows: Sequence[ProjectRecord]) -> None: ...
    def write_members(self, rows: Sequence[MemberRecord]) -> None: ...
    def write_team_memberships(self, rows: Sequence[TeamMembershipRecord]) -> None: ...
    def write_team_project_ownership(
        self, rows: Sequence[TeamProjectOwnershipRecord]
    ) -> None: ...
    def write_sprints(self, rows: Sequence[Sprint]) -> None: ...
    async def insert_teams(self, teams: list[dict[str, Any]]) -> None: ...
    def close(self) -> None: ...


def _run(coro: Coroutine[Any, Any, _T]) -> _T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: _T | None = None
    error: BaseException | None = None

    def _target() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(coro)
        except BaseException as exc:
            error = exc

    thread = Thread(target=_target)
    thread.start()
    thread.join()
    if error is not None:
        raise error
    return cast(_T, result)


def _sink_from_kwargs(
    scope: dict[str, Any], kwargs: dict[str, Any]
) -> tuple[_DimensionSink, bool]:
    injected = kwargs.get("sink")
    if injected is not None:
        return cast(_DimensionSink, injected), False
    return cast(_DimensionSink, ClickHouseMetricsSink(dsn=_clickhouse_dsn(scope))), True


def _clickhouse_dsn(scope: dict[str, Any]) -> str:
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("ClickHouse DSN is required for Linear team auto-import")
    return dsn


def _team_store_from_kwargs(sink: _DimensionSink, kwargs: dict[str, Any]) -> Any | None:
    return kwargs.get("team_store") or (
        sink if hasattr(sink, "insert_team_provider_observations") else None
    )


def _team_id(team: Any) -> str:
    return str(team.provider_team_id)


def _project_id(org_id: str, provider: str, project_key: str) -> str:
    return f"{org_id}:{provider}:{project_key}"


def _member_id(provider: str, provider_identity: str) -> str:
    return f"{provider}:{provider_identity.strip().lower()}"


def _provider_identities(provider: str, provider_identity: str) -> str:
    return json.dumps({provider: [provider_identity]}, sort_keys=True)


def _dedupe_projects(rows: list[ProjectRecord]) -> list[ProjectRecord]:
    return list({(row.org_id, row.provider, row.id): row for row in rows}.values())


def _dedupe_members(rows: list[MemberRecord]) -> list[MemberRecord]:
    return list({(row.org_id, row.member_id): row for row in rows}.values())


def _dedupe_memberships(
    rows: list[TeamMembershipRecord],
) -> list[TeamMembershipRecord]:
    return list(
        {
            (row.org_id, row.provider, row.team_id, row.member_id, row.source): row
            for row in rows
        }.values()
    )


def _dedupe_ownership(
    rows: list[TeamProjectOwnershipRecord],
) -> list[TeamProjectOwnershipRecord]:
    return list(
        {
            (row.org_id, row.provider, row.project_id, row.team_id, row.source): row
            for row in rows
        }.values()
    )


def populate(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    strict = bool(scope.get("strict_reference_discovery"))
    linear_credentials = linear_credentials_from_mapping(credentials)
    if linear_credentials is None:
        if strict:
            raise ValueError(
                "missing Linear credentials for strict reference discovery"
            )
        return {
            "status": "skipped",
            "reason": "missing_linear_credentials",
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }

    now = datetime.now(timezone.utc)
    resolver = load_identity_resolver()
    discovery = TeamDiscoveryService(None, org_id)
    membership = TeamMembershipService(cast(AsyncSession, None), org_id)
    teams = _run(discovery.discover_linear(api_key=linear_credentials.api_key))

    team_rows: list[dict[str, Any]] = []
    project_rows: list[ProjectRecord] = []
    member_rows: list[MemberRecord] = []
    membership_rows: list[TeamMembershipRecord] = []
    ownership_rows: list[TeamProjectOwnershipRecord] = []
    sprint_rows: list[Sprint] = []

    for team in teams:
        team_id = _team_id(team)
        associations = dict(team.associations or {})
        project_keys = [str(key) for key in associations.get("project_keys", []) if key]
        if not project_keys:
            project_keys = [team_id]

        team_rows.append(
            {
                "id": team_id,
                "name": team.name,
                "description": team.description,
                "members": [],
                "project_keys": project_keys,
                "repo_patterns": [],
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": "linear",
                "native_team_key": team_id,
                "parent_team_id": None,
            }
        )

        for project_key in project_keys:
            project_rows.append(
                ProjectRecord(
                    id=_project_id(org_id, "linear", project_key),
                    org_id=org_id,
                    provider="linear",
                    project_key=project_key,
                    name=team.name,
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            )
            ownership_rows.append(
                TeamProjectOwnershipRecord(
                    org_id=org_id,
                    provider="linear",
                    team_id=team_id,
                    project_id=_project_id(org_id, "linear", project_key),
                    project_key=project_key,
                    source="native",
                    is_primary=1,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )

        discovered_members = _run(
            membership.discover_members_linear(
                api_key=linear_credentials.api_key,
                team_key=team_id,
            )
        )
        roster_facets: list[str] = []
        for member in discovered_members:
            member_id = _member_id("linear", member.provider_identity)
            facets = resolver.membership_facets(
                provider="linear",
                username=member.provider_identity,
                email=member.email,
            ) or [member.provider_identity]
            for facet in facets:
                if facet not in roster_facets:
                    roster_facets.append(facet)
            member_rows.append(
                MemberRecord(
                    org_id=org_id,
                    member_id=member_id,
                    name=member.display_name or member.provider_identity,
                    email=member.email,
                    provider_identities=_provider_identities(
                        "linear", member.provider_identity
                    ),
                    is_active=1,
                    updated_at=now,
                )
            )
            membership_rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider="linear",
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=member.email,
                    identity_facets=facets,
                    source="native",
                    is_primary=1,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )
        team_rows[-1]["members"] = roster_facets

    try:
        with LinearClient(
            auth=LinearAuth(api_key=linear_credentials.api_key), org_id=org_id
        ) as client:
            for team in teams:
                api_team = client.get_team_by_key(_team_id(team))
                if not api_team or not api_team.get("id"):
                    continue
                for cycle in client.iter_cycles(team_id=str(api_team["id"])):
                    sprint_rows.append(
                        replace(linear_cycle_to_sprint(cycle), org_id=org_id)
                    )
    except Exception:
        if strict:
            raise
        sprint_rows = []

    sink, should_close = _sink_from_kwargs(scope, kwargs)
    try:
        team_store = _team_store_from_kwargs(sink, kwargs)
        if team_store is not None:
            _run(
                project_team_rows_with_store(
                    store=team_store,
                    org_id=org_id,
                    provider="linear",
                    team_rows=team_rows,
                    team_writer=sink.insert_teams,
                    discovered_at=now,
                )
            )
        elif isinstance(sink, _REAL_CLICKHOUSE_SINK_TYPE):
            _run(
                project_provider_team_rows(
                    dsn=_clickhouse_dsn(scope),
                    org_id=org_id,
                    provider="linear",
                    team_rows=team_rows,
                    team_writer=sink.insert_teams,
                    discovered_at=now,
                )
            )
        else:
            _run(sink.insert_teams(team_rows))
        projects = _dedupe_projects(project_rows)
        members = _dedupe_members(member_rows)
        memberships = _dedupe_memberships(membership_rows)
        ownership = _dedupe_ownership(ownership_rows)
        sink.write_projects(projects)
        sink.write_members(members)
        sink.write_team_memberships(memberships)
        sink.write_team_project_ownership(ownership)
        if hasattr(sink, "write_sprints"):
            sink.write_sprints(sprint_rows)
    finally:
        if should_close:
            sink.close()

    return {
        "mode": scope.get("mode"),
        "teams_imported": len(team_rows),
        "reference_team_keys": [str(row["native_team_key"]) for row in team_rows],
        "reference_sprint_ids": [str(row.sprint_id) for row in sprint_rows],
        "projects_imported": len(projects),
        "members_imported": len(members),
        "team_memberships_imported": len(memberships),
        "team_project_ownership_imported": len(ownership),
        "sprints_imported": len(sprint_rows),
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }
