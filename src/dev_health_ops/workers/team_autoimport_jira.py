from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Coroutine, Sequence
from datetime import datetime, timezone
from threading import Thread
from typing import Any, Protocol, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.team_discovery import (
    TeamDiscoveryService,
)
from dev_health_ops.api.services.configuration.team_membership import (
    TeamMembershipService,
)
from dev_health_ops.credentials.resolver import jira_credentials_from_mapping
from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.identity import load_identity_resolver

_T = TypeVar("_T")


class _DimensionSink(Protocol):
    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]: ...
    def write_projects(self, rows: Sequence[ProjectRecord]) -> None: ...
    def write_members(self, rows: Sequence[MemberRecord]) -> None: ...
    def write_team_memberships(self, rows: Sequence[TeamMembershipRecord]) -> None: ...
    def write_team_project_ownership(
        self, rows: Sequence[TeamProjectOwnershipRecord]
    ) -> None: ...
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
    dsn = str(scope.get("analytics_db") or os.getenv("CLICKHOUSE_URI") or "")
    if not dsn:
        raise ValueError("ClickHouse DSN is required for Jira team auto-import")
    return cast(_DimensionSink, ClickHouseMetricsSink(dsn=dsn)), True


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


def _load_jira_legacy_links(
    sink: _DimensionSink,
    *,
    org_id: str,
) -> list[dict[str, Any]]:
    try:
        return sink.query_dicts(
            """
            SELECT project_key, ops_team_id, project_name, ops_team_name, updated_at
            FROM jira_project_ops_team_links FINAL
            WHERE org_id = {org_id:String}
            """,
            {"org_id": org_id},
        )
    except Exception:
        return []


def populate(
    *,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    jira_credentials = jira_credentials_from_mapping(credentials)
    if jira_credentials is None:
        return {
            "status": "skipped",
            "reason": "missing_jira_credentials",
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
        }

    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(None, org_id)
    membership = TeamMembershipService(cast(AsyncSession, None), org_id)
    try:
        teams = _run(
            discovery.discover_jira(
                email=jira_credentials.email,
                api_token=jira_credentials.api_token,
                url=jira_credentials.base_url,
            )
        )
    except Exception as exc:
        # Mirror github/gitlab: a discovery failure (e.g. HTTP 403) skips the
        # import INTERNALLY without writing anything, so a manual ownership row
        # is never clobbered and the sync stays successful (CHAOS-2609).
        logging.getLogger(__name__).info(
            "Skipping Jira team auto-import for org_id=%s: discovery failed: %s",
            org_id,
            exc,
        )
        return {
            "status": "skipped",
            "reason": "provider_discovery_skipped",
            "teams_imported": 0,
            "projects_imported": 0,
            "members_imported": 0,
            "team_memberships_imported": 0,
            "team_project_ownership_imported": 0,
            "team_repo_ownership_imported": 0,
            "work_item_team_attributions_imported": 0,
        }

    # Same alias map the assignee/compute path uses (load_identity_resolver in
    # job_work_items / providers.base), so an aliased member resolves to the SAME
    # canonical identity an aliased assignee does (CHAOS-2609).
    resolver = load_identity_resolver()

    team_rows: list[dict[str, Any]] = []
    project_rows: list[ProjectRecord] = []
    member_rows: list[MemberRecord] = []
    membership_rows: list[TeamMembershipRecord] = []
    ownership_rows: list[TeamProjectOwnershipRecord] = []

    for team in teams:
        team_id = str(team.provider_team_id)
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
                "provider": "jira",
                "native_team_key": team_id,
                "parent_team_id": None,
            }
        )

        for project_key in project_keys:
            project_rows.append(
                ProjectRecord(
                    id=_project_id(org_id, "jira", project_key),
                    org_id=org_id,
                    provider="jira",
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
                    provider="jira",
                    team_id=team_id,
                    project_id=_project_id(org_id, "jira", project_key),
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
            membership.discover_members_jira_bulk(
                email=jira_credentials.email,
                api_token=jira_credentials.api_token,
                url=jira_credentials.base_url,
                project_keys=project_keys,
            )
        )
        # Resolve each member through the SAME org alias map an assignee uses:
        # facets[0] is the alias-resolved identity (canonical email when the
        # accountId is aliased, else jira:accountid:<id>) — the facet a no-email
        # assignee resolves to. raw_provider_user_id stores it (member_id PK keeps
        # the jira:<id> form); the roster carries all facets so BOTH attribution
        # paths match aliased and non-aliased members (CHAOS-2609).
        roster_facets: list[str] = []
        for member in discovered_members:
            member_id = _member_id("jira", member.provider_identity)
            facets = resolver.membership_facets(
                provider="jira",
                account_id=member.provider_identity,
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
                        "jira", member.provider_identity
                    ),
                    is_active=1,
                    updated_at=now,
                )
            )
            membership_rows.append(
                TeamMembershipRecord(
                    org_id=org_id,
                    provider="jira",
                    team_id=team_id,
                    member_id=member_id,
                    raw_provider_user_id=facets[0],
                    raw_email=member.email,
                    source="native",
                    is_primary=1 if member.role == "lead" else 0,
                    specificity=100,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            )
        team_rows[-1]["members"] = roster_facets

    sink, should_close = _sink_from_kwargs(scope, kwargs)
    try:
        for link in _load_jira_legacy_links(sink, org_id=org_id):
            project_key = str(link.get("project_key") or "")
            ops_team_id = str(link.get("ops_team_id") or "")
            if not project_key or not ops_team_id:
                continue
            project_id = _project_id(org_id, "jira", project_key)
            if not any(row.id == project_id for row in project_rows):
                project_rows.append(
                    ProjectRecord(
                        id=project_id,
                        org_id=org_id,
                        provider="jira",
                        project_key=project_key,
                        name=str(link.get("project_name") or project_key),
                        is_active=1,
                        updated_at=now,
                        last_synced=now,
                    )
                )
            ownership_rows.append(
                TeamProjectOwnershipRecord(
                    org_id=org_id,
                    provider="jira",
                    team_id=ops_team_id,
                    project_id=project_id,
                    project_key=project_key,
                    source="jira_legacy",
                    is_primary=1,
                    specificity=90,
                    priority=20,
                    valid_from=now,
                    updated_at=now,
                )
            )

        _run(sink.insert_teams(team_rows))
        projects = _dedupe_projects(project_rows)
        members = _dedupe_members(member_rows)
        memberships = _dedupe_memberships(membership_rows)
        ownership = _dedupe_ownership(ownership_rows)
        sink.write_projects(projects)
        sink.write_members(members)
        sink.write_team_memberships(memberships)
        sink.write_team_project_ownership(ownership)
    finally:
        if should_close:
            sink.close()

    jira_legacy_count = sum(1 for row in ownership if row.source == "jira_legacy")
    return {
        "mode": scope.get("mode"),
        "teams_imported": len(team_rows),
        "projects_imported": len(projects),
        "members_imported": len(members),
        "team_memberships_imported": len(memberships),
        "team_project_ownership_imported": len(ownership),
        "jira_legacy_project_ownership_imported": jira_legacy_count,
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }
