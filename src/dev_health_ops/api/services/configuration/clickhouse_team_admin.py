"""ClickHouse-backed team admin service.

ClickHouse is the system of record for the team catalog (CHAOS-2600 CS5).
This service replaces the Postgres ``TeamMappingService`` for the admin
team surface: the admin team CRUD endpoints read and write the ClickHouse
``teams`` table directly, with no Postgres ``TeamMapping`` projection.

The ``teams`` table is a ``ReplacingMergeTree(updated_at)`` keyed on
``(org_id, id)``: a create/update is a row insert with a fresh
``updated_at`` (the latest row wins under ``FINAL``); a delete is a
ClickHouse lightweight ``DELETE``.

Admin-created teams carry ``provider=""`` and ``native_team_key=None`` —
they are not owned by any provider sync, so a later drift/auto-import run
will not silently reclaim or overwrite them by native key.

The wire shape (:class:`TeamMappingResponse`) is preserved. Fields that
only existed on the Postgres ``TeamMapping`` model and have no ClickHouse
counterpart are surfaced with stable defaults:

* ``id`` -> the team's ``team_uuid`` (string)
* ``team_id`` -> the team's ClickHouse ``id`` (the slug)
* ``extra_data`` -> ``{}``; ``managed_fields`` -> ``[]``
* ``sync_policy`` -> ``2`` (manual; admin teams are not drift-managed)
* ``flagged_changes`` / ``last_drift_sync_at`` -> ``None``
* ``created_at`` -> mirrors ``updated_at`` (ClickHouse keeps no insert ts)
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    ClickHouseTeamDriftProjector,
)

if TYPE_CHECKING:
    from dev_health_ops.storage.clickhouse import ClickHouseStore

# Columns selected for the admin surface (superset of get_all_teams()).
_TEAM_COLUMNS = (
    "id",
    "team_uuid",
    "name",
    "description",
    "members",
    "project_keys",
    "repo_patterns",
    "is_active",
    "updated_at",
    "org_id",
)

_TEAM_SELECT = ", ".join(_TEAM_COLUMNS)


def _as_str_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None]
    return []


def _resolve_list_field(
    provided: list[str] | None,
    existing: ClickHouseTeam | None,
    field: str,
) -> list[str]:
    """Resolve a list field: explicit value wins, else keep existing, else []."""
    if provided is not None:
        return [str(item) for item in provided if item is not None]
    if existing is not None:
        return list(getattr(existing, field) or [])
    return []


def member_facets(
    *,
    canonical_id: str | None = None,
    email: str | None = None,
    display_name: str | None = None,
    provider_identities: dict[str, list[str]] | None = None,
) -> set[str]:
    """Compute the member-identity facets stored in a CH team's ``members``.

    Mirrors ``team_member_resolver.members_by_team``: every confirmed facet
    (email, canonical_id, all provider identities) is included so the
    membership-based attribution fallback can match work-item assignees.
    ``display_name`` is only added when no email exists (avoids false
    positives on common names).
    """
    facets: set[str] = set()
    if email:
        facets.add(str(email))
    if canonical_id:
        facets.add(str(canonical_id))
    for values in (provider_identities or {}).values():
        if isinstance(values, list):
            facets.update(str(v) for v in values if v)
        elif values:
            facets.add(str(values))
    if not email and display_name:
        facets.add(str(display_name))
    return facets


class ClickHouseTeam:
    """Lightweight row view exposing the attributes the response mapper reads.

    Mirrors the attribute surface the admin router's ``_team_mapping_response``
    expects from a Postgres ``TeamMapping`` so the wire shape stays stable.
    """

    def __init__(
        self,
        *,
        team_id: str,
        team_uuid: uuid.UUID,
        name: str,
        description: str | None,
        members: list[str],
        project_keys: list[str],
        repo_patterns: list[str],
        is_active: bool,
        updated_at: datetime,
        org_id: str,
    ) -> None:
        # ``id`` is the Postgres-style surrogate (the team_uuid); ``team_id``
        # is the ClickHouse slug. This matches the TeamMapping response shape.
        self.id = str(team_uuid)
        self.team_id = team_id
        self.team_uuid = team_uuid
        self.name = name
        self.description = description
        self.members = members
        self.project_keys = project_keys
        self.repo_patterns = repo_patterns
        self.is_active = is_active
        self.updated_at = updated_at
        self.created_at = updated_at
        self.org_id = org_id
        # Drift-only Postgres fields with no ClickHouse counterpart.
        self.extra_data: dict[str, Any] = {}
        self.managed_fields: list[str] = []
        self.sync_policy = 2
        self.flagged_changes: dict[str, Any] | None = None
        self.last_drift_sync_at: datetime | None = None


class ClickHouseTeamAdminService:
    """CRUD for the ClickHouse team catalog used by the admin surface."""

    def __init__(self, store: ClickHouseStore, org_id: str) -> None:
        self.store = store
        self.org_id = org_id

    def _row_to_team(self, row: Any) -> ClickHouseTeam:
        team_uuid = row[1]
        if not isinstance(team_uuid, uuid.UUID):
            team_uuid = uuid.UUID(str(team_uuid))
        updated_at = row[8]
        if not isinstance(updated_at, datetime):
            updated_at = datetime.now(timezone.utc)
        return ClickHouseTeam(
            team_id=str(row[0]),
            team_uuid=team_uuid,
            name=str(row[2]),
            description=row[3],
            members=_as_str_list(row[4]),
            project_keys=_as_str_list(row[5]),
            repo_patterns=_as_str_list(row[6]),
            is_active=bool(row[7]),
            updated_at=updated_at,
            org_id=str(row[9] or ""),
        )

    async def _query_teams(
        self, *, team_id: str | None, active_only: bool
    ) -> list[ClickHouseTeam]:
        client = self.store.client
        assert client is not None
        conditions = ["org_id = {org_id:String}"]
        params: dict[str, Any] = {"org_id": self.org_id}
        if team_id is not None:
            conditions.append("id = {team_id:String}")
            params["team_id"] = team_id
        if active_only:
            conditions.append("is_active = 1")
        where = " AND ".join(conditions)
        query = f"SELECT {_TEAM_SELECT} FROM teams FINAL WHERE {where}"
        async with self.store._lock:
            result = await asyncio.to_thread(client.query, query, parameters=params)
        return [self._row_to_team(row) for row in (result.result_rows or [])]

    async def list_all(self, active_only: bool = True) -> list[ClickHouseTeam]:
        return await self._query_teams(team_id=None, active_only=active_only)

    async def get(self, team_id: str) -> ClickHouseTeam | None:
        # ``get`` must surface soft-deleted/inactive rows too so callers can
        # distinguish "missing" from "inactive"; active filtering is only for
        # the list view.
        teams = await self._query_teams(team_id=team_id, active_only=False)
        return teams[0] if teams else None

    async def create_or_update(
        self,
        team_id: str,
        name: str,
        description: str | None = None,
        repo_patterns: list[str] | None = None,
        project_keys: list[str] | None = None,
        members: list[str] | None = None,
    ) -> ClickHouseTeam:
        existing = await self.get(team_id)
        team_uuid = (
            existing.team_uuid
            if existing is not None
            else uuid.uuid5(uuid.NAMESPACE_URL, f"team:{self.org_id}:{team_id}")
        )
        resolved_members = (
            members
            if members is not None
            else (existing.members if existing is not None else [])
        )
        resolved_projects = _resolve_list_field(project_keys, existing, "project_keys")
        resolved_repos = _resolve_list_field(repo_patterns, existing, "repo_patterns")
        now = datetime.now(timezone.utc)
        await self.store.insert_teams(
            [
                {
                    "id": team_id,
                    "team_uuid": team_uuid,
                    "name": name,
                    "description": description,
                    "members": resolved_members,
                    "project_keys": resolved_projects,
                    "repo_patterns": resolved_repos,
                    "is_active": 1,
                    "org_id": self.org_id,
                    "provider": "",
                    "native_team_key": None,
                    "updated_at": now,
                }
            ]
        )
        return ClickHouseTeam(
            team_id=team_id,
            team_uuid=team_uuid,
            name=name,
            description=description,
            members=_as_str_list(resolved_members),
            project_keys=resolved_projects,
            repo_patterns=resolved_repos,
            is_active=True,
            updated_at=now,
            org_id=self.org_id,
        )

    async def set_members(
        self, team_id: str, members: list[str]
    ) -> ClickHouseTeam | None:
        """Replace a team's member list in ClickHouse, preserving other fields."""
        existing = await self.get(team_id)
        if existing is None:
            return None
        return await self.create_or_update(
            team_id=team_id,
            name=existing.name,
            description=existing.description,
            repo_patterns=existing.repo_patterns,
            project_keys=existing.project_keys,
            members=sorted({str(m) for m in members if m}),
        )

    async def add_members(
        self, team_id: str, members: list[str]
    ) -> ClickHouseTeam | None:
        """Union new members into a team's member list in ClickHouse."""
        existing = await self.get(team_id)
        if existing is None:
            return None
        merged = sorted({*existing.members, *(str(m) for m in members if m)})
        return await self.set_members(team_id, merged)

    async def remove_members(
        self, team_id: str, facets: set[str]
    ) -> ClickHouseTeam | None:
        """Surgically drop the given facets from a team's member list.

        Edits ``members`` in place (does NOT recompute from scratch), so
        Auto Import / team-catalog members not in ``facets`` are preserved.
        """
        existing = await self.get(team_id)
        if existing is None:
            return None
        return await self.set_members(
            team_id, [m for m in existing.members if m not in facets]
        )

    async def delete(self, team_id: str) -> bool:
        existing = await self.get(team_id)
        if existing is None:
            return False
        client = self.store.client
        assert client is not None
        async with self.store._lock:
            await asyncio.to_thread(
                client.command,
                "DELETE FROM teams WHERE org_id = {org_id:String} "
                "AND id = {team_id:String}",
                parameters={"org_id": self.org_id, "team_id": team_id},
            )
        return True

    async def import_teams(
        self,
        teams: list[Any],
        on_conflict: str = "skip",
    ) -> dict[str, Any]:
        """Import discovered teams into the ClickHouse team catalog.

        Mirrors the prior Postgres ``TeamDiscoveryService.import_teams`` shape
        (imported / skipped / merged / details) but writes ClickHouse directly.
        ``teams`` are :class:`DiscoveredTeam`-shaped objects.
        """
        imported = 0
        skipped = 0
        merged = 0
        details: list[dict[str, Any]] = []
        projector = ClickHouseTeamDriftProjector(store=self.store, org_id=self.org_id)

        for team in teams:
            provider_type = getattr(team, "provider_type", "")
            provider_team_id = str(getattr(team, "provider_team_id", ""))
            if provider_type == "github":
                team_id = f"gh:{provider_team_id}"
            elif provider_type == "gitlab":
                team_id = f"gl:{provider_team_id}"
            elif provider_type == "ms-teams":
                team_id = f"ms-teams:{provider_team_id}"
            else:
                team_id = provider_team_id

            associations = getattr(team, "associations", None) or {}
            name = getattr(team, "name", team_id)
            description = getattr(team, "description", None)
            now = datetime.now(timezone.utc)
            observed_row = {
                "id": team_id,
                "name": name,
                "description": description,
                "members": [],
                "project_keys": associations.get("project_keys", []),
                "repo_patterns": associations.get("repo_patterns", []),
                "is_active": True,
                "updated_at": now,
                "org_id": self.org_id,
                "provider": provider_type,
                "native_team_key": provider_team_id,
                "parent_team_id": None,
            }
            existing = await self.get(team_id)
            if existing is not None and on_conflict == "skip":
                await projector.record_observation(observed_row, discovered_at=now)
                skipped += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": provider_team_id,
                        "action": "skipped",
                    }
                )
                continue

            await projector.project_team(
                observed_row,
                catalog_row={
                    "id": team_id,
                    "team_uuid": existing.team_uuid
                    if existing is not None
                    else uuid.uuid5(
                        uuid.NAMESPACE_URL, f"team:{self.org_id}:{team_id}"
                    ),
                    "name": name,
                    "description": description,
                    "members": existing.members if existing is not None else [],
                    "project_keys": associations.get("project_keys", []),
                    "repo_patterns": associations.get("repo_patterns", []),
                    "is_active": 1,
                    "org_id": self.org_id,
                    "provider": "",
                    "native_team_key": None,
                    "updated_at": now,
                },
                discovered_at=now,
            )

            if existing is None:
                imported += 1
                action = "imported"
            else:
                merged += 1
                action = "merged"
            details.append(
                {
                    "team_id": team_id,
                    "provider_team_id": provider_team_id,
                    "action": action,
                }
            )

        return {
            "imported": imported,
            "skipped": skipped,
            "merged": merged,
            "details": details,
        }
