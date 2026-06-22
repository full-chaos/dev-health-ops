"""ClickHouse-backed identity admin store (CHAOS-2600 CS5).

ClickHouse is the system of record for identity records (canonical_id ->
provider identities + team membership), not just the team catalog. This
store mirrors the Postgres ``IdentityMappingService`` contract so the admin
identity surface keeps the same wire shape while writing the ClickHouse
``identities`` table directly — no Postgres ``IdentityMapping`` rows. The CH
table is named ``identities`` (parallel to CH ``teams``) so it does not collide
with the Postgres ``identity_mappings`` table during org-deletion purge.

The ``identities`` table is a ``ReplacingMergeTree(updated_at)`` keyed
on ``(org_id, canonical_id)``: a create/update is a row insert with a fresh
``updated_at`` (the latest row wins under ``FINAL``); a delete is a ClickHouse
lightweight ``DELETE``. ``provider_identities`` is JSON-encoded on the wire.

``create_or_update`` uses REPLACEMENT semantics mirroring the Postgres
service: a provided field replaces; ``team_ids`` / ``provider_identities``
replace wholesale.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dev_health_ops.storage.clickhouse import ClickHouseStore

# Column order selected from the ClickHouse `identities` table read path.
_IDENTITY_COLUMNS = (
    "canonical_id",
    "identity_uuid",
    "display_name",
    "email",
    "provider_identities",
    "team_ids",
    "is_active",
    "updated_at",
    "org_id",
)

_IDENTITY_SELECT = ", ".join(_IDENTITY_COLUMNS)


def _decode_provider_identities(value: Any) -> dict[str, list[str]]:
    if isinstance(value, dict):
        return {str(k): [str(v) for v in (vals or [])] for k, vals in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return {}
        if isinstance(parsed, dict):
            return {
                str(k): [str(v) for v in (vals or [])] for k, vals in parsed.items()
            }
    return {}


class ClickHouseIdentity:
    """Row wrapper exposing the attribute surface ``_identity_mapping_response``
    expects from a Postgres ``IdentityMapping`` so the wire shape stays stable.
    """

    def __init__(
        self,
        *,
        canonical_id: str,
        identity_uuid: uuid.UUID,
        display_name: str | None,
        email: str | None,
        provider_identities: dict[str, list[str]],
        team_ids: list[str],
        is_active: bool,
        updated_at: datetime,
        org_id: str,
    ) -> None:
        self.id = str(identity_uuid)
        self.identity_uuid = identity_uuid
        self.canonical_id = canonical_id
        self.display_name = display_name
        self.email = email
        self.provider_identities = provider_identities
        self.team_ids = team_ids
        self.is_active = is_active
        self.updated_at = updated_at
        self.created_at = updated_at
        self.org_id = org_id


class ClickHouseIdentityStore:
    """CRUD for the ClickHouse-native identity catalog (admin surface)."""

    def __init__(self, store: ClickHouseStore, org_id: str) -> None:
        self.store = store
        self.org_id = org_id

    def _row_to_identity(self, row: Any) -> ClickHouseIdentity:
        identity_uuid = row[1]
        if not isinstance(identity_uuid, uuid.UUID):
            identity_uuid = uuid.UUID(str(identity_uuid))
        updated_at = row[7]
        if not isinstance(updated_at, datetime):
            updated_at = datetime.now(timezone.utc)
        return ClickHouseIdentity(
            canonical_id=str(row[0]),
            identity_uuid=identity_uuid,
            display_name=row[2],
            email=row[3],
            provider_identities=_decode_provider_identities(row[4]),
            team_ids=[str(t) for t in (row[5] or [])],
            is_active=bool(row[6]),
            updated_at=updated_at,
            org_id=str(row[8] or ""),
        )

    async def _query(
        self, *, canonical_id: str | None, active_only: bool
    ) -> list[ClickHouseIdentity]:
        client = self.store.client
        assert client is not None
        conditions = ["org_id = {org_id:String}"]
        params: dict[str, Any] = {"org_id": self.org_id}
        if canonical_id is not None:
            conditions.append("canonical_id = {canonical_id:String}")
            params["canonical_id"] = canonical_id
        if active_only:
            conditions.append("is_active = 1")
        where = " AND ".join(conditions)
        query = f"SELECT {_IDENTITY_SELECT} FROM identities FINAL WHERE {where}"
        async with self.store._lock:
            result = await asyncio.to_thread(client.query, query, parameters=params)
        return [self._row_to_identity(row) for row in (result.result_rows or [])]

    async def get(self, canonical_id: str) -> ClickHouseIdentity | None:
        # Surface inactive rows too so callers can distinguish missing from
        # inactive; active filtering is only for the list view.
        rows = await self._query(canonical_id=canonical_id, active_only=False)
        return rows[0] if rows else None

    async def list_all(self, active_only: bool = True) -> list[ClickHouseIdentity]:
        return await self._query(canonical_id=None, active_only=active_only)

    async def find_by_provider_identity(
        self, provider: str, identity: str
    ) -> ClickHouseIdentity | None:
        # Scan the org's identities and match the provider identity. Mirrors the
        # Postgres service which loads all org rows and matches in Python.
        for record in await self.list_all(active_only=False):
            if identity in record.provider_identities.get(provider, []):
                return record
        return None

    async def create_or_update(
        self,
        canonical_id: str,
        display_name: str | None = None,
        email: str | None = None,
        provider_identities: dict[str, list[str]] | None = None,
        team_ids: list[str] | None = None,
    ) -> ClickHouseIdentity:
        existing = await self.get(canonical_id)
        identity_uuid = (
            existing.identity_uuid
            if existing is not None
            else uuid.uuid5(
                uuid.NAMESPACE_URL, f"identity:{self.org_id}:{canonical_id}"
            )
        )
        # Replacement semantics (mirror the Postgres service): a provided field
        # replaces; team_ids / provider_identities replace wholesale.
        resolved_display = (
            display_name
            if display_name is not None
            else (existing.display_name if existing is not None else None)
        )
        resolved_email = (
            email if email is not None else (existing.email if existing else None)
        )
        resolved_providers = (
            provider_identities
            if provider_identities is not None
            else (existing.provider_identities if existing is not None else {})
        )
        resolved_team_ids = (
            list(team_ids)
            if team_ids is not None
            else (list(existing.team_ids) if existing is not None else [])
        )
        now = datetime.now(timezone.utc)
        await self.store.insert_identities(
            [
                {
                    "org_id": self.org_id,
                    "canonical_id": canonical_id,
                    "identity_uuid": identity_uuid,
                    "display_name": resolved_display,
                    "email": resolved_email,
                    "provider_identities": json.dumps(resolved_providers),
                    "team_ids": resolved_team_ids,
                    "is_active": 1,
                    "updated_at": now,
                }
            ]
        )
        return ClickHouseIdentity(
            canonical_id=canonical_id,
            identity_uuid=identity_uuid,
            display_name=resolved_display,
            email=resolved_email,
            provider_identities=resolved_providers,
            team_ids=resolved_team_ids,
            is_active=True,
            updated_at=now,
            org_id=self.org_id,
        )

    async def delete(self, canonical_id: str) -> bool:
        existing = await self.get(canonical_id)
        if existing is None:
            return False
        client = self.store.client
        assert client is not None
        async with self.store._lock:
            await asyncio.to_thread(
                client.command,
                "DELETE FROM identities WHERE org_id = {org_id:String} "
                "AND canonical_id = {canonical_id:String}",
                parameters={"org_id": self.org_id, "canonical_id": canonical_id},
            )
        return True
