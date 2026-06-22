"""In-memory fake ClickHouse store for admin team/identity router tests.

CHAOS-2600 CS5 moves the admin team catalog onto ClickHouse. The admin
endpoints depend on an open :class:`ClickHouseStore` (via the
``get_clickhouse_store`` FastAPI dependency) and drive it through
``ClickHouseTeamAdminService``. These tests replace that store with an
in-memory fake that implements exactly the surface the service touches:

* ``insert_teams`` — upsert rows keyed by ``(org_id, id)`` (ReplacingMergeTree
  semantics: latest write wins).
* ``client.query`` — return the ``_TEAM_SELECT`` column tuples for the org,
  honouring the optional ``id`` filter and the ``is_active = 1`` clause.
* ``client.command`` — apply a ``DELETE FROM teams`` for an ``(org_id, id)``.

Mirrors the ``MockClient`` pattern in ``tests/graphql/test_work_graph.py`` but
backs it with real row state so create/list/get/update/delete round-trip.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
    _IDENTITY_COLUMNS,
)
from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    _TEAM_COLUMNS,
)


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v is not None]
    return []


class _QueryResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _FakeClient:
    def __init__(self, store: FakeClickHouseTeamStore) -> None:
        self._store = store

    def query(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> _QueryResult:
        params = parameters or {}
        org_id = str(params.get("org_id", ""))
        active_only = "is_active = 1" in query
        rows: list[tuple[Any, ...]] = []
        if "FROM identities" in query:
            canonical_id = params.get("canonical_id")
            for (row_org, row_cid), row in self._store.identities.items():
                if row_org != org_id:
                    continue
                if canonical_id is not None and row_cid != canonical_id:
                    continue
                if active_only and not int(row.get("is_active", 1)):
                    continue
                rows.append(tuple(row[col] for col in _IDENTITY_COLUMNS))
            return _QueryResult(rows)

        team_id = params.get("team_id")
        for (row_org, row_id), row in self._store.rows.items():
            if row_org != org_id:
                continue
            if team_id is not None and row_id != team_id:
                continue
            if active_only and not int(row.get("is_active", 1)):
                continue
            rows.append(tuple(row[col] for col in _TEAM_COLUMNS))
        return _QueryResult(rows)

    def command(self, query: str, parameters: dict[str, Any] | None = None) -> None:
        params = parameters or {}
        if "DELETE FROM teams" in query:
            key = (str(params.get("org_id", "")), str(params.get("team_id", "")))
            self._store.rows.pop(key, None)
        elif "DELETE FROM identities" in query:
            key = (
                str(params.get("org_id", "")),
                str(params.get("canonical_id", "")),
            )
            self._store.identities.pop(key, None)


class FakeClickHouseTeamStore:
    """Minimal in-memory stand-in for ``ClickHouseStore`` (team surface only)."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.identities: dict[tuple[str, str], dict[str, Any]] = {}
        self.org_id: str | None = None
        self._lock = asyncio.Lock()
        self.client = _FakeClient(self)

    @staticmethod
    def _get(item: Any, key: str, default: Any = None) -> Any:
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    async def insert_teams(self, teams: list[Any]) -> None:
        for item in teams:

            def get(key: str, default: Any = None, _item: Any = item) -> Any:
                return self._get(_item, key, default)

            team_id = str(get("id") or "")
            org_id = str(get("org_id") or self.org_id or "")
            team_uuid = get("team_uuid") or uuid.uuid5(
                uuid.NAMESPACE_URL, f"team:{org_id}:{team_id}"
            )
            if not isinstance(team_uuid, uuid.UUID):
                team_uuid = uuid.UUID(str(team_uuid))
            updated_at = get("updated_at") or datetime.now(timezone.utc)
            self.rows[(org_id, team_id)] = {
                "id": team_id,
                "team_uuid": team_uuid,
                "name": str(get("name") or team_id),
                "description": get("description"),
                "members": _as_list(get("members")),
                "project_keys": _as_list(get("project_keys")),
                "repo_patterns": _as_list(get("repo_patterns")),
                "is_active": int(get("is_active", 1) or 0),
                "updated_at": updated_at,
                "org_id": org_id,
            }

    async def insert_identities(self, mappings: list[Any]) -> None:
        for item in mappings:

            def get(key: str, default: Any = None, _item: Any = item) -> Any:
                return self._get(_item, key, default)

            canonical_id = str(get("canonical_id") or "")
            org_id = str(get("org_id") or self.org_id or "")
            identity_uuid = get("identity_uuid") or uuid.uuid5(
                uuid.NAMESPACE_URL, f"identity:{org_id}:{canonical_id}"
            )
            if not isinstance(identity_uuid, uuid.UUID):
                identity_uuid = uuid.UUID(str(identity_uuid))
            updated_at = get("updated_at") or datetime.now(timezone.utc)
            self.identities[(org_id, canonical_id)] = {
                "canonical_id": canonical_id,
                "identity_uuid": identity_uuid,
                "display_name": get("display_name"),
                "email": get("email"),
                # Service serializes to JSON before insert; store the string.
                "provider_identities": str(get("provider_identities") or "{}"),
                "team_ids": _as_list(get("team_ids")),
                "is_active": int(get("is_active", 1) or 0),
                "updated_at": updated_at,
                "org_id": org_id,
            }

    # Async context manager so it can be yielded like ``get_clickhouse_store``.
    async def __aenter__(self) -> FakeClickHouseTeamStore:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None
