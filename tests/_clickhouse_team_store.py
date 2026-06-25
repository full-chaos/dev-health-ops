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
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
    _IDENTITY_COLUMNS,
)
from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    _TEAM_COLUMNS,
)
from dev_health_ops.api.services.configuration.clickhouse_team_drift import (
    _OBSERVATION_COLUMNS,
)

# Column aliases produced by ClickHouseTeamDriftService._pending_rows SELECT.
_PENDING_ALIASES = (
    "change_id",
    "team_id",
    "team_name",
    "provider",
    "native_team_key",
    "change_type",
    "field",
    "old_value_json",
    "new_value_json",
    "first_seen_at",
    "last_seen_at",
)


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v is not None]
    return []


class _QueryResult:
    def __init__(
        self,
        rows: list[tuple[Any, ...]],
        column_names: tuple[str, ...] = (),
    ) -> None:
        self.result_rows = rows
        self.column_names = list(column_names)


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

        if "team_drift_changes" in query:
            team_id = params.get("team_id")
            drift_rows: list[tuple[Any, ...]] = []
            for (row_org, _change_id), row in self._store.drift_changes.items():
                if row_org != org_id:
                    continue
                if row.get("status") != "pending":
                    continue
                if str(row.get("entity_type") or "team") != "team":
                    continue
                entity_id = str(row.get("entity_id") or "")
                if team_id is not None and entity_id != team_id:
                    continue
                team_row = self._store.rows.get((row_org, entity_id))
                team_name = team_row["name"] if team_row else entity_id
                drift_rows.append(
                    (
                        row.get("change_id"),
                        entity_id,
                        team_name,
                        row.get("provider"),
                        row.get("native_team_key"),
                        row.get("change_type"),
                        row.get("field"),
                        row.get("old_value_json"),
                        row.get("new_value_json"),
                        row.get("first_seen_at"),
                        row.get("last_seen_at"),
                    )
                )
            return _QueryResult(drift_rows, _PENDING_ALIASES)

        if "team_provider_observations" in query:
            obs = self._store.observations.get((org_id, str(params.get("team_id", ""))))
            obs_rows = (
                [tuple(obs.get(col) for col in _OBSERVATION_COLUMNS)] if obs else []
            )
            return _QueryResult(obs_rows, _OBSERVATION_COLUMNS)

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
        self.drift_changes: dict[tuple[str, str], dict[str, Any]] = {}
        self.observations: dict[tuple[str, str], dict[str, Any]] = {}
        self.sync_policies: dict[tuple[str, str], dict[str, Any]] = {}
        self.provider_observations: list[dict[str, Any]] = []
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
                "name": str(get("name")) if get("name") is not None else team_id,
                "description": get("description"),
                "members": _as_list(get("members")),
                "project_keys": _as_list(get("project_keys")),
                "repo_patterns": _as_list(get("repo_patterns")),
                "is_active": int(get("is_active", 1) or 0),
                "updated_at": updated_at,
                "org_id": org_id,
            }

    async def insert_team_drift_changes(self, rows: list[Any]) -> None:
        for item in rows:

            def get(key: str, default: Any = None, _item: Any = item) -> Any:
                return self._get(_item, key, default)

            org_id = str(get("org_id") or self.org_id or "")
            change_id = str(get("change_id") or "")
            self.drift_changes[(org_id, change_id)] = {
                "org_id": org_id,
                "change_id": change_id,
                "entity_type": get("entity_type") or "team",
                "entity_id": get("entity_id"),
                "provider": get("provider"),
                "native_team_key": get("native_team_key"),
                "change_type": get("change_type"),
                "field": get("field"),
                "old_value_json": get("old_value_json"),
                "new_value_json": get("new_value_json"),
                "status": get("status") or "pending",
                "first_seen_at": get("first_seen_at"),
                "last_seen_at": get("last_seen_at"),
                "decided_at": get("decided_at"),
                "decided_by": get("decided_by"),
                "updated_at": get("updated_at") or datetime.now(timezone.utc),
            }

    async def insert_team_provider_observations(self, rows: list[Any]) -> None:
        for item in rows:

            def get(key: str, default: Any = None, _item: Any = item) -> Any:
                return self._get(_item, key, default)

            org_id = str(get("org_id") or self.org_id or "")
            team_id = str(get("team_id") or "")
            self.provider_observations.append(
                {
                    "org_id": org_id,
                    "provider": get("provider"),
                    "native_team_key": get("native_team_key"),
                    "team_id": team_id,
                    "name": get("name"),
                    "description": get("description"),
                    "members": _as_list(get("members")),
                    "project_keys": _as_list(get("project_keys")),
                    "repo_patterns": _as_list(get("repo_patterns")),
                    "is_active": int(get("is_active", 1) or 0),
                    "parent_team_id": get("parent_team_id"),
                }
            )
            # Mirror into the service-readable observation view (JSON columns).
            self.observations[(org_id, team_id)] = {
                "team_id": team_id,
                "name": get("name"),
                "description": get("description"),
                "members_json": json.dumps(_as_list(get("members"))),
                "project_keys_json": json.dumps(_as_list(get("project_keys"))),
                "repo_patterns_json": json.dumps(_as_list(get("repo_patterns"))),
                "is_active": int(get("is_active", 1) or 0),
                "parent_team_id": get("parent_team_id"),
            }

    def set_provider_observation(
        self, *, org_id: str, team_id: str, **fields: Any
    ) -> None:
        obs: dict[str, Any] = {col: None for col in _OBSERVATION_COLUMNS}
        obs["team_id"] = team_id
        obs.update(fields)
        self.observations[(org_id, team_id)] = obs

    def set_sync_policy(
        self,
        *,
        org_id: str,
        team_id: str,
        sync_policy: int,
        managed_fields: list[str] | None = None,
    ) -> None:
        self.sync_policies[(org_id, team_id)] = {
            "sync_policy": sync_policy,
            "managed_fields": list(managed_fields or []),
        }

    def query_dicts(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        params = parameters or {}
        org_id = str(params.get("org_id", ""))
        if "team_sync_policies" in query:
            row = self.sync_policies.get((org_id, str(params.get("team_id", ""))))
            return [dict(row)] if row else []
        if "team_drift_changes" in query:
            team_id = params.get("team_id")
            provider = params.get("provider")
            pending_only = "status = 'pending'" in query
            changes: list[dict[str, Any]] = []
            for (row_org, _change_id), row in self.drift_changes.items():
                if row_org != org_id:
                    continue
                if str(row.get("entity_type") or "team") != "team":
                    continue
                if team_id is not None and str(row.get("entity_id")) != str(team_id):
                    continue
                if provider is not None and str(row.get("provider") or "") != str(
                    provider
                ):
                    continue
                if pending_only and row.get("status") != "pending":
                    continue
                changes.append(dict(row))
            return changes
        if "teams FINAL" in query:
            team_id = params.get("team_id")
            teams_out: list[dict[str, Any]] = []
            for (row_org, row_id), row in self.rows.items():
                if row_org != org_id:
                    continue
                if team_id is not None and row_id != team_id:
                    continue
                teams_out.append(dict(row))
            return teams_out
        return []

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
