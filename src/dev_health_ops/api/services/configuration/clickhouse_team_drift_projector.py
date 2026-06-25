from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from dev_health_ops.storage.clickhouse import ClickHouseStore


AUTO_APPLY_POLICY = 0
FLAG_FOR_REVIEW_POLICY = 1
MANUAL_POLICY = 2

ENTITY_TYPE_TEAM = "team"
FIELD_CHANGED = "field_changed"
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_DISMISSED = "dismissed"
STATUS_RESOLVED = "resolved"
STATUS_SUPERSEDED = "superseded"

DEFAULT_MANAGED_FIELDS = (
    "name",
    "description",
    "members",
    "project_keys",
    "repo_patterns",
)
JSON_FIELDS = {"members", "project_keys", "repo_patterns"}
DECIDED_STATUSES = {STATUS_APPROVED, STATUS_DISMISSED}

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
    "provider",
    "native_team_key",
    "parent_team_id",
)
_CHANGE_COLUMNS = (
    "org_id",
    "change_id",
    "entity_id",
    "provider",
    "native_team_key",
    "change_type",
    "field",
    "old_value_json",
    "new_value_json",
    "status",
    "first_seen_at",
    "last_seen_at",
    "decided_at",
    "decided_by",
    "updated_at",
)

TeamWriter = Callable[[list[dict[str, Any]]], Awaitable[None]]


def change_id_for_team_field(
    *,
    org_id: str,
    team_id: str,
    field: str,
    old_value_json: str,
    new_value_json: str,
    change_type: str = FIELD_CHANGED,
) -> str:
    payload = {
        "org_id": org_id,
        "entity_type": ENTITY_TYPE_TEAM,
        "entity_id": team_id,
        "change_type": change_type,
        "field": field,
        "old_value_json": old_value_json,
        "new_value_json": new_value_json,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def project_provider_team_rows(
    *,
    dsn: str,
    org_id: str,
    provider: str | None = None,
    team_rows: list[dict[str, Any]],
    team_writer: TeamWriter,
    discovered_at: datetime | None = None,
    resolve_missing_provider_changes: bool = False,
) -> None:
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    async with ClickHouseStore(dsn) as store:
        store.org_id = org_id
        await project_team_rows_with_store(
            store=store,
            org_id=org_id,
            provider=provider,
            team_rows=team_rows,
            team_writer=team_writer,
            discovered_at=discovered_at,
            resolve_missing_provider_changes=resolve_missing_provider_changes,
        )


async def project_team_rows_with_store(
    *,
    store: ClickHouseStore,
    org_id: str,
    provider: str | None = None,
    team_rows: list[dict[str, Any]],
    team_writer: TeamWriter | None = None,
    discovered_at: datetime | None = None,
    resolve_missing_provider_changes: bool = False,
) -> None:
    projector = ClickHouseTeamDriftProjector(
        store=store,
        org_id=org_id,
        team_writer=team_writer,
    )
    await projector.project_many(
        team_rows,
        provider=provider,
        discovered_at=discovered_at,
        resolve_missing_provider_changes=resolve_missing_provider_changes,
    )


class ClickHouseTeamDriftProjector:
    def __init__(
        self,
        *,
        store: ClickHouseStore,
        org_id: str,
        team_writer: TeamWriter | None = None,
    ) -> None:
        self.store = store
        self.org_id = org_id
        self.team_writer = team_writer or store.insert_teams

    async def project_many(
        self,
        team_rows: Sequence[dict[str, Any]],
        *,
        provider: str | None = None,
        discovered_at: datetime | None = None,
        resolve_missing_provider_changes: bool = False,
    ) -> None:
        observed: list[dict[str, Any]] = []
        for team_row in team_rows:
            observed.append(
                await self.project_team(team_row, discovered_at=discovered_at)
            )
        provider_name = provider or _provider_from_observed(observed)
        if provider_name and resolve_missing_provider_changes:
            await self.resolve_missing_provider_changes(
                provider=provider_name,
                observed=observed,
                now=_utc_now(),
            )

    async def project_team(
        self,
        team_row: dict[str, Any],
        *,
        catalog_row: dict[str, Any] | None = None,
        discovered_at: datetime | None = None,
        apply_catalog: bool = True,
        detect_drift: bool = True,
    ) -> dict[str, Any]:
        now = _utc_now()
        observed = _observed_row(
            self.org_id,
            team_row,
            discovered_at=discovered_at or now,
            updated_at=now,
        )
        await self._insert_observation(observed)

        if not apply_catalog and not detect_drift:
            return observed

        policy, managed_fields = await self._sync_policy(observed["team_id"])
        pending_changes = await self._change_rows(
            team_id=observed["team_id"],
        )

        if policy == AUTO_APPLY_POLICY:
            if apply_catalog:
                await self.team_writer(
                    [_catalog_row_for_write(self.org_id, catalog_row or team_row)]
                )
            await self._mark_pending(
                pending_changes,
                status=STATUS_RESOLVED,
                now=now,
            )
            return observed

        if policy != FLAG_FOR_REVIEW_POLICY or not detect_drift:
            return observed

        existing = await self._team_row(observed["team_id"])
        await self._project_field_changes(
            observed=observed,
            existing=existing,
            managed_fields=managed_fields,
            existing_changes=pending_changes,
            now=now,
        )
        return observed

    async def record_observation(
        self,
        team_row: dict[str, Any],
        *,
        discovered_at: datetime | None = None,
    ) -> None:
        now = _utc_now()
        await self._insert_observation(
            _observed_row(
                self.org_id,
                team_row,
                discovered_at=discovered_at or now,
                updated_at=now,
            )
        )

    async def resolve_missing_provider_changes(
        self,
        *,
        provider: str,
        observed: Sequence[dict[str, Any]],
        now: datetime,
    ) -> None:
        pending = await self._pending_provider_changes(provider=provider)
        if not pending:
            return
        observed_team_ids = {str(row.get("team_id") or "") for row in observed}
        observed_native_keys = {
            str(row.get("native_team_key") or "")
            for row in observed
            if row.get("native_team_key")
        }
        missing = [
            row
            for row in pending
            if not _change_observed(
                row,
                observed_team_ids=observed_team_ids,
                observed_native_keys=observed_native_keys,
            )
        ]
        await self._insert_changes(
            _status_rows(missing, status=STATUS_RESOLVED, now=now)
        )

    async def _project_field_changes(
        self,
        *,
        observed: dict[str, Any],
        existing: dict[str, Any] | None,
        managed_fields: tuple[str, ...],
        existing_changes: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        changes_by_id = {str(row["change_id"]): row for row in existing_changes}
        pending_by_field = _pending_by_field(existing_changes)
        rows_to_insert: list[dict[str, Any]] = []

        for field in managed_fields:
            old_value_json = _field_json(existing, field)
            new_value_json = _field_json(observed, field)
            field_pending = pending_by_field.get(field, [])

            if old_value_json == new_value_json:
                rows_to_insert.extend(
                    _status_rows(field_pending, status=STATUS_RESOLVED, now=now)
                )
                continue

            change_id = change_id_for_team_field(
                org_id=self.org_id,
                team_id=str(observed["team_id"]),
                field=field,
                old_value_json=old_value_json,
                new_value_json=new_value_json,
            )
            rows_to_insert.extend(
                _status_rows(
                    [row for row in field_pending if row["change_id"] != change_id],
                    status=STATUS_SUPERSEDED,
                    now=now,
                )
            )

            existing_change = changes_by_id.get(change_id)
            if existing_change and existing_change.get("status") in DECIDED_STATUSES:
                continue

            first_seen_at = (
                existing_change.get("first_seen_at")
                if existing_change
                and existing_change.get("status") == STATUS_PENDING
                and existing_change.get("first_seen_at")
                else now
            )
            rows_to_insert.append(
                {
                    "org_id": self.org_id,
                    "change_id": change_id,
                    "entity_type": ENTITY_TYPE_TEAM,
                    "entity_id": observed["team_id"],
                    "provider": observed["provider"],
                    "native_team_key": observed["native_team_key"],
                    "change_type": FIELD_CHANGED,
                    "field": field,
                    "old_value_json": old_value_json,
                    "new_value_json": new_value_json,
                    "status": STATUS_PENDING,
                    "first_seen_at": first_seen_at,
                    "last_seen_at": now,
                    "decided_at": None,
                    "decided_by": None,
                    "updated_at": now,
                }
            )

        await self._insert_changes(rows_to_insert)

    async def _insert_observation(self, row: dict[str, Any]) -> None:
        await self.store.insert_team_provider_observations([row])

    async def _insert_changes(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            await self.store.insert_team_drift_changes(rows)

    async def _mark_pending(
        self,
        rows: list[dict[str, Any]],
        *,
        status: str,
        now: datetime,
    ) -> None:
        await self._insert_changes(
            _status_rows(_pending_rows(rows), status=status, now=now)
        )

    async def _sync_policy(self, team_id: str) -> tuple[int, tuple[str, ...]]:
        rows = await self._query_dicts(
            """
            SELECT sync_policy, managed_fields
            FROM team_sync_policies FINAL
            WHERE org_id = {org_id:String} AND team_id = {team_id:String}
            LIMIT 1
            """,
            {"org_id": self.org_id, "team_id": team_id},
        )
        if not rows:
            return AUTO_APPLY_POLICY, DEFAULT_MANAGED_FIELDS
        policy = int(rows[0].get("sync_policy", AUTO_APPLY_POLICY) or AUTO_APPLY_POLICY)
        managed = _managed_fields(rows[0].get("managed_fields"))
        return policy, managed

    async def _team_row(self, team_id: str) -> dict[str, Any] | None:
        rows = await self._query_dicts(
            f"""
            SELECT {", ".join(_TEAM_COLUMNS)}
            FROM teams FINAL
            WHERE org_id = {{org_id:String}} AND id = {{team_id:String}}
            LIMIT 1
            """,
            {"org_id": self.org_id, "team_id": team_id},
        )
        return rows[0] if rows else None

    async def _change_rows(
        self,
        *,
        team_id: str,
    ) -> list[dict[str, Any]]:
        return await self._query_dicts(
            f"""
            SELECT {", ".join(_CHANGE_COLUMNS)}
            FROM team_drift_changes FINAL
            WHERE org_id = {{org_id:String}}
              AND entity_type = 'team'
              AND entity_id = {{team_id:String}}
              AND change_type = 'field_changed'
            """,
            {"org_id": self.org_id, "team_id": team_id},
        )

    async def _pending_provider_changes(self, *, provider: str) -> list[dict[str, Any]]:
        return await self._query_dicts(
            f"""
            SELECT {", ".join(_CHANGE_COLUMNS)}
            FROM team_drift_changes FINAL
            WHERE org_id = {{org_id:String}}
              AND entity_type = 'team'
              AND provider = {{provider:String}}
              AND change_type = 'field_changed'
              AND status = 'pending'
            """,
            {"org_id": self.org_id, "provider": provider},
        )

    async def _query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        query_dicts = getattr(self.store, "query_dicts", None)
        if callable(query_dicts):
            result = query_dicts(query, parameters)
            if inspect.isawaitable(result):
                result = await result
            return list(cast(Sequence[dict[str, Any]], result or []))

        client = self.store.client
        assert client is not None
        async with self.store._lock:
            result = await asyncio.to_thread(client.query, query, parameters=parameters)
        columns = tuple(getattr(result, "column_names", ()) or ())
        return [
            dict(zip(columns, row, strict=False))
            for row in (getattr(result, "result_rows", None) or [])
        ]


def _observed_row(
    org_id: str,
    team_row: dict[str, Any],
    *,
    discovered_at: datetime,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "provider": str(team_row.get("provider") or ""),
        "native_team_key": str(
            team_row.get("native_team_key") or team_row.get("id") or ""
        ),
        "team_id": str(team_row.get("id") or ""),
        "name": team_row.get("name"),
        "description": team_row.get("description"),
        "members": _list_field(team_row.get("members")),
        "project_keys": _list_field(team_row.get("project_keys")),
        "repo_patterns": _list_field(team_row.get("repo_patterns")),
        "is_active": 1 if team_row.get("is_active", True) else 0,
        "parent_team_id": team_row.get("parent_team_id"),
        "discovered_at": discovered_at,
        "updated_at": updated_at,
    }


def _field_json(row: dict[str, Any] | None, field: str) -> str:
    if field in JSON_FIELDS:
        value: Any = _comparison_list_field(None if row is None else row.get(field))
    else:
        value = None if row is None else row.get(field)
    return _canonical_json(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _list_field(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item is not None]
    return []


def _comparison_list_field(value: Any) -> list[str]:
    return sorted(set(_list_field(value)))


def _catalog_row_for_write(org_id: str, team_row: dict[str, Any]) -> dict[str, Any]:
    row = dict(team_row)
    row["org_id"] = org_id
    return row


def _provider_from_observed(observed: Sequence[dict[str, Any]]) -> str | None:
    providers = {str(row.get("provider") or "") for row in observed}
    providers.discard("")
    if len(providers) == 1:
        return next(iter(providers))
    return None


def _change_observed(
    row: dict[str, Any],
    *,
    observed_team_ids: set[str],
    observed_native_keys: set[str],
) -> bool:
    entity_id = str(row.get("entity_id") or row.get("team_id") or "")
    native_team_key = str(row.get("native_team_key") or "")
    return entity_id in observed_team_ids or native_team_key in observed_native_keys


def _managed_fields(value: Any) -> tuple[str, ...]:
    if not value:
        return DEFAULT_MANAGED_FIELDS
    fields = [str(field) for field in value if str(field) in DEFAULT_MANAGED_FIELDS]
    return tuple(fields) or DEFAULT_MANAGED_FIELDS


def _pending_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("status") == STATUS_PENDING]


def _pending_by_field(
    rows: Sequence[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_field: dict[str, list[dict[str, Any]]] = {}
    for row in _pending_rows(rows):
        field = row.get("field")
        if field:
            by_field.setdefault(str(field), []).append(row)
    return by_field


def _status_rows(
    rows: Sequence[dict[str, Any]],
    *,
    status: str,
    now: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "org_id": row["org_id"],
            "change_id": row["change_id"],
            "entity_type": ENTITY_TYPE_TEAM,
            "entity_id": row.get("entity_id") or row.get("team_id"),
            "provider": row.get("provider") or "",
            "native_team_key": row.get("native_team_key"),
            "change_type": row.get("change_type") or FIELD_CHANGED,
            "field": row.get("field"),
            "old_value_json": row.get("old_value_json") or "null",
            "new_value_json": row.get("new_value_json") or "null",
            "status": status,
            "first_seen_at": row.get("first_seen_at") or now,
            "last_seen_at": now,
            "decided_at": row.get("decided_at") if status in DECIDED_STATUSES else None,
            "decided_by": row.get("decided_by") if status in DECIDED_STATUSES else None,
            "updated_at": now,
        }
        for row in rows
    ]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
