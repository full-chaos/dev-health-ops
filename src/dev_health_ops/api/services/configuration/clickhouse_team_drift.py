from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    ClickHouseTeamAdminService,
)

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas_flat import FlaggedChange
    from dev_health_ops.storage.clickhouse import ClickHouseStore


_CHANGE_COLUMNS = (
    "change_id",
    "entity_id",
    "provider",
    "native_team_key",
    "change_type",
    "field",
    "old_value_json",
    "new_value_json",
    "first_seen_at",
    "last_seen_at",
)

_OBSERVATION_COLUMNS = (
    "team_id",
    "name",
    "description",
    "members_json",
    "project_keys_json",
    "repo_patterns_json",
    "is_active",
    "parent_team_id",
)

_JSON_FIELD_COLUMNS = {
    "members": "members_json",
    "project_keys": "project_keys_json",
    "repo_patterns": "repo_patterns_json",
}


def _loads_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _json_list(value: Any) -> list[str]:
    loaded = _loads_json(value)
    if isinstance(loaded, list):
        return [str(item) for item in loaded if item is not None]
    return []


def _row_dict(column_names: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(column_names, row, strict=False))


class ClickHouseTeamDriftService:
    def __init__(self, store: ClickHouseStore, org_id: str) -> None:
        self.store = store
        self.org_id = org_id
        self.team_admin = ClickHouseTeamAdminService(store, org_id)

    async def get_pending_changes(self) -> list[FlaggedChange]:
        # Imported lazily: a module-top import of schemas_flat triggers the
        # api.admin package __init__ (router chain), which re-enters
        # routers/teams.py mid-init and creates a circular import.
        from dev_health_ops.api.admin.schemas_flat import FlaggedChange

        rows = await self._pending_rows(team_id=None)
        return [
            FlaggedChange(
                change_id=str(row["change_id"]),
                team_id=str(row["team_id"]),
                team_name=str(row["team_name"] or row["team_id"]),
                change_type=str(row["change_type"]),
                field=row["field"],
                old_value=_loads_json(row["old_value_json"]),
                new_value=_loads_json(row["new_value_json"]),
                discovered_at=row["first_seen_at"],
            )
            for row in rows
        ]

    async def approve(
        self,
        *,
        team_id: str,
        change_ids: list[str] | None = None,
        approve_all: bool = False,
        decided_by: str | None = None,
    ) -> dict[str, Any]:
        rows = await self._select_changes_for_decision(
            team_id=team_id, change_ids=change_ids or [], decide_all=approve_all
        )
        for row in rows:
            await self._apply_change(row)
        await self._insert_status_rows(rows, status="approved", decided_by=decided_by)
        return {
            "approved": len(rows),
            "change_ids": [str(r["change_id"]) for r in rows],
        }

    async def dismiss(
        self,
        *,
        team_id: str,
        change_ids: list[str] | None = None,
        dismiss_all: bool = False,
        decided_by: str | None = None,
    ) -> dict[str, Any]:
        rows = await self._select_changes_for_decision(
            team_id=team_id, change_ids=change_ids or [], decide_all=dismiss_all
        )
        await self._insert_status_rows(rows, status="dismissed", decided_by=decided_by)
        return {
            "dismissed": len(rows),
            "change_ids": [str(r["change_id"]) for r in rows],
        }

    async def _pending_rows(self, *, team_id: str | None) -> list[dict[str, Any]]:
        client = self.store.client
        assert client is not None
        conditions = [
            "c.org_id = {org_id:String}",
            "c.entity_type = 'team'",
            "c.status = 'pending'",
        ]
        params: dict[str, Any] = {"org_id": self.org_id}
        if team_id is not None:
            conditions.append("c.entity_id = {team_id:String}")
            params["team_id"] = team_id
        where = " AND ".join(conditions)
        query = f"""
            SELECT
                c.change_id,
                c.entity_id AS team_id,
                coalesce(t.name, c.entity_id) AS team_name,
                c.provider,
                c.native_team_key,
                c.change_type,
                c.field,
                c.old_value_json,
                c.new_value_json,
                c.first_seen_at,
                c.last_seen_at
            FROM team_drift_changes FINAL AS c
            LEFT JOIN teams FINAL AS t
                ON t.org_id = c.org_id AND t.id = c.entity_id
            WHERE {where}
            ORDER BY c.first_seen_at DESC, c.change_id
        """
        async with self.store._lock:
            result = await asyncio.to_thread(client.query, query, parameters=params)
        columns = tuple(getattr(result, "column_names", ()) or ())
        return [
            _row_dict(columns, row)
            for row in (getattr(result, "result_rows", None) or [])
        ]

    async def _select_changes_for_decision(
        self, *, team_id: str, change_ids: list[str], decide_all: bool
    ) -> list[dict[str, Any]]:
        if not decide_all and not change_ids:
            return []
        rows = await self._pending_rows(team_id=team_id)
        if decide_all:
            return rows
        requested = set(change_ids)
        return [row for row in rows if str(row["change_id"]) in requested]

    async def _apply_change(self, row: dict[str, Any]) -> None:
        field = row.get("field")
        if not field:
            return
        observation = await self._observation_for(row)
        observed_value = self._observed_field_value(
            field=str(field), observation=observation
        )
        if observed_value is None:
            observed_value = _loads_json(row.get("new_value_json"))

        team_id = str(row["team_id"])
        existing = await self.team_admin.get(team_id)
        name = (
            existing.name
            if existing is not None
            else str(row.get("team_name") or team_id)
        )
        description = existing.description if existing is not None else None
        members = existing.members if existing is not None else []
        project_keys = existing.project_keys if existing is not None else []
        repo_patterns = existing.repo_patterns if existing is not None else []

        if field == "name":
            name = str(observed_value or name)
        elif field == "description":
            description = None if observed_value is None else str(observed_value)
        elif field == "members":
            members = _json_list(observed_value)
        elif field == "project_keys":
            project_keys = _json_list(observed_value)
        elif field == "repo_patterns":
            repo_patterns = _json_list(observed_value)
        else:
            return

        await self.team_admin.create_or_update(
            team_id=team_id,
            name=name,
            description=description,
            members=members,
            project_keys=project_keys,
            repo_patterns=repo_patterns,
        )

    async def _observation_for(self, row: dict[str, Any]) -> dict[str, Any]:
        client = self.store.client
        assert client is not None
        native_team_key = row.get("native_team_key")
        conditions = ["org_id = {org_id:String}", "provider = {provider:String}"]
        params: dict[str, Any] = {
            "org_id": self.org_id,
            "provider": str(row.get("provider") or ""),
            "team_id": str(row["team_id"]),
        }
        if native_team_key:
            conditions.append("native_team_key = {native_team_key:String}")
            params["native_team_key"] = str(native_team_key)
        else:
            conditions.append("team_id = {team_id:String}")
        query = f"""
            SELECT {", ".join(_OBSERVATION_COLUMNS)}
            FROM team_provider_observations FINAL
            WHERE {" AND ".join(conditions)}
            ORDER BY updated_at DESC
            LIMIT 1
        """
        async with self.store._lock:
            result = await asyncio.to_thread(client.query, query, parameters=params)
        rows = getattr(result, "result_rows", None) or []
        if not rows:
            return {}
        columns = tuple(getattr(result, "column_names", ()) or _OBSERVATION_COLUMNS)
        return _row_dict(columns, rows[0])

    def _observed_field_value(self, *, field: str, observation: dict[str, Any]) -> Any:
        column = _JSON_FIELD_COLUMNS.get(field, field)
        if column not in observation:
            return None
        value = observation[column]
        if column.endswith("_json"):
            return _loads_json(value)
        return value

    async def _insert_status_rows(
        self, rows: list[dict[str, Any]], *, status: str, decided_by: str | None
    ) -> None:
        if not rows:
            return
        insert_changes = getattr(self.store, "insert_team_drift_changes", None)
        if insert_changes is None:
            raise RuntimeError(
                "ClickHouseStore.insert_team_drift_changes is required by "
                "CHAOS-2622 schema lane before drift decisions can be persisted."
            )
        now = datetime.now(timezone.utc)
        payload = [
            {
                "org_id": self.org_id,
                "change_id": row["change_id"],
                "entity_type": "team",
                "entity_id": row["team_id"],
                "provider": row.get("provider") or "",
                "native_team_key": row.get("native_team_key"),
                "change_type": row["change_type"],
                "field": row.get("field"),
                "old_value_json": row.get("old_value_json") or "null",
                "new_value_json": row.get("new_value_json") or "null",
                "status": status,
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
                "decided_at": now,
                "decided_by": decided_by,
                "updated_at": now,
            }
            for row in rows
        ]
        await insert_changes(payload)
