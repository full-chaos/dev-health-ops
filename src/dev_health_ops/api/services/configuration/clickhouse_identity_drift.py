from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    DECIDED_STATUSES,
    STATUS_PENDING,
    STATUS_RESOLVED,
    STATUS_SUPERSEDED,
)

ENTITY_TYPE_IDENTITY = "identity"
CHANGE_TYPE_MEMBERSHIP = "membership_changed"
FIELD_TEAM_MEMBERSHIP = "team_memberships"
FIELD_MEMBER_FALLBACK = "manual_attribution_fallbacks.member"


def change_id_for_identity_membership(
    *,
    org_id: str,
    team_id: str,
    provider: str,
    member_id: str,
    field: str,
    old_value_json: str,
    new_value_json: str,
) -> str:
    payload = {
        "org_id": org_id,
        "entity_type": ENTITY_TYPE_IDENTITY,
        "entity_id": team_id,
        "provider": provider,
        "member_id": member_id,
        "change_type": CHANGE_TYPE_MEMBERSHIP,
        "field": field,
        "old_value_json": old_value_json,
        "new_value_json": new_value_json,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def split_memberships_for_review(
    *,
    store: Any,
    org_id: str,
    rows: Sequence[Any],
    observed_team_ids: Sequence[tuple[str, str]] | None = None,
    discovered_at: datetime | None = None,
) -> list[Any]:
    if (not rows and not observed_team_ids) or not _can_review(store):
        return list(rows)

    now = discovered_at or datetime.now(timezone.utc)
    incoming = [_row_dict(row, org_id=org_id) for row in rows]
    manual_memberships = await _manual_memberships(store, org_id=org_id)
    member_fallbacks = await _member_fallbacks(store, org_id=org_id)
    existing_changes = await _identity_changes(store, org_id=org_id)
    changes_by_id = {str(row.get("change_id") or ""): row for row in existing_changes}
    incoming_keys = {_member_key(row) for row in incoming}
    observed_scopes = _observed_scopes(incoming, observed_team_ids)
    conflicted_keys: set[tuple[str, str, str]] = set()
    refreshed_change_ids: set[str] = set()

    safe_rows: list[Any] = []
    rows_to_insert: list[dict[str, Any]] = []
    for original, row in zip(rows, incoming, strict=False):
        conflict = _conflict_for(
            row,
            manual_memberships=manual_memberships,
            member_fallbacks=member_fallbacks,
        )
        if conflict is None:
            safe_rows.append(original)
            continue

        conflicted_keys.add(_member_key(row))
        old_value_json = _canonical_json(conflict)
        new_value_json = _canonical_json(row)
        change_id = change_id_for_identity_membership(
            org_id=org_id,
            team_id=str(row["team_id"]),
            provider=str(row["provider"]),
            member_id=str(row["member_id"]),
            field=str(conflict["field"]),
            old_value_json=old_value_json,
            new_value_json=new_value_json,
        )
        existing = changes_by_id.get(change_id)
        if existing and existing.get("status") in DECIDED_STATUSES:
            continue
        refreshed_change_ids.add(change_id)
        rows_to_insert.append(
            {
                "org_id": org_id,
                "change_id": change_id,
                "entity_type": ENTITY_TYPE_IDENTITY,
                "entity_id": row["team_id"],
                "provider": row["provider"],
                "native_team_key": row["team_id"],
                "change_type": CHANGE_TYPE_MEMBERSHIP,
                "field": conflict["field"],
                "old_value_json": old_value_json,
                "new_value_json": new_value_json,
                "status": STATUS_PENDING,
                "first_seen_at": existing.get("first_seen_at") if existing else now,
                "last_seen_at": now,
                "decided_at": None,
                "decided_by": None,
                "updated_at": now,
            }
        )

    rows_to_insert.extend(
        _stale_status_rows(
            existing_changes,
            incoming_keys=incoming_keys,
            observed_scopes=observed_scopes,
            conflicted_keys=conflicted_keys,
            refreshed_change_ids=refreshed_change_ids,
            now=now,
        )
    )
    if rows_to_insert:
        await store.insert_team_drift_changes(rows_to_insert)
    return safe_rows


async def apply_identity_membership_change(
    *,
    store: Any,
    org_id: str,
    row: dict[str, Any],
    decided_at: datetime | None = None,
) -> None:
    now = decided_at or datetime.now(timezone.utc)
    new_value = _loads_json(row.get("new_value_json"))
    if not isinstance(new_value, dict):
        raise ValueError(
            "Cannot approve identity drift change without membership payload"
        )

    await store.insert_team_memberships(
        [{**new_value, "org_id": org_id, "updated_at": now}]
    )

    old_value = _loads_json(row.get("old_value_json"))
    if isinstance(old_value, dict):
        await _expire_conflict(store=store, org_id=org_id, conflict=old_value, now=now)

    from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
        ClickHouseTeamAdminService,
    )

    team_id = str(
        new_value.get("team_id") or row.get("team_id") or row.get("entity_id")
    )
    team_admin = ClickHouseTeamAdminService(store, org_id)
    facets = _membership_facets(new_value)
    if facets:
        await team_admin.add_members(team_id, sorted(facets))


def _can_review(store: Any) -> bool:
    return callable(getattr(store, "insert_team_drift_changes", None)) and (
        callable(getattr(store, "query_dicts", None))
        or getattr(store, "client", None) is not None
    )


async def _manual_memberships(store: Any, *, org_id: str) -> list[dict[str, Any]]:
    return await _query_dicts(
        store,
        """
        SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email,
               source, is_primary, specificity, priority, valid_from, valid_to, updated_at
        FROM team_memberships FINAL
        WHERE org_id = {org_id:String}
          AND source = 'manual'
          AND (valid_to IS NULL OR valid_to > now())
        """,
        {"org_id": org_id},
    )


async def _member_fallbacks(store: Any, *, org_id: str) -> list[dict[str, Any]]:
    return await _query_dicts(
        store,
        """
        SELECT org_id, provider, scope_type, scope_id, team_id, team_name, reason,
               priority, valid_from, valid_to, created_by, created_at, updated_at
        FROM manual_attribution_fallbacks FINAL
        WHERE org_id = {org_id:String}
          AND scope_type = 'member'
          AND (valid_to IS NULL OR valid_to > now())
        """,
        {"org_id": org_id},
    )


async def _identity_changes(store: Any, *, org_id: str) -> list[dict[str, Any]]:
    return await _query_dicts(
        store,
        """
        SELECT org_id, change_id, entity_id, provider, native_team_key, change_type,
               field, old_value_json, new_value_json, status, first_seen_at,
               last_seen_at, decided_at, decided_by, updated_at
        FROM team_drift_changes FINAL
        WHERE org_id = {org_id:String}
          AND entity_type = 'identity'
          AND change_type = 'membership_changed'
        """,
        {"org_id": org_id},
    )


async def _query_dicts(
    store: Any, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    query_dicts = getattr(store, "query_dicts", None)
    if callable(query_dicts):
        result = query_dicts(query, parameters)
        if inspect.isawaitable(result):
            result = await result
        return list(cast(Sequence[dict[str, Any]], result or []))

    client = store.client
    assert client is not None
    async with store._lock:
        result = await asyncio.to_thread(client.query, query, parameters=parameters)
    columns = tuple(getattr(result, "column_names", ()) or ())
    return [
        dict(zip(columns, record, strict=False)) for record in result.result_rows or []
    ]


def _conflict_for(
    row: dict[str, Any],
    *,
    manual_memberships: Sequence[dict[str, Any]],
    member_fallbacks: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    if str(row.get("source") or "") == "manual":
        return None
    provider = str(row.get("provider") or "")
    member_id = str(row.get("member_id") or "")
    team_id = str(row.get("team_id") or "")
    for manual in manual_memberships:
        if str(manual.get("provider") or "") != provider:
            continue
        if str(manual.get("member_id") or "") != member_id:
            continue
        if str(manual.get("team_id") or "") == team_id:
            continue
        return {"field": FIELD_TEAM_MEMBERSHIP, "manual_membership": manual}

    facets = _normalized_membership_facets(row)
    for fallback in member_fallbacks:
        if str(fallback.get("provider") or "") != provider:
            continue
        if _normalize_identity(fallback.get("scope_id")) not in facets:
            continue
        if str(fallback.get("team_id") or "") == team_id:
            continue
        return {"field": FIELD_MEMBER_FALLBACK, "manual_fallback": fallback}
    return None


async def _expire_conflict(
    *, store: Any, org_id: str, conflict: dict[str, Any], now: datetime
) -> None:
    if conflict.get("field") == FIELD_TEAM_MEMBERSHIP:
        manual = dict(conflict.get("manual_membership") or {})
        if not manual:
            return
        manual.update({"org_id": org_id, "valid_to": now, "updated_at": now})
        await store.insert_team_memberships([manual])
        return
    if conflict.get("field") == FIELD_MEMBER_FALLBACK:
        fallback = dict(conflict.get("manual_fallback") or {})
        if not fallback:
            return
        fallback.update({"org_id": org_id, "valid_to": now, "updated_at": now})
        await store.insert_manual_attribution_fallbacks([fallback])


def _row_dict(row: Any, *, org_id: str) -> dict[str, Any]:
    if isinstance(row, dict):
        data = dict(row)
    else:
        data = {
            key: getattr(row, key)
            for key in (
                "org_id",
                "provider",
                "team_id",
                "member_id",
                "raw_provider_user_id",
                "raw_email",
                "source",
                "is_primary",
                "specificity",
                "priority",
                "valid_from",
                "valid_to",
                "updated_at",
            )
            if hasattr(row, key)
        }
    data["org_id"] = str(data.get("org_id") or org_id)
    return data


def _membership_facets(row: dict[str, Any]) -> set[str]:
    facets = {
        str(value)
        for value in (
            row.get("member_id"),
            row.get("raw_provider_user_id"),
            row.get("raw_email"),
        )
        if value
    }
    return facets


def _normalized_membership_facets(row: dict[str, Any]) -> set[str]:
    return {_normalize_identity(value) for value in _membership_facets(row)}


def _normalize_identity(value: Any) -> str:
    return str(value or "").strip().lower()


def _member_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("provider") or ""),
        str(row.get("team_id") or ""),
        str(row.get("member_id") or ""),
    )


def _change_member_key(row: dict[str, Any]) -> tuple[str, str, str]:
    new_value = _loads_json(row.get("new_value_json"))
    if isinstance(new_value, dict):
        return _member_key(new_value)
    return (
        str(row.get("provider") or ""),
        str(row.get("entity_id") or row.get("team_id") or ""),
        "",
    )


def _observed_scopes(
    rows: Sequence[dict[str, Any]], observed_team_ids: Sequence[tuple[str, str]] | None
) -> set[tuple[str, str]]:
    scopes = {
        (str(row.get("provider") or ""), str(row.get("team_id") or ""))
        for row in rows
        if row.get("provider") and row.get("team_id")
    }
    scopes.update(
        (str(provider), str(team_id))
        for provider, team_id in (observed_team_ids or [])
        if provider and team_id
    )
    return scopes


def _stale_status_rows(
    rows: Sequence[dict[str, Any]],
    *,
    incoming_keys: set[tuple[str, str, str]],
    observed_scopes: set[tuple[str, str]],
    conflicted_keys: set[tuple[str, str, str]],
    refreshed_change_ids: set[str],
    now: datetime,
) -> list[dict[str, Any]]:
    status_rows: list[dict[str, Any]] = []
    for row in rows:
        change_id = str(row.get("change_id") or "")
        if row.get("status") != STATUS_PENDING or change_id in refreshed_change_ids:
            continue
        key = _change_member_key(row)
        scope = (key[0], key[1])
        if scope not in observed_scopes:
            continue
        if key not in incoming_keys:
            status = STATUS_RESOLVED
        elif key in conflicted_keys:
            status = STATUS_SUPERSEDED
        else:
            status = STATUS_RESOLVED
        status_rows.append(_status_row(row, status=status, now=now))
    return status_rows


def _status_row(row: dict[str, Any], *, status: str, now: datetime) -> dict[str, Any]:
    return {
        "org_id": row["org_id"],
        "change_id": row["change_id"],
        "entity_type": ENTITY_TYPE_IDENTITY,
        "entity_id": row.get("entity_id") or row.get("team_id"),
        "provider": row.get("provider") or "",
        "native_team_key": row.get("native_team_key"),
        "change_type": row.get("change_type") or CHANGE_TYPE_MEMBERSHIP,
        "field": row.get("field"),
        "old_value_json": row.get("old_value_json") or "null",
        "new_value_json": row.get("new_value_json") or "null",
        "status": status,
        "first_seen_at": row.get("first_seen_at") or now,
        "last_seen_at": now,
        "decided_at": None,
        "decided_by": None,
        "updated_at": now,
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _loads_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value
