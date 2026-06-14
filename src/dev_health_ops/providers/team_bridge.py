from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

from sqlalchemy import select

from dev_health_ops.db import (
    get_postgres_session_sync,
    get_postgres_session_sync_for_uri,
)
from dev_health_ops.models.settings import IdentityMapping, TeamMapping
from dev_health_ops.storage.clickhouse import ClickHouseStore


def _members_by_team(identity_mappings: Any) -> dict[str, set[str]]:
    """Collect member identities per team from confirmed identity mappings.

    The membership-based ``TeamResolver`` matches work-item assignees
    (emails, provider logins/account ids) against ``teams.members`` in
    ClickHouse, so every confirmed identity facet is included: email,
    canonical_id, and all provider identities. ``display_name`` is used
    only when no email exists (mirrors the worker ``sync_teams`` path and
    avoids false-positive matches on common names).
    """
    members: dict[str, set[str]] = {}
    for ident in identity_mappings:
        identities: set[str] = set()
        email = getattr(ident, "email", None)
        if email:
            identities.add(str(email))
        canonical_id = getattr(ident, "canonical_id", None)
        if canonical_id:
            identities.add(str(canonical_id))
        for values in (getattr(ident, "provider_identities", None) or {}).values():
            if isinstance(values, list):
                identities.update(str(v) for v in values if v)
            elif values:
                identities.add(str(values))
        if not email:
            display_name = getattr(ident, "display_name", None)
            if display_name:
                identities.add(str(display_name))
        if not identities:
            continue
        for team_id in getattr(ident, "team_ids", None) or []:
            key = str(team_id).strip()
            if key:
                members.setdefault(key, set()).update(identities)
    return members


def _parse_json_array(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(v) for v in parsed if v is not None]
        except Exception:
            return []
    return []


def _clickhouse_uri(db_url: str | None = None) -> str:
    if db_url:
        return db_url
    uri = os.getenv("CLICKHOUSE_URI")
    if not uri:
        raise RuntimeError("Missing CLICKHOUSE_URI for team bridge")
    return uri


def bridge_teams_to_clickhouse(
    org_id: str | None = None,
    db_url: str | None = None,
    postgres_db_url: str | None = None,
) -> int:
    teams_payload: list[dict[str, Any]] = []

    session_context = (
        get_postgres_session_sync_for_uri(postgres_db_url)
        if postgres_db_url
        else get_postgres_session_sync()
    )
    with session_context as session:
        mappings = (
            session.execute(
                select(TeamMapping).where(
                    TeamMapping.org_id == org_id,
                    TeamMapping.is_active.is_(True),
                )
            )
            .scalars()
            .all()
        )

        identity_mappings = (
            session.execute(
                select(IdentityMapping).where(
                    IdentityMapping.org_id == org_id,
                    IdentityMapping.is_active.is_(True),
                )
            )
            .scalars()
            .all()
        )
        members_by_team = _members_by_team(identity_mappings)

        for mapping in mappings:
            team_id = str(mapping.team_id or "").strip()
            if not team_id:
                continue
            teams_payload.append(
                {
                    "id": team_id,
                    "team_uuid": uuid.uuid5(
                        uuid.NAMESPACE_URL, f"team:{org_id}:{team_id}"
                    ),
                    "name": str(mapping.name or team_id),
                    "description": mapping.description,
                    "project_keys": _parse_json_array(mapping.project_keys),
                    "repo_patterns": _parse_json_array(mapping.repo_patterns),
                    "members": sorted(members_by_team.get(team_id, ())),
                    "is_active": 1,
                    "org_id": org_id,
                    "updated_at": mapping.updated_at,
                }
            )

    async def _run() -> None:
        async with ClickHouseStore(_clickhouse_uri(db_url)) as store:
            await store.insert_teams(teams_payload)

    asyncio.run(_run())
    return len(teams_payload)
