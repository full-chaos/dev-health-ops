from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any, List

from sqlalchemy import select

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.models.settings import TeamMapping
from dev_health_ops.storage.clickhouse import ClickHouseStore


def _parse_json_array(value: Any) -> List[str]:
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


def _clickhouse_uri() -> str:
    uri = (
        os.getenv("CLICKHOUSE_URI")
        or os.getenv("DATABASE_URI")
        or os.getenv("DATABASE_URL")
    )
    if not uri:
        raise RuntimeError("Missing CLICKHOUSE_URI or DATABASE_URI for team bridge")
    return uri


def bridge_teams_to_clickhouse(org_id: str = "default") -> int:
    teams_payload: List[dict[str, Any]] = []

    with get_postgres_session_sync() as session:
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
                    "members": [],
                    "is_active": 1,
                    "org_id": org_id,
                    "updated_at": mapping.updated_at,
                }
            )

    async def _run() -> None:
        async with ClickHouseStore(_clickhouse_uri()) as store:
            await store.ensure_tables()
            await store.insert_teams(teams_payload)

    asyncio.run(_run())
    return len(teams_payload)
