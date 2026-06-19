from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.db import resolve_db_uri, resolve_sink_uri
from dev_health_ops.models.settings import TeamMapping
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.utils.cli import validate_sink

logger = logging.getLogger(__name__)


def _team_value(team: Any, field: str, default: Any = None) -> Any:
    if isinstance(team, dict):
        return team.get(field, default)
    return getattr(team, field, default)


def _team_list(team: Any, field: str) -> list[str]:
    value = _team_value(team, field, []) or []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


async def _clickhouse_org_teams(org_id: str, db_url: str) -> list[Any]:
    async with ClickHouseStore(db_url) as store:
        store.org_id = org_id
        teams = await store.get_all_teams()
    return [
        team for team in teams if str(_team_value(team, "org_id", "") or "") == org_id
    ]


async def find_unmapped_clickhouse_teams(
    org_id: str,
    *,
    db_url: str,
    postgres_db_url: str,
) -> list[str]:
    ch_teams = await _clickhouse_org_teams(org_id, db_url)
    ch_team_ids = {
        str(_team_value(team, "id", "") or "").strip()
        for team in ch_teams
        if str(_team_value(team, "id", "") or "").strip()
    }
    if not ch_team_ids:
        return []

    engine = create_async_engine(postgres_db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            result = await session.execute(
                select(TeamMapping.team_id).where(
                    TeamMapping.org_id == org_id,
                    TeamMapping.team_id.in_(ch_team_ids),
                )
            )
            mapped_ids = {str(team_id) for team_id in result.scalars()}
    finally:
        await engine.dispose()
    return sorted(ch_team_ids - mapped_ids)


async def reconcile_clickhouse_teams_to_postgres(
    org_id: str,
    *,
    db_url: str,
    postgres_db_url: str,
) -> dict[str, Any]:
    ch_teams = await _clickhouse_org_teams(org_id, db_url)
    engine = create_async_engine(postgres_db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    created = 0
    existing = 0
    unmanaged: list[str] = []
    missing_before: list[str] = []

    try:
        async with factory() as session:
            for team in ch_teams:
                team_id = str(_team_value(team, "id", "") or "").strip()
                if not team_id:
                    unmanaged.append(
                        str(_team_value(team, "name", "<unnamed>") or "<unnamed>")
                    )
                    continue

                mapping = await session.scalar(
                    select(TeamMapping).where(
                        TeamMapping.org_id == org_id,
                        TeamMapping.team_id == team_id,
                    )
                )
                if mapping is not None:
                    existing += 1
                    continue

                missing_before.append(team_id)
                session.add(
                    TeamMapping(
                        team_id=team_id,
                        name=str(_team_value(team, "name", team_id) or team_id),
                        org_id=org_id,
                        description=_team_value(team, "description", None),
                        repo_patterns=_team_list(team, "repo_patterns"),
                        project_keys=_team_list(team, "project_keys"),
                        extra_data={
                            "provider_type": "clickhouse",
                            "provider_team_id": team_id,
                            "sync_source": "teams-reconcile",
                        },
                        managed_fields=[],
                        sync_policy=2,
                        is_active=True,
                    )
                )
                created += 1
            await session.commit()
    finally:
        await engine.dispose()

    from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

    bridged = bridge_teams_to_clickhouse(
        org_id=org_id,
        db_url=db_url,
        postgres_db_url=postgres_db_url,
    )
    missing_after = await find_unmapped_clickhouse_teams(
        org_id,
        db_url=db_url,
        postgres_db_url=postgres_db_url,
    )
    return {
        "org_id": org_id,
        "clickhouse_teams": len(ch_teams),
        "created": created,
        "existing": existing,
        "unmanaged": unmanaged,
        "missing_before": missing_before,
        "missing_after": missing_after,
        "bridged": bridged,
    }


def reconcile_teams(ns: argparse.Namespace) -> int:
    validate_sink(ns)
    org_id = str(getattr(ns, "org", "") or "").strip()
    result = asyncio.run(
        reconcile_clickhouse_teams_to_postgres(
            org_id,
            db_url=resolve_sink_uri(ns),
            postgres_db_url=resolve_db_uri(ns),
        )
    )
    logger.info(
        "teams reconcile complete: org=%s ch=%d created=%d existing=%d bridged=%d missing_after=%d unmanaged=%d",
        org_id,
        result["clickhouse_teams"],
        result["created"],
        result["existing"],
        result["bridged"],
        len(result["missing_after"]),
        len(result["unmanaged"]),
    )
    if result["unmanaged"]:
        logger.warning(
            "teams reconcile skipped unmanaged ClickHouse rows without team ids: %s",
            ", ".join(result["unmanaged"]),
        )
    return 0 if not result["missing_after"] else 1


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    teams = subparsers.add_parser("teams", help="Team catalog operations.")
    teams_subparsers = teams.add_subparsers(dest="teams_command", required=True)
    reconcile = teams_subparsers.add_parser(
        "reconcile",
        help="Reconcile org-scoped ClickHouse teams into Postgres TeamMappings.",
    )
    reconcile.set_defaults(func=reconcile_teams)
