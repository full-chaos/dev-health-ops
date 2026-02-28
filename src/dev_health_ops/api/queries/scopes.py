from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts


def parse_uuid(value: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


async def resolve_repo_id(
    sink: BaseMetricsSink, repo_ref: str, *, org_id: str = ""
) -> str | None:
    repo_uuid = parse_uuid(repo_ref)
    if repo_uuid:
        query = """
            SELECT id
            FROM repos
            WHERE id = %(repo_id)s
              AND org_id = %(org_id)s
            LIMIT 1
        """
        rows = await query_dicts(
            sink,
            query,
            {"repo_id": str(repo_uuid), "org_id": org_id},
        )
        if not rows:
            return None
        return str(rows[0]["id"])
    query = """
        SELECT id
        FROM repos
        WHERE repo = %(repo_name)s
          AND org_id = %(org_id)s
        LIMIT 1
    """
    params = {"repo_name": repo_ref, "org_id": org_id}
    rows = await query_dicts(sink, query, params)
    if not rows:
        return None
    return str(rows[0]["id"])


async def resolve_repo_ids(
    sink: BaseMetricsSink, repo_refs: Iterable[str], *, org_id: str = ""
) -> list[str]:
    resolved: list[str] = []
    for repo_ref in repo_refs:
        if not repo_ref:
            continue
        repo_uuid = parse_uuid(repo_ref)
        if repo_uuid:
            verified_repo_id = await resolve_repo_id(sink, repo_ref, org_id=org_id)
            if verified_repo_id:
                resolved.append(verified_repo_id)
            continue
        repo_id = await resolve_repo_id(sink, repo_ref, org_id=org_id)
        if repo_id:
            resolved.append(repo_id)
    return resolved


async def resolve_repo_ids_for_teams(
    sink: BaseMetricsSink,
    team_ids: Iterable[str],
    *,
    org_id: str = "",
) -> list[str]:
    team_list = [team_id for team_id in team_ids if team_id]
    if not team_list:
        return []
    query = """
        SELECT distinct repo_id AS id
        FROM user_metrics_daily
        WHERE team_id IN %(team_ids)s
          AND org_id = %(org_id)s
    """
    params = {"team_ids": team_list, "org_id": org_id}
    rows = await query_dicts(sink, query, params)
    return [str(row.get("id")) for row in rows if row.get("id")]


def build_scope_filter(
    scope_type: str,
    scope_id: str,
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> tuple[str, dict[str, Any]]:
    if scope_type == "team" and scope_id:
        return f" AND {team_column} = %(scope_id)s", {"scope_id": scope_id}
    if scope_type == "repo" and scope_id:
        return f" AND {repo_column} = %(repo_id)s", {"repo_id": scope_id}
    return "", {}


def build_scope_filter_multi(
    scope_level: str,
    scope_ids: list[str],
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> tuple[str, dict[str, Any]]:
    if not scope_ids:
        return "", {}
    if scope_level == "team":
        return f" AND {team_column} IN %(scope_ids)s", {"scope_ids": scope_ids}
    if scope_level == "repo":
        return f" AND {repo_column} IN %(scope_ids)s", {"scope_ids": scope_ids}
    return "", {}
