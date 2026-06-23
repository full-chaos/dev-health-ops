from __future__ import annotations

from datetime import datetime
from typing import Any

from dev_health_ops.api.graphql.authz import require_org_id
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.outputs import (
    FeatureFlagEventItem,
    FeatureFlagEventsResult,
    FeatureFlagItem,
    FeatureFlagRegistryResult,
)
from dev_health_ops.work_graph.ids import generate_feature_flag_id

FEATURE_FLAG_NOT_MATERIALIZED = "FEATURE_FLAG_NOT_MATERIALIZED"
FEATURE_FLAG_EVENT_NOT_MATERIALIZED = "FEATURE_FLAG_EVENT_NOT_MATERIALIZED"

FEATURE_FLAG_LIMIT_MAX = 1000


def _clamp_limit(limit: int) -> int:
    """Bound a caller-supplied limit to a safe 1..MAX range.

    Negative values error in ClickHouse and arbitrarily large values force
    expensive org-wide sorts, so clamp before building the query.
    """
    return max(1, min(int(limit), FEATURE_FLAG_LIMIT_MAX))


def _isoformat(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _nullable_isoformat(value: Any) -> str | None:
    if value is None:
        return None
    return _isoformat(value)


def _is_missing_clickhouse_table_error(exc: BaseException, table_name: str) -> bool:
    from dev_health_ops.api.graphql.resolvers.work_graph import _unknown_table_names

    text = str(exc)
    is_unknown_table = (
        getattr(exc, "code", None) == 60
        or "UNKNOWN_TABLE" in text
        or "code: 60" in text
    )
    return is_unknown_table and table_name in _unknown_table_names(text)


def _empty_feature_flags_result(
    degraded_reason: str | None = None,
) -> FeatureFlagRegistryResult:
    return FeatureFlagRegistryResult(
        flags=[], total_count=0, degraded_reason=degraded_reason
    )


def _empty_feature_flag_events_result(
    degraded_reason: str | None = None,
) -> FeatureFlagEventsResult:
    return FeatureFlagEventsResult(
        events=[], total_count=0, degraded_reason=degraded_reason
    )


async def resolve_feature_flags(
    context: GraphQLContext,
    *,
    provider: str | None = None,
    project: str | None = None,
    include_archived: bool = False,
    limit: int = 1000,
) -> FeatureFlagRegistryResult:
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    where_clauses = ["org_id = %(org_id)s"]
    params: dict[str, Any] = {"org_id": org_id, "limit": _clamp_limit(limit)}
    if provider is not None:
        where_clauses.append("provider = %(provider)s")
        params["provider"] = provider
    if project is not None:
        where_clauses.append("project_key = %(project)s")
        params["project"] = project
    if not include_archived:
        where_clauses.append("archived_at IS NULL")

    query = f"""
        SELECT
            provider,
            flag_key,
            project_key,
            flag_type,
            created_at,
            archived_at
        FROM feature_flag FINAL
        WHERE {" AND ".join(where_clauses)}
        ORDER BY provider, project_key, flag_key
        LIMIT %(limit)s
    """

    count_query = f"""
        SELECT count() AS total
        FROM feature_flag FINAL
        WHERE {" AND ".join(where_clauses)}
    """
    count_params = {k: v for k, v in params.items() if k != "limit"}

    try:
        rows = await query_dicts(client, query, params)
        count_rows = await query_dicts(client, count_query, count_params)
    except Exception as exc:
        if _is_missing_clickhouse_table_error(exc, "feature_flag"):
            return _empty_feature_flags_result(FEATURE_FLAG_NOT_MATERIALIZED)
        raise

    flags = [
        FeatureFlagItem(
            flag_id=generate_feature_flag_id(
                org_id,
                str(row.get("provider") or ""),
                str(row.get("project_key") or ""),
                str(row.get("flag_key") or ""),
            ),
            flag_key=str(row.get("flag_key") or ""),
            provider=str(row.get("provider") or ""),
            project_key=str(row.get("project_key") or ""),
            flag_type=str(row.get("flag_type") or ""),
            created_at=_isoformat(row.get("created_at")),
            archived_at=_nullable_isoformat(row.get("archived_at")),
        )
        for row in rows
    ]
    total_count = int(count_rows[0]["total"]) if count_rows else 0
    return FeatureFlagRegistryResult(flags=flags, total_count=total_count)


async def resolve_feature_flag_events(
    context: GraphQLContext,
    *,
    flag_key: str | None = None,
    environment: str | None = None,
    limit: int = 1000,
) -> FeatureFlagEventsResult:
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    where_clauses = ["org_id = %(org_id)s"]
    params: dict[str, Any] = {"org_id": org_id, "limit": _clamp_limit(limit)}
    if flag_key is not None:
        where_clauses.append("flag_key = %(flag_key)s")
        params["flag_key"] = flag_key
    if environment is not None:
        where_clauses.append("environment = %(environment)s")
        params["environment"] = environment

    query = f"""
        SELECT
            flag_key,
            event_type,
            prev_state,
            next_state,
            actor_type,
            environment,
            event_ts
        FROM feature_flag_event
        WHERE {" AND ".join(where_clauses)}
        ORDER BY event_ts ASC
        LIMIT %(limit)s
    """

    count_query = f"""
        SELECT count() AS total
        FROM feature_flag_event
        WHERE {" AND ".join(where_clauses)}
    """
    count_params = {k: v for k, v in params.items() if k != "limit"}

    try:
        rows = await query_dicts(client, query, params)
        count_rows = await query_dicts(client, count_query, count_params)
    except Exception as exc:
        if _is_missing_clickhouse_table_error(exc, "feature_flag_event"):
            return _empty_feature_flag_events_result(
                FEATURE_FLAG_EVENT_NOT_MATERIALIZED
            )
        raise

    events = [
        FeatureFlagEventItem(
            flag_key=str(row.get("flag_key") or ""),
            event_type=str(row.get("event_type") or ""),
            prev_state=str(row.get("prev_state") or ""),
            next_state=str(row.get("next_state") or ""),
            actor_type=str(row.get("actor_type") or ""),
            environment=str(row.get("environment") or ""),
            event_ts=_isoformat(row.get("event_ts")),
        )
        for row in rows
    ]
    total_count = int(count_rows[0]["total"]) if count_rows else 0
    return FeatureFlagEventsResult(events=events, total_count=total_count)
