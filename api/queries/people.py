from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from .client import query_dicts
from .sql_loader import load_sql


def _sql_params(value: Iterable[str]) -> List[str]:
    return [str(item) for item in value if item]


async def search_people(
    client: Any,
    *,
    query: str,
    limit: int,
) -> List[Dict[str, Any]]:
    sql = load_sql("people/people_search.sql")
    params = {"query": query, "limit": limit}
    return await query_dicts(client, sql, params)


async def resolve_person_identity(
    client: Any,
    *,
    person_id: str,
) -> Optional[str]:
    sql = load_sql("people/person_lookup.sql")
    rows = await query_dicts(client, sql, {"person_id": person_id})
    if not rows:
        return None
    return str(rows[0].get("identity_id") or "") or None


async def fetch_identity_coverage(
    client: Any,
    *,
    identities: Iterable[str],
) -> int:
    sql = load_sql("people/person_identity_coverage.sql")
    rows = await query_dicts(client, sql, {"identities": _sql_params(identities)})
    if not rows:
        return 0
    return int(rows[0].get("sources") or 0)


async def fetch_person_team_id(
    client: Any,
    *,
    identities: Iterable[str],
) -> Optional[str]:
    sql = load_sql("people/person_team.sql")
    rows = await query_dicts(client, sql, {"identities": _sql_params(identities)})
    if not rows:
        return None
    return str(rows[0].get("team_id") or "") or None


async def fetch_person_metric_value(
    client: Any,
    *,
    table: str,
    column: str,
    aggregator: str,
    identity_column: str,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
    extra_where: str = "",
) -> float:
    template = load_sql("people/person_summary_deltas.sql")
    sql = template.format(
        table=table,
        column=column,
        aggregator=aggregator,
        identity_column=identity_column,
        extra_where=extra_where,
    )
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
    }
    rows = await query_dicts(client, sql, params)
    if not rows:
        return 0.0
    return float(rows[0].get("value") or 0.0)


async def fetch_person_metric_series(
    client: Any,
    *,
    table: str,
    column: str,
    aggregator: str,
    identity_column: str,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
    extra_where: str = "",
) -> List[Dict[str, Any]]:
    template = load_sql("people/person_metric_timeseries.sql")
    sql = template.format(
        table=table,
        column=column,
        aggregator=aggregator,
        identity_column=identity_column,
        extra_where=extra_where,
    )
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
    }
    return await query_dicts(client, sql, params)


async def fetch_person_breakdown(
    client: Any,
    *,
    table: str,
    column: str,
    aggregator: str,
    identity_column: str,
    identities: Iterable[str],
    group_expr: str,
    join_clause: str = "",
    start_day: date,
    end_day: date,
    extra_where: str = "",
    limit: int = 12,
) -> List[Dict[str, Any]]:
    template = load_sql("people/person_metric_breakdowns.sql")
    sql = template.format(
        table=table,
        column=column,
        aggregator=aggregator,
        identity_column=identity_column,
        group_expr=group_expr,
        join_clause=join_clause,
        extra_where=extra_where,
    )
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
        "limit": limit,
    }
    return await query_dicts(client, sql, params)


async def fetch_person_work_mix(
    client: Any,
    *,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    sql = load_sql("people/person_summary_work_mix.sql")
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
    }
    return await query_dicts(client, sql, params)


async def fetch_person_flow_breakdown(
    client: Any,
    *,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    sql = load_sql("people/person_summary_flow_breakdown.sql")
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
    }
    return await query_dicts(client, sql, params)


async def fetch_person_collaboration(
    client: Any,
    *,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    sql = load_sql("people/person_summary_collaboration.sql")
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
    }
    return await query_dicts(client, sql, params)


async def fetch_person_pull_requests(
    client: Any,
    *,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
    limit: int,
    cursor: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    template = load_sql("people/person_drilldown_prs.sql")
    cursor_filter = ""
    if cursor is not None:
        cursor_filter = "AND created_at < %(cursor)s"
    sql = template.format(cursor_filter=cursor_filter)
    params = {
        "start_ts": start_day,
        "end_ts": end_day,
        "identities": _sql_params(identities),
        "limit": limit,
    }
    if cursor is not None:
        params["cursor"] = cursor
    return await query_dicts(client, sql, params)


async def fetch_person_issues(
    client: Any,
    *,
    identities: Iterable[str],
    start_day: date,
    end_day: date,
    limit: int,
    cursor: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    template = load_sql("people/person_drilldown_issues.sql")
    cursor_filter = ""
    if cursor is not None:
        cursor_filter = "AND completed_at < %(cursor)s"
    sql = template.format(cursor_filter=cursor_filter)
    params = {
        "start_day": start_day,
        "end_day": end_day,
        "identities": _sql_params(identities),
        "limit": limit,
    }
    if cursor is not None:
        params["cursor"] = cursor
    return await query_dicts(client, sql, params)
