from __future__ import annotations

from datetime import date
from typing import Any

from dev_health_ops.clickhouse_dedup import dedup_from
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts
from .investment import PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE


def _bucket_expr(bucket: str) -> str:
    if bucket == "month":
        return "toStartOfMonth(day)"
    return "toStartOfWeek(day)"


async def fetch_quadrant_metric(
    sink: BaseMetricsSink,
    *,
    table: str,
    value_expr: str,
    start_day: date,
    end_day: date,
    bucket: str,
    entity_expr: str,
    label_expr: str,
    join_clause: str = "",
    where_clause: str = "",
    scope_filter: str = "",
    scope_params: dict[str, Any] | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    bucket_expr = _bucket_expr(bucket)
    join_sql = f"\n{join_clause}" if join_clause else ""
    where_sql = f"\n{where_clause}" if where_clause else ""
    scope_sql = f"\n{scope_filter}" if scope_filter else ""
    query = f"""
        SELECT
            {bucket_expr} AS bucket,
            {entity_expr} AS entity_id,
            {label_expr} AS entity_label,
            {value_expr} AS value
        FROM {dedup_from(table)}
        {join_sql}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {where_sql}
          AND org_id = %(org_id)s
        {scope_sql}
        GROUP BY bucket, entity_id, entity_label
        ORDER BY bucket
    """
    params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    if scope_params:
        params.update(scope_params)
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)


async def fetch_work_item_team_quadrant_metric(
    sink: BaseMetricsSink,
    *,
    metric: str,
    start_day: date,
    end_day: date,
    bucket: str,
    org_id: str = "",
) -> list[dict[str, Any]]:
    bucket_expr = _bucket_expr(bucket)
    if metric == "throughput":
        value_expr = "uniqExact(work_item_id)"
        metric_filter = ""
    elif metric == "cycle_time":
        value_expr = "avg(cycle_time_hours)"
        metric_filter = "AND cycle_time_hours IS NOT NULL"
    else:
        raise ValueError(f"Unsupported attributed team quadrant metric: {metric}")

    query = f"""
        WITH team_activity AS (
            SELECT
                wct.day,
                wct.work_item_id,
                wct.cycle_time_hours,
                t.team_id AS team_id,
                t.team_name AS team_name
            FROM work_item_cycle_times AS wct FINAL
            INNER JOIN {PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE} AS t
              ON t.work_item_id = wct.work_item_id
            WHERE wct.day >= %(start_day)s AND wct.day < %(end_day)s
              AND wct.org_id = %(org_id)s
              AND t.team_id IS NOT NULL
              AND t.team_id != ''
              {metric_filter}
        )
        SELECT
            {bucket_expr} AS bucket,
            toString(team_id) AS entity_id,
            ifNull(nullIf(any(team_name), ''), toString(team_id)) AS entity_label,
            {value_expr} AS value
        FROM team_activity
        GROUP BY bucket, entity_id
        ORDER BY bucket
    """
    params: dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    params["org_id"] = org_id
    return await query_dicts(sink, query, params)
