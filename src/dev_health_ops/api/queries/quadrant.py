from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from .client import query_dicts
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


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
    scope_params: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
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
        FROM {table}
        {join_sql}
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {where_sql}
        {scope_sql}
        GROUP BY bucket, entity_id, entity_label
        ORDER BY bucket
    """
    params: Dict[str, Any] = {"start_day": start_day, "end_day": end_day}
    if scope_params:
        params.update(scope_params)
    return await query_dicts(sink, query, params)
