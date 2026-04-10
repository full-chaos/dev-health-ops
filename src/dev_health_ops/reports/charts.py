"""Chart query planning and execution for report rendering."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.testops_schemas import ChartSpec
from dev_health_ops.reports.metric_registry import (
    MetricDefinition,
    get_metric_definition,
)

TIME_GROUPINGS = {
    "day": ("toDate(day)", "Date", "day"),
    "week": ("toStartOfWeek(day)", "Date", "week"),
    "month": ("toStartOfMonth(day)", "Date", "month"),
}

DIMENSION_GROUPINGS = {
    "team": ("team_id", "String", "team"),
    "repo": ("repo_id", "String", "repo"),
    "service": ("service_id", "String", "service"),
}


@dataclass(frozen=True)
class ChartResult:
    spec: ChartSpec
    data_points: list[dict[str, Any]]
    title: str
    empty: bool


def _aggregate_expression(metric: str, definition: MetricDefinition) -> str:
    if metric.endswith("_count") or definition.unit == "count":
        return f"sum({metric})"
    return f"avg({metric})"


def _dimension_available(definition: MetricDefinition, dimension: str) -> bool:
    return dimension in definition.dimensions


def _resolve_grouping(
    spec: ChartSpec, definition: MetricDefinition
) -> tuple[str, str, str, bool]:
    group_by = spec.group_by
    if group_by in TIME_GROUPINGS:
        expr, type_name, label = TIME_GROUPINGS[group_by]
        return expr, type_name, label, True
    if group_by in DIMENSION_GROUPINGS:
        expr, type_name, label = DIMENSION_GROUPINGS[group_by]
        if _dimension_available(definition, group_by):
            return expr, type_name, label, False
        return "'unscoped'", type_name, label, False
    if spec.chart_type in {"line", "heatmap"} and _dimension_available(
        definition, "day"
    ):
        expr, type_name, label = TIME_GROUPINGS["day"]
        return expr, type_name, label, True
    return "'total'", "String", "total", False


def build_chart_query(spec: ChartSpec) -> tuple[str, dict[str, Any]]:
    """Build ClickHouse SQL query from ChartSpec."""
    definition = get_metric_definition(spec.metric)
    if definition is None:
        raise ValueError(f"Unsupported chart metric: {spec.metric}")

    x_expr, x_type, _, x_is_temporal = _resolve_grouping(spec, definition)
    y_expr = _aggregate_expression(spec.metric, definition)

    params: dict[str, Any] = {}
    clauses = [f"{spec.metric} IS NOT NULL"]

    if spec.org_id:
        clauses.append("org_id = {org_id:String}")
        params["org_id"] = spec.org_id
    if spec.time_range_start is not None:
        clauses.append("day >= {time_range_start:Date}")
        params["time_range_start"] = spec.time_range_start
    if spec.time_range_end is not None:
        clauses.append("day <= {time_range_end:Date}")
        params["time_range_end"] = spec.time_range_end
    if spec.filter_teams and _dimension_available(definition, "team"):
        clauses.append("team_id IN {filter_teams:Array(String)}")
        params["filter_teams"] = spec.filter_teams
    if spec.filter_repos and _dimension_available(definition, "repo"):
        clauses.append("repo_id IN {filter_repos:Array(String)}")
        params["filter_repos"] = spec.filter_repos

    where_clause = " AND\n        ".join(clauses)
    order_by = "x" if x_is_temporal else "y DESC, x"

    query = f"""
    SELECT
        {x_expr} AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        {y_expr} AS y
    FROM {definition.source_table}
    WHERE
        {where_clause}
    GROUP BY x
    ORDER BY {order_by}
    """.strip()

    if (
        spec.chart_type in {"scorecard", "trend_delta", "table"}
        and spec.group_by is None
    ):
        query = f"""
        SELECT
            CAST('total', '{x_type}') AS x,
            CAST(NULL, 'Nullable(String)') AS group_value,
            {y_expr} AS y
        FROM {definition.source_table}
        WHERE
            {where_clause}
        """.strip()

    return query, params


def _normalize_x_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _client_query_dicts(
    client: Any, query: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    if hasattr(client, "query_dicts"):
        return client.query_dicts(query, params)
    result = client.query(query, parameters=params)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


async def execute_chart(spec: ChartSpec, client: Any) -> ChartResult:
    """Execute a chart spec against ClickHouse."""
    query, params = build_chart_query(spec)
    rows = await asyncio.to_thread(_client_query_dicts, client, query, params)
    data_points = [
        {
            "x": _normalize_x_value(row.get("x")),
            "y": row.get("y"),
            "group": row.get("group_value"),
        }
        for row in rows
    ]
    return ChartResult(
        spec=spec,
        data_points=data_points,
        title=spec.title or definition_title(spec),
        empty=not data_points,
    )


def definition_title(spec: ChartSpec) -> str:
    definition = get_metric_definition(spec.metric)
    if definition is None:
        return spec.metric.replace("_", " ").title()
    return spec.title or definition.display_name
