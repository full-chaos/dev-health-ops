"""Resolver for capacity planning queries."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import CapacityForecastFilterInput, CapacityForecastInput
from ..models.outputs import (
    CapacityForecast,
    CapacityForecastConnection,
    CapacityForecastEdge,
    PageInfo,
)


logger = logging.getLogger(__name__)


def _row_to_forecast(row: Dict[str, Any]) -> CapacityForecast:
    return CapacityForecast(
        forecast_id=str(row.get("forecast_id", "")),
        computed_at=str(row.get("computed_at", "")),
        team_id=row.get("team_id"),
        work_scope_id=row.get("work_scope_id"),
        backlog_size=int(row.get("backlog_size", 0)),
        target_items=row.get("target_items"),
        target_date=row.get("target_date"),
        p50_date=row.get("p50_date"),
        p85_date=row.get("p85_date"),
        p95_date=row.get("p95_date"),
        p50_days=row.get("p50_days"),
        p85_days=row.get("p85_days"),
        p95_days=row.get("p95_days"),
        p50_items=row.get("p50_items"),
        p85_items=row.get("p85_items"),
        p95_items=row.get("p95_items"),
        throughput_mean=float(row.get("throughput_mean", 0.0)),
        throughput_stddev=float(row.get("throughput_stddev", 0.0)),
        history_days=int(row.get("history_days", 0)),
        insufficient_history=bool(row.get("insufficient_history", False)),
        high_variance=bool(row.get("high_variance", False)),
    )


def _result_to_forecast(result: Any) -> CapacityForecast:
    return CapacityForecast(
        forecast_id=result.forecast_id,
        computed_at=str(result.computed_at),
        team_id=result.team_id,
        work_scope_id=result.work_scope_id,
        backlog_size=result.backlog_size,
        target_items=result.target_items,
        target_date=result.target_date,
        p50_date=result.p50_date,
        p85_date=result.p85_date,
        p95_date=result.p95_date,
        p50_days=result.p50_days,
        p85_days=result.p85_days,
        p95_days=result.p95_days,
        p50_items=result.p50_items,
        p85_items=result.p85_items,
        p95_items=result.p95_items,
        throughput_mean=result.throughput_mean,
        throughput_stddev=result.throughput_stddev,
        history_days=result.history_days,
        insufficient_history=result.insufficient_history,
        high_variance=result.high_variance,
    )


async def resolve_capacity_forecast(
    context: GraphQLContext,
    input: Optional[CapacityForecastInput] = None,
) -> Optional[CapacityForecast]:
    from dev_health_ops.metrics.compute_capacity import forecast_capacity
    from dev_health_ops.metrics.job_capacity import (
        get_backlog_from_sink,
        load_throughput_from_sink,
    )
    from dev_health_ops.metrics.sinks.factory import create_sink

    require_org_id(context)

    if context.client is None:
        raise RuntimeError("Database client not available")

    team_id = input.team_id if input else None
    work_scope_id = input.work_scope_id if input else None
    target_items = input.target_items if input else None
    target_date = input.target_date if input else None
    history_days = input.history_days if input else 90
    simulations = input.simulations if input else 10000

    sink = create_sink(context.db_url)
    try:
        history = await load_throughput_from_sink(
            sink,
            team_id=team_id,
            work_scope_id=work_scope_id,
            history_days=history_days,
        )

        if not history.daily_throughputs:
            logger.warning(
                "No throughput history for team=%s, scope=%s",
                team_id,
                work_scope_id,
            )
            return None

        backlog = await get_backlog_from_sink(
            sink, team_id=team_id, work_scope_id=work_scope_id
        )

        items = target_items if target_items else backlog
        if items <= 0:
            logger.warning(
                "No target items for team=%s, scope=%s",
                team_id,
                work_scope_id,
            )
            return None

        result = forecast_capacity(
            history=history,
            target_items=items,
            target_date=target_date,
            backlog_size=backlog,
            team_id=team_id,
            work_scope_id=work_scope_id,
            simulations=simulations,
        )

        return _result_to_forecast(result)

    except Exception as e:
        logger.error("Failed to compute capacity forecast: %s", e)
        raise
    finally:
        sink.close()


async def resolve_capacity_forecasts(
    context: GraphQLContext,
    filters: Optional[CapacityForecastFilterInput] = None,
) -> CapacityForecastConnection:
    from dev_health_ops.api.queries.client import query_dicts

    require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    limit = filters.limit if filters else 10
    params: Dict[str, Any] = {"limit": int(limit)}
    where_clauses: List[str] = []

    if filters:
        if filters.team_id:
            where_clauses.append("team_id = %(team_id)s")
            params["team_id"] = filters.team_id

        if filters.work_scope_id:
            where_clauses.append("work_scope_id = %(work_scope_id)s")
            params["work_scope_id"] = filters.work_scope_id

        if filters.from_date:
            where_clauses.append("toDate(computed_at) >= %(from_date)s")
            params["from_date"] = filters.from_date.isoformat()

        if filters.to_date:
            where_clauses.append("toDate(computed_at) <= %(to_date)s")
            params["to_date"] = filters.to_date.isoformat()

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT
            forecast_id,
            computed_at,
            team_id,
            work_scope_id,
            backlog_size,
            target_items,
            target_date,
            p50_date,
            p85_date,
            p95_date,
            p50_days,
            p85_days,
            p95_days,
            p50_items,
            p85_items,
            p95_items,
            throughput_mean,
            throughput_stddev,
            history_days,
            insufficient_history,
            high_variance
        FROM capacity_forecasts
        {where_sql}
        ORDER BY computed_at DESC
        LIMIT %(limit)s
    """

    rows = await query_dicts(client, query, params)
    forecasts = [_row_to_forecast(row) for row in rows]

    edges = [CapacityForecastEdge(node=f, cursor=f.forecast_id) for f in forecasts]

    return CapacityForecastConnection(
        edges=edges,
        total_count=len(forecasts),
        page_info=PageInfo(
            has_next_page=len(forecasts) == limit,
            has_previous_page=False,
            start_cursor=edges[0].cursor if edges else None,
            end_cursor=edges[-1].cursor if edges else None,
        ),
    )
