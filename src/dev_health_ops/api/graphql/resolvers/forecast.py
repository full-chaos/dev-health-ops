"""Resolver for throughput-based capacity forecast queries."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.metrics.forecast import (
    RiskOverlay,
    ThroughputForecastResult,
    forecast_throughput_capacity,
)
from dev_health_ops.utils.datetime import utc_today

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import ThroughputForecastInput
from ..models.outputs import (
    ThroughputForecast,
    ThroughputRiskOverlay,
    ThroughputRollingWindow,
)


def _risk_to_output(risk: RiskOverlay) -> ThroughputRiskOverlay:
    return ThroughputRiskOverlay(
        kind=risk.kind.value,
        score=risk.score,
        label=risk.label,
        value=risk.value,
        threshold=risk.threshold,
        active=risk.active,
    )


def _result_to_output(result: ThroughputForecastResult) -> ThroughputForecast:
    if result.team_id is None:
        raise ValueError("team_id is required for throughput forecasts")
    return ThroughputForecast(
        forecast_id=result.forecast_id,
        computed_at=result.computed_at.isoformat(),
        team_id=result.team_id,
        work_scope_id=result.work_scope_id,
        backlog_size=result.backlog_size,
        history_weeks=result.history_weeks,
        p50_weeks=result.p50_weeks,
        p75_weeks=result.p75_weeks,
        p90_weeks=result.p90_weeks,
        rolling_windows=[
            ThroughputRollingWindow(
                window_weeks=window.window_weeks,
                mean_weekly_throughput=window.mean_weekly_throughput,
                sample_count=len(window.samples),
                insufficient_history=window.insufficient_history,
            )
            for window in result.rolling_windows
        ],
        primary_risk=_risk_to_output(result.primary_risk),
        wip_congestion=_risk_to_output(result.wip_congestion),
        review_bottleneck=_risk_to_output(result.review_bottleneck),
        incident_load=_risk_to_output(result.incident_load),
        insufficient_history=result.insufficient_history,
    )


async def _load_throughput_history(
    context: GraphQLContext,
    *,
    team_id: str,
    work_scope_id: str | None,
    history_weeks: int,
) -> ThroughputHistory:
    start_date = utc_today() - timedelta(weeks=history_weeks)
    conditions = ["day >= {start_date:Date}", "team_id = {team_id:String}"]
    params: dict[str, Any] = {
        "start_date": start_date,
        "team_id": team_id,
        "org_id": context.org_id,
    }
    if work_scope_id:
        conditions.append("work_scope_id = {work_scope_id:String}")
        params["work_scope_id"] = work_scope_id

    rows = await query_dicts(
        context.client,
        f"""
        SELECT day, sum(items_completed) AS items_completed
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                argMax(items_completed, computed_at) AS items_completed
            FROM work_item_metrics_daily
            WHERE {" AND ".join(conditions)}
            GROUP BY day, provider, work_scope_id, team_id
        )
        GROUP BY day
        ORDER BY day
        """,
        params,
    )
    return ThroughputHistory(
        [
            ThroughputSample(
                day=row["day"]
                if isinstance(row["day"], date)
                else date.fromisoformat(str(row["day"])),
                items_completed=int(row.get("items_completed") or 0),
                team_id=team_id,
                work_scope_id=work_scope_id,
            )
            for row in rows
        ]
    )


async def _load_work_item_overlay(
    context: GraphQLContext,
    *,
    team_id: str,
    work_scope_id: str | None,
    history_weeks: int,
) -> tuple[float, float]:
    start_date = utc_today() - timedelta(weeks=history_weeks)
    conditions = ["day >= {start_date:Date}", "team_id = {team_id:String}"]
    params: dict[str, Any] = {
        "start_date": start_date,
        "team_id": team_id,
        "org_id": context.org_id,
    }
    if work_scope_id:
        conditions.append("work_scope_id = {work_scope_id:String}")
        params["work_scope_id"] = work_scope_id

    rows = await query_dicts(
        context.client,
        f"""
        SELECT
            avg(wip_count_end_of_day) AS average_wip,
            argMax(wip_count_end_of_day, day) AS current_wip
        FROM (
            SELECT
                day,
                sum(wip_count_end_of_day) AS wip_count_end_of_day
            FROM (
                SELECT
                    day,
                    provider,
                    work_scope_id,
                    team_id,
                    argMax(wip_count_end_of_day, computed_at) AS wip_count_end_of_day
                FROM work_item_metrics_daily
                WHERE {" AND ".join(conditions)}
                GROUP BY day, provider, work_scope_id, team_id
            )
            GROUP BY day
        )
        """,
        params,
    )
    row = rows[0] if rows else {}
    return float(row.get("current_wip") or 0.0), float(row.get("average_wip") or 0.0)


async def _load_review_overlay(
    context: GraphQLContext,
    *,
    history_weeks: int,
) -> float:
    start_date = utc_today() - timedelta(weeks=history_weeks)
    rows = await query_dicts(
        context.client,
        """
        SELECT avg(pr_first_review_p50_hours) AS review_latency_hours
        FROM (
            SELECT
                repo_id,
                day,
                argMax(pr_first_review_p50_hours, computed_at) AS pr_first_review_p50_hours
            FROM repo_metrics_daily
            WHERE day >= {start_date:Date}
            GROUP BY repo_id, day
        )
        WHERE pr_first_review_p50_hours IS NOT NULL
        """,
        {"start_date": start_date, "org_id": context.org_id},
    )
    return float((rows[0] if rows else {}).get("review_latency_hours") or 0.0)


async def _load_incident_overlay(
    context: GraphQLContext,
    *,
    history_weeks: int,
) -> float:
    start_date = utc_today() - timedelta(weeks=history_weeks)
    rows = await query_dicts(
        context.client,
        """
        SELECT sum(incidents_count) / greatest(dateDiff('week', {start_date:Date}, today()), 1) AS incident_count
        FROM (
            SELECT
                repo_id,
                day,
                argMax(incidents_count, computed_at) AS incidents_count
            FROM incident_metrics_daily
            WHERE day >= {start_date:Date}
            GROUP BY repo_id, day
        )
        """,
        {"start_date": start_date, "org_id": context.org_id},
    )
    return float((rows[0] if rows else {}).get("incident_count") or 0.0)


async def resolve_throughput_forecast(
    context: GraphQLContext,
    input: ThroughputForecastInput,
) -> ThroughputForecast | None:
    """Compute a throughput-based capacity forecast on demand."""
    require_org_id(context)
    if context.client is None:
        raise RuntimeError("Database client not available")

    history = await _load_throughput_history(
        context,
        team_id=input.team_id,
        work_scope_id=input.work_scope_id,
        history_weeks=input.history_weeks,
    )
    if not history.samples:
        return None

    current_wip, average_wip = await _load_work_item_overlay(
        context,
        team_id=input.team_id,
        work_scope_id=input.work_scope_id,
        history_weeks=input.history_weeks,
    )
    review_latency_hours = await _load_review_overlay(
        context,
        history_weeks=input.history_weeks,
    )
    incident_count = await _load_incident_overlay(
        context,
        history_weeks=input.history_weeks,
    )
    result = forecast_throughput_capacity(
        history=history,
        backlog_size=input.backlog_size,
        team_id=input.team_id,
        work_scope_id=input.work_scope_id,
        history_weeks=input.history_weeks,
        current_wip=current_wip,
        average_wip=average_wip,
        review_latency_hours=review_latency_hours,
        incident_count=incident_count,
    )
    return _result_to_output(result)
