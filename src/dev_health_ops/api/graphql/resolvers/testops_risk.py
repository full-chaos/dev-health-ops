from __future__ import annotations

from datetime import date
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.testops_risk import (
    TestOpsRiskBreakdownItem,
    TestOpsRiskInput,
    TestOpsRiskQuadrantPoint,
    TestOpsRiskResult,
    TestOpsRiskSparkPoint,
    TestOpsRiskTrendPoint,
)


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for TestOps Risk resolver")
    return context.client


def _float_or_none(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _delta(points: list[TestOpsRiskSparkPoint]) -> float | None:
    if len(points) < 2:
        return None
    previous = points[0].value
    current = points[-1].value
    if previous == 0:
        return None
    return ((current - previous) / abs(previous)) * 100


async def _fetch_daily_rows(
    client: Any, org_id: str, risk_input: TestOpsRiskInput
) -> list[dict[str, Any]]:
    query = """
        WITH
        release_daily AS (
            SELECT
                day,
                avg(confidence_score) AS release_confidence
            FROM (
                SELECT
                    day,
                    repo_id,
                    argMax(confidence_score, computed_at) AS confidence_score
                FROM testops_release_confidence
                WHERE org_id = {org_id:String}
                  AND day >= {start:Date}
                  AND day <= {end:Date}
                GROUP BY day, repo_id
            )
            GROUP BY day
        ),
        drag_daily AS (
            SELECT
                day,
                sum(drag_hours) AS quality_drag_hours,
                sum(failure_rework_hours) AS failure_rework_hours,
                sum(flake_investigation_hours) AS flake_investigation_hours,
                sum(queue_wait_hours) AS queue_wait_hours,
                sum(retry_overhead_hours) AS retry_overhead_hours
            FROM (
                SELECT
                    day,
                    repo_id,
                    argMax(drag_hours, computed_at) AS drag_hours,
                    argMax(failure_rework_hours, computed_at) AS failure_rework_hours,
                    argMax(flake_investigation_hours, computed_at) AS flake_investigation_hours,
                    argMax(queue_wait_hours, computed_at) AS queue_wait_hours,
                    argMax(retry_overhead_hours, computed_at) AS retry_overhead_hours
                FROM testops_quality_drag
                WHERE org_id = {org_id:String}
                  AND day >= {start:Date}
                  AND day <= {end:Date}
                GROUP BY day, repo_id
            )
            GROUP BY day
        ),
        stability_daily AS (
            SELECT
                day,
                avg(stability_index) AS pipeline_stability
            FROM (
                SELECT
                    day,
                    repo_id,
                    argMax(stability_index, computed_at) AS stability_index
                FROM testops_pipeline_stability
                WHERE org_id = {org_id:String}
                  AND day >= {start:Date}
                  AND day <= {end:Date}
                GROUP BY day, repo_id
            )
            GROUP BY day
        )
        SELECT
            day AS day,
            release_confidence,
            quality_drag_hours,
            failure_rework_hours,
            flake_investigation_hours,
            queue_wait_hours,
            retry_overhead_hours,
            pipeline_stability
        FROM release_daily
        FULL OUTER JOIN drag_daily USING (day)
        FULL OUTER JOIN stability_daily USING (day)
        ORDER BY day ASC
        SETTINGS join_use_nulls = 1
        """
    return await query_dicts(
        client,
        query,
        {"org_id": org_id, "start": risk_input.start_date, "end": risk_input.end_date},
    )


async def _fetch_quadrant_rows(
    client: Any, org_id: str, risk_input: TestOpsRiskInput
) -> list[dict[str, Any]]:
    query = """
        SELECT
            coalesce(nullIf(repos.repo, ''), toString(latest.repo_id)) AS repo_label,
            latest.pipeline_success_rate,
            latest.test_pass_rate
        FROM (
            SELECT
                repo_id,
                argMax(
                    JSONExtractFloat(factors_json, 'pipeline_success_rate'),
                    (day, computed_at)
                ) AS pipeline_success_rate,
                argMax(
                    JSONExtractFloat(factors_json, 'test_pass_rate'),
                    (day, computed_at)
                ) AS test_pass_rate,
                argMax(confidence_score, (day, computed_at)) AS confidence_score
            FROM testops_release_confidence
            WHERE org_id = {org_id:String}
              AND day >= {start:Date}
              AND day <= {end:Date}
            GROUP BY repo_id
        ) AS latest
        LEFT JOIN repos
          ON repos.org_id = {org_id:String}
         AND repos.id = latest.repo_id
        ORDER BY latest.confidence_score ASC
        LIMIT 50
        """
    return await query_dicts(
        client,
        query,
        {"org_id": org_id, "start": risk_input.start_date, "end": risk_input.end_date},
    )


def _latest_with(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    return next((row for row in reversed(rows) if row.get(field) is not None), {})


async def resolve_testops_risk(
    context: GraphQLContext, org_id: str, input: TestOpsRiskInput
) -> TestOpsRiskResult:
    context_org_id = require_org_id(context)
    client = _require_client(context)
    rows = await _fetch_daily_rows(client, context_org_id, input)
    quadrant_rows = await _fetch_quadrant_rows(client, context_org_id, input)

    timeseries = [
        TestOpsRiskTrendPoint(
            date=row["day"],
            risk_score=max(0.0, min(1.0, 1.0 - float(row["release_confidence"]))),
        )
        for row in rows
        if isinstance(row.get("day"), date)
        and row.get("release_confidence") is not None
    ]
    confidence_spark = [
        TestOpsRiskSparkPoint(ts=point.date, value=(1.0 - point.risk_score) * 100.0)
        for point in timeseries
    ]
    drag_spark = [
        TestOpsRiskSparkPoint(ts=row["day"], value=float(row["quality_drag_hours"]))
        for row in rows
        if isinstance(row.get("day"), date)
        and row.get("quality_drag_hours") is not None
    ]
    stability_spark = [
        TestOpsRiskSparkPoint(
            ts=row["day"], value=float(row["pipeline_stability"]) * 100.0
        )
        for row in rows
        if isinstance(row.get("day"), date)
        and row.get("pipeline_stability") is not None
    ]

    latest_release = _latest_with(rows, "release_confidence")
    latest_drag = _latest_with(rows, "quality_drag_hours")
    latest_stability = _latest_with(rows, "pipeline_stability")
    breakdown = (
        [
            TestOpsRiskBreakdownItem(
                category="Failure Rework",
                hours=float(latest_drag.get("failure_rework_hours") or 0.0),
            ),
            TestOpsRiskBreakdownItem(
                category="Flake Investigation",
                hours=float(latest_drag.get("flake_investigation_hours") or 0.0),
            ),
            TestOpsRiskBreakdownItem(
                category="Queue Wait",
                hours=float(latest_drag.get("queue_wait_hours") or 0.0),
            ),
            TestOpsRiskBreakdownItem(
                category="Retry Overhead",
                hours=float(latest_drag.get("retry_overhead_hours") or 0.0),
            ),
        ]
        if latest_drag
        else []
    )

    return TestOpsRiskResult(
        org_id=context_org_id,
        release_confidence=_float_or_none(latest_release.get("release_confidence")),
        quality_drag_hours=_float_or_none(latest_drag.get("quality_drag_hours")),
        pipeline_stability=_float_or_none(latest_stability.get("pipeline_stability")),
        timeseries=timeseries,
        quality_drag_breakdown=breakdown,
        quadrant_data=[
            TestOpsRiskQuadrantPoint(
                id=str(row["repo_label"]),
                pipeline_success_rate=_float_or_none(row.get("pipeline_success_rate")),
                test_pass_rate=_float_or_none(row.get("test_pass_rate")),
            )
            for row in quadrant_rows
        ],
        confidence_spark=confidence_spark,
        confidence_delta=_delta(confidence_spark),
        drag_spark=drag_spark,
        drag_delta=_delta(drag_spark),
        stability_spark=stability_spark,
        stability_delta=_delta(stability_spark),
    )
