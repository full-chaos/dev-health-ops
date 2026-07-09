from __future__ import annotations

from datetime import date

import strawberry


@strawberry.input
class TestOpsRiskInput:
    start_date: date = strawberry.field(name="startDate")
    end_date: date = strawberry.field(name="endDate")


@strawberry.type
class TestOpsRiskTrendPoint:
    date: date
    risk_score: float = strawberry.field(name="riskScore")


@strawberry.type
class TestOpsRiskBreakdownItem:
    category: str
    hours: float


@strawberry.type
class TestOpsRiskQuadrantPoint:
    id: str
    pipeline_success_rate: float | None = strawberry.field(name="pipelineSuccessRate")
    test_pass_rate: float | None = strawberry.field(name="testPassRate")


@strawberry.type
class TestOpsRiskSparkPoint:
    ts: date
    value: float


@strawberry.type
class TestOpsRiskResult:
    org_id: str = strawberry.field(name="orgId")
    release_confidence: float | None = strawberry.field(name="releaseConfidence")
    quality_drag_hours: float | None = strawberry.field(name="qualityDragHours")
    pipeline_stability: float | None = strawberry.field(name="pipelineStability")
    timeseries: list[TestOpsRiskTrendPoint]
    quality_drag_breakdown: list[TestOpsRiskBreakdownItem] = strawberry.field(
        name="qualityDragBreakdown"
    )
    quadrant_data: list[TestOpsRiskQuadrantPoint] = strawberry.field(
        name="quadrantData"
    )
    confidence_spark: list[TestOpsRiskSparkPoint] = strawberry.field(
        name="confidenceSpark"
    )
    confidence_delta: float | None = strawberry.field(name="confidenceDelta")
    drag_spark: list[TestOpsRiskSparkPoint] = strawberry.field(name="dragSpark")
    drag_delta: float | None = strawberry.field(name="dragDelta")
    stability_spark: list[TestOpsRiskSparkPoint] = strawberry.field(
        name="stabilitySpark"
    )
    stability_delta: float | None = strawberry.field(name="stabilityDelta")
