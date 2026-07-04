from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.testops_risk import resolve_testops_risk
from dev_health_ops.api.graphql.types.testops_risk import TestOpsRiskInput

ORG_ID = "org-test"
START = date(2026, 5, 19)
END = date(2026, 5, 20)


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/d")
    ctx.client = MagicMock(spec=["query"])
    return ctx


def _qresult(columns: list[str], rows: list[list[Any]]) -> Any:
    result = MagicMock()
    result.column_names = columns
    result.result_rows = rows
    return result


@pytest.mark.asyncio
async def test_returns_empty_persisted_risk_when_tables_have_no_window_rows() -> None:
    ctx = _ctx()
    ctx.client.query.side_effect = [
        _qresult([], []),
        _qresult([], []),
    ]

    result = await resolve_testops_risk(
        ctx, ORG_ID, TestOpsRiskInput(start_date=START, end_date=END)
    )

    assert result.release_confidence is None
    assert result.quality_drag_hours is None
    assert result.pipeline_stability is None
    assert result.timeseries == []
    assert result.quality_drag_breakdown == []
    assert result.quadrant_data == []


@pytest.mark.asyncio
async def test_maps_persisted_risk_rows_without_recomputing_components() -> None:
    ctx = _ctx()
    ctx.client.query.side_effect = [
        _qresult(
            [
                "day",
                "release_confidence",
                "quality_drag_hours",
                "failure_rework_hours",
                "flake_investigation_hours",
                "queue_wait_hours",
                "retry_overhead_hours",
                "pipeline_stability",
            ],
            [
                [START, 0.60, 20.0, 8.0, 6.0, 4.0, 2.0, 0.80],
                [END, 0.75, 10.0, 3.0, 2.5, 1.5, 3.0, 0.90],
            ],
        ),
        _qresult(
            ["repo_label", "pipeline_success_rate", "test_pass_rate"],
            [["web", 0.92, 0.98], ["ops", 0.84, 0.95]],
        ),
    ]

    result = await resolve_testops_risk(
        ctx, "client-supplied-org", TestOpsRiskInput(start_date=START, end_date=END)
    )

    assert result.org_id == ORG_ID
    assert result.release_confidence == 0.75
    assert result.quality_drag_hours == 10.0
    assert result.pipeline_stability == 0.90
    assert [(point.date, point.risk_score) for point in result.timeseries] == [
        (START, 0.4),
        (END, 0.25),
    ]
    assert [(point.ts, point.value) for point in result.confidence_spark] == [
        (START, 60.0),
        (END, 75.0),
    ]
    assert result.confidence_delta == 25.0
    assert [(item.category, item.hours) for item in result.quality_drag_breakdown] == [
        ("Failure Rework", 3.0),
        ("Flake Investigation", 2.5),
        ("Queue Wait", 1.5),
        ("Retry Overhead", 3.0),
    ]
    assert [
        (point.id, point.pipeline_success_rate, point.test_pass_rate)
        for point in result.quadrant_data
    ] == [
        ("web", 0.92, 0.98),
        ("ops", 0.84, 0.95),
    ]

    daily_sql = ctx.client.query.call_args_list[0].args[0]
    quadrant_sql = ctx.client.query.call_args_list[1].args[0]
    assert "testops_release_confidence" in daily_sql
    assert "testops_quality_drag" in daily_sql
    assert "testops_pipeline_stability" in daily_sql
    assert "argMax(confidence_score, computed_at)" in daily_sql
    assert "SETTINGS join_use_nulls = 1" in daily_sql
    assert "JSONExtractFloat(factors_json, 'pipeline_success_rate')" in quadrant_sql


@pytest.mark.asyncio
async def test_uses_latest_non_null_row_for_each_headline_metric() -> None:
    ctx = _ctx()
    ctx.client.query.side_effect = [
        _qresult(
            [
                "day",
                "release_confidence",
                "quality_drag_hours",
                "failure_rework_hours",
                "flake_investigation_hours",
                "queue_wait_hours",
                "retry_overhead_hours",
                "pipeline_stability",
            ],
            [
                [START, 0.60, None, None, None, None, None, None],
                [END, None, 10.0, 3.0, 2.5, 1.5, 3.0, 0.90],
            ],
        ),
        _qresult(["repo_label", "pipeline_success_rate", "test_pass_rate"], []),
    ]

    result = await resolve_testops_risk(
        ctx, ORG_ID, TestOpsRiskInput(start_date=START, end_date=END)
    )

    assert result.release_confidence == 0.60
    assert result.quality_drag_hours == 10.0
    assert result.pipeline_stability == 0.90
    assert [(item.category, item.hours) for item in result.quality_drag_breakdown] == [
        ("Failure Rework", 3.0),
        ("Flake Investigation", 2.5),
        ("Queue Wait", 1.5),
        ("Retry Overhead", 3.0),
    ]
