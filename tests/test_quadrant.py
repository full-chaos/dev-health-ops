from contextlib import asynccontextmanager
from datetime import date

import pytest

from dev_health_ops.api.models.schemas import (
    QuadrantAnnotation,
    QuadrantAxes,
    QuadrantAxis,
    QuadrantPoint,
    QuadrantPointTrajectory,
    QuadrantResponse,
)
from dev_health_ops.api.services.quadrant import (
    QUADRANT_DEFINITIONS,
    build_quadrant_response,
)


def _model_fields(model) -> set[str]:
    if hasattr(model, "model_fields"):
        return set(model.model_fields.keys())
    return set(model.__fields__.keys())


def test_quadrant_schema_shape():
    response = QuadrantResponse(
        axes=QuadrantAxes(
            x=QuadrantAxis(metric="churn", label="Churn", unit="loc"),
            y=QuadrantAxis(metric="throughput", label="Throughput", unit="items"),
        ),
        points=[
            QuadrantPoint(
                entity_id="team-a",
                entity_label="Team A",
                x=120.0,
                y=42.0,
                window_start=date(2024, 1, 1),
                window_end=date(2024, 1, 8),
                evidence_link="/api/v1/explain?metric=throughput",
                trajectory=[
                    QuadrantPointTrajectory(x=100.0, y=40.0, window="2024-01-01")
                ],
            )
        ],
        annotations=[
            QuadrantAnnotation(
                type="boundary",
                description="Saturation zone",
                x_range=[20.0, 80.0],
                y_range=[5.0, 40.0],
            )
        ],
    )
    assert response.axes.x.metric == "churn"


def test_quadrant_no_rank_fields():
    forbidden = {"rank", "percentile", "score"}
    assert forbidden.isdisjoint(_model_fields(QuadrantPoint))


def test_quadrant_axis_label_snapshot():
    expected = {
        "churn_throughput": ("Churn", "Throughput"),
        "cycle_throughput": ("Cycle Time", "Throughput"),
        "wip_throughput": ("WIP", "Throughput"),
        "review_load_latency": ("Review Load", "Review Latency"),
    }
    assert {
        key: (definition.x.label, definition.y.label)
        for key, definition in QUADRANT_DEFINITIONS.items()
    } == expected


@pytest.mark.asyncio
async def test_quadrant_resolves_team_uuid_label_from_team_catalog(monkeypatch):
    team_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"

    @asynccontextmanager
    async def _fake_client(_db_url):
        yield object()

    async def _fake_metric(
        _sink,
        *,
        value_expr,
        start_day,
        end_day,
        bucket,
        entity_expr,
        label_expr,
        **_,
    ):
        value = 100.0 if "loc_touched" in value_expr else 8.0
        return [
            {
                "bucket": date(2024, 1, 1),
                "entity_id": team_uuid,
                "entity_label": team_uuid,
                "value": value,
            }
        ]

    async def _fake_query_dicts(_sink, _query, params):
        assert params["team_ids"] == [team_uuid]
        return [{"team_id": team_uuid, "team_name": "Platform Team"}]

    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.clickhouse_client", _fake_client
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.fetch_quadrant_metric", _fake_metric
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.query_dicts",
        _fake_query_dicts,
        raising=False,
    )

    # churn_throughput now forces repo grain (CHAOS-2079), so exercise team-label
    # resolution through a quadrant that keeps team grain (cycle_throughput).
    response = await build_quadrant_response(
        db_url="clickhouse://test",
        org_id="test-org",
        type="cycle_throughput",
        scope_type="team",
        scope_id="",
        range_days=30,
        bucket="week",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 8),
    )

    assert response.points[0].entity_id == team_uuid
    assert response.points[0].entity_label == "Platform Team"


@pytest.mark.asyncio
async def test_churn_quadrant_forces_repo_grain(monkeypatch):
    """churn_throughput must enumerate repos even when the caller asks for team/org
    scope. Churn is repo-attributed at ingest, so team-grain churn collapses to ~0 and
    the scatter degenerates onto the y-axis (CHAOS-2079). Assert both axes source from
    repo_metrics_daily and that team-label resolution is skipped."""

    @asynccontextmanager
    async def _fake_client(_db_url):
        yield object()

    captured_tables: list[str] = []
    captured_value_exprs: list[str] = []

    async def _fake_metric(_sink, *, table, value_expr, entity_expr, **_):
        captured_tables.append(table)
        captured_value_exprs.append(value_expr)
        return [
            {
                "bucket": date(2024, 1, 1),
                "entity_id": "checkout-service",
                "entity_label": "checkout-service",
                "value": 120.0,
            }
        ]

    async def _unexpected_query_dicts(*_args, **_kwargs):
        raise AssertionError("team-label resolution must not run for repo-grain churn")

    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.clickhouse_client", _fake_client
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.fetch_quadrant_metric", _fake_metric
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.quadrant.query_dicts",
        _unexpected_query_dicts,
        raising=False,
    )

    response = await build_quadrant_response(
        db_url="clickhouse://test",
        org_id="test-org",
        type="churn_throughput",
        scope_type="team",
        scope_id="",
        range_days=30,
        bucket="week",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 8),
    )

    # Both axes must source from the repo grain (repo_metrics_daily), not the
    # team/user tables that produce the degenerate churn≈0 collapse.
    assert captured_tables, "fetch_quadrant_metric was never called"
    assert all(table == "repo_metrics_daily AS m" for table in captured_tables)
    assert any("total_loc_touched" in expr for expr in captured_value_exprs)
    assert any("prs_merged" in expr for expr in captured_value_exprs)
    # Repo labels pass through untouched (no team-catalog resolution).
    assert response.points[0].entity_label == "checkout-service"
