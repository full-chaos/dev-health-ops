from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, cast

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    BucketIntervalInput,
    DateRangeInput,
    DimensionInput,
    MeasureInput,
    TimeseriesRequestInput,
)
from dev_health_ops.api.graphql.resolvers import analytics as analytics_resolver
from dev_health_ops.api.queries import investment as investment_queries
from dev_health_ops.api.services.investment import _compute_quality_stats


@pytest.mark.asyncio
async def test_fetch_investment_quality_stats_reads_persisted_work_unit_bands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {"sql": "", "params": {}}

    async def fake_query_dicts(_sink: object, sql: str, params: dict[str, Any]):
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "total": 4,
                "quality_known_count": 4,
                "quality_mean": 0.6875,
                "quality_stddev": 0.147902,
                "high_count": 1,
                "moderate_count": 2,
                "low_count": 1,
                "very_low_count": 0,
                "unknown_count": 0,
            }
        ]

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    row = await investment_queries.fetch_investment_quality_stats(
        cast(Any, object()),
        start_ts=datetime(2026, 5, 26, tzinfo=timezone.utc),
        end_ts=datetime(2026, 6, 9, tzinfo=timezone.utc),
        scope_filter=" AND work_unit_investments.repo_id IN %(repo_ids)s",
        scope_params={"repo_ids": ["repo-1"]},
        org_id="org-1",
    )

    sql = str(captured["sql"])
    assert "FROM work_unit_investments" in sql
    assert "avgIf(evidence_quality" in sql
    assert "stddevPopIf(evidence_quality" in sql
    assert "countIf(evidence_quality_band = 'high')" in sql
    assert captured["params"]["org_id"] == "org-1"
    assert captured["params"]["repo_ids"] == ["repo-1"]
    assert row["high_count"] == 1
    assert row["moderate_count"] == 2
    assert row["low_count"] == 1
    assert row["quality_mean"] == 0.6875


def test_compute_quality_stats_exposes_band_counts_mean_stddev_total() -> None:
    stats = _compute_quality_stats(
        {
            "total": 4,
            "quality_known_count": 4,
            "quality_mean": 0.6875,
            "quality_stddev": 0.147902,
            "high_count": 1,
            "moderate_count": 2,
            "low_count": 1,
            "very_low_count": 0,
            "unknown_count": 0,
        }
    )

    assert stats.total == 4
    assert stats.mean == 0.6875
    assert stats.stddev == 0.147902
    assert stats.band_counts == {
        "high": 1,
        "moderate": 2,
        "low": 1,
        "very_low": 0,
        "unknown": 0,
    }


@pytest.mark.asyncio
async def test_graphql_analytics_returns_evidence_quality_stats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_timeseries(*_args: Any, **_kwargs: Any):
        return []

    async def fake_quality_stats(*_args: Any, **_kwargs: Any):
        return {
            "total": 4,
            "quality_known_count": 4,
            "quality_mean": 0.6875,
            "quality_stddev": 0.147902,
            "high_count": 1,
            "moderate_count": 2,
            "low_count": 1,
            "very_low_count": 0,
            "unknown_count": 0,
        }

    monkeypatch.setattr(
        analytics_resolver, "_execute_timeseries_query", fake_timeseries
    )
    monkeypatch.setattr(
        analytics_resolver, "fetch_investment_quality_stats", fake_quality_stats
    )

    result = await analytics_resolver.resolve_analytics(
        GraphQLContext(org_id="org-1", db_url="clickhouse://test", client=object()),
        AnalyticsRequestInput(
            timeseries=[
                TimeseriesRequestInput(
                    dimension=DimensionInput.THEME,
                    measure=MeasureInput.COUNT,
                    interval=BucketIntervalInput.DAY,
                    date_range=DateRangeInput(
                        start_date=date(2026, 5, 26), end_date=date(2026, 6, 9)
                    ),
                )
            ],
            use_investment=True,
        ),
    )

    assert result.evidence_quality_distribution == {
        "high": 1,
        "moderate": 2,
        "low": 1,
        "very_low": 0,
        "unknown": 0,
    }
    assert result.evidence_quality_stats is not None
    assert result.evidence_quality_stats.mean == 0.6875
    assert result.evidence_quality_stats.stddev == 0.147902
    assert result.evidence_quality_stats.total == 4
