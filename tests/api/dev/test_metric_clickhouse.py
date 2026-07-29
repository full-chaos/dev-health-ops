from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.dev.contracts import (
    DevScope,
    DevTimeRange,
    DirectScope,
    MetricID,
)
from dev_health_ops.api.dev.metrics.clickhouse import ClickHouseMetricSource
from dev_health_ops.api.dev.metrics.definitions import get_metric

UTC = timezone.utc


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-1",
        direct_scope=DirectScope.ORGANIZATION,
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 8, tzinfo=UTC),
            timezone="UTC",
        ),
        comparison_range=DevTimeRange(
            start=datetime(2026, 6, 24, tzinfo=UTC),
            end=datetime(2026, 7, 1, tzinfo=UTC),
            timezone="UTC",
        ),
    )


@pytest.mark.asyncio
async def test_metric_05_uses_weighted_deployment_counts_and_no_revert_fallback(
    monkeypatch,
) -> None:
    captured: list[str] = []

    async def fake_query(_client, sql, params):
        captured.append(sql)
        assert params["org_id"] == "org-1"
        return [
            {
                "value": None,
                "matched_rows": 7,
                "deployments": 10,
                "failed_deployments": 2,
                "covered_days": 7,
                "latest_day": date(2026, 7, 7),
                "watermark": datetime(2026, 7, 8, 2, tzinfo=UTC),
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.metrics.clickhouse.query_dicts", fake_query
    )
    result = await ClickHouseMetricSource(object()).query(
        "org-1",
        get_metric(MetricID.CHANGE_FAILURE_RATE),
        _scope(),
        comparison=False,
        include_series=False,
        max_series_points=90,
    )
    assert result.rows[0].value == pytest.approx(0.2)
    sql = "\n".join(captured)
    assert "sum(deployments_count)" in sql
    assert "sum(failed_deployments_count)" in sql
    assert "repo_metrics_daily" not in sql
    assert "change_failure_rate" not in sql


@pytest.mark.asyncio
async def test_watermark_read_is_bounded_to_current_and_comparison_windows(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_query(_client, sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [{"latest_day": date(2026, 7, 7), "watermark": None}]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.metrics.clickhouse.query_dicts", fake_query
    )
    await ClickHouseMetricSource(object()).watermark(
        "org-1", get_metric(MetricID.DEPLOYMENTS_COUNT), _scope()
    )

    assert "day >= %(watermark_start)s AND day < %(watermark_end)s" in str(
        captured["sql"]
    )
    assert captured["params"] == {
        "org_id": "org-1",
        "watermark_start": date(2026, 6, 24),
        "watermark_end": date(2026, 7, 8),
    }


@pytest.mark.asyncio
async def test_compounding_component_weight_version_is_order_independent(
    monkeypatch,
) -> None:
    rows = [
        {
            "scope_id": "repo-b",
            "score": 0.6,
            "churn_norm": 0.5,
            "complexity_norm": 0.7,
            "ownership_norm": 0.4,
            "review_norm": 0.8,
            "w_churn": 0.3,
            "w_complexity": 0.3,
            "w_ownership": 0.2,
            "w_review": 0.2,
            "threshold_elevated": 0.4,
            "threshold_high": 0.7,
            "latest_day": date(2026, 7, 7),
            "watermark": datetime(2026, 7, 8, 2, tzinfo=UTC),
        },
        {
            "scope_id": "repo-a",
            "score": 0.4,
            "churn_norm": 0.3,
            "complexity_norm": 0.5,
            "ownership_norm": 0.2,
            "review_norm": 0.6,
            "w_churn": 0.3,
            "w_complexity": 0.3,
            "w_ownership": 0.2,
            "w_review": 0.2,
            "threshold_elevated": 0.4,
            "threshold_high": 0.7,
            "latest_day": date(2026, 7, 7),
            "watermark": datetime(2026, 7, 8, 2, tzinfo=UTC),
        },
    ]
    responses = [rows, list(reversed(rows))]

    async def fake_query(_client, _sql, _params):
        return responses.pop(0)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.metrics.clickhouse.query_dicts", fake_query
    )
    source = ClickHouseMetricSource(object())
    first = await source.query(
        "org-1",
        get_metric(MetricID.COMPOUNDING_RISK_SCORE),
        _scope(),
        comparison=False,
        include_series=False,
        max_series_points=90,
    )
    second = await source.query(
        "org-1",
        get_metric(MetricID.COMPOUNDING_RISK_SCORE),
        _scope(),
        comparison=False,
        include_series=False,
        max_series_points=90,
    )
    first_meta = dict(first.metadata)
    second_meta = dict(second.metadata)
    assert first.rows[0].value == pytest.approx(0.5)
    assert (
        first_meta["component_weight_version"]
        == second_meta["component_weight_version"]
    )
    assert first_meta["component_weight_version"].startswith("sha256:")


@pytest.mark.asyncio
async def test_investment_coverage_accounts_for_excluded_unclassified_work(
    monkeypatch,
) -> None:
    async def fake_query(_client, sql, _params):
        assert "WHERE canonical_theme != ''" not in sql
        assert "LIMIT 6" in sql
        return [
            {
                "theme": "feature_delivery",
                "allocation": 75,
                "covered_days": 7,
                "latest_day": date(2026, 7, 7),
                "watermark": datetime(2026, 7, 8, 2, tzinfo=UTC),
            },
            {
                "theme": "",
                "allocation": 25,
                "covered_days": 7,
                "latest_day": date(2026, 7, 7),
                "watermark": datetime(2026, 7, 8, 2, tzinfo=UTC),
            },
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.metrics.clickhouse.query_dicts", fake_query
    )
    result = await ClickHouseMetricSource(object()).query(
        "org-1",
        get_metric(MetricID.INVESTMENT_ALLOCATION_PCT),
        _scope(),
        comparison=False,
        include_series=False,
        max_series_points=90,
    )

    assert result.rows[0].value == pytest.approx(100.0)
    assert result.coverage == pytest.approx(0.75)
    assert dict(result.metadata)["classification_coverage"] == "0.75"
