from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from dev_health_ops.api.models.filters import (
    MetricFilter,
    ScopeFilter,
    TimeFilter,
    WhyFilter,
)
from dev_health_ops.api.services import investment as investment_service
from dev_health_ops.api.services import sankey as sankey_service


@asynccontextmanager
async def _fake_clickhouse_client(_dsn):
    yield object()


@pytest.mark.asyncio
async def test_investment_response_applies_work_category_filter(monkeypatch):
    captured = {}

    async def _fake_resolve_repo_filter_ids(*_args, **_kwargs):
        return []

    async def _fake_tables_present(*_args, **_kwargs):
        return True

    async def _fake_columns_present(*_args, **_kwargs):
        return True

    async def _fake_breakdown(
        _client,
        *,
        start_ts,
        end_ts,
        scope_filter,
        scope_params,
        themes=None,
        subcategories=None,
    ):
        captured["breakdown"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "themes": themes,
            "subcategories": subcategories,
        }
        return [
            {
                "theme": "feature_delivery",
                "subcategory": "feature_delivery.roadmap",
                "value": 2,
            }
        ]

    async def _fake_edges(
        _client,
        *,
        start_ts,
        end_ts,
        scope_filter,
        scope_params,
        themes=None,
    ):
        captured["edges"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "themes": themes,
        }
        return [{"source": "feature_delivery", "target": "repo-a", "value": 2}]

    monkeypatch.setattr(investment_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(investment_service, "fetch_investment_breakdown", _fake_breakdown)
    monkeypatch.setattr(investment_service, "fetch_investment_edges", _fake_edges)
    monkeypatch.setattr(
        investment_service, "resolve_repo_filter_ids", _fake_resolve_repo_filter_ids
    )
    monkeypatch.setattr(investment_service, "_tables_present", _fake_tables_present)
    monkeypatch.setattr(investment_service, "_columns_present", _fake_columns_present)

    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=["feature_delivery.roadmap"]),
    )
    response = await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert captured["breakdown"]["themes"] == ["feature_delivery"]
    assert captured["breakdown"]["subcategories"] == ["feature_delivery.roadmap"]
    assert captured["edges"]["themes"] == ["feature_delivery"]
    assert response.categories[0].key == "feature_delivery"


@pytest.mark.asyncio
async def test_sankey_investment_applies_work_category_filter(monkeypatch):
    captured = {}

    async def _fake_tables_present(*_args, **_kwargs):
        return True

    async def _fake_columns_present(*_args, **_kwargs):
        return True

    async def _fake_flow_items(
        _client, *, start_ts, end_ts, scope_filter, scope_params, limit
    ):
        captured["flow"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        }
        return [{"source": "feature_delivery", "target": "repo-a", "value": 3}]

    async def _fake_repo_scope_filter(*_args, **_kwargs):
        return "", {}

    monkeypatch.setattr(sankey_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(sankey_service, "_tables_present", _fake_tables_present)
    monkeypatch.setattr(sankey_service, "_columns_present", _fake_columns_present)
    monkeypatch.setattr(sankey_service, "fetch_investment_flow_items", _fake_flow_items)
    monkeypatch.setattr(sankey_service, "_repo_scope_filter", _fake_repo_scope_filter)

    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=["feature_delivery.roadmap"]),
    )
    response = await sankey_service.build_sankey_response(
        db_url="clickhouse://",
        mode="investment",
        filters=filters,
    )

    assert captured["flow"]["scope_params"]["themes"] == ["feature_delivery"]
    assert "theme_kv.1" in captured["flow"]["scope_filter"]
    assert "themes" in captured["flow"]["scope_filter"]
    assert " AND theme_kv.1 IN %(themes)s" in captured["flow"]["scope_filter"]
    assert response.nodes


@pytest.mark.asyncio
async def test_investment_response_without_work_category_filter(monkeypatch):
    """Test that when work_category filter is absent, no filter is applied."""
    captured = {}

    async def _fake_resolve_repo_filter_ids(*_args, **_kwargs):
        return []

    async def _fake_tables_present(*_args, **_kwargs):
        return True

    async def _fake_columns_present(*_args, **_kwargs):
        return True

    async def _fake_breakdown(
        _client,
        *,
        start_ts,
        end_ts,
        scope_filter,
        scope_params,
        themes=None,
        subcategories=None,
    ):
        captured["breakdown"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "themes": themes,
            "subcategories": subcategories,
        }
        return [
            {
                "theme": "feature_delivery",
                "subcategory": "feature_delivery.roadmap",
                "value": 2,
            }
        ]

    async def _fake_edges(
        _client,
        *,
        start_ts,
        end_ts,
        scope_filter,
        scope_params,
        themes=None,
    ):
        captured["edges"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "themes": themes,
        }
        return [{"source": "feature_delivery", "target": "repo-a", "value": 2}]

    monkeypatch.setattr(investment_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(investment_service, "fetch_investment_breakdown", _fake_breakdown)
    monkeypatch.setattr(investment_service, "fetch_investment_edges", _fake_edges)
    monkeypatch.setattr(
        investment_service, "resolve_repo_filter_ids", _fake_resolve_repo_filter_ids
    )
    monkeypatch.setattr(investment_service, "_tables_present", _fake_tables_present)
    monkeypatch.setattr(investment_service, "_columns_present", _fake_columns_present)

    # Test with None work_category
    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=None),
    )
    await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert captured["breakdown"]["themes"] is None
    assert captured["breakdown"]["subcategories"] is None
    assert captured["edges"]["themes"] is None

    # Test with empty list
    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=[]),
    )
    await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert captured["breakdown"]["themes"] is None
    assert captured["breakdown"]["subcategories"] is None
