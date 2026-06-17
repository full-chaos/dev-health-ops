from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    BreakdownRequestInput,
    DateRangeInput,
    DimensionInput,
    FilterInput,
    MeasureInput,
    SankeyRequestInput,
    ScopeFilterInput,
    ScopeLevelInput,
    WhatFilterInput,
    WhoFilterInput,
    WhyFilterInput,
)
from dev_health_ops.api.graphql.resolvers import analytics as analytics_resolver
from dev_health_ops.api.queries import investment as investment_queries

REPO_FULL_NAME = "full-chaos/dev-health-ops"
REPO_UUID = "11111111-1111-4111-8111-111111111111"


def _breakdown_batch(filters: FilterInput) -> AnalyticsRequestInput:
    return AnalyticsRequestInput(
        breakdowns=[
            BreakdownRequestInput(
                dimension=DimensionInput.THEME,
                measure=MeasureInput.COUNT,
                date_range=DateRangeInput(
                    start_date=date(2026, 6, 1),
                    end_date=date(2026, 6, 17),
                ),
            )
        ],
        use_investment=True,
        filters=filters,
    )


def _sankey_batch(filters: FilterInput | None = None) -> AnalyticsRequestInput:
    return AnalyticsRequestInput(
        sankey=SankeyRequestInput(
            path=[DimensionInput.THEME, DimensionInput.REPO],
            measure=MeasureInput.COUNT,
            date_range=DateRangeInput(
                start_date=date(2026, 6, 1),
                end_date=date(2026, 6, 17),
            ),
            use_investment=True,
        ),
        use_investment=True,
        filters=filters,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filters", "param_name"),
    [
        (FilterInput(what=WhatFilterInput(repos=[REPO_FULL_NAME])), "repo_filter_ids"),
        (
            FilterInput(
                scope=ScopeFilterInput(
                    level=ScopeLevelInput.REPO,
                    ids=[REPO_FULL_NAME],
                )
            ),
            "scope_ids",
        ),
    ],
)
async def test_analytics_resolves_repo_name_filters_to_repo_ids(
    monkeypatch: pytest.MonkeyPatch,
    filters: FilterInput,
    param_name: str,
) -> None:
    captured_params: list[dict[str, Any]] = []

    async def fake_query_dicts(_client: object, sql: str, params: dict[str, Any]):
        captured_params.append(dict(params))
        if "FROM repos" in sql:
            assert params["repo_names"] == [REPO_FULL_NAME]
            return [{"repo_id": REPO_UUID, "repo": REPO_FULL_NAME}]
        if "countIf(evidence_quality_band" in sql:
            return [
                {
                    "total": 1,
                    "quality_known_count": 1,
                    "quality_mean": 1.0,
                    "quality_stddev": 0.0,
                    "high_count": 1,
                    "moderate_count": 0,
                    "low_count": 0,
                    "very_low_count": 0,
                    "unknown_count": 0,
                }
            ]
        return [{"dimension_value": "Feature Delivery", "value": 1.0}]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )
    monkeypatch.setattr(
        investment_queries,
        "query_dicts",
        fake_query_dicts,
    )

    result = await analytics_resolver.resolve_analytics(
        GraphQLContext(org_id="org-1", db_url="clickhouse://test", client=object()),
        _breakdown_batch(filters),
    )

    matching_params = [params for params in captured_params if param_name in params]
    assert matching_params
    assert all(params[param_name] == [REPO_UUID] for params in matching_params)
    assert result.breakdowns[0].items[0].value == 1.0


@pytest.mark.asyncio
async def test_sankey_coverage_respects_resolved_repo_name_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_coverage_params: list[dict[str, Any]] = []

    async def fake_query_dicts(_client: object, sql: str, params: dict[str, Any]):
        if "FROM repos" in sql:
            assert params["repo_names"] == [REPO_FULL_NAME]
            return [{"repo_id": REPO_UUID, "repo": REPO_FULL_NAME}]
        if "assigned_team" in sql:
            captured_coverage_params.append(dict(params))
            if params.get("repo_filter_ids") == [REPO_UUID]:
                return [{"total": 2, "assigned_team": 1, "assigned_repo": 2}]
            return [{"total": 4, "assigned_team": 4, "assigned_repo": 4}]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    context = GraphQLContext(
        org_id="org-1", db_url="clickhouse://test", client=object()
    )

    org_wide = await analytics_resolver.resolve_analytics(
        context,
        _sankey_batch(),
    )
    filtered = await analytics_resolver.resolve_analytics(
        context,
        _sankey_batch(FilterInput(what=WhatFilterInput(repos=[REPO_FULL_NAME]))),
    )

    assert captured_coverage_params[0].get("repo_filter_ids") is None
    assert captured_coverage_params[1]["repo_filter_ids"] == [REPO_UUID]
    assert org_wide.sankey is not None
    assert org_wide.sankey.coverage is not None
    assert filtered.sankey is not None
    assert filtered.sankey.coverage is not None
    assert org_wide.sankey.coverage.team_coverage == 1.0
    assert filtered.sankey.coverage.team_coverage == 0.5


@pytest.mark.asyncio
async def test_sankey_coverage_unavailable_for_investment_developer_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    developer_id = "dev-1"
    captured_sankey_params: list[dict[str, Any]] = []
    captured_coverage_params: list[dict[str, Any]] = []

    async def fake_query_dicts(_client: object, sql: str, params: dict[str, Any]):
        if "assigned_team" in sql:
            captured_coverage_params.append(dict(params))
            return [{"total": 4, "assigned_team": 4, "assigned_repo": 4}]
        if "developer_ids" in params:
            captured_sankey_params.append(dict(params))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    result = await analytics_resolver.resolve_analytics(
        GraphQLContext(org_id="org-1", db_url="clickhouse://test", client=object()),
        _sankey_batch(FilterInput(who=WhoFilterInput(developers=[developer_id]))),
    )

    assert captured_sankey_params
    assert all(
        params["developer_ids"] == [developer_id] for params in captured_sankey_params
    )
    assert captured_coverage_params == []
    assert result.sankey is not None
    assert result.sankey.coverage is None


@pytest.mark.asyncio
async def test_sankey_coverage_respects_work_category_filter_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_category = "Feature Delivery"
    captured_sankey_params: list[dict[str, Any]] = []
    captured_coverage_params: list[dict[str, Any]] = []

    async def fake_query_dicts(_client: object, sql: str, params: dict[str, Any]):
        if "assigned_team" in sql:
            captured_coverage_params.append(dict(params))
            if params.get("work_categories") == [work_category]:
                return [{"total": 2, "assigned_team": 1, "assigned_repo": 2}]
            return [{"total": 4, "assigned_team": 4, "assigned_repo": 4}]
        if "work_categories" in params:
            captured_sankey_params.append(dict(params))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    context = GraphQLContext(
        org_id="org-1", db_url="clickhouse://test", client=object()
    )

    org_wide = await analytics_resolver.resolve_analytics(
        context,
        _sankey_batch(),
    )
    filtered = await analytics_resolver.resolve_analytics(
        context,
        _sankey_batch(FilterInput(why=WhyFilterInput(work_category=[work_category]))),
    )

    assert captured_sankey_params
    assert all(
        params["work_categories"] == [work_category]
        for params in captured_sankey_params
    )
    assert captured_coverage_params[0].get("work_categories") is None
    assert captured_coverage_params[1]["work_categories"] == [work_category]
    assert org_wide.sankey is not None
    assert org_wide.sankey.coverage is not None
    assert filtered.sankey is not None
    assert filtered.sankey.coverage is not None
    assert org_wide.sankey.coverage.team_coverage == 1.0
    assert filtered.sankey.coverage.team_coverage == 0.5
