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
    ScopeFilterInput,
    ScopeLevelInput,
    WhatFilterInput,
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
