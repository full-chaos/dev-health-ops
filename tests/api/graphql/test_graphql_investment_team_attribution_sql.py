from __future__ import annotations

from datetime import date
from typing import Any

import pytest

import dev_health_ops.api.graphql.resolvers.analytics as analytics_resolver
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    DateRangeInput,
    DimensionInput,
    MeasureInput,
    SankeyRequestInput,
)
from dev_health_ops.api.graphql.sql.compiler import SankeyRequest, compile_sankey


def _assert_team_attribution_sql(sql: str) -> None:
    assert "WHERE org_id = %(org_id)s" in sql
    assert "arrayDistinct(arrayConcat(" in sql
    assert "JSONExtract(structural_evidence_json, 'issues', 'Array(String)')" in sql
    assert "[work_unit_investments.work_unit_id]" in sql
    assert "t.work_item_id = issue_id" in sql


def test_graphql_sankey_team_join_scopes_org_and_uses_work_unit_fallback() -> None:
    nodes_queries, edges_queries = compile_sankey(
        SankeyRequest(
            path=[DimensionInput.THEME.value, DimensionInput.TEAM.value],
            measure=MeasureInput.COUNT.value,
            start_date=date(2026, 5, 24),
            end_date=date(2026, 6, 8),
            use_investment=True,
        ),
        org_id="org-1",
    )

    compiled_sql = "\n".join(sql for sql, _params in [*nodes_queries, *edges_queries])

    _assert_team_attribution_sql(compiled_sql)
    assert all(params["org_id"] == "org-1" for _sql, params in nodes_queries)
    assert all(params["org_id"] == "org-1" for _sql, params in edges_queries)


@pytest.mark.asyncio
async def test_graphql_sankey_coverage_team_join_scopes_org_and_uses_work_unit_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {"sql": "", "params": {}}

    async def fake_execute_sankey_inner(*_args: Any, **_kwargs: Any):
        return [], []

    async def fake_query_dicts(_client: object, sql: str, params: dict[str, Any]):
        captured["sql"] = sql
        captured["params"] = params
        return [{"total": 2, "assigned_team": 1, "assigned_repo": 2}]

    monkeypatch.setattr(
        analytics_resolver, "_execute_sankey_inner", fake_execute_sankey_inner
    )
    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await analytics_resolver.resolve_analytics(
        GraphQLContext(org_id="org-1", db_url="clickhouse://test", client=object()),
        AnalyticsRequestInput(
            sankey=SankeyRequestInput(
                path=[DimensionInput.THEME, DimensionInput.TEAM],
                measure=MeasureInput.COUNT,
                date_range=DateRangeInput(
                    start_date=date(2026, 5, 24), end_date=date(2026, 6, 8)
                ),
                use_investment=True,
            ),
            use_investment=True,
        ),
    )

    _assert_team_attribution_sql(str(captured["sql"]))
    assert captured["params"]["org_id"] == "org-1"
    assert result.sankey is not None
    assert result.sankey.coverage is not None
    assert result.sankey.coverage.team_coverage == 0.5
