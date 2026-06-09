from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def assert_team_attribution_sql(sql: str) -> None:
    assert "WHERE org_id = %(org_id)s" in sql
    assert "arrayDistinct(arrayConcat(" in sql
    assert "JSONExtract(structural_evidence_json, 'issues', 'Array(String)')" in sql
    assert "[work_unit_investments.work_unit_id]" in sql
    assert "t.work_item_id = issue_id" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetcher",
    [
        investment_queries.fetch_investment_team_edges,
        investment_queries.fetch_investment_repo_team_edges,
        investment_queries.fetch_investment_team_category_repo_edges,
        investment_queries.fetch_investment_team_subcategory_repo_edges,
        investment_queries.fetch_investment_unassigned_counts,
    ],
)
async def test_investment_team_attribution_sql_scopes_org_and_uses_work_unit_fallback(
    monkeypatch: pytest.MonkeyPatch,
    fetcher: Any,
) -> None:
    captured: dict[str, Any] = {"sql": "", "params": {}}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    await fetcher(
        cast(BaseMetricsSink, object()),
        start_ts=datetime(2026, 5, 24, tzinfo=timezone.utc),
        end_ts=datetime(2026, 6, 8, tzinfo=timezone.utc),
        scope_filter="",
        scope_params={},
        org_id="org-1",
    )

    assert_team_attribution_sql(str(captured["sql"]))
    assert captured["params"]["org_id"] == "org-1"
