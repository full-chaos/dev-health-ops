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
    # CHAOS-2416: the `prs` array is the second bridge to a team. A unit whose
    # PR work item already carries a resolver-computed team must not collapse
    # to TEAM:unassigned just because the unit has no issue ref of its own.
    assert "JSONExtract(structural_evidence_json, 'prs', 'Array(String)')" in sql
    # ... resolved into the work_items id space through the repos table, per
    # provider (`ghpr:{repo}#{n}` / `gitlab:{repo}!{n}`).
    assert "evidence_repo.repo_uuid = splitByString('#pr', evidence_ref)[1]" in sql
    assert "'gitlab:', 'ghpr:'" in sql
    # The PR arm is keyed on the ref SHAPE, so an `issues` ref is never
    # rewritten and an unresolvable PR ref resolves to '' rather than leaking
    # a raw work-graph node id in as a live join key.
    assert "'^[0-9a-fA-F-]{36}#pr[0-9]+$'" in sql
    assert "t.work_item_id = multiIf(" in sql
    # CHAOS-2416: the old `[work_unit_id]` arm was dead code -- a work_unit_id
    # is a content hash, never a work_item_id, so it could not match a single
    # attribution row. It must stay deleted.
    assert "[work_unit_investments.work_unit_id]" not in sql
    assert "t.work_item_id = issue_id" not in sql
    # CHAOS-2833: team resolution MUST read the authoritative primary
    # ClickHouse attribution rows, never the legacy cycle-times rollup.
    assert "FROM work_item_team_attributions FINAL" in sql
    assert "is_primary = 1" in sql
    assert "(work_item_id, computed_at) IN" in sql
    assert "max(computed_at)" in sql
    assert "work_item_cycle_times" not in sql


def test_graphql_sankey_team_join_scopes_org_and_bridges_pr_evidence() -> None:
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
async def test_graphql_sankey_coverage_team_join_scopes_org_and_bridges_pr_evidence(
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
