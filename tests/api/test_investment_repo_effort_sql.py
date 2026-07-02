"""CHAOS-2777: the Allocation-tab Sankey fetchers must read
``work_unit_repo_effort`` so multi-repo work units (whose scalar
``work_unit_investments.repo_id`` is NULL) map their effort to their real repos
instead of collapsing onto 'Unassigned repo'.

These are pure SQL-shape assertions (no DB) that guard the wiring:
``latest_work_unit_repo_effort`` is chained into the WITH clause, the per-repo
allocation is LEFT JOINed on (org_id, work_unit_id) with a scalar fallback that
never drops a unit, and the membership-scope CTE chain still comes first. The
end-to-end sum-invariant / fan-out / coverage behaviour is proven against real
ClickHouse in ``test_investment_repo_effort_live.py`` (pytest -m clickhouse).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

# The three Allocation-tab Sankey edge fetchers that surface a repo dimension
# and effort-weight their values, so they must fan effort out per repo.
REPO_EDGE_FETCHERS = [
    investment_queries.fetch_investment_repo_team_edges,
    investment_queries.fetch_investment_team_category_repo_edges,
    investment_queries.fetch_investment_team_subcategory_repo_edges,
]


async def _capture_sql(
    monkeypatch: pytest.MonkeyPatch, fetcher: Any, **kwargs: Any
) -> str:
    captured: dict[str, str] = {"sql": ""}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        captured["sql"] = sql
        return []

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)
    await fetcher(
        cast(BaseMetricsSink, object()),
        start_ts=datetime(2026, 5, 1, tzinfo=timezone.utc),
        end_ts=datetime(2026, 6, 1, tzinfo=timezone.utc),
        scope_filter="",
        scope_params={},
        org_id="org-1",
        **kwargs,
    )
    return captured["sql"]


def test_repo_effort_cte_dedups_by_org_work_unit_repo() -> None:
    """The shared CTE dedups per (org_id, work_unit_id, repo_id) via
    argMax(computed_at) and is tenant-scoped before aggregating."""
    cte = investment_queries.LATEST_WORK_UNIT_REPO_EFFORT_CTE
    assert "latest_work_unit_repo_effort AS (" in cte
    assert "FROM work_unit_repo_effort" in cte
    assert "WHERE org_id = %(org_id)s" in cte
    assert "argMax(effort_value, computed_at) AS repo_effort_value" in cte
    assert "GROUP BY org_id, work_unit_id, repo_id" in cte


def test_repo_allocated_source_falls_back_to_scalar_repo() -> None:
    """The derived source LEFT JOINs the per-repo allocation on
    (org_id, work_unit_id) and falls back to the scalar repo_id / effort_value
    when a unit has no allocation row (so units are never dropped)."""
    src = investment_queries.REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE
    assert "LEFT JOIN latest_work_unit_repo_effort AS wure" in src
    assert "ON wure.org_id = wui.org_id" in src
    assert "AND wure.work_unit_id = wui.work_unit_id" in src
    # Scalar fallback (mirrors compiler.py): an unmatched LEFT JOIN leaves the
    # String key '', so the scalar branch only fires for units with no alloc row.
    assert "if(wure.work_unit_id != '', wure.repo_id, wui.repo_id) AS repo_id" in src
    assert (
        "if(wure.work_unit_id != '', wure.repo_effort_value, wui.effort_value) AS effort_value"
        in src
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fetcher", REPO_EDGE_FETCHERS)
async def test_repo_edge_fetchers_read_allocation(
    monkeypatch: pytest.MonkeyPatch, fetcher: Any
) -> None:
    sql = await _capture_sql(monkeypatch, fetcher)
    # The allocation CTE is chained into the WITH clause and consumed as the
    # per-repo fan-out source.
    assert "latest_work_unit_repo_effort AS (" in sql
    assert "LEFT JOIN latest_work_unit_repo_effort AS wure" in sql
    assert (
        "if(wure.work_unit_id != '', wure.repo_effort_value, wui.effort_value)" in sql
    )
    # Effort is still subcategory-weighted; the per-repo split preserves the
    # unit total by construction.
    assert "sum(subcategory_kv.2 * effort_value) AS value" in sql
    # Membership-scope CTE chain must stay FIRST in the WITH clause, before the
    # repo-effort CTE (org isolation / stale-scope fallback depend on order).
    assert sql.index("latest_complete_membership_run AS") < sql.index(
        "latest_work_unit_repo_effort AS ("
    )


@pytest.mark.asyncio
async def test_unassigned_counts_excludes_allocated_multi_repo_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """missing_repo must count only units with a NULL scalar repo_id AND no
    per-repo allocation row. Multi-repo units carry a NULL scalar repo_id but DO
    have allocation rows, so they must not be counted as unassigned."""
    sql = await _capture_sql(
        monkeypatch, investment_queries.fetch_investment_unassigned_counts
    )
    assert "latest_work_unit_repo_effort AS (" in sql
    # Per-unit existence check (aggregated, so it does not fan the count out).
    assert ") AS unit_repo_alloc" in sql
    assert "ON unit_repo_alloc.org_id = work_unit_investments.org_id" in sql
    assert (
        "AND unit_repo_alloc.work_unit_id = work_unit_investments.work_unit_id" in sql
    )
    assert "repo_id IS NULL AND unit_repo_alloc.work_unit_id = ''" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("fetcher", REPO_EDGE_FETCHERS)
async def test_repo_edge_fetchers_thread_category_filter(
    monkeypatch: pytest.MonkeyPatch, fetcher: Any
) -> None:
    """Theme/subcategory filters still parameterize the query after the repo
    fan-out is wired in (no accidental literal interpolation)."""
    sql = await _capture_sql(
        monkeypatch,
        fetcher,
        themes=["Feature Delivery"],
        subcategories=["Feature Delivery.product"],
    )
    assert "splitByChar('.', subcategory_kv.1)[1] IN %(themes)s" in sql
    assert "subcategory_kv.1 IN %(subcategories)s" in sql
