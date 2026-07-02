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


def test_repo_effort_cte_scopes_to_latest_allocation_generation() -> None:
    """The CTE must keep only the unit's LATEST allocation generation, so stale
    per-repo rows from a shrunk repo set (same work_unit_id, older computed_at)
    do not survive under the per-repo RMT key and inflate the fan-out."""
    cte = investment_queries.LATEST_WORK_UNIT_REPO_EFFORT_CTE
    # Per-unit generation clock = max(computed_at) over ALL of the unit's repo
    # rows (the allocation table's OWN clock, not the investments computed_at).
    assert "max(computed_at) AS unit_generation_at" in cte
    assert "GROUP BY org_id, work_unit_id" in cte  # the clock groups per unit
    assert "WHERE d.latest_repo_effort_computed_at = g.unit_generation_at" in cte
    # Explicit match flag so consumers never depend on a non-empty id sentinel.
    assert "1 AS has_allocation" in cte


def test_repo_allocated_source_falls_back_to_scalar_repo() -> None:
    """The derived source LEFT JOINs the per-repo allocation on
    (org_id, work_unit_id) and falls back to the scalar repo_id / effort_value
    when a unit has no allocation row (so units are never dropped)."""
    src = investment_queries.REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE
    assert "LEFT JOIN latest_work_unit_repo_effort AS wure" in src
    assert "ON wure.org_id = wui.org_id" in src
    assert "AND wure.work_unit_id = wui.work_unit_id" in src
    # Scalar fallback gated on the explicit has_allocation flag (not a
    # non-empty work_unit_id sentinel): unmatched rows fall through to scalar.
    assert "if(wure.has_allocation = 1, wure.repo_id, wui.repo_id) AS repo_id" in src
    assert (
        "if(wure.has_allocation = 1, wure.repo_effort_value, wui.effort_value) AS effort_value"
        in src
    )
    # has_allocation is re-exposed so downstream can tell "no allocation row"
    # apart from "allocation row with a NULL repo".
    assert "if(wure.has_allocation = 1, 1, 0) AS has_allocation" in src


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
        "if(wure.has_allocation = 1, wure.repo_effort_value, wui.effort_value)" in sql
    )
    # Effort is still subcategory-weighted; the per-repo split preserves the
    # unit total by construction.
    assert "sum(subcategory_kv.2 * effort_value) AS value" in sql
    # Membership-scope CTE chain must stay FIRST in the WITH clause, before the
    # repo-effort CTE (org isolation / stale-scope fallback depend on order).
    assert sql.index("latest_complete_membership_run AS") < sql.index(
        "latest_work_unit_repo_effort AS ("
    )
    # HIGH 2: unit_team must read the SAME repo-allocated source so a repo
    # scope_filter is applied to the fanned repo_id, not the scalar one. The
    # source therefore appears at least twice (outer flow + unit_team), and the
    # base table is never the direct FROM of the issue-fanning unit_team.
    assert sql.count("LEFT JOIN latest_work_unit_repo_effort AS wure") >= 2
    assert (
        "FROM latest_work_unit_investments AS work_unit_investments\n"
        "                ARRAY JOIN arrayDistinct(arrayConcat(" not in sql
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
    # Reads the same repo-allocated source as the Sankey edges: on that source
    # ``has_allocation = 0 AND repo_id IS NULL`` is exactly a no-allocation,
    # NULL-scalar unit (allocated repos are non-null and flagged).
    assert "has_allocation = 0 AND repo_id IS NULL" in sql
    assert "LEFT JOIN latest_work_unit_repo_effort AS wure" in sql
    # scope_filter now applies to the fanned repo_id consistently (unit_team +
    # main both read the source).
    assert sql.count("LEFT JOIN latest_work_unit_repo_effort AS wure") >= 2


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
