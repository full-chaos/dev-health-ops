from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

import dev_health_ops.api.queries.investment as investment_queries
import dev_health_ops.api.queries.sankey as sankey_queries
from dev_health_ops.api.graphql.sql.compiler import (
    TimeseriesRequest,
    compile_timeseries,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def _window() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 31, tzinfo=timezone.utc),
    )


def _assert_membership_scope_sql(sql: str) -> None:
    assert "latest_complete_membership_run AS" in sql
    assert "investment_membership_scope_state AS" in sql
    assert "membership_scoped_work_unit_ids AS" in sql
    assert "argMax(run_id, completed_at) AS latest_run_id" in sql
    assert "latest_run.latest_run_id != ''" in sql
    assert "m.run_id = latest_run.latest_run_id" in sql
    assert "max(computed_at) AS legacy_max_computed_at" in sql
    assert "m.computed_at = lnm.legacy_max_computed_at" in sql
    assert "SELECT DISTINCT m.work_unit_id AS work_unit_id" in sql
    assert "(SELECT scope_enabled FROM investment_membership_scope_state) = 0" in sql


def test_latest_row_cte_does_not_shadow_argmax_ordering_column() -> None:
    """Regression: the aggregate in the latest-row CTE must NOT be aliased to
    ``computed_at``.

    ``computed_at`` is the ordering column of every ``argMax(col, computed_at)``
    in the CTE. Aliasing ``max(computed_at) AS computed_at`` makes ClickHouse
    26.5.x resolve that name to the aggregate alias, turning each argMax into
    ``argMax(col, max(computed_at))`` — an aggregate inside an aggregate — which
    fails at analysis time with ``Code: 184 ILLEGAL_AGGREGATION``. Because the
    GraphQL ``analytics`` breakdown/sankey resolvers reuse this exact CTE, that
    error silently emptied the Investment treemap and the allocation flows /
    team-&-repo coverage. The aggregate must keep a distinct alias.
    """
    cte = investment_queries.LATEST_WORK_UNIT_INVESTMENTS_CTE
    assert "argMax(effort_value, computed_at)" in cte  # ordering column is used
    assert "max(computed_at) AS computed_at" not in cte  # the shadowing alias
    assert "max(computed_at) AS latest_computed_at" in cte
    _assert_membership_scope_sql(cte)


def test_work_unit_authors_cte_scopes_dedup_by_org() -> None:
    """Regression for an Oracle NO-GO CRITICAL tenant-isolation leak on
    CHAOS-2492: ``LATEST_WORK_UNIT_AUTHORS_CTE`` deduped ``git_commits`` by
    (repo_id, hash) and ``git_pull_requests`` by (repo_id, number) WITHOUT
    org_id. Those tables are keyed by (org_id, repo_id, hash/number)
    (migration 027), and repo_id+hash / repo_id+number values CAN collide
    across tenants -- this exact collision risk is documented at
    work_graph/builder.py:1643 for the equivalent git_commits join. As
    written, ``argMax(author_email, ...)`` could pull ANOTHER org's
    author_email into this org's investment developer filter. Both inner
    dedupe subqueries must now filter AND group by org_id, and the outer
    join must pin ca.org_id/pa.org_id to the work unit's own org_id -- a
    real live-ClickHouse exec proof of this lives in
    tests/graphql/test_investment_developer_filter_tenant_isolation_live.py
    (pytest -m clickhouse, opt-in).
    """
    cte = investment_queries.LATEST_WORK_UNIT_AUTHORS_CTE

    # git_commits dedupe subquery: filtered AND grouped by org_id, and the
    # outer join is additionally pinned to the work unit's own org_id.
    assert "FROM git_commits" in cte
    assert "GROUP BY org_id, repo_id, hash" in cte
    assert "ca.org_id = wui.org_id" in cte

    # git_pull_requests dedupe subquery: same tenant scoping.
    assert "FROM git_pull_requests" in cte
    assert "GROUP BY org_id, repo_id, number" in cte
    assert "pa.org_id = wui.org_id" in cte

    # Both inner subqueries filter by the query's own org_id param before
    # aggregating -- not just after, via the join.
    assert cte.count("WHERE org_id = %(org_id)s") == 2


@pytest.mark.asyncio
async def test_investment_breakdown_aggregates_only_latest_work_unit_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older = {
        "work_unit_id": "wu-1",
        "org_id": "org-1",
        "from_ts": datetime(2026, 1, 10, tzinfo=timezone.utc),
        "to_ts": datetime(2026, 1, 11, tzinfo=timezone.utc),
        "effort_value": 10.0,
        "subcategory_distribution_json": [("Feature Delivery.product", 1.0)],
        "computed_at": datetime(2026, 1, 12, tzinfo=timezone.utc),
    }
    latest = {
        **older,
        "effort_value": 20.0,
        "computed_at": datetime(2026, 1, 13, tzinfo=timezone.utc),
    }
    physical_rows: list[dict[str, Any]] = [older, latest]

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert "latest_work_unit_investments AS" in sql
        assert "argMax(effort_value, computed_at) AS effort_value" in sql
        # The fixed CTE filters by org before aggregating and groups by
        # (org_id, work_unit_id), so the dedup key is the full tenant identity.
        assert "WHERE org_id = %(org_id)s" in sql
        assert "GROUP BY org_id, work_unit_id" in sql
        scoped_org = params["org_id"]
        latest_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in physical_rows:
            if row["org_id"] != scoped_org:
                continue
            key = (row["org_id"], row["work_unit_id"])
            current = latest_by_key.get(key)
            if current is None or row["computed_at"] > current["computed_at"]:
                latest_by_key[key] = row

        totals: dict[tuple[str, str], float] = {}
        for row in latest_by_key.values():
            for subcategory, probability in row["subcategory_distribution_json"]:
                theme = subcategory.split(".", 1)[0]
                totals[(subcategory, theme)] = totals.get((subcategory, theme), 0.0) + (
                    probability * row["effort_value"]
                )
        return [
            {"subcategory": subcategory, "theme": theme, "value": value}
            for (subcategory, theme), value in totals.items()
        ]

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    start_ts, end_ts = _window()
    rows = await investment_queries.fetch_investment_breakdown(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        scope_filter="",
        scope_params={},
        org_id="org-1",
    )

    assert rows == [
        {
            "subcategory": "Feature Delivery.product",
            "theme": "Feature Delivery",
            "value": 20.0,
        }
    ]


@pytest.mark.asyncio
async def test_investment_breakdown_does_not_collapse_across_tenants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two orgs sharing a work_unit_id must each see their own latest row.

    Regression for CHAOS-2374: the shared latest-row CTE used to
    `GROUP BY work_unit_id` only and then expose `argMax(org_id, computed_at)`.
    A later materialization for org B with the same (colliding) provider-native
    work_unit_id would win the argMax, become the single selected row, and the
    outer `WHERE org_id = %(org_id)s` would then drop org A entirely. The CTE
    now filters org_id first and groups by (org_id, work_unit_id), so each
    tenant retains its own row regardless of the other tenant's sync timing.
    """
    org_a_row = {
        "work_unit_id": "ENG-100",  # provider-native id that collides across tenants
        "org_id": "org-a",
        "from_ts": datetime(2026, 1, 10, tzinfo=timezone.utc),
        "to_ts": datetime(2026, 1, 11, tzinfo=timezone.utc),
        "effort_value": 7.0,
        "subcategory_distribution_json": [("Feature Delivery.product", 1.0)],
        # Older computed_at: org-b would win a cross-tenant argMax(org_id).
        "computed_at": datetime(2026, 1, 12, tzinfo=timezone.utc),
    }
    org_b_row = {
        "work_unit_id": "ENG-100",
        "org_id": "org-b",
        "from_ts": datetime(2026, 1, 10, tzinfo=timezone.utc),
        "to_ts": datetime(2026, 1, 11, tzinfo=timezone.utc),
        "effort_value": 99.0,
        "subcategory_distribution_json": [("Keeping the Lights On.ops", 1.0)],
        # Newer computed_at: this row would win a global argMax and hide org-a.
        "computed_at": datetime(2026, 1, 20, tzinfo=timezone.utc),
    }
    physical_rows: list[dict[str, Any]] = [org_a_row, org_b_row]

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # Faithfully model the fixed CTE: org filter applied BEFORE aggregation,
        # dedup key = (org_id, work_unit_id).
        assert "WHERE org_id = %(org_id)s" in sql
        assert "GROUP BY org_id, work_unit_id" in sql
        scoped_org = params["org_id"]
        latest_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in physical_rows:
            if row["org_id"] != scoped_org:
                continue
            key = (row["org_id"], row["work_unit_id"])
            current = latest_by_key.get(key)
            if current is None or row["computed_at"] > current["computed_at"]:
                latest_by_key[key] = row
        totals: dict[tuple[str, str], float] = {}
        for row in latest_by_key.values():
            for subcategory, probability in row["subcategory_distribution_json"]:
                theme = subcategory.split(".", 1)[0]
                totals[(subcategory, theme)] = totals.get((subcategory, theme), 0.0) + (
                    probability * row["effort_value"]
                )
        return [
            {"subcategory": subcategory, "theme": theme, "value": value}
            for (subcategory, theme), value in totals.items()
        ]

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    start_ts, end_ts = _window()

    # org-a must still see its own row even though org-b has a newer computed_at
    # on the same work_unit_id.
    rows_a = await investment_queries.fetch_investment_breakdown(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        scope_filter="",
        scope_params={},
        org_id="org-a",
    )
    assert rows_a == [
        {
            "subcategory": "Feature Delivery.product",
            "theme": "Feature Delivery",
            "value": 7.0,
        }
    ]

    # org-b sees only its own row — no bleed from org-a.
    rows_b = await investment_queries.fetch_investment_breakdown(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        scope_filter="",
        scope_params={},
        org_id="org-b",
    )
    assert rows_b == [
        {
            "subcategory": "Keeping the Lights On.ops",
            "theme": "Keeping the Lights On",
            "value": 99.0,
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetcher",
    [
        investment_queries.fetch_investment_breakdown,
        investment_queries.fetch_investment_edges,
        investment_queries.fetch_investment_subcategory_edges,
        investment_queries.fetch_investment_team_edges,
        investment_queries.fetch_investment_repo_team_edges,
        investment_queries.fetch_investment_team_category_repo_edges,
        investment_queries.fetch_investment_team_subcategory_repo_edges,
        investment_queries.fetch_investment_unassigned_counts,
        investment_queries.fetch_investment_sunburst,
        investment_queries.fetch_investment_quality_stats,
    ],
)
async def test_investment_queries_read_latest_work_unit_rows(
    monkeypatch: pytest.MonkeyPatch,
    fetcher: Any,
) -> None:
    captured: dict[str, str] = {"sql": ""}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        captured["sql"] = sql
        return []

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    start_ts, end_ts = _window()
    await fetcher(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        scope_filter="",
        scope_params={},
        org_id="org-1",
    )

    sql = captured["sql"]
    assert "latest_work_unit_investments AS" in sql
    assert "FROM latest_work_unit_investments AS work_unit_investments" in sql
    assert "argMax(effort_value, computed_at) AS effort_value" in sql
    assert "work_unit_investments.from_ts < %(end_ts)s" in sql
    assert "work_unit_investments.to_ts >= %(start_ts)s" in sql
    assert "work_unit_investments.org_id = %(org_id)s" in sql
    # Tenant-scope the latest-row CTE itself (CHAOS-2374): org filter must be
    # applied before aggregation and the dedup key must include org_id.
    assert "WHERE org_id = %(org_id)s" in sql
    assert "GROUP BY org_id, work_unit_id" in sql
    _assert_membership_scope_sql(sql)


@pytest.mark.asyncio
async def test_sankey_flow_items_read_latest_work_unit_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {"sql": ""}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        captured["sql"] = sql
        return []

    monkeypatch.setattr(sankey_queries, "query_dicts", fake_query_dicts)
    start_ts, end_ts = _window()

    await sankey_queries.fetch_investment_flow_items(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        scope_filter="",
        scope_params={},
        limit=10,
        org_id="org-1",
    )

    assert "latest_work_unit_investments AS" in captured["sql"]
    assert (
        "FROM latest_work_unit_investments AS work_unit_investments" in captured["sql"]
    )
    _assert_membership_scope_sql(captured["sql"])


def test_investment_timeseries_compiler_reads_latest_work_unit_rows() -> None:
    sql, params = compile_timeseries(
        TimeseriesRequest(
            dimension="theme",
            measure="count",
            interval="day",
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            end_date=datetime(2026, 1, 31, tzinfo=timezone.utc).date(),
        ),
        org_id="org-1",
    )

    assert params["org_id"] == "org-1"
    assert "latest_work_unit_investments AS" in sql
    assert "FROM latest_work_unit_investments AS work_unit_investments" in sql
    assert "work_unit_investments.org_id = %(org_id)s" in sql
    _assert_membership_scope_sql(sql)
