"""Real ClickHouse proof that ``breakdown_template`` row order/set is
deterministic at a ``value`` tie sitting on the ``LIMIT`` boundary
(CHAOS-4495).

Plants five distinct-repo rows that share the same ``work_items_completed``
(a genuine tie on the primary sort key ``value = SUM(work_items_completed)``)
with ``top_n=3`` sitting exactly at the tie boundary, and asserts the
returned row order -- and row SET, since a tie at the LIMIT boundary can
otherwise drop a different subset of tied rows each run -- is byte-for-byte
identical across repeated calls against the same real ClickHouse server.

Without the CHAOS-4495 fix (``ORDER BY value DESC`` with no tie-break),
ClickHouse's block-parallel execution does not guarantee a stable row
order/set among rows with equal ``value``: a repeated identical query can
legitimately return a different row order/set. This test proves the FIX
(``ORDER BY value DESC, dimension_value ASC``, a total order over the
deduplicated GROUP BY key) makes the result the same on every call, not
merely "usually the same". Same shape as CHAOS-4421's
``test_review_edges_tie_order_live.py`` and CHAOS-4472's
``test_hotspots_tie_order_live.py``.

Exercises the compiler layer directly (``compile_breakdown`` +
``sql/templates.py``'s ``breakdown_template``), not the GraphQL resolver --
CHAOS-4495's scope fence is ``sql/templates.py`` (+ ``sql/compiler.py``)
only; the resolver layer (``api/graphql/resolvers/analytics.py``) is
unmodified by this fix and out of scope.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.graphql.sql.compiler import BreakdownRequest, compile_breakdown
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.metrics.schemas import InvestmentMetricsRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


def test_tied_rows_return_stable_order_and_set_across_repeated_calls() -> None:
    assert CLICKHOUSE_URI is not None
    # codex review precedent (CHAOS-4421/CHAOS-4472, P1): reject a
    # shared/default database BEFORE ever constructing the sink or calling
    # ensure_schema/insert/delete -- this test issues destructive-capable
    # ClickHouse calls (schema DDL, insert, OPTIMIZE, ALTER ... DELETE),
    # which must never reach the real dev `default` database (AGENTS.md's
    # ops pre-push gate safety rule).
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4495-tie-order-{uuid.uuid4()}"
    day = date(2026, 8, 15)
    computed_at = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)

    # Five distinct repos, all sharing work_items_completed=7 -- a genuine
    # tie on breakdown_template's primary sort key (value = SUM(work_items_
    # completed)). top_n=3 sits exactly at the tie boundary, so which 3 of
    # the 5 tied repos survive the LIMIT (and in what order) is exactly what
    # an absent tie-break leaves undefined. team_id/project_stream held
    # constant (None) so the dedup CTE's GROUP BY key varies only by
    # repo_id -- the tie-break is exercised purely through dimension_value
    # (repo_id) ordering.
    repo_ids = [uuid.uuid4() for _ in range(5)]
    records = [
        InvestmentMetricsRecord(
            repo_id=repo_id,
            day=day,
            team_id=None,
            investment_area="feature_delivery",
            project_stream=None,
            delivery_units=7,
            work_items_completed=7,
            prs_merged=0,
            churn_loc=0,
            cycle_p50_hours=0.0,
            computed_at=computed_at,
            org_id=org_id,
        )
        for repo_id in repo_ids
    ]

    try:
        sink.write_investment_metrics(records)
        sink.client.command("OPTIMIZE TABLE investment_metrics_daily FINAL")

        def _fetch_order() -> list[str]:
            request = BreakdownRequest(
                dimension="repo",
                measure="count",
                start_date=day,
                end_date=day,
                top_n=3,
            )
            sql, params = compile_breakdown(request, org_id, timeout=30)
            result = sink.client.query(sql, parameters=params)
            got_rows = list(result.result_rows or [])
            assert len(got_rows) == 3, (
                f"expected exactly 3 of the 5 tied rows under top_n=3, got "
                f"{len(got_rows)}"
            )
            # dimension_value (repo_id) is the first selected column.
            return [str(row[0]) for row in got_rows]

        # Repeat the identical query several times. With the CHAOS-4495 fix,
        # the tie-break makes both the row SET (which 3 of 5 survive LIMIT)
        # and the row ORDER identical every time -- the deterministic
        # tie-break sorts by dimension_value (repo_id) ascending among equal
        # value, so the 3 lexicographically-smallest repo_ids must win every
        # run.
        orders = [_fetch_order() for _ in range(8)]

        expected = sorted(str(r) for r in repo_ids)[:3]
        for i, order in enumerate(orders):
            assert order == expected, (
                f"run {i}: tied-row order/set was not deterministic: got "
                f"{order}, expected {expected} (CHAOS-4495 regression -- "
                f"ORDER BY value DESC alone does not guarantee a stable "
                f"result among tied rows)"
            )
    finally:
        sink.client.command(
            "ALTER TABLE investment_metrics_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
