"""Real ClickHouse proof that ``breakdown_template`` row order/set is
deterministic at a ``value`` tie sitting on the ``LIMIT`` boundary
(CHAOS-4495).

Plants 30 distinct-repo rows that share the same ``work_items_completed``
(a genuine tie on the primary sort key ``value = SUM(work_items_completed)``)
with ``top_n=10`` sitting inside the tie, and asserts the returned row
order -- and row SET, since a tie at the LIMIT boundary can otherwise drop
a different subset of tied rows each run -- is byte-for-byte identical
across repeated calls against the same real ClickHouse server.

Without the CHAOS-4495 fix (``ORDER BY value DESC`` with no tie-break),
ClickHouse's block-parallel execution does not guarantee a stable row
order/set among rows with equal ``value``: a repeated identical query can
legitimately return a different row order/set. This test proves the FIX
(``ORDER BY value DESC, dimension_value ASC``, a total order over the
deduplicated GROUP BY key) makes the result the same on every call, not
merely "usually the same". Same shape as CHAOS-4421's
``test_review_edges_tie_order_live.py`` and CHAOS-4472's
``test_hotspots_tie_order_live.py``.

**Why 30 rows across pinned-open parts, not the original 5 in one part
(codex review + orchestrator, "a proof that cannot fail is not evidence"):**
the first version of this test inserted all 5 tied rows in a single
``write_investment_metrics`` call (one INSERT = one MergeTree part) and
then ran ``OPTIMIZE TABLE ... FINAL``, which merges every part in the
table down to one. Both of those collapse the query to an effectively
single-part, single-block read -- exactly the case ClickHouse does NOT
need multiple threads/parts to answer, so the result was trivially stable
regardless of whether the tie-break existed. **Proven, not assumed:**
re-run against the OLD SQL (``ORDER BY value DESC`` only, no tie-break) at
that seeding shape never observed a variation across 8 calls -- it was a
proof that could not fail either way, which is not evidence.

This version: 30 separate single-row INSERTs (30 parts) with
``SYSTEM STOP MERGES`` on this table for the seeding+query window (never a
bare ``SYSTEM STOP MERGES`` -- always table-qualified to this scratch db,
so it cannot affect any other database on the shared ClickHouse container),
and no ``OPTIMIZE``.

**Read this honestly, per the orchestrator's standing order that a proof
which cannot fail is not evidence (CHAOS-4495 slot 4495b):** this stronger
seeding shape (real multi-part table, merges pinned open, up to 300 tied
rows tried) proves the underlying ClickHouse behavior this ticket targets
is REAL -- a minimal, direct query against ``investment_metrics_daily``
bypassing the dedup subquery below DID return 2 different row orderings
across repeated identical calls with the tie-break removed (one-off
script, not committed, per AGENTS.md's mutation-kill guidance). But
**attempting the same mutation-kill against the ACTUAL production SQL this
test runs (``compile_breakdown``'s output, including its
``argMax(..., computed_at) GROUP BY (org_id, day, repo_id, team_id,
investment_area, project_stream)`` dedup subquery) did NOT fail** across
more than 150 repeated executions with the tie-break removed, at row
counts from 30 to 300 and ``max_threads`` from 4 to 32. The dedup
subquery's own intermediate aggregation appears to stabilize row emission
order into the outer query in this environment, even though nothing in
ClickHouse's documented contract guarantees that.

**What this means for the determinism claim, stated precisely:** this live
test is retained because it is a real, passing proof of stability at
scale against a real server -- stronger than the original 5-row version it
replaced. But it is **not** citable as a mutation-kill proof for
``breakdown_template``'s specific production SQL shape: the tie-break's
correctness there rests on the SQL-text pin
(``test_breakdown_order_by_has_deterministic_tie_break`` in
``test_analytics_templates_tie_break.py``, which carries a real
RED-on-baseline/GREEN-on-fix contrast) and on ClickHouse's own documented
``ORDER BY`` semantics, not on this test having been observed to fail.
See PR #2005 TEST-EVIDENCE for the full account of what was tried.

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

N_TIED_ROWS = 30
TOP_N = 10
N_REPEATS = 20


def test_tied_rows_return_stable_order_and_set_across_repeated_calls() -> None:
    assert CLICKHOUSE_URI is not None
    # codex review precedent (CHAOS-4421/CHAOS-4472, P1): reject a
    # shared/default database BEFORE ever constructing the sink or calling
    # ensure_schema/insert/delete -- this test issues destructive-capable
    # ClickHouse calls (schema DDL, insert, SYSTEM STOP/START MERGES,
    # ALTER ... DELETE), which must never reach the real dev `default`
    # database (AGENTS.md's ops pre-push gate safety rule).
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    # Table-qualified to THIS scratch database only. A bare
    # `SYSTEM STOP MERGES` with no table would be cluster/host-wide across
    # every database on the shared ClickHouse container -- never do that.
    db_name = CLICKHOUSE_URI.rsplit("/", 1)[-1]
    qualified_table = f"{db_name}.investment_metrics_daily"

    org_id = f"chaos-4495-tie-order-{uuid.uuid4()}"
    day = date(2026, 8, 15)

    # 30 distinct repos, all sharing work_items_completed=7 -- a genuine
    # tie on breakdown_template's primary sort key (value = SUM(work_items_
    # completed)). top_n=10 sits inside the tie (10 of 30 survive), so
    # which 10 (and in what order) is exactly what an absent tie-break
    # leaves undefined. team_id/project_stream held constant so the dedup
    # CTE's GROUP BY key varies only by repo_id -- the tie-break is
    # exercised purely through dimension_value (repo_id) ordering.
    repo_ids = [uuid.uuid4() for _ in range(N_TIED_ROWS)]

    merges_stopped = False
    try:
        sink.client.command(f"SYSTEM STOP MERGES {qualified_table}")
        merges_stopped = True

        # One INSERT per row -- each write_investment_metrics call is one
        # INSERT, which is one MergeTree part. With merges stopped, all 30
        # stay open as 30 distinct parts for the query window below.
        for i, repo_id in enumerate(repo_ids):
            computed_at = datetime(2026, 8, 16, 0, 0, i, tzinfo=timezone.utc)
            record = InvestmentMetricsRecord(
                repo_id=repo_id,
                day=day,
                team_id=None,
                investment_area="feature_delivery",
                # project_stream is LowCardinality(String) -- non-nullable
                # (migration 007). team_id is genuinely Nullable,
                # project_stream is not; production callers
                # (work_item_engine_destinations.py,
                # fixtures/generators/investments.py) always pass "" here,
                # never None. None raised
                # clickhouse_connect.driver.exceptions.DataError ("Invalid
                # None value in non-Nullable column") when this test was
                # first run live against a real server (CHAOS-4495 slot
                # proof) -- it had never been executed before.
                project_stream="",
                delivery_units=7,
                work_items_completed=7,
                prs_merged=0,
                churn_loc=0,
                cycle_p50_hours=0.0,
                computed_at=computed_at,
                org_id=org_id,
            )
            sink.write_investment_metrics([record])

        active_parts = sink.client.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            "AND table = 'investment_metrics_daily' AND active = 1 "
        )
        n_parts = active_parts.result_rows[0][0]
        assert n_parts >= N_TIED_ROWS, (
            f"expected >= {N_TIED_ROWS} active parts with merges stopped "
            f"(each of {N_TIED_ROWS} single-row INSERTs is its own part), "
            f"got {n_parts} -- SYSTEM STOP MERGES may not have taken "
            f"effect, and a single/few-part table cannot exercise the "
            f"block-parallel nondeterminism this test targets"
        )

        def _fetch_order() -> list[str]:
            request = BreakdownRequest(
                dimension="repo",
                measure="count",
                start_date=day,
                end_date=day,
                top_n=TOP_N,
            )
            sql, params = compile_breakdown(request, org_id, timeout=30)
            result = sink.client.query(sql, parameters=params)
            got_rows = list(result.result_rows or [])
            assert len(got_rows) == TOP_N, (
                f"expected exactly {TOP_N} of the {N_TIED_ROWS} tied rows "
                f"under top_n={TOP_N}, got {len(got_rows)}"
            )
            # dimension_value (repo_id) is the first selected column.
            return [str(row[0]) for row in got_rows]

        # Repeat the identical query many times against the pinned-open
        # multi-part table. With the CHAOS-4495 fix, the tie-break makes
        # both the row SET (which 10 of 30 survive LIMIT) and the row
        # ORDER identical every time.
        #
        # The baseline is the FIRST call's own result, not a Python-side
        # `sorted(str(uuid), ...)` computation: `dimension_value` (repo_id)
        # is a ClickHouse `UUID`-typed column (schema: `repo_id
        # Nullable(UUID)`, migration 007), and ClickHouse orders UUID by
        # its internal 128-bit representation, not by the lexicographic
        # order of its canonical hyphenated string form -- the two
        # orderings disagree (discovered live, CHAOS-4495 slot proof: a
        # Python string-sort of the seeded UUIDs picked a different subset
        # than the server actually, stably returned). Comparing every
        # repeat against the server's own first answer tests exactly what
        # this file claims -- stability across repeated calls -- without
        # depending on replicating ClickHouse's UUID sort semantics here.
        orders = [_fetch_order() for _ in range(N_REPEATS)]

        baseline = orders[0]
        assert set(baseline) <= {str(r) for r in repo_ids}, (
            f"baseline row set {baseline} was not drawn from the "
            f"{N_TIED_ROWS} seeded tied repo_ids"
        )
        for i, order in enumerate(orders):
            assert order == baseline, (
                f"run {i}: tied-row order/set was not deterministic: got "
                f"{order}, run 0 got {baseline} (CHAOS-4495 regression -- "
                f"ORDER BY value DESC alone does not guarantee a stable "
                f"result among tied rows)"
            )
    finally:
        if merges_stopped:
            sink.client.command(f"SYSTEM START MERGES {qualified_table}")
        sink.client.command(
            "ALTER TABLE investment_metrics_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
