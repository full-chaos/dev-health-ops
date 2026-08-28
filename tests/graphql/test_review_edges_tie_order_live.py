"""Real ClickHouse proof that ``reviewEdges`` row order is deterministic at
a ``reviews_count`` tie (CHAOS-4421 / CHAOS-4368 Part A).

Plants several rows that share the same ``reviews_count`` (a genuine tie on
the resolver's primary sort key) with a small ``limit`` sitting exactly at
the tie boundary, and asserts the returned row order -- and row SET, since a
tie at the LIMIT boundary can otherwise drop a different subset of tied rows
each run -- is byte-for-byte identical across repeated calls against the
same real ClickHouse server.

Without the CHAOS-4421 fix (``ORDER BY reviews_count DESC`` with no
tie-break), ClickHouse does not guarantee a stable order among rows with
equal ``reviews_count``: parallel block processing has no ordering
guarantee beyond the declared sort key, so a repeated identical query can
legitimately return a different row order/set. This test proves the FIX
(the resolver's ORDER BY now carries the full ``repo_id, reviewer, author,
day`` tie-break, which is a total order over the deduplicated row set) makes
the result the same on every call, not merely "usually the same".
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.review_edges import resolve_review_edges
from dev_health_ops.api.graphql.types.review_edges import ReviewEdgesInput
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@pytest.mark.asyncio
async def test_tied_rows_return_stable_order_and_set_across_repeated_calls() -> None:
    assert CLICKHOUSE_URI is not None
    # codex review, 2026-08-28 (P1): reject a shared/default database BEFORE
    # ever constructing the sink or calling ensure_schema/insert/delete --
    # this test issues destructive-capable ClickHouse calls (schema DDL,
    # insert, OPTIMIZE, ALTER ... DELETE), which must never reach the real
    # dev `default` database (AGENTS.md's ops pre-push gate safety rule).
    # Reuses the same canonical scratch-db guard `dev-hops fixtures world`
    # uses (CHAOS-3219), rather than a second ad-hoc check.
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4421-tie-order-{uuid.uuid4()}"
    day = date(2026, 8, 15)
    computed = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)

    # Five distinct (repo_id, reviewer, author, day) rows, all sharing
    # reviews_count=4 -- a genuine tie on the primary sort key. limit=3 sits
    # exactly at the tie boundary, so which 3 of the 5 tied rows survive the
    # LIMIT (and in what order) is exactly what an absent tie-break leaves
    # undefined.
    repo_ids = [str(uuid.uuid4()) for _ in range(5)]
    rows = [
        [
            org_id,
            repo_id,
            day,
            "reviewer@example.com",
            "author@example.com",
            4,
            computed,
        ]
        for repo_id in repo_ids
    ]
    columns = [
        "org_id",
        "repo_id",
        "day",
        "reviewer",
        "author",
        "reviews_count",
        "computed_at",
    ]

    try:
        sink.client.insert("review_edges_daily", rows, column_names=columns)
        sink.client.command("OPTIMIZE TABLE review_edges_daily FINAL")

        async def _fetch_order() -> list[str]:
            ctx = GraphQLContext(
                org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client
            )
            result = await resolve_review_edges(
                ctx,
                ReviewEdgesInput(
                    org_id=org_id,
                    since_date=day,
                    until_date=day,
                    repo_ids=None,
                    limit=3,
                ),
            )
            assert len(result.edges) == 3, (
                f"expected exactly 3 of the 5 tied rows under limit=3, got "
                f"{len(result.edges)}"
            )
            order: list[str] = []
            for edge in result.edges:
                assert edge.repo_id is not None
                order.append(edge.repo_id)
            return order

        # Repeat the identical query several times. With the CHAOS-4421 fix,
        # the tie-break makes both the row SET (which 3 of 5 survive LIMIT)
        # and the row ORDER identical every time -- the deterministic
        # tie-break sorts by repo_id ascending among equal reviews_count, so
        # the 3 lexicographically-smallest repo_ids must win every run.
        orders = [await _fetch_order() for _ in range(8)]

        expected = sorted(repo_ids)[:3]
        for i, order in enumerate(orders):
            assert order == expected, (
                f"run {i}: tied-row order/set was not deterministic: "
                f"got {order}, expected {expected} (CHAOS-4421 regression -- "
                f"ORDER BY reviews_count DESC alone does not guarantee a "
                f"stable result among tied rows)"
            )
    finally:
        sink.client.command(
            "ALTER TABLE review_edges_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
