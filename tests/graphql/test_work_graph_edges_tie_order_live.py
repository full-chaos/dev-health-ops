"""Real ClickHouse proof that the UNFILTERED ``workGraphEdges`` row order is
deterministic at a ``confidence`` tie (CHAOS-4493).

Plants several edges that share the same ``confidence`` (a genuine tie on
the resolver's sort key) with a small ``limit`` sitting exactly at the tie
boundary, and issues the query with NO narrowing filter -- the path that,
before this fix, emitted no ``ORDER BY`` at all. Asserts the returned row
order -- and row SET, since a tie at the LIMIT boundary can otherwise drop a
different subset of tied rows each run -- is byte-for-byte identical across
repeated calls against the same real ClickHouse server.

Without the CHAOS-4493 fix, the unfiltered path's SQL carried no ORDER BY,
so ClickHouse's row set/order among the tied rows was entirely undefined:
the table itself is sorted by (source_type, source_id, edge_type,
target_type, target_id) -- NOT confidence -- so nothing pinned which rows
survive a tie under LIMIT (parallel block processing has no ordering
guarantee beyond the declared sort key). This test proves the FIX (the
resolver now unconditionally emits ``ORDER BY confidence DESC, edge_id ASC``)
makes the result the same on every call, not merely "usually the same".
Same shape as CHAOS-4421's ``test_review_edges_tie_order_live.py`` and
CHAOS-4472's ``test_hotspots_tie_order_live.py``.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeFilterInput
from dev_health_ops.api.graphql.resolvers.work_graph import resolve_work_graph_edges
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.metrics.schemas import WorkGraphEdgeRecord
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
async def test_unfiltered_tied_edges_return_stable_order_and_set_across_repeated_calls() -> (
    None
):
    assert CLICKHOUSE_URI is not None
    # codex review precedent (CHAOS-4421/CHAOS-4472, P1): reject a
    # shared/default database BEFORE ever constructing the sink or calling
    # ensure_schema/insert/delete -- this test issues destructive-capable
    # ClickHouse calls (schema DDL, insert, ALTER ... DELETE), which must
    # never reach the real dev `default` database (AGENTS.md's ops pre-push
    # gate safety rule).
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4493-tie-order-{uuid.uuid4()}"
    day = date(2026, 8, 15)
    discovered_at = datetime(2026, 8, 15, 0, 0, 0, tzinfo=timezone.utc)
    last_synced = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)
    event_ts = datetime(2026, 8, 15, 0, 0, 0, tzinfo=timezone.utc)

    # Five distinct edges, all sharing confidence=0.75 -- a genuine tie on
    # the resolver's primary sort key. limit=3 sits exactly at the tie
    # boundary, so which 3 of the 5 tied rows survive the LIMIT (and in what
    # order) is exactly what an absent ORDER BY leaves undefined. edge_id is
    # the varying column, so the tie-break is exercised purely through
    # edge_id ordering.
    edge_ids = [f"edge-{uuid.uuid4()}" for _ in range(5)]
    edges = [
        WorkGraphEdgeRecord(
            edge_id=edge_id,
            source_type="issue",
            source_id=f"PROJ-{i}",
            target_type="pr",
            target_id=f"repo:{i}",
            edge_type="references",
            repo_id=None,
            provider="github",
            provenance="native",
            confidence=0.75,
            evidence="tie-order-fixture",
            discovered_at=discovered_at,
            last_synced=last_synced,
            event_ts=event_ts,
            day=day,
            org_id=org_id,
        )
        for i, edge_id in enumerate(edge_ids)
    ]

    try:
        sink.write_work_graph_edges(edges)

        async def _fetch_order() -> list[str]:
            ctx = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)
            # No filters at all -- this is the path CHAOS-4493 fixes.
            result = await resolve_work_graph_edges(
                ctx, WorkGraphEdgeFilterInput(limit=3)
            )
            assert len(result.edges) == 3, (
                f"expected exactly 3 of the 5 tied edges under limit=3, got "
                f"{len(result.edges)}"
            )
            return [e.edge_id for e in result.edges]

        # Repeat the identical query several times. With the CHAOS-4493 fix,
        # the tie-break makes both the row SET (which 3 of 5 survive LIMIT)
        # and the row ORDER identical every time -- the deterministic
        # tie-break sorts by edge_id ascending among equal confidence, so the
        # 3 lexicographically-smallest edge_ids must win every run.
        orders = [await _fetch_order() for _ in range(8)]

        expected = sorted(edge_ids)[:3]
        for i, order in enumerate(orders):
            assert order == expected, (
                f"run {i}: tied-edge order/set was not deterministic: got "
                f"{order}, expected {expected} (CHAOS-4493 regression -- the "
                f"unfiltered workGraphEdges path must carry a deterministic "
                f"ORDER BY, not rely on an absent sort key)"
            )
    finally:
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()
