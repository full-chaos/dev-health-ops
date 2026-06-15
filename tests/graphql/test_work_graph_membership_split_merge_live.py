"""Live-ClickHouse regression for CHAOS-2430/2433 run_id scoping (split/merge).

work_unit_id is a hash of the connected component. When edge churn moves a node
into a new component, the OLD work_unit_id is never re-emitted. With the run_id /
completion-marker protocol (CHAOS-2433):
  - Every membership write stamps a run_id on ALL rows and writes a completion
    marker to work_unit_membership_runs LAST.
  - Readers select the latest COMPLETE run via
    argMax(run_id, completed_at) FROM work_unit_membership_runs
    and scope ALL membership reads to that run_id.
  - A node absent from the latest complete run (e.g. old component rows whose
    run_id is no longer the latest) is simply not selectable.

This test seeds TWO runs for one node — an OLD run (run_id=RUN_OLD, dominant
maintenance, complete) and a NEW run (run_id=RUN_NEW, dominant feature_delivery,
complete at a later completed_at) — and asserts via the real resolver that:
  * filtering by the OLD theme (maintenance) returns NO edge (RUN_NEW is latest),
  * filtering by the NEW theme (feature_delivery) returns the edge,
  * the unfiltered annotation shows exactly the NEW dominant (no stale rows).

Also tests the CONCURRENCY RACE: when RUN_NEW has membership rows but NO marker
yet, the resolver uses RUN_OLD (the prior complete run) — not the half-written
RUN_NEW. After RUN_NEW's marker is written, the resolver switches to RUN_NEW.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeFilterInput
from dev_health_ops.api.graphql.resolvers.work_graph import resolve_work_graph_edges

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _membership_cols() -> list[str]:
    return [
        "org_id",
        "node_type",
        "node_id",
        "work_unit_id",
        "category_kind",
        "category",
        "weight",
        "is_dominant",
        "categorization_status",
        "computed_at",
        "run_id",
    ]


def _membership_run_cols() -> list[str]:
    return ["org_id", "run_id", "completed_at"]


def _edge_cols() -> list[str]:
    return [
        "org_id",
        "edge_id",
        "source_type",
        "source_id",
        "target_type",
        "target_id",
        "edge_type",
        "repo_id",
        "provider",
        "provenance",
        "confidence",
        "evidence",
        "discovered_at",
        "last_synced",
        "event_ts",
        "day",
    ]


@pytest.mark.asyncio
async def test_split_merge_old_categories_disappear(sink):
    """Run_id protocol: only rows in the latest COMPLETE run are visible.

    Scenario: node was in OLD component (run_id=RUN_OLD, maintenance dominant),
    then moved to NEW component (run_id=RUN_NEW, feature_delivery dominant).
    Both runs are COMPLETE (markers written). RUN_NEW has a later completed_at
    so it is the latest complete run. Old rows (RUN_OLD) are invisible.
    """
    org_id = f"test-chaos-2430-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    old_run_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new_run_ts = datetime(2026, 2, 1, tzinfo=timezone.utc)
    old_run_id = uuid.uuid4().hex
    new_run_id = uuid.uuid4().hex

    # OLD run: node dominant maintenance; stamped with old_run_id.
    # NEW run (later completed_at): node dominant feature_delivery; stamped with
    # new_run_id.  NEW run is the latest complete run → OLD rows invisible.
    membership_rows = [
        # OLD run rows (run_id=old_run_id)
        [
            org_id,
            "issue",
            node_id,
            "U_OLD",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            old_run_ts,
            old_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_OLD",
            "subcategory",
            "maintenance.refactor",
            0.9,
            1,
            "ok",
            old_run_ts,
            old_run_id,
        ],
        # NEW run rows (run_id=new_run_id)
        [
            org_id,
            "issue",
            node_id,
            "U_NEW",
            "theme",
            "feature_delivery",
            0.95,
            1,
            "ok",
            new_run_ts,
            new_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_NEW",
            "subcategory",
            "feature_delivery.roadmap",
            0.95,
            1,
            "ok",
            new_run_ts,
            new_run_id,
        ],
    ]
    # Completion markers: both runs are complete; RUN_NEW has later completed_at.
    run_marker_rows = [
        [org_id, old_run_id, old_run_ts],
        [org_id, new_run_id, new_run_ts],
    ]
    edge_rows = [
        [
            org_id,
            edge_id,
            "issue",
            node_id,
            "pr",
            pr_id,
            "implements",
            repo_uuid,
            "github",
            "native",
            1.0,
            "",
            new_run_ts,
            new_run_ts,
            new_run_ts,
            new_run_ts.date(),
        ]
    ]

    sink.client.insert(
        "work_unit_membership", membership_rows, column_names=_membership_cols()
    )
    sink.client.insert(
        "work_unit_membership_runs",
        run_marker_rows,
        column_names=_membership_run_cols(),
    )
    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())

    assert CLICKHOUSE_URI is not None  # narrowed by pytestmark.skipif
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # Filter by the OLD theme → the old run's rows are invisible (not the
        # latest complete run). No edge returned.
        old_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert old_result.edges == [], (
            "stale OLD-run theme should not match after a NEW complete run supersedes"
        )

        # Filter by the NEW theme → the edge is returned.
        new_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in new_result.edges] == [edge_id], (
            f"expected [{edge_id!r}], got {[e.edge_id for e in new_result.edges]}"
        )
        assert new_result.edges[0].theme == "feature_delivery"

        # Unfiltered annotation → exactly the NEW dominant, no stale rows.
        unfiltered = await resolve_work_graph_edges(context)
        edge = next(e for e in unfiltered.edges if e.edge_id == edge_id)
        assert edge.theme == "feature_delivery"
        assert edge.subcategory == "feature_delivery.roadmap"
    finally:
        sink.client.command(
            "ALTER TABLE work_unit_membership DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.client.command(
            "ALTER TABLE work_unit_membership_runs DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )


@pytest.mark.asyncio
async def test_concurrency_race_incomplete_run_invisible(sink):
    """CONCURRENCY RACE (CHAOS-2433 round-5 bug): materializer has written some
    membership rows but NOT its completion marker yet. A prior COMPLETE backfill
    run exists. Resolver must use the backfill's complete run, not the in-flight
    materializer rows. After the marker is written, resolver switches to new run.
    """
    org_id = f"test-chaos-2433-race-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    backfill_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    materialize_ts = datetime(2026, 2, 1, tzinfo=timezone.utc)
    backfill_run_id = uuid.uuid4().hex
    materialize_run_id = uuid.uuid4().hex

    # BACKFILL run: complete (has marker). Node is feature_delivery dominant.
    # MATERIALIZER run: membership rows written BUT no marker yet (in-flight).
    #   Node in materializer run is maintenance dominant.
    membership_rows = [
        # Backfill run (complete)
        [
            org_id,
            "issue",
            node_id,
            "U_BF",
            "theme",
            "feature_delivery",
            0.95,
            1,
            "ok",
            backfill_ts,
            backfill_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_BF",
            "subcategory",
            "feature_delivery.roadmap",
            0.95,
            1,
            "ok",
            backfill_ts,
            backfill_run_id,
        ],
        # Materializer run (in-flight — rows present but NO marker yet)
        [
            org_id,
            "issue",
            node_id,
            "U_MAT",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            materialize_ts,
            materialize_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_MAT",
            "subcategory",
            "maintenance.refactor",
            0.9,
            1,
            "ok",
            materialize_ts,
            materialize_run_id,
        ],
    ]
    # Only the backfill run has a marker; materializer has not written its marker yet.
    run_marker_rows = [
        [org_id, backfill_run_id, backfill_ts],
    ]
    edge_rows = [
        [
            org_id,
            edge_id,
            "issue",
            node_id,
            "pr",
            pr_id,
            "implements",
            repo_uuid,
            "github",
            "native",
            1.0,
            "",
            materialize_ts,
            materialize_ts,
            materialize_ts,
            materialize_ts.date(),
        ]
    ]

    sink.client.insert(
        "work_unit_membership", membership_rows, column_names=_membership_cols()
    )
    sink.client.insert(
        "work_unit_membership_runs",
        run_marker_rows,
        column_names=_membership_run_cols(),
    )
    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())

    assert CLICKHOUSE_URI is not None
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # PHASE 1: Materializer in-flight (no marker for materialize_run_id).
        # Resolver must use the backfill's complete run (feature_delivery).

        # The maintenance filter (from the in-flight materializer rows) must NOT
        # match — in-flight rows are invisible.
        race_maintenance = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert race_maintenance.edges == [], (
            "in-flight materializer rows (no marker) must NOT be visible to readers"
        )

        # The feature_delivery filter (from the backfill's complete run) MUST match.
        race_feature = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in race_feature.edges] == [edge_id], (
            "prior COMPLETE backfill run must remain visible while materializer "
            "is in-flight (concurrency race fix)"
        )
        assert race_feature.edges[0].theme == "feature_delivery", (
            "annotation must reflect the complete backfill run's dominant"
        )

        # PHASE 2: Materializer writes its completion marker.
        sink.client.insert(
            "work_unit_membership_runs",
            [[org_id, materialize_run_id, materialize_ts]],
            column_names=_membership_run_cols(),
        )

        # Now resolver must switch to the materializer's run (latest completed_at).
        # maintenance filter now matches (materializer's run is complete + latest).
        after_maintenance = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert [e.edge_id for e in after_maintenance.edges] == [edge_id], (
            "after marker write, materializer's run becomes the latest complete run"
        )
        assert after_maintenance.edges[0].theme == "maintenance"

        # feature_delivery filter no longer matches (node moved out of that theme).
        after_feature = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert after_feature.edges == [], (
            "after marker write, resolver must use the new run; feature_delivery "
            "is no longer in scope for this node"
        )
    finally:
        for table in (
            "work_unit_membership",
            "work_unit_membership_runs",
            "work_graph_edges",
        ):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"o": org_id},
            )
