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


@pytest.mark.asyncio
async def test_empty_complete_run_supersedes_previous(sink):
    """FINDING #1 (empty-but-complete run MUST supersede): an OLD complete run
    with real membership rows, then a NEWER ALL-SKIPPED run that wrote ONLY a
    completion marker (zero membership rows). The newer empty marker is the
    latest complete run, so the OLD run's categories no longer match — the node
    is absent from the latest complete run. This is the no-tombstone retirement
    path: an empty complete run drops churned nodes.
    """
    org_id = f"test-chaos-2433-empty-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    old_run_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    empty_run_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
    old_run_id = uuid.uuid4().hex
    empty_run_id = uuid.uuid4().hex

    # OLD run: complete, real rows (maintenance dominant).
    membership_rows = [
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
    ]
    # Two markers: OLD (real rows) and EMPTY (later completed_at, ZERO rows).
    run_marker_rows = [
        [org_id, old_run_id, old_run_ts],
        [org_id, empty_run_id, empty_run_ts],  # all-skipped run: marker, no rows
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
            empty_run_ts,
            empty_run_ts,
            empty_run_ts,
            empty_run_ts.date(),
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
        # The empty run (empty_run_id) is the latest complete run. The OLD run's
        # maintenance rows are no longer in scope → no edge matches.
        result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert result.edges == [], (
            "an empty-but-complete newer run must supersede the previous run; "
            "the stale OLD-run categories must stop matching (CHAOS-2433 #1)"
        )

        # Unfiltered annotation also shows no category (node absent from the
        # latest complete run, which has zero rows).
        unfiltered = await resolve_work_graph_edges(context)
        edge = next(e for e in unfiltered.edges if e.edge_id == edge_id)
        assert edge.theme is None
        assert edge.subcategory is None
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


@pytest.mark.asyncio
async def test_legacy_rows_readable_then_superseded_by_real_run(sink):
    """FINDING #3 (migration orphans existing rows): simulate PRE-existing
    membership rows (run_id='') with the seeded '__legacy__' marker (migration
    048). Those rows must remain filterable/annotated. Once a REAL marked run
    lands (later completed_at), it supersedes the legacy path.
    """
    org_id = f"test-chaos-2433-legacy-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    # Legacy marker completed_at = max(existing computed_at) — in the past.
    legacy_ts = datetime(2026, 1, 15, tzinfo=timezone.utc)
    real_ts = datetime(2026, 4, 1, tzinfo=timezone.utc)
    real_run_id = uuid.uuid4().hex

    # Pre-existing rows: run_id='' (the 047 default), maintenance dominant.
    legacy_rows = [
        [
            org_id,
            "issue",
            node_id,
            "U_LEGACY",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            legacy_ts,
            "",  # run_id default from migration 047
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_LEGACY",
            "subcategory",
            "maintenance.refactor",
            0.9,
            1,
            "ok",
            legacy_ts,
            "",
        ],
    ]
    # Migration 048 seeds ONE '__legacy__' marker per org, completed_at=max
    # existing computed_at.
    legacy_marker = [[org_id, "__legacy__", legacy_ts]]

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
            real_ts,
            real_ts,
            real_ts,
            real_ts.date(),
        ]
    ]

    sink.client.insert(
        "work_unit_membership", legacy_rows, column_names=_membership_cols()
    )
    sink.client.insert(
        "work_unit_membership_runs", legacy_marker, column_names=_membership_run_cols()
    )
    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())

    assert CLICKHOUSE_URI is not None
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # PHASE 1: legacy marker is the latest complete run → pre-existing rows
        # (run_id='') are matched via the legacy fallback.
        legacy_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert [e.edge_id for e in legacy_result.edges] == [edge_id], (
            "pre-existing membership rows (run_id='') must stay filterable under "
            "the seeded __legacy__ marker (CHAOS-2433 #3)"
        )
        # Annotation also resolves from the legacy rows.
        unfiltered = await resolve_work_graph_edges(context)
        edge = next(e for e in unfiltered.edges if e.edge_id == edge_id)
        assert edge.theme == "maintenance"
        assert edge.subcategory == "maintenance.refactor"

        # PHASE 2: a REAL run lands (later completed_at), different dominant.
        real_rows = [
            [
                org_id,
                "issue",
                node_id,
                "U_REAL",
                "theme",
                "feature_delivery",
                0.95,
                1,
                "ok",
                real_ts,
                real_run_id,
            ],
            [
                org_id,
                "issue",
                node_id,
                "U_REAL",
                "subcategory",
                "feature_delivery.roadmap",
                0.95,
                1,
                "ok",
                real_ts,
                real_run_id,
            ],
        ]
        sink.client.insert(
            "work_unit_membership", real_rows, column_names=_membership_cols()
        )
        sink.client.insert(
            "work_unit_membership_runs",
            [[org_id, real_run_id, real_ts]],
            column_names=_membership_run_cols(),
        )

        # The real run supersedes the legacy path (argMax picks real_ts > legacy_ts).
        after = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in after.edges] == [edge_id]
        assert after.edges[0].theme == "feature_delivery"

        # Legacy theme no longer matches (real run is now latest complete).
        old = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert old.edges == [], (
            "once a real run lands, the legacy path retires automatically via "
            "argMax(run_id, completed_at)"
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


@pytest.mark.asyncio
async def test_optimize_final_preserves_prior_complete_run_row(sink):
    """ROUND-2 FINDING #1 (run_id in the dedup key): a background merge / OPTIMIZE
    FINAL must NOT collapse rows for the same (org, node, category) across runs.

    Without run_id in the ORDER BY, ReplacingMergeTree(computed_at) would keep
    only the max-computed_at version, so a newer INCOMPLETE run (no marker) would
    EVICT the prior COMPLETE run's row — after which the resolver scoped to the
    complete run_id reads nothing. With migration 049 (run_id appended to the
    sort key), per-run rows coexist: the complete run's row survives OPTIMIZE
    FINAL and the resolver scoped to it still returns the node.

    This test MUST fail on the 046 sort key and pass after the 049 rebuild
    (the fixture's ensure_schema(force=True) applies all migrations incl. 049).
    """
    org_id = f"test-chaos-2433-optfinal-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    complete_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    incomplete_ts = datetime(2026, 2, 1, tzinfo=timezone.utc)  # newer computed_at
    complete_run_id = uuid.uuid4().hex
    incomplete_run_id = uuid.uuid4().hex

    # SAME node + SAME (category_kind, category) under two runs:
    #   COMPLETE run (older computed_at, marker published) — maintenance.
    #   INCOMPLETE run (newer computed_at, NO marker) — same category, lower weight.
    # On the old key these collapse to the newer (incomplete) version on merge.
    membership_rows = [
        [
            org_id,
            "issue",
            node_id,
            "U_C",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            complete_ts,
            complete_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_C",
            "subcategory",
            "maintenance.refactor",
            0.9,
            1,
            "ok",
            complete_ts,
            complete_run_id,
        ],
        # In-flight run: same node + SAME categories, newer computed_at, no marker.
        [
            org_id,
            "issue",
            node_id,
            "U_I",
            "theme",
            "maintenance",
            0.5,
            1,
            "ok",
            incomplete_ts,
            incomplete_run_id,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_I",
            "subcategory",
            "maintenance.refactor",
            0.5,
            1,
            "ok",
            incomplete_ts,
            incomplete_run_id,
        ],
    ]
    # Only the COMPLETE run has a marker (the in-flight run has not published).
    run_marker_rows = [[org_id, complete_run_id, complete_ts]]
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
            incomplete_ts,
            incomplete_ts,
            incomplete_ts,
            incomplete_ts.date(),
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

    # Force a merge: this is the operation that would EVICT the complete run's
    # row under the old (run_id-less) dedup key.
    sink.client.command("OPTIMIZE TABLE work_unit_membership FINAL")

    assert CLICKHOUSE_URI is not None
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # The COMPLETE run's row must STILL be physically present after the merge.
        remaining = sink.client.query(
            "SELECT run_id, weight FROM work_unit_membership "
            "WHERE org_id = {o:String} AND run_id = {r:String}",
            parameters={"o": org_id, "r": complete_run_id},
        ).result_rows
        assert remaining, (
            "the prior COMPLETE run's membership rows were EVICTED by OPTIMIZE "
            "FINAL — run_id must be in the dedup key (CHAOS-2433 finding #1)"
        )

        # The resolver (scoped to the COMPLETE run via its marker, since the
        # in-flight run has no marker) still returns the node under maintenance.
        result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert [e.edge_id for e in result.edges] == [edge_id], (
            "after OPTIMIZE FINAL the resolver scoped to the complete run must "
            "still return the node (the complete run's row survived)"
        )
        assert result.edges[0].theme == "maintenance"
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


@pytest.mark.asyncio
async def test_overlap_later_marker_wins_over_earlier_marker(sink):
    """ROUND-3 FINDING #1 (marker completed_at = COMPLETION order, not start):
    a run whose membership ROWS carry an EARLIER computed_at but whose MARKER has
    a LATER completed_at must WIN argMax(run_id, completed_at).

    This is the overlap race: a long materializer/projection that STARTS before
    another run (so its rows carry an earlier run-start computed_at) but FINISHES
    after it (so its marker carries a later completion timestamp) must be the one
    readers select. We invert the two axes — run A's rows are NEWER (computed_at)
    but its marker is OLDER (completed_at); run B's rows are OLDER but its marker
    is NEWER — and assert the resolver follows the MARKER (run B wins).
    """
    org_id = f"test-chaos-2433-overlap-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())

    run_a = uuid.uuid4().hex  # rows NEWER (start later) but marker OLDER (finish first)
    run_b = uuid.uuid4().hex  # rows OLDER (start first) but marker NEWER (finish last)

    rows_ts_a = datetime(2026, 5, 1, tzinfo=timezone.utc)  # A rows: newer computed_at
    rows_ts_b = datetime(2026, 4, 1, tzinfo=timezone.utc)  # B rows: older computed_at
    marker_ts_a = datetime(2026, 6, 1, tzinfo=timezone.utc)  # A marker: OLDER finish
    marker_ts_b = datetime(2026, 7, 1, tzinfo=timezone.utc)  # B marker: NEWER finish

    membership_rows = [
        # Run A — maintenance, rows carry the NEWER computed_at.
        [
            org_id,
            "issue",
            node_id,
            "U_A",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            rows_ts_a,
            run_a,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_A",
            "subcategory",
            "maintenance.refactor",
            0.9,
            1,
            "ok",
            rows_ts_a,
            run_a,
        ],
        # Run B — feature_delivery, rows carry the OLDER computed_at.
        [
            org_id,
            "issue",
            node_id,
            "U_B",
            "theme",
            "feature_delivery",
            0.95,
            1,
            "ok",
            rows_ts_b,
            run_b,
        ],
        [
            org_id,
            "issue",
            node_id,
            "U_B",
            "subcategory",
            "feature_delivery.roadmap",
            0.95,
            1,
            "ok",
            rows_ts_b,
            run_b,
        ],
    ]
    # Markers: B finishes LATER (greater completed_at) → B must win argMax.
    run_marker_rows = [
        [org_id, run_a, marker_ts_a],
        [org_id, run_b, marker_ts_b],
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
            marker_ts_b,
            marker_ts_b,
            marker_ts_b,
            marker_ts_b.date(),
        ],
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
        # Run B finished later → its marker wins → feature_delivery matches.
        feature = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in feature.edges] == [edge_id], (
            "the LATER-FINISHING run (greater marker completed_at) must win, even "
            "though its rows carry an OLDER computed_at (CHAOS-2433 round-3 #1)"
        )
        assert feature.edges[0].theme == "feature_delivery"

        # Run A's maintenance must NOT match even though A's ROWS are newer:
        # selection follows the MARKER's completed_at, not the rows' computed_at.
        maintenance = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert maintenance.edges == [], (
            "run A's newer-computed_at rows must lose because its marker finished "
            "FIRST — marker completion order drives selection, not row computed_at"
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


@pytest.mark.asyncio
async def test_full_coverage_projection_does_not_blank_out_of_window_nodes(sink):
    """ROUND-3 FINDING #2 (date-window): a full-coverage projection run must keep
    ALL current components filterable — including ones a date-windowed materialize
    would have skipped. We seed a prior full-coverage run covering an OLD node and
    a NEW node, then a newer full-coverage projection run that ALSO covers both
    (the projection iterates the whole current graph, no time window). Both nodes
    stay filterable under the latest run — neither is blanked.

    (Contrast: a windowed materialize covering only the NEW node would, if it
    published an org-wide marker, blank the OLD node. The unified writer prevents
    that because the materializer no longer publishes membership/markers.)
    """
    org_id = f"test-chaos-2433-window-{uuid.uuid4()}"
    old_node = f"OLD-{uuid.uuid4()}"
    new_node = f"NEW-{uuid.uuid4()}"
    old_pr = f"PR-{uuid.uuid4()}"
    new_pr = f"PR-{uuid.uuid4()}"
    old_edge = f"edge-{uuid.uuid4()}"
    new_edge = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    rows_ts = datetime(2026, 8, 1, tzinfo=timezone.utc)
    marker_ts = datetime(2026, 8, 1, 0, 0, 1, tzinfo=timezone.utc)
    run_id = uuid.uuid4().hex

    # Latest full-coverage projection run covers BOTH the old and the new node.
    membership_rows = [
        [
            org_id,
            "issue",
            old_node,
            "U_OLD",
            "theme",
            "maintenance",
            0.9,
            1,
            "ok",
            rows_ts,
            run_id,
        ],
        [
            org_id,
            "issue",
            new_node,
            "U_NEW",
            "theme",
            "feature_delivery",
            0.95,
            1,
            "ok",
            rows_ts,
            run_id,
        ],
    ]
    run_marker_rows = [[org_id, run_id, marker_ts]]
    edge_rows = [
        [
            org_id,
            old_edge,
            "issue",
            old_node,
            "pr",
            old_pr,
            "implements",
            repo_uuid,
            "github",
            "native",
            1.0,
            "",
            rows_ts,
            rows_ts,
            rows_ts,
            rows_ts.date(),
        ],
        [
            org_id,
            new_edge,
            "issue",
            new_node,
            "pr",
            new_pr,
            "implements",
            repo_uuid,
            "github",
            "native",
            1.0,
            "",
            rows_ts,
            rows_ts,
            rows_ts,
            rows_ts.date(),
        ],
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
        # The OLD node (would be out-of-window for a 30d materialize) is STILL
        # filterable from the full-coverage projection run — not blanked.
        old_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert [e.edge_id for e in old_result.edges] == [old_edge], (
            "the full-coverage projection must keep out-of-window components "
            "filterable — a windowed marker would have blanked them (round-3 #2)"
        )
        # The NEW (in-window) node is filterable too.
        new_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in new_result.edges] == [new_edge]
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


def _wait_for_mutations(sink, table: str) -> None:
    """Block until all pending mutations on ``table`` complete.

    ``prune_membership_runs`` issues ``ALTER TABLE ... DELETE`` mutations, which
    are asynchronous by default. Tests must wait for them to materialize before
    asserting row counts. Polls system.mutations for unfinished entries.
    """
    import time

    for _ in range(100):  # ~10s max
        res = sink.client.query(
            "SELECT count() FROM system.mutations "
            "WHERE database = currentDatabase() AND table = {t:String} "
            "AND is_done = 0",
            parameters={"t": table},
        )
        pending = int(res.result_rows[0][0]) if res.result_rows else 0
        if pending == 0:
            return
        time.sleep(0.1)


def _seed_run(sink, org_id, node_id, run_id, category, ts):
    """Seed one membership row + its completion marker for a complete run."""
    sink.client.insert(
        "work_unit_membership",
        [
            [
                org_id,
                "issue",
                node_id,
                f"U_{run_id}",
                "theme",
                category,
                0.9,
                1,
                "ok",
                ts,
                run_id,
            ]
        ],
        column_names=_membership_cols(),
    )
    sink.client.insert(
        "work_unit_membership_runs",
        [[org_id, run_id, ts]],
        column_names=_membership_run_cols(),
    )


@pytest.mark.asyncio
async def test_retention_keeps_latest_two_complete_runs(sink):
    """ROUND-5 (unbounded growth): after multiple org-wide projections, only the
    latest 2 COMPLETE runs' membership rows AND markers remain; older runs'
    rows and markers are deleted."""
    org_id = f"test-chaos-2433-retain-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run_ids = [uuid.uuid4().hex for _ in range(4)]

    # Seed 4 successive complete runs (older -> newer completed_at).
    from datetime import timedelta

    for i, rid in enumerate(run_ids):
        _seed_run(
            sink, org_id, node_id, rid, "feature_delivery", base + timedelta(days=i)
        )

    try:
        # Retention keeps the latest 2 complete runs.
        pruned = sink.prune_membership_runs(org_id, keep=2)
        assert pruned == 2, "two oldest run generations should be pruned"
        _wait_for_mutations(sink, "work_unit_membership")
        _wait_for_mutations(sink, "work_unit_membership_runs")

        # Only the latest 2 run_ids survive in BOTH tables.
        kept = run_ids[2:]
        dropped = run_ids[:2]

        m_rows = sink.client.query(
            "SELECT DISTINCT run_id FROM work_unit_membership "
            "WHERE org_id = {o:String} ORDER BY run_id",
            parameters={"o": org_id},
        ).result_rows
        surviving_m = {str(r[0]) for r in m_rows}
        assert surviving_m == set(kept), (
            f"membership rows: expected {set(kept)}, got {surviving_m}"
        )
        assert not (surviving_m & set(dropped)), "old runs' rows must be deleted"

        r_rows = sink.client.query(
            "SELECT run_id FROM work_unit_membership_runs FINAL "
            "WHERE org_id = {o:String} ORDER BY run_id",
            parameters={"o": org_id},
        ).result_rows
        surviving_r = {str(r[0]) for r in r_rows}
        assert surviving_r == set(kept), (
            f"markers: expected {set(kept)}, got {surviving_r}"
        )
    finally:
        for table in ("work_unit_membership", "work_unit_membership_runs"):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"o": org_id},
            )


@pytest.mark.asyncio
async def test_retention_never_deletes_markerless_inflight_run(sink):
    """An in-flight run (rows written, NO marker yet) must survive a concurrent
    retention pass — retention only ever deletes MARKERED runs."""
    org_id = f"test-chaos-2433-inflight-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    from datetime import timedelta

    # 3 complete (markered) runs.
    complete = [uuid.uuid4().hex for _ in range(3)]
    for i, rid in enumerate(complete):
        _seed_run(
            sink, org_id, node_id, rid, "feature_delivery", base + timedelta(days=i)
        )

    # An in-flight run: rows ONLY, no marker — the next generation mid-write.
    inflight = uuid.uuid4().hex
    sink.client.insert(
        "work_unit_membership",
        [
            [
                org_id,
                "issue",
                node_id,
                f"U_{inflight}",
                "theme",
                "maintenance",
                0.5,
                1,
                "ok",
                base + timedelta(days=10),
                inflight,
            ]
        ],
        column_names=_membership_cols(),
    )

    try:
        sink.prune_membership_runs(org_id, keep=2)
        _wait_for_mutations(sink, "work_unit_membership")
        _wait_for_mutations(sink, "work_unit_membership_runs")

        # The in-flight run's rows MUST still be present (never markered → never
        # in the delete set).
        inflight_count = sink.client.query(
            "SELECT count() FROM work_unit_membership "
            "WHERE org_id = {o:String} AND run_id = {r:String}",
            parameters={"o": org_id, "r": inflight},
        ).result_rows[0][0]
        assert int(inflight_count) >= 1, (
            "a markerless in-flight run must NEVER be deleted by retention"
        )

        # Of the markered runs, only the latest 2 survive.
        surviving_complete = {
            str(r[0])
            for r in sink.client.query(
                "SELECT DISTINCT run_id FROM work_unit_membership "
                "WHERE org_id = {o:String} AND run_id IN {ids:Array(String)}",
                parameters={"o": org_id, "ids": complete},
            ).result_rows
        }
        assert surviving_complete == set(complete[1:]), (
            "only the latest 2 markered runs survive"
        )
    finally:
        for table in ("work_unit_membership", "work_unit_membership_runs"):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"o": org_id},
            )


@pytest.mark.asyncio
async def test_retention_resolver_still_correct_after_prune(sink):
    """After retention, the resolver still returns the latest run's results, and
    the immediately-previous complete run is still present (the keep=2 overlap
    safety margin)."""
    org_id = f"test-chaos-2433-retain-resolve-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    from datetime import timedelta

    # 3 complete runs; the LATEST is feature_delivery, the prior is maintenance.
    old_runs = [uuid.uuid4().hex, uuid.uuid4().hex]
    for i, rid in enumerate(old_runs):
        _seed_run(sink, org_id, node_id, rid, "operational", base + timedelta(days=i))
    prior_run = uuid.uuid4().hex
    _seed_run(sink, org_id, node_id, prior_run, "maintenance", base + timedelta(days=5))
    latest_run = uuid.uuid4().hex
    _seed_run(
        sink, org_id, node_id, latest_run, "feature_delivery", base + timedelta(days=6)
    )

    sink.client.insert(
        "work_graph_edges",
        [
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
                base,
                base,
                base,
                base.date(),
            ]
        ],
        column_names=_edge_cols(),
    )

    assert CLICKHOUSE_URI is not None
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        sink.prune_membership_runs(org_id, keep=2)
        _wait_for_mutations(sink, "work_unit_membership")
        _wait_for_mutations(sink, "work_unit_membership_runs")

        # The latest run drives the resolver: feature_delivery matches.
        latest = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in latest.edges] == [edge_id]
        assert latest.edges[0].theme == "feature_delivery"

        # The immediately-previous complete run (maintenance) is STILL present
        # (keep=2 overlap margin) — its rows were not pruned.
        prior_rows = sink.client.query(
            "SELECT count() FROM work_unit_membership "
            "WHERE org_id = {o:String} AND run_id = {r:String}",
            parameters={"o": org_id, "r": prior_run},
        ).result_rows[0][0]
        assert int(prior_rows) >= 1, "the prior complete run must survive (keep=2)"

        # The two oldest 'operational' runs were pruned.
        old_rows = sink.client.query(
            "SELECT count() FROM work_unit_membership "
            "WHERE org_id = {o:String} AND run_id IN {ids:Array(String)}",
            parameters={"o": org_id, "ids": old_runs},
        ).result_rows[0][0]
        assert int(old_rows) == 0, "the two oldest runs must be pruned"
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


@pytest.mark.asyncio
async def test_retention_bounds_row_count_across_many_runs(sink):
    """Live bound check: across N successive prune-after-publish cycles, the row
    count stays bounded (it does NOT grow with N)."""
    org_id = f"test-chaos-2433-bound-{uuid.uuid4()}"
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    from datetime import timedelta

    # Each run seeds the SAME 5 nodes (a stable current graph), then prunes.
    nodes = [f"ISSUE-{i}-{uuid.uuid4()}" for i in range(5)]
    counts: list[int] = []
    try:
        for run_i in range(6):
            rid = uuid.uuid4().hex
            ts = base + timedelta(days=run_i)
            rows = [
                [
                    org_id,
                    "issue",
                    n,
                    f"U_{rid}",
                    "theme",
                    "feature_delivery",
                    0.9,
                    1,
                    "ok",
                    ts,
                    rid,
                ]
                for n in nodes
            ]
            sink.client.insert(
                "work_unit_membership", rows, column_names=_membership_cols()
            )
            sink.client.insert(
                "work_unit_membership_runs",
                [[org_id, rid, ts]],
                column_names=_membership_run_cols(),
            )
            sink.prune_membership_runs(org_id, keep=2)
            _wait_for_mutations(sink, "work_unit_membership")
            _wait_for_mutations(sink, "work_unit_membership_runs")
            total = sink.client.query(
                "SELECT count() FROM work_unit_membership WHERE org_id = {o:String}",
                parameters={"o": org_id},
            ).result_rows[0][0]
            counts.append(int(total))

        # After the 2nd run onward, the count is bounded at keep(2) * 5 nodes = 10
        # — it does NOT grow with the number of runs (would be 6*5=30 unpruned).
        assert counts[-1] <= 2 * len(nodes), (
            f"row count must be bounded by keep*nodes, got {counts}"
        )
        # And it never exceeds the bound at any steady-state step.
        assert max(counts[1:]) <= 2 * len(nodes), (
            f"steady-state row count must stay bounded, got {counts}"
        )
    finally:
        for table in ("work_unit_membership", "work_unit_membership_runs"):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"o": org_id},
            )
