"""Live-ClickHouse regression for CHAOS-2430 per-node stale scoping (split/merge).

work_unit_id is a hash of the connected component. When edge churn moves a node
into a new component, the OLD work_unit_id is never re-emitted. A per-work_unit_id
"latest run" guard would keep that dead unit's rows alive forever, so the node
would keep matching obsolete theme/subcategory categories (filter path) and
annotation would see multiple is_dominant rows. The fix scopes staleness per
NODE: keep only rows where computed_at == max(computed_at) per
(org_id, node_type, node_id).

This test seeds two runs for one node — an OLD component (dominant maintenance)
superseded by a NEW component (dominant feature_delivery) at a later
computed_at — and asserts via the real resolver that:
  * filtering by the OLD theme (maintenance) returns NO edge,
  * filtering by the NEW theme (feature_delivery) returns the edge,
  * the unfiltered annotation shows exactly the NEW dominant (no duplicates).

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
    ]


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
    org_id = f"test-chaos-2430-{uuid.uuid4()}"
    node_id = f"ISSUE-{uuid.uuid4()}"
    pr_id = f"PR-{uuid.uuid4()}"
    edge_id = f"edge-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    old_run = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new_run = datetime(2026, 2, 1, tzinfo=timezone.utc)

    # OLD component: node dominant maintenance. NEW component (later run,
    # different work_unit_id): node dominant feature_delivery; maintenance gone.
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
            old_run,
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
            old_run,
        ],
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
            new_run,
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
            new_run,
        ],
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
            new_run,
            new_run,
            new_run,
            new_run.date(),
        ]
    ]

    sink.client.insert(
        "work_unit_membership", membership_rows, column_names=_membership_cols()
    )
    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())

    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # Filter by the OLD theme → the node's prior-component category must be
        # gone (per-node latest run supersedes it). No edge returned.
        old_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="maintenance")
        )
        assert old_result.edges == [], (
            "stale OLD-component theme should not match after the node moved "
            "to a new work_unit_id"
        )

        # Filter by the NEW theme → the edge is returned.
        new_result = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(theme="feature_delivery")
        )
        assert [e.edge_id for e in new_result.edges] == [edge_id]
        assert new_result.edges[0].theme == "feature_delivery"

        # Unfiltered annotation → exactly the NEW dominant, no duplicate/stale.
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
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
