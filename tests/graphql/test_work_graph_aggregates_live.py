"""Live-ClickHouse acceptance test for CHAOS-2442 work-graph aggregates.

Reproduces the production starvation bug end-to-end: a test org whose edge mix is
dominated by ``references`` edges (>1000) plus a handful of dependency
(issue↔issue) edges. On main, a single unordered ``LIMIT 1000`` fetch returns
almost all ``references`` rows, so:
  * the Dependencies tab renders empty, and
  * Inflow/Outflow collapses to a degenerate split.

This test asserts the fix:
  (a) a dependencies fetch (``edge_types`` = issue↔issue dependency types)
      returns the dependency edges DIRECTLY — never starved by the cap; and
  (b) ``work_graph_flow`` / ``work_graph_artifacts``, computed over the FULL
      edge set, reflect the true mix (not a capped-page artifact).

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    WorkGraphEdgeFilterInput,
    WorkGraphEdgeTypeInput,
)
from dev_health_ops.api.graphql.models.outputs import WorkGraphNodeType
from dev_health_ops.api.graphql.resolvers.work_graph import (
    resolve_work_graph_artifacts,
    resolve_work_graph_edges,
    resolve_work_graph_flow,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

# Number of `references` edges to seed — exceeds the default LIMIT 1000 so the
# unordered cap would, on main, swallow the page and starve dependency edges.
N_REFERENCES = 1200
N_DEPENDENCIES = 8


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


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
async def test_aggregates_reflect_full_mix_not_capped_page(sink):
    org_id = f"test-chaos-2442-{uuid.uuid4()}"
    repo_uuid = str(uuid.uuid4())
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)

    edge_rows: list[list[object]] = []

    # >1000 references edges: issue -> commit (dominant mass that, unordered,
    # fills the entire LIMIT 1000 page on main).
    for i in range(N_REFERENCES):
        edge_rows.append(
            [
                org_id,
                f"ref-{i}-{uuid.uuid4()}",
                "issue",
                f"ISSUE-REF-{i}",
                "commit",
                f"COMMIT-{i}",
                "references",
                repo_uuid,
                "github",
                "heuristic",
                0.30,  # low confidence → sorted AFTER dependency edges
                "",
                ts,
                ts,
                ts,
                ts.date(),
            ]
        )

    # A handful of dependency (issue <-> issue) edges with HIGH confidence.
    dep_edge_ids: list[str] = []
    for i in range(N_DEPENDENCIES):
        eid = f"dep-{i}-{uuid.uuid4()}"
        dep_edge_ids.append(eid)
        edge_rows.append(
            [
                org_id,
                eid,
                "issue",
                f"ISSUE-A-{i}",
                "issue",
                f"ISSUE-B-{i}",
                "blocks" if i % 2 == 0 else "relates",
                repo_uuid,
                "github",
                "native",
                0.99,  # high confidence
                "",
                ts,
                ts,
                ts,
                ts.date(),
            ]
        )

    # A DUPLICATE physical version of the first dependency edge (SAME edge_id,
    # later last_synced) — simulates an un-merged ReplacingMergeTree retry. The
    # aggregates dedup via uniqExact(edge_id), so this must NOT inflate counts.
    dup = list(edge_rows[N_REFERENCES])  # first dependency edge row
    dup[10] = 0.95  # different confidence (a later version of the same edge)
    dup[13] = datetime(2026, 6, 2, tzinfo=timezone.utc)  # later last_synced
    edge_rows.append(dup)

    sink.client.insert("work_graph_edges", edge_rows, column_names=_edge_cols())

    assert CLICKHOUSE_URI is not None  # narrowed by pytestmark.skipif
    context = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink)

    try:
        # (a) Dependencies fetch via plural edge_types returns the dependency
        # edges DIRECTLY — they are filtered BEFORE the cap, so the >1000
        # references can never starve them.
        dep_result = await resolve_work_graph_edges(
            context,
            WorkGraphEdgeFilterInput(
                edge_types=[
                    WorkGraphEdgeTypeInput.BLOCKS,
                    WorkGraphEdgeTypeInput.RELATES,
                ],
                limit=1000,
            ),
        )
        returned_dep_ids = {e.edge_id for e in dep_result.edges}
        assert set(dep_edge_ids) <= returned_dep_ids, (
            "dependency edges must be fetched directly via edge_types, never "
            "starved by the >1000 references cap"
        )
        assert all(e.edge_type.value in ("blocks", "relates") for e in dep_result.edges)

        # Sanity: with a NARROWING filter active (repo_ids), the candidate set is
        # bounded so the relevance sort (ORDER BY confidence DESC) applies and the
        # high-confidence dependency edges surface on the first page. (The
        # fully-unfiltered overview intentionally emits NO ORDER BY to preserve
        # early-LIMIT termination — see resolve_work_graph_edges.)
        overview = await resolve_work_graph_edges(
            context, WorkGraphEdgeFilterInput(repo_ids=[repo_uuid], limit=1000)
        )
        assert any(
            e.edge_type.value in ("blocks", "relates") for e in overview.edges
        ), (
            "with a narrowing filter, ordering by confidence must surface "
            "dependency edges on the capped overview page"
        )
        assert overview.edges[0].edge_type.value in ("blocks", "relates"), (
            "highest-confidence (dependency) edge should sort first under the "
            "relevance order"
        )

        # (b) work_graph_flow reflects the FULL mix (all 1208 edges), not a cap.
        flow = await resolve_work_graph_flow(context)
        by_type = {r.node_type: r for r in flow.rows}
        # issue: outflow = N_REFERENCES (issue->commit) + N_DEPENDENCIES (issue->issue)
        assert by_type[WorkGraphNodeType.ISSUE].outflow == N_REFERENCES + N_DEPENDENCIES
        # issue inflow = N_DEPENDENCIES (issue->issue targets)
        assert by_type[WorkGraphNodeType.ISSUE].inflow == N_DEPENDENCIES
        # commit inflow = N_REFERENCES; outflow 0
        assert by_type[WorkGraphNodeType.COMMIT].inflow == N_REFERENCES
        assert by_type[WorkGraphNodeType.COMMIT].outflow == 0
        assert flow.degraded_reason is None

        # (b) work_graph_artifacts ranks by degree over the FULL set. The busiest
        # node types are present and the count is bounded by the limit.
        artifacts = await resolve_work_graph_artifacts(
            context, WorkGraphEdgeFilterInput(limit=50)
        )
        assert len(artifacts.rows) <= 50
        assert len(artifacts.rows) > 0
        # Degrees are non-increasing (ORDER BY degree DESC).
        degrees = [r.degree for r in artifacts.rows]
        assert degrees == sorted(degrees, reverse=True)
        assert artifacts.degraded_reason is None
    finally:
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {o:String} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )
