"""Live-ClickHouse integration tests for analytics.flowMatrix (CHAOS-1289).

Exercises the real compile → execute pipeline against a running ClickHouse
with seeded demo data. Complements test_flow_matrix.py (which mocks
query_dicts) by proving that the SQL actually returns non-empty, asymmetric
cross-team edges end-to-end.

Run locally with:
  CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default \\
  TEST_ORG_ID=<uuid of a seeded org> \\
  pytest tests/graphql/test_flow_matrix_live.py -v

Skips automatically when CLICKHOUSE_URI is unset so it's CI-safe.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

from dev_health_ops.api.graphql.resolvers.analytics import _execute_sankey_inner
from dev_health_ops.api.graphql.sql.compiler import (
    FlowMatrixRequest,
    compile_flow_matrix,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
TEST_ORG_ID = os.environ.get("TEST_ORG_ID", "70f20609-2156-4f9d-9b9b-90c125755988")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


async def _run_team_flow_matrix(days: int = 90):
    """Compile and execute a TEAM flow matrix against live ClickHouse."""
    from dev_health_ops.api.queries.client import get_global_client

    end = date.today()
    start = end - timedelta(days=days)

    req = FlowMatrixRequest(
        dimension="team",
        measure="count",
        start_date=start,
        end_date=end,
        max_nodes=50,
        max_edges=200,
        use_investment=False,
    )
    nodes_queries, edges_queries = compile_flow_matrix(req, org_id=TEST_ORG_ID)
    client = await get_global_client(CLICKHOUSE_URI)
    return await _execute_sankey_inner(client, nodes_queries, edges_queries)


async def test_team_flow_matrix_returns_nodes() -> None:
    """Nodes must surface at least one team with work items in the window."""
    nodes, _ = await _run_team_flow_matrix()
    assert len(nodes) > 0, "expected at least one team node"
    assert all(n.dimension == "TEAM" for n in nodes)
    assert all(n.id.startswith("TEAM:") for n in nodes)
    assert all(n.value > 0 for n in nodes)


async def test_team_flow_matrix_returns_cross_team_edges() -> None:
    """The whole point of CHAOS-1289: cross-team edges must exist.

    If this fails, the chord renders empty in production — which is the bug
    that occasioned the rewrite of this feature from self-loops to
    directional co-occurrence.
    """
    _, edges = await _run_team_flow_matrix()
    assert len(edges) > 0, (
        "flow matrix returned ZERO edges; chord will render empty. "
        "Demo likely has no cross-team scope+day overlap."
    )
    # All edges must be cross-team (self-loops are filtered server-side).
    assert all(e.source != e.target for e in edges), (
        "self-loop leaked through; server-side filter failed"
    )
    # All edge endpoints must be TEAM-prefixed (prefix parity with nodes).
    assert all(e.source.startswith("TEAM:") for e in edges)
    assert all(e.target.startswith("TEAM:") for e in edges)


async def test_team_flow_matrix_edges_are_asymmetric() -> None:
    """Directional signal proof: at least one bidirectional pair (A,B)/(B,A)
    must have different values. Symmetric edges would re-introduce the bug
    that caused inflow/outflow/net chord modes to collapse.
    """
    _, edges = await _run_team_flow_matrix()
    pair_values = {(e.source, e.target): e.value for e in edges}
    asymmetric_pairs = 0
    for (src, tgt), forward in pair_values.items():
        reverse = pair_values.get((tgt, src))
        if reverse is not None and forward != reverse:
            asymmetric_pairs += 1
    assert asymmetric_pairs > 0, (
        "all bidirectional pairs are symmetric — directional chord modes "
        "(inflow, outflow, net) will collapse. Edges were:\n"
        + "\n".join(f"  {s} -> {t} = {v}" for (s, t), v in pair_values.items())
    )


async def test_team_flow_matrix_edge_endpoints_subset_of_nodes() -> None:
    """Edge endpoints must appear in the node set — otherwise the frontend
    adapter drops them (it looks up nodes by id). This guards against the
    source table divergence that caused the earlier iteration to silently
    hide edges.
    """
    nodes, edges = await _run_team_flow_matrix()
    node_ids = {n.id for n in nodes}
    orphan_sources = {e.source for e in edges if e.source not in node_ids}
    orphan_targets = {e.target for e in edges if e.target not in node_ids}
    assert not orphan_sources, f"edge sources missing from nodes: {orphan_sources}"
    assert not orphan_targets, f"edge targets missing from nodes: {orphan_targets}"
