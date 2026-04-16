"""Assert Sankey resolver executes inner queries concurrently, not serially."""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_sankey_nodes_queries_run_concurrently(monkeypatch):
    """When compile_sankey returns N node queries, fetch_nodes must dispatch them
    in parallel via asyncio.gather. We prove this by checking that the maximum
    observed overlap is > 1 (all N queries are in flight at the same time)."""
    from dev_health_ops.api.graphql.resolvers import analytics as mod

    # Build three fake queries
    fake_node_queries = [("SQL1", {"p": 1}), ("SQL2", {"p": 2}), ("SQL3", {"p": 3})]
    fake_edge_queries = [("SQLE", {"p": 9})]

    active = 0
    peak = 0

    async def fake_query_dicts(client, sql, params):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            if sql.startswith("SQLE"):
                return [
                    {
                        "source_dimension": "team",
                        "target_dimension": "repo",
                        "source": "t1",
                        "target": "r1",
                        "value": 1,
                    }
                ]
            return [
                {"dimension": "team", "node_id": "t1", "value": 1.0},
            ]
        finally:
            active -= 1

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    nodes, edges = await mod._execute_sankey_inner(
        client=object(),
        nodes_queries=fake_node_queries,
        edges_queries=fake_edge_queries,
    )

    assert peak >= 3, f"Expected >=3 concurrent queries, saw peak={peak}"
    assert len(nodes) == 3  # one row per query
    assert len(edges) == 1
