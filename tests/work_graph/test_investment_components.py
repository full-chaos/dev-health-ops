"""Tests for the shared investment work-unit component builder (CHAOS-2775).

Covers the two safety nets that stop a single densely-linked hub from
percolating thousands of unrelated nodes into one giant "work unit":

1. Heuristic edges are excluded at the ``fetch_work_graph_edges`` choke point.
2. Oversized connected components are deterministically split (drop lowest-
   confidence edges, then remove highest-degree hubs) and never materialized as
   a single unit.

Also proves the materializer and the no-LLM membership backfill derive
IDENTICAL components (→ identical ``work_unit_id`` hashes) from the same edges,
which is required for membership scoping to line up across the two paths.
"""

from __future__ import annotations

from typing import Any, cast

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.work_graph.investment import queries as q
from dev_health_ops.work_graph.investment.backfill import (
    _build_components_for_backfill,
)
from dev_health_ops.work_graph.investment.components import (
    ComponentBuildStats,
    build_components,
)
from dev_health_ops.work_graph.investment.constants import (
    INVESTMENT_MAX_COMPONENT_NODES,
    resolve_max_component_nodes,
)
from dev_health_ops.work_graph.investment.materialize import _build_components
from dev_health_ops.work_graph.investment.utils import work_unit_id


def _edge(
    src_type: str,
    src_id: str,
    tgt_type: str,
    tgt_id: str,
    *,
    provenance: str = "native",
    confidence: float = 1.0,
    edge_id: str | None = None,
) -> dict[str, Any]:
    return {
        "edge_id": edge_id or f"{src_type}:{src_id}->{tgt_type}:{tgt_id}",
        "source_type": src_type,
        "source_id": src_id,
        "target_type": tgt_type,
        "target_id": tgt_id,
        "edge_type": "relates",
        "provenance": provenance,
        "confidence": confidence,
    }


def _component_node_sets(
    components: list[tuple[list[tuple[str, str]], list[dict[str, Any]]]],
) -> list[frozenset[tuple[str, str]]]:
    return [frozenset(nodes) for nodes, _edges in components]


def _max_component_size(
    components: list[tuple[list[tuple[str, str]], list[dict[str, Any]]]],
) -> int:
    return max((len(nodes) for nodes, _edges in components), default=0)


# ---------------------------------------------------------------------------
# (d) Heuristic edges excluded from fetch; native edges kept.
# ---------------------------------------------------------------------------


def test_fetch_excludes_heuristic_by_default(monkeypatch):
    """The fetch choke point drops provenance='heuristic' rows by default and
    keeps native/explicit_text rows — with the filter PARAMETERIZED, never
    interpolated."""
    db_rows = [
        {"edge_id": "e1", "provenance": "heuristic"},
        {"edge_id": "e2", "provenance": "native"},
        {"edge_id": "e3", "provenance": "explicit_text"},
    ]
    captured: dict[str, Any] = {}

    def fake_query_dicts(_sink, query, params):
        captured["query"] = query
        captured["params"] = params
        # Emulate ClickHouse honoring the parameterized predicate.
        rows = db_rows
        if "heuristic_provenance" in params:
            rows = [
                r for r in rows if r["provenance"] != params["heuristic_provenance"]
            ]
        return rows

    monkeypatch.setattr(q, "query_dicts", fake_query_dicts)
    sink = cast(BaseMetricsSink, object())

    rows = q.fetch_work_graph_edges(sink)

    kept = {r["provenance"] for r in rows}
    assert kept == {"native", "explicit_text"}
    # Parameterized: the placeholder is in the SQL, the value lives in params.
    assert "provenance != %(heuristic_provenance)s" in captured["query"]
    assert captured["params"]["heuristic_provenance"] == "heuristic"
    # No value interpolation: the literal string is not spliced into the SQL.
    assert "'heuristic'" not in captured["query"]


def test_fetch_keeps_heuristic_when_disabled(monkeypatch):
    """exclude_heuristic=False restores the pre-CHAOS-2775 behavior (all rows,
    no provenance predicate)."""
    db_rows = [
        {"edge_id": "e1", "provenance": "heuristic"},
        {"edge_id": "e2", "provenance": "native"},
    ]
    captured: dict[str, Any] = {}

    def fake_query_dicts(_sink, query, params):
        captured["query"] = query
        captured["params"] = params
        return db_rows

    monkeypatch.setattr(q, "query_dicts", fake_query_dicts)
    sink = cast(BaseMetricsSink, object())

    rows = q.fetch_work_graph_edges(sink, exclude_heuristic=False)

    assert len(rows) == 2
    assert "provenance !=" not in captured["query"]
    assert "heuristic_provenance" not in captured["params"]


# ---------------------------------------------------------------------------
# (a) Percolation regression: a dense blob glued ONLY by heuristic edges must
#     NOT survive as a giant work unit once fetch strips heuristic edges.
# ---------------------------------------------------------------------------


def test_percolation_glued_only_by_heuristic_edges_does_not_form_giant_unit(
    monkeypatch,
):
    # 60-node star glued exclusively by heuristic edges (the time_window_7d
    # pathology), plus two genuine native components.
    hub = ("pr", "changelog-pr")
    heuristic_edges = [
        _edge(
            "pr",
            "changelog-pr",
            "issue",
            f"H-{i}",
            provenance="heuristic",
            confidence=0.3,
            edge_id=f"heur-{i}",
        )
        for i in range(60)
    ]
    native_edges = [
        _edge("issue", "N-1", "pr", "NP-1"),
        _edge("pr", "NP-1", "commit", "NC-1"),
        _edge("issue", "N-2", "commit", "NC-2"),
    ]
    db_rows = heuristic_edges + native_edges

    def fake_query_dicts(_sink, query, params):
        rows = db_rows
        if "heuristic_provenance" in params:
            rows = [
                r for r in rows if r.get("provenance") != params["heuristic_provenance"]
            ]
        return rows

    monkeypatch.setattr(q, "query_dicts", fake_query_dicts)
    sink = cast(BaseMetricsSink, object())

    edges = q.fetch_work_graph_edges(sink)
    components = build_components(edges)

    # The heuristic-glued blob is gone; the changelog hub isn't in any unit.
    node_sets = _component_node_sets(components)
    assert all(hub not in ns for ns in node_sets)
    # Only the small native components remain — no giant unit.
    assert _max_component_size(components) <= 3
    assert not any(("issue", "H-0") in ns for ns in node_sets)


# ---------------------------------------------------------------------------
# (b) Size-cap split determinism.
# ---------------------------------------------------------------------------


def test_size_cap_splits_on_low_confidence_bridge_deterministically():
    # Two 4-node native paths joined by a single low-confidence bridge; cap 5.
    x_path = [
        _edge("issue", "x0", "pr", "x1"),
        _edge("pr", "x1", "commit", "x2"),
        _edge("commit", "x2", "issue", "x3"),
    ]
    y_path = [
        _edge("issue", "y0", "pr", "y1"),
        _edge("pr", "y1", "commit", "y2"),
        _edge("commit", "y2", "issue", "y3"),
    ]
    bridge = _edge(
        "issue", "x0", "issue", "y0", provenance="explicit_text", confidence=0.3
    )
    edges = x_path + y_path + [bridge]

    stats1 = ComponentBuildStats()
    first = build_components(edges, max_component_nodes=5, stats=stats1)
    stats2 = ComponentBuildStats()
    second = build_components(edges, max_component_nodes=5, stats=stats2)

    # Deterministic: identical fragments (same order, same node sets) both runs.
    assert _component_node_sets(first) == _component_node_sets(second)
    # Split into exactly the two 4-node clusters; low-conf bridge dropped.
    assert sorted(len(nodes) for nodes, _e in first) == [4, 4]
    assert stats1.oversized_components == 1
    assert stats1.dropped_edges == 1
    assert stats1.dropped_nodes == 0
    assert stats1.as_dict() == stats2.as_dict()


def test_size_cap_removes_hub_when_only_max_confidence_edges_remain():
    # A star: one hub with 20 native (max-confidence) spokes. No low-confidence
    # edge exists to drop, so the split must remove the hub node.
    edges = [_edge("pr", "hub", "issue", f"L-{i}") for i in range(20)]

    stats1 = ComponentBuildStats()
    first = build_components(edges, max_component_nodes=5, stats=stats1)
    stats2 = ComponentBuildStats()
    second = build_components(edges, max_component_nodes=5, stats=stats2)

    assert _component_node_sets(first) == _component_node_sets(second)
    assert stats1.oversized_components == 1
    assert stats1.dropped_edges == 0
    assert stats1.dropped_nodes == 1
    # Hub removed → every spoke is isolated → no surviving component.
    assert all(("pr", "hub") not in frozenset(nodes) for nodes, _e in first)
    assert _max_component_size(first) <= 5
    assert stats1.as_dict() == stats2.as_dict()


def test_size_cap_splits_long_chain_by_removing_hubs_until_it_fits():
    # A 30-node native chain, cap 8: no droppable edges (all conf 1.0), so the
    # chain is split by removing highest-degree nodes until fragments fit.
    nodes = [("issue", f"c{i}") for i in range(30)]
    edges = [
        _edge(nodes[i][0], nodes[i][1], nodes[i + 1][0], nodes[i + 1][1])
        for i in range(len(nodes) - 1)
    ]

    stats = ComponentBuildStats()
    components = build_components(edges, max_component_nodes=8, stats=stats)

    assert _max_component_size(components) <= 8
    assert stats.oversized_components == 1
    assert stats.dropped_nodes >= 1
    # Re-running yields identical fragments.
    again = build_components(edges, max_component_nodes=8)
    assert _component_node_sets(components) == _component_node_sets(again)


# ---------------------------------------------------------------------------
# (c) Hash consistency: materializer vs backfill produce identical work units.
# ---------------------------------------------------------------------------


def test_materialize_and_backfill_produce_identical_work_unit_ids():
    # Mixed graph of several small components at the default cap (no split): the
    # two production entry points must agree on the derived work_unit_ids.
    cluster_a = [
        _edge("issue", "a0", "pr", "a1"),
        _edge("pr", "a1", "commit", "a2"),
    ]
    cluster_b = [
        _edge("issue", "b0", "pr", "b1"),
        _edge("issue", "a0", "issue", "b0", provenance="explicit_text", confidence=0.2),
    ]
    small = [_edge("issue", "z0", "pr", "z1")]
    edges = cluster_a + cluster_b + small

    mat = _build_components(edges)
    back = _build_components_for_backfill(edges)

    mat_ids = {work_unit_id(nodes) for nodes, _edges in mat}
    back_ids = {work_unit_id(nodes) for nodes in back}
    assert mat_ids == back_ids
    # Sanity: both derived the same number of units.
    assert len(mat) == len(back)


def test_materialize_and_backfill_identical_when_component_is_split(monkeypatch):
    # Force a small cap via env so the star is oversized and split; both paths
    # must still agree on the resulting unit ids.
    monkeypatch.setenv("INVESTMENT_MAX_COMPONENT_NODES", "5")
    edges = [_edge("pr", "hub", "issue", f"S-{i}") for i in range(20)] + [
        _edge("issue", "p0", "pr", "p1"),
        _edge("pr", "p1", "commit", "p2"),
    ]

    mat = _build_components(edges)
    back = _build_components_for_backfill(edges)

    assert {work_unit_id(n) for n, _e in mat} == {work_unit_id(n) for n in back}


# ---------------------------------------------------------------------------
# Constant / resolver behavior.
# ---------------------------------------------------------------------------


def test_resolve_max_component_nodes_precedence(monkeypatch):
    assert resolve_max_component_nodes(42) == 42
    monkeypatch.delenv("INVESTMENT_MAX_COMPONENT_NODES", raising=False)
    assert resolve_max_component_nodes() == INVESTMENT_MAX_COMPONENT_NODES
    monkeypatch.setenv("INVESTMENT_MAX_COMPONENT_NODES", "77")
    assert resolve_max_component_nodes() == 77
    monkeypatch.setenv("INVESTMENT_MAX_COMPONENT_NODES", "not-an-int")
    assert resolve_max_component_nodes() == INVESTMENT_MAX_COMPONENT_NODES
    monkeypatch.setenv("INVESTMENT_MAX_COMPONENT_NODES", "0")
    assert resolve_max_component_nodes() == INVESTMENT_MAX_COMPONENT_NODES


def test_fetch_dedups_rmt_versions_and_orders_deterministically(monkeypatch):
    """CHAOS-2775 codex round 2 (MEDIUM + HIGH): the fetch must

    1. argMax-collapse ReplacingMergeTree row versions per edge identity and
       judge the heuristic filter on the LATEST provenance (a stale pre-merge
       row must not resurrect an excluded edge), and
    2. ORDER BY the full identity key so component discovery order — and with
       it the positional component_indexes used by partitioned dispatch — is
       identical across dispatcher and chunk workers.

    The HAVING clause must reference the SELECT alias, NOT repeat
    argMax(provenance, ...): the alias shadows the raw column and
    re-aggregating raises ILLEGAL_AGGREGATION (184) on ClickHouse 26.x — the
    same trap documented on LATEST_WORK_UNIT_INVESTMENTS_CTE.
    """
    captured: dict[str, Any] = {}

    def fake_query_dicts(_sink, query, params):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(q, "query_dicts", fake_query_dicts)
    sink = cast(BaseMetricsSink, object())

    q.fetch_work_graph_edges(sink, org_id="org-1")

    query = captured["query"]
    identity = "org_id, source_type, source_id, edge_type, target_type, target_id"
    assert f"GROUP BY {identity}" in query
    assert f"ORDER BY {identity}" in query
    assert "argMax(provenance, last_synced) AS provenance" in query
    assert "argMax(confidence, last_synced) AS confidence" in query
    # Alias (not aggregate) in HAVING — ILLEGAL_AGGREGATION regression guard.
    assert "HAVING provenance != %(heuristic_provenance)s" in query
    assert "HAVING argMax" not in query
