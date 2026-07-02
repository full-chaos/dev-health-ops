"""Shared connected-component builder for investment work units (CHAOS-2775).

Both the LLM materializer (``materialize._build_components``) and the no-LLM
membership backfill (``backfill._build_components_for_backfill``) MUST derive
IDENTICAL connected components from the SAME ``work_graph_edges``. The
``work_unit_id`` is a hash of a component's node set (``utils.work_unit_id``) and
the backfill projects membership onto those hashes, so any divergence in how the
two paths group nodes silently breaks membership scoping. This module is the
single source of truth for that grouping — including the oversized-component
split — so the two callers cannot drift.

Two safety nets guard against the "one giant work unit" pathology (CHAOS-2775):

1. Heuristic edges are excluded upstream at the ``fetch_work_graph_edges`` choke
   point (see ``queries.py``); they never reach this builder for unit grouping.
2. Any connected component larger than ``max_component_nodes`` is deterministically
   split here rather than materialized as a single unit:
     a. Drop the component's lowest-confidence edges (ordered by
        ``(confidence, edge_id)`` for determinism) until every resulting fragment
        fits. Edges tied at the component's MAX confidence are never dropped in
        this phase.
     b. If a fragment still exceeds the cap using only max-confidence edges,
        remove the highest-degree hub node(s) (tie-broken by node id) until it
        fits. A removed hub is dropped from every output fragment.
   Nothing is silently truncated: counts of oversized components, dropped edges,
   and dropped nodes are accumulated into a :class:`ComponentBuildStats`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from dev_health_ops.work_graph.investment.constants import (
    resolve_max_component_nodes,
)

logger = logging.getLogger(__name__)

NodeKey = tuple[str, str]
Edge = dict[str, object]
Component = tuple[list[NodeKey], list[Edge]]


@dataclass
class ComponentBuildStats:
    """Accounting for the oversized-component split (CHAOS-2775).

    Exposed in the run stats of both the materializer and the membership
    backfill so an oversized-graph split is observable, never silent.
    """

    oversized_components: int = 0
    dropped_edges: int = 0
    dropped_nodes: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "oversized_components": self.oversized_components,
            "dropped_edges": self.dropped_edges,
            "dropped_nodes": self.dropped_nodes,
        }


def _edge_endpoints(edge: Edge) -> tuple[NodeKey, NodeKey]:
    source = (str(edge.get("source_type")), str(edge.get("source_id")))
    target = (str(edge.get("target_type")), str(edge.get("target_id")))
    return source, target


def _edge_confidence(edge: Edge) -> float:
    value = edge.get("confidence")
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _edge_id(edge: Edge) -> str:
    return str(edge.get("edge_id") or "")


def _connected_components(
    nodes: Iterable[NodeKey],
    edges: Iterable[Edge],
) -> list[list[NodeKey]]:
    """Connected components over ``nodes`` using only ``edges`` whose BOTH
    endpoints are in ``nodes``.

    Grouping is content-based (union-find), so the node SET of each component is
    independent of input iteration order — the property the ``work_unit_id`` hash
    relies on. Node order WITHIN a component is irrelevant because
    ``work_unit_id`` sorts its tokens.
    """
    node_set = set(nodes)
    parent: dict[NodeKey, NodeKey] = {node: node for node in node_set}

    def find(node: NodeKey) -> NodeKey:
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    for edge in edges:
        source, target = _edge_endpoints(edge)
        if source in node_set and target in node_set:
            root_a, root_b = find(source), find(target)
            if root_a != root_b:
                parent[root_b] = root_a

    groups: dict[NodeKey, list[NodeKey]] = {}
    for node in node_set:
        groups.setdefault(find(node), []).append(node)
    return list(groups.values())


def _discover_components(edges: list[Edge]) -> list[Component]:
    """Raw connected components with their deduped edge bundles.

    This is the historical ``materialize._build_components`` traversal, preserved
    verbatim so that for graphs WITHOUT any oversized component the output
    (including component order and per-component edge lists) is byte-identical to
    the pre-CHAOS-2775 behavior.
    """
    adjacency: dict[NodeKey, list[NodeKey]] = {}
    edges_by_node: dict[NodeKey, list[Edge]] = {}

    for edge in edges:
        source, target = _edge_endpoints(edge)
        adjacency.setdefault(source, []).append(target)
        adjacency.setdefault(target, []).append(source)
        edges_by_node.setdefault(source, []).append(edge)
        edges_by_node.setdefault(target, []).append(edge)

    visited: set[NodeKey] = set()
    components: list[Component] = []

    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        component_nodes: list[NodeKey] = []
        component_edges: dict[str, Edge] = {}
        while stack:
            current = stack.pop()
            component_nodes.append(current)
            for edge in edges_by_node.get(current, []):
                edge_id = _edge_id(edge)
                if edge_id and edge_id not in component_edges:
                    component_edges[edge_id] = edge
            for neighbor in adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append((component_nodes, list(component_edges.values())))
    return components


def _degrees(edges: Iterable[Edge], within: set[NodeKey]) -> dict[NodeKey, int]:
    degree: dict[NodeKey, int] = {node: 0 for node in within}
    for edge in edges:
        source, target = _edge_endpoints(edge)
        if source in within and target in within:
            degree[source] += 1
            degree[target] += 1
    return degree


def _remove_hubs(
    nodes: set[NodeKey],
    edges: list[Edge],
    max_nodes: int,
    stats: ComponentBuildStats | None,
) -> list[list[NodeKey]]:
    """Drop highest-degree hub nodes until every fragment fits ``max_nodes``.

    Deterministic: among the nodes of every still-oversized fragment, the node
    with the highest degree is removed (ties broken by the smallest node id). A
    removed node is dropped entirely — it is excluded from all output fragments
    and its incident edges disappear with it. Terminates because every iteration
    shrinks the active node set.
    """
    active = set(nodes)
    while True:
        active_edges = [
            edge
            for edge in edges
            if _edge_endpoints(edge)[0] in active and _edge_endpoints(edge)[1] in active
        ]
        fragments = _connected_components(active, active_edges)
        oversized_nodes: set[NodeKey] = set()
        for fragment in fragments:
            if len(fragment) > max_nodes:
                oversized_nodes.update(fragment)
        if not oversized_nodes:
            return fragments
        degree = _degrees(active_edges, oversized_nodes)
        max_degree = max(degree.get(node, 0) for node in oversized_nodes)
        hub = min(node for node in oversized_nodes if degree.get(node, 0) == max_degree)
        active.discard(hub)
        if stats is not None:
            stats.dropped_nodes += 1
        logger.warning(
            "Investment component split: dropped hub node %s (degree %d) to "
            "enforce max_component_nodes=%d",
            hub,
            max_degree,
            max_nodes,
        )


def _split_oversized_component(
    nodes: list[NodeKey],
    edges: list[Edge],
    max_nodes: int,
    stats: ComponentBuildStats | None,
) -> list[Component]:
    """Split one oversized component into fragments that each fit ``max_nodes``.

    Edge-drop phase (deterministic): edges below the component's max confidence
    are dropped in ``(confidence, edge_id)`` order; we binary-search the smallest
    prefix of that ordering whose removal makes every fragment fit (fragment size
    is monotonically non-increasing as more edges are dropped). Whatever the edge
    phase cannot resolve — a fragment held together purely by max-confidence
    edges — is handed to :func:`_remove_hubs`.
    """
    node_set = set(nodes)
    if edges:
        max_confidence = max(_edge_confidence(edge) for edge in edges)
    else:
        max_confidence = 0.0

    protected_edges = [
        edge for edge in edges if _edge_confidence(edge) >= max_confidence
    ]
    droppable_edges = sorted(
        (edge for edge in edges if _edge_confidence(edge) < max_confidence),
        key=lambda edge: (_edge_confidence(edge), _edge_id(edge)),
    )

    def fits(drop_count: int) -> bool:
        kept = protected_edges + droppable_edges[drop_count:]
        return all(
            len(fragment) <= max_nodes
            for fragment in _connected_components(node_set, kept)
        )

    lo, hi = 0, len(droppable_edges)
    while lo < hi:
        mid = (lo + hi) // 2
        if fits(mid):
            hi = mid
        else:
            lo = mid + 1
    drop_count = lo

    if stats is not None:
        stats.dropped_edges += drop_count
    if drop_count:
        logger.warning(
            "Investment component split: dropped %d lowest-confidence edge(s) "
            "to enforce max_component_nodes=%d",
            drop_count,
            max_nodes,
        )

    kept_edges = protected_edges + droppable_edges[drop_count:]
    fragments = _connected_components(node_set, kept_edges)

    fragment_node_lists: list[list[NodeKey]] = []
    for fragment in fragments:
        if len(fragment) <= max_nodes:
            fragment_node_lists.append(fragment)
        else:
            fragment_node_lists.extend(
                _remove_hubs(set(fragment), kept_edges, max_nodes, stats)
            )

    result: list[Component] = []
    for fragment_nodes in fragment_node_lists:
        fragment_set = set(fragment_nodes)
        fragment_edges = [
            edge
            for edge in kept_edges
            if _edge_endpoints(edge)[0] in fragment_set
            and _edge_endpoints(edge)[1] in fragment_set
        ]
        result.append((fragment_nodes, fragment_edges))

    # Deterministic fragment order, independent of set-iteration order: sort by
    # the fragment's sorted node tuple.
    result.sort(key=lambda component: sorted(component[0]))
    return result


def build_components(
    edges: list[Edge],
    *,
    max_component_nodes: int | None = None,
    stats: ComponentBuildStats | None = None,
) -> list[Component]:
    """Build investment work-unit components from ``work_graph_edges`` rows.

    Returns ``(node_list, edge_list)`` per component. Components exceeding
    ``max_component_nodes`` (defaulting to
    :func:`constants.resolve_max_component_nodes`, env-overridable via
    ``INVESTMENT_MAX_COMPONENT_NODES``) are deterministically split; the split's
    drop counts are accumulated into ``stats`` when provided.

    This is the SINGLE implementation shared by the materializer and the
    membership backfill so their component sets — and therefore ``work_unit_id``
    hashes — stay identical for identical edge input.
    """
    cap = resolve_max_component_nodes(max_component_nodes)

    result: list[Component] = []
    for component_nodes, component_edges in _discover_components(edges):
        unit_nodes = list(dict.fromkeys(component_nodes))
        if len(unit_nodes) <= cap:
            result.append((unit_nodes, component_edges))
            continue
        if stats is not None:
            stats.oversized_components += 1
        logger.warning(
            "Investment component with %d nodes exceeds max_component_nodes=%d; "
            "splitting deterministically (CHAOS-2775)",
            len(unit_nodes),
            cap,
        )
        result.extend(
            _split_oversized_component(unit_nodes, component_edges, cap, stats)
        )
    return result
