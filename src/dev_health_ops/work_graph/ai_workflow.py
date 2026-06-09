"""Typed loaders for AI workflow evidence in the Work Graph."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

AIWorkflowRootType = Literal["issue", "pr", "work_unit"]


@dataclass(frozen=True)
class AIWorkflowGraphNode:
    """A node returned by AI workflow traversal."""

    node_type: str
    node_id: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AIWorkflowGraphEdge:
    """A typed edge returned by AI workflow traversal."""

    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    edge_type: str
    confidence: float
    source: str
    evidence: str
    provider: str | None = None
    repo_id: str | None = None


@dataclass(frozen=True)
class AIWorkflowTraversalResult:
    """Partial AI workflow graph rooted at an issue, PR, or WorkUnit."""

    root_type: str
    root_id: str
    nodes: list[AIWorkflowGraphNode]
    edges: list[AIWorkflowGraphEdge]
    partial: bool = False


def _string(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _metadata(value: object | None) -> dict[str, object]:
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _edge_from_row(row: dict[str, Any]) -> AIWorkflowGraphEdge:
    return AIWorkflowGraphEdge(
        edge_id=str(row.get("edge_id") or ""),
        source_type=str(row.get("source_type") or ""),
        source_id=str(row.get("source_id") or ""),
        target_type=str(row.get("target_type") or ""),
        target_id=str(row.get("target_id") or ""),
        edge_type=str(row.get("edge_type") or ""),
        confidence=float(row.get("confidence") or 0.0),
        source=str(row.get("source") or ""),
        evidence=str(row.get("evidence") or ""),
        provider=_string(row.get("provider")),
        repo_id=_string(row.get("repo_id")),
    )


def _node_key(node_type: str, node_id: str) -> tuple[str, str]:
    return (node_type, node_id)


def _add_node(
    nodes: dict[tuple[str, str], AIWorkflowGraphNode],
    node_type: str,
    node_id: str,
    metadata: dict[str, object] | None = None,
) -> None:
    if not node_id:
        return
    key = _node_key(node_type, node_id)
    if key in nodes:
        if metadata:
            merged = {**nodes[key].metadata, **metadata}
            nodes[key] = AIWorkflowGraphNode(node_type, node_id, merged)
        return
    nodes[key] = AIWorkflowGraphNode(node_type, node_id, metadata or {})


async def load_ai_workflow_graph(
    client: object,
    org_id: str,
    root_type: AIWorkflowRootType,
    root_id: str,
    *,
    depth: int = 3,
    limit: int = 100,
) -> AIWorkflowTraversalResult:
    """Load AI workflow evidence reachable from an issue, PR, or WorkUnit root.

    The loader is intentionally lenient: missing ``ai_workflow_runs`` metadata does
    not hide edges, and malformed JSON metadata is ignored rather than raised.
    """

    from dev_health_ops.api.queries.client import query_dicts

    normalized_root_type = "issue" if root_type == "work_unit" else root_type
    nodes: dict[tuple[str, str], AIWorkflowGraphNode] = {}
    edges_by_id: dict[str, AIWorkflowGraphEdge] = {}
    frontier = {_node_key(normalized_root_type, root_id)}
    visited: set[tuple[str, str]] = set()
    partial = False

    _add_node(nodes, normalized_root_type, root_id)

    for _ in range(max(depth, 0)):
        current = [key for key in frontier if key not in visited]
        if not current or len(edges_by_id) >= limit:
            break
        visited.update(current)

        node_types = [node_type for node_type, _ in current]
        node_ids = [node_id for _, node_id in current]

        # Role-typed frontier ids: each UNION branch filters on its own key
        # columns with ONLY the ids that can play that role this hop. Empty
        # arrays collapse a branch's predicate to constant-false, so branches
        # with no relevant frontier nodes are pruned before their FINAL pass.
        def ids_of(*types: str) -> list[str]:
            return [i for t, i in current if t in types]

        rows = await query_dicts(
            client,
            _AI_EDGE_UNION_QUERY,
            {
                "org_id": org_id,
                "node_types": node_types,
                "node_ids": node_ids,
                "issue_ids": ids_of("issue"),
                "run_ids": ids_of("ai_workflow_run"),
                "pr_ids": ids_of("pr"),
                "review_outcome_ids": ids_of("review_outcome"),
                "deployment_ids": ids_of("deployment"),
                "incident_ids": ids_of("incident"),
                # Artifact targets carry their own type taxonomy (pr, diff,
                # ...): superset of every frontier id that is not an
                # issue/run, so unknown artifact types keep matching.
                "artifact_ids": [
                    i for t, i in current if t not in ("issue", "ai_workflow_run")
                ],
                "limit": max(limit - len(edges_by_id), 0),
            },
        )
        if len(rows) >= max(limit - len(edges_by_id), 0):
            partial = True

        next_frontier: set[tuple[str, str]] = set()
        for row in rows:
            edge = _edge_from_row(row)
            if not edge.edge_id or edge.edge_id in edges_by_id:
                continue
            edges_by_id[edge.edge_id] = edge
            _add_node(nodes, edge.source_type, edge.source_id)
            _add_node(nodes, edge.target_type, edge.target_id)
            for key in (
                _node_key(edge.source_type, edge.source_id),
                _node_key(edge.target_type, edge.target_id),
            ):
                if key not in visited:
                    next_frontier.add(key)
        frontier = next_frontier

    run_ids = [
        node_id for node_type, node_id in nodes if node_type == "ai_workflow_run"
    ]
    if run_ids:
        run_rows = await query_dicts(
            client,
            _AI_RUN_QUERY,
            {"org_id": org_id, "run_ids": run_ids},
        )
        for row in run_rows:
            run_id = str(row.get("run_id") or "")
            metadata: dict[str, object] = {
                "provider": str(row.get("provider") or ""),
                "run_kind": str(row.get("run_kind") or "unknown"),
                "status": str(row.get("status") or "unknown"),
                "tool": str(row.get("tool") or ""),
                "model": str(row.get("model") or ""),
                "actor": str(row.get("actor") or ""),
                "repo_id": str(row.get("repo_id") or ""),
                "prompts_redacted": bool(row.get("prompts_redacted", True)),
            }
            metadata.update(_metadata(row.get("metadata")))
            _add_node(nodes, "ai_workflow_run", run_id, metadata)

    return AIWorkflowTraversalResult(
        root_type=root_type,
        root_id=root_id,
        nodes=list(nodes.values()),
        edges=list(edges_by_id.values()),
        partial=partial,
    )


async def load_ai_workflow_graph_for_issue(
    client: object,
    org_id: str,
    issue_id: str,
    *,
    depth: int = 3,
    limit: int = 100,
) -> AIWorkflowTraversalResult:
    return await load_ai_workflow_graph(
        client, org_id, "issue", issue_id, depth=depth, limit=limit
    )


async def load_ai_workflow_graph_for_pr(
    client: object,
    org_id: str,
    pr_id: str,
    *,
    depth: int = 3,
    limit: int = 100,
) -> AIWorkflowTraversalResult:
    return await load_ai_workflow_graph(
        client, org_id, "pr", pr_id, depth=depth, limit=limit
    )


async def load_ai_workflow_graph_for_work_unit(
    client: object,
    org_id: str,
    work_unit_id: str,
    *,
    depth: int = 3,
    limit: int = 100,
) -> AIWorkflowTraversalResult:
    return await load_ai_workflow_graph(
        client, org_id, "work_unit", work_unit_id, depth=depth, limit=limit
    )


_AI_RUN_QUERY = """
    SELECT
        run_id,
        provider,
        run_kind,
        status,
        tool,
        model,
        actor,
        toString(repo_id) AS repo_id,
        prompts_redacted,
        metadata
    FROM ai_workflow_runs FINAL
    WHERE org_id = {org_id:String} AND run_id IN {run_ids:Array(String)}
"""

# Every branch reads its ReplacingMergeTree with FINAL (the repo precedent —
# see the governance loaders): daily-job reruns insert new computed_at
# versions of the same deterministic edge ids, and without FINAL those
# duplicates are query-visible until background merges run. Duplicates would
# also burn the traversal LIMIT and falsely mark results partial (CHAOS-2187).
#
# Each branch ALSO repeats the frontier-id predicate on its own raw key
# columns (issue_id/run_id/pr_id/... are all in that table's ORDER BY): the
# key condition then prunes parts/granules BEFORE the FINAL merge, instead
# of FINAL-merging the org's full history and filtering outside the union.
# Predicates are role-typed (issue_ids/run_ids/pr_ids/...): an OR across two
# key columns defeats range pruning, but a role array that is EMPTY for this
# hop collapses its disjunct to constant-false, so inactive branches prune to
# zero parts and the active disjunct usually prunes via the (org_id, <id>)
# key prefix. The arrays are role supersets (artifact_ids = every frontier id
# that is not an issue/run); the outer WHERE still enforces the exact
# (type, id) match, so traversal semantics are unchanged and no history
# window is introduced.
_AI_EDGE_UNION_QUERY = """
    SELECT * FROM
    (
        SELECT
            edge_id,
            'issue' AS source_type,
            issue_id AS source_id,
            'ai_workflow_run' AS target_type,
            run_id AS target_id,
            'has_ai_workflow' AS edge_type,
            confidence,
            source,
            evidence,
            provider,
            toString(repo_id) AS repo_id
        FROM ai_workflow_issue_edges FINAL
        WHERE org_id = {org_id:String}
          AND (issue_id IN {issue_ids:Array(String)} OR run_id IN {run_ids:Array(String)})

        UNION ALL

        SELECT
            edge_id,
            'ai_workflow_run' AS source_type,
            run_id AS source_id,
            if(artifact_type = 'pull_request', 'pr', artifact_type) AS target_type,
            artifact_id AS target_id,
            'generates' AS edge_type,
            confidence,
            source,
            evidence,
            provider,
            toString(repo_id) AS repo_id
        FROM ai_workflow_artifact_edges FINAL
        WHERE org_id = {org_id:String}
          AND (run_id IN {run_ids:Array(String)} OR artifact_id IN {artifact_ids:Array(String)})

        UNION ALL

        SELECT
            edge_id,
            'pr' AS source_type,
            pr_id AS source_id,
            'review_outcome' AS target_type,
            review_outcome_id AS target_id,
            'has_review_outcome' AS edge_type,
            confidence,
            source,
            evidence,
            provider,
            toString(repo_id) AS repo_id
        FROM work_graph_pr_review_outcome_edges FINAL
        WHERE org_id = {org_id:String}
          AND (pr_id IN {pr_ids:Array(String)} OR review_outcome_id IN {review_outcome_ids:Array(String)})

        UNION ALL

        SELECT
            edge_id,
            'pr' AS source_type,
            pr_id AS source_id,
            'deployment' AS target_type,
            deployment_id AS target_id,
            'deploys' AS edge_type,
            confidence,
            source,
            evidence,
            provider,
            toString(repo_id) AS repo_id
        FROM work_graph_pr_deployment_edges FINAL
        WHERE org_id = {org_id:String}
          AND (pr_id IN {pr_ids:Array(String)} OR deployment_id IN {deployment_ids:Array(String)})

        UNION ALL

        SELECT
            edge_id,
            'deployment' AS source_type,
            deployment_id AS source_id,
            'incident' AS target_type,
            incident_id AS target_id,
            'linked_incident' AS edge_type,
            confidence,
            source,
            evidence,
            provider,
            toString(repo_id) AS repo_id
        FROM work_graph_deployment_incident_edges FINAL
        WHERE org_id = {org_id:String}
          AND (deployment_id IN {deployment_ids:Array(String)} OR incident_id IN {incident_ids:Array(String)})
    )
    WHERE
        (source_type IN {node_types:Array(String)} AND source_id IN {node_ids:Array(String)})
        OR (target_type IN {node_types:Array(String)} AND target_id IN {node_ids:Array(String)})
    LIMIT {limit:UInt32}
"""
