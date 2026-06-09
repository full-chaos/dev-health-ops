from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from dev_health_ops.work_graph.ai_workflow import (
    load_ai_workflow_graph_for_issue,
    load_ai_workflow_graph_for_pr,
)
from dev_health_ops.work_graph.extractors.ai_workflow import (
    extract_ai_workflow_from_pull_requests,
    extract_review_deployment_incident_edges,
)
from dev_health_ops.work_graph.models import (
    EdgeType,
    NodeType,
    Provenance,
    WorkGraphEdge,
)

ORG = uuid4()
REPO = uuid4()
NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)


def test_extracts_ai_workflow_from_pr_without_raw_prompt_fields() -> None:
    result = extract_ai_workflow_from_pull_requests(
        [
            {
                "repo_id": REPO,
                "number": 42,
                "labels": ["agent-created"],
                "author_name": "claude[bot]",
                "author_user_type": "Bot",
                "head_branch": "claude/chaos-1583",
                "created_at": NOW,
                "merged_at": NOW,
            }
        ],
        org_id=ORG,
        provider="github",
        issue_ids_by_pr={f"{REPO}:42": ["CHAOS-1583"]},
    )

    assert len(result.runs) == 1
    assert len(result.issue_edges) == 1
    assert len(result.artifact_edges) == 1
    run = result.runs[0]
    assert run.prompts_redacted is True
    serialized_run = str(run)
    assert "transcript" not in serialized_run
    assert "keystroke" not in serialized_run
    assert result.issue_edges[0].issue_id == "CHAOS-1583"
    assert result.artifact_edges[0].artifact_id == f"{REPO}:42"


def test_extracts_pr_review_deployment_incident_edges_with_partial_links() -> None:
    result = extract_review_deployment_incident_edges(
        org_id=ORG,
        provider="github",
        reviews=[
            {
                "repo_id": REPO,
                "number": 42,
                "review_id": "review-1",
                "state": "APPROVED",
                "submitted_at": NOW,
            }
        ],
        deployments=[
            {
                "repo_id": REPO,
                "deployment_id": "deploy-1",
                "pull_request_number": 42,
                "deployed_at": NOW,
            }
        ],
        incidents=[{"repo_id": REPO, "incident_id": "inc-1", "started_at": NOW}],
    )

    assert result.review_outcome_edges[0].review_outcome_id == "review-1"
    assert result.pr_deployment_edges[0].deployment_id == "deploy-1"
    assert result.deployment_incident_edges[0].incident_id == "inc-1"
    assert result.deployment_incident_edges[0].confidence == 0.3


def test_traversal_from_issue_root_loads_partial_ai_metadata() -> None:
    edge_rows = [
        {
            "edge_id": "e1",
            "source_type": "issue",
            "source_id": "CHAOS-1583",
            "target_type": "ai_workflow_run",
            "target_id": "run-1",
            "edge_type": "has_ai_workflow",
            "confidence": 0.95,
            "source": "pr_label",
            "evidence": "label",
            "provider": "github",
            "repo_id": str(REPO),
        },
        {
            "edge_id": "e2",
            "source_type": "ai_workflow_run",
            "source_id": "run-1",
            "target_type": "pr",
            "target_id": f"{REPO}:42",
            "edge_type": "generates",
            "confidence": 0.95,
            "source": "pr_label",
            "evidence": "label",
            "provider": "github",
            "repo_id": str(REPO),
        },
    ]
    run_rows = [
        {
            "run_id": "run-1",
            "provider": "github",
            "run_kind": "agent_autonomous",
            "status": None,
            "tool": None,
            "model": None,
            "actor": None,
            "repo_id": str(REPO),
            "prompts_redacted": True,
            "metadata": "not-json",
        }
    ]

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "FROM ai_workflow_runs" in query:
            return run_rows
        return edge_rows

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new=AsyncMock(side_effect=fake_query),
    ):
        result = asyncio.run(
            load_ai_workflow_graph_for_issue(object(), str(ORG), "CHAOS-1583", depth=2)
        )

    assert {edge.edge_id for edge in result.edges} == {"e1", "e2"}
    run_node = next(node for node in result.nodes if node.node_id == "run-1")
    assert run_node.metadata["prompts_redacted"] is True
    assert run_node.metadata["status"] == "unknown"


def test_traversal_from_pr_root_reaches_review_deployment_incident() -> None:
    pr_id = f"{REPO}:42"
    edge_rows = [
        {
            "edge_id": "review-edge",
            "source_type": "pr",
            "source_id": pr_id,
            "target_type": "review_outcome",
            "target_id": "review-1",
            "edge_type": "has_review_outcome",
            "confidence": 1.0,
            "source": "native",
            "evidence": "review",
            "provider": "github",
            "repo_id": str(REPO),
        },
        {
            "edge_id": "deploy-edge",
            "source_type": "pr",
            "source_id": pr_id,
            "target_type": "deployment",
            "target_id": "deploy-1",
            "edge_type": "deploys",
            "confidence": 1.0,
            "source": "native",
            "evidence": "deployment",
            "provider": "github",
            "repo_id": str(REPO),
        },
        {
            "edge_id": "incident-edge",
            "source_type": "deployment",
            "source_id": "deploy-1",
            "target_type": "incident",
            "target_id": "inc-1",
            "edge_type": "linked_incident",
            "confidence": 0.3,
            "source": "heuristic",
            "evidence": "incident",
            "provider": "github",
            "repo_id": str(REPO),
        },
    ]

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "FROM ai_workflow_runs" in query:
            return []
        return edge_rows

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new=AsyncMock(side_effect=fake_query),
    ):
        result = asyncio.run(
            load_ai_workflow_graph_for_pr(object(), str(ORG), pr_id, depth=2)
        )

    assert {edge.edge_type for edge in result.edges} == {
        "has_review_outcome",
        "deploys",
        "linked_incident",
    }
    assert any(
        node.node_type == "incident" and node.node_id == "inc-1"
        for node in result.nodes
    )


def test_ai_workflow_queries_use_clickhouse_param_syntax() -> None:
    """Regression: queries must use {name:Type} syntax for clickhouse-connect, not %(name)s.

    clickhouse-connect's query(..., parameters=...) requires the {name:Type} placeholder
    format.  The old %(name)s style is never substituted — it reaches the DB as a literal
    string, causing empty result sets or syntax errors (CHAOS-2205).
    """
    from dev_health_ops.work_graph.ai_workflow import (
        _AI_EDGE_UNION_QUERY,
        _AI_RUN_QUERY,
    )

    for name, query in [
        ("_AI_RUN_QUERY", _AI_RUN_QUERY),
        ("_AI_EDGE_UNION_QUERY", _AI_EDGE_UNION_QUERY),
    ]:
        assert "%(org_id)s" not in query, f"{name} still uses %(org_id)s"
        assert "{org_id:String}" in query, f"{name} missing {{org_id:String}}"

    assert "%(run_ids)s" not in _AI_RUN_QUERY
    assert "{run_ids:Array(String)}" in _AI_RUN_QUERY

    assert "%(node_types)s" not in _AI_EDGE_UNION_QUERY
    assert "{node_types:Array(String)}" in _AI_EDGE_UNION_QUERY

    assert "%(node_ids)s" not in _AI_EDGE_UNION_QUERY
    assert "{node_ids:Array(String)}" in _AI_EDGE_UNION_QUERY

    assert "%(limit)s" not in _AI_EDGE_UNION_QUERY
    assert "{limit:UInt32}" in _AI_EDGE_UNION_QUERY


def test_non_ai_work_graph_edge_record_shape_regression() -> None:
    edge = WorkGraphEdge(
        edge_id="edge-1",
        source_type=NodeType.ISSUE,
        source_id="CHAOS-1",
        target_type=NodeType.PR,
        target_id="repo:1",
        edge_type=EdgeType.IMPLEMENTS,
        provenance=Provenance.NATIVE,
        confidence=1.0,
        evidence="Closes CHAOS-1",
        discovered_at=NOW,
        last_synced=NOW,
        event_ts=NOW,
        day=NOW.date(),
    )

    assert edge.source_type.value == "issue"
    assert edge.target_type.value == "pr"
    assert edge.edge_type.value == "implements"
    assert edge.evidence == "Closes CHAOS-1"
