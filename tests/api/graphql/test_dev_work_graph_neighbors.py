from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.work_graph_neighbors_service import (
    GraphDirection,
    WorkGraphNeighborEdge,
    WorkGraphNeighborNode,
    WorkGraphNeighborsResult,
    WorkGraphResultState,
    WorkGraphSourceRef,
)
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser

ORG_A = "00000000-0000-0000-0000-000000000001"
ORG_B = "00000000-0000-0000-0000-000000000002"
NOW = datetime(2026, 7, 28, tzinfo=UTC)

_QUERY = """
query Neighbors($orgId: String!, $input: DevWorkGraphNeighborsInput!) {
  devWorkGraphNeighbors(orgId: $orgId, input: $input) {
    schemaVersion state totalCount returnedCount truncated depth queryVersion
    nodes { nodeType nodeId displayLabel resolutionState repositoryId }
    edges {
      edgeId sourceType sourceId targetType targetId relationshipType direction
      provenance confidence sourceRefId evidenceRefIds
    }
    sourceRefs { refId sourceTable sourceVersion queryVersion watermark }
  }
}
"""


def _context() -> GraphQLContext:
    return GraphQLContext(
        org_id=ORG_A,
        db_url="clickhouse://test",
        client=object(),
        user=AuthenticatedUser(
            user_id="user-a",
            email="member@example.com",
            org_id=ORG_A,
            role="member",
            token_version=1,
        ),
    )


def _variables(org_id: str = ORG_A) -> dict[str, object]:
    return {
        "orgId": org_id,
        "input": {
            "scope": {
                "directScope": "ISSUE",
                "refs": [{"kind": "ISSUE", "value": "issue-a"}],
                "presetDays": 30,
            },
            "rootRefs": [{"nodeType": "issue", "nodeId": "issue-a"}],
            "relationshipTypes": ["blocks"],
            "direction": "OUTGOING",
            "depth": 1,
            "limit": 25,
        },
    }


class FakeGraphService:
    calls = 0

    async def neighbors(self, *, org_id, permission_fingerprint, request):
        type(self).calls += 1
        assert org_id == ORG_A
        assert permission_fingerprint
        assert request.depth == 1
        assert request.relationship_types == ("blocks",)
        return WorkGraphNeighborsResult(
            "work_graph_neighbors.v1",
            WorkGraphResultState.COMPLETE,
            (
                WorkGraphNeighborNode(
                    "issue", "issue-a", "Issue A", "resolved", "repo-a"
                ),
                WorkGraphNeighborNode(
                    "issue", "issue-b", "Issue B", "resolved", "repo-a"
                ),
            ),
            (
                WorkGraphNeighborEdge(
                    "edge-a",
                    "issue",
                    "issue-a",
                    "issue",
                    "issue-b",
                    "blocks",
                    GraphDirection.OUTGOING,
                    "native",
                    1,
                    "graph-source:work_graph_edges",
                    NOW,
                ),
            ),
            (
                WorkGraphSourceRef(
                    "graph-source:work_graph_edges",
                    "work_graph_edges",
                    "work-graph-edges.v1",
                    NOW,
                ),
            ),
            (),
            1,
            1,
            False,
            1,
            "work-graph-neighbors.v1",
            NOW,
        )


@pytest.mark.asyncio
async def test_graphql_work_graph_is_contract_equivalent_to_shared_service() -> None:
    FakeGraphService.calls = 0
    context = _context()
    context.dev_work_graph_service = FakeGraphService()  # type: ignore[attr-defined]
    result = await schema.execute(
        _QUERY, variable_values=_variables(), context_value=context
    )
    assert result.errors is None
    assert result.data is not None
    assert FakeGraphService.calls == 1
    graph = result.data["devWorkGraphNeighbors"]
    assert graph["depth"] == 1
    assert graph["edges"] == [
        {
            "edgeId": "edge-a",
            "sourceType": "issue",
            "sourceId": "issue-a",
            "targetType": "issue",
            "targetId": "issue-b",
            "relationshipType": "blocks",
            "direction": "OUTGOING",
            "provenance": "native",
            "confidence": 1.0,
            "sourceRefId": "graph-source:work_graph_edges",
            "evidenceRefIds": [],
        }
    ]


@pytest.mark.asyncio
async def test_graphql_work_graph_rejects_cross_tenant_before_service() -> None:
    FakeGraphService.calls = 0
    context = _context()
    context.dev_work_graph_service = FakeGraphService()  # type: ignore[attr-defined]
    result = await schema.execute(
        _QUERY, variable_values=_variables(ORG_B), context_value=context
    )
    assert result.errors is not None
    assert result.data is None
    assert FakeGraphService.calls == 0


@pytest.mark.asyncio
async def test_graphql_work_graph_rejects_depth_over_one_before_service() -> None:
    FakeGraphService.calls = 0
    variables = _variables()
    variables["input"]["depth"] = 2  # type: ignore[index]
    context = _context()
    context.dev_work_graph_service = FakeGraphService()  # type: ignore[attr-defined]
    result = await schema.execute(
        _QUERY, variable_values=variables, context_value=context
    )
    assert result.errors is not None
    assert FakeGraphService.calls == 0
