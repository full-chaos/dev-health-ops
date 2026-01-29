from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    WorkGraphEdgeFilterInput,
    WorkGraphEdgeTypeInput,
    WorkGraphNodeTypeInput,
)
from dev_health_ops.api.graphql.models.outputs import (
    WorkGraphEdgeType,
    WorkGraphNodeType,
    WorkGraphProvenance,
)
from dev_health_ops.api.graphql.resolvers.work_graph import resolve_work_graph_edges


class MockClient:
    pass


@pytest.fixture
def mock_context():
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(),
    )


def make_edge_row(
    edge_id: str = "edge-1",
    source_type: str = "issue",
    source_id: str = "PROJ-123",
    target_type: str = "pr",
    target_id: str = "repo:42",
    edge_type: str = "implements",
    provenance: str = "native",
    confidence: float = 1.0,
    evidence: str = "Closes #123",
    repo_id: str = "abc-def",
    provider: str = "github",
) -> Dict[str, Any]:
    return {
        "edge_id": edge_id,
        "source_type": source_type,
        "source_id": source_id,
        "target_type": target_type,
        "target_id": target_id,
        "edge_type": edge_type,
        "provenance": provenance,
        "confidence": confidence,
        "evidence": evidence,
        "repo_id": repo_id,
        "provider": provider,
    }


class TestResolveWorkGraphEdges:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_edges(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            result = await resolve_work_graph_edges(mock_context)

            assert result.edges == []
            assert result.total_count == 0
            assert result.page_info.has_next_page is False

    @pytest.mark.asyncio
    async def test_returns_edges_with_correct_types(self, mock_context):
        rows = [
            make_edge_row(
                edge_id="e1",
                source_type="issue",
                target_type="pr",
                edge_type="implements",
                provenance="native",
            ),
            make_edge_row(
                edge_id="e2",
                source_type="pr",
                target_type="commit",
                edge_type="contains",
                provenance="explicit_text",
            ),
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows

            result = await resolve_work_graph_edges(mock_context)

            assert len(result.edges) == 2
            assert result.total_count == 2

            e1 = result.edges[0]
            assert e1.edge_id == "e1"
            assert e1.source_type == WorkGraphNodeType.ISSUE
            assert e1.target_type == WorkGraphNodeType.PR
            assert e1.edge_type == WorkGraphEdgeType.IMPLEMENTS
            assert e1.provenance == WorkGraphProvenance.NATIVE

            e2 = result.edges[1]
            assert e2.edge_id == "e2"
            assert e2.source_type == WorkGraphNodeType.PR
            assert e2.target_type == WorkGraphNodeType.COMMIT
            assert e2.edge_type == WorkGraphEdgeType.CONTAINS
            assert e2.provenance == WorkGraphProvenance.EXPLICIT_TEXT

    @pytest.mark.asyncio
    async def test_applies_repo_ids_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(repo_ids=["repo-1", "repo-2"])
            await resolve_work_graph_edges(mock_context, filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "repo_id IN %(repo_ids)s" in sql
            assert params["repo_ids"] == ["repo-1", "repo-2"]

    @pytest.mark.asyncio
    async def test_applies_source_type_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(source_type=WorkGraphNodeTypeInput.ISSUE)
            await resolve_work_graph_edges(mock_context, filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "source_type = %(source_type)s" in sql
            assert params["source_type"] == "issue"

    @pytest.mark.asyncio
    async def test_applies_edge_type_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(
                edge_type=WorkGraphEdgeTypeInput.IMPLEMENTS
            )
            await resolve_work_graph_edges(mock_context, filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "edge_type = %(edge_type)s" in sql
            assert params["edge_type"] == "implements"

    @pytest.mark.asyncio
    async def test_applies_node_id_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(node_id="PROJ-123")
            await resolve_work_graph_edges(mock_context, filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "(source_id = %(node_id)s OR target_id = %(node_id)s)" in sql
            assert params["node_id"] == "PROJ-123"

    @pytest.mark.asyncio
    async def test_applies_limit(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(limit=500)
            await resolve_work_graph_edges(mock_context, filters)

            call_args = mock_query.call_args
            params = call_args[0][2]

            assert params["limit"] == 500

    @pytest.mark.asyncio
    async def test_page_info_has_next_when_at_limit(self, mock_context):
        rows = [make_edge_row(edge_id=f"e{i}") for i in range(100)]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows

            filters = WorkGraphEdgeFilterInput(limit=100)
            result = await resolve_work_graph_edges(mock_context, filters)

            assert result.page_info.has_next_page is True
            assert result.page_info.start_cursor == "e0"
            assert result.page_info.end_cursor == "e99"

    @pytest.mark.asyncio
    async def test_handles_unknown_enum_values_gracefully(self, mock_context):
        rows = [
            make_edge_row(
                source_type="unknown_type",
                target_type="also_unknown",
                edge_type="mystery_edge",
                provenance="magic",
            )
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows

            result = await resolve_work_graph_edges(mock_context)

            assert len(result.edges) == 1
            edge = result.edges[0]
            assert edge.source_type == WorkGraphNodeType.ISSUE
            assert edge.target_type == WorkGraphNodeType.ISSUE
            assert edge.edge_type == WorkGraphEdgeType.RELATES
            assert edge.provenance == WorkGraphProvenance.HEURISTIC

    @pytest.mark.asyncio
    async def test_raises_when_client_missing(self):
        context = GraphQLContext(
            org_id="test-org",
            db_url="clickhouse://localhost:8123/default",
            client=None,
        )

        with pytest.raises(RuntimeError, match="Database client not available"):
            await resolve_work_graph_edges(context)
