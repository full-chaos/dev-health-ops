"""Tests for CHAOS-2430 server-side theme/subcategory filtering of work graph edges.

The filter MUST constrain the candidate edge set BEFORE the LIMIT is enforced:
a sparse theme's edges may all sit beyond row 1000, so filtering after the cap
(e.g. client-side) produces false-empty graphs. These tests prove:

- An edge whose theme matches the filter is RETURNED even when an unfiltered
  fetch would push it beyond the page limit (the cap-safety guarantee).
- A theme filter returns only matching edges.
- A subcategory filter narrows further.
- The no-filter path is unchanged (issue>pr precedence annotation preserved).
- Org isolation: both the node-match query and the edge query carry org_id.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeFilterInput
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
    target_id: str = "repo-pr-42",
    **kwargs: Any,
) -> dict[str, Any]:
    base = {
        "edge_id": edge_id,
        "source_type": source_type,
        "source_id": source_id,
        "target_type": target_type,
        "target_id": target_id,
        "edge_type": "implements",
        "provenance": "native",
        "confidence": 1.0,
        "evidence": "",
        "repo_id": None,
        "provider": "github",
    }
    base.update(kwargs)
    return base


def node_match_row(node_type: str, node_id: str) -> dict[str, Any]:
    return {"node_type": node_type, "node_id": node_id}


class TestThemeFilterServerSide:
    @pytest.mark.asyncio
    async def test_filter_constrains_before_limit(self, mock_context):
        """Cap-safety: the theme filter is applied as a WHERE constraint in the
        SAME query that carries the LIMIT, so it reduces the candidate set
        BEFORE the cap. This is the core guarantee — proven by asserting the
        tuple-IN node constraint and the LIMIT both appear in the edge query.
        """
        # The matching node sits on an edge that, unfiltered, would be at row
        # 5000 (well beyond the 1000 cap). With the filter it is returned.
        matched_node_rows = [node_match_row("issue", "SPARSE-1")]
        edge_rows = [
            make_edge_row(
                edge_id="sparse-edge",
                source_type="issue",
                source_id="SPARSE-1",
                target_type="pr",
                target_id="repo-pr-99",
            )
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Call 1: node-match query; Call 2: edge query (no display-name lookup
            # for human-readable ids; no separate membership query on filter path).
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery", limit=1000)
            result = await resolve_work_graph_edges(mock_context, filters)

        # The sparse edge is returned despite (hypothetically) sitting past the cap.
        assert len(result.edges) == 1
        assert result.edges[0].edge_id == "sparse-edge"

        # The edge query (2nd call) must contain BOTH the node constraint and the
        # LIMIT — proving the filter is enforced server-side, before the cap.
        edge_call = mock_query.call_args_list[1]
        edge_sql = edge_call[0][1]
        edge_params = edge_call[0][2]
        assert "(source_type, source_id) IN %(matched_nodes)s" in edge_sql
        assert "(target_type, target_id) IN %(matched_nodes)s" in edge_sql
        assert "LIMIT %(limit)s" in edge_sql
        # The IN predicate appears before LIMIT in the SQL text.
        assert edge_sql.index("matched_nodes") < edge_sql.index("LIMIT")
        # The matched node set is bound as a param (list of (type, id) tuples).
        assert ("issue", "SPARSE-1") in edge_params["matched_nodes"]

    @pytest.mark.asyncio
    async def test_theme_filter_returns_only_matching_edges(self, mock_context):
        """Edges returned all touch the matched node set (the DB enforces it)."""
        matched_node_rows = [
            node_match_row("issue", "FD-1"),
            node_match_row("issue", "FD-2"),
        ]
        # The DB would only return edges touching FD-1/FD-2; mock that contract.
        edge_rows = [
            make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1"),
            make_edge_row(edge_id="e2", source_type="issue", source_id="FD-2"),
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert {e.edge_id for e in result.edges} == {"e1", "e2"}
        # Filtered edges are annotated with the requested theme.
        assert all(e.theme == "feature_delivery" for e in result.edges)

    @pytest.mark.asyncio
    async def test_empty_match_set_returns_empty_without_edge_query(self, mock_context):
        """When no node matches the theme, return empty WITHOUT issuing the edge
        query (no edge can touch an empty set)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[]]  # node-match query returns nothing
            filters = WorkGraphEdgeFilterInput(theme="risk")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.total_count == 0
        # Only the node-match query ran; the edge query was skipped.
        assert mock_query.call_count == 1

    @pytest.mark.asyncio
    async def test_subcategory_narrows_match(self, mock_context):
        """A subcategory filter adds a HAVING predicate on the argMax subcategory."""
        matched_node_rows = [node_match_row("issue", "FD-1")]
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery", subcategory="feature_delivery.roadmap"
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        # Node-match query carries both theme and subcategory predicates.
        node_call = mock_query.call_args_list[0]
        node_sql = node_call[0][1]
        node_params = node_call[0][2]
        assert "argMax(dominant_theme, computed_at) = %(theme)s" in node_sql
        assert "argMax(dominant_subcategory, computed_at) = %(subcategory)s" in node_sql
        assert node_params["theme"] == "feature_delivery"
        assert node_params["subcategory"] == "feature_delivery.roadmap"
        # Returned edges are annotated with the matched subcategory.
        assert result.edges[0].subcategory == "feature_delivery.roadmap"
        assert result.edges[0].theme == "feature_delivery"

    @pytest.mark.asyncio
    async def test_subcategory_only_derives_theme(self, mock_context):
        """A subcategory-only filter derives the theme prefix ('theme.sub')."""
        matched_node_rows = [node_match_row("issue", "FD-1")]
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(subcategory="quality.testing")
            result = await resolve_work_graph_edges(mock_context, filters)

        node_params = mock_query.call_args_list[0][0][2]
        # Theme prefix derived from the subcategory.
        assert node_params["theme"] == "quality"
        assert node_params["subcategory"] == "quality.testing"
        assert result.edges[0].theme == "quality"
        assert result.edges[0].subcategory == "quality.testing"

    @pytest.mark.asyncio
    async def test_org_isolation_on_both_queries(self, mock_context):
        """Both the node-match query and the edge query carry org_id."""
        matched_node_rows = [node_match_row("issue", "FD-1")]
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            await resolve_work_graph_edges(mock_context, filters)

        node_params = mock_query.call_args_list[0][0][2]
        edge_params = mock_query.call_args_list[1][0][2]
        assert node_params["org_id"] == "test-org"
        assert edge_params["org_id"] == "test-org"
        # Both SQL bodies scope by org_id.
        assert "org_id = %(org_id)s" in mock_query.call_args_list[0][0][1]
        assert "org_id = %(org_id)s" in mock_query.call_args_list[1][0][1]

    @pytest.mark.asyncio
    async def test_no_filter_path_unchanged(self, mock_context):
        """Without a theme filter, the unfiltered path runs: edge query +
        membership batch, with issue>pr precedence annotation (no node-match
        pre-query, no tuple-IN constraint)."""
        edge_rows = [
            make_edge_row(
                edge_id="e1",
                source_type="issue",
                source_id="PROJ-1",
                target_type="pr",
                target_id="PR-9",
            )
        ]
        membership_rows = [
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "dominant_theme": "operational",
                "dominant_subcategory": "operational.reliability",
            },
            {
                "node_type": "pr",
                "node_id": "PR-9",
                "dominant_theme": "maintenance",
                "dominant_subcategory": "maintenance.dependency_updates",
            },
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Unfiltered: edge query + membership batch (human-readable ids, so
            # no display-name lookup).
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        # No node-match pre-query: first call is the edge SELECT with no tuple-IN.
        edge_sql = mock_query.call_args_list[0][0][1]
        assert "matched_nodes" not in edge_sql
        # Issue endpoint wins (precedence preserved).
        assert result.edges[0].theme == "operational"
        assert result.edges[0].subcategory == "operational.reliability"

    @pytest.mark.asyncio
    async def test_filter_combines_with_repo_and_edge_type(self, mock_context):
        """The theme constraint is ANDed with existing repo/edge_type filters."""
        from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeTypeInput

        matched_node_rows = [node_match_row("issue", "FD-1")]
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [matched_node_rows, edge_rows]
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery",
                repo_ids=["repo-a"],
                edge_type=WorkGraphEdgeTypeInput.IMPLEMENTS,
            )
            await resolve_work_graph_edges(mock_context, filters)

        edge_sql = mock_query.call_args_list[1][0][1]
        edge_params = mock_query.call_args_list[1][0][2]
        assert "repo_id IN %(repo_ids)s" in edge_sql
        assert "edge_type = %(edge_type)s" in edge_sql
        assert "matched_nodes" in edge_sql
        assert edge_params["repo_ids"] == ["repo-a"]
        assert edge_params["edge_type"] == "implements"
