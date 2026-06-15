"""Tests for CHAOS-2430: theme/subcategory edge attribution via work_unit_membership.

Covers:
- Edge whose endpoint maps to a known work unit carries expected dominant_theme
  and dominant_subcategory.
- Edge with no membership → theme and subcategory are None.
- Batch lookup is ONE query (not N per edge).
- Org isolation: membership lookup includes org_id predicate.
- Endpoint precedence: ISSUE beats PR when both are members.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
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


def make_membership_rows(
    node_type: str,
    node_id: str,
    dominant_theme: str = "feature_delivery",
    dominant_subcategory: str = "feature_delivery.roadmap",
) -> list[dict[str, Any]]:
    """Return the is_dominant rows (one per kind) that the annotation query emits.

    The annotation query selects is_dominant=1 rows of the latest run and
    returns (node_type, node_id, category_kind, category) tuples.
    """
    return [
        {
            "node_type": node_type,
            "node_id": node_id,
            "category_kind": "theme",
            "category": dominant_theme,
        },
        {
            "node_type": node_type,
            "node_id": node_id,
            "category_kind": "subcategory",
            "category": dominant_subcategory,
        },
    ]


class TestEdgeThemeAttribution:
    @pytest.mark.asyncio
    async def test_edge_with_issue_membership_carries_theme(self, mock_context):
        """Edge whose source is a known work unit member carries the dominant theme."""
        edge_rows = [make_edge_row(source_type="issue", source_id="PROJ-1")]
        membership_rows = make_membership_rows(
            "issue", "PROJ-1", "feature_delivery", "feature_delivery.roadmap"
        )

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Call 1: edge query; Call 2: membership lookup (no display-name lookups needed
            # since PROJ-1 is human-readable and target_id "repo:42" has no UUID format).
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].theme == "feature_delivery"
        assert result.edges[0].subcategory == "feature_delivery.roadmap"

    @pytest.mark.asyncio
    async def test_edge_with_no_membership_returns_null_theme(self, mock_context):
        """Edge with neither endpoint in work_unit_membership → theme/subcategory are None."""
        edge_rows = [make_edge_row(source_id="PROJ-UNKNOWN", target_id="INC-99")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]  # empty membership result
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].theme is None
        assert result.edges[0].subcategory is None

    @pytest.mark.asyncio
    async def test_batch_lookup_is_one_query(self, mock_context):
        """Membership for all edges is resolved in ONE query, not N."""
        edge_rows = [
            make_edge_row(edge_id="e1", source_id="PROJ-1", target_id="deploy-a"),
            make_edge_row(edge_id="e2", source_id="PROJ-2", target_id="deploy-b"),
            make_edge_row(edge_id="e3", source_id="PROJ-3", target_id="deploy-c"),
        ]
        membership_rows = make_membership_rows(
            "issue", "PROJ-1"
        ) + make_membership_rows("issue", "PROJ-2")

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # 1 edge query + 1 membership query (no display-name lookups for
            # human-readable ids).
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        # Exactly 2 calls: edge query + single membership batch query.
        assert mock_query.call_count == 2

        e1 = next(e for e in result.edges if e.edge_id == "e1")
        e2 = next(e for e in result.edges if e.edge_id == "e2")
        e3 = next(e for e in result.edges if e.edge_id == "e3")
        assert e1.theme == "feature_delivery"
        assert e2.theme == "feature_delivery"
        assert e3.theme is None  # PROJ-3 not in membership

    @pytest.mark.asyncio
    async def test_membership_lookup_includes_org_id(self, mock_context):
        """Membership batch query must include org_id to prevent cross-tenant leaks."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]
            await resolve_work_graph_edges(mock_context)

        # Second call is the membership lookup — verify org_id in params.
        membership_call = mock_query.call_args_list[1]
        membership_params = membership_call[0][2]
        assert "org_id" in membership_params
        assert membership_params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_issue_endpoint_beats_pr_endpoint(self, mock_context):
        """When both source (issue) and target (pr) are in membership, the issue wins."""
        edge_rows = [
            make_edge_row(
                source_type="issue",
                source_id="PROJ-1",
                target_type="pr",
                target_id="PR-99",
            )
        ]
        membership_rows = make_membership_rows(
            "issue", "PROJ-1", "feature_delivery", "feature_delivery.roadmap"
        ) + make_membership_rows("pr", "PR-99", "maintenance", "maintenance.refactor")

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        # Issue endpoint takes precedence.
        assert result.edges[0].theme == "feature_delivery"
        assert result.edges[0].subcategory == "feature_delivery.roadmap"

    @pytest.mark.asyncio
    async def test_pr_endpoint_used_when_no_issue(self, mock_context):
        """When only the PR endpoint is in membership, use it."""
        edge_rows = [
            make_edge_row(
                source_type="pr",
                source_id="PR-77",
                target_type="commit",
                target_id="commit-abc",
            )
        ]
        membership_rows = make_membership_rows(
            "pr", "PR-77", "quality", "quality.testing"
        )

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].theme == "quality"
        assert result.edges[0].subcategory == "quality.testing"

    @pytest.mark.asyncio
    async def test_empty_dominant_theme_treated_as_null(self, mock_context):
        """A membership row with empty dominant_theme string is treated as None."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]
        # is_dominant rows whose category is an empty string (defensive).
        membership_rows = [
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "category_kind": "theme",
                "category": "",
            },
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "category_kind": "subcategory",
                "category": "",
            },
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].theme is None
        assert result.edges[0].subcategory is None

    @pytest.mark.asyncio
    async def test_annotation_missing_membership_table_degrades_gracefully(
        self, mock_context
    ):
        """If the annotation lookup hits a missing work_unit_membership table
        (rolling deploy / pre-migration), edges are still returned with null
        theme/subcategory — the EXPECTED recognized state, swallowed."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]

        call_count = 0

        async def _mock_query(client, sql, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return edge_rows  # edge query (unfiltered) succeeds
            # Annotation lookup: work_unit_membership does not exist yet.
            err = Exception(
                "Code: 60. DB::Exception: Unknown table expression identifier "
                "'work_unit_membership'. (UNKNOWN_TABLE)"
            )
            err.code = 60  # type: ignore[attr-defined]
            raise err

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            side_effect=_mock_query,
        ):
            result = await resolve_work_graph_edges(mock_context)

        assert len(result.edges) == 1
        assert result.edges[0].theme is None
        assert result.edges[0].subcategory is None

    @pytest.mark.asyncio
    async def test_annotation_unexpected_error_propagates(self, mock_context):
        """A non-recognized error during the annotation lookup (e.g. a timeout /
        connection loss) must PROPAGATE, not be silently served as null
        annotation — the inconsistent-twin fix of the filtered-path handling."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]

        call_count = 0

        async def _mock_query(client, sql, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return edge_rows  # edge query succeeds
            raise RuntimeError("ClickHouse connection lost")

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            side_effect=_mock_query,
        ):
            with pytest.raises(RuntimeError, match="ClickHouse connection lost"):
                await resolve_work_graph_edges(mock_context)

    @pytest.mark.asyncio
    async def test_annotation_other_unknown_table_propagates(self, mock_context):
        """A code-60 UNKNOWN_TABLE for a DIFFERENT table during annotation also
        propagates (only work_unit_membership degrades)."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]

        call_count = 0

        async def _mock_query(client, sql, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return edge_rows
            err = Exception(
                "Code: 60. DB::Exception: Unknown table expression identifier "
                "'work_graph_edges'. (UNKNOWN_TABLE)"
            )
            err.code = 60  # type: ignore[attr-defined]
            raise err

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            side_effect=_mock_query,
        ):
            with pytest.raises(Exception, match="work_graph_edges"):
                await resolve_work_graph_edges(mock_context)

    @pytest.mark.asyncio
    async def test_genuine_no_membership_yields_null_annotation(self, mock_context):
        """A node that genuinely has no membership row → empty result set (NOT an
        exception) → null theme/subcategory, edges returned. Unchanged."""
        edge_rows = [make_edge_row(source_id="PROJ-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # edge query, then an EMPTY annotation result (no membership rows).
            mock_query.side_effect = [edge_rows, []]
            result = await resolve_work_graph_edges(mock_context)

        assert len(result.edges) == 1
        assert result.edges[0].theme is None
        assert result.edges[0].subcategory is None
