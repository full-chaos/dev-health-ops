from __future__ import annotations

from typing import Any
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
from dev_health_ops.api.graphql.resolvers.work_graph import (
    resolve_work_graph_artifacts,
    resolve_work_graph_edges,
    resolve_work_graph_flow,
)


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
) -> dict[str, Any]:
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
        # The web global filter bar sends repo *slugs* ("org/repo"), not UUIDs.
        # The resolver MUST resolve them to catalog UUIDs (via resolve_repo_ids)
        # BEFORE they reach SQL: a raw slug against the UUID repo_id column
        # throws CANNOT_PARSE_UUID and empties the graph. After resolution the
        # SQL filters the UUID column directly.
        resolved_uuid = "920f9442-07df-4217-4dc4-c5833c0b8268"
        with (
            patch(
                "dev_health_ops.api.graphql.resolvers.work_graph.resolve_repo_ids",
                new_callable=AsyncMock,
                return_value=[resolved_uuid],
            ) as mock_resolve,
            patch(
                "dev_health_ops.api.queries.client.query_dicts",
                new_callable=AsyncMock,
            ) as mock_query,
        ):
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(repo_ids=["full-chaos/dev-health-ops"])
            await resolve_work_graph_edges(mock_context, filters)

            # Resolution happened, org-scoped, on the raw slug ref.
            mock_resolve.assert_awaited_once()
            assert mock_resolve.await_args is not None
            assert mock_resolve.await_args.args[1] == ["full-chaos/dev-health-ops"]
            assert mock_resolve.await_args.kwargs.get("org_id") == "test-org"

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            # Raw UUID-column filter (inputs are already resolved UUIDs).
            assert "repo_id IN %(repo_ids)s" in sql
            # The RESOLVED UUID reaches SQL, never the slug.
            assert params["repo_ids"] == [resolved_uuid]

    @pytest.mark.asyncio
    async def test_unresolvable_repo_ids_returns_empty(self, mock_context):
        # A repo filter whose refs resolve to NO known repo must yield an empty
        # result, NOT fall through to an unscoped whole-org query.
        with (
            patch(
                "dev_health_ops.api.graphql.resolvers.work_graph.resolve_repo_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "dev_health_ops.api.queries.client.query_dicts",
                new_callable=AsyncMock,
            ) as mock_query,
        ):
            filters = WorkGraphEdgeFilterInput(repo_ids=["nonexistent/repo"])
            result = await resolve_work_graph_edges(mock_context, filters)

            assert result.total_count == 0
            assert result.edges == []
            # Short-circuited before any edge query touched SQL.
            mock_query.assert_not_called()

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
    async def test_raises_for_unknown_persisted_enum_values(self, mock_context):
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

            with pytest.raises(ValueError, match="unknown_type"):
                await resolve_work_graph_edges(mock_context)

    @pytest.mark.asyncio
    async def test_raises_when_client_missing(self):
        context = GraphQLContext(
            org_id="test-org",
            db_url="clickhouse://localhost:8123/default",
            client=None,
        )

        with pytest.raises(RuntimeError, match="Database client not available"):
            await resolve_work_graph_edges(context)


class TestWorkGraphEdgeDisplayNames:
    """CHAOS-2089: WorkGraphEdgeResult carries server-resolved display names.

    A7/A8 contract: human-readable source/target IDs pass through as
    display names; UUID-style IDs that cannot be looked up return None so
    the client renders a controlled Unresolved badge rather than a bare UUID.
    """

    @pytest.mark.asyncio
    async def test_human_readable_source_id_becomes_display_name(self, mock_context):
        """Non-UUID source_id (e.g. PROJ-123) passes through as source_display_name."""
        rows = [
            make_edge_row(source_id="PROJ-123", target_id="dep-abc"),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name == "PROJ-123"

    @pytest.mark.asyncio
    async def test_human_readable_target_id_becomes_display_name(self, mock_context):
        """Non-UUID target_id (e.g. INC-001) passes through as target_display_name."""
        rows = [
            make_edge_row(source_id="deploy-xyz", target_id="INC-001"),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].target_display_name == "INC-001"

    @pytest.mark.asyncio
    async def test_uuid_source_id_yields_none_display_name(self, mock_context):
        """UUID source_id without a resolvable name -> source_display_name is None (A8)."""
        uuid_id = "4e00fff2-df66-5028-8ebd-e4535332300b"
        rows = [
            make_edge_row(source_id=uuid_id, target_id="INC-001"),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name is None

    @pytest.mark.asyncio
    async def test_uuid_target_id_yields_none_display_name(self, mock_context):
        """UUID target_id without a resolvable name -> target_display_name is None (A8)."""
        uuid_id = "698c0211-e29b-41d4-a716-446655440000"
        rows = [
            make_edge_row(source_id="dep-xyz", target_id=uuid_id),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].target_display_name is None

    @pytest.mark.asyncio
    async def test_hash_like_ids_yield_none_display_name(self, mock_context):
        hash_id = "4e00fff2df6650288ebde4535332300b"
        rows = [
            make_edge_row(source_id=hash_id, target_id="INC-001"),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name is None

    @pytest.mark.asyncio
    async def test_display_names_present_on_every_edge(self, mock_context):
        """Every edge result carries source/target display name fields (may be None)."""
        rows = [
            make_edge_row(edge_id="e1", source_id="PROJ-1", target_id="INC-2"),
            make_edge_row(
                edge_id="e2",
                source_id="4e00fff2-df66-5028-8ebd-e4535332300b",
                target_id="INC-5",
            ),
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows
            result = await resolve_work_graph_edges(mock_context)

        assert hasattr(result.edges[0], "source_display_name")
        assert hasattr(result.edges[0], "target_display_name")
        assert result.edges[0].source_display_name == "PROJ-1"
        assert result.edges[0].target_display_name == "INC-2"
        assert result.edges[1].source_display_name is None
        assert result.edges[1].target_display_name == "INC-5"


class TestWorkGraphEdgeLookupResolution:
    """CHAOS-2120: Lookup-backed display-name resolution for WorkGraph edges.

    UUID-derived ids that appear in persisted edges are resolved to
    human-readable labels via one ClickHouse query per entity type (no N+1).
    Lookup-resolved names take precedence over the pattern-based fallback.
    """

    @pytest.mark.asyncio
    async def test_pr_uuid_id_resolved_to_title(self, mock_context):
        """PR id in {uuid}#pr{N} format resolves to PR title via git_pull_requests."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        pr_id = f"{repo_uuid}#pr160"
        edge_rows = [
            make_edge_row(
                source_type="pr",
                source_id=pr_id,
                target_type="deployment",
                target_id="synth-deploy-1",
            )
        ]
        pr_lookup_rows = [
            {"repo_id": repo_uuid, "number": 160, "title": "Add feature X"}
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, pr_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name == "Add feature X"
        # Human-readable target passes through unchanged
        assert result.edges[0].target_display_name == "synth-deploy-1"

    @pytest.mark.asyncio
    async def test_pr_uuid_id_with_no_db_match_yields_none(self, mock_context):
        """PR id in {uuid}#pr{N} format with no matching title in DB → None (A8)."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        pr_id = f"{repo_uuid}#pr999"
        edge_rows = [
            make_edge_row(source_type="pr", source_id=pr_id, target_id="deploy-xyz")
        ]
        # Empty result — PR not in git_pull_requests
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, [], []]  # PR lookup empty + membership
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name is None

    @pytest.mark.asyncio
    async def test_human_readable_id_passes_through_without_lookup(self, mock_context):
        """Human-readable ids need no display-name table lookup.
        query_dicts is called twice: once for edges, once for the membership
        batch (CHAOS-2429/2430 — theme attribution always fires).
        """
        edge_rows = [make_edge_row(source_id="PROJ-123", target_id="deploy-prod")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]  # edge query + empty membership
            result = await resolve_work_graph_edges(mock_context)

        # 2 calls: edge query + membership batch (no display-name lookups needed).
        assert mock_query.call_count == 2
        assert result.edges[0].source_display_name == "PROJ-123"
        assert result.edges[0].target_display_name == "deploy-prod"

    @pytest.mark.asyncio
    async def test_opaque_hex_id_never_triggers_lookup(self, mock_context):
        """Opaque hex ids are not resolvable via display-name lookup.
        query_dicts is still called twice: edge query + membership batch
        (CHAOS-2429/2430 — theme attribution always fires).
        """
        hex_id = "032adec80b86fd88759f19e65f133d6cacc136f3276cabc79e851ccd22de1cd2"
        edge_rows = [make_edge_row(source_id=hex_id, target_id="INC-001")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]  # edge query + empty membership
            result = await resolve_work_graph_edges(mock_context)

        # 2 calls: edge query + membership batch (no display-name lookup for hex ids).
        assert mock_query.call_count == 2
        assert result.edges[0].source_display_name is None

    @pytest.mark.asyncio
    async def test_batching_multiple_pr_ids_use_one_query(self, mock_context):
        """Multiple unresolved PR ids from the same batch resolve in ONE query (no N+1)."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        edge_rows = [
            make_edge_row(
                edge_id="e1",
                source_type="pr",
                source_id=f"{repo_uuid}#pr10",
                target_type="issue",
                target_id="PROJ-1",
            ),
            make_edge_row(
                edge_id="e2",
                source_type="pr",
                source_id=f"{repo_uuid}#pr20",
                target_type="issue",
                target_id="PROJ-2",
            ),
        ]
        pr_lookup_rows = [
            {"repo_id": repo_uuid, "number": 10, "title": "PR Ten"},
            {"repo_id": repo_uuid, "number": 20, "title": "PR Twenty"},
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, pr_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        # 3 calls: edge query + 1 batch PR lookup (not one per PR) + membership
        assert mock_query.call_count == 3
        assert result.edges[0].source_display_name == "PR Ten"
        assert result.edges[1].source_display_name == "PR Twenty"

    @pytest.mark.asyncio
    async def test_deployment_uuid_resolved_to_environment_label(self, mock_context):
        """UUID deployment_id is resolved to '{env} deploy' via the deployments table."""
        dep_uuid = "aabbccdd-1234-5678-9abc-ddeeff001122"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id=dep_uuid,
                target_type="incident",
                target_id="INC-001",
            )
        ]
        dep_lookup_rows = [{"deployment_id": dep_uuid, "environment": "production"}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, dep_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].source_display_name == "production deploy"

    @pytest.mark.asyncio
    async def test_incident_uuid_resolved_to_status_label(self, mock_context):
        """UUID incident_id is resolved to 'incident ({status})' via the incidents table."""
        inc_uuid = "11223344-aabb-ccdd-eeff-001122334455"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id="synth-deploy-1",
                target_type="incident",
                target_id=inc_uuid,
            )
        ]
        inc_lookup_rows = [{"incident_id": inc_uuid, "status": "resolved"}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, inc_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        # Status normalised to title-case customer label (not raw enum string)
        assert result.edges[0].target_display_name == "incident (Resolved)"

    @pytest.mark.asyncio
    async def test_deployment_empty_environment_yields_none(self, mock_context):
        """UUID deployment_id with empty environment in DB → None (A8 — no raw UUID)."""
        dep_uuid = "aabbccdd-1234-5678-9abc-ddeeff001122"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id=dep_uuid,
                target_id="INC-001",
            )
        ]
        # Row exists in DB but environment is empty/null
        dep_lookup_rows = [{"deployment_id": dep_uuid, "environment": ""}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, dep_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        # Must not leak the raw UUID — Unresolved badge (None)
        assert result.edges[0].source_display_name is None

    @pytest.mark.asyncio
    async def test_incident_empty_status_yields_none(self, mock_context):
        """UUID incident_id with empty status in DB → None (A8 — no raw UUID)."""
        inc_uuid = "11223344-aabb-ccdd-eeff-001122334455"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id="synth-deploy-1",
                target_type="incident",
                target_id=inc_uuid,
            )
        ]
        inc_lookup_rows = [{"incident_id": inc_uuid, "status": ""}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, inc_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].target_display_name is None

    @pytest.mark.asyncio
    async def test_incident_unknown_status_uses_neutral_label(self, mock_context):
        """Unknown incident status normalises to the neutral 'Incident' label."""
        inc_uuid = "11223344-aabb-ccdd-eeff-001122334455"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id="synth-deploy-1",
                target_type="incident",
                target_id=inc_uuid,
            )
        ]
        inc_lookup_rows = [{"incident_id": inc_uuid, "status": "some_future_status"}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, inc_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        assert result.edges[0].target_display_name == "incident (Incident)"

    @pytest.mark.asyncio
    async def test_lookup_sql_includes_org_id_for_pr(self, mock_context):
        """PR batch lookup query must include org_id to prevent cross-tenant leaks."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        edge_rows = [
            make_edge_row(source_type="pr", source_id=f"{repo_uuid}#pr5", target_id="X")
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, [], []]  # PR lookup empty + membership
            await resolve_work_graph_edges(mock_context)

        # Second call is the PR lookup — verify org_id in params
        pr_lookup_call = mock_query.call_args_list[1]
        pr_lookup_params = pr_lookup_call[0][2]
        assert "org_id" in pr_lookup_params
        assert pr_lookup_params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_lookup_sql_includes_org_id_for_deployment(self, mock_context):
        """Deployment batch lookup query must include org_id."""
        dep_uuid = "aabbccdd-1234-5678-9abc-ddeeff001122"
        edge_rows = [
            make_edge_row(source_type="deployment", source_id=dep_uuid, target_id="X")
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                [],
                [],
            ]  # dep lookup empty + membership
            await resolve_work_graph_edges(mock_context)

        dep_lookup_call = mock_query.call_args_list[1]
        dep_lookup_params = dep_lookup_call[0][2]
        assert "org_id" in dep_lookup_params
        assert dep_lookup_params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_lookup_sql_includes_org_id_for_incident(self, mock_context):
        """Incident batch lookup query must include org_id."""
        inc_uuid = "11223344-aabb-ccdd-eeff-001122334455"
        edge_rows = [
            make_edge_row(
                source_type="deployment",
                source_id="synth-deploy-1",
                target_type="incident",
                target_id=inc_uuid,
            )
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                [],
                [],
            ]  # inc lookup empty + membership
            await resolve_work_graph_edges(mock_context)

        inc_lookup_call = mock_query.call_args_list[1]
        inc_lookup_params = inc_lookup_call[0][2]
        assert "org_id" in inc_lookup_params
        assert inc_lookup_params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_lookup_resolved_name_beats_pattern_fallback(self, mock_context):
        """Lookup result takes precedence over the pattern-based pass-through (A7)."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        pr_id = f"{repo_uuid}#pr1"
        edge_rows = [make_edge_row(source_type="pr", source_id=pr_id, target_id="X")]
        # Lookup returns a title
        pr_lookup_rows = [{"repo_id": repo_uuid, "number": 1, "title": "Lookup Title"}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, pr_lookup_rows, []]  # + membership
            result = await resolve_work_graph_edges(mock_context)

        # Lookup win — not the raw {uuid}#pr1 id, not None
        assert result.edges[0].source_display_name == "Lookup Title"

    @pytest.mark.asyncio
    async def test_pr_lookup_filters_by_number(self, mock_context):
        """PR batch lookup params include number list to avoid fetching all repo PRs."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        edge_rows = [
            make_edge_row(
                source_type="pr", source_id=f"{repo_uuid}#pr42", target_id="X"
            )
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, [], []]  # PR lookup empty + membership
            await resolve_work_graph_edges(mock_context)

        pr_lookup_params = mock_query.call_args_list[1][0][2]
        assert "pr_numbers" in pr_lookup_params
        assert 42 in pr_lookup_params["pr_numbers"]


class TestRepoScopeResolutionContract:
    """Guardrail: EVERY work-graph resolver must route repo refs through the
    shared ``resolve_repo_ids`` choke point (slug/UUID -> org-scoped catalog
    UUID) BEFORE they reach SQL. A resolver that filters ``repo_id`` on a raw
    slug reintroduces the CANNOT_PARSE_UUID / empty-graph regression. This test
    fails if any resolver stops calling the shared resolver or drops org_id."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "resolver",
        [
            resolve_work_graph_edges,
            resolve_work_graph_flow,
            resolve_work_graph_artifacts,
        ],
    )
    async def test_resolver_routes_repo_refs_through_shared_resolver(
        self, resolver, mock_context
    ):
        with (
            patch(
                "dev_health_ops.api.graphql.resolvers.work_graph.resolve_repo_ids",
                new_callable=AsyncMock,
                return_value=["920f9442-07df-4217-4dc4-c5833c0b8268"],
            ) as mock_resolve,
            patch(
                "dev_health_ops.api.queries.client.query_dicts",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            filters = WorkGraphEdgeFilterInput(repo_ids=["full-chaos/dev-health-ops"])
            await resolver(mock_context, filters)

            # The shared choke point was invoked exactly once, org-scoped, on the
            # raw ref (NOT a bespoke inline SQL subquery, NOT a raw slug in SQL).
            mock_resolve.assert_awaited_once()
            assert mock_resolve.await_args is not None
            assert mock_resolve.await_args.args[1] == ["full-chaos/dev-health-ops"]
            assert mock_resolve.await_args.kwargs.get("org_id") == "test-org"
