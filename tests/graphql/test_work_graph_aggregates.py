"""Unit tests for CHAOS-2442 work-graph aggregates + plural edge_types filter.

The Dependencies / Inflow-Outflow / Artifacts tabs no longer feed off a single
``LIMIT 1000`` unordered edge page (which, for the demo org, was dominated by
``references`` edges and starved every other edge type). Instead:

  * ``edge_types`` (plural) lets a tab fetch its OWN edge types BEFORE the cap,
    so a sparse category can never be starved.
  * ``work_graph_flow`` / ``work_graph_artifacts`` are TRUE server-side
    aggregates computed over the FULL edge set (correct at any scale).
  * The edge-list query now ``ORDER BY confidence DESC, edge_id ASC`` so the
    capped overview canvas is no longer pure table-order ``references``.

These tests mock ``query_dicts`` and assert on the captured SQL/params and the
aggregated Python result (skewed mixes prove the result reflects the TRUE mix,
not a cap artifact). They also prove the degraded-state contract is identical to
``resolve_work_graph_edges``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    WorkGraphEdgeFilterInput,
    WorkGraphEdgeTypeInput,
)
from dev_health_ops.api.graphql.models.outputs import WorkGraphNodeType
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


class _UnknownMembershipTableError(Exception):
    """code-60 UNKNOWN_TABLE naming work_unit_membership (rolling-deploy state)."""

    def __init__(self) -> None:
        super().__init__(
            "Received ClickHouse exception, code: 60, server response: "
            "Code: 60. DB::Exception: Unknown table expression identifier "
            "'work_unit_membership'. (UNKNOWN_TABLE)"
        )
        self.code = 60


# ---------------------------------------------------------------------------
# 1. Plural edge_types filter (dependency edges fetched BEFORE the cap)
# ---------------------------------------------------------------------------
class TestEdgeTypesFilter:
    @pytest.mark.asyncio
    async def test_edge_types_produces_in_clause(self, mock_context):
        """edge_types=[BLOCKS, RELATES, ...] → `edge_type IN` with those values
        in params, applied in the edge WHERE (before the LIMIT) so the
        Dependencies tab's edges can never be starved by the row cap."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            dep_types = [
                WorkGraphEdgeTypeInput.BLOCKS,
                WorkGraphEdgeTypeInput.RELATES,
                WorkGraphEdgeTypeInput.IS_BLOCKED_BY,
                WorkGraphEdgeTypeInput.PARENT_OF,
            ]
            filters = WorkGraphEdgeFilterInput(edge_types=dep_types)
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            params = mock_query.call_args_list[0][0][2]

            assert "edge_type IN %(edge_types)s" in sql
            assert params["edge_types"] == [
                "blocks",
                "relates",
                "is_blocked_by",
                "parent_of",
            ]
            # The IN clause precedes the LIMIT — proof it filters before the cap.
            assert sql.index("edge_type IN") < sql.index("LIMIT")

    @pytest.mark.asyncio
    async def test_dependency_edge_types_project_work_item_dependencies(
        self, mock_context
    ):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                [],
                [
                    {
                        "edge_id": "wid:abc",
                        "source_type": "pr",
                        "source_id": "ghpr:full-chaos/dev-health-ops#1171",
                        "target_type": "issue",
                        "target_id": "linear:CHAOS-2852",
                        "edge_type": "relates",
                        "repo_id": None,
                        "provider": None,
                        "provenance": "native",
                        "confidence": 1.0,
                        "evidence": "linear_attachment",
                    }
                ],
                [],
                [],
            ]

            filters = WorkGraphEdgeFilterInput(
                edge_types=[WorkGraphEdgeTypeInput.RELATES]
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        dependency_sql = mock_query.call_args_list[1][0][1]
        dependency_params = mock_query.call_args_list[1][0][2]
        assert "FROM work_item_dependencies FINAL" in dependency_sql
        assert dependency_params["edge_types"] == ["relates"]
        assert len(result.edges) == 1
        edge = result.edges[0]
        assert edge.source_type == WorkGraphNodeType.PR
        assert edge.source_id == "ghpr:full-chaos/dev-health-ops#1171"
        assert edge.target_type == WorkGraphNodeType.ISSUE
        assert edge.target_id == "linear:CHAOS-2852"

    @pytest.mark.asyncio
    async def test_dependency_projection_preserves_edge_type_and_semantics(
        self, mock_context
    ):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(
                edge_type=WorkGraphEdgeTypeInput.BLOCKS,
                edge_types=[WorkGraphEdgeTypeInput.RELATES],
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert len(mock_query.call_args_list) == 1
        assert (
            "FROM work_item_dependencies FINAL"
            not in mock_query.call_args_list[0][0][1]
        )

    @pytest.mark.asyncio
    async def test_dependency_projection_skips_unscopable_repo_filter(
        self, mock_context
    ):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(
                edge_types=[WorkGraphEdgeTypeInput.RELATES],
                repo_ids=["00000000-0000-0000-0000-000000000001"],
            )
            await resolve_work_graph_edges(mock_context, filters)

        assert all(
            "FROM work_item_dependencies FINAL" not in call_args[0][1]
            for call_args in mock_query.call_args_list
        )

    @pytest.mark.asyncio
    async def test_dependency_projection_applies_theme_membership_filter(
        self, mock_context
    ):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                [],
                [],
                [{"complete_run_markers": 1, "investment_rows": 5}],
            ]

            filters = WorkGraphEdgeFilterInput(
                edge_types=[WorkGraphEdgeTypeInput.RELATES],
                theme="feature_delivery",
            )
            await resolve_work_graph_edges(mock_context, filters)

        dependency_sql = mock_query.call_args_list[1][0][1]
        dependency_params = mock_query.call_args_list[1][0][2]
        assert "FROM work_item_dependencies FINAL" in dependency_sql
        assert "FROM work_unit_membership AS m" in dependency_sql
        assert "source_work_item_id" in dependency_sql
        assert "target_work_item_id" in dependency_sql
        assert dependency_params["category_tuples"] == [("theme", "feature_delivery")]

    @pytest.mark.asyncio
    async def test_dependency_projection_deduplicates_persisted_edges(
        self, mock_context
    ):
        persisted_row = {
            "edge_id": "edge:persisted",
            "source_type": "pr",
            "source_id": "ghpr:full-chaos/dev-health-ops#1171",
            "target_type": "issue",
            "target_id": "linear:CHAOS-2852",
            "edge_type": "relates",
            "repo_id": None,
            "provider": None,
            "provenance": "native",
            "confidence": 0.9,
            "evidence": "persisted",
        }
        duplicate_dependency_row = persisted_row | {
            "edge_id": "wid:duplicate",
            "confidence": 1.0,
            "evidence": "linear_attachment",
        }
        unique_dependency_row = duplicate_dependency_row | {
            "edge_id": "wid:unique",
            "target_id": "linear:CHAOS-2853",
        }

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                [persisted_row],
                [duplicate_dependency_row, unique_dependency_row],
                [],
                [],
            ]

            filters = WorkGraphEdgeFilterInput(
                edge_types=[WorkGraphEdgeTypeInput.RELATES]
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        assert [edge.edge_id for edge in result.edges] == [
            "edge:persisted",
            "wid:unique",
        ]

    @pytest.mark.asyncio
    async def test_singular_edge_type_still_works(self, mock_context):
        """The existing singular edge_type filter is unchanged."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(
                edge_type=WorkGraphEdgeTypeInput.IMPLEMENTS
            )
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            params = mock_query.call_args_list[0][0][2]

            assert "edge_type = %(edge_type)s" in sql
            assert params["edge_type"] == "implements"
            assert "edge_type IN" not in sql

    @pytest.mark.asyncio
    async def test_singular_and_plural_are_anded(self, mock_context):
        """When BOTH edge_type and edge_types are given, both clauses apply (AND)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = WorkGraphEdgeFilterInput(
                edge_type=WorkGraphEdgeTypeInput.BLOCKS,
                edge_types=[
                    WorkGraphEdgeTypeInput.BLOCKS,
                    WorkGraphEdgeTypeInput.RELATES,
                ],
            )
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            params = mock_query.call_args_list[0][0][2]

            assert "edge_type = %(edge_type)s" in sql
            assert "edge_type IN %(edge_types)s" in sql
            assert params["edge_type"] == "blocks"
            assert params["edge_types"] == ["blocks", "relates"]


# ---------------------------------------------------------------------------
# 4. Edge-list ORDER BY — GATED on a narrowing filter (preserves early-LIMIT
#    termination on the unfiltered hot path; relevance sort only where the
#    candidate set is already bounded).
# ---------------------------------------------------------------------------
class TestEdgeListOrdering:
    @pytest.mark.asyncio
    async def test_no_order_by_when_unfiltered(self, mock_context):
        """Fully-unfiltered default overview must emit NO ORDER BY confidence so
        ClickHouse keeps early-LIMIT termination (a global sort would read the
        org's ENTIRE edge set before returning the first page)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            await resolve_work_graph_edges(mock_context)

            sql = mock_query.call_args_list[0][0][1]
            assert "ORDER BY" not in sql
            assert "LIMIT" in sql

    @pytest.mark.asyncio
    async def test_no_order_by_with_empty_filter_object(self, mock_context):
        """An empty filters object (no narrowing field set) is still the hot
        path — no ORDER BY."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            await resolve_work_graph_edges(mock_context, WorkGraphEdgeFilterInput())

            sql = mock_query.call_args_list[0][0][1]
            assert "ORDER BY" not in sql

    @pytest.mark.asyncio
    async def test_order_by_when_edge_types_filter_active(self, mock_context):
        """A narrowing filter bounds the candidate set, so the relevance sort is
        applied (deterministic, before the cap)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            filters = WorkGraphEdgeFilterInput(
                edge_types=[WorkGraphEdgeTypeInput.BLOCKS]
            )
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            assert "ORDER BY confidence DESC, edge_id ASC" in sql
            assert sql.index("ORDER BY") < sql.index("LIMIT")

    @pytest.mark.asyncio
    async def test_order_by_when_theme_filter_active(self, mock_context):
        """A theme filter is narrowing → relevance sort applied."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # edge query empty, then degraded probe.
            mock_query.side_effect = [
                [],
                [{"complete_run_markers": 1, "investment_rows": 5}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            assert "ORDER BY confidence DESC, edge_id ASC" in sql

    @pytest.mark.asyncio
    async def test_order_by_when_repo_ids_filter_active(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            filters = WorkGraphEdgeFilterInput(repo_ids=["repo-1"])
            await resolve_work_graph_edges(mock_context, filters)

            sql = mock_query.call_args[0][1]
            assert "ORDER BY confidence DESC, edge_id ASC" in sql


# ---------------------------------------------------------------------------
# 2. work_graph_flow aggregates the TRUE direction mix
# ---------------------------------------------------------------------------
class TestWorkGraphFlow:
    @pytest.mark.asyncio
    async def test_flow_aggregates_inflow_outflow_per_node_type(self, mock_context):
        """A skewed GROUP-BY result aggregates into correct per-node-type
        inflow/outflow — reflecting the TRUE mix, not a cap artifact."""
        group_rows = [
            # issue -> pr (heavy)
            {"source_type": "issue", "target_type": "pr", "cnt": 100},
            # pr -> commit
            {"source_type": "pr", "target_type": "commit", "cnt": 40},
            # issue -> issue (dependencies — would be starved in an unordered cap)
            {"source_type": "issue", "target_type": "issue", "cnt": 10},
            # commit -> file
            {"source_type": "commit", "target_type": "file", "cnt": 25},
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = group_rows
            result = await resolve_work_graph_flow(mock_context)

        by_type = {r.node_type: r for r in result.rows}

        # issue: outflow = 100 (->pr) + 10 (->issue); inflow = 10 (issue->issue)
        assert by_type[WorkGraphNodeType.ISSUE].outflow == 110
        assert by_type[WorkGraphNodeType.ISSUE].inflow == 10
        # pr: inflow 100 (issue->pr); outflow 40 (pr->commit)
        assert by_type[WorkGraphNodeType.PR].inflow == 100
        assert by_type[WorkGraphNodeType.PR].outflow == 40
        # commit: inflow 40 (pr->commit); outflow 25 (commit->file)
        assert by_type[WorkGraphNodeType.COMMIT].inflow == 40
        assert by_type[WorkGraphNodeType.COMMIT].outflow == 25
        # file: inflow 25; outflow 0
        assert by_type[WorkGraphNodeType.FILE].inflow == 25
        assert by_type[WorkGraphNodeType.FILE].outflow == 0

        # Sorted by total degree desc.
        totals = [r.inflow + r.outflow for r in result.rows]
        assert totals == sorted(totals, reverse=True)
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_flow_dedups_by_edge_id_not_raw_count(self, mock_context):
        """work_graph_edges is a ReplacingMergeTree keyed logically on edge_id;
        un-merged duplicate physical rows must NOT inflate the direction mix.
        The flow SQL must aggregate via uniqExact(edge_id), not raw count()."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            await resolve_work_graph_flow(mock_context)

            sql = mock_query.call_args[0][1]
            assert "uniqExact(edge_id) AS cnt" in sql
            assert "count() AS cnt" not in sql

    @pytest.mark.asyncio
    async def test_flow_aggregation_uses_db_dedup_count(self, mock_context):
        """The GROUP-BY rows already carry the DB-deduped uniqExact(edge_id)
        count; Python sums those per direction. Duplicate physical edge versions
        collapse server-side, so a row's cnt is the LOGICAL edge count."""
        # Two source/target groups; cnt is the already-deduped uniqExact value.
        group_rows = [
            {"source_type": "issue", "target_type": "pr", "cnt": 3},
            {"source_type": "issue", "target_type": "issue", "cnt": 2},
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = group_rows
            result = await resolve_work_graph_flow(mock_context)

        by_type = {r.node_type: r for r in result.rows}
        # issue outflow = 3 (->pr) + 2 (->issue); inflow = 2 (issue->issue)
        assert by_type[WorkGraphNodeType.ISSUE].outflow == 5
        assert by_type[WorkGraphNodeType.ISSUE].inflow == 2
        assert by_type[WorkGraphNodeType.PR].inflow == 3

    @pytest.mark.asyncio
    async def test_flow_uses_group_by_over_full_set_no_limit(self, mock_context):
        """The flow query is a GROUP BY over the full edge set (no LIMIT) and is
        org-scoped — it describes the WHOLE graph."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            await resolve_work_graph_flow(mock_context)

            sql = mock_query.call_args[0][1]
            params = mock_query.call_args[0][2]

            assert "GROUP BY source_type, target_type" in sql
            assert "uniqExact(edge_id) AS cnt" in sql
            assert "LIMIT" not in sql
            assert "org_id = %(org_id)s" in sql
            assert params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_flow_ignores_edge_list_only_filters(self, mock_context):
        """Aggregates describe the whole graph: edge-list-only filters
        (edge_type/source_type/node_id) are NOT applied; graph-wide repo_ids IS."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []
            filters = WorkGraphEdgeFilterInput(
                edge_type=WorkGraphEdgeTypeInput.REFERENCES,
                edge_types=[WorkGraphEdgeTypeInput.BLOCKS],
                node_id="PROJ-1",
                repo_ids=["repo-1"],
            )
            await resolve_work_graph_flow(mock_context, filters)

            sql = mock_query.call_args[0][1]
            params = mock_query.call_args[0][2]

            assert "edge_type" not in sql
            assert "node_id" not in sql
            assert "repo_id IN %(repo_ids)s" in sql
            assert params["repo_ids"] == ["repo-1"]

    @pytest.mark.asyncio
    async def test_flow_degraded_on_missing_membership_table(self, mock_context):
        """theme filter + missing-membership-table error → MEMBERSHIP_NOT_MATERIALIZED."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _UnknownMembershipTableError()
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_flow(mock_context, filters)

        assert result.rows == []
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"

    @pytest.mark.asyncio
    async def test_flow_other_error_propagates(self, mock_context):
        """A non-membership error must propagate (no blanket except/return-default)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = RuntimeError("connection reset")
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            with pytest.raises(RuntimeError, match="connection reset"):
                await resolve_work_graph_flow(mock_context, filters)

    @pytest.mark.asyncio
    async def test_flow_empty_theme_filter_runs_degraded_probe(self, mock_context):
        """theme filter + empty aggregate → degraded probe runs (parity with edges)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Aggregate empty, then degraded probe: investments exist, no marker.
            mock_query.side_effect = [
                [],
                [{"complete_run_markers": 0, "investment_rows": 5}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_flow(mock_context, filters)

        assert result.rows == []
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"
        assert mock_query.call_count == 2


# ---------------------------------------------------------------------------
# 3. work_graph_artifacts aggregates degree + resolves display names
# ---------------------------------------------------------------------------
class TestWorkGraphArtifacts:
    @pytest.mark.asyncio
    async def test_artifacts_aggregates_degree_ordered_desc(self, mock_context):
        """Degree rows map to artifact rows in order, with display-name resolution
        / fallback (human-readable passes through; UUID → None)."""
        uuid_id = "4e00fff2-df66-5028-8ebd-e4535332300b"
        degree_rows = [
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "degree": 12,
                "evidence": "Closes #1",
            },
            {
                "node_type": "pr",
                "node_id": uuid_id,
                "degree": 7,
                "evidence": None,
            },
        ]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # degree query, then display-name lookup (PR uuid not resolvable here).
            mock_query.side_effect = [degree_rows, []]
            result = await resolve_work_graph_artifacts(mock_context)

        assert [r.node_id for r in result.rows] == ["PROJ-1", uuid_id]
        assert result.rows[0].degree == 12
        assert result.rows[1].degree == 7
        # Degree order preserved (desc).
        assert result.rows[0].degree >= result.rows[1].degree
        # Human-readable id passes through; bare UUID (no lookup) → None badge.
        assert result.rows[0].display_name == "PROJ-1"
        assert result.rows[1].display_name is None
        # Evidence passes through (str or None).
        assert result.rows[0].evidence == "Closes #1"
        assert result.rows[1].evidence is None
        assert result.rows[0].node_type == WorkGraphNodeType.ISSUE
        assert result.rows[1].node_type == WorkGraphNodeType.PR
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_artifacts_degree_sql_union_and_limit(self, mock_context):
        """The degree SQL UNIONs source+target projections, GROUPs by node, orders
        by degree desc, and honours filters.limit as the top-N."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[], []]
            filters = WorkGraphEdgeFilterInput(limit=50)
            await resolve_work_graph_artifacts(mock_context, filters)

            sql = mock_query.call_args_list[0][0][1]
            params = mock_query.call_args_list[0][0][2]

            assert "UNION ALL" in sql
            assert "source_type AS node_type, source_id AS node_id" in sql
            assert "target_type AS node_type, target_id AS node_id" in sql
            assert "GROUP BY node_type, node_id" in sql
            assert "ORDER BY degree DESC, node_id ASC" in sql
            assert "LIMIT %(limit)s" in sql
            assert params["limit"] == 50
            # org_id appears for cross-tenant safety.
            assert params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_artifacts_dedups_degree_by_edge_id(self, mock_context):
        """Degree must be uniqExact(edge_id) per node (not raw count()), and
        edge_id must be projected in BOTH UNION ALL legs so the dedup works.
        This prevents un-merged ReplacingMergeTree duplicate rows from inflating
        degree."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[], []]
            await resolve_work_graph_artifacts(mock_context)

            sql = mock_query.call_args_list[0][0][1]
            assert "uniqExact(edge_id) AS degree" in sql
            assert "count() AS degree" not in sql
            # edge_id carried through BOTH legs of the UNION ALL.
            assert (
                "SELECT source_type AS node_type, source_id AS node_id, edge_id" in sql
            )
            assert (
                "SELECT target_type AS node_type, target_id AS node_id, edge_id" in sql
            )

    @pytest.mark.asyncio
    async def test_artifacts_self_loop_counts_once(self, mock_context):
        """A self-referential edge (source==target, same node) appears as TWO
        UNION ALL rows carrying the SAME edge_id. uniqExact(edge_id) counts it
        ONCE (one edge touching one node → degree 1, not 2). Assert the SQL
        shape that guarantees this (query_dicts is mocked)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[], []]
            await resolve_work_graph_artifacts(mock_context)

            sql = mock_query.call_args_list[0][0][1]
            # Distinct edge_id per node → a self-loop's two rows share one edge_id
            # and collapse to degree 1.
            assert "uniqExact(edge_id) AS degree" in sql
            assert "GROUP BY node_type, node_id" in sql

    @pytest.mark.asyncio
    async def test_artifacts_resolves_pr_display_name(self, mock_context):
        """A {uuid}#pr{N} node id resolves to its PR title via the batch resolver."""
        repo_uuid = "4e00fff2-df66-5028-8ebd-e4535332300b"
        pr_id = f"{repo_uuid}#pr160"
        degree_rows = [
            {"node_type": "pr", "node_id": pr_id, "degree": 5, "evidence": None}
        ]
        pr_lookup_rows = [{"repo_id": repo_uuid, "number": 160, "title": "Add X"}]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [degree_rows, pr_lookup_rows]
            result = await resolve_work_graph_artifacts(mock_context)

        assert result.rows[0].display_name == "Add X"

    @pytest.mark.asyncio
    async def test_artifacts_honours_limit_default_when_no_filters(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[], []]
            await resolve_work_graph_artifacts(mock_context)

            params = mock_query.call_args_list[0][0][2]
            assert params["limit"] == 1000

    @pytest.mark.asyncio
    async def test_artifacts_degraded_on_missing_membership_table(self, mock_context):
        """theme filter + missing-membership-table error → MEMBERSHIP_NOT_MATERIALIZED."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _UnknownMembershipTableError()
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_artifacts(mock_context, filters)

        assert result.rows == []
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"

    @pytest.mark.asyncio
    async def test_artifacts_other_error_propagates(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = RuntimeError("timeout")
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            with pytest.raises(RuntimeError, match="timeout"):
                await resolve_work_graph_artifacts(mock_context, filters)


# ---------------------------------------------------------------------------
# 5. Theme/subcategory conflict short-circuits both aggregates (rows=[])
# ---------------------------------------------------------------------------
class TestAggregateConflictShortCircuit:
    @pytest.mark.asyncio
    async def test_flow_conflict_returns_empty_without_query(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # maintenance theme does not own a feature_delivery subcategory.
            filters = WorkGraphEdgeFilterInput(
                theme="maintenance", subcategory="feature_delivery.roadmap"
            )
            result = await resolve_work_graph_flow(mock_context, filters)

        assert result.rows == []
        assert result.degraded_reason is None
        mock_query.assert_not_called()

    @pytest.mark.asyncio
    async def test_artifacts_conflict_returns_empty_without_query(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            filters = WorkGraphEdgeFilterInput(
                theme="maintenance", subcategory="feature_delivery.roadmap"
            )
            result = await resolve_work_graph_artifacts(mock_context, filters)

        assert result.rows == []
        assert result.degraded_reason is None
        mock_query.assert_not_called()
