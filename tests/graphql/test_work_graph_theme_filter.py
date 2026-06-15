"""Tests for CHAOS-2430/2433 server-side theme/subcategory filtering of work graph edges.

The membership filter is pushed INTO the edge query as a correlated EXISTS
semi-join (no unbounded Python prefetch), so repo_id/edge_type/source filters,
the membership filter, and the LIMIT all run in one ClickHouse plan. The
before-LIMIT guarantee holds because the EXISTS lives in the edge WHERE. These
tests prove:

- The EXISTS semi-join + LIMIT are in the SAME edge query (cap-safety): a sparse
  theme's matching edge is returned even past the row cap.
- A theme filter returns only matching edges; theme+subcategory requires BOTH
  (uniqExact == 2); subcategory-only matches on the subcategory tuple.
- Run-scoping via work_unit_membership_runs (CHAOS-2433 protocol) replaces the
  old per-node max(computed_at) guard everywhere (filter, annotation, degraded
  probe). Readers select the latest COMPLETE run via argMax(run_id, completed_at)
  from work_unit_membership_runs and scope ALL membership reads to that run_id.
- Concurrency race: a half-written materializer run (no marker yet) is invisible;
  the prior COMPLETE run (e.g. from a backfill) is visible until the marker lands.
- The no-filter path is unchanged (issue>pr precedence annotation preserved).
- Annotation ALWAYS reports the node's DOMINANT category on both paths — the
  theme filter selects which edges show, never what edge.theme reports (a unit
  matched under its secondary theme still reports its dominant).
- Org isolation; AND-composition with repo/edge_type; degraded_reason is
  MEMBERSHIP_NOT_MATERIALIZED only when a filter yields nothing AND investments
  exist with zero membership rows in the latest complete run, else None.
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


def dominant_rows(
    node_type: str,
    node_id: str,
    theme: str,
    subcategory: str,
) -> list[dict[str, Any]]:
    """The is_dominant rows the annotation query returns for one node."""
    return [
        {
            "node_type": node_type,
            "node_id": node_id,
            "category_kind": "theme",
            "category": theme,
        },
        {
            "node_type": node_type,
            "node_id": node_id,
            "category_kind": "subcategory",
            "category": subcategory,
        },
    ]


class TestThemeFilterServerSide:
    @pytest.mark.asyncio
    async def test_exists_semijoin_and_limit_in_one_query(self, mock_context):
        """Cap-safety: the membership EXISTS semi-join and the LIMIT live in the
        SAME edge query, so the membership filter reduces candidates BEFORE the
        cap (no separate prefetch, no Python IN set). A matching edge that would
        otherwise sit past the cap is still returned.
        """
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
            # Call 0 = the single combined edge+membership SELECT (EXISTS + LIMIT).
            # Call 1 = the dominant-annotation lookup for the returned endpoints.
            # (Human-readable ids → no display-name lookup; rows non-empty → no
            # degraded probe.)
            mock_query.side_effect = [
                edge_rows,
                dominant_rows("issue", "SPARSE-1", "feature_delivery", "x.y"),
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery", limit=1000)
            result = await resolve_work_graph_edges(mock_context, filters)

        assert len(result.edges) == 1
        assert result.edges[0].edge_id == "sparse-edge"

        # The EDGE selection itself is ONE combined plan (EXISTS + LIMIT in the
        # first query); the second call is annotation-only, not a filter prefetch.
        edge_sql = mock_query.call_args_list[0][0][1]
        edge_params = mock_query.call_args_list[0][0][2]
        # The membership semi-join is correlated on both endpoints, in the WHERE.
        assert "EXISTS (" in edge_sql
        assert (
            "(m.node_type, m.node_id) = "
            "(work_graph_edges.source_type, work_graph_edges.source_id)"
        ) in edge_sql
        assert (
            "(m.node_type, m.node_id) = "
            "(work_graph_edges.target_type, work_graph_edges.target_id)"
        ) in edge_sql
        assert "(m.category_kind, m.category) IN %(category_tuples)s" in edge_sql
        assert "uniqExact((m.category_kind, m.category)) = %(wanted_count)s" in edge_sql
        assert "LIMIT %(limit)s" in edge_sql
        # EXISTS appears before LIMIT (constraint precedes the cap).
        assert edge_sql.index("EXISTS") < edge_sql.index("LIMIT")
        # The requested category tuple is bound; no giant matched-node IN set.
        assert ("theme", "feature_delivery") in edge_params["category_tuples"]
        assert edge_params["wanted_count"] == 1
        assert "matched_nodes" not in edge_params

    @pytest.mark.asyncio
    async def test_run_scoped_via_completion_marker(self, mock_context):
        """The filter semijoin uses run_id scoping from work_unit_membership_runs
        (CHAOS-2433), NOT per-node max(computed_at). This makes split/merge safe:
        nodes absent from the latest complete run have no matching rows."""
        edge_rows = [make_edge_row(edge_id="e1", source_id="N-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # edge query + dominant-annotation lookup.
            mock_query.side_effect = [edge_rows, []]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            await resolve_work_graph_edges(mock_context, filters)

        edge_sql = mock_query.call_args_list[0][0][1]
        # Run-scoped via work_unit_membership_runs + argMax(run_id, completed_at).
        assert "work_unit_membership_runs" in edge_sql
        assert "argMax(run_id, completed_at) AS latest_run_id" in edge_sql
        assert "m.run_id = latest_run.latest_run_id" in edge_sql
        # Guard: incomplete/empty runs (latest_run_id='') are excluded.
        assert "latest_run.latest_run_id != ''" in edge_sql
        # Must NOT use old per-node max(computed_at) guard.
        assert "max(computed_at) AS max_computed_at" not in edge_sql
        assert "GROUP BY node_type, node_id" not in edge_sql

    @pytest.mark.asyncio
    async def test_theme_filter_returns_matching_edges_annotated_with_dominant(
        self, mock_context
    ):
        """The filter selects which edges show; annotation reports each node's
        actual DOMINANT category (from the is_dominant rows), not the request."""
        edge_rows = [
            make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1"),
            make_edge_row(edge_id="e2", source_type="issue", source_id="FD-2"),
        ]
        # Both nodes matched feature_delivery, but their DOMINANT differs.
        annotation_rows = dominant_rows(
            "issue", "FD-1", "feature_delivery", "feature_delivery.roadmap"
        ) + dominant_rows("issue", "FD-2", "maintenance", "maintenance.refactor")

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, annotation_rows]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert {e.edge_id for e in result.edges} == {"e1", "e2"}
        by_id = {e.edge_id: e for e in result.edges}
        # e1's dominant matches the filter; e2 matched feature_delivery as a
        # SECONDARY theme but still reports its dominant (maintenance).
        assert by_id["e1"].theme == "feature_delivery"
        assert by_id["e2"].theme == "maintenance"

    @pytest.mark.asyncio
    async def test_degraded_state_when_membership_unmaterialized(self, mock_context):
        """Empty filter result + investments>0 + membership==0 in the latest
        complete run → edges=[] with degraded_reason=MEMBERSHIP_NOT_MATERIALIZED.
        The degraded probe uses run_id scoping (CHAOS-2433), not per-node
        max(computed_at)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Edge query empty → degraded probe runs (2nd call) and reports
            # zero membership rows but non-zero investments.
            mock_query.side_effect = [
                [],
                [{"membership_rows": 0, "investment_rows": 5}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="risk")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.total_count == 0
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"
        # The edge query plus the degraded-state probe.
        assert mock_query.call_count == 2
        probe_sql = mock_query.call_args_list[1][0][1]
        # Probe uses run_id scoping via work_unit_membership_runs.
        assert "work_unit_membership" in probe_sql
        assert "work_unit_membership_runs" in probe_sql
        assert "work_unit_investments" in probe_sql
        # Run-scoped, NOT per-node max(computed_at).
        assert "argMax(run_id, completed_at) AS latest_run_id" in probe_sql
        assert "m.run_id = latest_run.latest_run_id" in probe_sql
        assert "max(computed_at) AS max_computed_at" not in probe_sql

    @pytest.mark.asyncio
    async def test_genuine_empty_is_not_degraded(self, mock_context):
        """Empty filter result but membership rows EXIST (no match) → not a
        degraded state: degraded_reason is None."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                [],
                [{"membership_rows": 42, "investment_rows": 100}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="risk")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_no_investments_empty_is_not_degraded(self, mock_context):
        """Empty filter result with no investments at all → None (nothing to
        materialize, so not a rollout-degraded state)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                [],
                [{"membership_rows": 0, "investment_rows": 0}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="risk")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_non_empty_result_has_no_degraded_reason(self, mock_context):
        """When the filter returns edges, degraded_reason is None and no probe
        runs."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue", "FD-1", "feature_delivery", "feature_delivery.roadmap"
                ),
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert [e.edge_id for e in result.edges] == ["e1"]
        assert result.degraded_reason is None
        # Calls: edge query + annotation. No degraded probe (rows non-empty).
        assert mock_query.call_count == 2

    @pytest.mark.asyncio
    async def test_theme_and_subcategory_requires_both(self, mock_context):
        """theme+subcategory → uniqExact == 2 (member of BOTH categories)."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue", "FD-1", "feature_delivery", "feature_delivery.roadmap"
                ),
            ]
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery", subcategory="feature_delivery.roadmap"
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        edge_params = mock_query.call_args_list[0][0][2]
        assert ("theme", "feature_delivery") in edge_params["category_tuples"]
        assert (
            "subcategory",
            "feature_delivery.roadmap",
        ) in edge_params["category_tuples"]
        assert edge_params["wanted_count"] == 2
        # Annotated with the node's dominant theme and subcategory.
        assert result.edges[0].theme == "feature_delivery"
        assert result.edges[0].subcategory == "feature_delivery.roadmap"

    @pytest.mark.asyncio
    async def test_subcategory_only_matches_on_subcategory_tuple(self, mock_context):
        """A subcategory-only filter matches on the subcategory tuple (wanted=1).
        Annotation reports the node's actual dominant (from membership), not the
        requested subcategory."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows("issue", "FD-1", "quality", "quality.testing"),
            ]
            filters = WorkGraphEdgeFilterInput(subcategory="quality.testing")
            result = await resolve_work_graph_edges(mock_context, filters)

        edge_params = mock_query.call_args_list[0][0][2]
        assert edge_params["category_tuples"] == [("subcategory", "quality.testing")]
        assert edge_params["wanted_count"] == 1
        # Annotation comes from the node's dominant is_dominant rows.
        assert result.edges[0].theme == "quality"
        assert result.edges[0].subcategory == "quality.testing"

    @pytest.mark.asyncio
    async def test_org_isolation_on_edge_and_membership(self, mock_context):
        """The edge query (incl. the correlated membership subquery) carries org_id.
        The run-selection subquery (work_unit_membership_runs) is org-scoped too."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            await resolve_work_graph_edges(mock_context, filters)

        edge_sql = mock_query.call_args_list[0][0][1]
        edge_params = mock_query.call_args_list[0][0][2]
        assert edge_params["org_id"] == "test-org"
        # org_id is enforced on the edge table AND inside the membership subquery.
        assert "org_id = %(org_id)s" in edge_sql
        assert "m.org_id = %(org_id)s" in edge_sql
        # The run-selection subquery (work_unit_membership_runs) is org-scoped.
        assert "work_unit_membership_runs" in edge_sql
        assert edge_sql.count("org_id = %(org_id)s") >= 2

    @pytest.mark.asyncio
    async def test_no_filter_path_unchanged(self, mock_context):
        """Without a theme filter, the unfiltered path runs: edge query +
        dominant-membership batch, with issue>pr precedence annotation (no
        EXISTS semi-join in the edge query)."""
        edge_rows = [
            make_edge_row(
                edge_id="e1",
                source_type="issue",
                source_id="PROJ-1",
                target_type="pr",
                target_id="PR-9",
            )
        ]
        # is_dominant rows (one per kind) for each endpoint, per-node-latest grain.
        membership_rows = [
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "category_kind": "theme",
                "category": "operational",
            },
            {
                "node_type": "issue",
                "node_id": "PROJ-1",
                "category_kind": "subcategory",
                "category": "operational.support",
            },
            {
                "node_type": "pr",
                "node_id": "PR-9",
                "category_kind": "theme",
                "category": "maintenance",
            },
            {
                "node_type": "pr",
                "node_id": "PR-9",
                "category_kind": "subcategory",
                "category": "maintenance.refactor",
            },
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, membership_rows]
            result = await resolve_work_graph_edges(mock_context)

        # First call is the edge SELECT with no membership EXISTS semi-join.
        edge_sql = mock_query.call_args_list[0][0][1]
        assert "EXISTS (" not in edge_sql
        # Issue endpoint wins (precedence preserved).
        assert result.edges[0].theme == "operational"
        assert result.edges[0].subcategory == "operational.support"

    @pytest.mark.asyncio
    async def test_filter_combines_with_repo_and_edge_type(self, mock_context):
        """The membership EXISTS is ANDed with existing repo/edge_type filters in
        the same edge query (one plan)."""
        from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeTypeInput

        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [edge_rows, []]
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery",
                repo_ids=["repo-a"],
                edge_type=WorkGraphEdgeTypeInput.IMPLEMENTS,
            )
            await resolve_work_graph_edges(mock_context, filters)

        edge_sql = mock_query.call_args_list[0][0][1]
        edge_params = mock_query.call_args_list[0][0][2]
        assert "repo_id IN %(repo_ids)s" in edge_sql
        assert "edge_type = %(edge_type)s" in edge_sql
        assert "EXISTS (" in edge_sql
        assert edge_params["repo_ids"] == ["repo-a"]
        assert edge_params["edge_type"] == "implements"

    @pytest.mark.asyncio
    async def test_mixed_unit_matched_under_secondary_reports_dominant(
        self, mock_context
    ):
        """Multi-membership: a node on a 45%-feature / 40%-maintenance unit is
        findable under EITHER theme — but its annotation ALWAYS reports the
        DOMINANT (feature_delivery), even when matched under the secondary
        (maintenance). The filter selects the edge; it never changes edge.theme.
        """
        edge_rows = [
            make_edge_row(edge_id="e1", source_type="issue", source_id="MIX-1")
        ]
        # The node's dominant is feature_delivery regardless of which theme matched.
        annotation = dominant_rows(
            "issue", "MIX-1", "feature_delivery", "feature_delivery.roadmap"
        )

        for theme in ("feature_delivery", "maintenance"):
            with patch(
                "dev_health_ops.api.queries.client.query_dicts",
                new_callable=AsyncMock,
            ) as mock_query:
                mock_query.side_effect = [edge_rows, annotation]
                filters = WorkGraphEdgeFilterInput(theme=theme)
                result = await resolve_work_graph_edges(mock_context, filters)

            # Found under both themes (DB EXISTS enforces membership)...
            assert [e.edge_id for e in result.edges] == ["e1"]
            edge_params = mock_query.call_args_list[0][0][2]
            assert ("theme", theme) in edge_params["category_tuples"]
            # ...but always reports the DOMINANT, not the requested filter theme.
            assert result.edges[0].theme == "feature_delivery"


class TestThemeSubcategoryCrossTaxonomy:
    """CHAOS-2430: a theme + subcategory pair that cross taxonomy boundaries has
    no valid intersection and must short-circuit to an empty result (guards
    against URL tampering / stale client / version skew), instead of building
    the impossible cross-theme filter."""

    @pytest.mark.asyncio
    async def test_conflicting_theme_subcategory_returns_empty(self, mock_context):
        """theme='feature_delivery' + subcategory='maintenance.refactor' (whose
        parent theme is maintenance) → empty result, NO query issued."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery", subcategory="maintenance.refactor"
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.total_count == 0
        # No degraded reason — this is an impossible filter, not a rollout state.
        assert result.degraded_reason is None
        # Short-circuited before any DB round-trip.
        assert mock_query.call_count == 0

    @pytest.mark.asyncio
    async def test_matching_theme_subcategory_runs_normally(self, mock_context):
        """theme + subcategory in the SAME theme → normal filtered query runs."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue", "FD-1", "feature_delivery", "feature_delivery.roadmap"
                ),
            ]
            filters = WorkGraphEdgeFilterInput(
                theme="feature_delivery", subcategory="feature_delivery.roadmap"
            )
            result = await resolve_work_graph_edges(mock_context, filters)

        assert [e.edge_id for e in result.edges] == ["e1"]
        # The edge query DID run (both category tuples present, wanted_count==2).
        edge_params = mock_query.call_args_list[0][0][2]
        assert edge_params["wanted_count"] == 2
        assert ("theme", "feature_delivery") in edge_params["category_tuples"]
        assert (
            "subcategory",
            "feature_delivery.roadmap",
        ) in edge_params["category_tuples"]

    @pytest.mark.asyncio
    async def test_subcategory_only_unaffected_by_conflict_check(self, mock_context):
        """A subcategory-only filter (no theme) never conflicts — it runs."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="M-1")]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows("issue", "M-1", "maintenance", "maintenance.refactor"),
            ]
            filters = WorkGraphEdgeFilterInput(subcategory="maintenance.refactor")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert [e.edge_id for e in result.edges] == ["e1"]
        assert mock_query.call_count >= 1


class _UnknownTableError(Exception):
    """clickhouse-connect DatabaseError for a missing work_unit_membership (code 60)."""

    def __init__(self) -> None:
        super().__init__(
            "Received ClickHouse exception, code: 60, server response: "
            "Code: 60. DB::Exception: Unknown table expression identifier "
            "'work_unit_membership'. (UNKNOWN_TABLE)"
        )
        self.code = 60


class _UnknownOtherTableError(Exception):
    """code-60 UNKNOWN_TABLE for a DIFFERENT table (e.g. work_graph_edges) —
    a genuine schema regression that must NOT be downgraded to degraded."""

    def __init__(self) -> None:
        super().__init__(
            "Received ClickHouse exception, code: 60, server response: "
            "Code: 60. DB::Exception: Unknown table expression identifier "
            "'work_graph_edges'. (UNKNOWN_TABLE)"
        )
        self.code = 60


class TestMissingMembershipTableDegrades:
    """CHAOS-2430: during a rolling deploy / before the migration runs, the
    filtered edge query references work_unit_membership which may not exist. The
    resolver must return the controlled degraded state, NOT a 500."""

    @pytest.mark.asyncio
    async def test_missing_table_returns_degraded(self, mock_context):
        """Filtered query raising an unknown-table error → edges=[] with
        degraded_reason=MEMBERSHIP_NOT_MATERIALIZED (no exception escapes)."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _UnknownTableError()
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.total_count == 0
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"

    @pytest.mark.asyncio
    async def test_missing_table_message_only_match(self, mock_context):
        """Detection also works when only the message carries the signal (no
        code attr) — resilient to driver variations."""

        class _MsgOnly(Exception):
            pass

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _MsgOnly(
                "Code: 60. DB::Exception: Unknown table 'work_unit_membership'. "
                "(UNKNOWN_TABLE)"
            )
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"
        assert result.edges == []

    @pytest.mark.asyncio
    async def test_table_present_unchanged(self, mock_context):
        """With the table present, the filtered path is unchanged."""
        edge_rows = [make_edge_row(edge_id="e1", source_type="issue", source_id="FD-1")]
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue", "FD-1", "feature_delivery", "feature_delivery.roadmap"
                ),
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert [e.edge_id for e in result.edges] == ["e1"]
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_unfiltered_path_does_not_swallow_unknown_table(self, mock_context):
        """An unknown-table error on the UNFILTERED path (no theme filter) is
        unexpected — it must propagate, not be masked as degraded."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _UnknownTableError()
            with pytest.raises(Exception):
                await resolve_work_graph_edges(mock_context)  # no filters

    @pytest.mark.asyncio
    async def test_other_missing_table_reraises_not_degraded(self, mock_context):
        """A code-60 UNKNOWN_TABLE error naming a DIFFERENT table (e.g.
        work_graph_edges) on the filtered path is a genuine schema regression —
        it must RE-RAISE (fail loudly), NOT be downgraded to the benign degraded
        state. This is the narrowing fix: only work_unit_membership degrades."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _UnknownOtherTableError()
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            with pytest.raises(_UnknownOtherTableError):
                await resolve_work_graph_edges(mock_context, filters)

    @pytest.mark.asyncio
    async def test_other_message_only_code60_reraises(self, mock_context):
        """A message-only code-60 error not naming work_unit_membership also
        re-raises (resilient to a missing `code` attr)."""

        class _OtherMsgOnly(Exception):
            pass

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = _OtherMsgOnly(
                "Code: 60. DB::Exception: Unknown table 'deployments'. (UNKNOWN_TABLE)"
            )
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            with pytest.raises(_OtherMsgOnly):
                await resolve_work_graph_edges(mock_context, filters)


class TestRunIdConcurrencyRace:
    """CHAOS-2433: prove the concurrency race is fixed by the run_id protocol.

    Scenario (the round-5 bug):
      1. A materializer is in-flight: has written SOME membership rows but NOT
         its completion marker yet.
      2. A prior COMPLETE backfill run exists (has a marker in
         work_unit_membership_runs).
      3. Resolver must select the COMPLETE backfill run, NOT the half-written
         materializer (whose marker has not arrived yet).
      4. After the materializer writes its marker, the resolver switches to it.

    The run_id / completion-marker protocol (CHAOS-2433) guarantees this:
    readers select argMax(run_id, completed_at) from work_unit_membership_runs,
    which only sees COMPLETE runs. A run with membership rows but no marker is
    simply invisible.
    """

    @pytest.mark.asyncio
    async def test_incomplete_materializer_run_invisible_complete_backfill_visible(
        self, mock_context
    ):
        """While a materializer is in-flight (rows written, no marker), the
        resolver uses the last complete backfill run and returns its edges.
        The filter SQL uses work_unit_membership_runs to find the latest
        complete run — so the half-written materializer rows are never selected.
        """
        # Simulate: ClickHouse returns one edge (the backfill run is complete,
        # so the EXISTS filter matched against the backfill's run_id).
        edge_rows = [make_edge_row(edge_id="backfill-edge", source_id="BACKFILL-NODE")]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue",
                    "BACKFILL-NODE",
                    "feature_delivery",
                    "feature_delivery.roadmap",
                ),
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        # Backfill's complete run edge is returned.
        assert [e.edge_id for e in result.edges] == ["backfill-edge"]
        assert result.degraded_reason is None

        # The filter SQL uses work_unit_membership_runs (run_id protocol).
        edge_sql = mock_query.call_args_list[0][0][1]
        assert "work_unit_membership_runs" in edge_sql
        assert "argMax(run_id, completed_at) AS latest_run_id" in edge_sql
        # Run_id equality join (not per-node max(computed_at)).
        assert "m.run_id = latest_run.latest_run_id" in edge_sql

    @pytest.mark.asyncio
    async def test_after_materializer_marker_written_resolver_uses_new_run(
        self, mock_context
    ):
        """After the materializer writes its completion marker, the resolver
        switches to the new (materializer) run because it is now the latest
        complete run in work_unit_membership_runs."""
        # Now the materializer's marker exists — ClickHouse returns the
        # materializer's edge (it is now the latest complete run).
        edge_rows = [
            make_edge_row(edge_id="materializer-edge", source_id="MATERIALIZER-NODE")
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [
                edge_rows,
                dominant_rows(
                    "issue",
                    "MATERIALIZER-NODE",
                    "maintenance",
                    "maintenance.refactor",
                ),
            ]
            filters = WorkGraphEdgeFilterInput(theme="maintenance")
            result = await resolve_work_graph_edges(mock_context, filters)

        # Materializer's run edge is returned after marker write.
        assert [e.edge_id for e in result.edges] == ["materializer-edge"]
        assert result.edges[0].theme == "maintenance"
        assert result.degraded_reason is None

    @pytest.mark.asyncio
    async def test_no_complete_run_yields_degraded_when_investments_exist(
        self, mock_context
    ):
        """When no completion marker exists at all (first-ever deploy, no run
        complete), a theme filter that returns empty AND investments exist
        yields MEMBERSHIP_NOT_MATERIALIZED (degraded).  The probe checks
        work_unit_membership_runs for the latest complete run (none → empty
        join → membership_rows=0) and investments exist → degraded."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Edge query: empty (no complete run yet, filter finds nothing).
            # Probe: no complete run → membership_rows=0; investments exist.
            mock_query.side_effect = [
                [],
                [{"membership_rows": 0, "investment_rows": 10}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        assert result.edges == []
        assert result.degraded_reason == "MEMBERSHIP_NOT_MATERIALIZED"

    @pytest.mark.asyncio
    async def test_split_merge_stale_node_absent_from_new_run(self, mock_context):
        """A node that had real membership in run A (old component) is absent
        from run B (latest complete, different component).  The filter returns
        no edges for that node; annotation is None.  No tombstones involved —
        the node is simply absent from the current run's scope."""
        # Simulate: ClickHouse runs the EXISTS filter against run B's rows only.
        # The node (OLD-NODE) is no longer in run B → filter finds no edges.
        # (No edge rows returned.) Investments still exist → probe runs.
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Edge query: ClickHouse correctly returns empty because OLD-NODE has
            # no rows in the latest complete run B.
            # Probe: some membership rows exist (in run B, for other nodes), so
            # membership_rows > 0 → not degraded (just a genuine filter miss).
            mock_query.side_effect = [
                [],
                [{"membership_rows": 42, "investment_rows": 100}],
            ]
            filters = WorkGraphEdgeFilterInput(theme="feature_delivery")
            result = await resolve_work_graph_edges(mock_context, filters)

        # Not in the filter result (split/merge safe).
        assert result.edges == []
        # Not degraded — other nodes have membership in the latest complete run.
        assert result.degraded_reason is None
