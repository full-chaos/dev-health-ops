"""Tests for the analytics.flowMatrix resolver (CHAOS-1289).

Validates the same-dimension flow matrix path end-to-end:
- compile_flow_matrix produces the expected (nodes, edges) SQL shape
- The edges query references the same column for source and target
- An invalid dimension is rejected upfront (via validate_dimension)
- _execute_sankey_inner correctly handles same-dim rows, prefixing ids with
  the shared dimension (e.g., "team:EngineeringA")
- validate_sub_request_count counts flow_matrix toward the budget
"""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from dev_health_ops.api.graphql.cost import (
    DEFAULT_LIMITS,
    validate_sub_request_count,
)
from dev_health_ops.api.graphql.errors import ValidationError
from dev_health_ops.api.graphql.sql.compiler import (
    FlowMatrixRequest,
    compile_flow_matrix,
)


def _req(dimension: str = "team") -> FlowMatrixRequest:
    return FlowMatrixRequest(
        dimension=dimension,
        measure="count",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        max_nodes=50,
        max_edges=200,
        use_investment=False,
    )


class TestCompileFlowMatrix:
    def test_returns_single_nodes_query_and_single_edges_query(self) -> None:
        nodes_queries, edges_queries = compile_flow_matrix(_req("team"), org_id="org-1")
        assert len(nodes_queries) == 1
        assert len(edges_queries) == 1

    def test_team_edges_use_asymmetric_cooccurrence(self) -> None:
        """TEAM edges come from a self-join on work_item_cycle_times counting
        the SOURCE team's distinct work items per shared scope+day — so edge
        (A, B).value counts A's items, (B, A).value counts B's, producing an
        asymmetric matrix whenever team volumes differ.
        """
        _, edges_queries = compile_flow_matrix(_req("team"), org_id="org-1")
        edge_sql, _params = edges_queries[0]
        assert "'TEAM' AS source_dimension" in edge_sql
        assert "'TEAM' AS target_dimension" in edge_sql
        assert "work_item_cycle_times AS a" in edge_sql
        assert "INNER JOIN work_item_cycle_times AS b" in edge_sql
        assert "a.work_scope_id = b.work_scope_id" in edge_sql
        assert "a.day = b.day" in edge_sql
        # asymmetric: count source-side items only, not product of both
        assert "uniqExact(a.work_item_id) AS value" in edge_sql
        # cross-team only (no self-loops)
        assert "a.team_id != b.team_id" in edge_sql

    def test_team_nodes_use_same_source_as_edges(self) -> None:
        """Node and edge sources must match so node ids and edge endpoints
        stay consistent after the adapter's prefix-strip."""
        nodes_queries, _ = compile_flow_matrix(_req("team"), org_id="org-1")
        nodes_sql, _ = nodes_queries[0]
        assert "work_item_cycle_times" in nodes_sql
        assert "'TEAM' AS dimension" in nodes_sql

    @pytest.mark.parametrize("dim", ["team", "repo", "work_type"])
    def test_compiles_for_each_same_dim_grouping(self, dim: str) -> None:
        nodes_queries, edges_queries = compile_flow_matrix(_req(dim), org_id="org-1")
        nodes_sql, _ = nodes_queries[0]
        edges_sql, _ = edges_queries[0]
        expected_tag = f"'{dim.upper()}' AS"
        assert expected_tag in nodes_sql
        assert expected_tag in edges_sql

    def test_org_scope_enforced_in_params(self) -> None:
        nodes_queries, edges_queries = compile_flow_matrix(_req(), org_id="org-42")
        _, nodes_params = nodes_queries[0]
        _, edges_params = edges_queries[0]
        assert nodes_params["org_id"] == "org-42"
        assert edges_params["org_id"] == "org-42"

    def test_rejects_invalid_dimension(self) -> None:
        with pytest.raises(ValidationError):
            compile_flow_matrix(_req("not_a_dimension"), org_id="org-1")

    def test_edges_query_limit_matches_request(self) -> None:
        req = _req()
        req.max_edges = 137
        _, edges_queries = compile_flow_matrix(req, org_id="org-1")
        _, edges_params = edges_queries[0]
        assert edges_params["max_edges"] == 137


class TestValidateSubRequestCount:
    def test_counts_flow_matrix_toward_total(self) -> None:
        # Should NOT raise — 1 + 1 = 2, well under max.
        validate_sub_request_count(
            timeseries_count=1,
            breakdowns_count=0,
            has_sankey=False,
            has_flow_matrix=True,
        )

    def test_default_flow_matrix_false_is_backward_compatible(self) -> None:
        # Existing callers that don't pass has_flow_matrix still work.
        validate_sub_request_count(
            timeseries_count=0,
            breakdowns_count=0,
            has_sankey=True,
        )


@pytest.mark.asyncio
async def test_flow_matrix_execution_prefixes_same_dim_ids(monkeypatch):
    """End-to-end shape check: _execute_sankey_inner correctly prefixes
    same-dimension entity ids like team:EngineeringA / team:EngineeringB.

    Confirms the "Returns edges where both source and target have
    dimension: team" acceptance criterion from CHAOS-1289.
    """
    from dev_health_ops.api.graphql.resolvers import analytics as mod

    async def fake_query_dicts(client, sql, params):
        # simulate two team nodes and one directional edge
        if "source_dimension" in sql:
            return [
                {
                    "source_dimension": "TEAM",
                    "target_dimension": "TEAM",
                    "source": "EngineeringA",
                    "target": "EngineeringB",
                    "value": 10.0,
                },
                {
                    "source_dimension": "TEAM",
                    "target_dimension": "TEAM",
                    "source": "EngineeringB",
                    "target": "EngineeringA",
                    "value": 3.0,
                },
            ]
        return [
            {"dimension": "TEAM", "node_id": "EngineeringA", "value": 13.0},
            {"dimension": "TEAM", "node_id": "EngineeringB", "value": 13.0},
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    nodes_queries, edges_queries = compile_flow_matrix(_req("team"), org_id="org-1")
    nodes, edges = await mod._execute_sankey_inner(
        client=object(),
        nodes_queries=nodes_queries,
        edges_queries=edges_queries,
    )

    node_ids = {n.id for n in nodes}
    assert node_ids == {"TEAM:EngineeringA", "TEAM:EngineeringB"}

    edge_pairs = {(e.source, e.target, e.value) for e in edges}
    assert edge_pairs == {
        ("TEAM:EngineeringA", "TEAM:EngineeringB", 10.0),
        ("TEAM:EngineeringB", "TEAM:EngineeringA", 3.0),
    }
    # Matrix is ASYMMETRIC — proves directional data is preserved end-to-end.
    forward = next(e for e in edges if e.source == "TEAM:EngineeringA")
    reverse = next(e for e in edges if e.source == "TEAM:EngineeringB")
    assert forward.value != reverse.value


@pytest.mark.asyncio
async def test_flow_matrix_queries_run_concurrently(monkeypatch):
    """flow_matrix shares _execute_sankey_inner, so nodes+edges queries must
    run concurrently (consistent with sankey's existing contract)."""
    from dev_health_ops.api.graphql.resolvers import analytics as mod

    active = 0
    peak = 0

    async def fake_query_dicts(client, sql, params):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            if "source_dimension" in sql:
                return [
                    {
                        "source_dimension": "TEAM",
                        "target_dimension": "TEAM",
                        "source": "a",
                        "target": "b",
                        "value": 1.0,
                    }
                ]
            return [{"dimension": "TEAM", "node_id": "a", "value": 1.0}]
        finally:
            active -= 1

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    nodes_queries, edges_queries = compile_flow_matrix(_req(), org_id="org-1")
    await mod._execute_sankey_inner(
        client=object(),
        nodes_queries=nodes_queries,
        edges_queries=edges_queries,
    )

    assert peak >= 2, f"Expected nodes + edges queries in flight; saw peak={peak}"


def test_default_limits_admit_flow_matrix() -> None:
    """Sanity: the flow matrix's default max_nodes/max_edges must be within
    the global cost ceilings (otherwise every request would be rejected)."""
    req = _req()
    assert req.max_nodes <= DEFAULT_LIMITS.max_sankey_nodes
    assert req.max_edges <= DEFAULT_LIMITS.max_sankey_edges
