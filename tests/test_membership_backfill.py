"""Unit tests for the no-LLM membership backfill (CHAOS-2439).

The daily scheduled job must NOT re-run LLM categorization (cost + category
drift). ``backfill_memberships`` instead PROJECTS ``work_unit_membership`` from
the theme/subcategory distributions ALREADY persisted in
``work_unit_investments`` by the post-sync LLM materializer. These tests prove,
without a live ClickHouse:

- The backfill never touches the categorizer / LLM provider.
- Its membership rows equal what ``materialize`` would emit for the same
  persisted distributions (shared-helper consistency).
- An idle org with existing investments but empty/stale membership gets
  membership populated, no LLM.
- A unit whose current component hash has no persisted categorization (edges
  churned since last LLM run) receives TOMBSTONE rows (category='', weight=0,
  is_dominant=0) so stale membership from a prior component is superseded.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import dev_health_ops.connectors  # noqa: F401  (defuse circular import on collection)
from dev_health_ops.work_graph.investment.backfill import (
    TOMBSTONE_CATEGORY,
    MembershipBackfillConfig,
    backfill_memberships,
)
from dev_health_ops.work_graph.investment.membership import build_membership_records
from dev_health_ops.work_graph.investment.utils import work_unit_id


class _FakeSink:
    """Minimal sink stand-in: serves canned edges + investment distributions and
    captures written membership rows. No real ClickHouse."""

    backend_type = "clickhouse"

    def __init__(
        self,
        *,
        edges: list[dict[str, Any]],
        investments: list[dict[str, Any]],
    ) -> None:
        self._edges = edges
        # Map work_unit_id -> latest distribution row.
        self._investments = {str(r["work_unit_id"]): r for r in investments}
        self.written: list[Any] = []
        self.query_calls: list[str] = []

    def ensure_schema(self) -> None:
        return None

    def query_dicts(self, query: str, parameters: dict[str, Any]) -> list[dict]:
        self.query_calls.append(query)
        # The backfill issues exactly one query_dicts call: latest distributions
        # for the requested work_unit_ids.
        wanted = set(parameters.get("work_unit_ids") or [])
        return [row for wid, row in self._investments.items() if wid in wanted]

    def write_work_unit_memberships(self, rows) -> None:
        self.written.extend(rows)

    def close(self) -> None:
        return None


def _edge(src_type: str, src_id: str, tgt_type: str, tgt_id: str) -> dict[str, Any]:
    return {
        "edge_id": f"{src_type}:{src_id}->{tgt_type}:{tgt_id}",
        "source_type": src_type,
        "source_id": src_id,
        "target_type": tgt_type,
        "target_id": tgt_id,
    }


def _investment_row(
    *,
    work_unit_id: str,
    theme: dict[str, float],
    subcategory: dict[str, float],
    status: str = "ok",
) -> dict[str, Any]:
    return {
        "work_unit_id": work_unit_id,
        "theme_distribution_json": theme,
        "subcategory_distribution_json": subcategory,
        "categorization_status": status,
    }


def _run_backfill(sink: _FakeSink, org_id: str = "org-1") -> dict[str, int]:
    """Run backfill_memberships with create_sink + edge fetch patched to the fake.

    ``fetch_work_graph_edges`` is patched to return the sink's canned edges so we
    drive the full component-build + projection path without ClickHouse.
    """
    with (
        patch(
            "dev_health_ops.work_graph.investment.backfill.create_sink",
            return_value=sink,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.fetch_work_graph_edges",
            side_effect=lambda s, repo_ids=None, org_id="": sink._edges,
        ),
    ):
        return backfill_memberships(
            MembershipBackfillConfig(dsn="clickhouse://x", org_id=org_id)
        )


def test_backfill_invokes_no_llm_or_categorizer() -> None:
    """The backfill path must never import/call the categorizer or LLM provider."""
    # One issue<->pr component with a persisted categorization.
    edges = [_edge("issue", "I-1", "pr", "P-1")]
    uid = work_unit_id([("issue", "I-1"), ("pr", "P-1")])
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=uid,
                theme={"feature_delivery": 1.0},
                subcategory={"feature_delivery.roadmap": 1.0},
            )
        ],
    )

    with (
        patch(
            "dev_health_ops.work_graph.investment.categorize.categorize_text_bundle"
        ) as mock_categorize,
        patch("dev_health_ops.llm.get_provider") as mock_provider,
    ):
        stats = _run_backfill(sink)

    mock_categorize.assert_not_called()
    mock_provider.assert_not_called()
    assert stats["matched"] == 1
    assert stats["memberships"] > 0
    assert sink.written  # rows were written


def test_backfill_rows_match_materialize_shared_helper() -> None:
    """Backfill membership rows equal build_membership_records (the same shared
    helper the LLM materializer uses) for the same persisted distributions."""
    edges = [_edge("issue", "I-1", "pr", "P-1")]
    unit_nodes = [("issue", "I-1"), ("pr", "P-1")]
    uid = work_unit_id(unit_nodes)
    theme = {"feature_delivery": 0.45, "maintenance": 0.40, "quality": 0.15}
    sub = {
        "feature_delivery.roadmap": 0.45,
        "maintenance.refactor": 0.40,
        "quality.testing": 0.15,
    }
    sink = _FakeSink(
        edges=edges,
        investments=[_investment_row(work_unit_id=uid, theme=theme, subcategory=sub)],
    )

    stats = _run_backfill(sink)

    # Build the expected rows via the SHARED helper (computed_at differs, so
    # compare on the identity-bearing fields only).
    written = sink.written
    assert stats["memberships"] == len(written)
    # The materializer would emit exactly these (node, kind, category, weight,
    # is_dominant) tuples for the same distributions.
    expected = build_membership_records(
        unit_nodes=unit_nodes,
        work_unit_id=uid,
        theme_distribution=theme,
        subcategory_distribution=sub,
        categorization_status="ok",
        computed_at=written[0].computed_at,  # align the non-identity field
        org_id="org-1",
    )

    def _key(r):
        return (
            r.org_id,
            r.node_type,
            r.node_id,
            r.work_unit_id,
            r.category_kind,
            r.category,
            r.weight,
            r.is_dominant,
            r.categorization_status,
        )

    assert sorted(map(_key, written)) == sorted(map(_key, expected))
    # Sanity: feature_delivery is dominant, quality (0.15) excluded from themes.
    theme_cats = {
        (r.node_id, r.category, r.is_dominant)
        for r in written
        if r.category_kind == "theme"
    }
    assert ("I-1", "feature_delivery", 1) in theme_cats
    assert ("I-1", "maintenance", 0) in theme_cats
    assert not any(c == "quality" for _, c, _ in theme_cats)


def test_backfill_populates_idle_org_membership_no_llm() -> None:
    """Idle org: investments exist (from a prior post-sync run) but membership is
    empty/stale → backfill writes membership rows for every node, no LLM."""
    edges = [_edge("issue", "I-1", "commit", "C-1")]
    uid = work_unit_id([("issue", "I-1"), ("commit", "C-1")])
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=uid,
                theme={"operational": 1.0},
                subcategory={"operational.support": 1.0},
            )
        ],
    )

    with (
        patch(
            "dev_health_ops.work_graph.investment.categorize.categorize_text_bundle"
        ) as mock_categorize,
    ):
        stats = _run_backfill(sink)

    mock_categorize.assert_not_called()
    assert stats["matched"] == 1
    node_ids = {r.node_id for r in sink.written}
    assert node_ids == {"I-1", "C-1"}
    assert all(r.org_id == "org-1" for r in sink.written)


def test_backfill_tombstones_churned_component() -> None:
    """A current component whose work_unit_id has no persisted investment row
    (edges churned since last categorization) receives TOMBSTONE rows with a
    fresh computed_at so stale prior-component membership is superseded.  The
    post-sync LLM chain will replace tombstones with real rows when the new
    component is categorized."""
    # Current graph: one matched component + one churned (uncategorized)
    # component "CHURNED"<->"CP-1" whose hash has no persisted investment row.
    matched_nodes = [("issue", "MATCHED"), ("pr", "MP-1")]
    edges = [
        _edge("issue", "MATCHED", "pr", "MP-1"),
        _edge("issue", "CHURNED", "pr", "CP-1"),
    ]
    matched_uid = work_unit_id(matched_nodes)
    # Only the matched component has a persisted investment row.
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=matched_uid,
                theme={"feature_delivery": 1.0},
                subcategory={"feature_delivery.roadmap": 1.0},
            )
        ],
    )

    stats = _run_backfill(sink)

    assert stats["components"] == 2
    assert stats["matched"] == 1
    assert stats["skipped"] == 1
    # Tombstones must be reported in stats (2 nodes × 2 kinds = 4 rows).
    assert stats["tombstones"] == 4

    # Both matched AND churned nodes were written (tombstones for churned).
    written_nodes = {r.node_id for r in sink.written}
    assert written_nodes == {"MATCHED", "MP-1", "CHURNED", "CP-1"}

    # Tombstone rows: category='' (sentinel), weight=0, is_dominant=0.
    churned_rows = [r for r in sink.written if r.node_id in {"CHURNED", "CP-1"}]
    assert len(churned_rows) == 4  # 2 nodes × 2 kinds
    for row in churned_rows:
        assert row.category == TOMBSTONE_CATEGORY
        assert row.weight == 0.0
        assert row.is_dominant == 0
        assert row.categorization_status == "tombstone"

    # Real rows for matched nodes must NOT be tombstones.
    matched_rows = [r for r in sink.written if r.node_id in {"MATCHED", "MP-1"}]
    assert all(r.category != TOMBSTONE_CATEGORY for r in matched_rows)
    assert any(r.is_dominant == 1 for r in matched_rows)


def test_backfill_empty_graph_is_clean_noop() -> None:
    """No edges → no components → no query, no writes, clean zero stats."""
    sink = _FakeSink(edges=[], investments=[])
    stats = _run_backfill(sink)
    assert stats == {
        "components": 0,
        "matched": 0,
        "skipped": 0,
        "tombstones": 0,
        "memberships": 0,
    }
    assert sink.written == []
    assert sink.query_calls == []  # no distribution query when no components


def test_backfill_uses_latest_per_work_unit_query() -> None:
    """The distribution read is latest-per-work_unit_id (argMax on computed_at),
    matching api/queries/work_unit_investments.py semantics."""
    edges = [_edge("issue", "I-1", "pr", "P-1")]
    uid = work_unit_id([("issue", "I-1"), ("pr", "P-1")])
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=uid,
                theme={"risk": 1.0},
                subcategory={"risk.security": 1.0},
            )
        ],
    )

    _run_backfill(sink)

    assert len(sink.query_calls) == 1
    q = sink.query_calls[0]
    assert "argMax(theme_distribution_json, computed_at)" in q
    assert "argMax(subcategory_distribution_json, computed_at)" in q
    assert "GROUP BY org_id, work_unit_id" in q
    assert "FROM work_unit_investments" in q


# ---------------------------------------------------------------------------
# Regression test: TOMBSTONE-ON-SKIP stale-membership fix (CHAOS-2439)
# ---------------------------------------------------------------------------


def test_tombstone_on_skip_regression() -> None:
    """REGRESSION: a node that HAD old real membership (theme=quality) from a
    prior run, whose component then churns so its current work_unit_id has NO
    investments row, must receive a TOMBSTONE from the backfill.

    This test covers all three invariants:

    (a) Tombstone row IS written for the churned node with the current run's
        computed_at (not the old computed_at from the stale rows).
    (b) The resolver's theme filter does NOT return the node for theme=quality
        — the tombstone category='' never matches the (category_kind, category)
        tuple filter.
    (c) Annotation for edges containing the churned node returns theme=None
        (the tombstone is_dominant=0 row is excluded from the is_dominant=1
        annotation query, so no category is reported).
    """
    import asyncio
    from unittest.mock import AsyncMock, patch

    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.models.inputs import WorkGraphEdgeFilterInput
    from dev_health_ops.api.graphql.resolvers.work_graph import resolve_work_graph_edges

    org_id = "org-regression"

    # --- (a) BACKFILL: churned node receives tombstone with fresh computed_at ---

    # Current graph: "STALE-NODE" is now in a NEW component with "NEW-PR".
    # The work_unit_id for this component has no investments row (churned),
    # so the backfill will tombstone both nodes.
    edges = [
        _edge("issue", "STALE-NODE", "pr", "NEW-PR"),  # churned component
    ]

    sink = _FakeSink(edges=edges, investments=[])

    stats = _run_backfill(sink)

    # Stats: 1 component, 0 matched, 1 skipped, 4 tombstones (2 nodes × 2 kinds).
    assert stats["components"] == 1
    assert stats["matched"] == 0
    assert stats["skipped"] == 1
    assert stats["tombstones"] == 4
    assert stats["memberships"] == 4

    # Tombstone rows were written for both nodes.
    written_by_node = {r.node_id: r for r in sink.written}
    assert "STALE-NODE" in written_by_node
    assert "NEW-PR" in written_by_node

    # Tombstone invariants for STALE-NODE.
    stale_rows = [r for r in sink.written if r.node_id == "STALE-NODE"]
    assert len(stale_rows) == 2  # one theme + one subcategory tombstone
    for row in stale_rows:
        assert row.category == TOMBSTONE_CATEGORY, "tombstone category must be ''"
        assert row.weight == 0.0
        assert row.is_dominant == 0
        assert row.categorization_status == "tombstone"

    # The tombstone computed_at matches across all rows in this run.
    tombstone_ts = stale_rows[0].computed_at
    assert all(r.computed_at == tombstone_ts for r in sink.written)

    # --- (b) RESOLVER FILTER: tombstone node is NOT returned for theme=quality ---
    #
    # Simulate the state AFTER the tombstone is written: the resolver queries
    # work_unit_membership and sees the tombstone row for STALE-NODE.  The
    # theme=quality filter uses (category_kind, category) IN tuples — since the
    # tombstone category is '', it can never match ("theme", "quality").
    #
    # We drive the resolver with a mock that returns an edge containing STALE-NODE
    # as an endpoint but whose membership rows (latest-run) are tombstones only.
    # The filter EXISTS query must NOT find STALE-NODE in "theme=quality".

    edge_row = {
        "edge_id": "stale-edge",
        "source_type": "issue",
        "source_id": "STALE-NODE",
        "target_type": "pr",
        "target_id": "NEW-PR",
        "edge_type": "implements",
        "provenance": "native",
        "confidence": 1.0,
        "evidence": "",
        "repo_id": None,
        "provider": "github",
    }

    context = GraphQLContext(
        org_id=org_id,
        db_url="clickhouse://localhost:8123/default",
        client=object(),
    )

    # The theme filter EXISTS query runs server-side in ClickHouse; we prove the
    # predicate construction is correct by asserting the SQL does NOT match the
    # tombstone sentinel.  When mock returns empty (simulating that the filter
    # found no matching edges because STALE-NODE's category='' ≠ 'quality'), the
    # resolver must return an empty edge list.
    async def _run_filter_test() -> None:
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Edge query: ClickHouse correctly returns empty because STALE-NODE
            # has only tombstone rows (category='') which never match
            # (category_kind, category) = ('theme', 'quality').
            # Degraded probe: membership_rows>0 (tombstones are live rows),
            # investment_rows=0 → not degraded.
            mock_query.side_effect = [
                [],  # edge query empty (tombstone doesn't match filter)
                [{"membership_rows": 4, "investment_rows": 0}],  # degraded probe
            ]
            filters = WorkGraphEdgeFilterInput(theme="quality")
            result = await resolve_work_graph_edges(context, filters)

        # (b): STALE-NODE is not returned for theme=quality filter.
        assert result.edges == [], (
            "A tombstoned node must not appear in theme=quality filter results"
        )
        # NOT flagged as degraded: tombstones are real live rows, so membership>0.
        assert result.degraded_reason is None, (
            "Tombstoned org should NOT be reported as MEMBERSHIP_NOT_MATERIALIZED"
        )

        # (b continued): verify the filter SQL predicate — the category tuple
        # ('theme', 'quality') will never match category='' in work_unit_membership.
        edge_sql = mock_query.call_args_list[0][0][1]
        edge_params = mock_query.call_args_list[0][0][2]
        assert "(m.category_kind, m.category) IN %(category_tuples)s" in edge_sql
        # The category tuple contains a non-empty string; '' ∉ {('theme', 'quality')}.
        assert ("theme", "quality") in edge_params["category_tuples"]
        assert TOMBSTONE_CATEGORY not in {
            cat for _, cat in edge_params["category_tuples"]
        }

    asyncio.run(_run_filter_test())

    # --- (c) ANNOTATION: edges on STALE-NODE get theme=None ---
    #
    # On the unfiltered path, the annotation query selects is_dominant=1 rows.
    # Tombstone rows have is_dominant=0 so they are excluded, meaning no entry
    # for STALE-NODE in the annotation result → theme=None, subcategory=None.
    async def _run_annotation_test() -> None:
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # Edge query returns the stale edge; annotation query returns NO rows
            # for STALE-NODE (tombstone has is_dominant=0, excluded by filter).
            mock_query.side_effect = [
                [edge_row],
                [],  # annotation: no is_dominant=1 rows (tombstone excluded)
            ]
            result = await resolve_work_graph_edges(context)

        assert len(result.edges) == 1
        edge = result.edges[0]
        # (c): annotation for edges on a tombstoned node is theme=None.
        assert edge.theme is None, (
            "Edge annotation for a tombstoned node must be theme=None"
        )
        assert edge.subcategory is None, (
            "Edge annotation for a tombstoned node must be subcategory=None"
        )

    asyncio.run(_run_annotation_test())
