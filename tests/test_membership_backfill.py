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
  churned since last LLM run) is skipped, not invented.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import dev_health_ops.connectors  # noqa: F401  (defuse circular import on collection)
from dev_health_ops.work_graph.investment.backfill import (
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


def test_backfill_skips_churned_component_without_categorization() -> None:
    """A current component whose work_unit_id has no persisted investment row
    (edges churned since last categorization) is SKIPPED — not invented. The
    post-sync LLM chain covers those."""
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
    # Only the matched component's nodes get membership rows.
    written_nodes = {r.node_id for r in sink.written}
    assert written_nodes == {"MATCHED", "MP-1"}
    assert "CHURNED" not in written_nodes
    assert "CP-1" not in written_nodes


def test_backfill_empty_graph_is_clean_noop() -> None:
    """No edges → no components → no query, no writes, clean zero stats."""
    sink = _FakeSink(edges=[], investments=[])
    stats = _run_backfill(sink)
    assert stats == {
        "components": 0,
        "matched": 0,
        "skipped": 0,
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
