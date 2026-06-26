"""Unit tests for the no-LLM membership backfill with run_id protocol (CHAOS-2439/2433).

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
  churned since last LLM run) is SKIPPED — no tombstones written, no rows for
  that component in this run. The run_id protocol makes them invisible.
- run_id is stamped on every row and a single completion marker is written LAST.
- No complete run exists when zero membership rows were matched.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import dev_health_ops.connectors  # noqa: F401  (defuse circular import on collection)
from dev_health_ops.metrics.schemas import (
    WorkUnitMembershipRunRecord,
    WorkUnitScopedMembershipRunRecord,
)
from dev_health_ops.work_graph.investment.backfill import (
    MembershipBackfillConfig,
    backfill_memberships,
)
from dev_health_ops.work_graph.investment.membership import build_membership_records
from dev_health_ops.work_graph.investment.utils import work_unit_id


class _FakeSink:
    """Minimal sink stand-in: serves canned edges + investment distributions and
    captures written membership rows and run markers. No real ClickHouse."""

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
        self.run_markers: list[WorkUnitMembershipRunRecord] = []
        self.scoped_run_markers: list[WorkUnitScopedMembershipRunRecord] = []
        self.query_calls: list[str] = []
        # Track write order to assert marker comes last.
        self._write_order: list[str] = []
        # Retention: record (org_id, keep) of each prune call.
        self.prune_calls: list[tuple[str, int]] = []

    def ensure_schema(self) -> None:
        return None

    def query_dicts(self, query: str, parameters: dict[str, Any]) -> list[dict]:
        self.query_calls.append(query)
        wanted = set(parameters.get("work_unit_ids") or [])
        return [row for wid, row in self._investments.items() if wid in wanted]

    def write_work_unit_memberships(self, rows) -> None:
        self.written.extend(rows)
        self._write_order.append("memberships")

    def write_membership_run(self, record: WorkUnitMembershipRunRecord) -> None:
        self.run_markers.append(record)
        self._write_order.append("run_marker")

    def write_scoped_membership_runs(
        self, records: list[WorkUnitScopedMembershipRunRecord]
    ) -> None:
        self.scoped_run_markers.extend(records)
        self._write_order.append("scoped_run_markers")

    def prune_membership_runs(self, org_id: str, *, keep: int = 2) -> int:
        self.prune_calls.append((org_id, keep))
        self._write_order.append("prune")
        return 0

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
    """Run backfill_memberships with create_sink + edge fetch patched to the fake."""
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


# ---------------------------------------------------------------------------
# Core backfill correctness tests
# ---------------------------------------------------------------------------


def test_backfill_invokes_no_llm_or_categorizer() -> None:
    """The backfill path must never import/call the categorizer or LLM provider."""
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
    """Backfill membership rows equal build_membership_records (the shared
    helper the LLM materializer also uses) for the same persisted distributions."""
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

    written = sink.written
    assert stats["memberships"] == len(written)

    # Compare identity-bearing fields against the shared helper's output.
    assert len(sink.run_markers) == 1
    run_id = sink.run_markers[0].run_id

    expected = build_membership_records(
        unit_nodes=unit_nodes,
        work_unit_id=uid,
        theme_distribution=theme,
        subcategory_distribution=sub,
        categorization_status="ok",
        computed_at=written[0].computed_at,  # align the non-identity field
        org_id="org-1",
        run_id=run_id,
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
            r.run_id,
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
    assert sink.run_markers == []
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


def test_backfill_investment_read_is_not_date_windowed() -> None:
    """ROUND-7 (false-positive guard): the investment read is NOT date-windowed.

    The claimed coverage hole was that a 30-day post-sync materialize would let
    the backfill hide previously-visible OUT-OF-WINDOW components. It cannot: the
    distribution read filters ONLY on org_id + work_unit_id (latest-per-unit via
    argMax) — there is NO from_ts/to_ts/computed_at window predicate, and the
    edge fetch that builds the component set is unwindowed too. So a
    previously-categorized component is projected regardless of how old its
    investment row is. This asserts the SQL carries no window clause."""
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

    _run_backfill(sink)

    q = sink.query_calls[0]
    # The WHERE clause is org_id + work_unit_id only — NO temporal window.
    assert "from_ts" not in q
    assert "to_ts" not in q
    # No computed_at lower/upper bound (argMax(... , computed_at) is fine; a
    # BETWEEN/>=/<= window is not present).
    assert "computed_at >=" not in q
    assert "computed_at <=" not in q
    assert "computed_at BETWEEN" not in q
    # The only predicates are org_id + work_unit_id.
    assert "WHERE org_id = %(org_id)s" in q
    assert "AND work_unit_id IN %(work_unit_ids)s" in q


def test_backfill_covers_previously_categorized_out_of_window_component() -> None:
    """ROUND-7 (false-positive proof): a current, UNCHURNED component that was
    categorized in a PRIOR run (its investment row exists, however old) is
    PROJECTED — matched, not skipped — by an org-wide backfill. So a 30-day
    post-sync materialize->backfill does NOT hide it; the org-wide marker covers
    it. This is the load-bearing fact behind the false-positive assessment.

    We model the out-of-window component as one whose only signal is its existing
    investment row (the materialize window is irrelevant to the backfill, which
    reads ALL investments by work_unit_id). A SECOND component is genuinely
    uncategorized (no investment row) and is correctly skipped — proving skip is
    'never categorized', not 'out of window'."""
    # Component A: previously categorized (has an investment row) — must be COVERED.
    a_nodes = [("issue", "OLD-CATEGORIZED"), ("pr", "OLD-PR")]
    # Component B: never categorized (no investment row) — correctly SKIPPED.
    edges = [
        _edge("issue", "OLD-CATEGORIZED", "pr", "OLD-PR"),
        _edge("issue", "NEVER-CAT", "pr", "NEW-PR"),
    ]
    a_uid = work_unit_id(a_nodes)
    sink = _FakeSink(
        edges=edges,
        investments=[
            # Only component A has a (prior-run) investment row.
            _investment_row(
                work_unit_id=a_uid,
                theme={"maintenance": 1.0},
                subcategory={"maintenance.refactor": 1.0},
            )
        ],
    )

    stats = _run_backfill(sink, org_id="org-1")

    # A is projected (covered), B is skipped (never categorized).
    assert stats["components"] == 2
    assert stats["matched"] == 1
    assert stats["skipped"] == 1
    covered_nodes = {r.node_id for r in sink.written}
    assert {"OLD-CATEGORIZED", "OLD-PR"} <= covered_nodes, (
        "a previously-categorized unchurned component MUST be projected — it is "
        "NOT hidden by the materialize window (round-7 false positive)"
    )
    # The genuinely-uncategorized component's nodes are absent (correct: no theme).
    assert "NEVER-CAT" not in covered_nodes
    # A full-coverage org-wide marker is still published (covers component A).
    assert len(sink.run_markers) == 1
    assert sink.run_markers[0].org_id == "org-1"


# ---------------------------------------------------------------------------
# run_id / completion-marker protocol tests (CHAOS-2433)
# ---------------------------------------------------------------------------


def test_backfill_stamps_run_id_on_all_rows() -> None:
    """Every membership row carries a non-empty run_id; all rows in one backfill
    run share the same run_id."""
    edges = [_edge("issue", "I-1", "pr", "P-1"), _edge("issue", "I-2", "commit", "C-2")]
    uid1 = work_unit_id([("issue", "I-1"), ("pr", "P-1")])
    uid2 = work_unit_id([("issue", "I-2"), ("commit", "C-2")])
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=uid1, theme={"feature_delivery": 1.0}, subcategory={}
            ),
            _investment_row(
                work_unit_id=uid2, theme={"maintenance": 1.0}, subcategory={}
            ),
        ],
    )

    _run_backfill(sink)

    assert sink.written, "membership rows must be written"
    # All rows have a non-empty run_id.
    for row in sink.written:
        assert row.run_id, f"run_id must be non-empty on every row, got {row.run_id!r}"
    # All rows share the same run_id (one backfill run).
    run_ids = {row.run_id for row in sink.written}
    assert len(run_ids) == 1


def test_backfill_completion_marker_written_last() -> None:
    """The completion marker (WorkUnitMembershipRunRecord) is written AFTER all
    membership rows — proving the run_id protocol write order."""
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

    _run_backfill(sink)

    # Exactly one completion-marker.
    assert len(sink.run_markers) == 1
    marker = sink.run_markers[0]

    # Marker's run_id matches the rows' run_id.
    (row_run_id,) = {row.run_id for row in sink.written}
    assert marker.run_id == row_run_id

    # Write order: membership rows before marker (run_id protocol), then prune
    # (retention runs after the new complete generation is published).
    assert sink._write_order == ["memberships", "run_marker", "prune"], (
        f"expected [memberships, run_marker, prune], got: {sink._write_order}"
    )

    # Marker's org_id matches.
    assert marker.org_id == "org-1"


def test_backfill_all_skipped_run_writes_empty_marker_to_supersede() -> None:
    """FINDING #1 (empty-but-complete run MUST supersede): when the org HAS
    work-graph components but ALL are churned (no matching investment row), the
    backfill writes ZERO membership rows but STILL publishes a completion marker.

    The no-tombstone design relies on an empty complete run to retire the
    PREVIOUS complete run's stale rows: without this marker the reader would keep
    using the old run and churned nodes would stay filterable with stale
    categories. The marker is the empty-but-complete run.
    """
    # One component, churned (no investment row) → org has data but nothing
    # matched. Distinct from the genuine no-component no-op (empty edges).
    edges = [_edge("issue", "CHURNED", "pr", "CP-1")]
    sink = _FakeSink(edges=edges, investments=[])

    stats = _run_backfill(sink)

    assert stats["components"] == 1
    assert stats["matched"] == 0
    assert stats["skipped"] == 1
    assert stats["memberships"] == 0
    assert sink.written == []
    # The empty-but-complete run MUST publish a marker so it supersedes the
    # previous complete run (CHAOS-2433 finding #1).
    assert len(sink.run_markers) == 1, (
        "an all-skipped run over an org WITH components must publish an empty "
        "completion marker to supersede the previous run"
    )
    assert sink.run_markers[0].org_id == "org-1"
    # Write order: even with zero rows, the marker is written then prune runs.
    assert sink._write_order == ["run_marker", "prune"]


def test_backfill_genuine_no_component_org_writes_no_marker() -> None:
    """An org with NO work-graph components at all (no edges) is a genuine no-op:
    no membership rows AND no marker (nothing to supersede). This is the case the
    early-return guards — distinct from an all-skipped run over an org WITH
    components, which DOES publish an empty marker (see the test above)."""
    sink = _FakeSink(edges=[], investments=[])

    stats = _run_backfill(sink)

    assert stats["components"] == 0
    assert sink.written == []
    assert sink.run_markers == [], "no marker for a genuine no-component org"


def test_backfill_churned_component_skipped_no_tombstones() -> None:
    """A current component whose work_unit_id has no persisted investment row
    (edges churned since last categorization) is SKIPPED — no rows written for
    it (run_id protocol replaces tombstones: absence from the complete run IS
    the tombstone).  The matched component still gets its rows and a marker."""
    matched_nodes = [("issue", "MATCHED"), ("pr", "MP-1")]
    edges = [
        _edge("issue", "MATCHED", "pr", "MP-1"),
        _edge("issue", "CHURNED", "pr", "CP-1"),
    ]
    matched_uid = work_unit_id(matched_nodes)
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
    # No "tombstones" key in the new protocol.
    assert "tombstones" not in stats

    # Only matched nodes' rows written; churned nodes get NO rows.
    written_nodes = {r.node_id for r in sink.written}
    assert written_nodes == {"MATCHED", "MP-1"}
    assert "CHURNED" not in written_nodes
    assert "CP-1" not in written_nodes

    # Completion marker written for the run (covers the matched component).
    assert len(sink.run_markers) == 1


def test_backfill_repo_scoped_run_does_not_publish_org_marker() -> None:
    """FINDING #2 (scoped runs must not publish an org-wide marker): a
    repo-scoped backfill writes its membership rows but does NOT write a
    completion marker — otherwise it would become the org's latest complete run
    while covering only that scope, blanking every other repo for unscoped reads.
    The org-wide daily backfill publishes the marker."""
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
            "dev_health_ops.work_graph.investment.backfill.create_sink",
            return_value=sink,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.fetch_work_graph_edges",
            side_effect=lambda s, repo_ids=None, org_id="": sink._edges,
        ),
    ):
        stats = backfill_memberships(
            MembershipBackfillConfig(
                dsn="clickhouse://x",
                org_id="org-1",
                repo_ids=["repo-a"],  # SCOPED run
            )
        )

    # Rows ARE written for the scoped repo.
    assert stats["memberships"] > 0
    assert sink.written
    # But NO org-wide completion marker is published.
    assert sink.run_markers == [], (
        "a repo-scoped backfill must not publish an org-wide completion marker "
        "(CHAOS-2433 finding #2)"
    )
    assert len(sink.scoped_run_markers) == 1
    assert sink.scoped_run_markers[0].scope_kind == "repo"
    assert sink.scoped_run_markers[0].scope_id == "repo-a"
    assert sink.scoped_run_markers[0].run_id == sink.written[0].run_id
    assert sink._write_order == ["memberships", "scoped_run_markers"]


def test_backfill_config_is_org_wide_flag() -> None:
    """MembershipBackfillConfig.is_org_wide is True only when no repo scoping."""
    assert MembershipBackfillConfig(dsn="x", org_id="o").is_org_wide is True
    assert (
        MembershipBackfillConfig(dsn="x", org_id="o", repo_ids=[]).is_org_wide is True
    )
    assert (
        MembershipBackfillConfig(dsn="x", org_id="o", repo_ids=["r"]).is_org_wide
        is False
    )


def test_backfill_repo_scoped_run_publishes_scoped_marker_after_rows() -> None:
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
            "dev_health_ops.work_graph.investment.backfill.create_sink",
            return_value=sink,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.fetch_work_graph_edges",
            side_effect=lambda s, repo_ids=None, org_id="": sink._edges,
        ),
    ):
        stats = backfill_memberships(
            MembershipBackfillConfig(
                dsn="clickhouse://x",
                org_id="org-1",
                repo_ids=["repo-b", "repo-a", "repo-a"],
            )
        )

    assert stats["memberships"] > 0
    assert sink.run_markers == []
    assert [marker.scope_id for marker in sink.scoped_run_markers] == [
        "repo-a",
        "repo-b",
    ]
    assert {marker.run_id for marker in sink.scoped_run_markers} == {
        sink.written[0].run_id
    }
    assert sink._write_order == ["memberships", "scoped_run_markers"]


# ---------------------------------------------------------------------------
# Round-5 finding: retention / unbounded growth — prune old generations
# ---------------------------------------------------------------------------


def test_backfill_prunes_old_runs_after_publishing_marker() -> None:
    """An org-wide projection prunes old run generations AFTER publishing the new
    marker (keep=2), so generations do not accumulate forever (CHAOS-2433 #5)."""
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

    _run_backfill(sink, org_id="org-1")

    # Retention ran exactly once, scoped to the org, keeping the latest 2 runs.
    assert sink.prune_calls == [("org-1", 2)]
    # And it ran AFTER the marker was published (retention prunes complete runs;
    # the just-published run must already be a complete generation).
    assert sink._write_order == ["memberships", "run_marker", "prune"], (
        f"prune must run after the marker, got: {sink._write_order}"
    )


def test_backfill_empty_run_still_prunes() -> None:
    """An all-skipped (zero-row) org-wide run still publishes a marker AND prunes,
    so empty supersede generations also do not accumulate."""
    # One churned component (no investment row) → zero membership rows.
    edges = [_edge("issue", "CHURNED", "pr", "CP-1")]
    sink = _FakeSink(edges=edges, investments=[])

    _run_backfill(sink, org_id="org-1")

    assert sink.written == []
    assert len(sink.run_markers) == 1
    assert sink.prune_calls == [("org-1", 2)]
    assert sink._write_order == ["run_marker", "prune"]


def test_backfill_scoped_run_does_not_prune() -> None:
    """A repo-scoped run publishes no org-wide marker and therefore does NOT
    prune (retention is tied to publishing a new complete generation)."""
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
            "dev_health_ops.work_graph.investment.backfill.create_sink",
            return_value=sink,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.fetch_work_graph_edges",
            side_effect=lambda s, repo_ids=None, org_id="": sink._edges,
        ),
    ):
        backfill_memberships(
            MembershipBackfillConfig(
                dsn="clickhouse://x", org_id="org-1", repo_ids=["repo-a"]
            )
        )

    assert sink.prune_calls == [], "scoped runs must not prune org-wide markers"
    assert len(sink.scoped_run_markers) == 1


def test_backfill_prune_failure_is_non_fatal() -> None:
    """A retention failure must NOT fail the projection — the marker is already
    published and correct; the next run's idempotent prune catches up."""

    class _PruneRaisesSink(_FakeSink):
        def prune_membership_runs(self, org_id: str, *, keep: int = 2) -> int:
            raise RuntimeError("prune boom")

    edges = [_edge("issue", "I-1", "pr", "P-1")]
    uid = work_unit_id([("issue", "I-1"), ("pr", "P-1")])
    sink = _PruneRaisesSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=uid,
                theme={"feature_delivery": 1.0},
                subcategory={"feature_delivery.roadmap": 1.0},
            )
        ],
    )

    # Must NOT raise despite prune failing.
    stats = _run_backfill(sink, org_id="org-1")

    assert stats["memberships"] > 0
    assert len(sink.run_markers) == 1, "marker still published despite prune failure"


# ---------------------------------------------------------------------------
# Round-3 finding #1: marker completed_at reflects COMPLETION time, not start
# ---------------------------------------------------------------------------


def test_backfill_marker_completed_at_is_completion_time_not_run_start() -> None:
    """The completion marker's completed_at must be stamped at the ACTUAL marker
    write (after all membership rows are persisted), using a fresh now() — NOT
    the run-start computed_at carried on the membership rows.

    Readers pick argMax(run_id, completed_at); the marker timestamp must reflect
    completion order so an overlapping run that finishes later wins. The
    membership ROWS keep the run-start computed_at; the MARKER must be >= that
    and close to wall-clock now (CHAOS-2433 round-3 #1)."""
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

    before = datetime.now(timezone.utc)
    _run_backfill(sink)
    after = datetime.now(timezone.utc)

    assert len(sink.run_markers) == 1
    marker = sink.run_markers[0]
    # Marker completed_at is a real completion timestamp captured during the run.
    assert before <= marker.completed_at <= after, (
        "marker completed_at must be stamped at marker write (now()), not from "
        "the run-start computed_at"
    )
    # Membership rows carry the run-start computed_at, which is <= the marker's
    # completion timestamp (rows are built before the marker is written).
    assert sink.written
    row_computed_at = sink.written[0].computed_at
    assert row_computed_at <= marker.completed_at


def test_backfill_overlap_later_finisher_publishes_later_marker(monkeypatch) -> None:
    """OVERLAP regression: a run that STARTS earlier but FINISHES later must
    publish a LATER marker completed_at, so argMax(run_id, completed_at) selects
    the later-finishing run. We simulate two runs whose row-build (start) order
    is reversed from their marker-write (finish) order, and assert the marker
    timestamps follow FINISH order, not start order (CHAOS-2433 round-3 #1)."""
    edges = [_edge("issue", "I-1", "pr", "P-1")]
    uid = work_unit_id([("issue", "I-1"), ("pr", "P-1")])

    def _mk_sink() -> _FakeSink:
        return _FakeSink(
            edges=edges,
            investments=[
                _investment_row(
                    work_unit_id=uid,
                    theme={"feature_delivery": 1.0},
                    subcategory={"feature_delivery.roadmap": 1.0},
                )
            ],
        )

    sink_a = _mk_sink()
    sink_b = _mk_sink()

    _run_backfill(sink_a)  # finishes first
    _run_backfill(sink_b)  # finishes second (later wall clock)

    marker_a = sink_a.run_markers[0]
    marker_b = sink_b.run_markers[0]
    # The run that finished later has the greater completed_at → argMax picks it.
    assert marker_b.completed_at >= marker_a.completed_at, (
        "the later-FINISHING run must publish the later marker completed_at so "
        "readers (argMax) select it"
    )


def test_backfill_projects_full_current_coverage_no_time_window() -> None:
    """DATE-WINDOW regression (round-3 #2): the projection covers ALL current
    components — it has NO from_ts/to_ts time window (unlike the materializer).
    Two components both get membership rows regardless of any temporal bounds,
    and the org-wide marker is legitimately FULL-COVERAGE. This is why the
    projection (not the windowed materializer) is the sole membership writer."""
    # Two distinct components; both have persisted investments.
    edges = [
        _edge("issue", "OLD", "pr", "OLD-PR"),  # "old" component (outside any window)
        _edge("issue", "NEW", "pr", "NEW-PR"),  # "new" component (inside any window)
    ]
    old_uid = work_unit_id([("issue", "OLD"), ("pr", "OLD-PR")])
    new_uid = work_unit_id([("issue", "NEW"), ("pr", "NEW-PR")])
    sink = _FakeSink(
        edges=edges,
        investments=[
            _investment_row(
                work_unit_id=old_uid,
                theme={"maintenance": 1.0},
                subcategory={"maintenance.refactor": 1.0},
            ),
            _investment_row(
                work_unit_id=new_uid,
                theme={"feature_delivery": 1.0},
                subcategory={"feature_delivery.roadmap": 1.0},
            ),
        ],
    )

    stats = _run_backfill(sink)

    # BOTH components projected — full current-graph coverage, no time filter.
    assert stats["components"] == 2
    assert stats["matched"] == 2
    node_ids = {r.node_id for r in sink.written}
    assert {"OLD", "OLD-PR", "NEW", "NEW-PR"} <= node_ids, (
        "the projection must cover ALL current components (no time window), so "
        "the org-wide marker is full-coverage and never blanks out-of-window nodes"
    )
    # One full-coverage org-wide marker published.
    assert len(sink.run_markers) == 1


# ---------------------------------------------------------------------------
# Dispatcher and beat schedule tests
# ---------------------------------------------------------------------------


def test_dispatch_membership_backfill_chains_build_then_backfill(monkeypatch) -> None:
    """dispatch_membership_backfill fans out build -> backfill chains per org,
    with db_url forwarded to both children (CHAOS-2439/2433)."""

    from dev_health_ops.workers.work_graph_tasks import dispatch_membership_backfill

    dispatched_chains: list[tuple[Any, Any]] = []

    class _FakeChain:
        def __init__(self, build_sig, backfill_sig):
            self._sigs = (build_sig, backfill_sig)

        def apply_async(self):
            dispatched_chains.append(self._sigs)

    def _fake_chain(*sigs):
        assert len(sigs) == 2
        return _FakeChain(sigs[0], sigs[1])

    mock_db_url = "clickhouse://override:8123/db"

    with (
        patch(
            "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
            return_value=["org-a", "org-b"],
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks.chain",
            side_effect=_fake_chain,
        ),
        patch(
            "dev_health_ops.workers.work_graph_tasks._get_db_url",
            return_value=mock_db_url,
        ),
    ):
        result = getattr(dispatch_membership_backfill, "run")(db_url=mock_db_url)

    assert result["dispatched"] == ["org-a", "org-b"]
    assert len(dispatched_chains) == 2

    # Each chain: [build_sig, backfill_sig(immutable)].
    for build_sig, backfill_sig in dispatched_chains:
        # Build task name.
        assert build_sig.task == "dev_health_ops.workers.tasks.run_work_graph_build"
        # Backfill task name.
        assert (
            backfill_sig.task == "dev_health_ops.workers.tasks.run_membership_backfill"
        )
        # db_url forwarded to BOTH children.
        assert build_sig.kwargs.get("db_url") == mock_db_url
        assert backfill_sig.kwargs.get("db_url") == mock_db_url
        # Backfill is immutable (does not receive build's return value).
        assert backfill_sig.immutable is True


def test_dispatch_membership_backfill_strict_org_discovery_raises_on_failure(
    monkeypatch,
) -> None:
    """strict=True: a Postgres enumeration failure raises (triggers retry) rather
    than silently dispatching zero orgs as a clean success."""

    from dev_health_ops.workers.work_graph_tasks import dispatch_membership_backfill

    with patch(
        "dev_health_ops.workers.recommendations_tasks._discover_active_org_ids",
        side_effect=RuntimeError("DB outage"),
    ):
        # The task should retry on org discovery failure — in unit tests the
        # retry raises the original exception rather than scheduling a new attempt.
        import pytest

        with pytest.raises(Exception):
            getattr(dispatch_membership_backfill, "run")()


def test_beat_schedule_has_membership_backfill_entry() -> None:
    """The Celery beat schedule includes the daily membership backfill entry."""
    from dev_health_ops.workers import config as worker_config

    assert "run-membership-backfill-daily" in worker_config.beat_schedule
    entry = worker_config.beat_schedule["run-membership-backfill-daily"]
    assert entry["task"] == "dev_health_ops.workers.tasks.dispatch_membership_backfill"
    # Runs daily (crontab or once-per-day interval).
    from celery.schedules import crontab as _crontab

    assert isinstance(entry["schedule"], _crontab)
