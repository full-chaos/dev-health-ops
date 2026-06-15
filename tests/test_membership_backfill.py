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

from typing import Any
from unittest.mock import patch

import dev_health_ops.connectors  # noqa: F401  (defuse circular import on collection)
from dev_health_ops.metrics.schemas import WorkUnitMembershipRunRecord
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
        self.query_calls: list[str] = []
        # Track write order to assert marker comes last.
        self._write_order: list[str] = []

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

    # Write order: membership rows before marker (run_id protocol).
    assert sink._write_order == ["memberships", "run_marker"], (
        f"expected [memberships, run_marker], got: {sink._write_order}"
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
    # Write order: even with zero rows, only the marker is written.
    assert sink._write_order == ["run_marker"]


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
    assert sink._write_order == ["memberships"]


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
        result = dispatch_membership_backfill.run(db_url=mock_db_url)

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
            dispatch_membership_backfill.run()


def test_beat_schedule_has_membership_backfill_entry() -> None:
    """The Celery beat schedule includes the daily membership backfill entry."""
    from dev_health_ops.workers import config as worker_config

    assert "run-membership-backfill-daily" in worker_config.beat_schedule
    entry = worker_config.beat_schedule["run-membership-backfill-daily"]
    assert entry["task"] == "dev_health_ops.workers.tasks.dispatch_membership_backfill"
    # Runs daily (crontab or once-per-day interval).
    from celery.schedules import crontab as _crontab

    assert isinstance(entry["schedule"], _crontab)
