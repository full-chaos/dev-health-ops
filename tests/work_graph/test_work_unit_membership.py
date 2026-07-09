"""Tests for CHAOS-2429/2430/2433: membership category helpers + materializer.

Covers:
- _lexical_argmax dominant selection + tie-break.
- _membership_categories: multi-membership above the weight threshold, always
  including the argmax (is_dominant=1) even when below threshold, threshold
  boundary (0.2 included, 0.19 excluded unless dominant).
- Materializer (CHAOS-2433 round-3 finding #2): writes work_unit_investments
  ONLY — it NO LONGER writes work_unit_membership rows or the completion marker.
  Membership is written exclusively by the no-LLM projection (backfill.py); see
  tests/test_membership_backfill.py for the per-node/category emission, run_id,
  marker-order, and coverage correctness.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
    WorkUnitMembershipRecord,
    WorkUnitMembershipRunRecord,
    WorkUnitRepoEffortRecord,
)
from dev_health_ops.work_graph.investment.categorize import CategorizationOutcome
from dev_health_ops.work_graph.investment.constants import (
    MEMBERSHIP_WEIGHT_THRESHOLD,
)
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    _lexical_argmax,
    _membership_categories,
    materialize_investments,
)

# ---------------------------------------------------------------------------
# _lexical_argmax unit tests
# ---------------------------------------------------------------------------


def test_lexical_argmax_single_key():
    assert _lexical_argmax({"feature_delivery": 1.0}) == "feature_delivery"


def test_lexical_argmax_clear_winner():
    dist = {"maintenance": 0.1, "feature_delivery": 0.8, "quality": 0.1}
    assert _lexical_argmax(dist) == "feature_delivery"


def test_lexical_argmax_tie_breaks_lexically():
    # "aaa" < "bbb" lexically, so "aaa" wins even with identical scores.
    dist = {"bbb": 1.0, "aaa": 1.0}
    assert _lexical_argmax(dist) == "aaa"


def test_lexical_argmax_empty_returns_unknown():
    assert _lexical_argmax({}) == "unknown"


def test_lexical_argmax_all_zero_tie_breaks_lexically():
    dist = {"z_theme": 0.0, "a_theme": 0.0}
    assert _lexical_argmax(dist) == "a_theme"


# ---------------------------------------------------------------------------
# _membership_categories unit tests
# ---------------------------------------------------------------------------


def test_membership_categories_threshold_constant_is_point_two():
    assert MEMBERSHIP_WEIGHT_THRESHOLD == 0.2


def test_membership_categories_multi_membership_45_40_split():
    """A 45/40/15 split records BOTH the 45% and 40% categories (>= 0.2),
    with the 45% one flagged dominant."""
    dist = {"feature_delivery": 0.45, "maintenance": 0.40, "quality": 0.15}
    rows = _membership_categories(dist)
    by_cat = {c: (w, d) for c, w, d in rows}
    assert "feature_delivery" in by_cat
    assert "maintenance" in by_cat
    # quality (0.15) is below threshold and not dominant → excluded.
    assert "quality" not in by_cat
    assert by_cat["feature_delivery"][1] == 1  # dominant
    assert by_cat["maintenance"][1] == 0  # member, not dominant


def test_membership_categories_threshold_boundary_included():
    """Weight exactly 0.2 is INCLUDED (>= threshold)."""
    dist = {"feature_delivery": 0.8, "maintenance": 0.2}
    cats = {c for c, _, _ in _membership_categories(dist)}
    assert "maintenance" in cats


def test_membership_categories_threshold_boundary_excluded():
    """Weight 0.19 is EXCLUDED (below threshold) when it is not the dominant."""
    dist = {"feature_delivery": 0.81, "maintenance": 0.19}
    cats = {c for c, _, _ in _membership_categories(dist)}
    assert "feature_delivery" in cats
    assert "maintenance" not in cats


def test_membership_categories_below_threshold_dominant_still_emitted():
    """If even the argmax is below threshold, it is STILL emitted (is_dominant=1)
    so every node is findable under at least its dominant."""
    # Spread thin across 6 categories, all below 0.2; argmax is the lexical-min
    # among the joint-max keys.
    dist = {
        "feature_delivery": 0.17,
        "maintenance": 0.17,
        "operational": 0.17,
        "quality": 0.17,
        "risk": 0.16,
        "extra": 0.16,
    }
    rows = _membership_categories(dist)
    dominant_rows = [r for r in rows if r[2] == 1]
    assert len(dominant_rows) == 1
    # The argmax (lexical tie-break among the 0.17 keys) is "feature_delivery".
    assert dominant_rows[0][0] == "feature_delivery"
    assert dominant_rows[0][0] in {c for c, _, _ in rows}


def test_membership_categories_dominant_tie_break_lexical():
    """The dominant flag uses the lexical tie-break for equal top weights."""
    dist = {"bbb": 0.5, "aaa": 0.5}
    rows = _membership_categories(dist)
    dominant = [c for c, _, d in rows if d == 1]
    assert dominant == ["aaa"]


def test_membership_categories_empty_returns_empty():
    assert _membership_categories({}) == []


# ---------------------------------------------------------------------------
# Shared helpers for integration-style tests
# ---------------------------------------------------------------------------


class FakeSink:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.client = object()
        self.investment_rows: list[WorkUnitInvestmentRecord] = []
        self.repo_effort_rows: list[WorkUnitRepoEffortRecord] = []
        self.quote_rows: list[WorkUnitInvestmentEvidenceQuoteRecord] = []
        self.membership_rows: list[WorkUnitMembershipRecord] = []
        self.membership_run_records: list[WorkUnitMembershipRunRecord] = []
        # Track write call order so we can assert marker comes LAST.
        self._write_order: list[str] = []

    def ensure_schema(self) -> None:
        return None

    def write_work_unit_investments(self, rows) -> None:
        self.investment_rows.extend(rows)

    def write_work_unit_repo_effort(self, rows) -> None:
        self.repo_effort_rows.extend(rows)

    def write_work_unit_investment_quotes(self, rows) -> None:
        self.quote_rows.extend(rows)

    def write_work_unit_memberships(self, rows) -> None:
        self.membership_rows.extend(rows)
        self._write_order.append("memberships")

    def write_membership_run(self, record: WorkUnitMembershipRunRecord) -> None:
        self.membership_run_records.append(record)
        self._write_order.append("run_marker")

    def close(self) -> None:
        return None


def _sample_two_node_data(repo_id: str | None = None):
    """A two-node component (issue + commit) useful for membership tests."""
    repo_id = repo_id or str(uuid.uuid4())
    edges = [
        {
            "edge_id": "edge-1",
            "source_type": "issue",
            "source_id": "gh:ISSUE-1",
            "target_type": "commit",
            "target_id": f"{repo_id}@abc123",
            "repo_id": repo_id,
            "confidence": 0.9,
        }
    ]
    work_items = [
        {
            "work_item_id": "gh:ISSUE-1",
            "provider": "github",
            "repo_id": repo_id,
            "title": "Add new API endpoint",
            "description": "Feature: add /users endpoint. " * 20,
            "type": "issue",
            "labels": [],
            "parent_id": "",
            "epic_id": "",
            "created_at": datetime.now(timezone.utc) - timedelta(days=3),
            "updated_at": datetime.now(timezone.utc) - timedelta(days=1),
            "completed_at": datetime.now(timezone.utc) - timedelta(days=1),
        }
    ]
    commits = [
        {
            "repo_id": repo_id,
            "hash": "abc123",
            "message": "Add /users endpoint",
            "author_when": datetime.now(timezone.utc) - timedelta(days=1),
            "committer_when": datetime.now(timezone.utc) - timedelta(days=1),
        }
    ]
    return repo_id, edges, work_items, commits


def _patch_queries(monkeypatch, edges, work_items, commits, repo_id):
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_graph_edges",
        lambda client, repo_ids=None, **kwargs: edges,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_items",
        lambda client, work_item_ids, **kwargs: work_items,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_work_item_active_hours",
        lambda client, work_item_ids, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_pull_requests",
        lambda client, repo_numbers, **kwargs: [],
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commits",
        lambda client, repo_commits, **kwargs: commits,
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_commit_churn",
        lambda client, repo_commits, **kwargs: {f"{repo_id}@abc123": 10.0},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.fetch_parent_titles",
        lambda client, work_item_ids, **kwargs: {},
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.resolve_repo_ids_for_teams",
        lambda client, team_ids, **kwargs: [],
    )


def _make_config(
    repo_id: str, org_id: str = "test-org", *, repo_ids: list[str] | None = None
) -> MaterializeConfig:
    """Build a MaterializeConfig.

    NOTE: org-wide by default (``repo_ids=None``).  CHAOS-2433 finding #2: only
    an org-wide run publishes a completion marker — a repo-scoped run writes its
    rows but NOT the org-wide marker.  These membership/marker tests must run
    org-wide so the marker is published (the patched edge fetch ignores repo_ids
    anyway).  Pass ``repo_ids=[...]`` to exercise the scoped (no-marker) path.
    """
    now = datetime.now(timezone.utc)
    return MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=repo_ids,
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model="test-model",
        org_id=org_id,
    )


# ---------------------------------------------------------------------------
# Materializer integration tests — CHAOS-2433 round-3 finding #2.
#
# The materializer writes work_unit_investments ONLY. Membership rows + the
# completion marker are written EXCLUSIVELY by the no-LLM projection
# (backfill.py); see tests/test_membership_backfill.py for membership emission
# correctness (per-node/category, run_id, marker order, coverage).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_materialize_writes_investments_only_no_membership(monkeypatch):
    """The materializer persists work_unit_investments but writes NO membership
    rows and NO completion marker (unified writer — backfill owns membership)."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={
                "feature_delivery.roadmap": 0.9,
                "operational.support": 0.1,
            },
            evidence_quotes=[],
            uncertainty="",
            status="ok",
            errors=[],
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits, repo_id)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    stats = await materialize_investments(_make_config(repo_id))

    # Investments ARE written, with the categorized distribution.
    assert sink.investment_rows, "materializer must persist work_unit_investments"
    inv = sink.investment_rows[0]
    assert inv.org_id == "test-org"
    assert inv.theme_distribution_json  # rolled-up themes present
    assert inv.subcategory_distribution_json.get("feature_delivery.roadmap") == 0.9

    # NO membership rows and NO marker — the projection owns those now.
    assert sink.membership_rows == [], (
        "materializer must NOT write work_unit_membership rows "
        "(CHAOS-2433 round-3 finding #2)"
    )
    assert sink.membership_run_records == [], (
        "materializer must NOT publish a completion marker (unified writer)"
    )
    assert sink._write_order == [], "no membership/marker writes from materializer"

    # The stats no longer report a memberships count (materializer doesn't emit).
    assert "memberships" not in stats
    assert stats["records"] >= 1


@pytest.mark.asyncio
async def test_materialize_scoped_run_writes_no_membership_or_marker(monkeypatch):
    """A repo-scoped materialize also writes investments only — no membership,
    no marker. (The org-wide projection publishes the full-coverage marker.)"""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"feature_delivery.roadmap": 1.0},
            evidence_quotes=[],
            uncertainty="",
            status="ok",
            errors=[],
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits, repo_id)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    await materialize_investments(_make_config(repo_id, repo_ids=[repo_id]))

    assert sink.investment_rows
    assert sink.membership_rows == []
    assert sink.membership_run_records == []


@pytest.mark.asyncio
async def test_materialize_does_not_call_membership_sink_methods(monkeypatch):
    """Defensive: the materializer never invokes write_work_unit_memberships or
    write_membership_run (they would raise here)."""
    repo_id, edges, work_items, commits = _sample_two_node_data()

    class _NoMembershipSink(FakeSink):
        def write_work_unit_memberships(self, rows):
            raise AssertionError(
                "materializer must not write membership rows (round-3 #2)"
            )

        def write_membership_run(self, record):
            raise AssertionError("materializer must not publish a marker (round-3 #2)")

    sink = _NoMembershipSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"maintenance.refactor": 1.0},
            evidence_quotes=[],
            uncertainty="",
            status="ok",
            errors=[],
        )

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits, repo_id)
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.categorize_text_bundle",
        _fake_categorize,
    )

    # Must NOT raise — proves neither membership method is called.
    await materialize_investments(_make_config(repo_id))
    assert sink.investment_rows
