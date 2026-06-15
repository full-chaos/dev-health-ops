"""Tests for CHAOS-2429/2430: work_unit_membership multi-membership emission.

Covers:
- _lexical_argmax dominant selection + tie-break.
- _membership_categories: multi-membership above the weight threshold, always
  including the argmax (is_dominant=1) even when below threshold, threshold
  boundary (0.2 included, 0.19 excluded unless dominant).
- Materializer emits one row per (node, category) for theme and subcategory
  kinds; 45/40 split is recorded under BOTH themes; is_dominant correctness.
- Org isolation; fallback-status rows still stamped; work_unit_id consistency.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.schemas import (
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
    WorkUnitMembershipRecord,
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
        self.quote_rows: list[WorkUnitInvestmentEvidenceQuoteRecord] = []
        self.membership_rows: list[WorkUnitMembershipRecord] = []

    def ensure_schema(self) -> None:
        return None

    def write_work_unit_investments(self, rows) -> None:
        self.investment_rows.extend(rows)

    def write_work_unit_investment_quotes(self, rows) -> None:
        self.quote_rows.extend(rows)

    def write_work_unit_memberships(self, rows) -> None:
        self.membership_rows.extend(rows)

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


def _make_config(repo_id: str, org_id: str = "test-org") -> MaterializeConfig:
    now = datetime.now(timezone.utc)
    return MaterializeConfig(
        dsn="clickhouse://localhost:8123/default",
        from_ts=now - timedelta(days=5),
        to_ts=now,
        repo_ids=[repo_id],
        llm_provider="mock",
        persist_evidence_snippets=False,
        llm_model="test-model",
        org_id=org_id,
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_membership_rows_emitted_per_node_and_category(monkeypatch):
    """Rows are emitted per (node, category) across both kinds.

    subcategory dist: feature_delivery.roadmap=0.9, operational.support=0.1
    → rolled-up themes: feature_delivery=0.9, operational=0.1.
    Theme rows kept: feature_delivery (>=0.2 + dominant). operational (0.1) is
    below threshold and not dominant → excluded.
    Subcategory rows kept: feature_delivery.roadmap (dominant). operational.support 0.1
    excluded. So 2 category rows per node, x2 nodes = 4 rows.
    """
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

    await materialize_investments(_make_config(repo_id))

    node_keys = {(r.node_type, r.node_id) for r in sink.membership_rows}
    assert ("issue", "gh:ISSUE-1") in node_keys
    assert ("commit", f"{repo_id}@abc123") in node_keys

    # Per node: 1 theme row (feature_delivery) + 1 subcategory row.
    issue_rows = [r for r in sink.membership_rows if r.node_id == "gh:ISSUE-1"]
    theme_rows = [r for r in issue_rows if r.category_kind == "theme"]
    sub_rows = [r for r in issue_rows if r.category_kind == "subcategory"]
    assert {r.category for r in theme_rows} == {"feature_delivery"}
    assert {r.category for r in sub_rows} == {"feature_delivery.roadmap"}
    assert all(r.is_dominant == 1 for r in issue_rows)


@pytest.mark.asyncio
async def test_membership_45_40_split_findable_under_both_themes(monkeypatch):
    """A 45%-feature / 40%-maintenance unit records BOTH theme rows so it is
    findable under either theme (the core multi-membership requirement)."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        # Subcategories roll up to themes 45% feature / 40% maintenance / 15% quality.
        return CategorizationOutcome(
            subcategories={
                "feature_delivery.roadmap": 0.45,
                "maintenance.refactor": 0.40,
                "quality.testing": 0.15,
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

    await materialize_investments(_make_config(repo_id))

    issue_theme_rows = [
        r
        for r in sink.membership_rows
        if r.node_id == "gh:ISSUE-1" and r.category_kind == "theme"
    ]
    theme_cats = {r.category for r in issue_theme_rows}
    # Both feature_delivery (0.45) and maintenance (0.40) recorded; quality (0.15) not.
    assert "feature_delivery" in theme_cats
    assert "maintenance" in theme_cats
    assert "quality" not in theme_cats
    # feature_delivery (0.45) is the dominant.
    dominant = {r.category for r in issue_theme_rows if r.is_dominant == 1}
    assert dominant == {"feature_delivery"}


@pytest.mark.asyncio
async def test_membership_dominant_tie_break_lexical(monkeypatch):
    """When two themes tie on rolled-up weight, the lexically smallest is dominant."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        # Two subcategories under different themes, equal weight → theme tie.
        return CategorizationOutcome(
            subcategories={
                "maintenance.refactor": 0.5,
                "feature_delivery.roadmap": 0.5,
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

    await materialize_investments(_make_config(repo_id))

    theme_rows = [
        r
        for r in sink.membership_rows
        if r.node_id == "gh:ISSUE-1" and r.category_kind == "theme"
    ]
    dominant = {r.category for r in theme_rows if r.is_dominant == 1}
    # "feature_delivery" < "maintenance" lexically → it is the dominant.
    assert dominant == {"feature_delivery"}


@pytest.mark.asyncio
async def test_membership_org_isolation(monkeypatch):
    """Membership rows carry config.org_id."""
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

    await materialize_investments(_make_config(repo_id, org_id="acme-corp"))

    assert len(sink.membership_rows) > 0
    assert all(r.org_id == "acme-corp" for r in sink.membership_rows)


@pytest.mark.asyncio
async def test_membership_fallback_status_still_stamped(monkeypatch):
    """Fallback-status rows (insufficient_evidence) are still emitted with a
    dominant category and categorization_status='insufficient_evidence'."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    # Make description too short to pass MIN_EVIDENCE_CHARS → triggers fallback path.
    work_items[0]["description"] = "short"
    sink = FakeSink()

    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.materialize.create_sink", lambda dsn: sink
    )
    _patch_queries(monkeypatch, edges, work_items, commits, repo_id)
    # categorize_text_bundle should NOT be called for fallback; no patch needed.

    await materialize_investments(_make_config(repo_id))

    assert len(sink.membership_rows) > 0
    for row in sink.membership_rows:
        assert row.categorization_status == "insufficient_evidence"
        assert row.category  # non-empty category string
        assert row.category_kind in {"theme", "subcategory"}
    # Every node has at least one dominant theme and one dominant subcategory.
    for node_id in ("gh:ISSUE-1", f"{repo_id}@abc123"):
        node_rows = [r for r in sink.membership_rows if r.node_id == node_id]
        dom_theme = [
            r for r in node_rows if r.category_kind == "theme" and r.is_dominant == 1
        ]
        dom_sub = [
            r
            for r in node_rows
            if r.category_kind == "subcategory" and r.is_dominant == 1
        ]
        assert len(dom_theme) == 1
        assert len(dom_sub) == 1


@pytest.mark.asyncio
async def test_membership_work_unit_id_matches_investment(monkeypatch):
    """All membership rows for a component share the investment record's work_unit_id."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"operational.support": 1.0},
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

    await materialize_investments(_make_config(repo_id))

    assert sink.investment_rows
    investment_unit_id = sink.investment_rows[0].work_unit_id
    for row in sink.membership_rows:
        assert row.work_unit_id == investment_unit_id
