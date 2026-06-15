"""Tests for CHAOS-2429: work_unit_membership row emission from materializer.

Covers:
- Multi-theme component emits one membership row per node with correct
  lexical-argmax dominant theme and subcategory.
- Deterministic lexical tie-break (smallest key wins).
- Org isolation: rows carry config.org_id.
- Fallback-status rows (llm_task_failed / insufficient_evidence) are still
  stamped with a dominant theme and subcategory derived from the fallback
  distribution.
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
from dev_health_ops.work_graph.investment.materialize import (
    MaterializeConfig,
    _lexical_argmax,
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
async def test_membership_rows_emitted_per_node(monkeypatch):
    """One membership row per node in the component."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={
                "feature_delivery.roadmap": 0.9,
                "operational.reliability": 0.1,
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

    # Component has 2 nodes: issue + commit → 2 membership rows.
    assert len(sink.membership_rows) == 2
    node_keys = {(r.node_type, r.node_id) for r in sink.membership_rows}
    assert ("issue", "gh:ISSUE-1") in node_keys
    assert ("commit", f"{repo_id}@abc123") in node_keys


@pytest.mark.asyncio
async def test_membership_dominant_theme_subcategory_correct(monkeypatch):
    """dominant_theme and dominant_subcategory use lexical argmax."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        # "feature_delivery.roadmap" dominates subcategories → theme "feature_delivery"
        return CategorizationOutcome(
            subcategories={
                "feature_delivery.roadmap": 0.7,
                "maintenance.dependency_updates": 0.2,
                "quality.testing": 0.1,
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

    for row in sink.membership_rows:
        assert row.dominant_theme == "feature_delivery"
        assert row.dominant_subcategory == "feature_delivery.roadmap"


@pytest.mark.asyncio
async def test_membership_lexical_tiebreak_deterministic(monkeypatch):
    """When two subcategories tie on score, the lexically smallest key wins."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        # Tie between "aaa.z_sub" and "bbb.a_sub"; same score → "aaa.z_sub" wins lexically.
        return CategorizationOutcome(
            subcategories={"aaa.z_sub": 0.5, "bbb.a_sub": 0.5},
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

    # "aaa.z_sub" < "bbb.a_sub" → "aaa.z_sub" wins.
    for row in sink.membership_rows:
        assert row.dominant_subcategory == "aaa.z_sub"


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
    """Fallback-status rows (insufficient_evidence) are still stamped with
    dominant theme/subcategory and categorization_status='insufficient_evidence'."""
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

    # Fallback still produces membership rows.
    assert len(sink.membership_rows) > 0
    for row in sink.membership_rows:
        # categorization_status comes from the fallback outcome.
        assert row.categorization_status == "insufficient_evidence"
        # dominant_theme and dominant_subcategory are always non-empty strings.
        assert row.dominant_theme
        assert row.dominant_subcategory


@pytest.mark.asyncio
async def test_membership_work_unit_id_matches_investment(monkeypatch):
    """All membership rows for a component share the same work_unit_id as the
    investment record."""
    repo_id, edges, work_items, commits = _sample_two_node_data()
    sink = FakeSink()

    async def _fake_categorize(bundle, llm_provider, llm_model=None, provider=None):
        return CategorizationOutcome(
            subcategories={"operational.reliability": 1.0},
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
