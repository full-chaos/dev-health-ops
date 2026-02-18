from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.work_graph.investment.constants import MIN_EVIDENCE_CHARS
from dev_health_ops.work_graph.investment.evidence import (
    _ensure_utc,
    build_text_bundle,
    compute_evidence_quality,
    compute_time_bounds,
    evidence_quality_band_for_bundle,
)
from dev_health_ops.work_graph.investment import queries as q


def test_min_evidence_chars_constant():
    assert MIN_EVIDENCE_CHARS == 300


def test_ensure_utc_parses_and_normalizes_values():
    parsed = _ensure_utc("2026-02-18T10:00:00Z")
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc

    naive = _ensure_utc(datetime(2026, 2, 18, 10, 0))
    assert naive is not None
    assert naive.tzinfo == timezone.utc

    assert _ensure_utc("not-a-date") is None


def test_compute_time_bounds_across_issue_pr_commit_nodes():
    bounds = compute_time_bounds(
        nodes=[("issue", "I-1"), ("pr", "PR-1"), ("commit", "C-1")],
        work_item_map={
            "I-1": {
                "created_at": datetime(2026, 2, 10, tzinfo=timezone.utc),
                "completed_at": datetime(2026, 2, 15, tzinfo=timezone.utc),
            }
        },
        pr_map={
            "PR-1": {
                "created_at": datetime(2026, 2, 11, tzinfo=timezone.utc),
                "merged_at": datetime(2026, 2, 16, tzinfo=timezone.utc),
            }
        },
        commit_map={
            "C-1": {
                "author_when": datetime(2026, 2, 12, tzinfo=timezone.utc),
            }
        },
    )

    assert bounds is not None
    assert bounds.start == datetime(2026, 2, 10, tzinfo=timezone.utc)
    assert bounds.end == datetime(2026, 2, 16, tzinfo=timezone.utc)


def test_build_text_bundle_and_quality_score():
    bundle = build_text_bundle(
        issue_ids=["ISS-1"],
        pr_ids=["PR-1"],
        commit_ids=["abc123"],
        work_item_map={
            "ISS-1": {
                "title": "Feature work",
                "description": "Implement endpoint and tests",
                "type": "Story",
                "labels": ["feature", "api"],
                "parent_id": "P-1",
                "epic_id": "E-1",
            }
        },
        pr_map={"PR-1": {"title": "Add endpoint", "body": "Includes tests"}},
        commit_map={"abc123": {"message": "feat(api): add endpoint\n\nBody"}},
        parent_titles={"P-1": "Parent title"},
        epic_titles={"E-1": "Epic title"},
        work_unit_id="WU-1",
    )

    assert "[issue] ISS-1" in bundle.source_block
    assert "[pr] PR-1" in bundle.source_block
    assert "[commit] abc123" in bundle.source_block
    assert bundle.text_source_count == 3
    assert bundle.text_char_count > 0

    quality = compute_evidence_quality(
        text_bundle=bundle,
        nodes_count=3,
        edges=[{"confidence": 0.8}, {"confidence": 0.6}],
    )
    assert 0.0 <= quality <= 1.0
    assert evidence_quality_band_for_bundle(value=quality) in {
        "very_low",
        "low",
        "moderate",
        "high",
    }


def test_queries_fetch_work_items_empty_short_circuit():
    class _Sink:
        def query_dicts(self, *_args, **_kwargs):
            raise AssertionError("query_dicts should not be called")

    assert q.fetch_work_items(_Sink(), work_item_ids=[]) == []


def test_queries_helpers_build_expected_params(monkeypatch):
    calls = []

    def fake_query_dicts(_sink, query, params):
        calls.append((query, params))
        if "FROM git_commit_stats" in query:
            return [{"commit_hash": "h1", "churn_loc": 42}]
        if "FROM work_items" in query and "title" in query and "WHERE work_item_id IN" in query:
            return [
                {"work_item_id": "W1", "title": "Title 1"},
                {"work_item_id": "W2", "title": None},
            ]
        if "FROM user_metrics_daily" in query:
            return [{"id": "repo-1"}, {"id": "repo-2"}]
        return [{"repo_id": "repo-1", "number": 1}]

    monkeypatch.setattr(q, "query_dicts", fake_query_dicts)

    parents = q.fetch_parent_titles(object(), work_item_ids=["W1", "W2"])
    assert parents == {"W1": "Title 1"}

    churn = q.fetch_commit_churn(object(), repo_commits={"repo-1": ["h1"]})
    assert churn == {"repo-1@h1": 42.0}

    rows = q.fetch_pull_requests(object(), repo_numbers={"repo-1": [1]})
    assert rows == [{"repo_id": "repo-1", "number": 1}]

    repo_ids = q.resolve_repo_ids_for_teams(object(), team_ids=["team-a"])
    assert repo_ids == ["repo-1", "repo-2"]

    assert len(calls) >= 4
