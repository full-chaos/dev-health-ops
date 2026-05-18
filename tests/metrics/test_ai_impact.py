from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from dev_health_ops.metrics.ai_impact import compute_ai_impact_metrics_daily
from dev_health_ops.metrics.schemas import (
    AIPullRequestAttributionRow,
    CommitStatRow,
    IncidentRow,
    PullRequestReviewRow,
    PullRequestRow,
)
from dev_health_ops.metrics.sinks.clickhouse.ai_impact import AIImpactMixin

DAY = date(2026, 5, 18)
COMPUTED_AT = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)


def _pr(
    repo_id,
    number: int,
    *,
    created_hour: int,
    merged_hour: int | None,
    reviews: int = 0,
    changes_requested: int = 0,
    additions: int = 10,
    deletions: int = 5,
) -> PullRequestRow:
    return {
        "repo_id": repo_id,
        "number": number,
        "author_email": "dev@example.com",
        "author_name": "Dev",
        "created_at": datetime(2026, 5, 18, created_hour, tzinfo=timezone.utc),
        "merged_at": (
            datetime(2026, 5, 18, merged_hour, tzinfo=timezone.utc)
            if merged_hour is not None
            else None
        ),
        "reviews_count": reviews,
        "changes_requested_count": changes_requested,
        "comments_count": 0,
        "additions": additions,
        "deletions": deletions,
        "changed_files": 2,
    }


def _attr(
    repo_id, number: int, kind: str, work_type: str = "pull_request"
) -> AIPullRequestAttributionRow:
    return {
        "repo_id": repo_id,
        "number": number,
        "kind": kind,
        "work_type": work_type,
        "team_id": None,
    }


def _rows(
    prs: list[PullRequestRow],
    attrs: list[AIPullRequestAttributionRow],
    *,
    reviews: list[PullRequestReviewRow] | None = None,
    incidents: list[IncidentRow] | None = None,
    commit_stats: list[CommitStatRow] | None = None,
    pr_commit_stats: dict | None = None,
):
    return compute_ai_impact_metrics_daily(
        day=DAY,
        org_id="org-a",
        pull_request_rows=prs,
        pull_request_review_rows=reviews or [],
        ai_attribution_rows=attrs,
        incident_rows=incidents or [],
        commit_stat_rows=commit_stats or [],
        computed_at=COMPUTED_AT,
        pr_commit_stats=pr_commit_stats,
    )


def _bucket(rows, bucket: str):
    return next(row for row in rows if row.attribution_bucket == bucket)


def test_ai_assisted_pr_ratio_keeps_unknown_out_of_human_baseline():
    repo = uuid4()
    rows = _rows(
        [
            _pr(repo, 1, created_hour=9, merged_hour=10),
            _pr(repo, 2, created_hour=9, merged_hour=11),
        ],
        [_attr(repo, 1, "ai_assisted")],
    )

    ai = _bucket(rows, "ai_assisted")
    unknown = _bucket(rows, "unknown")

    assert ai.ai_assisted_pr_ratio == pytest.approx(0.5)
    assert ai.human_prs == 0
    assert ai.unknown_prs == 1
    assert unknown.prs_total == 1


def test_agent_created_pr_count_is_reported():
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 7, created_hour=9, merged_hour=10)],
        [_attr(repo, 7, "agent_created")],
    )

    assert _bucket(rows, "agent_created").agent_created_pr_count == 1


def test_ai_cycle_time_delta_uses_human_baseline_only():
    repo = uuid4()
    rows = _rows(
        [
            _pr(repo, 1, created_hour=8, merged_hour=10),
            _pr(repo, 2, created_hour=8, merged_hour=13),
        ],
        [_attr(repo, 1, "ai_assisted"), _attr(repo, 2, "human")],
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.cycle_time_avg_hours == pytest.approx(2)
    assert ai.baseline_cycle_time_avg_hours == pytest.approx(5)
    assert ai.ai_cycle_time_delta_hours == pytest.approx(-3)
    assert ai.leverage.cycle_time_component == pytest.approx(0.6)


def test_ai_review_amplification_is_decomposed():
    repo = uuid4()
    rows = _rows(
        [
            _pr(repo, 1, created_hour=8, merged_hour=10, reviews=4),
            _pr(repo, 2, created_hour=8, merged_hour=10, reviews=2),
        ],
        [_attr(repo, 1, "ai_assisted"), _attr(repo, 2, "human")],
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.reviews_per_pr == pytest.approx(4)
    assert ai.baseline_reviews_per_pr == pytest.approx(2)
    assert ai.ai_review_amplification == pytest.approx(1)
    assert ai.leverage.review_component == pytest.approx(1)


def test_ai_rework_drag_counts_changes_requested():
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 1, created_hour=8, merged_hour=10, changes_requested=1)],
        [_attr(repo, 1, "ai_assisted")],
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.rework_prs == 1
    assert ai.rework_drag_rate == pytest.approx(1)


def test_ai_revert_and_incident_drag_are_separate_fields():
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 1, created_hour=8, merged_hour=10, additions=10, deletions=100)],
        [_attr(repo, 1, "ai_assisted")],
        incidents=[
            {
                "repo_id": repo,
                "incident_id": "inc-1",
                "status": "resolved",
                "started_at": datetime(2026, 5, 18, 11, tzinfo=timezone.utc),
                "resolved_at": None,
            }
        ],
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.revert_prs == 1
    assert ai.revert_rate == pytest.approx(1)
    assert ai.incidents_count == 1
    assert ai.incident_drag_rate == pytest.approx(1)


def test_ai_test_gap_rate_uses_pr_commit_file_evidence():
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=10)
    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        pr_commit_stats={
            (repo, 1): [
                {
                    "repo_id": repo,
                    "commit_hash": "abc",
                    "author_email": "dev@example.com",
                    "author_name": "Dev",
                    "committer_when": datetime(2026, 5, 18, 9, tzinfo=timezone.utc),
                    "file_path": "src/app.py",
                    "additions": 4,
                    "deletions": 1,
                }
            ]
        },
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.test_gap_prs == 1
    assert ai.test_gap_rate == pytest.approx(1)
    assert ai.leverage.test_component is None


def test_operating_leverage_is_decomposed_not_single_score():
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 1, created_hour=8, merged_hour=10)], [_attr(repo, 1, "ai_assisted")]
    )

    leverage = _bucket(rows, "ai_assisted").leverage
    assert leverage.prs_component == 1
    assert hasattr(leverage, "cycle_time_component")
    assert hasattr(leverage, "review_component")
    assert not hasattr(leverage, "score")


def test_clickhouse_sink_writes_ai_impact_rows_with_dimensions():
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 1, created_hour=8, merged_hour=10)], [_attr(repo, 1, "ai_assisted")]
    )

    class Client:
        def __init__(self):
            self.calls = []

        def insert(self, table, matrix, column_names):
            self.calls.append((table, matrix, column_names))

    class Sink(AIImpactMixin):
        def __init__(self):
            self.client = Client()

    sink = Sink()
    sink.write_ai_impact_metrics(rows)

    table, matrix, columns = sink.client.calls[0]
    assert table == "ai_impact_metrics_daily"
    assert "org_id" in columns
    assert "team_id" in columns
    assert "repo_id" in columns
    assert "work_type" in columns
    assert matrix[0][columns.index("attribution_bucket")] == "ai_assisted"
