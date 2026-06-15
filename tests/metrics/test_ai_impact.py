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


def _review(repo_id, number: int, *, hour: int, state: str = "COMMENTED"):
    return {
        "repo_id": repo_id,
        "number": number,
        "reviewer": "reviewer@example.com",
        "submitted_at": datetime(2026, 5, 18, hour, tzinfo=timezone.utc),
        "state": state,
    }


def _commit(
    repo_id,
    commit_hash: str,
    *,
    hour: int,
    file_path: str = "src/app.py",
    evidence: str | None = None,
):
    return {
        "repo_id": repo_id,
        "commit_hash": commit_hash,
        "author_email": "dev@example.com",
        "author_name": "Dev",
        "committer_when": datetime(2026, 5, 18, hour, tzinfo=timezone.utc),
        "file_path": file_path,
        "additions": 4,
        "deletions": 1,
        "evidence": evidence,
    }


def test_followup_commits_after_first_review_drive_rework():
    """A commit pushed after the first review is a follow-up commit and marks
    the PR as rework -- even with zero changes-requested (CHAOS-2437).

    Exercises the rework path in _aggregate (changes_requested OR
    followup_commits). Only the post-review commit counts: the commit landed at
    PR-open time (before the review boundary) must be excluded.
    """
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=12)  # changes_requested=0
    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        reviews=[_review(repo, 1, hour=9)],
        pr_commit_stats={
            (repo, 1): [
                _commit(repo, "initial", hour=8),  # before review -> excluded
                _commit(repo, "followup", hour=10),  # after review -> counted
            ]
        },
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.followup_commits_count == 1
    assert ai.changes_requested_per_pr == pytest.approx(0)
    assert ai.rework_prs == 1  # rework driven by the follow-up commit alone
    assert ai.rework_drag_rate == pytest.approx(1)


def test_followup_commits_dedupe_per_commit_across_file_rows():
    """Per-file linkage rows for one commit collapse to a single follow-up."""
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=12)
    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        reviews=[_review(repo, 1, hour=9)],
        pr_commit_stats={
            (repo, 1): [
                _commit(repo, "follow", hour=10, file_path="src/a.py"),
                _commit(repo, "follow", hour=10, file_path="src/b.py"),
            ]
        },
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.followup_commits_count == 1


def test_squash_merge_commit_excluded_by_evidence_not_just_timestamp():
    """A squash-merge PR's only linked commit is the squash artifact, tagged
    ``commit_message_squash_pr_ref`` by CHAOS-2435. It must be excluded by its
    linkage *evidence* -- crucially even though its commit time (11:00) is
    strictly BEFORE merged_at (12:00), which is exactly the live shape that
    defeats a timestamp-only bound (org a78c1a6a). No phantom follow-up/rework.
    """
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=12)  # no reviews -> boundary=open
    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        pr_commit_stats={
            (repo, 1): [
                _commit(
                    repo, "squash", hour=11, evidence="commit_message_squash_pr_ref"
                )
            ]
        },
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.followup_commits_count == 0
    assert ai.rework_prs == 0


def test_merge_commit_evidence_excluded_from_followup():
    """An explicit merge commit (``commit_message_pr_ref``) is the merge
    artifact too, not follow-up work -- excluded regardless of timing."""
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=12)
    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        reviews=[_review(repo, 1, hour=9)],
        pr_commit_stats={
            (repo, 1): [
                _commit(repo, "merge", hour=11, evidence="commit_message_pr_ref")
            ]
        },
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.followup_commits_count == 0
    assert ai.rework_prs == 0


def test_followup_commits_zero_without_linkage():
    """No PR↔commit linkage -> follow-up count is 0, never fabricated."""
    repo = uuid4()
    rows = _rows(
        [_pr(repo, 1, created_hour=8, merged_hour=12, changes_requested=0)],
        [_attr(repo, 1, "ai_assisted")],
        pr_commit_stats=None,
    )

    ai = _bucket(rows, "ai_assisted")
    assert ai.followup_commits_count == 0
    assert ai.rework_prs == 0


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


def test_test_gap_rate_below_100_when_some_prs_touch_test_files():
    """Regression: test_gap_rate must be < 100% when some PRs include test-file commits.

    Root cause (CHAOS-2183): job_daily.py never passed pr_commit_stats to
    compute_ai_impact_metrics_daily, so _test_changes_by_pr returned {} and every PR
    was counted as a test gap, inflating test_gap_rate to 100%.
    """
    repo = uuid4()
    prs = [
        _pr(repo, 1, created_hour=8, merged_hour=10),  # touches a test file
        _pr(repo, 2, created_hour=8, merged_hour=10),  # no test file
    ]
    attrs = [
        _attr(repo, 1, "ai_assisted"),
        _attr(repo, 2, "ai_assisted"),
    ]
    test_commit: CommitStatRow = {
        "repo_id": repo,
        "commit_hash": "abc123",
        "author_email": "dev@example.com",
        "author_name": "Dev",
        "committer_when": datetime(2026, 5, 18, 9, tzinfo=timezone.utc),
        "file_path": "tests/test_feature.py",
        "additions": 20,
        "deletions": 0,
    }
    non_test_commit: CommitStatRow = {
        "repo_id": repo,
        "commit_hash": "def456",
        "author_email": "dev@example.com",
        "author_name": "Dev",
        "committer_when": datetime(2026, 5, 18, 9, tzinfo=timezone.utc),
        "file_path": "src/feature.py",
        "additions": 10,
        "deletions": 2,
    }

    rows = _rows(
        prs,
        attrs,
        pr_commit_stats={
            (repo, 1): [test_commit],
            (repo, 2): [non_test_commit],
        },
    )

    ai = _bucket(rows, "ai_assisted")
    # PR 2 has no test change → 1 gap out of 2 PRs → 50%, not 100%
    assert ai.test_gap_prs == 1
    assert ai.test_gap_rate == pytest.approx(0.5)
    assert ai.test_gap_rate < 1.0


def test_test_gap_rate_is_unavailable_when_pr_commit_stats_absent():
    """When pr_commit_stats=None, test_gap_rate must be None (unavailable), NOT 100%.

    Before the fix, a None pr_commit_stats caused every PR to get has_test_change=False
    → test_gap_rate=1.0 (100%).  That 100% then fed ai_detector.py's >=0.50 threshold,
    producing a false "AI PRs had 100% test gap" recommendation (CHAOS-2183 root cause).
    After the fix, each PR gets has_test_change=None, the known-count denominator is 0,
    and test_gap_rate=None signals "data unavailable" rather than "every PR is a gap".
    """
    repo = uuid4()
    rows = _rows(
        [
            _pr(repo, 1, created_hour=8, merged_hour=10),
            _pr(repo, 2, created_hour=8, merged_hour=10),
        ],
        [_attr(repo, 1, "ai_assisted"), _attr(repo, 2, "ai_assisted")],
        pr_commit_stats=None,
    )
    ai = _bucket(rows, "ai_assisted")
    # No commit linkage → has_test_change=None for every PR → denominator=0 → rate=None
    assert ai.test_gap_prs == 0
    assert ai.test_gap_rate is None


def test_test_gap_not_inflated_for_prior_day_test_commit():
    """PR merged today (day N) whose test commit landed on day N-1 must NOT be a gap.

    job_daily.py (post-fix) queries work_graph_pr_commit JOIN git_commit_stats for all
    commits belonging to in-window PRs, regardless of commit date.  This verifies that
    compute_ai_impact_metrics_daily correctly handles pr_commit_stats populated with
    commits from outside the current day window — i.e. the formula is window-agnostic.
    """
    repo = uuid4()
    pr = _pr(repo, 1, created_hour=8, merged_hour=10)  # merged on DAY (2026-05-18)
    prior_day_test_commit: CommitStatRow = {
        "repo_id": repo,
        "commit_hash": "abc123",
        "author_email": "dev@example.com",
        "author_name": "Dev",
        # Commit timestamp is DAY-1, outside the job's day window.
        "committer_when": datetime(2026, 5, 17, 20, tzinfo=timezone.utc),
        "file_path": "tests/test_auth.py",
        "additions": 30,
        "deletions": 0,
    }

    rows = _rows(
        [pr],
        [_attr(repo, 1, "ai_assisted")],
        pr_commit_stats={(repo, 1): [prior_day_test_commit]},
    )

    ai = _bucket(rows, "ai_assisted")
    # Prior-day test commit must be recognised → has_test_change=True → zero gaps
    assert ai.test_gap_prs == 0
    assert ai.test_gap_rate == pytest.approx(0.0)
    assert ai.test_gap_rate < 1.0


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
