from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute import commit_size_bucket, compute_daily_metrics


def test_commit_size_bucket_boundaries() -> None:
    assert commit_size_bucket(0) == "small"
    assert commit_size_bucket(50) == "small"
    assert commit_size_bucket(51) == "medium"
    assert commit_size_bucket(300) == "medium"
    assert commit_size_bucket(301) == "large"


def test_daily_user_aggregation_distinct_files_and_prs() -> None:
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    commit_stat_rows = [
        # user1 commit1 touches file1 + file2 (small)
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "author_email": "a@example.com",
            "author_name": "Alice",
            "committer_when": start + timedelta(hours=1),
            "file_path": "file1.py",
            "additions": 10,
            "deletions": 5,
        },
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "author_email": "a@example.com",
            "author_name": "Alice",
            "committer_when": start + timedelta(hours=1),
            "file_path": "file2.py",
            "additions": 1,
            "deletions": 1,
        },
        # user1 commit2 touches file1 again (large)
        {
            "repo_id": repo_id,
            "commit_hash": "c2",
            "author_email": "a@example.com",
            "author_name": "Alice",
            "committer_when": start + timedelta(hours=2),
            "file_path": "file1.py",
            "additions": 200,
            "deletions": 150,
        },
        # user2 commit3 touches file3
        {
            "repo_id": repo_id,
            "commit_hash": "c3",
            "author_email": "b@example.com",
            "author_name": "Bob",
            "committer_when": start + timedelta(hours=3),
            "file_path": "file3.py",
            "additions": 5,
            "deletions": 0,
        },
    ]

    pull_request_rows = [
        # PR authored today by user1, not merged
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": "a@example.com",
            "author_name": "Alice",
            "created_at": start + timedelta(hours=4),
            "merged_at": None,
        },
        # PR merged today by user1 (created earlier)
        {
            "repo_id": repo_id,
            "number": 2,
            "author_email": "a@example.com",
            "author_name": "Alice",
            "created_at": start - timedelta(hours=10),
            "merged_at": start + timedelta(hours=5),
        },
        # PR created+merged today by user2 but missing email (fallback to author_name)
        {
            "repo_id": repo_id,
            "number": 3,
            "author_email": None,
            "author_name": "Bob",
            "created_at": start + timedelta(hours=1),
            "merged_at": start + timedelta(hours=2),
        },
    ]

    computed_at = start + timedelta(days=1)
    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=commit_stat_rows,
        pull_request_rows=pull_request_rows,
        computed_at=computed_at,
        include_commit_metrics=True,
    )

    by_user = {(m.repo_id, m.author_email): m for m in result.user_metrics}

    user1 = by_user[(repo_id, "a@example.com")]
    assert user1.commits_count == 2
    assert user1.loc_added == 211
    assert user1.loc_deleted == 156
    assert user1.files_changed == 2  # distinct across day: file1.py + file2.py
    assert user1.large_commits_count == 1
    assert user1.avg_commit_size_loc == (211 + 156) / 2
    assert user1.prs_authored == 1
    assert user1.prs_merged == 1
    assert user1.avg_pr_cycle_hours == 15.0  # (merged 5h - created -10h) = 15h
    assert user1.median_pr_cycle_hours == 15.0

    user2 = by_user[(repo_id, "b@example.com")]
    assert user2.commits_count == 1
    assert user2.loc_added == 5
    assert user2.loc_deleted == 0
    assert user2.files_changed == 1
    assert user2.large_commits_count == 0
    assert user2.avg_commit_size_loc == 5.0
    assert user2.prs_authored == 0  # email identity differs from PR fallback identity
    assert user2.prs_merged == 0

    # PR identity fallback stores under author_name when email missing.
    user2_fallback = by_user[(repo_id, "Bob")]
    assert user2_fallback.commits_count == 0
    assert user2_fallback.prs_authored == 1
    assert user2_fallback.prs_merged == 1
    assert user2_fallback.avg_pr_cycle_hours == 1.0
    assert user2_fallback.median_pr_cycle_hours == 1.0


def test_repo_median_pr_cycle_even_count() -> None:
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    commit_stat_rows = []

    # Cycle times in hours: 1, 3, 5, 10 -> median = (3+5)/2 = 4
    cycles = [1, 3, 5, 10]
    pull_request_rows = []
    for i, hours in enumerate(cycles, start=1):
        pull_request_rows.append(
            {
                "repo_id": repo_id,
                "number": i,
                "author_email": "a@example.com",
                "author_name": "Alice",
                "created_at": start,
                "merged_at": start + timedelta(hours=hours),
            }
        )

    computed_at = start + timedelta(days=1)
    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=commit_stat_rows,
        pull_request_rows=pull_request_rows,
        computed_at=computed_at,
        include_commit_metrics=False,
    )

    repo_metrics = {m.repo_id: m for m in result.repo_metrics}
    assert repo_metrics[repo_id].median_pr_cycle_hours == 4.0
    assert repo_metrics[repo_id].pr_cycle_p75_hours == 6.25
    assert repo_metrics[repo_id].pr_cycle_p90_hours == 8.5


def test_reviews_given_aggregation() -> None:
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=[],
        pull_request_rows=[],
        pull_request_review_rows=[
            {
                "repo_id": repo_id,
                "number": 123,
                "reviewer": "reviewer@example.com",
                "submitted_at": start + timedelta(hours=1),
                "state": "CHANGES_REQUESTED",
            }
        ],
        computed_at=start + timedelta(days=1),
        include_commit_metrics=False,
    )

    by_user = {(m.repo_id, m.author_email): m for m in result.user_metrics}
    reviewer = by_user[(repo_id, "reviewer@example.com")]
    assert reviewer.reviews_given == 1
    assert reviewer.changes_requested_given == 1
