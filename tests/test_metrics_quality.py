import uuid
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.quality import (
    compute_rework_churn_ratio,
    compute_single_owner_file_ratio,
)


def test_rework_churn_ratio_proxy():
    repo_id = uuid.uuid4()
    rows = [
        {
            "repo_id": repo_id,
            "commit_hash": "a1",
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_a.py",
            "additions": 10,
            "deletions": 0,
        },
        {
            "repo_id": repo_id,
            "commit_hash": "b1",
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_b.py",
            "additions": 5,
            "deletions": 5,
        },
        {
            "repo_id": repo_id,
            "commit_hash": "b2",
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_b.py",
            "additions": 10,
            "deletions": 0,
        },
    ]
    ratio = compute_rework_churn_ratio(repo_id=str(repo_id), window_stats=rows)
    # file_b.py has two commits: churn = 20; total churn = 30 => 20/30
    assert abs(ratio - (20.0 / 30.0)) < 1e-6


def test_single_owner_file_ratio():
    repo_id = uuid.uuid4()
    rows = [
        {
            "repo_id": repo_id,
            "commit_hash": "a1",
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_a.py",
            "additions": 1,
            "deletions": 0,
        },
        {
            "repo_id": repo_id,
            "commit_hash": "a2",
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_b.py",
            "additions": 1,
            "deletions": 0,
        },
        {
            "repo_id": repo_id,
            "commit_hash": "b1",
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "file_b.py",
            "additions": 1,
            "deletions": 0,
        },
    ]
    ratio = compute_single_owner_file_ratio(repo_id=str(repo_id), window_stats=rows)
    # file_a.py single owner, file_b.py split => 1/2
    assert abs(ratio - 0.5) < 1e-6


def test_review_load_top_reviewer_ratio():
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    pr_rows = [
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": "a@ex.com",
            "author_name": "A",
            "created_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
            "merged_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
        }
    ]
    review_rows = [
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": "reviewer-1",
            "submitted_at": datetime(2025, 2, 1, 1, tzinfo=timezone.utc),
            "state": "APPROVED",
        },
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": "reviewer-1",
            "submitted_at": datetime(2025, 2, 1, 2, tzinfo=timezone.utc),
            "state": "COMMENTED",
        },
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": "reviewer-2",
            "submitted_at": datetime(2025, 2, 1, 3, tzinfo=timezone.utc),
            "state": "COMMENTED",
        },
    ]
    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=[],
        pull_request_rows=pr_rows,
        pull_request_review_rows=review_rows,
        computed_at=datetime(2025, 2, 2, tzinfo=timezone.utc),
        include_commit_metrics=False,
    )
    repo_metrics = {m.repo_id: m for m in result.repo_metrics}
    assert repo_id in repo_metrics
    # reviewer-1 has 2/3 of reviews
    assert (
        abs(repo_metrics[repo_id].review_load_top_reviewer_ratio - (2.0 / 3.0)) < 1e-6
    )
