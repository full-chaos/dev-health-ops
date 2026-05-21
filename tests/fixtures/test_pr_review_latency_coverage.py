from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.schemas import CommitStatRow, PullRequestRow


def _pr_row(pr) -> PullRequestRow:
    return {
        "repo_id": pr.repo_id,
        "number": pr.number,
        "author_email": pr.author_email,
        "author_name": pr.author_name,
        "created_at": pr.created_at,
        "merged_at": pr.merged_at,
        "first_review_at": pr.first_review_at,
        "first_comment_at": pr.first_comment_at,
        "reviews_count": pr.reviews_count,
        "changes_requested_count": pr.changes_requested_count,
        "comments_count": pr.comments_count,
        "additions": pr.additions,
        "deletions": pr.deletions,
        "changed_files": pr.changed_files,
    }


def _commit_row(repo_id, day: date) -> CommitStatRow:
    return {
        "repo_id": repo_id,
        "commit_hash": f"commit-{day.isoformat()}",
        "author_email": "alice@example.com",
        "author_name": "Alice Smith",
        "committer_when": datetime.combine(day, time(hour=12), tzinfo=timezone.utc),
        "file_path": "src/main.py",
        "additions": 10,
        "deletions": 2,
    }


def test_synthetic_prs_populate_review_latency_for_most_daily_rows() -> None:
    days = 90
    generator = SyntheticDataGenerator(repo_name="acme/demo-app", seed=1749)
    pr_rows = [
        _pr_row(item["pr"])
        for item in generator.generate_prs(count=days * 2, days=days)
    ]
    today = datetime.now(timezone.utc).date()
    start_day = today - timedelta(days=days - 1)
    non_null_review_days = 0

    for offset in range(days):
        day = start_day + timedelta(days=offset)
        window_start = datetime.combine(day, time.min, tzinfo=timezone.utc)
        window_end = window_start + timedelta(days=1)
        daily_pr_rows = [
            pr
            for pr in pr_rows
            if window_start <= pr["created_at"] < window_end
            or (
                pr["merged_at"] is not None
                and window_start <= pr["merged_at"] < window_end
            )
        ]
        result = compute_daily_metrics(
            day=day,
            commit_stat_rows=[_commit_row(generator.repo_id, day)],
            pull_request_rows=daily_pr_rows,
            pull_request_review_rows=[],
            computed_at=window_end,
        )
        repo_metric = result.repo_metrics[0]
        if repo_metric.pr_first_review_p90_hours is not None:
            non_null_review_days += 1

    assert non_null_review_days / days >= 0.5
