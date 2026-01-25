import uuid
from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute import compute_daily_metrics


def test_pr_rework_ratio():
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)

    # 3 merged PRs:
    # 1. Clean merge (0 changes requested)
    # 2. Reworked merge (1 change requested)
    # 3. Reworked merge (2 changes requested)
    # Rework ratio should be 2/3 = 0.666...

    pull_request_rows = [
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": "a@ex.com",
            "author_name": "Alice",
            "created_at": start,
            "merged_at": start + timedelta(hours=1),
            "changes_requested_count": 0,
        },
        {
            "repo_id": repo_id,
            "number": 2,
            "author_email": "b@ex.com",
            "author_name": "Bob",
            "created_at": start,
            "merged_at": start + timedelta(hours=2),
            "changes_requested_count": 1,
        },
        {
            "repo_id": repo_id,
            "number": 3,
            "author_email": "c@ex.com",
            "author_name": "Charlie",
            "created_at": start,
            "merged_at": start + timedelta(hours=3),
            "changes_requested_count": 2,
        },
        # Unmerged PR should not count towards denominator or numerator
        {
            "repo_id": repo_id,
            "number": 4,
            "author_email": "d@ex.com",
            "author_name": "Dave",
            "created_at": start,
            "merged_at": None,
            "changes_requested_count": 5,
        },
    ]

    computed_at = start + timedelta(days=1)
    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=[],
        pull_request_rows=pull_request_rows,
        computed_at=computed_at,
        include_commit_metrics=False,
    )

    repo_metrics = {m.repo_id: m for m in result.repo_metrics}
    assert repo_id in repo_metrics
    m = repo_metrics[repo_id]

    assert m.prs_merged == 3
    # 2 reworked / 3 merged
    assert abs(m.pr_rework_ratio - 0.6666666) < 1e-6
