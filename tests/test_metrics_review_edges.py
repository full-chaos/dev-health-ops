import uuid
from datetime import date, datetime, timezone

from dev_health_ops.metrics.reviews import compute_review_edges_daily


def test_review_edges_daily():
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    pr_rows = [
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": "author@ex.com",
            "author_name": "Author",
            "created_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
            "merged_at": datetime(2025, 2, 1, tzinfo=timezone.utc),
        }
    ]
    review_rows = [
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": "reviewer",
            "submitted_at": datetime(2025, 2, 1, 1, tzinfo=timezone.utc),
            "state": "APPROVED",
        },
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": "reviewer",
            "submitted_at": datetime(2025, 2, 1, 2, tzinfo=timezone.utc),
            "state": "COMMENTED",
        },
    ]
    edges = compute_review_edges_daily(
        day=day,
        pull_request_rows=pr_rows,
        pull_request_review_rows=review_rows,
        computed_at=datetime(2025, 2, 2, tzinfo=timezone.utc),
    )
    assert len(edges) == 1
    edge = edges[0]
    assert edge.reviews_count == 2
