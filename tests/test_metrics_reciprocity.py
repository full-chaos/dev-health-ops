import uuid
from datetime import date, datetime, timezone
from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.schemas import (
    PullRequestRow,
    PullRequestReviewRow,
    CommitStatRow,
)


def test_review_reciprocity():
    day = date(2024, 1, 1)
    repo_id = uuid.uuid4()

    # Author identities
    alice = "alice@example.com"
    bob = "bob@example.com"
    charlie = "charlie@example.com"

    # 1. PRs
    prs = [
        # Alice's PR
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": alice,
            "created_at": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            "merged_at": datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc),
        },
        # Bob's PR
        {
            "repo_id": repo_id,
            "number": 2,
            "author_email": bob,
            "created_at": datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
            "merged_at": datetime(2024, 1, 1, 16, 0, tzinfo=timezone.utc),
        },
    ]

    # 2. Reviews
    reviews = [
        # Bob reviews Alice's PR
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": bob,
            "submitted_at": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            "state": "APPROVED",
        },
        # Alice reviews Bob's PR (Reciprocity logic: Alice gives 1 to Bob, Bob receives 1 from Alice)
        {
            "repo_id": repo_id,
            "number": 2,
            "reviewer": alice,
            "submitted_at": datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
            "state": "COMMENTED",
        },
        # Charlie reviews Alice's PR (Alice receives another review)
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": charlie,
            "submitted_at": datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
            "state": "CHANGES_REQUESTED",
        },
    ]

    # 3. Commits (to ensure users exist in aggs)
    commits = [
        {
            "repo_id": repo_id,
            "commit_hash": "h1",
            "author_email": alice,
            "committer_when": datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
            "additions": 10,
            "deletions": 0,
            "file_path": "a.txt",
        }
    ]

    result = compute_daily_metrics(
        day=day,
        commit_stat_rows=[CommitStatRow(c) for c in commits],  # type: ignore
        pull_request_rows=[PullRequestRow(p) for p in prs],  # type: ignore
        pull_request_review_rows=[PullRequestReviewRow(r) for r in reviews],  # type: ignore
        computed_at=datetime.now(timezone.utc),
        team_resolver=None,
    )

    # Alice:
    #   Authored PR 1.
    #   Reviewed PR 2 (Bob's). -> reviews_given = 1
    #   PR 1 reviewed by Bob and Charlie. -> reviews_received = 2
    #   Reciprocity = min(1, 2) / max(1, 2) = 0.5
    alice_metric = next(u for u in result.user_metrics if u.author_email == alice)
    assert alice_metric.reviews_given == 1
    assert alice_metric.reviews_received == 2
    assert alice_metric.review_reciprocity == 0.5

    # Bob:
    #   Authored PR 2.
    #   Reviewed PR 1 (Alice's). -> reviews_given = 1
    #   PR 2 reviewed by Alice. -> reviews_received = 1
    #   Reciprocity = min(1, 1) / max(1, 1) = 1.0
    bob_metric = next(u for u in result.user_metrics if u.author_email == bob)
    assert bob_metric.reviews_given == 1
    assert bob_metric.reviews_received == 1
    # Note: min(1,1)/max(1,1) = 1.0.
    # If reviews_received was 0, it would be min(1,0)/max(1,1) = 0.0
    assert bob_metric.review_reciprocity == 1.0

    # Charlie (not an author of PRs or commits in this test, but reviewed someone)
    # Charlie metric might not exist if we only aggregate by authors found in commits/PRs.
    # In compute.py: for review (reviewer_identity, rua)...
    # reviewer_identity charlie will be added to user_aggs.
    charlie_metric = next(
        (u for u in result.user_metrics if u.author_email == charlie), None
    )
    if charlie_metric:
        assert charlie_metric.reviews_given == 1
        assert charlie_metric.reviews_received == 0
        assert charlie_metric.review_reciprocity == 0.0
