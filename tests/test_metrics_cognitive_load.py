"""
Tests for the privacy-first cognitive-load derived metrics.

Fields under test (added in migration 016_cognitive_load_metrics.sql):
  - pr_interruption_load  : distinct PRs reviewed by the user in the day window
  - context_spread_count  : distinct repos the user had any activity in
  - review_request_load   : distinct authored PRs that received review activity in the day

Data sources used: PR rows, PR review rows, commit stat rows only.
No IDE / keystroke / session telemetry involved.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from dev_health_ops.metrics.compute import compute_daily_metrics
from dev_health_ops.metrics.schemas import (
    CommitStatRow,
    PullRequestReviewRow,
    PullRequestRow,
)


_DAY = date(2026, 1, 15)
_START = datetime(_DAY.year, _DAY.month, _DAY.day, tzinfo=timezone.utc)
_COMPUTED_AT = _START + timedelta(days=1)


def _repo() -> uuid.UUID:
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# pr_interruption_load
# ---------------------------------------------------------------------------


def test_pr_interruption_load_counts_distinct_prs_reviewed() -> None:
    """
    User reviews PR #1 twice (two review events) and PR #2 once.
    pr_interruption_load should be 2 (distinct PRs), not 3 (raw event count).
    """
    repo = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[],
        pull_request_rows=[
            {
                "repo_id": repo,
                "number": 1,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START,
                "merged_at": None,
            },
            {
                "repo_id": repo,
                "number": 2,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START,
                "merged_at": None,
            },
        ],
        pull_request_review_rows=[
            # PR #1 — first review event
            {
                "repo_id": repo,
                "number": 1,
                "reviewer": "reviewer@example.com",
                "submitted_at": _START + timedelta(hours=1),
                "state": "COMMENTED",
            },
            # PR #1 — second review event (same PR, same reviewer)
            {
                "repo_id": repo,
                "number": 1,
                "reviewer": "reviewer@example.com",
                "submitted_at": _START + timedelta(hours=2),
                "state": "APPROVED",
            },
            # PR #2 — one review event
            {
                "repo_id": repo,
                "number": 2,
                "reviewer": "reviewer@example.com",
                "submitted_at": _START + timedelta(hours=3),
                "state": "CHANGES_REQUESTED",
            },
        ],
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )

    by_user = {(m.repo_id, m.author_email): m for m in result.user_metrics}
    reviewer = by_user[(repo, "reviewer@example.com")]
    # 3 review events but only 2 distinct PRs
    assert reviewer.reviews_given == 3
    assert reviewer.pr_interruption_load == 2


def test_pr_interruption_load_zero_when_no_reviews() -> None:
    """No review rows → pr_interruption_load stays 0."""
    repo = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[
            {
                "repo_id": repo,
                "commit_hash": "abc",
                "author_email": "dev@example.com",
                "author_name": "Dev",
                "committer_when": _START + timedelta(hours=1),
                "file_path": "main.py",
                "additions": 10,
                "deletions": 0,
            }
        ],
        pull_request_rows=[],
        pull_request_review_rows=None,
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    by_user = {m.author_email: m for m in result.user_metrics}
    assert by_user["dev@example.com"].pr_interruption_load == 0


def test_pr_interruption_load_excludes_reviews_outside_day_window() -> None:
    """Reviews submitted outside the day window must not count."""
    repo = _repo()
    yesterday = _START - timedelta(hours=1)  # one hour before day start
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[],
        pull_request_rows=[
            {
                "repo_id": repo,
                "number": 5,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START - timedelta(days=1),
                "merged_at": None,
            }
        ],
        pull_request_review_rows=[
            {
                "repo_id": repo,
                "number": 5,
                "reviewer": "reviewer@example.com",
                "submitted_at": yesterday,
                "state": "COMMENTED",
            }
        ],
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    by_user = {m.author_email: m for m in result.user_metrics}
    # reviewer had no in-window review activity → they won't appear
    assert "reviewer@example.com" not in by_user


# ---------------------------------------------------------------------------
# context_spread_count
# ---------------------------------------------------------------------------


def test_context_spread_single_repo() -> None:
    """A user active in only one repo has context_spread_count == 1."""
    repo = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[
            {
                "repo_id": repo,
                "commit_hash": "abc",
                "author_email": "dev@example.com",
                "author_name": "Dev",
                "committer_when": _START + timedelta(hours=1),
                "file_path": "main.py",
                "additions": 5,
                "deletions": 0,
            }
        ],
        pull_request_rows=[],
        pull_request_review_rows=None,
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    by_user = {m.author_email: m for m in result.user_metrics}
    assert by_user["dev@example.com"].context_spread_count == 1


def test_context_spread_multiple_repos_via_reviews_and_commits() -> None:
    """
    User commits to repo_a and reviews a PR in repo_b.
    context_spread_count should be 2.
    """
    repo_a = _repo()
    repo_b = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[
            {
                "repo_id": repo_a,
                "commit_hash": "abc",
                "author_email": "dev@example.com",
                "author_name": "Dev",
                "committer_when": _START + timedelta(hours=1),
                "file_path": "main.py",
                "additions": 5,
                "deletions": 0,
            }
        ],
        pull_request_rows=[
            {
                "repo_id": repo_b,
                "number": 10,
                "author_email": "other@example.com",
                "author_name": "Other",
                "created_at": _START,
                "merged_at": None,
            }
        ],
        pull_request_review_rows=[
            {
                "repo_id": repo_b,
                "number": 10,
                "reviewer": "dev@example.com",
                "submitted_at": _START + timedelta(hours=2),
                "state": "APPROVED",
            }
        ],
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    # dev@example.com appears as (repo_a, dev) and (repo_b, dev)
    dev_records = [m for m in result.user_metrics if m.author_email == "dev@example.com"]
    # context_spread_count is identical across all rows for the same identity
    assert len(dev_records) == 2
    for rec in dev_records:
        assert rec.context_spread_count == 2


def test_context_spread_author_pr_activity_in_repo_counts() -> None:
    """PR authorship in a repo counts as activity for context spread."""
    repo_a = _repo()
    repo_b = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[
            {
                "repo_id": repo_a,
                "commit_hash": "x1",
                "author_email": "dev@example.com",
                "author_name": "Dev",
                "committer_when": _START + timedelta(hours=1),
                "file_path": "src/main.py",
                "additions": 3,
                "deletions": 0,
            }
        ],
        pull_request_rows=[
            # dev authors a PR in repo_b
            {
                "repo_id": repo_b,
                "number": 99,
                "author_email": "dev@example.com",
                "author_name": "Dev",
                "created_at": _START + timedelta(hours=2),
                "merged_at": None,
            }
        ],
        pull_request_review_rows=None,
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    dev_records = [m for m in result.user_metrics if m.author_email == "dev@example.com"]
    for rec in dev_records:
        assert rec.context_spread_count == 2


# ---------------------------------------------------------------------------
# review_request_load
# ---------------------------------------------------------------------------


def test_review_request_load_counts_distinct_authored_prs_receiving_reviews() -> None:
    """
    Author has 3 PRs.  PRs #1 and #2 receive reviews in the day window; PR #3 does not.
    review_request_load should be 2.
    """
    repo = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[],
        pull_request_rows=[
            {
                "repo_id": repo,
                "number": 1,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START - timedelta(days=1),
                "merged_at": None,
            },
            {
                "repo_id": repo,
                "number": 2,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START - timedelta(days=2),
                "merged_at": None,
            },
            {
                "repo_id": repo,
                "number": 3,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START - timedelta(days=3),
                "merged_at": None,
            },
        ],
        pull_request_review_rows=[
            # PR #1 gets 2 review events (should still count as 1 distinct PR)
            {
                "repo_id": repo,
                "number": 1,
                "reviewer": "r1@example.com",
                "submitted_at": _START + timedelta(hours=1),
                "state": "COMMENTED",
            },
            {
                "repo_id": repo,
                "number": 1,
                "reviewer": "r2@example.com",
                "submitted_at": _START + timedelta(hours=2),
                "state": "CHANGES_REQUESTED",
            },
            # PR #2 gets 1 review event
            {
                "repo_id": repo,
                "number": 2,
                "reviewer": "r1@example.com",
                "submitted_at": _START + timedelta(hours=3),
                "state": "APPROVED",
            },
            # PR #3 receives NO review events today
        ],
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )

    by_user = {(m.repo_id, m.author_email): m for m in result.user_metrics}
    author = by_user[(repo, "author@example.com")]
    # 3 review events but only 2 distinct authored PRs received reviews
    assert author.reviews_received == 3
    assert author.review_request_load == 2


def test_review_request_load_zero_when_no_reviews_on_authored_prs() -> None:
    """Author has PRs open but no review activity that day → load is 0."""
    repo = _repo()
    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[],
        pull_request_rows=[
            {
                "repo_id": repo,
                "number": 10,
                "author_email": "author@example.com",
                "author_name": "Author",
                "created_at": _START + timedelta(hours=1),
                "merged_at": None,
            }
        ],
        pull_request_review_rows=None,
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )
    by_user = {(m.repo_id, m.author_email): m for m in result.user_metrics}
    author = by_user[(repo, "author@example.com")]
    assert author.review_request_load == 0


# ---------------------------------------------------------------------------
# Combined scenario
# ---------------------------------------------------------------------------


def test_cognitive_load_combined_scenario() -> None:
    """
    Alice commits to repo_a, reviews PR #5 in repo_b (twice), and has PR #7 in repo_a reviewed.
    Expected:
      repo_a row for alice: pr_interruption_load=0, context_spread=2, review_request_load=1
      repo_b row for alice: pr_interruption_load=1, context_spread=2, review_request_load=0
    """
    repo_a = _repo()
    repo_b = _repo()

    result = compute_daily_metrics(
        day=_DAY,
        commit_stat_rows=[
            {
                "repo_id": repo_a,
                "commit_hash": "c1",
                "author_email": "alice@example.com",
                "author_name": "Alice",
                "committer_when": _START + timedelta(hours=1),
                "file_path": "app.py",
                "additions": 20,
                "deletions": 5,
            }
        ],
        pull_request_rows=[
            # Alice's PR in repo_a
            {
                "repo_id": repo_a,
                "number": 7,
                "author_email": "alice@example.com",
                "author_name": "Alice",
                "created_at": _START - timedelta(days=1),
                "merged_at": None,
            },
            # Bob's PR in repo_b (Alice reviews it)
            {
                "repo_id": repo_b,
                "number": 5,
                "author_email": "bob@example.com",
                "author_name": "Bob",
                "created_at": _START - timedelta(days=2),
                "merged_at": None,
            },
        ],
        pull_request_review_rows=[
            # Alice reviews PR #5 in repo_b twice
            {
                "repo_id": repo_b,
                "number": 5,
                "reviewer": "alice@example.com",
                "submitted_at": _START + timedelta(hours=2),
                "state": "COMMENTED",
            },
            {
                "repo_id": repo_b,
                "number": 5,
                "reviewer": "alice@example.com",
                "submitted_at": _START + timedelta(hours=4),
                "state": "APPROVED",
            },
            # Bob reviews Alice's PR #7 in repo_a
            {
                "repo_id": repo_a,
                "number": 7,
                "reviewer": "bob@example.com",
                "submitted_at": _START + timedelta(hours=3),
                "state": "CHANGES_REQUESTED",
            },
        ],
        computed_at=_COMPUTED_AT,
        include_commit_metrics=False,
    )

    by_key = {(m.repo_id, m.author_email): m for m in result.user_metrics}

    # Alice in repo_a: had a commit + PR authored; no reviews given in repo_a this day
    alice_a = by_key[(repo_a, "alice@example.com")]
    assert alice_a.pr_interruption_load == 0  # reviewed no PRs in repo_a
    assert alice_a.context_spread_count == 2  # active in both repos
    assert alice_a.review_request_load == 1  # PR #7 got reviewed today

    # Alice in repo_b: reviewed PR #5 twice (1 distinct PR)
    alice_b = by_key[(repo_b, "alice@example.com")]
    assert alice_b.pr_interruption_load == 1  # 1 distinct PR reviewed
    assert alice_b.context_spread_count == 2  # active in both repos
    assert alice_b.review_request_load == 0  # no authored PRs in repo_b received reviews
