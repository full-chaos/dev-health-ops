"""CHAOS-2436: GitHub PR sync must populate ``changes_requested_count`` and
``reviews_count`` on ``git_pull_requests`` from the PR review summary.

The count of reviews whose state is ``CHANGES_REQUESTED`` is the authoritative
source for AI Rework rate (``metrics/ai_impact.py`` falls back to it when the
column is unset). A regression here silently zeroes the rework signal even when
the underlying ``git_pull_request_reviews`` rows are present, so this guards the
writer end-to-end through ``_sync_github_prs_to_store``.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import patch

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation (mirrors test_processors_pr_mr_rate_limit.py).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.processors.github import _sync_github_prs_to_store


class _NoSleepGate:
    def wait_sync(self) -> None:
        return

    def penalize(self, delay_seconds=None) -> float:
        return float(delay_seconds or 0)

    def reset(self) -> None:
        return


class _FakeStore:
    def __init__(self):
        self.pr_batches: list[list[Any]] = []
        self.review_batches: list[list[Any]] = []

    async def insert_git_pull_requests(self, batch):
        self.pr_batches.append(list(batch))

    async def insert_git_pull_request_reviews(self, batch):
        self.review_batches.append(list(batch))


class _FakeGithub:
    def __init__(self, repo):
        self._repo = repo

    def get_repo(self, _full_name: str):
        return self._repo


class _PRIter:
    def __init__(self, items):
        self._items = list(items)
        self._idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx >= len(self._items):
            raise StopIteration
        item = self._items[self._idx]
        self._idx += 1
        return item


class _FakePRUser:
    def __init__(self, login="octo"):
        self.login = login
        self.email = None


class _FakePRRef:
    def __init__(self, ref):
        self.ref = ref


class _FakePR:
    def __init__(self, number: int):
        self.number = number
        self.title = f"PR {number}"
        self.body = None
        self.state = "closed"
        self.author_login = "octo"
        self.user = _FakePRUser()
        self.created_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self.updated_at = datetime(2020, 1, 2, tzinfo=timezone.utc)
        self.merged_at = None
        self.closed_at = None
        self.head_ref = "feature"
        self.base_ref = "main"
        self.head = _FakePRRef("feature")
        self.base = _FakePRRef("main")
        self.additions = 0
        self.deletions = 0
        self.changed_files = 0
        self.comments = 0
        self.comments_count = 0


class _FakeRepo:
    def __init__(self, pull_items):
        self._pull_items = pull_items

    def get_pulls(self, state="all", sort=None, direction=None):
        raise AssertionError("legacy PyGithub get_pulls must not be called")


class _ListingCodeClient:
    def __init__(self, pulls):
        self._pulls = list(pulls)

    async def iter_pulls(self, owner, repo, *, state, sort, direction, since=None):
        assert (owner, repo, state, sort, direction) == (
            "o",
            "r",
            "all",
            "updated",
            "desc",
        )
        self.since = since
        return list(self._pulls)

    async def get_pull_detail(self, owner, repo, number):
        assert (owner, repo) == ("o", "r")
        for pull in self._pulls:
            if pull.number == number:
                return pull
        raise AssertionError(f"unexpected pull detail request for {number}")

    def drain_usage_observations(self):
        return []

    async def close(self):
        return None


class _FakeReview:
    def __init__(self, review_id: int, state: str, reviewer: str = "carol"):
        self.id = review_id
        self.state = state
        self.reviewer = reviewer
        self.submitted_at = datetime(2020, 1, 2, tzinfo=timezone.utc)
        self.body = f"review {review_id}"
        self.url = f"https://github.test/reviews/{review_id}"


@pytest.mark.asyncio
async def test_github_pr_sync_writes_changes_requested_count_from_reviews():
    """A PR with two CHANGES_REQUESTED reviews (plus approve/comment noise)
    must persist ``changes_requested_count == 2`` and ``reviews_count == 4``."""
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()

    fake_repo = _FakeRepo([_FakePR(1), _FakePR(2)])

    reviews_by_pr = {
        1: [
            _FakeReview(101, "CHANGES_REQUESTED"),
            _FakeReview(102, "APPROVED"),
            _FakeReview(103, "CHANGES_REQUESTED"),
            _FakeReview(104, "COMMENTED"),
        ],
        2: [_FakeReview(201, "APPROVED")],
    }

    class _Connector:
        def __init__(self):
            self.github = _FakeGithub(fake_repo)
            self.token = "token"
            self.per_page = 100
            self.graphql = object()

        def _rest_base_url(self):
            return "https://api.github.com"

        def get_pull_request_reviews(self, owner, repo, number):
            raise AssertionError("per-PR review fetch should not be used")

    class _BatchReviewClient:
        calls: list[tuple[str, str, list[int], int | None, str | None]] = []

        def __init__(self, **_kwargs):
            self.graphql = None

        def iter_pr_reviews_batch(
            self, *, owner, repo, prs, limit, operation_family=None
        ):
            self.calls.append(
                (owner, repo, [pr.number for pr in prs], limit, operation_family)
            )
            for pr in prs:
                yield pr.number, tuple(reviews_by_pr.get(pr.number, []))

    with (
        patch(
            "dev_health_ops.processors.github.GitHubWorkClient",
            _BatchReviewClient,
        ),
        patch(
            "dev_health_ops.processors.github._github_code_client_from_connector",
            lambda _connector: _ListingCodeClient(fake_repo._pull_items),
        ),
    ):
        total = await loop.run_in_executor(
            None,
            cast(Any, _sync_github_prs_to_store),
            _Connector(),
            "o",
            "r",
            repo_id,
            store,
            loop,
            50,  # batch_size
            "all",
            _NoSleepGate(),
        )

    assert total == 2
    # CHAOS-2803/CS2: the review batch labels its GraphQL calls with the
    # "pr_social" family so they re-bucket off the work_item_prs default.
    assert _BatchReviewClient.calls == [("o", "r", [1, 2], None, "pr_social")]

    prs = {pr.number: pr for batch in store.pr_batches for pr in batch}
    assert prs[1].changes_requested_count == 2
    assert prs[1].reviews_count == 4
    # No formal review action on PR #2 -> changes_requested_count stays 0.
    assert prs[2].changes_requested_count == 0
    assert prs[2].reviews_count == 1

    # Reviews themselves are still persisted for git_pull_request_reviews.
    persisted_reviews = [r for batch in store.review_batches for r in batch]
    assert len(persisted_reviews) == 5
    first_review = persisted_reviews[0]
    assert first_review.review_id == "101"
    assert first_review.reviewer == "carol"
    assert first_review.state == "CHANGES_REQUESTED"
    assert first_review.submitted_at == datetime(2020, 1, 2, tzinfo=timezone.utc)
