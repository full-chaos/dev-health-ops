import asyncio
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation (mirrors tests/test_deployment_pr_inference.py).
import dev_health_ops.connectors  # noqa: F401
import dev_health_ops.processors.gitlab as gitlab_processor
from dev_health_ops.processors.github import _sync_github_prs_to_store
from dev_health_ops.processors.gitlab import _sync_gitlab_mrs_to_store


class _NoSleepGate:
    def __init__(self):
        self.penalties = []

    def wait_sync(self) -> None:
        return

    def penalize(self, delay_seconds=None) -> float:
        self.penalties.append(delay_seconds)
        return float(delay_seconds or 0)

    def reset(self) -> None:
        return


class _FakeStore:
    def __init__(self):
        self.pr_batches = []

    async def insert_git_pull_requests(self, batch):
        # store a shallow copy to avoid later mutation surprises
        self.pr_batches.append(list(batch))


def _fake_pr(number: int):
    return SimpleNamespace(
        number=number,
        title=f"PR {number}",
        body=None,
        state="closed",
        author_login="octo",
        created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        merged_at=None,
        closed_at=None,
        head_ref="feature",
        base_ref="main",
        additions=0,
        deletions=0,
        changed_files=0,
        comments_count=0,
    )


class _FakeCodeClient:
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


class _NoReviewClient:
    def __init__(self, **_kwargs):
        self.graphql = None

    def iter_pr_reviews_batch(self, *, owner, repo, prs, limit, operation_family=None):
        for pr in prs:
            yield pr.number, ()

    def drain_usage_observations(self):
        return []


@pytest.mark.asyncio
async def test_github_pr_sync_uses_code_client_and_persists():
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()

    class _Connector:
        def __init__(self):
            self.github = SimpleNamespace(
                get_repo=lambda _full_name: (_ for _ in ()).throw(
                    AssertionError("legacy PyGithub get_repo must not be called")
                )
            )
            self.token = "token"
            self.per_page = 100
            self.graphql = object()

        def _rest_base_url(self):
            return "https://api.github.com"

    connector = _Connector()
    gate = _NoSleepGate()

    pulls = [_fake_pr(1), _fake_pr(2)]
    with (
        patch(
            "dev_health_ops.processors.github._github_code_client_from_connector",
            lambda _connector: _FakeCodeClient(pulls),
        ),
        patch("dev_health_ops.processors.github.GitHubWorkClient", _NoReviewClient),
    ):
        total = await loop.run_in_executor(
            None,
            cast(Any, _sync_github_prs_to_store),
            connector,
            "o",
            "r",
            repo_id,
            store,
            loop,
            1,  # batch_size
            "all",
            gate,
        )

    assert total == 2
    assert len(store.pr_batches) == 2
    assert [b[0].number for b in store.pr_batches] == [1, 2]
    assert gate.penalties == []


@pytest.mark.asyncio
async def test_gitlab_mr_sync_retries_on_retry_after_and_persists(
    monkeypatch: pytest.MonkeyPatch,
):
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()

    class _RetryAfter(Exception):
        def __init__(self):
            super().__init__("retry later")
            self.retry_after_seconds = 0

    class _CodeClient:
        def __init__(self):
            self.calls = 0
            self.pages: list[int] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get_merge_requests_page(
            self,
            *,
            project_id,
            page,
            state,
            per_page,
        ):
            assert project_id is not None
            assert per_page is not None
            assert state == "all"
            self.pages.append(page)
            self.calls += 1
            if self.calls == 1:
                return [
                    {
                        "iid": 7,
                        "title": "MR 7",
                        "state": "merged",
                        "created_at": "2020-01-01T00:00:00Z",
                        "merged_at": None,
                        "closed_at": None,
                        "source_branch": "feature",
                        "target_branch": "main",
                        "author": {"username": "alice"},
                    }
                ]
            if self.calls == 2:
                raise _RetryAfter()
            return []

        async def get_mr_approvals(self, project_id, iid):
            return {"approved_by": []}

        async def iter_mr_notes(self, project_id, iid, *, per_page):
            return []

        def drain_usage_observations(self):
            return []

    class _Connector:
        def __init__(self):
            self.per_page = 100

    client = _CodeClient()
    connector = _Connector()
    monkeypatch.setattr(
        gitlab_processor,
        "_gitlab_code_client_from_connector",
        lambda _connector: client,
    )
    gate = _NoSleepGate()

    total = await loop.run_in_executor(
        None,
        cast(Any, _sync_gitlab_mrs_to_store),
        connector,
        123,
        repo_id,
        store,
        loop,
        1,  # batch_size
        "all",
        gate,
    )

    assert total == 1
    assert len(store.pr_batches) == 1
    assert store.pr_batches[0][0].number == 7
    assert client.pages == [1, 2, 2]
    assert gate.penalties and gate.penalties[0] == 0.0
