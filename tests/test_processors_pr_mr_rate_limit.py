import asyncio
import uuid

import pytest

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


class _FakeGithub:
    def __init__(self, repo):
        self._repo = repo

    def get_repo(self, _full_name: str):
        return self._repo


class _RetryAfterException(Exception):
    def __init__(self, headers):
        super().__init__("rate limited")
        self.headers = headers


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
        if isinstance(item, Exception):
            raise item
        return item


class _FakePRUser:
    def __init__(self, login="octo", email=None):
        self.login = login
        self.email = email


class _FakePRRef:
    def __init__(self, ref):
        self.ref = ref


class _FakePR:
    def __init__(self, number: int):
        self.number = number
        self.title = f"PR {number}"
        self.state = "closed"
        self.user = _FakePRUser()
        self.created_at = None
        self.merged_at = None
        self.closed_at = None
        self.head = _FakePRRef("feature")
        self.base = _FakePRRef("main")


class _FakeRepo:
    def __init__(self, pull_items):
        self._pull_items = pull_items

    def get_pulls(self, state="all"):
        assert state == "all"
        return _PRIter(self._pull_items)


@pytest.mark.asyncio
async def test_github_pr_sync_retries_on_retry_after_and_persists():
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()

    # First call hits a Retry-After style limit, then we yield two PRs.
    fake_repo = _FakeRepo(
        [
            _RetryAfterException({"Retry-After": "0"}),
            _FakePR(1),
            _FakePR(2),
        ]
    )

    class _Connector:
        def __init__(self):
            self.github = _FakeGithub(fake_repo)

    connector = _Connector()
    gate = _NoSleepGate()

    total = await loop.run_in_executor(
        None,
        _sync_github_prs_to_store,
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
    assert gate.penalties and gate.penalties[0] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_gitlab_mr_sync_retries_on_retry_after_and_persists():
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()

    class _RateLimit(Exception):
        def __init__(self, retry_after_seconds):
            super().__init__("rate limited")
            self.retry_after_seconds = retry_after_seconds

    class _Rest:
        def __init__(self):
            self.calls = 0

        def get_merge_requests(
            self,
            project_id=None,
            state=None,
            page=None,
            per_page=None,
            **_kwargs,
        ):
            assert project_id is not None
            assert page is not None
            assert per_page is not None
            assert state == "all"
            self.calls += 1
            if self.calls == 1:
                raise _RateLimit(0)
            if self.calls == 2:
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
            return []

    class _Connector:
        def __init__(self):
            self.per_page = 100
            self.rest_client = _Rest()

    connector = _Connector()
    gate = _NoSleepGate()

    total = await loop.run_in_executor(
        None,
        _sync_gitlab_mrs_to_store,
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
    assert gate.penalties and gate.penalties[0] == 0.0
