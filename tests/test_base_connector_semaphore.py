"""Assert GitConnector base class exposes an asyncio.Semaphore with max_workers permits."""

from __future__ import annotations

import asyncio

import pytest

from dev_health_ops.connectors.base import GitConnector


class _Dummy(GitConnector):
    """Minimal concrete subclass for testing the base connector."""

    def list_organizations(self, max_orgs=None):  # pragma: no cover - unused
        return []

    def list_repositories(  # pragma: no cover - unused
        self,
        org_name=None,
        user_name=None,
        search=None,
        pattern=None,
        max_repos=None,
    ):
        return []

    def get_contributors(  # pragma: no cover - unused
        self, owner, repo, max_contributors=None
    ):
        return []

    def get_commit_stats(self, owner, repo, sha):  # pragma: no cover - unused
        raise NotImplementedError

    def get_repo_stats(  # pragma: no cover - unused
        self, owner, repo, max_commits=None
    ):
        raise NotImplementedError

    def get_pull_requests(  # pragma: no cover - unused
        self, owner, repo, state="all", max_prs=None
    ):
        return []

    def get_file_blame(self, owner, repo, path, ref="HEAD"):  # pragma: no cover - unused
        raise NotImplementedError

    def close(self):  # pragma: no cover - unused
        return None


def test_semaphore_created_with_max_workers():
    c = _Dummy(per_page=25, max_workers=7)
    sem = c.concurrency_semaphore
    assert isinstance(sem, asyncio.Semaphore)
    # BoundedSemaphore/Semaphore expose ._value on CPython
    assert sem._value == 7


@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    c = _Dummy(per_page=10, max_workers=2)
    active = 0
    peak = 0

    async def worker():
        nonlocal active, peak
        async with c.concurrency_semaphore:
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.02)
            finally:
                active -= 1

    await asyncio.gather(*(worker() for _ in range(8)))
    assert peak == 2
